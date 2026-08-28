"""Development-only repository contract for the fixed Atlanta six-area plan.

The browser-facing caller supplies only an immutable in-memory bundle and fixed
workflow fields.  Table names, paths, and SQL are never caller controlled.  The
repository stores versioned plan data beside the legacy runtime masters; it does
not delete or update ``common_region_master``.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import stat
import sys
import zipfile
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Mapping
from pathlib import Path

from tools.data.atlanta_6area_plan import (
    BOUNDARY_PENALTY_COST,
    BOUNDARY_POLICY_FILENAME,
    EXPECTED_AMBIGUOUS_POSTALS,
    EXPECTED_TECHNICIAN_ROWS,
    FIXED_REGION_FILENAME,
    MANIFEST_FILENAME,
    MANIFEST_SCHEMA,
    PLAN_ID,
    POLICY_VERSION,
    STRATEGIC_CITY_NAME,
    TECHNICIAN_POLICY_FILENAME,
    ZONE_TO_SEQ,
    Atlanta6AreaPlanError,
    derive_atlanta_6area_plan_identity,
    is_allowed_atlanta_6area_plan_id,
    preview_atlanta_6area_plan,
    validate_atlanta_6area_bundle,
)


SUBSIDIARY_NAME = "LGEAI"
SOURCE_STRATEGIC_CITY_NAME = "Atlanta, GA"
# Keep the persisted context version aligned with the validated, PII-redacted
# bundle contract.  The import never accepts technician names from a bundle.
SCHEMA_VERSION = MANIFEST_SCHEMA
EXPECTED_BUNDLE_FILES = frozenset(
    {
        FIXED_REGION_FILENAME,
        BOUNDARY_POLICY_FILENAME,
        TECHNICIAN_POLICY_FILENAME,
        MANIFEST_FILENAME,
    }
)
MAX_BUNDLE_BYTES = 32 * 1024 * 1024
MANAGED_BUNDLE_DATASET = "fixed_region_plan_bundle"
MANAGED_BUNDLE_FILENAME = "payload.zip"
MIGRATION_ID = "V001__atlanta_6area_region_plan"
MIGRATIONS_ROOT = Path(__file__).resolve().parent / "migrations"
MIGRATION_REGISTRY = MIGRATIONS_ROOT / "manifest.json"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_TECH_ID_RE = re.compile(r"^AI\d{6}$")
GENERIC_LIFECYCLE_CONTRACT = "region-plan-lifecycle-request/v1"
GENERIC_POLICY_VERSION = "active_roster_area_type_fallback_region_soft/v1"
GENERIC_POLICY_ALLOWLIST = frozenset({
    "home_distance_only",
    "preferred_region_soft",
    "active_roster_area_type_fallback_region_soft/v1",
    "active_roster_type_hard_region_soft/v1",
    "explicit_workbook_membership/v1",
    "own_region_with_approved_boundary_overflow/v2",
})
GENERIC_SCHEMA_VERSION = "region-workbook-import/v1"
# Legacy test fixture/export compatibility; lifecycle validation is dynamic.
GENERIC_EXPECTED_COUNTS = (6, 413, 54, 0)

# These are deliberately the complete operational master rows, excluding only
# database-maintained audit timestamps.  The reviewed plan stores stable IDs
# and region policy only; activation copies the current source roster into the
# scenario master so runtime never falls back to an unreviewed base-city row.
_TECHNICIAN_SYNC_COLUMNS = (
    "employee_code",
    "employee_name",
    "center_type",
    "home_address",
    "home_city",
    "home_state",
    "home_country",
    "home_postal_code",
    "home_latitude",
    "home_longitude",
    "active_flag",
    "priority_group",
    "max_home_to_job_min",
)
_CAPABILITY_SYNC_COLUMNS = (
    "employee_code",
    "product_group_code",
    "product_code",
    "repair_allowed",
    "heavy_repair_allowed",
    "priority_score",
    "effective_start_date",
    "effective_end_date",
)


class RegionPlanContractError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class ValidatedRegionPlanBundle:
    plan_id: str
    source_sha256: str
    manifest_sha256: str
    bundle_sha256: str
    artifact_sha256: Mapping[str, str]
    regions: tuple[tuple[int, str, str, str], ...]
    postals: tuple[tuple[str, int, str, int], ...]
    technicians: tuple[tuple[str, int, str], ...]
    boundary_resolutions: tuple[tuple[str, int, int, bool, int | None, str], ...]
    row_accounting: Mapping[str, int]
    verification_only: bool = True

    def safe_summary(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "strategic_city_name": STRATEGIC_CITY_NAME,
            "source_sha256": self.source_sha256,
            "manifest_sha256": self.manifest_sha256,
            "bundle_sha256": self.bundle_sha256,
            "verification_only": self.verification_only,
            "promotable": False,
            "approval_status": "resolved_for_development_verification",
            "lifecycle_stage": "resolved_candidate",
            **dict(self.row_accounting),
        }


@dataclass(frozen=True)
class CandidateImportResult:
    status: str
    plan_id: str
    revision: int
    bundle_sha256: str
    verification_only: bool = True


@dataclass(frozen=True)
class ReviewResult:
    status: str
    plan_id: str
    revision: int
    verification_only: bool = True


@dataclass(frozen=True)
class ActivationPreview:
    plan_id: str
    plan_revision: int
    expected_activation_revision: int
    current_active_plan_id: str | None
    preview_digest: str
    checksum: str
    region_count: int
    postal_count: int
    technician_count: int
    boundary_resolution_count: int
    verification_only: bool = True


@dataclass(frozen=True)
class ActivationResult:
    status: str
    plan_id: str
    activation_revision: int
    preview_digest: str
    verification_only: bool = True


@dataclass(frozen=True)
class GenericPlanIdentity:
    subsidiary_name: str
    strategic_city_name: str
    source_strategic_city_name: str
    plan_id: str
    policy_version: str
    source_sha256: str
    manifest_sha256: str
    bundle_sha256: str
    region_count: int
    postal_count: int
    technician_count: int
    boundary_resolution_count: int


@dataclass(frozen=True)
class GenericActivationPreview:
    identity: GenericPlanIdentity
    plan_revision: int
    expected_activation_revision: int
    current_active_plan_id: str | None
    source_roster_digest: str
    preview_digest: str


def _generic_identity(request: Mapping[str, Any]) -> GenericPlanIdentity:
    if str(request.get("contract_version", "")) != GENERIC_LIFECYCLE_CONTRACT:
        raise RegionPlanContractError("LIFECYCLE_CONTRACT_INVALID")
    subsidiary = _require_token(str(request.get("subsidiary_name", "")), "SUBSIDIARY_INVALID")
    target = _require_context_name(request.get("strategic_city_name", ""), "STRATEGIC_CITY_INVALID")
    source = _require_context_name(request.get("source_strategic_city_name", ""), "SOURCE_CITY_INVALID")
    plan_id = _require_token(str(request.get("plan_id", "")), "PLAN_ID_NOT_ALLOWED")
    policy = str(request.get("policy_version", "")).strip()
    if target == STRATEGIC_CITY_NAME or policy not in GENERIC_POLICY_ALLOWLIST:
        raise RegionPlanContractError("GENERIC_POLICY_NOT_ALLOWED")
    hashes = []
    for key in ("source_sha256", "manifest_sha256", "bundle_sha256"):
        value = str(request.get(key, "")).strip().lower()
        if not _SHA256_RE.fullmatch(value):
            raise RegionPlanContractError("PLAN_CHECKSUM_INVALID")
        hashes.append(value)
    try:
        counts = tuple(int(request[key]) for key in (
            "region_count", "postal_count", "technician_count", "boundary_resolution_count"
        ))
    except (KeyError, TypeError, ValueError) as exc:
        raise RegionPlanContractError("PLAN_ROW_COUNTS_INVALID") from exc
    if any(value < 0 for value in counts) or not all(counts[:3]):
        raise RegionPlanContractError("PLAN_ROW_COUNTS_INVALID")
    return GenericPlanIdentity(
        subsidiary, target, source, plan_id, policy, *hashes, *counts
    )


def _generic_roster_digest(master_rows: tuple[tuple[Any, ...], ...], capability_rows: tuple[tuple[Any, ...], ...]) -> str:
    payload = json.dumps(
        {"master": master_rows, "capabilities": capability_rows},
        sort_keys=True, separators=(",", ":"), default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _generic_preview_digest(
    identity: GenericPlanIdentity, plan_revision: int, activation_revision: int,
    active_plan_id: str | None, roster_digest: str,
) -> str:
    payload = {
        **identity.__dict__, "plan_revision": plan_revision,
        "activation_revision": activation_revision,
        "current_active_plan_id": active_plan_id,
        "source_roster_digest": roster_digest,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


ConnectionFactory = Callable[[str, str], Any]


def _require_development(environment: str, dbname: str) -> None:
    if str(environment).strip().lower() != "development":
        raise RegionPlanContractError("PRODUCTION_WRITE_DISABLED")
    if str(dbname).strip().lower() != "vrp_db_dev":
        raise RegionPlanContractError("DATABASE_TARGET_MISMATCH")


def _require_token(value: str, code: str) -> str:
    text = str(value).strip()
    if not _TOKEN_RE.fullmatch(text):
        raise RegionPlanContractError(code)
    return text


def _require_context_name(value: object, code: str) -> str:
    text = str(value).strip()
    if not text or len(text) > 128 or any(ord(ch) < 32 for ch in text):
        raise RegionPlanContractError(code)
    return text


def _require_plan_id(value: object) -> str:
    plan_id = str(value).strip()
    if not is_allowed_atlanta_6area_plan_id(plan_id):
        raise RegionPlanContractError("PLAN_ID_NOT_ALLOWED")
    return plan_id


def _read_csv(payload: bytes) -> list[dict[str, str]]:
    try:
        return list(csv.DictReader(io.StringIO(payload.decode("utf-8-sig"), newline="")))
    except Exception as exc:
        raise RegionPlanContractError("BUNDLE_CSV_INVALID") from exc


def _read_bundle(bundle_bytes: bytes) -> dict[str, bytes]:
    if not isinstance(bundle_bytes, bytes) or not bundle_bytes or len(bundle_bytes) > MAX_BUNDLE_BYTES:
        raise RegionPlanContractError("BUNDLE_SIZE_INVALID")
    try:
        with zipfile.ZipFile(io.BytesIO(bundle_bytes)) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)) or set(names) != EXPECTED_BUNDLE_FILES:
                raise RegionPlanContractError("BUNDLE_FILES_INVALID")
            if archive.testzip() is not None:
                raise RegionPlanContractError("BUNDLE_CRC_INVALID")
            payloads: dict[str, bytes] = {}
            for name in names:
                info = archive.getinfo(name)
                if info.is_dir() or info.file_size > MAX_BUNDLE_BYTES:
                    raise RegionPlanContractError("BUNDLE_FILE_INVALID")
                payloads[name] = archive.read(name)
            return payloads
    except RegionPlanContractError:
        raise
    except Exception as exc:
        raise RegionPlanContractError("BUNDLE_ZIP_INVALID") from exc


def validate_region_plan_bundle(bundle_bytes: bytes) -> ValidatedRegionPlanBundle:
    try:
        validate_atlanta_6area_bundle(bundle_bytes)
    except Atlanta6AreaPlanError as exc:
        if exc.code in {
            "BUNDLE_PLAN_ID_INVALID",
            "BUNDLE_RESOLUTION_DIGEST_INVALID",
            "BUNDLE_PLAN_IDENTITY_INVALID",
        }:
            code = "BUNDLE_PLAN_IDENTITY_INVALID"
        else:
            code = exc.code if exc.code.startswith("BUNDLE_") else f"BUNDLE_{exc.code}"
        raise RegionPlanContractError(code) from exc
    payloads = _read_bundle(bundle_bytes)
    try:
        manifest = json.loads(payloads[MANIFEST_FILENAME].decode("utf-8"))
    except Exception as exc:
        raise RegionPlanContractError("BUNDLE_MANIFEST_INVALID") from exc
    if not isinstance(manifest, dict):
        raise RegionPlanContractError("BUNDLE_MANIFEST_INVALID")
    if (
        manifest.get("schema") != MANIFEST_SCHEMA
        or manifest.get("strategic_city_name") != STRATEGIC_CITY_NAME
        or manifest.get("promotable") is not False
        or manifest.get("approval_status") != "resolved_for_development_verification"
        or manifest.get("verification_only") is not True
        or manifest.get("lifecycle_stage") != "resolved_candidate"
    ):
        raise RegionPlanContractError("BUNDLE_MANIFEST_CONTRACT_INVALID")
    plan_id = _require_plan_id(manifest.get("plan_id"))
    source = manifest.get("source")
    if not isinstance(source, dict) or not _SHA256_RE.fullmatch(str(source.get("sha256", ""))):
        raise RegionPlanContractError("BUNDLE_SOURCE_CHECKSUM_INVALID")
    raw_resolutions = manifest.get("boundary_resolutions")
    if not isinstance(raw_resolutions, dict) or set(raw_resolutions) != set(EXPECTED_AMBIGUOUS_POSTALS):
        raise RegionPlanContractError("BOUNDARY_RESOLUTION_SET_INVALID")
    try:
        resolution_digest, expected_plan_id = derive_atlanta_6area_plan_identity(
            str(source["sha256"]), raw_resolutions
        )
    except Atlanta6AreaPlanError as exc:
        raise RegionPlanContractError("BUNDLE_PLAN_IDENTITY_INVALID") from exc
    if (
        plan_id != expected_plan_id
        or manifest.get("resolution_digest") != resolution_digest
        or manifest.get("schema_version") != MANIFEST_SCHEMA
        or manifest.get("policy_version") != POLICY_VERSION
    ):
        raise RegionPlanContractError("BUNDLE_PLAN_IDENTITY_INVALID")

    artifact_hashes: dict[str, str] = {}
    artifact_manifest = manifest.get("artifacts")
    if not isinstance(artifact_manifest, dict):
        raise RegionPlanContractError("BUNDLE_ARTIFACT_MANIFEST_INVALID")
    for name in (FIXED_REGION_FILENAME, BOUNDARY_POLICY_FILENAME, TECHNICIAN_POLICY_FILENAME):
        metadata = artifact_manifest.get(name)
        actual = hashlib.sha256(payloads[name]).hexdigest()
        if not isinstance(metadata, dict) or metadata.get("sha256") != actual:
            raise RegionPlanContractError("BUNDLE_ARTIFACT_CHECKSUM_INVALID")
        artifact_hashes[name] = actual

    fixed_rows = _read_csv(payloads[FIXED_REGION_FILENAME])
    if len(fixed_rows) != 297:
        raise RegionPlanContractError("FIXED_REGION_ROW_COUNT_INVALID")
    postals: list[tuple[str, int, str, int]] = []
    regions: dict[int, tuple[int, str, str, str]] = {}
    seen_postals: set[str] = set()
    for row in fixed_rows:
        postal = str(row.get("POSTAL_CODE", "")).strip()
        territory = str(row.get("AREA_NAME", "")).strip()
        area_type = str(row.get("area_type", "")).strip().upper() or "DMS"
        try:
            region_seq = int(str(row.get("region_seq", "")).strip())
        except ValueError as exc:
            raise RegionPlanContractError("FIXED_REGION_VALUE_INVALID") from exc
        expected_seq = ZONE_TO_SEQ.get(territory)
        expected_id = f"atlanta_6area_r{region_seq:02d}"
        expected_name = f"Atlanta_6area {territory}"
        if (
            not re.fullmatch(r"[0-9]{5}", postal)
            or postal in seen_postals
            or row.get("STRATEGIC_CITY_NAME") != STRATEGIC_CITY_NAME
            or area_type not in {"DMS", "DMS2"}
            or expected_seq != region_seq
            or row.get("region_id") != expected_id
            or row.get("new_region_name") != expected_name
        ):
            raise RegionPlanContractError("FIXED_REGION_VALUE_INVALID")
        seen_postals.add(postal)
        membership_count = 2 if postal in EXPECTED_AMBIGUOUS_POSTALS else 1
        postals.append((postal, region_seq, area_type, membership_count))
        regions[region_seq] = (region_seq, expected_id, expected_name, territory)
    if set(regions) != set(ZONE_TO_SEQ.values()):
        raise RegionPlanContractError("FIXED_REGION_REGION_SET_INVALID")

    technician_rows = _read_csv(payloads[TECHNICIAN_POLICY_FILENAME])
    technicians: list[tuple[str, int, str]] = []
    tech_ids: set[str] = set()
    for row in technician_rows:
        employee_code = str(row.get("SVC_ENGINEER_CODE", "")).strip()
        policy_mode = str(row.get("policy_mode", "")).strip()
        try:
            region_seq = int(str(row.get("assigned_region_seq", "")).strip())
        except ValueError as exc:
            raise RegionPlanContractError("TECHNICIAN_POLICY_VALUE_INVALID") from exc
        if (
            not _TECH_ID_RE.fullmatch(employee_code)
            or employee_code in tech_ids
            or region_seq not in ZONE_TO_SEQ.values()
            or policy_mode != "assigned_region_boundary_spillover"
            or row.get("STRATEGIC_CITY_NAME") != STRATEGIC_CITY_NAME
            or row.get("plan_id") != plan_id
        ):
            raise RegionPlanContractError("TECHNICIAN_POLICY_VALUE_INVALID")
        tech_ids.add(employee_code)
        technicians.append((employee_code, region_seq, policy_mode))
    if len(tech_ids) != EXPECTED_TECHNICIAN_ROWS:
        raise RegionPlanContractError("TECHNICIAN_POLICY_SET_INVALID")

    decisions: list[tuple[str, int, int, bool, int | None, str]] = []
    for postal in EXPECTED_AMBIGUOUS_POSTALS:
        decision = raw_resolutions[postal]
        if not isinstance(decision, dict):
            raise RegionPlanContractError("BOUNDARY_RESOLUTION_VALUE_INVALID")
        primary = decision.get("primary_region")
        alternate = decision.get("alternate_region")
        allow = decision.get("allow_overflow")
        penalty = decision.get("penalty_cost")
        rationale = str(decision.get("rationale", ""))
        if (
            primary not in {"Zone 2", "Zone 3"}
            or alternate != ("Zone 3" if primary == "Zone 2" else "Zone 2")
            or not isinstance(allow, bool)
            or penalty != (BOUNDARY_PENALTY_COST if allow else None)
        ):
            raise RegionPlanContractError("BOUNDARY_RESOLUTION_VALUE_INVALID")
        primary_seq = int(ZONE_TO_SEQ[primary])
        alternate_seq = int(ZONE_TO_SEQ[alternate])
        postal_owner = next(item[1] for item in postals if item[0] == postal)
        if postal_owner != primary_seq:
            raise RegionPlanContractError("BOUNDARY_PRIMARY_OWNER_MISMATCH")
        decisions.append((postal, primary_seq, alternate_seq, allow, penalty, rationale))

    boundary_rows = _read_csv(payloads[BOUNDARY_POLICY_FILENAME])
    expected_overflow = {item[0]: item for item in decisions if item[3]}
    if len(boundary_rows) != len(expected_overflow):
        raise RegionPlanContractError("BOUNDARY_POLICY_ROW_COUNT_INVALID")
    seen_boundary: set[str] = set()
    for row in boundary_rows:
        postal = str(row.get("POSTAL_CODE", "")).strip()
        expected = expected_overflow.get(postal)
        if expected is None or postal in seen_boundary:
            raise RegionPlanContractError("BOUNDARY_POLICY_VALUE_INVALID")
        try:
            actual_values = (
                int(str(row.get("primary_region_seq", ""))),
                int(str(row.get("alternate_region_seq", ""))),
                int(str(row.get("penalty_cost", ""))),
            )
        except ValueError as exc:
            raise RegionPlanContractError("BOUNDARY_POLICY_VALUE_INVALID") from exc
        if (
            actual_values != (expected[1], expected[2], BOUNDARY_PENALTY_COST)
            or row.get("plan_id") != plan_id
        ):
            raise RegionPlanContractError("BOUNDARY_POLICY_VALUE_INVALID")
        seen_boundary.add(postal)

    accounting = manifest.get("row_accounting")
    membership = accounting.get("membership") if isinstance(accounting, dict) else None
    technician = accounting.get("technician") if isinstance(accounting, dict) else None
    if (
        not isinstance(membership, dict)
        or membership.get("input") != 301
        or membership.get("accepted") != 301
        or membership.get("rejected") != 0
        or membership.get("unique_postals") != 297
        or membership.get("ambiguous_postals") != 4
        or not isinstance(technician, dict)
        or technician.get("input") != 14
        or technician.get("accepted") != 14
        or technician.get("rejected") != 0
    ):
        raise RegionPlanContractError("ROW_ACCOUNTING_INVALID")
    row_accounting = MappingProxyType(
        {
            "membership_input_rows": 301,
            "membership_accepted_rows": 301,
            "membership_rejected_rows": 0,
            "unique_postal_count": 297,
            "technician_count": 14,
            "ambiguous_postal_count": 4,
        }
    )
    return ValidatedRegionPlanBundle(
        plan_id=plan_id,
        source_sha256=str(source["sha256"]),
        manifest_sha256=hashlib.sha256(payloads[MANIFEST_FILENAME]).hexdigest(),
        bundle_sha256=hashlib.sha256(bundle_bytes).hexdigest(),
        artifact_sha256=MappingProxyType(artifact_hashes),
        regions=tuple(regions[index] for index in sorted(regions)),
        postals=tuple(postals),
        technicians=tuple(sorted(technicians)),
        boundary_resolutions=tuple(decisions),
        row_accounting=row_accounting,
    )


def preview_candidate_import(bundle_bytes: bytes) -> dict[str, Any]:
    plan = validate_region_plan_bundle(bundle_bytes)
    return {
        "status": "ready",
        "write_allowed": False,
        "target_environment": "development",
        "lifecycle_stage": "resolved_candidate",
        "verification_only": True,
        "promotable": False,
        **plan.safe_summary(),
    }


def _config_target(config_path: Path | str) -> tuple[dict[str, Any], str, str]:
    path = Path(config_path).expanduser().resolve()
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RegionPlanContractError("CONFIG_INVALID") from exc
    database = config.get("database") if isinstance(config, dict) else None
    environment = str(config.get("environment", "")).strip().lower() if isinstance(config, dict) else ""
    dbname = str(database.get("dbname", "")).strip().lower() if isinstance(database, dict) else ""
    expected = {"development": "vrp_db_dev", "production": "vrp_db"}.get(environment)
    if expected is None or dbname != expected or not isinstance(database, dict):
        raise RegionPlanContractError("DATABASE_TARGET_MISMATCH")
    return database, environment, dbname


def _connect_config(config_path: Path | str) -> tuple[Any, str, str]:
    database, environment, dbname = _config_target(config_path)
    try:
        import psycopg2

        connection = psycopg2.connect(
            host=database["host"],
            port=int(database.get("port", 5432)),
            dbname=dbname,
            user=database["user"],
            password=database.get("password", ""),
            connect_timeout=10,
        )
    except RegionPlanContractError:
        raise
    except Exception as exc:
        raise RuntimeError("REGION_PLAN_DATABASE_UNAVAILABLE") from exc
    return connection, environment, dbname


def _query_dicts(cursor: Any, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cursor.execute(sql, params)
    description = cursor.description or ()
    columns = [str(item[0]) for item in description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_active_plan_snapshot(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path | str,
) -> dict[str, Any]:
    """Return the active immutable plan snapshot; production access is read-only."""

    if str(subsidiary_name).strip() != SUBSIDIARY_NAME or str(strategic_city_name).strip() != STRATEGIC_CITY_NAME:
        raise RegionPlanContractError("PLAN_CONTEXT_NOT_ALLOWED")
    connection, environment, _dbname = _connect_config(config_path)
    if environment == "production":
        connection.close()
        return {
            "enabled": False,
            "status": "development_verification_only",
            "context_status": "inactive",
            "plan_id": None,
            "verification_only": True,
            "promotable": False,
            "postals": [],
            "technicians": [],
            "boundary_overflow": [],
            "regions": [],
        }
    try:
        with connection.cursor() as cursor:
            cursor.execute("set transaction read only")
            headers = _query_dicts(
                cursor,
                """
                select
                    c.context_status,
                    p.plan_status as status,
                    p.plan_id,
                    p.revision,
                    p.policy_version,
                    p.verification_only,
                    p.bundle_sha256 as checksum,
                    p.source_sha256,
                    p.manifest_sha256,
                    a.activation_revision,
                    a.activated_at,
                    a.activation_reference
                from common_region_plan_activation a
                join common_region_plan p
                  on p.subsidiary_name=a.subsidiary_name
                 and p.strategic_city_name=a.strategic_city_name
                 and p.plan_id=a.plan_id
                join common_city_context c
                  on c.subsidiary_name=a.subsidiary_name
                 and c.strategic_city_name=a.strategic_city_name
                where a.subsidiary_name=%s and a.strategic_city_name=%s
                  and a.active_flag and p.plan_status='active' and c.context_status='active'
                """,
                (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME),
            )
            if not headers:
                connection.rollback()
                return {
                    "enabled": False,
                    "status": "inactive",
                    "context_status": "inactive",
                    "plan_id": None,
                    "postals": [],
                    "technicians": [],
                    "boundary_overflow": [],
                    "regions": [],
                }
            header = headers[0]
            if header.get("verification_only") is not True:
                raise RegionPlanContractError("ACTIVE_PLAN_VERIFICATION_CONTRACT_INVALID")
            checksum = str(header.get("checksum") or "")
            if not _SHA256_RE.fullmatch(checksum):
                raise RegionPlanContractError("ACTIVE_PLAN_CHECKSUM_INVALID")
            plan_id = str(header["plan_id"])
            scoped = (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan_id)
            regions = _query_dicts(
                cursor,
                """
                select region_seq, region_id, region_name, source_territory
                from common_region_plan_region
                where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
                order by region_seq
                """,
                scoped,
            )
            postals = _query_dicts(
                cursor,
                """
                select p.postal_code, p.region_seq, r.region_id, r.region_name,
                       p.area_type, p.source_membership_count, p.resolution_status
                from common_region_plan_postal p
                join common_region_plan_region r
                  on r.subsidiary_name=p.subsidiary_name
                 and r.strategic_city_name=p.strategic_city_name
                 and r.plan_id=p.plan_id and r.region_seq=p.region_seq
                where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s
                order by p.postal_code
                """,
                scoped,
            )
            technicians = _query_dicts(
                cursor,
                """
                select t.employee_code, ''::text as employee_name, t.assigned_region_seq,
                       r.region_id as assigned_region_id,
                       r.region_name as assigned_region_name,
                       t.policy_mode, t.active_flag
                from common_region_plan_technician t
                join common_region_plan_region r
                  on r.subsidiary_name=t.subsidiary_name
                 and r.strategic_city_name=t.strategic_city_name
                 and r.plan_id=t.plan_id and r.region_seq=t.assigned_region_seq
                where t.subsidiary_name=%s and t.strategic_city_name=%s and t.plan_id=%s
                order by t.employee_code
                """,
                scoped,
            )
            boundary = _query_dicts(
                cursor,
                """
                select postal_code, primary_region_seq, alternate_region_seq,
                       allow_overflow, penalty_cost, rationale, policy_version
                from common_region_plan_boundary_overflow
                where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
                  and allow_overflow
                order by postal_code
                """,
                scoped,
            )
            if len(regions) != 6 or len(postals) != 297 or len(technicians) != 14:
                raise RegionPlanContractError("ACTIVE_PLAN_ROW_COUNTS_INVALID")
        connection.rollback()
        return {
            "enabled": True,
            "verification_only": True,
            "promotable": False,
            "approval_status": "resolved_for_development_verification",
            "lifecycle_stage": "resolved_candidate",
            **header,
            "checksum": checksum,
            "regions": regions,
            "postals": postals,
            "technicians": technicians,
            "boundary_overflow": boundary,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def list_region_plans(config_path: Path | str) -> dict[str, Any]:
    connection, _environment, _dbname = _connect_config(config_path)
    try:
        with connection.cursor() as cursor:
            cursor.execute("set transaction read only")
            rows = _query_dicts(
                cursor,
                """
                select plan_id, plan_status as status, revision, policy_version,
                       source_sha256, bundle_sha256 as checksum, created_at,
                       reviewed_at, unique_postal_count, technician_count,
                       ambiguous_postal_count
                from common_region_plan
                where subsidiary_name=%s and strategic_city_name=%s
                order by created_at desc, plan_id
                """,
                (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME),
            )
        connection.rollback()
        return {"plans": rows}
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _activation_digest(
    plan_id: str,
    plan_revision: int,
    activation_revision: int,
    current_active_plan_id: str | None,
    checksums: tuple[str, str, str],
    counts: tuple[int, int, int, int],
) -> str:
    payload = json.dumps(
        {
            "plan_id": plan_id,
            "plan_revision": plan_revision,
            "activation_revision": activation_revision,
            "current_active_plan_id": current_active_plan_id,
            "checksums": checksums,
            "counts": counts,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_source_technicians(
    cursor: Any, technicians: tuple[tuple[str, int, str], ...]
) -> None:
    """Validate submitted codes against the source master without reading PII."""

    submitted_codes = tuple(row[0] for row in technicians)
    cursor.execute(
        """
        select employee_code, active_flag
        from common_technician_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code
        """,
        (SUBSIDIARY_NAME, SOURCE_STRATEGIC_CITY_NAME, list(submitted_codes)),
    )
    rows = cursor.fetchall()
    by_code: dict[str, bool] = {}
    for employee_code, active_flag in rows:
        code = str(employee_code).strip()
        if code in by_code:
            raise RegionPlanContractError("TECHNICIAN_MASTER_DUPLICATE")
        by_code[code] = bool(active_flag)
    if set(by_code) != set(submitted_codes):
        raise RegionPlanContractError("TECHNICIAN_MASTER_MISSING")
    if any(not by_code[code] for code in submitted_codes):
        raise RegionPlanContractError("TECHNICIAN_MASTER_INACTIVE")


def _activation_selected_technician_codes(cursor: Any, plan_id: str) -> tuple[str, ...]:
    """Return the immutable reviewed-plan roster, never attendance inputs."""

    cursor.execute(
        """
        select employee_code
        from common_region_plan_technician
        where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
        order by employee_code
        for share
        """,
        (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan_id),
    )
    codes = tuple(str(row[0]).strip() for row in cursor.fetchall())
    if (
        len(codes) != 14
        or len(set(codes)) != len(codes)
        or any(not _TECH_ID_RE.fullmatch(code) for code in codes)
    ):
        raise RegionPlanContractError("PLAN_TECHNICIAN_SET_INVALID")
    return codes


def _activation_source_roster(
    cursor: Any, selected_codes: tuple[str, ...]
) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[Any, ...], ...]]:
    """Read and validate the active source roster under the activation lock.

    Only master and capability policy are synchronized.  Request-scoped
    technician/attendance rows intentionally never participate in activation.
    """

    cursor.execute(
        """
        select employee_code, employee_name, center_type, home_address, home_city,
               home_state, home_country, home_postal_code, home_latitude,
               home_longitude, active_flag, priority_group, max_home_to_job_min
        from common_technician_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code
        for share
        """,
        (SUBSIDIARY_NAME, SOURCE_STRATEGIC_CITY_NAME, list(selected_codes)),
    )
    master_rows = tuple(tuple(row) for row in cursor.fetchall())
    master_by_code: dict[str, tuple[Any, ...]] = {}
    for row in master_rows:
        code = str(row[0]).strip()
        if code in master_by_code:
            raise RegionPlanContractError("TECHNICIAN_MASTER_DUPLICATE")
        master_by_code[code] = row
    if set(master_by_code) != set(selected_codes):
        raise RegionPlanContractError("TECHNICIAN_MASTER_MISSING")
    if any(not bool(row[10]) for row in master_by_code.values()):
        raise RegionPlanContractError("TECHNICIAN_MASTER_INACTIVE")

    cursor.execute(
        """
        select employee_code, product_group_code, product_code, repair_allowed,
               heavy_repair_allowed, priority_score, effective_start_date,
               effective_end_date
        from common_technician_capability_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code, product_group_code, product_code
        for share
        """,
        (SUBSIDIARY_NAME, SOURCE_STRATEGIC_CITY_NAME, list(selected_codes)),
    )
    capability_rows = tuple(tuple(row) for row in cursor.fetchall())
    capability_keys: set[tuple[Any, ...]] = set()
    capability_codes: set[str] = set()
    for row in capability_rows:
        key = tuple(row[:3])
        if key in capability_keys:
            raise RegionPlanContractError("TECHNICIAN_CAPABILITY_DUPLICATE")
        capability_keys.add(key)
        capability_codes.add(str(row[0]).strip())
    if not capability_codes.issubset(set(selected_codes)):
        raise RegionPlanContractError("TECHNICIAN_CAPABILITY_SOURCE_INVALID")
    if set(selected_codes) - capability_codes:
        raise RegionPlanContractError("TECHNICIAN_CAPABILITY_MISSING")
    return master_rows, capability_rows


def _synchronize_activation_technician_roster(cursor: Any, plan_id: str) -> None:
    """Synchronize the reviewed roster to the Atlanta_6area runtime masters.

    This is called inside ``apply_activation``'s transaction.  It reads source
    master/address and capability policy afresh, replaces selected target
    capabilities exactly, and verifies the resulting target rows before plan
    status can change.
    """

    selected_codes = _activation_selected_technician_codes(cursor, plan_id)
    source_master_rows, source_capability_rows = _activation_source_roster(
        cursor, selected_codes
    )

    cursor.execute(
        """
        insert into common_technician_master (
            subsidiary_name, strategic_city_name, employee_code, employee_name,
            center_type, home_address, home_city, home_state, home_country,
            home_postal_code, home_latitude, home_longitude, active_flag,
            priority_group, max_home_to_job_min
        )
        select %s, %s, employee_code, employee_name, center_type, home_address,
               home_city, home_state, home_country, home_postal_code,
               home_latitude, home_longitude, active_flag, priority_group,
               max_home_to_job_min
        from common_technician_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code
        on conflict (subsidiary_name, strategic_city_name, employee_code)
        do update set
            employee_name=excluded.employee_name,
            center_type=excluded.center_type,
            home_address=excluded.home_address,
            home_city=excluded.home_city,
            home_state=excluded.home_state,
            home_country=excluded.home_country,
            home_postal_code=excluded.home_postal_code,
            home_latitude=excluded.home_latitude,
            home_longitude=excluded.home_longitude,
            active_flag=excluded.active_flag,
            priority_group=excluded.priority_group,
            max_home_to_job_min=excluded.max_home_to_job_min,
            updated_at=now()
        """,
        (
            SUBSIDIARY_NAME,
            STRATEGIC_CITY_NAME,
            SUBSIDIARY_NAME,
            SOURCE_STRATEGIC_CITY_NAME,
            list(selected_codes),
        ),
    )
    # Delete-and-copy is intentional: source is the full authority for every
    # selected capability set, including removals.  It is safe because source
    # coverage was checked above and the whole operation is one transaction.
    cursor.execute(
        """
        delete from common_technician_capability_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        """,
        (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, list(selected_codes)),
    )
    cursor.execute(
        """
        insert into common_technician_capability_master (
            subsidiary_name, strategic_city_name, employee_code,
            product_group_code, product_code, repair_allowed,
            heavy_repair_allowed, priority_score, effective_start_date,
            effective_end_date
        )
        select %s, %s, employee_code, product_group_code, product_code,
               repair_allowed, heavy_repair_allowed, priority_score,
               effective_start_date, effective_end_date
        from common_technician_capability_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code, product_group_code, product_code
        """,
        (
            SUBSIDIARY_NAME,
            STRATEGIC_CITY_NAME,
            SUBSIDIARY_NAME,
            SOURCE_STRATEGIC_CITY_NAME,
            list(selected_codes),
        ),
    )

    cursor.execute(
        """
        select employee_code, employee_name, center_type, home_address, home_city,
               home_state, home_country, home_postal_code, home_latitude,
               home_longitude, active_flag, priority_group, max_home_to_job_min
        from common_technician_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code
        """,
        (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, list(selected_codes)),
    )
    target_master_rows = tuple(tuple(row) for row in cursor.fetchall())
    if target_master_rows != source_master_rows:
        raise RegionPlanContractError("ACTIVATION_TARGET_MASTER_INVALID")

    cursor.execute(
        """
        select employee_code, product_group_code, product_code, repair_allowed,
               heavy_repair_allowed, priority_score, effective_start_date,
               effective_end_date
        from common_technician_capability_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code, product_group_code, product_code
        """,
        (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, list(selected_codes)),
    )
    target_capability_rows = tuple(tuple(row) for row in cursor.fetchall())
    target_capability_codes = {str(row[0]).strip() for row in target_capability_rows}
    if set(selected_codes) - target_capability_codes:
        raise RegionPlanContractError("ACTIVATION_TARGET_CAPABILITY_MISSING")
    if target_capability_rows != source_capability_rows:
        raise RegionPlanContractError("ACTIVATION_TARGET_CAPABILITY_INVALID")


class GenericRegionPlanLifecycleRepository:
    """Transactional lifecycle for V004 non-Atlanta region-plan candidates."""

    def __init__(self, connection_factory: ConnectionFactory):
        self._connection_factory = connection_factory

    @staticmethod
    def _begin(cursor: Any, identity: GenericPlanIdentity) -> None:
        cursor.execute("set transaction isolation level serializable")
        cursor.execute(
            "select pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"region-plan:{identity.subsidiary_name}:{identity.strategic_city_name}",),
        )

    @staticmethod
    def _load_candidate(cursor: Any, identity: GenericPlanIdentity, *, allowed_statuses: tuple[str, ...]) -> tuple[int, int, str | None]:
        cursor.execute(
            """select exists(select 1 from information_schema.columns where table_schema='public'
                       and table_name='common_region_plan_region' and column_name='required_center_type')
                      and exists(select 1 from pg_constraint where conrelid='public.common_region_plan_technician'::regclass
                       and conname='common_region_plan_technician_policy_mode_v004_check')"""
        )
        schema_row = cursor.fetchone()
        if schema_row is None or schema_row[0] is not True:
            raise RegionPlanContractError("V004_SCHEMA_REQUIRED")
        cursor.execute(
            """
            select p.plan_status, p.revision, p.schema_version, p.policy_version,
                   p.source_sha256, p.manifest_sha256, p.bundle_sha256,
                   p.membership_accepted_rows, p.membership_rejected_rows,
                   p.unique_postal_count, p.technician_count,
                   c.source_strategic_city_name, c.policy_version, c.activation_revision
            from common_region_plan p
            join common_city_context c using (subsidiary_name, strategic_city_name)
            where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s
            for update
            """,
            (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id),
        )
        row = cursor.fetchone()
        if row is None:
            raise RegionPlanContractError("PLAN_NOT_FOUND")
        if str(row[0]) not in allowed_statuses:
            raise RegionPlanContractError("PLAN_STATUS_INVALID")
        if (
            str(row[2]) != GENERIC_SCHEMA_VERSION
            or str(row[3]) != identity.policy_version
            or tuple(str(value or "") for value in row[4:7])
            != (identity.source_sha256, identity.manifest_sha256, identity.bundle_sha256)
            or (int(row[7]), int(row[8]), int(row[9]), int(row[10])) != (identity.postal_count, 0, identity.postal_count, identity.technician_count)
            or str(row[11]) != identity.source_strategic_city_name
        ):
            raise RegionPlanContractError("PLAN_IDENTITY_MISMATCH")
        cursor.execute(
            """
            select
              (select count(*) from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
              (select count(*) from common_region_plan_postal where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
              (select count(*) from common_region_plan_technician where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and active_flag),
              (select count(*) from common_region_plan_boundary_overflow where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
              (select count(*) from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and (required_center_type is null or required_center_type not in ('DMS','DMS2'))),
              (select count(*) from common_region_plan_postal p join common_region_plan_region r using (subsidiary_name,strategic_city_name,plan_id,region_seq)
                 where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s and p.area_type<>r.required_center_type)
            """,
            (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id) * 6,
        )
        counts = tuple(int(value) for value in cursor.fetchone())
        if counts[:4] != (identity.region_count, identity.postal_count, identity.technician_count, identity.boundary_resolution_count):
            raise RegionPlanContractError("PLAN_ROW_COUNTS_INVALID")
        if counts[4:] != (0, 0):
            raise RegionPlanContractError("REGION_REQUIRED_CENTER_TYPE_INVALID")
        cursor.execute(
            """select a.plan_id, p.policy_version
               from common_region_plan_activation a
               join common_region_plan p using (subsidiary_name,strategic_city_name,plan_id)
               where a.subsidiary_name=%s and a.strategic_city_name=%s and a.active_flag for update""",
            (identity.subsidiary_name, identity.strategic_city_name),
        )
        active = cursor.fetchone()
        context_policy = str(row[12])
        if active is not None:
            if context_policy != str(active[1]):
                raise RegionPlanContractError("CONTEXT_POLICY_ACTIVE_PLAN_MISMATCH")
        elif context_policy != identity.policy_version:
            raise RegionPlanContractError("CONTEXT_POLICY_INVALID")
        return int(row[1]), int(row[13]), str(active[0]) if active else None

    @staticmethod
    def _source_roster(cursor: Any, identity: GenericPlanIdentity) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[Any, ...], ...]]:
        cursor.execute(
            """
            select m.employee_code, m.employee_name, m.center_type, m.home_address,
                   m.home_city, m.home_state, m.home_country, m.home_postal_code,
                   m.home_latitude, m.home_longitude, m.active_flag, m.priority_group,
                   m.max_home_to_job_min
            from common_region_plan_technician t
            join common_technician_master m on m.subsidiary_name=t.subsidiary_name
              and m.strategic_city_name=%s and m.employee_code=t.employee_code
            join common_region_plan_region r on r.subsidiary_name=t.subsidiary_name
              and r.strategic_city_name=t.strategic_city_name and r.plan_id=t.plan_id
              and r.region_seq=t.assigned_region_seq
            where t.subsidiary_name=%s and t.strategic_city_name=%s and t.plan_id=%s
              and t.active_flag and m.active_flag
              and m.center_type=r.required_center_type
            order by m.employee_code for share
            """,
            (identity.source_strategic_city_name, identity.subsidiary_name, identity.strategic_city_name, identity.plan_id),
        )
        masters = tuple(tuple(row) for row in cursor.fetchall())
        if len(masters) != identity.technician_count or len({str(row[0]) for row in masters}) != len(masters):
            raise RegionPlanContractError("SOURCE_ROSTER_INVALID")
        codes = [str(row[0]) for row in masters]
        cursor.execute(
            """select employee_code, product_group_code, product_code, repair_allowed,
                      heavy_repair_allowed, priority_score, effective_start_date, effective_end_date
               from common_technician_capability_master
               where subsidiary_name=%s and strategic_city_name=%s and employee_code=any(%s)
               order by employee_code, product_group_code, product_code for share""",
            (identity.subsidiary_name, identity.source_strategic_city_name, codes),
        )
        capabilities = tuple(tuple(row) for row in cursor.fetchall())
        if not capabilities or {str(row[0]) for row in capabilities} != set(codes):
            raise RegionPlanContractError("SOURCE_CAPABILITY_INVALID")
        return masters, capabilities

    def review(self, request: Mapping[str, Any], *, environment: str, dbname: str) -> ReviewResult:
        _require_development(environment, dbname)
        identity = _generic_identity(request)
        expected = int(request.get("expected_plan_revision", -1))
        expected_activation = int(request.get("expected_activation_revision", -1))
        actor = _require_token(str(request.get("reviewed_by", "")), "REVIEWED_BY_INVALID")
        reference = _require_token(str(request.get("review_reference", "")), "REVIEW_REFERENCE_INVALID")
        connection = self._connection_factory("development", "vrp_db_dev")
        try:
            with connection.cursor() as cursor:
                self._begin(cursor, identity)
                revision, _activation_revision, _active = self._load_candidate(cursor, identity, allowed_statuses=("candidate",))
                if revision != expected or _activation_revision != expected_activation:
                    raise RegionPlanContractError("PLAN_REVIEW_REVISION_CONFLICT")
                self._source_roster(cursor, identity)
                cursor.execute(
                    """update common_region_plan set plan_status='reviewed', reviewed_by=%s,
                              review_reference=%s, reviewed_at=now(), revision=revision+1, updated_at=now()
                       where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
                         and plan_status='candidate' and revision=%s returning revision""",
                    (actor, reference, identity.subsidiary_name, identity.strategic_city_name, identity.plan_id, expected),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RegionPlanContractError("PLAN_REVIEW_REVISION_CONFLICT")
            connection.commit()
            return ReviewResult("reviewed", identity.plan_id, int(row[0]))
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def preview(self, request: Mapping[str, Any], *, environment: str, dbname: str) -> GenericActivationPreview:
        _require_development(environment, dbname)
        identity = _generic_identity(request)
        expected_plan = int(request.get("expected_plan_revision", -1))
        expected_activation = int(request.get("expected_activation_revision", -1))
        connection = self._connection_factory("development", "vrp_db_dev")
        try:
            with connection.cursor() as cursor:
                self._begin(cursor, identity)
                revision, activation_revision, active = self._load_candidate(
                    cursor, identity, allowed_statuses=("reviewed", "superseded")
                )
                if revision != expected_plan or activation_revision != expected_activation:
                    raise RegionPlanContractError("ACTIVATION_PREVIEW_STALE")
                masters, capabilities = self._source_roster(cursor, identity)
                roster_digest = _generic_roster_digest(masters, capabilities)
                digest = _generic_preview_digest(identity, revision, activation_revision, active, roster_digest)
            connection.rollback()
            return GenericActivationPreview(identity, revision, activation_revision, active, roster_digest, digest)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def activate(self, request: Mapping[str, Any], *, environment: str, dbname: str) -> ActivationResult:
        _require_development(environment, dbname)
        identity = _generic_identity(request)
        preview_digest = str(request.get("preview_digest", ""))
        if not _SHA256_RE.fullmatch(preview_digest):
            raise RegionPlanContractError("ACTIVATION_PREVIEW_INVALID")
        actor = _require_token(str(request.get("activated_by", "")), "ACTIVATED_BY_INVALID")
        reference = _require_token(str(request.get("activation_reference", "")), "ACTIVATION_REFERENCE_INVALID")
        idem = _require_token(str(request.get("idempotency_key", "")), "IDEMPOTENCY_KEY_INVALID")
        expected_plan = int(request.get("expected_plan_revision", -1))
        expected_activation = int(request.get("expected_activation_revision", -1))
        connection = self._connection_factory("development", "vrp_db_dev")
        try:
            with connection.cursor() as cursor:
                self._begin(cursor, identity)
                cursor.execute(
                    """select plan_id, plan_revision, preview_digest, activation_revision,
                              activated_by, activation_reference
                       from common_region_plan_activation
                       where subsidiary_name=%s and strategic_city_name=%s and idempotency_key=%s for update""",
                    (identity.subsidiary_name, identity.strategic_city_name, idem),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if (
                        str(existing[0]) != identity.plan_id
                        or int(existing[1]) != expected_plan
                        or str(existing[2]) != preview_digest
                        or int(existing[3]) != expected_activation + 1
                        or str(existing[4]) != actor
                        or str(existing[5]) != reference
                    ):
                        raise RegionPlanContractError("ACTIVATION_IDEMPOTENCY_CONFLICT")
                    cursor.execute(
                        """select activation_revision from common_city_context
                           where subsidiary_name=%s and strategic_city_name=%s for update""",
                        (identity.subsidiary_name, identity.strategic_city_name),
                    )
                    current_context = cursor.fetchone()
                    cursor.execute(
                        """select plan_id, activation_revision from common_region_plan_activation
                           where subsidiary_name=%s and strategic_city_name=%s and active_flag for update""",
                        (identity.subsidiary_name, identity.strategic_city_name),
                    )
                    current_activation = cursor.fetchone()
                    if (
                        current_context is None
                        or int(current_context[0]) != int(existing[3])
                        or current_activation is None
                        or str(current_activation[0]) != identity.plan_id
                        or int(current_activation[1]) != int(existing[3])
                    ):
                        raise RegionPlanContractError("ACTIVATION_IDEMPOTENCY_STALE")
                    connection.rollback()
                    return ActivationResult(
                        "already_active", identity.plan_id, int(existing[3]), preview_digest
                    )
                revision, activation_revision, active = self._load_candidate(
                    cursor, identity, allowed_statuses=("reviewed", "superseded")
                )
                masters, capabilities = self._source_roster(cursor, identity)
                actual = _generic_preview_digest(identity, revision, activation_revision, active, _generic_roster_digest(masters, capabilities))
                if revision != expected_plan or activation_revision != expected_activation or actual != preview_digest:
                    raise RegionPlanContractError("ACTIVATION_PREVIEW_STALE")
                # Runtime projections are replaced exactly inside this transaction.
                cursor.execute("delete from common_region_master where subsidiary_name=%s and strategic_city_name=%s", (identity.subsidiary_name, identity.strategic_city_name))
                cursor.execute(
                    """insert into common_region_master (subsidiary_name,strategic_city_name,postal_code,region_seq,region_name,area_type)
                       select p.subsidiary_name,p.strategic_city_name,p.postal_code,p.region_seq,r.region_name,p.area_type
                       from common_region_plan_postal p join common_region_plan_region r using (subsidiary_name,strategic_city_name,plan_id,region_seq)
                       where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s""",
                    (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id),
                )
                if cursor.rowcount != identity.postal_count:
                    raise RegionPlanContractError("ACTIVATION_REGION_PROJECTION_INVALID")
                cursor.execute("delete from common_technician_capability_master where subsidiary_name=%s and strategic_city_name=%s", (identity.subsidiary_name, identity.strategic_city_name))
                cursor.execute("delete from common_technician_master where subsidiary_name=%s and strategic_city_name=%s", (identity.subsidiary_name, identity.strategic_city_name))
                cursor.executemany(
                    """insert into common_technician_master (subsidiary_name,strategic_city_name,employee_code,employee_name,center_type,home_address,home_city,home_state,home_country,home_postal_code,home_latitude,home_longitude,active_flag,priority_group,max_home_to_job_min)
                       values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [(identity.subsidiary_name, identity.strategic_city_name, *row) for row in masters],
                )
                if cursor.rowcount != identity.technician_count:
                    raise RegionPlanContractError("ACTIVATION_TECHNICIAN_PROJECTION_INVALID")
                cursor.executemany(
                    """insert into common_technician_capability_master (subsidiary_name,strategic_city_name,employee_code,product_group_code,product_code,repair_allowed,heavy_repair_allowed,priority_score,effective_start_date,effective_end_date)
                       values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [(identity.subsidiary_name, identity.strategic_city_name, *row) for row in capabilities],
                )
                next_revision = activation_revision + 1
                cursor.execute("update common_region_plan_activation set active_flag=false,superseded_at=now() where subsidiary_name=%s and strategic_city_name=%s and active_flag", (identity.subsidiary_name, identity.strategic_city_name))
                cursor.execute("update common_region_plan set plan_status='superseded',revision=revision+1,updated_at=now() where subsidiary_name=%s and strategic_city_name=%s and plan_status='active' and plan_id<>%s", (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id))
                cursor.execute("update common_region_plan set plan_status='active',revision=revision+1,updated_at=now() where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and plan_status in ('reviewed','superseded') and revision=%s", (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id, revision))
                if cursor.rowcount != 1:
                    raise RegionPlanContractError("ACTIVATION_PLAN_REVISION_CONFLICT")
                cursor.execute("update common_city_context set policy_version=%s,activation_revision=%s,context_status='active',updated_at=now() where subsidiary_name=%s and strategic_city_name=%s and activation_revision=%s", (identity.policy_version, next_revision, identity.subsidiary_name, identity.strategic_city_name, activation_revision))
                if cursor.rowcount != 1:
                    raise RegionPlanContractError("ACTIVATION_REVISION_CONFLICT")
                cursor.execute(
                    """insert into common_region_plan_activation (subsidiary_name,strategic_city_name,activation_revision,plan_id,plan_revision,preview_digest,idempotency_key,activated_by,activation_reference)
                       values (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (identity.subsidiary_name, identity.strategic_city_name, next_revision, identity.plan_id, revision, preview_digest, idem, actor, reference),
                )
            connection.commit()
            return ActivationResult("activated", identity.plan_id, next_revision, preview_digest)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


class Atlanta6AreaPlanRepository:
    def __init__(self, connection_factory: ConnectionFactory):
        if not callable(connection_factory):
            raise TypeError("connection_factory must be callable")
        self._connection_factory = connection_factory

    def import_candidate(
        self,
        bundle_bytes: bytes,
        *,
        environment: str,
        dbname: str,
        imported_by: str,
        idempotency_key: str,
    ) -> CandidateImportResult:
        _require_development(environment, dbname)
        actor = _require_token(imported_by, "IMPORTED_BY_INVALID")
        idem = _require_token(idempotency_key, "IDEMPOTENCY_KEY_INVALID")
        plan = validate_region_plan_bundle(bundle_bytes)
        connection = self._connection_factory("development", "vrp_db_dev")
        try:
            with connection.cursor() as cursor:
                # The source context is part of the signed v2 bundle contract.
                # Validate every code in this transaction so a master change cannot
                # leave a partially imported plan or put PII in plan artifacts.
                _validate_source_technicians(cursor, plan.technicians)
                cursor.execute(
                    """
                    insert into common_city_context (
                        subsidiary_name, strategic_city_name, source_strategic_city_name,
                        context_version, policy_version, context_status
                    ) values (%s, %s, %s, %s, %s, 'candidate')
                    on conflict (subsidiary_name, strategic_city_name) do nothing
                    """,
                    (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, SOURCE_STRATEGIC_CITY_NAME,
                     SCHEMA_VERSION, POLICY_VERSION),
                )
                cursor.execute(
                    """
                    insert into common_region_plan (
                        subsidiary_name, strategic_city_name, plan_id, schema_version,
                        policy_version, source_file_name, source_sha256, manifest_sha256,
                        bundle_sha256, fixed_region_sha256, boundary_policy_sha256,
                        technician_policy_sha256, membership_input_rows,
                        membership_accepted_rows, membership_rejected_rows,
                        unique_postal_count, technician_count, ambiguous_postal_count,
                        import_idempotency_key, imported_by
                    ) values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    on conflict (subsidiary_name, strategic_city_name, plan_id) do update
                    set updated_at = common_region_plan.updated_at
                    where common_region_plan.source_sha256 = excluded.source_sha256
                      and common_region_plan.manifest_sha256 = excluded.manifest_sha256
                      and common_region_plan.bundle_sha256 = excluded.bundle_sha256
                    returning revision
                    """,
                    (
                        SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan.plan_id, SCHEMA_VERSION,
                        POLICY_VERSION, "New ATL Buckets.xlsx", plan.source_sha256,
                        plan.manifest_sha256, plan.bundle_sha256,
                        plan.artifact_sha256[FIXED_REGION_FILENAME],
                        plan.artifact_sha256[BOUNDARY_POLICY_FILENAME],
                        plan.artifact_sha256[TECHNICIAN_POLICY_FILENAME],
                        301, 301, 0, 297, 14, 4, idem, actor,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted is None:
                    raise RegionPlanContractError("PLAN_ID_CHECKSUM_CONFLICT")
                revision = int(inserted[0])
                cursor.executemany(
                    """
                    insert into common_region_plan_region (
                        subsidiary_name, strategic_city_name, plan_id, region_seq,
                        region_id, region_name, source_territory
                    ) values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict (subsidiary_name, strategic_city_name, plan_id, region_seq)
                    do nothing
                    """,
                    [
                        (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan.plan_id, *row)
                        for row in plan.regions
                    ],
                )
                cursor.executemany(
                    """
                    insert into common_region_plan_postal (
                        subsidiary_name, strategic_city_name, plan_id, postal_code,
                        region_seq, area_type, source_membership_count, resolution_status,
                        source_region_seqs, resolution_metadata
                    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                    on conflict (subsidiary_name, strategic_city_name, plan_id, postal_code)
                    do nothing
                    """,
                    [
                        (
                            SUBSIDIARY_NAME,
                            STRATEGIC_CITY_NAME,
                            plan.plan_id,
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            "resolved" if row[3] == 2 else "not_required",
                            json.dumps([2, 3] if row[3] == 2 else [row[1]]),
                            json.dumps(
                                {"source": "approved_bundle"} if row[3] == 2 else None
                            ),
                        )
                        for row in plan.postals
                    ],
                )
                cursor.executemany(
                    """
                    insert into common_region_plan_technician (
                        subsidiary_name, strategic_city_name, plan_id, employee_code,
                        assigned_region_seq, policy_mode
                    ) values (%s, %s, %s, %s, %s, %s)
                    on conflict (subsidiary_name, strategic_city_name, plan_id, employee_code)
                    do nothing
                    """,
                    [
                        (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan.plan_id, *row)
                        for row in plan.technicians
                    ],
                )
                cursor.executemany(
                    """
                    insert into common_region_plan_boundary_overflow (
                        subsidiary_name, strategic_city_name, plan_id, postal_code,
                        primary_region_seq, alternate_region_seq, allow_overflow,
                        penalty_cost, rationale, policy_version
                    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (subsidiary_name, strategic_city_name, plan_id, postal_code)
                    do nothing
                    """,
                    [
                        (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan.plan_id, *row, POLICY_VERSION)
                        for row in plan.boundary_resolutions
                    ],
                )
                cursor.execute(
                    """
                    select
                        (select count(*) from common_region_plan_region
                         where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
                        (select count(*) from common_region_plan_postal
                         where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
                        (select count(*) from common_region_plan_technician
                         where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
                        (select count(*) from common_region_plan_boundary_overflow
                         where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s)
                    """,
                    (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan.plan_id) * 4,
                )
                if tuple(int(value) for value in cursor.fetchone()) != (6, 297, 14, 4):
                    raise RegionPlanContractError("IMPORTED_ROW_COUNTS_INVALID")
            connection.commit()
            return CandidateImportResult(
                "candidate_imported_for_development_verification",
                plan.plan_id,
                revision,
                plan.bundle_sha256,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def review_plan(
        self,
        *,
        environment: str,
        dbname: str,
        plan_id: str,
        expected_revision: int,
        reviewed_by: str,
        review_reference: str,
    ) -> ReviewResult:
        _require_development(environment, dbname)
        plan_id = _require_plan_id(plan_id)
        actor = _require_token(reviewed_by, "REVIEWED_BY_INVALID")
        reference = _require_token(review_reference, "REVIEW_REFERENCE_INVALID")
        connection = self._connection_factory("development", "vrp_db_dev")
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    update common_region_plan
                    set plan_status='reviewed', reviewed_by=%s, review_reference=%s,
                        reviewed_at=now(), revision=revision+1, updated_at=now()
                    where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
                      and plan_status='candidate' and revision=%s
                    returning revision
                    """,
                    (actor, reference, SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan_id, int(expected_revision)),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RegionPlanContractError("PLAN_REVIEW_REVISION_CONFLICT")
                revision = int(row[0])
                cursor.execute(
                    """
                    update common_city_context
                    set context_status='reviewed', updated_at=now()
                    where subsidiary_name=%s and strategic_city_name=%s
                    """,
                    (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME),
                )
            connection.commit()
            return ReviewResult(
                "reviewed_for_development_verification", plan_id, revision
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _load_activation_state(
        cursor: Any, *, plan_id: str, for_update: bool
    ) -> tuple[Any, ...]:
        suffix = " for update" if for_update else ""
        cursor.execute(
            """
            select plan_status, revision, source_sha256, manifest_sha256, bundle_sha256
            from common_region_plan
            where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
            """ + suffix,
            (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan_id),
        )
        plan_row = cursor.fetchone()
        if plan_row is None or str(plan_row[0]) not in {"reviewed", "active"}:
            raise RegionPlanContractError("PLAN_NOT_REVIEWED")
        cursor.execute(
            """
            select activation_revision from common_city_context
            where subsidiary_name=%s and strategic_city_name=%s
            """ + suffix,
            (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME),
        )
        context_row = cursor.fetchone()
        if context_row is None:
            raise RegionPlanContractError("CITY_CONTEXT_MISSING")
        cursor.execute(
            """
            select plan_id from common_region_plan_activation
            where subsidiary_name=%s and strategic_city_name=%s and active_flag
            """ + suffix,
            (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME),
        )
        active_row = cursor.fetchone()
        cursor.execute(
            """
            select
                (select count(*) from common_region_plan_region
                 where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
                (select count(*) from common_region_plan_postal
                 where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
                (select count(*) from common_region_plan_technician
                 where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),
                (select count(*) from common_region_plan_boundary_overflow
                 where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s)
            """,
            (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan_id) * 4,
        )
        counts = tuple(int(value) for value in cursor.fetchone())
        if counts != (6, 297, 14, 4):
            raise RegionPlanContractError("PLAN_ROW_COUNTS_INVALID")
        return plan_row, int(context_row[0]), (str(active_row[0]) if active_row else None), counts

    def preview_activation(
        self, *, environment: str, dbname: str, plan_id: str
    ) -> ActivationPreview:
        _require_development(environment, dbname)
        plan_id = _require_plan_id(plan_id)
        connection = self._connection_factory("development", "vrp_db_dev")
        try:
            with connection.cursor() as cursor:
                plan_row, activation_revision, active_plan, counts = self._load_activation_state(
                    cursor, plan_id=plan_id, for_update=False
                )
            digest = _activation_digest(
                plan_id, int(plan_row[1]), activation_revision, active_plan,
                (str(plan_row[2]), str(plan_row[3]), str(plan_row[4])), counts,
            )
            connection.rollback()
            return ActivationPreview(
                plan_id=plan_id,
                plan_revision=int(plan_row[1]),
                expected_activation_revision=activation_revision,
                current_active_plan_id=active_plan,
                preview_digest=digest,
                checksum=str(plan_row[4]),
                region_count=counts[0],
                postal_count=counts[1],
                technician_count=counts[2],
                boundary_resolution_count=counts[3],
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def apply_activation(
        self,
        preview: ActivationPreview,
        *,
        environment: str,
        dbname: str,
        activated_by: str,
        activation_reference: str,
        idempotency_key: str,
    ) -> ActivationResult:
        _require_development(environment, dbname)
        plan_id = _require_plan_id(preview.plan_id)
        if not _SHA256_RE.fullmatch(preview.preview_digest):
            raise RegionPlanContractError("ACTIVATION_PREVIEW_INVALID")
        actor = _require_token(activated_by, "ACTIVATED_BY_INVALID")
        reference = _require_token(activation_reference, "ACTIVATION_REFERENCE_INVALID")
        idem = _require_token(idempotency_key, "IDEMPOTENCY_KEY_INVALID")
        connection = self._connection_factory("development", "vrp_db_dev")
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select activation_revision, plan_id, preview_digest
                    from common_region_plan_activation
                    where subsidiary_name=%s and strategic_city_name=%s and idempotency_key=%s
                    """,
                    (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, idem),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if str(existing[1]) != plan_id or str(existing[2]) != preview.preview_digest:
                        raise RegionPlanContractError("ACTIVATION_IDEMPOTENCY_CONFLICT")
                    connection.rollback()
                    return ActivationResult(
                        "already_active_for_development_verification",
                        plan_id,
                        int(existing[0]),
                        preview.preview_digest,
                    )
                plan_row, activation_revision, active_plan, counts = self._load_activation_state(
                    cursor, plan_id=plan_id, for_update=True
                )
                actual_digest = _activation_digest(
                    plan_id, int(plan_row[1]), activation_revision, active_plan,
                    (str(plan_row[2]), str(plan_row[3]), str(plan_row[4])), counts,
                )
                if (
                    activation_revision != preview.expected_activation_revision
                    or int(plan_row[1]) != preview.plan_revision
                    or actual_digest != preview.preview_digest
                ):
                    raise RegionPlanContractError("ACTIVATION_PREVIEW_STALE")
                # Activation makes the scenario runtime-visible.  Refresh its
                # selected roster from the active source in this same
                # transaction before any plan/context state changes.
                _synchronize_activation_technician_roster(cursor, plan_id)
                next_revision = activation_revision + 1
                cursor.execute(
                    """
                    update common_city_context
                    set activation_revision=%s, context_status='active', updated_at=now()
                    where subsidiary_name=%s and strategic_city_name=%s
                      and activation_revision=%s
                    returning activation_revision
                    """,
                    (next_revision, SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, activation_revision),
                )
                if cursor.fetchone() is None:
                    raise RegionPlanContractError("ACTIVATION_REVISION_CONFLICT")
                cursor.execute(
                    """
                    update common_region_plan_activation
                    set active_flag=false, superseded_at=now()
                    where subsidiary_name=%s and strategic_city_name=%s and active_flag
                    """,
                    (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME),
                )
                cursor.execute(
                    """
                    update common_region_plan
                    set plan_status='superseded', revision=revision+1, updated_at=now()
                    where subsidiary_name=%s and strategic_city_name=%s
                      and plan_status='active' and plan_id<>%s
                    """,
                    (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan_id),
                )
                cursor.execute(
                    """
                    update common_region_plan
                    set plan_status='active', revision=revision+1, updated_at=now()
                    where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s
                      and revision=%s
                    """,
                    (SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, plan_id, preview.plan_revision),
                )
                if cursor.rowcount != 1:
                    raise RegionPlanContractError("ACTIVATION_PLAN_REVISION_CONFLICT")
                cursor.execute(
                    """
                    insert into common_region_plan_activation (
                        subsidiary_name, strategic_city_name, activation_revision, plan_id,
                        plan_revision, preview_digest, idempotency_key, activated_by,
                        activation_reference
                    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, next_revision, plan_id,
                        preview.plan_revision, preview.preview_digest, idem, actor, reference,
                    ),
                )
            connection.commit()
            return ActivationResult(
                "activated_for_development_verification",
                plan_id,
                next_revision,
                preview.preview_digest,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def _repository_from_config(config_path: Path | str) -> tuple[Atlanta6AreaPlanRepository, str, str]:
    _database, environment, dbname = _config_target(config_path)

    def factory(requested_environment: str, requested_dbname: str):
        if requested_environment != environment or requested_dbname != dbname:
            raise RegionPlanContractError("DATABASE_TARGET_MISMATCH")
        connection, actual_environment, actual_dbname = _connect_config(config_path)
        if actual_environment != environment or actual_dbname != dbname:
            connection.close()
            raise RegionPlanContractError("DATABASE_TARGET_MISMATCH")
        return connection

    return Atlanta6AreaPlanRepository(factory), environment, dbname


def dispatch(
    operation: str,
    payload: Mapping[str, Any],
    *,
    config_path: Path | str,
) -> dict[str, Any]:
    """Fixed API bridge; no caller-controlled table, SQL, or filesystem path."""

    name = str(operation).strip().replace("-", "_")
    request = dict(payload or {})
    if name == "active":
        return get_active_plan_snapshot(
            str(request.get("subsidiary_name", SUBSIDIARY_NAME)),
            str(request.get("strategic_city_name", STRATEGIC_CITY_NAME)),
            config_path,
        )
    if name == "list":
        return list_region_plans(config_path)
    if name == "get":
        plan_id = _require_plan_id(request.get("plan_id", ""))
        snapshot = get_active_plan_snapshot(SUBSIDIARY_NAME, STRATEGIC_CITY_NAME, config_path)
        if snapshot.get("enabled") is not True or snapshot.get("plan_id") != plan_id:
            raise RegionPlanContractError("ACTIVE_PLAN_NOT_FOUND")
        return snapshot

    repository, environment, dbname = _repository_from_config(config_path)
    if request.get("contract_version") == GENERIC_LIFECYCLE_CONTRACT:
        generic = GenericRegionPlanLifecycleRepository(repository._connection_factory)
        if name == "review":
            result = generic.review(request, environment=environment, dbname=dbname)
            return {
                "contract_version": GENERIC_LIFECYCLE_CONTRACT,
                "status": "reviewed", "plan_id": result.plan_id,
                "revision": result.revision,
                "approval_status": "reviewed",
            }
        if name == "activation_preview":
            preview = generic.preview(request, environment=environment, dbname=dbname)
            return {
                "contract_version": GENERIC_LIFECYCLE_CONTRACT,
                "status": "ready", "plan_id": preview.identity.plan_id,
                "policy_version": preview.identity.policy_version,
                "plan_revision": preview.plan_revision,
                "expected_activation_revision": preview.expected_activation_revision,
                "current_active_plan_id": preview.current_active_plan_id,
                "source_roster_digest": preview.source_roster_digest,
                "preview_digest": preview.preview_digest,
                "checksum": preview.identity.bundle_sha256,
            }
        if name == "activate":
            result = generic.activate(request, environment=environment, dbname=dbname)
            return {
                "contract_version": GENERIC_LIFECYCLE_CONTRACT,
                "status": "already_active" if result.status == "already_active" else "activated",
                "plan_id": result.plan_id,
                "activation_revision": result.activation_revision,
                "preview_digest": result.preview_digest,
                "approval_status": "active",
            }
        raise RegionPlanContractError("REGION_PLAN_OPERATION_NOT_ALLOWED")
    if name == "activation_preview":
        preview = repository.preview_activation(
            environment=environment,
            dbname=dbname,
            plan_id=str(request.get("plan_id", PLAN_ID)),
        )
        return {
            "status": "ready",
            "verification_only": True,
            "promotable": False,
            "approval_status": "resolved_for_development_verification",
            "preview_id": preview.preview_digest,
            "preview_digest": preview.preview_digest,
            "checksum": preview.checksum,
            "plan_id": preview.plan_id,
            "plan_revision": preview.plan_revision,
            "expected_activation_revision": preview.expected_activation_revision,
            "current_active_plan_id": preview.current_active_plan_id,
            "region_count": preview.region_count,
            "postal_count": preview.postal_count,
            "technician_count": preview.technician_count,
            "boundary_resolution_count": preview.boundary_resolution_count,
        }
    if name == "review":
        result = repository.review_plan(
            environment=environment,
            dbname=dbname,
            plan_id=str(request.get("plan_id", "")),
            expected_revision=int(request.get("expected_revision", -1)),
            reviewed_by=str(request.get("reviewed_by", "")),
            review_reference=str(request.get("review_reference", "")),
        )
        return {
            "status": "reviewed",
            "plan_id": result.plan_id,
            "revision": result.revision,
            "verification_only": True,
            "promotable": False,
        }
    if name == "activate":
        current = repository.preview_activation(
            environment=environment, dbname=dbname, plan_id=str(request.get("plan_id", ""))
        )
        if (
            str(request.get("preview_id", "")) != current.preview_digest
            or str(request.get("preview_digest", "")) != current.preview_digest
            or str(request.get("checksum", "")) != current.checksum
            or int(request.get("expected_activation_revision", -1))
            != current.expected_activation_revision
        ):
            raise RegionPlanContractError("ACTIVATION_PREVIEW_STALE")
        result = repository.apply_activation(
            current,
            environment=environment,
            dbname=dbname,
            activated_by=str(request.get("activated_by", "")),
            activation_reference=str(request.get("activation_reference", "")),
            idempotency_key=str(request.get("idempotency_key", "")),
        )
        return {
            "status": (
                "already_active"
                if result.status.startswith("already_")
                else "activated"
            ),
            "plan_id": result.plan_id,
            "activation_revision": result.activation_revision,
            "preview_digest": result.preview_digest,
            "verification_only": True,
            "promotable": False,
        }
    if name in {"candidate", "import", "resolution", "validate"}:
        raise RegionPlanContractError("REGION_PLAN_OPERATION_NOT_IMPLEMENTED")
    raise RegionPlanContractError("REGION_PLAN_OPERATION_NOT_ALLOWED")


def _fixed_migration_backend():
    from admin_tools.db.release_backend import DatabaseReleaseBackend, MigrationSpec

    try:
        registry = json.loads(MIGRATION_REGISTRY.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RegionPlanContractError("MIGRATION_REGISTRY_INVALID") from exc
    entries = registry.get("migrations") if isinstance(registry, dict) else None
    if not isinstance(entries, list) or not entries or not all(isinstance(x, dict) for x in entries):
        raise RegionPlanContractError("MIGRATION_REGISTRY_INVALID")
    ids = [str(x.get("migration_id", "")) for x in entries]
    if len(ids) != len(set(ids)) or ids != sorted(ids):
        raise RegionPlanContractError("MIGRATION_REGISTRY_INVALID")
    item = next((x for x in entries if x.get("migration_id") == MIGRATION_ID), None)
    if item is None or item.get("sql_file") != f"{MIGRATION_ID}.sql":
        raise RegionPlanContractError("MIGRATION_NOT_ALLOWED")
    specs=[]
    for entry in entries:
        sql_path=(MIGRATIONS_ROOT / str(entry.get("sql_file", ""))).resolve()
        try: sql_path.relative_to(MIGRATIONS_ROOT.resolve())
        except ValueError as exc: raise RegionPlanContractError("MIGRATION_NOT_ALLOWED") from exc
        checksum=str(entry.get("checksum_sha256", "")).strip().lower()
        if not sql_path.is_file() or not _SHA256_RE.fullmatch(checksum) or hashlib.sha256(sql_path.read_bytes()).hexdigest()!=checksum:
            raise RegionPlanContractError("MIGRATION_CHECKSUM_INVALID")
        specs.append(MigrationSpec(migration_id=str(entry["migration_id"]),description=str(entry["description"]),sql_path=sql_path,checksum_sha256=checksum,rollback_instructions=str(entry["rollback_instructions"]),reversible=entry.get("reversible") is True,rollback_migration_id=entry.get("rollback_migration_id")))
    return DatabaseReleaseBackend(tuple(specs), migrations_root=MIGRATIONS_ROOT)


def preview_fixed_schema_migration(config_path: Path | str, migration_id: str = MIGRATION_ID) -> dict[str, Any]:
    _database, environment, dbname = _config_target(config_path)
    _require_development(environment, dbname)
    preview = _fixed_migration_backend().preview_migration(migration_id, config_path)
    return {
        "contract_version": "region-plan-migration/v1",
        "status": "ready",
        "environment": "development",
        "dbname": "vrp_db_dev",
        "target_id": "development:vrp_db_dev",
        "migration_id": migration_id,
        "checksum_sha256": preview.plan.checksum_sha256,
        "statement_count": preview.plan.statement_count,
        "statement_types": list(preview.plan.statement_types),
        "required_confirmation": preview.plan.required_confirmation,
        "rollback_instructions": preview.plan.rollback_instructions,
    }


def install_fixed_schema(
    config_path: Path | str, *, typed_confirmation: str, migration_id: str = MIGRATION_ID
) -> dict[str, Any]:
    _database, environment, dbname = _config_target(config_path)
    _require_development(environment, dbname)
    backend = _fixed_migration_backend()

    def connection_factory(_target):
        connection, actual_environment, actual_dbname = _connect_config(config_path)
        if actual_environment != "development" or actual_dbname != "vrp_db_dev":
            connection.close()
            raise RegionPlanContractError("DATABASE_TARGET_MISMATCH")
        return connection

    result = backend.apply(
        migration_id,
        config_path,
        typed_confirmation=str(typed_confirmation),
        connection_factory=connection_factory,
    )
    return {
        "contract_version": "region-plan-migration/v1",
        "status": result.status,
        "environment": "development",
        "dbname": "vrp_db_dev",
        "target_id": "development:vrp_db_dev",
        "migration_id": result.migration_id,
        "checksum_sha256": result.checksum_sha256,
        "statement_count": result.statement_count,
    }


def _fixed_request_payload(
    config_path: Path,
    request_path: Path,
    request_sha256: str,
    *,
    expected_schema: str,
) -> dict[str, Any]:
    expected_sha = str(request_sha256).strip().lower()
    if not _SHA256_RE.fullmatch(expected_sha):
        raise RegionPlanContractError("REQUEST_CHECKSUM_INVALID")
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RegionPlanContractError("CONFIG_INVALID") from exc
    configured_root = str(
        config.get(
            "region_plan_request_root",
            "/home/csda/AI_Routing/state/development/region_plan_requests",
        )
    ).strip()
    root = Path(configured_root).expanduser().resolve()
    resolved = request_path.expanduser().resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise RegionPlanContractError("REQUEST_PATH_NOT_ALLOWED") from exc
    if not resolved.is_file() or resolved.stat().st_size > 1024 * 1024:
        raise RegionPlanContractError("REQUEST_FILE_INVALID")
    payload_bytes = resolved.read_bytes()
    if hashlib.sha256(payload_bytes).hexdigest() != expected_sha:
        raise RegionPlanContractError("REQUEST_CHECKSUM_MISMATCH")
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise RegionPlanContractError("REQUEST_JSON_INVALID") from exc
    if not isinstance(payload, dict) or payload.get("schema") != expected_schema:
        raise RegionPlanContractError("REQUEST_CONTRACT_INVALID")
    forbidden = {"table", "sql", "query", "destination", "confirm_production"}
    if forbidden.intersection(payload):
        raise RegionPlanContractError("REQUEST_FIELD_NOT_ALLOWED")
    return payload


def _managed_region_plan_bundle(
    config_path: Path,
    *,
    managed_version: object,
    bundle_sha256: object,
    source_path: Path | None = None,
) -> bytes:
    """Read one immutable managed bundle without accepting an arbitrary path."""

    version = str(managed_version).strip().lower()
    expected_sha = str(bundle_sha256).strip().lower()
    if not _SHA256_RE.fullmatch(version) or expected_sha != version:
        raise RegionPlanContractError("BUNDLE_VERSION_INVALID")
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RegionPlanContractError("CONFIG_INVALID") from exc
    managed_root = Path(
        str(
            config.get(
                "managed_data_root",
                "/home/csda/AI_Routing/state/development/managed_data",
            )
        )
    ).expanduser().resolve()
    expected_path = (
        managed_root / MANAGED_BUNDLE_DATASET / version / MANAGED_BUNDLE_FILENAME
    ).resolve()
    try:
        expected_path.relative_to(managed_root)
    except ValueError as exc:
        # Resolving the fixed path must not permit a managed-data symlink to
        # redirect the bundle read outside the configured managed-data root.
        raise RegionPlanContractError("BUNDLE_PATH_NOT_ALLOWED") from exc
    if source_path is not None and source_path.expanduser().resolve() != expected_path:
        raise RegionPlanContractError("BUNDLE_PATH_NOT_ALLOWED")
    if not expected_path.is_file():
        raise RegionPlanContractError("BUNDLE_NOT_FOUND")
    file_stat = expected_path.stat()
    if file_stat.st_size <= 0 or file_stat.st_size > MAX_BUNDLE_BYTES:
        raise RegionPlanContractError("BUNDLE_SIZE_INVALID")
    if os.name != "nt" and stat.S_IMODE(file_stat.st_mode) != 0o600:
        raise RegionPlanContractError("BUNDLE_MODE_INVALID")
    bundle_bytes = expected_path.read_bytes()
    if hashlib.sha256(bundle_bytes).hexdigest() != expected_sha:
        raise RegionPlanContractError("BUNDLE_CHECKSUM_MISMATCH")
    return bundle_bytes


def _stage_bundle_command(
    *,
    config_path: Path,
    source_path: Path,
    bundle_sha256: str,
    managed_version: str,
) -> dict[str, Any]:
    _database, environment, dbname = _config_target(config_path)
    _require_development(environment, dbname)
    bundle_bytes = _managed_region_plan_bundle(
        config_path,
        managed_version=managed_version,
        bundle_sha256=bundle_sha256,
        source_path=source_path,
    )
    return {
        "contract_version": "region-plan-bundle-import/v1",
        "environment": "development",
        "dbname": "vrp_db_dev",
        "target_id": "development:vrp_db_dev",
        "managed_version": str(managed_version).strip().lower(),
        **preview_candidate_import(bundle_bytes),
    }


def _status_bundle_command(
    *, config_path: Path, bundle_sha256: str, managed_version: str
) -> dict[str, Any]:
    """Return the persisted lifecycle state bound to one immutable bundle."""

    _database, environment, dbname = _config_target(config_path)
    _require_development(environment, dbname)
    bundle_bytes = _managed_region_plan_bundle(
        config_path,
        managed_version=managed_version,
        bundle_sha256=bundle_sha256,
    )
    bundle = validate_region_plan_bundle(bundle_bytes)
    plans = list_region_plans(config_path).get("plans", [])
    matches = [
        row
        for row in plans
        if isinstance(row, Mapping) and row.get("plan_id") == bundle.plan_id
    ]
    if len(matches) > 1:
        raise RegionPlanContractError("BUNDLE_PLAN_STATE_INVALID")
    result: dict[str, Any] = {
        "contract_version": "region-plan-bundle-import/v1",
        "environment": "development",
        "managed_version": str(managed_version).strip().lower(),
        "bundle_sha256": bundle.bundle_sha256,
        "plan_id": bundle.plan_id,
        "status": "not_imported",
        "lifecycle_stage": "resolved_candidate",
        "verification_only": True,
        "promotable": False,
        "region_count": len(bundle.regions),
        "postal_count": len(bundle.postals),
        "technician_count": len(bundle.technicians),
        "boundary_resolution_count": len(bundle.boundary_resolutions),
    }
    if matches:
        row = matches[0]
        if row.get("checksum") != bundle.bundle_sha256:
            raise RegionPlanContractError("BUNDLE_PLAN_STATE_CHECKSUM_MISMATCH")
        status = str(row.get("status", "")).strip().lower()
        if status not in {"candidate", "reviewed", "active", "superseded"}:
            raise RegionPlanContractError("BUNDLE_PLAN_STATE_INVALID")
        result.update(
            {
                "status": status,
                "lifecycle_stage": status,
                "revision": int(row.get("revision", -1)),
                "checksum": bundle.bundle_sha256,
            }
        )
    return result


def _import_bundle_request_command(
    config_path: Path, request_path: Path, request_sha256: str
) -> dict[str, Any]:
    _database, environment, dbname = _config_target(config_path)
    _require_development(environment, dbname)
    request = _fixed_request_payload(
        config_path,
        request_path,
        request_sha256,
        expected_schema="region-plan-bundle-import-request/v1",
    )
    allowed_fields = {
        "schema",
        "managed_version",
        "bundle_sha256",
        "imported_by",
        "idempotency_key",
    }
    if set(request) != allowed_fields:
        raise RegionPlanContractError("REQUEST_FIELD_NOT_ALLOWED")
    imported_by = _require_token(
        str(request.get("imported_by", "")), "IMPORTED_BY_INVALID"
    )
    idempotency_key = _require_token(
        str(request.get("idempotency_key", "")), "IDEMPOTENCY_KEY_INVALID"
    )
    bundle_bytes = _managed_region_plan_bundle(
        config_path,
        managed_version=request.get("managed_version"),
        bundle_sha256=request.get("bundle_sha256"),
    )
    repository, _, _ = _repository_from_config(config_path)
    result = repository.import_candidate(
        bundle_bytes,
        environment=environment,
        dbname=dbname,
        imported_by=imported_by,
        idempotency_key=idempotency_key,
    )
    return {
        "contract_version": "region-plan-bundle-import/v1",
        "environment": "development",
        "status": "candidate_imported",
        "plan_id": result.plan_id,
        "revision": result.revision,
        "checksum": result.bundle_sha256,
        "managed_version": str(request.get("managed_version", "")).strip().lower(),
        "lifecycle_stage": "candidate",
        "verification_only": True,
        "promotable": False,
    }


def _resolve_request_command(config_path: Path, request_path: Path, request_sha256: str) -> dict[str, Any]:
    """Reject the retired raw-workbook import workflow.

    Candidate imports must use the managed, checksum-addressed ZIP bundle so
    the review and activation lifecycle has one validated immutable parent.
    """

    del config_path, request_path, request_sha256
    raise RegionPlanContractError("RAW_WORKBOOK_WORKFLOW_DISABLED")


def _workflow_request_command(
    command: str, config_path: Path, request_path: Path, request_sha256: str
) -> dict[str, Any]:
    _database, environment, dbname = _config_target(config_path)
    _require_development(environment, dbname)
    schema = {
        "review": "region-plan-review-request/v1",
        "activation-preview": "region-plan-activation-preview-request/v1",
        "activate": "region-plan-activate-request/v1",
    }[command]
    request = _fixed_request_payload(
        config_path, request_path, request_sha256, expected_schema=schema
    )
    operation = command.replace("-", "_")
    return {
        "contract_version": "region-plan-workflow/v1",
        "environment": "development",
        **dispatch(operation, request, config_path=config_path),
    }


def _stage_candidate_command(
    *, config_path: Path, source_path: Path, source_sha256: str, managed_version: str
) -> dict[str, Any]:
    del config_path, source_path, source_sha256, managed_version
    raise RegionPlanContractError("RAW_WORKBOOK_WORKFLOW_DISABLED")


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m admin_tools.db.region_plan_backend")
    parser.add_argument("--json", action="store_true", dest="json_output")
    commands = parser.add_subparsers(dest="command", required=True)
    stage = commands.add_parser("stage-candidate")
    stage.add_argument("--config", type=Path, required=True)
    stage.add_argument("--source", type=Path, required=True)
    stage.add_argument("--source-sha256", required=True)
    stage.add_argument("--managed-version", required=True)
    stage_bundle = commands.add_parser("stage-bundle")
    stage_bundle.add_argument("--config", type=Path, required=True)
    stage_bundle.add_argument("--source", type=Path, required=True)
    stage_bundle.add_argument("--bundle-sha256", required=True)
    stage_bundle.add_argument("--managed-version", required=True)
    status_bundle = commands.add_parser("status-bundle")
    status_bundle.add_argument("--config", type=Path, required=True)
    status_bundle.add_argument("--bundle-sha256", required=True)
    status_bundle.add_argument("--managed-version", required=True)
    migration_preview = commands.add_parser("migration-preview")
    migration_preview.add_argument("--config", type=Path, required=True)
    migration_preview.add_argument("--migration-id", choices=(MIGRATION_ID, "V002__region_plan_unbounded_region_seq", "V003__region_plan_technician_source_id", "V004__region_plan_area_type_region_soft"), default=MIGRATION_ID)
    install_schema = commands.add_parser("install-schema")
    install_schema.add_argument("--config", type=Path, required=True)
    install_schema.add_argument("--confirmation", required=True)
    install_schema.add_argument("--migration-id", choices=(MIGRATION_ID, "V002__region_plan_unbounded_region_seq", "V003__region_plan_technician_source_id", "V004__region_plan_area_type_region_soft"), default=MIGRATION_ID)
    for command in ("resolve", "import-bundle", "review", "activation-preview", "activate"):
        workflow = commands.add_parser(command)
        workflow.add_argument("--config", type=Path, required=True)
        workflow.add_argument("--request", type=Path, required=True)
        workflow.add_argument("--request-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "stage-candidate":
            result = _stage_candidate_command(
                config_path=args.config,
                source_path=args.source,
                source_sha256=args.source_sha256,
                managed_version=args.managed_version,
            )
        elif args.command == "stage-bundle":
            result = _stage_bundle_command(
                config_path=args.config,
                source_path=args.source,
                bundle_sha256=args.bundle_sha256,
                managed_version=args.managed_version,
            )
        elif args.command == "status-bundle":
            result = _status_bundle_command(
                config_path=args.config,
                bundle_sha256=args.bundle_sha256,
                managed_version=args.managed_version,
            )
        elif args.command == "migration-preview":
            result = preview_fixed_schema_migration(args.config, args.migration_id)
        elif args.command == "install-schema":
            result = install_fixed_schema(
                args.config, typed_confirmation=args.confirmation, migration_id=args.migration_id
            )
        elif args.command == "resolve":
            result = _resolve_request_command(
                args.config, args.request, args.request_sha256
            )
        elif args.command == "import-bundle":
            result = _import_bundle_request_command(
                args.config, args.request, args.request_sha256
            )
        elif args.command in {"review", "activation-preview", "activate"}:
            result = _workflow_request_command(
                args.command, args.config, args.request, args.request_sha256
            )
        else:
            raise RegionPlanContractError("COMMAND_NOT_ALLOWED")
    except RegionPlanContractError as exc:
        error = {
            "contract_version": (
                "region-plan-migration/v1"
                if args.command in {"migration-preview", "install-schema"}
                else "region-plan-bundle-import/v1"
                if args.command in {"stage-bundle", "status-bundle", "import-bundle"}
                else "region-plan-workflow/v1"
                if args.command in {"resolve", "import-bundle", "review", "activation-preview", "activate"}
                else "region-plan/v1"
            ),
            "environment": "development",
            "status": "rejected",
            "error_code": exc.code,
        }
        if args.json_output:
            print(json.dumps(error, sort_keys=True, separators=(",", ":")))
        else:
            print(exc.code, file=sys.stderr)
        return 2
    if args.json_output:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        identifier = result.get("plan_id", result.get("migration_id", ""))
        print(f"{result['status']} {identifier}".strip())
    return 0


__all__ = [
    "ActivationPreview",
    "ActivationResult",
    "Atlanta6AreaPlanRepository",
    "CandidateImportResult",
    "dispatch",
    "get_active_plan_snapshot",
    "install_fixed_schema",
    "list_region_plans",
    "PLAN_ID",
    "preview_fixed_schema_migration",
    "RegionPlanContractError",
    "ReviewResult",
    "ValidatedRegionPlanBundle",
    "preview_candidate_import",
    "validate_region_plan_bundle",
]


if __name__ == "__main__":
    raise SystemExit(main())
