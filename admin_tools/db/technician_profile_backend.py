"""Fixed Development-only Technician Data preview/apply backend.

The source is the four-sheet profile workbook.  Address and capability rows are
upserted atomically.  Assigned regions are never inferred or overwritten here;
they are loaded from the reviewed Region Data plan and transactionally rechecked
before Technician Data is applied.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import secrets
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from admin_tools.db import master_data_backend as master
from tools.data.technician_profile_data import (
    CanonicalTechnicianProfile,
    TechnicianProfileDataError,
    canonicalize_technician_profile,
)


CONTRACT_VERSION = "technician-profile/v1"
TARGET_CITY = "Atlanta_6area"
SUBSIDIARY_NAME = "LGEAI"
# Region-plan assignments deliberately contain only the stable technician key.
# The source master remains the authority for the technician name and status.
SOURCE_TECHNICIAN_CITY = "Atlanta, GA"
PREVIEW_TTL_SECONDS = 30 * 60
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")

TECHNICIAN_COLUMNS = (
    "subsidiary_name", "strategic_city_name", "employee_code", "employee_name",
    "center_type", "home_address", "home_city", "home_state", "home_country",
    "home_postal_code", "home_latitude", "home_longitude", "active_flag",
    "priority_group", "max_home_to_job_min",
)
CAPABILITY_COLUMNS = (
    "subsidiary_name", "strategic_city_name", "employee_code", "product_group_code",
    "product_code", "repair_allowed", "heavy_repair_allowed", "priority_score",
    "effective_start_date", "effective_end_date",
)


class TechnicianProfileBackendError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _config(config_path: Path) -> tuple[dict[str, Any], dict[str, str]]:
    config = master._read_config(config_path)
    target = master._target(config)
    if target["environment"] != "development" or target["dbname"] != "vrp_db_dev":
        raise TechnicianProfileBackendError("PRODUCTION_TECHNICIAN_WRITES_DISABLED")
    return config, target


def _envelope(target: Mapping[str, str], **values: Any) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "environment": target["environment"],
        "dbname": target["dbname"],
        "target_id": target["target_id"],
        **values,
    }


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _managed_source(
    config: Mapping[str, Any], source: Path, source_sha256: str, managed_version: str
) -> bytes:
    expected = str(source_sha256).strip().lower()
    version = str(managed_version).strip().lower()
    if not _SHA256_RE.fullmatch(expected) or version != expected:
        raise TechnicianProfileBackendError("SOURCE_VERSION_INVALID")
    root = Path(
        str(
            config.get(
                "managed_data_root",
                "/home/csda/AI_Routing/state/development/managed_data",
            )
        )
    ).expanduser().resolve()
    resolved = source.expanduser().resolve()
    allowed_parents = {
        (root / dataset / version).resolve()
        for dataset in ("technician_data_workbook", "technician_profile_workbook")
    }
    if resolved.name != "payload.xlsx" or not resolved.is_file():
        raise TechnicianProfileBackendError("SOURCE_PATH_NOT_ALLOWED")
    if not any(_is_relative_to(resolved, parent) for parent in allowed_parents):
        raise TechnicianProfileBackendError("SOURCE_PATH_NOT_ALLOWED")
    payload = resolved.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected:
        raise TechnicianProfileBackendError("SOURCE_CHECKSUM_MISMATCH")
    return payload


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _assignment_rows(cursor: Any) -> tuple[str, dict[str, dict[str, Any]]]:
    cursor.execute(
        """
        select p.plan_id, t.employee_code, m.employee_name, m.active_flag,
               t.assigned_region_seq, r.region_name as assigned_region_name,
               t.policy_mode, t.active_flag
        from common_region_plan_activation a
        join common_city_context c
          on c.subsidiary_name=a.subsidiary_name
         and c.strategic_city_name=a.strategic_city_name
         and c.context_status='active'
        join common_region_plan p
          on p.subsidiary_name=a.subsidiary_name
         and p.strategic_city_name=a.strategic_city_name
         and p.plan_id=a.plan_id
         and p.plan_status='active'
         and p.verification_only
        join common_region_plan_technician t
          on t.subsidiary_name=p.subsidiary_name
         and t.strategic_city_name=p.strategic_city_name and t.plan_id=p.plan_id
        left join common_technician_master m
          on m.subsidiary_name=%s
         and m.strategic_city_name=%s
         and m.employee_code=t.employee_code
        join common_region_plan_region r
          on r.subsidiary_name=t.subsidiary_name
         and r.strategic_city_name=t.strategic_city_name
         and r.plan_id=t.plan_id and r.region_seq=t.assigned_region_seq
        where a.subsidiary_name=%s and a.strategic_city_name=%s
          and a.active_flag
        order by t.employee_code
        """,
        (
            SUBSIDIARY_NAME,
            SOURCE_TECHNICIAN_CITY,
            SUBSIDIARY_NAME,
            TARGET_CITY,
        ),
    )
    rows = cursor.fetchall()
    if not rows:
        raise TechnicianProfileBackendError("ACTIVE_REGION_ASSIGNMENTS_REQUIRED")
    plan_id = str(rows[0][0])
    selected = [row for row in rows if str(row[0]) == plan_id]
    assignments: dict[str, dict[str, Any]] = {}
    for row in selected:
        code = str(row[1]).strip()
        if code in assignments:
            # The plan table key makes duplicate assignments impossible.  A
            # duplicate result therefore indicates duplicate authoritative
            # master rows for the stable employee code.
            raise TechnicianProfileBackendError("TECHNICIAN_MASTER_DUPLICATE")
        if row[2] is None:
            raise TechnicianProfileBackendError("TECHNICIAN_MASTER_MISSING")
        if row[3] is not True:
            raise TechnicianProfileBackendError("TECHNICIAN_MASTER_INACTIVE")
        assignments[code] = {
            "employee_name": str(row[2]).strip(),
            "assigned_region_seq": int(row[4]),
            "assigned_region_name": str(row[5]).strip(),
            "policy_mode": str(row[6]).strip(),
            "active_flag": bool(row[7]),
        }
    if len(assignments) != 14:
        raise TechnicianProfileBackendError("REGION_ASSIGNMENT_COUNT_INVALID")
    return plan_id, assignments


def _fetch_current(cursor: Any, canonical: CanonicalTechnicianProfile) -> dict[str, list[dict[str, Any]]]:
    codes = [str(row["employee_code"]) for row in canonical.technician_rows]
    cursor.execute(
        """
        select subsidiary_name, strategic_city_name, employee_code, employee_name,
               center_type, home_address, home_city, home_state, home_country,
               home_postal_code, home_latitude, home_longitude, active_flag,
               priority_group, max_home_to_job_min
        from common_technician_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code
        """,
        (SUBSIDIARY_NAME, TARGET_CITY, codes),
    )
    technicians = [dict(zip(TECHNICIAN_COLUMNS, row)) for row in cursor.fetchall()]
    cursor.execute(
        """
        select subsidiary_name, strategic_city_name, employee_code, product_group_code,
               product_code, repair_allowed, heavy_repair_allowed, priority_score,
               effective_start_date, effective_end_date
        from common_technician_capability_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code = any(%s)
        order by employee_code, product_group_code, product_code
        """,
        (SUBSIDIARY_NAME, TARGET_CITY, codes),
    )
    capabilities = [dict(zip(CAPABILITY_COLUMNS, row)) for row in cursor.fetchall()]
    return {"technicians": technicians, "capabilities": capabilities}


def _diff(
    desired: tuple[Mapping[str, Any], ...], current: list[dict[str, Any]], key_columns: tuple[str, ...]
) -> tuple[int, int, int]:
    current_by_key = {
        tuple(row.get(column) for column in key_columns): row for row in current
    }
    created = updated = unchanged = 0
    for desired_row in desired:
        row = dict(desired_row)
        key = tuple(row.get(column) for column in key_columns)
        old = current_by_key.get(key)
        if old is None:
            created += 1
        elif _canonical_hash(old) == _canonical_hash(row):
            unchanged += 1
        else:
            updated += 1
    return created, updated, unchanged


def _stale_capability_keys(
    desired: tuple[Mapping[str, Any], ...], current: list[dict[str, Any]]
) -> tuple[tuple[str, str, str], ...]:
    key_columns = ("employee_code", "product_group_code", "product_code")
    desired_keys = {
        tuple(str(row.get(column) or "") for column in key_columns)
        for row in desired
    }
    stale = {
        tuple(str(row.get(column) or "") for column in key_columns)
        for row in current
        if tuple(str(row.get(column) or "") for column in key_columns)
        not in desired_keys
    }
    if any(not all(key) for key in stale):
        raise TechnicianProfileBackendError("CAPABILITY_KEY_INVALID")
    return tuple(sorted(stale))


def _rejection_summary(canonical: CanonicalTechnicianProfile) -> tuple[int, list[dict[str, Any]]]:
    reasons: dict[str, int] = {}
    for values in canonical.row_accounting.values():
        for reason, count in values["rejected_by_reason"].items():
            reasons[reason] = reasons.get(reason, 0) + int(count)
    return sum(reasons.values()), [
        {"code": reason.upper(), "count": count} for reason, count in sorted(reasons.items())
    ]


def preview(
    config_path: Path,
    source: Path,
    source_sha256: str,
    managed_version: str,
    environment: str,
) -> dict[str, Any]:
    config, target = _config(config_path)
    if str(environment).strip().lower() != "development":
        raise TechnicianProfileBackendError("ENVIRONMENT_MISMATCH")
    source_bytes = _managed_source(config, source, source_sha256, managed_version)
    connection = master._connect(config)
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '30000ms'")
            plan_id, assignments = _assignment_rows(cursor)
            canonical = canonicalize_technician_profile(
                source_bytes, plan_id=plan_id, assignments=assignments
            )
            current = _fetch_current(cursor, canonical)
            connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    technician_counts = _diff(
        canonical.technician_rows,
        current["technicians"],
        ("subsidiary_name", "strategic_city_name", "employee_code"),
    )
    capability_counts = _diff(
        canonical.capability_rows,
        current["capabilities"],
        (
            "subsidiary_name", "strategic_city_name", "employee_code",
            "product_group_code", "product_code",
        ),
    )
    stale_capabilities = _stale_capability_keys(
        canonical.capability_rows, current["capabilities"]
    )
    assignment_digest = _canonical_hash(
        [dict(row) for row in canonical.assignment_rows]
    )
    preview_id = str(uuid.uuid4())
    confirmation = secrets.token_urlsafe(32)
    state = {
        "preview_id": preview_id,
        "target": target,
        "managed_version": managed_version,
        "source_sha256": canonical.source_sha256,
        "canonical_sha256": canonical.canonical_sha256,
        "plan_id": canonical.plan_id,
        "technicians": [dict(row) for row in canonical.technician_rows],
        "capabilities": [dict(row) for row in canonical.capability_rows],
        "assignments": [dict(row) for row in canonical.assignment_rows],
        "assignment_digest": assignment_digest,
        "current_digest": _canonical_hash(current),
        "capability_delete_count": len(stale_capabilities),
        "confirmation_token": confirmation,
        "created_at_epoch": time.time(),
        "expires_at_epoch": time.time() + PREVIEW_TTL_SECONDS,
    }
    digest = _canonical_hash(
        {key: value for key, value in state.items() if key != "confirmation_token"}
    )
    state["preview_digest"] = digest
    preview_root = master._state_root(config_path, target) / "technician-profile-previews"
    preview_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    master._write_encrypted_preview(preview_root / f"{preview_id}.json", state, config)
    rejected_count, errors = _rejection_summary(canonical)
    return _envelope(
        target,
        status="ready",
        managed_version=managed_version,
        preview_id=preview_id,
        preview_digest=digest,
        confirmation_token=confirmation,
        expires_at=datetime.fromtimestamp(state["expires_at_epoch"], timezone.utc).isoformat(),
        technician_create_count=technician_counts[0],
        technician_update_count=technician_counts[1],
        technician_unchanged_count=technician_counts[2],
        capability_create_count=capability_counts[0],
        capability_update_count=capability_counts[1],
        capability_unchanged_count=capability_counts[2],
        capability_delete_count=len(stale_capabilities),
        region_mapping_create_count=0,
        region_mapping_update_count=0,
        region_mapping_unchanged_count=len(canonical.assignment_rows),
        rejected_count=rejected_count,
        error_count=0,
        errors=errors,
        row_accounting={
            name: dict(values) for name, values in canonical.row_accounting.items()
        },
        source_sha256=canonical.source_sha256,
        canonical_sha256=canonical.canonical_sha256,
        plan_id=canonical.plan_id,
        region_mapping_source="active_region_data",
    )


def _preview_state(config_path: Path, config: Mapping[str, Any], target: Mapping[str, str], preview_id: str) -> dict[str, Any]:
    if not _UUID_RE.fullmatch(str(preview_id)):
        raise TechnicianProfileBackendError("PREVIEW_ID_INVALID")
    path = master._state_root(config_path, target) / "technician-profile-previews" / f"{preview_id}.json"
    try:
        return master._read_encrypted_preview(path, config)
    except Exception as exc:
        raise TechnicianProfileBackendError("PREVIEW_STATE_INVALID") from exc


def _receipt_path(config_path: Path, target: Mapping[str, str], operation_id: str) -> Path:
    root = master._state_root(config_path, target) / "technician-profile-receipts"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    return root / f"{operation_id}.json"


def apply(
    config_path: Path,
    preview_id: str,
    preview_digest: str,
    idempotency_key: str,
    confirmation: str,
    environment: str,
) -> dict[str, Any]:
    config, target = _config(config_path)
    if str(environment).strip().lower() != "development":
        raise TechnicianProfileBackendError("ENVIRONMENT_MISMATCH")
    operation_id = str(uuid.UUID(idempotency_key))
    state = _preview_state(config_path, config, target, preview_id)
    if (
        state.get("target") != target
        or state.get("preview_digest") != preview_digest
        or state.get("confirmation_token") != confirmation
    ):
        raise TechnicianProfileBackendError("PREVIEW_MISMATCH")
    if float(state.get("expires_at_epoch", 0)) < time.time():
        raise TechnicianProfileBackendError("PREVIEW_EXPIRED")
    receipt_path = _receipt_path(config_path, target, operation_id)
    if receipt_path.is_file():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("preview_id") != preview_id or receipt.get("preview_digest") != preview_digest:
            raise TechnicianProfileBackendError("IDEMPOTENCY_CONFLICT")
        if receipt.get("status") == "applied":
            receipt["status"] = "already_applied"
            return receipt
    technicians = tuple(state.get("technicians") or ())
    capabilities = tuple(state.get("capabilities") or ())
    planned_delete_count = state.get("capability_delete_count")
    if (
        not technicians
        or not capabilities
        or not isinstance(planned_delete_count, int)
        or planned_delete_count < 0
    ):
        raise TechnicianProfileBackendError("PREVIEW_STATE_INVALID")
    pending = _envelope(
        target,
        status="pending",
        managed_version=str(state["managed_version"]),
        preview_id=preview_id,
        preview_digest=preview_digest,
        operation_id=operation_id,
        capability_delete_count=planned_delete_count,
    )
    master._write_private(receipt_path, pending)
    connection = master._connect(config)
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30000ms'")
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("technician-profile:Atlanta_6area",),
            )
            plan_id, assignments = _assignment_rows(cursor)
            if plan_id != state.get("plan_id") or _canonical_hash(
                [assignments[code] | {"employee_code": code} for code in sorted(assignments)]
            ) != state.get("assignment_digest"):
                raise TechnicianProfileBackendError("REGION_ASSIGNMENTS_CHANGED")
            canonical_stub = type("CanonicalStub", (), {
                "technician_rows": technicians,
                "capability_rows": capabilities,
            })()
            current = _fetch_current(cursor, canonical_stub)
            desired_current = {
                "technicians": [dict(row) for row in technicians],
                "capabilities": [dict(row) for row in capabilities],
            }
            if _canonical_hash(current) != state.get("current_digest"):
                if _canonical_hash(current) == _canonical_hash(desired_current):
                    recovered = {
                        **pending,
                        "status": "applied",
                        "recovered": True,
                        "applied_at": datetime.now(timezone.utc).isoformat(),
                        "technician_applied_count": len(technicians),
                        "capability_applied_count": len(capabilities),
                        "region_mapping_verified_count": len(assignments),
                        "rejected_count": 0,
                        "error_count": 0,
                        "errors": [],
                    }
                    connection.rollback()
                    master._write_private(receipt_path, recovered)
                    return recovered
                raise TechnicianProfileBackendError("PREVIEW_STALE")
            stale_capabilities = _stale_capability_keys(
                capabilities, current["capabilities"]
            )
            if len(stale_capabilities) != planned_delete_count:
                raise TechnicianProfileBackendError("PREVIEW_STALE")
            _delete_capabilities(cursor, stale_capabilities)
            _upsert_rows(
                cursor,
                "common_technician_master",
                TECHNICIAN_COLUMNS,
                ("subsidiary_name", "strategic_city_name", "employee_code"),
                technicians,
            )
            _upsert_rows(
                cursor,
                "common_technician_capability_master",
                CAPABILITY_COLUMNS,
                (
                    "subsidiary_name", "strategic_city_name", "employee_code",
                    "product_group_code", "product_code",
                ),
                capabilities,
            )
        connection.commit()
        applied = {
            **pending,
            "status": "applied",
            "applied_at": datetime.now(timezone.utc).isoformat(),
            "technician_applied_count": len(technicians),
            "capability_applied_count": len(capabilities),
            "region_mapping_verified_count": len(assignments),
            "rejected_count": 0,
            "error_count": 0,
            "errors": [],
        }
        master._write_private(receipt_path, applied)
        return applied
    except Exception:
        connection.rollback()
        failed = {**pending, "status": "failed", "failed_at": datetime.now(timezone.utc).isoformat()}
        master._write_private(receipt_path, failed)
        raise
    finally:
        connection.close()


def _upsert_rows(
    cursor: Any,
    table: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    rows: tuple[Mapping[str, Any], ...],
) -> None:
    placeholders = ",".join(["%s"] * len(columns))
    update_columns = [column for column in columns if column not in key_columns]
    updates = ",".join(f"{column}=excluded.{column}" for column in update_columns)
    statement = (
        f"insert into {table} ({','.join(columns)}) values ({placeholders}) "
        f"on conflict ({','.join(key_columns)}) do update set {updates}, updated_at=now()"
    )
    cursor.executemany(statement, [tuple(row.get(column) for column in columns) for row in rows])


def _delete_capabilities(
    cursor: Any, stale_keys: tuple[tuple[str, str, str], ...]
) -> None:
    if not stale_keys:
        return
    cursor.executemany(
        """
        delete from common_technician_capability_master
        where subsidiary_name=%s and strategic_city_name=%s
          and employee_code=%s and product_group_code=%s and product_code=%s
        """,
        [
            (SUBSIDIARY_NAME, TARGET_CITY, employee_code, product_group, product_code)
            for employee_code, product_group, product_code in stale_keys
        ],
    )
    if cursor.rowcount != len(stale_keys):
        raise TechnicianProfileBackendError("CAPABILITY_DELETE_CONFLICT")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m admin_tools.db.technician_profile_backend")
    parser.add_argument("--json", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    item = commands.add_parser("preview")
    item.add_argument("--config", type=Path, required=True)
    item.add_argument("--source", type=Path, required=True)
    item.add_argument("--source-sha256", required=True)
    item.add_argument("--managed-version", required=True)
    item.add_argument("--environment", required=True)
    item = commands.add_parser("apply")
    item.add_argument("--config", type=Path, required=True)
    item.add_argument("--preview-id", required=True)
    item.add_argument("--preview-digest", required=True)
    item.add_argument("--idempotency-key", required=True)
    item.add_argument("--confirmation", required=True)
    item.add_argument("--environment", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    target = {"environment": "unknown", "dbname": "unknown", "target_id": "unknown:unknown"}
    try:
        _config_payload, target = _config(args.config)
        if args.command == "preview":
            result = preview(
                args.config, args.source, args.source_sha256,
                args.managed_version, args.environment,
            )
        else:
            result = apply(
                args.config, args.preview_id, args.preview_digest,
                args.idempotency_key, args.confirmation, args.environment,
            )
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except (TechnicianProfileBackendError, TechnicianProfileDataError) as exc:
        code = exc.code
    except Exception:
        code = "TECHNICIAN_PROFILE_BACKEND_FAILED"
    print(json.dumps(_envelope(target, status="error", error_code=code, error_count=1, errors=[{"code": code, "count": 1}])))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["CONTRACT_VERSION", "TechnicianProfileBackendError", "apply", "main", "preview"]
