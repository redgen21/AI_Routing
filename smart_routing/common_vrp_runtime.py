from __future__ import annotations

import json
import threading
import uuid
import copy
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .common_vrp_db import (
    COMMON_CONFIG_PATH,
    _default_heavy_repair_lookup_path,
    _default_symptom_path,
    delete_routing_result,
    delete_routing_requests_for_date,
    get_latest_routing_request,
    load_common_config,
    get_routing_config,
    get_routing_request,
    get_routing_result,
    get_active_region_plan_snapshot,
    get_configured_region_plan_snapshot,
    list_active_region_plan_contexts,
    list_avoid_areas,
    list_capabilities,
    list_contexts,
    list_engineers,
    list_jobs,
    list_regions,
    list_heavy_repair_rules,
    upsert_routing_request,
    upsert_routing_result,
)
from .live_atlanta_runtime import _load_config as _load_runtime_config
from .live_atlanta_runtime import _merge_service_geocodes
from .vrp_api_service import create_job_id, run_routing_request


COMMON_JOB_ARCHIVE_ROOT = Path("260310/common_vrp_api_jobs")
LOGGER = logging.getLogger(__name__)
AREA_TYPE_DMS = {"DMS", "DMS_CORE", "DMS_ONLY"}
AREA_TYPE_OVERLAP = {"OVERLAP", "OVERLAB"}
AREA_TYPE_DMS2 = {"DMS2", "DMS2_EXCLUSIVE", "DMS2_ONLY"}
AREA_TYPE_ROUTING_CITY_NAMES = set()
AREA_TYPE_ROUTING_CITY_SUFFIXES = (" - AREA TYPE CLUSTERS", " - BUCKET SIM DRAFT")
ATLANTA_6AREA_CITY = "Atlanta_6area"
ATLANTA_6AREA_MANAGED_MASTER_REQUIRED = "ATLANTA_6AREA_MANAGED_MASTER_REQUIRED"
HOME_DISTANCE_ONLY = "home_distance_only"
PREFERRED_REGION_SOFT = "preferred_region_soft"
OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1 = "own_region_with_approved_boundary_overflow/v1"
OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2 = "own_region_with_approved_boundary_overflow/v2"
ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1 = "active_roster_type_hard_region_soft/v1"
ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1 = "active_roster_area_type_fallback_region_soft/v1"
ATLANTA_6AREA_SUPPORTED_POLICY_VERSIONS = frozenset({
    HOME_DISTANCE_ONLY,
    PREFERRED_REGION_SOFT,
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
    "explicit_workbook_membership/v1",
    ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1,
    ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
})


def _runtime_source_city(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> str:
    try:
        snapshot = _active_atlanta6_plan(subsidiary_name, strategic_city_name, config_path)
    except RuntimeError:
        return strategic_city_name
    if snapshot is None:
        return strategic_city_name
    return str(snapshot.get("source_strategic_city_name", "")).strip() or strategic_city_name


def _runtime_engineer_master(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> pd.DataFrame:
    """Load the execution master; legacy Atlanta-6 retains its dedicated roster."""
    if str(strategic_city_name).strip() == ATLANTA_6AREA_CITY:
        scenario_rows = list_engineers(subsidiary_name, strategic_city_name, config_path=config_path)
        if scenario_rows.empty:
            raise ValueError(ATLANTA_6AREA_MANAGED_MASTER_REQUIRED)
        return scenario_rows
    source_city = _runtime_source_city(subsidiary_name, strategic_city_name, config_path)
    if source_city == strategic_city_name:
        return list_engineers(subsidiary_name, strategic_city_name, config_path=config_path)
    scenario_rows = list_engineers(subsidiary_name, strategic_city_name, config_path=config_path)
    if not scenario_rows.empty:
        return scenario_rows
    # Workbook-import scenarios only carry plan membership.  Their operational
    # home locations, shifts, and limits remain in the base Atlanta master.
    return list_engineers(subsidiary_name, source_city, config_path=config_path)


def _managed_capability_rows(
    subsidiary_name: str,
    strategic_city_name: str,
    legacy_rows: list[dict[str, Any]],
    config_path: Path = COMMON_CONFIG_PATH,
) -> tuple[list[dict[str, Any]], bool]:
    """Return the managed policy for this scenario, with an explicit legacy escape hatch."""
    managed_df = list_capabilities(subsidiary_name, strategic_city_name, config_path=config_path)
    if not managed_df.empty:
        return managed_df.to_dict(orient="records"), True
    if str(strategic_city_name).strip() == ATLANTA_6AREA_CITY:
        # This scenario is administered through Technician Data.  Falling back
        # to a workbook here would silently widen a managed policy.
        raise ValueError("ATLANTA_6AREA_MANAGED_CAPABILITIES_REQUIRED")
    source_city = _runtime_source_city(subsidiary_name, strategic_city_name, config_path)
    if source_city != strategic_city_name:
        base_df = list_capabilities(subsidiary_name, source_city, config_path=config_path)
        if not base_df.empty:
            return base_df.to_dict(orient="records"), True
    return [dict(row) for row in legacy_rows], False


def _capability_snapshot(capabilities: list[dict[str, Any]], *, managed: bool) -> dict[str, Any]:
    """PII-free, deterministic evidence of the capability policy queued for execution."""
    canonical = json.dumps(capabilities, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return {
        "count": len(capabilities),
        "sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "source": "managed_master" if managed else "legacy_payload",
    }


def _set_capability_snapshot(
    payload: dict[str, Any],
    capabilities: list[dict[str, Any]],
    *,
    managed: bool,
) -> dict[str, Any]:
    enriched = copy.deepcopy(payload)
    enriched["capabilities"] = capabilities
    options = dict(enriched.get("options") or {})
    options["capability_snapshot"] = _capability_snapshot(capabilities, managed=managed)
    enriched["options"] = options
    return enriched


def list_runtime_contexts(config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any]:
    """Expose every configured context whose Development plan is immutable and active."""
    contexts = copy.deepcopy(list_contexts(config_path=config_path))
    try:
        plan_contexts = list_active_region_plan_contexts(config_path=config_path)
    except RuntimeError:
        return contexts
    for plan_context in plan_contexts:
        subsidiary_name = str(plan_context.get("subsidiary_name", "")).strip()
        city = str(plan_context.get("strategic_city_name", "")).strip()
        if not subsidiary_name or not city:
            continue
        try:
            snapshot = _active_atlanta6_plan(subsidiary_name, city, config_path)
        except (RuntimeError, ValueError):
            continue
        if snapshot is None:
            continue
        source_city = str(snapshot.get("source_strategic_city_name", "")).strip()
        if not source_city:
            # A plan context without an explicit source cannot safely select
            # operational masters, geometry, or a routing profile.
            continue
        cities = contexts.setdefault("cities", [])
        if city not in cities:
            cities.append(city)
            cities.sort()
        mapping = contexts.setdefault("cities_by_subsidiary", {})
        scoped = mapping.setdefault(subsidiary_name, [])
        if city not in scoped:
            scoped.append(city)
            scoped.sort()
        subsidiaries = contexts.setdefault("subsidiaries", [])
        if subsidiary_name not in subsidiaries:
            subsidiaries.append(subsidiary_name)
            subsidiaries.sort()
        entry = {
            "subsidiary_name": subsidiary_name,
            "strategic_city_name": city,
            "source_strategic_city_name": source_city,
            "geometry_city_name": source_city,
            "profile_city_name": source_city,
        }
        region_plan_cities = contexts.setdefault("region_plan_cities", [])
        region_plan_cities[:] = [
            row for row in region_plan_cities
            if not (
                isinstance(row, dict)
                and str(row.get("subsidiary_name", "")).strip() == subsidiary_name
                and str(row.get("strategic_city_name", "")).strip() == city
            )
        ]
        region_plan_cities.append(entry)
    return contexts


def _active_atlanta6_plan(subsidiary_name: str, strategic_city_name: str, config_path: Path) -> dict[str, Any] | None:
    try:
        # City Config owns the selected plan.  The activation table remains a
        # backward-compatible fallback only for cities that have not migrated
        # to explicit plan selection yet.
        snapshot = get_configured_region_plan_snapshot(
            subsidiary_name,
            strategic_city_name,
            config_path=config_path,
        )
        if snapshot is None:
            snapshot = get_active_region_plan_snapshot(subsidiary_name, strategic_city_name, config_path=config_path)
    except RuntimeError as exc:
        if str(exc) in {"ACTIVE_REGION_PLAN_NOT_FOUND", "REGION_PLAN_RUNTIME_DISABLED_IN_PRODUCTION"}:
            return None
        raise
    status = str(snapshot.get("status", snapshot.get("plan_status", ""))).strip().lower()
    context_status = str(snapshot.get("context_status", "")).strip().lower()
    enabled = snapshot.get("enabled", snapshot.get("feature_enabled", False))
    if enabled is not True or status not in {"active", "reviewed"} or context_status not in {"", "active", "reviewed"}:
        raise ValueError("CONFIGURED_REGION_PLAN_MUST_BE_REVIEWED_OR_ACTIVE")
    if not str(snapshot.get("plan_id", "")).strip() or not str(snapshot.get("checksum", snapshot.get("bundle_sha256", "")).strip()):
        raise ValueError("ATLANTA_6AREA_PLAN_SNAPSHOT_INVALID")
    _atlanta6_snapshot_policy_version(snapshot)
    return snapshot


def _plan_rows(snapshot: dict[str, Any], key: str) -> list[dict[str, Any]]:
    rows = snapshot.get(key, [])
    return [dict(row) for row in rows] if isinstance(rows, list) else []


def _atlanta6_snapshot_policy_version(snapshot: dict[str, Any]) -> str:
    """Return the immutable plan policy, rejecting absent or unknown policies.

    V2 is the policy for the current reviewed Atlanta-6 plan.  V1 remains
    executable only when an older immutable snapshot explicitly records it;
    it is never inferred as a fallback for incomplete snapshots.
    """
    policy_version = str(snapshot.get("policy_version", "")).strip()
    if not policy_version:
        raise ValueError("ATLANTA_6AREA_PLAN_POLICY_VERSION_REQUIRED")
    if policy_version not in ATLANTA_6AREA_SUPPORTED_POLICY_VERSIONS:
        raise ValueError("ATLANTA_6AREA_PLAN_POLICY_VERSION_UNSUPPORTED")
    return policy_version


def _apply_active_region_plan(payload: dict[str, Any], snapshot: dict[str, Any] | None) -> dict[str, Any]:
    """Apply immutable Atlanta-6 membership as hard eligibility, never a fallback."""
    if snapshot is None:
        return payload
    policy_version = _atlanta6_snapshot_policy_version(snapshot)
    execution = copy.deepcopy(payload)
    postal_rows = _plan_rows(snapshot, "postals") or _plan_rows(snapshot, "postal_memberships")
    technician_rows = _plan_rows(snapshot, "technicians") or _plan_rows(snapshot, "assigned_technicians")
    overflow_rows = _plan_rows(snapshot, "boundary_overflow")
    region_rows = _plan_rows(snapshot, "regions")
    region_names = {
        str(row.get("region_seq", "")).strip(): str(row.get("region_name", row.get("region_id", ""))).strip()
        for row in region_rows
    }
    region_required_center_types: dict[str, str] = {}
    if policy_version in {
        ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1,
        ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
    }:
        # The reviewed plan must make the center type unambiguous for every
        # region.  The roster is the hard gate; regions are affinity only.
        region_area_types: dict[str, set[str]] = {}
        for row in postal_rows:
            region = str(row.get("region_seq", "")).strip()
            area_type = _normalize_area_type(row.get("area_type"))
            if not region or area_type not in {"DMS", "DMS2"}:
                raise ValueError("ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_REGION_AREA_TYPE_INVALID")
            region_area_types.setdefault(region, set()).add(area_type)
        for region, area_types in region_area_types.items():
            if len(area_types) != 1:
                raise ValueError("ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_REGION_AREA_TYPE_NOT_UNIFORM")
            region_required_center_types[region] = next(iter(area_types))
    postal_region = {
        str(row.get("postal_code", "")).strip().zfill(5): row
        for row in postal_rows if str(row.get("postal_code", "")).strip()
    }
    technicians_by_region: dict[str, set[str]] = {}
    assigned_codes: set[str] = set()
    for row in technician_rows:
        code = str(row.get("employee_code", "")).strip()
        if not code or row.get("active_flag", True) is False:
            continue
        region = str(row.get("assigned_region_seq", row.get("region_seq", ""))).strip()
        assigned_codes.add(code)
        technicians_by_region.setdefault(region, set()).add(code)
    if not assigned_codes:
        raise ValueError("ATLANTA_6AREA_PLAN_HAS_NO_ASSIGNED_TECHNICIANS")
    assignment_by_code = {
        str(row.get("employee_code", "")).strip(): row
        for row in technician_rows
        if str(row.get("employee_code", "")).strip()
    }
    selected_technicians: list[dict[str, Any]] = []
    for tech in list(execution.get("technicians", [])):
        code = str(tech.get("employee_code", "")).strip()
        assignment = assignment_by_code.get(code)
        if code not in assigned_codes or assignment is None:
            continue
        enriched_tech = dict(tech)
        assigned_region_seq = assignment.get("assigned_region_seq", assignment.get("region_seq"))
        enriched_tech["assigned_region_seq"] = assigned_region_seq
        enriched_tech["assigned_region_name"] = str(
            assignment.get("assigned_region_name", assignment.get("region_name", region_names.get(str(assigned_region_seq), "")))
        ).strip()
        if policy_version == PREFERRED_REGION_SOFT:
            enriched_tech["preferred_region_name"] = enriched_tech["assigned_region_name"]
        selected_technicians.append(enriched_tech)
    execution["technicians"] = selected_technicians
    active_codes = {str(tech.get("employee_code", "")).strip() for tech in execution["technicians"]}
    dms_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in execution["technicians"]
        if str(tech.get("center_type", "")).strip().upper() == "DMS"
    }
    dms2_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in execution["technicians"]
        if str(tech.get("center_type", "")).strip().upper() == "DMS2"
    }
    if policy_version in {HOME_DISTANCE_ONLY, PREFERRED_REGION_SOFT}:
        # These two policies use the selected plan as the roster/context only.
        # Home-distance ignores region membership; preferred-region exposes it
        # as an affinity and never as a hard eligibility restriction.
        if policy_version == PREFERRED_REGION_SOFT:
            for job in execution.get("jobs", []):
                postal = str(job.get("postal_code", job.get("zip_code", ""))).strip().replace(".0", "").zfill(5)
                membership = postal_region.get(postal)
                if membership:
                    region = str(membership.get("region_seq", "")).strip()
                    name = str(membership.get("region_name") or region_names.get(region) or f"Region {region}").strip()
                    job["region_seq"] = membership.get("region_seq")
                    job["region_name"] = name
                    job["region_preference"] = {"region_seq": membership.get("region_seq"), "region_name": name}
                else:
                    job["region_preference_diagnostic"] = "REGION_PREFERENCE_UNRESOLVED"
        options = dict(execution.get("options") or {})
        options["region_policy"] = policy_version
        options["region_plan"] = {
            "plan_id": str(snapshot["plan_id"]),
            "plan_revision": snapshot.get("revision", snapshot.get("plan_revision")),
            "policy_version": policy_version,
            "checksum": str(snapshot.get("checksum", snapshot.get("bundle_sha256"))),
            "activation_revision": snapshot.get("activation_revision"),
        }
        execution["options"] = options
        execution["region_plan"] = dict(options["region_plan"])
        return execution
    overflow_by_postal = {str(row.get("postal_code", "")).strip().zfill(5): row for row in overflow_rows}
    jobs: list[dict[str, Any]] = []
    for job in list(execution.get("jobs", [])):
        updated = dict(job)
        postal = str(updated.get("postal_code", updated.get("zip_code", ""))).strip().replace(".0", "").zfill(5)
        membership = postal_region.get(postal)
        if policy_version in {
            ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1,
            ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
        }:
            existing = updated.get("eligible_employee_codes")
            existing_codes: set[str] | None = None
            if isinstance(existing, (list, tuple, set)):
                existing_codes = {str(code).strip() for code in existing if str(code).strip()}
            if membership:
                region = str(membership.get("region_seq", "")).strip()
                required_center_type = region_required_center_types.get(region)
                if not required_center_type:
                    raise ValueError("ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_REGION_AREA_TYPE_INVALID")
                if policy_version == ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1:
                    # DMS2 is the fallback pool for DMS work; the solver's
                    # area-type preference cost keeps DMS technicians preferred.
                    # DMS2 work remains exclusive to the DMS2 roster.
                    candidates = (
                        set(dms_codes | dms2_codes)
                        if required_center_type == "DMS"
                        else set(dms2_codes)
                    )
                else:
                    # Immutable v1 type-hard snapshots retain exact center-type
                    # matching and are not reinterpreted by the fallback policy.
                    candidates = set(dms_codes if required_center_type == "DMS" else dms2_codes)
                updated["area_type"] = required_center_type
                updated["region_seq"] = membership.get("region_seq")
                updated["region_name"] = membership.get("region_name") or region_names.get(region) or f"Region {region}"
                updated["region_preference"] = {
                    "region_seq": membership.get("region_seq"),
                    "region_name": updated["region_name"],
                }
            else:
                # Absence from the preference map must not make work
                # infeasible.  It remains eligible to the whole active roster.
                candidates = set(active_codes)
                updated["region_preference_diagnostic"] = "REGION_PREFERENCE_UNRESOLVED"
            if existing_codes is not None:
                candidates &= existing_codes
            updated["hard_eligible_employee_codes"] = sorted(candidates)
            updated["eligible_employee_codes"] = sorted(candidates)
            updated["boundary_overflow_employee_codes"] = []
            updated.pop("region_plan_unassigned_marker", None)
            jobs.append(updated)
            continue
        if not membership:
            # Preserve the job so the solver reports POSTAL_NOT_IN_ACTIVE_PLAN;
            # aborting the batch would hide the individual unassigned reason.
            updated.pop("region_name", None)
            updated.pop("region_seq", None)
            updated["eligible_employee_codes"] = []
            updated["hard_eligible_employee_codes"] = []
            updated["boundary_overflow_employee_codes"] = []
            updated["region_plan_unassigned_marker"] = "POSTAL_NOT_IN_ACTIVE_PLAN"
            jobs.append(updated)
            continue
        region = str(membership.get("region_seq", "")).strip()
        hard = technicians_by_region.get(region, set()) & active_codes
        area_type = _normalize_area_type(membership.get("area_type", updated.get("area_type")))
        if area_type:
            updated["area_type"] = area_type
        if area_type in AREA_TYPE_DMS | AREA_TYPE_OVERLAP | AREA_TYPE_DMS2:
            area_eligible = set(_area_type_eligible_codes(area_type, dms_codes, dms2_codes))
            hard &= area_eligible
        existing = updated.get("eligible_employee_codes")
        existing_codes: set[str] | None = None
        if isinstance(existing, (list, tuple, set)):
            # An explicit empty set is a capability decision, never a request to
            # widen candidates from the region plan.
            existing_codes = {str(code).strip() for code in existing if str(code).strip()}
            hard &= existing_codes
        overflow = overflow_by_postal.get(postal, {})
        alternate = str(overflow.get("alternate_region_seq", "")).strip()
        approved = set()
        if overflow.get("allow_overflow") is True and alternate:
            approved = technicians_by_region.get(alternate, set()) & active_codes
            if area_type in AREA_TYPE_DMS | AREA_TYPE_OVERLAP | AREA_TYPE_DMS2:
                approved &= area_eligible
            if existing_codes is not None:
                approved &= existing_codes
        updated["region_seq"] = membership.get("region_seq")
        updated["region_name"] = (
            membership.get("region_name")
            or region_names.get(region)
            or str(updated.get("region_name") or "").strip()
            or f"Region {region}"
        )
        updated["hard_eligible_employee_codes"] = sorted(hard)
        # Solver treats eligible_employee_codes as the full hard candidate set;
        # overflow candidates must be present there to remain usable.
        updated["eligible_employee_codes"] = sorted(hard | approved)
        updated["boundary_overflow_employee_codes"] = sorted(approved - hard)
        if not (hard | approved):
            updated["region_plan_unassigned_marker"] = "NO_ELIGIBLE_TECHNICIAN"
        jobs.append(updated)
    execution["jobs"] = jobs
    options = dict(execution.get("options") or {})
    options["region_policy"] = policy_version
    options["region_plan"] = {
        "plan_id": str(snapshot["plan_id"]),
        "plan_revision": snapshot.get("revision", snapshot.get("plan_revision")),
        "policy_version": policy_version,
        "checksum": str(snapshot.get("checksum", snapshot.get("bundle_sha256"))),
        "activation_revision": snapshot.get("activation_revision"),
    }
    execution["options"] = options
    execution["region_plan"] = dict(options["region_plan"])
    return execution


def _normalize_area_type(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _area_type_eligible_codes(
    area_type: object,
    dms_codes: set[str],
    dms2_codes: set[str],
) -> list[str]:
    normalized = _normalize_area_type(area_type)
    if normalized in AREA_TYPE_DMS:
        # DMS areas are attempted by DMS first in the solver cost model.
        # DMS2 stays eligible as a fallback so remaining DMS-area jobs are
        # assigned instead of left unassigned when DMS capacity is exhausted.
        return sorted(dms_codes | dms2_codes)
    if normalized in AREA_TYPE_OVERLAP:
        return sorted(dms_codes | dms2_codes)
    if normalized in AREA_TYPE_DMS2:
        return sorted(dms2_codes)
    return []


def _uses_area_type_routing(strategic_city_name: object) -> bool:
    city = str(strategic_city_name or "").strip().upper()
    return city in AREA_TYPE_ROUTING_CITY_NAMES or any(city.endswith(suffix) for suffix in AREA_TYPE_ROUTING_CITY_SUFFIXES)


def _apply_job_area_type_rules(
    jobs: list[dict[str, Any]],
    technicians: list[dict[str, Any]],
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> list[dict[str, Any]]:
    if not jobs:
        return []
    if not _uses_area_type_routing(strategic_city_name):
        stripped_jobs: list[dict[str, Any]] = []
        for job in jobs:
            stripped_job = dict(job)
            plan_restricted = any(key in stripped_job for key in ("hard_eligible_employee_codes", "boundary_overflow_employee_codes", "region_plan_unassigned_marker"))
            if not plan_restricted:
                stripped_job.pop("area_type", None)
            # Atlanta_6area requests have already been restricted by their
            # immutable active-region snapshot.  This common preparation step
            # is also used by queued execution, so it must not erase the hard
            # candidate set (or the policy audit fields) on the second pass.
            # Keep the historic city behaviour unchanged.
            if not plan_restricted:
                stripped_job.pop("eligible_employee_codes", None)
            stripped_jobs.append(stripped_job)
        return stripped_jobs

    active_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
    }
    dms_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
        and str(tech.get("center_type", "")).strip().upper() == "DMS"
    }
    dms2_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
        and str(tech.get("center_type", "")).strip().upper() == "DMS2"
    }
    if not dms_codes and not dms2_codes:
        engineer_df = list_engineers(subsidiary_name, strategic_city_name, config_path=config_path)
        for _, row in engineer_df.iterrows():
            code = str(row.get("employee_code", "")).strip()
            if not code or (active_codes and code not in active_codes):
                continue
            center_type = str(row.get("center_type", "")).strip().upper()
            if center_type == "DMS":
                dms_codes.add(code)
            elif center_type == "DMS2":
                dms2_codes.add(code)

    region_area_lookup: dict[str, str] = {}
    region_df = list_regions(subsidiary_name, strategic_city_name, config_path=config_path)
    if not region_df.empty and {"postal_code", "area_type"}.issubset(region_df.columns):
        for _, row in region_df.iterrows():
            postal_code = str(row.get("postal_code", "")).strip().replace(".0", "")
            postal_code = postal_code.zfill(5) if postal_code else ""
            if postal_code:
                region_area_lookup[postal_code] = _normalize_area_type(row.get("area_type"))

    enriched_jobs: list[dict[str, Any]] = []
    for job in jobs:
        enriched_job = dict(job)
        postal_code = str(job.get("postal_code", "") or job.get("zip_code", "")).strip().replace(".0", "")
        postal_code = postal_code.zfill(5) if postal_code else ""
        area_type = _normalize_area_type(job.get("area_type")) or region_area_lookup.get(postal_code, "")
        if area_type:
            area_eligible = set(_area_type_eligible_codes(area_type, dms_codes, dms2_codes))
            existing_eligible = job.get("eligible_employee_codes")
            if isinstance(existing_eligible, (list, tuple, set)) and existing_eligible:
                existing_codes = {str(code).strip() for code in existing_eligible if str(code).strip()}
                area_eligible &= existing_codes
            if area_eligible:
                enriched_job["area_type"] = area_type
                enriched_job["eligible_employee_codes"] = sorted(area_eligible)
        enriched_jobs.append(enriched_job)
    return enriched_jobs


def _coerce_bool_value(value: object, default: bool = False) -> bool:
    if pd.isna(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "y", "yes", "t"}:
        return True
    if text in {"false", "0", "n", "no", "f", ""}:
        return False
    return default


def _priority_load_config(value: object) -> tuple[int, int]:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        priority_group = int(numeric)
    else:
        text = str(value or "").strip().upper()
        if text in {"A", "HIGH", "P3", "PRIORITY 3"}:
            priority_group = 3
        elif text in {"C", "LOW", "P1", "PRIORITY 1"}:
            priority_group = 1
        else:
            priority_group = 2
    priority_group = min(max(priority_group, 1), 3)
    return priority_group, 8


def _priority_group_label(value: object) -> str:
    priority_group, _ = _priority_load_config(value)
    return {3: "A", 2: "B", 1: "C"}.get(priority_group, "B")


def _shift_minutes(start_value: object, end_value: object, default_minutes: int = 540) -> int:
    start = pd.to_datetime(str(start_value or ""), format="%H:%M", errors="coerce")
    end = pd.to_datetime(str(end_value or ""), format="%H:%M", errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return int(default_minutes)
    minutes = int((end - start).total_seconds() / 60)
    if minutes <= 0:
        minutes += 24 * 60
    return max(1, minutes)


def _slot_based_max_minutes(slot_count: int, shift_minutes: int) -> int:
    slot_minutes = (max(0, int(slot_count)) + 1) * 60
    return max(1, min(int(shift_minutes), int(slot_minutes)))


def _positive_float_or_none(value: object) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric) or float(numeric) <= 0:
        return None
    return float(numeric)


def _default_time_limit_seconds(job_count: int, technician_count: int) -> int:
    if job_count <= 15:
        return 8
    if job_count <= 30:
        return 15
    if job_count <= 60:
        return 60
    if job_count <= 100:
        return 60
    if job_count <= 150:
        return 90
    return 120


def _routing_config_with_fallback(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    source_city = _runtime_source_city(subsidiary_name, strategic_city_name, config_path)
    config_row = get_routing_config(subsidiary_name, source_city, config_path=config_path) or {}
    seed = load_common_config(config_path).get("routing_seed", {}) or {}
    city_osrm_urls = seed.get("city_osrm_urls", {}) or {}
    city_overrides = seed.get("city_overrides", {}) or {}
    fallback = dict(seed)
    fallback.pop("city_osrm_urls", None)
    fallback.pop("city_overrides", None)
    fallback["osrm_url"] = city_osrm_urls.get(str(source_city), seed.get("osrm_url"))
    override = city_overrides.get(str(source_city), {})
    if isinstance(override, dict):
        fallback.update(override)
    nullable_constraint_keys = {
        "max_travel_min_per_sm_day",
        "max_travel_km_per_sm_day",
        "max_single_leg_min",
        "max_home_to_job_min",
        "long_leg_penalty_start_min",
        "long_leg_penalty_multiplier",
    }
    for key, value in config_row.items():
        if value is not None or key in nullable_constraint_keys:
            fallback[key] = value
    return fallback


def _build_server_routing_options(
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    routing_config = _routing_config_with_fallback(subsidiary_name, strategic_city_name, config_path=config_path)
    avoid_polygons: list[dict[str, Any]] = []
    avoid_area_df = list_avoid_areas(subsidiary_name, strategic_city_name, active_only=True, config_path=config_path)
    if not avoid_area_df.empty:
        for _, area_row in avoid_area_df.iterrows():
            try:
                geometry = json.loads(str(area_row.get("geometry_json", "")).strip())
            except Exception:
                continue
            avoid_polygons.append(
                {
                    "id": str(area_row.get("avoid_area_id", "")).strip(),
                    "name": str(area_row.get("area_name", "")).strip(),
                    "geometry": geometry,
                }
            )
    return {
        "distance_backend": str(routing_config.get("distance_backend", "city_osrm_else_haversine")),
        "osrm_url": str(routing_config.get("osrm_url", "")).rstrip("/"),
        "osrm_profile": str(routing_config.get("osrm_profile", "driving")),
        "region_policy": str(routing_config.get("region_policy", "home_distance_only")),
        "timezone_offset": str(routing_config.get("timezone_offset", "-04:00")).strip() or "-04:00",
        "avoid_polygons": avoid_polygons,
        "avoid_penalty_multiplier": 4.0,
        "max_work_min_per_sm_day": _positive_float_or_none(routing_config.get("max_work_min_per_sm_day")),
        "max_travel_min_per_sm_day": _positive_float_or_none(routing_config.get("max_travel_min_per_sm_day")),
        "max_travel_km_per_sm_day": _positive_float_or_none(routing_config.get("max_travel_km_per_sm_day")),
        "max_single_leg_min": _positive_float_or_none(routing_config.get("max_single_leg_min")),
        "max_home_to_job_min": _positive_float_or_none(routing_config.get("max_home_to_job_min")),
        "long_leg_penalty_start_min": _positive_float_or_none(routing_config.get("long_leg_penalty_start_min")),
        "long_leg_penalty_multiplier": _positive_float_or_none(routing_config.get("long_leg_penalty_multiplier")),
    }


def _with_server_routing_options(
    payload: dict[str, Any],
    subsidiary_name: str,
    strategic_city_name: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    execution_payload = copy.deepcopy(payload)
    options = dict(execution_payload.get("options") or {})
    options.update(_build_server_routing_options(subsidiary_name, strategic_city_name, config_path=config_path))
    existing_plan = options.get("region_plan")
    if (
        isinstance(existing_plan, dict)
        and existing_plan.get("plan_id")
    ):
        # The queued payload is the immutable active-plan snapshot.  Server
        # defaults must not downgrade its routing policy while it is running.
        options["region_policy"] = _atlanta6_snapshot_policy_version(existing_plan)
    execution_payload["options"] = options

    active_employee_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in list(execution_payload.get("technicians", []))
        if str(tech.get("employee_code", "")).strip()
    }
    capability_snapshot = options.get("capability_snapshot")
    if isinstance(capability_snapshot, dict) and capability_snapshot.get("sha256"):
        # A queued request is an immutable policy snapshot.  Do not let a
        # later master-data update change an already accepted routing job.
        capabilities = _normalize_capabilities(
            list(execution_payload.get("capabilities", [])),
            allowed_employee_codes=active_employee_codes or None,
        )
        capability_policy_present = True
    else:
        capability_rows, managed_capabilities = _managed_capability_rows(
            subsidiary_name,
            strategic_city_name,
            list(execution_payload.get("capabilities", [])),
            config_path=config_path,
        )
        capabilities = _normalize_capabilities(capability_rows, allowed_employee_codes=active_employee_codes or None)
        capability_policy_present = managed_capabilities
        execution_payload = _set_capability_snapshot(
            execution_payload,
            capabilities,
            managed=managed_capabilities,
        )
        options = dict(execution_payload.get("options") or {})
    execution_payload["capabilities"] = capabilities
    heavy_jobs = _enrich_jobs_heavy_repair(list(execution_payload.get("jobs", [])), config_path=config_path)
    area_jobs = _apply_job_area_type_rules(
        heavy_jobs,
        list(execution_payload.get("technicians", [])),
        subsidiary_name,
        strategic_city_name,
        config_path=config_path,
    )
    execution_payload["jobs"] = _apply_job_capabilities(
        area_jobs,
        capabilities,
        capability_policy_present=capability_policy_present,
    )

    engineer_master_df = _runtime_engineer_master(subsidiary_name, strategic_city_name, config_path=config_path)
    home_to_job_lookup: dict[str, int] = {}
    if not engineer_master_df.empty and "max_home_to_job_min" in engineer_master_df.columns:
        for _, row in engineer_master_df.iterrows():
            employee_code = str(row.get("employee_code", "")).strip()
            home_to_job_min = pd.to_numeric(pd.Series([row.get("max_home_to_job_min")]), errors="coerce").iloc[0]
            if employee_code and pd.notna(home_to_job_min):
                home_to_job_lookup[employee_code] = int(home_to_job_min)

    technicians: list[dict[str, Any]] = []
    for tech in list(execution_payload.get("technicians", [])):
        enriched_tech = dict(tech)
        employee_code = str(enriched_tech.get("employee_code", "")).strip()
        if employee_code in home_to_job_lookup:
            enriched_tech["max_home_to_job_min"] = home_to_job_lookup[employee_code]
        technicians.append(enriched_tech)
    execution_payload["technicians"] = technicians
    # A queued request already carries its immutable snapshot.  Only legacy direct
    # callers without it resolve the currently active plan.
    existing_plan = dict(execution_payload.get("options") or {}).get("region_plan")
    return _apply_active_region_plan(
        execution_payload,
        None if isinstance(existing_plan, dict) and existing_plan.get("plan_id") else _active_atlanta6_plan(subsidiary_name, strategic_city_name, config_path),
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_request_id(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    mode: str,
) -> str:
    raw = f"{subsidiary_name}_{strategic_city_name}_{promise_date}_{mode}".strip()
    return "common_" + "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in raw)


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_storage_config(config_path: Path = COMMON_CONFIG_PATH) -> dict[str, Any]:
    storage_cfg = load_common_config(config_path).get("storage", {}) or {}
    retention_days = int(storage_cfg.get("job_file_retention_days", 5) or 5)
    return {
        "save_job_files": bool(storage_cfg.get("save_job_files", False)),
        "job_file_retention_days": max(retention_days, 1),
        "job_archive_root": Path(storage_cfg.get("job_archive_root", COMMON_JOB_ARCHIVE_ROOT)),
    }


def _cleanup_common_job_archives(job_archive_root: Path, retention_days: int) -> None:
    if not job_archive_root.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(int(retention_days), 1))
    for job_dir in job_archive_root.iterdir():
        if not job_dir.is_dir():
            continue
        status_path = job_dir / "status.json"
        created_at: datetime | None = None
        if status_path.exists():
            try:
                status_payload = json.loads(status_path.read_text(encoding="utf-8"))
            except Exception:
                status_payload = {}
            created_at = _parse_iso_datetime(status_payload.get("created_at"))
        if created_at is None:
            created_at = datetime.fromtimestamp(job_dir.stat().st_mtime, tz=timezone.utc)
        if created_at >= cutoff:
            continue
        for path in sorted(job_dir.glob("**/*"), reverse=True):
            if path.is_file():
                path.unlink(missing_ok=True)
        for path in sorted(job_dir.glob("**/*"), reverse=True):
            if path.is_dir():
                path.rmdir()
        job_dir.rmdir()


def _write_common_job_archive(
    routing_job_id: str,
    *,
    request_payload: dict[str, Any] | None = None,
    status_payload: dict[str, Any] | None = None,
    result_payload: dict[str, Any] | None = None,
    error_message: str | None = None,
    config_path: Path = COMMON_CONFIG_PATH,
) -> None:
    storage_cfg = _load_storage_config(config_path)
    if not storage_cfg["save_job_files"]:
        return
    job_archive_root = storage_cfg["job_archive_root"]
    job_archive_root.mkdir(parents=True, exist_ok=True)
    _cleanup_common_job_archives(job_archive_root, storage_cfg["job_file_retention_days"])
    job_dir = job_archive_root / str(routing_job_id).strip()
    job_dir.mkdir(parents=True, exist_ok=True)
    if request_payload is not None:
        (job_dir / "request.json").write_text(
            json.dumps(request_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    if status_payload is not None:
        (job_dir / "status.json").write_text(
            json.dumps(status_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    if result_payload is not None:
        (job_dir / "result.json").write_text(
            json.dumps(result_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    error_path = job_dir / "error.txt"
    if error_message:
        error_path.write_text(str(error_message), encoding="utf-8")
    elif error_path.exists():
        error_path.unlink()


def _safe_write_common_job_archive(
    routing_job_id: str,
    *,
    request_payload: dict[str, Any] | None = None,
    status_payload: dict[str, Any] | None = None,
    result_payload: dict[str, Any] | None = None,
    error_message: str | None = None,
    config_path: Path = COMMON_CONFIG_PATH,
) -> bool:
    """Write the optional archive without failing the routing transaction.

    The database request/result is authoritative.  A missing or unwritable
    archive directory must be observable in logs, but must not kill the worker
    before it can persist the routing result.
    """
    try:
        _write_common_job_archive(
            routing_job_id,
            request_payload=request_payload,
            status_payload=status_payload,
            result_payload=result_payload,
            error_message=error_message,
            config_path=config_path,
        )
        return True
    except Exception:
        LOGGER.exception(
            "Common VRP job archive write failed; continuing without archive: job_id=%s config=%s",
            routing_job_id,
            config_path,
        )
        return False


def _update_routing_request_status(
    request_row: dict[str, Any],
    status_payload: dict[str, Any],
    *,
    config_path: Path = COMMON_CONFIG_PATH,
) -> None:
    updated_row = dict(request_row)
    updated_row["routing_status"] = str(status_payload.get("status", "")).strip()
    updated_row["status_json"] = json.dumps(status_payload, ensure_ascii=False)
    upsert_routing_request(updated_row, config_path=config_path)


def _process_common_routing_job(
    request_id: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> None:
    request_row = get_routing_request(request_id, config_path=config_path)
    if not request_row:
        return
    routing_job_id = str(request_row.get("routing_job_id", "")).strip()
    payload_text = str(request_row.get("payload_json", "") or "{}")
    request_payload = json.loads(payload_text)
    subsidiary_name = str(request_row.get("subsidiary_name", "")).strip()
    strategic_city_name = str(request_row.get("strategic_city_name", "")).strip()
    queued_status = {
        "job_id": routing_job_id,
        "status": "queued",
        "request_id": request_id,
        "mode": str(request_payload.get("mode", "")).strip(),
        "city": str(request_payload.get("city", "")).strip(),
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
    }
    running_status = dict(queued_status)
    running_status["status"] = "running"
    running_status["started_at"] = _utc_now_iso()
    running_status["updated_at"] = _utc_now_iso()
    _update_routing_request_status(request_row, running_status, config_path=config_path)
    _safe_write_common_job_archive(
        routing_job_id,
        request_payload=request_payload,
        status_payload=running_status,
        config_path=config_path,
    )
    try:
        execution_payload = _with_server_routing_options(
            request_payload,
            subsidiary_name,
            strategic_city_name,
            config_path=config_path,
        )
        result_payload = run_routing_request(execution_payload)
        region_plan = dict(execution_payload.get("options") or {}).get("region_plan")
        if isinstance(region_plan, dict) and region_plan.get("plan_id"):
            metadata = dict(result_payload.get("metadata") or {})
            metadata["region_plan"] = dict(region_plan)
            result_payload["metadata"] = metadata
        completed_status = dict(running_status)
        completed_status["status"] = "completed"
        completed_status["completed_at"] = _utc_now_iso()
        completed_status["updated_at"] = _utc_now_iso()
        completed_status["summary"] = result_payload.get("summary", {})
        if isinstance(region_plan, dict) and region_plan.get("plan_id"):
            completed_status["region_plan"] = dict(region_plan)
        _update_routing_request_status(request_row, completed_status, config_path=config_path)
        upsert_routing_result(
            {
                "request_id": request_id,
                "routing_job_id": routing_job_id,
                "result_json": json.dumps(result_payload, ensure_ascii=False),
            },
            config_path=config_path,
        )
        _safe_write_common_job_archive(
            routing_job_id,
            request_payload=request_payload,
            status_payload=completed_status,
            result_payload=result_payload,
            config_path=config_path,
        )
    except Exception as exc:
        failed_status = dict(running_status)
        failed_status["status"] = "failed"
        failed_status["completed_at"] = _utc_now_iso()
        failed_status["updated_at"] = _utc_now_iso()
        failed_status["error_message"] = str(exc)
        _update_routing_request_status(request_row, failed_status, config_path=config_path)
        _safe_write_common_job_archive(
            routing_job_id,
            request_payload=request_payload,
            status_payload=failed_status,
            error_message=str(exc),
            config_path=config_path,
        )


def _normalize_heavy_repair_rules(rule_df: pd.DataFrame) -> pd.DataFrame:
    if rule_df.empty:
        return pd.DataFrame(columns=["product_group_code", "product_code", "detailed_symptom_code"])
    rename_map = {
        "SERVICE_PRODUCT_GROUP_CODE": "product_group_code",
        "SERVICE_PRODUCT_CODE": "product_code",
        "SYMP_CODE_THREE": "detailed_symptom_code",
    }
    working = rule_df.rename(columns={k: v for k, v in rename_map.items() if k in rule_df.columns}).copy()
    required = ["product_group_code", "product_code", "detailed_symptom_code"]
    for col in required:
        if col not in working.columns:
            working[col] = ""
        working[col] = working[col].fillna("").astype(str).str.strip().str.upper()
    working = working[required]
    working = working[
        working["product_group_code"].ne("")
        & working["product_code"].ne("")
        & working["detailed_symptom_code"].ne("")
    ].copy()
    return working.drop_duplicates().reset_index(drop=True)


def _load_fallback_heavy_repair_rules() -> pd.DataFrame:
    heavy_repair_lookup_path = _default_heavy_repair_lookup_path()
    if heavy_repair_lookup_path.exists():
        lookup_df = pd.read_csv(heavy_repair_lookup_path, encoding="utf-8-sig")
    else:
        from .production_atlanta import _build_heavy_repair_lookup

        lookup_df = _build_heavy_repair_lookup(_default_symptom_path())
    return _normalize_heavy_repair_rules(lookup_df)


def _normalize_capabilities(capability_rows: list[dict[str, Any]], allowed_employee_codes: set[str] | None = None) -> list[dict[str, Any]]:
    if not capability_rows:
        return []

    def _coerce_bool(value: object, default: bool = False) -> bool:
        if pd.isna(value):
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"true", "1", "y", "yes", "t"}:
            return True
        if text in {"false", "0", "n", "no", "f", ""}:
            return False
        return bool(text)

    normalized_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for row in capability_rows:
        employee_code = str(row.get("employee_code", "")).strip()
        product_group_code = str(row.get("product_group_code", "")).strip().upper()
        product_code = str(row.get("product_code", "")).strip().upper()
        if not employee_code or not product_group_code:
            continue
        if allowed_employee_codes is not None and employee_code not in allowed_employee_codes:
            continue
        repair_allowed = _coerce_bool(row.get("repair_allowed", True), default=True)
        if not repair_allowed:
            continue
        capability_key = (employee_code, product_group_code, product_code)
        if capability_key in seen_keys:
            continue
        seen_keys.add(capability_key)
        normalized_rows.append(
            {
                "employee_code": employee_code,
                "product_group_code": product_group_code,
                "product_code": product_code,
                "heavy_repair_allowed": _coerce_bool(row.get("heavy_repair_allowed", True), default=True),
            }
        )
    return normalized_rows


def _apply_job_capabilities(
    jobs: list[dict[str, Any]],
    capabilities: list[dict[str, Any]],
    *,
    capability_policy_present: bool = False,
) -> list[dict[str, Any]]:
    if not jobs:
        return []
    if not capabilities and not capability_policy_present:
        return [dict(job) for job in jobs]

    capability_lookup: dict[tuple[str, str], dict[str, set[str]]] = {}
    group_capability_lookup: dict[str, dict[str, set[str]]] = {}
    for capability in capabilities:
        employee_code = str(capability.get("employee_code", "")).strip()
        product_group_code = str(capability.get("product_group_code", "")).strip().upper()
        product_code = str(capability.get("product_code", "")).strip().upper()
        if not employee_code or not product_group_code:
            continue
        bucket = (
            capability_lookup.setdefault((product_group_code, product_code), {"all": set(), "heavy": set()})
            if product_code
            else group_capability_lookup.setdefault(product_group_code, {"all": set(), "heavy": set()})
        )
        bucket["all"].add(employee_code)
        if bool(capability.get("heavy_repair_allowed", True)):
            bucket["heavy"].add(employee_code)

    enriched_jobs: list[dict[str, Any]] = []
    for job in jobs:
        enriched_job = dict(job)
        lookup_key = (
            str(job.get("product_group", "")).strip().upper(),
            str(job.get("product", "")).strip().upper(),
        )
        matched = capability_lookup.get(lookup_key)
        if matched is None:
            matched = group_capability_lookup.get(lookup_key[0], {"all": set(), "heavy": set()})
        if bool(job.get("is_heavy_repair", False)):
            eligible_codes = sorted(matched["heavy"])
        else:
            eligible_codes = sorted(matched["all"])
        existing_eligible = job.get("eligible_employee_codes")
        explicit_eligibility = isinstance(existing_eligible, (list, tuple, set))
        if explicit_eligibility:
            existing_codes = {str(code).strip() for code in existing_eligible if str(code).strip()}
            # Explicit candidates, including [], are a hard request policy.
            # Capabilities may only narrow that policy; a failed intersection
            # must remain empty rather than widening back to either source.
            eligible_codes = sorted(set(eligible_codes) & existing_codes)
        if eligible_codes or explicit_eligibility or capability_policy_present:
            enriched_job["eligible_employee_codes"] = eligible_codes
        else:
            enriched_job.pop("eligible_employee_codes", None)
        enriched_jobs.append(enriched_job)
    return enriched_jobs


def _build_payload_from_dataframes(
    jobs_df: pd.DataFrame,
    technicians_df: pd.DataFrame,
    capability_rows: list[dict[str, Any]],
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    config_path: Path = COMMON_CONFIG_PATH,
    mode: str = "na_general",
) -> dict[str, Any]:
    if jobs_df.empty:
        raise ValueError("No jobs found for the selected PROMISE_DATE.")
    if technicians_df.empty:
        raise ValueError("No technician list found for the selected PROMISE_DATE.")

    source_city = _runtime_source_city(subsidiary_name, strategic_city_name, config_path)
    active_plan = _active_atlanta6_plan(subsidiary_name, strategic_city_name, config_path)
    engineer_master_df = _runtime_engineer_master(subsidiary_name, strategic_city_name, config_path=config_path)
    active_flag_lookup = {
        str(row.get("employee_code", "")).strip(): _coerce_bool_value(row.get("active_flag", True), default=True)
        for _, row in engineer_master_df.iterrows()
        if str(row.get("employee_code", "")).strip()
    }
    technicians_df = technicians_df.copy()
    technicians_df["available"] = technicians_df["available"].fillna(False).astype(bool)
    technicians_df["available"] = technicians_df.apply(
        lambda row: bool(row.get("available", False))
        and active_flag_lookup.get(str(row.get("employee_code", "")).strip(), True),
        axis=1,
    )
    active_technicians = technicians_df[technicians_df["available"].fillna(False).astype(bool)].copy()
    if active_technicians.empty:
        raise ValueError("No available technicians selected.")
    master_employee_codes = {
        str(code).strip()
        for code in engineer_master_df.get("employee_code", pd.Series(dtype=str)).astype(str).tolist()
        if str(code).strip()
    }
    if master_employee_codes:
        active_technicians = active_technicians[
            active_technicians["employee_code"].astype(str).str.strip().isin(master_employee_codes)
        ].copy()
    if active_technicians.empty:
        raise ValueError("No available technicians with technician master rows selected.")
    active_employee_codes = {
        str(code).strip()
        for code in active_technicians.get("employee_code", pd.Series(dtype=str)).astype(str).tolist()
        if str(code).strip()
    }

    jobs_df = jobs_df.copy()
    if "reschedule" not in jobs_df.columns:
        jobs_df["reschedule"] = False
    jobs_df["reschedule"] = jobs_df["reschedule"].map(lambda value: _coerce_bool_value(value, default=False))
    if "fixed" not in jobs_df.columns:
        jobs_df["fixed"] = False
    jobs_df["fixed"] = jobs_df["fixed"].map(lambda value: _coerce_bool_value(value, default=False))
    if {"gsfs_receipt_no", "promise_date"}.issubset(jobs_df.columns):
        all_jobs_df = list_jobs(subsidiary_name, source_city, config_path=config_path)
        if not all_jobs_df.empty and {"gsfs_receipt_no", "promise_date"}.issubset(all_jobs_df.columns):
            historical_pairs = {
                (str(row.get("gsfs_receipt_no", "")).strip(), str(row.get("promise_date", "")).strip())
                for _, row in all_jobs_df.iterrows()
                if str(row.get("gsfs_receipt_no", "")).strip() and str(row.get("promise_date", "")).strip()
            }
            current_receipts = jobs_df["gsfs_receipt_no"].astype(str).str.strip()
            current_dates = jobs_df["promise_date"].astype(str).str.strip()
            historical_mask = [
                any(receipt == historical_receipt and date != historical_date for historical_receipt, historical_date in historical_pairs)
                for receipt, date in zip(current_receipts, current_dates)
            ]
            jobs_df["reschedule"] = jobs_df["reschedule"].astype(bool) | pd.Series(historical_mask, index=jobs_df.index)
    jobs_df["reschedule"] = (jobs_df["reschedule"].astype(bool) & ~jobs_df["fixed"].astype(bool)).astype(bool)

    state_value = str(jobs_df["state_name"].dropna().astype(str).iloc[0]).strip() if "state_name" in jobs_df.columns else ""
    custom_geo_rows: list[dict[str, Any]] = []
    missing_home_geo_rows: list[dict[str, Any]] = []
    tech_location_lookup: dict[str, tuple[float, float]] = {}

    for _, tech in active_technicians.iterrows():
        employee_code = str(tech["employee_code"]).strip()
        master_row = engineer_master_df[engineer_master_df["employee_code"].astype(str) == employee_code].head(1)
        if master_row.empty:
            continue
        master_row = master_row.iloc[0]

        start_type = str(tech.get("start_location_type", "Home")).strip() or "Home"
        if start_type == "Custom Address":
            address_line = str(tech.get("start_location_address", "")).strip()
            if not address_line:
                raise ValueError(f"Custom Address is selected but empty for technician {employee_code}")
            custom_geo_rows.append(
                {
                    "GSFS_RECEIPT_NO": employee_code,
                    "ADDRESS_LINE1_INFO": address_line,
                    "CITY_NAME": str(master_row.get("home_city", "")).strip(),
                    "STATE_NAME": str(master_row.get("home_state", state_value)).strip() or state_value,
                    "COUNTRY_NAME": str(master_row.get("home_country", "USA")).strip() or "USA",
                    "POSTAL_CODE": str(master_row.get("home_postal_code", "")).strip().replace(".0", ""),
                }
            )
            continue

        home_lat = pd.to_numeric(master_row.get("home_latitude"), errors="coerce")
        home_lng = pd.to_numeric(master_row.get("home_longitude"), errors="coerce")
        if pd.isna(home_lat) or pd.isna(home_lng):
            home_address = str(master_row.get("home_address", "")).strip()
            home_city = str(master_row.get("home_city", "")).strip()
            home_state = str(master_row.get("home_state", state_value)).strip() or state_value
            home_postal_code = str(master_row.get("home_postal_code", "")).strip().replace(".0", "")
            if not any([home_address, home_city, home_state, home_postal_code]):
                raise ValueError(f"Missing home coordinates for technician {employee_code}")
            missing_home_geo_rows.append(
                {
                    "GSFS_RECEIPT_NO": employee_code,
                    "ADDRESS_LINE1_INFO": home_address,
                    "CITY_NAME": home_city,
                    "STATE_NAME": home_state,
                    "COUNTRY_NAME": str(master_row.get("home_country", "USA")).strip() or "USA",
                    "POSTAL_CODE": home_postal_code,
                }
            )
            continue
        tech_location_lookup[employee_code] = (float(home_lat), float(home_lng))

    geo_rows = custom_geo_rows + missing_home_geo_rows
    if geo_rows:
        geocoded_custom_df = _merge_service_geocodes(pd.DataFrame(geo_rows), _load_runtime_config())
        geocoded_custom_df["latitude"] = pd.to_numeric(geocoded_custom_df.get("latitude"), errors="coerce")
        geocoded_custom_df["longitude"] = pd.to_numeric(geocoded_custom_df.get("longitude"), errors="coerce")
        failed_df = geocoded_custom_df[geocoded_custom_df["latitude"].isna() | geocoded_custom_df["longitude"].isna()].copy()
        if not failed_df.empty:
            failed_codes = ", ".join(failed_df["GSFS_RECEIPT_NO"].astype(str).tolist())
            raise ValueError(f"Failed to geocode technician home/start locations: {failed_codes}")
        tech_location_lookup.update(
            {
                str(row["GSFS_RECEIPT_NO"]).strip(): (float(row["latitude"]), float(row["longitude"]))
                for _, row in geocoded_custom_df.iterrows()
            }
        )

    planning_date = f"{str(promise_date)[:4]}-{str(promise_date)[4:6]}-{str(promise_date)[6:8]}"

    # Sort technicians geographically (west to east, then south to north)
    # so PATH_CHEAPEST_ARC builds regionally coherent initial routes consistently.
    if tech_location_lookup:
        _sort_lng = active_technicians["employee_code"].astype(str).str.strip().map(
            lambda c: tech_location_lookup.get(c, (0.0, 0.0))[1]
        )
        _sort_lat = active_technicians["employee_code"].astype(str).str.strip().map(
            lambda c: tech_location_lookup.get(c, (0.0, 0.0))[0]
        )
        active_technicians = active_technicians.copy()
        active_technicians["_sort_lng"] = _sort_lng
        active_technicians["_sort_lat"] = _sort_lat
        active_technicians = (
            active_technicians
            .sort_values(["_sort_lng", "_sort_lat"])
            .drop(columns=["_sort_lng", "_sort_lat"])
            .reset_index(drop=True)
        )

    technicians_payload: list[dict[str, Any]] = []
    for _, tech in active_technicians.iterrows():
        code = str(tech["employee_code"]).strip()
        if code not in tech_location_lookup:
            raise ValueError(f"Missing start location for technician {code}")
        lat, lng = tech_location_lookup[code]
        priority_score, _priority_default_max_jobs = _priority_load_config(tech.get("priority_group", "B"))
        priority_group = _priority_group_label(priority_score)
        shift_start = str(tech.get("shift_start", "08:00")).strip() or "08:00"
        shift_end = str(tech.get("shift_end", "18:00")).strip() or "18:00"
        slot_capacity = pd.to_numeric(pd.Series([tech.get("slot_count", 8)]), errors="coerce").iloc[0]
        max_slots = int(slot_capacity) if pd.notna(slot_capacity) else 8
        max_slots = max(0, max_slots)
        shift_minutes = _shift_minutes(shift_start, shift_end, default_minutes=540)
        configured_max_minutes = pd.to_numeric(pd.Series([tech.get("max_minutes")]), errors="coerce").iloc[0]
        max_minutes = (
            max(1, int(configured_max_minutes))
            if pd.notna(configured_max_minutes) and float(configured_max_minutes) > 0
            else _slot_based_max_minutes(max_slots, shift_minutes)
        )
        technicians_payload.append(
            {
                "employee_code": code,
                "employee_name": str(tech.get("employee_name", code)).strip() or code,
                "center_type": str(tech.get("center_type", "DMS")).strip().upper() or "DMS",
                "start_location": {"lat": float(lat), "lng": float(lng)},
                "end_location": {"lat": float(lat), "lng": float(lng)},
                "shift_start": shift_start,
                "shift_end": shift_end,
                "max_minutes": max_minutes,
                "slot_count": max_slots,
                "priority_group": priority_group,
                "preferred_region_name": str(tech.get("preferred_region_name", tech.get("preferred_area_name", ""))).strip(),
                "max_jobs": max_slots,
                "max_slots": max_slots,
            }
        )

    engineer_center_type_lookup: dict[str, str] = {
        str(r["employee_code"]).strip(): str(r.get("center_type", "DMS")).strip().upper() or "DMS"
        for _, r in engineer_master_df.iterrows()
        if str(r.get("employee_code", "")).strip()
    }
    active_center_lookup = {
        str(tech.get("employee_code", "")).strip(): str(tech.get("center_type", "")).strip().upper()
        for _, tech in active_technicians.iterrows()
        if str(tech.get("employee_code", "")).strip()
    }
    for code, center_type in engineer_center_type_lookup.items():
        if code in active_employee_codes and not active_center_lookup.get(code):
            active_center_lookup[code] = center_type
    dms_codes = {
        code
        for code, center_type in active_center_lookup.items()
        if code in active_employee_codes and center_type == "DMS"
    }
    dms2_codes = {
        code
        for code, center_type in active_center_lookup.items()
        if code in active_employee_codes and center_type == "DMS2"
    }
    region_area_lookup: dict[str, dict[str, Any]] = {}
    region_df = list_regions(subsidiary_name, source_city, config_path=config_path)
    if not region_df.empty and "postal_code" in region_df.columns:
        for _, region_row in region_df.iterrows():
            postal_code = str(region_row.get("postal_code", "")).strip().replace(".0", "")
            postal_code = postal_code.zfill(5) if postal_code else ""
            if not postal_code:
                continue
            region_area_lookup[postal_code] = {
                "area_type": _normalize_area_type(region_row.get("area_type")),
                "region_seq": region_row.get("region_seq"),
                "region_name": region_row.get("region_name"),
            }

    jobs_payload: list[dict[str, Any]] = []
    for _, row in jobs_df.iterrows():
        product_group = str(row.get("service_product_group_code", "")).strip().upper()
        product_code = str(row.get("service_product_code", "")).strip().upper()
        symptom = str(row.get("receipt_detail_symptom_code", "")).strip().upper()
        current_employee_code = str(row.get("svc_engineer_code", "")).strip()
        postal_code = str(row.get("postal_code", "")).strip().replace(".0", "")
        postal_code = postal_code.zfill(5) if postal_code else ""
        region_info = region_area_lookup.get(postal_code, {})
        area_type = (
            _normalize_area_type(row.get("area_type")) or _normalize_area_type(region_info.get("area_type"))
            if _uses_area_type_routing(strategic_city_name)
            else ""
        )
        eligible_employee_codes = _area_type_eligible_codes(area_type, dms_codes, dms2_codes)
        job_payload = {
            "salesforce_id": str(row.get("gsfs_receipt_no", "")).strip(),
            "receipt_no": str(row.get("gsfs_receipt_no", "")).strip(),
            "product_group": product_group,
            "product": product_code,
            "symptom": symptom,
            "address": str(row.get("address_line1_info", "")).strip(),
            "city_name": str(row.get("city_name", "")).strip(),
            "state_name": str(row.get("state_name", "")).strip(),
            "country_name": str(row.get("country_name", "USA")).strip() or "USA",
            "postal_code": postal_code,
            "location": {"lat": float(row["latitude"]), "lng": float(row["longitude"])},
            "time_window": [],
            "priority": 0,
            "fixed": _coerce_bool_value(row.get("fixed", False)),
            "reschedule": _coerce_bool_value(row.get("reschedule", False)),
            "job_slot_count": max(1, int(pd.to_numeric(pd.Series([row.get("job_slot_count", 2 if _coerce_bool_value(row.get("two_slot_job", False)) else 1)]), errors="coerce").fillna(1).iloc[0])),
            "current_employee_code": current_employee_code,
            "current_center_type": engineer_center_type_lookup.get(current_employee_code, "DMS"),
            "region_seq": region_info.get("region_seq"),
            "region_name": region_info.get("region_name"),
        }
        if area_type:
            job_payload["area_type"] = area_type
            if eligible_employee_codes:
                job_payload["eligible_employee_codes"] = eligible_employee_codes
        jobs_payload.append(job_payload)

    managed_capability_rows, managed_capabilities = _managed_capability_rows(
        subsidiary_name,
        strategic_city_name,
        capability_rows,
        config_path=config_path,
    )
    base_payload = {
        "request_id": uuid.uuid4().hex,
        "mode": mode,
        "city": strategic_city_name,
        "planning_date": planning_date,
        "options": {
            "respect_fixed_jobs": True,
            "objective": "min_total_travel_time",
            "time_limit_seconds": _default_time_limit_seconds(len(jobs_payload), len(technicians_payload)),
        },
        "technicians": technicians_payload,
        "jobs": jobs_payload,
        "capabilities": [],
    }
    planned_payload = _apply_active_region_plan(base_payload, active_plan)
    planned_employee_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in list(planned_payload.get("technicians", []))
        if str(tech.get("employee_code", "")).strip()
    }
    normalized_capabilities = _normalize_capabilities(
        managed_capability_rows,
        allowed_employee_codes=planned_employee_codes if active_plan is not None else active_employee_codes,
    )
    return _set_capability_snapshot(
        planned_payload,
        normalized_capabilities,
        managed=managed_capabilities,
    )


def build_payload_from_inputs(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    job_rows: list[dict[str, Any]],
    technician_rows: list[dict[str, Any]],
    capability_rows: list[dict[str, Any]] | None = None,
    config_path: Path = COMMON_CONFIG_PATH,
    mode: str = "na_general",
) -> dict[str, Any]:
    jobs_df = pd.DataFrame(job_rows)
    technicians_df = pd.DataFrame(technician_rows)
    return _build_payload_from_dataframes(
        jobs_df,
        technicians_df,
        capability_rows or [],
        subsidiary_name,
        strategic_city_name,
        promise_date,
        config_path=config_path,
        mode=mode,
    )


def _enrich_jobs_heavy_repair(jobs: list[dict[str, Any]], config_path: Path = COMMON_CONFIG_PATH) -> list[dict[str, Any]]:
    heavy_repair_rule_df = _normalize_heavy_repair_rules(list_heavy_repair_rules(config_path=config_path))
    if heavy_repair_rule_df.empty:
        heavy_repair_rule_df = _load_fallback_heavy_repair_rules()
    heavy_repair_exact_key = {
        (
            str(row.get("product_group_code", "")).strip().upper(),
            str(row.get("product_code", "")).strip().upper(),
            str(row.get("detailed_symptom_code", "")).strip().upper(),
        )
        for _, row in heavy_repair_rule_df.iterrows()
        if str(row.get("product_group_code", "")).strip() and str(row.get("product_code", "")).strip()
    }
    heavy_repair_group_key = {
        (
            str(row.get("product_group_code", "")).strip().upper(),
            str(row.get("detailed_symptom_code", "")).strip().upper(),
        )
        for _, row in heavy_repair_rule_df.iterrows()
        if str(row.get("product_group_code", "")).strip() and str(row.get("detailed_symptom_code", "")).strip()
    }
    enriched = []
    for job in jobs:
        product_group = str(job.get("product_group", "")).strip().upper()
        product_code = str(job.get("product", "")).strip().upper()
        symptom = str(job.get("symptom", "")).strip().upper()
        symptom_candidates = [symptom]
        if symptom:
            symptom_candidates.append(symptom[:5])
            symptom_candidates.append(symptom[:3])
        is_heavy_repair = any(
            (
                (product_group, product_code, candidate) in heavy_repair_exact_key
                or (product_group, candidate) in heavy_repair_group_key
            )
            for candidate in symptom_candidates
            if candidate
        )
        enriched_job = dict(job)
        numeric_slot = pd.to_numeric(pd.Series([job.get("job_slot_count")]), errors="coerce").iloc[0]
        job_slot_count = max(1, int(numeric_slot)) if pd.notna(numeric_slot) else (2 if _coerce_bool_value(job.get("two_slot_job", job.get("2slot_job", False)), default=False) else 1)
        if is_heavy_repair and job_slot_count < 2:
            job_slot_count = 2
        slot_minutes = 45 * job_slot_count
        enriched_job["is_heavy_repair"] = is_heavy_repair
        enriched_job["job_slot_count"] = job_slot_count
        enriched_job["service_minutes"] = max(slot_minutes, 100 if is_heavy_repair else 45)
        enriched.append(enriched_job)
    return enriched


def submit_routing_from_payload(
    payload: dict[str, Any],
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    enriched_payload = dict(payload)
    mode = str(enriched_payload.get("mode", "na_general")).strip() or "na_general"
    promise_text = str(promise_date).strip()
    planning_date = promise_text
    if len(promise_text) == 8 and promise_text.isdigit():
        planning_date = f"{promise_text[:4]}-{promise_text[4:6]}-{promise_text[6:8]}"
    request_id = _stable_request_id(subsidiary_name, strategic_city_name, str(promise_date), mode)
    enriched_payload["request_id"] = request_id
    enriched_payload["mode"] = mode
    enriched_payload["city"] = str(enriched_payload.get("city", "") or strategic_city_name).strip()
    enriched_payload["planning_date"] = str(enriched_payload.get("planning_date", "") or planning_date).strip()
    enriched_payload = _apply_active_region_plan(
        enriched_payload,
        _active_atlanta6_plan(subsidiary_name, strategic_city_name, config_path),
    )
    active_employee_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in list(enriched_payload.get("technicians", []))
        if str(tech.get("employee_code", "")).strip()
    }
    managed_capability_rows, managed_capabilities = _managed_capability_rows(
        subsidiary_name,
        strategic_city_name,
        list(payload.get("capabilities", [])),
        config_path=config_path,
    )
    enriched_payload = _set_capability_snapshot(
        enriched_payload,
        _normalize_capabilities(managed_capability_rows, allowed_employee_codes=active_employee_codes or None),
        managed=managed_capabilities,
    )
    routing_job_id = create_job_id(request_id)
    enriched_payload["routing_job_id"] = routing_job_id
    initial_status = {
        "job_id": routing_job_id,
        "status": "queued",
        "request_id": request_id,
        "mode": str(enriched_payload.get("mode", "")).strip(),
        "city": str(enriched_payload.get("city", "")).strip(),
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
    }
    delete_routing_requests_for_date(
        subsidiary_name,
        strategic_city_name,
        str(promise_date),
        keep_request_id=request_id,
        config_path=config_path,
    )
    delete_routing_result(request_id, config_path=config_path)
    request_row = {
        "request_id": request_id,
        "subsidiary_name": subsidiary_name,
        "strategic_city_name": strategic_city_name,
        "promise_date": str(promise_date),
        "routing_job_id": routing_job_id,
        "routing_status": "queued",
        "payload_json": json.dumps(enriched_payload, ensure_ascii=False),
        "status_json": json.dumps(initial_status, ensure_ascii=False),
    }
    upsert_routing_request(request_row, config_path=config_path)
    _safe_write_common_job_archive(
        routing_job_id,
        request_payload=enriched_payload,
        status_payload=initial_status,
        config_path=config_path,
    )
    threading.Thread(
        target=_process_common_routing_job,
        args=(request_id, config_path),
        daemon=True,
    ).start()
    return {
        "request_id": request_id,
        "routing_job_id": routing_job_id,
        "status": "queued",
    }


def submit_routing_from_inputs(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    job_rows: list[dict[str, Any]],
    technician_rows: list[dict[str, Any]],
    capability_rows: list[dict[str, Any]] | None = None,
    config_path: Path = COMMON_CONFIG_PATH,
    mode: str = "na_general",
) -> dict[str, Any]:
    payload = build_payload_from_inputs(
        subsidiary_name,
        strategic_city_name,
        promise_date,
        job_rows,
        technician_rows,
        capability_rows=capability_rows,
        config_path=config_path,
        mode=mode,
    )
    response = submit_routing_from_payload(
        payload,
        subsidiary_name,
        strategic_city_name,
        promise_date,
        config_path=config_path,
    )
    response["payload"] = payload
    return response


def refresh_routing_result(
    request_id: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    request_row = get_routing_request(request_id, config_path=config_path)
    if not request_row:
        raise ValueError(f"Unknown request_id: {request_id}")
    routing_job_id = str(request_row.get("routing_job_id", "")).strip()
    if not routing_job_id:
        raise ValueError("Missing routing_job_id for request.")

    status_payload = json.loads(str(request_row.get("status_json", "") or "{}"))
    if not status_payload:
        status_payload = {
            "job_id": routing_job_id,
            "status": str(request_row.get("routing_status", "")).strip() or "queued",
            "request_id": request_id,
        }

    result_payload: dict[str, Any] | None = None
    saved_result = get_routing_result(request_id, config_path=config_path)
    if saved_result and saved_result.get("result_json"):
        result_payload = json.loads(str(saved_result["result_json"]))

    return {
        "request_id": request_id,
        "routing_job_id": routing_job_id,
        "status": status_payload,
        "result": result_payload,
    }


def get_latest_routing_snapshot(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any] | None:
    request_row = get_latest_routing_request(subsidiary_name, strategic_city_name, promise_date, config_path=config_path)
    if not request_row:
        return None
    result_row = get_routing_result(str(request_row["request_id"]), config_path=config_path)
    return {
        "request": request_row,
        "result": json.loads(str(result_row["result_json"])) if result_row and result_row.get("result_json") else None,
    }
