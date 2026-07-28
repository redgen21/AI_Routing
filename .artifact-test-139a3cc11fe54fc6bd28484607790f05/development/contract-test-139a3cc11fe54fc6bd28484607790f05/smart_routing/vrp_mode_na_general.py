from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from .osrm_routing import OSRMConfig, OSRMTripClient
from .vrp_api_common import (
    DEFAULT_TIMEZONE_OFFSET,
    build_empty_result,
    format_planned_timestamp,
    normalize_city,
    normalize_mode,
)


HOME_DISTANCE_ONLY = "home_distance_only"
PREFERRED_REGION_SOFT = "preferred_region_soft"
LA_6AREA_CITY = "LA_6area"
ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1 = "active_roster_type_hard_region_soft/v1"
ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1 = "active_roster_area_type_fallback_region_soft/v1"
OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1 = "own_region_with_approved_boundary_overflow/v1"
OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2 = "own_region_with_approved_boundary_overflow/v2"
EXPLICIT_WORKBOOK_MEMBERSHIP_V1 = "explicit_workbook_membership/v1"
REGION_PLAN_MEMBERSHIP_POLICIES = frozenset({
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
    EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
})
SOFT_REGION_AFFINITY_POLICIES = frozenset({
    PREFERRED_REGION_SOFT,
    ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1,
    ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
})
CITY_ROUTING_POLICY = {
    "Atlanta, GA": HOME_DISTANCE_ONLY,
    # Compatibility fallback for existing Atlanta_6area requests that predate
    # immutable active-plan metadata.  New plans are selected by policy, not
    # strategic-city name.
    "Atlanta_6area": OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
    "Los Angeles, CA": PREFERRED_REGION_SOFT,
    "Los Angeles, CA - Area Type Clusters": PREFERRED_REGION_SOFT,
    "Los Angeles, CA - Bucket Sim Draft": PREFERRED_REGION_SOFT,
    # LA_6area has a mixed DMS/DMS2 active plan.  Region membership is a
    # preference there; the hard split is the job's DMS/DMS2 area type.
    LA_6AREA_CITY: PREFERRED_REGION_SOFT,
}


def resolve_city_routing_policy(request_payload: dict[str, Any]) -> str:
    city_name = str(request_payload.get("city", "")).strip()
    options = request_payload.get("options") or {}
    configured = str(options.get("region_policy", "")).strip()
    active_plan = options.get("region_plan") or {}
    active_plan_policy = (
        str(active_plan.get("policy_version", "")).strip()
        if isinstance(active_plan, dict)
        else ""
    )
    if configured and active_plan_policy and configured != active_plan_policy:
        raise ValueError(
            "options.region_policy conflicts with immutable active-plan policy_version: "
            f"{configured!r} != {active_plan_policy!r}"
        )
    immutable_policy = configured or active_plan_policy
    if immutable_policy in REGION_PLAN_MEMBERSHIP_POLICIES:
        return immutable_policy
    if city_name == "Atlanta_6area" and immutable_policy:
        raise ValueError(
            "Atlanta_6area region_policy must be an explicitly supported active-plan policy: "
            f"{sorted(REGION_PLAN_MEMBERSHIP_POLICIES)}; got {immutable_policy!r}"
        )
    if immutable_policy in {HOME_DISTANCE_ONLY, *SOFT_REGION_AFFINITY_POLICIES}:
        return immutable_policy
    if active_plan_policy:
        raise ValueError(
            "active-plan policy_version must be a supported routing policy: "
            f"{[HOME_DISTANCE_ONLY, *sorted(SOFT_REGION_AFFINITY_POLICIES), *sorted(REGION_PLAN_MEMBERSHIP_POLICIES)]}; "
            f"got {active_plan_policy!r}"
        )
    return CITY_ROUTING_POLICY.get(city_name, HOME_DISTANCE_ONLY)


def _uses_own_region_boundary_policy(region_policy: object) -> bool:
    return str(region_policy or "").strip() in REGION_PLAN_MEMBERSHIP_POLICIES


def _explicit_employee_codes(value: object) -> list[str] | None:
    if not isinstance(value, (list, tuple, set)):
        return None
    return sorted({str(code).strip() for code in value if str(code).strip()})


def _hard_eligible_employee_codes(job: dict[str, Any]) -> list[str] | None:
    """Return the explicit hard candidate set without widening an empty set."""

    return _explicit_employee_codes(job.get("eligible_employee_codes"))


def _approved_boundary_overflow_employee_codes(job: dict[str, Any]) -> list[str]:
    """Return approved overflow candidates intersected with the hard candidate set."""

    raw_codes = job.get("boundary_overflow_employee_codes")
    if not isinstance(raw_codes, (list, tuple, set)):
        return []
    approved_codes = {str(code).strip() for code in raw_codes if str(code).strip()}
    hard_codes = _hard_eligible_employee_codes(job)
    if hard_codes is None:
        return []
    return sorted(approved_codes & set(hard_codes))


def _technician_assigned_region_name(technician: dict[str, Any]) -> str:
    assigned_region = str(technician.get("assigned_region_name") or "").strip()
    if assigned_region:
        return assigned_region
    return str(
        technician.get("preferred_region_name")
        or technician.get("preferred_area_name")
        or ""
    ).strip()


def _job_region_preference_name(job: dict[str, Any]) -> str:
    """Normalize the runtime's soft-region preference without stringifying it."""

    preference = job.get("region_preference")
    if isinstance(preference, dict):
        name = str(preference.get("region_name") or "").strip()
        if name:
            return name
    elif preference is not None:
        name = str(preference).strip()
        if name:
            return name
    return str(job.get("region_name") or "").strip()


def _fixed_technician_outside_active_plan(
    job: dict[str, Any],
    technician_codes: set[str],
    region_policy: str,
) -> bool:
    if not _uses_own_region_boundary_policy(region_policy):
        return False
    current_code = str(job.get("current_employee_code") or "").strip()
    if not current_code or current_code not in technician_codes:
        return False
    hard_codes = _hard_eligible_employee_codes(job)
    return hard_codes is not None and current_code not in hard_codes


def _postal_is_in_active_region_plan(job: dict[str, Any], region_policy: str) -> bool:
    if not _uses_own_region_boundary_policy(region_policy):
        return True
    return bool(str(job.get("region_name") or "").strip())


def _build_city_route_client(request_payload: dict[str, Any]) -> OSRMTripClient:
    options = request_payload.get("options") or {}
    osrm_url = str(options.get("osrm_url", "")).strip().rstrip("/")
    if not osrm_url:
        import smart_routing.production_assign_atlanta as base

        routing_cfg = base._load_config().get("routing", {})
        city_urls = routing_cfg.get("city_osrm_urls", {}) or {}
        osrm_url = str(
            city_urls.get(str(request_payload.get("city", "")).strip(), routing_cfg.get("osrm_url", ""))
        ).strip().rstrip("/")
    if not osrm_url:
        raise ValueError(f"Missing OSRM URL for city {request_payload.get('city', '')!r}")
    backend = str(options.get("distance_backend", "city_osrm_else_haversine")).strip().lower()
    mode = "haversine" if backend == "haversine" else "osrm"
    city_slug = re.sub(r"[^a-z0-9]+", "_", str(request_payload.get("city", "city")).lower()).strip("_")
    return OSRMTripClient(
        OSRMConfig(
            osrm_url=osrm_url,
            mode=mode,
            osrm_profile=str(options.get("osrm_profile", "driving")).strip() or "driving",
            cache_file=Path(f"data/cache/osrm_trip_cache_common_{city_slug}.csv"),
            fail_closed_on_osrm_error=_coerce_bool_value(
                options.get("fail_closed_on_osrm_error", False)
            ),
        )
    )


def _load_reference_inputs(
    request_payload: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return optional legacy Atlanta references.

    Common routing is payload/DB-driven and must not require workstation files.
    The legacy fallback remains opt-in for historical comparison requests only.
    """

    use_legacy = bool(((request_payload or {}).get("options") or {}).get("use_legacy_reference_inputs", False))
    if not use_legacy:
        return (
            pd.DataFrame(columns=["POSTAL_CODE", "region_seq", "new_region_name"]),
            pd.DataFrame(columns=["SVC_ENGINEER_CODE"]),
            pd.DataFrame(columns=["SVC_ENGINEER_CODE"]),
        )

    import smart_routing.production_assign_atlanta as base

    required_paths = (base.REGION_ZIP_PATH, base.ENGINEER_REGION_PATH, base.HOME_GEOCODE_PATH)
    missing = [str(path) for path in required_paths if not Path(path).is_file()]
    if missing:
        raise FileNotFoundError(f"Legacy reference inputs were explicitly requested but are missing: {missing}")
    region_zip_df = pd.read_csv(base.REGION_ZIP_PATH, encoding="utf-8-sig")
    engineer_region_df = pd.read_csv(base.ENGINEER_REGION_PATH, encoding="utf-8-sig")
    home_df = pd.read_csv(base.HOME_GEOCODE_PATH, encoding="utf-8-sig")
    region_zip_df["POSTAL_CODE"] = region_zip_df["POSTAL_CODE"].astype(str).str.zfill(5)
    return region_zip_df, engineer_region_df, home_df


def _build_region_lookup(region_zip_df: pd.DataFrame) -> dict[str, tuple[int, str]]:
    if region_zip_df.empty or not {"POSTAL_CODE", "region_seq", "new_region_name"} <= set(region_zip_df.columns):
        return {}
    lookup_df = region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].dropna(subset=["POSTAL_CODE"]).drop_duplicates()
    lookup_df["POSTAL_CODE"] = lookup_df["POSTAL_CODE"].astype(str).str.zfill(5)
    return {
        str(row["POSTAL_CODE"]).zfill(5): (int(row["region_seq"]), str(row["new_region_name"]))
        for _, row in lookup_df.iterrows()
        if pd.notna(row["region_seq"])
    }


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


def _build_region_centers_from_service_df(service_df: pd.DataFrame) -> dict[int, tuple[float, float]]:
    if service_df.empty or "region_seq" not in service_df.columns:
        return {}
    working = service_df.copy()
    working["region_seq"] = pd.to_numeric(working["region_seq"], errors="coerce")
    working["latitude"] = pd.to_numeric(working["latitude"], errors="coerce")
    working["longitude"] = pd.to_numeric(working["longitude"], errors="coerce")
    working = working.dropna(subset=["region_seq", "latitude", "longitude"]).copy()
    if working.empty:
        return {}
    centers = (
        working.groupby("region_seq")
        .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"))
        .reset_index()
    )
    return {
        int(row["region_seq"]): (float(row["longitude"]), float(row["latitude"]))
        for _, row in centers.iterrows()
    }


def _nearest_region(
    lon: float,
    lat: float,
    region_centers: dict[int, tuple[float, float]],
) -> tuple[int, str]:
    import smart_routing.production_assign_atlanta as base

    best_region = 1
    best_km = None
    for region_seq, center in region_centers.items():
        km = base._haversine_distance_km((float(lon), float(lat)), center)
        if best_km is None or km < best_km:
            best_km = km
            best_region = int(region_seq)
    return best_region, f"Atlanta New Region {best_region}"


def _build_engineer_frames_from_payload(
    request_payload: dict[str, Any],
    reference_engineer_region_df: pd.DataFrame,
    reference_home_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    region_policy = resolve_city_routing_policy(request_payload)
    ref_engineer = reference_engineer_region_df.copy()
    ref_engineer["SVC_ENGINEER_CODE"] = ref_engineer["SVC_ENGINEER_CODE"].astype(str)
    ref_home = reference_home_df.copy()
    ref_home["SVC_ENGINEER_CODE"] = ref_home["SVC_ENGINEER_CODE"].astype(str)
    reference_order = {
        code: idx
        for idx, code in enumerate(ref_engineer["SVC_ENGINEER_CODE"].astype(str).str.strip().tolist())
        if code
    }
    technicians = sorted(
        list(request_payload.get("technicians", [])),
        key=lambda tech: (
            reference_order.get(str(tech.get("employee_code", "")).strip(), 1_000_000),
            str(tech.get("employee_code", "")).strip(),
        ),
    )

    requested_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
    }
    if not requested_codes:
        requested_codes = {
            str(job.get("current_employee_code", "")).strip()
            for job in request_payload.get("jobs", [])
            if str(job.get("current_employee_code", "")).strip()
        }
    if requested_codes:
        ref_engineer = ref_engineer[ref_engineer["SVC_ENGINEER_CODE"].astype(str).isin(requested_codes)].copy()
        ref_home = ref_home[ref_home["SVC_ENGINEER_CODE"].astype(str).isin(requested_codes)].copy()

    engineer_rows: list[dict[str, Any]] = []
    home_rows: list[dict[str, Any]] = []
    for tech in technicians:
        code = str(tech.get("employee_code", "")).strip()
        if not code:
            continue
        name = str(tech.get("employee_name", code)).strip() or code
        start_location = tech.get("start_location") or {}
        start_lat = pd.to_numeric(pd.Series([start_location.get("lat")]), errors="coerce").iloc[0]
        start_lng = pd.to_numeric(pd.Series([start_location.get("lng")]), errors="coerce").iloc[0]

        matched_region = ref_engineer[ref_engineer["SVC_ENGINEER_CODE"].astype(str) == code].head(1)
        matched_home = ref_home[ref_home["SVC_ENGINEER_CODE"].astype(str) == code].head(1)
        if not matched_region.empty:
            region_row = matched_region.iloc[0].to_dict()
        else:
            if pd.notna(start_lat) and pd.notna(start_lng):
                region_seq, region_name = _nearest_region(float(start_lng), float(start_lat), region_centers)
            else:
                region_seq, region_name = 1, "Atlanta New Region 1"
            region_row = {
                "SVC_ENGINEER_CODE": code,
                "assigned_region_seq": int(region_seq),
                "zip_overlap_count": 0,
                "zip_overlap_ratio": 0.0,
                "AREA_NAME": f"{code}_{name}",
                "SVC_CENTER_TYPE": str(tech.get("center_type", "DMS")).strip().upper() or "DMS",
                "assigned_region_name": region_name,
                "preferred_region_rank_1": pd.NA,
                "preferred_region_rank_2": pd.NA,
                "preferred_region_rank_3": pd.NA,
                "anchor_region_seq": int(region_seq),
                "anchor_region_name": region_name,
                "Name": name,
                "normalized_slot": 8,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            }
        region_row["SVC_ENGINEER_CODE"] = code
        region_row["Name"] = name
        region_row["SVC_CENTER_TYPE"] = str(tech.get("center_type", region_row.get("SVC_CENTER_TYPE", "DMS"))).strip().upper() or "DMS"
        if pd.notna(start_lat) and pd.notna(start_lng):
            region_row["latitude"] = float(start_lat)
            region_row["longitude"] = float(start_lng)
        elif not matched_home.empty:
            matched_home_lat = pd.to_numeric(pd.Series([matched_home.iloc[0].get("latitude")]), errors="coerce").iloc[0]
            matched_home_lng = pd.to_numeric(pd.Series([matched_home.iloc[0].get("longitude")]), errors="coerce").iloc[0]
            if pd.notna(matched_home_lat) and pd.notna(matched_home_lng):
                region_row["latitude"] = float(matched_home_lat)
                region_row["longitude"] = float(matched_home_lng)
        priority_value = tech.get("priority_group", 2)
        priority_group = pd.to_numeric(pd.Series([priority_value]), errors="coerce").fillna(2).iloc[0]
        if pd.isna(pd.to_numeric(pd.Series([priority_value]), errors="coerce").iloc[0]):
            priority_text = str(priority_value or "").strip().upper()
            if priority_text in {"A", "HIGH", "P3", "PRIORITY 3"}:
                priority_group = 3
            elif priority_text in {"C", "LOW", "P1", "PRIORITY 1"}:
                priority_group = 1
            else:
                priority_group = 2
        slot_capacity = pd.to_numeric(pd.Series([tech.get("slot_count", tech.get("max_slots", tech.get("max_jobs", 8)))]), errors="coerce").fillna(8).iloc[0]
        max_slot_capacity = max(0, int(slot_capacity))
        max_minutes = pd.to_numeric(pd.Series([tech.get("max_minutes", 540)]), errors="coerce").fillna(540).iloc[0]
        slot_based_max_minutes = (max_slot_capacity + 1) * 60
        max_minutes = min(int(max_minutes), int(slot_based_max_minutes))
        region_row["priority_group"] = min(max(int(priority_group), 1), 3)
        assigned_region_name = _technician_assigned_region_name(tech)
        if _uses_own_region_boundary_policy(region_policy) and assigned_region_name:
            region_row["assigned_region_name"] = assigned_region_name
        region_row["preferred_region_name"] = (
            _technician_assigned_region_name(tech)
            if region_policy in SOFT_REGION_AFFINITY_POLICIES
            else ""
        )
        region_row["max_jobs"] = max_slot_capacity
        region_row["max_slots"] = max_slot_capacity
        region_row["max_minutes"] = max(1, int(max_minutes))
        max_home_to_job_override = pd.to_numeric(pd.Series([tech.get("max_home_to_job_min")]), errors="coerce").iloc[0]
        region_row["max_home_to_job_min"] = int(max_home_to_job_override) if pd.notna(max_home_to_job_override) else pd.NA
        if "ref_heavy_repair_flag" in tech:
            region_row["REF_HEAVY_REPAIR_FLAG"] = str(tech.get("ref_heavy_repair_flag", "Y")).strip().upper() or "Y"
        engineer_rows.append(region_row)

        if not matched_home.empty:
            home_row = matched_home.iloc[0].to_dict()
        else:
            home_row = {
                "SVC_ENGINEER_CODE": code,
                "Name": name,
                "Home Street Address": "",
                "City ": "",
                "State": "",
                "Zip": "",
                "matched_address": "",
                "match_indicator": "",
                "match_type": "",
                "source": "api_payload",
                "SVC_CENTER_TYPE": region_row["SVC_CENTER_TYPE"],
                "normalized_slot": region_row.get("normalized_slot", 8),
                "REF_HEAVY_REPAIR_FLAG": region_row.get("REF_HEAVY_REPAIR_FLAG", "Y"),
                "assigned_region_seq": region_row.get("assigned_region_seq"),
                "assigned_region_name": region_row.get("assigned_region_name"),
            }
        home_row["SVC_ENGINEER_CODE"] = code
        home_row["Name"] = name
        home_row["SVC_CENTER_TYPE"] = region_row["SVC_CENTER_TYPE"]
        if pd.notna(start_lat) and pd.notna(start_lng):
            home_row["latitude"] = float(start_lat)
            home_row["longitude"] = float(start_lng)
        else:
            if "latitude" not in home_row:
                home_row["latitude"] = pd.NA
            if "longitude" not in home_row:
                home_row["longitude"] = pd.NA
        home_rows.append(home_row)

    if not engineer_rows and requested_codes:
        engineer_rows = ref_engineer.to_dict("records")
    if not home_rows and requested_codes:
        home_rows = ref_home.to_dict("records")
    return pd.DataFrame(engineer_rows), pd.DataFrame(home_rows)


def _build_service_frame_from_payload(
    request_payload: dict[str, Any],
    region_lookup: dict[str, tuple[int, str]],
) -> pd.DataFrame:
    import smart_routing.production_atlanta as prod

    planning_date = str(request_payload.get("planning_date", "")).strip()
    region_policy = resolve_city_routing_policy(request_payload)
    jobs = list(request_payload.get("jobs", []))
    rows: list[dict[str, Any]] = []
    for job in jobs:
        receipt_no = str(job.get("receipt_no", "")).strip()
        salesforce_id = str(job.get("salesforce_id", "")).strip()
        location = job.get("location") or {}
        lat = pd.to_numeric(pd.Series([location.get("lat")]), errors="coerce").iloc[0]
        lng = pd.to_numeric(pd.Series([location.get("lng")]), errors="coerce").iloc[0]
        if pd.isna(lat) or pd.isna(lng):
            continue
        postal_code = str(job.get("postal_code", "") or job.get("zip_code", "")).strip()
        if postal_code:
            postal_code = postal_code.zfill(5)
        requested_region_seq = pd.to_numeric(pd.Series([job.get("region_seq")]), errors="coerce").iloc[0]
        requested_region_name = _job_region_preference_name(job)
        if _uses_own_region_boundary_policy(region_policy):
            region_seq = int(requested_region_seq) if pd.notna(requested_region_seq) else pd.NA
            region_name = requested_region_name
        elif pd.notna(requested_region_seq):
            region_seq = int(requested_region_seq)
            region_name = requested_region_name or f"Region {region_seq}"
        elif postal_code and postal_code in region_lookup:
            region_seq, region_name = region_lookup[postal_code]
        else:
            region_seq, region_name = pd.NA, ""
        time_window = job.get("time_window") or []
        center_type = str(job.get("current_center_type", "")).strip().upper() or "DMS"
        if not prod.ENABLE_DMS2 and center_type == prod.DMS2_CENTER_TYPE:
            center_type = prod.DMS_CENTER_TYPE
        numeric_slot = pd.to_numeric(pd.Series([job.get("job_slot_count")]), errors="coerce").iloc[0]
        if pd.isna(numeric_slot):
            two_slot_text = str(job.get("two_slot_job", job.get("2slot_job", False))).strip().lower()
            job_slot_count = 2 if two_slot_text in {"true", "1", "y", "yes", "t"} else 1
        else:
            job_slot_count = max(1, int(numeric_slot))
        service_minutes = pd.to_numeric(pd.Series([job.get("service_minutes")]), errors="coerce").iloc[0]
        if pd.isna(service_minutes):
            heavy_text = str(job.get("is_heavy_repair", False)).strip().lower()
            is_heavy_repair = bool(job.get("is_heavy_repair", False)) if heavy_text not in {"true", "1", "y", "yes", "t", "false", "0", "n", "no", "f", ""} else heavy_text in {"true", "1", "y", "yes", "t"}
            slot_minutes = 45 * job_slot_count
            service_minutes = max(slot_minutes, 100 if is_heavy_repair else 45)
        eligible_employee_codes = _hard_eligible_employee_codes(job)
        if not _postal_is_in_active_region_plan(job, region_policy):
            eligible_employee_codes = []
        boundary_overflow_employee_codes = (
            _approved_boundary_overflow_employee_codes(job)
            if _uses_own_region_boundary_policy(region_policy)
            else []
        )
        rows.append(
            {
                "salesforce_id": salesforce_id,
                "GSFS_RECEIPT_NO": receipt_no or salesforce_id,
                "SVC_ENGINEER_CODE": str(job.get("current_employee_code", "")).strip(),
                "SVC_ENGINEER_NAME": str(job.get("current_employee_name", job.get("current_employee_code", ""))).strip(),
                "SERVICE_PRODUCT_GROUP_CODE": str(job.get("product_group", "") or job.get("product", "")).strip().upper(),
                "SERVICE_PRODUCT_CODE": str(job.get("product", "") or job.get("service_product_code", "")).strip().upper(),
                "RECEIPT_DETAIL_SYMPTOM_CODE": str(job.get("symptom", "") or job.get("symptom_code", "")).strip().upper(),
                "ADDRESS_LINE1_INFO": str(job.get("address", "")).strip(),
                "CITY_NAME": str(job.get("city_name", "")).strip(),
                "STATE_NAME": str(job.get("state_name", "")).strip(),
                "COUNTRY_NAME": str(job.get("country_name", "USA")).strip() or "USA",
                "POSTAL_CODE": postal_code,
                "latitude": float(lat),
                "longitude": float(lng),
                "service_date": pd.to_datetime(planning_date, errors="coerce"),
                "service_date_key": str(planning_date),
                "PROMISE_DATE": planning_date.replace("-", ""),
                "PROMISE_TIMESTAMP": f"{planning_date}T{str(time_window[0]).strip() if len(time_window) >= 1 else '09:00'}:00",
                "time_window_start": str(time_window[0]).strip() if len(time_window) >= 1 else "",
                "time_window_end": str(time_window[1]).strip() if len(time_window) >= 2 else "",
                "priority": int(pd.to_numeric(pd.Series([job.get('priority', 0)]), errors='coerce').fillna(0).iloc[0]),
                "fixed": _coerce_bool_value(job.get("fixed", False)),
                "reschedule": (
                    _coerce_bool_value(job.get("reschedule", False))
                    and not _coerce_bool_value(job.get("fixed", False))
                ),
                "job_slot_count": job_slot_count,
                "current_employee_code": str(job.get("current_employee_code", "")).strip(),
                "eligible_employee_codes": eligible_employee_codes if eligible_employee_codes is not None else pd.NA,
                "boundary_overflow_employee_codes": boundary_overflow_employee_codes,
                "region_policy": region_policy,
                "region_seq": int(region_seq) if pd.notna(region_seq) else pd.NA,
                "new_region_name": region_name,
                "area_type": str(job.get("area_type", "")).strip().upper(),
                # LA_6area keeps Area Type Clusters behavior: DMS is preferred
                # but permits DMS2 capacity fallback; DMS2 remains DMS2-only.
                "enforce_area_type_center_match": (
                    str(request_payload.get("city", "")).strip() == LA_6AREA_CITY
                    or region_policy in {
                        ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1,
                        ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
                    }
                ),
                "area_type_dms_fallback_allowed": (
                    (
                        str(request_payload.get("city", "")).strip() == LA_6AREA_CITY
                        and region_policy != ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1
                    )
                    or region_policy == ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1
                ),
                "SVC_CENTER_TYPE": center_type,
                "is_tv_job": bool(job.get("is_tv_job", False)),
                "is_heavy_repair": bool(job.get("is_heavy_repair", False)),
                "service_time_min": int(service_minutes),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        region_centers = _build_region_centers_from_service_df(df)
        missing_region_mask = df["region_seq"].isna() | (df["new_region_name"].astype(str).str.strip() == "")
        if missing_region_mask.any() and not _uses_own_region_boundary_policy(region_policy):
            for idx in df.index[missing_region_mask]:
                row = df.loc[idx]
                if region_centers:
                    region_seq, region_name = _nearest_region(float(row["longitude"]), float(row["latitude"]), region_centers)
                else:
                    region_seq, region_name = 1, "Atlanta New Region 1"
                df.at[idx, "region_seq"] = int(region_seq)
                df.at[idx, "new_region_name"] = region_name
        df = df.sort_values(["service_date_key", "GSFS_RECEIPT_NO"]).reset_index(drop=True)
        payload_service_times = (
            df.set_index("GSFS_RECEIPT_NO")["service_time_min"].to_dict()
            if "service_time_min" in df.columns
            else {}
        )
        heavy_lookup_df = prod._build_heavy_repair_lookup(prod.DEFAULT_SYMPTOM_FILE)
        df = prod._enrich_service_df(df, heavy_lookup_df)
        if payload_service_times:
            df["service_time_min"] = df["GSFS_RECEIPT_NO"].map(payload_service_times).fillna(df["service_time_min"])
            df["service_time_min"] = pd.to_numeric(df["service_time_min"], errors="coerce").fillna(45)
            df["is_heavy_repair"] = df["service_time_min"].astype(float).gt(45)
    return df


def _build_response_payload(
    request_payload: dict[str, Any],
    summary_df: pd.DataFrame,
    schedule_df: pd.DataFrame,
    diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    planning_date = str(request_payload.get("planning_date", "")).strip()
    timezone_offset = str(request_payload.get("options", {}).get("timezone_offset", DEFAULT_TIMEZONE_OFFSET)).strip() or DEFAULT_TIMEZONE_OFFSET
    jobs = list(request_payload.get("jobs", []))
    job_lookup = {
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(): job
        for job in jobs
    }
    technician_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in request_payload.get("technicians", [])
        if str(tech.get("employee_code", "")).strip()
    }
    region_policy = resolve_city_routing_policy(request_payload)
    diagnostics_payload = dict(diagnostics or {})
    respect_fixed_jobs = _coerce_bool_value(
        (request_payload.get("options") or {}).get("respect_fixed_jobs", True)
    )
    invalid_location_receipts = {
        str(value).strip()
        for value in diagnostics_payload.get("invalid_location_receipts", [])
        if str(value).strip()
    }
    fixed_outside_active_plan_relaxed_receipts = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
        if respect_fixed_jobs
        and _coerce_bool_value(job.get("fixed", False))
        and _fixed_technician_outside_active_plan(job, technician_codes, region_policy)
        and bool(str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip())
        and str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        not in invalid_location_receipts
    ]
    assigned_receipts: set[str] = set()
    assignments: list[dict[str, Any]] = []
    if not schedule_df.empty:
        for _, row in schedule_df.iterrows():
            receipt_no = str(row.get("GSFS_RECEIPT_NO", "")).strip()
            payload_job = job_lookup.get(receipt_no, {})
            assigned_receipts.add(receipt_no)
            current_employee_code = str(payload_job.get("current_employee_code", "")).strip()
            numeric_slot = pd.to_numeric(
                pd.Series([row.get("job_slot_count", payload_job.get("job_slot_count", 1))]),
                errors="coerce",
            ).iloc[0]
            service_time_min = pd.to_numeric(
                pd.Series([row.get("service_time_min", payload_job.get("service_minutes", 45))]),
                errors="coerce",
            ).fillna(45).iloc[0]
            base_service_time_min = pd.to_numeric(
                pd.Series([row.get("base_service_time_min", payload_job.get("service_minutes", service_time_min))]),
                errors="coerce",
            ).fillna(service_time_min).iloc[0]
            service_time_multiplier = pd.to_numeric(
                pd.Series([row.get("service_time_multiplier", 1.0)]),
                errors="coerce",
            ).fillna(1.0).iloc[0]
            assignments.append(
                {
                    "salesforce_id": str(payload_job.get("salesforce_id", row.get("salesforce_id", ""))).strip(),
                    "receipt_no": receipt_no,
                    "employee_code": str(row.get("assigned_sm_code", "")).strip(),
                    "sequence": int(pd.to_numeric(pd.Series([row.get("visit_seq", 0)]), errors="coerce").fillna(0).iloc[0]),
                    "planned_start": format_planned_timestamp(str(row.get("service_date_key", planning_date)), str(row.get("visit_start_time", "")), timezone_offset),
                    "planned_end": format_planned_timestamp(str(row.get("service_date_key", planning_date)), str(row.get("visit_end_time", "")), timezone_offset),
                    "changed": bool(current_employee_code and current_employee_code != str(row.get("assigned_sm_code", "")).strip()),
                    "fixed_technician_outside_active_plan_relaxed": _coerce_bool_value(
                        row.get("fixed_technician_outside_active_plan_relaxed", False)
                    ),
                    "job_slot_count": max(1, int(numeric_slot)) if pd.notna(numeric_slot) else 1,
                    "base_service_time_min": round(float(base_service_time_min), 2),
                    "service_time_multiplier": round(float(service_time_multiplier), 3),
                    "service_time_min": round(float(service_time_min), 2),
                    "priority_minimums_relaxed": _coerce_bool_value(row.get("priority_minimums_relaxed", False)),
                    "fixed_capacity_forced": _coerce_bool_value(row.get("fixed_capacity_forced", False)),
                    "reschedule_mandatory_relaxed": _coerce_bool_value(row.get("reschedule_mandatory_relaxed", False)),
                    "distance_caps_relaxed": _coerce_bool_value(row.get("distance_caps_relaxed", False)),
                }
            )

    diagnostics_payload["fixed_technician_outside_active_plan_relaxed_job_count"] = len(
        fixed_outside_active_plan_relaxed_receipts
    )
    diagnostics_payload["fixed_technician_outside_active_plan_relaxed_job_sample"] = (
        fixed_outside_active_plan_relaxed_receipts[:20]
    )
    relaxations_applied = {
        "fixed_capacity_override": bool(diagnostics_payload.get("fixed_capacity_violations")),
        "fixed_technician_outside_active_plan_relaxed": bool(
            diagnostics_payload.get("fixed_technician_outside_active_plan_relaxed_job_count", 0)
        ),
        "priority_minimums_relaxed": any(bool(item.get("priority_minimums_relaxed")) for item in assignments),
        "fixed_capacity_forced": any(bool(item.get("fixed_capacity_forced")) for item in assignments),
        "reschedule_mandatory_relaxed": any(bool(item.get("reschedule_mandatory_relaxed")) for item in assignments),
        "distance_caps_relaxed": any(bool(item.get("distance_caps_relaxed")) for item in assignments),
    }
    messages: list[str] = []
    routing_engine = str(diagnostics_payload.get("routing_engine", "")).strip()
    if routing_engine:
        messages.append(f"Routing engine: {routing_engine}")
    fixed_violations = diagnostics_payload.get("fixed_capacity_violations") or {}
    if isinstance(fixed_violations, dict) and fixed_violations:
        labels = []
        for employee_code, info in fixed_violations.items():
            if not isinstance(info, dict):
                continue
            name = str(info.get("employee_name", "")).strip()
            label = f"{name} ({employee_code})" if name else str(employee_code)
            labels.append(
                f"{label}: fixed slots {info.get('fixed_slots', 0)} > slot_count {info.get('slot_capacity', 0)}"
            )
        messages.append("Fixed capacity override: " + "; ".join(labels))
    if relaxations_applied["fixed_technician_outside_active_plan_relaxed"]:
        count = int(diagnostics_payload["fixed_technician_outside_active_plan_relaxed_job_count"])
        messages.append(
            "Fixed technician outside active plan relaxed: "
            f"{count} fixed job(s) had their fixed-technician constraint released and were "
            "evaluated with reschedule-like priority; existing eligible candidates remain hard-limited."
        )
    if relaxations_applied["priority_minimums_relaxed"]:
        messages.append("Priority minimums relaxed: technician minimum slot targets were softened to find a feasible route.")
    if relaxations_applied["fixed_capacity_forced"]:
        messages.append("Fixed capacity forced: fixed calls were assigned to their fixed technicians even when fixed slots exceeded slot_count or full routing was infeasible.")
    if relaxations_applied["reschedule_mandatory_relaxed"]:
        messages.append("Reschedule mandatory relaxed: some reschedule calls were allowed to be unassigned if needed for feasibility.")
    if relaxations_applied["distance_caps_relaxed"]:
        messages.append("Distance caps relaxed: max home-to-job, single-leg, or daily travel caps were relaxed to avoid full routing failure.")
    invalid_location_count = int(diagnostics_payload.get("invalid_location_count", 0) or 0)
    if invalid_location_count:
        sample = ", ".join(str(value) for value in diagnostics_payload.get("invalid_location_receipts", [])[:10])
        messages.append(
            f"Invalid location skipped: {invalid_location_count} job(s) had missing/invalid coordinates"
            + (f" ({sample})" if sample else "")
        )
    if not messages:
        messages.append("Standard routing: no feasibility relaxation was applied.")
    diagnostics_payload["relaxations_applied"] = relaxations_applied
    diagnostics_payload["routing_condition_messages"] = messages

    unassigned: list[dict[str, Any]] = []
    for job in jobs:
        receipt_no = str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        if receipt_no in assigned_receipts:
            continue
        eligible_codes = job.get("eligible_employee_codes")
        fixed = _coerce_bool_value(job.get("fixed", False))
        reschedule = _coerce_bool_value(job.get("reschedule", False)) and not fixed
        current_employee_code = str(job.get("current_employee_code", "")).strip()
        fixed_outside_active_plan_relaxed = receipt_no in fixed_outside_active_plan_relaxed_receipts
        if receipt_no in invalid_location_receipts:
            reason = "INVALID_LOCATION"
        elif fixed and (not current_employee_code or current_employee_code not in technician_codes):
            reason = "FIXED_TECHNICIAN_NOT_AVAILABLE"
        elif not _postal_is_in_active_region_plan(job, region_policy):
            reason = "POSTAL_NOT_IN_ACTIVE_PLAN"
        elif isinstance(eligible_codes, list) and len(eligible_codes) == 0:
            reason = "NO_ELIGIBLE_TECHNICIAN"
        elif (fixed and respect_fixed_jobs) or reschedule:
            # Preserve the existing mandatory-route reason for both hard fixed
            # work and fixed-outside-plan work that uses reschedule-like drop
            # priority.  Empty hard eligibility has already returned the more
            # specific NO_ELIGIBLE_TECHNICIAN reason above.
            reason = "NO_FEASIBLE_MANDATORY_ROUTE"
        else:
            reason = "NO_FEASIBLE_ROUTE"
        unassigned.append(
            {
                "salesforce_id": str(job.get("salesforce_id", "")).strip(),
                "receipt_no": receipt_no,
                "reason": reason,
                "product_group": str(job.get("product_group", "")).strip(),
                "product": str(job.get("product", "")).strip(),
                "eligible_employee_count": len(eligible_codes) if isinstance(eligible_codes, list) else None,
                "fixed": fixed,
                "reschedule": reschedule,
                "current_employee_code": current_employee_code,
                "fixed_technician_outside_active_plan_relaxed": fixed_outside_active_plan_relaxed,
            }
        )

    return {
        "request_id": str(request_payload.get("request_id", "")).strip(),
        "routing_job_id": str(request_payload.get("routing_job_id", "")).strip(),
        "mode": normalize_mode(request_payload.get("mode")),
        "city": normalize_city(request_payload.get("city")),
        "status": "completed",
        "summary": {
            "total_jobs": len(jobs),
            "assigned_jobs": len(assignments),
            "unassigned_jobs": len(unassigned),
        },
        "assignments": assignments,
        "unassigned": unassigned,
        "engineer_summary": summary_df.to_dict("records") if not summary_df.empty else [],
        "diagnostics": diagnostics_payload,
    }


def _build_routing_diagnostics(
    request_payload: dict[str, Any],
    service_df: pd.DataFrame,
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
) -> dict[str, Any]:
    jobs = list(request_payload.get("jobs", []))
    technicians = list(request_payload.get("technicians", []))
    payload_area_type_counts: dict[str, int] = {}
    for job in jobs:
        area_type = str(job.get("area_type", "") or "").strip().upper() or "BLANK"
        payload_area_type_counts[area_type] = payload_area_type_counts.get(area_type, 0) + 1
    service_area_type_counts: dict[str, int] = {}
    if not service_df.empty and "area_type" in service_df.columns:
        for area_type in service_df["area_type"].fillna("").astype(str).str.strip().str.upper().tolist():
            key = area_type or "BLANK"
            service_area_type_counts[key] = service_area_type_counts.get(key, 0) + 1
    no_eligible = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
        if isinstance(job.get("eligible_employee_codes"), list) and len(job.get("eligible_employee_codes", [])) == 0
    ]
    fixed_jobs = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
        if _coerce_bool_value(job.get("fixed", False))
    ]
    reschedule_jobs = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
        if _coerce_bool_value(job.get("reschedule", False)) and not _coerce_bool_value(job.get("fixed", False))
    ]
    technician_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
    }
    unavailable_fixed = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
        if _coerce_bool_value(job.get("fixed", False))
        and (
            not str(job.get("current_employee_code", "")).strip()
            or str(job.get("current_employee_code", "")).strip() not in technician_codes
        )
    ]
    fixed_outside_active_plan = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
        if _coerce_bool_value(job.get("fixed", False))
        and _fixed_technician_outside_active_plan(
            job,
            technician_codes,
            resolve_city_routing_policy(request_payload),
        )
    ]
    postal_not_in_active_plan = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
        if not _postal_is_in_active_region_plan(
            job,
            resolve_city_routing_policy(request_payload),
        )
    ]
    technician_slot_capacity: dict[str, int] = {}
    technician_name_lookup: dict[str, str] = {}
    for tech in technicians:
        employee_code = str(tech.get("employee_code", "")).strip()
        if not employee_code:
            continue
        technician_name_lookup[employee_code] = str(tech.get("employee_name", employee_code)).strip() or employee_code
        slot_value = pd.to_numeric(pd.Series([tech.get("slot_count", tech.get("max_slots", 8))]), errors="coerce").fillna(8).iloc[0]
        technician_slot_capacity[employee_code] = max(0, int(slot_value))

    fixed_slots_by_employee: dict[str, int] = {}
    fixed_jobs_by_employee: dict[str, int] = {}
    reschedule_slot_count = 0
    total_slots = 0
    for job in jobs:
        slot_value = pd.to_numeric(pd.Series([job.get("job_slot_count", 1)]), errors="coerce").fillna(1).iloc[0]
        job_slots = max(1, int(slot_value))
        total_slots += job_slots
        if _coerce_bool_value(job.get("fixed", False)):
            employee_code = str(job.get("current_employee_code", "")).strip()
            fixed_slots_by_employee[employee_code] = fixed_slots_by_employee.get(employee_code, 0) + job_slots
            fixed_jobs_by_employee[employee_code] = fixed_jobs_by_employee.get(employee_code, 0) + 1
        elif _coerce_bool_value(job.get("reschedule", False)):
            reschedule_slot_count += job_slots
    total_capacity = sum(technician_slot_capacity.values())
    service_receipts = set()
    if not service_df.empty and "GSFS_RECEIPT_NO" in service_df.columns:
        service_receipts = {
            str(value).strip()
            for value in service_df["GSFS_RECEIPT_NO"].dropna().astype(str).tolist()
            if str(value).strip()
        }
    all_receipts = [
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in jobs
    ]
    invalid_location_receipts = [
        receipt
        for receipt in all_receipts
        if receipt and receipt not in service_receipts
    ]
    respect_fixed_jobs = _coerce_bool_value(
        (request_payload.get("options") or {}).get("respect_fixed_jobs", True)
    )
    fixed_outside_active_plan_relaxed = [
        receipt
        for receipt in fixed_outside_active_plan
        if respect_fixed_jobs and receipt in service_receipts
    ]
    fixed_capacity_violations = {
        employee_code: {
            "employee_name": technician_name_lookup.get(employee_code, employee_code),
            "fixed_jobs": fixed_jobs_by_employee.get(employee_code, 0),
            "fixed_slots": fixed_slots,
            "slot_capacity": technician_slot_capacity.get(employee_code, 0),
        }
        for employee_code, fixed_slots in sorted(fixed_slots_by_employee.items())
        if employee_code and fixed_slots > technician_slot_capacity.get(employee_code, 0)
    }
    return {
        "job_count": len(jobs),
        "service_frame_count": int(len(service_df)),
        "payload_area_type_counts": payload_area_type_counts,
        "service_area_type_counts": service_area_type_counts,
        "technician_count": len(technicians),
        "engineer_frame_count": int(len(engineer_region_df)),
        "home_frame_count": int(len(home_df)),
        "capability_count": len(list(request_payload.get("capabilities", []))),
        "jobs_without_eligible_technician_count": len(no_eligible),
        "jobs_without_eligible_technician_sample": no_eligible[:20],
        "fixed_job_count": len(fixed_jobs),
        "reschedule_job_count": len(reschedule_jobs),
        "mandatory_job_count": len(fixed_jobs) + len(reschedule_jobs),
        "unavailable_fixed_job_count": len(unavailable_fixed),
        "unavailable_fixed_job_sample": unavailable_fixed[:20],
        "fixed_outside_active_plan_job_count": len(fixed_outside_active_plan),
        "fixed_outside_active_plan_job_sample": fixed_outside_active_plan[:20],
        "fixed_technician_outside_active_plan_relaxed_job_count": len(fixed_outside_active_plan_relaxed),
        "fixed_technician_outside_active_plan_relaxed_job_sample": fixed_outside_active_plan_relaxed[:20],
        "postal_not_in_active_plan_job_count": len(postal_not_in_active_plan),
        "postal_not_in_active_plan_job_sample": postal_not_in_active_plan[:20],
        "total_job_slots": int(total_slots),
        "total_technician_slots": int(total_capacity),
        "reschedule_slot_count": int(reschedule_slot_count),
        "technician_slot_capacity": technician_slot_capacity,
        "technician_names": technician_name_lookup,
        "fixed_jobs_by_employee": fixed_jobs_by_employee,
        "fixed_slots_by_employee": fixed_slots_by_employee,
        "fixed_capacity_violation_employee_codes": list(fixed_capacity_violations.keys()),
        "fixed_capacity_violations": fixed_capacity_violations,
        "invalid_location_count": len(invalid_location_receipts),
        "invalid_location_receipts": invalid_location_receipts[:50],
    }


def run_mode(request_payload: dict[str, Any]) -> dict[str, Any]:
    from smart_routing.production_assign_atlanta_vrp import (
        VRP_APPROVED_BOUNDARY_OVERFLOW_PENALTY_COST,
        VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST,
        build_atlanta_production_assignment_vrp_from_frames,
    )

    region_zip_df, reference_engineer_region_df, reference_home_df = _load_reference_inputs(request_payload)
    region_lookup = _build_region_lookup(region_zip_df)
    service_df = _build_service_frame_from_payload(request_payload, region_lookup)
    region_centers = _build_region_centers_from_service_df(service_df)
    engineer_region_df, home_df = _build_engineer_frames_from_payload(
        request_payload,
        reference_engineer_region_df,
        reference_home_df,
        region_centers,
    )
    diagnostics = _build_routing_diagnostics(request_payload, service_df, engineer_region_df, home_df)
    diagnostics["routing_engine"] = "na_general"
    city_policy = resolve_city_routing_policy(request_payload)
    diagnostics["city_policy"] = city_policy
    diagnostics["distance_backend"] = str((request_payload.get("options") or {}).get("distance_backend", ""))
    diagnostics["preferred_region_mismatch_penalty_cost"] = (
        int(VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST)
        if city_policy in SOFT_REGION_AFFINITY_POLICIES
        else 0
    )
    diagnostics["approved_boundary_overflow_penalty_cost"] = (
        int(VRP_APPROVED_BOUNDARY_OVERFLOW_PENALTY_COST)
        if (
            _uses_own_region_boundary_policy(city_policy)
            and any(
                _approved_boundary_overflow_employee_codes(job)
                for job in request_payload.get("jobs", [])
                if isinstance(job, dict)
            )
        )
        else 0
    )
    diagnostics["region_split_overflow_mode"] = (
        "approved_boundary_only"
        if diagnostics["approved_boundary_overflow_penalty_cost"]
        else "disabled"
        if _uses_own_region_boundary_policy(city_policy)
        else "not_applicable"
    )
    diagnostics["solver_seed"] = "ortools_default"
    if service_df.empty:
        return _build_response_payload(request_payload, pd.DataFrame(), pd.DataFrame(), diagnostics=diagnostics)
    if engineer_region_df.empty or home_df.empty:
        return build_empty_result(request_payload, reason="INVALID_INPUT_DATA", mode="na_general")

    time_limit_seconds = int(pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("time_limit_seconds", 20)]),
        errors="coerce",
    ).fillna(20).clip(lower=10).iloc[0])
    respect_fixed_jobs = _coerce_bool_value(
        request_payload.get("options", {}).get("respect_fixed_jobs", True)
    )
    avoid_polygons = list(request_payload.get("options", {}).get("avoid_polygons", []) or [])
    avoid_penalty_multiplier = float(
        pd.to_numeric(
            pd.Series([request_payload.get("options", {}).get("avoid_penalty_multiplier", 4.0)]),
            errors="coerce",
        ).fillna(4.0).iloc[0]
    )
    max_travel_min_per_sm_day = pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("max_travel_min_per_sm_day")]),
        errors="coerce",
    ).iloc[0]
    max_work_min_per_sm_day = pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("max_work_min_per_sm_day")]),
        errors="coerce",
    ).iloc[0]
    max_travel_km_per_sm_day = pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("max_travel_km_per_sm_day")]),
        errors="coerce",
    ).iloc[0]
    max_single_leg_min = pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("max_single_leg_min")]),
        errors="coerce",
    ).iloc[0]
    max_home_to_job_min = pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("max_home_to_job_min")]),
        errors="coerce",
    ).iloc[0]
    long_leg_penalty_start_min = pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("long_leg_penalty_start_min")]),
        errors="coerce",
    ).iloc[0]
    long_leg_penalty_multiplier = pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("long_leg_penalty_multiplier")]),
        errors="coerce",
    ).iloc[0]
    route_client = _build_city_route_client(request_payload)
    diagnostics["osrm_url"] = route_client.cfg.osrm_url
    diagnostics["osrm_profile"] = route_client.cfg.osrm_profile
    diagnostics["matrix_fallback"] = (
        "fail_closed_on_osrm_error"
        if route_client.cfg.mode == "osrm" and route_client.cfg.fail_closed_on_osrm_error
        else "haversine_on_osrm_error"
        if route_client.cfg.mode == "osrm"
        else "haversine"
    )
    assignment_df, summary_df, schedule_df = build_atlanta_production_assignment_vrp_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=False,
        time_limit_seconds=time_limit_seconds,
        respect_fixed_jobs=respect_fixed_jobs,
        avoid_polygons=avoid_polygons,
        avoid_penalty_multiplier=avoid_penalty_multiplier,
        max_work_min_per_sm_day=float(max_work_min_per_sm_day) if pd.notna(max_work_min_per_sm_day) and float(max_work_min_per_sm_day) > 0 else None,
        max_travel_min_per_sm_day=float(max_travel_min_per_sm_day) if pd.notna(max_travel_min_per_sm_day) and float(max_travel_min_per_sm_day) > 0 else None,
        max_travel_km_per_sm_day=float(max_travel_km_per_sm_day) if pd.notna(max_travel_km_per_sm_day) and float(max_travel_km_per_sm_day) > 0 else None,
        max_single_leg_min=float(max_single_leg_min) if pd.notna(max_single_leg_min) and float(max_single_leg_min) > 0 else None,
        max_home_to_job_min=float(max_home_to_job_min) if pd.notna(max_home_to_job_min) and float(max_home_to_job_min) > 0 else None,
        long_leg_penalty_start_min=float(long_leg_penalty_start_min) if pd.notna(long_leg_penalty_start_min) and float(long_leg_penalty_start_min) > 0 else None,
        long_leg_penalty_multiplier=float(long_leg_penalty_multiplier) if pd.notna(long_leg_penalty_multiplier) and float(long_leg_penalty_multiplier) > 0 else None,
        route_client=route_client,
    )
    diagnostics["matrix_telemetry"] = route_client.get_matrix_telemetry()
    diagnostics["assignment_frame_count"] = int(len(assignment_df)) if assignment_df is not None else 0
    diagnostics["summary_frame_count"] = int(len(summary_df)) if summary_df is not None else 0
    diagnostics["schedule_frame_count"] = int(len(schedule_df)) if schedule_df is not None else 0
    if schedule_df.empty and not assignment_df.empty:
        schedule_df = assignment_df.copy()
        if "visit_seq" not in schedule_df.columns:
            schedule_df["visit_seq"] = schedule_df.get("vrp_visit_seq", 0)
        schedule_df["visit_seq"] = pd.to_numeric(schedule_df["visit_seq"], errors="coerce").fillna(0).astype(int)
        schedule_df["visit_start_time"] = schedule_df.get("visit_start_time", "")
        schedule_df["visit_end_time"] = schedule_df.get("visit_end_time", "")
    return _build_response_payload(request_payload, summary_df, schedule_df, diagnostics=diagnostics)
