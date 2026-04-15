from __future__ import annotations

from typing import Any

import pandas as pd

from .vrp_api_common import (
    DEFAULT_TIMEZONE_OFFSET,
    build_empty_result,
    format_planned_timestamp,
    normalize_city,
    normalize_mode,
)


def _load_reference_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    import smart_routing.production_assign_atlanta as base

    region_zip_df = pd.read_csv(base.REGION_ZIP_PATH, encoding="utf-8-sig")
    engineer_region_df = pd.read_csv(base.ENGINEER_REGION_PATH, encoding="utf-8-sig")
    home_df = pd.read_csv(base.HOME_GEOCODE_PATH, encoding="utf-8-sig")
    region_zip_df["POSTAL_CODE"] = region_zip_df["POSTAL_CODE"].astype(str).str.zfill(5)
    return region_zip_df, engineer_region_df, home_df


def _build_region_lookup(region_zip_df: pd.DataFrame) -> dict[str, tuple[int, str]]:
    lookup_df = region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].dropna(subset=["POSTAL_CODE"]).drop_duplicates()
    lookup_df["POSTAL_CODE"] = lookup_df["POSTAL_CODE"].astype(str).str.zfill(5)
    return {
        str(row["POSTAL_CODE"]).zfill(5): (int(row["region_seq"]), str(row["new_region_name"]))
        for _, row in lookup_df.iterrows()
        if pd.notna(row["region_seq"])
    }


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
        home_row["latitude"] = float(start_lat) if pd.notna(start_lat) else pd.NA
        home_row["longitude"] = float(start_lng) if pd.notna(start_lng) else pd.NA
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
        if pd.notna(requested_region_seq):
            region_seq = int(requested_region_seq)
            region_name = str(job.get("region_name", "")).strip() or f"Region {region_seq}"
        elif postal_code and postal_code in region_lookup:
            region_seq, region_name = region_lookup[postal_code]
        else:
            region_seq, region_name = pd.NA, ""
        time_window = job.get("time_window") or []
        center_type = str(job.get("current_center_type", "")).strip().upper() or "DMS"
        if not prod.ENABLE_DMS2 and center_type == prod.DMS2_CENTER_TYPE:
            center_type = prod.DMS_CENTER_TYPE
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
                "fixed": bool(job.get("fixed", False)),
                "current_employee_code": str(job.get("current_employee_code", "")).strip(),
                "region_seq": int(region_seq) if pd.notna(region_seq) else pd.NA,
                "new_region_name": region_name,
                "SVC_CENTER_TYPE": center_type,
                "is_tv_job": bool(job.get("is_tv_job", False)),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        region_centers = _build_region_centers_from_service_df(df)
        missing_region_mask = df["region_seq"].isna() | (df["new_region_name"].astype(str).str.strip() == "")
        if missing_region_mask.any():
            for idx in df.index[missing_region_mask]:
                row = df.loc[idx]
                if region_centers:
                    region_seq, region_name = _nearest_region(float(row["longitude"]), float(row["latitude"]), region_centers)
                else:
                    region_seq, region_name = 1, "Atlanta New Region 1"
                df.at[idx, "region_seq"] = int(region_seq)
                df.at[idx, "new_region_name"] = region_name
        df = df.sort_values(["service_date_key", "GSFS_RECEIPT_NO"]).reset_index(drop=True)
        heavy_lookup_df = prod._build_heavy_repair_lookup(prod.DEFAULT_SYMPTOM_FILE)
        df = prod._enrich_service_df(df, heavy_lookup_df)
    return df


def _build_response_payload(
    request_payload: dict[str, Any],
    summary_df: pd.DataFrame,
    schedule_df: pd.DataFrame,
) -> dict[str, Any]:
    planning_date = str(request_payload.get("planning_date", "")).strip()
    timezone_offset = str(request_payload.get("options", {}).get("timezone_offset", DEFAULT_TIMEZONE_OFFSET)).strip() or DEFAULT_TIMEZONE_OFFSET
    jobs = list(request_payload.get("jobs", []))
    job_lookup = {
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(): job
        for job in jobs
    }
    assigned_receipts: set[str] = set()
    assignments: list[dict[str, Any]] = []
    if not schedule_df.empty:
        for _, row in schedule_df.iterrows():
            receipt_no = str(row.get("GSFS_RECEIPT_NO", "")).strip()
            payload_job = job_lookup.get(receipt_no, {})
            assigned_receipts.add(receipt_no)
            current_employee_code = str(payload_job.get("current_employee_code", "")).strip()
            assignments.append(
                {
                    "salesforce_id": str(payload_job.get("salesforce_id", row.get("salesforce_id", ""))).strip(),
                    "receipt_no": receipt_no,
                    "employee_code": str(row.get("assigned_sm_code", "")).strip(),
                    "sequence": int(pd.to_numeric(pd.Series([row.get("visit_seq", 0)]), errors="coerce").fillna(0).iloc[0]),
                    "planned_start": format_planned_timestamp(str(row.get("service_date_key", planning_date)), str(row.get("visit_start_time", "")), timezone_offset),
                    "planned_end": format_planned_timestamp(str(row.get("service_date_key", planning_date)), str(row.get("visit_end_time", "")), timezone_offset),
                    "changed": bool(current_employee_code and current_employee_code != str(row.get("assigned_sm_code", "")).strip()),
                }
            )

    unassigned: list[dict[str, Any]] = []
    for job in jobs:
        receipt_no = str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        if receipt_no in assigned_receipts:
            continue
        unassigned.append(
            {
                "salesforce_id": str(job.get("salesforce_id", "")).strip(),
                "receipt_no": receipt_no,
                "reason": "NO_FEASIBLE_ROUTE",
            }
        )

    return {
        "request_id": str(request_payload.get("request_id", "")).strip(),
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
    }


def run_mode(request_payload: dict[str, Any]) -> dict[str, Any]:
    from smart_routing.production_assign_atlanta_vrp import build_atlanta_production_assignment_vrp_from_frames

    region_zip_df, reference_engineer_region_df, reference_home_df = _load_reference_inputs()
    region_lookup = _build_region_lookup(region_zip_df)
    service_df = _build_service_frame_from_payload(request_payload, region_lookup)
    region_centers = _build_region_centers_from_service_df(service_df)
    engineer_region_df, home_df = _build_engineer_frames_from_payload(
        request_payload,
        reference_engineer_region_df,
        reference_home_df,
        region_centers,
    )
    if engineer_region_df.empty or home_df.empty or service_df.empty:
        return build_empty_result(request_payload, reason="INVALID_INPUT_DATA", mode="na_general")

    time_limit_seconds = int(pd.to_numeric(
        pd.Series([request_payload.get("options", {}).get("time_limit_seconds", 20)]),
        errors="coerce",
    ).fillna(20).clip(lower=10).iloc[0])
    _, summary_df, schedule_df = build_atlanta_production_assignment_vrp_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=True,
        time_limit_seconds=time_limit_seconds,
    )
    return _build_response_payload(request_payload, summary_df, schedule_df)
