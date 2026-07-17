from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from smart_routing.common_vrp_runtime import _apply_job_capabilities  # noqa: E402
from smart_routing.vrp_api_service import run_routing_request  # noqa: E402


BASE_DIR = Path("260310/la bucket test")
REPORT_DIR = BASE_DIR / "routing_report"
SCENARIOS = {
    "area_type_clusters": "Los Angeles, CA - Area Type Clusters",
    "bucket_sim_draft": "Los Angeles, CA - Bucket Sim Draft",
}
SCENARIO_LABELS = {
    "actual": "Actual",
    "area_type_clusters": "Area Type Cluster",
    "bucket_sim_draft": "Bucket Sim Draft",
}
PROFILE_FILE = Path("260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx")


def _clean(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _truthy(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "y", "yes", "t"}


def _norm_zip(value: object) -> str:
    text = _clean(value).replace(".0", "")
    return text.zfill(5) if text else ""


def _haversine_miles(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, a)
    lon2, lat2 = map(math.radians, b)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 3958.7613 * 2 * math.asin(math.sqrt(h))


def _nearest_neighbor_order(
    start: tuple[float, float] | None,
    coords: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    remaining = list(coords)
    if not remaining:
        return []
    if start is None:
        start = remaining.pop(0)
        ordered = [start]
    else:
        ordered = []
    current = start
    while remaining:
        best_idx = min(range(len(remaining)), key=lambda idx: _haversine_miles(current, remaining[idx]))
        current = remaining.pop(best_idx)
        ordered.append(current)
    return ordered


def _route_miles(
    home: tuple[float, float] | None,
    coords: list[tuple[float, float]],
    *,
    ordered: bool,
) -> float:
    if not coords:
        return 0.0
    route = coords if ordered else _nearest_neighbor_order(home, coords)
    total = 0.0
    current = home
    for coord in route:
        if current is not None:
            total += _haversine_miles(current, coord)
        current = coord
    return total


def _is_valid_la_coord(lon: object, lat: object) -> bool:
    lon_num = pd.to_numeric(pd.Series([lon]), errors="coerce").iloc[0]
    lat_num = pd.to_numeric(pd.Series([lat]), errors="coerce").iloc[0]
    if pd.isna(lon_num) or pd.isna(lat_num):
        return False
    return 32.0 <= float(lat_num) <= 35.5 and -120.0 <= float(lon_num) <= -116.0


def _load_capability_df() -> pd.DataFrame:
    df = pd.read_excel(PROFILE_FILE, sheet_name="3. Product")
    for col in ["STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE", "REPAIR_FLAG", "AREA_PRODUCT_FLAG"]:
        if col in df.columns:
            df[col] = df[col].map(_clean)
    return df


def _build_capability_rows(city_name: str, tech_df: pd.DataFrame, jobs_df: pd.DataFrame, capability_df: pd.DataFrame) -> list[dict[str, Any]]:
    employee_codes = {
        _clean(code)
        for code in tech_df.get("employee_code", pd.Series(dtype=str)).tolist()
        if _clean(code)
    }
    requested_products = {
        (_clean(row.get("SERVICE_PRODUCT_GROUP_CODE")).upper(), _clean(row.get("SERVICE_PRODUCT_CODE")).upper())
        for _, row in jobs_df.iterrows()
        if _clean(row.get("SERVICE_PRODUCT_GROUP_CODE")) and _clean(row.get("SERVICE_PRODUCT_CODE"))
    }
    if not employee_codes or not requested_products or capability_df.empty:
        return []
    profile_city = "Los Angeles, CA" if str(city_name).startswith("Los Angeles, CA - ") else city_name
    filtered = capability_df[
        capability_df["STRATEGIC_CITY_NAME"].astype(str).eq(profile_city)
        & capability_df["SVC_ENGINEER_CODE"].astype(str).isin(employee_codes)
        & capability_df["REPAIR_FLAG"].astype(str).str.upper().eq("T")
    ].copy()
    if filtered.empty:
        return []
    filtered["SERVICE_PRODUCT_GROUP_CODE"] = filtered["SERVICE_PRODUCT_GROUP_CODE"].astype(str).str.upper()
    filtered["SERVICE_PRODUCT_CODE"] = filtered["SERVICE_PRODUCT_CODE"].astype(str).str.upper()
    filtered = filtered[
        filtered.apply(
            lambda row: (
                _clean(row["SERVICE_PRODUCT_GROUP_CODE"]).upper(),
                _clean(row["SERVICE_PRODUCT_CODE"]).upper(),
            )
            in requested_products,
            axis=1,
        )
    ].drop_duplicates(subset=["SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE"])
    rows = []
    for _, row in filtered.iterrows():
        rows.append(
            {
                "employee_code": _clean(row["SVC_ENGINEER_CODE"]),
                "product_group_code": _clean(row["SERVICE_PRODUCT_GROUP_CODE"]).upper(),
                "product_code": _clean(row["SERVICE_PRODUCT_CODE"]).upper(),
                "heavy_repair_allowed": not (
                    _clean(row["SERVICE_PRODUCT_GROUP_CODE"]).upper() == "REF"
                    and _clean(row.get("AREA_PRODUCT_FLAG")).upper() == "N"
                ),
            }
        )
    return rows


def _eligible_codes(area_type: object, dms_codes: set[str], dms2_codes: set[str]) -> list[str]:
    text = _clean(area_type).upper()
    if text in {"DMS", "DMS_CORE", "DMS_ONLY", "OVERLAP", "OVERLAB"}:
        return sorted(dms_codes | dms2_codes)
    if text in {"DMS2", "DMS2_EXCLUSIVE", "DMS2_ONLY"}:
        return sorted(dms2_codes)
    return sorted(dms_codes | dms2_codes)


def _build_payload(scenario_key: str, date_key: str, jobs_df: pd.DataFrame, tech_df: pd.DataFrame, capability_rows: list[dict[str, Any]], time_limit_seconds: int) -> dict[str, Any]:
    city = SCENARIOS[scenario_key]
    dms_codes = set(tech_df.loc[tech_df["center_type"].astype(str).str.upper().eq("DMS"), "employee_code"].astype(str))
    dms2_codes = set(tech_df.loc[tech_df["center_type"].astype(str).str.upper().eq("DMS2"), "employee_code"].astype(str))

    tech_payload = []
    for _, row in tech_df.iterrows():
        code = _clean(row.get("employee_code"))
        if not code or not _truthy(row.get("available", True)):
            continue
        max_home = pd.to_numeric(pd.Series([row.get("max_home_to_job_min")]), errors="coerce").iloc[0]
        tech_payload.append(
            {
                "employee_code": code,
                "employee_name": _clean(row.get("employee_name")) or code,
                "center_type": _clean(row.get("center_type")).upper() or "DMS",
                "available": True,
                "shift_start": _clean(row.get("shift_start")) or "08:00",
                "shift_end": _clean(row.get("shift_end")) or "18:00",
                "slot_count": int(pd.to_numeric(pd.Series([row.get("slot_count", 8)]), errors="coerce").fillna(8).iloc[0]),
                "max_slots": int(pd.to_numeric(pd.Series([row.get("max_slots", row.get("slot_count", 8))]), errors="coerce").fillna(8).iloc[0]),
                "max_jobs": int(pd.to_numeric(pd.Series([row.get("max_jobs", row.get("slot_count", 8))]), errors="coerce").fillna(8).iloc[0]),
                "max_minutes": int(pd.to_numeric(pd.Series([row.get("max_minutes", 540)]), errors="coerce").fillna(540).iloc[0]),
                "max_home_to_job_min": float(max_home) if pd.notna(max_home) else None,
                "priority_group": _clean(row.get("priority_group")) or "B",
                "start_location": {"lat": float(row["home_latitude"]), "lng": float(row["home_longitude"])},
            }
        )

    job_payload = []
    for _, row in jobs_df.iterrows():
        area_type = _clean(row.get("area_type")).upper()
        slot = max(1, int(pd.to_numeric(pd.Series([row.get("job_slot_count", 1)]), errors="coerce").fillna(1).iloc[0]))
        job_payload.append(
            {
                "salesforce_id": _clean(row.get("GSFS_RECEIPT_NO")),
                "receipt_no": _clean(row.get("GSFS_RECEIPT_NO")),
                "product_group": _clean(row.get("SERVICE_PRODUCT_GROUP_CODE")).upper(),
                "product": _clean(row.get("SERVICE_PRODUCT_CODE")).upper(),
                "symptom": _clean(row.get("RECEIPT_DETAIL_SYMPTOM_CODE")).upper(),
                "address": _clean(row.get("ADDRESS_LINE1_INFO")),
                "city_name": _clean(row.get("CITY_NAME")),
                "state_name": _clean(row.get("STATE_NAME")),
                "country_name": _clean(row.get("COUNTRY_NAME")) or "USA",
                "postal_code": _norm_zip(row.get("POSTAL_CODE")),
                "location": {"lat": float(row["latitude"]), "lng": float(row["longitude"])},
                "priority": 0,
                "fixed": _truthy(row.get("fixed", False)),
                "reschedule": _truthy(row.get("reschedule", False)),
                "job_slot_count": slot,
                "current_employee_code": _clean(row.get("SVC_ENGINEER_CODE")),
                "current_employee_name": _clean(row.get("SVC_ENGINEER_NAME")),
                "current_center_type": _clean(row.get("SVC_CENTER_TYPE")).upper() or "DMS",
                "region_seq": int(pd.to_numeric(pd.Series([row.get("region_seq")]), errors="coerce").iloc[0]),
                "region_name": _clean(row.get("new_region_name")) or _clean(row.get("AREA_NAME")),
                "area_type": area_type,
                "eligible_employee_codes": _eligible_codes(area_type, dms_codes, dms2_codes),
            }
        )
    job_payload = _apply_job_capabilities(job_payload, capability_rows)
    return {
        "request_id": f"report_{scenario_key}_{date_key}",
        "mode": "na_general",
        "city": city,
        "planning_date": f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}",
        "options": {
            "respect_fixed_jobs": True,
            "objective": "min_total_travel_time",
            "time_limit_seconds": time_limit_seconds,
            "max_work_min_per_sm_day": 600,
            "max_travel_min_per_sm_day": 150,
            "max_travel_km_per_sm_day": 150,
            "max_single_leg_min": 40,
            "max_home_to_job_min": 60,
            "long_leg_penalty_start_min": 25,
            "long_leg_penalty_multiplier": 4,
        },
        "technicians": tech_payload,
        "jobs": job_payload,
        "capabilities": capability_rows,
    }


def _actual_assignment_rows(jobs_df: pd.DataFrame, tech_df: pd.DataFrame) -> pd.DataFrame:
    tech_lookup = tech_df.set_index("employee_code").to_dict("index") if not tech_df.empty else {}
    rows = []
    sequence_by_tech: dict[str, int] = {}
    for _, job in jobs_df.iterrows():
        code = _clean(job.get("SVC_ENGINEER_CODE"))
        center_type = _clean(job.get("SVC_CENTER_TYPE")).upper()
        if not code:
            continue
        if not center_type:
            center_type = "UNKNOWN"
        sequence_by_tech[code] = sequence_by_tech.get(code, 0) + 1
        tech = tech_lookup.get(code, {})
        rows.append(
            {
                "employee_code": code,
                "employee_name": _clean(job.get("SVC_ENGINEER_NAME")) or _clean(tech.get("employee_name")) or code,
                "center_type": center_type,
                "receipt_no": _clean(job.get("GSFS_RECEIPT_NO")),
                "lat": float(job["latitude"]),
                "lng": float(job["longitude"]),
                "valid_coord": _is_valid_la_coord(job.get("longitude"), job.get("latitude")),
                "home_lat": pd.to_numeric(pd.Series([tech.get("home_latitude")]), errors="coerce").iloc[0],
                "home_lng": pd.to_numeric(pd.Series([tech.get("home_longitude")]), errors="coerce").iloc[0],
                "sequence": sequence_by_tech[code],
            }
        )
    return pd.DataFrame(rows)


def _smart_assignment_rows(result: dict[str, Any], jobs_df: pd.DataFrame, tech_df: pd.DataFrame) -> pd.DataFrame:
    job_lookup = jobs_df.set_index(jobs_df["GSFS_RECEIPT_NO"].astype(str).str.strip()).to_dict("index")
    tech_lookup = tech_df.set_index("employee_code").to_dict("index") if not tech_df.empty else {}
    rows = []
    for item in result.get("assignments", []):
        receipt = _clean(item.get("receipt_no"))
        code = _clean(item.get("employee_code"))
        job = job_lookup.get(receipt, {})
        tech = tech_lookup.get(code, {})
        if not receipt or not code or not job or not tech:
            continue
        rows.append(
            {
                "employee_code": code,
                "employee_name": _clean(tech.get("employee_name")) or code,
                "center_type": _clean(tech.get("center_type")).upper() or "DMS",
                "receipt_no": receipt,
                "lat": float(job["latitude"]),
                "lng": float(job["longitude"]),
                "valid_coord": _is_valid_la_coord(job.get("longitude"), job.get("latitude")),
                "home_lat": pd.to_numeric(pd.Series([tech.get("home_latitude")]), errors="coerce").iloc[0],
                "home_lng": pd.to_numeric(pd.Series([tech.get("home_longitude")]), errors="coerce").iloc[0],
                "sequence": int(item.get("sequence", 0) or 0),
            }
        )
    return pd.DataFrame(rows)


def _summarize_assignments(date_key: str, scenario: str, assignment_df: pd.DataFrame, *, ordered: bool) -> list[dict[str, Any]]:
    if assignment_df.empty:
        return []
    rows = []
    for (center_type, employee_code), group in assignment_df.groupby(["center_type", "employee_code"], dropna=False):
        group = group.copy()
        home_lat = pd.to_numeric(pd.Series([group["home_lat"].iloc[0]]), errors="coerce").iloc[0]
        home_lng = pd.to_numeric(pd.Series([group["home_lng"].iloc[0]]), errors="coerce").iloc[0]
        home = (float(home_lng), float(home_lat)) if pd.notna(home_lat) and pd.notna(home_lng) else None
        if ordered and "sequence" in group.columns:
            group = group.sort_values("sequence")
            coords = [
                (float(row["lng"]), float(row["lat"]))
                for _, row in group.iterrows()
                if bool(row.get("valid_coord", True))
            ]
            miles = _route_miles(home, coords, ordered=True)
        else:
            coords = [
                (float(row["lng"]), float(row["lat"]))
                for _, row in group.iterrows()
                if bool(row.get("valid_coord", True))
            ]
            miles = _route_miles(home, coords, ordered=False)
        rows.append(
            {
                "date": date_key,
                "scenario": scenario,
                "center_type": str(center_type).upper(),
                "employee_code": employee_code,
                "employee_name": _clean(group["employee_name"].iloc[0]),
                "job_count": int(group["receipt_no"].nunique()),
                "distance_job_count": int(group.loc[group.get("valid_coord", True).astype(bool), "receipt_no"].nunique())
                if "valid_coord" in group.columns
                else int(group["receipt_no"].nunique()),
                "route_miles": round(float(miles), 2),
            }
        )
    return rows


def _summary_by_center(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    return (
        detail_df.groupby(["date", "scenario", "center_type"], dropna=False)
        .agg(
            technician_count=("employee_code", "nunique"),
            job_count=("job_count", "sum"),
            avg_jobs_per_tech=("job_count", "mean"),
            avg_miles_per_tech=("route_miles", "mean"),
            total_miles=("route_miles", "sum"),
        )
        .reset_index()
    )


def _assigned_job_rows(
    jobs_df: pd.DataFrame,
    tech_df: pd.DataFrame,
    result: dict[str, Any] | None,
    *,
    scenario: str,
) -> pd.DataFrame:
    """Return one row per assigned job, including the source job slot count."""
    if scenario == "Actual":
        rows = jobs_df.copy()
        rows["employee_code"] = rows.get("SVC_ENGINEER_CODE", "").map(_clean)
        rows["center_type"] = rows.get("SVC_CENTER_TYPE", "").map(_clean).str.upper()
        rows["assigned"] = rows["employee_code"].ne("")
        rows["job_slot_count"] = pd.to_numeric(rows.get("job_slot_count", 1), errors="coerce").fillna(1).clip(lower=1)
        return rows[rows["assigned"]].copy()

    assignments = (result or {}).get("assignments", [])
    if not assignments:
        return pd.DataFrame(columns=["employee_code", "center_type", "job_slot_count"])
    job_lookup = jobs_df.set_index(jobs_df["GSFS_RECEIPT_NO"].astype(str).str.strip()).to_dict("index")
    tech_lookup = tech_df.set_index("employee_code").to_dict("index")
    rows: list[dict[str, Any]] = []
    for item in assignments:
        receipt = _clean(item.get("receipt_no") or item.get("salesforce_id"))
        code = _clean(item.get("employee_code"))
        job = job_lookup.get(receipt, {})
        tech = tech_lookup.get(code, {})
        if not code or not job:
            continue
        rows.append(
            {
                "employee_code": code,
                "center_type": _clean(tech.get("center_type")).upper() or "DMS",
                "job_slot_count": max(1, int(pd.to_numeric(pd.Series([job.get("job_slot_count", item.get("job_slot_count", 1))]), errors="coerce").fillna(1).iloc[0])),
            }
        )
    return pd.DataFrame(rows)


def _scenario_metrics(
    date_key: str,
    scenario: str,
    jobs_df: pd.DataFrame,
    tech_df: pd.DataFrame,
    result: dict[str, Any] | None,
    detail_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    assigned = _assigned_job_rows(jobs_df, tech_df, result, scenario=scenario)
    total_jobs = len(jobs_df)
    assigned_jobs = len(assigned)
    unassigned_jobs = max(0, total_jobs - assigned_jobs)
    rows: list[dict[str, Any]] = []
    centers = sorted(set(tech_df.get("center_type", pd.Series(dtype=str)).dropna().astype(str).str.upper()) | {"DMS", "DMS2"})
    for center in centers:
        center_tech = tech_df[tech_df["center_type"].astype(str).str.upper().eq(center)]
        center_assigned = assigned[assigned["center_type"].eq(center)] if not assigned.empty else assigned
        codes = set(center_assigned.get("employee_code", pd.Series(dtype=str)).astype(str))
        active_tech = center_tech[center_tech["employee_code"].astype(str).isin(codes)]
        tech_count = len(active_tech)
        job_count = len(center_assigned)
        slot_count = float(center_assigned.get("job_slot_count", pd.Series(dtype=float)).sum())
        capacity = float(pd.to_numeric(active_tech.get("slot_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        details = detail_df[detail_df["center_type"].astype(str).str.upper().eq(center)] if not detail_df.empty else pd.DataFrame()
        miles = pd.to_numeric(details.get("route_miles", pd.Series(dtype=float)), errors="coerce").dropna()
        rows.append(
            {
                "date": date_key,
                "scenario": scenario,
                "total_jobs": total_jobs,
                "assigned_jobs": assigned_jobs,
                "unassigned_jobs": unassigned_jobs,
                "center_type": center,
                "assigned_technician_count": tech_count,
                "job_count": job_count,
                "slot_count": slot_count,
                "slot_capacity": capacity,
                "avg_jobs_per_tech": job_count / tech_count if tech_count else 0.0,
                "avg_slots_per_tech": slot_count / tech_count if tech_count else 0.0,
                "fill_rate_pct": slot_count / capacity * 100.0 if capacity else 0.0,
                "avg_miles_per_tech": float(miles.mean()) if not miles.empty else 0.0,
                "total_miles": float(miles.sum()) if not miles.empty else 0.0,
                "avg_duration_min": float(miles.mean() / 35.0 * 60.0) if not miles.empty else 0.0,
                "jobs_std": float(center_assigned.groupby("employee_code").size().std(ddof=0)) if not center_assigned.empty else 0.0,
                "slots_std": float(center_assigned.groupby("employee_code")["job_slot_count"].sum().std(ddof=0)) if not center_assigned.empty else 0.0,
            }
        )
    return rows


def _overall_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame()
    return (
        summary_df.groupby(["scenario", "center_type"], dropna=False)
        .agg(
            avg_technicians=("technician_count", "mean"),
            avg_jobs=("job_count", "mean"),
            avg_jobs_per_tech=("avg_jobs_per_tech", "mean"),
            avg_miles_per_tech=("avg_miles_per_tech", "mean"),
            avg_total_miles=("total_miles", "mean"),
        )
        .reset_index()
    )


def _render_html(summary_df: pd.DataFrame, overall_df: pd.DataFrame, metrics_df: pd.DataFrame, metrics_overall_df: pd.DataFrame, output_path: Path) -> None:
    def fmt_table(df: pd.DataFrame) -> str:
        if df.empty:
            return "<p>No data.</p>"
        formatted = df.copy()
        for col in formatted.columns:
            if pd.api.types.is_numeric_dtype(formatted[col]):
                formatted[col] = formatted[col].map(lambda x: f"{x:,.2f}" if isinstance(x, float) else f"{x:,}")
        return formatted.to_html(index=False, escape=False, classes="report-table")

    report_date = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <title>LA Bucket Routing Comparison Report</title>
  <style>
    body {{ font-family: Arial, 'Malgun Gothic', sans-serif; margin: 28px; color: #172033; }}
    h1 {{ margin-bottom: 4px; }}
    h2 {{ margin-top: 30px; border-bottom: 2px solid #e5e7eb; padding-bottom: 6px; }}
    .note {{ background: #f8fafc; border: 1px solid #dbe3ef; padding: 12px 14px; border-radius: 8px; line-height: 1.5; }}
    .report-table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 10px; }}
    .report-table th, .report-table td {{ border: 1px solid #d1d5db; padding: 7px 8px; text-align: right; }}
    .report-table th {{ background: #eff6ff; color: #0f172a; }}
    .report-table td:first-child, .report-table td:nth-child(2), .report-table td:nth-child(3) {{ text-align: left; }}
    .small {{ color: #64748b; font-size: 12px; }}
  </style>
</head>
<body>
  <h1>LA Bucket Routing Comparison Report</h1>
  <div class="small">Generated: {report_date}</div>
  <div class="note">
    <b>Basis</b><br>
    Actual is based on the assigned technicians in the original service data. Area Type Cluster and Bucket Sim Draft are based on VRP results.<br>
    Distance is an estimated route distance in miles starting from each technician home. Smart Routing uses the VRP sequence, and Actual uses the input order as the visit sequence.
  </div>
  <div class="note" style="display:none">
    <b>기준</b><br>
    Actual은 원본 서비스 데이터의 실제 배정 기사 기준입니다. Area Type Cluster와 Bucket Sim Draft는 VRP 실행 결과 기준입니다.<br>
    이동거리는 기사 home에서 시작해 job들을 방문하는 경로의 miles 추정값입니다. Smart Routing은 VRP sequence를 사용했고, Actual은 실제 방문 순서가 없어 nearest-neighbor 순서로 계산했습니다.
  </div>
  <h2>전체 평균</h2>
  {fmt_table(metrics_overall_df)}
  <h2>날짜별 / 시나리오별 통계</h2>
  {fmt_table(summary_df)}
  <h2>VRP DMS / DMS2 Metrics</h2>
  {fmt_table(metrics_df)}
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    capability_df = _load_capability_df()
    dates = sorted(
        p.name.split("_")[1]
        for p in (BASE_DIR / "area_type_clusters").glob("jobs_*_area_type_clusters.csv")
    )
    if args.dates:
        allowed = {str(value).strip() for value in args.dates.split(",") if str(value).strip()}
        dates = [date for date in dates if date in allowed]
    scenarios = list(SCENARIOS)
    if args.scenarios:
        requested = {str(value).strip() for value in args.scenarios.split(",") if str(value).strip()}
        scenarios = [scenario for scenario in scenarios if scenario in requested]

    detail_rows: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []

    for date_key in dates:
        actual_jobs = pd.read_csv(BASE_DIR / "area_type_clusters" / f"jobs_{date_key}_area_type_clusters.csv", encoding="utf-8-sig", low_memory=False)
        actual_tech = pd.read_csv(BASE_DIR / "area_type_clusters" / f"technicians_{date_key}_area_type_clusters.csv", encoding="utf-8-sig", low_memory=False)
        actual_detail = _summarize_assignments(date_key, "Actual", _actual_assignment_rows(actual_jobs, actual_tech), ordered=True)
        detail_rows.extend(actual_detail)
        metric_rows.extend(_scenario_metrics(date_key, "Actual", actual_jobs, actual_tech, None, pd.DataFrame(actual_detail)))

        for scenario_key in scenarios:
            city = SCENARIOS[scenario_key]
            jobs_path = BASE_DIR / scenario_key / f"jobs_{date_key}_{scenario_key}.csv"
            tech_path = BASE_DIR / scenario_key / f"technicians_{date_key}_{scenario_key}.csv"
            jobs_df = pd.read_csv(jobs_path, encoding="utf-8-sig", low_memory=False)
            tech_df = pd.read_csv(tech_path, encoding="utf-8-sig", low_memory=False)
            capability_rows = _build_capability_rows(city, tech_df, jobs_df, capability_df)
            payload = _build_payload(scenario_key, date_key, jobs_df, tech_df, capability_rows, args.time_limit_seconds)
            cache_path = REPORT_DIR / f"result_{scenario_key}_{date_key}.json"
            if cache_path.exists() and not args.force:
                result = json.loads(cache_path.read_text(encoding="utf-8"))
            else:
                print(f"Routing {SCENARIO_LABELS[scenario_key]} {date_key} jobs={len(jobs_df)} techs={len(tech_df)} caps={len(capability_rows)}")
                result = run_routing_request(payload)
                cache_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            run_rows.append(
                {
                    "date": date_key,
                    "scenario": SCENARIO_LABELS[scenario_key],
                    "jobs": len(jobs_df),
                    "technicians": len(tech_df),
                    "assigned": int(result.get("summary", {}).get("assigned_jobs", 0)),
                    "unassigned": int(result.get("summary", {}).get("unassigned_jobs", 0)),
                }
            )
            scenario_detail = _summarize_assignments(
                date_key,
                SCENARIO_LABELS[scenario_key],
                _smart_assignment_rows(result, jobs_df, tech_df),
                ordered=True,
            )
            detail_rows.extend(scenario_detail)
            metric_rows.extend(_scenario_metrics(date_key, SCENARIO_LABELS[scenario_key], jobs_df, tech_df, result, pd.DataFrame(scenario_detail)))

    detail_df = pd.DataFrame(detail_rows)
    summary_df = _summary_by_center(detail_df)
    overall_df = _overall_summary(summary_df)

    detail_df.to_csv(REPORT_DIR / "la_bucket_routing_technician_detail.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(REPORT_DIR / "la_bucket_routing_daily_summary.csv", index=False, encoding="utf-8-sig")
    overall_df.to_csv(REPORT_DIR / "la_bucket_routing_overall_summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(run_rows).to_csv(REPORT_DIR / "la_bucket_routing_run_status.csv", index=False, encoding="utf-8-sig")
    metrics_df = pd.DataFrame(metric_rows)
    metric_numeric = [
        "total_jobs", "assigned_jobs", "unassigned_jobs", "assigned_technician_count",
        "job_count", "slot_count", "slot_capacity", "avg_jobs_per_tech", "avg_slots_per_tech",
        "fill_rate_pct", "avg_miles_per_tech", "total_miles", "avg_duration_min", "jobs_std", "slots_std",
    ]
    metrics_overall_df = (
        metrics_df.groupby(["scenario", "center_type"], as_index=False)[metric_numeric].mean()
        if not metrics_df.empty
        else pd.DataFrame()
    )
    metrics_overall_df = metrics_overall_df.drop(
        columns=["unassigned_jobs", "slot_capacity"],
        errors="ignore",
    )
    for frame, filename in (
        (metrics_df, "la_bucket_routing_metrics.csv"),
        (metrics_overall_df, "la_bucket_routing_metrics_overall.csv"),
    ):
        try:
            frame.to_csv(REPORT_DIR / filename, index=False, encoding="utf-8-sig")
        except PermissionError:
            # Keep report generation usable while a CSV is open in Excel.
            print(f"Skipped locked output: {REPORT_DIR / filename}")
    _render_html(summary_df, overall_df, metrics_df, metrics_overall_df, REPORT_DIR / "la_bucket_routing_report.html")
    print(f"Report written: {REPORT_DIR / 'la_bucket_routing_report.html'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dates", default="", help="Comma-separated YYYYMMDD dates. Default: all dates.")
    parser.add_argument("--scenarios", default="", help="Comma-separated scenario keys. Default: all scenarios.")
    parser.add_argument("--time-limit-seconds", type=int, default=20)
    parser.add_argument("--force", action="store_true", help="Re-run even if cached result JSON exists.")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
