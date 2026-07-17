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


BASE_CITY = "Atlanta, GA"
BASE_DIR = Path("260310/atlanta 2604 test")
REPORT_DIR = BASE_DIR / "routing_report"
SERVICE_FILE = Path("260310/input/Atlanta test 2604.xlsx")
REGION_FILE = Path("260310/production_input/atlanta_fixed_region_zip_3.csv")
ENGINEER_REGION_FILE = Path("260310/production_input/atlanta_engineer_region_assignment.csv")
HOME_FILE = Path("260310/production_input/atlanta_engineer_home_geocoded.csv")
PROFILE_FILE = Path("260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx")


def _clean(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _norm_zip(value: object) -> str:
    text = _clean(value).replace(".0", "")
    return text.zfill(5) if text else ""


def _truthy(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "y", "yes", "t"}


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


def _route_miles(home: tuple[float, float] | None, coords: list[tuple[float, float]], *, ordered: bool) -> float:
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


def _valid_coord(lon: object, lat: object) -> bool:
    lon_num = pd.to_numeric(pd.Series([lon]), errors="coerce").iloc[0]
    lat_num = pd.to_numeric(pd.Series([lat]), errors="coerce").iloc[0]
    if pd.isna(lon_num) or pd.isna(lat_num):
        return False
    return 29.0 <= float(lat_num) <= 36.5 and -90.0 <= float(lon_num) <= -80.0


def _load_jobs() -> pd.DataFrame:
    if not SERVICE_FILE.exists():
        raise FileNotFoundError(SERVICE_FILE)
    df = pd.read_excel(SERVICE_FILE, dtype={"POSTAL_CODE": str})
    df = df[df["STRATEGIC_CITY_NAME"].astype(str).str.strip().eq(BASE_CITY)].copy()
    df["PROMISE_DATE"] = df["PROMISE_DATE"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    df["POSTAL_CODE"] = df["POSTAL_CODE"].map(_norm_zip)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["PROMISE_DATE", "GSFS_RECEIPT_NO", "latitude", "longitude"]).copy()
    df = df.sort_values(["PROMISE_DATE", "GSFS_RECEIPT_NO"]).drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first")
    return df.reset_index(drop=True)


def _load_region() -> pd.DataFrame:
    df = pd.read_csv(REGION_FILE, encoding="utf-8-sig", dtype={"POSTAL_CODE": str}, low_memory=False)
    df["POSTAL_CODE"] = df["POSTAL_CODE"].map(_norm_zip)
    if "area_type" not in df.columns:
        df["area_type"] = "DMS"
    df["area_type"] = df["area_type"].fillna("DMS").astype(str).str.strip().replace("", "DMS").str.upper()
    return df


def _load_technicians() -> pd.DataFrame:
    region_df = pd.read_csv(ENGINEER_REGION_FILE, encoding="utf-8-sig", low_memory=False)
    home_df = pd.read_csv(HOME_FILE, encoding="utf-8-sig", low_memory=False)
    region_df["SVC_ENGINEER_CODE"] = region_df["SVC_ENGINEER_CODE"].map(_clean)
    home_df["SVC_ENGINEER_CODE"] = home_df["SVC_ENGINEER_CODE"].map(_clean)
    merged = region_df.merge(
        home_df[["SVC_ENGINEER_CODE", "Home Street Address", "City ", "State", "Zip", "latitude", "longitude"]],
        on="SVC_ENGINEER_CODE",
        how="left",
    )
    merged["home_latitude"] = pd.to_numeric(merged["latitude"], errors="coerce")
    merged["home_longitude"] = pd.to_numeric(merged["longitude"], errors="coerce")
    merged = merged.dropna(subset=["home_latitude", "home_longitude"]).copy()
    merged = merged[merged["SVC_CENTER_TYPE"].astype(str).str.upper().eq("DMS")].copy()
    return pd.DataFrame(
        {
            "employee_code": merged["SVC_ENGINEER_CODE"].astype(str).str.strip(),
            "employee_name": merged["Name"].fillna("").astype(str).str.strip(),
            "center_type": "DMS",
            "home_address": merged["Home Street Address"].fillna("").astype(str).str.strip(),
            "home_city": merged["City "].fillna("").astype(str).str.strip(),
            "home_state": merged["State"].fillna("").astype(str).str.strip(),
            "home_country": "USA",
            "home_postal_code": merged["Zip"].map(_norm_zip),
            "home_latitude": merged["home_latitude"],
            "home_longitude": merged["home_longitude"],
            "slot_count": pd.to_numeric(merged.get("normalized_slot", 8), errors="coerce").fillna(8).astype(int),
            "max_slots": pd.to_numeric(merged.get("normalized_slot", 8), errors="coerce").fillna(8).astype(int),
            "max_jobs": pd.to_numeric(merged.get("normalized_slot", 8), errors="coerce").fillna(8).astype(int),
            "max_minutes": 540,
            "priority_group": "B",
            "available": True,
            "shift_start": "09:00",
            "shift_end": "18:00",
        }
    ).drop_duplicates(subset=["employee_code"])


def _load_capability_df() -> pd.DataFrame:
    df = pd.read_excel(PROFILE_FILE, sheet_name="3. Product")
    for col in [
        "STRATEGIC_CITY_NAME",
        "SVC_ENGINEER_CODE",
        "SERVICE_PRODUCT_GROUP_CODE",
        "SERVICE_PRODUCT_CODE",
        "REPAIR_FLAG",
        "AREA_PRODUCT_FLAG",
    ]:
        if col in df.columns:
            df[col] = df[col].map(_clean)
    return df


def _build_capability_rows(tech_df: pd.DataFrame, jobs_df: pd.DataFrame, capability_df: pd.DataFrame) -> list[dict[str, Any]]:
    employee_codes = {_clean(code) for code in tech_df["employee_code"].tolist() if _clean(code)}
    requested_products = {
        (_clean(row.get("SERVICE_PRODUCT_GROUP_CODE")).upper(), _clean(row.get("SERVICE_PRODUCT_CODE")).upper())
        for _, row in jobs_df.iterrows()
        if _clean(row.get("SERVICE_PRODUCT_GROUP_CODE")) and _clean(row.get("SERVICE_PRODUCT_CODE"))
    }
    filtered = capability_df[
        capability_df["STRATEGIC_CITY_NAME"].astype(str).eq(BASE_CITY)
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
    return [
        {
            "employee_code": _clean(row["SVC_ENGINEER_CODE"]),
            "product_group_code": _clean(row["SERVICE_PRODUCT_GROUP_CODE"]).upper(),
            "product_code": _clean(row["SERVICE_PRODUCT_CODE"]).upper(),
            "heavy_repair_allowed": not (
                _clean(row["SERVICE_PRODUCT_GROUP_CODE"]).upper() == "REF"
                and _clean(row.get("AREA_PRODUCT_FLAG")).upper() == "N"
            ),
        }
        for _, row in filtered.iterrows()
    ]


def _technician_upload_frame(tech_df: pd.DataFrame, date_key: str) -> pd.DataFrame:
    out = tech_df.copy()
    out["subsidiary_name"] = "LGEAI"
    out["strategic_city_name"] = BASE_CITY
    out["PROMISE_DATE"] = date_key
    out["start_location_type"] = "Home"
    out["start_location_address"] = ""
    cols = [
        "subsidiary_name",
        "strategic_city_name",
        "PROMISE_DATE",
        "employee_code",
        "employee_name",
        "center_type",
        "available",
        "shift_start",
        "shift_end",
        "slot_count",
        "max_slots",
        "max_jobs",
        "max_minutes",
        "priority_group",
        "start_location_type",
        "start_location_address",
        "home_address",
        "home_city",
        "home_state",
        "home_country",
        "home_postal_code",
        "home_latitude",
        "home_longitude",
    ]
    return out.reindex(columns=cols)


def _job_upload_frame(day_df: pd.DataFrame, region_df: pd.DataFrame) -> pd.DataFrame:
    lookup_cols = [col for col in ["POSTAL_CODE", "region_seq", "new_region_name", "AREA_NAME", "area_type"] if col in region_df.columns]
    lookup = region_df[lookup_cols].drop_duplicates(subset=["POSTAL_CODE"])
    merged = day_df.merge(lookup, on="POSTAL_CODE", how="left")
    merged["STRATEGIC_CITY_NAME"] = BASE_CITY
    merged["fixed"] = False
    merged["reschedule"] = False
    merged["job_slot_count"] = 1
    merged["area_type"] = merged.get("area_type", "DMS")
    merged["area_type"] = merged["area_type"].fillna("DMS").astype(str).str.strip().replace("", "DMS").str.upper()
    cols = [
        "SUBSIDIARY_NAME",
        "STRATEGIC_CITY_NAME",
        "SVC_ENGINEER_CODE",
        "SVC_ENGINEER_NAME",
        "SVC_CENTER_TYPE",
        "SERVICE_PRODUCT_GROUP_CODE",
        "SERVICE_PRODUCT_CODE",
        "RECEIPT_DETAIL_SYMPTOM_CODE",
        "GSFS_RECEIPT_NO",
        "PROMISE_DATE",
        "CITY_NAME",
        "STATE_NAME",
        "COUNTRY_NAME",
        "POSTAL_CODE",
        "ADDRESS_LINE1_INFO",
        "latitude",
        "longitude",
        "fixed",
        "reschedule",
        "job_slot_count",
        "region_seq",
        "new_region_name",
        "AREA_NAME",
        "area_type",
    ]
    return merged.reindex(columns=cols)


def _build_payload(date_key: str, jobs_df: pd.DataFrame, tech_df: pd.DataFrame, capability_rows: list[dict[str, Any]], time_limit_seconds: int) -> dict[str, Any]:
    dms_codes = sorted(_clean(code) for code in tech_df["employee_code"].tolist() if _clean(code))
    tech_payload = []
    for _, row in tech_df.iterrows():
        tech_payload.append(
            {
                "employee_code": _clean(row.get("employee_code")),
                "employee_name": _clean(row.get("employee_name")) or _clean(row.get("employee_code")),
                "center_type": "DMS",
                "available": _truthy(row.get("available", True)),
                "shift_start": _clean(row.get("shift_start")) or "09:00",
                "shift_end": _clean(row.get("shift_end")) or "18:00",
                "slot_count": int(pd.to_numeric(pd.Series([row.get("slot_count", 8)]), errors="coerce").fillna(8).iloc[0]),
                "max_slots": int(pd.to_numeric(pd.Series([row.get("max_slots", row.get("slot_count", 8))]), errors="coerce").fillna(8).iloc[0]),
                "max_jobs": int(pd.to_numeric(pd.Series([row.get("max_jobs", row.get("slot_count", 8))]), errors="coerce").fillna(8).iloc[0]),
                "max_minutes": int(pd.to_numeric(pd.Series([row.get("max_minutes", 540)]), errors="coerce").fillna(540).iloc[0]),
                "priority_group": _clean(row.get("priority_group")) or "B",
                "start_location": {"lat": float(row["home_latitude"]), "lng": float(row["home_longitude"])},
            }
        )

    job_payload = []
    for _, row in jobs_df.iterrows():
        receipt = _clean(row.get("GSFS_RECEIPT_NO"))
        if not receipt:
            continue
        region_seq_num = pd.to_numeric(pd.Series([row.get("region_seq")]), errors="coerce").iloc[0]
        area_type = _clean(row.get("area_type")).upper() or "DMS"
        job_payload.append(
            {
                "salesforce_id": receipt,
                "receipt_no": receipt,
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
                "fixed": False,
                "reschedule": False,
                "job_slot_count": int(pd.to_numeric(pd.Series([row.get("job_slot_count", 1)]), errors="coerce").fillna(1).iloc[0]),
                "current_employee_code": _clean(row.get("SVC_ENGINEER_CODE")),
                "current_employee_name": _clean(row.get("SVC_ENGINEER_NAME")),
                "current_center_type": _clean(row.get("SVC_CENTER_TYPE")).upper(),
                "region_seq": int(region_seq_num) if pd.notna(region_seq_num) else None,
                "region_name": _clean(row.get("new_region_name")) or _clean(row.get("AREA_NAME")),
                "area_type": area_type,
                "eligible_employee_codes": dms_codes,
            }
        )
    job_payload = _apply_job_capabilities(job_payload, capability_rows)
    return {
        "request_id": f"atlanta_2604_{date_key}",
        "mode": "na_general",
        "city": BASE_CITY,
        "planning_date": f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}",
        "options": {
            "respect_fixed_jobs": True,
            "objective": "min_total_travel_time",
            "time_limit_seconds": time_limit_seconds,
        },
        "technicians": tech_payload,
        "jobs": job_payload,
        "capabilities": capability_rows,
    }


def _fallback_actual_tech(jobs_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        jobs_df.groupby(["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "SVC_CENTER_TYPE"], dropna=False)
        .agg(home_latitude=("latitude", "mean"), home_longitude=("longitude", "mean"))
        .reset_index()
    )
    return pd.DataFrame(
        {
            "employee_code": grouped["SVC_ENGINEER_CODE"].astype(str).str.strip(),
            "employee_name": grouped["SVC_ENGINEER_NAME"].fillna("").astype(str).str.strip(),
            "center_type": grouped["SVC_CENTER_TYPE"].fillna("UNKNOWN").astype(str).str.upper(),
            "home_latitude": grouped["home_latitude"],
            "home_longitude": grouped["home_longitude"],
        }
    )


def _actual_assignment_rows(jobs_df: pd.DataFrame, tech_df: pd.DataFrame, fallback_tech_df: pd.DataFrame) -> pd.DataFrame:
    tech_all = pd.concat([tech_df, fallback_tech_df], ignore_index=True).drop_duplicates(subset=["employee_code"], keep="first")
    tech_lookup = tech_all.set_index("employee_code").to_dict("index") if not tech_all.empty else {}
    sequence_by_tech: dict[str, int] = {}
    rows = []
    for _, job in jobs_df.iterrows():
        code = _clean(job.get("SVC_ENGINEER_CODE"))
        if not code:
            continue
        sequence_by_tech[code] = sequence_by_tech.get(code, 0) + 1
        tech = tech_lookup.get(code, {})
        rows.append(
            {
                "employee_code": code,
                "employee_name": _clean(job.get("SVC_ENGINEER_NAME")) or _clean(tech.get("employee_name")) or code,
                "center_type": _clean(job.get("SVC_CENTER_TYPE")).upper() or _clean(tech.get("center_type")).upper() or "UNKNOWN",
                "receipt_no": _clean(job.get("GSFS_RECEIPT_NO")),
                "lat": float(job["latitude"]),
                "lng": float(job["longitude"]),
                "valid_coord": _valid_coord(job.get("longitude"), job.get("latitude")),
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
                "valid_coord": _valid_coord(job.get("longitude"), job.get("latitude")),
                "home_lat": pd.to_numeric(pd.Series([tech.get("home_latitude")]), errors="coerce").iloc[0],
                "home_lng": pd.to_numeric(pd.Series([tech.get("home_longitude")]), errors="coerce").iloc[0],
                "sequence": int(item.get("sequence", item.get("visit_seq", 0)) or 0),
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
        miles = _route_miles(home, coords, ordered=ordered)
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


def _write_inputs(jobs_df: pd.DataFrame, region_df: pd.DataFrame, tech_df: pd.DataFrame) -> list[str]:
    input_dir = BASE_DIR / "daily_inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    tech_df.to_csv(BASE_DIR / "atlanta_technician_master.csv", index=False, encoding="utf-8-sig")
    dates: list[str] = []
    for date_key, day_df in jobs_df.groupby("PROMISE_DATE", sort=True):
        date_key = str(date_key)
        dates.append(date_key)
        jobs_out = _job_upload_frame(day_df.copy(), region_df)
        tech_out = _technician_upload_frame(tech_df, date_key)
        jobs_out.to_csv(input_dir / f"jobs_{date_key}_atlanta.csv", index=False, encoding="utf-8-sig")
        tech_out.to_csv(input_dir / f"technicians_{date_key}_atlanta.csv", index=False, encoding="utf-8-sig")
    return dates


def _render_html(summary_df: pd.DataFrame, overall_df: pd.DataFrame, run_df: pd.DataFrame, output_path: Path) -> None:
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
  <title>Atlanta 2604 Routing Comparison Report</title>
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
  <h1>Atlanta 2604 Routing Comparison Report</h1>
  <div class="small">Generated: {report_date}</div>
  <div class="note">
    <b>Basis</b><br>
    Input: {SERVICE_FILE}<br>
    Smart Routing uses all Atlanta DMS technicians from the Atlanta technician master, not only technicians appearing in each day's job list.<br>
    Actual is based on original assigned technicians in the service data. Distance is haversine route miles from technician home; Smart Routing uses VRP sequence, Actual uses input order.
  </div>
  <h2>Overall Average</h2>
  {fmt_table(overall_df)}
  <h2>Daily Summary</h2>
  {fmt_table(summary_df)}
  <h2>Run Status</h2>
  {fmt_table(run_df)}
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    jobs_df = _load_jobs()
    region_df = _load_region()
    tech_df = _load_technicians()
    capability_df = _load_capability_df()
    fallback_actual_tech_df = _fallback_actual_tech(jobs_df)
    dates = _write_inputs(jobs_df, region_df, tech_df)
    if args.dates:
        requested = {value.strip() for value in args.dates.split(",") if value.strip()}
        dates = [date for date in dates if date in requested]

    detail_rows: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []
    input_dir = BASE_DIR / "daily_inputs"
    for date_key in dates:
        jobs_path = input_dir / f"jobs_{date_key}_atlanta.csv"
        tech_path = input_dir / f"technicians_{date_key}_atlanta.csv"
        jobs_day = pd.read_csv(jobs_path, encoding="utf-8-sig", low_memory=False, dtype={"POSTAL_CODE": str})
        tech_day = pd.read_csv(tech_path, encoding="utf-8-sig", low_memory=False)
        detail_rows.extend(
            _summarize_assignments(
                date_key,
                "Actual",
                _actual_assignment_rows(jobs_day, tech_day, fallback_actual_tech_df),
                ordered=True,
            )
        )
        capability_rows = _build_capability_rows(tech_day, jobs_day, capability_df)
        payload = _build_payload(date_key, jobs_day, tech_day, capability_rows, args.time_limit_seconds)
        result_path = REPORT_DIR / f"result_smart_routing_{date_key}.json"
        if result_path.exists() and not args.force:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        else:
            print(f"Routing Atlanta {date_key} jobs={len(jobs_day)} techs={len(tech_day)} caps={len(capability_rows)}")
            result = run_routing_request(payload)
            result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        run_rows.append(
            {
                "date": date_key,
                "jobs": len(jobs_day),
                "technicians": len(tech_day),
                "capabilities": len(capability_rows),
                "assigned": int(result.get("summary", {}).get("assigned_jobs", 0)),
                "unassigned": int(result.get("summary", {}).get("unassigned_jobs", 0)),
            }
        )
        detail_rows.extend(
            _summarize_assignments(
                date_key,
                "Smart Routing",
                _smart_assignment_rows(result, jobs_day, tech_day),
                ordered=True,
            )
        )

    detail_df = pd.DataFrame(detail_rows)
    summary_df = _summary_by_center(detail_df)
    overall_df = _overall_summary(summary_df)
    run_df = pd.DataFrame(run_rows)

    detail_df.to_csv(REPORT_DIR / "atlanta_2604_routing_technician_detail.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(REPORT_DIR / "atlanta_2604_routing_daily_summary.csv", index=False, encoding="utf-8-sig")
    overall_df.to_csv(REPORT_DIR / "atlanta_2604_routing_overall_summary.csv", index=False, encoding="utf-8-sig")
    run_df.to_csv(REPORT_DIR / "atlanta_2604_routing_run_status.csv", index=False, encoding="utf-8-sig")
    _render_html(summary_df, overall_df, run_df, REPORT_DIR / "atlanta_2604_routing_report.html")
    print(f"Inputs written: {BASE_DIR / 'daily_inputs'}")
    print(f"Report written: {REPORT_DIR / 'atlanta_2604_routing_report.html'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dates", default="", help="Comma-separated YYYYMMDD dates. Default: all dates.")
    parser.add_argument("--time-limit-seconds", type=int, default=10)
    parser.add_argument("--force", action="store_true", help="Re-run even if cached result JSON exists.")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
