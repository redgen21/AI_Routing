from __future__ import annotations

from pathlib import Path

import pandas as pd

import smart_routing.production_assign_atlanta as base
from smart_routing.vrp_api_service import run_routing_request


TARGET_DATES = ["2026-01-19", "2026-01-20", "2026-01-21", "2026-01-22", "2026-01-23"]
OUT_DIR = Path("260310/production_output")
OUTPUT_SUFFIX = "actual_vs_smart_routing_5days_no_home_first"


def _dedupe_service(service_df: pd.DataFrame) -> pd.DataFrame:
    deduped = service_df.copy()
    sort_cols = [col for col in ["service_date_key", "GSFS_RECEIPT_NO", "service_time_min"] if col in deduped.columns]
    if sort_cols:
        ascending = [True] * len(sort_cols)
        if "service_time_min" in sort_cols:
            ascending[sort_cols.index("service_time_min")] = False
        deduped = deduped.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)
    if "GSFS_RECEIPT_NO" in deduped.columns:
        deduped = deduped.drop_duplicates(subset=["service_date_key", "GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
    return deduped


def _dms_only(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "SVC_CENTER_TYPE" not in df.columns:
        return df.copy()
    return df[df["SVC_CENTER_TYPE"].astype(str).str.upper().eq(base.DMS_CENTER_TYPE)].copy()


def _build_actual_assignment(service_df: pd.DataFrame, engineer_master_df: pd.DataFrame) -> pd.DataFrame:
    actual_df = _dms_only(service_df)
    actual_df["SVC_ENGINEER_CODE"] = actual_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    if actual_df.empty:
        return pd.DataFrame()

    name_lookup = (
        engineer_master_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        .set_index("SVC_ENGINEER_CODE")["Name"]
        .astype(str)
        .to_dict()
    )
    actual_df["assigned_sm_code"] = actual_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    actual_df["assigned_sm_name"] = actual_df["assigned_sm_code"].map(name_lookup).fillna(actual_df.get("SVC_ENGINEER_NAME", ""))
    actual_df["assigned_center_type"] = base.DMS_CENTER_TYPE
    return actual_df


def _build_actual_summary(
    actual_assignment_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
    route_client,
) -> pd.DataFrame:
    del engineer_master_df, region_centers
    rows: list[dict[str, object]] = []
    for (service_date_key, engineer_code), group_df in actual_assignment_df.groupby(["service_date_key", "assigned_sm_code"]):
        stop_coords = [
            (float(row["longitude"]), float(row["latitude"]))
            for _, row in group_df.iterrows()
            if pd.notna(row.get("longitude")) and pd.notna(row.get("latitude"))
        ]
        if len(stop_coords) > 1:
            payload = route_client.build_ordered_route(stop_coords, preserve_first=False)
            travel_distance_km = float(payload.get("distance_km", 0.0))
            travel_time_min = float(payload.get("duration_min", 0.0))
        else:
            travel_distance_km = 0.0
            travel_time_min = 0.0
        service_time_min = float(pd.to_numeric(group_df.get("service_time_min"), errors="coerce").fillna(0).sum())
        rows.append(
            {
                "service_date_key": str(service_date_key),
                "SVC_ENGINEER_CODE": str(engineer_code),
                "SVC_ENGINEER_NAME": str(group_df.get("assigned_sm_name", pd.Series([""])).dropna().astype(str).iloc[0]) if not group_df.empty else "",
                "assigned_center_type": base.DMS_CENTER_TYPE,
                "assigned_region_seq": pd.NA,
                "job_count": int(group_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "service_time_min": round(service_time_min, 2),
                "travel_time_min": round(travel_time_min, 2),
                "travel_distance_km": round(travel_distance_km, 2),
                "total_work_min": round(service_time_min + travel_time_min, 2),
                "overflow_480": bool(service_time_min + travel_time_min > base.MAX_WORK_MIN),
            }
        )
    return pd.DataFrame(rows)


def _sort_service_like_common(service_df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [col for col in ["service_date_key", "GSFS_RECEIPT_NO"] if col in service_df.columns]
    return service_df.sort_values(sort_cols).reset_index(drop=True) if sort_cols else service_df.reset_index(drop=True)


def _sort_technicians_like_common(engineer_region_df: pd.DataFrame, home_df: pd.DataFrame) -> pd.DataFrame:
    tech_df = engineer_region_df.merge(
        home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]],
        on="SVC_ENGINEER_CODE",
        how="left",
    )
    tech_df["latitude"] = pd.to_numeric(tech_df["latitude"], errors="coerce")
    tech_df["longitude"] = pd.to_numeric(tech_df["longitude"], errors="coerce")
    return (
        tech_df.sort_values(["longitude", "latitude", "SVC_ENGINEER_CODE"], na_position="last")
        .drop(columns=["latitude", "longitude"])
        .reset_index(drop=True)
    )


def _build_common_payload(
    service_day_df: pd.DataFrame,
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_date_key: str,
) -> dict[str, object]:
    home_lookup = home_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).set_index("SVC_ENGINEER_CODE")
    technicians: list[dict[str, object]] = []
    for _, tech in _sort_technicians_like_common(engineer_region_df, home_df).iterrows():
        code = str(tech["SVC_ENGINEER_CODE"]).strip()
        if code not in home_lookup.index:
            continue
        home_row = home_lookup.loc[code]
        home_lat = pd.to_numeric(home_row.get("latitude"), errors="coerce")
        home_lng = pd.to_numeric(home_row.get("longitude"), errors="coerce")
        if pd.isna(home_lat) or pd.isna(home_lng):
            continue
        technicians.append(
            {
                "employee_code": code,
                "employee_name": str(tech.get("Name", code)).strip() or code,
                "center_type": base.DMS_CENTER_TYPE,
                "start_location": {"lat": float(home_lat), "lng": float(home_lng)},
                "end_location": {"lat": float(home_lat), "lng": float(home_lng)},
                "shift_start": "08:00",
                "shift_end": "18:00",
                "slot_count": int(pd.to_numeric(tech.get("normalized_slot", 8), errors="coerce") or 8),
                "ref_heavy_repair_flag": str(tech.get("REF_HEAVY_REPAIR_FLAG", "Y")).strip().upper() or "Y",
            }
        )

    jobs: list[dict[str, object]] = []
    for _, row in _sort_service_like_common(service_day_df).iterrows():
        lat = pd.to_numeric(row.get("latitude"), errors="coerce")
        lng = pd.to_numeric(row.get("longitude"), errors="coerce")
        if pd.isna(lat) or pd.isna(lng):
            continue
        jobs.append(
            {
                "salesforce_id": str(row.get("GSFS_RECEIPT_NO", "")).strip(),
                "receipt_no": str(row.get("GSFS_RECEIPT_NO", "")).strip(),
                "product_group": str(row.get("SERVICE_PRODUCT_GROUP_CODE", "")).strip().upper(),
                "product": str(row.get("SERVICE_PRODUCT_CODE", "")).strip().upper(),
                "symptom": str(row.get("RECEIPT_DETAIL_SYMPTOM_CODE", "")).strip().upper(),
                "address": str(row.get("ADDRESS_LINE1_INFO", "")).strip(),
                "city_name": str(row.get("CITY_NAME", "")).strip(),
                "state_name": str(row.get("STATE_NAME", "")).strip(),
                "country_name": str(row.get("COUNTRY_NAME", "USA")).strip() or "USA",
                "postal_code": str(row.get("POSTAL_CODE", "")).strip(),
                "location": {"lat": float(lat), "lng": float(lng)},
                "time_window": [],
                "priority": 0,
                "fixed": False,
                "current_employee_code": str(row.get("SVC_ENGINEER_CODE", "")).strip(),
                "current_employee_name": str(row.get("SVC_ENGINEER_NAME", "")).strip(),
                "current_center_type": base.DMS_CENTER_TYPE,
                "region_seq": int(pd.to_numeric(row.get("region_seq"), errors="coerce")) if pd.notna(pd.to_numeric(row.get("region_seq"), errors="coerce")) else None,
                "region_name": str(row.get("new_region_name", "")).strip(),
                "is_tv_job": bool(row.get("is_tv_job", False)),
            }
        )

    return {
        "request_id": f"actual-vs-vrp-common-{service_date_key}",
        "mode": "na_general",
        "city": base.ATLANTA_CITY,
        "planning_date": service_date_key,
        "options": {
            "respect_fixed_jobs": True,
            "objective": "min_total_travel_time",
            "time_limit_seconds": 30,
            "timezone_offset": "-04:00",
        },
        "technicians": technicians,
        "jobs": jobs,
        "capabilities": [],
    }


def _metric_rows(mode: str, summary_df: pd.DataFrame, service_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame()
    working = summary_df.copy()
    working["job_count"] = pd.to_numeric(working["job_count"], errors="coerce").fillna(0)
    working["travel_distance_km"] = pd.to_numeric(working["travel_distance_km"], errors="coerce").fillna(0)
    working["travel_time_min"] = pd.to_numeric(working["travel_time_min"], errors="coerce").fillna(0)
    if working.empty:
        return pd.DataFrame()

    service_counts = (
        service_df.groupby("service_date_key")
        .agg(service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()))
        .reset_index()
    )
    daily = (
        working.groupby("service_date_key")
        .agg(
            deployed_sm_count=("SVC_ENGINEER_CODE", lambda s: s.dropna().astype(str).nunique()),
            total_travel_distance_km=("travel_distance_km", "sum"),
            total_travel_time_min=("travel_time_min", "sum"),
            avg_travel_distance_km=("travel_distance_km", "mean"),
            avg_travel_time_min=("travel_time_min", "mean"),
            std_travel_distance_km=("travel_distance_km", lambda s: float(s.std(ddof=0))),
            std_travel_time_min=("travel_time_min", lambda s: float(s.std(ddof=0))),
            avg_jobs_per_sm=("job_count", "mean"),
            std_jobs_per_sm=("job_count", lambda s: float(s.std(ddof=0))),
        )
        .reset_index()
        .merge(service_counts, on="service_date_key", how="left")
    )
    daily.insert(0, "mode", mode)
    numeric_cols = [col for col in daily.columns if col not in {"mode", "service_date_key"}]
    daily[numeric_cols] = daily[numeric_cols].round(2)
    return daily


def _aggregate_rows(daily_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for mode, group in daily_df.groupby("mode"):
        rows.append(
            {
                "mode": mode,
                "date_count": int(group["service_date_key"].nunique()),
                "service_count": int(pd.to_numeric(group["service_count"], errors="coerce").fillna(0).sum()),
                "avg_deployed_sm_count": round(float(group["deployed_sm_count"].mean()), 2),
                "avg_travel_distance_km": round(float(group["avg_travel_distance_km"].mean()), 2),
                "avg_travel_time_min": round(float(group["avg_travel_time_min"].mean()), 2),
                "std_travel_distance_km": round(float(group["std_travel_distance_km"].mean()), 2),
                "std_travel_time_min": round(float(group["std_travel_time_min"].mean()), 2),
                "avg_jobs_per_sm": round(float(group["avg_jobs_per_sm"].mean()), 2),
                "std_jobs_per_sm": round(float(group["std_jobs_per_sm"].mean()), 2),
            }
        )
    return pd.DataFrame(rows)


def _build_vrp_with_actual_engineers(
    service_df: pd.DataFrame,
    actual_assignment_df: pd.DataFrame,
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    assignment_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    schedule_frames: list[pd.DataFrame] = []
    for service_date_key, service_day_df in service_df.groupby("service_date_key"):
        actual_codes = set(
            actual_assignment_df[
                actual_assignment_df["service_date_key"].astype(str).eq(str(service_date_key))
            ]["assigned_sm_code"]
            .dropna()
            .astype(str)
            .str.strip()
        )
        if not actual_codes:
            continue
        day_engineer_region_df = engineer_region_df[
            engineer_region_df["SVC_ENGINEER_CODE"].astype(str).str.strip().isin(actual_codes)
        ].copy()
        day_home_df = home_df[
            home_df["SVC_ENGINEER_CODE"].astype(str).str.strip().isin(actual_codes)
        ].copy()
        payload = _build_common_payload(
            service_day_df=service_day_df.copy(),
            engineer_region_df=day_engineer_region_df.copy(),
            home_df=day_home_df.copy(),
            service_date_key=str(service_date_key),
        )
        result = run_routing_request(payload)
        assignment_df = pd.DataFrame(result.get("assignments", []))
        summary_df = pd.DataFrame(result.get("engineer_summary", []))
        schedule_df = assignment_df.copy()
        if not assignment_df.empty:
            assignment_df["service_date_key"] = str(service_date_key)
        if not assignment_df.empty:
            assignment_frames.append(assignment_df)
        if not summary_df.empty:
            summary_frames.append(summary_df)
        if not schedule_df.empty:
            schedule_frames.append(schedule_df)
    return (
        pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame(),
        pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame(),
        pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame(),
    )


def _comparison_deltas(daily_df: pd.DataFrame) -> pd.DataFrame:
    actual_df = daily_df[daily_df["mode"] == "actual"].copy()
    vrp_df = daily_df[daily_df["mode"] == "vrp"].copy()
    if actual_df.empty or vrp_df.empty:
        return pd.DataFrame()
    merged = actual_df.merge(vrp_df, on="service_date_key", suffixes=("_actual", "_vrp"))
    rows = merged[["service_date_key", "service_count_actual"]].rename(columns={"service_count_actual": "service_count"})
    for metric in [
        "deployed_sm_count",
        "avg_travel_distance_km",
        "avg_travel_time_min",
        "std_travel_distance_km",
        "std_travel_time_min",
        "avg_jobs_per_sm",
        "std_jobs_per_sm",
    ]:
        rows[f"{metric}_actual"] = merged[f"{metric}_actual"]
        rows[f"{metric}_vrp"] = merged[f"{metric}_vrp"]
        rows[f"{metric}_delta"] = (merged[f"{metric}_vrp"] - merged[f"{metric}_actual"]).round(2)
        base_value = merged[f"{metric}_actual"].replace(0, pd.NA)
        rows[f"{metric}_delta_pct"] = ((rows[f"{metric}_delta"] / base_value) * 100.0).round(2)
    return rows


def main() -> None:
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    wanted = set(TARGET_DATES)
    service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()
    service_df = _dedupe_service(_dms_only(service_df))

    engineer_region_df = _dms_only(engineer_region_df)
    home_df = _dms_only(home_df)
    engineer_master_df = base._build_engineer_master(engineer_region_df.copy(), home_df.copy())
    region_centers = base._region_centers(service_df)
    route_client = base._build_route_client()

    actual_assignment_df = _build_actual_assignment(service_df, engineer_master_df)
    actual_summary_df = _build_actual_summary(actual_assignment_df, engineer_master_df, region_centers, route_client)

    vrp_assignment_df, vrp_summary_df, vrp_schedule_df = _build_vrp_with_actual_engineers(
        service_df=service_df.copy(),
        actual_assignment_df=actual_assignment_df.copy(),
        engineer_region_df=engineer_region_df.copy(),
        home_df=home_df.copy(),
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    actual_assignment_df.to_csv(OUT_DIR / f"atlanta_assignment_result_actual_{OUTPUT_SUFFIX}.csv", index=False, encoding="utf-8-sig")
    actual_summary_df.to_csv(OUT_DIR / f"atlanta_engineer_day_summary_actual_{OUTPUT_SUFFIX}.csv", index=False, encoding="utf-8-sig")
    vrp_assignment_df.to_csv(OUT_DIR / f"atlanta_assignment_result_vrp_{OUTPUT_SUFFIX}.csv", index=False, encoding="utf-8-sig")
    vrp_summary_df.to_csv(OUT_DIR / f"atlanta_engineer_day_summary_vrp_{OUTPUT_SUFFIX}.csv", index=False, encoding="utf-8-sig")
    vrp_schedule_df.to_csv(OUT_DIR / f"atlanta_schedule_vrp_{OUTPUT_SUFFIX}.csv", index=False, encoding="utf-8-sig")

    daily_df = pd.concat(
        [
            _metric_rows("actual", actual_summary_df, service_df),
            _metric_rows("vrp", vrp_summary_df, service_df),
        ],
        ignore_index=True,
    )
    aggregate_df = _aggregate_rows(daily_df)
    delta_df = _comparison_deltas(daily_df)

    daily_path = OUT_DIR / f"atlanta_daily_compare_{OUTPUT_SUFFIX}.csv"
    aggregate_path = OUT_DIR / f"atlanta_aggregate_compare_{OUTPUT_SUFFIX}.csv"
    delta_path = OUT_DIR / f"atlanta_daily_delta_{OUTPUT_SUFFIX}.csv"
    daily_df.to_csv(daily_path, index=False, encoding="utf-8-sig")
    aggregate_df.to_csv(aggregate_path, index=False, encoding="utf-8-sig")
    delta_df.to_csv(delta_path, index=False, encoding="utf-8-sig")

    print(f"daily_compare_path={daily_path}")
    print(f"aggregate_compare_path={aggregate_path}")
    print(f"daily_delta_path={delta_path}")
    if not aggregate_df.empty:
        print(aggregate_df.to_string(index=False))


if __name__ == "__main__":
    main()
