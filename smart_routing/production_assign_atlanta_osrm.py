from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

import smart_routing.production_assign_atlanta as base


PRODUCTION_OUTPUT_DIR = Path("260310/production_output")


@dataclass
class AtlantaProductionOSRMAssignmentResult:
    assignment_path: Path
    engineer_day_summary_path: Path
    schedule_path: Path
    daily_compare_path: Path


def _output_paths(output_suffix: str) -> tuple[Path, Path, Path, Path]:
    suffix = str(output_suffix).strip()
    if not suffix:
        suffix = "osrm"
    return (
        PRODUCTION_OUTPUT_DIR / f"atlanta_assignment_result_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_schedule_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_daily_compare_line_vs_{suffix}.csv",
    )


def _weighted_jobs_std(assignment_df: pd.DataFrame) -> float:
    if assignment_df.empty:
        return 0.0
    weighted_df = assignment_df.copy()
    weighted_df["weighted_job_unit"] = weighted_df["is_heavy_repair"].fillna(False).astype(bool).map(lambda flag: 2.0 if flag else 1.0)
    weighted_jobs = weighted_df.groupby(weighted_df["assigned_sm_code"].astype(str))["weighted_job_unit"].sum()
    weighted_jobs = weighted_jobs[weighted_jobs > 0]
    return float(weighted_jobs.std(ddof=0)) if not weighted_jobs.empty else 0.0


def _daily_metrics(assignment_df: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    if assignment_df.empty or summary_df.empty:
        return pd.DataFrame()
    service_counts = (
        assignment_df.groupby("service_date_key")["GSFS_RECEIPT_NO"]
        .nunique()
        .rename("service_count")
        .reset_index()
    )
    heavy_counts = (
        assignment_df.groupby("service_date_key")["is_heavy_repair"]
        .sum()
        .rename("heavy_repair_count")
        .reset_index()
    )
    tv_counts = (
        assignment_df.groupby("service_date_key")["is_tv_job"]
        .sum()
        .rename("tv_job_count")
        .reset_index()
    )
    service_time = (
        assignment_df.groupby("service_date_key")["service_time_min"]
        .sum()
        .rename("total_service_time_min")
        .reset_index()
    )
    summary = (
        summary_df.groupby("service_date_key")
        .agg(
            assigned_engineer_count=("SVC_ENGINEER_CODE", "nunique"),
            total_distance_km=("route_distance_km", "sum"),
            total_duration_min=("route_duration_min", "sum"),
            avg_distance_km=("route_distance_km", "mean"),
            avg_duration_min=("route_duration_min", "mean"),
            jobs_std=("job_count", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).std(ddof=0))),
            max_total_work_min=("total_work_min", "max"),
            overflow_480_count=("overflow_480", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        )
        .reset_index()
    )
    weighted_std_rows: list[dict[str, object]] = []
    for service_date_key, group in assignment_df.groupby("service_date_key"):
        weighted_std_rows.append(
            {
                "service_date_key": str(service_date_key),
                "weighted_jobs_std": _weighted_jobs_std(group),
            }
        )
    weighted_std_df = pd.DataFrame(weighted_std_rows)
    return (
        summary.merge(service_counts, on="service_date_key", how="left")
        .merge(heavy_counts, on="service_date_key", how="left")
        .merge(tv_counts, on="service_date_key", how="left")
        .merge(service_time, on="service_date_key", how="left")
        .merge(weighted_std_df, on="service_date_key", how="left")
        .sort_values("service_date_key")
        .reset_index(drop=True)
    )


def build_atlanta_production_assignment_osrm(
    date_keys: list[str] | None = None,
    output_suffix: str = "osrm",
    include_daily_compare: bool = True,
    attendance_limited: bool = False,
) -> AtlantaProductionOSRMAssignmentResult:
    assignment_path, summary_path, schedule_path, daily_compare_path = _output_paths(output_suffix)
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    engineer_master_df = base._build_engineer_master(engineer_region_df, home_df)
    region_centers = base._region_centers(service_df)
    border_expansion_zip_map = base._build_border_expansion_zip_map(service_df, region_centers)
    attendance_master_df, attendance_by_date = base._build_actual_attendance_master(service_df, engineer_master_df)
    route_client = base._build_route_client()
    if date_keys:
        wanted = {str(v) for v in date_keys}
        service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()

    orig_estimate = base._estimate_incremental_travel
    orig_targeted = base._targeted_region_worst_move_rebalance

    def osrm_estimate(prev_coord, next_coord, route_client_unused=None):
        if prev_coord is None or next_coord is None:
            return 0.0, 0.0
        return route_client.pair_distance(prev_coord, next_coord)

    base._estimate_incremental_travel = osrm_estimate
    base._targeted_region_worst_move_rebalance = lambda assignment_df, engineer_master_df, region_centers, route_client: assignment_df

    assignment_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    schedule_frames: list[pd.DataFrame] = []

    try:
        for _, service_day_df in service_df.groupby("service_date_key"):
            day_engineer_master_df = engineer_master_df.copy()
            if attendance_limited:
                allowed_codes = attendance_by_date.get(str(service_day_df["service_date_key"].iloc[0]), set())
                day_engineer_master_df = attendance_master_df[
                    attendance_master_df["SVC_ENGINEER_CODE"].astype(str).isin(allowed_codes)
                ].copy()
                if day_engineer_master_df.empty:
                    continue
            assignment_df, summary_df = base._assign_day(
                service_day_df.copy(),
                day_engineer_master_df.copy(),
                region_centers,
                route_client,
                border_expansion_zip_map,
            )
            if assignment_df.empty:
                continue
            assignment_frames.append(assignment_df)
            summary_frames.append(summary_df)

            for _, group_df in assignment_df.groupby("assigned_sm_code"):
                schedule_df, route_payload = base._build_schedule_for_group(group_df.copy(), route_client)
                if schedule_df.empty:
                    continue
                schedule_df["route_distance_km"] = round(float(route_payload["distance_km"]), 2)
                schedule_df["route_duration_min"] = round(float(route_payload["duration_min"]), 2)
                schedule_frames.append(schedule_df)
    finally:
        base._estimate_incremental_travel = orig_estimate
        base._targeted_region_worst_move_rebalance = orig_targeted

    assignment_result_df = pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame()
    engineer_day_summary_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()
    schedule_result_df = pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame()

    if not schedule_result_df.empty:
        route_summary_df = (
            schedule_result_df.groupby(["service_date_key", "assigned_sm_code"])
            .agg(route_distance_km=("route_distance_km", "max"), route_duration_min=("route_duration_min", "max"))
            .reset_index()
        )
        engineer_day_summary_df = engineer_day_summary_df.merge(
            route_summary_df,
            left_on=["service_date_key", "SVC_ENGINEER_CODE"],
            right_on=["service_date_key", "assigned_sm_code"],
            how="left",
        ).drop(columns=["assigned_sm_code"], errors="ignore")
        if "route_duration_min" in engineer_day_summary_df.columns:
            engineer_day_summary_df["travel_time_min"] = pd.to_numeric(
                engineer_day_summary_df["route_duration_min"], errors="coerce"
            ).fillna(pd.to_numeric(engineer_day_summary_df["travel_time_min"], errors="coerce").fillna(0))
        if "route_distance_km" in engineer_day_summary_df.columns:
            engineer_day_summary_df["travel_distance_km"] = pd.to_numeric(
                engineer_day_summary_df["route_distance_km"], errors="coerce"
            ).fillna(pd.to_numeric(engineer_day_summary_df["travel_distance_km"], errors="coerce").fillna(0))
        engineer_day_summary_df["total_work_min"] = (
            pd.to_numeric(engineer_day_summary_df["service_time_min"], errors="coerce").fillna(0)
            + pd.to_numeric(engineer_day_summary_df["travel_time_min"], errors="coerce").fillna(0)
        ).round(2)
        engineer_day_summary_df["overflow_480"] = engineer_day_summary_df["total_work_min"] > base.MAX_WORK_MIN

    daily_compare_df = pd.DataFrame()
    if include_daily_compare:
        line_assignment_df = pd.read_csv(base.ASSIGNMENT_PATH, encoding="utf-8-sig", low_memory=False) if base.ASSIGNMENT_PATH.exists() else pd.DataFrame()
        line_summary_df = pd.read_csv(base.ENGINEER_DAY_SUMMARY_PATH, encoding="utf-8-sig", low_memory=False) if base.ENGINEER_DAY_SUMMARY_PATH.exists() else pd.DataFrame()
        for df in [line_assignment_df, engineer_day_summary_df, assignment_result_df, line_summary_df]:
            if not df.empty and "service_date_key" in df.columns:
                df["service_date_key"] = df["service_date_key"].astype(str)
        if date_keys:
            wanted = {str(v) for v in date_keys}
            if not line_assignment_df.empty:
                line_assignment_df = line_assignment_df[line_assignment_df["service_date_key"].astype(str).isin(wanted)].copy()
            if not line_summary_df.empty:
                line_summary_df = line_summary_df[line_summary_df["service_date_key"].astype(str).isin(wanted)].copy()
        line_daily = _daily_metrics(line_assignment_df, line_summary_df)
        osrm_daily = _daily_metrics(assignment_result_df, engineer_day_summary_df)
        if not line_daily.empty or not osrm_daily.empty:
            daily_compare_df = line_daily.merge(osrm_daily, on="service_date_key", how="outer", suffixes=("_line", "_osrm"))
            rename_map = {}
            for col in daily_compare_df.columns:
                if col.endswith("_line"):
                    rename_map[col] = f"line_{col[:-5]}"
                elif col.endswith("_osrm"):
                    rename_map[col] = f"osrm_{col[:-5]}"
            daily_compare_df = daily_compare_df.rename(columns=rename_map).sort_values("service_date_key").reset_index(drop=True)

    PRODUCTION_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    assignment_result_df.to_csv(assignment_path, index=False, encoding="utf-8-sig")
    engineer_day_summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    schedule_result_df.to_csv(schedule_path, index=False, encoding="utf-8-sig")
    daily_compare_df.to_csv(daily_compare_path, index=False, encoding="utf-8-sig")
    return AtlantaProductionOSRMAssignmentResult(
        assignment_path=assignment_path,
        engineer_day_summary_path=summary_path,
        schedule_path=schedule_path,
        daily_compare_path=daily_compare_path,
    )
