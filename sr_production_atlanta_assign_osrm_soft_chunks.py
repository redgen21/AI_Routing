from __future__ import annotations

from pathlib import Path

import pandas as pd

from smart_routing.production_assign_atlanta import _load_inputs
from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm


OUT_DIR = Path("260310/production_output")


def main() -> None:
    _, _, _, service_df = _load_inputs()
    dates = sorted(service_df["service_date_key"].dropna().astype(str).unique().tolist())
    chunk_size = 6
    chunks = [dates[i : i + chunk_size] for i in range(0, len(dates), chunk_size)]

    for idx, chunk in enumerate(chunks, start=1):
        suffix = f"osrm_soft_chunk_{idx:02d}"
        assignment_chunk_path = OUT_DIR / f"atlanta_assignment_result_{suffix}.csv"
        summary_chunk_path = OUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv"
        schedule_chunk_path = OUT_DIR / f"atlanta_schedule_{suffix}.csv"
        if assignment_chunk_path.exists() and summary_chunk_path.exists() and schedule_chunk_path.exists():
            print(f"chunk={idx} skipped_existing={assignment_chunk_path.name}")
            continue
        result = build_atlanta_production_assignment_osrm(
            date_keys=chunk,
            output_suffix=suffix,
            include_daily_compare=False,
        )
        print(f"chunk={idx} start={chunk[0]} end={chunk[-1]} assignment={result.assignment_path}")

    assignment_parts = []
    summary_parts = []
    schedule_parts = []
    for idx in range(1, len(chunks) + 1):
        suffix = f"osrm_soft_chunk_{idx:02d}"
        assignment_parts.append(pd.read_csv(OUT_DIR / f"atlanta_assignment_result_{suffix}.csv", low_memory=False))
        summary_parts.append(pd.read_csv(OUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv", low_memory=False))
        schedule_parts.append(pd.read_csv(OUT_DIR / f"atlanta_schedule_{suffix}.csv", low_memory=False))

    assignment_df = pd.concat(assignment_parts, ignore_index=True)
    engineer_day_summary_df = pd.concat(summary_parts, ignore_index=True)
    schedule_df = pd.concat(schedule_parts, ignore_index=True)

    assignment_df.to_csv(OUT_DIR / "atlanta_assignment_result_osrm.csv", index=False, encoding="utf-8-sig")
    engineer_day_summary_df.to_csv(OUT_DIR / "atlanta_engineer_day_summary_osrm.csv", index=False, encoding="utf-8-sig")
    schedule_df.to_csv(OUT_DIR / "atlanta_schedule_osrm.csv", index=False, encoding="utf-8-sig")

    line_assignment_df = pd.read_csv(OUT_DIR / "atlanta_assignment_result.csv", low_memory=False)
    line_summary_df = pd.read_csv(OUT_DIR / "atlanta_engineer_day_summary.csv", low_memory=False)

    def daily_metrics(assignment_df: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
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
            weighted_df = group.copy()
            weighted_df["weighted_job_unit"] = weighted_df["is_heavy_repair"].fillna(False).astype(bool).map(lambda flag: 2.0 if flag else 1.0)
            weighted_jobs = weighted_df.groupby(weighted_df["assigned_sm_code"].astype(str))["weighted_job_unit"].sum()
            weighted_jobs = weighted_jobs[weighted_jobs > 0]
            weighted_std_rows.append(
                {
                    "service_date_key": str(service_date_key),
                    "weighted_jobs_std": float(weighted_jobs.std(ddof=0)) if not weighted_jobs.empty else 0.0,
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

    line_daily = daily_metrics(line_assignment_df, line_summary_df)
    osrm_daily = daily_metrics(assignment_df, engineer_day_summary_df)
    daily_compare_df = line_daily.merge(osrm_daily, on="service_date_key", how="outer", suffixes=("_line", "_osrm"))
    rename_map = {}
    for col in daily_compare_df.columns:
        if col.endswith("_line"):
            rename_map[col] = f"line_{col[:-5]}"
        elif col.endswith("_osrm"):
            rename_map[col] = f"osrm_{col[:-5]}"
    daily_compare_df = daily_compare_df.rename(columns=rename_map).sort_values("service_date_key").reset_index(drop=True)
    daily_compare_df.to_csv(OUT_DIR / "atlanta_daily_compare_line_vs_osrm.csv", index=False, encoding="utf-8-sig")

    print("merged_osrm_outputs_ready")


if __name__ == "__main__":
    main()
