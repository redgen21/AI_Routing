from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .region_sweep import _assign_city_regions, _build_clients
from .routing_compare import (
    DEFAULT_EFFECTIVE_SERVICE_PER_SM,
    _build_current_routes,
    _build_daily_summary,
    _build_integrated_routes,
    _load_service_df,
)

OUTPUT_DIR = Path("260310/output")
DEFAULT_SERVICE_FILE = Path("260310/input/Service_202603181109_geocoded.csv")


@dataclass
class DailyStatsExportResult:
    output_path: Path
    workbook_sheets: list[str]


def _load_config(config_file: Path) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def _build_daily_max_sm_df(route_df: pd.DataFrame) -> pd.DataFrame:
    if route_df.empty:
        return pd.DataFrame(columns=["service_date", "max_total_work_sm_code", "total_work_min", "job_count", "distance_km", "duration_min"])
    work_df = route_df.copy()
    work_df["total_work_min"] = work_df["duration_min"] + (work_df["job_count"] * 60.0)
    idx = work_df.groupby("service_date")["total_work_min"].idxmax()
    max_df = work_df.loc[idx, ["service_date", "assignment_unit_id", "total_work_min", "job_count", "distance_km", "duration_min"]].copy()
    max_df = max_df.rename(columns={"assignment_unit_id": "max_total_work_sm_code"})
    return max_df.sort_values("service_date").reset_index(drop=True)


def _write_sheet(writer: pd.ExcelWriter, sheet_name: str, daily_df: pd.DataFrame, max_df: pd.DataFrame) -> None:
    daily_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0)
    startrow = len(daily_df) + 3
    pd.DataFrame([{"service_date": "날짜별 최대 총업무 SM"}]).to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        header=False,
        startrow=startrow,
    )
    max_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=startrow + 1)


def _build_city_overall_row(
    daily_df: pd.DataFrame,
    route_df: pd.DataFrame,
    max_df: pd.DataFrame,
    region_type_label: str,
    region_count: int | None,
) -> dict[str, object]:
    if daily_df.empty or route_df.empty:
        return {"region_type": region_type_label, "region_count": region_count if region_count is not None else "기존"}

    route_work_df = route_df.copy()
    route_work_df["total_work_min"] = route_work_df["duration_min"] + (route_work_df["job_count"] * 60.0)
    return {
        "region_type": region_type_label,
        "region_count": region_count if region_count is not None else "기존",
        "service_day_count": int(daily_df["service_date"].nunique()),
        "avg_daily_service_count": round(float(daily_df["service_count"].mean()), 2),
        "avg_daily_deployed_sm": round(float(daily_df["deployed_sm_count"].mean()), 2),
        "avg_jobs_per_sm": round(float(daily_df["jobs_per_sm_avg"].mean()), 2),
        "avg_jobs_per_sm_std": round(float(daily_df["jobs_per_sm_std"].mean()), 2),
        "avg_distance_per_sm_km": round(float(daily_df["distance_per_sm_km"].mean()), 2),
        "avg_duration_per_sm_min": round(float(daily_df["duration_per_sm_min"].mean()), 2),
        "p95_total_work_min": round(float(route_work_df["total_work_min"].quantile(0.95)), 2),
        "max_total_work_min": round(float(route_work_df["total_work_min"].max()), 2),
        "overflow_480_ratio": round(float((route_work_df["total_work_min"] > 480.0).mean() * 100.0), 2),
        "max_sm_code_worst_day": str(max_df.sort_values("total_work_min", ascending=False)["max_total_work_sm_code"].iloc[0]) if not max_df.empty else "",
        "worst_day_total_work_min": round(float(max_df["total_work_min"].max()), 2) if not max_df.empty else 0.0,
    }


def export_daily_stats_workbook(
    service_file: Path = DEFAULT_SERVICE_FILE,
    config_file: Path = Path("config.json"),
    output_dir: Path = OUTPUT_DIR,
    city_candidates: dict[str, list[int]] | None = None,
) -> DailyStatsExportResult:
    if city_candidates is None:
        city_candidates = {
            "Atlanta, GA": [2, 3, 4, 5],
            "Los Angeles, CA": [3, 4, 5, 6],
        }

    cfg = _load_config(config_file)
    routing_cfg = cfg.get("routing", {})
    effective_service_per_sm = float(routing_cfg.get("effective_service_per_sm", DEFAULT_EFFECTIVE_SERVICE_PER_SM))
    assignment_distance_backend = str(routing_cfg.get("assignment_distance_backend", "haversine")).strip().lower()
    service_time_per_job_min = float(routing_cfg.get("service_time_per_job_min", 60.0))
    max_work_min_per_sm_day = float(routing_cfg.get("max_work_min_per_sm_day", 480.0))
    max_travel_min_per_sm_day = routing_cfg.get("max_travel_min_per_sm_day")
    max_travel_km_per_sm_day = routing_cfg.get("max_travel_km_per_sm_day")
    max_travel_min_per_sm_day = float(max_travel_min_per_sm_day) if max_travel_min_per_sm_day not in (None, "", 0) else None
    max_travel_km_per_sm_day = float(max_travel_km_per_sm_day) if max_travel_km_per_sm_day not in (None, "", 0) else None

    client_map, default_client = _build_clients(routing_cfg)
    service_df = _load_service_df(service_file)
    service_df = service_df[service_df["STRATEGIC_CITY_NAME"].isin(set(city_candidates.keys()))].copy()
    current_routes_all = _build_current_routes(service_df, client_map, default_client)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"daily_stats_by_city_region_{service_file.stem}.xlsx"
    written_sheets: list[str] = []

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for city_name, candidate_counts in city_candidates.items():
            service_city_df = service_df[service_df["STRATEGIC_CITY_NAME"] == city_name].copy()
            current_city_routes = current_routes_all[current_routes_all["STRATEGIC_CITY_NAME"] == city_name].copy()
            current_daily_df = _build_daily_summary(current_city_routes)
            current_daily_df = current_daily_df[current_daily_df["scenario"] == "current"].copy()
            current_daily_df["total_work_min"] = current_daily_df["duration_min"] + (current_daily_df["service_count"] * 60.0)
            current_daily_df["sheet_region_type"] = "기존지역"
            current_sheet = f"{city_name[:12]}_기존"
            current_max_df = _build_daily_max_sm_df(current_city_routes)
            _write_sheet(writer, current_sheet, current_daily_df, current_max_df)
            written_sheets.append(current_sheet)

            city_summary_rows: list[dict] = [
                _build_city_overall_row(current_daily_df, current_city_routes, current_max_df, "기존지역", None)
            ]

            for region_count in candidate_counts:
                region_service_df = _assign_city_regions(service_city_df, city_name, region_count)
                integrated_routes = _build_integrated_routes(
                    region_service_df=region_service_df,
                    client_map=client_map,
                    default_client=default_client,
                    effective_service_per_sm=effective_service_per_sm,
                    service_time_per_job_min=service_time_per_job_min,
                    max_work_min_per_sm_day=max_work_min_per_sm_day,
                    max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                    max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                    assignment_distance_backend=assignment_distance_backend,
                )
                integrated_daily_df = _build_daily_summary(integrated_routes)
                integrated_daily_df = integrated_daily_df[integrated_daily_df["scenario"] == "integrated"].copy()
                integrated_daily_df["total_work_min"] = integrated_daily_df["duration_min"] + (
                    integrated_daily_df["service_count"] * 60.0
                )
                integrated_daily_df["sheet_region_type"] = f"신규지역{region_count}"
                integrated_max_df = _build_daily_max_sm_df(integrated_routes)
                region_sheet = f"{city_name[:8]}_신규{region_count}"
                _write_sheet(writer, region_sheet, integrated_daily_df, integrated_max_df)
                written_sheets.append(region_sheet)
                city_summary_rows.append(
                    _build_city_overall_row(
                        integrated_daily_df,
                        integrated_routes,
                        integrated_max_df,
                        f"신규지역{region_count}",
                        region_count,
                    )
                )

            city_summary_df = pd.DataFrame(city_summary_rows)
            summary_sheet = f"{city_name[:10]}_전체통계"
            city_summary_df.to_excel(writer, sheet_name=summary_sheet, index=False)
            written_sheets.append(summary_sheet)

    return DailyStatsExportResult(output_path=output_path, workbook_sheets=written_sheets)
