from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .data_catalog import na_data_path
from .osrm_routing import OSRMConfig, OSRMTripClient
from .region_design import _build_postal_stats, _weighted_kmeans
from .routing_compare import (
    evaluate_region_plan,
    prepare_region_plan_evaluation,
)

OUTPUT_DIR = na_data_path("reports_dir")
DEFAULT_SERVICE_FILE = na_data_path("service_geocoded")
FIXED_REGION_DIR = na_data_path("reviewed_regions_dir")


@dataclass
class RegionSweepResult:
    summary_path: Path
    detail_path: Path
    summary_df: pd.DataFrame
    detail_df: pd.DataFrame


def _load_config(config_file: Path) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def _build_clients(routing_cfg: dict) -> tuple[dict[str, OSRMTripClient], OSRMTripClient]:
    """Compatibility helper retained for the daily-statistics exporter."""

    distance_backend = str(routing_cfg.get("distance_backend", "osrm")).strip().lower()
    default_client = OSRMTripClient(
        OSRMConfig(
            osrm_url=str(routing_cfg.get("osrm_url", "https://router.project-osrm.org")).rstrip("/"),
            mode="haversine" if distance_backend == "city_osrm_else_haversine" else distance_backend,
            osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
            cache_file=Path(str(routing_cfg.get("osrm_cache_file", "data/cache/osrm_trip_cache.csv"))),
        )
    )
    client_map: dict[str, OSRMTripClient] = {}
    for city_name, city_url in routing_cfg.get("city_osrm_urls", {}).items():
        cache_name = city_name.lower().replace(",", "").replace(" ", "_")
        client_map[str(city_name)] = OSRMTripClient(
            OSRMConfig(
                osrm_url=str(city_url).rstrip("/"),
                mode="osrm" if distance_backend == "city_osrm_else_haversine" else distance_backend,
                osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
                cache_file=Path(f"data/cache/osrm_trip_cache_{cache_name}.csv"),
                fallback_osrm_url=(
                    None
                    if distance_backend == "city_osrm_else_haversine"
                    else str(routing_cfg.get("osrm_url", "https://router.project-osrm.org")).rstrip("/")
                ),
            )
        )
    return client_map, default_client


def _assign_city_regions(service_city_df: pd.DataFrame, city_name: str, region_count: int) -> pd.DataFrame:
    fixed_region_df = _load_fixed_region_map(city_name, region_count)
    if not fixed_region_df.empty:
        return service_city_df.merge(
            fixed_region_df[["STRATEGIC_CITY_NAME", "POSTAL_CODE", "region_id", "region_seq"]],
            on=["STRATEGIC_CITY_NAME", "POSTAL_CODE"],
            how="left",
        )

    postal_df = _build_postal_stats(service_city_df)
    postal_city_df = postal_df[postal_df["STRATEGIC_CITY_NAME"] == city_name].copy()
    coords = postal_city_df[["latitude", "longitude"]].to_numpy(dtype=float)
    weights = postal_city_df["service_count"].to_numpy(dtype=float)
    labels = _weighted_kmeans(coords, weights, int(region_count))
    postal_city_df["region_seq"] = labels + 1
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in city_name).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    postal_city_df["region_id"] = postal_city_df["region_seq"].apply(lambda n: f"{slug}_r{int(n):02d}")

    merged = service_city_df.merge(
        postal_city_df[["STRATEGIC_CITY_NAME", "POSTAL_CODE", "region_id", "region_seq"]],
        on=["STRATEGIC_CITY_NAME", "POSTAL_CODE"],
        how="left",
    )
    return merged


def _slugify_city_name(city_name: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(city_name))
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")


def fixed_region_map_path(city_name: str, region_count: int, fixed_region_dir: Path = FIXED_REGION_DIR) -> Path:
    return fixed_region_dir / f"fixed_region_postal_{_slugify_city_name(city_name)}_{int(region_count)}.csv"


def _load_fixed_region_map(city_name: str, region_count: int) -> pd.DataFrame:
    path = fixed_region_map_path(city_name, region_count)
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    required = {"STRATEGIC_CITY_NAME", "POSTAL_CODE", "region_id", "region_seq"}
    if not required.issubset(df.columns):
        return pd.DataFrame()
    df = df[df["STRATEGIC_CITY_NAME"].astype(str).str.strip().eq(str(city_name).strip())].copy()
    df["POSTAL_CODE"] = df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    df["region_seq"] = pd.to_numeric(df["region_seq"], errors="coerce")
    df = df[df["region_seq"].notna()].copy()
    return df.drop_duplicates(subset=["STRATEGIC_CITY_NAME", "POSTAL_CODE"], keep="first").reset_index(drop=True)


def _extract_candidate_summary(city_summary_df: pd.DataFrame) -> dict[str, float]:
    current_row = city_summary_df[city_summary_df["scenario"] == "current"].iloc[0]
    integrated_row = city_summary_df[city_summary_df["scenario"] == "integrated"].iloc[0]
    metrics = {
        "avg_daily_deployed_sm_current": float(current_row["avg_daily_deployed_sm"]),
        "avg_daily_deployed_sm_integrated": float(integrated_row["avg_daily_deployed_sm"]),
        "avg_jobs_per_sm_current": float(current_row["avg_jobs_per_sm"]),
        "avg_jobs_per_sm_integrated": float(integrated_row["avg_jobs_per_sm"]),
        "avg_jobs_per_sm_std_current": float(current_row["avg_jobs_per_sm_std"]),
        "avg_jobs_per_sm_std_integrated": float(integrated_row["avg_jobs_per_sm_std"]),
        "avg_distance_per_sm_km_current": float(current_row["avg_distance_per_sm_km"]),
        "avg_distance_per_sm_km_integrated": float(integrated_row["avg_distance_per_sm_km"]),
        "avg_duration_per_sm_min_current": float(current_row["avg_duration_per_sm_min"]),
        "avg_duration_per_sm_min_integrated": float(integrated_row["avg_duration_per_sm_min"]),
    }
    for key in [
        "avg_daily_deployed_sm",
        "avg_jobs_per_sm",
        "avg_jobs_per_sm_std",
        "avg_distance_per_sm_km",
        "avg_duration_per_sm_min",
    ]:
        cur = metrics[f"{key}_current"]
        nxt = metrics[f"{key}_integrated"]
        delta = nxt - cur
        pct = (delta / cur * 100.0) if cur else np.nan
        metrics[f"{key}_delta"] = round(delta, 2)
        metrics[f"{key}_delta_pct"] = round(pct, 2)
    score = (
        max(metrics["avg_distance_per_sm_km_delta_pct"], 0.0) * 0.35
        + max(metrics["avg_duration_per_sm_min_delta_pct"], 0.0) * 0.35
        + max(metrics["avg_jobs_per_sm_std_delta_pct"], 0.0) * 0.20
        + max(metrics["avg_jobs_per_sm_delta_pct"], 0.0) * 0.10
        - abs(min(metrics["avg_daily_deployed_sm_delta_pct"], 0.0)) * 0.20
    )
    metrics["balance_score"] = round(score, 2)
    return metrics


def _extract_outlier_metrics(route_df: pd.DataFrame) -> dict[str, float]:
    work_df = route_df.copy()
    work_df["total_work_min"] = work_df["duration_min"] + (work_df["job_count"] * 60.0)
    rows: dict[str, float] = {}
    for scenario in ["current", "integrated"]:
        sdf = work_df[work_df["scenario"] == scenario].copy()
        if sdf.empty:
            continue
        rows[f"p95_total_work_min_{scenario}"] = round(float(sdf["total_work_min"].quantile(0.95)), 2)
        rows[f"max_total_work_min_{scenario}"] = round(float(sdf["total_work_min"].max()), 2)
        rows[f"overflow_480_count_{scenario}"] = int((sdf["total_work_min"] > 480.0).sum())
        rows[f"overflow_480_ratio_{scenario}"] = round(float((sdf["total_work_min"] > 480.0).mean() * 100.0), 2)
        rows[f"p95_job_count_{scenario}"] = round(float(sdf["job_count"].quantile(0.95)), 2)
        rows[f"max_job_count_{scenario}"] = round(float(sdf["job_count"].max()), 2)
        rows[f"p95_duration_min_{scenario}"] = round(float(sdf["duration_min"].quantile(0.95)), 2)
        rows[f"max_duration_min_{scenario}"] = round(float(sdf["duration_min"].max()), 2)
    for metric in ["p95_total_work_min", "max_total_work_min", "overflow_480_ratio", "p95_job_count", "max_job_count", "p95_duration_min", "max_duration_min"]:
        cur = float(rows.get(f"{metric}_current", 0.0))
        nxt = float(rows.get(f"{metric}_integrated", 0.0))
        delta = round(nxt - cur, 2)
        rows[f"{metric}_delta"] = delta
        rows[f"{metric}_delta_pct"] = round((delta / cur * 100.0), 2) if cur else np.nan
    rows["overflow_480_count_delta"] = int(rows.get("overflow_480_count_integrated", 0)) - int(rows.get("overflow_480_count_current", 0))
    return rows


def sweep_region_counts(
    service_file: Path = DEFAULT_SERVICE_FILE,
    config_file: Path = Path("config/config.json"),
    output_dir: Path = OUTPUT_DIR,
    city_candidates: dict[str, list[int]] | None = None,
) -> RegionSweepResult:
    if city_candidates is None:
        city_candidates = {
            "Atlanta, GA": [2, 3, 4, 5],
            "Los Angeles, CA": [3, 4, 5, 6],
        }

    cfg = _load_config(config_file)
    routing_cfg = cfg.get("routing", {})
    target_cities = set(city_candidates.keys())
    evaluation_context = prepare_region_plan_evaluation(
        service_file=service_file,
        routing_config=routing_cfg,
        cities=sorted(target_cities),
    )
    service_df = evaluation_context.service_df

    summary_rows: list[dict] = []
    detail_frames: list[pd.DataFrame] = []

    for city_name, candidate_counts in city_candidates.items():
        service_city_df = service_df[service_df["STRATEGIC_CITY_NAME"] == city_name].copy()

        for region_count in candidate_counts:
            region_service_df = _assign_city_regions(service_city_df, city_name, region_count)
            evaluation = evaluate_region_plan(evaluation_context, region_service_df)
            route_df = evaluation.route_detail_df
            city_summary_df = evaluation.city_summary_df
            metrics = _extract_candidate_summary(city_summary_df)
            metrics.update(_extract_outlier_metrics(route_df))
            metrics["balance_score"] = round(metrics["balance_score"] + max(metrics["overflow_480_ratio_delta"], 0.0) * 0.50, 2)
            summary_rows.append(
                {
                    "STRATEGIC_CITY_NAME": city_name,
                    "candidate_region_count": int(region_count),
                    **metrics,
                }
            )

            city_summary_df = city_summary_df.copy()
            city_summary_df["candidate_region_count"] = int(region_count)
            detail_frames.append(city_summary_df)

    summary_df = pd.DataFrame(summary_rows).sort_values(["STRATEGIC_CITY_NAME", "balance_score", "candidate_region_count"]).reset_index(drop=True)
    best_rows = summary_df.groupby("STRATEGIC_CITY_NAME", as_index=False).first()
    best_rows["is_best_candidate"] = True
    summary_df = summary_df.merge(
        best_rows[["STRATEGIC_CITY_NAME", "candidate_region_count", "is_best_candidate"]],
        on=["STRATEGIC_CITY_NAME", "candidate_region_count"],
        how="left",
    )
    summary_df["is_best_candidate"] = summary_df["is_best_candidate"].astype("boolean").fillna(False).astype(bool)

    detail_df = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()

    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / f"region_count_sweep_summary_{service_file.stem}.csv"
    detail_path = output_dir / f"region_count_sweep_detail_{service_file.stem}.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig")

    return RegionSweepResult(
        summary_path=summary_path,
        detail_path=detail_path,
        summary_df=summary_df,
        detail_df=detail_df,
    )
