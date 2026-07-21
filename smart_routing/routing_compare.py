from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from .data_catalog import na_data_path
from .osrm_routing import OSRMConfig, OSRMTripClient
from .region_design import (
    DEFAULT_BALANCE_WEIGHT,
    DEFAULT_EFFECTIVE_SERVICE_PER_SM,
    DEFAULT_RADIUS_WEIGHT,
    _rebalance_weighted_regions,
)

INPUT_DIR = na_data_path("region_candidates_dir")
OUTPUT_DIR = na_data_path("reports_dir")
DEFAULT_SERVICE_FILE = na_data_path("service_geocoded")
ROUTE_CITY_ALIASES = {
    "North Jersey, NJ": "Northeast",
    "Philadelphia, PA": "Northeast",
}


def _route_client_key(city_name: object) -> str:
    city_text = str(city_name).strip()
    return ROUTE_CITY_ALIASES.get(city_text, city_text)


def _haversine_km_pair(a: tuple[float, float], b: tuple[float, float]) -> float:
    lon1, lat1 = a
    lon2, lat2 = b
    rad = math.pi / 180.0
    dlat = (lat2 - lat1) * rad
    dlon = (lon2 - lon1) * rad
    aa = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(dlon / 2.0) ** 2
    )
    return 6371.0 * (2.0 * math.asin(math.sqrt(aa)))


def _haversine_matrices(coords: list[tuple[float, float]]) -> tuple[list[list[float]], list[list[float]]]:
    dist: list[list[float]] = []
    dur: list[list[float]] = []
    for src in coords:
        dist_row: list[float] = []
        dur_row: list[float] = []
        for dst in coords:
            km = _haversine_km_pair(src, dst)
            dist_row.append(km)
            dur_row.append((km / 50.0) * 60.0)
        dist.append(dist_row)
        dur.append(dur_row)
    return dist, dur


@dataclass
class RoutingCompareResult:
    route_detail_path: Path
    daily_summary_path: Path
    city_summary_path: Path
    overall_summary_path: Path
    route_detail_df: pd.DataFrame
    daily_summary_df: pd.DataFrame
    city_summary_df: pd.DataFrame
    overall_summary_df: pd.DataFrame


@dataclass
class RegionPlanEvaluationContext:
    """Reusable routing baseline and policy for scoring region candidates."""

    service_df: pd.DataFrame
    current_route_df: pd.DataFrame
    routing_config: dict
    client_map: dict[str, OSRMTripClient]
    default_client: OSRMTripClient


@dataclass
class RegionPlanEvaluationResult:
    """Quantitative routing outputs for one versioned region-plan candidate."""

    route_detail_df: pd.DataFrame
    daily_summary_df: pd.DataFrame
    city_summary_df: pd.DataFrame


def _load_config(config_file: Path) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def _build_routing_clients(routing_cfg: dict) -> tuple[dict[str, OSRMTripClient], OSRMTripClient]:
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


def _infer_region_service_file(service_file: Path, explicit_file: Path | None) -> Path:
    if explicit_file is not None:
        return explicit_file
    return service_file.parent / f"region_design_service_{service_file.stem}.csv"


def _load_service_df(service_file: Path) -> pd.DataFrame:
    df = pd.read_csv(service_file, encoding="utf-8-sig", low_memory=False)
    keep_cols = [
        "GSFS_RECEIPT_NO",
        "STRATEGIC_CITY_NAME",
        "SVC_ENGINEER_CODE",
        "SVC_ENGINEER_NAME",
        "SVC_CENTER_TYPE",
        "POSTAL_CODE",
        "latitude",
        "longitude",
        "REPAIR_END_DATE_YYYYMMDD",
        "source",
    ]
    df = df[[c for c in keep_cols if c in df.columns]].copy()
    for col in ["GSFS_RECEIPT_NO", "STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "SVC_CENTER_TYPE", "POSTAL_CODE"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    for col in ["latitude", "longitude"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "source" in df.columns:
        df = df[df["source"].astype(str).str.strip().ne("failed")].copy()
    df = df[df["latitude"].notna() & df["longitude"].notna()].copy()
    df["service_date"] = pd.to_datetime(df["REPAIR_END_DATE_YYYYMMDD"].astype(str), format="%Y%m%d", errors="coerce")
    df = df[df["service_date"].notna()].copy()
    return df


def _load_region_service_df(region_service_file: Path) -> pd.DataFrame:
    df = pd.read_csv(region_service_file, encoding="utf-8-sig", low_memory=False)
    for col in ["GSFS_RECEIPT_NO", "STRATEGIC_CITY_NAME", "region_id"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    for col in ["latitude", "longitude"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["service_date"] = pd.to_datetime(df["REPAIR_END_DATE_YYYYMMDD"].astype(str), format="%Y%m%d", errors="coerce")
    df = df[df["service_date"].notna()].copy()
    if "source" in df.columns:
        df = df[df["source"].astype(str).str.strip().ne("failed")].copy()
    df = df[df["latitude"].notna() & df["longitude"].notna()].copy()
    return df


def _dedupe_stops(group_df: pd.DataFrame) -> list[tuple[float, float]]:
    stops = (
        group_df[["longitude", "latitude"]]
        .dropna()
        .drop_duplicates()
        .apply(lambda r: (float(r["longitude"]), float(r["latitude"])), axis=1)
        .tolist()
    )
    return stops


def _estimate_group_route(group_df: pd.DataFrame, client: OSRMTripClient) -> dict:
    coords = _dedupe_stops(group_df)
    route_payload = client.build_ordered_route(coords)
    return {
        "job_count": int(len(group_df)),
        "unique_stop_count": int(len(coords)),
        "distance_km": round(float(route_payload["distance_km"]), 3),
        "duration_min": round(float(route_payload["duration_min"]), 2),
    }


def _assignment_route_client(client: OSRMTripClient, assignment_distance_backend: str) -> OSRMTripClient:
    backend = str(assignment_distance_backend or "haversine").strip().lower()
    if backend == "osrm":
        return client
    return OSRMTripClient(
        OSRMConfig(
            osrm_url=client.cfg.osrm_url,
            mode="haversine",
            osrm_profile=client.cfg.osrm_profile,
            cache_file=client.cfg.cache_file,
        )
    )


def _get_client_for_city(city_name: str, client_map: dict[str, OSRMTripClient], default_client: OSRMTripClient) -> OSRMTripClient:
    return client_map.get(_route_client_key(city_name), default_client)


def _build_current_routes(service_df: pd.DataFrame, client_map: dict[str, OSRMTripClient], default_client: OSRMTripClient) -> pd.DataFrame:
    grouped = list(service_df.groupby(["STRATEGIC_CITY_NAME", "service_date", "SVC_ENGINEER_CODE"], sort=True))

    def _calc(item: tuple[tuple[str, pd.Timestamp, str], pd.DataFrame]) -> dict:
        (city_name, service_date, engineer_code), group_df = item
        client = _get_client_for_city(city_name, client_map, default_client)
        metrics = _estimate_group_route(group_df, client)
        return {
            "scenario": "current",
            "STRATEGIC_CITY_NAME": city_name,
            "service_date": service_date,
            "assignment_unit_id": engineer_code,
            "job_count": metrics["job_count"],
            "unique_stop_count": metrics["unique_stop_count"],
            "distance_km": metrics["distance_km"],
            "duration_min": metrics["duration_min"],
        }

    worker_count = min(16, max(4, (os.cpu_count() or 8)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        rows = list(executor.map(_calc, grouped))
    return pd.DataFrame(rows)


def _build_region_day_cluster_labels(group_df: pd.DataFrame, sm_count: int) -> pd.Series:
    if group_df.empty:
        return pd.Series(dtype=int)
    cluster_count = max(1, min(int(sm_count), len(group_df)))
    if cluster_count == 1:
        return pd.Series(np.zeros(len(group_df), dtype=int), index=group_df.index)

    coords = group_df[["latitude", "longitude"]].to_numpy(dtype=float)
    weights = np.ones(len(group_df), dtype=float)
    labels = _rebalance_weighted_regions(
        points=coords,
        weights=weights,
        cluster_count=cluster_count,
        target_service=float(len(group_df)) / cluster_count,
        balance_weight=DEFAULT_BALANCE_WEIGHT,
        radius_weight=DEFAULT_RADIUS_WEIGHT,
    )
    return pd.Series(labels, index=group_df.index, dtype=int)


def _batch_assign_region_day_jobs(
    group_df: pd.DataFrame,
    client: OSRMTripClient,
    effective_service_per_sm: float,
    service_time_per_job_min: float,
    max_work_min_per_sm_day: float,
    max_travel_min_per_sm_day: float | None,
    max_travel_km_per_sm_day: float | None,
    assignment_distance_backend: str,
) -> pd.Series:
    if group_df.empty:
        return pd.Series(dtype=int)
    route_client = _assignment_route_client(client, assignment_distance_backend)
    min_sm_count = max(1, math.ceil(len(group_df) / max(effective_service_per_sm, 1.0)))
    max_sm_count = max(1, len(group_df))
    accepted_labels: pd.Series | None = None

    for sm_count in range(min_sm_count, max_sm_count + 1):
        labels = _build_region_day_cluster_labels(group_df, sm_count)
        is_valid = True
        for _, cluster_df in group_df.groupby(labels, sort=True):
            metrics = _estimate_group_route(cluster_df, route_client)
            total_work_min = float(metrics["duration_min"]) + float(metrics["job_count"]) * service_time_per_job_min
            if total_work_min > max_work_min_per_sm_day:
                is_valid = False
                break
            if max_travel_min_per_sm_day is not None and float(metrics["duration_min"]) > max_travel_min_per_sm_day:
                is_valid = False
                break
            if max_travel_km_per_sm_day is not None and float(metrics["distance_km"]) > max_travel_km_per_sm_day:
                is_valid = False
                break
        accepted_labels = labels
        if is_valid:
            break

    if accepted_labels is None:
        accepted_labels = _build_region_day_cluster_labels(group_df, min_sm_count)
    return _reassign_single_job_clusters(
        group_df=group_df,
        labels=accepted_labels.astype(int),
        client=route_client,
        service_time_per_job_min=service_time_per_job_min,
        max_work_min_per_sm_day=max_work_min_per_sm_day,
        candidate_job_cap=4,
    )


def _reassign_single_job_clusters(
    group_df: pd.DataFrame,
    labels: pd.Series,
    client: OSRMTripClient,
    service_time_per_job_min: float,
    max_work_min_per_sm_day: float,
    candidate_job_cap: int = 3,
) -> pd.Series:
    if group_df.empty:
        return labels.astype(int)

    adjusted = labels.astype(int).copy()

    while True:
        changed = False
        cluster_counts = (
            group_df.groupby(adjusted)["GSFS_RECEIPT_NO"]
            .apply(lambda s: s.dropna().astype(str).nunique())
            .to_dict()
        )
        singleton_clusters = [int(cluster_id) for cluster_id, count in cluster_counts.items() if int(count) <= 1]
        if not singleton_clusters:
            break

        for singleton_cluster in singleton_clusters:
            singleton_df = group_df[adjusted == singleton_cluster].copy()
            if singleton_df.empty:
                continue
            src_coord = _dedupe_stops(singleton_df)
            if not src_coord:
                continue
            src_coord = src_coord[0]

            candidate_rows: list[tuple[float, float, float, int]] = []
            for candidate_cluster, job_count in cluster_counts.items():
                candidate_cluster = int(candidate_cluster)
                if candidate_cluster == singleton_cluster or int(job_count) > candidate_job_cap:
                    continue
                candidate_df = group_df[adjusted == candidate_cluster].copy()
                candidate_coords = _dedupe_stops(candidate_df)
                if not candidate_coords:
                    continue
                merged_df = group_df[(adjusted == singleton_cluster) | (adjusted == candidate_cluster)].copy()
                metrics = _estimate_group_route(merged_df, client)
                total_work_min = float(metrics["duration_min"]) + float(metrics["job_count"]) * service_time_per_job_min
                if total_work_min > max_work_min_per_sm_day:
                    continue
                min_distance = min(_haversine_km_pair(src_coord, dst_coord) for dst_coord in candidate_coords)
                candidate_rows.append(
                    (
                        float(metrics["distance_km"]),
                        float(total_work_min),
                        float(min_distance),
                        candidate_cluster,
                    )
                )

            if not candidate_rows:
                continue

            candidate_rows.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
            _, _, _, best_candidate_cluster = candidate_rows[0]
            adjusted.loc[adjusted == singleton_cluster] = int(best_candidate_cluster)
            changed = True

        if not changed:
            break

    unique_labels = {old_label: new_label for new_label, old_label in enumerate(sorted(adjusted.unique()))}
    return adjusted.map(unique_labels).astype(int)


def _build_integrated_routes(
    region_service_df: pd.DataFrame,
    client_map: dict[str, OSRMTripClient],
    default_client: OSRMTripClient,
    effective_service_per_sm: float,
    service_time_per_job_min: float,
    max_work_min_per_sm_day: float,
    max_travel_min_per_sm_day: float | None,
    max_travel_km_per_sm_day: float | None,
    assignment_distance_backend: str,
) -> pd.DataFrame:
    work_df = region_service_df.copy()
    work_df["cluster_seq"] = -1
    grouped = work_df.groupby(["STRATEGIC_CITY_NAME", "service_date", "region_id"], sort=True)
    for (city_name, _, _), idx_df in grouped:
        client = _get_client_for_city(city_name, client_map, default_client)
        work_df.loc[idx_df.index, "cluster_seq"] = _batch_assign_region_day_jobs(
            idx_df,
            client=client,
            effective_service_per_sm=effective_service_per_sm,
            service_time_per_job_min=service_time_per_job_min,
            max_work_min_per_sm_day=max_work_min_per_sm_day,
            max_travel_min_per_sm_day=max_travel_min_per_sm_day,
            max_travel_km_per_sm_day=max_travel_km_per_sm_day,
            assignment_distance_backend=assignment_distance_backend,
        )

    route_grouped = list(work_df.groupby(["STRATEGIC_CITY_NAME", "service_date", "region_id", "cluster_seq"], sort=True))

    def _calc(item: tuple[tuple[str, pd.Timestamp, str, int], pd.DataFrame]) -> dict:
        (city_name, service_date, region_id, cluster_seq), group_df = item
        client = _get_client_for_city(city_name, client_map, default_client)
        metrics = _estimate_group_route(group_df, client)
        return {
            "scenario": "integrated",
            "STRATEGIC_CITY_NAME": city_name,
            "service_date": service_date,
            "assignment_unit_id": f"{region_id}_sm{int(cluster_seq) + 1:02d}",
            "region_id": region_id,
            "job_count": metrics["job_count"],
            "unique_stop_count": metrics["unique_stop_count"],
            "distance_km": metrics["distance_km"],
            "duration_min": metrics["duration_min"],
        }

    worker_count = min(16, max(4, (os.cpu_count() or 8)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        rows = list(executor.map(_calc, route_grouped))
    return pd.DataFrame(rows)


def _build_daily_summary(route_df: pd.DataFrame) -> pd.DataFrame:
    base = (
        route_df.groupby(["scenario", "STRATEGIC_CITY_NAME", "service_date"])
        .agg(
            deployed_sm_count=("assignment_unit_id", "nunique"),
            service_count=("job_count", "sum"),
            distance_km=("distance_km", "sum"),
            duration_min=("duration_min", "sum"),
        )
        .reset_index()
    )
    load_stats = (
        route_df.groupby(["scenario", "STRATEGIC_CITY_NAME", "service_date"])
        .agg(
            jobs_per_sm_std=("job_count", lambda s: float(np.std(s.to_numpy(dtype=float), ddof=0))),
            jobs_per_sm_min=("job_count", "min"),
            jobs_per_sm_max=("job_count", "max"),
        )
        .reset_index()
    )
    daily_df = base.merge(load_stats, on=["scenario", "STRATEGIC_CITY_NAME", "service_date"], how="left")
    daily_df["jobs_per_sm_avg"] = (daily_df["service_count"] / daily_df["deployed_sm_count"].replace(0, 1)).round(2)
    daily_df["distance_per_sm_km"] = (daily_df["distance_km"] / daily_df["deployed_sm_count"].replace(0, 1)).round(2)
    daily_df["duration_per_sm_min"] = (daily_df["duration_min"] / daily_df["deployed_sm_count"].replace(0, 1)).round(2)
    return daily_df


def _build_city_summary(daily_df: pd.DataFrame) -> pd.DataFrame:
    city_df = (
        daily_df.groupby(["scenario", "STRATEGIC_CITY_NAME"])
        .agg(
            service_day_count=("service_date", "nunique"),
            avg_daily_service_count=("service_count", "mean"),
            avg_daily_distance_km=("distance_km", "mean"),
            avg_daily_duration_min=("duration_min", "mean"),
            avg_daily_deployed_sm=("deployed_sm_count", "mean"),
            avg_jobs_per_sm=("jobs_per_sm_avg", "mean"),
            avg_jobs_per_sm_std=("jobs_per_sm_std", "mean"),
            avg_distance_per_sm_km=("distance_per_sm_km", "mean"),
            avg_duration_per_sm_min=("duration_per_sm_min", "mean"),
        )
        .reset_index()
    )
    numeric_cols = [c for c in city_df.columns if c not in {"scenario", "STRATEGIC_CITY_NAME"}]
    city_df[numeric_cols] = city_df[numeric_cols].round(2)
    return city_df


def _build_overall_summary(city_df: pd.DataFrame) -> pd.DataFrame:
    current_df = city_df[city_df["scenario"] == "current"].drop(columns="scenario").copy()
    integrated_df = city_df[city_df["scenario"] == "integrated"].drop(columns="scenario").copy()
    merged = current_df.merge(integrated_df, on="STRATEGIC_CITY_NAME", suffixes=("_current", "_integrated"), how="outer")

    for metric in [
        "avg_daily_service_count",
        "avg_daily_distance_km",
        "avg_daily_duration_min",
        "avg_daily_deployed_sm",
        "avg_jobs_per_sm",
        "avg_jobs_per_sm_std",
        "avg_distance_per_sm_km",
        "avg_duration_per_sm_min",
    ]:
        merged[f"{metric}_delta"] = (merged[f"{metric}_integrated"] - merged[f"{metric}_current"]).round(2)
        base = merged[f"{metric}_current"].replace(0, np.nan)
        merged[f"{metric}_delta_pct"] = ((merged[f"{metric}_delta"] / base) * 100.0).round(2)

    overall_current = current_df.drop(columns="STRATEGIC_CITY_NAME").mean(numeric_only=True).to_dict()
    overall_integrated = integrated_df.drop(columns="STRATEGIC_CITY_NAME").mean(numeric_only=True).to_dict()
    overall_row = {"STRATEGIC_CITY_NAME": "ALL"}
    for key, value in overall_current.items():
        overall_row[f"{key}_current"] = round(float(value), 2)
    for key, value in overall_integrated.items():
        overall_row[f"{key}_integrated"] = round(float(value), 2)
    for metric in [
        "avg_daily_service_count",
        "avg_daily_distance_km",
        "avg_daily_duration_min",
        "avg_daily_deployed_sm",
        "avg_jobs_per_sm",
        "avg_jobs_per_sm_std",
        "avg_distance_per_sm_km",
        "avg_duration_per_sm_min",
    ]:
        cur = overall_row.get(f"{metric}_current", 0.0)
        nxt = overall_row.get(f"{metric}_integrated", 0.0)
        delta = round(float(nxt) - float(cur), 2)
        overall_row[f"{metric}_delta"] = delta
        overall_row[f"{metric}_delta_pct"] = round((delta / cur) * 100.0, 2) if cur else np.nan

    merged = pd.concat([merged, pd.DataFrame([overall_row])], ignore_index=True)
    return merged


def prepare_region_plan_evaluation(
    service_file: Path,
    routing_config: dict,
    cities: list[str] | None = None,
) -> RegionPlanEvaluationContext:
    """Load one routing baseline that can be reused across region candidates.

    ``routing_config`` is the contents of the configuration's ``routing``
    section. The returned context is opaque to region planners and keeps the
    current-assignment route calculation out of each candidate evaluation.
    """

    service_df = _load_service_df(service_file)
    if cities:
        allowed = {str(city).strip() for city in cities}
        service_df = service_df[service_df["STRATEGIC_CITY_NAME"].isin(allowed)].copy()
    client_map, default_client = _build_routing_clients(routing_config)
    current_route_df = _build_current_routes(service_df, client_map, default_client)
    return RegionPlanEvaluationContext(
        service_df=service_df,
        current_route_df=current_route_df,
        routing_config=dict(routing_config),
        client_map=client_map,
        default_client=default_client,
    )


def _validated_receipt_ids(service_df: pd.DataFrame, *, dataset_name: str, city_name: str) -> set[str]:
    raw_ids = service_df["GSFS_RECEIPT_NO"]
    normalized_ids = raw_ids.astype("string").str.strip()
    null_mask = normalized_ids.isna() | normalized_ids.eq("") | normalized_ids.str.lower().isin(
        {"nan", "none", "nat", "<na>", "null"}
    )
    if null_mask.any():
        raise ValueError(
            f"{dataset_name} for {city_name} contains {int(null_mask.sum())} null GSFS_RECEIPT_NO values"
        )

    duplicate_mask = normalized_ids.duplicated(keep=False)
    if duplicate_mask.any():
        duplicate_ids = sorted(normalized_ids[duplicate_mask].unique().tolist())
        duplicate_preview = ", ".join(duplicate_ids[:5])
        raise ValueError(
            f"{dataset_name} for {city_name} contains duplicate GSFS_RECEIPT_NO values: {duplicate_preview}"
        )
    return set(normalized_ids.tolist())


def evaluate_region_plan(
    context: RegionPlanEvaluationContext,
    region_service_df: pd.DataFrame,
) -> RegionPlanEvaluationResult:
    """Score one postal-to-region candidate with the routing pipeline.

    The candidate must contain a non-null ``region_id`` for every service row.
    Within each city, ``GSFS_RECEIPT_NO`` is the unique job identifier and the
    candidate must contain exactly the same identifier set as the baseline.
    Results preserve the existing route-detail, daily-summary, and city-summary
    schemas used by routing comparison and region-count sweep reports.
    """

    required_columns = {"GSFS_RECEIPT_NO", "STRATEGIC_CITY_NAME", "region_id"}
    missing_columns = sorted(required_columns.difference(region_service_df.columns))
    if missing_columns:
        raise ValueError(f"region plan is missing required columns: {', '.join(missing_columns)}")
    if region_service_df.empty:
        raise ValueError("region plan contains no service rows")
    candidate_region_values = region_service_df["region_id"].astype("string").str.strip()
    invalid_region_mask = (
        candidate_region_values.isna()
        | candidate_region_values.eq("")
        | candidate_region_values.str.lower().isin({"nan", "none", "nat", "<na>", "null"})
    )
    if invalid_region_mask.any():
        missing_count = int(invalid_region_mask.sum())
        raise ValueError(f"region plan leaves {missing_count} service rows without region_id")
    candidate_city_values = region_service_df["STRATEGIC_CITY_NAME"].astype("string").str.strip()
    invalid_city_mask = candidate_city_values.isna() | candidate_city_values.eq("")
    if invalid_city_mask.any():
        raise ValueError(f"region plan contains {int(invalid_city_mask.sum())} service rows without city")

    candidate_cities = {
        str(city).strip()
        for city in region_service_df["STRATEGIC_CITY_NAME"].dropna().unique()
        if str(city).strip()
    }
    baseline_required_columns = {
        "GSFS_RECEIPT_NO",
        "STRATEGIC_CITY_NAME",
        "service_date",
        "latitude",
        "longitude",
    }
    missing_baseline_columns = sorted(baseline_required_columns.difference(context.service_df.columns))
    if missing_baseline_columns:
        raise ValueError(f"routing baseline is missing required columns: {', '.join(missing_baseline_columns)}")

    baseline_cities = {
        str(city).strip()
        for city in context.service_df["STRATEGIC_CITY_NAME"].dropna().unique()
        if str(city).strip()
    }
    unknown_cities = sorted(candidate_cities.difference(baseline_cities))
    if unknown_cities:
        raise ValueError(f"region plan contains cities outside the routing baseline: {', '.join(unknown_cities)}")

    baseline_city_values = context.service_df["STRATEGIC_CITY_NAME"].astype("string").str.strip()
    for city_name in sorted(candidate_cities):
        baseline_city_df = context.service_df[baseline_city_values.eq(city_name).fillna(False)]
        candidate_city_df = region_service_df[candidate_city_values.eq(city_name).fillna(False)]
        baseline_receipt_ids = _validated_receipt_ids(
            baseline_city_df,
            dataset_name="routing baseline",
            city_name=city_name,
        )
        candidate_receipt_ids = _validated_receipt_ids(
            candidate_city_df,
            dataset_name="region plan",
            city_name=city_name,
        )
        missing_receipt_ids = sorted(baseline_receipt_ids.difference(candidate_receipt_ids))
        extra_receipt_ids = sorted(candidate_receipt_ids.difference(baseline_receipt_ids))
        if missing_receipt_ids or extra_receipt_ids:
            details: list[str] = []
            if missing_receipt_ids:
                details.append(
                    f"missing {len(missing_receipt_ids)} baseline jobs ({', '.join(missing_receipt_ids[:5])})"
                )
            if extra_receipt_ids:
                details.append(
                    f"contains {len(extra_receipt_ids)} extra jobs ({', '.join(extra_receipt_ids[:5])})"
                )
            raise ValueError(f"region plan job coverage mismatch for {city_name}: {'; '.join(details)}")

    baseline_routing_df = context.service_df[baseline_city_values.isin(candidate_cities).fillna(False)].copy()
    baseline_routing_df = baseline_routing_df.drop(columns=["region_id"], errors="ignore")
    baseline_routing_df["_evaluation_city"] = baseline_routing_df["STRATEGIC_CITY_NAME"].astype("string").str.strip()
    baseline_routing_df["_evaluation_receipt"] = baseline_routing_df["GSFS_RECEIPT_NO"].astype("string").str.strip()

    candidate_mapping_df = region_service_df[["STRATEGIC_CITY_NAME", "GSFS_RECEIPT_NO", "region_id"]].copy()
    candidate_mapping_df["_evaluation_city"] = candidate_city_values
    candidate_mapping_df["_evaluation_receipt"] = candidate_mapping_df["GSFS_RECEIPT_NO"].astype("string").str.strip()
    candidate_mapping_df["region_id"] = candidate_region_values
    candidate_mapping_df = candidate_mapping_df[["_evaluation_city", "_evaluation_receipt", "region_id"]]

    try:
        evaluated_region_service_df = baseline_routing_df.merge(
            candidate_mapping_df,
            on=["_evaluation_city", "_evaluation_receipt"],
            how="left",
            sort=False,
            validate="one_to_one",
        )
    except pd.errors.MergeError as exc:
        raise ValueError(f"region plan mapping is not one-to-one with routing baseline: {exc}") from exc
    if len(evaluated_region_service_df) != len(baseline_routing_df):
        raise ValueError("region plan mapping changed routing baseline row cardinality")
    if evaluated_region_service_df["region_id"].isna().any():
        raise ValueError("region plan mapping did not cover every routing baseline row")
    evaluated_region_service_df = evaluated_region_service_df.drop(
        columns=["_evaluation_city", "_evaluation_receipt"]
    )

    routing_cfg = context.routing_config
    effective_service_per_sm = float(routing_cfg.get("effective_service_per_sm", DEFAULT_EFFECTIVE_SERVICE_PER_SM))
    assignment_distance_backend = str(routing_cfg.get("assignment_distance_backend", "haversine")).strip().lower()
    service_time_per_job_min = float(routing_cfg.get("service_time_per_job_min", 60.0))
    max_work_min_per_sm_day = float(routing_cfg.get("max_work_min_per_sm_day", 480.0))
    max_travel_min_per_sm_day = routing_cfg.get("max_travel_min_per_sm_day")
    max_travel_km_per_sm_day = routing_cfg.get("max_travel_km_per_sm_day")
    max_travel_min_per_sm_day = (
        float(max_travel_min_per_sm_day) if max_travel_min_per_sm_day not in (None, "", 0) else None
    )
    max_travel_km_per_sm_day = (
        float(max_travel_km_per_sm_day) if max_travel_km_per_sm_day not in (None, "", 0) else None
    )

    current_route_df = context.current_route_df[
        context.current_route_df["STRATEGIC_CITY_NAME"].astype(str).str.strip().isin(candidate_cities)
    ].copy()
    integrated_route_df = _build_integrated_routes(
        region_service_df=evaluated_region_service_df,
        client_map=context.client_map,
        default_client=context.default_client,
        effective_service_per_sm=effective_service_per_sm,
        service_time_per_job_min=service_time_per_job_min,
        max_work_min_per_sm_day=max_work_min_per_sm_day,
        max_travel_min_per_sm_day=max_travel_min_per_sm_day,
        max_travel_km_per_sm_day=max_travel_km_per_sm_day,
        assignment_distance_backend=assignment_distance_backend,
    )
    route_detail_df = pd.concat([current_route_df, integrated_route_df], ignore_index=True)
    daily_summary_df = _build_daily_summary(route_detail_df)
    city_summary_df = _build_city_summary(daily_summary_df)
    return RegionPlanEvaluationResult(
        route_detail_df=route_detail_df,
        daily_summary_df=daily_summary_df,
        city_summary_df=city_summary_df,
    )


def build_routing_compare(
    service_file: Path = DEFAULT_SERVICE_FILE,
    region_service_file: Path | None = None,
    config_file: Path = Path("config/config.json"),
    output_dir: Path = OUTPUT_DIR,
    cities: list[str] | None = None,
) -> RoutingCompareResult:
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
    resolved_region_service_file = _infer_region_service_file(service_file, region_service_file)

    service_df = _load_service_df(service_file)
    region_service_df = _load_region_service_df(resolved_region_service_file)
    if cities:
        allowed = {str(c).strip() for c in cities}
        service_df = service_df[service_df["STRATEGIC_CITY_NAME"].isin(allowed)].copy()
        region_service_df = region_service_df[region_service_df["STRATEGIC_CITY_NAME"].isin(allowed)].copy()

    client_map, default_client = _build_routing_clients(routing_cfg)

    current_route_df = _build_current_routes(service_df, client_map, default_client)
    integrated_route_df = _build_integrated_routes(
        region_service_df,
        client_map,
        default_client,
        effective_service_per_sm,
        service_time_per_job_min,
        max_work_min_per_sm_day,
        max_travel_min_per_sm_day,
        max_travel_km_per_sm_day,
        assignment_distance_backend,
    )
    route_detail_df = pd.concat([current_route_df, integrated_route_df], ignore_index=True)
    daily_summary_df = _build_daily_summary(route_detail_df)
    city_summary_df = _build_city_summary(daily_summary_df)
    overall_summary_df = _build_overall_summary(city_summary_df)

    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = service_file.stem
    route_detail_path = output_dir / f"routing_compare_route_detail_{suffix}.csv"
    daily_summary_path = output_dir / f"routing_compare_daily_summary_{suffix}.csv"
    city_summary_path = output_dir / f"routing_compare_city_summary_{suffix}.csv"
    overall_summary_path = output_dir / f"routing_compare_overall_summary_{suffix}.csv"

    route_detail_df.to_csv(route_detail_path, index=False, encoding="utf-8-sig")
    daily_summary_df.to_csv(daily_summary_path, index=False, encoding="utf-8-sig")
    city_summary_df.to_csv(city_summary_path, index=False, encoding="utf-8-sig")
    overall_summary_df.to_csv(overall_summary_path, index=False, encoding="utf-8-sig")

    return RoutingCompareResult(
        route_detail_path=route_detail_path,
        daily_summary_path=daily_summary_path,
        city_summary_path=city_summary_path,
        overall_summary_path=overall_summary_path,
        route_detail_df=route_detail_df,
        daily_summary_df=daily_summary_df,
        city_summary_df=city_summary_df,
        overall_summary_df=overall_summary_df,
    )
