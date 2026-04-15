from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from .osrm_routing import OSRMConfig, OSRMTripClient
from .region_design import (
    DEFAULT_BALANCE_WEIGHT,
    DEFAULT_EFFECTIVE_SERVICE_PER_SM,
    DEFAULT_RADIUS_WEIGHT,
    _rebalance_weighted_regions,
)

INPUT_DIR = Path("260310/input")
OUTPUT_DIR = Path("260310/output")
DEFAULT_SERVICE_FILE = INPUT_DIR / "Service_202603181109_geocoded.csv"
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


def _load_config(config_file: Path) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


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


def _get_client_for_city(city_name: str, client_map: dict[str, OSRMTripClient], default_client: OSRMTripClient) -> OSRMTripClient:
    return client_map.get(city_name, default_client)


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
    min_sm_count = max(1, math.ceil(len(group_df) / max(effective_service_per_sm, 1.0)))
    max_sm_count = max(1, len(group_df))
    accepted_labels: pd.Series | None = None

    for sm_count in range(min_sm_count, max_sm_count + 1):
        labels = _build_region_day_cluster_labels(group_df, sm_count)
        is_valid = True
        for _, cluster_df in group_df.groupby(labels, sort=True):
            metrics = _estimate_group_route(cluster_df, client)
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
        client=client,
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


def build_routing_compare(
    service_file: Path = DEFAULT_SERVICE_FILE,
    region_service_file: Path | None = None,
    config_file: Path = Path("config.json"),
    output_dir: Path = OUTPUT_DIR,
    cities: list[str] | None = None,
) -> RoutingCompareResult:
    cfg = _load_config(config_file)
    routing_cfg = cfg.get("routing", {})
    distance_backend = str(routing_cfg.get("distance_backend", "osrm")).strip().lower()
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

    default_client = OSRMTripClient(
        OSRMConfig(
            osrm_url=str(routing_cfg.get("osrm_url", "https://router.project-osrm.org")).rstrip("/"),
            mode="haversine" if distance_backend == "city_osrm_else_haversine" else distance_backend,
            osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
            cache_file=Path(str(routing_cfg.get("osrm_cache_file", "data/cache/osrm_trip_cache.csv"))),
        )
    )
    client_map: dict[str, OSRMTripClient] = {}
    city_osrm_urls = routing_cfg.get("city_osrm_urls", {})
    for city_name, city_url in city_osrm_urls.items():
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
