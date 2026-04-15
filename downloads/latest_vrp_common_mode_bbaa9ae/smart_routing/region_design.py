from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

INPUT_DIR = Path("260310/input")
OUTPUT_DIR = Path("260310/output")
DEFAULT_SERVICE_FILE = INPUT_DIR / "Service_202603181109_geocoded.csv"
DEFAULT_SLOT_FILE = INPUT_DIR / "Slot_updated_Service_202603181109.csv"
DEFAULT_TARGET_SM_PER_REGION = 5
DEFAULT_FALLBACK_SLOT = 7.0
DEFAULT_EFFECTIVE_SERVICE_PER_SM = 4.0
DEFAULT_BALANCE_WEIGHT = 120.0
DEFAULT_RADIUS_WEIGHT = 40.0
DEFAULT_MAX_ITER = 20
DEFAULT_REGION_ALGORITHM = "balanced"


@dataclass
class RegionDesignResult:
    city_summary_df: pd.DataFrame
    region_summary_df: pd.DataFrame
    postal_assignment_df: pd.DataFrame
    service_assignment_df: pd.DataFrame
    city_summary_path: Path
    region_summary_path: Path
    postal_assignment_path: Path
    service_assignment_path: Path


def _slugify(text: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(text))
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")


def _load_service_df(service_file: Path) -> pd.DataFrame:
    df = pd.read_csv(service_file, encoding="utf-8-sig", low_memory=False)
    for col in ["GSFS_RECEIPT_NO", "POSTAL_CODE", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE", "SVC_ENGINEER_CODE", "REPAIR_END_DATE_YYYYMMDD"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "source" in df.columns:
        df = df[df["source"].astype(str).str.strip().ne("failed")].copy()
    df = df[df["latitude"].notna() & df["longitude"].notna()].copy()
    df["POSTAL_CODE"] = df["POSTAL_CODE"].astype(str).str.zfill(5)
    if "REPAIR_END_DATE_YYYYMMDD" in df.columns:
        df["service_date"] = pd.to_datetime(df["REPAIR_END_DATE_YYYYMMDD"], format="%Y%m%d", errors="coerce")
        df = df[df["service_date"].notna()].copy()
    return df


def _load_slot_df(slot_file: Path) -> pd.DataFrame:
    df = pd.read_csv(slot_file, encoding="utf-8-sig", low_memory=False)
    for col in ["SVC_ENGINEER_CODE", "STRATEGIC_CITY_NAME"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    if "Slot" in df.columns:
        df["Slot"] = pd.to_numeric(df["Slot"], errors="coerce")
    return df


def _build_postal_stats(service_df: pd.DataFrame) -> pd.DataFrame:
    daily_postal_df = (
        service_df.groupby(["STRATEGIC_CITY_NAME", "POSTAL_CODE", "service_date"])
        .agg(
            daily_service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
            engineer_count=("SVC_ENGINEER_CODE", "nunique"),
        )
        .reset_index()
    )
    city_day_counts = (
        service_df.groupby("STRATEGIC_CITY_NAME")
        .agg(service_day_count=("service_date", "nunique"))
        .reset_index()
    )

    postal_df = (
        daily_postal_df.groupby(["STRATEGIC_CITY_NAME", "POSTAL_CODE"])
        .agg(
            total_service_count=("daily_service_count", "sum"),
            peak_daily_service_count=("daily_service_count", "max"),
            active_service_days=("service_date", "nunique"),
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
            engineer_count=("engineer_count", "max"),
        )
        .reset_index()
    )
    postal_df = postal_df.merge(city_day_counts, on="STRATEGIC_CITY_NAME", how="left")
    postal_df["service_count"] = postal_df["total_service_count"] / postal_df["service_day_count"].replace(0, 1)
    postal_df["service_count"] = postal_df["service_count"].round(2)
    return postal_df[postal_df["service_count"] > 0].copy()


def _build_city_daily_demand(service_df: pd.DataFrame) -> pd.DataFrame:
    daily_city_df = (
        service_df.groupby(["STRATEGIC_CITY_NAME", "service_date"])
        .agg(daily_service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()))
        .reset_index()
    )
    city_daily_df = (
        daily_city_df.groupby("STRATEGIC_CITY_NAME")
        .agg(
            total_service_count=("daily_service_count", "sum"),
            service_day_count=("service_date", "nunique"),
            avg_daily_service_count=("daily_service_count", "mean"),
            peak_daily_service_count=("daily_service_count", "max"),
        )
        .reset_index()
    )
    city_daily_df["avg_daily_service_count"] = city_daily_df["avg_daily_service_count"].round(2)
    return city_daily_df


def _build_city_capacity(slot_df: pd.DataFrame) -> pd.DataFrame:
    city_capacity_df = (
        slot_df.groupby("STRATEGIC_CITY_NAME")
        .agg(
            active_sm_count=("SVC_ENGINEER_CODE", "nunique"),
            total_slot_capacity=("Slot", "sum"),
            avg_slot_capacity=("Slot", "mean"),
            median_slot_capacity=("Slot", "median"),
        )
        .reset_index()
    )
    city_capacity_df["avg_slot_capacity"] = city_capacity_df["avg_slot_capacity"].fillna(DEFAULT_FALLBACK_SLOT)
    city_capacity_df["median_slot_capacity"] = city_capacity_df["median_slot_capacity"].fillna(DEFAULT_FALLBACK_SLOT)
    city_capacity_df["total_slot_capacity"] = city_capacity_df["total_slot_capacity"].fillna(0)
    return city_capacity_df


def _haversine_km(points_a: np.ndarray, points_b: np.ndarray) -> np.ndarray:
    lat1 = np.radians(points_a[:, 0])[:, None]
    lon1 = np.radians(points_a[:, 1])[:, None]
    lat2 = np.radians(points_b[:, 0])[None, :]
    lon2 = np.radians(points_b[:, 1])[None, :]
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371.0 * (2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0))))


def _initialize_weighted_center_indices(points: np.ndarray, weights: np.ndarray, cluster_count: int) -> list[int]:
    order = np.argsort(-weights)
    center_indices = [int(order[0])]
    for _ in range(1, cluster_count):
        dists = _haversine_km(points, points[np.array(center_indices, dtype=int)])
        min_dist = np.min(dists, axis=1)
        next_idx = int(np.argmax(min_dist * np.maximum(weights, 1.0)))
        if next_idx in center_indices:
            remaining = [idx for idx in range(len(points)) if idx not in center_indices]
            if not remaining:
                break
            next_idx = int(remaining[0])
        center_indices.append(next_idx)
    return center_indices


def _estimate_target_radius_km(points: np.ndarray, weights: np.ndarray, cluster_count: int) -> float:
    if len(points) <= 1 or cluster_count <= 1:
        return 25.0
    centroid = np.average(points, axis=0, weights=weights)
    spread = _haversine_km(points, np.array([centroid], dtype=float)).ravel()
    weighted_p85 = float(np.percentile(np.repeat(spread, np.maximum(weights.astype(int), 1)), 85))
    return max(15.0, weighted_p85 / max(math.sqrt(cluster_count), 1.0) * 1.8)


def _weighted_kmeans(points: np.ndarray, weights: np.ndarray, cluster_count: int, max_iter: int = 50) -> np.ndarray:
    if cluster_count <= 1 or len(points) <= 1:
        return np.zeros(len(points), dtype=int)
    if cluster_count >= len(points):
        return np.arange(len(points), dtype=int)

    center_indices = _initialize_weighted_center_indices(points, weights, cluster_count)
    centers = points[np.array(center_indices, dtype=int)].astype(float)
    labels = np.zeros(len(points), dtype=int)

    for _ in range(max_iter):
        dists = _haversine_km(points, centers)
        new_labels = np.argmin(dists, axis=1)
        if np.array_equal(labels, new_labels):
            break
        labels = new_labels
        for idx in range(cluster_count):
            mask = labels == idx
            if not mask.any():
                seed_idx = center_indices[idx]
                centers[idx] = points[seed_idx]
                labels[seed_idx] = idx
                continue
            centers[idx] = np.average(points[mask], axis=0, weights=weights[mask])

    unique_labels = sorted(set(int(label) for label in labels.tolist()))
    label_map = {old_label: new_idx for new_idx, old_label in enumerate(unique_labels)}
    return np.array([label_map[int(label)] for label in labels], dtype=int)


def _rebalance_weighted_regions(
    points: np.ndarray,
    weights: np.ndarray,
    cluster_count: int,
    target_service: float,
    balance_weight: float,
    radius_weight: float,
    max_iter: int = DEFAULT_MAX_ITER,
) -> np.ndarray:
    if cluster_count <= 1 or len(points) <= 1:
        return np.zeros(len(points), dtype=int)
    if cluster_count >= len(points):
        return np.arange(len(points), dtype=int)

    center_indices = _initialize_weighted_center_indices(points, weights, cluster_count)
    centers = points[np.array(center_indices, dtype=int)].astype(float)
    labels = np.full(len(points), -1, dtype=int)
    target_radius_km = _estimate_target_radius_km(points, weights, cluster_count)

    for _ in range(max_iter):
        prev_labels = labels.copy()
        cluster_service = np.zeros(cluster_count, dtype=float)
        cluster_radius = np.zeros(cluster_count, dtype=float)
        labels.fill(-1)

        for cluster_idx, point_idx in enumerate(center_indices):
            labels[point_idx] = cluster_idx
            cluster_service[cluster_idx] = weights[point_idx]

        point_order = [idx for idx in np.argsort(-weights) if idx not in center_indices]

        for point_idx in point_order:
            point = points[point_idx : point_idx + 1]
            dists_km = _haversine_km(point, centers).ravel()
            best_cluster = 0
            best_score = float("inf")
            for cluster_idx in range(cluster_count):
                new_service = cluster_service[cluster_idx] + weights[point_idx]
                balance_penalty = ((new_service - target_service) / max(target_service, 1.0)) ** 2
                new_radius = max(cluster_radius[cluster_idx], float(dists_km[cluster_idx]))
                radius_penalty = max(0.0, (new_radius - target_radius_km) / max(target_radius_km, 1.0)) ** 2
                score = dists_km[cluster_idx] + (balance_weight * balance_penalty) + (radius_weight * radius_penalty)
                if score < best_score:
                    best_score = score
                    best_cluster = cluster_idx
            labels[point_idx] = best_cluster
            cluster_service[best_cluster] += weights[point_idx]
            cluster_radius[best_cluster] = max(cluster_radius[best_cluster], float(dists_km[best_cluster]))

        for cluster_idx in range(cluster_count):
            mask = labels == cluster_idx
            if not mask.any():
                seed_idx = center_indices[cluster_idx]
                labels[seed_idx] = cluster_idx
                centers[cluster_idx] = points[seed_idx]
                continue
            centers[cluster_idx] = np.average(points[mask], axis=0, weights=weights[mask])

        if np.array_equal(labels, prev_labels):
            break

    return labels


def _design_city_regions(
    city_name: str,
    postal_df: pd.DataFrame,
    avg_slot_capacity: float,
    target_sm_per_region: int,
    balance_weight: float,
    radius_weight: float,
    avg_daily_service_count: float,
    algorithm: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    city_postal_df = postal_df[postal_df["STRATEGIC_CITY_NAME"] == city_name].copy()
    total_services = float(avg_daily_service_count)
    effective_slot = avg_slot_capacity if avg_slot_capacity and avg_slot_capacity > 0 else DEFAULT_FALLBACK_SLOT
    required_sm_total = max(1, math.ceil(total_services / effective_slot))
    region_count = min(len(city_postal_df), max(1, math.ceil(required_sm_total / max(target_sm_per_region, 1))))
    target_service_per_region = total_services / max(region_count, 1)

    coords = city_postal_df[["latitude", "longitude"]].to_numpy(dtype=float)
    weights = city_postal_df["service_count"].to_numpy(dtype=float)
    algo = str(algorithm).strip().lower()
    if algo == "weighted_kmeans":
        labels = _weighted_kmeans(coords, weights, region_count)
    else:
        labels = _rebalance_weighted_regions(
            points=coords,
            weights=weights,
            cluster_count=region_count,
            target_service=target_service_per_region,
            balance_weight=balance_weight,
            radius_weight=radius_weight,
        )
    city_postal_df["region_seq"] = labels + 1
    city_postal_df["region_id"] = city_postal_df["region_seq"].apply(lambda n: f"{_slugify(city_name)}_r{int(n):02d}")

    region_summary_df = (
        city_postal_df.groupby(["STRATEGIC_CITY_NAME", "region_id", "region_seq"])
        .agg(
            postal_count=("POSTAL_CODE", "nunique"),
            service_count=("service_count", "sum"),
            peak_daily_service_count=("peak_daily_service_count", "sum"),
            centroid_latitude=("latitude", lambda s: float(np.average(s, weights=city_postal_df.loc[s.index, "service_count"]))),
            centroid_longitude=("longitude", lambda s: float(np.average(s, weights=city_postal_df.loc[s.index, "service_count"]))),
        )
        .reset_index()
        .sort_values("region_seq")
    )
    region_centers = region_summary_df[["centroid_latitude", "centroid_longitude"]].to_numpy(dtype=float)
    point_to_center_km = _haversine_km(coords, region_centers)
    assigned_center_km = point_to_center_km[np.arange(len(city_postal_df)), city_postal_df["region_seq"].astype(int).to_numpy() - 1]
    city_postal_df["distance_to_region_center_km"] = assigned_center_km

    radius_df = (
        city_postal_df.groupby("region_id")
        .agg(
            max_radius_km=("distance_to_region_center_km", "max"),
            avg_radius_km=("distance_to_region_center_km", "mean"),
        )
        .reset_index()
    )
    region_summary_df["avg_slot_capacity"] = effective_slot
    region_summary_df["required_sm_count"] = np.ceil(region_summary_df["service_count"] / effective_slot).astype(int)
    region_summary_df["target_sm_per_region"] = int(target_sm_per_region)
    region_summary_df["algorithm"] = algo
    region_summary_df["target_service_per_region"] = round(target_service_per_region, 2)
    region_summary_df["service_per_required_sm"] = (
        region_summary_df["service_count"] / region_summary_df["required_sm_count"].replace(0, 1)
    ).round(2)
    region_summary_df["service_gap_vs_target"] = (region_summary_df["service_count"] - target_service_per_region).round(2)
    region_summary_df["service_gap_pct"] = (
        region_summary_df["service_gap_vs_target"] / max(target_service_per_region, 1.0) * 100.0
    ).round(2)
    region_summary_df = region_summary_df.merge(radius_df, on="region_id", how="left")

    city_postal_df = city_postal_df.merge(
        region_summary_df[["region_id", "required_sm_count", "max_radius_km", "avg_radius_km", "service_gap_pct"]],
        on="region_id",
        how="left",
    )
    return city_postal_df, region_summary_df


def build_region_design(
    service_file: Path = DEFAULT_SERVICE_FILE,
    slot_file: Path = DEFAULT_SLOT_FILE,
    input_dir: Path = INPUT_DIR,
    output_dir: Path = OUTPUT_DIR,
    target_sm_per_region: int = DEFAULT_TARGET_SM_PER_REGION,
    effective_service_per_sm: float = DEFAULT_EFFECTIVE_SERVICE_PER_SM,
    balance_weight: float = DEFAULT_BALANCE_WEIGHT,
    radius_weight: float = DEFAULT_RADIUS_WEIGHT,
    algorithm: str = DEFAULT_REGION_ALGORITHM,
) -> RegionDesignResult:
    service_df = _load_service_df(service_file)
    slot_df = _load_slot_df(slot_file)
    postal_df = _build_postal_stats(service_df)
    city_capacity_df = _build_city_capacity(slot_df)
    city_daily_demand_df = _build_city_daily_demand(service_df)

    postal_parts: list[pd.DataFrame] = []
    region_parts: list[pd.DataFrame] = []
    city_summary_rows: list[dict] = []

    for city_name, city_postal_df in postal_df.groupby("STRATEGIC_CITY_NAME", sort=True):
        cap_row = city_capacity_df[city_capacity_df["STRATEGIC_CITY_NAME"] == city_name]
        demand_row = city_daily_demand_df[city_daily_demand_df["STRATEGIC_CITY_NAME"] == city_name]
        avg_slot_capacity = float(cap_row["avg_slot_capacity"].iloc[0]) if not cap_row.empty else DEFAULT_FALLBACK_SLOT
        effective_slot_capacity = float(effective_service_per_sm) if effective_service_per_sm and effective_service_per_sm > 0 else avg_slot_capacity
        active_sm_count = int(cap_row["active_sm_count"].iloc[0]) if not cap_row.empty else 0
        total_slot_capacity = float(cap_row["total_slot_capacity"].iloc[0]) if not cap_row.empty else 0.0
        avg_daily_service_count = float(demand_row["avg_daily_service_count"].iloc[0]) if not demand_row.empty else float(city_postal_df["service_count"].sum())
        total_service_count = float(demand_row["total_service_count"].iloc[0]) if not demand_row.empty else float(city_postal_df["service_count"].sum())
        service_day_count = int(demand_row["service_day_count"].iloc[0]) if not demand_row.empty else 0
        peak_daily_service_count = float(demand_row["peak_daily_service_count"].iloc[0]) if not demand_row.empty else float(city_postal_df["peak_daily_service_count"].sum())

        postal_city_df, region_city_df = _design_city_regions(
            city_name=city_name,
            postal_df=postal_df,
            avg_slot_capacity=effective_slot_capacity,
            target_sm_per_region=target_sm_per_region,
            balance_weight=balance_weight,
            radius_weight=radius_weight,
            avg_daily_service_count=avg_daily_service_count,
            algorithm=algorithm,
        )
        postal_parts.append(postal_city_df)
        region_parts.append(region_city_df)

        required_sm_total = int(region_city_df["required_sm_count"].sum())
        city_summary_rows.append(
            {
                "STRATEGIC_CITY_NAME": city_name,
                "postal_count": int(city_postal_df["POSTAL_CODE"].nunique()),
                "service_count": round(avg_daily_service_count, 2),
                "avg_daily_service_count": round(avg_daily_service_count, 2),
                "peak_daily_service_count": round(peak_daily_service_count, 2),
                "total_service_count": int(total_service_count),
                "service_day_count": service_day_count,
                "active_sm_count": active_sm_count,
                "avg_slot_capacity": round(avg_slot_capacity, 2),
                "effective_service_per_sm": round(effective_slot_capacity, 2),
                "algorithm": str(algorithm).strip().lower(),
                "total_slot_capacity": round(total_slot_capacity, 2),
                "recommended_region_count": int(region_city_df["region_id"].nunique()),
                "required_sm_total": required_sm_total,
                "sm_gap_vs_current": int(required_sm_total - active_sm_count),
                "avg_region_service_gap_pct": round(region_city_df["service_gap_pct"].abs().mean(), 2),
                "max_region_radius_km": round(float(region_city_df["max_radius_km"].max()), 2),
            }
        )

    postal_assignment_df = pd.concat(postal_parts, ignore_index=True) if postal_parts else pd.DataFrame()
    region_summary_df = pd.concat(region_parts, ignore_index=True) if region_parts else pd.DataFrame()
    city_summary_df = pd.DataFrame(city_summary_rows).sort_values("STRATEGIC_CITY_NAME").reset_index(drop=True)

    service_assignment_df = service_df.merge(
        postal_assignment_df[["STRATEGIC_CITY_NAME", "POSTAL_CODE", "region_id", "region_seq"]],
        on=["STRATEGIC_CITY_NAME", "POSTAL_CODE"],
        how="left",
    )

    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = service_file.stem
    city_summary_path = output_dir / f"region_design_city_summary_{suffix}.csv"
    region_summary_path = output_dir / f"region_design_region_summary_{suffix}.csv"
    postal_assignment_path = input_dir / f"region_design_postal_{suffix}.csv"
    service_assignment_path = input_dir / f"region_design_service_{suffix}.csv"

    city_summary_df.to_csv(city_summary_path, index=False, encoding="utf-8-sig")
    region_summary_df.to_csv(region_summary_path, index=False, encoding="utf-8-sig")
    postal_assignment_df.to_csv(postal_assignment_path, index=False, encoding="utf-8-sig")
    service_assignment_df.to_csv(service_assignment_path, index=False, encoding="utf-8-sig")

    return RegionDesignResult(
        city_summary_df=city_summary_df,
        region_summary_df=region_summary_df,
        postal_assignment_df=postal_assignment_df,
        service_assignment_df=service_assignment_df,
        city_summary_path=city_summary_path,
        region_summary_path=region_summary_path,
        postal_assignment_path=postal_assignment_path,
        service_assignment_path=service_assignment_path,
    )
