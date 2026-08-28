"""Build reviewable, city-scoped region-plan candidates from service demand.

This module deliberately creates candidate artifacts only.  It never writes a
reviewed region map, modifies a technician master, or updates the database.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from .data_catalog import na_data_path
from .region_design import _initialize_weighted_center_indices, _rebalance_weighted_regions, _weighted_kmeans


SERVICE_DATE_COLUMNS = (
    "PROMISE_DATE",
    "REPAIR_END_DATE_YYYYMMDD",
    "REPAIR_RECEIPT_DATE_YYYYMMDD",
    "GERP_INPUT_DATE_YYYYMMDD_ID_LAST",
)
AREA_MAP_POSTAL_COLUMNS = (
    "POSTAL_CODE",
    "STRATEGIC_CITY_NAME",
    "region_id",
    "region_seq",
    "AREA_NAME",
    "area_type",
)


@dataclass(frozen=True)
class CandidatePlanResult:
    output_dir: Path
    plan_id: str
    region_postals_path: Path
    technician_assignments_path: Path
    region_summary_path: Path
    evidence_path: Path
    rejects_path: Path
    manifest_path: Path


def _clean(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def _slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", _clean(value).lower()).strip("_")
    return text or "city"


def _city_key(value: object) -> str:
    """Compare a strategic city (``Atlanta, GA``) to an address city.

    The Address sheet has a city-only value, so the state suffix is intentionally
    ignored for this candidate-only selection.
    """
    return re.sub(r"[^a-z0-9]+", "", _clean(value).split(",", 1)[0].lower())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _haversine_km(points_a: np.ndarray, points_b: np.ndarray) -> np.ndarray:
    lat1 = np.radians(points_a[:, 0])[:, None]
    lon1 = np.radians(points_a[:, 1])[:, None]
    lat2 = np.radians(points_b[:, 0])[None, :]
    lon2 = np.radians(points_b[:, 1])[None, :]
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371.0 * (2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0))))


def _service_dates(frame: pd.DataFrame) -> pd.Series:
    result = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    for column in SERVICE_DATE_COLUMNS:
        if column not in frame.columns:
            continue
        # Input exports mix ISO dates, Excel-like values, and YYYYMMDD integers.
        parsed = pd.to_datetime(frame[column].astype(str), errors="coerce")
        result = result.fillna(parsed)
    return result.dt.normalize()


def _load_city_service(service_file: Path, city_name: str) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    required = {"STRATEGIC_CITY_NAME", "POSTAL_CODE", "latitude", "longitude"}
    frame = pd.read_csv(service_file, encoding="utf-8-sig", low_memory=False)
    missing = required - set(frame.columns)
    if missing:
        raise ValueError("SERVICE_COLUMNS_MISSING:" + ",".join(sorted(missing)))
    city = frame[frame["STRATEGIC_CITY_NAME"].map(_clean).eq(_clean(city_name))].copy()
    if city.empty:
        raise ValueError(f"CITY_NOT_FOUND_IN_SERVICE:{city_name}")
    city["POSTAL_CODE"] = city["POSTAL_CODE"].map(_clean).str.replace(r"\.0$", "", regex=True).str.zfill(5)
    city["latitude"] = pd.to_numeric(city["latitude"], errors="coerce")
    city["longitude"] = pd.to_numeric(city["longitude"], errors="coerce")
    rejects: list[dict[str, str]] = []
    invalid = city["POSTAL_CODE"].eq("")
    for index in city.index[invalid]:
        rejects.append({"artifact": "service", "record_id": str(index), "reason": "MISSING_POSTAL_CODE"})
    city = city.loc[~invalid].copy()
    if city.empty:
        raise ValueError(f"NO_VALID_GEO_SERVICES:{city_name}")
    job_key = city.get("GSFS_RECEIPT_NO", pd.Series("", index=city.index)).map(_clean)
    city["_job_key"] = job_key.where(job_key.ne(""), "row-" + city.index.astype(str))
    city = city.drop_duplicates(subset=["_job_key"], keep="first").copy()
    city["service_date"] = _service_dates(city)
    return city, rejects


def _load_city_zip_coverage(profile_file: Path, city_name: str) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    coverage = pd.read_excel(profile_file, sheet_name="1. Zip Coverage")
    required = {"POSTAL_CODE", "STRATEGIC_CITY_NAME"}
    missing = required - set(coverage.columns)
    if missing:
        raise ValueError("ZIP_COVERAGE_COLUMNS_MISSING:" + ",".join(sorted(missing)))
    selected = coverage[coverage["STRATEGIC_CITY_NAME"].map(_clean).eq(_clean(city_name))].copy()
    if selected.empty:
        raise ValueError(f"CITY_NOT_FOUND_IN_ZIP_COVERAGE:{city_name}")
    selected["POSTAL_CODE"] = selected["POSTAL_CODE"].map(_clean).str.replace(r"\.0$", "", regex=True).str.zfill(5)
    rejects: list[dict[str, str]] = []
    invalid = ~selected["POSTAL_CODE"].str.fullmatch(r"\d{5}")
    for index in selected.index[invalid]:
        rejects.append({"artifact": "zip_coverage", "record_id": str(index), "reason": "INVALID_POSTAL_CODE"})
    selected = selected.loc[~invalid].copy()
    aggregation: dict[str, object] = {"STRATEGIC_CITY_NAME": "first"}
    if "SVC_CENTER_TYPE" in selected.columns:
        aggregation["SVC_CENTER_TYPE"] = lambda values: "|".join(sorted({_clean(value) for value in values if _clean(value)}))
    selected = selected.groupby("POSTAL_CODE", as_index=False).agg(aggregation)
    if "SVC_CENTER_TYPE" not in selected.columns:
        selected["SVC_CENTER_TYPE"] = ""
    return selected, rejects


def _zcta_centroids(postal_codes: list[str], zcta_geometry_file: Path) -> pd.DataFrame:
    if not postal_codes or not zcta_geometry_file.is_file():
        return pd.DataFrame(columns=["POSTAL_CODE", "latitude", "longitude"])
    try:
        import pyogrio

        geometry = pyogrio.read_dataframe(
            zcta_geometry_file,
            columns=["ZCTA5CE20", "INTPTLAT20", "INTPTLON20"],
            read_geometry=False,
        )
    except (ImportError, OSError, ValueError, RuntimeError):
        return pd.DataFrame(columns=["POSTAL_CODE", "latitude", "longitude"])
    if "ZCTA5CE20" not in geometry.columns:
        return pd.DataFrame(columns=["POSTAL_CODE", "latitude", "longitude"])
    geometry["POSTAL_CODE"] = geometry["ZCTA5CE20"].map(_clean).str.zfill(5)
    geometry = geometry[geometry["POSTAL_CODE"].isin(postal_codes)].copy()
    if geometry.empty:
        return pd.DataFrame(columns=["POSTAL_CODE", "latitude", "longitude"])
    if not {"INTPTLAT20", "INTPTLON20"}.issubset(geometry.columns):
        return pd.DataFrame(columns=["POSTAL_CODE", "latitude", "longitude"])
    geometry["latitude"] = pd.to_numeric(geometry["INTPTLAT20"], errors="coerce")
    geometry["longitude"] = pd.to_numeric(geometry["INTPTLON20"], errors="coerce")
    return geometry[["POSTAL_CODE", "latitude", "longitude"]].drop_duplicates("POSTAL_CODE")


def _coverage_postals_with_demand(
    coverage: pd.DataFrame,
    service: pd.DataFrame,
    zcta_geometry_file: Path,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    demand = _postal_demand(service)
    result = coverage.merge(demand, on="POSTAL_CODE", how="left", suffixes=("", "_service"))
    result["service_count"] = result["service_count"].fillna(0.0)
    for column in ("avg_daily_jobs", "p95_daily_jobs", "max_daily_jobs"):
        result[column] = result[column].fillna(0.0)
    service_coords = demand[["POSTAL_CODE", "latitude", "longitude"]].rename(
        columns={"latitude": "service_latitude", "longitude": "service_longitude"}
    )
    result = result.drop(columns=[column for column in ("latitude", "longitude") if column in result.columns]).merge(
        service_coords, on="POSTAL_CODE", how="left"
    )
    missing = result["service_latitude"].isna() | result["service_longitude"].isna()
    zcta = _zcta_centroids(result.loc[missing, "POSTAL_CODE"].tolist(), zcta_geometry_file).rename(
        columns={"latitude": "zcta_latitude", "longitude": "zcta_longitude"}
    )
    result = result.merge(zcta, on="POSTAL_CODE", how="left")
    result["latitude"] = result["service_latitude"].fillna(result.get("zcta_latitude"))
    result["longitude"] = result["service_longitude"].fillna(result.get("zcta_longitude"))
    rejects: list[dict[str, str]] = []
    invalid = result["latitude"].isna() | result["longitude"].isna()
    for postal_code in result.loc[invalid, "POSTAL_CODE"]:
        rejects.append({"artifact": "zip_coverage", "record_id": str(postal_code), "reason": "MISSING_SERVICE_AND_ZCTA_COORDINATE"})
    result = result.loc[~invalid].copy()
    return result.drop(columns=[column for column in ["service_latitude", "service_longitude", "zcta_latitude", "zcta_longitude"] if column in result.columns]), rejects


def _postal_demand(service_df: pd.DataFrame) -> pd.DataFrame:
    postal = (
        service_df.groupby("POSTAL_CODE", as_index=False)
        .agg(
            latitude=("latitude", "median"),
            longitude=("longitude", "median"),
            service_count=("_job_key", "nunique"),
        )
        .sort_values("POSTAL_CODE")
        .reset_index(drop=True)
    )
    dated = service_df.dropna(subset=["service_date"])
    if dated.empty:
        postal["avg_daily_jobs"] = 0.0
        postal["p95_daily_jobs"] = 0.0
        postal["max_daily_jobs"] = 0
        return postal
    daily = dated.groupby(["POSTAL_CODE", "service_date"])["_job_key"].nunique().rename("daily_jobs").reset_index()
    days = max(1, int((dated["service_date"].max() - dated["service_date"].min()).days) + 1)
    stats = daily.groupby("POSTAL_CODE")["daily_jobs"].agg(
        avg_daily_jobs=lambda values: float(values.sum()) / days,
        p95_daily_jobs=lambda values: float(values.quantile(0.95)),
        max_daily_jobs="max",
    ).reset_index()
    return postal.merge(stats, on="POSTAL_CODE", how="left").fillna({"avg_daily_jobs": 0.0, "p95_daily_jobs": 0.0, "max_daily_jobs": 0})


def _load_city_technicians(profile_file: Path, city_name: str, technician_city: str | None) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    address = pd.read_excel(profile_file, sheet_name="4. Address")
    required = {"SVC_ENGINEER_CODE", "City ", "latitude", "longitude"}
    missing = required - set(address.columns)
    if missing:
        raise ValueError("ADDRESS_COLUMNS_MISSING:" + ",".join(sorted(missing)))
    requested_city = technician_city or city_name
    requested_city_key = _city_key(requested_city)
    # Address-sheet City is a physical home municipality (for example,
    # ``Acworth``), whereas this profile stores the strategic city (for
    # example, ``Atlanta, GA``) in State.  Other cities can use City directly.
    city_match = address["City "].map(_city_key).eq(requested_city_key)
    strategic_city_match = (
        address["State"].map(_city_key).eq(requested_city_key)
        if "State" in address.columns
        else pd.Series(False, index=address.index)
    )
    selected = address[city_match | strategic_city_match].copy()
    rejects: list[dict[str, str]] = []
    selected["SVC_ENGINEER_CODE"] = selected["SVC_ENGINEER_CODE"].map(_clean)
    selected["latitude"] = pd.to_numeric(selected["latitude"], errors="coerce")
    selected["longitude"] = pd.to_numeric(selected["longitude"], errors="coerce")
    invalid = selected["SVC_ENGINEER_CODE"].eq("") | selected["latitude"].isna() | selected["longitude"].isna()
    for index in selected.index[invalid]:
        rejects.append({"artifact": "technician", "record_id": str(index), "reason": "MISSING_TECHNICIAN_CODE_OR_HOME_COORDINATE"})
    selected = selected.loc[~invalid].copy()
    duplicates = selected[selected["SVC_ENGINEER_CODE"].duplicated(keep="first")]
    for _, row in duplicates.iterrows():
        rejects.append({"artifact": "technician", "record_id": row["SVC_ENGINEER_CODE"], "reason": "DUPLICATE_TECHNICIAN_CODE"})
    selected = selected.drop_duplicates(subset=["SVC_ENGINEER_CODE"], keep="first").copy()
    if selected.empty:
        raise ValueError(f"NO_VALID_TECHNICIAN_HOMES:{requested_city}")
    if "Name" in selected.columns:
        selected["SVC_ENGINEER_NAME"] = selected["Name"].map(_clean)
    else:
        selected["SVC_ENGINEER_NAME"] = ""
    return selected[["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "City ", "latitude", "longitude"]].reset_index(drop=True), rejects


def load_city_technician_roster(profile_file: Path, city_name: str) -> pd.DataFrame:
    """Return the selectable Address-sheet roster for a strategic city."""
    technicians, _ = _load_city_technicians(profile_file, city_name, technician_city=None)
    return technicians.copy()


def _cluster_postals(postal: pd.DataFrame, region_count: int, algorithm: str) -> pd.DataFrame:
    if region_count < 1:
        raise ValueError("REGION_COUNT_MUST_BE_POSITIVE")
    if region_count > len(postal):
        raise ValueError(f"REGION_COUNT_EXCEEDS_POSTAL_COUNT:{region_count}>{len(postal)}")
    points = postal[["latitude", "longitude"]].to_numpy(dtype=float)
    weights = postal["service_count"].to_numpy(dtype=float)
    selected = algorithm.strip().lower()
    if selected in {"weighted_kmeans", "weighted_kmeans_staffing"}:
        labels = _weighted_kmeans(points, weights, region_count)
    elif selected in {"balanced", "contiguous_balanced"}:
        # Radius deliberately has no weight in this first candidate version.
        labels = _rebalance_weighted_regions(
            points=points,
            weights=weights,
            cluster_count=region_count,
            target_service=float(weights.sum()) / region_count,
            balance_weight=120.0,
            radius_weight=0.0,
        )
        labels = _repair_sparse_balanced_clusters(points, weights, labels, region_count)
    else:
        raise ValueError(f"UNSUPPORTED_ALGORITHM:{algorithm}")
    result = postal.copy()
    result["region_seq"] = labels.astype(int) + 1
    return result


def _load_zcta_polygons(postal_codes: list[str], zcta_geometry_file: Path):
    """Load only the candidate ZCTAs; full national geometry is too large."""
    try:
        import geopandas as gpd

        where = "ZCTA5CE20 IN (" + ",".join(f"'{code}'" for code in postal_codes) + ")"
        layer = gpd.read_file(
            f"zip://{zcta_geometry_file.as_posix()}", where=where, columns=["ZCTA5CE20", "geometry"]
        ).to_crs("EPSG:4326")
        layer["POSTAL_CODE"] = layer["ZCTA5CE20"].astype(str).str.zfill(5)
        return layer[["POSTAL_CODE", "geometry"]].reset_index(drop=True)
    except Exception:
        return None


def _adjacency_graph(postals: pd.DataFrame, zcta_geometry_file: Path) -> dict[str, set[str]]:
    geometry = _load_zcta_polygons(postals["POSTAL_CODE"].astype(str).tolist(), zcta_geometry_file)
    graph = {str(code): set() for code in postals["POSTAL_CODE"]}
    if geometry is None or geometry.empty:
        return graph
    lookup = dict(zip(geometry["POSTAL_CODE"].astype(str), geometry.geometry))
    spatial_index = geometry.sindex
    for index, row in geometry.iterrows():
        postal = str(row["POSTAL_CODE"])
        for candidate_index in spatial_index.query(row.geometry, predicate="intersects"):
            if candidate_index <= index:
                continue
            other_postal = str(geometry.iloc[candidate_index]["POSTAL_CODE"])
            # A shared point is sufficient for a graph connection.  Road-barrier
            # policy remains a later, separate refinement.
            if row.geometry.intersects(lookup[other_postal]):
                graph[postal].add(other_postal)
                graph[other_postal].add(postal)
    return graph


def _attach_postal_areas(postals: pd.DataFrame, zcta_geometry_file: Path) -> pd.DataFrame:
    geometry = _load_zcta_polygons(postals["POSTAL_CODE"].astype(str).tolist(), zcta_geometry_file)
    result = postals.copy()
    if geometry is None or geometry.empty:
        result["area_km2"] = 0.0
        return result
    areas = geometry.to_crs("EPSG:6933").assign(area_km2=lambda frame: frame.geometry.area / 1_000_000.0)
    result = result.merge(areas[["POSTAL_CODE", "area_km2"]], on="POSTAL_CODE", how="left")
    result["area_km2"] = result["area_km2"].fillna(0.0)
    return result


def _contiguous_nearest_growth(
    postals: pd.DataFrame,
    zcta_geometry_file: Path,
    seed_indices: list[int],
    service_balance_weight: float = 0.0,
    area_balance_weight: float = 0.0,
) -> tuple[pd.DataFrame, int]:
    """Grow connected territories from seeds using centroid distance only.

    Demand chooses the seeds, but never permits a non-adjacent ZIP jump.  This
    keeps geography compact.  Optional soft quota penalties prevent a central
    seed from absorbing a disproportionate share of the service area while
    preserving the adjacent-ZIP-only rule.
    """
    result = postals.reset_index(drop=True).copy()
    graph = _adjacency_graph(result, zcta_geometry_file)
    count = len(seed_indices)
    labels = np.full(len(result), -1, dtype=int)
    for region, index in enumerate(seed_indices):
        labels[int(index)] = region
    points = result[["latitude", "longitude"]].to_numpy(dtype=float)
    weights = np.maximum(result["service_count"].to_numpy(dtype=float), 1.0)
    areas = np.maximum(pd.to_numeric(result.get("area_km2", pd.Series(0.0, index=result.index)), errors="coerce").fillna(0.0).to_numpy(dtype=float), 0.0)
    target_service = max(float(weights.sum()) / count, 1.0)
    target_area = max(float(areas.sum()) / count, 1.0)
    island_assignments = 0
    while (labels < 0).any():
        assigned_by_postal = {str(result.iloc[index]["POSTAL_CODE"]): int(labels[index]) for index in np.where(labels >= 0)[0]}
        best: tuple[float, int, int] | None = None
        for index in np.where(labels < 0)[0]:
            adjacent_regions = {
                assigned_by_postal[neighbor]
                for neighbor in graph.get(str(result.iloc[index]["POSTAL_CODE"]), set())
                if neighbor in assigned_by_postal
            }
            for region in adjacent_regions:
                members = np.where(labels == region)[0]
                center = np.average(points[members], axis=0, weights=weights[members])
                distance = float(_haversine_km(points[index:index + 1], np.asarray([center]))[0, 0])
                projected_service = (float(weights[members].sum()) + weights[index]) / target_service
                projected_area = (float(areas[members].sum()) + areas[index]) / target_area
                quota_penalty = (
                    service_balance_weight * max(0.0, projected_service - 1.0)
                    + area_balance_weight * max(0.0, projected_area - 1.0)
                )
                # 55 km is sufficiently material to stop a large central
                # territory from consuming a second/third target share, but
                # does not permit a non-adjacent jump.
                candidate = (distance + 55.0 * quota_penalty, int(index), int(region))
                if best is None or candidate < best:
                    best = candidate
        if best is None:
            # A disconnected ZCTA graph component has no shared boundary.  It
            # cannot be made contiguous with another component, so attach it to
            # the nearest existing territory and record the exception.
            index = int(np.where(labels < 0)[0][0])
            centers = []
            for region in range(count):
                members = np.where(labels == region)[0]
                centers.append(np.average(points[members], axis=0, weights=weights[members]))
            region = int(np.argmin(_haversine_km(points[index:index + 1], np.asarray(centers))[0]))
            labels[index] = region
            island_assignments += 1
        else:
            _, index, region = best
            labels[index] = region
    result["region_seq"] = labels + 1
    return result, island_assignments


def _weighted_kmeans_seed_indices(points: np.ndarray, weights: np.ndarray, region_count: int) -> list[int]:
    """Use demand-aware K-Means only to select geographically distributed seeds.

    Territory boundaries are deliberately *not* the K-Means labels.  They are
    subsequently produced by contiguous nearest-centroid growth.
    """
    labels = _weighted_kmeans(points, weights, region_count)
    seeds: list[int] = []
    for label in sorted(set(labels.tolist())):
        members = np.flatnonzero(labels == label)
        # Stable ties make candidate output reproducible.
        seed = min(members.tolist(), key=lambda index: (-float(weights[index]), int(index)))
        seeds.append(int(seed))
    if len(seeds) < region_count:
        for index in _initialize_weighted_center_indices(points, weights, region_count):
            if index not in seeds:
                seeds.append(index)
            if len(seeds) == region_count:
                break
    if len(seeds) < region_count:
        for index in range(len(points)):
            if index not in seeds:
                seeds.append(index)
            if len(seeds) == region_count:
                break
    return seeds


def _center_shared_radial_labels(postals: pd.DataFrame, region_count: int) -> np.ndarray:
    """Divide the demand core into contiguous radial sectors.

    Unlike seed growth, no territory owns the highest-demand ZIP merely because
    it was selected first.  ZIPs are ordered around the city demand centre and
    each region receives one continuous angular sector extending from the core
    toward the edge.  Individual ZIPs remain indivisible; a core ZIP that falls
    on a sector edge can be handled later as an explicit overlap policy.
    """
    if region_count < 1 or region_count > len(postals):
        raise ValueError("RADIAL_REGION_COUNT_INVALID")
    points = postals[["latitude", "longitude"]].to_numpy(dtype=float)
    weights = np.maximum(postals["service_count"].to_numpy(dtype=float), 0.0)
    area_source = postals["area_km2"] if "area_km2" in postals.columns else pd.Series(1.0, index=postals.index)
    area_weights = np.maximum(pd.to_numeric(area_source, errors="coerce").fillna(0.0).to_numpy(dtype=float), 0.0)
    if not area_weights.any():
        area_weights = np.ones(len(postals), dtype=float)
    center_weights = np.maximum(weights, 1.0)
    center = np.average(points, axis=0, weights=center_weights)
    east = (points[:, 1] - center[1]) * np.cos(np.radians(center[0]))
    north = points[:, 0] - center[0]
    angles = (np.degrees(np.arctan2(east, north)) + 360.0) % 360.0
    angle_order = np.argsort(angles, kind="stable")
    target = float(weights.sum()) / region_count
    target_area = float(area_weights.sum()) / region_count
    target_postals = float(len(postals)) / region_count
    minimum_postals = max(1, int(np.floor(target_postals * 0.60)))

    best_labels: np.ndarray | None = None
    best_score: tuple[float, float, int] | None = None
    # Try every ZIP as the zero-degree cut.  This avoids a hard-coded Region 1
    # and chooses the set of radial cuts with the fairest service allocation.
    for start_offset in range(len(angle_order)):
        ordered = np.roll(angle_order, -start_offset)
        labels = np.empty(len(postals), dtype=int)
        region = 0
        load = 0.0
        area = 0.0
        postal_count = 0
        loads = [0.0] * region_count
        areas = [0.0] * region_count
        counts = [0] * region_count
        for position, index in enumerate(ordered):
            remaining_zip_count = len(ordered) - position
            remaining_regions = region_count - region
            weight = float(weights[index])
            area_weight = float(area_weights[index])
            if region < region_count - 1 and remaining_zip_count >= remaining_regions:
                def imbalance_score(service: float, size: float, count: int) -> float:
                    # Demand is the main objective.  Area and ZIP count prevent
                    # a sparse direction becoming a giant catch-all territory.
                    return (
                        ((service - target) / max(target, 1.0)) ** 2
                        + 0.45 * ((size - target_area) / max(target_area, 1.0)) ** 2
                        + 0.25 * ((count - target_postals) / max(target_postals, 1.0)) ** 2
                    )

                keep_score = imbalance_score(load + weight, area + area_weight, postal_count + 1)
                cut_score = imbalance_score(load, area, postal_count)
                enough_for_future = remaining_zip_count >= minimum_postals * (region_count - region - 1)
                must_cut = remaining_zip_count == remaining_regions and postal_count > 0
                if postal_count >= minimum_postals and enough_for_future and (must_cut or cut_score <= keep_score):
                    region += 1
                    load = 0.0
                    area = 0.0
                    postal_count = 0
            labels[index] = region
            load += weight
            area += area_weight
            postal_count += 1
            loads[region] += weight
            areas[region] += area_weight
            counts[region] += 1
        # A compact angular sector is inherently straight-edged.  Span spread
        # is only a tie-breaker after service balance, never a reason to split
        # the urban demand core unevenly.
        score = (
            float(np.var(np.asarray(loads) / max(target, 1.0)))
            + 0.45 * float(np.var(np.asarray(areas) / max(target_area, 1.0)))
            + 0.25 * float(np.var(np.asarray(counts) / max(target_postals, 1.0))),
            float(np.var(counts)),
            int(start_offset),
        )
        if best_score is None or score < best_score:
            best_labels, best_score = labels, score
    if best_labels is None:
        raise ValueError("RADIAL_LABELS_UNAVAILABLE")
    return best_labels


def _remaining_disconnected_region_components(postals: pd.DataFrame, zcta_geometry_file: Path) -> int:
    """Count detached components left only where the source ZCTA graph is split."""
    graph = _adjacency_graph(postals, zcta_geometry_file)
    return sum(
        max(0, len(_connected_components(set(group["POSTAL_CODE"].astype(str)), graph)) - 1)
        for _, group in postals.groupby("region_seq")
    )


def _connected_components(nodes: set[str], graph: dict[str, set[str]]) -> list[set[str]]:
    remaining = set(nodes)
    components: list[set[str]] = []
    while remaining:
        seed = remaining.pop()
        component = {seed}
        frontier = [seed]
        while frontier:
            current = frontier.pop()
            adjacent = (graph.get(current) or set()) & remaining & nodes
            remaining.difference_update(adjacent)
            component.update(adjacent)
            frontier.extend(adjacent)
        components.append(component)
    return components


def _enforce_contiguous_regions(postals: pd.DataFrame, zcta_geometry_file: Path) -> tuple[pd.DataFrame, int, int]:
    """Move detached ZIP components to an adjacent region, preserving demand as far as possible."""
    graph = _adjacency_graph(postals, zcta_geometry_file)
    if not any(graph.values()):
        return postals, 0, 0
    result = postals.copy()
    target_demand = float(result["service_count"].sum()) / max(result["region_seq"].nunique(), 1)
    changes = 0
    for _ in range(len(result)):
        changed = False
        demand_by_region = result.groupby("region_seq")["service_count"].sum().to_dict()
        for region_seq in sorted(result["region_seq"].unique()):
            members = set(result.loc[result["region_seq"].eq(region_seq), "POSTAL_CODE"].astype(str))
            components = _connected_components(members, graph)
            if len(components) <= 1:
                continue
            main = max(components, key=lambda component: (float(result[result["POSTAL_CODE"].isin(component)]["service_count"].sum()), len(component)))
            for component in components:
                if component == main:
                    continue
                neighbor_regions = set(
                    result.loc[
                        result["POSTAL_CODE"].astype(str).isin(
                            set().union(*(graph.get(postal, set()) for postal in component))
                        ),
                        "region_seq",
                    ].astype(int)
                ) - {int(region_seq)}
                if not neighbor_regions:
                    continue
                component_demand = float(result[result["POSTAL_CODE"].isin(component)]["service_count"].sum())
                destination = min(
                    neighbor_regions,
                    key=lambda candidate: (abs((demand_by_region.get(candidate, 0.0) + component_demand) - target_demand), candidate),
                )
                result.loc[result["POSTAL_CODE"].isin(component), "region_seq"] = destination
                changes += len(component)
                changed = True
        if not changed:
            break
    remaining_components = sum(
        max(0, len(_connected_components(set(group["POSTAL_CODE"].astype(str)), graph)) - 1)
        for _, group in result.groupby("region_seq")
    )
    return result, changes, remaining_components


def _staffing_snapshot(
    postals: pd.DataFrame,
    service: pd.DataFrame,
    max_daily_jobs_per_technician: int,
    technician_count: int,
) -> pd.DataFrame:
    summary = (
        postals.groupby("region_seq", as_index=False)
        .agg(annual_service_count=("service_count", "sum"), area_km2=("area_km2", "sum"))
        .sort_values("region_seq")
    )
    summary = summary.merge(_region_daily_demand(service, postals), on="region_seq", how="left").fillna(0.0)
    summary["required_technician_count"] = np.ceil(
        summary["p95_daily_jobs"] / max_daily_jobs_per_technician
    ).astype(int).clip(lower=1)
    summary["candidate_technician_target"] = _fair_staffing_targets(
        summary["required_technician_count"], technician_count, summary["annual_service_count"]
    )
    summary["annual_jobs_per_target_technician"] = (
        summary["annual_service_count"] / summary["candidate_technician_target"].replace(0, 1)
    )
    summary["avg_daily_jobs_per_target_technician"] = (
        summary["avg_daily_jobs"] / summary["candidate_technician_target"].replace(0, 1)
    )
    return summary


def _optimize_boundaries_for_staffing(
    postals: pd.DataFrame,
    service: pd.DataFrame,
    zcta_geometry_file: Path,
    max_daily_jobs_per_technician: int,
    technician_count: int,
    area_balance_weight: float,
    max_iterations: int = 80,
) -> tuple[pd.DataFrame, int]:
    """Rebalance only boundary ZIPs while preserving connected territories.

    Region demand is compared per planned technician, not per ZIP count or
    polygon area.  Actual technician-home assignment happens after this loop.
    """
    graph = _adjacency_graph(postals, zcta_geometry_file)
    if not any(graph.values()):
        return postals, 0
    result = postals.copy()
    completed = 0
    for _ in range(max_iterations):
        snapshot = _staffing_snapshot(result, service, max_daily_jobs_per_technician, technician_count)
        annual = snapshot.set_index("region_seq")["annual_service_count"].to_dict()
        areas = snapshot.set_index("region_seq")["area_km2"].to_dict()
        targets = snapshot.set_index("region_seq")["candidate_technician_target"].to_dict()
        average_service_per_target = max(float(snapshot["annual_jobs_per_target_technician"].mean()), 1.0)
        average_area = max(float(snapshot["area_km2"].mean()), 1.0)
        best: tuple[float, str, int] | None = None
        for source in sorted(result["region_seq"].unique()):
            source_members = set(result.loc[result["region_seq"].eq(source), "POSTAL_CODE"].astype(str))
            if len(source_members) <= 1:
                continue
            source_load = annual[int(source)] / max(int(targets[int(source)]), 1)
            for postal_code in sorted(source_members):
                remaining = source_members - {postal_code}
                if len(_connected_components(remaining, graph)) > 1:
                    continue
                destinations = set(
                    result.loc[
                        result["POSTAL_CODE"].astype(str).isin(graph.get(postal_code, set())), "region_seq"
                    ].astype(int)
                ) - {int(source)}
                if not destinations:
                    continue
                zip_demand = float(result.loc[result["POSTAL_CODE"].astype(str).eq(postal_code), "service_count"].iloc[0])
                zip_area = float(result.loc[result["POSTAL_CODE"].astype(str).eq(postal_code), "area_km2"].iloc[0])
                for destination in destinations:
                    destination_load = annual[destination] / max(int(targets[destination]), 1)
                    before_service_gap = abs(source_load - destination_load) / average_service_per_target
                    after_service_gap = abs(
                        (annual[int(source)] - zip_demand) / max(int(targets[int(source)]), 1)
                        - (annual[destination] + zip_demand) / max(int(targets[destination]), 1)
                    ) / average_service_per_target
                    before_area_gap = abs(areas[int(source)] - areas[destination]) / average_area
                    after_area_gap = abs((areas[int(source)] - zip_area) - (areas[destination] + zip_area)) / average_area
                    improvement = (before_service_gap + area_balance_weight * before_area_gap) - (
                        after_service_gap + area_balance_weight * after_area_gap
                    )
                    if improvement > 0 and (best is None or improvement > best[0]):
                        best = (improvement, postal_code, destination)
        if best is None:
            break
        _, postal_code, destination = best
        result.loc[result["POSTAL_CODE"].astype(str).eq(postal_code), "region_seq"] = destination
        completed += 1
    return result, completed


def _repair_sparse_balanced_clusters(
    points: np.ndarray,
    weights: np.ndarray,
    labels: np.ndarray,
    region_count: int,
) -> np.ndarray:
    """Avoid candidate regions that contain a remote but negligible workload.

    The first balanced pass still has a compactness term.  With no radius
    constraint requested, a remote ZIP could otherwise become a one-ZIP,
    one-technician territory.  Merge clusters below half of the target demand,
    then split the heaviest viable clusters back to the requested count.
    """
    working = labels.astype(int).copy()
    target = float(weights.sum()) / max(region_count, 1)
    minimum = target * 0.50
    while True:
        unique = sorted(set(working.tolist()))
        totals = {label: float(weights[working == label].sum()) for label in unique}
        sparse = [label for label in unique if totals[label] < minimum]
        healthy = [label for label in unique if totals[label] >= minimum]
        if not sparse or not healthy:
            break
        source = min(sparse, key=lambda label: (totals[label], label))
        source_mask = working == source
        healthy_centers = np.vstack(
            [np.average(points[working == label], axis=0, weights=weights[working == label]) for label in healthy]
        )
        nearest = np.argmin(_haversine_km(points[source_mask], healthy_centers), axis=1)
        source_indices = np.flatnonzero(source_mask)
        for point_index, center_index in zip(source_indices, nearest):
            working[point_index] = healthy[int(center_index)]
    while len(set(working.tolist())) < region_count:
        totals = {label: float(weights[working == label].sum()) for label in set(working.tolist())}
        candidates = sorted(
            (label for label in totals if int((working == label).sum()) >= 2),
            key=lambda label: (-totals[label], label),
        )
        if not candidates:
            break
        source = candidates[0]
        source_indices = np.flatnonzero(working == source)
        split = _weighted_kmeans(points[source_indices], weights[source_indices], 2)
        if len(set(split.tolist())) < 2:
            break
        new_label = max(set(working.tolist())) + 1
        working[source_indices[split == 1]] = new_label
    # Stable, geographic ordering keeps filenames and tables reproducible.
    centers = {
        label: np.average(points[working == label], axis=0, weights=weights[working == label])
        for label in set(working.tolist())
    }
    ordered = sorted(centers, key=lambda label: (float(centers[label][0]), float(centers[label][1]), label))
    return np.array([{label: index for index, label in enumerate(ordered)}[label] for label in working], dtype=int)


def _region_daily_demand(service_df: pd.DataFrame, postal_df: pd.DataFrame) -> pd.DataFrame:
    """Compute region peaks from real dates, never by summing ZIP-level peaks."""
    joined = service_df.merge(postal_df[["POSTAL_CODE", "region_seq"]], on="POSTAL_CODE", how="inner")
    dated = joined.dropna(subset=["service_date"])
    if dated.empty:
        return pd.DataFrame({"region_seq": sorted(postal_df["region_seq"].unique())}).assign(
            avg_daily_jobs=0.0, p95_daily_jobs=0.0, max_daily_jobs=0
        )
    day_span = max(1, int((dated["service_date"].max() - dated["service_date"].min()).days) + 1)
    daily = dated.groupby(["region_seq", "service_date"])["_job_key"].nunique().rename("daily_jobs").reset_index()
    return daily.groupby("region_seq", as_index=False)["daily_jobs"].agg(
        avg_daily_jobs=lambda values: float(values.sum()) / day_span,
        p95_daily_jobs=lambda values: float(values.quantile(0.95)),
        max_daily_jobs="max",
    )


def _fair_staffing_targets(
    required: pd.Series,
    technician_count: int,
    workload: pd.Series | None = None,
) -> pd.Series:
    """Set regional staffing quotas before assigning homes by distance.

    Peak capacity is a hard minimum.  When the roster has additional people,
    distribute them by sustained workload rather than letting every overflow
    technician fall into the geographically nearest region.
    """
    required_values = [max(1, int(value)) for value in required.tolist()]
    target_total = int(technician_count)
    if workload is None:
        workload_values = [1.0] * len(required_values)
    else:
        workload_values = [max(0.0, float(value)) for value in workload.reindex(required.index).fillna(0.0).tolist()]
    if not any(workload_values):
        workload_values = [1.0] * len(required_values)
    allocation = [0] * len(required_values)
    if target_total <= 0:
        return pd.Series(allocation, index=required.index, dtype=int)
    for index in sorted(range(len(required_values)), key=lambda idx: (-required_values[idx], idx)):
        if sum(allocation) >= target_total:
            break
        allocation[index] = 1
    while sum(allocation) < min(sum(required_values), target_total):
        remaining = [required_values[index] - allocation[index] for index in range(len(required_values))]
        total_remaining = sum(max(0, value) for value in remaining)
        if total_remaining <= 0:
            break
        slots_left = target_total - sum(allocation)
        raw = [slots_left * max(0, value) / total_remaining for value in remaining]
        whole = [min(max(0, remaining[index]), int(np.floor(raw[index]))) for index in range(len(raw))]
        if sum(whole) == 0:
            best = max(range(len(remaining)), key=lambda idx: (remaining[idx], -idx))
            allocation[best] += 1
            continue
        for index, count in enumerate(whole):
            allocation[index] += count
        for index in sorted(
            range(len(raw)),
            key=lambda idx: (raw[idx] - np.floor(raw[idx]), remaining[idx], workload_values[idx], -idx),
            reverse=True,
        ):
            if sum(allocation) >= target_total:
                break
            if allocation[index] < required_values[index]:
                allocation[index] += 1
    if sum(allocation) < target_total:
        # Divisor allocation keeps workload per technician as even as possible.
        # Stable ties preserve reproducible candidate outputs.
        while sum(allocation) < target_total:
            best = max(
                range(len(allocation)),
                key=lambda index: (workload_values[index] / max(allocation[index] + 1, 1), -index),
            )
            allocation[best] += 1
    return pd.Series(allocation, index=required.index, dtype=int)


def _assign_technicians(technicians: pd.DataFrame, regions: pd.DataFrame) -> pd.DataFrame:
    homes = technicians[["latitude", "longitude"]].to_numpy(dtype=float)
    centers = regions[["centroid_latitude", "centroid_longitude"]].to_numpy(dtype=float)
    cost = _haversine_km(homes, centers)
    targets = regions["candidate_technician_target"].astype(int).to_list()
    slots = [region_idx for region_idx, count in enumerate(targets) for _ in range(count)]
    assignment = np.full(len(technicians), -1, dtype=int)
    stage = np.full(len(technicians), "overflow_nearest_region", dtype=object)
    if slots:
        slot_cost = cost[:, slots]
        try:
            from scipy.optimize import linear_sum_assignment

            tech_indices, slot_indices = linear_sum_assignment(slot_cost)
            for tech_idx, slot_idx in zip(tech_indices, slot_indices):
                assignment[tech_idx] = slots[int(slot_idx)]
                stage[tech_idx] = "required_capacity"
        except ImportError:
            # Deterministic fallback for environments without SciPy.
            remaining_tech = set(range(len(technicians)))
            remaining_slots = set(range(len(slots)))
            while remaining_tech and remaining_slots:
                tech_idx, slot_idx = min(
                    ((t, s) for t in remaining_tech for s in remaining_slots),
                    key=lambda pair: (slot_cost[pair[0], pair[1]], pair[0], pair[1]),
                )
                assignment[tech_idx] = slots[slot_idx]
                stage[tech_idx] = "required_capacity"
                remaining_tech.remove(tech_idx)
                remaining_slots.remove(slot_idx)
    for tech_idx in np.where(assignment < 0)[0]:
        assignment[tech_idx] = int(np.argmin(cost[tech_idx]))
    result = technicians.copy()
    result["assigned_region_seq"] = assignment + 1
    result["assignment_stage"] = stage
    result["home_to_region_centroid_km"] = [round(float(cost[idx, region]), 3) for idx, region in enumerate(assignment)]
    return result


def build_city_region_candidate(
    *,
    service_file: Path,
    profile_file: Path,
    city_name: str,
    region_count: int,
    algorithm: str,
    output_root: Path,
    max_daily_jobs_per_technician: int = 8,
    technician_city: str | None = None,
    selected_technician_codes: set[str] | None = None,
    plan_id: str | None = None,
    zcta_geometry_file: Path | None = None,
) -> CandidatePlanResult:
    """Create a reviewable candidate; all selected technicians are assigned once."""
    if max_daily_jobs_per_technician < 1:
        raise ValueError("MAX_DAILY_JOBS_PER_TECHNICIAN_MUST_BE_POSITIVE")
    if region_count < 1:
        raise ValueError("REGION_COUNT_MUST_BE_POSITIVE")
    service, rejects = _load_city_service(service_file, city_name)
    coverage, coverage_rejects = _load_city_zip_coverage(profile_file, city_name)
    rejects.extend(coverage_rejects)
    coverage_postals = set(coverage["POSTAL_CODE"])
    outside_coverage = service[~service["POSTAL_CODE"].isin(coverage_postals)].copy()
    if not outside_coverage.empty:
        for postal_code, group in outside_coverage.groupby("POSTAL_CODE"):
            rejects.append(
                {
                    "artifact": "service",
                    "record_id": str(postal_code),
                    "reason": "POSTAL_NOT_IN_PROFILE_COVERAGE",
                    "count": str(int(group["_job_key"].nunique())),
                }
            )
    service = service[service["POSTAL_CODE"].isin(coverage_postals)].copy()
    if service.empty:
        raise ValueError(f"NO_SERVICE_IN_PROFILE_ZIP_COVERAGE:{city_name}")
    zcta_file = zcta_geometry_file or na_data_path("zcta_geometry")
    coverage_postals_df, coordinate_rejects = _coverage_postals_with_demand(coverage, service, zcta_file)
    rejects.extend(coordinate_rejects)
    if coverage_postals_df.empty:
        raise ValueError(f"NO_CLUSTERABLE_PROFILE_ZIP_COVERAGE:{city_name}")
    if region_count > len(coverage_postals_df):
        raise ValueError(f"REGION_COUNT_EXCEEDS_POSTAL_COUNT:{region_count}>{len(coverage_postals_df)}")
    technicians, technician_rejects = _load_city_technicians(profile_file, city_name, technician_city)
    rejects.extend(technician_rejects)
    if selected_technician_codes is not None:
        selected_codes = {str(code).strip() for code in selected_technician_codes if str(code).strip()}
        technicians = technicians[technicians["SVC_ENGINEER_CODE"].isin(selected_codes)].copy()
        if technicians.empty:
            raise ValueError("NO_TECHNICIANS_SELECTED")
    selected_algorithm = algorithm.strip().lower()
    if selected_algorithm == "center_shared_radial":
        postal = _attach_postal_areas(coverage_postals_df, zcta_file)
        postal["region_seq"] = _center_shared_radial_labels(postal, region_count) + 1
        postal, contiguous_reassignment_count, remaining_contiguity_components = _enforce_contiguous_regions(postal, zcta_file)
        contiguous_growth_island_assignments = 0
    elif selected_algorithm in {"contiguous_balanced", "weighted_kmeans_staffing", "capacity_balanced_contiguous"}:
        planning_postals = _attach_postal_areas(coverage_postals_df, zcta_file)
        points = planning_postals[["latitude", "longitude"]].to_numpy(dtype=float)
        weights = planning_postals["service_count"].to_numpy(dtype=float)
        if selected_algorithm in {"contiguous_balanced", "capacity_balanced_contiguous"}:
            seed_indices = _initialize_weighted_center_indices(points, weights, region_count)
        else:
            seed_indices = _weighted_kmeans_seed_indices(points, weights, region_count)
        postal, contiguous_growth_island_assignments = _contiguous_nearest_growth(
            planning_postals,
            zcta_file,
            seed_indices,
            service_balance_weight=0.75 if selected_algorithm in {"contiguous_balanced", "capacity_balanced_contiguous"} else 0.0,
            area_balance_weight=0.45 if selected_algorithm in {"contiguous_balanced", "capacity_balanced_contiguous"} else 0.0,
        )
        contiguous_reassignment_count = 0
        remaining_contiguity_components = _remaining_disconnected_region_components(postal, zcta_file)
    else:
        postal = _cluster_postals(coverage_postals_df, region_count, algorithm)
        contiguous_growth_island_assignments = 0
        contiguous_reassignment_count = 0
        remaining_contiguity_components = 0
    if "area_km2" not in postal.columns:
        postal = _attach_postal_areas(postal, zcta_file)
    staffing_boundary_moves = 0
    if selected_algorithm == "capacity_balanced_contiguous":
        # Legacy-style capacity balancing: only a ZIP touching the destination
        # may move, and the source must remain connected.  This retains a
        # compact territory while reducing annual demand per planned technician.
        postal, staffing_boundary_moves = _optimize_boundaries_for_staffing(
            postal,
            service,
            zcta_file,
            max_daily_jobs_per_technician,
            len(technicians),
            area_balance_weight=0.45,
        )
        remaining_contiguity_components = _remaining_disconnected_region_components(postal, zcta_file)
    slug = _slug(city_name)
    if plan_id is None:
        roster_signature = hashlib.sha256(
            "|".join(sorted(technicians["SVC_ENGINEER_CODE"].astype(str))).encode("utf-8")
        ).hexdigest()[:8]
        plan_id = f"candidate_{slug}_{region_count}_{algorithm.strip().lower()}_{roster_signature}"
    postal["region_id"] = postal["region_seq"].map(lambda seq: f"{plan_id}_r{int(seq):02d}")
    postal["AREA_NAME"] = postal["region_seq"].map(lambda seq: f"Region {int(seq)}")
    postal["STRATEGIC_CITY_NAME"] = city_name
    # The Region Plan v2 workbook requires an explicit DMS/DMS2 membership
    # type.  This comes from the selected Zip Coverage, not from service
    # history, so preserve it in the candidate artifact for later promotion.
    postal["area_type"] = postal.get("SVC_CENTER_TYPE", pd.Series("", index=postal.index)).map(_clean).str.upper()
    region_summary = (
        postal.groupby(["region_seq", "region_id", "AREA_NAME"], as_index=False)
        .agg(
            postal_count=("POSTAL_CODE", "nunique"),
            annual_service_count=("service_count", "sum"),
            area_km2=("area_km2", "sum"),
            centroid_latitude=("latitude", lambda values: float(np.average(values, weights=postal.loc[values.index, "service_count"]))),
            centroid_longitude=("longitude", lambda values: float(np.average(values, weights=postal.loc[values.index, "service_count"]))),
        )
        .sort_values("region_seq")
        .reset_index(drop=True)
    )
    region_summary = region_summary.merge(_region_daily_demand(service, postal), on="region_seq", how="left")
    region_summary[["avg_daily_jobs", "p95_daily_jobs", "max_daily_jobs"]] = region_summary[
        ["avg_daily_jobs", "p95_daily_jobs", "max_daily_jobs"]
    ].fillna(0.0)
    region_summary["required_technician_count"] = np.ceil(
        region_summary["p95_daily_jobs"] / max_daily_jobs_per_technician
    ).astype(int)
    region_summary["required_technician_count"] = region_summary["required_technician_count"].clip(lower=1)
    region_summary["max_daily_jobs_per_technician"] = int(max_daily_jobs_per_technician)
    region_summary["algorithm"] = algorithm.strip().lower()
    region_summary["candidate_technician_target"] = _fair_staffing_targets(
        region_summary["required_technician_count"], len(technicians), region_summary["annual_service_count"]
    )
    assignments = _assign_technicians(technicians, region_summary)
    assignments = assignments.merge(region_summary[["region_seq", "region_id", "AREA_NAME"]], left_on="assigned_region_seq", right_on="region_seq", how="left")
    assignments["policy_mode"] = "candidate_home_distance_capacity"
    staff_counts = assignments.groupby("assigned_region_seq")["SVC_ENGINEER_CODE"].nunique()
    region_summary["assigned_technician_count"] = region_summary["region_seq"].map(staff_counts).fillna(0).astype(int)
    region_summary["technician_shortfall"] = (region_summary["required_technician_count"] - region_summary["assigned_technician_count"]).clip(lower=0)
    region_summary["mean_assigned_home_distance_km"] = region_summary["region_seq"].map(
        assignments.groupby("assigned_region_seq")["home_to_region_centroid_km"].mean()
    ).round(3)
    technician_names = assignments.groupby("assigned_region_seq")["SVC_ENGINEER_NAME"].agg(
        lambda values: " | ".join(sorted({_clean(value) or "(name unavailable)" for value in values}))
    )
    region_summary["assigned_technician_names"] = region_summary["region_seq"].map(technician_names).fillna("")
    region_summary["annual_service_per_assigned_technician"] = (
        region_summary["annual_service_count"] / region_summary["assigned_technician_count"].replace(0, np.nan)
    ).round(2)
    region_summary["avg_daily_jobs_per_assigned_technician"] = (
        region_summary["avg_daily_jobs"] / region_summary["assigned_technician_count"].replace(0, np.nan)
    ).round(3)
    region_summary["max_daily_jobs_per_assigned_technician"] = (
        region_summary["max_daily_jobs"] / region_summary["assigned_technician_count"].replace(0, np.nan)
    ).round(3)

    candidate_dir = output_root / slug / plan_id
    candidate_dir.mkdir(parents=True, exist_ok=True)
    area_map_postals = postal[list(AREA_MAP_POSTAL_COLUMNS)].sort_values(["region_seq", "POSTAL_CODE"])
    region_postals_path = candidate_dir / f"fixed_region_postal_{slug}_{region_count}_candidate.csv"
    area_map_postals.to_csv(region_postals_path, index=False, encoding="utf-8-sig")
    # Canonical plan artifact uses lower-case names; the area-map artifact above
    # retains its established column names.
    canonical_postals = area_map_postals.rename(columns={"POSTAL_CODE": "postal_code", "region_seq": "primary_region_seq"})[
        ["postal_code", "primary_region_seq", "area_type"]
    ]
    canonical_postals.to_csv(candidate_dir / "region_postals.csv", index=False, encoding="utf-8-sig")
    technician_assignments = assignments.rename(columns={"SVC_ENGINEER_CODE": "employee_code"})[
        ["employee_code", "SVC_ENGINEER_NAME", "assigned_region_seq", "region_id", "AREA_NAME", "policy_mode", "assignment_stage", "home_to_region_centroid_km"]
    ].rename(columns={"AREA_NAME": "assigned_region_name"})
    technician_assignments_path = candidate_dir / "technician_assignments.csv"
    technician_assignments.to_csv(technician_assignments_path, index=False, encoding="utf-8-sig")
    technician_policy = technician_assignments.rename(columns={"employee_code": "SVC_ENGINEER_CODE"})
    technician_policy.to_csv(candidate_dir / "technician_region_policy.csv", index=False, encoding="utf-8-sig")
    region_summary_path = candidate_dir / "region_summary.csv"
    region_summary.to_csv(region_summary_path, index=False, encoding="utf-8-sig")
    evidence_path = candidate_dir / "technician_assignment_evidence.csv"
    assignments.to_csv(evidence_path, index=False, encoding="utf-8-sig")
    rejects_path = candidate_dir / "rejects.csv"
    pd.DataFrame(rejects, columns=["artifact", "record_id", "reason", "count"]).to_csv(rejects_path, index=False, encoding="utf-8-sig")
    manifest = {
        "schema": "region-candidate-home-allocation/v1",
        "lifecycle_stage": "candidate",
        "review_activation_required": True,
        "city": city_name,
        "plan_id": plan_id,
        "algorithm": algorithm.strip().lower(),
        "region_count": int(region_count),
        "max_daily_jobs_per_technician": int(max_daily_jobs_per_technician),
        "radius_constraint": "not_applied",
        "geographic_growth_policy": (
            "adjacent ZIP only; choose the available boundary ZIP nearest the region's demand-weighted centroid; "
            "then rebalance connected boundary ZIPs by service per planned technician"
            if selected_algorithm == "capacity_balanced_contiguous"
            else "shared demand-centre radial sectors; angular cuts balance service demand and no post-sector boundary rebalancing"
            if selected_algorithm == "center_shared_radial"
            else "adjacent ZIP only; choose the available boundary ZIP nearest the region's demand-weighted centroid; no post-growth boundary rebalancing"
        ),
        "fixed_or_exception_technician_policy": "none; every selected technician assigned once",
        "staffing_basis": "ceil(region p95 observed daily jobs / max_daily_jobs_per_technician); available roster then apportioned by annual service workload before home-distance assignment",
        "staffing_peak_basis": "p95_daily_jobs",
        "inputs": {
            "service_file": str(service_file),
            "service_sha256": _sha256(service_file),
            "profile_file": str(profile_file),
            "profile_sha256": _sha256(profile_file),
            "technician_city_filter": technician_city or city_name,
            "selected_technician_codes": sorted(technicians["SVC_ENGINEER_CODE"].astype(str).tolist()),
        },
        "row_accounting": {
            "valid_service_jobs": int(len(service)),
            "service_jobs_excluded_outside_profile_zip_coverage": int(outside_coverage["_job_key"].nunique()),
            "coverage_postal_count": int(len(coverage)),
            "clustered_coverage_postal_count": int(len(postal)),
            "coverage_postals_excluded_missing_coordinates": int(len(coordinate_rejects)),
            "contiguous_zip_reassignments": int(contiguous_reassignment_count),
            "contiguous_growth_island_assignments": int(contiguous_growth_island_assignments),
            "remaining_disconnected_zip_components": int(remaining_contiguity_components),
            "staffing_boundary_zip_moves": int(staffing_boundary_moves),
            "postal_count": int(len(postal)),
            "selected_technicians": int(len(technicians)),
            "assigned_technicians": int(len(assignments)),
            "reject_count": int(len(rejects)),
            "required_technicians": int(region_summary["required_technician_count"].sum()),
            "candidate_technician_target": int(region_summary["candidate_technician_target"].sum()),
            "technician_shortfall": int(region_summary["technician_shortfall"].sum()),
        },
        "artifacts": {},
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    for path in candidate_dir.glob("*.csv"):
        with path.open(encoding="utf-8-sig") as handle:
            row_count = max(0, sum(1 for _ in handle) - 1)
        manifest["artifacts"][path.name] = {"sha256": _sha256(path), "rows": row_count}
    manifest_path = candidate_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return CandidatePlanResult(
        output_dir=candidate_dir,
        plan_id=plan_id,
        region_postals_path=region_postals_path,
        technician_assignments_path=technician_assignments_path,
        region_summary_path=region_summary_path,
        evidence_path=evidence_path,
        rejects_path=rejects_path,
        manifest_path=manifest_path,
    )
