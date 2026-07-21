from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from smart_routing.data_catalog import na_data_path

ROOT = Path(__file__).resolve().parents[2]
SERVICE_FILE = na_data_path("service_geocoded")
SOURCE_PROFILE_FILE = na_data_path("profile_raw")
PROFILE_FILE = na_data_path("profile_production")
# This builder writes drafts only. Promotion to reviewed/seed paths is explicit.
FIXED_REGION_DIR = na_data_path("region_candidates_dir")
PRODUCTION_DIR = na_data_path("region_candidates_dir")
OUTPUT_DIR = na_data_path("reports_dir")


AREA_TYPE_DMS_CORE = "DMS_CORE"
AREA_TYPE_OVERLAP = "OVERLAP"
AREA_TYPE_DMS2_EXCLUSIVE = "DMS2_EXCLUSIVE"
ASC_TYPES = {"ASC"}
RELEVANT_TYPES = {"DMS", "DMS2"}
DMS_TARGET_JOBS_PER_DAY = 4.5
DMS_CORE_MAX_HOME_DISTANCE_KM = 18.0
DMS_OVERLAP_MAX_HOME_DISTANCE_KM = 35.0
DMS_HOME_ANCHOR_MIN_ZIPS = 1
DMS_CORE_MIN_DENSITY_QUANTILE = 0.50
DMS_CORE_USABLE_CAPACITY_RATIO = 0.72
DMS_CORE_MAX_SHARE = 0.45
CITY_AREA_TYPE_TARGET_SHARES = {
    "Los Angeles, CA": {
        "dms_core": {"min": 0.30, "preferred": 0.35, "max": 0.40},
        "overlap": {"min": 0.30, "preferred": 0.35, "max": 0.40},
    },
    "North Jersey, NJ": {
        "dms_core": {"min": 0.17, "preferred": 0.20, "max": 0.24},
        "overlap": {"min": 0.30, "preferred": 0.35, "max": 0.40},
    },
    "Philadelphia, PA": {
        "dms_core": {"min": 0.30, "preferred": 0.35, "max": 0.40},
        "overlap": {"min": 0.30, "preferred": 0.35, "max": 0.40},
    },
    "San Diego, CA": {
        "dms_core": {"min": 0.55, "preferred": 0.60, "max": 0.65},
        "overlap": {"min": 0.25, "preferred": 0.30, "max": 0.35},
    },
    "Washington, DC": {
        "dms_core": {"min": 0.17, "preferred": 0.20, "max": 0.24},
        "overlap": {"min": 0.30, "preferred": 0.35, "max": 0.40},
    },
}
CITY_DMS_CORE_MAX_HOME_DISTANCE_KM = {
    "Philadelphia, PA": 32.0,
    "San Diego, CA": 45.0,
}
CITY_DMS_CORE_MIN_DENSITY_QUANTILE = {
    "Philadelphia, PA": 0.35,
    "San Diego, CA": 0.20,
}
HOME_CITY_OVERRIDES = {
    "AI103541": "North Jersey, NJ",
}


CITY_SPECS = [
    {
        "city": "Los Angeles, CA",
        "target_region_count": 3,
        "output_file": "los_angeles_fixed_region_zip_6_area_type.csv",
        "fixed_region_file": "fixed_region_postal_los_angeles_ca_6_area_type.csv",
    },
    {
        "city": "North Jersey, NJ",
        "target_region_count": 3,
        "output_file": "north_jersey_nj_fixed_region_zip_5_area_type.csv",
        "fixed_region_file": "fixed_region_postal_north_jersey_nj_5_area_type.csv",
    },
    {
        "city": "Philadelphia, PA",
        "target_region_count": 3,
        "output_file": "philadelphia_pa_fixed_region_zip_3_area_type.csv",
        "fixed_region_file": "fixed_region_postal_philadelphia_pa_3_area_type.csv",
    },
    {
        "city": "San Diego, CA",
        "target_region_count": 3,
        "output_file": "san_diego_ca_fixed_region_zip_3_area_type.csv",
        "fixed_region_file": "fixed_region_postal_san_diego_ca_3_area_type.csv",
    },
    {
        "city": "Washington, DC",
        "target_region_count": 3,
        "output_file": "washington_dc_fixed_region_zip_3_area_type.csv",
        "fixed_region_file": "fixed_region_postal_washington_dc_3_area_type.csv",
    },
]


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def _norm_zip(value: object) -> str:
    text = _clean_text(value)
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(5) if text else ""


def _normalize_center_bucket(value: object) -> str:
    upper = _clean_text(value).upper()
    if upper in {"DMS", "DMS2"}:
        return upper
    return "ASC"


def _slugify_city_name(city_name: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(city_name))
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _minmax(series: pd.Series, default: float = 0.0) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if values.notna().sum() == 0:
        return pd.Series(default, index=series.index, dtype=float)
    min_v = float(values.min())
    max_v = float(values.max())
    if math.isclose(min_v, max_v):
        return pd.Series(1.0, index=series.index, dtype=float)
    return ((values - min_v) / (max_v - min_v)).fillna(default).astype(float)


def _read_service() -> pd.DataFrame:
    df = pd.read_csv(SERVICE_FILE, encoding="utf-8-sig", low_memory=False)
    if "GSFS_RECEIPT_NO" in df.columns:
        df = df.drop_duplicates(subset=["GSFS_RECEIPT_NO"]).copy()
    df["POSTAL_CODE"] = df["POSTAL_CODE"].map(_norm_zip)
    df["STRATEGIC_CITY_NAME"] = df["STRATEGIC_CITY_NAME"].map(_clean_text)
    df["SVC_CENTER_TYPE"] = df["SVC_CENTER_TYPE"].map(_normalize_center_bucket)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    date_text = pd.Series("", index=df.index, dtype="object")
    for date_col in ["PROMISE_DATE", "REPAIR_END_DATE_YYYYMMDD", "REPAIR_RECEIPT_DATE_YYYYMMDD"]:
        if date_col in df.columns:
            candidate = df[date_col].astype(str).str.replace(r"\.0+$", "", regex=True).str.strip()
            date_text = date_text.mask(date_text.eq(""), candidate)
    df["service_date_key"] = date_text
    df = df[df["POSTAL_CODE"].ne("") & df["latitude"].notna() & df["longitude"].notna()].copy()
    return df


def _read_dms_homes() -> pd.DataFrame:
    # Source 4. Address keeps the strategic city in the State column.
    # The production copy has geocoded coordinates but State is normalized to the US state code.
    source_address_df = pd.read_excel(SOURCE_PROFILE_FILE, sheet_name="4. Address")
    geocoded_address_df = pd.read_excel(PROFILE_FILE, sheet_name="4. Address")
    source_address_df["SVC_ENGINEER_CODE"] = source_address_df["SVC_ENGINEER_CODE"].map(_clean_text)
    source_address_df["STRATEGIC_CITY_NAME"] = source_address_df["State"].map(_clean_text)
    source_address_df["STRATEGIC_CITY_NAME"] = source_address_df.apply(
        lambda row: HOME_CITY_OVERRIDES.get(_clean_text(row["SVC_ENGINEER_CODE"]), row["STRATEGIC_CITY_NAME"]),
        axis=1,
    )
    geocoded_address_df["SVC_ENGINEER_CODE"] = geocoded_address_df["SVC_ENGINEER_CODE"].map(_clean_text)
    home_df = source_address_df.merge(
        geocoded_address_df[
            [
                "SVC_ENGINEER_CODE",
                "matched_address",
                "match_indicator",
                "match_type",
                "latitude",
                "longitude",
                "geocoded_date",
                "source",
            ]
        ],
        on="SVC_ENGINEER_CODE",
        how="left",
    )
    home_df["SVC_CENTER_TYPE"] = "DMS"
    home_df["latitude"] = pd.to_numeric(home_df["latitude"], errors="coerce")
    home_df["longitude"] = pd.to_numeric(home_df["longitude"], errors="coerce")
    return home_df[home_df["SVC_CENTER_TYPE"].eq("DMS")].copy()


def _read_zip_coverage() -> pd.DataFrame:
    zip_df = pd.read_excel(PROFILE_FILE, sheet_name="1. Zip Coverage")
    zip_df["SVC_ENGINEER_CODE"] = zip_df["SVC_ENGINEER_CODE"].map(_clean_text)
    zip_df["STRATEGIC_CITY_NAME"] = zip_df["STRATEGIC_CITY_NAME"].map(_clean_text)
    zip_df["SVC_CENTER_TYPE"] = zip_df["SVC_CENTER_TYPE"].map(lambda value: _clean_text(value).upper())
    zip_df["POSTAL_CODE"] = zip_df["POSTAL_CODE"].map(_norm_zip)
    return zip_df


def _city_home_locations(city: str, city_service: pd.DataFrame, home_df: pd.DataFrame) -> pd.DataFrame:
    city_home = home_df[home_df["STRATEGIC_CITY_NAME"].eq(city)].copy()
    if city_home.empty:
        return city_home

    service_zip_centers = (
        city_service.groupby("POSTAL_CODE")
        .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"))
        .reset_index()
    )
    coverage_df = _read_zip_coverage()
    coverage_city = coverage_df[
        coverage_df["STRATEGIC_CITY_NAME"].eq(city)
        & coverage_df["SVC_CENTER_TYPE"].eq("DMS")
    ].copy()

    rows: list[dict[str, object]] = []
    for _, home_row in city_home.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).iterrows():
        code = _clean_text(home_row.get("SVC_ENGINEER_CODE"))
        lat = pd.to_numeric(pd.Series([home_row.get("latitude")]), errors="coerce").iloc[0]
        lon = pd.to_numeric(pd.Series([home_row.get("longitude")]), errors="coerce").iloc[0]
        source = "address"
        if pd.isna(lat) or pd.isna(lon):
            svc_by_engineer = city_service[city_service["SVC_ENGINEER_CODE"].map(_clean_text).eq(code)].copy()
            if not svc_by_engineer.empty:
                lat = float(svc_by_engineer["latitude"].mean())
                lon = float(svc_by_engineer["longitude"].mean())
                source = "service_centroid"
        if pd.isna(lat) or pd.isna(lon):
            coverage_zips = coverage_city.loc[coverage_city["SVC_ENGINEER_CODE"].eq(code), "POSTAL_CODE"].dropna().astype(str)
            covered_centers = service_zip_centers[service_zip_centers["POSTAL_CODE"].isin(set(coverage_zips))]
            if not covered_centers.empty:
                lat = float(covered_centers["latitude"].mean())
                lon = float(covered_centers["longitude"].mean())
                source = "coverage_zip_centroid"
        rows.append(
            {
                "SVC_ENGINEER_CODE": code,
                "STRATEGIC_CITY_NAME": city,
                "latitude": lat,
                "longitude": lon,
                "home_location_source": source if pd.notna(lat) and pd.notna(lon) else "missing",
            }
        )
    result = pd.DataFrame(rows)
    result["latitude"] = pd.to_numeric(result["latitude"], errors="coerce")
    result["longitude"] = pd.to_numeric(result["longitude"], errors="coerce")
    return result


def _zip_metrics(city: str, service_df: pd.DataFrame, home_df: pd.DataFrame) -> pd.DataFrame:
    city_service = service_df[service_df["STRATEGIC_CITY_NAME"].eq(city)].copy()
    if city_service.empty:
        return pd.DataFrame()

    city_home = _city_home_locations(city, city_service, home_df)
    city_home_geo = city_home[city_home["latitude"].notna() & city_home["longitude"].notna()].copy()
    rows: list[dict[str, object]] = []
    for postal_code, group in city_service.groupby("POSTAL_CODE", dropna=False):
        relevant_group = group[group["SVC_CENTER_TYPE"].isin(RELEVANT_TYPES)].copy()
        if relevant_group.empty:
            continue
        center_lat = float(relevant_group["latitude"].mean())
        center_lon = float(relevant_group["longitude"].mean())
        dms_calls = int(relevant_group.loc[relevant_group["SVC_CENTER_TYPE"].eq("DMS"), "GSFS_RECEIPT_NO"].nunique())
        dms2_calls = int(relevant_group.loc[relevant_group["SVC_CENTER_TYPE"].eq("DMS2"), "GSFS_RECEIPT_NO"].nunique())
        relevant_calls = int(dms_calls + dms2_calls)
        asc_calls = int(group.loc[group["SVC_CENTER_TYPE"].isin(ASC_TYPES), "GSFS_RECEIPT_NO"].nunique())
        point_distances = [
            _haversine_km(center_lat, center_lon, float(row.latitude), float(row.longitude))
            for row in relevant_group.itertuples(index=False)
        ]
        avg_radius_km = max(float(np.mean(point_distances)), 0.25) if point_distances else 1.0
        nearest_home_code = ""
        nearest_home_distance = np.nan
        if not city_home_geo.empty:
            home_distance_rows = [
                (
                    _haversine_km(center_lat, center_lon, float(row.latitude), float(row.longitude)),
                    _clean_text(row.SVC_ENGINEER_CODE),
                )
                for row in city_home_geo.itertuples(index=False)
            ]
            home_distances = [distance for distance, _ in home_distance_rows]
            nearest_home_distance, nearest_home_code = min(home_distance_rows, key=lambda item: item[0])
        else:
            dms_points = city_service[city_service["SVC_CENTER_TYPE"].eq("DMS")]
            home_distances = [
                _haversine_km(center_lat, center_lon, float(row.latitude), float(row.longitude))
                for row in dms_points.itertuples(index=False)
            ]
        rows.append(
            {
                "POSTAL_CODE": str(postal_code),
                "latitude": center_lat,
                "longitude": center_lon,
                "relevant_calls": relevant_calls,
                "dms_calls": dms_calls,
                "dms2_calls": dms2_calls,
                "asc_calls": asc_calls,
                "dms_share_zip": dms_calls / relevant_calls if relevant_calls else 0.0,
                "density_raw": relevant_calls / avg_radius_km,
                "dms_avg_distance_km": float(np.mean(home_distances)) if home_distances else np.nan,
                "nearest_dms_home_distance_km": float(nearest_home_distance) if pd.notna(nearest_home_distance) else np.nan,
                "nearest_dms_home_code": nearest_home_code,
            }
        )

    zip_df = pd.DataFrame(rows)
    if zip_df.empty:
        return zip_df
    zip_df["density_score"] = _minmax(zip_df["density_raw"])
    zip_df["demand_score"] = _minmax(zip_df["relevant_calls"])
    zip_df["dms_access_score"] = 1.0 - _minmax(zip_df["dms_avg_distance_km"])
    zip_df["dms_fit_score"] = (
        0.70 * zip_df["dms_share_zip"]
        + 0.15 * zip_df["density_score"]
        + 0.10 * zip_df["dms_access_score"].fillna(0.0)
        + 0.05 * zip_df["demand_score"]
    )
    return zip_df


def _dms_home_capacity_stats(city: str, service_df: pd.DataFrame, home_df: pd.DataFrame) -> dict[str, float]:
    city_service = service_df[
        service_df["STRATEGIC_CITY_NAME"].eq(city)
        & service_df["SVC_CENTER_TYPE"].isin(RELEVANT_TYPES)
    ].copy()
    city_home = home_df[home_df["STRATEGIC_CITY_NAME"].eq(city)].copy()
    city_home_locations = _city_home_locations(city, city_service, home_df)
    service_day_count = int(city_service["service_date_key"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique())
    service_day_count = max(service_day_count, 1)
    dms_home_count = int(city_home["SVC_ENGINEER_CODE"].dropna().astype(str).nunique())
    dms_home_location_count = int(
        city_home_locations[
            city_home_locations["latitude"].notna()
            & city_home_locations["longitude"].notna()
        ]["SVC_ENGINEER_CODE"].dropna().astype(str).nunique()
    )
    total_calls = int(city_service["GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
    capacity_calls = float(dms_home_count * service_day_count * DMS_TARGET_JOBS_PER_DAY)
    capacity_share = capacity_calls / max(float(total_calls), 1.0)
    target_shares = CITY_AREA_TYPE_TARGET_SHARES.get(city)
    if target_shares:
        core_range = target_shares["dms_core"]
        overlap_range = target_shares["overlap"]
        core_share = float(core_range["preferred"])
        overlap_share = float(overlap_range["preferred"])
        core_min_share = float(core_range["min"])
        core_max_share = float(core_range["max"])
        overlap_min_share = float(overlap_range["min"])
        overlap_max_share = float(overlap_range["max"])
    else:
        # Capacity is an upper bound, not a fill target. Keep DMS core conservative so
        # low-density outer ZIPs are not pulled in only to fill the daily target.
        core_share = min(capacity_share * DMS_CORE_USABLE_CAPACITY_RATIO, DMS_CORE_MAX_SHARE)
        core_share = max(core_share, 0.08 if dms_home_count else 0.0)
        overlap_share = 0.20
        core_min_share = core_share
        core_max_share = core_share
        overlap_min_share = overlap_share
        overlap_max_share = overlap_share
    if core_share + overlap_share > 0.92:
        overlap_share = max(0.05, 0.92 - core_share)
    return {
        "dms_home_count": float(dms_home_count),
        "dms_home_location_count": float(dms_home_location_count),
        "service_day_count": float(service_day_count),
        "dms_target_jobs_per_day": float(DMS_TARGET_JOBS_PER_DAY),
        "dms_capacity_calls": capacity_calls,
        "dms_capacity_share": capacity_share,
        "dms_core_target_share": core_share,
        "dms_core_min_share": core_min_share,
        "dms_core_max_share": core_max_share,
        "overlap_target_share": overlap_share,
        "overlap_min_share": overlap_min_share,
        "overlap_max_share": overlap_max_share,
        "dms2_target_share": max(0.0, 1.0 - core_share - overlap_share),
    }


def _overlap_share(dms_share: float) -> float:
    mix_index = 1.0 - abs(dms_share - 0.5) * 2.0
    overlap = 0.20 * max(0.0, min(1.0, mix_index))
    return min(overlap, max(0.0, min(dms_share, 1.0 - dms_share) * 0.80))


def _city_ratio_stats(zip_df: pd.DataFrame) -> dict[str, float]:
    total_calls = float(zip_df["relevant_calls"].sum())
    dms_calls = float(zip_df["dms_calls"].sum())
    dms2_calls = float(zip_df["dms2_calls"].sum())
    dms_share = dms_calls / max(dms_calls + dms2_calls, 1.0)
    overlap_target = _overlap_share(dms_share)
    dms_core_target = max(0.0, dms_share - overlap_target / 2.0)
    dms2_target = max(0.0, 1.0 - dms_core_target - overlap_target)
    stats = {
        "dms_share": dms_share,
        "dms_core_target_share": dms_core_target,
        "overlap_target_share": overlap_target,
        "dms2_target_share": dms2_target,
        "city_relevant_calls": total_calls,
        "city_dms_calls": dms_calls,
        "city_dms2_calls": dms2_calls,
    }
    return stats


def _spatial_cluster_zips(zip_df: pd.DataFrame, target_count: int) -> pd.DataFrame:
    work = zip_df.copy().reset_index(drop=True)
    n_clusters = min(max(1, int(target_count)), len(work))
    if n_clusters <= 1:
        work["_spatial_cluster"] = 1
        return work
    mean_lat = float(work["latitude"].mean())
    lat_scale = 111.0
    lon_scale = 111.0 * max(math.cos(math.radians(mean_lat)), 0.25)
    coords = np.column_stack(
        [
            (work["longitude"].to_numpy(dtype=float) - float(work["longitude"].mean())) * lon_scale,
            (work["latitude"].to_numpy(dtype=float) - mean_lat) * lat_scale,
        ]
    )
    weights = np.maximum(work["relevant_calls"].to_numpy(dtype=float), 1.0)
    model = KMeans(n_clusters=n_clusters, random_state=42, n_init=50)
    labels = model.fit_predict(coords, sample_weight=weights)
    work["_spatial_cluster"] = labels + 1
    return work


def _cluster_metrics(clustered_df: pd.DataFrame) -> pd.DataFrame:
    metrics = (
        clustered_df.groupby("_spatial_cluster", dropna=False)
        .agg(
            zip_count=("POSTAL_CODE", "nunique"),
            latitude=("latitude", "mean"),
            longitude=("longitude", "mean"),
            relevant_calls=("relevant_calls", "sum"),
            dms_calls=("dms_calls", "sum"),
            dms2_calls=("dms2_calls", "sum"),
            dms_fit_score=("dms_fit_score", lambda s: float(np.average(s, weights=clustered_df.loc[s.index, "relevant_calls"]))),
        )
        .reset_index()
    )
    metrics["cluster_dms_share"] = metrics["dms_calls"] / metrics["relevant_calls"].replace(0, np.nan)
    metrics["cluster_dms_share"] = metrics["cluster_dms_share"].fillna(0.0)
    metrics["demand_score"] = _minmax(metrics["relevant_calls"])
    metrics["dms_region_score"] = (
        0.65 * metrics["cluster_dms_share"]
        + 0.25 * metrics["dms_fit_score"]
        + 0.10 * metrics["demand_score"]
    )
    return metrics


def _centroid_distance(row_a: pd.Series, row_b: pd.Series) -> float:
    return _haversine_km(
        float(row_a["latitude"]),
        float(row_a["longitude"]),
        float(row_b["latitude"]),
        float(row_b["longitude"]),
    )


def _min_distance_to_selected(candidate: pd.Series, selected_rows: pd.DataFrame) -> float:
    if selected_rows.empty:
        return 0.0
    return min(_centroid_distance(candidate, selected_row) for _, selected_row in selected_rows.iterrows())


def _assign_cluster_area_types(cluster_metrics: pd.DataFrame, stats: dict[str, float]) -> dict[int, str]:
    metrics = cluster_metrics.copy().reset_index(drop=True)
    total_calls = max(float(metrics["relevant_calls"].sum()), 1.0)
    dms_core_target_calls = float(stats.get("dms_core_target_share", 0.0)) * total_calls
    overlap_target_calls = float(stats.get("overlap_target_share", 0.0)) * total_calls
    assignments = {int(row["_spatial_cluster"]): AREA_TYPE_DMS2_EXCLUSIVE for _, row in metrics.iterrows()}
    if metrics.empty:
        return assignments

    seed_idx = int(metrics.sort_values(["dms_region_score", "relevant_calls"], ascending=[False, False]).index[0])
    selected_indices = {seed_idx}
    dms_calls_selected = float(metrics.loc[seed_idx, "relevant_calls"])

    def select_next_for_dms() -> int | None:
        selected_rows = metrics.loc[sorted(selected_indices)]
        candidates = metrics[~metrics.index.isin(selected_indices)].copy()
        if candidates.empty:
            return None
        distances = candidates.apply(lambda row: _min_distance_to_selected(row, selected_rows), axis=1)
        distance_score = 1.0 - _minmax(distances)
        candidate_score = 0.72 * candidates["dms_region_score"].to_numpy(dtype=float) + 0.28 * distance_score.to_numpy(dtype=float)
        return int(candidates.index[int(np.argmax(candidate_score))])

    while dms_calls_selected < dms_core_target_calls and len(selected_indices) < len(metrics):
        next_idx = select_next_for_dms()
        if next_idx is None:
            break
        selected_indices.add(next_idx)
        dms_calls_selected += float(metrics.loc[next_idx, "relevant_calls"])

    for idx in selected_indices:
        assignments[int(metrics.loc[idx, "_spatial_cluster"])] = AREA_TYPE_DMS_CORE

    overlap_indices: set[int] = set()
    remaining = metrics[~metrics.index.isin(selected_indices)].copy()
    if not remaining.empty:
        selected_rows = metrics.loc[sorted(selected_indices)]
        remaining["_distance_to_dms"] = remaining.apply(lambda row: _min_distance_to_selected(row, selected_rows), axis=1)
        remaining["_mix_score"] = 1.0 - (remaining["cluster_dms_share"] - 0.5).abs() * 2.0
        remaining["_overlap_score"] = 0.65 * (1.0 - _minmax(remaining["_distance_to_dms"])) + 0.35 * remaining["_mix_score"].clip(lower=0.0)
        overlap_calls = 0.0
        for idx, row in remaining.sort_values(["_overlap_score", "relevant_calls"], ascending=[False, False]).iterrows():
            if overlap_calls >= overlap_target_calls and overlap_indices:
                break
            overlap_indices.add(int(idx))
            overlap_calls += float(row["relevant_calls"])
            if len(overlap_indices) >= max(1, round(len(metrics) * 0.25)):
                break

    if len(metrics) >= 3 and not overlap_indices:
        non_dms = metrics[~metrics.index.isin(selected_indices)].copy()
        if not non_dms.empty:
            selected_rows = metrics.loc[sorted(selected_indices)]
            non_dms["_distance_to_dms"] = non_dms.apply(lambda row: _min_distance_to_selected(row, selected_rows), axis=1)
            overlap_indices.add(int(non_dms.sort_values("_distance_to_dms").index[0]))

    for idx in overlap_indices:
        assignments[int(metrics.loc[idx, "_spatial_cluster"])] = AREA_TYPE_OVERLAP

    # Keep at least one DMS2 island/outer region when there are enough clusters.
    if len(metrics) >= 3 and AREA_TYPE_DMS2_EXCLUSIVE not in set(assignments.values()):
        selected_or_overlap = metrics[metrics["_spatial_cluster"].map(lambda value: assignments[int(value)] != AREA_TYPE_DMS2_EXCLUSIVE)].copy()
        dms_rows = metrics[metrics["_spatial_cluster"].map(lambda value: assignments[int(value)] == AREA_TYPE_DMS_CORE)]
        if not selected_or_overlap.empty and not dms_rows.empty:
            selected_or_overlap["_distance_to_dms"] = selected_or_overlap.apply(lambda row: _min_distance_to_selected(row, dms_rows), axis=1)
            fallback_idx = int(selected_or_overlap.sort_values(["dms_region_score", "_distance_to_dms"], ascending=[True, False]).index[0])
            assignments[int(metrics.loc[fallback_idx, "_spatial_cluster"])] = AREA_TYPE_DMS2_EXCLUSIVE
    return assignments


def _name_and_order_regions(clustered_df: pd.DataFrame) -> pd.DataFrame:
    clustered = clustered_df.copy()

    region_rows: list[pd.DataFrame] = []
    region_seq = 1
    for area_type in [AREA_TYPE_DMS_CORE, AREA_TYPE_OVERLAP, AREA_TYPE_DMS2_EXCLUSIVE]:
        type_df = clustered[clustered["area_type"].eq(area_type)].copy()
        if type_df.empty:
            continue
        centers = (
            type_df.groupby("_spatial_cluster")
            .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"), relevant_calls=("relevant_calls", "sum"))
            .reset_index()
            .sort_values(["longitude", "latitude"])
        )
        label_map = {int(row["_spatial_cluster"]): idx for idx, row in enumerate(centers.to_dict("records"), start=1)}
        type_label = {
            AREA_TYPE_DMS_CORE: "DMS",
            AREA_TYPE_OVERLAP: "Overlap",
            AREA_TYPE_DMS2_EXCLUSIVE: "DMS2",
        }[area_type]
        for spatial_cluster, group in type_df.groupby("_spatial_cluster"):
            display_idx = label_map[int(spatial_cluster)]
            piece = group.copy()
            piece["region_seq"] = region_seq
            piece["AREA_NAME"] = f"{type_label} Region {display_idx}"
            piece["new_region_name"] = piece["AREA_NAME"]
            region_rows.append(piece)
            region_seq += 1
    return pd.concat(region_rows, ignore_index=True)


def _density_peak_center(zip_df: pd.DataFrame) -> tuple[float, float, str]:
    work = zip_df.copy().reset_index(drop=True)
    if work.empty:
        return 0.0, 0.0, ""
    coords = work[["latitude", "longitude"]].to_numpy(dtype=float)
    calls = np.maximum(work["relevant_calls"].to_numpy(dtype=float), 1.0)
    distance_matrix = np.zeros((len(work), len(work)), dtype=float)
    for idx, source in enumerate(work.itertuples(index=False)):
        for jdx, target in enumerate(work.itertuples(index=False)):
            if idx == jdx:
                continue
            distance_matrix[idx, jdx] = _haversine_km(float(source.latitude), float(source.longitude), float(target.latitude), float(target.longitude))
    nonzero_distances = distance_matrix[distance_matrix > 0]
    bandwidth_km = float(np.quantile(nonzero_distances, 0.18)) if len(nonzero_distances) else 8.0
    bandwidth_km = max(bandwidth_km, 8.0)
    density = []
    for idx in range(len(work)):
        kernel = np.exp(-np.square(distance_matrix[idx] / bandwidth_km))
        density.append(float(np.sum(calls * kernel)))
    center_idx = int(np.argmax(density))
    return (
        float(work.loc[center_idx, "latitude"]),
        float(work.loc[center_idx, "longitude"]),
        str(work.loc[center_idx, "POSTAL_CODE"]),
    )


def _assign_three_area_bands(zip_df: pd.DataFrame, stats: dict[str, float]) -> pd.DataFrame:
    city = str(stats.get("city", ""))
    total_calls = max(float(zip_df["relevant_calls"].sum()), 1.0)
    dms_share = float(stats.get("dms_share", 0.5))
    overlap_share = max(0.05, float(stats.get("overlap_target_share", 0.15)))
    overlap_share = min(overlap_share, 0.45)
    dms_core_share = min(max(0.0, float(stats.get("dms_core_target_share", dms_share - overlap_share / 2.0))), 0.85)
    dms_core_min_share = min(max(0.0, float(stats.get("dms_core_min_share", dms_core_share))), dms_core_share)
    dms_core_max_share = max(dms_core_share, float(stats.get("dms_core_max_share", dms_core_share)))
    overlap_min_share = min(max(0.0, float(stats.get("overlap_min_share", overlap_share))), overlap_share)
    overlap_max_share = max(overlap_share, float(stats.get("overlap_max_share", overlap_share)))
    dms_core_max_home_distance = CITY_DMS_CORE_MAX_HOME_DISTANCE_KM.get(city, DMS_CORE_MAX_HOME_DISTANCE_KM)
    dms_core_density_quantile = CITY_DMS_CORE_MIN_DENSITY_QUANTILE.get(city, DMS_CORE_MIN_DENSITY_QUANTILE)
    center_lat, center_lon, center_zip = _density_peak_center(zip_df)
    best_df = zip_df.copy()
    best_df["distance_to_density_center_km"] = best_df.apply(
        lambda row: _haversine_km(center_lat, center_lon, float(row["latitude"]), float(row["longitude"])),
        axis=1,
    )
    if "nearest_dms_home_distance_km" in best_df.columns and best_df["nearest_dms_home_distance_km"].notna().any():
        best_df["_dms_boundary_distance_km"] = best_df["nearest_dms_home_distance_km"].fillna(best_df["nearest_dms_home_distance_km"].max())
    else:
        best_df["_dms_boundary_distance_km"] = best_df["distance_to_density_center_km"]
    best_df = best_df.sort_values(
        ["_dms_boundary_distance_km", "distance_to_density_center_km", "relevant_calls"],
        ascending=[True, True, False],
    ).reset_index(drop=True)
    best_df["area_type"] = AREA_TYPE_DMS2_EXCLUSIVE
    best_df["is_dms_home_anchor"] = False
    density_threshold = float(best_df["density_raw"].quantile(dms_core_density_quantile))
    strong_density_threshold = float(best_df["density_raw"].quantile(min(dms_core_density_quantile + 0.12, 0.75)))
    if {"nearest_dms_home_code", "nearest_dms_home_distance_km"}.issubset(best_df.columns):
        distance_score = 1.0 - (
            best_df["nearest_dms_home_distance_km"].fillna(dms_core_max_home_distance)
            / max(dms_core_max_home_distance, 0.1)
        ).clip(lower=0.0, upper=1.0)
        best_df["_home_candidate_score"] = (
            0.42 * best_df["density_score"].fillna(0.0)
            + 0.25 * best_df["demand_score"].fillna(0.0)
            + 0.23 * distance_score.fillna(0.0)
            + 0.10 * best_df["dms_share_zip"].fillna(0.0)
        )
        best_df["_home_rank"] = (
            best_df.sort_values(
                ["nearest_dms_home_code", "_home_candidate_score", "nearest_dms_home_distance_km", "relevant_calls"],
                ascending=[True, False, True, False],
            )
            .groupby("nearest_dms_home_code")
            .cumcount()
            + 1
        )
        anchor_mask = (
            best_df["nearest_dms_home_code"].astype(str).str.strip().ne("")
            & best_df["_home_rank"].le(DMS_HOME_ANCHOR_MIN_ZIPS)
        )
        core_anchor_mask = (
            anchor_mask
            & best_df["nearest_dms_home_distance_km"].le(dms_core_max_home_distance)
            & best_df["density_raw"].ge(density_threshold)
        )
        overlap_anchor_mask = anchor_mask & ~core_anchor_mask
        best_df.loc[core_anchor_mask, "area_type"] = AREA_TYPE_DMS_CORE
        best_df.loc[overlap_anchor_mask, "area_type"] = AREA_TYPE_OVERLAP
        best_df.loc[anchor_mask, "is_dms_home_anchor"] = True

    running = float(best_df.loc[best_df["area_type"].eq(AREA_TYPE_DMS_CORE), "relevant_calls"].sum())
    dms_candidate_df = best_df[best_df["area_type"].ne(AREA_TYPE_DMS_CORE)].copy()
    if "_home_candidate_score" in dms_candidate_df.columns:
        seed_rows = (
            best_df[best_df["nearest_dms_home_code"].astype(str).str.strip().ne("")]
            .sort_values(
                ["nearest_dms_home_code", "_home_candidate_score", "nearest_dms_home_distance_km", "relevant_calls"],
                ascending=[True, False, True, False],
            )
            .drop_duplicates(subset=["nearest_dms_home_code"], keep="first")
            .set_index("nearest_dms_home_code")
        )

        seed_map = {
            str(home_code): (float(row["latitude"]), float(row["longitude"]))
            for home_code, row in seed_rows.iterrows()
        }

        def _seed_distance(row: pd.Series) -> float:
            home_code = str(row.get("nearest_dms_home_code", "")).strip()
            seed = seed_map.get(home_code)
            if seed is None:
                return float(row.get("nearest_dms_home_distance_km", float("inf")))
            return _haversine_km(float(row["latitude"]), float(row["longitude"]), seed[0], seed[1])

        dms_candidate_df["_seed_distance_km"] = dms_candidate_df.apply(_seed_distance, axis=1)
        dms_candidate_df["_radial_rank"] = (
            dms_candidate_df.sort_values(
                ["nearest_dms_home_code", "_seed_distance_km", "nearest_dms_home_distance_km", "density_raw"],
                ascending=[True, True, True, False],
            )
            .groupby("nearest_dms_home_code")
            .cumcount()
            + 1
        )
        dms_candidate_df = dms_candidate_df.sort_values(
            ["_radial_rank", "_seed_distance_km", "nearest_dms_home_distance_km", "_home_candidate_score"],
            ascending=[True, True, True, False],
        )
    for idx, row in dms_candidate_df.iterrows():
        current_share = running / total_calls
        if current_share >= dms_core_max_share:
            break
        if current_share >= dms_core_share:
            break
        if best_df.at[idx, "area_type"] == AREA_TYPE_DMS_CORE:
            continue
        row_calls = float(row["relevant_calls"])
        if (running + row_calls) / total_calls > dms_core_max_share + 0.015:
            continue
        nearest_home_distance = float(row.get("nearest_dms_home_distance_km", float("inf")))
        if nearest_home_distance > dms_core_max_home_distance:
            continue
        if "_seed_distance_km" in row and float(row.get("_seed_distance_km", float("inf"))) > dms_core_max_home_distance * 0.85:
            continue
        density_raw = float(row.get("density_raw", 0.0))
        if density_raw < density_threshold:
            continue
        if current_share >= dms_core_min_share:
            if nearest_home_distance > dms_core_max_home_distance * 0.82:
                continue
            if density_raw < strong_density_threshold:
                continue
        best_df.at[idx, "area_type"] = AREA_TYPE_DMS_CORE
        running += row_calls

    overlap_running = float(best_df.loc[best_df["area_type"].eq(AREA_TYPE_OVERLAP), "relevant_calls"].sum())
    overlap_candidate_df = best_df[best_df["area_type"].eq(AREA_TYPE_DMS2_EXCLUSIVE)].copy()
    dms_core_df = best_df[best_df["area_type"].eq(AREA_TYPE_DMS_CORE)].copy()
    if not overlap_candidate_df.empty and not dms_core_df.empty:
        def _nearest_dms_core_distance(row: pd.Series) -> float:
            distances = [
                _haversine_km(float(row["latitude"]), float(row["longitude"]), float(dms_row["latitude"]), float(dms_row["longitude"]))
                for _, dms_row in dms_core_df.iterrows()
            ]
            return min(distances) if distances else float("inf")

        overlap_candidate_df["_distance_to_dms_core_km"] = overlap_candidate_df.apply(_nearest_dms_core_distance, axis=1)
        overlap_candidate_df = overlap_candidate_df.sort_values(
            ["_distance_to_dms_core_km", "nearest_dms_home_distance_km", "relevant_calls"],
            ascending=[True, True, False],
        )
    for idx, row in overlap_candidate_df.iterrows():
        current_overlap_share = overlap_running / total_calls
        if current_overlap_share >= overlap_max_share:
            break
        if current_overlap_share >= overlap_share:
            break
        row_calls = float(row["relevant_calls"])
        if (overlap_running + row_calls) / total_calls > overlap_max_share + 0.015:
            continue
        if float(row.get("nearest_dms_home_distance_km", float("inf"))) > DMS_OVERLAP_MAX_HOME_DISTANCE_KM:
            continue
        if float(row.get("_distance_to_dms_core_km", 0.0)) > DMS_OVERLAP_MAX_HOME_DISTANCE_KM * 0.75:
            continue
        if current_overlap_share >= overlap_min_share:
            if float(row.get("_distance_to_dms_core_km", 0.0)) > DMS_OVERLAP_MAX_HOME_DISTANCE_KM * 0.55:
                continue
        best_df.at[idx, "area_type"] = AREA_TYPE_OVERLAP
        overlap_running += row_calls

    if AREA_TYPE_OVERLAP not in set(best_df["area_type"]) and len(best_df) >= 3:
        dms_indices = set(best_df.index[best_df["area_type"].eq(AREA_TYPE_DMS_CORE)].tolist())
        candidates = [idx for idx in best_df.index.tolist() if idx not in dms_indices]
        if candidates:
            best_df.at[candidates[0], "area_type"] = AREA_TYPE_OVERLAP
    if AREA_TYPE_DMS2_EXCLUSIVE not in set(best_df["area_type"]) and len(best_df) >= 3:
        best_df.at[best_df.index[-1], "area_type"] = AREA_TYPE_DMS2_EXCLUSIVE

    best_df["density_center_latitude"] = center_lat
    best_df["density_center_longitude"] = center_lon
    best_df["density_center_postal_code"] = center_zip

    label_map = {
        AREA_TYPE_DMS_CORE: (1, "DMS Region 1"),
        AREA_TYPE_OVERLAP: (2, "Overlap Region 1"),
        AREA_TYPE_DMS2_EXCLUSIVE: (3, "DMS2 Region 1"),
    }
    output = best_df.copy()
    output["region_seq"] = output["area_type"].map(lambda value: label_map[value][0])
    output["AREA_NAME"] = output["area_type"].map(lambda value: label_map[value][1])
    output["new_region_name"] = output["AREA_NAME"]
    return output.drop(
        columns=[
            col
            for col in ["_dms_boundary_distance_km", "_home_rank", "_home_candidate_score"]
            if col in output.columns
        ]
    )


def _build_city(spec: dict[str, object], service_df: pd.DataFrame, home_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    city = str(spec["city"])
    target_count = int(spec["target_region_count"])
    zip_df = _zip_metrics(city, service_df, home_df)
    if zip_df.empty:
        return pd.DataFrame(), pd.DataFrame(), {}
    stats = _city_ratio_stats(zip_df)
    stats.update(_dms_home_capacity_stats(city, service_df, home_df))
    stats["city"] = city
    if target_count == 3:
        clustered_df = _assign_three_area_bands(zip_df, stats)
    else:
        spatial_df = _spatial_cluster_zips(zip_df, target_count)
        cluster_metrics = _cluster_metrics(spatial_df)
        assignments = _assign_cluster_area_types(cluster_metrics, stats)
        spatial_df["area_type"] = spatial_df["_spatial_cluster"].map(lambda value: assignments.get(int(value), AREA_TYPE_DMS2_EXCLUSIVE))
        clustered_df = _name_and_order_regions(spatial_df)
    slug = _slugify_city_name(city)
    clustered_df["STRATEGIC_CITY_NAME"] = city
    clustered_df["candidate_region_count"] = int(clustered_df["region_seq"].nunique())
    clustered_df["region_id"] = clustered_df["region_seq"].map(lambda seq: f"{slug}_r{int(seq):02d}")
    clustered_df["service_count"] = clustered_df["relevant_calls"]
    output_cols = [
        "STRATEGIC_CITY_NAME",
        "candidate_region_count",
        "POSTAL_CODE",
        "region_id",
        "region_seq",
        "AREA_NAME",
        "new_region_name",
        "area_type",
        "service_count",
        "relevant_calls",
        "dms_calls",
        "dms2_calls",
        "asc_calls",
        "dms_share_zip",
        "dms_fit_score",
        "density_raw",
        "dms_avg_distance_km",
        "distance_to_density_center_km",
        "nearest_dms_home_distance_km",
        "nearest_dms_home_code",
        "is_dms_home_anchor",
        "density_center_latitude",
        "density_center_longitude",
        "density_center_postal_code",
        "latitude",
        "longitude",
    ]
    for col in output_cols:
        if col not in clustered_df.columns:
            clustered_df[col] = pd.NA
    zip_rows = clustered_df[output_cols].sort_values(["region_seq", "POSTAL_CODE"]).reset_index(drop=True)
    summary_rows = (
        zip_rows.groupby(["STRATEGIC_CITY_NAME", "region_seq", "AREA_NAME", "area_type"], dropna=False)
        .agg(
            zip_count=("POSTAL_CODE", "nunique"),
            relevant_calls=("relevant_calls", "sum"),
            dms_calls=("dms_calls", "sum"),
            dms2_calls=("dms2_calls", "sum"),
            asc_calls=("asc_calls", "sum"),
            dms_fit_score=("dms_fit_score", "mean"),
            dms_avg_distance_km=("dms_avg_distance_km", "mean"),
            nearest_dms_home_distance_km=("nearest_dms_home_distance_km", "mean"),
            dms_home_anchor_zip_count=("is_dms_home_anchor", "sum"),
            distance_to_density_center_km=("distance_to_density_center_km", "mean"),
        )
        .reset_index()
    )
    for key, value in stats.items():
        summary_rows[key] = value
    return zip_rows, summary_rows, stats


def main() -> None:
    PRODUCTION_DIR.mkdir(parents=True, exist_ok=True)
    FIXED_REGION_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    service_df = _read_service()
    home_df = _read_dms_homes()
    all_zip_rows: list[pd.DataFrame] = []
    all_summary_rows: list[pd.DataFrame] = []
    for spec in CITY_SPECS:
        zip_rows, summary_rows, stats = _build_city(spec, service_df, home_df)
        if zip_rows.empty:
            print(f"{spec['city']}: no rows")
            continue
        production_path = PRODUCTION_DIR / str(spec["output_file"])
        fixed_path = FIXED_REGION_DIR / str(spec["fixed_region_file"])
        review_path = OUTPUT_DIR / str(spec["output_file"])
        zip_rows.to_csv(production_path, index=False, encoding="utf-8-sig")
        zip_rows.to_csv(fixed_path, index=False, encoding="utf-8-sig")
        zip_rows.to_csv(review_path, index=False, encoding="utf-8-sig")
        all_zip_rows.append(zip_rows)
        all_summary_rows.append(summary_rows)
        print(
            f"{spec['city']}: DMS share={stats.get('dms_share', 0):.1%}, "
            f"regions={zip_rows['region_seq'].nunique()}, wrote {production_path.relative_to(ROOT)}"
        )

    if all_zip_rows:
        pd.concat(all_zip_rows, ignore_index=True).to_csv(
            OUTPUT_DIR / "fixed_region_area_type_zip_scores.csv", index=False, encoding="utf-8-sig"
        )
    if all_summary_rows:
        pd.concat(all_summary_rows, ignore_index=True).to_csv(
            OUTPUT_DIR / "fixed_region_area_type_summary.csv", index=False, encoding="utf-8-sig"
        )
        pd.concat(all_summary_rows, ignore_index=True).to_csv(
            OUTPUT_DIR / "reclustered_area_type_summary.csv", index=False, encoding="utf-8-sig"
        )


if __name__ == "__main__":
    main()
