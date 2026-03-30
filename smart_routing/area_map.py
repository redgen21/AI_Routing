from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd

from .osrm_routing import OSRMConfig, OSRMTripClient
from .region_sweep import _assign_city_regions
from .routing_compare import _batch_assign_region_day_jobs

PROFILE_FILE = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")
ZCTA_ZIP_FILE = Path("data/geo/tl_2024_us_zcta520.zip")
INPUT_DIR = Path("260310/input")
CACHE_OUTPUT_DIR = Path("260310/output")
CACHE_DIR = Path("data/cache/area_map")
ROUTE_EXPLORER_CACHE_DIR = Path("data/cache/route_explorer")
DEFAULT_CITY = "Atlanta, GA"
ALL_CITIES = "ALL"
CACHE_VERSION = "2026-03-22-route-explorer-v6"
ZIP_SIMPLIFY_TOLERANCE_M = 120
AREA_SIMPLIFY_TOLERANCE_M = 180
CONTEXT_SIMPLIFY_TOLERANCE_M = 250
EXPLORER_CITIES = ["Atlanta, GA", "Los Angeles, CA"]
REQUIRED_SERVICE_COLUMNS = {"STRATEGIC_CITY_NAME", "POSTAL_CODE", "GSFS_RECEIPT_NO", "SVC_ENGINEER_CODE", "latitude", "longitude"}


@dataclass
class RouteExplorerData:
    city_name: str
    best_region_count: int
    selected_region_count: int | None
    current_zip_layer: gpd.GeoDataFrame
    current_area_layer: gpd.GeoDataFrame
    current_service_df: pd.DataFrame
    integrated_zip_layer: gpd.GeoDataFrame
    integrated_area_layer: gpd.GeoDataFrame
    integrated_service_df: pd.DataFrame


def _normalize_text(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    return " ".join(text.replace("\r", " ").replace("\n", " ").split()).strip()


@dataclass
class CityMapData:
    city_name: str
    zip_layer: gpd.GeoDataFrame
    area_layer: gpd.GeoDataFrame
    context_zip_layer: gpd.GeoDataFrame
    slot_df: pd.DataFrame
    product_df: pd.DataFrame
    zip_coverage_df: pd.DataFrame
    service_df: pd.DataFrame
    area_stats_df: pd.DataFrame


def _slugify_city_name(city_name: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in city_name)
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_") or "all"


def _cache_file_map(city_name: str) -> dict[str, Path]:
    city_dir = CACHE_DIR / _slugify_city_name(city_name)
    return {
        "dir": city_dir,
        "meta": city_dir / "meta.json",
        "zip_layer": city_dir / "zip_layer.pkl",
        "area_layer": city_dir / "area_layer.pkl",
        "context_zip_layer": city_dir / "context_zip_layer.pkl",
        "slot_df": city_dir / "slot_df.pkl",
        "product_df": city_dir / "product_df.pkl",
        "zip_coverage_df": city_dir / "zip_coverage_df.pkl",
        "service_df": city_dir / "service_df.pkl",
        "area_stats_df": city_dir / "area_stats_df.pkl",
    }


def _route_explorer_cache_file_map(city_name: str, region_count: int | None) -> dict[str, Path]:
    city_dir = ROUTE_EXPLORER_CACHE_DIR / _slugify_city_name(city_name)
    suffix = "current" if region_count is None else f"region_{int(region_count)}"
    cache_dir = city_dir / suffix
    return {
        "dir": cache_dir,
        "meta": cache_dir / "meta.json",
        "current_zip_layer": cache_dir / "current_zip_layer.pkl",
        "current_area_layer": cache_dir / "current_area_layer.pkl",
        "current_service_df": cache_dir / "current_service_df.pkl",
        "integrated_zip_layer": cache_dir / "integrated_zip_layer.pkl",
        "integrated_area_layer": cache_dir / "integrated_area_layer.pkl",
        "integrated_service_df": cache_dir / "integrated_service_df.pkl",
    }


def _build_cache_meta(
    city_name: str,
    profile_path: Path,
    zcta_zip_path: Path,
    service_file: Path | None,
) -> dict[str, object]:
    return {
        "cache_version": CACHE_VERSION,
        "city_name": city_name,
        "profile_path": str(profile_path.resolve()),
        "profile_mtime": profile_path.stat().st_mtime if profile_path.exists() else None,
        "zcta_zip_path": str(zcta_zip_path.resolve()),
        "zcta_zip_mtime": zcta_zip_path.stat().st_mtime if zcta_zip_path.exists() else None,
        "service_file": str(service_file.resolve()) if service_file and service_file.exists() else None,
        "service_mtime": service_file.stat().st_mtime if service_file and service_file.exists() else None,
    }


def _load_cached_city_map(cache_files: dict[str, Path]) -> CityMapData:
    return CityMapData(
        city_name=json.loads(cache_files["meta"].read_text(encoding="utf-8"))["city_name"],
        zip_layer=pd.read_pickle(cache_files["zip_layer"]),
        area_layer=pd.read_pickle(cache_files["area_layer"]),
        context_zip_layer=pd.read_pickle(cache_files["context_zip_layer"]),
        slot_df=pd.read_pickle(cache_files["slot_df"]),
        product_df=pd.read_pickle(cache_files["product_df"]),
        zip_coverage_df=pd.read_pickle(cache_files["zip_coverage_df"]),
        service_df=pd.read_pickle(cache_files["service_df"]),
        area_stats_df=pd.read_pickle(cache_files["area_stats_df"]),
    )


def _is_city_map_content_valid(city_data: CityMapData) -> bool:
    required_zip_cols = {"POSTAL_CODE", "geometry"}
    required_coverage_cols = {"POSTAL_CODE", "AREA_NAME", "SVC_ENGINEER_CODE"}
    required_service_cols = {"POSTAL_CODE", "SVC_ENGINEER_CODE", "service_date"}
    if not required_zip_cols.issubset(set(city_data.zip_layer.columns)):
        return False
    if not required_coverage_cols.issubset(set(city_data.zip_coverage_df.columns)):
        return False
    if not city_data.service_df.empty and not required_service_cols.issubset(set(city_data.service_df.columns)):
        return False
    return True


def _save_cached_city_map(cache_files: dict[str, Path], city_data: CityMapData, meta: dict[str, object]) -> None:
    cache_files["dir"].mkdir(parents=True, exist_ok=True)
    city_data.zip_layer.to_pickle(cache_files["zip_layer"])
    city_data.area_layer.to_pickle(cache_files["area_layer"])
    city_data.context_zip_layer.to_pickle(cache_files["context_zip_layer"])
    city_data.slot_df.to_pickle(cache_files["slot_df"])
    city_data.product_df.to_pickle(cache_files["product_df"])
    city_data.zip_coverage_df.to_pickle(cache_files["zip_coverage_df"])
    city_data.service_df.to_pickle(cache_files["service_df"])
    city_data.area_stats_df.to_pickle(cache_files["area_stats_df"])
    cache_files["meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_cache_valid(cache_files: dict[str, Path], expected_meta: dict[str, object]) -> bool:
    required_files = [path for key, path in cache_files.items() if key != "dir"]
    if any(not path.exists() for path in required_files):
        return False
    cached_meta = json.loads(cache_files["meta"].read_text(encoding="utf-8"))
    return cached_meta == expected_meta


def _load_cached_route_explorer(cache_files: dict[str, Path]) -> RouteExplorerData:
    meta = json.loads(cache_files["meta"].read_text(encoding="utf-8"))
    return RouteExplorerData(
        city_name=str(meta["city_name"]),
        best_region_count=int(meta["best_region_count"]),
        selected_region_count=(None if meta["selected_region_count"] in (None, "current") else int(meta["selected_region_count"])),
        current_zip_layer=pd.read_pickle(cache_files["current_zip_layer"]),
        current_area_layer=pd.read_pickle(cache_files["current_area_layer"]),
        current_service_df=pd.read_pickle(cache_files["current_service_df"]),
        integrated_zip_layer=pd.read_pickle(cache_files["integrated_zip_layer"]),
        integrated_area_layer=pd.read_pickle(cache_files["integrated_area_layer"]),
        integrated_service_df=pd.read_pickle(cache_files["integrated_service_df"]),
    )


def _is_route_explorer_content_valid(explorer_data: RouteExplorerData) -> bool:
    required_current_cols = {"POSTAL_CODE", "AREA_NAME", "assigned_sm_code", "service_date"}
    required_integrated_cols = {"POSTAL_CODE", "AREA_NAME", "assigned_sm_code", "service_date"}
    if not required_current_cols.issubset(set(explorer_data.current_service_df.columns)):
        return False
    if not required_integrated_cols.issubset(set(explorer_data.integrated_service_df.columns)):
        return False
    return True


def _save_cached_route_explorer(cache_files: dict[str, Path], explorer_data: RouteExplorerData, meta: dict[str, object]) -> None:
    cache_files["dir"].mkdir(parents=True, exist_ok=True)
    explorer_data.current_zip_layer.to_pickle(cache_files["current_zip_layer"])
    explorer_data.current_area_layer.to_pickle(cache_files["current_area_layer"])
    explorer_data.current_service_df.to_pickle(cache_files["current_service_df"])
    explorer_data.integrated_zip_layer.to_pickle(cache_files["integrated_zip_layer"])
    explorer_data.integrated_area_layer.to_pickle(cache_files["integrated_area_layer"])
    explorer_data.integrated_service_df.to_pickle(cache_files["integrated_service_df"])
    cache_files["meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_route_explorer_meta(
    city_name: str,
    best_region_count: int,
    selected_region_count: int | None,
    profile_path: Path,
    zcta_zip_path: Path,
    service_file: Path | None,
    config_file: Path,
) -> dict[str, object]:
    routing_cfg = _load_json_config(config_file).get("routing", {})
    relevant_cfg = {
        "distance_backend": routing_cfg.get("distance_backend"),
        "assignment_distance_backend": routing_cfg.get("assignment_distance_backend"),
        "effective_service_per_sm": routing_cfg.get("effective_service_per_sm"),
        "service_time_per_job_min": routing_cfg.get("service_time_per_job_min"),
        "max_work_min_per_sm_day": routing_cfg.get("max_work_min_per_sm_day"),
        "max_travel_min_per_sm_day": routing_cfg.get("max_travel_min_per_sm_day"),
        "max_travel_km_per_sm_day": routing_cfg.get("max_travel_km_per_sm_day"),
        "city_osrm_urls": routing_cfg.get("city_osrm_urls", {}),
    }
    return {
        "cache_version": CACHE_VERSION,
        "city_name": city_name,
        "best_region_count": int(best_region_count),
        "selected_region_count": ("current" if selected_region_count is None else int(selected_region_count)),
        "profile_path": str(profile_path),
        "profile_mtime_ns": profile_path.stat().st_mtime_ns if profile_path.exists() else None,
        "zcta_zip_path": str(zcta_zip_path),
        "zcta_zip_mtime_ns": zcta_zip_path.stat().st_mtime_ns if zcta_zip_path.exists() else None,
        "service_file": (str(service_file) if service_file is not None else None),
        "service_file_mtime_ns": (service_file.stat().st_mtime_ns if service_file is not None and service_file.exists() else None),
        "config_file": str(config_file),
        "config_mtime_ns": config_file.stat().st_mtime_ns if config_file.exists() else None,
        "routing_cfg": relevant_cfg,
    }


def _simplify_geometry_layer(gdf: gpd.GeoDataFrame, tolerance_m: float) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf
    simplified = gdf.copy()
    projected = simplified.to_crs(epsg=3857)
    projected["geometry"] = projected.geometry.simplify(tolerance=tolerance_m, preserve_topology=True)
    return projected.to_crs(epsg=4326)


def load_profile_data(profile_path: Path = PROFILE_FILE) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    zip_df = pd.read_excel(profile_path, sheet_name="1. Zip Coverage")
    slot_df = pd.read_excel(profile_path, sheet_name="2. Slot")
    product_df = pd.read_excel(profile_path, sheet_name="3. Product")

    zip_df["POSTAL_CODE"] = zip_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    zip_df["SVC_ENGINEER_CODE"] = zip_df["SVC_ENGINEER_CODE"].map(_normalize_text)
    zip_df["STRATEGIC_CITY_NAME"] = zip_df["STRATEGIC_CITY_NAME"].map(_normalize_text)
    zip_df["AREA_NAME"] = zip_df["AREA_NAME"].map(_normalize_text)
    zip_df["SVC_CENTER_TYPE"] = zip_df["SVC_CENTER_TYPE"].map(_normalize_text)
    slot_df["SVC_ENGINEER_CODE"] = slot_df["SVC_ENGINEER_CODE"].map(_normalize_text)
    slot_df["STRATEGIC_CITY_NAME"] = slot_df["STRATEGIC_CITY_NAME"].map(_normalize_text)
    slot_df["Name"] = slot_df["Name"].map(_normalize_text)
    product_df["SVC_ENGINEER_CODE"] = product_df["SVC_ENGINEER_CODE"].map(_normalize_text)
    product_df["SERVICE_PRODUCT_GROUP_CODE"] = product_df["SERVICE_PRODUCT_GROUP_CODE"].map(_normalize_text)
    product_df["SERVICE_PRODUCT_CODE"] = product_df["SERVICE_PRODUCT_CODE"].map(_normalize_text)
    product_df["REPAIR_FLAG"] = product_df["REPAIR_FLAG"].astype(str).str.strip().str.upper()
    return zip_df, slot_df, product_df


def load_available_cities(profile_path: Path = PROFILE_FILE) -> list[str]:
    zip_df, slot_df, _ = load_profile_data(profile_path)
    cities = sorted(
        set(zip_df["STRATEGIC_CITY_NAME"].dropna().astype(str).str.strip())
        | set(slot_df["STRATEGIC_CITY_NAME"].dropna().astype(str).str.strip())
    )
    return [ALL_CITIES] + cities


def _is_valid_service_file(service_file: Path) -> bool:
    if service_file is None or not service_file.exists():
        return False
    try:
        sample_df = pd.read_csv(service_file, encoding="utf-8-sig", low_memory=False, nrows=5)
    except Exception:
        return False
    return REQUIRED_SERVICE_COLUMNS.issubset(set(sample_df.columns))


def get_latest_geocoded_service_file(input_dir: Path = INPUT_DIR) -> Path | None:
    candidates = sorted(input_dir.glob("Service_*_geocoded.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    for candidate in candidates:
        if _is_valid_service_file(candidate):
            return candidate
    return None


def load_city_map_data(
    city_name: str = DEFAULT_CITY,
    profile_path: Path = PROFILE_FILE,
    zcta_zip_path: Path = ZCTA_ZIP_FILE,
    service_file: Path | None = None,
) -> CityMapData:
    resolved_service_file = service_file or get_latest_geocoded_service_file()
    cache_files = _cache_file_map(city_name)
    expected_meta = _build_cache_meta(city_name, profile_path, zcta_zip_path, resolved_service_file)
    if _is_cache_valid(cache_files, expected_meta):
        cached_city_data = _load_cached_city_map(cache_files)
        if _is_city_map_content_valid(cached_city_data):
            return cached_city_data

    zip_df, slot_df, product_df = load_profile_data(profile_path)
    service_df = load_service_points(resolved_service_file)

    if city_name == ALL_CITIES:
        zip_city = zip_df.copy()
        slot_city = slot_df.copy()
        service_city = service_df.copy()
    else:
        zip_city = zip_df[zip_df["STRATEGIC_CITY_NAME"].astype(str).str.strip() == city_name].copy()
        slot_city = slot_df[slot_df["STRATEGIC_CITY_NAME"].astype(str).str.strip() == city_name].copy()
        if not service_df.empty and "STRATEGIC_CITY_NAME" in service_df.columns:
            service_city = service_df[service_df["STRATEGIC_CITY_NAME"].astype(str).str.strip() == city_name].copy()
        else:
            service_city = service_df.copy()
    city_sms = set(zip_city["SVC_ENGINEER_CODE"]).union(set(slot_city["SVC_ENGINEER_CODE"]))
    product_city = product_df[product_df["SVC_ENGINEER_CODE"].isin(city_sms)].copy()

    zips = sorted(zip_city["POSTAL_CODE"].dropna().astype(str).str.zfill(5).unique().tolist())
    zcta = _load_zcta_subset(zcta_zip_path, zips)

    zip_layer = _build_zip_layer(zcta, zip_city, slot_city, product_city, service_city)
    area_layer = _build_area_layer(zip_layer)
    context_zip_layer = _build_context_zip_layer(zcta_zip_path, zip_layer, city_name)
    area_stats_df = _build_area_stats(zip_city, service_city, zip_layer, area_layer)
    zip_layer = _simplify_geometry_layer(zip_layer, ZIP_SIMPLIFY_TOLERANCE_M)
    area_layer = _simplify_geometry_layer(area_layer, AREA_SIMPLIFY_TOLERANCE_M)
    context_zip_layer = _simplify_geometry_layer(context_zip_layer, CONTEXT_SIMPLIFY_TOLERANCE_M)
    city_data = CityMapData(
        city_name=city_name,
        zip_layer=zip_layer,
        area_layer=area_layer,
        context_zip_layer=context_zip_layer,
        slot_df=slot_city,
        product_df=product_city,
        zip_coverage_df=zip_city,
        service_df=service_city,
        area_stats_df=area_stats_df,
    )
    _save_cached_city_map(cache_files, city_data, expected_meta)
    return city_data


def load_service_points(service_file: Path | None) -> pd.DataFrame:
    if service_file is None or not service_file.exists():
        return pd.DataFrame()
    df = pd.read_csv(service_file, encoding="utf-8-sig", low_memory=False)
    if not REQUIRED_SERVICE_COLUMNS.issubset(set(df.columns)):
        return pd.DataFrame()
    for col in ["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "STRATEGIC_CITY_NAME", "GSFS_RECEIPT_NO", "POSTAL_CODE", "ADDRESS_LINE1_INFO", "source"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    date_candidates = [
        "REPAIR_END_DATE_YYYYMMDD",
        "REPAIR_RECEIPT_DATE_YYYYMMDD",
        "GERP_INPUT_DATE_YYYYMMDD_ID_LAST",
    ]
    for date_col in date_candidates:
        if date_col in df.columns:
            df["service_date"] = pd.to_datetime(df[date_col].astype(str), format="%Y%m%d", errors="coerce")
            if df["service_date"].notna().any():
                break
    if "source" in df.columns:
        df = df[df["source"].astype(str).str.strip().ne("failed")].copy()
    if {"latitude", "longitude"}.issubset(df.columns):
        df = df[df["latitude"].notna() & df["longitude"].notna()].copy()
    return df


def _build_service_count_by_postal(service_city: pd.DataFrame) -> pd.DataFrame:
    if service_city.empty or not {"POSTAL_CODE", "GSFS_RECEIPT_NO"}.issubset(service_city.columns):
        return pd.DataFrame(columns=["POSTAL_CODE", "service_count"])

    svc = service_city.copy()
    svc["POSTAL_CODE"] = svc["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    svc["GSFS_RECEIPT_NO"] = svc["GSFS_RECEIPT_NO"].astype(str).str.strip()
    svc = svc[svc["POSTAL_CODE"].ne("") & svc["GSFS_RECEIPT_NO"].ne("")]
    return (
        svc.groupby("POSTAL_CODE")
        .agg(service_count=("GSFS_RECEIPT_NO", "nunique"))
        .reset_index()
    )


def _build_primary_area_assignment(zip_city: pd.DataFrame) -> pd.DataFrame:
    assignment_counts = (
        zip_city.groupby(["POSTAL_CODE", "AREA_NAME"])
        .agg(
            assignment_rows=("AREA_NAME", "size"),
            strategic_city_name=("STRATEGIC_CITY_NAME", "first"),
        )
        .reset_index()
        .sort_values(["POSTAL_CODE", "assignment_rows", "AREA_NAME"], ascending=[True, False, True])
    )
    return assignment_counts.drop_duplicates(subset=["POSTAL_CODE"], keep="first").copy()


def _load_zcta_subset(zcta_zip_path: Path, zips: list[str]) -> gpd.GeoDataFrame:
    if not zcta_zip_path.exists():
        raise FileNotFoundError(f"Missing ZCTA zip file: {zcta_zip_path}")

    where = None
    if zips:
        zip_sql = ",".join(f"'{z}'" for z in zips)
        where = f"ZCTA5CE20 IN ({zip_sql})"

    path = f"zip://{zcta_zip_path.as_posix()}"
    try:
        gdf = gpd.read_file(path, where=where, columns=["ZCTA5CE20", "INTPTLAT20", "INTPTLON20", "geometry"])
    except Exception:
        gdf = gpd.read_file(path, columns=["ZCTA5CE20", "INTPTLAT20", "INTPTLON20", "geometry"])
        gdf = gdf[gdf["ZCTA5CE20"].isin(zips)].copy()
    gdf["POSTAL_CODE"] = gdf["ZCTA5CE20"].astype(str).str.zfill(5)
    return gdf.to_crs(epsg=4326)


def _build_zip_layer(
    zcta: gpd.GeoDataFrame,
    zip_city: pd.DataFrame,
    slot_city: pd.DataFrame,
    product_city: pd.DataFrame,
    service_city: pd.DataFrame,
) -> gpd.GeoDataFrame:
    slot_summary = (
        slot_city.groupby("SVC_ENGINEER_CODE")
        .agg(slot=("Slot", "first"), engineer_name=("Name", "first"))
        .reset_index()
    )
    product_t = product_city[product_city["REPAIR_FLAG"] == "T"].copy()
    product_summary = (
        product_t.groupby("SVC_ENGINEER_CODE")
        .agg(
            repair_product_group_cnt=("SERVICE_PRODUCT_GROUP_CODE", "nunique"),
            repair_product_code_cnt=("SERVICE_PRODUCT_CODE", "nunique"),
        )
        .reset_index()
    )

    zip_group = (
        zip_city.groupby("POSTAL_CODE")
        .agg(
            strategic_city_name=("STRATEGIC_CITY_NAME", lambda s: " | ".join(sorted(set(map(str, s))))),
            strategic_city_count=("STRATEGIC_CITY_NAME", "nunique"),
            area_count=("AREA_NAME", "nunique"),
            area_names=("AREA_NAME", lambda s: " | ".join(sorted(set(map(str, s))))),
            sm_count=("SVC_ENGINEER_CODE", "nunique"),
            sm_codes=("SVC_ENGINEER_CODE", lambda s: " | ".join(sorted(set(map(str, s))))),
            center_types=("SVC_CENTER_TYPE", lambda s: " | ".join(sorted(set(map(str, s))))),
        )
        .reset_index()
    )
    primary_area = _build_primary_area_assignment(zip_city)[["POSTAL_CODE", "AREA_NAME"]].rename(columns={"AREA_NAME": "primary_area_name"})

    zip_sm = zip_city[["POSTAL_CODE", "AREA_NAME", "SVC_ENGINEER_CODE", "SVC_CENTER_TYPE"]].drop_duplicates().copy()
    zip_sm = zip_sm.merge(slot_summary, on="SVC_ENGINEER_CODE", how="left")
    zip_sm = zip_sm.merge(product_summary, on="SVC_ENGINEER_CODE", how="left")
    zip_sm["slot"] = zip_sm["slot"].fillna(0)
    zip_sm["repair_product_group_cnt"] = zip_sm["repair_product_group_cnt"].fillna(0).astype(int)
    zip_sm["repair_product_code_cnt"] = zip_sm["repair_product_code_cnt"].fillna(0).astype(int)
    zip_sm["engineer_name"] = zip_sm["engineer_name"].fillna("")
    zip_sm["sm_detail"] = zip_sm.apply(
        lambda row: (
            f"{row['AREA_NAME']} / {row['SVC_ENGINEER_CODE']} / {row['engineer_name']} / "
            f"center={row['SVC_CENTER_TYPE']} / "
            f"slot={int(row['slot'])} / product_codes={int(row['repair_product_code_cnt'])}"
        ),
        axis=1,
    )
    zip_detail = (
        zip_sm.groupby("POSTAL_CODE")
        .agg(
            slot_sum=("slot", "sum"),
            slot_avg=("slot", "mean"),
            sm_detail=("sm_detail", lambda s: "<br>".join(sorted(set(map(str, s))))),
            area_sm_pairs=("AREA_NAME", "nunique"),
        )
        .reset_index()
    )

    zip_layer = zcta.merge(zip_group, on="POSTAL_CODE", how="inner")
    zip_layer = zip_layer.merge(primary_area, on="POSTAL_CODE", how="left")
    zip_layer = zip_layer.merge(zip_detail, on="POSTAL_CODE", how="left")
    zip_layer = zip_layer.merge(_build_service_count_by_postal(service_city), on="POSTAL_CODE", how="left")
    zip_layer["slot_sum"] = pd.to_numeric(zip_layer["slot_sum"], errors="coerce").fillna(0)
    zip_layer["slot_avg"] = pd.to_numeric(zip_layer["slot_avg"], errors="coerce").fillna(0)
    zip_layer["service_count"] = pd.to_numeric(zip_layer["service_count"], errors="coerce").fillna(0).astype(int)
    zip_layer["primary_area_name"] = zip_layer["primary_area_name"].fillna("")
    return zip_layer


def _build_area_layer(zip_layer: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    area_gdf = zip_layer[[
        "strategic_city_name",
        "primary_area_name",
        "POSTAL_CODE",
        "sm_codes",
        "center_types",
        "slot_sum",
        "service_count",
        "sm_detail",
        "geometry",
    ]].copy()
    area_gdf = area_gdf.rename(columns={"primary_area_name": "AREA_NAME"})
    area_gdf = area_gdf[area_gdf["AREA_NAME"].astype(str).str.strip().ne("")].copy()
    if area_gdf.empty:
        return area_gdf

    area_layer = (
        area_gdf.groupby("AREA_NAME")
        .agg(
            strategic_city_name=("strategic_city_name", lambda s: " | ".join(sorted(set(map(str, s))))),
            postal_count=("POSTAL_CODE", "nunique"),
            postal_codes=("POSTAL_CODE", lambda s: " | ".join(sorted(set(map(str, s))))),
            sm_codes=("sm_codes", lambda s: " | ".join(sorted(set(map(str, s))))),
            center_types=("center_types", lambda s: " | ".join(sorted(set(map(str, s))))),
            slot_sum=("slot_sum", "sum"),
            service_count=("service_count", "sum"),
            sm_detail=("sm_detail", lambda s: "<br>".join(sorted(set(map(str, s))))),
            geometry=("geometry", lambda g: g.union_all()),
        )
        .reset_index()
    )
    area_layer = gpd.GeoDataFrame(area_layer, geometry="geometry", crs="EPSG:4326")
    area_layer["area_km2"] = area_layer.to_crs(epsg=3857).geometry.area / 1_000_000
    return area_layer


def _build_context_zip_layer(
    zcta_zip_path: Path,
    zip_layer: gpd.GeoDataFrame,
    city_name: str,
) -> gpd.GeoDataFrame:
    if city_name == ALL_CITIES or zip_layer.empty:
        return gpd.GeoDataFrame(columns=["POSTAL_CODE", "is_assigned", "geometry"], geometry="geometry", crs="EPSG:4326")

    bbox = zip_layer.to_crs(epsg=4326).total_bounds
    minx, miny, maxx, maxy = bbox
    pad_x = max((maxx - minx) * 0.08, 0.05)
    pad_y = max((maxy - miny) * 0.08, 0.05)
    path = f"zip://{zcta_zip_path.as_posix()}"
    gdf = gpd.read_file(path, bbox=(minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y), columns=["ZCTA5CE20", "geometry"])
    gdf = gdf.to_crs(epsg=4326)
    gdf["POSTAL_CODE"] = gdf["ZCTA5CE20"].astype(str).str.zfill(5)
    assigned = set(zip_layer["POSTAL_CODE"].astype(str).str.zfill(5))
    gdf["is_assigned"] = gdf["POSTAL_CODE"].isin(assigned)
    return gdf[["POSTAL_CODE", "is_assigned", "geometry"]].copy()


def _build_area_stats(
    zip_city: pd.DataFrame,
    service_city: pd.DataFrame,
    zip_layer: gpd.GeoDataFrame,
    area_layer: gpd.GeoDataFrame,
) -> pd.DataFrame:
    primary_area_postal = zip_layer[["primary_area_name", "POSTAL_CODE"]].drop_duplicates().copy()
    primary_area_postal = primary_area_postal.rename(columns={"primary_area_name": "AREA_NAME"})
    primary_area_postal = primary_area_postal[primary_area_postal["AREA_NAME"].astype(str).str.strip().ne("")].copy()

    area_base = (
        primary_area_postal.merge(
            zip_city[["AREA_NAME", "STRATEGIC_CITY_NAME", "SVC_ENGINEER_CODE"]].drop_duplicates(),
            on="AREA_NAME",
            how="left",
        )
        .groupby("AREA_NAME")
        .agg(
            postal_count=("POSTAL_CODE", "nunique"),
            strategic_city_name=("STRATEGIC_CITY_NAME", lambda s: " | ".join(sorted(set(map(str, s))))),
            sm_count=("SVC_ENGINEER_CODE", "nunique"),
        )
        .reset_index()
    )

    if not service_city.empty and {"POSTAL_CODE", "GSFS_RECEIPT_NO"}.issubset(service_city.columns):
        svc = service_city.copy()
        svc["POSTAL_CODE"] = svc["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
        svc_area = primary_area_postal.merge(
            svc[["POSTAL_CODE", "GSFS_RECEIPT_NO"]],
            on="POSTAL_CODE",
            how="left",
        )
        svc_stats = (
            svc_area.groupby("AREA_NAME")
            .agg(
                service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
            )
            .reset_index()
        )
    else:
        svc_stats = pd.DataFrame(columns=["AREA_NAME", "service_count"])

    area_metrics = area_layer[["AREA_NAME", "area_km2"]].copy()
    stats = area_base.merge(svc_stats, on="AREA_NAME", how="left").merge(area_metrics, on="AREA_NAME", how="left")
    stats["service_count"] = pd.to_numeric(stats["service_count"], errors="coerce").fillna(0).astype(int)
    stats["area_km2"] = pd.to_numeric(stats["area_km2"], errors="coerce").fillna(0.0)
    stats = stats.sort_values(["service_count", "postal_count", "AREA_NAME"], ascending=[False, False, True]).reset_index(drop=True)
    return stats


def _load_json_config(config_file: Path = Path("config.json")) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def get_latest_region_count_sweep_summary_file(output_dir: Path = CACHE_OUTPUT_DIR) -> Path | None:
    candidates = sorted(
        output_dir.glob("region_count_sweep_summary_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_best_region_count_by_city(output_dir: Path = CACHE_OUTPUT_DIR) -> dict[str, int]:
    summary_file = get_latest_region_count_sweep_summary_file(output_dir)
    if summary_file is None:
        return {}
    df = pd.read_csv(summary_file, encoding="utf-8-sig", low_memory=False)
    df["STRATEGIC_CITY_NAME"] = df["STRATEGIC_CITY_NAME"].map(_normalize_text)
    best_df = df[df["is_best_candidate"].astype(str).str.lower().isin({"true", "1"})].copy()
    if best_df.empty:
        best_df = (
            df.sort_values(["STRATEGIC_CITY_NAME", "balance_score", "candidate_region_count"])
            .drop_duplicates(subset=["STRATEGIC_CITY_NAME"], keep="first")
            .copy()
        )
    return {
        str(row["STRATEGIC_CITY_NAME"]): int(row["candidate_region_count"])
        for _, row in best_df.iterrows()
    }


def load_region_count_sweep_summary(output_dir: Path = CACHE_OUTPUT_DIR) -> pd.DataFrame:
    summary_file = get_latest_region_count_sweep_summary_file(output_dir)
    if summary_file is None:
        return pd.DataFrame()
    df = pd.read_csv(summary_file, encoding="utf-8-sig", low_memory=False)
    if "STRATEGIC_CITY_NAME" in df.columns:
        df["STRATEGIC_CITY_NAME"] = df["STRATEGIC_CITY_NAME"].map(_normalize_text)
    return df


def load_region_count_options(city_name: str, output_dir: Path = CACHE_OUTPUT_DIR) -> list[int]:
    df = load_region_count_sweep_summary(output_dir)
    if df.empty:
        return []
    options = sorted(df[df["STRATEGIC_CITY_NAME"] == city_name]["candidate_region_count"].dropna().astype(int).unique().tolist())
    return options


def load_region_count_stats(city_name: str, output_dir: Path = CACHE_OUTPUT_DIR) -> pd.DataFrame:
    df = load_region_count_sweep_summary(output_dir)
    if df.empty:
        return df
    city_df = df[df["STRATEGIC_CITY_NAME"] == city_name].copy()
    if city_df.empty:
        return city_df
    keep_cols = [
        "candidate_region_count",
        "is_best_candidate",
        "avg_daily_deployed_sm_current",
        "avg_daily_deployed_sm_integrated",
        "avg_jobs_per_sm_current",
        "avg_jobs_per_sm_integrated",
        "avg_jobs_per_sm_std_current",
        "avg_jobs_per_sm_std_integrated",
        "avg_distance_per_sm_km_current",
        "avg_distance_per_sm_km_integrated",
        "avg_duration_per_sm_min_current",
        "avg_duration_per_sm_min_integrated",
        "overflow_480_ratio_current",
        "overflow_480_ratio_integrated",
        "balance_score",
    ]
    keep_cols = [col for col in keep_cols if col in city_df.columns]
    return city_df[keep_cols].sort_values("candidate_region_count").reset_index(drop=True)


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
        cache_name = str(city_name).lower().replace(",", "").replace(" ", "_")
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


def _build_current_service_assignments(service_city: pd.DataFrame, zip_city: pd.DataFrame) -> pd.DataFrame:
    primary_area = _build_primary_area_assignment(zip_city)[["POSTAL_CODE", "AREA_NAME"]].copy()
    current_df = service_city.merge(primary_area, on="POSTAL_CODE", how="left")
    current_df["AREA_NAME"] = current_df["AREA_NAME"].fillna("Unassigned")
    current_df["assignment_unit_id"] = current_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    current_df["assigned_sm_code"] = current_df["assignment_unit_id"]
    current_df["scenario"] = "current"
    current_df["display_region_name"] = current_df["AREA_NAME"]
    current_df["display_region_seq"] = pd.NA
    return current_df


def _build_integrated_assignments(
    service_city: pd.DataFrame,
    city_name: str,
    region_count: int,
    routing_cfg: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    assigned_df = _assign_city_regions(service_city, city_name, region_count).copy()
    assigned_df["cluster_seq"] = -1
    client_map, default_client = _build_routing_clients(routing_cfg)
    city_client = client_map.get(city_name, default_client)
    effective_service_per_sm = float(routing_cfg.get("effective_service_per_sm", 5.0))
    assignment_distance_backend = str(routing_cfg.get("assignment_distance_backend", "haversine")).strip().lower()
    service_time_per_job_min = float(routing_cfg.get("service_time_per_job_min", 60.0))
    max_work_min_per_sm_day = float(routing_cfg.get("max_work_min_per_sm_day", 480.0))
    max_travel_min_per_sm_day = routing_cfg.get("max_travel_min_per_sm_day")
    max_travel_km_per_sm_day = routing_cfg.get("max_travel_km_per_sm_day")
    max_travel_min_per_sm_day = float(max_travel_min_per_sm_day) if max_travel_min_per_sm_day not in (None, "", 0) else None
    max_travel_km_per_sm_day = float(max_travel_km_per_sm_day) if max_travel_km_per_sm_day not in (None, "", 0) else None

    for (_, _, _), group_df in assigned_df.groupby(["STRATEGIC_CITY_NAME", "service_date", "region_id"], sort=True):
        assigned_df.loc[group_df.index, "cluster_seq"] = _batch_assign_region_day_jobs(
            group_df=group_df,
            client=city_client,
            effective_service_per_sm=effective_service_per_sm,
            service_time_per_job_min=service_time_per_job_min,
            max_work_min_per_sm_day=max_work_min_per_sm_day,
            max_travel_min_per_sm_day=max_travel_min_per_sm_day,
            max_travel_km_per_sm_day=max_travel_km_per_sm_day,
            assignment_distance_backend=assignment_distance_backend,
        )

    assigned_df["region_seq"] = assigned_df["region_seq"].astype(int)
    assigned_df["cluster_seq"] = assigned_df["cluster_seq"].astype(int)
    assigned_df["AREA_NAME"] = assigned_df["region_seq"].apply(lambda n: f"Region {int(n)}")
    assigned_df["assignment_unit_id"] = assigned_df.apply(
        lambda row: f"R{int(row['region_seq']):02d}_SM{int(row['cluster_seq']) + 1:02d}",
        axis=1,
    )
    assigned_df["assigned_sm_code"] = assigned_df["assignment_unit_id"]
    assigned_df["scenario"] = "integrated"
    assigned_df["display_region_name"] = assigned_df["AREA_NAME"]
    assigned_df["display_region_seq"] = assigned_df["region_seq"]

    postal_region_df = (
        assigned_df[["POSTAL_CODE", "STRATEGIC_CITY_NAME", "region_id", "region_seq", "AREA_NAME"]]
        .drop_duplicates()
        .copy()
    )
    return assigned_df, postal_region_df


def _build_integrated_zip_layer(postal_region_df: pd.DataFrame, service_df: pd.DataFrame) -> gpd.GeoDataFrame:
    zcta = _load_zcta_subset(ZCTA_ZIP_FILE, sorted(postal_region_df["POSTAL_CODE"].unique().tolist()))
    zip_layer = zcta.merge(postal_region_df, on="POSTAL_CODE", how="inner")
    zip_layer = gpd.GeoDataFrame(zip_layer, geometry="geometry", crs="EPSG:4326")
    service_stats = (
        service_df.groupby(["POSTAL_CODE", "region_id"])
        .agg(
            service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
            assigned_sm_count=("assigned_sm_code", "nunique"),
        )
        .reset_index()
    )
    zip_layer = zip_layer.merge(service_stats, on=["POSTAL_CODE", "region_id"], how="left")
    zip_layer["service_count"] = zip_layer["service_count"].fillna(0).astype(int)
    zip_layer["assigned_sm_count"] = zip_layer["assigned_sm_count"].fillna(0).astype(int)
    return _simplify_geometry_layer(zip_layer, ZIP_SIMPLIFY_TOLERANCE_M)


def _build_integrated_area_layer(zip_layer: gpd.GeoDataFrame, service_df: pd.DataFrame) -> gpd.GeoDataFrame:
    if zip_layer.empty:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    area_layer = (
        zip_layer.groupby(["AREA_NAME", "region_id", "region_seq"])
        .agg(
            postal_count=("POSTAL_CODE", "nunique"),
            service_count=("service_count", "sum"),
            assigned_sm_count=("assigned_sm_count", "max"),
            geometry=("geometry", lambda g: g.union_all()),
        )
        .reset_index()
        .sort_values("region_seq")
    )
    daily_sm_df = (
        service_df.groupby(["AREA_NAME", "service_date"])
        .agg(
            daily_service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
            daily_assigned_sm_count=("assigned_sm_code", "nunique"),
        )
        .reset_index()
    )
    area_daily_stats = (
        daily_sm_df.groupby("AREA_NAME")
        .agg(
            avg_daily_service_count=("daily_service_count", "mean"),
            avg_daily_assigned_sm_count=("daily_assigned_sm_count", "mean"),
        )
        .reset_index()
    )
    area_layer = area_layer.merge(area_daily_stats, on="AREA_NAME", how="left")
    area_layer["avg_daily_service_count"] = area_layer["avg_daily_service_count"].fillna(0).round(2)
    area_layer["avg_daily_assigned_sm_count"] = area_layer["avg_daily_assigned_sm_count"].fillna(0).round(2)
    area_layer = gpd.GeoDataFrame(area_layer, geometry="geometry", crs="EPSG:4326")
    area_layer["area_km2"] = area_layer.to_crs(epsg=3857).geometry.area / 1_000_000
    return _simplify_geometry_layer(area_layer, AREA_SIMPLIFY_TOLERANCE_M)


def load_route_explorer_data(
    city_name: str,
    region_count: int | None = None,
    profile_path: Path = PROFILE_FILE,
    config_file: Path = Path("config.json"),
) -> RouteExplorerData:
    if city_name not in EXPLORER_CITIES:
        raise ValueError(f"Unsupported explorer city: {city_name}")

    resolved_service_file = get_latest_geocoded_service_file()
    routing_cfg = _load_json_config(config_file).get("routing", {})
    best_region_count = load_best_region_count_by_city().get(city_name, int(routing_cfg.get("target_sm_per_region", 5)))
    resolved_region_count = int(region_count) if region_count is not None else best_region_count
    route_cache_files = _route_explorer_cache_file_map(city_name, region_count)
    expected_meta = _build_route_explorer_meta(
        city_name=city_name,
        best_region_count=best_region_count,
        selected_region_count=region_count,
        profile_path=profile_path,
        zcta_zip_path=ZCTA_ZIP_FILE,
        service_file=resolved_service_file,
        config_file=config_file,
    )
    if _is_cache_valid(route_cache_files, expected_meta):
        cached_explorer = _load_cached_route_explorer(route_cache_files)
        if _is_route_explorer_content_valid(cached_explorer):
            return cached_explorer

    city_data = load_city_map_data(city_name=city_name, profile_path=profile_path)

    service_city_all = city_data.service_df.copy()
    current_service_df = _build_current_service_assignments(service_city_all, city_data.zip_coverage_df)
    service_city = service_city_all.copy()
    integrated_service_df, postal_region_df = _build_integrated_assignments(
        service_city=service_city,
        city_name=city_name,
        region_count=resolved_region_count,
        routing_cfg=routing_cfg,
    )
    integrated_zip_layer = _build_integrated_zip_layer(postal_region_df, integrated_service_df)
    integrated_area_layer = _build_integrated_area_layer(integrated_zip_layer, integrated_service_df)

    explorer_data = RouteExplorerData(
        city_name=city_name,
        best_region_count=best_region_count,
        selected_region_count=resolved_region_count,
        current_zip_layer=city_data.zip_layer.copy(),
        current_area_layer=city_data.area_layer.copy(),
        current_service_df=current_service_df.copy(),
        integrated_zip_layer=integrated_zip_layer.copy(),
        integrated_area_layer=integrated_area_layer.copy(),
        integrated_service_df=integrated_service_df.copy(),
    )
    _save_cached_route_explorer(route_cache_files, explorer_data, expected_meta)
    return explorer_data
