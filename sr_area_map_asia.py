from __future__ import annotations

import colorsys
import json
from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
import requests
import streamlit as st
from folium.plugins import MarkerCluster

from smart_routing.area_map import (
    ALL_CITIES,
    CACHE_VERSION as AREA_MAP_CACHE_VERSION,
    get_latest_geocoded_service_file as _area_map_get_latest_geocoded_service_file,
    load_city_map_data as _area_map_load_city_map_data,
    load_region_count_options,
    load_region_count_stats,
    load_route_explorer_data as _area_map_load_route_explorer_data,
)
from smart_routing.census_geocoder import load_geocode_cache, merge_service_with_geocodes
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient


st.set_page_config(page_title="Asia Routing Map", layout="wide")

CONFIG_FILE = Path("config/config.json")
AREA_MAP_CONFIG_SECTION = "area_map_asia"
PROFILE_FILE = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")
PRODUCTION_INPUT_DIR = Path("260310/production_input")
CURRENT_REGION_LABEL = "Current Region"
ALL_OPTION = "ALL"
BLANK_SHIP_TO_OPTION = "(Blank)"
DEFAULT_OSRM_URL = "http://20.51.244.68:5000"
DEFAULT_CITY_OSRM_URLS = {
    "Los Angeles, CA": "http://20.51.244.68:5001",
    "Atlanta, GA": "http://20.51.244.68:5002",
}
COUNTRY_ROUTE_KEYS = {"THAILAND", "INDONESIA", "MALAYSIA"}
ROUTE_HEALTH_PROBES = {
    "THAILAND": ((100.5018, 13.7563), (100.5167, 13.7450)),
    "INDONESIA": ((106.8456, -6.2088), (106.8272, -6.1754)),
    "MALAYSIA": ((101.6869, 3.1390), (101.7000, 3.1500)),
}
KM_TO_MILE = 0.621371
JOB_INPUT_COLUMNS = [
    "SVC_ENGINEER_CODE",
    "SVC_ENGINEER_NAME",
    "SERVICE_PRODUCT_GROUP_CODE",
    "SERVICE_PRODUCT_CODE",
    "RECEIPT_DETAIL_SYMPTOM_CODE",
    "SVC_RECEIPT_TYPE",
    "SVC_TYPE_CODE",
    "GSFS_RECEIPT_NO",
    "PROMISE_DATE",
    "CITY_NAME",
    "POSTAL_CODE",
    "ADDRESS_LINE1_INFO",
    "fixed",
    "job_slot_count",
]


def _load_config(config_file: Path = CONFIG_FILE) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def get_latest_geocoded_service_file():
    return _area_map_get_latest_geocoded_service_file(config_section=AREA_MAP_CONFIG_SECTION)


def load_city_map_data(city_name: str = ALL_CITIES):
    return _area_map_load_city_map_data(city_name=city_name, config_section=AREA_MAP_CONFIG_SECTION)


def load_route_explorer_data(city_name: str, region_count: int | None = None):
    return _area_map_load_route_explorer_data(
        city_name=city_name,
        region_count=region_count,
        config_section=AREA_MAP_CONFIG_SECTION,
    )


@st.cache_data(show_spinner=False)
def get_route_explorer_data(city_name: str, region_count: int | None, cache_version: str):
    return load_route_explorer_data(city_name=city_name, region_count=region_count)


@st.cache_data(show_spinner=False)
def get_region_stats(city_name: str):
    return load_region_count_stats(city_name)


@st.cache_resource(show_spinner=False)
def get_clients():
    routing_cfg = _load_config().get("routing", {})
    distance_backend = str(routing_cfg.get("distance_backend", "osrm")).strip().lower()
    default_osrm_url = str(routing_cfg.get("osrm_url", DEFAULT_OSRM_URL) or DEFAULT_OSRM_URL).rstrip("/")
    default_client = OSRMTripClient(
        OSRMConfig(
            osrm_url=default_osrm_url,
            mode="haversine" if distance_backend == "city_osrm_else_haversine" else distance_backend,
            osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
            cache_file=Path(str(routing_cfg.get("osrm_cache_file", "data/cache/osrm_trip_cache.csv"))),
        )
    )
    client_map: dict[str, OSRMTripClient] = {}
    city_osrm_urls = dict(DEFAULT_CITY_OSRM_URLS)
    configured_city_urls = routing_cfg.get("city_osrm_urls", {})
    if isinstance(configured_city_urls, dict):
        city_osrm_urls.update({str(k): str(v) for k, v in configured_city_urls.items() if str(v).strip()})
    for city_name, city_url in city_osrm_urls.items():
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
                    else default_osrm_url
                ),
            )
        )
    return client_map, default_client


def _generate_color_map(labels: list[str]) -> dict[str, str]:
    color_map: dict[str, str] = {}
    hue = 0.11
    golden_ratio = 0.618033988749895
    for label in sorted({str(v).strip() for v in labels if str(v).strip()}):
        hue = (hue + golden_ratio) % 1.0
        rgb = colorsys.hsv_to_rgb(hue, 0.62, 0.92)
        color_map[label] = "#{:02x}{:02x}{:02x}".format(int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
    return color_map


def _parse_region_option(region_option: str) -> int | None:
    if region_option == CURRENT_REGION_LABEL:
        return None
    return int(region_option.replace("New Region ", "").strip())


def _normalize_center_bucket(center_type: object) -> str:
    text = _normalize_filter_text(center_type)
    upper = text.upper()
    if upper in {"DMS", "DMS2", "ASC", "DSC"}:
        return upper
    if upper == "EXCLUSIVE ASC":
        return "Exclusive ASC"
    if upper == "GENERAL ASC":
        return "General ASC"
    return text or "UNKNOWN"


def _center_bucket_options(service_df: pd.DataFrame) -> list[str]:
    preferred = ["DMS", "DMS2", "ASC", "DSC", "Exclusive ASC", "General ASC", "UNKNOWN"]
    values = [_normalize_center_bucket(value) for value in service_df.get("CENTER_BUCKET", pd.Series(dtype="object")).dropna()]
    unique_values = sorted({value for value in values if value})
    return [value for value in preferred if value in unique_values] + [value for value in unique_values if value not in preferred]


def _normalize_filter_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = " ".join(str(value).strip().split())
    return "" if text.casefold() in {"nan", "none", "nat", "<na>"} else text


@st.cache_data(show_spinner=False)
def get_service_scope_options(service_file: str | None):
    if not service_file:
        return [ALL_OPTION], {ALL_OPTION: [ALL_OPTION]}
    path = Path(service_file)
    if not path.exists():
        return [ALL_OPTION], {ALL_OPTION: [ALL_OPTION]}
    try:
        df = pd.read_csv(
            path,
            encoding="utf-8-sig",
            low_memory=False,
            usecols=lambda col: col in {"SUBSIDIARY_NAME", "SHIP_TO_FULL_NAME"},
        )
    except Exception:
        return [ALL_OPTION], {ALL_OPTION: [ALL_OPTION]}
    if "SUBSIDIARY_NAME" not in df.columns:
        df["SUBSIDIARY_NAME"] = ""
    if "SHIP_TO_FULL_NAME" not in df.columns:
        df["SHIP_TO_FULL_NAME"] = ""
    df["SUBSIDIARY_NAME"] = df["SUBSIDIARY_NAME"].map(_normalize_filter_text)
    df["SHIP_TO_FULL_NAME"] = df["SHIP_TO_FULL_NAME"].map(_normalize_filter_text)
    subsidiaries = sorted(value for value in df["SUBSIDIARY_NAME"].dropna().unique().tolist() if value)
    subsidiary_options = [ALL_OPTION] + subsidiaries
    ship_to_options_by_subsidiary: dict[str, list[str]] = {}
    for subsidiary in subsidiary_options:
        scoped = df if subsidiary == ALL_OPTION else df[df["SUBSIDIARY_NAME"].eq(subsidiary)]
        ship_to_names = sorted(value for value in scoped["SHIP_TO_FULL_NAME"].dropna().unique().tolist() if value)
        options = [ALL_OPTION] + ship_to_names
        if scoped["SHIP_TO_FULL_NAME"].fillna("").astype(str).str.strip().eq("").any():
            options.append(BLANK_SHIP_TO_OPTION)
        ship_to_options_by_subsidiary[subsidiary] = options
    return subsidiary_options, ship_to_options_by_subsidiary


def _apply_service_scope_filters(service_df: pd.DataFrame, subsidiary_name: str, ship_to_full_name: str) -> pd.DataFrame:
    filtered = service_df.copy()
    if subsidiary_name != ALL_OPTION and "SUBSIDIARY_NAME" in filtered.columns:
        filtered = filtered[filtered["SUBSIDIARY_NAME"].map(_normalize_filter_text).eq(subsidiary_name)].copy()
    if ship_to_full_name == BLANK_SHIP_TO_OPTION and "SHIP_TO_FULL_NAME" in filtered.columns:
        filtered = filtered[filtered["SHIP_TO_FULL_NAME"].map(_normalize_filter_text).eq("")].copy()
    elif ship_to_full_name != ALL_OPTION and "SHIP_TO_FULL_NAME" in filtered.columns:
        selected_key = ship_to_full_name.casefold()
        filtered = filtered[
            filtered["SHIP_TO_FULL_NAME"].map(_normalize_filter_text).str.casefold().eq(selected_key)
        ].copy()
    return filtered


def _is_specific_ship_to(ship_to_full_name: str) -> bool:
    return _normalize_filter_text(ship_to_full_name) not in {ALL_OPTION, BLANK_SHIP_TO_OPTION, ""}


def _apply_ship_to_area_scope(
    zip_layer: gpd.GeoDataFrame | pd.DataFrame,
    area_layer: gpd.GeoDataFrame,
    service_df: pd.DataFrame,
    ship_to_full_name: str,
    area_col: str,
):
    if not _is_specific_ship_to(ship_to_full_name):
        return zip_layer, area_layer
    selected_area_names = {
        _normalize_filter_text(value)
        for value in service_df.get("AREA_NAME", pd.Series(dtype="object")).dropna().tolist()
        if _normalize_filter_text(value)
        and _normalize_filter_text(value).casefold() not in {"unassigned", "unknown"}
    }
    if not selected_area_names:
        selected_area_names = {_normalize_filter_text(ship_to_full_name)}
    selected_area_keys = {value.casefold() for value in selected_area_names}
    if "AREA_NAME" in area_layer.columns:
        area_layer = area_layer[
            area_layer["AREA_NAME"].map(_normalize_filter_text).str.casefold().isin(selected_area_keys)
        ].copy()
    if area_col in zip_layer.columns:
        zip_layer = zip_layer[
            zip_layer[area_col].map(_normalize_filter_text).str.casefold().isin(selected_area_keys)
        ].copy()
    return zip_layer, area_layer


def _apply_center_bucket_rules(service_df: pd.DataFrame, region_count: int | None) -> pd.DataFrame:
    service_df = service_df.copy()
    if "SVC_CENTER_TYPE" in service_df.columns:
        service_df["CENTER_BUCKET"] = service_df["SVC_CENTER_TYPE"].map(_normalize_center_bucket)
    elif "SERVICE_CENTER_TYPE" in service_df.columns:
        service_df["CENTER_BUCKET"] = service_df["SERVICE_CENTER_TYPE"].map(_normalize_center_bucket)
    else:
        service_df["CENTER_BUCKET"] = "DMS" if region_count is not None else "ASC"
    return service_df


def _marker_border_style(center_bucket: object) -> tuple[str, str]:
    bucket = str(center_bucket or "").strip().upper()
    if "ASC" in bucket:
        return "3px", "#111111"
    if bucket == "DMS2":
        return "3px", "#dc2626"
    return "2px", "#ffffff"


def _marker_icon_size(center_bucket: object) -> tuple[int, int]:
    bucket = str(center_bucket or "").strip().upper()
    if bucket == "DMS2":
        return 42, 26
    if "ASC" in bucket:
        return 34, 26
    return 24, 24


def _marker_icon_label(center_bucket: object, seq_label: str) -> str:
    bucket = _normalize_center_bucket(center_bucket)
    if bucket.upper() in {"ASC", "DMS2", "DSC"} or "ASC" in bucket.upper():
        return bucket
    return seq_label


def _truthy_marker_value(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "y", "yes", "t"}


def _classify_assignment_group_bucket(bucket_values: pd.Series) -> str:
    buckets = sorted({_normalize_center_bucket(value) for value in bucket_values.dropna() if _normalize_center_bucket(value)})
    if len(buckets) == 1:
        return buckets[0]
    return "MIXED" if buckets else "UNKNOWN"


def _get_service_time_series(service_df: pd.DataFrame) -> pd.Series:
    if "service_time_min" in service_df.columns:
        return pd.to_numeric(service_df["service_time_min"], errors="coerce").fillna(0)
    if "service_minutes" in service_df.columns:
        return pd.to_numeric(service_df["service_minutes"], errors="coerce").fillna(0)
    return pd.Series(60.0, index=service_df.index)


def _city_slug(city_name: str) -> str:
    city_part = str(city_name).split(",", 1)[0]
    return city_part.lower().strip().replace(" ", "_")


def _split_city_state(value: object) -> tuple[str, str]:
    text = "" if pd.isna(value) else str(value).strip()
    if "," not in text:
        return "", text
    city_part, state_part = text.rsplit(",", 1)
    return city_part.strip(), state_part.strip()


def _is_all_area_selection(area_names: list[str]) -> bool:
    return not area_names or "ALL" in {str(area).strip() for area in area_names}


def _format_jobs_promise_date(df: pd.DataFrame) -> pd.Series:
    if "PROMISE_DATE" in df.columns:
        promise_text = df["PROMISE_DATE"].astype(str).str.replace(r"\.0+$", "", regex=True).str.strip()
        has_value = promise_text.ne("") & ~promise_text.str.lower().isin({"nan", "none", "nat"})
        if has_value.any():
            return promise_text.where(has_value, "")
    if "service_date" in df.columns:
        return pd.to_datetime(df["service_date"], errors="coerce").dt.strftime("%Y%m%d").fillna("")
    if "service_date_key" in df.columns:
        return pd.to_datetime(df["service_date_key"], errors="coerce").dt.strftime("%Y%m%d").fillna("")
    return pd.Series("", index=df.index, dtype="object")


def _build_vrp_jobs_input_df(service_df: pd.DataFrame) -> pd.DataFrame:
    if service_df.empty:
        return pd.DataFrame(columns=JOB_INPUT_COLUMNS)
    jobs_df = pd.DataFrame(index=service_df.index)
    for col in JOB_INPUT_COLUMNS:
        if col in {"PROMISE_DATE", "fixed", "job_slot_count"}:
            continue
        jobs_df[col] = service_df[col] if col in service_df.columns else ""
    jobs_df["PROMISE_DATE"] = _format_jobs_promise_date(service_df)
    jobs_df["fixed"] = service_df["fixed"] if "fixed" in service_df.columns else ""
    jobs_df["job_slot_count"] = service_df["job_slot_count"] if "job_slot_count" in service_df.columns else 1
    postal_text = jobs_df["POSTAL_CODE"].astype(str).str.replace(r"\.0+$", "", regex=True).str.strip()
    postal_text = postal_text.mask(postal_text.str.lower().isin({"", "nan", "none", "nat"}), "")
    jobs_df["POSTAL_CODE"] = postal_text.where(postal_text.eq(""), postal_text.str.zfill(5))
    jobs_df["job_slot_count"] = pd.to_numeric(jobs_df["job_slot_count"], errors="coerce").fillna(1).astype(int)
    return jobs_df[JOB_INPUT_COLUMNS].drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def _get_selected_frames(explorer_data, region_count: int | None):
    if region_count is None:
        return (
            explorer_data.current_zip_layer.copy(),
            explorer_data.current_area_layer.copy(),
            explorer_data.current_service_df.copy(),
        )
    return (
        explorer_data.integrated_zip_layer.copy(),
        explorer_data.integrated_area_layer.copy(),
        explorer_data.integrated_service_df.copy(),
    )


def _get_missing_geometry_zips(city_name: str) -> list[str]:
    city_data = load_city_map_data(city_name)
    coverage_zips = set(city_data.zip_coverage_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5))
    mapped_zips = set(city_data.zip_layer["POSTAL_CODE"].astype(str).str.strip().str.zfill(5))
    return sorted(coverage_zips - mapped_zips)


@st.cache_data(show_spinner=False)
def get_missing_geometry_zip_df(city_name: str) -> pd.DataFrame:
    city_data = load_city_map_data(city_name)
    coverage_df = city_data.zip_coverage_df.copy()
    coverage_df["POSTAL_CODE"] = coverage_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    mapped_zips = set(city_data.zip_layer["POSTAL_CODE"].astype(str).str.strip().str.zfill(5))

    area_choice_df = (
        coverage_df.groupby(["POSTAL_CODE", "AREA_NAME"])
        .size()
        .reset_index(name="row_count")
        .sort_values(["POSTAL_CODE", "row_count", "AREA_NAME"], ascending=[True, False, True])
        .drop_duplicates(subset=["POSTAL_CODE"], keep="first")
        [["POSTAL_CODE", "AREA_NAME"]]
        .copy()
    )
    missing_df = area_choice_df[~area_choice_df["POSTAL_CODE"].isin(mapped_zips)].copy()

    service_df = city_data.service_df.copy()
    if service_df.empty:
        missing_df["service_count"] = 0
        missing_df["latitude"] = pd.NA
        missing_df["longitude"] = pd.NA
    else:
        service_df["POSTAL_CODE"] = service_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
        service_stats = (
            service_df.groupby("POSTAL_CODE")
            .agg(
                service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
                latitude=("latitude", "mean"),
                longitude=("longitude", "mean"),
            )
            .reset_index()
        )
        missing_df = missing_df.merge(service_stats, on="POSTAL_CODE", how="left")
        missing_df["service_count"] = missing_df["service_count"].fillna(0).astype(int)

    missing_df["has_point"] = missing_df["latitude"].notna() & missing_df["longitude"].notna()
    return missing_df.sort_values(["service_count", "POSTAL_CODE"], ascending=[False, True]).reset_index(drop=True)


def _get_area_column_name(region_count: int | None, zip_layer: pd.DataFrame) -> str:
    if "AREA_NAME" in zip_layer.columns:
        return "AREA_NAME"
    if region_count is None and "primary_area_name" in zip_layer.columns:
        return "primary_area_name"
    return "AREA_NAME"


def _center_from_layers(area_layer, service_df):
    if not area_layer.empty:
        center_points = area_layer.to_crs(epsg=3857).geometry.centroid.to_crs(epsg=4326)
        return float(center_points.y.mean()), float(center_points.x.mean())
    if not service_df.empty:
        return float(service_df["latitude"].mean()), float(service_df["longitude"].mean())
    return 39.8283, -98.5795


def _route_city_name_for_scope(city_name: str, subsidiary_name: str, service_df: pd.DataFrame) -> str:
    countries = set()
    if "COUNTRY_NAME" in service_df.columns:
        countries = {
            _normalize_filter_text(value).upper()
            for value in service_df["COUNTRY_NAME"].dropna().unique()
            if _normalize_filter_text(value)
        }
    subsidiary = _normalize_filter_text(subsidiary_name).upper()
    if "IDN" in countries or subsidiary == "LGEID":
        return "INDONESIA"
    if "THA" in countries:
        return "THAILAND"
    if countries & {"MYS", "MALAYSIA"}:
        return "MALAYSIA"
    if city_name != ALL_CITIES:
        return city_name
    return city_name


def _build_asm_boundary_layer(area_layer: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if area_layer.empty or "asm_boundary_name" not in area_layer.columns:
        return gpd.GeoDataFrame(columns=["asm_boundary_name", "area_count", "area_names", "geometry"], geometry="geometry", crs="EPSG:4326")
    working_df = area_layer.copy()
    working_df["asm_boundary_name"] = working_df["asm_boundary_name"].fillna("").astype(str).str.strip()
    working_df = working_df[
        working_df["asm_boundary_name"].ne("")
        & working_df["asm_boundary_name"].ne("Unmapped")
    ].copy()
    if working_df.empty:
        return gpd.GeoDataFrame(columns=["asm_boundary_name", "area_count", "area_names", "geometry"], geometry="geometry", crs=area_layer.crs)
    boundary_df = (
        working_df.groupby("asm_boundary_name")
        .agg(
            area_count=("AREA_NAME", "nunique"),
            area_names=("AREA_NAME", lambda s: " | ".join(sorted(set(map(str, s))))),
            geometry=("geometry", lambda g: g.union_all()),
        )
        .reset_index()
    )
    return gpd.GeoDataFrame(boundary_df, geometry="geometry", crs=area_layer.crs)


@st.cache_data(show_spinner=False)
def get_route_payload(city_name: str, sm_code: str, date_key: str, coords: tuple[tuple[float, float], ...], preserve_first: bool = False):
    if not coords:
        return {"ordered_coords": [], "distance_km": 0.0, "duration_min": 0.0, "geometry": []}
    client_map, default_client = get_clients()
    client = client_map.get(city_name, default_client)
    return client.build_ordered_route(coords, preserve_first=preserve_first)


@st.cache_data(show_spinner=False, ttl=60)
def is_route_backend_ready(city_name: str) -> bool:
    probe = ROUTE_HEALTH_PROBES.get(str(city_name).strip().upper())
    if not probe:
        return True
    client_map, default_client = get_clients()
    client = client_map.get(city_name, default_client)
    (lon1, lat1), (lon2, lat2) = probe
    url = (
        f"{client.cfg.osrm_url}/route/v1/{client.cfg.osrm_profile}/"
        f"{lon1},{lat1};{lon2},{lat2}?overview=false"
    )
    try:
        response = requests.get(url, timeout=2)
        if not response.ok:
            return False
        return response.json().get("code") == "Ok"
    except Exception:
        return False


def _build_fallback_route_payload(coords: tuple[tuple[float, float], ...], preserve_first: bool = False) -> dict[str, object]:
    fallback_client = OSRMTripClient(
        OSRMConfig(
            osrm_url="",
            mode="haversine",
        )
    )
    return fallback_client.build_ordered_route(coords, preserve_first=preserve_first)


@st.cache_data(show_spinner=False)
def _load_saved_home_geocode_df(city_name: str) -> pd.DataFrame:
    path = PRODUCTION_INPUT_DIR / f"{_city_slug(city_name)}_engineer_home_geocoded.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8-sig")
    rename_map = {
        "GSFS_RECEIPT_NO": "SVC_ENGINEER_CODE",
        "ADDRESS_LINE1_INFO": "Home Street Address",
        "CITY_NAME": "City ",
        "STATE_NAME": "State",
        "POSTAL_CODE": "Zip",
    }
    df = df.rename(columns={key: value for key, value in rename_map.items() if key in df.columns})
    required = {"SVC_ENGINEER_CODE", "Home Street Address", "latitude", "longitude"}
    if not required.issubset(df.columns):
        return pd.DataFrame()
    df["SVC_ENGINEER_CODE"] = df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    return df.dropna(subset=["latitude", "longitude"]).copy()


def _load_profile_home_geocode_df() -> pd.DataFrame:
    profile_file = Path(str(_load_config().get("area_map_asia", {}).get("profile_file", PROFILE_FILE)))
    if not profile_file.exists():
        return pd.DataFrame()
    try:
        address_df = pd.read_excel(profile_file, sheet_name="4. Address")
    except Exception:
        return pd.DataFrame()
    required = {"SVC_ENGINEER_CODE", "Home Street Address", "latitude", "longitude"}
    if not required.issubset(set(address_df.columns)):
        return pd.DataFrame()
    address_df = address_df.copy()
    address_df["SVC_ENGINEER_CODE"] = address_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    address_df["latitude"] = pd.to_numeric(address_df["latitude"], errors="coerce")
    address_df["longitude"] = pd.to_numeric(address_df["longitude"], errors="coerce")
    return address_df.dropna(subset=["latitude", "longitude"]).copy()


def _load_profile_home_input_df(city_name: str, dms_codes: set[str]) -> pd.DataFrame:
    if not PROFILE_FILE.exists():
        return pd.DataFrame()
    try:
        address_df = pd.read_excel(PROFILE_FILE, sheet_name="4. Address")
    except Exception:
        return pd.DataFrame()
    required = {"SVC_ENGINEER_CODE", "Home Street Address", "City ", "State", "Zip"}
    if not required.issubset(set(address_df.columns)):
        return pd.DataFrame()

    home_input_df = address_df[address_df["SVC_ENGINEER_CODE"].astype(str).str.strip().isin(dms_codes)].copy()
    if home_input_df.empty:
        return pd.DataFrame()
    home_input_df = home_input_df.rename(
        columns={
            "SVC_ENGINEER_CODE": "GSFS_RECEIPT_NO",
            "Home Street Address": "ADDRESS_LINE1_INFO",
            "City ": "CITY_NAME",
            "State": "STATE_NAME",
            "Zip": "POSTAL_CODE",
        }
    )
    home_input_df["CITY_NAME"] = home_input_df["CITY_NAME"].fillna("").astype(str).str.strip()
    city_state_mask = home_input_df["CITY_NAME"].eq("") & home_input_df["STATE_NAME"].astype(str).str.contains(",", na=False)
    split_state = home_input_df.loc[city_state_mask, "STATE_NAME"].map(_split_city_state)
    if not split_state.empty:
        home_input_df.loc[city_state_mask, "CITY_NAME"] = split_state.map(lambda value: value[0])
        home_input_df.loc[city_state_mask, "STATE_NAME"] = split_state.map(lambda value: value[1])
    home_input_df["COUNTRY_NAME"] = "USA"
    return home_input_df


def _build_home_lookup_from_geocode_df(home_df: pd.DataFrame) -> dict[str, dict]:
    if home_df.empty:
        return {}
    code_col = "SVC_ENGINEER_CODE" if "SVC_ENGINEER_CODE" in home_df.columns else "GSFS_RECEIPT_NO"
    address_col = "Home Street Address" if "Home Street Address" in home_df.columns else "ADDRESS_LINE1_INFO"
    home_df = home_df.copy()
    home_df["latitude"] = pd.to_numeric(home_df.get("latitude"), errors="coerce")
    home_df["longitude"] = pd.to_numeric(home_df.get("longitude"), errors="coerce")
    home_df = home_df.dropna(subset=["latitude", "longitude"]).copy()
    lookup: dict[str, dict] = {}
    for _, row in home_df.iterrows():
        payload = {
            "coord": (float(row["longitude"]), float(row["latitude"])),
            "address": str(row.get(address_col, "")).strip(),
        }
        for key_col in [code_col, "Name", "SVC_ENGINEER_NAME"]:
            if key_col not in home_df.columns:
                continue
            key = str(row.get(key_col, "")).strip()
            if key:
                lookup[key] = payload
                lookup[key.upper()] = payload
    return lookup


@st.cache_data(show_spinner=False)
def get_home_location_lookup(city_name: str) -> dict[str, dict]:
    if str(city_name).strip().upper() in COUNTRY_ROUTE_KEYS:
        return _build_home_lookup_from_geocode_df(_load_profile_home_geocode_df())

    city_data = load_city_map_data(city_name)
    city_engineer_codes = set(
        city_data.zip_coverage_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    )

    saved_home_df = _load_saved_home_geocode_df(city_name)
    lookup = _build_home_lookup_from_geocode_df(saved_home_df)
    missing_codes = city_engineer_codes - set(lookup.keys())
    if not missing_codes:
        return lookup

    home_input_df = _load_profile_home_input_df(city_name, missing_codes)
    if home_input_df.empty:
        return lookup

    cfg = _load_config().get("geocoding", {})
    cache_frames = [
        load_geocode_cache(Path(str(cfg.get("census_cache_file", "data/geocode_cache_us_census.csv"))))
    ]
    google_cache_path = Path(str(cfg.get("google_cache_file", "data/geocode_cache_google.csv")))
    if google_cache_path.exists():
        cache_frames.append(load_geocode_cache(google_cache_path))
    cache_df = pd.concat(cache_frames, ignore_index=True).drop_duplicates(subset=["address_key"], keep="first")
    merged_df = merge_service_with_geocodes(home_input_df, cache_df)
    merged_df["latitude"] = pd.to_numeric(merged_df.get("latitude"), errors="coerce")
    merged_df["longitude"] = pd.to_numeric(merged_df.get("longitude"), errors="coerce")
    merged_df = merged_df.dropna(subset=["latitude", "longitude"]).copy()
    lookup.update(_build_home_lookup_from_geocode_df(merged_df))
    return lookup


def _build_route_groups(service_df: pd.DataFrame, city_name: str, selected_date: str, selected_sm: str) -> list[dict]:
    if selected_date == "ALL":
        return []
    route_df = service_df.copy()
    if selected_sm != "ALL":
        route_df = route_df[route_df["assigned_sm_code"] == selected_sm].copy()
    if route_df.empty:
        return []
    route_df = route_df[route_df["service_date_key"] == selected_date].copy()
    if route_df.empty:
        return []

    groups: list[dict] = []
    home_lookup = get_home_location_lookup(city_name)
    use_route_backend = is_route_backend_ready(city_name)
    for (service_date, sm_code), group_df in route_df.groupby(["service_date_key", "assigned_sm_code"], sort=True):
        service_coords = tuple(
            group_df[["longitude", "latitude"]]
            .dropna()
            .drop_duplicates()
            .apply(lambda r: (float(r["longitude"]), float(r["latitude"])), axis=1)
            .tolist()
        )
        home_info = None
        home_keys = [str(sm_code).strip()]
        for key_col in ["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME"]:
            if key_col in group_df.columns:
                home_keys.extend(group_df[key_col].dropna().astype(str).str.strip().unique().tolist())
        for home_key in home_keys:
            if not home_key:
                continue
            home_info = home_lookup.get(home_key) or home_lookup.get(home_key.upper())
            if home_info:
                break
        home_coord = home_info.get("coord") if home_info else None
        coords = ((home_coord,) + service_coords) if home_coord else service_coords
        route_payload = (
            get_route_payload(city_name, str(sm_code), str(service_date), coords, preserve_first=bool(home_coord))
            if use_route_backend
            else _build_fallback_route_payload(coords, preserve_first=bool(home_coord))
        )
        groups.append(
            {
                "service_date_key": str(service_date),
                "assigned_sm_code": str(sm_code),
                "route_payload": route_payload,
                "service_count": int(group_df["GSFS_RECEIPT_NO"].astype(str).nunique()),
                "home_coord": home_coord,
                "home_address": home_info.get("address", "") if home_info else "",
            }
        )
    return groups


def _build_stop_order_lookup(route_groups: list[dict]) -> dict[tuple[str, tuple[float, float]], int]:
    lookup: dict[tuple[str, tuple[float, float]], int] = {}
    for group in route_groups:
        sm_code = str(group.get("assigned_sm_code", "")).strip()
        home_coord = group.get("home_coord")
        home_key = (
            round(float(home_coord[0]), 6),
            round(float(home_coord[1]), 6),
        ) if home_coord else None
        seq = 1
        for coord in group["route_payload"].get("ordered_coords", []):
            key = (sm_code, (round(float(coord[0]), 6), round(float(coord[1]), 6)))
            if home_key is not None and key[1] == home_key:
                continue
            lookup[key] = seq
            seq += 1
    return lookup


def _build_dms_home_marker_rows(
    city_name: str,
    area_names: list[str],
    selected_sm: str,
    route_groups: list[dict],
) -> list[dict[str, object]]:
    city_data = load_city_map_data(city_name)
    coverage_df = city_data.zip_coverage_df.copy()
    if coverage_df.empty or "SVC_ENGINEER_CODE" not in coverage_df.columns:
        return []
    if not _is_all_area_selection(area_names) and "AREA_NAME" in coverage_df.columns:
        selected_area_set = {str(area).strip() for area in area_names if str(area).strip()}
        coverage_df = coverage_df[coverage_df["AREA_NAME"].astype(str).isin(selected_area_set)].copy()
    if "SVC_CENTER_TYPE" in coverage_df.columns:
        coverage_df = coverage_df[coverage_df["SVC_CENTER_TYPE"].astype(str).str.upper().eq("DMS")].copy()
    if selected_sm != "ALL":
        coverage_df = coverage_df[coverage_df["SVC_ENGINEER_CODE"].astype(str).str.strip().eq(str(selected_sm).strip())].copy()
    if coverage_df.empty:
        return []

    name_lookup = {}
    if not city_data.slot_df.empty and {"SVC_ENGINEER_CODE", "Name"}.issubset(city_data.slot_df.columns):
        name_lookup = {
            str(row["SVC_ENGINEER_CODE"]).strip(): str(row.get("Name", "")).strip()
            for _, row in city_data.slot_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).iterrows()
        }
    home_lookup = get_home_location_lookup(city_name)
    routed_codes = {str(group.get("assigned_sm_code", "")).strip() for group in route_groups}
    rows: list[dict[str, object]] = []
    for code in sorted(coverage_df["SVC_ENGINEER_CODE"].dropna().astype(str).str.strip().unique()):
        if not code or code in routed_codes:
            continue
        home_info = home_lookup.get(code)
        home_coord = home_info.get("coord") if home_info else None
        if not home_coord:
            continue
        rows.append(
            {
                "assigned_sm_code": code,
                "engineer_name": name_lookup.get(code, code),
                "home_coord": home_coord,
                "home_address": home_info.get("address", "") if home_info else "",
            }
        )
    return rows


def _add_home_marker(
    map_obj: folium.Map,
    sm_code: str,
    home_coord: tuple[float, float],
    *,
    marker_color: str,
    engineer_name: str = "",
    home_address: str = "",
    tooltip_suffix: str = "Home",
) -> None:
    home_lon, home_lat = home_coord
    icon_html = (
        f"<div style=\"font-size:10px;font-weight:700;color:{marker_color};"
        f"background:#ffffff;border:2px solid {marker_color};border-radius:12px;"
        "padding:2px 6px;text-align:center;white-space:nowrap;"
        "box-shadow:0 1px 5px rgba(0,0,0,0.28);\">Home</div>"
    )
    name_line = f"<b>Technician</b>: {engineer_name}<br>" if str(engineer_name).strip() else ""
    popup_html = (
        f"<b>Home</b><br>"
        f"{name_line}"
        f"<b>Assigned SM</b>: {sm_code}<br>"
        f"<b>Address</b>: {home_address}"
    )
    folium.Marker(
        location=[float(home_lat), float(home_lon)],
        icon=folium.DivIcon(html=icon_html, icon_size=(34, 18), icon_anchor=(17, 9)),
        popup=folium.Popup(popup_html, max_width=320),
        tooltip=f"{sm_code} | {tooltip_suffix}",
    ).add_to(map_obj)


def build_map(
    city_name: str,
    subsidiary_name: str,
    ship_to_full_name: str,
    region_count: int | None,
    area_names: list[str],
    selected_date: str,
    selected_sm: str,
    selected_center_buckets: list[str],
):
    explorer_data = get_route_explorer_data(city_name, region_count, AREA_MAP_CACHE_VERSION)
    zip_layer, area_layer, service_df = _get_selected_frames(explorer_data, region_count)
    area_col = _get_area_column_name(region_count, zip_layer)

    service_df = _apply_service_scope_filters(service_df, subsidiary_name, ship_to_full_name)
    zip_layer, area_layer = _apply_ship_to_area_scope(zip_layer, area_layer, service_df, ship_to_full_name, area_col)
    service_df["service_date_key"] = pd.to_datetime(service_df["service_date"]).dt.strftime("%Y-%m-%d")
    if not _is_all_area_selection(area_names):
        selected_area_set = {str(area).strip() for area in area_names if str(area).strip()}
        area_layer = area_layer[area_layer["AREA_NAME"].astype(str).isin(selected_area_set)].copy()
        zip_layer = zip_layer[zip_layer[area_col].astype(str).isin(selected_area_set)].copy()
        service_df = service_df[service_df["AREA_NAME"].astype(str).isin(selected_area_set)].copy()
    if selected_date != "ALL":
        service_df = service_df[service_df["service_date_key"] == selected_date].copy()
    if selected_sm != "ALL":
        service_df = service_df[service_df["assigned_sm_code"] == selected_sm].copy()
    service_df = _apply_center_bucket_rules(service_df, region_count)
    selected_center_set = {str(bucket).strip().upper() for bucket in selected_center_buckets if str(bucket).strip()}
    service_df = service_df[service_df["CENTER_BUCKET"].astype(str).str.upper().isin(selected_center_set)].copy()

    center_lat, center_lon = _center_from_layers(area_layer, service_df)
    zoom_start = 6 if city_name == ALL_CITIES else (10 if _is_all_area_selection(area_names) else 11)
    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_start, tiles="cartodbpositron")
    area_color_map = _generate_color_map(area_layer["AREA_NAME"].astype(str).tolist())

    if not area_layer.empty:
        area_fields = ["AREA_NAME", "postal_count", "service_count"]
        area_aliases = ["Area", "Postal Count", "Service Count"]
        for field, alias in [
            ("asm_names", "ASM"),
            ("tech_names", "Tech Name"),
            ("tech_detail", "Tech Detail"),
        ]:
            if field in area_layer.columns:
                area_fields.append(field)
                area_aliases.append(alias)
        if "avg_daily_assigned_sm_count" in area_layer.columns:
            area_fields.extend(["avg_daily_service_count", "avg_daily_assigned_sm_count"])
            area_aliases.extend(["Avg Daily Service", "Avg Daily Assigned SM"])
        folium.GeoJson(
            area_layer,
            name="Area",
            style_function=lambda feat: {
                "fillColor": area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                "color": area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                "weight": 1.0,
                "fillOpacity": 0.10,
            },
            highlight_function=lambda feat: {
                "fillColor": area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                "color": "#111111",
                "weight": 2.0,
                "fillOpacity": 0.18,
            },
            tooltip=folium.GeoJsonTooltip(fields=area_fields, aliases=area_aliases, localize=True),
            popup=folium.GeoJsonPopup(fields=area_fields, aliases=area_aliases, localize=True),
        ).add_to(map_obj)

        asm_boundary_layer = _build_asm_boundary_layer(area_layer)
        if not asm_boundary_layer.empty:
            asm_color_map = _generate_color_map(asm_boundary_layer["asm_boundary_name"].astype(str).tolist())
            folium.GeoJson(
                asm_boundary_layer,
                name="ASM Boundary",
                style_function=lambda feat: {
                    "fillColor": "transparent",
                    "color": asm_color_map.get(feat["properties"].get("asm_boundary_name", ""), "#111827"),
                    "weight": 3.0,
                    "fillOpacity": 0.0,
                    "opacity": 0.95,
                },
                highlight_function=lambda feat: {
                    "fillColor": "transparent",
                    "color": asm_color_map.get(feat["properties"].get("asm_boundary_name", ""), "#111827"),
                    "weight": 5.0,
                    "fillOpacity": 0.0,
                    "opacity": 1.0,
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=["asm_boundary_name", "area_count"],
                    aliases=["ASM", "Area Count"],
                    localize=True,
                ),
                popup=folium.GeoJsonPopup(
                    fields=["asm_boundary_name", "area_count", "area_names"],
                    aliases=["ASM", "Area Count", "Areas"],
                    localize=True,
                ),
            ).add_to(map_obj)

    route_city_name = _route_city_name_for_scope(city_name, subsidiary_name, service_df)
    route_groups = _build_route_groups(service_df, route_city_name, selected_date, selected_sm)
    stop_order_lookup = _build_stop_order_lookup(route_groups)
    route_color_map = _generate_color_map(service_df["assigned_sm_code"].dropna().astype(str).tolist())

    service_layer_map: dict[str, folium.FeatureGroup] = {}
    route_bucket_lookup: dict[tuple[str, str], str] = {}
    if not service_df.empty:
        if selected_date == "ALL":
            cluster = MarkerCluster(name="Service Points")
            for _, row in service_df.iterrows():
                border_width, border_color = _marker_border_style(row.get("CENTER_BUCKET", ""))
                popup_html = (
                    f"<b>Date</b>: {row.get('service_date_key', '')}<br>"
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')}<br>"
                    f"<b>Area</b>: {row.get('AREA_NAME', '')}<br>"
                    f"<b>Center Type</b>: {row.get('CENTER_BUCKET', '')}<br>"
                    f"<b>Assigned SM</b>: {row.get('assigned_sm_code', '')}<br>"
                    f"<b>Current SM</b>: {row.get('SVC_ENGINEER_CODE', '')} / {row.get('SVC_ENGINEER_NAME', '')}<br>"
                    f"<b>Postal</b>: {row.get('POSTAL_CODE', '')}<br>"
                    f"<b>Address</b>: {row.get('ADDRESS_LINE1_INFO', '')}"
                )
                folium.CircleMarker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    radius=3.2,
                    color=border_color,
                    weight=3 if border_width == "3px" else 1,
                    fill=True,
                    fill_color="#22c55e",
                    fill_opacity=0.72,
                    popup=folium.Popup(popup_html, max_width=360),
                ).add_to(cluster)
            cluster.add_to(map_obj)
        else:
            present_buckets = _center_bucket_options(service_df)
            service_layer_map = {
                bucket: folium.FeatureGroup(name=f"Numbered Service Points - {bucket}", show=True)
                for bucket in present_buckets
            }
            for _, row in service_df.iterrows():
                sm_code = str(row.get("assigned_sm_code", "")).strip()
                center_bucket = _normalize_center_bucket(row.get("CENTER_BUCKET", ""))
                marker_parent = service_layer_map.setdefault(
                    center_bucket,
                    folium.FeatureGroup(name=f"Numbered Service Points - {center_bucket}", show=True),
                )
                coord_key = (sm_code, (round(float(row["longitude"]), 6), round(float(row["latitude"]), 6)))
                seq = stop_order_lookup.get(coord_key)
                seq_label = str(seq) if seq is not None else "?"
                marker_color = route_color_map.get(sm_code, "#dc2626")
                popup_html = (
                    f"<b>Seq</b>: {seq_label}<br>"
                    f"<b>Date</b>: {row.get('service_date_key', '')}<br>"
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')}<br>"
                    f"<b>Area</b>: {row.get('AREA_NAME', '')}<br>"
                    f"<b>Center Type</b>: {row.get('CENTER_BUCKET', '')}<br>"
                    f"<b>Assigned SM</b>: {row.get('assigned_sm_code', '')}<br>"
                    f"<b>Current SM</b>: {row.get('SVC_ENGINEER_CODE', '')} / {row.get('SVC_ENGINEER_NAME', '')}<br>"
                    f"<b>Postal</b>: {row.get('POSTAL_CODE', '')}<br>"
                    f"<b>Address</b>: {row.get('ADDRESS_LINE1_INFO', '')}"
                )
                border_width, border_color = _marker_border_style(row.get("CENTER_BUCKET", ""))
                icon_width, icon_height = _marker_icon_size(row.get("CENTER_BUCKET", ""))
                icon_label = _marker_icon_label(row.get("CENTER_BUCKET", ""), seq_label)
                fixed_marker = any(
                    _truthy_marker_value(row.get(col, False))
                    for col in ("fixed", "fixed_x", "fixed_y")
                )
                if fixed_marker and border_color.lower() == "#ffffff":
                    border_color = "#111827"
                icon_html = (
                    f"<div style=\"background:{marker_color};color:#fff;border:{border_width} solid {border_color};"
                    f"border-radius:999px;width:{icon_width}px;height:{icon_height}px;line-height:{icon_height - 4}px;text-align:center;"
                    f"font-size:11px;font-weight:700;box-shadow:0 1px 6px rgba(0,0,0,0.35);\">{icon_label}</div>"
                )
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.DivIcon(
                        html=icon_html,
                        icon_size=(icon_width, icon_height),
                        icon_anchor=(icon_width // 2, icon_height // 2),
                    ),
                    popup=folium.Popup(popup_html, max_width=360),
                    tooltip=f"{sm_code} | {icon_label if icon_label in {'ASC', 'DMS2'} else f'Seq {seq_label}'}",
                ).add_to(marker_parent)
            route_bucket_df = (
                service_df.groupby(["service_date_key", "assigned_sm_code"])["CENTER_BUCKET"]
                .agg(_classify_assignment_group_bucket)
                .reset_index()
            )
            route_bucket_lookup = {
                (str(row["service_date_key"]), str(row["assigned_sm_code"]).strip()): _normalize_center_bucket(row["CENTER_BUCKET"])
                for _, row in route_bucket_df.iterrows()
            }

    for group in route_groups:
        geometry = group["route_payload"].get("geometry", [])
        if not geometry:
            continue
        route_parent = map_obj
        if service_layer_map:
            route_bucket = route_bucket_lookup.get(
                (str(group["service_date_key"]), str(group["assigned_sm_code"]).strip()),
                "UNKNOWN",
            )
            route_parent = service_layer_map.setdefault(
                route_bucket,
                folium.FeatureGroup(name=f"Numbered Service Points - {route_bucket}", show=True),
            )
        folium.PolyLine(
            locations=geometry,
            color=route_color_map.get(group["assigned_sm_code"], "#dc2626"),
            weight=4.0,
            opacity=0.85,
            tooltip=(
                f"{group['assigned_sm_code']} | {group['service_date_key']} | "
                f"{group['service_count']} jobs | {group['route_payload']['distance_km']:.1f} km | "
                f"{group['route_payload']['duration_min']:.1f} min"
            ),
        ).add_to(route_parent)

    for bucket in ["DMS", "DMS2", "ASC", "MIXED", "UNKNOWN"]:
        layer = service_layer_map.get(bucket)
        if layer is not None:
            layer.add_to(map_obj)
    for bucket, layer in service_layer_map.items():
        if bucket not in {"DMS", "DMS2", "ASC", "MIXED", "UNKNOWN"}:
            layer.add_to(map_obj)

    home_layer = folium.FeatureGroup(name="Home", show=True)
    has_home_marker = False
    for group in route_groups:
        home_coord = group.get("home_coord")
        if not home_coord:
            continue
        sm_code = str(group.get("assigned_sm_code", "")).strip()
        marker_color = route_color_map.get(sm_code, "#dc2626")
        route_bucket = route_bucket_lookup.get(
            (str(group["service_date_key"]), sm_code),
            "DMS" if selected_date == "ALL" else "UNKNOWN",
        )
        has_home_marker = True
        _add_home_marker(
            home_layer,
            sm_code,
            home_coord,
            marker_color=marker_color,
            home_address=str(group.get("home_address", "")),
        )

    if selected_date != "ALL" and "DMS" in selected_center_set:
        idle_home_rows = _build_dms_home_marker_rows(city_name, area_names, selected_sm, route_groups)
        for home_row in idle_home_rows:
            has_home_marker = True
            _add_home_marker(
                home_layer,
                str(home_row["assigned_sm_code"]),
                home_row["home_coord"],
                marker_color="#111827",
                engineer_name=str(home_row.get("engineer_name", "")),
                home_address=str(home_row.get("home_address", "")),
                tooltip_suffix="Home | no service",
            )

    if has_home_marker:
        home_layer.add_to(map_obj)

    folium.LayerControl(collapsed=False).add_to(map_obj)
    return map_obj, service_df, area_layer, route_groups


def _build_candidate_display_df(city_name: str) -> pd.DataFrame:
    stats_df = get_region_stats(city_name)
    if stats_df.empty:
        return stats_df
    current_row = {
        "Region Type": CURRENT_REGION_LABEL,
        "Assigned SM Count": stats_df["avg_daily_deployed_sm_current"].iloc[0],
        "Jobs per SM": stats_df["avg_jobs_per_sm_current"].iloc[0],
        "Jobs Std Dev": stats_df["avg_jobs_per_sm_std_current"].iloc[0],
        "Avg Distance (km)": stats_df["avg_distance_per_sm_km_current"].iloc[0],
        "Avg Duration (min)": stats_df["avg_duration_per_sm_min_current"].iloc[0],
        "Over 480 min (%)": stats_df["overflow_480_ratio_current"].iloc[0],
        "Best": "",
    }
    new_rows = []
    for _, row in stats_df.iterrows():
        new_rows.append(
            {
                "Region Type": f"New Region {int(row['candidate_region_count'])}",
                "Assigned SM Count": row["avg_daily_deployed_sm_integrated"],
                "Jobs per SM": row["avg_jobs_per_sm_integrated"],
                "Jobs Std Dev": row["avg_jobs_per_sm_std_integrated"],
                "Avg Distance (km)": row["avg_distance_per_sm_km_integrated"],
                "Avg Duration (min)": row["avg_duration_per_sm_min_integrated"],
                "Over 480 min (%)": row["overflow_480_ratio_integrated"],
                "Best": "Y" if bool(row.get("is_best_candidate", False)) else "",
            }
        )
    display_df = pd.DataFrame([current_row] + new_rows)
    numeric_cols = [col for col in display_df.columns if col not in {"Region Type", "Best"}]
    display_df[numeric_cols] = display_df[numeric_cols].apply(pd.to_numeric, errors="coerce").round(2)
    return display_df


def _build_monthly_area_stats_df(service_df: pd.DataFrame) -> pd.DataFrame:
    if service_df.empty or not {"AREA_NAME", "GSFS_RECEIPT_NO", "assigned_sm_code", "service_date"}.issubset(service_df.columns):
        return pd.DataFrame()
    working = service_df.copy()
    service_date = pd.to_datetime(working["service_date"], errors="coerce")
    working["service_month"] = service_date.dt.strftime("%Y-%m")
    working["service_date_key"] = service_date.dt.strftime("%Y-%m-%d")
    if "CENTER_BUCKET" not in working.columns:
        if "SVC_CENTER_TYPE" in working.columns:
            working["CENTER_BUCKET"] = working["SVC_CENTER_TYPE"].map(_normalize_center_bucket)
        else:
            working["CENTER_BUCKET"] = "UNKNOWN"
    working["CENTER_BUCKET"] = working["CENTER_BUCKET"].map(_normalize_center_bucket)
    working = working[
        working["service_month"].notna()
        & working["service_date_key"].notna()
        & working["AREA_NAME"].notna()
        & working["GSFS_RECEIPT_NO"].notna()
    ].copy()
    if working.empty:
        return pd.DataFrame()

    monthly_service = (
        working.groupby(["AREA_NAME", "service_month"])
        .agg(
            service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
        )
        .reset_index()
    )

    daily_sm = (
        working.dropna(subset=["assigned_sm_code"])
        .groupby(["AREA_NAME", "service_month", "service_date_key"])
        .agg(sm_count=("assigned_sm_code", lambda s: s.dropna().astype(str).nunique()))
        .reset_index()
    )
    monthly_sm = (
        daily_sm.groupby(["AREA_NAME", "service_month"])
        .agg(sm_count=("sm_count", "sum"))
        .reset_index()
    )

    monthly_total = monthly_service.merge(monthly_sm, on=["AREA_NAME", "service_month"], how="left")
    monthly_total["sm_count"] = pd.to_numeric(monthly_total["sm_count"], errors="coerce").fillna(0).astype(int)
    monthly_total["avg_service_count"] = monthly_total.apply(
        lambda row: round(float(row["service_count"]) / float(row["sm_count"]), 2) if int(row["sm_count"]) > 0 else 0.0,
        axis=1,
    )

    bucket_service = (
        working.groupby(["AREA_NAME", "service_month", "CENTER_BUCKET"])
        .agg(service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()))
        .reset_index()
    )
    bucket_daily_sm = (
        working.dropna(subset=["assigned_sm_code"])
        .groupby(["AREA_NAME", "service_month", "service_date_key", "CENTER_BUCKET"])
        .agg(sm_count=("assigned_sm_code", lambda s: s.dropna().astype(str).nunique()))
        .reset_index()
    )
    bucket_sm = (
        bucket_daily_sm.groupby(["AREA_NAME", "service_month", "CENTER_BUCKET"])
        .agg(sm_count=("sm_count", "sum"))
        .reset_index()
    )
    bucket_monthly = bucket_service.merge(bucket_sm, on=["AREA_NAME", "service_month", "CENTER_BUCKET"], how="left")
    bucket_monthly["sm_count"] = pd.to_numeric(bucket_monthly["sm_count"], errors="coerce").fillna(0).astype(int)
    bucket_monthly["avg_service_count"] = bucket_monthly.apply(
        lambda row: round(float(row["service_count"]) / float(row["sm_count"]), 2) if int(row["sm_count"]) > 0 else 0.0,
        axis=1,
    )

    bucket_order = _center_bucket_options(working)
    metric_order = ["Service Count", "SM Count", "Avg Service Count"]
    for bucket in bucket_order:
        metric_order.extend(
            [
                f"{bucket} Service Count",
                f"{bucket} SM Count",
                f"{bucket} Avg Service Count",
            ]
        )
    rows: list[dict[str, object]] = []
    for _, row in monthly_total.iterrows():
        rows.extend(
            [
                {"AREA_NAME": row["AREA_NAME"], "service_month": row["service_month"], "Metric": "Service Count", "value": int(row["service_count"])},
                {"AREA_NAME": row["AREA_NAME"], "service_month": row["service_month"], "Metric": "SM Count", "value": int(row["sm_count"])},
                {"AREA_NAME": row["AREA_NAME"], "service_month": row["service_month"], "Metric": "Avg Service Count", "value": float(row["avg_service_count"])},
            ]
        )
    for _, row in bucket_monthly.iterrows():
        bucket = str(row["CENTER_BUCKET"])
        rows.extend(
            [
                {"AREA_NAME": row["AREA_NAME"], "service_month": row["service_month"], "Metric": f"{bucket} Service Count", "value": int(row["service_count"])},
                {"AREA_NAME": row["AREA_NAME"], "service_month": row["service_month"], "Metric": f"{bucket} SM Count", "value": int(row["sm_count"])},
                {"AREA_NAME": row["AREA_NAME"], "service_month": row["service_month"], "Metric": f"{bucket} Avg Service Count", "value": float(row["avg_service_count"])},
            ]
        )
    if not rows:
        return pd.DataFrame()

    long_df = pd.DataFrame(rows)
    months = sorted(long_df["service_month"].dropna().astype(str).unique().tolist())
    area_names = sorted(long_df["AREA_NAME"].dropna().astype(str).unique().tolist())
    output_rows = []
    for area_name in area_names:
        area_df = long_df[long_df["AREA_NAME"].astype(str) == area_name]
        for metric in metric_order:
            metric_df = area_df[area_df["Metric"] == metric]
            output_row = {"AREA_NAME": area_name, "Metric": metric}
            month_values = metric_df.set_index("service_month")["value"].to_dict()
            for month in months:
                value = month_values.get(month, 0)
                output_row[month] = round(float(value), 2) if "Avg" in metric else int(value)
            output_rows.append(output_row)
    return pd.DataFrame(output_rows, columns=["AREA_NAME", "Metric"] + months)


def main():
    st.title("Asia Routing Map")
    latest_service = get_latest_geocoded_service_file()
    if latest_service is not None:
        st.caption(f"Service source: `{latest_service}`")

    with st.sidebar:
        st.header("Filters")
        subsidiary_options, ship_to_options_by_subsidiary = get_service_scope_options(str(latest_service) if latest_service else None)
        selected_subsidiary = st.selectbox("SUBSIDIARY_NAME", subsidiary_options, index=0)
        ship_to_options = ship_to_options_by_subsidiary.get(selected_subsidiary, [ALL_OPTION])
        selected_ship_to = st.selectbox("SHIP_TO_FULL_NAME", ship_to_options, index=0)
        city_name = ALL_CITIES

        candidate_counts = load_region_count_options(city_name)
        region_options = [CURRENT_REGION_LABEL] + [f"New Region {count}" for count in candidate_counts]
        region_option = st.selectbox("Region Type", region_options, index=0)
        selected_region_count = _parse_region_option(region_option)

        explorer_data = get_route_explorer_data(city_name, selected_region_count, AREA_MAP_CACHE_VERSION)
        zip_layer, area_layer, service_df = _get_selected_frames(explorer_data, selected_region_count)
        area_col = _get_area_column_name(selected_region_count, zip_layer)
        service_df = service_df.copy()
        service_df = _apply_service_scope_filters(service_df, selected_subsidiary, selected_ship_to)
        zip_layer, area_layer = _apply_ship_to_area_scope(zip_layer, area_layer, service_df, selected_ship_to, area_col)
        service_df["service_date_key"] = pd.to_datetime(service_df["service_date"]).dt.strftime("%Y-%m-%d")
        service_df = _apply_center_bucket_rules(service_df, selected_region_count)

        date_options = ["ALL"] + sorted(service_df["service_date_key"].dropna().unique().tolist())
        selected_date = st.selectbox("Date", date_options, index=0)

        st.caption("Center Type")
        center_bucket_options = _center_bucket_options(service_df)
        center_cols = st.columns(max(1, min(3, len(center_bucket_options))))
        selected_center_buckets = []
        for idx, center_bucket in enumerate(center_bucket_options):
            center_col = center_cols[idx % len(center_cols)]
            if center_col.checkbox(
                center_bucket,
                value=True,
                key=f"center_bucket_filter::{city_name}::{selected_ship_to}::{region_option}::{selected_date}::{center_bucket}",
            ):
                selected_center_buckets.append(center_bucket)

        area_source_df = area_layer if not area_layer.empty else service_df
        area_options = sorted(
            value
            for value in area_source_df["AREA_NAME"].dropna().astype(str).unique().tolist()
            if value.strip()
        ) if "AREA_NAME" in area_source_df.columns else []
        selected_area_names = st.multiselect(
            "AREA NAME",
            area_options,
            key=f"area_name_filter::{city_name}::{selected_ship_to}::{region_option}",
        )

        date_service_df = service_df[service_df["service_date_key"] == selected_date].copy() if selected_date != "ALL" else service_df.copy()
        selected_center_set = {str(bucket).strip().upper() for bucket in selected_center_buckets if str(bucket).strip()}
        date_center_service_df = date_service_df[date_service_df["CENTER_BUCKET"].astype(str).str.upper().isin(selected_center_set)].copy()

        sm_df = date_center_service_df.copy()
        if not _is_all_area_selection(selected_area_names):
            sm_df = sm_df[sm_df["AREA_NAME"].astype(str).isin(selected_area_names)].copy()
        sm_options = ["ALL"] + sorted(sm_df["assigned_sm_code"].astype(str).unique().tolist())
        selected_sm = st.selectbox("Assigned SM Code", sm_options, index=0)

        total_service_count = int(service_df["GSFS_RECEIPT_NO"].astype(str).nunique()) if not service_df.empty else 0
        area_service_df = (
            date_center_service_df.copy()
            if _is_all_area_selection(selected_area_names)
            else date_center_service_df[date_center_service_df["AREA_NAME"].astype(str).isin(selected_area_names)].copy()
        )
        sm_service_df = area_service_df[area_service_df["assigned_sm_code"] == selected_sm].copy() if selected_sm != "ALL" else area_service_df.copy()
        area_count_df = (
            sm_service_df.groupby("AREA_NAME")
            .agg(
                service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
            )
            .reset_index()
            .sort_values(["service_count", "AREA_NAME"], ascending=[False, True])
            if not sm_service_df.empty and "CENTER_BUCKET" in sm_service_df.columns
            else pd.DataFrame(columns=["AREA_NAME", "service_count"])
        )
        selected_service_count_df = (
            sm_service_df.groupby(["service_date_key", "assigned_sm_code"])
            .agg(service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()))
            .reset_index()
            if not sm_service_df.empty
            else pd.DataFrame(columns=["service_date_key", "assigned_sm_code", "service_count"])
        )
        avg_service_count = (
            float(selected_service_count_df["service_count"].mean())
            if not selected_service_count_df.empty
            else 0.0
        )
        service_count_std = (
            float(selected_service_count_df["service_count"].std(ddof=0))
            if len(selected_service_count_df) > 1
            else 0.0
        )
        scope_service_count = int(sm_service_df["GSFS_RECEIPT_NO"].astype(str).nunique()) if not sm_service_df.empty else 0
        sm_count_col = "SVC_ENGINEER_CODE" if "SVC_ENGINEER_CODE" in sm_service_df.columns else "assigned_sm_code"
        scope_center_counts = (
            sm_service_df.groupby("CENTER_BUCKET")
            .agg(service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()))
            .to_dict("index")
            if not sm_service_df.empty and "CENTER_BUCKET" in sm_service_df.columns
            else {}
        )
        service_breakdown = ", ".join(
            f"{bucket} {int(values.get('service_count', 0))}"
            for bucket, values in sorted(scope_center_counts.items())
        )
        scope_sm_bucket_counts = (
            sm_service_df.groupby(["service_date_key", sm_count_col])["CENTER_BUCKET"]
            .agg(_classify_assignment_group_bucket)
            .reset_index()
            .groupby("CENTER_BUCKET")
            .size()
            .to_dict()
            if not sm_service_df.empty and "CENTER_BUCKET" in sm_service_df.columns
            else {}
        )
        mixed_sm_count = int(scope_sm_bucket_counts.get("MIXED", 0))
        scope_assigned_sm_count = int(sum(scope_sm_bucket_counts.values()))
        assigned_sm_breakdown = ", ".join(f"{bucket} {count}" for bucket, count in sorted(scope_sm_bucket_counts.items())) or "None"
        assigned_sm_label = "Assigned SM Count" if selected_date == "ALL" else "Assigned SM Count"
        route_city_name = _route_city_name_for_scope(city_name, selected_subsidiary, sm_service_df)
        route_backend_ready = is_route_backend_ready(route_city_name) if selected_date != "ALL" else True
        scope_route_groups = _build_route_groups(sm_service_df, route_city_name, selected_date, selected_sm)
        moving_route_groups = [
            group
            for group in scope_route_groups
            if float(group["route_payload"]["distance_km"]) > 0 or float(group["route_payload"]["duration_min"]) > 0
        ]
        if moving_route_groups:
            avg_distance = sum(float(group["route_payload"]["distance_km"]) for group in moving_route_groups) / len(moving_route_groups)
            avg_duration = sum(float(group["route_payload"]["duration_min"]) for group in moving_route_groups) / len(moving_route_groups)
        else:
            avg_distance = 0.0
            avg_duration = 0.0

        st.divider()
        st.caption(f"Total Service Count: {total_service_count}")
        if selected_region_count is None:
            st.caption(f"Service Count: {scope_service_count} ({service_breakdown or 'None'})")
            st.caption(f"{assigned_sm_label}: {scope_assigned_sm_count} ({assigned_sm_breakdown})")
            st.caption(f"Avg. Service Count: {avg_service_count:.2f}")
        else:
            st.caption(f"Service Count: {scope_service_count} ({service_breakdown or 'None'})")
            st.caption(f"{assigned_sm_label}: {scope_assigned_sm_count} ({assigned_sm_breakdown})")
            st.caption(f"Avg. Service Count: {avg_service_count:.2f}")
        selected_center_label = ", ".join(selected_center_buckets) if selected_center_buckets else "None"
        st.caption(f"Average Distance ({selected_center_label}): {avg_distance * KM_TO_MILE:.2f} mile")
        st.caption(f"Average Duration ({selected_center_label}): {avg_duration:.2f} min")
        st.caption(f"Service Std Dev ({selected_center_label}): {service_count_std:.2f}")
        if selected_date != "ALL" and not route_backend_ready:
            st.warning(f"{route_city_name} OSRM is not reachable from this app. Showing straight-line fallback routes.")
        st.subheader("Service Count by Area")
        st.dataframe(area_count_df, width="stretch", height=220)

    map_obj, filtered_service_df, filtered_area_layer, route_groups = build_map(
        city_name=city_name,
        subsidiary_name=selected_subsidiary,
        ship_to_full_name=selected_ship_to,
        region_count=selected_region_count,
        area_names=selected_area_names,
        selected_date=selected_date,
        selected_sm=selected_sm,
        selected_center_buckets=selected_center_buckets,
    )

    metric_cols = st.columns(4)
    area_count_value = (
        int(filtered_area_layer["AREA_NAME"].nunique())
        if not filtered_area_layer.empty and "AREA_NAME" in filtered_area_layer.columns
        else int(filtered_service_df["AREA_NAME"].nunique())
        if not filtered_service_df.empty and "AREA_NAME" in filtered_service_df.columns
        else 0
    )
    metric_cols[0].metric("Area Count", area_count_value)
    metric_cols[1].metric("Service Count", int(filtered_service_df["GSFS_RECEIPT_NO"].astype(str).nunique()) if not filtered_service_df.empty else 0)
    filtered_sm_count_col = "SVC_ENGINEER_CODE" if "SVC_ENGINEER_CODE" in filtered_service_df.columns else "assigned_sm_code"
    metric_cols[2].metric("Assigned SM Count", int(filtered_service_df[filtered_sm_count_col].astype(str).nunique()) if not filtered_service_df.empty else 0)
    metric_cols[3].metric("Visible Routes", len(route_groups))

    st.iframe(map_obj.get_root().render(), height=880)

    candidate_col, summary_col, detail_col = st.columns([1.25, 1.0, 1.75], gap="medium")
    with candidate_col:
        st.subheader("Candidate Region Summary")
        candidate_df = _build_candidate_display_df(city_name)
        if candidate_df.empty:
            st.caption("No candidate summary data.")
        else:
            st.dataframe(candidate_df, width="stretch", height=300)
        st.subheader("Jobs Input")
        jobs_input_df = _build_vrp_jobs_input_df(filtered_service_df)
        if jobs_input_df.empty:
            st.caption("No jobs for the selected filters.")
        else:
            selected_area_label = "all_areas" if _is_all_area_selection(selected_area_names) else f"{len(selected_area_names)}_areas"
            selected_date_label = str(selected_date).replace("-", "") if selected_date != "ALL" else "all_dates"
            st.download_button(
                "Download Jobs CSV",
                data=_to_csv_bytes(jobs_input_df),
                file_name=f"jobs_input_{city_name.split(',')[0].lower().replace(' ', '_')}_{selected_area_label}_{selected_date_label}.csv",
                mime="text/csv",
                width="stretch",
            )
            st.dataframe(jobs_input_df, width="stretch", height=300)

    with summary_col:
        st.subheader("Area Summary")
        if filtered_area_layer.empty:
            st.caption("No area data.")
        else:
            area_cols = [c for c in ["AREA_NAME", "postal_count", "service_count", "avg_daily_service_count", "avg_daily_assigned_sm_count", "area_km2"] if c in filtered_area_layer.columns]
            area_df = filtered_area_layer.drop(columns="geometry")[area_cols].copy()
            if "area_km2" in area_df.columns:
                area_df["area_km2"] = pd.to_numeric(area_df["area_km2"], errors="coerce").round(2)
            st.dataframe(area_df.sort_values(area_cols[0]), width="stretch", height=300)

    with detail_col:
        st.subheader("Assigned SM Summary")
        if filtered_service_df.empty:
            st.caption("No service data.")
        else:
            detail_working_df = filtered_service_df.copy()
            detail_working_df["service_time_min"] = _get_service_time_series(detail_working_df)
            sm_summary_df = (
                detail_working_df.groupby(["assigned_sm_code", "service_date_key"])
                .agg(
                    region_name=("AREA_NAME", "first"),
                    center_bucket=("CENTER_BUCKET", "first"),
                    service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
                    postal_count=("POSTAL_CODE", "nunique"),
                    service_time_min=("service_time_min", "sum"),
                )
                .reset_index()
                .sort_values(["assigned_sm_code", "service_date_key"])
            )
            route_distance_df = pd.DataFrame(
                [
                    {
                        "assigned_sm_code": str(group["assigned_sm_code"]),
                        "service_date_key": str(group["service_date_key"]),
                        "route_distance_mile": round(float(group["route_payload"]["distance_km"]) * KM_TO_MILE, 2),
                    }
                    for group in route_groups
                ]
            )
            if not route_distance_df.empty:
                sm_summary_df = sm_summary_df.merge(
                    route_distance_df,
                    on=["assigned_sm_code", "service_date_key"],
                    how="left",
                )
            else:
                sm_summary_df["route_distance_mile"] = pd.NA
            sm_summary_df["service_time_min"] = pd.to_numeric(
                sm_summary_df["service_time_min"],
                errors="coerce",
            ).round(2)
            st.dataframe(sm_summary_df, width="stretch", height=300)
            if selected_date == "ALL":
                st.subheader("Monthly Area Stats")
                monthly_area_stats_df = _build_monthly_area_stats_df(filtered_service_df)
                if monthly_area_stats_df.empty:
                    st.caption("No monthly area data.")
                else:
                    st.dataframe(monthly_area_stats_df, width="stretch", height=480)


if __name__ == "__main__":
    main()
