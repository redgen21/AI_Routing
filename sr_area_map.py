from __future__ import annotations

import colorsys
import hashlib
import html
import io
import json
from pathlib import Path
import re
import shutil
from typing import Mapping

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
    load_zcta_geometry as _area_map_load_zcta_geometry,
    load_profile_data as _area_map_load_profile_data,
    load_region_count_stats,
    load_route_explorer_data as _area_map_load_route_explorer_data,
    load_service_points as _area_map_load_service_points,
)
from smart_routing.census_geocoder import load_geocode_cache, merge_service_with_geocodes
from smart_routing.data_catalog import na_data_path
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient
from smart_routing.routing_policy_catalog import (
    routing_policy_description,
    routing_policy_label,
)
from smart_routing.region_candidate_planner import build_city_region_candidate, load_city_technician_roster
from tools.data.atlanta_6area_plan import (
    Atlanta6AreaPlanError,
    parse_atlanta_6area_workbook,
)
from tools.data.region_plan_area_map import (
    AreaMapRegionPlanError,
    POLICY_MODES as REGION_PLAN_POLICY_MODES,
    build_area_map_region_plan,
    save_area_map_region_plan,
)


st.set_page_config(page_title="Routing Map", layout="wide")

CONFIG_FILE = Path("config/config.json")
COMMON_CONFIG_FILE = Path("config/common_vrp.dev.json")
AREA_MAP_CONFIG_SECTION = "area_map_usa"
PROFILE_FILE = na_data_path("profile_production")
ATLANTA_6AREA_WORKBOOK = Path("260310/New ATL Buckets.xlsx")
PRODUCTION_INPUT_DIR = na_data_path("region_seed_dir")
ALL_OPTION = "ALL"
BLANK_CITY_OPTION = "(Blank)"
AREA_TYPE_FILTERS = [
    ("DMS", "DMS_CORE"),
    ("DMS2", "DMS2_EXCLUSIVE"),
    ("OVERLAP", "OVERLAP"),
]
DEFAULT_OSRM_URL = "http://20.51.244.68:5000"
DEFAULT_CITY_OSRM_URLS = {
    "Los Angeles, CA": "http://20.51.244.68:5001",
    "Atlanta, GA": "http://20.51.244.68:5002",
}
REGION_PLAN_ROOT = Path("data/region_plans")
REGION_CANDIDATE_SERVICE_FILE = Path("260310/input/Service_202606151658_final_geocoded.csv")
REGION_CANDIDATE_PROFILE_FILE = Path("260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx")
REGION_CANDIDATE_OUTPUT_ROOT = na_data_path("region_candidates_dir") / "home_allocation"
REGION_SUMMARY_DISPLAY_COLUMNS = (
    "region_seq", "AREA_NAME", "postal_count", "area_km2", "annual_service_count",
    "avg_daily_jobs", "assigned_technician_count", "avg_daily_jobs_per_assigned_technician",
    "assigned_technician_names",
)
COUNTRY_ROUTE_KEYS = {"THAILAND", "INDONESIA", "MALAYSIA"}
ROUTE_CITY_ALIASES = {
    "North Jersey, NJ": "Northeast",
    "Philadelphia, PA": "Northeast",
}
PINK_SM_LABEL_CODES = {
    "AI102692",
    "AI103347",
    "AI103428",
    "AI103452",
    "AI103544",
    "AI103583",
    "AI103741",
    "AI105047",
    "AI105122",
}
ORANGE_SM_LABEL_CODES = {
    "AI103127",
    "AI104034",
    "AI105028",
    "AI105051",
}
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


def get_latest_geocoded_service_file():
    return _area_map_get_latest_geocoded_service_file(config_section=AREA_MAP_CONFIG_SECTION)


def _base_city_name(city_name: str) -> str:
    # Area Plans carry their own source-strategic-city lineage.  Do not map
    # policy-city names through a hardcoded Atlanta alias table.
    return str(city_name or "").strip()


def load_city_map_data(city_name: str = ALL_CITIES):
    return _area_map_load_city_map_data(
        city_name=_base_city_name(city_name), config_section=AREA_MAP_CONFIG_SECTION
    )


def load_route_explorer_data(city_name: str, region_count: int | None = None):
    return _area_map_load_route_explorer_data(
        city_name=_base_city_name(city_name),
        region_count=region_count,
        config_section=AREA_MAP_CONFIG_SECTION,
    )


def _load_config(config_file: Path = CONFIG_FILE) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def _current_coverage_source_fingerprint(service_file: Path | None) -> str:
    if service_file is None or not service_file.exists():
        return "unavailable"
    stat = service_file.stat()
    return f"{service_file.resolve()}::{stat.st_mtime_ns}::{stat.st_size}"


@st.cache_data(show_spinner=False)
def get_route_explorer_data(
    city_name: str,
    region_count: int | None,
    cache_version: str,
    service_source_fingerprint: str = "",
):
    # Region membership is supplied by the selected Area Plan.  The legacy
    # region-count loader is intentionally never used by the map runtime.
    source_region_count = None
    # The fingerprint exists only to invalidate Streamlit's in-memory cache.
    # The authoritative loader resolves the configured source and validates its
    # own disk-cache metadata (source path, mtime, and config) before returning.
    _ = service_source_fingerprint
    return load_route_explorer_data(city_name=city_name, region_count=source_region_count)


@st.cache_data(show_spinner=False)
def get_region_stats(city_name: str):
    return load_region_count_stats(_base_city_name(city_name))


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


def _area_type_color(area_type: object, fallback: str) -> str:
    text = str(area_type or "").strip().upper()
    if text == "DMS_CORE":
        return "#16a34a"
    if text == "OVERLAP":
        return "#f59e0b"
    if text == "DMS2_EXCLUSIVE":
        return "#94a3b8"
    return fallback


def _available_area_types(*frames: pd.DataFrame) -> list[str]:
    available: set[str] = set()
    for frame in frames:
        if frame is None or frame.empty or "area_type" not in frame.columns:
            continue
        available.update(frame["area_type"].dropna().astype(str).str.strip().str.upper().tolist())
    return [value for _, value in AREA_TYPE_FILTERS if value in available]


def _area_type_label(area_type: str) -> str:
    labels = {value: label for label, value in AREA_TYPE_FILTERS}
    return labels.get(str(area_type).strip().upper(), str(area_type))


def _filter_area_type(df: pd.DataFrame, selected_area_types: list[str] | None) -> pd.DataFrame:
    if df.empty or "area_type" not in df.columns or selected_area_types is None:
        return df.copy()
    selected = {str(value).strip().upper() for value in selected_area_types if str(value).strip()}
    if not selected:
        return df.iloc[0:0].copy()
    return df[df["area_type"].astype(str).str.strip().str.upper().isin(selected)].copy()


def _region_options_for_city(city_name: str) -> tuple[list[str], dict[str, int | None]]:
    # Retained as a compatibility shim for imports from older local helpers;
    # region choices are no longer exposed by Area Map.
    return [], {}


def _normalize_center_bucket(center_type: object) -> str:
    text = _normalize_filter_text(center_type)
    upper = text.upper()
    if upper in {"DMS", "DMS2"}:
        return upper
    return "ASC"


def _center_bucket_options(service_df: pd.DataFrame) -> list[str]:
    preferred = ["DMS", "DMS2", "ASC"]
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
            usecols=lambda col: col in {"SUBSIDIARY_NAME", "STRATEGIC_CITY_NAME"},
        )
    except Exception:
        return [ALL_OPTION], {ALL_OPTION: [ALL_OPTION]}
    if "SUBSIDIARY_NAME" not in df.columns:
        df["SUBSIDIARY_NAME"] = ""
    if "STRATEGIC_CITY_NAME" not in df.columns:
        df["STRATEGIC_CITY_NAME"] = ""
    df["SUBSIDIARY_NAME"] = df["SUBSIDIARY_NAME"].map(_normalize_filter_text)
    df["STRATEGIC_CITY_NAME"] = df["STRATEGIC_CITY_NAME"].map(_normalize_filter_text)
    subsidiaries = sorted(value for value in df["SUBSIDIARY_NAME"].dropna().unique().tolist() if value)
    subsidiary_options = [ALL_OPTION] + subsidiaries
    city_options_by_subsidiary: dict[str, list[str]] = {}
    for subsidiary in subsidiary_options:
        scoped = df if subsidiary == ALL_OPTION else df[df["SUBSIDIARY_NAME"].eq(subsidiary)]
        cities = sorted(value for value in scoped["STRATEGIC_CITY_NAME"].dropna().unique().tolist() if value)
        options = [ALL_OPTION] + cities
        if scoped["STRATEGIC_CITY_NAME"].fillna("").astype(str).str.strip().eq("").any():
            options.append(BLANK_CITY_OPTION)
        city_options_by_subsidiary[subsidiary] = options
    return subsidiary_options, city_options_by_subsidiary


def _apply_service_scope_filters(service_df: pd.DataFrame, subsidiary_name: str, strategic_city_name: str) -> pd.DataFrame:
    filtered = service_df.copy()
    if subsidiary_name != ALL_OPTION and "SUBSIDIARY_NAME" in filtered.columns:
        filtered = filtered[filtered["SUBSIDIARY_NAME"].map(_normalize_filter_text).eq(subsidiary_name)].copy()
    if strategic_city_name == BLANK_CITY_OPTION and "STRATEGIC_CITY_NAME" in filtered.columns:
        filtered = filtered[filtered["STRATEGIC_CITY_NAME"].map(_normalize_filter_text).eq("")].copy()
    elif strategic_city_name != ALL_OPTION and "STRATEGIC_CITY_NAME" in filtered.columns:
        selected_key = _base_city_name(strategic_city_name).casefold()
        filtered = filtered[
            filtered["STRATEGIC_CITY_NAME"].map(_normalize_filter_text).str.casefold().eq(selected_key)
        ].copy()
    return filtered


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
    if bucket.upper() in {"ASC", "DMS2"}:
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
    city_part = _base_city_name(city_name).split(",", 1)[0]
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


def _empty_area_plan_frames(explorer_data):
    """Return service points without silently applying a legacy region map."""
    service_df = explorer_data.current_service_df.copy()
    service_df["AREA_NAME"] = "NO_ACTIVE_AREA_PLAN"
    service_df["region_id"] = ""
    service_df["region_seq"] = pd.NA
    service_df["area_type"] = ""
    crs = getattr(explorer_data.current_zip_layer, "crs", "EPSG:4326")
    empty_zip = gpd.GeoDataFrame(
        columns=["POSTAL_CODE", "AREA_NAME", "geometry"],
        geometry="geometry",
        crs=crs,
    )
    empty_area = gpd.GeoDataFrame(
        columns=["AREA_NAME", "region_id", "region_seq", "area_type", "geometry"],
        geometry="geometry",
        crs=crs,
    )
    return empty_zip, empty_area, service_df


def _load_area_plan_frames(candidate: Mapping[str, object], explorer_data):
    """Build map layers from a locally selected Area Plan candidate.

    The normal Area View loader reads the legacy/current fixed-region files.
    A selected candidate must explicitly replace those layers; otherwise the
    Area Plan selector only changes the sidebar label while the map remains on
    the old source data.
    """
    plan_dir = Path(str(candidate.get("path") or ""))
    area_path = plan_dir / "normalized" / "area.csv"
    if not area_path.is_file():
        return None
    try:
        area_df = pd.read_csv(area_path, dtype=str, keep_default_na=False)
    except (OSError, ValueError, pd.errors.ParserError):
        return None
    required = {"postal_code", "region_name"}
    if not required.issubset(area_df.columns):
        return None

    area_df = area_df.copy()
    area_df["POSTAL_CODE"] = (
        area_df["postal_code"].astype(str).str.strip().str.replace(r"\.0+$", "", regex=True).str.zfill(5)
    )
    area_df["AREA_NAME"] = area_df["region_name"].astype(str).str.strip()
    if "region_code" in area_df.columns:
        area_df["region_id"] = area_df["region_code"].astype(str).str.strip()
    if "region_seq" in area_df.columns:
        area_df["region_seq"] = pd.to_numeric(area_df["region_seq"], errors="coerce")
    if "area_type" in area_df.columns:
        area_df["area_type"] = area_df["area_type"].astype(str).str.strip().str.upper()
    if "membership_rank" in area_df.columns:
        area_df["membership_rank"] = pd.to_numeric(area_df["membership_rank"], errors="coerce").fillna(1)
    else:
        area_df["membership_rank"] = area_df.groupby("POSTAL_CODE", sort=False).cumcount() + 1
    area_df = area_df[area_df["POSTAL_CODE"].str.fullmatch(r"\d{5}") & area_df["AREA_NAME"].ne("")].copy()
    if area_df.empty:
        return None

    primary_area = (
        area_df.sort_values(["POSTAL_CODE", "membership_rank"])
        .drop_duplicates("POSTAL_CODE", keep="first")
        .set_index("POSTAL_CODE")
    )
    requested_postals = sorted(area_df["POSTAL_CODE"].unique().tolist())
    base_zip = _area_map_load_zcta_geometry(
        requested_postals,
        config_section=AREA_MAP_CONFIG_SECTION,
    )
    if base_zip.empty or "POSTAL_CODE" not in base_zip.columns:
        return None
    base_zip = base_zip.copy()
    base_zip["POSTAL_CODE"] = base_zip["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    zip_layer = gpd.GeoDataFrame(
        area_df.merge(base_zip, on="POSTAL_CODE", how="left", suffixes=("", "_zcta")),
        geometry="geometry",
        crs=base_zip.crs,
    )

    service_df = explorer_data.current_service_df.copy()
    if "POSTAL_CODE" not in service_df.columns:
        return None
    service_df["POSTAL_CODE"] = service_df["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    for target_column, source_column in (
        ("AREA_NAME", "AREA_NAME"),
        ("region_id", "region_id"),
        ("region_seq", "region_seq"),
        ("area_type", "area_type"),
    ):
        if source_column in primary_area.columns:
            service_df[target_column] = service_df["POSTAL_CODE"].map(primary_area[source_column])
    service_df["AREA_NAME"] = service_df["AREA_NAME"].fillna("POSTAL_NOT_IN_ACTIVE_PLAN")
    service_df["ambiguity_status"] = service_df["POSTAL_CODE"].map(
        lambda postal: "resolved" if postal in primary_area.index else "unmapped"
    )

    area_rows = []
    for area_name, group in zip_layer.groupby("AREA_NAME", sort=False):
        geometries = group.geometry.dropna()
        geometry = geometries.union_all() if not geometries.empty else None
        area_rows.append({
            "AREA_NAME": area_name,
            "region_id": group["region_id"].dropna().iloc[0] if "region_id" in group and group["region_id"].notna().any() else "",
            "region_seq": group["region_seq"].dropna().iloc[0] if "region_seq" in group and group["region_seq"].notna().any() else None,
            "area_type": group["area_type"].dropna().iloc[0] if "area_type" in group and group["area_type"].notna().any() else "",
            "postal_count": int(group["POSTAL_CODE"].nunique()),
            "service_count": int(service_df.loc[service_df["AREA_NAME"].eq(area_name), "GSFS_RECEIPT_NO"].nunique())
            if "GSFS_RECEIPT_NO" in service_df.columns else 0,
            "geometry": geometry,
        })
    area_layer = gpd.GeoDataFrame(area_rows, geometry="geometry", crs=base_zip.crs)
    return zip_layer, area_layer, service_df


def _candidate_workbook_source() -> tuple[bytes | Path | None, str]:
    """Return the uploaded canonical source, falling back only to the local file."""
    widget_value = st.session_state.get("atlanta-canonical-workbook-upload")
    if widget_value is not None and callable(getattr(widget_value, "getvalue", None)):
        try:
            widget_bytes = widget_value.getvalue()
        except Exception:
            widget_bytes = b""
        if isinstance(widget_bytes, bytes):
            # An invalid upload deliberately wins over the local fallback so a
            # failed validation can never leave a partial/stale local map shown.
            return widget_bytes, "uploaded canonical workbook"
    uploaded = st.session_state.get("atlanta-canonical-workbook-bytes")
    if isinstance(uploaded, bytes) and uploaded:
        return uploaded, "uploaded canonical workbook"
    if ATLANTA_6AREA_WORKBOOK.is_file():
        return ATLANTA_6AREA_WORKBOOK, "local canonical workbook fallback"
    return None, "no canonical workbook"


def _load_atlanta_6area_workbook(source: bytes | Path | str | None = None) -> pd.DataFrame:
    """Normalize only through the canonical parser; invalid inputs produce no map rows."""
    actual_source = source if source is not None else _candidate_workbook_source()[0]
    if actual_source is None:
        return pd.DataFrame(columns=["POSTAL_CODE", "Territory", "ambiguity_status"])
    try:
        parsed = parse_atlanta_6area_workbook(actual_source)
    except Atlanta6AreaPlanError:
        return pd.DataFrame(columns=["POSTAL_CODE", "Territory", "ambiguity_status"])
    ambiguous = set(parsed.ambiguous_postals)
    return pd.DataFrame(
        [
            {
                "POSTAL_CODE": membership.postal_code,
                "Territory": membership.territory,
                "ambiguity_status": "unresolved" if membership.postal_code in ambiguous else "resolved",
            }
            for membership in parsed.memberships
        ]
    )


def _load_atlanta_6area_zcta_geometry() -> tuple[gpd.GeoDataFrame, tuple[str, ...]]:
    workbook = _load_atlanta_6area_workbook()
    requested = sorted(workbook["POSTAL_CODE"].unique().tolist()) if not workbook.empty else []
    geometry = _area_map_load_zcta_geometry(
        requested,
        config_section=AREA_MAP_CONFIG_SECTION,
    )
    if not geometry.empty:
        geometry = geometry.copy()
        geometry["POSTAL_CODE"] = geometry["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    loaded = set(geometry["POSTAL_CODE"].tolist()) if "POSTAL_CODE" in geometry.columns else set()
    return geometry, tuple(sorted(set(requested) - loaded))


def _get_atlanta_6area_frames(explorer_data) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    workbook = _load_atlanta_6area_workbook()
    base_zip, _missing_geometry = _load_atlanta_6area_zcta_geometry()
    base_zip = base_zip.copy()
    base_service = explorer_data.current_service_df.copy()
    if workbook.empty or base_zip.empty:
        return (
            gpd.GeoDataFrame(columns=["POSTAL_CODE", "AREA_NAME", "geometry"], geometry="geometry", crs=getattr(base_zip, "crs", "EPSG:4326")),
            gpd.GeoDataFrame(columns=["AREA_NAME", "postal_count", "service_count", "geometry"], geometry="geometry", crs=getattr(base_zip, "crs", "EPSG:4326")),
            base_service.iloc[0:0].copy(),
        )
    base_zip["POSTAL_CODE"] = base_zip["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    # Workbook membership is authoritative for the 6-area preview. Keep all
    # 301 rows (297 unique ZIPs) even when Census has no ZCTA polygon; missing
    # geometry is reported explicitly instead of dropping or interpolating it.
    zip_layer = workbook.merge(base_zip, on="POSTAL_CODE", how="left")
    zip_layer = gpd.GeoDataFrame(zip_layer, geometry="geometry", crs=base_zip.crs)
    zip_layer["AREA_NAME"] = zip_layer["Territory"]
    zone_order = {
        "Zone 1": 1, "Zone 2": 2, "Zone 3": 3,
        "Zone 4": 4, "Zone 5": 5, "ATL Outer Area": 6,
    }
    zip_layer["region_seq"] = zip_layer["AREA_NAME"].map(zone_order)
    zip_layer["region_id"] = zip_layer["region_seq"].map(lambda value: f"atlanta-6area-{int(value)}")

    base_service["POSTAL_CODE"] = base_service["POSTAL_CODE"].astype(str).str.strip().str.zfill(5)
    territory_choices = (
        workbook.groupby("POSTAL_CODE")["Territory"]
        .agg(lambda values: tuple(sorted(set(map(str, values)))))
        .to_dict()
    )
    def display_owner(postal_code: object) -> str:
        choices = territory_choices.get(str(postal_code), ())
        if len(choices) == 1:
            return choices[0]
        if len(choices) > 1:
            return "UNRESOLVED: " + " | ".join(choices)
        return "POSTAL_NOT_IN_ACTIVE_PLAN"
    service_df = base_service.copy()
    service_df["AREA_NAME"] = service_df["POSTAL_CODE"].map(display_owner)
    service_df["ambiguity_status"] = "resolved"
    service_df.loc[
        service_df["AREA_NAME"].str.startswith("UNRESOLVED:"), "ambiguity_status"
    ] = "unresolved"
    service_df.loc[
        service_df["AREA_NAME"].eq("POSTAL_NOT_IN_ACTIVE_PLAN"), "ambiguity_status"
    ] = "unmapped"
    service_df["scenario"] = "atlanta_6area_workbook"

    resolved_service = service_df[service_df["ambiguity_status"].eq("resolved")]
    service_counts = (
        resolved_service.groupby("AREA_NAME")["GSFS_RECEIPT_NO"].nunique().to_dict()
        if "GSFS_RECEIPT_NO" in resolved_service.columns
        else {}
    )
    area_rows: list[dict[str, object]] = []
    for area_name, group in zip_layer.groupby("AREA_NAME", sort=False):
        geometry = group.geometry.dropna().union_all()
        area_rows.append(
            {
                "AREA_NAME": area_name,
                "postal_count": int(group["POSTAL_CODE"].nunique()),
                "postal_codes": " | ".join(sorted(group["POSTAL_CODE"].unique())),
                "service_count": int(service_counts.get(area_name, 0)),
                "ambiguity_zip_count": int(
                    group[group["ambiguity_status"].eq("unresolved")]["POSTAL_CODE"].nunique()
                ),
                "geometry": geometry,
            }
        )
    area_layer = gpd.GeoDataFrame(area_rows, geometry="geometry", crs=zip_layer.crs)
    if not area_layer.empty:
        projected = area_layer.to_crs(epsg=3857)
        area_layer["area_km2"] = projected.geometry.area / 1_000_000.0
    return zip_layer, area_layer, service_df


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
    route_alias = ROUTE_CITY_ALIASES.get(str(city_name).strip())
    if route_alias:
        return route_alias
    if city_name != ALL_CITIES:
        return city_name
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


@st.cache_data(show_spinner=False)
def _load_profile_home_geocode_df() -> pd.DataFrame:
    if not PROFILE_FILE.exists():
        return pd.DataFrame()
    try:
        address_df = pd.read_excel(PROFILE_FILE, sheet_name="4. Address")
    except Exception:
        return pd.DataFrame()
    required = {"SVC_ENGINEER_CODE", "Home Street Address", "latitude", "longitude"}
    if not required.issubset(set(address_df.columns)):
        return pd.DataFrame()
    address_df = address_df.copy()
    address_df["SVC_ENGINEER_CODE"] = address_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    address_df["latitude"] = pd.to_numeric(address_df["latitude"], errors="coerce")
    address_df["longitude"] = pd.to_numeric(address_df["longitude"], errors="coerce")
    return address_df.dropna(subset=["SVC_ENGINEER_CODE", "latitude", "longitude"]).copy()


def _build_home_lookup_from_geocode_df(home_df: pd.DataFrame) -> dict[str, dict]:
    if home_df.empty:
        return {}
    code_col = "SVC_ENGINEER_CODE" if "SVC_ENGINEER_CODE" in home_df.columns else "GSFS_RECEIPT_NO"
    address_col = "Home Street Address" if "Home Street Address" in home_df.columns else "ADDRESS_LINE1_INFO"
    home_df = home_df.copy()
    home_df["latitude"] = pd.to_numeric(home_df.get("latitude"), errors="coerce")
    home_df["longitude"] = pd.to_numeric(home_df.get("longitude"), errors="coerce")
    home_df = home_df.dropna(subset=["latitude", "longitude"]).copy()
    return {
        str(row[code_col]).strip(): {
            "coord": (float(row["longitude"]), float(row["latitude"])),
            "address": str(row.get(address_col, "")).strip(),
        }
        for _, row in home_df.iterrows()
        if str(row.get(code_col, "")).strip()
    }


@st.cache_data(show_spinner=False)
def get_home_location_lookup(city_name: str, engineer_codes: tuple[str, ...] = ()) -> dict[str, dict]:
    city_name = _base_city_name(city_name)
    if str(city_name).strip().upper() in COUNTRY_ROUTE_KEYS:
        return _build_home_lookup_from_geocode_df(_load_saved_home_geocode_df(city_name))

    city_engineer_codes = {str(code).strip() for code in engineer_codes if str(code).strip()}
    if not city_engineer_codes:
        city_key = str(city_name).strip().casefold()
        zip_df, slot_df, _ = _area_map_load_profile_data(PROFILE_FILE)
        zip_city = zip_df[zip_df["STRATEGIC_CITY_NAME"].astype(str).str.strip().str.casefold().eq(city_key)].copy()
        slot_city = slot_df[slot_df["STRATEGIC_CITY_NAME"].astype(str).str.strip().str.casefold().eq(city_key)].copy()
        if "SVC_ENGINEER_CODE" in zip_city.columns:
            city_engineer_codes.update(zip_city["SVC_ENGINEER_CODE"].dropna().astype(str).str.strip())
        if "SVC_ENGINEER_CODE" in slot_city.columns:
            city_engineer_codes.update(slot_city["SVC_ENGINEER_CODE"].dropna().astype(str).str.strip())

        service_df = _area_map_load_service_points(get_latest_geocoded_service_file())
        if not service_df.empty and "STRATEGIC_CITY_NAME" in service_df.columns:
            service_city = service_df[
                service_df["STRATEGIC_CITY_NAME"].astype(str).str.strip().str.casefold().eq(city_key)
            ].copy()
            for code_col in ["SVC_ENGINEER_CODE", "assigned_sm_code"]:
                if code_col in service_city.columns:
                    city_engineer_codes.update(service_city[code_col].dropna().astype(str).str.strip())
    city_engineer_codes = {code for code in city_engineer_codes if code}

    profile_home_df = _load_profile_home_geocode_df()
    if not profile_home_df.empty:
        profile_home_df = profile_home_df[
            profile_home_df["SVC_ENGINEER_CODE"].astype(str).str.strip().isin(city_engineer_codes)
        ].copy()
    lookup = _build_home_lookup_from_geocode_df(profile_home_df)

    saved_home_df = _load_saved_home_geocode_df(city_name)
    saved_lookup = _build_home_lookup_from_geocode_df(saved_home_df)
    for code, home_info in saved_lookup.items():
        lookup.setdefault(code, home_info)
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
    route_engineer_codes: set[str] = set()
    for code_col in ["SVC_ENGINEER_CODE", "assigned_sm_code"]:
        if code_col in route_df.columns:
            route_engineer_codes.update(route_df[code_col].dropna().astype(str).str.strip())
    home_lookup = get_home_location_lookup(city_name, tuple(sorted(code for code in route_engineer_codes if code)))
    use_route_backend = is_route_backend_ready(city_name)
    for (service_date, sm_code), group_df in route_df.groupby(["service_date_key", "assigned_sm_code"], sort=True):
        engineer_name = ""
        if "SVC_ENGINEER_NAME" in group_df.columns:
            names = [
                str(value).strip()
                for value in group_df["SVC_ENGINEER_NAME"].dropna().unique().tolist()
                if str(value).strip()
            ]
            engineer_name = names[0] if names else ""
        service_coords = tuple(
            group_df[["longitude", "latitude"]]
            .dropna()
            .drop_duplicates()
            .apply(lambda r: (float(r["longitude"]), float(r["latitude"])), axis=1)
            .tolist()
        )
        home_info = home_lookup.get(str(sm_code).strip())
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
                "engineer_name": engineer_name,
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
    service_name_df = city_data.service_df
    if not service_name_df.empty and {"SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME"}.issubset(service_name_df.columns):
        name_lookup.update(
            {
                str(row["SVC_ENGINEER_CODE"]).strip(): str(row.get("SVC_ENGINEER_NAME", "")).strip()
                for _, row in service_name_df.dropna(subset=["SVC_ENGINEER_CODE"]).drop_duplicates(subset=["SVC_ENGINEER_CODE"]).iterrows()
                if str(row.get("SVC_ENGINEER_NAME", "")).strip()
            }
        )
    if not city_data.slot_df.empty and {"SVC_ENGINEER_CODE", "Name"}.issubset(city_data.slot_df.columns):
        for _, row in city_data.slot_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).iterrows():
            code = str(row["SVC_ENGINEER_CODE"]).strip()
            name_lookup.setdefault(code, str(row.get("Name", "")).strip())
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


def _home_label_options(sm_code: str) -> tuple[str, tuple[int, int]]:
    label_slots = [
        ("top", (0, -12)),
        ("right", (16, 0)),
        ("bottom", (0, 12)),
        ("left", (-16, 0)),
        ("top", (36, -18)),
        ("right", (24, 18)),
        ("bottom", (-36, 18)),
        ("left", (-24, -18)),
    ]
    slot = sum(ord(ch) for ch in str(sm_code)) % len(label_slots)
    return label_slots[slot]


def _add_home_marker(
    map_obj: folium.Map,
    sm_code: str,
    home_coord: tuple[float, float],
    *,
    marker_color: str,
    engineer_name: str = "",
    home_address: str = "",
    tooltip_suffix: str = "Home",
    info_layer: folium.FeatureGroup | None = None,
) -> None:
    home_lon, home_lat = home_coord
    icon_html = (
        f"<div style=\"font-size:10px;font-weight:700;color:{marker_color};"
        f"background:#ffffff;border:2px solid {marker_color};border-radius:12px;"
        "padding:2px 6px;text-align:center;white-space:nowrap;"
        "box-shadow:0 1px 5px rgba(0,0,0,0.28);\">Home</div>"
    )
    name_value = str(engineer_name).strip() or str(sm_code).strip()
    popup_html = (
        f"<b>Home</b><br>"
        f"<b>Name</b>: {name_value}<br>"
        f"<b>Assigned SM</b>: {sm_code}<br>"
        f"<b>Address</b>: {html.escape(str(home_address).strip())}"
    )
    label_html = (
        f"<div><b>Home</b></div>"
        f"<div><b>Name</b>: {html.escape(name_value)}</div>"
        f"<div><b>Assigned SM</b>: {html.escape(str(sm_code))}</div>"
    )
    label_direction, label_offset = _home_label_options(sm_code)
    sm_code_key = str(sm_code).strip().upper()
    if sm_code_key in ORANGE_SM_LABEL_CODES:
        label_background = "#ffedd5"
        label_border = "#f97316"
    elif sm_code_key in PINK_SM_LABEL_CODES:
        label_background = "#fce7f3"
        label_border = "#db2777"
    else:
        label_background = "#ffffff"
        label_border = "#475569"
    folium.Marker(
        location=[float(home_lat), float(home_lon)],
        icon=folium.DivIcon(html=icon_html, icon_size=(34, 18), icon_anchor=(17, 9)),
        popup=folium.Popup(popup_html, max_width=320),
        tooltip=f"{sm_code} | {tooltip_suffix}",
    ).add_to(map_obj)
    if info_layer is None:
        info_layer = map_obj
    folium.Marker(
        location=[float(home_lat), float(home_lon)],
        icon=folium.DivIcon(html="<div></div>", icon_size=(1, 1), icon_anchor=(0, 0)),
        tooltip=folium.Tooltip(
            label_html,
            permanent=True,
            sticky=False,
            direction=label_direction,
            offset=label_offset,
            opacity=0.96,
            style=(
                f"background:{label_background};border:1px solid {label_border};border-radius:4px;"
                "box-shadow:0 2px 8px rgba(15,23,42,0.24);color:#111827;"
                "font-size:11px;line-height:1.3;padding:5px 7px;max-width:260px;"
                "white-space:normal;"
            ),
        ),
    ).add_to(info_layer)


def build_map(
    city_name: str,
    subsidiary_name: str,
    strategic_city_name: str,
    region_count: int | None,
    area_names: list[str],
    selected_area_types: list[str] | None,
    selected_date: str,
    selected_sm: str,
    selected_center_buckets: list[str],
    area_plan_candidate: Mapping[str, object] | None = None,
):
    explorer_data = get_route_explorer_data(
        city_name,
        region_count,
        AREA_MAP_CACHE_VERSION,
        _current_coverage_source_fingerprint(get_latest_geocoded_service_file()),
    )
    plan_frames = _load_area_plan_frames(area_plan_candidate, explorer_data) if area_plan_candidate else None
    if plan_frames is not None:
        zip_layer, area_layer, service_df = plan_frames
    else:
        zip_layer, area_layer, service_df = _empty_area_plan_frames(explorer_data)
    area_col = _get_area_column_name(region_count, zip_layer)

    service_df = _apply_service_scope_filters(service_df, subsidiary_name, strategic_city_name)
    area_layer = _filter_area_type(area_layer, selected_area_types)
    zip_layer = _filter_area_type(zip_layer, selected_area_types)
    service_df = _filter_area_type(service_df, selected_area_types)
    if "service_date" not in service_df.columns:
        service_df["service_date"] = pd.NaT
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
    area_color_map = (
        _generate_color_map(area_layer["AREA_NAME"].astype(str).tolist())
        if "AREA_NAME" in area_layer.columns
        else {}
    )
    color_by_area_type = True

    if not area_layer.empty and "AREA_NAME" in area_layer.columns:
        area_fields = ["AREA_NAME", "postal_count", "service_count"]
        area_aliases = ["Area", "Postal Count", "Service Count"]
        if "area_type" in area_layer.columns:
            area_fields.insert(1, "area_type")
            area_aliases.insert(1, "Area Type")
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
                "fillColor": (
                    _area_type_color(
                        feat["properties"].get("area_type", ""),
                        area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                    )
                    if color_by_area_type
                    else area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e")
                ),
                "color": (
                    _area_type_color(
                        feat["properties"].get("area_type", ""),
                        area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                    )
                    if color_by_area_type
                    else area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e")
                ),
                "weight": 1.0,
                "fillOpacity": 0.10,
            },
            highlight_function=lambda feat: {
                "fillColor": (
                    _area_type_color(
                        feat["properties"].get("area_type", ""),
                        area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                    )
                    if color_by_area_type
                    else area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e")
                ),
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

    dms_home_layer = folium.FeatureGroup(name="DMS Home", show=True)
    dms_home_info_layer = folium.FeatureGroup(name="DMS Home Info", show=True)
    has_dms_home_marker = False
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
        home_parent = dms_home_layer if route_bucket == "DMS" else map_obj
        home_info_parent = dms_home_info_layer if route_bucket == "DMS" else None
        if route_bucket == "DMS":
            has_dms_home_marker = True
        _add_home_marker(
            home_parent,
            sm_code,
            home_coord,
            marker_color=marker_color,
            engineer_name=str(group.get("engineer_name", "")),
            home_address=str(group.get("home_address", "")),
            info_layer=home_info_parent,
        )

    if selected_date != "ALL" and "DMS" in selected_center_set:
        idle_home_rows = _build_dms_home_marker_rows(city_name, area_names, selected_sm, route_groups)
        for home_row in idle_home_rows:
            has_dms_home_marker = True
            _add_home_marker(
                dms_home_layer,
                str(home_row["assigned_sm_code"]),
                home_row["home_coord"],
                marker_color="#111827",
                engineer_name=str(home_row.get("engineer_name", "")),
                home_address=str(home_row.get("home_address", "")),
                tooltip_suffix="Home | no service",
                info_layer=dms_home_info_layer,
            )

    if has_dms_home_marker:
        dms_home_layer.add_to(map_obj)
        dms_home_info_layer.add_to(map_obj)

    folium.LayerControl(collapsed=False).add_to(map_obj)
    return map_obj, service_df, area_layer, route_groups


def _build_candidate_display_df(city_name: str) -> pd.DataFrame:
    if city_name == ATLANTA_6AREA_CITY_NAME:
        workbook = _load_atlanta_6area_workbook()
        unresolved_count = int(
            workbook[workbook["ambiguity_status"].eq("unresolved")]["POSTAL_CODE"].nunique()
        ) if not workbook.empty else 0
        return pd.DataFrame(
            [
                {"Area View": CURRENT_REGION_LABEL, "Source": "Atlanta, GA reviewed baseline", "Unresolved ZIPs": 0},
                {"Area View": ATLANTA_6AREA_LABEL, "Source": "New ATL Buckets.xlsx", "Unresolved ZIPs": unresolved_count},
            ]
        )
    stats_df = get_region_stats(city_name)
    if stats_df.empty:
        return stats_df
    current_row = {
        "Area View": CURRENT_REGION_LABEL,
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
                "Area View": f"{AREA_TYPE_REGION_PREFIX} ({int(row['candidate_region_count'])} regions)",
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
    numeric_cols = [col for col in display_df.columns if col not in {"Area View", "Best"}]
    display_df[numeric_cols] = display_df[numeric_cols].apply(pd.to_numeric, errors="coerce").round(2)
    return display_df


def _region_plan_api_origin(config_path: Path = COMMON_CONFIG_FILE) -> str:
    area_map_origin = str(
        (_load_config().get(AREA_MAP_CONFIG_SECTION, {}) or {}).get("common_vrp_api_url")
        or ""
    ).strip().rstrip("/")
    if area_map_origin.startswith(("http://", "https://")) and "@" not in area_map_origin:
        return area_map_origin
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return ""
    origin = str(payload.get("routing_api_url") or "").strip().rstrip("/")
    if not origin.startswith(("http://", "https://")) or "@" in origin:
        return ""
    return origin


@st.cache_data(show_spinner=False, ttl=30)
def _load_atlanta_6area_plans(api_origin: str) -> dict[str, object]:
    if not api_origin:
        return {"status": "unavailable", "plans": []}
    try:
        response = requests.get(
            f"{api_origin}/api/v1/common/region-plans",
            params={"city_key": ATLANTA_6AREA_CITY_NAME},
            timeout=5,
        )
        if not response.ok:
            return {"status": "unavailable", "plans": []}
        payload = response.json()
    except Exception:
        return {"status": "unavailable", "plans": []}
    if isinstance(payload, list):
        plans = payload
    elif isinstance(payload, dict):
        plans = payload.get("plans") or payload.get("items") or []
        if not isinstance(plans, list) and payload.get("plan_id"):
            plans = [payload]
    else:
        plans = []
    return {
        "status": "available" if isinstance(plans, list) else "unavailable",
        "plans": [dict(item) for item in plans if isinstance(item, dict)],
    }


@st.cache_data(show_spinner=False, ttl=30)
def _load_atlanta_6area_active_plan(api_origin: str) -> dict[str, object]:
    """Read the runtime snapshot; this UI never writes or activates a plan."""
    if not api_origin:
        return {"status": "unavailable", "plan": None}
    try:
        response = requests.get(
            f"{api_origin}/api/v1/common/region-plans/active",
            params={"city_key": ATLANTA_6AREA_CITY_NAME},
            timeout=5,
        )
        if not response.ok:
            return {"status": "unavailable", "plan": None}
        payload = response.json()
    except Exception:
        return {"status": "unavailable", "plan": None}
    return {
        "status": "available" if isinstance(payload, dict) else "unavailable",
        "plan": dict(payload) if isinstance(payload, dict) else None,
    }


def _region_plan_ambiguity_rows(plan: dict[str, object]) -> list[dict[str, object]]:
    values = plan.get("ambiguities") or plan.get("ambiguity_resolutions") or []
    if not isinstance(values, list):
        return []
    rows: list[dict[str, object]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        owner = item.get("owner_region") or item.get("selected_owner") or ""
        overflow = item.get("overflow_region") or item.get("selected_overflow") or ""
        status = str(item.get("status") or "").strip().lower()
        resolved = status in {"resolved", "approved"} or bool(owner and overflow)
        rows.append(
            {
                "ZIP": str(item.get("postal_code") or item.get("zip") or ""),
                "Owner decision": str(owner),
                "Overflow decision": str(overflow),
                "Status": "resolved" if resolved else "unresolved",
            }
        )
    return rows


def _safe_plan_artifact(plan: dict[str, object], key: str) -> tuple[bytes, str] | None:
    artifacts = plan.get("artifacts")
    if not isinstance(artifacts, dict):
        return None
    item = artifacts.get(key)
    if not isinstance(item, dict):
        return None
    content = item.get("content")
    if isinstance(content, str):
        payload = content.encode("utf-8")
    elif isinstance(content, bytes):
        payload = content
    else:
        return None
    if key == "technician_policy_csv":
        try:
            technician_df = pd.read_csv(
                io.BytesIO(payload),
                encoding="utf-8-sig",
                dtype=str,
                keep_default_na=False,
            )
        except Exception:
            return None
        if "SVC_ENGINEER_NAME" in technician_df.columns:
            technician_df["SVC_ENGINEER_NAME"] = ""
        payload = _to_csv_bytes(technician_df)
    file_name = Path(str(item.get("file_name") or f"{key}.csv")).name
    return payload, file_name


def _candidate_decision_digest(decisions: dict[str, dict[str, object]]) -> str:
    """Bind one local candidate download to its exact canonical UI decisions."""
    canonical = {
        postal_code: {
            "primary_region": str(decision.get("primary_region") or ""),
            "allow_overflow": decision.get("allow_overflow"),
            "rationale": str(decision.get("rationale") or ""),
        }
        for postal_code, decision in sorted(decisions.items())
    }
    payload = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _snapshot_plan_artifacts(plan: dict[str, object], api_origin: str) -> dict[str, tuple[bytes, str]]:
    """Create deterministic exports only when the API snapshot is internally complete."""
    checksum = str(plan.get("checksum") or "").strip().lower()
    plan_id = str(plan.get("plan_id") or "").strip()
    city_name = str(plan.get("strategic_city_name") or ATLANTA_6AREA_CITY_NAME).strip()
    regions = plan.get("regions")
    postals = plan.get("postals")
    technicians = plan.get("technicians")
    if (
        not plan_id
        or not re.fullmatch(r"[0-9a-f]{64}", checksum)
        or not isinstance(regions, list)
        or not isinstance(postals, list)
        or not isinstance(technicians, list)
    ):
        return {}
    region_lookup: dict[int, dict[str, object]] = {}
    for region in regions:
        if not isinstance(region, dict):
            return {}
        try:
            seq = int(region.get("region_seq"))
        except (TypeError, ValueError):
            return {}
        if seq in region_lookup or not str(region.get("region_id") or "").strip():
            return {}
        region_lookup[seq] = region
    if len(region_lookup) != 6:
        return {}
    fixed_rows: list[dict[str, object]] = []
    seen_postals: set[str] = set()
    region_postal_counts = {seq: 0 for seq in region_lookup}
    for postal in postals:
        if not isinstance(postal, dict) or str(postal.get("plan_id") or plan_id) != plan_id:
            return {}
        try:
            seq = int(postal.get("region_seq"))
        except (TypeError, ValueError):
            return {}
        region = region_lookup.get(seq)
        postal_code = str(postal.get("postal_code") or "").strip()
        postal_code = postal_code.zfill(5) if postal_code else ""
        if region is None or not re.fullmatch(r"[0-9]{5}", postal_code) or postal_code in seen_postals:
            return {}
        area_type = str(postal.get("area_type") or "").strip().upper() or "DMS"
        if area_type not in {"DMS", "DMS2"}:
            return {}
        seen_postals.add(postal_code)
        region_postal_counts[seq] += 1
        fixed_rows.append({
            "POSTAL_CODE": postal_code, "STRATEGIC_CITY_NAME": city_name,
            "region_id": str(region["region_id"]), "region_seq": seq,
            "AREA_NAME": str(region.get("source_territory") or ""),
            "new_region_name": str(region.get("region_name") or ""), "area_type": area_type,
        })
    technician_rows: list[dict[str, object]] = []
    seen_employee_codes: set[str] = set()
    for technician in technicians:
        if not isinstance(technician, dict) or str(technician.get("plan_id") or plan_id) != plan_id:
            return {}
        if technician.get("active_flag") is False:
            return {}
        try:
            seq = int(technician.get("assigned_region_seq"))
        except (TypeError, ValueError):
            return {}
        region = region_lookup.get(seq)
        employee_code = str(technician.get("employee_code") or "").strip()
        if region is None or not employee_code or employee_code in seen_employee_codes:
            return {}
        seen_employee_codes.add(employee_code)
        technician_rows.append({
            "plan_id": plan_id, "STRATEGIC_CITY_NAME": city_name,
            "SVC_ENGINEER_CODE": employee_code,
            # This local debug UI has no authenticated PII entitlement.  Keep
            # the canonical column for import compatibility but never export
            # the runtime snapshot's technician name.
            "SVC_ENGINEER_NAME": "",
            "assigned_region_id": str(region["region_id"]),
            "assigned_region_name": str(region.get("region_name") or ""),
            "assigned_region_seq": seq, "policy_mode": str(technician.get("policy_mode") or ""),
        })
    if (
        len(fixed_rows) != 297
        or len(technician_rows) != 14
        or any(count <= 0 for count in region_postal_counts.values())
    ):
        return {}
    fixed_df = pd.DataFrame(fixed_rows).sort_values(["POSTAL_CODE", "region_seq"]).reset_index(drop=True)
    technician_df = pd.DataFrame(technician_rows).sort_values("SVC_ENGINEER_CODE").reset_index(drop=True)
    fixed_bytes, technician_bytes = _to_csv_bytes(fixed_df), _to_csv_bytes(technician_df)
    manifest = {
        "schema": "region-plan-runtime-snapshot-export/v1", "source_endpoint": f"{api_origin}/api/v1/common/region-plans/active",
        "city_key": ATLANTA_6AREA_CITY_NAME, "plan_id": plan_id,
        "lifecycle": str(plan.get("lifecycle_stage") or plan.get("status") or "active"), "checksum": checksum,
        "row_counts": {"fixed_regions": len(fixed_df), "technician_policy": len(technician_df)},
        "generated_file_sha256": {"fixed_region_csv": hashlib.sha256(fixed_bytes).hexdigest(), "technician_policy_csv": hashlib.sha256(technician_bytes).hexdigest()},
        "privacy": "Technician names are excluded from this manifest.",
    }
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", plan_id)[:80] or "active_plan"
    return {
        "fixed_region_csv": (fixed_bytes, f"atlanta_6area_{stem}_fixed_regions.csv"),
        "technician_policy_csv": (technician_bytes, f"atlanta_6area_{stem}_technician_policy.csv"),
        "manifest": (json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"), f"atlanta_6area_{stem}_runtime_snapshot_manifest.json"),
    }


def _render_legacy_region_plan_builder(selected_subsidiary: str, selected_city: str) -> None:
    """Create the common Region Plan v2 source artifact from Area Map."""
    with st.expander("Region Plan 데이터 생성 (Admin Tools 연계)", expanded=False):
        st.caption(
            "Region/Technician 원본을 검증한 뒤 공통 Area + Technician workbook을 생성합니다. "
            "DB에는 직접 저장하지 않으며, 생성된 파일은 Admin Tools의 Region Plans v2에서 "
            "review/activation해야 합니다."
        )
        sample_region = pd.DataFrame(columns=[
            "POSTAL_CODE", "STRATEGIC_CITY_NAME", "region_id", "region_seq",
            "AREA_NAME", "new_region_name", "area_type",
        ])
        sample_technician = pd.DataFrame(columns=["Tech ID", "Tech Name", "Assignment"])
        sample_cols = st.columns(2)
        with sample_cols[0]:
            st.download_button(
                "Region CSV 샘플",
                data=_to_csv_bytes(sample_region),
                file_name="region_plan_region_template.csv",
                mime="text/csv",
                key="region-plan-region-template",
            )
        with sample_cols[1]:
            st.download_button(
                "Technician CSV 샘플",
                data=_to_csv_bytes(sample_technician),
                file_name="region_plan_technician_template.csv",
                mime="text/csv",
                key="region-plan-technician-template",
            )
        region_upload = st.file_uploader(
            "Region 데이터 (CSV/XLSX)", type=["csv", "xlsx"], key="area-map-region-plan-region-upload"
        )
        technician_upload = st.file_uploader(
            "Technician 데이터 (CSV/XLSX)", type=["csv", "xlsx"], key="area-map-region-plan-technician-upload"
        )
        metadata_cols = st.columns(3)
        default_source_city = selected_city if selected_city not in {ALL_OPTION, BLANK_CITY_OPTION, ALL_CITIES} else ""
        default_target_city = (
            "Atlanta_6area" if default_source_city == "Atlanta, GA"
            else re.sub(r"[^A-Za-z0-9]+", "_", default_source_city).strip("_")
        )
        with metadata_cols[0]:
            subsidiary_id = st.text_input(
                "법인 ID", value=selected_subsidiary if selected_subsidiary not in {ALL_OPTION, BLANK_CITY_OPTION} else "",
                key="area-map-region-plan-subsidiary",
            ).strip()
        with metadata_cols[1]:
            source_city_id = st.text_input(
                "source_strategic_city_name", value=default_source_city,
                key="area-map-region-plan-source-city",
            ).strip()
        with metadata_cols[2]:
            target_city_id = st.text_input(
                "정책 도시 / Plan ID 대상", value=default_target_city,
                key="area-map-region-plan-target-city",
            ).strip()
        penalty = st.number_input(
            "중복 우편번호 overflow penalty",
            min_value=1, value=4500, step=1,
            help="중복 postal membership이 있을 때 alternate Region에 적용할 정책 비용입니다.",
            key="area-map-region-plan-penalty",
        )
        build_key = "area-map-region-plan-export"
        if st.button(
            "검증 후 Region Plan 생성/저장",
            type="primary",
            disabled=region_upload is None or technician_upload is None,
            key="area-map-region-plan-build",
        ):
            try:
                export = build_area_map_region_plan(
                    region_upload.name, region_upload.getvalue(),
                    technician_upload.name, technician_upload.getvalue(),
                    subsidiary_id=subsidiary_id,
                    source_city_id=source_city_id,
                    target_city_id=target_city_id,
                    plan_display_name=target_city_id,
                    overflow_penalty_minutes=int(penalty),
                )
                output_dir = save_area_map_region_plan(export, REGION_PLAN_ROOT)
                st.session_state[build_key] = {
                    "manifest": dict(export.manifest),
                    "workbook_bytes": export.workbook_bytes,
                    "output_dir": str(output_dir),
                }
                st.success(f"Region Plan candidate 파일을 저장했습니다: `{output_dir}`")
            except (AreaMapRegionPlanError, ValueError, OSError) as exc:
                st.session_state.pop(build_key, None)
                st.error(f"Region Plan 생성 실패: {exc}")
        generated = st.session_state.get(build_key)
        if isinstance(generated, dict) and generated.get("workbook_bytes"):
            manifest = generated.get("manifest") or {}
            st.json({
                "plan_id": manifest.get("plan_id"),
                "source_city_id": (manifest.get("city_metadata") or {}).get("source_city_id"),
                "target_city_id": (manifest.get("city_metadata") or {}).get("target_city_id"),
                "row_accounting": manifest.get("row_accounting"),
                "canonical_sha256": manifest.get("canonical_sha256"),
                "saved_directory": generated.get("output_dir"),
            })
            stem = str(manifest.get("plan_id") or "region_plan")
            st.download_button(
                "생성된 Area + Technician workbook 다운로드",
                data=generated["workbook_bytes"],
                file_name=f"{stem}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="area-map-region-plan-workbook-download",
            )
            st.caption("Admin Tools → Region Plans v2에서 이 workbook을 선택하거나, 생성된 `data/region_plans` 항목을 선택해 업로드하세요.")


def _region_plan_candidate_rows(
    subsidiary_name: str,
    selected_city: str,
) -> list[dict[str, object]]:
    """Load local plan manifests without treating them as active runtime data."""
    if selected_city in {ALL_OPTION, BLANK_CITY_OPTION, ALL_CITIES}:
        return []
    root = REGION_PLAN_ROOT.resolve()
    if not root.is_dir():
        return []
    source_city = _base_city_name(selected_city)
    selected_city_key = re.sub(r"[^A-Za-z0-9]+", "_", str(selected_city)).strip("_").casefold()
    selected_city_keys = {str(selected_city).strip().casefold(), selected_city_key}
    rows: list[dict[str, object]] = []
    for manifest_path in sorted(root.glob("*/*/*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
        metadata = manifest.get("city_metadata") or {}
        manifest_subsidiary = str(
            manifest.get("subsidiary_name") or metadata.get("subsidiary_id") or ""
        ).strip()
        manifest_source_city = str(
            manifest.get("source_strategic_city_name")
            or metadata.get("source_city_id")
            or ""
        ).strip()
        target_city = str(
            manifest.get("target_city_id")
            or manifest.get("strategic_city_name")
            or metadata.get("target_city_id")
            or ""
        ).strip()
        if subsidiary_name not in {ALL_OPTION, BLANK_CITY_OPTION} and manifest_subsidiary != subsidiary_name:
            continue
        # Strategic city is the source roster city. Area Plan then lists all
        # policy-city variants belonging to that source city (for example
        # Atlanta, GA -> Atlanta_3area / Atlanta_6area).
        source_candidates = {
            manifest_source_city,
            _base_city_name(manifest_source_city),
            manifest_source_city.split(" - ", 1)[0].strip(),
        }
        source_keys = {
            re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").casefold()
            for value in source_candidates
            if value
        }
        if manifest_source_city and (
            source_city.casefold() not in {value.casefold() for value in source_candidates}
            and selected_city.casefold() not in {value.casefold() for value in source_candidates}
            and re.sub(r"[^A-Za-z0-9]+", "_", source_city).strip("_").casefold() not in source_keys
        ):
            continue
        if not manifest_source_city:
            # Very old manifests may omit source metadata. Keep them
            # discoverable by the policy-city directory/target as a fallback.
            target_city_key = re.sub(r"[^A-Za-z0-9]+", "_", target_city).strip("_").casefold()
            if target_city.casefold() not in selected_city_keys and target_city_key not in selected_city_keys:
                continue
        plan_id = str(manifest.get("plan_id") or manifest_path.parent.name).strip()
        display_name = str(manifest.get("plan_display_name") or plan_id).strip()
        status = str(manifest.get("status") or manifest.get("lifecycle_stage") or "candidate").strip()
        rows.append({
            "label": f"{display_name} ({target_city or source_city}) [{status}]",
            "path": manifest_path.parent.resolve(),
            "manifest": manifest,
            "plan_id": plan_id,
            "target_city_id": target_city,
            "source_city_id": manifest_source_city or source_city,
        })
    return rows


def _plan_source_bytes(candidate: Mapping[str, object], kind: str) -> tuple[str, bytes] | None:
    plan_dir = Path(str(candidate.get("path") or ""))
    manifest = candidate.get("manifest") if isinstance(candidate.get("manifest"), Mapping) else {}
    source = manifest.get("source") if isinstance(manifest, Mapping) else {}
    source = source if isinstance(source, Mapping) else {}
    if kind == "region":
        configured = source.get("region_file")
        names = [source.get("region_file_name")]
    else:
        configured = source.get("technician_file")
        names = [source.get("technician_file_name")]
    if configured:
        path = Path(str(configured))
        if path.is_file():
            return path.name, path.read_bytes()
    for name in names:
        if name:
            path = plan_dir / "source" / str(name)
            if path.is_file():
                return path.name, path.read_bytes()
    source_dir = plan_dir / "source"
    if source_dir.is_dir():
        candidates = sorted(path for path in source_dir.iterdir() if path.is_file())
        if kind == "region":
            candidates = [path for path in candidates if "technician" not in path.name.casefold()]
        else:
            candidates = [path for path in candidates if "technician" in path.name.casefold()]
        if candidates:
            path = candidates[0]
            return path.name, path.read_bytes()
    return None


def _update_candidate_display_name(candidate: Mapping[str, object], display_name: str) -> Path:
    """Update only a local candidate's display name without rebuilding data."""
    plan_dir = Path(str(candidate.get("path") or "")).resolve()
    manifest_path = plan_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError("Area Plan manifest not found")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["plan_display_name"] = str(display_name).strip()
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    manifest_path.write_bytes(manifest_bytes)
    checksums_path = plan_dir / "checksums.json"
    if checksums_path.is_file():
        checksums = json.loads(checksums_path.read_text(encoding="utf-8"))
        checksums["manifest.json"] = hashlib.sha256(manifest_bytes).hexdigest()
        checksums_path.write_text(
            json.dumps(checksums, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return manifest_path


def _delete_local_area_plan_candidate(candidate: Mapping[str, object]) -> None:
    """Delete one managed local Area Plan candidate, never a DB Plan."""
    plan_dir = Path(str(candidate.get("path") or "")).resolve()
    root = REGION_PLAN_ROOT.resolve()
    if plan_dir == root or root not in plan_dir.parents or not (plan_dir / "manifest.json").is_file():
        raise ValueError("Area Plan candidate path is invalid")
    shutil.rmtree(plan_dir)


def _adapt_region_source_city(region_name: str, region_bytes: bytes, target_city_id: str) -> tuple[str, bytes]:
    """Keep source-city lineage; the builder accepts source or legacy target IDs.

    Older code rewrote ``STRATEGIC_CITY_NAME`` to the policy city before
    validation.  That hid the distinction between roster city and policy city
    and could make a valid source upload appear to have changed.  The common
    builder now handles both legacy and current files, so the bytes must remain
    untouched here.
    """
    return region_name, region_bytes


def _render_area_plan_sidebar(selected_subsidiary: str, selected_city: str) -> dict[str, object] | None:
    candidates = _region_plan_candidate_rows(selected_subsidiary, selected_city)
    labels = ["(No Area Plan)"] + [str(row["label"]) for row in candidates]
    label_to_candidate = {str(row["label"]): row for row in candidates}
    selected_label = st.selectbox(
        "Area Plan",
        labels,
        key=f"area-plan-select::{selected_subsidiary}::{selected_city}",
    )
    preview_selection_key = f"area-plan-preview-selection::{selected_subsidiary}::{selected_city}"
    previous_selection = st.session_state.get(preview_selection_key)
    if previous_selection is not None and previous_selection != selected_label:
        # A newly selected saved plan must not remain hidden behind the last
        # New Cluster preview for the same city.
        st.session_state.pop("area-plan-cluster-result", None)
    st.session_state[preview_selection_key] = selected_label
    selected = label_to_candidate.get(selected_label)
    action_cols = st.columns(2)
    if action_cols[0].button(
        "Add",
        disabled=selected_city in {ALL_OPTION, BLANK_CITY_OPTION, ALL_CITIES},
        key=f"area-plan-add::{selected_subsidiary}::{selected_city}",
    ):
        st.session_state["area-plan-editor"] = {
            "mode": "add",
            "selected_city": selected_city,
            "selected_subsidiary": selected_subsidiary,
            "candidate_path": "",
        }
        st.rerun()
    if action_cols[1].button(
        "Edit",
        disabled=selected is None,
        key=f"area-plan-edit::{selected_subsidiary}::{selected_city}",
    ):
        st.session_state["area-plan-editor"] = {
            "mode": "edit",
            "selected_city": selected_city,
            "selected_subsidiary": selected_subsidiary,
            "candidate_path": str(selected["path"]) if selected else "",
        }
        st.rerun()
    if st.button(
        "New Cluster",
        disabled=selected_city in {ALL_OPTION, BLANK_CITY_OPTION, ALL_CITIES},
        key=f"area-plan-new-cluster::{selected_subsidiary}::{selected_city}",
        help="Create a candidate-only ZIP region and technician-home allocation plan.",
    ):
        st.session_state["area-plan-cluster-editor"] = {
            "selected_city": selected_city,
            "selected_subsidiary": selected_subsidiary,
        }
        st.rerun()
    if selected:
        manifest = selected.get("manifest") or {}
        reasons = ((manifest.get("quality") or {}).get("needs_review_reasons") or []) if isinstance(manifest, Mapping) else []
        if reasons:
            st.caption("검토 필요: " + ", ".join(map(str, reasons[:2])))
    return selected


def _region_summary_display_frame(summary: pd.DataFrame) -> pd.DataFrame:
    """Keep candidate and saved-plan summaries on one comparable, compact schema."""
    result = summary.copy()
    for column in REGION_SUMMARY_DISPLAY_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
    for column in ("area_km2", "avg_daily_jobs", "avg_daily_jobs_per_assigned_technician"):
        result[column] = pd.to_numeric(result[column], errors="coerce").round(3)
    return result.loc[:, list(REGION_SUMMARY_DISPLAY_COLUMNS)].sort_values("region_seq").reset_index(drop=True)


def _render_candidate_cluster_editor(selected_subsidiary: str, selected_city: str) -> None:
    """Render the candidate-only clustering workflow in the main page area."""
    editor = st.session_state.get("area-plan-cluster-editor")
    if not isinstance(editor, Mapping):
        return
    city_name = str(editor.get("selected_city") or selected_city).strip()
    if city_name in {"", ALL_OPTION, BLANK_CITY_OPTION, ALL_CITIES}:
        st.warning("Select one STRATEGIC_CITY_NAME before creating a cluster candidate.")
        return
    token = re.sub(r"[^A-Za-z0-9]+", "_", city_name).strip("_").lower() or "city"
    st.divider()
    heading_cols = st.columns([5, 1])
    heading_cols[0].subheader("New Cluster")
    if heading_cols[1].button("Close", key=f"candidate-cluster-close::{token}"):
        st.session_state.pop("area-plan-cluster-editor", None)
        st.rerun()
    st.caption(
        "Candidate only: approved Area Plans and runtime masters are not changed. "
        "Selected technicians are assigned once using home-to-region-centroid distance."
    )
    if not REGION_CANDIDATE_SERVICE_FILE.is_file():
        st.error(f"Configured North America service source is unavailable: `{REGION_CANDIDATE_SERVICE_FILE}`")
        return
    if not REGION_CANDIDATE_PROFILE_FILE.is_file():
        st.error(f"Configured technician Address profile is unavailable: `{REGION_CANDIDATE_PROFILE_FILE}`")
        return
    try:
        technician_roster = load_city_technician_roster(REGION_CANDIDATE_PROFILE_FILE, city_name)
    except (OSError, ValueError, KeyError, pd.errors.ParserError) as exc:
        st.error(f"Technician roster could not be loaded: {exc}")
        return
    option_cols = st.columns([1, 1, 2])
    region_count = int(option_cols[0].number_input("Region count", min_value=1, max_value=30, value=4, step=1))
    max_daily_jobs = int(
        option_cols[0].number_input(
            "Max daily jobs / technician",
            min_value=1,
            max_value=100,
            value=8,
            step=1,
            help="Used with each region's p95 observed daily demand to calculate the required technician count. Rare peak days are retained as reviewable exceptions.",
        )
    )
    algorithm = option_cols[1].selectbox(
        "Clustering method",
        ["center_shared_radial", "contiguous_balanced", "weighted_kmeans_staffing"],
        format_func=lambda value: {
            "contiguous_balanced": "Contiguous compact growth (geographic baseline)",
            "weighted_kmeans_staffing": "Weighted K-Means seeds + contiguous growth",
            "center_shared_radial": "Shared-centre radial balance (recommended)",
        }[value],
        key=f"candidate-clustering-method::{token}",
    )
    algorithm_descriptions = {
        "center_shared_radial": (
            "**Recommended for single-core, radial cities such as Atlanta.** Uses the service-demand-weighted city centre "
            "and creates continuous radial sectors from the core to the suburbs. It balances service demand, ZIP count, "
            "and polygon area. Avoid it as the first choice for multi-centre metros because one centre can create long wedges.\n\n"
            "**애틀란타처럼 단일 중심지에서 방사형으로 확장된 도시에 권장합니다.** 서비스 수요 가중 중심점을 기준으로 "
            "중심부에서 외곽까지 이어지는 방사형 권역을 만들며, 서비스량·ZIP 수·면적을 함께 균형화합니다. 여러 중심지가 "
            "분산된 도시는 긴 부채꼴 권역이 생길 수 있으므로 우선 선택하지 않는 편이 좋습니다."
        ),
        "contiguous_balanced": (
            "**Best for continuous service areas where simple connected boundaries and local travel compactness matter most.** "
            "It grows only through touching ZIPs and favours ZIPs closest to the evolving regional centre. Service and area "
            "targets limit overgrowth, but compactness is the priority; a dense central ZIP can still make one region larger.\n\n"
            "**단순하고 연결된 경계와 가까운 이동거리가 중요한 연속 생활권에 적합합니다.** 인접 ZIP만 붙이고 권역 중심에서 "
            "가까운 ZIP을 우선 선택합니다. 서비스량·면적 목표로 과성장을 억제하지만 컴팩트한 경계가 우선입니다. 고밀도 "
            "중심 ZIP이 있으면 한 권역이 다소 커질 수 있습니다."
        ),
        "weighted_kmeans_staffing": (
            "**Best for multi-centre metros such as Los Angeles.** It uses demand-weighted K-Means to identify separate "
            "service centres, then expands each through adjacent ZIPs. Choose it when demand is split across hubs such as "
            "downtown, the Valley, and Long Beach; region area can vary when a hub is sparse or geographically broad.\n\n"
            "**LA처럼 여러 서비스 중심지가 분산된 다핵 도시에 적합합니다.** 수요 가중 K-Means로 분리된 서비스 중심지를 "
            "찾고, 각 중심지에서 인접 ZIP으로 확장합니다. 도심·밸리·롱비치처럼 별도 수요 거점이 있을 때 선택합니다. "
            "한 거점의 밀도가 낮거나 범위가 넓으면 권역 면적 편차가 생길 수 있습니다."
        ),
    }
    option_cols[1].info(algorithm_descriptions[algorithm])
    option_cols[2].caption("Technicians to include")
    roster_editor = technician_roster[["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "City "]].copy()
    roster_editor.insert(0, "Include", True)
    selected_roster = option_cols[2].data_editor(
        roster_editor,
        column_config={
            "Include": st.column_config.CheckboxColumn("Include", help="Unchecked technicians are excluded from this candidate."),
            "SVC_ENGINEER_CODE": st.column_config.TextColumn("Technician code"),
            "SVC_ENGINEER_NAME": st.column_config.TextColumn("Name"),
            "City ": st.column_config.TextColumn("Home city"),
        },
        disabled=["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME", "City "],
        hide_index=True,
        width="stretch",
        key=f"candidate-technician-roster::{token}",
    )
    st.caption(f"Service: `{REGION_CANDIDATE_SERVICE_FILE}`")
    st.caption(f"Technician home profile: `{REGION_CANDIDATE_PROFILE_FILE}` / sheet `4. Address`")
    submitted = st.button(
        "Create cluster candidate",
        type="primary",
        key=f"candidate-cluster-submit::{token}",
    )
    if submitted:
        try:
            with st.spinner("Clustering ZIP demand and allocating technicians by home distance..."):
                result = build_city_region_candidate(
                    service_file=REGION_CANDIDATE_SERVICE_FILE,
                    profile_file=REGION_CANDIDATE_PROFILE_FILE,
                    city_name=city_name,
                    region_count=region_count,
                    algorithm=algorithm,
                    output_root=REGION_CANDIDATE_OUTPUT_ROOT,
                    max_daily_jobs_per_technician=max_daily_jobs,
                    selected_technician_codes=set(
                        selected_roster.loc[selected_roster["Include"].astype(bool), "SVC_ENGINEER_CODE"].astype(str)
                    ),
                )
            st.session_state["area-plan-cluster-result"] = {
                "city": city_name,
                "plan_id": result.plan_id,
                "output_dir": str(result.output_dir),
                "region_postals": str(result.region_postals_path),
                "technician_assignments": str(result.technician_assignments_path),
                "region_summary": str(result.region_summary_path),
                "evidence": str(result.evidence_path),
                "rejects": str(result.rejects_path),
                "manifest": str(result.manifest_path),
            }
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            missing_coordinates = int(
                ((manifest.get("row_accounting") or {}).get("coverage_postals_excluded_missing_coordinates") or 0)
            )
            st.session_state["area-plan-cluster-result"]["coverage_postals_excluded_missing_coordinates"] = missing_coordinates
            st.success(f"Candidate plan created: `{result.output_dir}`")
            if missing_coordinates:
                st.warning(
                    f"{missing_coordinates} Coverage ZIP code(s) without service/ZCTA coordinates were excluded "
                    "from candidate clustering and map preview. See rejects.csv."
                )
        except (OSError, ValueError, KeyError, pd.errors.ParserError) as exc:
            st.error(f"Candidate clustering failed: {exc}")
    created = st.session_state.get("area-plan-cluster-result")
    if not isinstance(created, Mapping) or str(created.get("city")) != city_name:
        return
    st.json(dict(created))
    missing_coordinates = int(created.get("coverage_postals_excluded_missing_coordinates") or 0)
    if missing_coordinates:
        st.warning(
            f"Map preview excludes {missing_coordinates} Coverage ZIP code(s) without usable coordinates."
        )
    summary_path = Path(str(created.get("region_summary") or ""))
    technician_path = Path(str(created.get("technician_assignments") or ""))
    if summary_path.is_file():
        st.subheader("Candidate region summary")
        st.dataframe(_region_summary_display_frame(pd.read_csv(summary_path, encoding="utf-8-sig")), width="stretch", hide_index=True)
    if technician_path.is_file():
        st.subheader("Candidate technician assignments")
        st.dataframe(pd.read_csv(technician_path, encoding="utf-8-sig"), width="stretch", hide_index=True)

    st.subheader("Create Area Plan artifact")
    st.caption(
        "Convert this reviewed clustering candidate into the immutable Area Plan workbook used by Admin Tools. "
        "This step writes only a local candidate under `data/region_plans`; it does not upload, activate, or change the database."
    )
    default_target_city_id = re.sub(r"[^A-Za-z0-9]+", "_", city_name).strip("_") or "city"
    artifact_cols = st.columns(2)
    subsidiary_id = artifact_cols[0].text_input(
        "Subsidiary ID",
        value=selected_subsidiary if selected_subsidiary not in {ALL_OPTION, BLANK_CITY_OPTION} else "",
        key=f"candidate-area-plan-subsidiary::{token}",
    ).strip()
    target_city_id = artifact_cols[1].text_input(
        "Target city ID",
        value=default_target_city_id,
        key=f"candidate-area-plan-target-city::{token}",
        help="Stable policy-city identifier used by Region Plans v2, for example Atlanta_GA.",
    ).strip()
    plan_display_name = st.text_input(
        "Area Plan name",
        value=str(created.get("plan_id") or default_target_city_id),
        key=f"candidate-area-plan-name::{token}",
    ).strip()
    if st.button(
        "Create Area Plan from candidate",
        type="primary",
        disabled=not subsidiary_id or not target_city_id or not plan_display_name,
        key=f"candidate-area-plan-create::{token}",
    ):
        try:
            postal_path = Path(str(created.get("region_postals") or ""))
            if not postal_path.is_file() or not technician_path.is_file():
                raise FileNotFoundError("Candidate ZIP or technician artifact is unavailable.")
            export = build_area_map_region_plan(
                postal_path.name,
                postal_path.read_bytes(),
                technician_path.name,
                technician_path.read_bytes(),
                subsidiary_id=subsidiary_id,
                source_city_id=city_name,
                target_city_id=target_city_id,
                plan_display_name=plan_display_name,
            )
            output_dir = save_area_map_region_plan(export, REGION_PLAN_ROOT)
            created = dict(created)
            created["area_plan"] = {
                "plan_id": export.manifest.get("plan_id"),
                "output_dir": str(output_dir),
                "workbook": str(output_dir / "region_plan.xlsx"),
                "target_city_id": target_city_id,
            }
            st.session_state["area-plan-cluster-result"] = created
            st.success(f"Area Plan candidate created: `{output_dir}`")
        except (AreaMapRegionPlanError, OSError, ValueError, pd.errors.ParserError) as exc:
            st.error(f"Area Plan creation failed: {exc}")
    area_plan = created.get("area_plan")
    if isinstance(area_plan, Mapping):
        st.success(
            "Area Plan is ready for Admin Tools → Region Plans v2 → Add. "
            "Upload it, review it, activate it, then select it in VRP Client → City Routing Config."
        )
        st.json(dict(area_plan))


def _build_candidate_cluster_map(candidate: Mapping[str, object]) -> tuple[folium.Map, gpd.GeoDataFrame, int]:
    """Render a candidate ZIP assignment without promoting it to an Area Plan."""
    postal_path = Path(str(candidate.get("region_postals") or ""))
    summary_path = Path(str(candidate.get("region_summary") or ""))
    evidence_path = Path(str(candidate.get("evidence") or ""))
    if not postal_path.is_file() or not summary_path.is_file():
        raise FileNotFoundError("Candidate cluster artifacts are unavailable.")
    postals = pd.read_csv(postal_path, encoding="utf-8-sig", dtype={"POSTAL_CODE": str})
    required = {"POSTAL_CODE", "AREA_NAME", "region_seq", "region_id"}
    if not required.issubset(postals.columns):
        raise ValueError("Candidate ZIP artifact does not match the area-map contract.")
    postals["POSTAL_CODE"] = postals["POSTAL_CODE"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(5)
    summary = pd.read_csv(summary_path, encoding="utf-8-sig")
    required_summary = {"region_seq", "centroid_latitude", "centroid_longitude"}
    if not required_summary.issubset(summary.columns):
        raise ValueError("Candidate region summary lacks centroid coordinates.")
    zip_geometry = _area_map_load_zcta_geometry(postals["POSTAL_CODE"].tolist(), config_section=AREA_MAP_CONFIG_SECTION)
    zip_layer = zip_geometry.merge(postals, on="POSTAL_CODE", how="inner") if not zip_geometry.empty else gpd.GeoDataFrame()
    missing_geometry = int(len(postals) - zip_layer["POSTAL_CODE"].nunique()) if not zip_layer.empty else int(len(postals))
    if not zip_layer.empty:
        try:
            area_km2 = (
                zip_layer.to_crs("EPSG:6933")
                .assign(_area_km2=lambda frame: frame.geometry.area / 1_000_000.0)
                .groupby("region_seq", as_index=False)["_area_km2"].sum()
            )
            summary = summary.merge(area_km2, on="region_seq", how="left")
        except (ValueError, TypeError):
            summary["_area_km2"] = pd.NA
    else:
        summary["_area_km2"] = pd.NA
    center_lat = float(summary["centroid_latitude"].mean())
    center_lon = float(summary["centroid_longitude"].mean())
    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles="cartodbpositron")
    color_map = _generate_color_map(postals["AREA_NAME"].astype(str).tolist())
    if not zip_layer.empty:
        folium.GeoJson(
            zip_layer,
            name="Candidate ZIP Regions",
            style_function=lambda feature: {
                "fillColor": color_map.get(feature["properties"].get("AREA_NAME", ""), "#0f766e"),
                "color": color_map.get(feature["properties"].get("AREA_NAME", ""), "#0f766e"),
                "weight": 1.0,
                "fillOpacity": 0.22,
            },
            highlight_function=lambda feature: {"color": "#111111", "weight": 2.0, "fillOpacity": 0.35},
            tooltip=folium.GeoJsonTooltip(
                fields=["AREA_NAME", "POSTAL_CODE", "region_id"], aliases=["Candidate Area", "ZIP", "Region ID"]
            ),
        ).add_to(map_obj)
    for _, row in summary.iterrows():
        area_name = str(row.get("AREA_NAME", f"Region {row.get('region_seq', '')}"))
        area_value = pd.to_numeric(pd.Series([row.get("_area_km2")]), errors="coerce").iloc[0]
        postal_count = int(row.get("postal_count", 0) or 0)
        if pd.notna(area_value):
            area_text = f"{float(area_value):,.1f} km² (ZIP {postal_count})"
        else:
            area_text = f"Area unavailable (ZIP {postal_count})"
        technician_count = int(row.get("assigned_technician_count", 0) or 0)
        technician_names = html.escape(str(row.get("assigned_technician_names", "") or "(none)"))
        popup_html = (
            f"<b>{html.escape(area_name)}</b><br>"
            f"<b>Area</b>: {area_text}<br>"
            f"<b>Service</b>: {int(row.get('annual_service_count', 0) or 0):,} total / "
            f"{float(row.get('avg_daily_jobs', 0.0) or 0.0):,.2f} daily average<br>"
            f"<b>Technicians</b>: {technician_count}<br>"
            f"<b>Names</b>: {technician_names}"
        )
        folium.Marker(
            location=[float(row["centroid_latitude"]), float(row["centroid_longitude"])],
            icon=folium.DivIcon(
                html=(
                    f"<div style='background:{color_map.get(area_name, '#0f766e')};color:white;border:2px solid white;"
                    "border-radius:12px;padding:3px 7px;font-size:11px;font-weight:700;white-space:nowrap;'>"
                    f"{html.escape(area_name)}</div>"
                )
            ),
            tooltip=f"{area_name} candidate center",
            popup=folium.Popup(popup_html, max_width=420),
        ).add_to(map_obj)
    if evidence_path.is_file():
        evidence = pd.read_csv(evidence_path, encoding="utf-8-sig")
        if {"latitude", "longitude", "SVC_ENGINEER_CODE", "AREA_NAME"}.issubset(evidence.columns):
            homes = folium.FeatureGroup(name="Candidate Technician Homes", show=True)
            for _, row in evidence.dropna(subset=["latitude", "longitude"]).iterrows():
                area_name = str(row.get("AREA_NAME", ""))
                folium.CircleMarker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    radius=5,
                    color=color_map.get(area_name, "#111827"),
                    fill=True,
                    fill_color=color_map.get(area_name, "#111827"),
                    fill_opacity=0.95,
                    tooltip=f"{row['SVC_ENGINEER_CODE']} → {area_name}",
                ).add_to(homes)
            homes.add_to(map_obj)
    folium.LayerControl(collapsed=False).add_to(map_obj)
    return map_obj, zip_layer, missing_geometry


def _build_selected_area_plan_preview(
    candidate: Mapping[str, object],
    area_layer: gpd.GeoDataFrame,
    service_df: pd.DataFrame,
    city_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create candidate-style, read-only facts for a selected Area Plan."""
    if area_layer.empty or "AREA_NAME" not in area_layer.columns:
        return pd.DataFrame(), pd.DataFrame()
    areas = area_layer.copy()
    areas["AREA_NAME"] = areas["AREA_NAME"].astype(str).str.strip()
    areas = areas[areas["AREA_NAME"].ne("")].copy()
    if areas.empty:
        return pd.DataFrame(), pd.DataFrame()
    if "region_seq" not in areas.columns:
        areas["region_seq"] = pd.NA
    areas["region_seq"] = pd.to_numeric(areas["region_seq"], errors="coerce")
    missing_sequence = areas["region_seq"].isna()
    if missing_sequence.any():
        sequence_map = {name: index + 1 for index, name in enumerate(sorted(areas.loc[missing_sequence, "AREA_NAME"].unique()))}
        areas.loc[missing_sequence, "region_seq"] = areas.loc[missing_sequence, "AREA_NAME"].map(sequence_map)
    areas["region_seq"] = areas["region_seq"].astype(int)
    if "region_id" not in areas.columns:
        areas["region_id"] = ""
    areas["region_id"] = areas["region_id"].astype(str).str.strip()

    summary_rows: list[dict[str, object]] = []
    working_service = service_df.copy()
    job_column = "GSFS_RECEIPT_NO" if "GSFS_RECEIPT_NO" in working_service.columns else None
    for (_, area_name), group in areas.groupby(["region_seq", "AREA_NAME"], sort=True):
        geometry = group.geometry.dropna().union_all() if "geometry" in group else None
        point = geometry.representative_point() if geometry is not None and not geometry.is_empty else None
        try:
            area_km2 = float(gpd.GeoSeries([geometry], crs=areas.crs).to_crs("EPSG:6933").area.iloc[0] / 1_000_000.0)
        except (AttributeError, TypeError, ValueError):
            area_km2 = pd.NA
        service_group = working_service[working_service.get("AREA_NAME", pd.Series(index=working_service.index, dtype=str)).astype(str).eq(area_name)]
        if job_column:
            annual_service_count = int(service_group[job_column].dropna().astype(str).nunique())
            daily = (
                service_group.groupby("service_date_key")[job_column].agg(lambda values: values.dropna().astype(str).nunique())
                if "service_date_key" in service_group.columns else pd.Series(dtype=float)
            )
        else:
            annual_service_count = int(len(service_group))
            daily = service_group.groupby("service_date_key").size() if "service_date_key" in service_group.columns else pd.Series(dtype=float)
        summary_rows.append({
            "region_seq": int(group["region_seq"].iloc[0]),
            "region_id": str(group["region_id"].iloc[0]),
            "AREA_NAME": area_name,
            "postal_count": int(
                pd.to_numeric(group["postal_count"], errors="coerce").fillna(0).sum()
                if "postal_count" in group.columns else 0
            ),
            "annual_service_count": annual_service_count,
            "avg_daily_jobs": round(float(daily.mean()) if not daily.empty else 0.0, 3),
            "area_km2": area_km2,
            "centroid_latitude": float(point.y) if point is not None else pd.NA,
            "centroid_longitude": float(point.x) if point is not None else pd.NA,
        })
    summary = pd.DataFrame(summary_rows).sort_values("region_seq").reset_index(drop=True)

    technician_path = Path(str(candidate.get("path") or "")) / "normalized" / "technician.csv"
    if not technician_path.is_file():
        summary["assigned_technician_count"] = 0
        summary["assigned_technician_names"] = ""
        summary["avg_daily_jobs_per_assigned_technician"] = pd.NA
        return summary, pd.DataFrame()
    try:
        technicians = pd.read_csv(technician_path, dtype=str, keep_default_na=False)
    except (OSError, ValueError, pd.errors.ParserError):
        technicians = pd.DataFrame()
    required = {"technician_id", "region_code"}
    if technicians.empty or not required.issubset(technicians.columns):
        summary["assigned_technician_count"] = 0
        summary["assigned_technician_names"] = ""
        summary["avg_daily_jobs_per_assigned_technician"] = pd.NA
        return summary, pd.DataFrame()
    technicians = technicians.copy()
    if "active" in technicians.columns:
        technicians = technicians[technicians["active"].astype(str).str.casefold().isin({"true", "1", "yes", "y"})].copy()
    technicians["employee_code"] = technicians["technician_id"].astype(str).str.strip()
    technicians["region_id"] = technicians["region_code"].astype(str).str.strip()
    technicians = technicians.merge(
        summary[["region_seq", "region_id", "AREA_NAME"]], on="region_id", how="left"
    ).rename(columns={"AREA_NAME": "assigned_region_name", "region_seq": "assigned_region_seq"})
    technicians["policy_mode"] = technicians["policy_mode"].astype(str) if "policy_mode" in technicians.columns else ""
    profile = _load_profile_home_geocode_df()
    if not profile.empty:
        profile["SVC_ENGINEER_CODE"] = profile["SVC_ENGINEER_CODE"].astype(str).str.strip()
        name_column = "Name" if "Name" in profile.columns else None
        if name_column:
            name_lookup = profile.drop_duplicates("SVC_ENGINEER_CODE").set_index("SVC_ENGINEER_CODE")[name_column].fillna("").astype(str).to_dict()
        else:
            name_lookup = {}
    else:
        name_lookup = {}
    technicians["SVC_ENGINEER_NAME"] = technicians["employee_code"].map(name_lookup).fillna("")
    home_lookup = get_home_location_lookup(city_name, tuple(technicians["employee_code"].tolist()))
    technicians["longitude"] = technicians["employee_code"].map(lambda code: (home_lookup.get(code) or {}).get("coord", (pd.NA, pd.NA))[0])
    technicians["latitude"] = technicians["employee_code"].map(lambda code: (home_lookup.get(code) or {}).get("coord", (pd.NA, pd.NA))[1])
    technicians = technicians[[
        "employee_code", "SVC_ENGINEER_NAME", "assigned_region_seq", "region_id", "assigned_region_name",
        "policy_mode", "latitude", "longitude",
    ]].sort_values(["assigned_region_seq", "employee_code"], na_position="last").reset_index(drop=True)
    counts = technicians.groupby("assigned_region_seq")["employee_code"].nunique()
    names = technicians.groupby("assigned_region_seq")["SVC_ENGINEER_NAME"].agg(
        lambda values: " | ".join(sorted({str(value).strip() or "(name unavailable)" for value in values}))
    )
    summary["assigned_technician_count"] = summary["region_seq"].map(counts).fillna(0).astype(int)
    summary["assigned_technician_names"] = summary["region_seq"].map(names).fillna("")
    daily_jobs = pd.to_numeric(summary["avg_daily_jobs"], errors="coerce")
    technician_counts = pd.to_numeric(summary["assigned_technician_count"], errors="coerce")
    summary["avg_daily_jobs_per_assigned_technician"] = (daily_jobs / technician_counts.mask(technician_counts.eq(0))).round(3)
    return summary, technicians


def _build_selected_area_plan_preview_map(
    area_layer: gpd.GeoDataFrame,
    summary: pd.DataFrame,
    technicians: pd.DataFrame,
) -> folium.Map:
    """Render the selected plan with the same region centers and home layer as a candidate."""
    display_areas = area_layer.drop(
        columns=[column for column in ["postal_count", "service_count"] if column in area_layer.columns]
    ).merge(summary, on=["region_seq", "region_id", "AREA_NAME"], how="inner")
    center_lat = float(summary["centroid_latitude"].dropna().mean()) if not summary.empty else 39.0
    center_lon = float(summary["centroid_longitude"].dropna().mean()) if not summary.empty else -98.0
    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles="cartodbpositron")
    colors = _generate_color_map(summary["AREA_NAME"].astype(str).tolist()) if not summary.empty else {}
    if not display_areas.empty:
        folium.GeoJson(
            display_areas,
            name="Area Plan Regions",
            style_function=lambda feature: {
                "fillColor": colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e"),
                "color": colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e"),
                "weight": 1.0, "fillOpacity": 0.22,
            },
            tooltip=folium.GeoJsonTooltip(fields=["AREA_NAME", "postal_count"], aliases=["Area", "ZIP count"]),
        ).add_to(map_obj)
    for _, row in summary.dropna(subset=["centroid_latitude", "centroid_longitude"]).iterrows():
        name = str(row["AREA_NAME"])
        popup = (
            f"<b>{html.escape(name)}</b><br><b>ZIPs</b>: {int(row['postal_count']):,}<br>"
            f"<b>Service</b>: {int(row['annual_service_count']):,} total / {float(row['avg_daily_jobs']):,.2f} daily average<br>"
            f"<b>Technicians</b>: {int(row['assigned_technician_count'])}<br>"
            f"<b>Names</b>: {html.escape(str(row['assigned_technician_names'] or '(none)'))}"
        )
        folium.Marker(
            location=[float(row["centroid_latitude"]), float(row["centroid_longitude"])],
            icon=folium.DivIcon(html=f"<div style='background:{colors.get(name, '#0f766e')};color:white;border:2px solid white;border-radius:12px;padding:3px 7px;font-size:11px;font-weight:700;white-space:nowrap;'>{html.escape(name)}</div>"),
            popup=folium.Popup(popup, max_width=420), tooltip=f"{name} center",
        ).add_to(map_obj)
    homes = folium.FeatureGroup(name="Area Plan Technician Homes", show=True)
    for _, row in technicians.dropna(subset=["latitude", "longitude"]).iterrows():
        name = str(row.get("assigned_region_name") or "")
        folium.CircleMarker(
            location=[float(row["latitude"]), float(row["longitude"])], radius=5,
            color=colors.get(name, "#111827"), fill=True, fill_color=colors.get(name, "#111827"), fill_opacity=0.95,
            tooltip=f"{row['employee_code']} → {name}",
        ).add_to(homes)
    homes.add_to(map_obj)
    folium.LayerControl(collapsed=False).add_to(map_obj)
    return map_obj


def _render_region_plan_editor(selected_subsidiary: str, selected_city: str) -> None:
    editor = st.session_state.get("area-plan-editor")
    if not isinstance(editor, Mapping):
        return
    mode = str(editor.get("mode") or "add")
    candidates = _region_plan_candidate_rows(selected_subsidiary, selected_city)
    candidate = next(
        (row for row in candidates if str(row.get("path")) == str(editor.get("candidate_path"))),
        None,
    )
    if mode == "edit" and candidate is None:
        st.warning("선택한 Area Plan을 찾을 수 없습니다. 목록을 새로고침해 주세요.")
        return

    manifest = (candidate or {}).get("manifest") if candidate else {}
    manifest = manifest if isinstance(manifest, Mapping) else {}
    metadata = manifest.get("city_metadata") if isinstance(manifest.get("city_metadata"), Mapping) else {}
    source_city_default = str(
        metadata.get("source_city_id")
        or manifest.get("source_strategic_city_name")
        or _base_city_name(selected_city)
    ).strip()
    target_default = str(
        metadata.get("target_city_id")
        or manifest.get("target_city_id")
        or re.sub(r"[^A-Za-z0-9]+", "_", source_city_default).strip("_")
    ).strip()
    name_default = str(manifest.get("plan_display_name") or manifest.get("plan_id") or target_default).strip()
    policy_default = str(
        metadata.get("policy_version")
        or manifest.get("policy_version")
        or "explicit_workbook_membership/v1"
    )
    token = re.sub(r"[^A-Za-z0-9]+", "_", str(editor.get("candidate_path") or "new")).strip("_")[-80:]

    st.divider()
    st.subheader("Area Plan " + ("추가" if mode == "add" else "수정"))
    st.caption("저장 시 데이터가 바뀌면 새로운 checksum Plan으로 저장되고, 이름만 바꾸면 현재 Plan의 표시 이름이 갱신됩니다.")
    close_col, delete_col, _ = st.columns([1, 1, 4])
    if close_col.button("닫기", key=f"area-plan-editor-close::{token}"):
        st.session_state.pop("area-plan-editor", None)
        st.rerun()
    if mode == "edit" and candidate is not None:
        with delete_col.popover("Delete"):
            st.warning("Deletes only this local Area Map candidate. DB data is not deleted.")
            if st.button(
                "Confirm delete",
                type="secondary",
                key=f"area-plan-delete::{token}",
            ):
                try:
                    _delete_local_area_plan_candidate(candidate)
                    st.session_state.pop("area-plan-editor", None)
                    st.session_state.pop("area-plan-editor-result", None)
                    st.success("Local Area Plan candidate deleted.")
                    st.rerun()
                except (OSError, ValueError) as exc:
                    st.error(f"Area Plan delete failed: {exc}")

    metadata_cols = st.columns(4)
    subsidiary_id = metadata_cols[0].text_input(
        "법인 ID", value=str(metadata.get("subsidiary_id") or manifest.get("subsidiary_name") or selected_subsidiary), key=f"area-plan-subsidiary::{token}"
    ).strip()
    source_city_id = metadata_cols[1].text_input(
        "source_strategic_city_name", value=source_city_default, key=f"area-plan-source-city::{token}"
    ).strip()
    # Area Plans are keyed by their operational city; the historical target
    # city ID is no longer an operator input.
    target_city_id = source_city_id
    plan_display_name = metadata_cols[3].text_input(
        "Plan 이름", value=name_default, key=f"area-plan-name::{token}"
    ).strip()
    existing_region = _plan_source_bytes(candidate, "region") if candidate else None
    existing_technician = _plan_source_bytes(candidate, "technician") if candidate else None
    if existing_region:
        st.caption(f"기존 Region 원본: `{existing_region[0]}`")
    if existing_technician:
        st.caption(f"기존 Technician 원본: `{existing_technician[0]}`")
    else:
        st.info("기존 Technician 지역 배정 파일이 없습니다. 새 파일을 업로드해야 합니다.")
    template_cols = st.columns(2)
    region_template = pd.DataFrame(columns=[
        "POSTAL_CODE", "STRATEGIC_CITY_NAME", "region_id", "region_seq",
        "AREA_NAME", "new_region_name", "area_type",
    ])
    technician_template = pd.DataFrame(columns=["Tech ID", "Tech Name", "Assignment"])
    with template_cols[0]:
        st.download_button(
            "Region 데이터 template 다운로드",
            data=_to_csv_bytes(region_template),
            file_name="region_plan_region_template.csv",
            mime="text/csv",
            key=f"area-plan-region-template::{token}",
        )
    with template_cols[1]:
        st.download_button(
            "Technician 데이터 template 다운로드",
            data=_to_csv_bytes(technician_template),
            file_name="region_plan_technician_template.csv",
            mime="text/csv",
            key=f"area-plan-technician-template::{token}",
        )
    region_upload = st.file_uploader(
        "Region 데이터 (새 파일을 올리면 교체)", type=["csv", "xlsx"], key=f"area-plan-region-upload::{token}"
    )
    technician_upload = st.file_uploader(
        "Technician 데이터 (새 파일을 올리면 교체)", type=["csv", "xlsx"], key=f"area-plan-technician-upload::{token}"
    )
    if st.button(
        "Area Plan 저장",
        type="primary",
        disabled=not subsidiary_id or not source_city_id or not target_city_id or not plan_display_name,
        key=f"area-plan-save::{token}",
    ):
        region_name, region_bytes = (
            (region_upload.name, region_upload.getvalue()) if region_upload else existing_region or ("", b"")
        )
        technician_name, technician_bytes = (
            (technician_upload.name, technician_upload.getvalue()) if technician_upload else existing_technician or ("", b"")
        )
        original_subsidiary = str(metadata.get("subsidiary_id") or manifest.get("subsidiary_name") or selected_subsidiary).strip()
        original_source_city = str(
            metadata.get("source_city_id")
            or manifest.get("source_strategic_city_name")
            or _base_city_name(selected_city)
        ).strip()
        original_target_city = str(
            metadata.get("target_city_id")
            or manifest.get("target_city_id")
            or ""
        ).strip()
        metadata_changed = (
            subsidiary_id != original_subsidiary
            or source_city_id != original_source_city
            or target_city_id != original_target_city
        )
        if (
            mode == "edit"
            and candidate is not None
            and not region_upload
            and not technician_upload
            and existing_technician is None
            and not metadata_changed
        ):
            try:
                manifest_path = _update_candidate_display_name(candidate, plan_display_name)
                updated_manifest = dict(manifest)
                updated_manifest["plan_display_name"] = plan_display_name
                st.session_state["area-plan-editor-result"] = {
                    "manifest": updated_manifest,
                    "output_dir": str(manifest_path.parent),
                }
                st.success(f"Area Plan 표시 이름을 저장했습니다: `{manifest_path.parent}`")
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                st.error(f"Area Plan 이름 저장 실패: {exc}")
            return
        if not region_bytes or not technician_bytes:
            st.error("데이터를 수정하려면 Region과 Technician 파일을 모두 준비해야 합니다. 이름만 수정할 때는 파일을 업로드하지 마세요.")
        else:
            adapted_name, adapted_region_bytes = _adapt_region_source_city(region_name, region_bytes, target_city_id)
            try:
                export = build_area_map_region_plan(
                    adapted_name, adapted_region_bytes,
                    technician_name, technician_bytes,
                    subsidiary_id=subsidiary_id,
                    source_city_id=source_city_id,
                    target_city_id=target_city_id,
                    plan_display_name=plan_display_name,
                    overflow_penalty_minutes=4500,
                )
                export.manifest["plan_display_name"] = plan_display_name
                output_dir = save_area_map_region_plan(export, REGION_PLAN_ROOT)
                st.session_state["area-plan-editor-result"] = {
                    "manifest": dict(export.manifest), "output_dir": str(output_dir),
                }
                st.success(f"Area Plan을 저장했습니다: `{output_dir}`")
            except (AreaMapRegionPlanError, ValueError, OSError) as exc:
                st.error(f"Area Plan 저장 실패: {exc}")
    result = st.session_state.get("area-plan-editor-result")
    if isinstance(result, Mapping):
        st.json({
            "plan_id": (result.get("manifest") or {}).get("plan_id"),
            "plan_display_name": (result.get("manifest") or {}).get("plan_display_name"),
            "saved_directory": result.get("output_dir"),
        })


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
        working["CENTER_BUCKET"] = "ASC"
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
    st.title("Routing Map")
    latest_service = get_latest_geocoded_service_file()
    if latest_service is not None:
        st.caption(f"Service source: `{latest_service}`")

    with st.sidebar:
        st.header("Filters")
        subsidiary_options, city_options_by_subsidiary = get_service_scope_options(str(latest_service) if latest_service else None)
        selected_subsidiary = st.selectbox("SUBSIDIARY_NAME", subsidiary_options, index=0)
        strategic_city_options = city_options_by_subsidiary.get(selected_subsidiary, [ALL_OPTION])
        selected_strategic_city = st.selectbox("STRATEGIC_CITY_NAME", strategic_city_options, index=0)
        selected_area_plan = _render_area_plan_sidebar(selected_subsidiary, selected_strategic_city)
        city_name = selected_strategic_city if selected_strategic_city not in {ALL_OPTION, BLANK_CITY_OPTION} else ALL_CITIES

        # Area Plan is the only supported region source.
        region_option = "Area Plan"
        selected_region_count = None

        explorer_data = get_route_explorer_data(
            city_name,
            selected_region_count,
            AREA_MAP_CACHE_VERSION,
            _current_coverage_source_fingerprint(latest_service),
        )
        plan_frames = _load_area_plan_frames(selected_area_plan, explorer_data) if selected_area_plan else None
        if selected_area_plan and plan_frames is None:
            st.error("선택한 Area Plan의 normalized/area.csv 또는 ZIP geometry를 불러오지 못했습니다.")
        if plan_frames is not None:
            _, area_layer, service_df = plan_frames
            effective_region_count = max(1, int(area_layer["AREA_NAME"].nunique())) if not area_layer.empty else None
        else:
            _, area_layer, service_df = _empty_area_plan_frames(explorer_data)
            effective_region_count = None
            if selected_area_plan is None:
                st.info("Area Plan을 선택하면 해당 Plan의 지역 경계와 우편번호가 지도에 표시됩니다.")
        service_df = service_df.copy()
        service_df = _apply_service_scope_filters(service_df, selected_subsidiary, selected_strategic_city)
        if "service_date" not in service_df.columns:
            service_df["service_date"] = pd.NaT
        service_df["service_date_key"] = pd.to_datetime(service_df["service_date"]).dt.strftime("%Y-%m-%d")
        service_df = _apply_center_bucket_rules(service_df, effective_region_count)

        area_type_options = _available_area_types(area_layer, service_df) if effective_region_count is not None else []
        selected_area_types: list[str] | None = None
        area_plan_key = str((selected_area_plan or {}).get("plan_id") or "none")
        if area_type_options:
            st.caption("Area Type")
            area_type_cols = st.columns(max(1, min(3, len(area_type_options))))
            selected_area_types = []
            for idx, area_type in enumerate(area_type_options):
                area_type_col = area_type_cols[idx % len(area_type_cols)]
                label = _area_type_label(area_type)
                if area_type_col.checkbox(
                    label,
                    value=True,
                    key=f"area_type_filter::{city_name}::{region_option}::{area_plan_key}::{area_type}",
                ):
                    selected_area_types.append(area_type)
            area_layer = _filter_area_type(area_layer, selected_area_types)
            service_df = _filter_area_type(service_df, selected_area_types)

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
                key=f"center_bucket_filter::{city_name}::{region_option}::{selected_date}::{center_bucket}",
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
            key=f"area_name_filter::{city_name}::{region_option}::{area_plan_key}",
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

    _render_region_plan_editor(selected_subsidiary, selected_strategic_city)
    _render_candidate_cluster_editor(selected_subsidiary, selected_strategic_city)

    candidate_preview = st.session_state.get("area-plan-cluster-result")
    selected_plan_summary = pd.DataFrame()
    selected_plan_technicians = pd.DataFrame()
    if isinstance(candidate_preview, Mapping) and str(candidate_preview.get("city")) == selected_strategic_city:
        try:
            map_obj, filtered_area_layer, missing_candidate_geometry = _build_candidate_cluster_map(candidate_preview)
            filtered_service_df = service_df.copy()
            route_groups = []
            st.info("Showing the unapproved candidate cluster map. Select or create an Area Plan to return to the approved-plan map.")
            if missing_candidate_geometry:
                st.warning(f"Candidate ZIP geometry is unavailable for {missing_candidate_geometry} ZIP code(s).")
        except (OSError, ValueError, pd.errors.ParserError) as exc:
            st.error(f"Candidate cluster map could not be rendered: {exc}")
            map_obj, filtered_service_df, filtered_area_layer, route_groups = build_map(
                city_name=city_name,
                subsidiary_name=selected_subsidiary,
                strategic_city_name=selected_strategic_city,
                region_count=selected_region_count,
                area_names=selected_area_names,
                selected_area_types=selected_area_types,
                selected_date=selected_date,
                selected_sm=selected_sm,
                selected_center_buckets=selected_center_buckets,
                area_plan_candidate=selected_area_plan,
            )
    else:
        map_obj, filtered_service_df, filtered_area_layer, route_groups = build_map(
            city_name=city_name,
            subsidiary_name=selected_subsidiary,
            strategic_city_name=selected_strategic_city,
            region_count=selected_region_count,
            area_names=selected_area_names,
            selected_area_types=selected_area_types,
            selected_date=selected_date,
            selected_sm=selected_sm,
            selected_center_buckets=selected_center_buckets,
            area_plan_candidate=selected_area_plan,
        )
    if (
        not (isinstance(candidate_preview, Mapping) and str(candidate_preview.get("city")) == selected_strategic_city)
        and selected_area_plan is not None
    ):
        try:
            selected_plan_summary, selected_plan_technicians = _build_selected_area_plan_preview(
                selected_area_plan,
                filtered_area_layer,
                service_df,
                selected_strategic_city,
            )
            if not selected_plan_summary.empty:
                map_obj = _build_selected_area_plan_preview_map(
                    filtered_area_layer,
                    selected_plan_summary,
                    selected_plan_technicians,
                )
                route_groups = []
                st.info("Showing Area Plan regions, plan technician assignments, and technician homes.")
        except (OSError, ValueError, KeyError, pd.errors.ParserError) as exc:
            st.warning(f"Area Plan summary preview could not be rendered: {exc}")

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

    if selected_area_plan is not None and not selected_plan_summary.empty:
        st.subheader("Area Plan region summary")
        st.dataframe(_region_summary_display_frame(selected_plan_summary), width="stretch", hide_index=True)
        st.subheader("Technician assignments")
        if selected_plan_technicians.empty:
            st.caption("This Area Plan has no active technician policy records.")
        else:
            st.dataframe(selected_plan_technicians, width="stretch", hide_index=True)
        return

    candidate_col, summary_col, detail_col = st.columns([1.25, 1.0, 1.75], gap="medium")
    with candidate_col:
        st.subheader("Area Plan Summary")
        selected_manifest = (selected_area_plan or {}).get("manifest", {}) if isinstance(selected_area_plan, Mapping) else {}
        selected_metadata = selected_manifest.get("city_metadata", {}) if isinstance(selected_manifest, Mapping) else {}
        candidate_df = pd.DataFrame([{
            "Plan": str((selected_area_plan or {}).get("label") or "No Area Plan selected"),
            "Source City": str((selected_area_plan or {}).get("source_city_id") or selected_metadata.get("source_city_id") or ""),
            "Policy City": str((selected_area_plan or {}).get("target_city_id") or selected_metadata.get("target_city_id") or ""),
            "Plan ID": str((selected_area_plan or {}).get("plan_id") or ""),
        }]) if selected_area_plan else pd.DataFrame()
        if candidate_df.empty:
            st.caption("No Area Plan selected.")
        else:
            st.dataframe(candidate_df, width="stretch", height=300)
        st.subheader("Jobs Input")
        jobs_input_df = _build_vrp_jobs_input_df(filtered_service_df)
        if jobs_input_df.empty:
            st.caption("No jobs for the selected filters.")
        else:
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
