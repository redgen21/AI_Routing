from __future__ import annotations

import colorsys
import copy
import io
import json
import math
import os
import re
import uuid
from datetime import date, datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib import parse, request as urllib_request

import folium
import pandas as pd
import streamlit as st
from folium.plugins import Draw
try:
    from streamlit_folium import st_folium
except Exception:
    st_folium = None

from smart_routing.area_map import load_city_map_data, load_zcta_geometry
from smart_routing.data_catalog import na_data_path
from smart_routing.live_atlanta_runtime import _load_config as _load_runtime_config
from smart_routing.live_atlanta_runtime import _merge_service_geocodes
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient
from services.api.common_vrp_config import (
    configured_api_url,
    load_and_validate_common_config,
)


st.set_page_config(page_title="Smart Routing Client", layout="wide")

_config_path_value = os.environ.get("COMMON_VRP_CONFIG_PATH", "").strip()
if not _config_path_value:
    raise RuntimeError(
        "COMMON_VRP_CONFIG_PATH is required; select an explicit development or production config."
    )
CONFIG_COMMON_PATH = Path(_config_path_value).expanduser().resolve()


def _load_configured_common_server_url(config_path: Path = CONFIG_COMMON_PATH) -> str:
    cfg = load_and_validate_common_config(config_path)
    return configured_api_url(cfg)


DEFAULT_COMMON_SERVER_URL = _load_configured_common_server_url()
MASTER_PATH = na_data_path("client_master")
PROFILE_SOURCE_PATH = na_data_path("profile_production")
DEFAULT_SUBSIDIARY_NAME = "LGEAI"
DEFAULT_STRATEGIC_CITY_NAME = "Atlanta, GA"
ATLANTA_6AREA_CITY_NAME = "Atlanta_6area"
ATLANTA_REGION_CITY_NAMES = (
    ATLANTA_6AREA_CITY_NAME,
    "Atlanta_3area",
    "Atlanta_6area_new",
    "Atlanta_6area_overlab",
)
STRATEGIC_CITY_BASE_ALIASES = {
    city_name: DEFAULT_STRATEGIC_CITY_NAME for city_name in ATLANTA_REGION_CITY_NAMES
}
_CITY_CONTEXT_METADATA: dict[str, dict[str, object]] = {}
DEFAULT_STRATEGIC_CITY_OPTIONS = [
    DEFAULT_STRATEGIC_CITY_NAME,
    *ATLANTA_REGION_CITY_NAMES,
    "Los Angeles, CA",
]
KM_TO_MILES = 0.621371
FORCE_ASSIGN_PREVIEW_KEY = "common_vrp_force_assign_preview"

JOB_REQUIRED_COLUMNS = [
    "SVC_ENGINEER_CODE",
    "SVC_ENGINEER_NAME",
    "SERVICE_PRODUCT_GROUP_CODE",
    "SERVICE_PRODUCT_CODE",
    "RECEIPT_DETAIL_SYMPTOM_CODE",
    "GSFS_RECEIPT_NO",
    "PROMISE_DATE",
    "CITY_NAME",
    "POSTAL_CODE",
    "ADDRESS_LINE1_INFO",
]

TECHNICIAN_UPLOAD_COLUMNS = [
    "employee_name",
    "employee_code",
    "center_type",
    "available",
    "slot_count",
    "priority_group",
    "preferred_region_name",
    "max_minutes",
]
JOB_SAMPLE_PATH = Path("data/common_vrp_jobs_sample_20260330.csv")
TECHNICIAN_SAMPLE_PATH = Path("data/common_vrp_technicians_sample_20260330.csv")


def _coerce_bool_value(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "y", "yes", "t"}:
        return True
    if text in {"false", "0", "n", "no", "f", ""}:
        return False
    return False


def _coerce_bool_series(series: pd.Series) -> pd.Series:
    return series.map(_coerce_bool_value).astype(bool)


def _coerce_job_slot_count_value(value: object, default: int = 1) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return int(default)
    return max(1, int(numeric))


def _coerce_job_slot_count_series(series: pd.Series) -> pd.Series:
    return series.map(_coerce_job_slot_count_value).astype(int)


def _build_engineer_slot_capacity_lookup(engineer_master_df: pd.DataFrame) -> dict[str, int]:
    capacity_lookup: dict[str, int] = {}
    if engineer_master_df.empty or "employee_code" not in engineer_master_df.columns:
        return capacity_lookup
    capacity_col = "slot_count" if "slot_count" in engineer_master_df.columns else "max_slots" if "max_slots" in engineer_master_df.columns else ""
    if not capacity_col:
        return capacity_lookup
    for _, row in engineer_master_df.iterrows():
        code = str(row.get("employee_code", "")).strip()
        capacity = pd.to_numeric(pd.Series([row.get(capacity_col, 8)]), errors="coerce").fillna(8).iloc[0]
        if code:
            capacity_lookup[code] = max(0, int(capacity))
    return capacity_lookup


def _build_result_slot_capacity_lookup(result_payload: dict[str, object]) -> dict[str, int]:
    summary_df = pd.DataFrame(result_payload.get("engineer_summary", [])) if isinstance(result_payload, dict) else pd.DataFrame()
    if summary_df.empty:
        return {}
    code_col = "SVC_ENGINEER_CODE" if "SVC_ENGINEER_CODE" in summary_df.columns else "employee_code" if "employee_code" in summary_df.columns else ""
    capacity_col = "max_slots" if "max_slots" in summary_df.columns else "slot_count" if "slot_count" in summary_df.columns else ""
    if not code_col or not capacity_col:
        return {}
    capacity_lookup: dict[str, int] = {}
    for _, row in summary_df.iterrows():
        code = str(row.get(code_col, "")).strip()
        capacity = pd.to_numeric(pd.Series([row.get(capacity_col)]), errors="coerce").iloc[0]
        if code and pd.notna(capacity):
            capacity_lookup[code] = max(0, int(capacity))
    return capacity_lookup


def _fixed_slot_capacity_warnings(jobs_df: pd.DataFrame, engineer_master_df: pd.DataFrame) -> pd.DataFrame:
    if jobs_df.empty or "fixed" not in jobs_df.columns:
        return pd.DataFrame()
    required_cols = {"svc_engineer_code", "promise_date", "job_slot_count"}
    if not required_cols.issubset(jobs_df.columns):
        return pd.DataFrame()
    fixed_df = jobs_df.copy()
    fixed_df["fixed"] = _coerce_bool_series(fixed_df["fixed"])
    fixed_df = fixed_df[fixed_df["fixed"]].copy()
    if fixed_df.empty:
        return pd.DataFrame()
    fixed_df["job_slot_count"] = _coerce_job_slot_count_series(fixed_df["job_slot_count"])
    fixed_df["svc_engineer_code"] = fixed_df["svc_engineer_code"].astype(str).str.strip()
    fixed_df["svc_engineer_name"] = fixed_df.get("svc_engineer_name", fixed_df["svc_engineer_code"]).astype(str).str.strip()
    fixed_df["promise_date"] = fixed_df["promise_date"].astype(str).str.strip()
    summary_df = (
        fixed_df.groupby(["promise_date", "svc_engineer_code", "svc_engineer_name"], dropna=False)
        .agg(
            fixed_job_count=("gsfs_receipt_no", "count") if "gsfs_receipt_no" in fixed_df.columns else ("job_slot_count", "count"),
            fixed_slot_count=("job_slot_count", "sum"),
        )
        .reset_index()
    )
    capacity_lookup = _build_engineer_slot_capacity_lookup(engineer_master_df)
    summary_df["slot_capacity"] = summary_df["svc_engineer_code"].map(lambda code: capacity_lookup.get(str(code).strip(), 8))
    warning_df = summary_df[summary_df["fixed_slot_count"].astype(int) > summary_df["slot_capacity"].astype(int)].copy()
    return warning_df.sort_values(["promise_date", "fixed_slot_count"], ascending=[False, False]).reset_index(drop=True)


def _render_fixed_slot_capacity_warnings(jobs_df: pd.DataFrame, engineer_master_df: pd.DataFrame) -> None:
    warning_df = _fixed_slot_capacity_warnings(jobs_df, engineer_master_df)
    if warning_df.empty:
        return
    labels = [
        f"{row.svc_engineer_name} ({row.svc_engineer_code}) on {row.promise_date}: fixed slots {int(row.fixed_slot_count)} > slot_count {int(row.slot_capacity)}"
        for row in warning_df.itertuples(index=False)
    ]
    st.warning("Fixed capacity override will be applied. " + "; ".join(labels[:5]))
    with st.expander("Fixed Capacity Pre-check", expanded=True):
        st.dataframe(warning_df, width="stretch", hide_index=True)


def _coerce_priority_group_value(value: object, default: str = "B") -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        numeric_priority = min(max(int(numeric), 1), 3)
        return {3: "A", 2: "B", 1: "C"}.get(numeric_priority, default)
    text = str(value or "").strip().upper()
    if text in {"A", "HIGH", "P3", "PRIORITY 3"}:
        return "A"
    if text in {"C", "LOW", "P1", "PRIORITY 1"}:
        return "C"
    if text in {"B", "MEDIUM", "MID", "P2", "PRIORITY 2"}:
        return "B"
    return str(default).strip().upper() or "B"


def _optional_job_slot_count_series(df: pd.DataFrame) -> pd.Series:
    normalized_lookup = {_canonicalize_column_name(col): col for col in df.columns}
    for candidate in ["job_slot_count"]:
        col = normalized_lookup.get(_canonicalize_column_name(candidate))
        if col:
            return _coerce_job_slot_count_series(df[col])
    for candidate, slot_count in [("3slot_job", 3), ("3slot", 3), ("2slot_job", 2), ("2slot", 2), ("two_slot_job", 2), ("two_slot", 2)]:
        col = normalized_lookup.get(_canonicalize_column_name(candidate))
        if col:
            return _coerce_bool_series(df[col]).map(lambda value: slot_count if value else 1).astype(int)
    return pd.Series(1, index=df.index, dtype=int)


def _optional_bool_series(df: pd.DataFrame, candidate_cols: list[str]) -> pd.Series:
    normalized_lookup = {_canonicalize_column_name(col): col for col in df.columns}
    for candidate in candidate_cols:
        source_col = normalized_lookup.get(_canonicalize_column_name(candidate))
        if source_col is not None:
            return _coerce_bool_series(df[source_col])
    return pd.Series(False, index=df.index)


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _popup_text(value: object) -> str:
    return _clean_text(value)


def _canonicalize_column_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _clean_text(value).lower()).strip("_")


def _normalize_start_location_type(value: object) -> str:
    text = _canonicalize_column_name(value)
    if text in {"", "home"}:
        return "Home"
    if text in {"custom", "custom_address", "address"}:
        return "Custom Address"
    if "custom" in text:
        return "Custom Address"
    return "Home"


# ---------------------------------------------------------------------------
# Map / UI helpers (independent copy, no dependency on sr_vrp_api_client)
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def _load_common_client_config(config_path: str = str(CONFIG_COMMON_PATH)) -> dict:
    return load_and_validate_common_config(Path(config_path))


def _register_context_city_metadata(contexts: object) -> None:
    """Cache optional API context metadata without assuming it is present."""
    global _CITY_CONTEXT_METADATA
    if not isinstance(contexts, dict):
        _CITY_CONTEXT_METADATA = {}
        return
    metadata: dict[str, dict[str, object]] = {}
    for key in ("city_metadata", "city_contexts", "region_plan_cities"):
        value = contexts.get(key)
        entries = value.items() if isinstance(value, dict) else (("", item) for item in value) if isinstance(value, list) else []
        for city_key, entry in entries:
            if not isinstance(entry, dict):
                continue
            city_name = _clean_text(entry.get("strategic_city_name") or entry.get("city_name") or entry.get("city_key") or city_key)
            if city_name:
                metadata[city_name] = dict(entry)
    _CITY_CONTEXT_METADATA = metadata


def _context_city_value(strategic_city_name: str, *keys: str) -> str:
    metadata = _CITY_CONTEXT_METADATA.get(_clean_text(strategic_city_name), {})
    for key in keys:
        value = _clean_text(metadata.get(key))
        if value:
            return value
    return ""


def _context_base_city_name(strategic_city_name: str) -> str:
    requested_city = _clean_text(strategic_city_name)
    return _context_city_value(
        requested_city,
        "source_strategic_city_name",
        "base_city_name",
        "geometry_city_name",
    ) or STRATEGIC_CITY_BASE_ALIASES.get(requested_city, requested_city)


def _context_source_city_name(strategic_city_name: str) -> str:
    """Return the API-declared operational source for an active plan context."""
    requested_city = _clean_text(strategic_city_name)
    return _context_city_value(
        requested_city,
        "source_strategic_city_name",
        "operational_source_city_name",
    ) or requested_city


def _resolve_city_osrm_url(strategic_city_name: str) -> str:
    cfg = _load_common_client_config()
    routing_seed = cfg.get("routing_seed", {}) if isinstance(cfg.get("routing_seed", {}), dict) else {}
    city_urls = routing_seed.get("city_osrm_urls", {}) if isinstance(routing_seed.get("city_osrm_urls", {}), dict) else {}
    requested_city = _clean_text(strategic_city_name)
    city_url = _context_city_value(requested_city, "osrm_url", "osrm_base_url")
    base_city = _context_base_city_name(requested_city)
    city_url = city_url or str(city_urls.get(base_city, "")).strip()
    if city_url:
        return city_url
    return ""


@st.cache_resource(show_spinner=False)
def get_route_client(strategic_city_name: str) -> OSRMTripClient:
    osrm_url = _resolve_city_osrm_url(strategic_city_name)
    if not osrm_url:
        raise ValueError(f"No OSRM URL configured for {strategic_city_name}")
    city_key = str(strategic_city_name).replace(",", "").replace(" ", "_")
    return OSRMTripClient(
        OSRMConfig(
            osrm_url=osrm_url,
            mode="osrm",
            osrm_profile="driving",
            cache_file=Path(f"data/cache/osrm_trip_cache_common_vrp_client_{city_key}.csv"),
            fallback_osrm_url=None,
        )
    )


def _popup(content: str, width: int = 360) -> folium.Popup:
    wrapped = (
        f"<div style='min-width:{width}px;max-width:{width}px;white-space:normal;"
        "line-height:1.4;font-size:13px;'>"
        f"{content}</div>"
    )
    return folium.Popup(wrapped, max_width=width + 40)


def _generate_color_map(labels: list[str]) -> dict[str, str]:
    color_map: dict[str, str] = {}
    hue = 0.11
    golden_ratio = 0.618033988749895
    for label in sorted({str(v).strip() for v in labels if str(v).strip()}):
        hue = (hue + golden_ratio) % 1.0
        rgb = colorsys.hsv_to_rgb(hue, 0.68, 0.92)
        color_map[label] = "#{:02x}{:02x}{:02x}".format(int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
    return color_map


def _region_color_map(labels: list[str] | None = None) -> dict[str, str]:
    if labels:
        return _generate_color_map(labels)
    return {
        "Atlanta New Region 1": "#db4437",
        "Atlanta New Region 2": "#0f9d58",
        "Atlanta New Region 3": "#4285f4",
    }


def _area_type_color(area_type: object, fallback: str) -> str:
    text = _clean_text(area_type).upper()
    if text == "DMS_CORE":
        return "#16a34a"
    if text == "OVERLAP":
        return "#f59e0b"
    if text == "DMS2_EXCLUSIVE":
        return "#94a3b8"
    return fallback


def _should_color_area_by_region(strategic_city_name: object) -> bool:
    return "BUCKET SIM DRAFT" in _clean_text(strategic_city_name).upper()


def _default_city_center(strategic_city_name: str) -> tuple[float, float]:
    city_text = str(strategic_city_name or "").strip().lower()
    if "los angeles" in city_text or city_text == "la":
        return 34.0522, -118.2437
    if "atlanta" in city_text:
        return 33.7490, -84.3880
    return 39.8283, -98.5795


def _geometry_city_name(strategic_city_name: str) -> str:
    city_name = _context_base_city_name(strategic_city_name)
    if city_name.startswith("Los Angeles, CA - "):
        return "Los Angeles, CA"
    return city_name


def _profile_city_name(strategic_city_name: str) -> str:
    return _context_city_value(strategic_city_name, "profile_city_name") or _geometry_city_name(strategic_city_name)


def _marker_border_style(center_type: object) -> tuple[str, str]:
    bucket = str(center_type or "").strip().upper()
    if bucket == "ASC":
        return "3px", "#111111"
    if bucket == "DMS2":
        return "3px", "#dc2626"
    return "2px", "#ffffff"


def _marker_icon_size(center_type: object) -> tuple[int, int]:
    bucket = str(center_type or "").strip().upper()
    if bucket == "DMS2":
        return 42, 26
    if bucket == "ASC":
        return 34, 26
    return 24, 24


def _marker_icon_label(center_type: object, seq_label: str) -> str:
    bucket = str(center_type or "").strip().upper()
    if bucket in {"ASC", "DMS2"}:
        return bucket
    return seq_label


def _center_type_bucket(center_type: object) -> str:
    bucket = str(center_type or "").strip().upper()
    if bucket == "DMS2":
        return "DMS2"
    if bucket == "ASC":
        return "ASC"
    if bucket == "DMS":
        return "DMS"
    return "UNKNOWN"


def _build_region_layers(strategic_city_name: str, region_zip_df: pd.DataFrame, service_df: pd.DataFrame):
    required_region_cols = ["POSTAL_CODE", "region_seq", "new_region_name"]
    if region_zip_df.empty or not set(required_region_cols).issubset(region_zip_df.columns):
        coverage_df = pd.DataFrame(columns=required_region_cols + ["area_type"])
    else:
        coverage_cols = required_region_cols + (["area_type"] if "area_type" in region_zip_df.columns else [])
        coverage_df = region_zip_df[coverage_cols].drop_duplicates().copy()
    if "area_type" not in coverage_df.columns:
        coverage_df["area_type"] = ""
    coverage_df["POSTAL_CODE"] = coverage_df["POSTAL_CODE"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
    coverage_df["area_type"] = coverage_df["area_type"].map(_clean_text)
    coverage_df["region_seq"] = pd.to_numeric(coverage_df["region_seq"], errors="coerce")
    coverage_df = coverage_df[coverage_df["POSTAL_CODE"].ne("") & coverage_df["region_seq"].notna()].copy()
    if not coverage_df.empty:
        coverage_df["region_seq"] = coverage_df["region_seq"].astype(int)
    city_data = load_city_map_data(_geometry_city_name(strategic_city_name), config_section="area_map_usa")
    # Region coverage is authoritative here. The profile/service-derived
    # city_data.zip_layer can omit fixed-region ZIPs with no current service.
    zip_layer = load_zcta_geometry(
        coverage_df["POSTAL_CODE"].tolist(),
        config_section="area_map_usa",
    )
    if zip_layer.empty:
        zip_layer = city_data.zip_layer.copy()
    zip_layer["POSTAL_CODE"] = zip_layer["POSTAL_CODE"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.zfill(5)
    merged = zip_layer.merge(coverage_df, on="POSTAL_CODE", how="inner")
    if merged.empty:
        coverage_zip_set = set(coverage_df["POSTAL_CODE"].astype(str).tolist()) if "POSTAL_CODE" in coverage_df.columns else set()
        layer_zip_set = set(zip_layer["POSTAL_CODE"].astype(str).tolist()) if "POSTAL_CODE" in zip_layer.columns else set()
        merged.attrs["area_debug"] = {
            "region_zip_count": len(coverage_zip_set),
            "geometry_zip_count": len(layer_zip_set),
            "intersection_count": len(coverage_zip_set & layer_zip_set),
            "region_zip_sample": sorted(coverage_zip_set)[:10],
            "geometry_zip_sample": sorted(layer_zip_set)[:10],
        }
    merged["AREA_NAME"] = merged["new_region_name"]
    if service_df.empty or "POSTAL_CODE" not in service_df.columns:
        postal_counts = pd.Series(dtype=int)
    else:
        postal_counts = service_df["POSTAL_CODE"].astype(str).str.zfill(5).value_counts()
    merged["service_count"] = merged["POSTAL_CODE"].map(postal_counts).fillna(0).astype(int)
    region_layer = (
        merged.dropna(subset=["new_region_name"])
        .dissolve(by="new_region_name", as_index=False, aggfunc="first")[["new_region_name", "region_seq", "area_type", "geometry"]]
        .sort_values("region_seq")
        .reset_index(drop=True)
    )
    region_layer["AREA_NAME"] = region_layer["new_region_name"]
    return merged, region_layer


def _render_folium_map(map_obj: folium.Map, height: int = 760) -> None:
    st.iframe(map_obj.get_root().render(), height=height)


def _extract_polygon_geometry(geojson_text: str) -> dict:
    payload = json.loads(str(geojson_text or "").strip())
    if payload.get("type") == "FeatureCollection":
        features = payload.get("features", [])
        for feature in features:
            geometry = feature.get("geometry") if isinstance(feature, dict) else None
            if isinstance(geometry, dict) and geometry.get("type") in {"Polygon", "MultiPolygon"}:
                return geometry
    if payload.get("type") == "Feature":
        geometry = payload.get("geometry")
        if isinstance(geometry, dict) and geometry.get("type") in {"Polygon", "MultiPolygon"}:
            return geometry
    if payload.get("type") in {"Polygon", "MultiPolygon"}:
        return payload
    raise ValueError("GeoJSON must contain a Polygon or MultiPolygon.")


def _load_avoid_areas(subsidiary_name: str, strategic_city_name: str, active_only: bool = False) -> pd.DataFrame:
    try:
        response = _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/avoid-areas",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
            active_only="true" if active_only else "false",
        )
        return pd.DataFrame(response.get("rows", []))
    except Exception as exc:
        st.warning(f"Failed to load avoid areas: {exc}")
        return pd.DataFrame()


def _build_avoid_area_map(strategic_city_name: str, avoid_area_df: pd.DataFrame) -> folium.Map:
    center_lat, center_lon = (34.0522, -118.2437) if "Los Angeles" in str(strategic_city_name) else (33.7490, -84.3880)
    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles="CartoDB positron")
    if not avoid_area_df.empty:
        for _, row in avoid_area_df.iterrows():
            try:
                geometry = json.loads(str(row.get("geometry_json", "")).strip())
            except Exception:
                continue
            active = _coerce_bool_value(row.get("active_flag", True))
            folium.GeoJson(
                data={"type": "Feature", "properties": {"name": row.get("area_name", "")}, "geometry": geometry},
                name=str(row.get("area_name", "Avoid Area")),
                style_function=lambda _feature, active=active: {
                    "color": "#dc2626" if active else "#6b7280",
                    "weight": 2,
                    "fillColor": "#f97316" if active else "#9ca3af",
                    "fillOpacity": 0.28 if active else 0.12,
                },
                tooltip=str(row.get("area_name", "Avoid Area")),
            ).add_to(fmap)
    Draw(
        export=True,
        filename="avoid_area.geojson",
        draw_options={
            "polyline": False,
            "rectangle": True,
            "circle": False,
            "circlemarker": False,
            "marker": False,
            "polygon": True,
        },
        edit_options={"edit": True, "remove": True},
    ).add_to(fmap)
    folium.LayerControl(collapsed=False).add_to(fmap)
    return fmap


def _capture_drawn_polygon_from_map(map_obj: folium.Map, map_key: str, height: int = 520) -> dict | None:
    if st_folium is None:
        _render_folium_map(map_obj, height=height)
        st.caption("Install streamlit-folium to auto-fill the Polygon GeoJSON field from map drawings.")
        return None

    map_state = st_folium(
        map_obj,
        height=height,
        width=None,
        returned_objects=["last_active_drawing", "all_drawings"],
        key=map_key,
    )
    if not isinstance(map_state, dict):
        return None
    drawing = map_state.get("last_active_drawing")
    if not isinstance(drawing, dict):
        drawings = map_state.get("all_drawings") or []
        drawing = drawings[-1] if drawings else None
    if not isinstance(drawing, dict):
        return None
    geometry = drawing.get("geometry") if drawing.get("type") == "Feature" else drawing
    if isinstance(geometry, dict) and geometry.get("type") in {"Polygon", "MultiPolygon"}:
        return geometry
    return None


def _render_avoid_area_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    st.caption("Draw a polygon on the map. The Polygon GeoJSON field is filled automatically when streamlit-folium is available.")
    avoid_area_df = _load_avoid_areas(subsidiary_name, strategic_city_name)
    geojson_key = f"avoid_area_geojson::{subsidiary_name}::{strategic_city_name}"
    drawn_geometry = _capture_drawn_polygon_from_map(
        _build_avoid_area_map(strategic_city_name, avoid_area_df),
        map_key=f"avoid_area_map::{subsidiary_name}::{strategic_city_name}",
        height=520,
    )
    if drawn_geometry:
        drawn_text = json.dumps(drawn_geometry, ensure_ascii=False)
        if st.session_state.get(geojson_key) != drawn_text:
            st.session_state[geojson_key] = drawn_text
            st.rerun()

    with st.form("avoid_area_form"):
        area_name = st.text_input("Area Name", value="Road Closure Area")
        description = st.text_area("Description", height=80)
        active_flag = st.checkbox("Active", value=True)
        geojson_text = st.text_area(
            "Polygon GeoJSON",
            height=160,
            placeholder='{"type":"Polygon","coordinates":[...]}',
            key=geojson_key,
        )
        submitted = st.form_submit_button("Save Avoid Area", width="stretch")
    if submitted:
        try:
            geometry = _extract_polygon_geometry(geojson_text)
            _api_post(
                DEFAULT_COMMON_SERVER_URL,
                "/api/v1/common/avoid-areas/upsert",
                {
                    "avoid_area_id": uuid.uuid4().hex,
                    "subsidiary_name": subsidiary_name,
                    "strategic_city_name": strategic_city_name,
                    "area_name": area_name,
                    "description": description,
                    "geometry": geometry,
                    "active_flag": active_flag,
                },
            )
            st.success("Avoid area saved.")
            st.rerun()
        except Exception as exc:
            st.error(f"Failed to save avoid area: {exc}")

    if avoid_area_df.empty:
        st.info("No avoid areas saved yet.")
        return
    display_df = avoid_area_df[["avoid_area_id", "area_name", "description", "active_flag", "updated_at"]].copy()
    st.dataframe(display_df, width="stretch", hide_index=True)
    for _, row in avoid_area_df.iterrows():
        cols = st.columns([3, 1, 1])
        cols[0].caption(f"{row.get('area_name', '')} | {row.get('avoid_area_id', '')}")
        toggle_label = "Deactivate" if _coerce_bool_value(row.get("active_flag", True)) else "Activate"
        if cols[1].button(toggle_label, key=f"avoid_toggle_{row.get('avoid_area_id')}", width="stretch"):
            payload = row.to_dict()
            payload["active_flag"] = not _coerce_bool_value(row.get("active_flag", True))
            _api_post(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/avoid-areas/upsert", payload)
            st.rerun()
        if cols[2].button("Delete", key=f"avoid_delete_{row.get('avoid_area_id')}", width="stretch"):
            _api_post(
                DEFAULT_COMMON_SERVER_URL,
                "/api/v1/common/avoid-areas/delete",
                {
                    "subsidiary_name": subsidiary_name,
                    "strategic_city_name": strategic_city_name,
                    "avoid_area_id": str(row.get("avoid_area_id", "")).strip(),
                },
            )
            st.rerun()


def _build_route_groups(schedule_df: pd.DataFrame, strategic_city_name: str):
    route_groups: list[dict] = []
    if schedule_df.empty:
        return route_groups
    for engineer_code, group in schedule_df.groupby("assigned_sm_code", dropna=True):
        group = group.sort_values("visit_seq").reset_index(drop=True)
        start_coord = None
        if pd.notna(group.iloc[0].get("home_start_longitude")) and pd.notna(group.iloc[0].get("home_start_latitude")):
            start_coord = (float(group.iloc[0]["home_start_longitude"]), float(group.iloc[0]["home_start_latitude"]))
        stop_coords = [(float(row["longitude"]), float(row["latitude"])) for _, row in group.iterrows()]
        coord_chain = [start_coord] + stop_coords if start_coord is not None else stop_coords
        route_payload = get_route_client(strategic_city_name).build_route_in_order(tuple(coord_chain))
        route_groups.append(
            {
                "engineer_code": str(engineer_code),
                "engineer_name": str(group["assigned_sm_name"].iloc[0]),
                "center_type": str(group.get("assigned_center_type", pd.Series([""])).iloc[0]).strip().upper()
                if "assigned_center_type" in group.columns
                else "",
                "route_payload": route_payload,
                "scheduled_rows": group.to_dict("records"),
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "home_coord": start_coord,
            }
        )
    return route_groups


def _build_region_staffing_view(service_df: pd.DataFrame) -> pd.DataFrame:
    output_columns = ["area", "dms_count", "dms_service_count", "service_count"]
    required_cols = {"new_region_name", "assigned_sm_code", "assigned_center_type", "GSFS_RECEIPT_NO"}
    if service_df.empty or not required_cols.issubset(service_df.columns):
        return pd.DataFrame(columns=output_columns)
    staffing_df = service_df[["new_region_name", "assigned_sm_code", "assigned_center_type", "GSFS_RECEIPT_NO"]].dropna(
        subset=["new_region_name", "assigned_sm_code"]
    ).copy()
    staffing_df["assigned_center_type"] = staffing_df["assigned_center_type"].astype(str).str.upper()
    rows: list[dict[str, object]] = []
    for region_name, group in staffing_df.groupby("new_region_name", dropna=False):
        rows.append(
            {
                "area": str(region_name),
                "dms_count": int(group.loc[group["assigned_center_type"] == "DMS", "assigned_sm_code"].astype(str).nunique()),
                "dms_service_count": int(group.loc[group["assigned_center_type"] == "DMS", "GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
            }
        )
    if not rows:
        return pd.DataFrame(columns=output_columns)
    return pd.DataFrame(rows, columns=output_columns).sort_values("area").reset_index(drop=True)


def _estimate_service_time_min(row: pd.Series) -> float:
    is_heavy = _coerce_bool_value(row.get("is_heavy_repair", False))
    for col in ("service_time_min", "service_minutes"):
        if col in row.index:
            value = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
            if pd.notna(value) and float(value) > 0:
                return max(float(value), 100.0 if is_heavy else 45.0)
    if "job_slot_count" in row.index or "two_slot_job" in row.index or "2slot_job" in row.index:
        job_slot_count = _coerce_job_slot_count_value(row.get("job_slot_count", 2 if _coerce_bool_value(row.get("two_slot_job", row.get("2slot_job", False))) else 1))
        slot_minutes = 50.0 * job_slot_count if job_slot_count >= 2 else 45.0
        return max(slot_minutes, 100.0 if is_heavy else 45.0)
    return 100.0 if is_heavy else 45.0


def build_map(
    strategic_city_name: str,
    region_name: str,
    display_service_df: pd.DataFrame,
    home_df: pd.DataFrame,
    route_groups: list[dict],
    region_zip_df: pd.DataFrame,
    unassigned_df: pd.DataFrame | None = None,
):
    zip_layer, region_layer = _build_region_layers(strategic_city_name, region_zip_df, display_service_df)
    engineer_colors = _generate_color_map([group["engineer_code"] for group in route_groups])
    unassigned_df = unassigned_df.copy() if unassigned_df is not None else pd.DataFrame()
    if zip_layer.empty:
        debug = zip_layer.attrs.get("area_debug", {}) if hasattr(zip_layer, "attrs") else {}
        st.warning(
            "Area layer is empty. Check common_region_master rows and ZIP geometry match "
            f"for {strategic_city_name}. region_zip_rows={len(region_zip_df)} "
            f"debug={debug}"
        )

    if region_name != "ALL":
        zip_layer = zip_layer[zip_layer["new_region_name"] == region_name].copy()
        region_layer = region_layer[region_layer["new_region_name"] == region_name].copy()
        display_service_df = display_service_df[display_service_df["new_region_name"] == region_name].copy()
        home_df = home_df[home_df["assigned_region_name"] == region_name].copy()
        if not unassigned_df.empty and "new_region_name" in unassigned_df.columns:
            unassigned_df = unassigned_df[unassigned_df["new_region_name"] == region_name].copy()

    area_labels = []
    if not zip_layer.empty and "AREA_NAME" in zip_layer.columns:
        area_labels.extend(zip_layer["AREA_NAME"].dropna().astype(str).tolist())
    if not display_service_df.empty and "new_region_name" in display_service_df.columns:
        area_labels.extend(display_service_df["new_region_name"].dropna().astype(str).tolist())
    region_colors = _region_color_map(area_labels)
    color_area_by_region = _should_color_area_by_region(strategic_city_name)

    if not display_service_df.empty and {"latitude", "longitude"}.issubset(display_service_df.columns):
        service_lat = pd.to_numeric(display_service_df["latitude"], errors="coerce")
        service_lon = pd.to_numeric(display_service_df["longitude"], errors="coerce")
        if service_lat.notna().any() and service_lon.notna().any():
            center_lat = float(service_lat.mean())
            center_lon = float(service_lon.mean())
        elif not region_layer.empty:
            center_points = region_layer.to_crs(epsg=3857).geometry.centroid.to_crs(epsg=4326)
            center_lat = float(center_points.y.mean())
            center_lon = float(center_points.x.mean())
        else:
            center_lat, center_lon = _default_city_center(strategic_city_name)
    elif not region_layer.empty:
        center_points = region_layer.to_crs(epsg=3857).geometry.centroid.to_crs(epsg=4326)
        center_lat = float(center_points.y.mean())
        center_lon = float(center_points.x.mean())
    else:
        center_lat, center_lon = _default_city_center(strategic_city_name)

    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=9, tiles="CartoDB positron")
    if not zip_layer.empty:
        folium.GeoJson(
            data=zip_layer.to_json(),
            name="ZIP Coverage",
            style_function=lambda feature: {
                "color": "transparent",
                "weight": 0,
                "fillColor": "#eceff3" if int(feature["properties"].get("service_count", 0) or 0) == 0 else (
                    region_colors.get(feature["properties"].get("AREA_NAME", ""), "#dddddd")
                    if color_area_by_region
                    else _area_type_color(
                        feature["properties"].get("area_type", ""),
                        region_colors.get(feature["properties"].get("AREA_NAME", ""), "#dddddd"),
                    )
                ),
                "fillOpacity": 0.08 if int(feature["properties"].get("service_count", 0) or 0) == 0 else 0.22,
            },
            tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "AREA_NAME", "area_type", "service_count"], aliases=["ZIP", "Area", "Area Type", "Service Count"]),
        ).add_to(fmap)
    if not region_layer.empty:
        folium.GeoJson(
            data=region_layer.to_json(),
            name="Area",
            style_function=lambda feature: {
                "fillColor": (
                    region_colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e")
                    if color_area_by_region
                    else _area_type_color(
                        feature["properties"].get("area_type", ""),
                        region_colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e"),
                    )
                ),
                "color": (
                    region_colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e")
                    if color_area_by_region
                    else _area_type_color(
                        feature["properties"].get("area_type", ""),
                        region_colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e"),
                    )
                ),
                "weight": 2,
                "fillOpacity": 0.12,
            },
            highlight_function=lambda feature: {
                "fillColor": (
                    region_colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e")
                    if color_area_by_region
                    else _area_type_color(
                        feature["properties"].get("area_type", ""),
                        region_colors.get(feature["properties"].get("AREA_NAME", ""), "#0f766e"),
                    )
                ),
                "color": "#111111",
                "weight": 3,
                "fillOpacity": 0.22,
            },
            tooltip=folium.GeoJsonTooltip(fields=["AREA_NAME", "area_type"], aliases=["Area", "Area Type"]),
        ).add_to(fmap)

    if route_groups:
        route_line_layers: dict[str, folium.FeatureGroup] = {}
        route_marker_layers: dict[str, folium.FeatureGroup] = {}

        def route_line_layer(bucket: str) -> folium.FeatureGroup:
            if bucket not in route_line_layers:
                route_line_layers[bucket] = folium.FeatureGroup(name=f"{bucket} Routes", show=True)
            return route_line_layers[bucket]

        def route_marker_layer(bucket: str) -> folium.FeatureGroup:
            if bucket not in route_marker_layers:
                route_marker_layers[bucket] = folium.FeatureGroup(name=f"{bucket} Markers", show=True)
            return route_marker_layers[bucket]

        for group in route_groups:
            engineer_color = engineer_colors.get(group["engineer_code"], "#111827")
            group_center_type = str(group.get("center_type", "")).upper()
            group_bucket = _center_type_bucket(group_center_type)
            geometry = group["route_payload"]["geometry"]
            if geometry:
                folium.PolyLine(
                    locations=geometry,
                    color=engineer_color,
                    weight=3,
                    opacity=0.85,
                    popup=_popup(
                        f"<b>Technician</b>: {group['engineer_name']}<br>"
                        f"<b>Technician Code</b>: {group['engineer_code']}<br>"
                        f"<b>Service Count</b>: {group['service_count']} | "
                        f"<b>Distance</b>: {group['route_payload']['distance_km']:.2f} km | "
                        f"<b>Duration</b>: {group['route_payload']['duration_min']:.2f} min",
                        width=420,
                    ),
                ).add_to(route_line_layer(group_bucket))
            if group["home_coord"] is not None:
                home_lon, home_lat = group["home_coord"]
                folium.Marker(
                    location=[home_lat, home_lon],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:10px;font-weight:700;color:{engineer_color};"
                            f"background:#ffffff;border:2px solid {engineer_color};border-radius:12px;"
                            "padding:2px 6px;text-align:center;white-space:nowrap;\">Home</div>"
                        )
                    ),
                    popup=_popup(f"<b>Home Start</b>: {group['engineer_name']}<br><b>Technician Code</b>: {group['engineer_code']}", width=280),
                ).add_to(route_marker_layer(group_bucket))
            for row in group["scheduled_rows"]:
                seq = int(row.get("visit_seq", 0))
                center_type = str(row.get("assigned_center_type", "")).strip().upper()
                marker_bucket = _center_type_bucket(center_type or group_center_type)
                border_width, border_color = _marker_border_style(center_type)
                icon_width, icon_height = _marker_icon_size(center_type)
                icon_label = _marker_icon_label(center_type, str(seq))
                fixed_marker = any(
                    _coerce_bool_value(row.get(col, False))
                    for col in ("fixed", "fixed_x", "fixed_y")
                )
                if fixed_marker and border_color.lower() == "#ffffff":
                    border_color = "#111827"
                changed_text = ""
                if "changed" in row:
                    changed_text = f"<b>Changed</b>: {'Y' if bool(row.get('changed', False)) else 'N'}<br>"
                heavy_text = "Y" if _coerce_bool_value(row.get("is_heavy_repair", False)) else "N"
                slot_job_count = _coerce_job_slot_count_value(row.get("job_slot_count", 1))
                home_region_text = _popup_text(row.get("assigned_region_name", ""))
                home_region_line = f"<b>Home Area</b>: {home_region_text}<br>" if home_region_text else ""
                popup_html = (
                    f"<b>Technician</b>: {_popup_text(row.get('assigned_sm_name', ''))}<br>"
                    f"<b>Technician Code</b>: {_popup_text(row.get('assigned_sm_code', ''))} | "
                    f"<b>Center Type</b>: {center_type} | "
                    f"<b>Receipt</b>: {_popup_text(row.get('GSFS_RECEIPT_NO', ''))} | "
                    f"<b>Seq</b>: {seq}<br>"
                    f"{changed_text}"
                    f"{home_region_line}"
                    f"<b>Product Group</b>: {_popup_text(row.get('SERVICE_PRODUCT_GROUP_CODE', ''))}<br>"
                    f"<b>Heavy</b>: {heavy_text} | "
                    f"<b>Slot Job</b>: {slot_job_count}<br>"
                    f"<b>Start</b>: {_popup_text(row.get('visit_start_time', ''))} | "
                    f"<b>End</b>: {_popup_text(row.get('visit_end_time', ''))}"
                )
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"background:{engineer_color};color:#fff;border:{border_width} solid {border_color};"
                            f"border-radius:999px;width:{icon_width}px;height:{icon_height}px;line-height:{icon_height - 4}px;text-align:center;"
                            f"font-size:11px;font-weight:700;box-shadow:0 1px 6px rgba(0,0,0,0.35);\">{icon_label}</div>"
                        ),
                        icon_size=(icon_width, icon_height),
                        icon_anchor=(icon_width // 2, icon_height // 2),
                    ),
                    popup=_popup(popup_html, width=460),
                ).add_to(route_marker_layer(marker_bucket))

        for bucket in ["DMS", "DMS2", "ASC", "UNKNOWN"]:
            if bucket in route_line_layers:
                route_line_layers[bucket].add_to(fmap)
            if bucket in route_marker_layers:
                route_marker_layers[bucket].add_to(fmap)

    if not unassigned_df.empty:
        unassigned_layer = folium.FeatureGroup(name="Unassigned Jobs", show=True).add_to(fmap)
        for _, row in unassigned_df.iterrows():
            if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
                continue
            popup_html = (
                f"<b>Unassigned</b><br>"
                f"<b>Receipt</b>: {_popup_text(row.get('receipt_no', ''))}<br>"
                f"<b>Reason</b>: {_popup_text(row.get('reason', ''))}<br>"
                f"<b>Address</b>: {_popup_text(row.get('city_name', ''))} "
                f"{_popup_text(row.get('postal_code', ''))}<br>"
                f"{_popup_text(row.get('address_line1_info', ''))}<br>"
                f"<b>Product</b>: {_popup_text(row.get('service_product_group_code', ''))} / "
                f"{_popup_text(row.get('service_product_code', ''))} | "
                f"<b>Slot</b>: {_popup_text(row.get('job_slot_count', ''))}"
            )
            folium.Marker(
                location=[float(row["latitude"]), float(row["longitude"])],
                icon=folium.DivIcon(
                    html=(
                        "<div style=\"font-size:13px;font-weight:900;color:#ffffff;"
                        "background:#dc2626;border:4px solid #7f1d1d;border-radius:14px;"
                        "width:28px;height:28px;line-height:20px;text-align:center;"
                        "box-shadow:0 0 0 2px #ffffff;\">!</div>"
                    )
                ),
                popup=_popup(popup_html, width=460),
            ).add_to(unassigned_layer)

    home_group = folium.FeatureGroup(name="Technician Homes").add_to(fmap)
    for _, row in home_df.iterrows():
        if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
            continue
        code = str(row.get("SVC_ENGINEER_CODE", ""))
        border_color = engineer_colors.get(code, "#444444")
        assigned_region_text = _popup_text(row.get("assigned_region_name", ""))
        assigned_region_line = f"<br><b>Assigned Area</b>: {assigned_region_text}" if assigned_region_text else ""
        folium.Marker(
            location=[float(row["latitude"]), float(row["longitude"])],
            icon=folium.DivIcon(
                html=(
                    f"<div style=\"font-size:10px;font-weight:700;color:{border_color};"
                    f"background:#fff;border:2px solid {border_color};border-radius:12px;"
                    "padding:2px 6px;text-align:center;white-space:nowrap;\">Home</div>"
                )
            ),
            popup=_popup(
                f"<b>Technician</b>: {_popup_text(row.get('Name', ''))}<br>"
                f"<b>Technician Code</b>: {_popup_text(row.get('SVC_ENGINEER_CODE', ''))}"
                f"{assigned_region_line}",
                width=440,
            ),
        ).add_to(home_group)

    folium.LayerControl(collapsed=False).add_to(fmap)
    return fmap


def _build_engineer_options(assignment_df: pd.DataFrame) -> tuple[list[str], dict[str, str]]:
    if assignment_df.empty:
        return ["ALL"], {}
    engineer_df = assignment_df[["assigned_sm_code", "assigned_sm_name"]].drop_duplicates().copy()
    engineer_df["assigned_sm_code"] = engineer_df["assigned_sm_code"].astype(str).str.strip()
    engineer_df["assigned_sm_name"] = engineer_df["assigned_sm_name"].astype(str).str.strip()
    name_counts = engineer_df["assigned_sm_name"].value_counts()
    labels = ["ALL"]
    label_to_code: dict[str, str] = {}
    for _, row in engineer_df.sort_values(["assigned_sm_name", "assigned_sm_code"]).iterrows():
        code = str(row["assigned_sm_code"])
        name = str(row["assigned_sm_name"])
        label = name if int(name_counts.get(name, 0)) <= 1 else f"{name} ({code})"
        labels.append(label)
        label_to_code[label] = code
    return labels, label_to_code


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "routing_result") -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()


def _to_multi_sheet_xlsx_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    """Build one workbook containing the period's source and result tables."""

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        if not sheets:
            pd.DataFrame().to_excel(writer, index=False, sheet_name="Statistics")
        for sheet_name, frame in sheets.items():
            safe_name = str(sheet_name).strip()[:31] or "Sheet1"
            output = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
            output.to_excel(writer, index=False, sheet_name=safe_name)
    return buffer.getvalue()


def _build_simple_assignment_export_df(
    schedule_df: pd.DataFrame,
    unassigned_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if not schedule_df.empty:
        for _, row in schedule_df.iterrows():
            rows.append(
                {
                    "promise_date": str(row.get("promise_date", row.get("service_date_key", ""))).strip(),
                    "serviceReceiptNo": str(row.get("GSFS_RECEIPT_NO", "")).strip(),
                    "serviceEngineerCode": str(row.get("assigned_sm_code", "")).strip(),
                    "changeFlag": "Y" if _coerce_bool_value(row.get("changed", False)) else "N",
                }
            )
    return pd.DataFrame(rows, columns=["promise_date", "serviceReceiptNo", "serviceEngineerCode", "changeFlag"])


def _routing_status_progress(status_value: str) -> tuple[float, str]:
    status = str(status_value or "").strip().lower()
    if status == "queued":
        return 0.2, "Routing request queued."
    if status == "running":
        return 0.6, "Smart Routing is running."
    if status == "completed":
        return 1.0, "Smart Routing completed."
    if status == "failed":
        return 1.0, "Smart Routing failed."
    return 0.0, "Routing request not submitted."

def _http_json(method: str, url: str, payload: dict | None = None, timeout_sec: int = 60) -> dict:
    data = None
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib_request.Request(url=url, method=method.upper(), data=data, headers=headers)
    try:
        with urllib_request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
            message = parsed.get("message") or parsed.get("error") or body
        except Exception:
            message = body or str(exc)
        raise RuntimeError(f"HTTP {exc.code}: {message}") from exc


def _api_get(server_url: str, path: str, **params: str) -> dict:
    query = parse.urlencode({k: v for k, v in params.items() if str(v).strip() != ""})
    url = f"{server_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    return _http_json("GET", url)


def _api_post(server_url: str, path: str, payload: dict) -> dict:
    return _http_json("POST", f"{server_url.rstrip('/')}{path}", payload=payload)


def _technician_draft_key(subsidiary_name: str, strategic_city_name: str, promise_date: str) -> str:
    return f"common_technician_draft::{subsidiary_name}::{strategic_city_name}::{promise_date}"


def _load_technician_draft(subsidiary_name: str, strategic_city_name: str, promise_date: str) -> pd.DataFrame:
    rows = st.session_state.get(_technician_draft_key(subsidiary_name, strategic_city_name, promise_date), [])
    return pd.DataFrame(rows)


def _save_technician_draft(subsidiary_name: str, strategic_city_name: str, promise_date: str, rows: list[dict]) -> None:
    st.session_state[_technician_draft_key(subsidiary_name, strategic_city_name, promise_date)] = rows


def _read_local_parquet(path: Path) -> pd.DataFrame:
    return pd.DataFrame()


def _write_local_parquet(path: Path, df: pd.DataFrame) -> None:
    return None


def _load_local_jobs(subsidiary_name: str, strategic_city_name: str) -> pd.DataFrame:
    rows = _api_get(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/jobs",
        subsidiary_name=subsidiary_name,
        strategic_city_name=strategic_city_name,
    ).get("rows", [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "fixed" not in df.columns:
        df["fixed"] = False
    df["fixed"] = _coerce_bool_series(df["fixed"])
    if "reschedule" not in df.columns:
        df["reschedule"] = False
    df["reschedule"] = _coerce_bool_series(df["reschedule"])
    if "job_slot_count" not in df.columns:
        df["job_slot_count"] = df["two_slot_job"].map(lambda value: 2 if _coerce_bool_value(value) else 1) if "two_slot_job" in df.columns else 1
    df["job_slot_count"] = _coerce_job_slot_count_series(df["job_slot_count"])
    for col in ["promise_date", "record_id", "gsfs_receipt_no"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def _save_local_jobs(subsidiary_name: str, strategic_city_name: str, new_rows_df: pd.DataFrame) -> None:
    working_df = new_rows_df.copy()
    if working_df.empty:
        return
    if "fixed" not in working_df.columns:
        working_df["fixed"] = False
    working_df["fixed"] = _coerce_bool_series(working_df["fixed"])
    if "reschedule" not in working_df.columns:
        working_df["reschedule"] = False
    working_df["reschedule"] = _coerce_bool_series(working_df["reschedule"])
    if "job_slot_count" not in working_df.columns:
        working_df["job_slot_count"] = working_df["two_slot_job"].map(lambda value: 2 if _coerce_bool_value(value) else 1) if "two_slot_job" in working_df.columns else 1
    working_df["job_slot_count"] = _coerce_job_slot_count_series(working_df["job_slot_count"])
    if "record_id" not in working_df.columns:
        working_df["record_id"] = [uuid.uuid4().hex for _ in range(len(working_df))]
    _api_post(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/jobs/bulk_upsert", {"rows": working_df.to_dict("records")})


def _delete_local_job(subsidiary_name: str, strategic_city_name: str, record_id: str) -> None:
    _api_post(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/jobs/delete",
        {
            "subsidiary_name": subsidiary_name,
            "strategic_city_name": strategic_city_name,
            "record_id": str(record_id),
        },
    )


def _load_local_technicians(subsidiary_name: str, strategic_city_name: str, promise_date: str) -> pd.DataFrame:
    rows = _api_get(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/technicians",
        subsidiary_name=subsidiary_name,
        strategic_city_name=strategic_city_name,
        promise_date=str(promise_date),
    ).get("rows", [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "available" in df.columns:
        df["available"] = _coerce_bool_series(df["available"])
    for col in ["promise_date", "record_id", "employee_code"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def _save_local_technicians(subsidiary_name: str, strategic_city_name: str, promise_date: str, rows_df: pd.DataFrame) -> None:
    working_df = rows_df.copy()
    if working_df.empty:
        return
    if "available" in working_df.columns:
        working_df["available"] = _coerce_bool_series(working_df["available"])
    _api_post(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/technicians/replace",
        {
            "subsidiary_name": subsidiary_name,
            "strategic_city_name": strategic_city_name,
            "promise_date": str(promise_date),
            "rows": working_df.to_dict("records"),
        },
    )


@st.cache_data(show_spinner=False)
def _load_master_df(master_path: str) -> pd.DataFrame:
    df = pd.read_excel(master_path)
    required_cols = [
        "Product Group Name",
        "Product Group Code",
        "Product Name",
        "Product Code",
        "Symptom Name",
        "Symptom Code",
        "Symtom Type Name",
        "Symtom Type Code",
        "Detailed Symptom Name",
        "Detailed Symptom Code",
    ]
    df = df[required_cols].dropna(subset=["Product Group Code", "Product Code", "Detailed Symptom Code"]).copy()
    for col in required_cols:
        df[col] = df[col].astype(str).str.strip()
    return df.drop_duplicates().reset_index(drop=True)


def _normalize_promise_date(value: str) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, float)) and pd.notna(value):
        try:
            if float(value).is_integer():
                text = str(int(value))
            else:
                text = str(value).strip()
        except Exception:
            text = str(value).strip()
    else:
        text = str(value).strip()
    text = re.sub(r"\.0+$", "", text)
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 8:
        parsed = pd.to_datetime(digits, format="%Y%m%d", errors="coerce")
        return digits if pd.notna(parsed) else ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return pd.Timestamp(parsed).strftime("%Y%m%d")
    return ""


def _promise_date_to_service_date_key(value: str) -> str:
    normalized = _normalize_promise_date(value)
    if len(normalized) != 8:
        return str(value or "").strip()
    return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}"


def _read_uploaded_service_csv(uploaded_file) -> pd.DataFrame:
    uploaded_file.seek(0)
    raw_bytes = uploaded_file.read()
    last_error: Exception | None = None
    for encoding in ["utf-8-sig", "utf-16", "cp949"]:
        try:
            text = raw_bytes.decode(encoding)
        except Exception as exc:
            last_error = exc
            continue
        header = text.splitlines()[0] if text.splitlines() else ""
        delimiter = max([",", "\t", ";", "|"], key=lambda sep: header.count(sep))
        if header.count(delimiter) == 0:
            delimiter = None
        try:
            return pd.read_csv(
                io.StringIO(text),
                keep_default_na=False,
                sep=delimiter,
                engine="python",
            )
        except Exception as exc:
            last_error = exc
    raise ValueError(f"Failed to read uploaded CSV: {last_error}")


@st.cache_data(show_spinner=False)
def _load_profile_capability_df(profile_path: str = str(PROFILE_SOURCE_PATH)) -> pd.DataFrame:
    path = Path(profile_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing capability profile file: {path}")
    df = pd.read_excel(path, sheet_name="3. Product")
    required_cols = [
        "STRATEGIC_CITY_NAME",
        "SVC_ENGINEER_CODE",
        "SERVICE_PRODUCT_GROUP_CODE",
        "SERVICE_PRODUCT_CODE",
        "REPAIR_FLAG",
        "AREA_PRODUCT_FLAG",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing capability columns in profile: {', '.join(missing_cols)}")
    working = df[required_cols].copy()
    for col in required_cols:
        working[col] = working[col].fillna("").astype(str).str.strip()
    working["REPAIR_FLAG"] = working["REPAIR_FLAG"].str.upper()
    working["AREA_PRODUCT_FLAG"] = working["AREA_PRODUCT_FLAG"].str.upper()
    return working


@st.cache_data(show_spinner=False)
def _load_profile_home_address_df(profile_path: str = str(PROFILE_SOURCE_PATH)) -> pd.DataFrame:
    path = Path(profile_path)
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "employee_code",
                "employee_name",
                "center_type",
                "home_address",
                "home_city",
                "home_state",
                "home_country",
                "home_postal_code",
            ]
        )
    slot_df = pd.read_excel(path, sheet_name="2. Slot").rename(
        columns={
            "Name": "employee_name",
            "SVC_ENGINEER_CODE": "employee_code",
            "SVC_CENTER_TYPE": "center_type",
        }
    )
    address_df = pd.read_excel(path, sheet_name="4. Address").rename(
        columns={
            "SVC_ENGINEER_CODE": "employee_code",
            "Name": "employee_name",
            "Home Street Address": "home_address",
            "City ": "home_city",
            "State": "home_state",
            "Zip": "home_postal_code",
        }
    )
    working = (
        slot_df[["employee_code", "employee_name", "center_type"]]
        .merge(
            address_df[["employee_code", "employee_name", "home_address", "home_city", "home_state", "home_postal_code"]],
            on=["employee_code", "employee_name"],
            how="left",
        )
        .copy()
    )
    for col in ["employee_code", "employee_name", "center_type", "home_address", "home_city", "home_state", "home_postal_code"]:
        working[col] = working[col].map(_clean_text)
    working["home_country"] = "USA"
    working["home_postal_code"] = working["home_postal_code"].str.replace(r"\.0+$", "", regex=True)
    return working


def _build_technician_master_lookup(engineer_master_df: pd.DataFrame) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    source_df = engineer_master_df.copy()
    if source_df.empty:
        return lookup
    for _, row in source_df.iterrows():
        employee_code = _clean_text(row.get("employee_code"))
        if not employee_code:
            continue
        current = lookup.setdefault(employee_code, {})
        for key in [
            "employee_name",
            "center_type",
            "home_address",
            "home_city",
            "home_state",
            "home_country",
            "home_postal_code",
            "active_flag",
            "priority_group",
        ]:
            value = _clean_text(row.get(key))
            if value and not current.get(key):
                current[key] = value
    return lookup


def _normalize_technician_rows(
    raw_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    *,
    default_source: str,
) -> pd.DataFrame:
    base_columns = [
        "record_id",
        "subsidiary_name",
        "strategic_city_name",
        "promise_date",
        "employee_name",
        "employee_code",
        "center_type",
        "available",
        "shift_start",
        "shift_end",
        "slot_count",
        "priority_group",
        "preferred_region_name",
        "max_minutes",
        "start_location_type",
        "start_location_address",
        "source",
    ]
    if raw_df.empty:
        return pd.DataFrame(columns=base_columns)
    if "employee_code" not in raw_df.columns:
        raise ValueError("Technician input must include employee_code.")

    master_lookup = _build_technician_master_lookup(engineer_master_df)
    normalized_rows: list[dict[str, object]] = []
    seen_codes: set[str] = set()
    for _, row in raw_df.iterrows():
        employee_code = _clean_text(row.get("employee_code"))
        if not employee_code:
            continue
        if employee_code in seen_codes:
            raise ValueError(f"Duplicate employee_code in technician input: {employee_code}")
        seen_codes.add(employee_code)
        master_row = master_lookup.get(employee_code, {})
        start_location_type = _normalize_start_location_type(row.get("start_location_type"))
        start_location_address = _clean_text(row.get("start_location_address"))
        if start_location_type == "Home" and _clean_text(master_row.get("home_address")):
            start_location_address = _clean_text(master_row.get("home_address"))
        slot_count = pd.to_numeric(row.get("slot_count", 8), errors="coerce")
        priority_group = _coerce_priority_group_value(row.get("priority_group", master_row.get("priority_group", "B")))
        max_minutes = pd.to_numeric(row.get("max_minutes", 540), errors="coerce")
        active_flag = _coerce_bool_value(master_row.get("active_flag", True))
        available = _coerce_bool_value(row.get("available", active_flag)) and active_flag
        normalized_rows.append(
            {
                "record_id": _clean_text(row.get("record_id")) or uuid.uuid4().hex,
                "subsidiary_name": subsidiary_name,
                "strategic_city_name": strategic_city_name,
                "promise_date": str(promise_date),
                "employee_name": _clean_text(row.get("employee_name")) or _clean_text(master_row.get("employee_name")) or employee_code,
                "employee_code": employee_code,
                "center_type": (_clean_text(row.get("center_type")) or _clean_text(master_row.get("center_type")) or "DMS").upper(),
                "available": available,
                "shift_start": _clean_text(row.get("shift_start")) or "09:00",
                "shift_end": _clean_text(row.get("shift_end")) or "18:00",
                "slot_count": int(slot_count) if pd.notna(slot_count) and float(slot_count) >= 0 else 8,
                "priority_group": priority_group,
                "preferred_region_name": _clean_text(row.get("preferred_region_name", row.get("preferred_area_name", ""))),
                "max_minutes": int(max_minutes) if pd.notna(max_minutes) and float(max_minutes) > 0 else 540,
                "start_location_type": start_location_type,
                "start_location_address": start_location_address,
                "source": _clean_text(row.get("source")) or default_source,
            }
        )
    return pd.DataFrame(normalized_rows, columns=base_columns)


def _read_uploaded_technician_csv(uploaded_file) -> pd.DataFrame:
    uploaded_file.seek(0)
    raw_df = pd.read_csv(
        uploaded_file,
        encoding="utf-8-sig",
        keep_default_na=False,
        sep=None,
        engine="python",
    )
    alias_map = {
        "employee_name": "employee_name",
        "name": "employee_name",
        "employee_code": "employee_code",
        "svc_engineer_code": "employee_code",
        "engineer_code": "employee_code",
        "center_type": "center_type",
        "svc_center_type": "center_type",
        "service_center_type": "center_type",
        "available": "available",
        "active": "available",
        "active_flag": "available",
        "shift_start": "shift_start",
        "start_time": "shift_start",
        "shift_end": "shift_end",
        "end_time": "shift_end",
        "slot_count": "slot_count",
        "slots": "slot_count",
        "priority": "priority_group",
        "priority_group": "priority_group",
        "preferred_region_name": "preferred_region_name",
        "preferred_area_name": "preferred_region_name",
        "area_name": "preferred_region_name",
        "technician_priority": "priority_group",
        "group": "priority_group",
        "max_minutes": "max_minutes",
        "max_min": "max_minutes",
        "work_minutes": "max_minutes",
        "max_work_minutes": "max_minutes",
        "start_location_type": "start_location_type",
        "start_type": "start_location_type",
        "start_location_address": "start_location_address",
        "start_address": "start_location_address",
        "address": "start_location_address",
        "home_address": "start_location_address",
        "max_jobs": "max_jobs",
    }
    rename_map: dict[str, str] = {}
    for column in raw_df.columns:
        canonical = _canonicalize_column_name(column)
        if canonical in alias_map and alias_map[canonical] not in rename_map.values():
            rename_map[column] = alias_map[canonical]
    working = raw_df.rename(columns=rename_map).copy()
    if "employee_code" not in working.columns:
        raise ValueError("Uploaded technician CSV must include employee_code or SVC_ENGINEER_CODE.")
    working["shift_start"] = "09:00"
    working["shift_end"] = "18:00"
    working["start_location_type"] = "Home"
    working["start_location_address"] = ""
    return working


def _split_city_state(value: object) -> tuple[str, str]:
    text = _clean_text(value)
    if "," not in text:
        return "", text
    city_part, state_part = text.rsplit(",", 1)
    return _clean_text(city_part), _clean_text(state_part)


def _normalize_state_code(value: object) -> str:
    _, parsed_state = _split_city_state(value)
    return _clean_text(parsed_state).upper()


def _with_default_city_options(cities: list[str]) -> list[str]:
    merged: list[str] = []
    for city in list(cities or []) + DEFAULT_STRATEGIC_CITY_OPTIONS:
        city_name = _clean_text(city)
        if city_name and city_name not in merged:
            merged.append(city_name)
    return merged


def _city_options_for_subsidiary(contexts: dict, subsidiary_name: str) -> list[str]:
    cities_by_subsidiary = contexts.get("cities_by_subsidiary", {}) if isinstance(contexts, dict) else {}
    city_options = []
    if isinstance(cities_by_subsidiary, dict):
        city_options = cities_by_subsidiary.get(str(subsidiary_name), []) or []
    if not city_options:
        city_options = contexts.get("cities", []) if isinstance(contexts, dict) else []
    cleaned = [_clean_text(city) for city in city_options if _clean_text(city)]
    # Static choices are an offline fallback only.  An API response is the
    # authority for active region-plan contexts, including LA_6area.
    if cleaned:
        return list(dict.fromkeys(cleaned))
    if str(subsidiary_name) == DEFAULT_SUBSIDIARY_NAME:
        return _with_default_city_options([])
    return []


def _load_payload_source_rows(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
) -> tuple[str, pd.DataFrame, pd.DataFrame]:
    """Load plan-context source work and roster through the versioned API."""
    source_city_name = _context_source_city_name(strategic_city_name)
    jobs_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/jobs",
            subsidiary_name=subsidiary_name,
            strategic_city_name=source_city_name,
        ).get("rows", [])
    )
    if not jobs_df.empty and "promise_date" in jobs_df.columns:
        jobs_df = jobs_df[jobs_df["promise_date"].astype(str) == str(promise_date)].copy()
    technicians_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/technicians",
            subsidiary_name=subsidiary_name,
            strategic_city_name=source_city_name,
            promise_date=str(promise_date),
        ).get("rows", [])
    )
    return source_city_name, jobs_df, technicians_df


def _build_payload_request(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    jobs_df: pd.DataFrame,
    technicians_df: pd.DataFrame,
    capability_rows: list[dict],
) -> dict:
    """Keep the submitted routing request traceable to the selected context."""
    return {
        "subsidiary_name": subsidiary_name,
        "strategic_city_name": strategic_city_name,
        "promise_date": str(promise_date),
        "jobs": jobs_df.to_dict("records"),
        "technicians": technicians_df.to_dict("records"),
        "capabilities": capability_rows,
        "mode": "na_general",
    }


def _read_uploaded_technician_master_csv(
    uploaded_file,
    subsidiary_name: str,
    strategic_city_name: str,
) -> pd.DataFrame:
    uploaded_file.seek(0)
    raw_df = pd.read_csv(
        uploaded_file,
        encoding="utf-8-sig",
        keep_default_na=False,
        sep=None,
        engine="python",
    )
    alias_map = {
        "employee_code": "employee_code",
        "svc_engineer_code": "employee_code",
        "engineer_code": "employee_code",
        "technician_code": "employee_code",
        "employee_name": "employee_name",
        "svc_engineer_name": "employee_name",
        "engineer_name": "employee_name",
        "technician_name": "employee_name",
        "name": "employee_name",
        "home_street_address": "home_address",
        "home_address": "home_address",
        "address": "home_address",
        "street_address": "home_address",
        "city": "home_city",
        "home_city": "home_city",
        "state": "home_state",
        "home_state": "home_state",
        "zip": "home_postal_code",
        "zipcode": "home_postal_code",
        "zip_code": "home_postal_code",
        "postal_code": "home_postal_code",
        "home_postal_code": "home_postal_code",
    }
    rename_map: dict[str, str] = {}
    for column in raw_df.columns:
        canonical = _canonicalize_column_name(column)
        if canonical in alias_map and alias_map[canonical] not in rename_map.values():
            rename_map[column] = alias_map[canonical]
    working = raw_df.rename(columns=rename_map).copy()
    if "employee_code" not in working.columns:
        raise ValueError("Uploaded technician master CSV must include SVC_ENGINEER_CODE or employee_code.")

    default_city, default_state = _split_city_state(strategic_city_name)
    rows: list[dict[str, object]] = []
    seen_codes: set[str] = set()
    for _, row in working.iterrows():
        employee_code = _clean_text(row.get("employee_code"))
        if not employee_code:
            continue
        if employee_code in seen_codes:
            raise ValueError(f"Duplicate employee_code in technician master CSV: {employee_code}")
        seen_codes.add(employee_code)

        home_city = _clean_text(row.get("home_city"))
        home_state = _clean_text(row.get("home_state"))
        parsed_city, parsed_state = _split_city_state(home_state)
        if not home_city and parsed_city:
            home_city = parsed_city
        if parsed_state:
            home_state = parsed_state
        if not home_city:
            home_city = default_city
        if not home_state:
            home_state = default_state

        rows.append(
            {
                "subsidiary_name": subsidiary_name,
                "strategic_city_name": strategic_city_name,
                "employee_code": employee_code,
                "employee_name": _clean_text(row.get("employee_name")) or employee_code,
                "center_type": "DMS",
                "home_address": _clean_text(row.get("home_address")),
                "home_city": home_city,
                "home_state": home_state,
                "home_country": "USA",
                "home_postal_code": _clean_text(row.get("home_postal_code")),
                "active_flag": True,
                "priority_group": "A",
            }
        )
    if not rows:
        raise ValueError("Uploaded technician master CSV does not contain any valid employee_code rows.")
    return pd.DataFrame(rows)


def _build_capability_rows_for_payload(
    strategic_city_name: str,
    technicians_df: pd.DataFrame,
    jobs_df: pd.DataFrame,
) -> list[dict]:
    if technicians_df.empty or "employee_code" not in technicians_df.columns:
        return []
    if jobs_df.empty:
        return []
    employee_codes = {
        str(code).strip()
        for code in technicians_df["employee_code"].astype(str).tolist()
        if str(code).strip()
    }
    if not employee_codes:
        return []
    required_job_cols = {"service_product_group_code", "service_product_code"}
    if not required_job_cols.issubset(jobs_df.columns):
        return []
    requested_products = {
        (
            str(row["service_product_group_code"]).strip().upper(),
            str(row["service_product_code"]).strip().upper(),
        )
        for _, row in jobs_df.iterrows()
        if str(row.get("service_product_group_code", "")).strip()
        and str(row.get("service_product_code", "")).strip()
    }
    if not requested_products:
        return []
    capability_df = _load_profile_capability_df()
    profile_city_name = _profile_city_name(strategic_city_name)
    filtered = capability_df[
        capability_df["STRATEGIC_CITY_NAME"].astype(str).eq(str(profile_city_name).strip())
        & capability_df["SVC_ENGINEER_CODE"].astype(str).isin(employee_codes)
        & capability_df["REPAIR_FLAG"].astype(str).eq("T")
    ].copy()
    if filtered.empty:
        return []
    filtered["SERVICE_PRODUCT_GROUP_CODE"] = filtered["SERVICE_PRODUCT_GROUP_CODE"].astype(str).str.upper()
    filtered["SERVICE_PRODUCT_CODE"] = filtered["SERVICE_PRODUCT_CODE"].astype(str).str.upper()
    filtered = filtered[
        filtered.apply(
            lambda row: (
                str(row["SERVICE_PRODUCT_GROUP_CODE"]).strip(),
                str(row["SERVICE_PRODUCT_CODE"]).strip(),
            ) in requested_products,
            axis=1,
        )
    ].copy()
    if filtered.empty:
        return []
    filtered["heavy_repair_allowed"] = ~(
        filtered["SERVICE_PRODUCT_GROUP_CODE"].eq("REF")
        & filtered["AREA_PRODUCT_FLAG"].astype(str).str.upper().eq("N")
    )
    filtered = filtered.drop_duplicates(
        subset=["SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE"]
    ).reset_index(drop=True)
    return [
        {
            "employee_code": str(row["SVC_ENGINEER_CODE"]).strip(),
            "product_group_code": str(row["SERVICE_PRODUCT_GROUP_CODE"]).strip(),
            "product_code": str(row["SERVICE_PRODUCT_CODE"]).strip(),
            "heavy_repair_allowed": bool(row["heavy_repair_allowed"]),
        }
        for _, row in filtered.iterrows()
    ]


def _build_capability_master_view_df(strategic_city_name: str) -> pd.DataFrame:
    capability_df = _load_profile_capability_df()
    profile_city_name = _profile_city_name(strategic_city_name)
    filtered = capability_df[
        capability_df["STRATEGIC_CITY_NAME"].astype(str).eq(str(profile_city_name).strip())
        & capability_df["REPAIR_FLAG"].astype(str).eq("T")
    ].copy()
    if filtered.empty:
        return pd.DataFrame()
    filtered["SERVICE_PRODUCT_GROUP_CODE"] = filtered["SERVICE_PRODUCT_GROUP_CODE"].astype(str).str.upper()
    filtered["SERVICE_PRODUCT_CODE"] = filtered["SERVICE_PRODUCT_CODE"].astype(str).str.upper()
    filtered["heavy_repair_allowed"] = ~(
        filtered["SERVICE_PRODUCT_GROUP_CODE"].eq("REF")
        & filtered["AREA_PRODUCT_FLAG"].astype(str).str.upper().eq("N")
    )
    return (
        filtered.rename(
            columns={
                "STRATEGIC_CITY_NAME": "strategic_city_name",
                "SVC_ENGINEER_CODE": "employee_code",
                "SERVICE_PRODUCT_GROUP_CODE": "product_group_code",
                "SERVICE_PRODUCT_CODE": "product_code",
            }
        )[["strategic_city_name", "employee_code", "product_group_code", "product_code", "heavy_repair_allowed"]]
        .drop_duplicates()
        .sort_values(["employee_code", "product_group_code", "product_code"])
        .reset_index(drop=True)
    )


def _prepare_jobs_df(
    raw_df: pd.DataFrame,
    subsidiary_name: str,
    strategic_city_name: str,
    existing_df: pd.DataFrame,
    *,
    allow_existing_receipt: str = "",
    replace_existing_receipts: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    working = raw_df.copy()
    missing = [col for col in JOB_REQUIRED_COLUMNS if col not in working.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    working = working[JOB_REQUIRED_COLUMNS].copy()
    has_fixed_column = "fixed" in raw_df.columns
    fixed_series = _coerce_bool_series(
        raw_df["fixed"] if has_fixed_column else pd.Series(False, index=raw_df.index)
    )
    job_slot_count_series = _optional_job_slot_count_series(raw_df)
    for col in JOB_REQUIRED_COLUMNS:
        working[col] = working[col].astype(str).str.strip().replace(
            {"nan": "", "None": "", "none": "", "NaN": "", "NAN": "", "NaT": "", "nat": ""}
        )
    blank_mask = working[JOB_REQUIRED_COLUMNS].eq("").all(axis=1)
    if blank_mask.any():
        working = working.loc[~blank_mask].copy()
        fixed_series = fixed_series.loc[working.index]
        job_slot_count_series = job_slot_count_series.loc[working.index]
    original_promise_dates = working["PROMISE_DATE"].copy()
    working["PROMISE_DATE"] = working["PROMISE_DATE"].map(_normalize_promise_date)
    if working["PROMISE_DATE"].eq("").any():
        invalid_values = original_promise_dates.loc[working["PROMISE_DATE"].eq("")].drop_duplicates().astype(str).tolist()
        invalid_preview = ", ".join(invalid_values[:10])
        raise ValueError(f"PROMISE_DATE must be YYYYMMDD. Invalid values: {invalid_preview}")
    working["POSTAL_CODE"] = working["POSTAL_CODE"].str.replace(r"\.0+$", "", regex=True).str.zfill(5)
    upload_duplicate_receipts: list[str] = []
    dup_mask = working.duplicated(subset=["GSFS_RECEIPT_NO", "PROMISE_DATE"], keep=False)
    if dup_mask.any():
        dup_pairs = (
            working.loc[dup_mask, ["GSFS_RECEIPT_NO", "PROMISE_DATE"]]
            .drop_duplicates()
            .sort_values(["PROMISE_DATE", "GSFS_RECEIPT_NO"])
        )
        upload_duplicate_receipts = [f"{row['GSFS_RECEIPT_NO']} ({row['PROMISE_DATE']})" for _, row in dup_pairs.iterrows()]
        working = working.drop_duplicates(subset=["GSFS_RECEIPT_NO", "PROMISE_DATE"], keep="first").copy()
        fixed_series = fixed_series.loc[working.index]
        job_slot_count_series = job_slot_count_series.loc[working.index]

    existing_lookup_df = existing_df.copy()
    if not existing_lookup_df.empty:
        for col in ["gsfs_receipt_no", "promise_date", "svc_engineer_code", "svc_engineer_name"]:
            if col not in existing_lookup_df.columns:
                existing_lookup_df[col] = ""
        existing_lookup_df["gsfs_receipt_no"] = existing_lookup_df["gsfs_receipt_no"].astype(str).str.strip()
        existing_lookup_df["promise_date"] = existing_lookup_df["promise_date"].astype(str).str.strip()
        existing_lookup_df["svc_engineer_code"] = existing_lookup_df["svc_engineer_code"].astype(str).str.strip()
        existing_lookup_df["svc_engineer_name"] = existing_lookup_df["svc_engineer_name"].astype(str).str.strip()

    existing_same_day_pairs: set[tuple[str, str]] = set()
    if not existing_lookup_df.empty:
        existing_same_day_pairs = {
            (str(row["gsfs_receipt_no"]).strip(), str(row["promise_date"]).strip())
            for _, row in existing_lookup_df.iterrows()
            if str(row["gsfs_receipt_no"]).strip() and str(row["promise_date"]).strip()
        }
    if allow_existing_receipt:
        existing_same_day_pairs = {
            pair for pair in existing_same_day_pairs if pair[0] != str(allow_existing_receipt).strip()
        }
    duplicate_mask = (
        pd.Series(False, index=working.index)
        if replace_existing_receipts
        else working.apply(
            lambda row: (str(row["GSFS_RECEIPT_NO"]).strip(), str(row["PROMISE_DATE"]).strip()) in existing_same_day_pairs,
            axis=1,
        )
    )
    duplicate_pairs = (
        working.loc[duplicate_mask, ["GSFS_RECEIPT_NO", "PROMISE_DATE"]]
        .drop_duplicates()
        .sort_values(["PROMISE_DATE", "GSFS_RECEIPT_NO"])
    )
    duplicate_receipts = upload_duplicate_receipts + [f"{row['GSFS_RECEIPT_NO']} ({row['PROMISE_DATE']})" for _, row in duplicate_pairs.iterrows()]
    working = working.loc[~duplicate_mask].copy()
    if working.empty:
        return pd.DataFrame(), duplicate_receipts
    working["SUBSIDIARY_NAME"] = subsidiary_name
    working["STRATEGIC_CITY_NAME"] = strategic_city_name
    working["fixed"] = fixed_series.loc[working.index].astype(bool)
    working["job_slot_count"] = job_slot_count_series.loc[working.index].astype(int)
    if not existing_lookup_df.empty:
        historical_pairs = {
            (str(row["gsfs_receipt_no"]).strip(), str(row["promise_date"]).strip())
            for _, row in existing_lookup_df.iterrows()
            if str(row.get("gsfs_receipt_no", "")).strip() and str(row.get("promise_date", "")).strip()
        }
        historical_mask = working.apply(
            lambda row: any(
                receipt_no == str(row["GSFS_RECEIPT_NO"]).strip()
                and promise_date != str(row["PROMISE_DATE"]).strip()
                for receipt_no, promise_date in historical_pairs
            ),
            axis=1,
        )
        reschedule_series = (
            _coerce_bool_series(raw_df["reschedule"]).loc[working.index]
            if "reschedule" in raw_df.columns
            else pd.Series(False, index=working.index)
        )
        working["reschedule"] = (
            (reschedule_series.astype(bool) | historical_mask.astype(bool))
            & ~working["fixed"].astype(bool)
        ).astype(bool)
    elif "reschedule" in raw_df.columns:
        working["reschedule"] = (
            _coerce_bool_series(raw_df["reschedule"]).loc[working.index].astype(bool)
            & ~working["fixed"].astype(bool)
        ).astype(bool)
    else:
        working["reschedule"] = False
    city_parts = [part.strip() for part in strategic_city_name.split(",")]
    working["STATE_NAME"] = city_parts[1] if len(city_parts) >= 2 else ""
    working["COUNTRY_NAME"] = "USA"
    working["record_id"] = [uuid.uuid4().hex for _ in range(len(working))]
    working["source"] = "csv_upload"
    return working, duplicate_receipts


def _geocode_jobs_df(job_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if job_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    geocode_input = job_df.rename(columns={"SUBSIDIARY_NAME": "SUBSIDIARY_NAME"})
    config = _load_runtime_config()
    merged_df = _merge_service_geocodes(geocode_input.copy(), config)
    merged_df["latitude"] = pd.to_numeric(merged_df.get("latitude"), errors="coerce")
    merged_df["longitude"] = pd.to_numeric(merged_df.get("longitude"), errors="coerce")
    failed_df = merged_df[merged_df["latitude"].isna() | merged_df["longitude"].isna()].copy()
    success_df = merged_df[merged_df["latitude"].notna() & merged_df["longitude"].notna()].copy()
    return success_df, failed_df


def _build_job_upsert_rows(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return [
        {
            "record_id": str(row["record_id"]),
            "subsidiary_name": str(row["SUBSIDIARY_NAME"]),
            "strategic_city_name": str(row["STRATEGIC_CITY_NAME"]),
            "svc_engineer_code": str(row["SVC_ENGINEER_CODE"]),
            "svc_engineer_name": str(row["SVC_ENGINEER_NAME"]),
            "service_product_group_code": str(row["SERVICE_PRODUCT_GROUP_CODE"]),
            "service_product_code": str(row["SERVICE_PRODUCT_CODE"]),
            "receipt_detail_symptom_code": str(row["RECEIPT_DETAIL_SYMPTOM_CODE"]),
            "gsfs_receipt_no": str(row["GSFS_RECEIPT_NO"]),
            "promise_date": str(row["PROMISE_DATE"]),
            "city_name": str(row["CITY_NAME"]),
            "state_name": str(row["STATE_NAME"]),
            "country_name": str(row["COUNTRY_NAME"]),
            "postal_code": str(row["POSTAL_CODE"]),
            "address_line1_info": str(row["ADDRESS_LINE1_INFO"]),
            "fixed": _coerce_bool_value(row.get("fixed", False)),
            "reschedule": _coerce_bool_value(row.get("reschedule", False)),
            "job_slot_count": _coerce_job_slot_count_value(row.get("job_slot_count", 2 if _coerce_bool_value(row.get("two_slot_job", False)) else 1)),
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "source": str(row.get("source", "csv_upload")),
        }
        for _, row in df.iterrows()
    ]


def _job_rows_to_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _reset_common_result_view(default_compare_mode: str = "Smart Routing") -> None:
    st.session_state["common_vrp_compare_mode"] = default_compare_mode
    st.session_state.pop("common_result_date", None)
    st.session_state.pop("common_result_region", None)
    st.session_state.pop("common_result_engineer", None)
    st.session_state.pop(FORCE_ASSIGN_PREVIEW_KEY, None)


def _common_runtime_context_key(subsidiary_name: str, strategic_city_name: str) -> str:
    return f"{_clean_text(subsidiary_name)}::{_clean_text(strategic_city_name)}"


def _clear_common_runtime_state() -> None:
    for key in [
        "common_vrp_payload",
        "common_vrp_payload_date",
        "common_vrp_payload_debug",
        "common_vrp_request_id",
        "common_vrp_job_id",
        "common_vrp_job_status",
        "common_vrp_job_result",
        "server_common_statistics_state",
        "server_common_statistics_view_active",
        "common_job_dialog_open",
        "common_job_dialog_record_id",
    ]:
        st.session_state.pop(key, None)
    _reset_common_result_view()


def _ensure_common_runtime_context(subsidiary_name: str, strategic_city_name: str) -> None:
    context_key = _common_runtime_context_key(subsidiary_name, strategic_city_name)
    if st.session_state.get("common_vrp_context_key") != context_key:
        _clear_common_runtime_state()
        st.session_state["common_vrp_context_key"] = context_key


def _close_common_job_dialog() -> None:
    st.session_state["common_job_dialog_open"] = False
    st.session_state["common_job_dialog_record_id"] = None


@st.fragment(run_every="5s")
def _auto_poll_common_routing_status() -> None:
    request_id = str(st.session_state.get("common_vrp_request_id", "")).strip()
    current_status_payload = st.session_state.get("common_vrp_job_status") or {}
    current_status = str(current_status_payload.get("status", "")).strip().lower()
    if not request_id or current_status not in {"submitted", "queued", "running"}:
        return
    try:
        snapshot = _api_post(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/routing/check", {"request_id": request_id})
        st.session_state["common_vrp_job_status"] = snapshot.get("status")
        st.session_state["common_vrp_job_result"] = snapshot.get("result")
        latest_status = str((snapshot.get("status") or {}).get("status", "")).strip().lower()
        if latest_status == "completed" and snapshot.get("result"):
            _reset_common_result_view()
        if latest_status in {"completed", "failed"}:
            st.rerun()
    except Exception:
        return


def _load_latest_routing_result_for_date(subsidiary_name: str, strategic_city_name: str, promise_date: str) -> bool:
    snapshot = _api_get(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/routing/latest",
        subsidiary_name=subsidiary_name,
        strategic_city_name=strategic_city_name,
        promise_date=str(promise_date),
    ).get("snapshot")
    if not snapshot:
        return False
    request_row = dict(snapshot.get("request") or {})
    payload = None
    payload_text = str(request_row.get("payload_json", "") or "").strip()
    if payload_text:
        try:
            payload = json.loads(payload_text)
        except Exception:
            payload = None
    status_payload = {}
    status_text = str(request_row.get("status_json", "") or "").strip()
    if status_text:
        try:
            status_payload = json.loads(status_text)
        except Exception:
            status_payload = {}
    st.session_state["common_vrp_payload"] = payload
    st.session_state["common_vrp_request_id"] = str(request_row.get("request_id", "")).strip()
    st.session_state["common_vrp_job_id"] = str(request_row.get("routing_job_id", "")).strip()
    st.session_state["common_vrp_job_status"] = status_payload
    st.session_state["common_vrp_job_result"] = snapshot.get("result")
    _reset_common_result_view()
    st.session_state["common_result_date"] = _promise_date_to_service_date_key(str(promise_date))
    return True


def _build_common_result_frames(
    result_payload: dict,
    jobs_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_zip_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    assignments_df = pd.DataFrame(result_payload.get("assignments", []))
    if assignments_df.empty or jobs_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    assignments_df = assignments_df.copy()
    if "receipt_no" not in assignments_df.columns:
        assignments_df["receipt_no"] = assignments_df.get("salesforce_id", "")
    assignments_df["receipt_no"] = assignments_df["receipt_no"].astype(str).str.strip()

    job_lookup = jobs_df.rename(
        columns={
            "gsfs_receipt_no": "GSFS_RECEIPT_NO",
            "svc_engineer_code": "SVC_ENGINEER_CODE",
            "svc_engineer_name": "SVC_ENGINEER_NAME",
            "service_product_group_code": "SERVICE_PRODUCT_GROUP_CODE",
            "service_product_code": "SERVICE_PRODUCT_CODE",
            "receipt_detail_symptom_code": "RECEIPT_DETAIL_SYMPTOM_CODE",
            "city_name": "CITY_NAME",
            "state_name": "STATE_NAME",
            "country_name": "COUNTRY_NAME",
            "postal_code": "POSTAL_CODE",
            "address_line1_info": "ADDRESS_LINE1_INFO",
            "latitude": "latitude",
            "longitude": "longitude",
        }
    ).copy()
    merged = job_lookup.merge(
        assignments_df.rename(columns={"receipt_no": "GSFS_RECEIPT_NO"}),
        on="GSFS_RECEIPT_NO",
        how="inner",
    )
    slot_candidates = [col for col in ["job_slot_count", "job_slot_count_x", "job_slot_count_y"] if col in merged.columns]
    if slot_candidates:
        slot_series = pd.Series(pd.NA, index=merged.index)
        for col in slot_candidates:
            slot_series = slot_series.combine_first(merged[col])
        merged["job_slot_count"] = _coerce_job_slot_count_series(slot_series)
        merged = merged.drop(columns=[col for col in ["job_slot_count_x", "job_slot_count_y"] if col in merged.columns], errors="ignore")
    else:
        merged["job_slot_count"] = 1
    service_candidates = [col for col in ["service_time_min_y", "service_time_min", "service_time_min_x", "service_minutes"] if col in merged.columns]
    if service_candidates:
        service_series = pd.Series(pd.NA, index=merged.index)
        for col in service_candidates:
            service_series = service_series.combine_first(merged[col])
        merged["service_time_min"] = pd.to_numeric(service_series, errors="coerce")
        merged = merged.drop(columns=[col for col in ["service_time_min_x", "service_time_min_y"] if col in merged.columns], errors="ignore")
    engineer_lookup = engineer_master_df.rename(
        columns={
            "employee_code": "assigned_sm_code",
            "employee_name": "assigned_sm_name",
            "center_type": "assigned_center_type",
            "home_latitude": "home_start_latitude",
            "home_longitude": "home_start_longitude",
        }
    )[
        ["assigned_sm_code", "assigned_sm_name", "assigned_center_type", "home_start_latitude", "home_start_longitude"]
    ].drop_duplicates(subset=["assigned_sm_code"])
    merged = merged.merge(engineer_lookup, left_on="employee_code", right_on="assigned_sm_code", how="left")
    original_engineer_lookup = (
        engineer_master_df.rename(columns={"employee_code": "SVC_ENGINEER_CODE", "employee_name": "lookup_svc_engineer_name"})[
            ["SVC_ENGINEER_CODE", "lookup_svc_engineer_name"]
        ]
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        .copy()
    )
    merged = merged.merge(original_engineer_lookup, on="SVC_ENGINEER_CODE", how="left")
    merged["SVC_ENGINEER_NAME"] = (
        merged.get("SVC_ENGINEER_NAME", pd.Series(index=merged.index))
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .fillna(merged["lookup_svc_engineer_name"])
        .fillna("")
    )
    merged = merged.drop(columns=["lookup_svc_engineer_name"], errors="ignore")
    if not region_zip_df.empty:
        region_cols = ["POSTAL_CODE", "region_seq", "new_region_name"]
        if "area_type" in region_zip_df.columns:
            region_cols.append("area_type")
        region_lookup = region_zip_df[region_cols].drop_duplicates().copy()
        region_lookup["POSTAL_CODE"] = region_lookup["POSTAL_CODE"].astype(str).str.zfill(5)
        merged["POSTAL_CODE"] = merged["POSTAL_CODE"].astype(str).str.zfill(5)
        merged = merged.merge(region_lookup, on="POSTAL_CODE", how="left")
    else:
        merged["region_seq"] = pd.NA
        merged["new_region_name"] = pd.NA
        merged["area_type"] = pd.NA
    merged["service_date_key"] = (
        merged.get("promise_date", pd.Series(index=merged.index)).astype(str).map(
            lambda value: f"{value[:4]}-{value[4:6]}-{value[6:8]}" if len(str(value)) == 8 else str(value)
        )
    )
    merged["visit_start_time"] = pd.to_datetime(merged.get("planned_start"), errors="coerce").dt.strftime("%H:%M").fillna("")
    merged["visit_end_time"] = pd.to_datetime(merged.get("planned_end"), errors="coerce").dt.strftime("%H:%M").fillna("")
    merged["visit_seq"] = pd.to_numeric(merged.get("sequence"), errors="coerce").fillna(0).astype(int)
    merged["assigned_sm_name"] = merged["assigned_sm_name"].fillna(merged.get("employee_code"))
    merged["changed"] = merged.get("changed", False).fillna(False)
    merged["assigned_region_name"] = pd.NA
    merged["travel_time_from_prev_min"] = pd.NA
    schedule_df = merged.sort_values(["assigned_sm_code", "visit_seq", "GSFS_RECEIPT_NO"]).reset_index(drop=True)
    assignment_df = schedule_df.copy()
    return assignment_df, schedule_df


def _build_unassigned_job_display_df(
    result_payload: dict,
    jobs_df: pd.DataFrame,
    region_zip_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    unassigned_df = pd.DataFrame(result_payload.get("unassigned", [])) if isinstance(result_payload, dict) else pd.DataFrame()
    if unassigned_df.empty:
        return pd.DataFrame()
    if "receipt_no" not in unassigned_df.columns:
        unassigned_df["receipt_no"] = unassigned_df.get("salesforce_id", "")
    unassigned_df["receipt_no"] = unassigned_df["receipt_no"].astype(str).str.strip()
    display_df = unassigned_df[
        ["receipt_no"]
        + [
            col
            for col in ["reason", "eligible_employee_count"]
            if col in unassigned_df.columns
        ]
    ].copy()
    if jobs_df.empty or "gsfs_receipt_no" not in jobs_df.columns:
        for col in ["city_name", "postal_code", "address_line1_info", "service_product_group_code", "service_product_code", "fixed", "reschedule", "job_slot_count", "eligible_employee_count"]:
            display_df[col] = ""
        return display_df

    job_cols = [
        "gsfs_receipt_no",
        "promise_date",
        "city_name",
        "postal_code",
        "address_line1_info",
        "service_product_group_code",
        "service_product_code",
        "fixed",
        "reschedule",
        "job_slot_count",
        "latitude",
        "longitude",
    ]
    job_cols = [col for col in job_cols if col in jobs_df.columns]
    job_lookup = jobs_df[job_cols].drop_duplicates(subset=["gsfs_receipt_no"]).copy()
    display_df = display_df.merge(job_lookup, left_on="receipt_no", right_on="gsfs_receipt_no", how="left")
    display_df = display_df.drop(columns=["gsfs_receipt_no"], errors="ignore")
    if "promise_date" in display_df.columns:
        display_df["service_date_key"] = display_df["promise_date"].astype(str).map(
            lambda value: f"{value[:4]}-{value[4:6]}-{value[6:8]}" if len(str(value)) == 8 else str(value)
        )
    if region_zip_df is not None and not region_zip_df.empty and "postal_code" in display_df.columns:
        region_cols = ["POSTAL_CODE", "region_seq", "new_region_name"]
        if "area_type" in region_zip_df.columns:
            region_cols.append("area_type")
        region_lookup = region_zip_df[region_cols].drop_duplicates().copy()
        region_lookup["POSTAL_CODE"] = region_lookup["POSTAL_CODE"].astype(str).str.zfill(5)
        display_df["POSTAL_CODE"] = display_df["postal_code"].astype(str).str.zfill(5)
        display_df = display_df.merge(region_lookup, on="POSTAL_CODE", how="left")
        display_df = display_df.drop(columns=["POSTAL_CODE"], errors="ignore")
    return display_df


def _build_unassigned_analysis_df(
    unassigned_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    assignment_df: pd.DataFrame,
    availability_by_date: dict[tuple[str, str], bool] | None = None,
) -> pd.DataFrame:
    columns = [
        "promise_date", "receipt_no", "reason", "overall_diagnosis", "postal_code", "address_line1_info",
        "latitude", "longitude", "rank", "technician_code", "technician_name",
        "home_distance_km", "available", "assigned_slots", "max_slots",
        "remaining_slots", "candidate_diagnosis",
    ]
    if unassigned_df.empty or engineer_master_df.empty:
        return pd.DataFrame(columns=columns)

    def _number(row: pd.Series, names: list[str], default: float | None = None) -> float | None:
        for name in names:
            if name in row.index and pd.notna(row.get(name)):
                value = pd.to_numeric(pd.Series([row.get(name)]), errors="coerce").iloc[0]
                if pd.notna(value):
                    return float(value)
        return default

    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        radius = 6371.0088
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        d_phi, d_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        value = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
        return radius * 2 * math.atan2(math.sqrt(value), math.sqrt(max(0.0, 1 - value)))

    assigned_slots: dict[tuple[str, str], int] = {}
    if not assignment_df.empty and "assigned_sm_code" in assignment_df.columns:
        working = assignment_df.copy()
        slot_values = working["job_slot_count"] if "job_slot_count" in working.columns else pd.Series(1, index=working.index)
        working["_slots"] = pd.to_numeric(slot_values, errors="coerce").fillna(1)
        working["_service_date_key"] = working.get("service_date_key", "").astype(str).str.strip()
        working["_assigned_code"] = working["assigned_sm_code"].astype(str).str.strip()
        grouped = working.groupby(["_service_date_key", "_assigned_code"])["_slots"].sum().astype(int)
        assigned_slots = {(str(day), str(code)): int(value) for (day, code), value in grouped.items()}

    candidates: list[dict[str, object]] = []
    for _, row in engineer_master_df.drop_duplicates(subset=["employee_code"]).iterrows():
        code = str(row.get("employee_code", "")).strip()
        if not code:
            continue
        lat = _number(row, ["home_latitude", "home_start_latitude", "latitude"])
        lon = _number(row, ["home_longitude", "home_start_longitude", "longitude"])
        if lat is None or lon is None:
            continue
        available = _coerce_bool_value(row.get("available", row.get("active_flag", True)))
        max_slots = int(_number(row, ["max_slots", "max_jobs", "slot_count"], 8) or 8)
        candidates.append({"code": code, "name": str(row.get("employee_name", code)).strip() or code, "lat": lat, "lon": lon, "available": available, "max_slots": max_slots, "assigned_slots": int(assigned_slots.get(code, 0))})

    rows: list[dict[str, object]] = []
    for _, job in unassigned_df.iterrows():
        job_lat = _number(job, ["latitude"])
        job_lon = _number(job, ["longitude"])
        if job_lat is None or job_lon is None:
            continue
        job_date = str(job.get("service_date_key", job.get("promise_date", ""))).strip()
        if len(job_date) == 8 and job_date.isdigit():
            job_date = f"{job_date[:4]}-{job_date[4:6]}-{job_date[6:8]}"
        ranked = sorted(candidates, key=lambda candidate: _haversine_km(job_lat, job_lon, candidate["lat"], candidate["lon"]))[:5]
        candidate_rows: list[dict[str, object]] = []
        for rank, candidate in enumerate(ranked, start=1):
            assigned_for_day = int(assigned_slots.get((job_date, str(candidate["code"])), 0))
            remaining = max(0, int(candidate["max_slots"]) - assigned_for_day)
            available_for_day = bool((availability_by_date or {}).get((job_date, str(candidate["code"])), candidate["available"]))
            diagnosis = "UNAVAILABLE" if not available_for_day else "SLOT_FULL" if remaining <= 0 else "NO_FEASIBLE_ROUTE"
            candidate_rows.append({
                "promise_date": str(job.get("promise_date", job.get("service_date_key", ""))).strip(),
                "receipt_no": str(job.get("receipt_no", "")).strip(), "reason": str(job.get("reason", "")).strip(),
                "postal_code": str(job.get("postal_code", "")).strip(), "address_line1_info": str(job.get("address_line1_info", "")).strip(),
                "latitude": job_lat, "longitude": job_lon, "rank": rank, "technician_code": candidate["code"],
                "technician_name": candidate["name"], "home_distance_km": round(_haversine_km(job_lat, job_lon, candidate["lat"], candidate["lon"]), 2),
                "available": available_for_day, "assigned_slots": 0 if not available_for_day else assigned_for_day,
                "max_slots": int(candidate["max_slots"]), "remaining_slots": 0 if not available_for_day else remaining, "candidate_diagnosis": diagnosis,
            })
        if candidate_rows:
            if any(row["candidate_diagnosis"] == "NO_FEASIBLE_ROUTE" for row in candidate_rows):
                overall = "NO_FEASIBLE_ROUTE"
            elif all(row["candidate_diagnosis"] == "SLOT_FULL" for row in candidate_rows):
                overall = "SLOT_FULL"
            elif all(row["candidate_diagnosis"] == "UNAVAILABLE" for row in candidate_rows):
                overall = "UNAVAILABLE"
            else:
                overall = str(job.get("reason", "UNKNOWN")).strip() or "UNKNOWN"
            for row in candidate_rows:
                row["overall_diagnosis"] = overall
                rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def _build_force_assign_technician_options(engineer_master_df: pd.DataFrame) -> tuple[list[str], dict[str, str]]:
    if engineer_master_df.empty or "employee_code" not in engineer_master_df.columns:
        return [], {}
    working = engineer_master_df.drop_duplicates(subset=["employee_code"]).copy()
    if "available" in working.columns:
        available_mask = working["available"].astype(str).str.strip().str.lower().isin({"true", "1", "y", "yes"})
        if available_mask.any():
            working = working[available_mask].copy()
    labels: list[str] = []
    label_to_code: dict[str, str] = {}
    for _, row in working.sort_values(["employee_name", "employee_code"], na_position="last").iterrows():
        code = str(row.get("employee_code", "")).strip()
        if not code:
            continue
        name = str(row.get("employee_name", "")).strip() or code
        label = f"{name} ({code})"
        labels.append(label)
        label_to_code[label] = code
    return labels, label_to_code


def _get_force_assignment_preview() -> dict[str, str]:
    preview = st.session_state.get(FORCE_ASSIGN_PREVIEW_KEY, {})
    if not isinstance(preview, dict):
        return {}
    return {
        str(receipt_no).strip(): str(employee_code).strip()
        for receipt_no, employee_code in preview.items()
        if str(receipt_no).strip() and str(employee_code).strip()
    }


def _save_force_assignment_preview(preview: dict[str, str]) -> None:
    clean_preview = {
        str(receipt_no).strip(): str(employee_code).strip()
        for receipt_no, employee_code in preview.items()
        if str(receipt_no).strip() and str(employee_code).strip()
    }
    if clean_preview:
        st.session_state[FORCE_ASSIGN_PREVIEW_KEY] = clean_preview
    else:
        st.session_state.pop(FORCE_ASSIGN_PREVIEW_KEY, None)


def _resequence_force_assigned_routes(result_payload: dict, affected_employee_codes: set[str]) -> dict:
    """Recalculate ``home -> all stops -> home`` for force-assigned routes."""
    if not affected_employee_codes:
        return result_payload
    request_payload = st.session_state.get("common_vrp_payload") or {}
    city_name = str(
        result_payload.get("strategic_city_name")
        or result_payload.get("city")
        or st.session_state.get("common_result_strategic_city_name", "")
    ).strip()
    if not city_name:
        return result_payload
    assignments = list(result_payload.get("assignments", []))
    result_job_lookup = {
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(): job
        for job in list(result_payload.get("jobs", []))
    }
    request_job_lookup = {
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(): job
        for job in list(request_payload.get("jobs", []))
    }
    request_technician_lookup = {
        str(technician.get("employee_code", "")).strip(): technician
        for technician in list(request_payload.get("technicians", []))
        if str(technician.get("employee_code", "")).strip()
    }
    try:
        route_client = get_route_client(city_name)
    except Exception:
        return result_payload

    for employee_code in affected_employee_codes:
        route_items = [
            item for item in assignments
            if str(item.get("employee_code", "")).strip() == employee_code
        ]
        coord_items: list[tuple[dict, tuple[float, float]]] = []
        for item in route_items:
            receipt_no = str(item.get("receipt_no", "") or item.get("salesforce_id", "")).strip()
            job = result_job_lookup.get(receipt_no) or request_job_lookup.get(receipt_no) or {}
            location = job.get("location") or {}
            try:
                coord_items.append((item, (float(location["lng"]), float(location["lat"]))))
            except (KeyError, TypeError, ValueError):
                continue
        if len(coord_items) < 2:
            continue
        technician = request_technician_lookup.get(employee_code, {})
        start_location = technician.get("start_location") or technician.get("end_location") or {}
        try:
            home_coord = (float(start_location.get("lng")), float(start_location.get("lat")))
        except (AttributeError, TypeError, ValueError):
            home_coord = None
        route_coords = [coord for _, coord in coord_items]
        if home_coord is not None:
            route_coords = [home_coord] + route_coords
        try:
            route = route_client.build_ordered_route(
                route_coords,
                preserve_first=home_coord is not None,
            )
            ordered_coords = list(route.get("ordered_coords", []))
        except Exception:
            continue
        if home_coord is not None and ordered_coords:
            first_coord = ordered_coords[0]
            if (
                round(float(first_coord[0]), 6) == round(float(home_coord[0]), 6)
                and round(float(first_coord[1]), 6) == round(float(home_coord[1]), 6)
            ):
                ordered_coords = ordered_coords[1:]
        if len(ordered_coords) != len(coord_items):
            continue
        remaining = list(coord_items)
        ordered_items: list[dict] = []
        for coord in ordered_coords:
            match_index = min(
                range(len(remaining)),
                key=lambda index: (remaining[index][1][0] - float(coord[0])) ** 2
                + (remaining[index][1][1] - float(coord[1])) ** 2,
            )
            ordered_items.append(remaining.pop(match_index)[0])
        ordered_items.extend(item for item, _ in remaining)
        for sequence, item in enumerate(ordered_items, start=1):
            item["sequence"] = sequence
            item["route_resequenced"] = True
            item["planned_start"] = ""
            item["planned_end"] = ""
    return result_payload


def _apply_force_assignments_to_result_payload(result_payload: dict, force_assignments: dict[str, str]) -> dict:
    result_payload = copy.deepcopy(result_payload or {})
    if not isinstance(result_payload, dict) or not force_assignments:
        return result_payload

    clean_force_assignments = {
        str(receipt_no).strip(): str(employee_code).strip()
        for receipt_no, employee_code in force_assignments.items()
        if str(receipt_no).strip() and str(employee_code).strip()
    }
    if not clean_force_assignments:
        return result_payload

    force_receipts = set(clean_force_assignments)
    affected_employee_codes = set(clean_force_assignments.values())
    assignments = [
        dict(item)
        for item in result_payload.get("assignments", [])
        if str(item.get("receipt_no", "") or item.get("salesforce_id", "")).strip() not in force_receipts
    ]

    payload = st.session_state.get("common_vrp_payload") or {}
    payload_jobs = list(payload.get("jobs", [])) if isinstance(payload, dict) else []
    job_lookup = {
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(): job
        for job in payload_jobs
    }

    for receipt_no, employee_code in clean_force_assignments.items():
        employee_sequences = [
            pd.to_numeric(pd.Series([item.get("sequence")]), errors="coerce").iloc[0]
            for item in assignments
            if str(item.get("employee_code", "")).strip() == employee_code
        ]
        max_sequence = max([int(value) for value in employee_sequences if pd.notna(value)], default=0)
        current_employee_code = str(job_lookup.get(receipt_no, {}).get("current_employee_code", "")).strip()
        assignments.append(
            {
                "salesforce_id": receipt_no,
                "receipt_no": receipt_no,
                "employee_code": employee_code,
                "sequence": max_sequence + 1,
                "planned_start": "",
                "planned_end": "",
                "changed": bool(current_employee_code and current_employee_code != employee_code),
                "force_assigned": True,
            }
        )

    result_payload["assignments"] = assignments
    result_payload = _resequence_force_assigned_routes(result_payload, affected_employee_codes)
    result_payload["unassigned"] = [
        dict(item)
        for item in result_payload.get("unassigned", [])
        if str(item.get("receipt_no", "") or item.get("salesforce_id", "")).strip() not in force_receipts
    ]
    summary = dict(result_payload.get("summary") or {})
    if summary:
        summary["assigned_jobs"] = len(assignments)
        summary["unassigned_jobs"] = len(result_payload["unassigned"])
        result_payload["summary"] = summary
    return result_payload


def _preview_force_assign_unassigned_job(receipt_no: str, employee_code: str) -> None:
    receipt_no = str(receipt_no).strip()
    employee_code = str(employee_code).strip()
    if not receipt_no or not employee_code:
        return
    preview = _get_force_assignment_preview()
    preview[receipt_no] = employee_code
    _save_force_assignment_preview(preview)
    st.session_state["common_vrp_compare_mode_pending"] = "Smart Routing"


def _remove_force_assignment_preview(receipt_no: str) -> None:
    preview = _get_force_assignment_preview()
    preview.pop(str(receipt_no).strip(), None)
    _save_force_assignment_preview(preview)
    st.session_state["common_vrp_compare_mode_pending"] = "Smart Routing"


def _commit_force_assignment_preview() -> None:
    preview = _get_force_assignment_preview()
    result_payload = st.session_state.get("common_vrp_job_result") or {}
    if not preview or not isinstance(result_payload, dict):
        return
    st.session_state["common_vrp_job_result"] = _apply_force_assignments_to_result_payload(result_payload, preview)
    _save_force_assignment_preview({})
    st.session_state["common_vrp_compare_mode_pending"] = "Smart Routing"


def _render_force_assignment_preview_actions() -> None:
    preview = _get_force_assignment_preview()
    if not preview:
        return
    st.divider()
    st.markdown("**Pending Force Assignments**")
    st.caption(f"{len(preview)} force assignment preview(s) are shown on the map and metrics but not saved yet.")
    save_col, clear_col = st.columns([1, 1])
    if save_col.button("Save Force Assignments", type="primary", width="stretch"):
        _commit_force_assignment_preview()
        st.success("Force assignments saved.")
        st.rerun()
    if clear_col.button("Clear Force Assignment Preview", width="stretch"):
        _save_force_assignment_preview({})
        st.session_state["common_vrp_compare_mode_pending"] = "Smart Routing"
        st.rerun()


def _build_jobs_df_from_payload(payload: dict | None) -> pd.DataFrame:
    if not isinstance(payload, dict):
        return pd.DataFrame()
    jobs = list(payload.get("jobs", []))
    if not jobs:
        return pd.DataFrame()
    planning_date = str(payload.get("planning_date", "")).strip().replace("-", "")
    rows: list[dict[str, object]] = []
    for job in jobs:
        receipt_no = str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        if not receipt_no:
            continue
        rows.append(
            {
                "gsfs_receipt_no": receipt_no,
                "svc_engineer_code": str(job.get("current_employee_code", "")).strip(),
                "svc_engineer_name": "",
                "service_product_group_code": str(job.get("product_group", "")).strip(),
                "service_product_code": str(job.get("product", "")).strip(),
                "receipt_detail_symptom_code": str(job.get("symptom", "")).strip(),
                "promise_date": planning_date,
                "city_name": str(job.get("city_name", "")).strip(),
                "state_name": str(job.get("state_name", "")).strip(),
                "country_name": str(job.get("country_name", "")).strip(),
                "postal_code": str(job.get("postal_code", "")).strip(),
                "address_line1_info": str(job.get("address", "")).strip(),
                "latitude": pd.to_numeric(pd.Series([((job.get("location") or {}).get("lat"))]), errors="coerce").iloc[0],
                "longitude": pd.to_numeric(pd.Series([((job.get("location") or {}).get("lng"))]), errors="coerce").iloc[0],
                "fixed": bool(job.get("fixed", False)),
                "reschedule": bool(job.get("reschedule", False)),
                "job_slot_count": _coerce_job_slot_count_value(job.get("job_slot_count", 2 if _coerce_bool_value(job.get("two_slot_job", False)) else 1)),
            }
        )
    return pd.DataFrame(rows)


def _filter_jobs_df_for_payload(jobs_df: pd.DataFrame, payload: dict | None) -> pd.DataFrame:
    if jobs_df.empty or not isinstance(payload, dict):
        return jobs_df.copy()
    filtered = jobs_df.copy()
    planning_date = str(payload.get("planning_date", "")).strip().replace("-", "")
    if planning_date and "promise_date" in filtered.columns:
        filtered = filtered[filtered["promise_date"].astype(str) == planning_date].copy()
    payload_receipts = {
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        for job in list(payload.get("jobs", []))
        if str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
    }
    if payload_receipts and "gsfs_receipt_no" in filtered.columns:
        filtered = filtered[filtered["gsfs_receipt_no"].astype(str).isin(payload_receipts)].copy()
    return filtered


def _build_common_actual_frames(
    jobs_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_zip_df: pd.DataFrame,
    strategic_city_name: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if jobs_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    actual_df = jobs_df.rename(
        columns={
            "gsfs_receipt_no": "GSFS_RECEIPT_NO",
            "svc_engineer_code": "SVC_ENGINEER_CODE",
            "svc_engineer_name": "SVC_ENGINEER_NAME",
            "service_product_group_code": "SERVICE_PRODUCT_GROUP_CODE",
            "service_product_code": "SERVICE_PRODUCT_CODE",
            "receipt_detail_symptom_code": "RECEIPT_DETAIL_SYMPTOM_CODE",
            "city_name": "CITY_NAME",
            "state_name": "STATE_NAME",
            "country_name": "COUNTRY_NAME",
            "postal_code": "POSTAL_CODE",
            "address_line1_info": "ADDRESS_LINE1_INFO",
        }
    ).copy()
    actual_df["service_date_key"] = actual_df["promise_date"].astype(str).map(
        lambda value: f"{value[:4]}-{value[4:6]}-{value[6:8]}" if len(str(value)) == 8 else str(value)
    )
    actual_df["assigned_sm_code"] = actual_df.get("SVC_ENGINEER_CODE", pd.Series(index=actual_df.index)).astype(str)
    actual_df["assigned_sm_name"] = actual_df.get("SVC_ENGINEER_NAME", pd.Series(index=actual_df.index)).astype(str)
    engineer_lookup = engineer_master_df.rename(
        columns={
            "employee_code": "assigned_sm_code",
            "employee_name": "assigned_sm_name_master",
            "center_type": "assigned_center_type",
            "home_latitude": "home_start_latitude",
            "home_longitude": "home_start_longitude",
        }
    )[
        ["assigned_sm_code", "assigned_sm_name_master", "assigned_center_type", "home_start_latitude", "home_start_longitude"]
    ].drop_duplicates(subset=["assigned_sm_code"])
    actual_df = actual_df.merge(engineer_lookup, on="assigned_sm_code", how="left")
    source_center_type = actual_df.get("SVC_CENTER_TYPE", pd.Series(index=actual_df.index)).fillna("").astype(str).str.strip()
    actual_df["assigned_center_type"] = actual_df["assigned_center_type"].fillna("").astype(str).str.strip()
    actual_df["assigned_center_type"] = actual_df["assigned_center_type"].where(
        actual_df["assigned_center_type"].ne(""),
        source_center_type,
    )
    actual_df["assigned_center_type"] = actual_df["assigned_center_type"].replace("", "UNKNOWN").str.upper()
    if not region_zip_df.empty:
        region_cols = ["POSTAL_CODE", "region_seq", "new_region_name"]
        if "area_type" in region_zip_df.columns:
            region_cols.append("area_type")
        region_lookup = region_zip_df[region_cols].drop_duplicates().copy()
        region_lookup["POSTAL_CODE"] = region_lookup["POSTAL_CODE"].astype(str).str.zfill(5)
        actual_df["POSTAL_CODE"] = actual_df["POSTAL_CODE"].astype(str).str.zfill(5)
        actual_df = actual_df.merge(region_lookup, on="POSTAL_CODE", how="left")
    else:
        actual_df["region_seq"] = pd.NA
        actual_df["new_region_name"] = pd.NA
        actual_df["area_type"] = pd.NA
    actual_df["assigned_sm_name"] = actual_df["assigned_sm_name"].replace("", pd.NA).fillna(actual_df["assigned_sm_name_master"])
    # Actual assignments are not changed, but display them in an OSRM-ordered
    # route so the map and route metrics reflect a practical technician tour.
    # Keep the home start fixed when it is available, matching area_map.
    actual_df["visit_seq"] = 0
    actual_df["actual_route_distance_km"] = 0.0
    actual_df["actual_route_duration_min"] = 0.0
    for (_, engineer_code), group in actual_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False, sort=False):
        group_indices = list(group.index)
        if not group_indices:
            continue
        start_coord = None
        first_row = actual_df.loc[group_indices[0]]
        if pd.notna(first_row.get("home_start_longitude")) and pd.notna(first_row.get("home_start_latitude")):
            start_coord = (float(first_row["home_start_longitude"]), float(first_row["home_start_latitude"]))
        stop_coords = [(float(actual_df.loc[idx, "longitude"]), float(actual_df.loc[idx, "latitude"])) for idx in group_indices]
        coord_chain = [start_coord] + stop_coords if start_coord is not None else stop_coords
        try:
            route_payload = get_route_client(strategic_city_name).build_ordered_route(
                tuple(coord_chain), preserve_first=start_coord is not None
            )
            ordered_coords = list(route_payload.get("ordered_coords", []))
        except Exception:
            route_payload = {"distance_km": 0.0, "duration_min": 0.0, "ordered_coords": coord_chain}
            ordered_coords = coord_chain
        # Match optimized stop coordinates back to source rows, preserving
        # duplicates by consuming each source row at most once.
        remaining = set(group_indices)
        ordered_indices: list[int] = []
        for coord in ordered_coords:
            if start_coord is not None and tuple(coord) == tuple(start_coord):
                continue
            candidates = [
                idx for idx in remaining
                if abs(float(actual_df.loc[idx, "longitude"]) - float(coord[0])) < 1e-7
                and abs(float(actual_df.loc[idx, "latitude"]) - float(coord[1])) < 1e-7
            ]
            if candidates:
                chosen = candidates[0]
                remaining.remove(chosen)
                ordered_indices.append(chosen)
        ordered_indices.extend(idx for idx in group_indices if idx in remaining)
        for sequence, idx in enumerate(ordered_indices, start=1):
            actual_df.at[idx, "visit_seq"] = sequence
        actual_df.loc[group_indices, "actual_route_distance_km"] = float(route_payload.get("distance_km", 0.0) or 0.0)
        actual_df.loc[group_indices, "actual_route_duration_min"] = float(route_payload.get("duration_min", 0.0) or 0.0)
    actual_df["visit_start_time"] = ""
    actual_df["visit_end_time"] = ""
    actual_df["travel_time_from_prev_min"] = pd.NA
    actual_df["assigned_region_name"] = pd.NA
    schedule_df = actual_df.sort_values(["service_date_key", "assigned_sm_code", "visit_seq"]).reset_index(drop=True)
    assignment_df = schedule_df.copy()
    return assignment_df, schedule_df


def _build_common_region_zip_df(subsidiary_name: str, strategic_city_name: str) -> pd.DataFrame:
    region_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/regions",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    if region_df.empty:
        return pd.DataFrame(columns=["POSTAL_CODE", "region_seq", "new_region_name", "area_type"])
    if "area_type" not in region_df.columns:
        region_df["area_type"] = ""
    return region_df.rename(columns={"postal_code": "POSTAL_CODE", "region_name": "new_region_name"})[
        ["POSTAL_CODE", "region_seq", "new_region_name", "area_type"]
    ].copy()


def _build_common_home_df(engineer_master_df: pd.DataFrame) -> pd.DataFrame:
    if engineer_master_df.empty:
        return pd.DataFrame(columns=["SVC_ENGINEER_CODE", "Name", "assigned_region_name", "latitude", "longitude"])
    return engineer_master_df.rename(
        columns={
            "employee_code": "SVC_ENGINEER_CODE",
            "employee_name": "Name",
            "home_latitude": "latitude",
            "home_longitude": "longitude",
        }
    ).assign(assigned_region_name=pd.NA)[["SVC_ENGINEER_CODE", "Name", "assigned_region_name", "latitude", "longitude"]].copy()


def _build_result_view_state(subsidiary_name: str, strategic_city_name: str) -> dict | None:
    st.session_state["common_result_strategic_city_name"] = strategic_city_name
    jobs_df = _load_local_jobs(subsidiary_name, strategic_city_name)
    engineer_master_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/engineers",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    payload = st.session_state.get("common_vrp_payload")
    status_payload = st.session_state.get("common_vrp_job_status") or {}
    result_payload = st.session_state.get("common_vrp_job_result")

    if payload is None and not status_payload and result_payload is None:
        return None
    if not result_payload:
        return {
            "payload": payload,
            "status_payload": status_payload,
            "result_payload": result_payload,
            "jobs_df": jobs_df,
            "engineer_master_df": engineer_master_df,
        }

    base_result_payload = result_payload
    force_assign_preview = _get_force_assignment_preview()
    result_payload = _apply_force_assignments_to_result_payload(result_payload, force_assign_preview)
    compare_mode = st.session_state.get("common_vrp_compare_mode", "Actual")
    region_zip_df = _build_common_region_zip_df(subsidiary_name, strategic_city_name)
    payload_jobs_df = _build_jobs_df_from_payload(payload)
    actual_jobs_df = _filter_jobs_df_for_payload(jobs_df, payload)
    if not payload_jobs_df.empty and not actual_jobs_df.empty and "job_slot_count" in actual_jobs_df.columns:
        slot_lookup = actual_jobs_df.drop_duplicates("gsfs_receipt_no").set_index("gsfs_receipt_no")["job_slot_count"]
        payload_jobs_df["job_slot_count"] = (
            payload_jobs_df["job_slot_count"].combine_first(payload_jobs_df["gsfs_receipt_no"].map(slot_lookup))
        )
        payload_jobs_df["job_slot_count"] = _coerce_job_slot_count_series(payload_jobs_df["job_slot_count"])
    result_jobs_df = payload_jobs_df if not payload_jobs_df.empty else actual_jobs_df
    assignment_df, schedule_df = _build_common_result_frames(result_payload, result_jobs_df, engineer_master_df, region_zip_df)
    if compare_mode == "Actual":
        assignment_df, schedule_df = _build_common_actual_frames(
            actual_jobs_df, engineer_master_df, region_zip_df, strategic_city_name
        )
    home_df = _build_common_home_df(engineer_master_df)
    unassigned_display_df = _build_unassigned_job_display_df(result_payload, result_jobs_df, region_zip_df)

    schedule_dates = set(schedule_df["service_date_key"].dropna().astype(str).unique().tolist()) if "service_date_key" in schedule_df.columns else set()
    unassigned_dates = set(unassigned_display_df["service_date_key"].dropna().astype(str).unique().tolist()) if "service_date_key" in unassigned_display_df.columns else set()
    available_dates = sorted(schedule_dates | unassigned_dates, reverse=True)
    schedule_regions = set(schedule_df["new_region_name"].dropna().astype(str).unique().tolist()) if "new_region_name" in schedule_df.columns else set()
    unassigned_regions = set(unassigned_display_df["new_region_name"].dropna().astype(str).unique().tolist()) if "new_region_name" in unassigned_display_df.columns else set()
    master_regions = set(region_zip_df["new_region_name"].dropna().astype(str).unique().tolist()) if not region_zip_df.empty and "new_region_name" in region_zip_df.columns else set()
    available_regions = ["ALL"] + sorted(master_regions | schedule_regions | unassigned_regions)
    engineer_options, engineer_label_to_code = _build_engineer_options(assignment_df)
    filtered_assignment = assignment_df.copy()
    filtered_schedule = schedule_df.copy()
    filtered_home = home_df.copy()
    filtered_unassigned = unassigned_display_df.copy()
    payload_planning_date = ""
    if isinstance(payload, dict):
        payload_planning_date = str(payload.get("planning_date", "")).strip()
    preferred_date = st.session_state.get("common_result_date")
    if preferred_date not in available_dates:
        preferred_date = payload_planning_date if payload_planning_date in available_dates else (available_dates[0] if available_dates else None)
    selected_date = preferred_date
    selected_region = st.session_state.get("common_result_region") if st.session_state.get("common_result_region") in available_regions else "ALL"
    selected_engineer_label = st.session_state.get("common_result_engineer") if st.session_state.get("common_result_engineer") in engineer_options else "ALL"
    selected_engineer_code = engineer_label_to_code.get(selected_engineer_label, "ALL")

    if selected_date:
        if "service_date_key" in filtered_assignment.columns:
            filtered_assignment = filtered_assignment[filtered_assignment["service_date_key"].astype(str) == str(selected_date)].copy()
        if "service_date_key" in filtered_schedule.columns:
            filtered_schedule = filtered_schedule[filtered_schedule["service_date_key"].astype(str) == str(selected_date)].copy()
        if not filtered_unassigned.empty and "service_date_key" in filtered_unassigned.columns:
            filtered_unassigned = filtered_unassigned[filtered_unassigned["service_date_key"].astype(str) == str(selected_date)].copy()
    if selected_region != "ALL":
        if "new_region_name" in filtered_assignment.columns:
            filtered_assignment = filtered_assignment[filtered_assignment["new_region_name"].astype(str) == str(selected_region)].copy()
        if "new_region_name" in filtered_schedule.columns:
            filtered_schedule = filtered_schedule[filtered_schedule["new_region_name"].astype(str) == str(selected_region)].copy()
        if not filtered_unassigned.empty and "new_region_name" in filtered_unassigned.columns:
            filtered_unassigned = filtered_unassigned[filtered_unassigned["new_region_name"].astype(str) == str(selected_region)].copy()
    if selected_engineer_code != "ALL":
        if "assigned_sm_code" in filtered_assignment.columns:
            filtered_assignment = filtered_assignment[filtered_assignment["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
        if "assigned_sm_code" in filtered_schedule.columns:
            filtered_schedule = filtered_schedule[filtered_schedule["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
        if "SVC_ENGINEER_CODE" in filtered_home.columns:
            filtered_home = filtered_home[filtered_home["SVC_ENGINEER_CODE"].astype(str) == str(selected_engineer_code)].copy()

    route_groups = _build_route_groups(filtered_schedule, strategic_city_name)
    service_count = int(filtered_assignment["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
    unassigned_count = int(filtered_unassigned["receipt_no"].dropna().astype(str).nunique()) if not filtered_unassigned.empty and "receipt_no" in filtered_unassigned.columns else 0
    engineer_count = int(filtered_assignment["assigned_sm_code"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
    dms_engineer_count = int(filtered_assignment.loc[filtered_assignment["assigned_center_type"].astype(str).str.upper() == "DMS", "assigned_sm_code"].astype(str).nunique()) if not filtered_assignment.empty and "assigned_center_type" in filtered_assignment.columns else 0
    dms2_engineer_count = int(filtered_assignment.loc[filtered_assignment["assigned_center_type"].astype(str).str.upper() == "DMS2", "assigned_sm_code"].astype(str).nunique()) if not filtered_assignment.empty and "assigned_center_type" in filtered_assignment.columns else 0
    route_distance_series = pd.Series([float(group["route_payload"]["distance_km"]) for group in route_groups], dtype=float)
    route_duration_series = pd.Series([float(group["route_payload"]["duration_min"]) for group in route_groups], dtype=float)
    avg_distance = float(route_distance_series.mean()) if not route_distance_series.empty else 0.0
    avg_duration = float(route_duration_series.mean()) if not route_duration_series.empty else 0.0
    if not filtered_assignment.empty:
        job_units_df = filtered_assignment.copy()
        if "job_slot_count" not in job_units_df.columns:
            job_units_df["job_slot_count"] = 1
        job_units_df["job_slot_count"] = _coerce_job_slot_count_series(job_units_df["job_slot_count"])
        job_units_df = job_units_df.drop_duplicates(subset=["assigned_sm_code", "GSFS_RECEIPT_NO"])
        jobs_per_engineer = job_units_df.groupby("assigned_sm_code", dropna=True)["GSFS_RECEIPT_NO"].nunique()
        slots_per_engineer = job_units_df.groupby("assigned_sm_code", dropna=True)["job_slot_count"].sum()
    else:
        jobs_per_engineer = pd.Series(dtype=float)
        slots_per_engineer = pd.Series(dtype=float)
    avg_jobs_per_engineer = float(jobs_per_engineer.mean()) if not jobs_per_engineer.empty else 0.0
    avg_slots_per_engineer = float(slots_per_engineer.mean()) if not slots_per_engineer.empty else 0.0
    jobs_std = float(jobs_per_engineer.std(ddof=0)) if not jobs_per_engineer.empty else 0.0
    slots_std = float(slots_per_engineer.std(ddof=0)) if not slots_per_engineer.empty else 0.0
    assigned_slot_count = int(slots_per_engineer.sum()) if not slots_per_engineer.empty else 0
    capacity_lookup = _build_engineer_slot_capacity_lookup(engineer_master_df)
    for engineer_code, capacity in _build_result_slot_capacity_lookup(result_payload).items():
        capacity_lookup.setdefault(str(engineer_code).strip(), int(capacity))
    assigned_slot_capacity = sum(capacity_lookup.get(str(engineer_code).strip(), 8) for engineer_code in slots_per_engineer.index)
    slot_occupancy_rate = float(assigned_slot_count / assigned_slot_capacity) if assigned_slot_capacity > 0 else 0.0
    center_type_stats: dict[str, dict[str, float | int]] = {}
    if not filtered_assignment.empty and "assigned_center_type" in filtered_assignment.columns:
        center_units_df = filtered_assignment.copy()
        if "job_slot_count" not in center_units_df.columns:
            center_units_df["job_slot_count"] = 1
        center_units_df["job_slot_count"] = _coerce_job_slot_count_series(center_units_df["job_slot_count"])
        center_units_df["assigned_center_type"] = center_units_df["assigned_center_type"].fillna("").astype(str).str.upper().replace("", "UNKNOWN")
        center_units_df = center_units_df.drop_duplicates(subset=["assigned_sm_code", "GSFS_RECEIPT_NO"])
        for center_type, center_df in center_units_df.groupby("assigned_center_type", dropna=False):
            center_jobs = center_df.groupby("assigned_sm_code", dropna=True)["GSFS_RECEIPT_NO"].nunique()
            center_slots = center_df.groupby("assigned_sm_code", dropna=True)["job_slot_count"].sum()
            center_assigned_slots = int(center_slots.sum()) if not center_slots.empty else 0
            center_capacity = sum(capacity_lookup.get(str(engineer_code).strip(), 8) for engineer_code in center_slots.index)
            center_type_stats[str(center_type)] = {
                "engineer_count": int(center_df["assigned_sm_code"].dropna().astype(str).nunique()),
                "job_count": int(center_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "avg_jobs": float(center_jobs.mean()) if not center_jobs.empty else 0.0,
                "avg_slots": float(center_slots.mean()) if not center_slots.empty else 0.0,
                "assigned_slots": center_assigned_slots,
                "slot_capacity": int(center_capacity),
                "fill_rate": float(center_assigned_slots / center_capacity) if center_capacity > 0 else 0.0,
                "jobs_std": float(center_jobs.std(ddof=0)) if not center_jobs.empty else 0.0,
                "slots_std": float(center_slots.std(ddof=0)) if not center_slots.empty else 0.0,
            }
    engineer_summary_rows: list[dict[str, object]] = []
    route_group_by_code = {str(group["engineer_code"]): group for group in route_groups}
    result_engineer_summary_df = pd.DataFrame(result_payload.get("engineer_summary", [])) if isinstance(result_payload, dict) else pd.DataFrame()
    result_summary_by_code: dict[str, dict[str, object]] = {}
    if not result_engineer_summary_df.empty:
        summary_code_col = "SVC_ENGINEER_CODE" if "SVC_ENGINEER_CODE" in result_engineer_summary_df.columns else "employee_code"
        if summary_code_col in result_engineer_summary_df.columns:
            result_summary_by_code = {
                str(row.get(summary_code_col, "")).strip(): row
                for row in result_engineer_summary_df.to_dict("records")
                if str(row.get(summary_code_col, "")).strip()
            }
    if not filtered_assignment.empty:
        for engineer_code, group in filtered_assignment.groupby("assigned_sm_code", dropna=True):
            group_units = group.drop_duplicates(subset=["assigned_sm_code", "GSFS_RECEIPT_NO"]).copy()
            if "job_slot_count" not in group_units.columns:
                group_units["job_slot_count"] = 1
            group_units["job_slot_count"] = _coerce_job_slot_count_series(group_units["job_slot_count"])
            route_group = route_group_by_code.get(str(engineer_code))
            route_duration_min = round(float(route_group["route_payload"]["duration_min"]), 2) if route_group else 0.0
            service_time_min = round(float(group_units.apply(_estimate_service_time_min, axis=1).sum()), 2)
            slot_count = int(group_units["job_slot_count"].sum())
            result_summary = result_summary_by_code.get(str(engineer_code).strip(), {})
            return_home_duration_min = float(pd.to_numeric(pd.Series([result_summary.get("return_home_duration_min", 0)]), errors="coerce").fillna(0).iloc[0])
            return_home_distance_km = float(pd.to_numeric(pd.Series([result_summary.get("return_home_distance_km", 0)]), errors="coerce").fillna(0).iloc[0])
            total_working_min = round(route_duration_min + service_time_min, 2)
            engineer_summary_rows.append(
                {
                    "Technician": str(group_units["assigned_sm_name"].iloc[0]) if "assigned_sm_name" in group_units.columns and not group_units.empty else str(engineer_code),
                    "job_count": int(group_units["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                    "slot_count": slot_count,
                    "route_distance_mile": round(float(route_group["route_payload"]["distance_km"]) * KM_TO_MILES, 2) if route_group else 0.0,
                    "return_home_distance_mile": round(return_home_distance_km * KM_TO_MILES, 2),
                    "service_time_min": service_time_min,
                    "total_working_min": total_working_min,
                    "total_day_duration_with_return_min": round(route_duration_min + service_time_min + return_home_duration_min, 2),
                }
            )
    engineer_summary_df = pd.DataFrame(engineer_summary_rows).sort_values(["slot_count", "job_count", "Technician"], ascending=[False, False, True]) if engineer_summary_rows else pd.DataFrame()
    if engineer_summary_rows and center_type_stats:
        for center_type, code_list in (
            filtered_assignment.dropna(subset=["assigned_sm_code"])
            .assign(assigned_center_type=lambda df: df["assigned_center_type"].fillna("").astype(str).str.upper().replace("", "UNKNOWN"))
            .groupby("assigned_center_type")["assigned_sm_code"]
            .apply(lambda s: sorted(set(s.astype(str))))
            .to_dict()
            .items()
        ):
            route_distance_values = []
            route_duration_values = []
            for code in code_list:
                route_group = route_group_by_code.get(str(code))
                if route_group:
                    route_distance_values.append(float(route_group["route_payload"]["distance_km"]))
                    route_duration_values.append(float(route_group["route_payload"]["duration_min"]))
            stats = center_type_stats.setdefault(str(center_type), {})
            stats["avg_distance"] = float(pd.Series(route_distance_values, dtype=float).mean()) if route_distance_values else 0.0
            stats["avg_duration"] = float(pd.Series(route_duration_values, dtype=float).mean()) if route_duration_values else 0.0
    staffing_df = _build_region_staffing_view(filtered_assignment)
    return {
        "payload": payload,
        "status_payload": status_payload,
        "base_result_payload": base_result_payload,
        "result_payload": result_payload,
        "force_assign_preview": force_assign_preview,
        "available_dates": available_dates,
        "available_regions": available_regions,
        "engineer_options": engineer_options,
        "service_count": service_count,
        "unassigned_count": unassigned_count,
        "compare_mode": compare_mode,
        "engineer_count": engineer_count,
        "dms_engineer_count": dms_engineer_count,
        "dms2_engineer_count": dms2_engineer_count,
        "avg_distance": avg_distance,
        "avg_duration": avg_duration,
        "avg_jobs_per_engineer": avg_jobs_per_engineer,
        "avg_slots_per_engineer": avg_slots_per_engineer,
        "assigned_slot_count": assigned_slot_count,
        "assigned_slot_capacity": assigned_slot_capacity,
        "slot_occupancy_rate": slot_occupancy_rate,
        "jobs_std": jobs_std,
        "slots_std": slots_std,
        "center_type_stats": center_type_stats,
        "staffing_df": staffing_df,
        "engineer_summary_df": engineer_summary_df,
        "region_zip_df": region_zip_df,
        "result_jobs_df": result_jobs_df,
        "engineer_master_df": engineer_master_df,
        "unassigned_display_df": unassigned_display_df,
        "filtered_assignment": filtered_assignment,
        "filtered_schedule": filtered_schedule,
        "filtered_home": filtered_home,
        "filtered_unassigned": filtered_unassigned,
        "selected_date": selected_date,
        "selected_region": selected_region,
    }


def _parse_history_date(value: object) -> date | None:
    text = str(value or "").strip().replace("-", "")
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _load_period_statistics(
    subsidiary_name: str,
    strategic_city_name: str,
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame | list[str]]:
    history_dates = _api_get(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/routing/history-dates",
        subsidiary_name=subsidiary_name,
        strategic_city_name=strategic_city_name,
    ).get("rows", [])
    selected_dates = []
    for raw_date in history_dates:
        parsed = _parse_history_date(raw_date)
        if parsed is not None and start_date <= parsed <= end_date:
            selected_dates.append(parsed.strftime("%Y%m%d"))

    jobs_store = _load_local_jobs(subsidiary_name, strategic_city_name)
    engineer_master_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/engineers",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    region_zip_df = _build_common_region_zip_df(subsidiary_name, strategic_city_name)
    daily_rows: list[dict[str, object]] = []
    technician_frames: list[pd.DataFrame] = []
    job_frames: list[pd.DataFrame] = []
    assignment_frames: list[pd.DataFrame] = []
    unassigned_frames: list[pd.DataFrame] = []
    availability_by_date: dict[tuple[str, str], bool] = {}
    errors: list[str] = []

    for promise_date in selected_dates:
        try:
            snapshot = _api_get(
                DEFAULT_COMMON_SERVER_URL,
                "/api/v1/common/routing/latest",
                subsidiary_name=subsidiary_name,
                strategic_city_name=strategic_city_name,
                promise_date=promise_date,
            ).get("snapshot") or {}
            request_row = dict(snapshot.get("request") or {})
            payload_text = str(request_row.get("payload_json", "") or "").strip()
            payload = json.loads(payload_text) if payload_text else None
            if isinstance(payload, dict):
                service_date_key = f"{promise_date[:4]}-{promise_date[4:6]}-{promise_date[6:8]}"
                request_technicians = _api_get(
                    DEFAULT_COMMON_SERVER_URL,
                    "/api/v1/common/technicians",
                    subsidiary_name=subsidiary_name,
                    strategic_city_name=strategic_city_name,
                    promise_date=promise_date,
                ).get("rows", [])
                for technician in list(payload.get("technicians", []) or []) + list(request_technicians or []):
                    code = str(technician.get("employee_code", technician.get("SVC_ENGINEER_CODE", ""))).strip()
                    if code:
                        active = _coerce_bool_value(technician.get("active_flag", True))
                        available = _coerce_bool_value(technician.get("available", active)) and active
                        availability_by_date[(service_date_key, code)] = available
            result_payload = snapshot.get("result") or {}
            if not isinstance(result_payload, dict) or not result_payload:
                continue
            payload_jobs_df = _build_jobs_df_from_payload(payload) if isinstance(payload, dict) else pd.DataFrame()
            if payload_jobs_df.empty and not jobs_store.empty and "promise_date" in jobs_store.columns:
                payload_jobs_df = jobs_store[
                    jobs_store["promise_date"].astype(str).str.replace("-", "", regex=False).eq(promise_date)
                ].copy()
            if not payload_jobs_df.empty:
                payload_jobs_df["service_date_key"] = f"{promise_date[:4]}-{promise_date[4:6]}-{promise_date[6:8]}"
                job_frames.append(payload_jobs_df.copy())
            assignment_df, schedule_df = _build_common_result_frames(
                result_payload, payload_jobs_df, engineer_master_df, region_zip_df
            )
            if not assignment_df.empty:
                service_date = f"{promise_date[:4]}-{promise_date[4:6]}-{promise_date[6:8]}"
                assignment_df["service_date_key"] = service_date
                schedule_df["service_date_key"] = service_date
                assignment_frames.append(schedule_df.copy())
            unassigned_df = _build_unassigned_job_display_df(result_payload, payload_jobs_df, region_zip_df)
            if not unassigned_df.empty:
                unassigned_df["service_date_key"] = f"{promise_date[:4]}-{promise_date[4:6]}-{promise_date[6:8]}"
                unassigned_frames.append(unassigned_df.copy())
            summary_df = pd.DataFrame(result_payload.get("engineer_summary", []))
            if not summary_df.empty:
                summary_df["service_date_key"] = summary_df.get(
                    "service_date_key", f"{promise_date[:4]}-{promise_date[4:6]}-{promise_date[6:8]}"
                )
                technician_frames.append(summary_df.copy())
            total_jobs = int(
                payload_jobs_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()
                if "GSFS_RECEIPT_NO" in payload_jobs_df.columns and not payload_jobs_df.empty
                else (result_payload.get("summary") or {}).get("total_jobs", 0)
            )
            assigned_jobs = int(
                assignment_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()
                if not assignment_df.empty and "GSFS_RECEIPT_NO" in assignment_df.columns
                else (result_payload.get("summary") or {}).get("assigned_jobs", 0)
            )
            slots = int(pd.to_numeric(payload_jobs_df.get("job_slot_count", pd.Series(dtype=float)), errors="coerce").fillna(1).sum()) if not payload_jobs_df.empty else 0
            daily_rows.append(
                {
                    "service_date": f"{promise_date[:4]}-{promise_date[4:6]}-{promise_date[6:8]}",
                    "jobs": total_jobs,
                    "assigned_jobs": assigned_jobs,
                    "unassigned_jobs": max(0, total_jobs - assigned_jobs),
                    "slots": slots,
                    "technicians": int(assignment_df["assigned_sm_code"].nunique()) if not assignment_df.empty else 0,
                    "distance_km": float(pd.to_numeric(summary_df.get("route_distance_km", pd.Series(dtype=float)), errors="coerce").sum()) if not summary_df.empty else 0.0,
                    "duration_min": float(pd.to_numeric(summary_df.get("route_duration_min", pd.Series(dtype=float)), errors="coerce").sum()) if not summary_df.empty else 0.0,
                }
            )
        except Exception as exc:
            errors.append(f"{promise_date}: {exc}")

    jobs_df = pd.concat(job_frames, ignore_index=True) if job_frames else pd.DataFrame()
    technicians_df = pd.concat(technician_frames, ignore_index=True) if technician_frames else engineer_master_df.copy()
    assignments_df = pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame()
    unassigned_df = pd.concat(unassigned_frames, ignore_index=True) if unassigned_frames else pd.DataFrame()
    unassigned_analysis_df = _build_unassigned_analysis_df(
        unassigned_df, engineer_master_df, assignments_df, availability_by_date
    )
    statistics_df = pd.DataFrame(daily_rows)
    if not statistics_df.empty:
        statistics_df = pd.concat(
            [statistics_df, pd.DataFrame([{
                "service_date": "TOTAL",
                "jobs": int(statistics_df["jobs"].sum()),
                "assigned_jobs": int(statistics_df["assigned_jobs"].sum()),
                "unassigned_jobs": int(statistics_df["unassigned_jobs"].sum()),
                "slots": int(statistics_df["slots"].sum()),
                "technicians": int(assignments_df["assigned_sm_code"].astype(str).nunique()) if not assignments_df.empty and "assigned_sm_code" in assignments_df.columns else int(statistics_df["technicians"].sum()),
                "distance_km": round(float(statistics_df["distance_km"].sum()), 2),
                "duration_min": round(float(statistics_df["duration_min"].sum()), 2),
            }])],
            ignore_index=True,
        )
    return {"statistics": statistics_df, "jobs": jobs_df, "technicians": technicians_df, "assignments": assignments_df, "unassigned": unassigned_df, "unassigned_analysis": unassigned_analysis_df, "errors": errors, "dates": selected_dates}


def _render_statistics_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    st.subheader("Statistics")
    history_dates = _api_get(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/routing/history-dates",
        subsidiary_name=subsidiary_name,
        strategic_city_name=strategic_city_name,
    ).get("rows", [])
    parsed_dates = sorted([parsed for parsed in (_parse_history_date(value) for value in history_dates) if parsed])
    if not parsed_dates:
        st.info("No saved routing results for the selected city.")
        return
    start_col, end_col, button_col = st.columns([1, 1, 0.8])
    start_date = start_col.date_input("Start Date", value=parsed_dates[0], min_value=parsed_dates[0], max_value=parsed_dates[-1], key="server_statistics_start_date")
    end_date = end_col.date_input("End Date", value=parsed_dates[-1], min_value=parsed_dates[0], max_value=parsed_dates[-1], key="server_statistics_end_date")
    if button_col.button("조회", type="primary", width="stretch", key="server_statistics_query_button"):
        if start_date > end_date:
            st.error("Start Date must be on or before End Date.")
        else:
            with st.spinner("Loading routing statistics..."):
                statistics_state = _load_period_statistics(subsidiary_name, strategic_city_name, start_date, end_date)
                statistics_state["start_date"] = start_date.isoformat()
                statistics_state["end_date"] = end_date.isoformat()
                st.session_state["server_common_statistics_state"] = statistics_state
                st.session_state["server_common_statistics_view_active"] = True
    statistics_state = st.session_state.get("server_common_statistics_state")
    if not statistics_state:
        st.info("Select a period and click 조회.")
        return


def _render_statistics_panel() -> None:
    st.subheader("Routing Statistics")
    statistics_state = st.session_state.get("server_common_statistics_state")
    if not statistics_state:
        st.info("Select a period in the Statistics tab and click 조회.")
        return
    if statistics_state.get("errors"):
        st.warning("Some dates could not be loaded: " + "; ".join(str(value) for value in statistics_state["errors"][:3]))
    statistics_df = statistics_state.get("statistics", pd.DataFrame())
    if statistics_df.empty:
        st.info("No routing results were found for the selected period.")
        return
    st.dataframe(statistics_df, width="stretch", hide_index=True)
    analysis_df = statistics_state.get("unassigned_analysis", pd.DataFrame())
    if not analysis_df.empty:
        st.subheader("Unassigned Analysis")
        st.dataframe(
            analysis_df[["promise_date", "receipt_no", "reason", "overall_diagnosis", "postal_code", "address_line1_info"]].drop_duplicates(["promise_date", "receipt_no"]),
            width="stretch",
            hide_index=True,
        )
        st.caption("Nearest five technicians by home-to-job straight-line distance")
        st.dataframe(analysis_df, width="stretch", hide_index=True)
    sheets = {
        "Statistics": statistics_df,
        "Jobs": statistics_state.get("jobs", pd.DataFrame()),
        "Technicians": statistics_state.get("technicians", pd.DataFrame()),
        "Assignment Result": _build_simple_assignment_export_df(statistics_state.get("assignments", pd.DataFrame()), statistics_state.get("unassigned", pd.DataFrame())),
        "Assignment CSV": statistics_state.get("assignments", pd.DataFrame()),
        "Unassigned": statistics_state.get("unassigned", pd.DataFrame()),
        "Unassigned Analysis": analysis_df,
    }
    start_text = str(statistics_state.get("start_date", "")).replace("-", "")
    end_text = str(statistics_state.get("end_date", "")).replace("-", "")
    st.download_button(
        "Download Period Statistics XLSX",
        data=_to_multi_sheet_xlsx_bytes(sheets),
        file_name=f"routing_statistics_{start_text}_{end_text}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )


def _render_result_summary(subsidiary_name: str, strategic_city_name: str) -> None:
    st.subheader("Routing Result")
    state = _build_result_view_state(subsidiary_name, strategic_city_name)
    if state is None:
        st.info("Build payload and run routing to see the result.")
        return
    status_payload = state.get("status_payload") or {}
    result_payload = state.get("result_payload")
    st.caption(f"Routing status: {str(status_payload.get('status', '')).strip().lower()}")
    if status_payload.get("error_message"):
        st.error(str(status_payload.get("error_message")))
    if not result_payload:
        st.info("Routing result is not ready yet.")
        return
    if str(status_payload.get("status", "")).strip().lower() == "completed":
        st.caption("Smart Routing job completed.")
        view_options = ["Actual", "Smart Routing"]
        pending_compare_mode = st.session_state.pop("common_vrp_compare_mode_pending", None)
        if pending_compare_mode in view_options:
            st.session_state["common_vrp_compare_mode"] = pending_compare_mode
        st.radio("Assignment View", view_options, horizontal=True, key="common_vrp_compare_mode")
    center_type_stats = state.get("center_type_stats") or {}
    dms_service_count = int((center_type_stats.get("DMS") or {}).get("job_count", 0))
    dms2_service_count = int((center_type_stats.get("DMS2") or {}).get("job_count", 0))
    has_dms2_service = bool((center_type_stats.get("DMS2") or {}) or state.get("dms2_engineer_count", 0))
    if state.get("compare_mode") == "Smart Routing":
        total_service_count = state["service_count"] + state.get("unassigned_count", 0)
        service_detail = (
            f"DMS {dms_service_count} / DMS2 {dms2_service_count} / Not Assigned {state.get('unassigned_count', 0)}"
            if has_dms2_service
            else f"Assigned {state['service_count']} / Not Assigned {state.get('unassigned_count', 0)}"
        )
        st.metric("Service Count", f"{total_service_count} ({service_detail})")
    else:
        service_detail = f"DMS {dms_service_count} / DMS2 {dms2_service_count}" if has_dms2_service else None
        st.metric("Service Count", state["service_count"], help=service_detail)
        if service_detail:
            st.caption(service_detail)
    st.metric("Assigned Technician Count", f"{state['engineer_count']} (DMS {state['dms_engineer_count']}, DMS2 {state['dms2_engineer_count']})")

    def _center_metric_lines(metric: str) -> str:
        lines: list[str] = []
        for center_type in ["DMS", "DMS2"]:
            stats = center_type_stats.get(center_type) or {}
            if not stats:
                continue
            if metric == "jobs_slots":
                lines.append(
                    f"{center_type}: Avg. jobs {float(stats.get('avg_jobs', 0.0)):.1f} / "
                    f"Avg. slots {float(stats.get('avg_slots', 0.0)):.1f} / "
                    f"Fill Rate {float(stats.get('fill_rate', 0.0)) * 100:.1f}% "
                    f"({int(stats.get('assigned_slots', 0))}/{int(stats.get('slot_capacity', 0))})"
                )
            elif metric == "distance":
                lines.append(f"{center_type}: {float(stats.get('avg_distance', 0.0)) * KM_TO_MILES:.2f}")
            elif metric == "duration":
                lines.append(f"{center_type}: {float(stats.get('avg_duration', 0.0)):.2f}")
            elif metric == "std":
                lines.append(
                    f"{center_type}: Jobs std {float(stats.get('jobs_std', 0.0)):.2f} / "
                    f"Slots std {float(stats.get('slots_std', 0.0)):.2f}"
                )
        return "\n".join(lines)

    jobs_slots_detail = _center_metric_lines("jobs_slots")
    st.metric(
        "Average Jobs / Slots",
        f"Avg. jobs : {state['avg_jobs_per_engineer']:.1f}   /   Avg. slots : {state['avg_slots_per_engineer']:.1f}"
        f"   /   Fill Rate : {state['slot_occupancy_rate'] * 100:.1f}%"
        f" ({state['assigned_slot_count']}/{state['assigned_slot_capacity']})",
        help=jobs_slots_detail or None,
    )
    if jobs_slots_detail:
        st.caption(jobs_slots_detail)
    distance_detail = _center_metric_lines("distance")
    st.metric("Average Distance (mile)", f"{state['avg_distance'] * KM_TO_MILES:.2f}", help=distance_detail or None)
    if distance_detail:
        st.caption(distance_detail)
    duration_detail = _center_metric_lines("duration")
    st.metric("Average Duration (min)", f"{state['avg_duration']:.2f}", help=duration_detail or None)
    if duration_detail:
        st.caption(duration_detail)
    std_detail = _center_metric_lines("std")
    st.metric(
        "Jobs / Slots per Technician Std",
        f"Jobs std : {state['jobs_std']:.2f}   /   Slots std : {state['slots_std']:.2f}",
        help=std_detail or None,
    )
    if std_detail:
        st.caption(std_detail)
    diagnostics = (state.get("result_payload") or {}).get("diagnostics", {}) if isinstance(state.get("result_payload"), dict) else {}
    if diagnostics:
        condition_messages = diagnostics.get("routing_condition_messages", []) or []
        for message in condition_messages:
            message_text = str(message).strip()
            if not message_text:
                continue
            if message_text.startswith("Standard routing"):
                st.success(message_text)
            else:
                st.warning(message_text)
        st.caption(
            "Routing diagnostics: "
            f"fixed {diagnostics.get('fixed_job_count', 0)}, "
            f"reschedule {diagnostics.get('reschedule_job_count', 0)}, "
            f"mandatory {diagnostics.get('mandatory_job_count', 0)}, "
            f"job slots {diagnostics.get('total_job_slots', 0)}, "
            f"tech slots {diagnostics.get('total_technician_slots', 0)}"
        )
        with st.expander("Routing Diagnostics", expanded=False):
            st.json(diagnostics)
    unassigned_df = state.get("filtered_unassigned", pd.DataFrame())
    force_assign_preview = state.get("force_assign_preview", {})
    technician_options, technician_code_by_label = _build_force_assign_technician_options(
        state.get("engineer_master_df", pd.DataFrame())
    )
    technician_label_by_code = {code: label for label, code in technician_code_by_label.items()}
    if not unassigned_df.empty:
        st.warning(f"Unassigned jobs: {len(unassigned_df)}")
        if not technician_options:
            st.info("No available technicians for force assignment.")
        for _, row in unassigned_df.iterrows():
            receipt_no = str(row.get("receipt_no", "")).strip()
            with st.container(border=True):
                st.markdown(f"**{receipt_no}**")
                st.caption(
                    " | ".join(
                        [
                            str(row.get("city_name", "")).strip(),
                            str(row.get("postal_code", "")).strip(),
                            str(row.get("address_line1_info", "")).strip(),
                        ]
                    )
                )
                st.caption(
                    f"{str(row.get('service_product_group_code', '')).strip()} / "
                    f"{str(row.get('service_product_code', '')).strip()} | "
                    f"slot {str(row.get('job_slot_count', '')).strip()} | "
                    f"{str(row.get('reason', '')).strip()}"
                )
                tech_col, button_col = st.columns([2.3, 1])
                selected_technician = ""
                if technician_options:
                    selected_technician = tech_col.selectbox(
                        "Assign technician",
                        technician_options,
                        key=f"force_assign_tech_{receipt_no}",
                        label_visibility="collapsed",
                    )
                if button_col.button(
                    "Preview Assign",
                    key=f"force_assign_button_{receipt_no}",
                    width="stretch",
                    disabled=not technician_options,
                ):
                    _preview_force_assign_unassigned_job(receipt_no, technician_code_by_label.get(selected_technician, ""))
                    st.success(f"Preview assigned {receipt_no}.")
                    st.rerun()
    if force_assign_preview:
        st.info(f"Pending force assignments: {len(force_assign_preview)}. Review the map and summary, then save at the bottom.")
        for receipt_no, employee_code in sorted(force_assign_preview.items()):
            with st.container(border=True):
                st.markdown(f"**{receipt_no}**")
                current_label = technician_label_by_code.get(str(employee_code), "")
                current_index = technician_options.index(current_label) if current_label in technician_options else 0
                tech_col, update_col, remove_col = st.columns([2.3, 1, 1])
                selected_technician = ""
                if technician_options:
                    selected_technician = tech_col.selectbox(
                        "Pending technician",
                        technician_options,
                        index=current_index,
                        key=f"pending_force_assign_tech_{receipt_no}",
                        label_visibility="collapsed",
                    )
                if update_col.button(
                    "Update",
                    key=f"pending_force_assign_update_{receipt_no}",
                    width="stretch",
                    disabled=not technician_options,
                ):
                    _preview_force_assign_unassigned_job(receipt_no, technician_code_by_label.get(selected_technician, ""))
                    st.rerun()
                if remove_col.button("Remove", key=f"pending_force_assign_remove_{receipt_no}", width="stretch"):
                    _remove_force_assignment_preview(receipt_no)
                    st.rerun()
    if not state["staffing_df"].empty:
        st.markdown("**Area Staffing / Jobs**")
        st.dataframe(state["staffing_df"], width="stretch", hide_index=True)
    if not state["engineer_summary_df"].empty:
        st.markdown("**Technician Summary**")
        st.dataframe(state["engineer_summary_df"], width="stretch", hide_index=True)


def _render_result_detail(subsidiary_name: str, strategic_city_name: str) -> None:
    state = _build_result_view_state(subsidiary_name, strategic_city_name)
    if state is None:
        return
    result_payload = state.get("result_payload")
    if not result_payload:
        return
    available_dates = state["available_dates"]
    available_regions = state["available_regions"]
    engineer_options = state["engineer_options"]
    preferred_date = state.get("selected_date")
    if preferred_date is not None and st.session_state.get("common_result_date") not in available_dates:
        st.session_state["common_result_date"] = preferred_date
    selected_date_col, selected_region_col, selected_engineer_col = st.columns(3)
    selected_date_col.selectbox("Date", options=available_dates, index=0 if available_dates else None, key="common_result_date")
    selected_region_col.selectbox("Area", options=available_regions, index=0, key="common_result_region")
    selected_engineer_col.selectbox("Technician", options=engineer_options, index=0, key="common_result_engineer")
    state = _build_result_view_state(subsidiary_name, strategic_city_name)
    filtered_assignment = state["filtered_assignment"]
    filtered_schedule = state["filtered_schedule"]
    filtered_home = state["filtered_home"]
    filtered_unassigned = state.get("filtered_unassigned", pd.DataFrame())
    selected_region = state["selected_region"]
    region_zip_df = state["region_zip_df"]
    if not filtered_assignment.empty or not filtered_unassigned.empty:
        route_groups = _build_route_groups(filtered_schedule, strategic_city_name)
        map_obj = build_map(
            strategic_city_name,
            selected_region,
            filtered_assignment,
            filtered_home,
            route_groups,
            region_zip_df,
            filtered_unassigned,
        )
        _render_folium_map(map_obj, height=700)
        simple_export_df = _build_simple_assignment_export_df(filtered_schedule, filtered_unassigned)
        if not simple_export_df.empty:
            st.download_button(
                "Download Assignment Result XLSX",
                data=_to_xlsx_bytes(simple_export_df),
                file_name=f"{st.session_state.get('common_vrp_job_id', 'common_vrp_job')}_assignment_result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )
    if not filtered_assignment.empty:
        st.subheader("Selected Schedule")
        display_cols = [
            "service_date_key",
            "assigned_sm_name",
            "assigned_sm_code",
            "GSFS_RECEIPT_NO",
            "fixed",
            "reschedule",
            "changed",
            "visit_seq",
            "visit_start_time",
            "visit_end_time",
            "job_slot_count",
            "SERVICE_PRODUCT_GROUP_CODE",
            "SERVICE_PRODUCT_CODE",
            "assigned_center_type",
            "new_region_name",
        ]
        display_cols = [col for col in display_cols if col in filtered_schedule.columns]
        schedule_display_df = filtered_schedule[display_cols].rename(
            columns={
                "assigned_sm_name": "technician_name",
                "assigned_sm_code": "technician_code",
                "new_region_name": "area",
            }
        )
        st.dataframe(schedule_display_df, width="stretch", hide_index=True)
        st.download_button(
            "Download Assignment CSV",
            data=_to_csv_bytes(filtered_schedule),
            file_name=f"{st.session_state.get('common_vrp_job_id', 'common_vrp_job')}_schedule.csv",
            mime="text/csv",
            width="stretch",
        )
    _render_force_assignment_preview_actions()


def _merge_technician_rows(existing_df: pd.DataFrame, new_rows: list[dict]) -> pd.DataFrame:
    new_df = pd.DataFrame(new_rows)
    if existing_df.empty:
        return new_df.copy()
    existing = existing_df.copy()
    if "employee_code" in existing.columns:
        existing = existing[~existing["employee_code"].astype(str).isin(new_df["employee_code"].astype(str))].copy()
    return pd.concat([existing, new_df], ignore_index=True)


def _build_default_technician_rows_from_jobs(jobs_df: pd.DataFrame, engineer_master_df: pd.DataFrame, subsidiary_name: str, strategic_city_name: str) -> list[dict]:
    if jobs_df.empty:
        return []
    unique_jobs = jobs_df[["svc_engineer_code", "svc_engineer_name"]].dropna().drop_duplicates().copy()
    master_lookup = _build_technician_master_lookup(engineer_master_df)
    rows: list[dict] = []
    for _, row in unique_jobs.iterrows():
        code = str(row["svc_engineer_code"]).strip()
        if not code:
            continue
        is_master_technician = code in master_lookup
        master_row = master_lookup.get(code, {})
        center_type = (_clean_text(master_row.get("center_type")) or "DMS").upper()
        active_flag = _coerce_bool_value(master_row.get("active_flag", True)) if is_master_technician else False
        rows.append(
            {
                "record_id": uuid.uuid4().hex,
                "subsidiary_name": subsidiary_name,
                "strategic_city_name": strategic_city_name,
                "employee_code": code,
                "employee_name": str(row["svc_engineer_name"]).strip(),
                "center_type": center_type,
                "shift_start": "09:00",
                "shift_end": "18:00",
                "slot_count": 8,
                "priority_group": _coerce_priority_group_value(master_row.get("priority_group", "B")),
                "available": active_flag,
                "start_location_type": "Home",
                "start_location_address": _clean_text(master_row.get("home_address")),
                "start_latitude": None,
                "start_longitude": None,
                "source": "same_as_jobs",
            }
        )
    return rows


def _build_default_technician_rows_from_master(engineer_master_df: pd.DataFrame, subsidiary_name: str, strategic_city_name: str) -> list[dict]:
    if engineer_master_df.empty:
        return []
    master_lookup = _build_technician_master_lookup(engineer_master_df)
    expected_state = _normalize_state_code(strategic_city_name)
    rows: list[dict] = []
    for _, row in engineer_master_df.drop_duplicates(subset=["employee_code"]).iterrows():
        employee_code = str(row["employee_code"]).strip()
        master_row = master_lookup.get(employee_code, {})
        home_state = _clean_text(master_row.get("home_state")) or _clean_text(row.get("home_state"))
        if expected_state and _normalize_state_code(home_state) != expected_state:
            continue
        active_flag = _coerce_bool_value(master_row.get("active_flag", row.get("active_flag", True)))
        rows.append(
            {
                "record_id": uuid.uuid4().hex,
                "subsidiary_name": subsidiary_name,
                "strategic_city_name": strategic_city_name,
                "employee_code": employee_code,
                "employee_name": str(row["employee_name"]).strip(),
                "center_type": str(row.get("center_type", "DMS")).strip().upper() or "DMS",
                "shift_start": "09:00",
                "shift_end": "18:00",
                "slot_count": 8,
                "priority_group": _coerce_priority_group_value(master_row.get("priority_group", row.get("priority_group", "B"))),
                "available": active_flag,
                "start_location_type": "Home",
                "start_location_address": _clean_text(master_row.get("home_address")),
                "start_latitude": None,
                "start_longitude": None,
                "source": "all_technicians",
            }
        )
    return rows


@st.dialog("Technician Master", width="large")
def _technician_master_dialog(subsidiary_name: str, strategic_city_name: str) -> None:
    engineer_master_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/engineers",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    display_cols = [
        "employee_code",
        "employee_name",
        "center_type",
        "home_address",
        "home_city",
        "home_state",
        "home_postal_code",
        "home_latitude",
        "home_longitude",
        "active_flag",
        "priority_group",
        "home_to_job_relaxed",
        "max_home_to_job_min",
    ]
    if engineer_master_df.empty:
        st.info("No technician master rows for the selected city.")
    else:
        for col in display_cols:
            if col not in engineer_master_df.columns:
                engineer_master_df[col] = ""
        engineer_master_df["priority_group"] = engineer_master_df["priority_group"].map(_coerce_priority_group_value)
        engineer_master_df["home_to_job_relaxed"] = pd.to_numeric(
            engineer_master_df["max_home_to_job_min"], errors="coerce"
        ).lt(0).fillna(False)
        edited_master_df = st.data_editor(
            engineer_master_df[display_cols],
            width="stretch",
            hide_index=True,
            disabled=[
                col
                for col in display_cols
                if col not in {"active_flag", "priority_group", "home_to_job_relaxed", "max_home_to_job_min"}
            ],
            column_config={
                "employee_code": st.column_config.TextColumn("technician_code"),
                "employee_name": st.column_config.TextColumn("technician_name"),
                "active_flag": st.column_config.CheckboxColumn("active_flag"),
                "priority_group": st.column_config.SelectboxColumn(
                    "priority_group",
                    options=["A", "B", "C"],
                    help="A is highest priority, C is lowest.",
                ),
                "home_to_job_relaxed": st.column_config.CheckboxColumn(
                    "outer technician override",
                    help="Checked stores -1. Routing treats it as relaxed home-to-job and first-leg limits.",
                ),
                "home_latitude": st.column_config.NumberColumn("home_latitude", format="%.6f"),
                "home_longitude": st.column_config.NumberColumn("home_longitude", format="%.6f"),
                "max_home_to_job_min": st.column_config.NumberColumn(
                    "custom home-to-job min",
                    help="Blank uses city default. If outer technician override is checked, this is saved as -1.",
                    step=5,
                ),
            },
            key=f"technician_master_view::{subsidiary_name}::{strategic_city_name}",
        )
        if st.button("Save Master Changes", type="primary", width="stretch"):
            try:
                for idx, edited_row in edited_master_df.reset_index(drop=True).iterrows():
                    update_row = engineer_master_df.iloc[int(idx)].to_dict()
                    update_row["active_flag"] = _coerce_bool_value(edited_row.get("active_flag", True))
                    update_row["priority_group"] = _coerce_priority_group_value(edited_row.get("priority_group", "B"))
                    if _coerce_bool_value(edited_row.get("home_to_job_relaxed", False)):
                        update_row["max_home_to_job_min"] = -1
                    else:
                        override_value = pd.to_numeric(pd.Series([edited_row.get("max_home_to_job_min")]), errors="coerce").iloc[0]
                        update_row["max_home_to_job_min"] = int(override_value) if pd.notna(override_value) and int(override_value) >= 0 else None
                    update_row.pop("home_to_job_relaxed", None)
                    _api_post(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/engineers/upsert", update_row)
                st.cache_data.clear()
                st.success("Technician master updated.")
            except Exception as exc:
                st.error(str(exc))
    master_message_key = f"technician_master_message::{subsidiary_name}::{strategic_city_name}"
    if st.session_state.get(master_message_key):
        st.success(str(st.session_state.pop(master_message_key)))

    st.divider()
    st.subheader("Bulk Upload New Technicians")
    master_upload_col, master_save_col = st.columns([2.4, 1])
    with master_upload_col:
        uploaded_master_file = st.file_uploader(
            "Upload Technician Master CSV",
            type=["csv"],
            label_visibility="collapsed",
            key=f"technician_master_csv::{subsidiary_name}::{strategic_city_name}",
        )
    if master_save_col.button("Upload Master CSV", type="secondary", width="stretch"):
        if uploaded_master_file is None:
            st.warning("Upload a technician master CSV file first.")
        else:
            try:
                upload_master_df = _read_uploaded_technician_master_csv(
                    uploaded_master_file,
                    subsidiary_name,
                    strategic_city_name,
                )
                saved_count = 0
                for row in upload_master_df.to_dict("records"):
                    response = _api_post(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/engineers/upsert", row)
                    saved_count += int(response.get("saved_rows", 0))
                st.cache_data.clear()
                st.session_state[master_message_key] = f"Technician master upload completed: {saved_count} rows saved."
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    st.divider()
    st.subheader("Add Or Update Technician")
    selected_row: pd.Series | None = None
    if not engineer_master_df.empty:
        edit_options = ["New Technician"] + (
            engineer_master_df["employee_name"].fillna("").astype(str)
            + " ("
            + engineer_master_df["employee_code"].fillna("").astype(str)
            + ")"
        ).tolist()
        selected_edit_label = st.selectbox("Edit Existing Technician", edit_options)
        selected_edit_idx = edit_options.index(selected_edit_label)
        if selected_edit_idx > 0:
            selected_row = engineer_master_df.iloc[selected_edit_idx - 1]

    default_employee_code = _clean_text(selected_row.get("employee_code")) if selected_row is not None else ""
    default_employee_name = _clean_text(selected_row.get("employee_name")) if selected_row is not None else ""
    default_center_type = _clean_text(selected_row.get("center_type")) if selected_row is not None else "DMS"
    default_home_address = _clean_text(selected_row.get("home_address")) if selected_row is not None else ""
    default_home_city = _clean_text(selected_row.get("home_city")) if selected_row is not None else str(strategic_city_name).split(",")[0].strip()
    default_home_state = _clean_text(selected_row.get("home_state")) if selected_row is not None else ""
    default_home_postal_code = _clean_text(selected_row.get("home_postal_code")) if selected_row is not None else ""
    default_active_flag = _coerce_bool_value(selected_row.get("active_flag", True)) if selected_row is not None else True
    default_priority_group = _coerce_priority_group_value(selected_row.get("priority_group", "B")) if selected_row is not None else "B"
    default_max_home_to_job_min = pd.to_numeric(pd.Series([selected_row.get("max_home_to_job_min")]), errors="coerce").iloc[0] if selected_row is not None else pd.NA
    default_home_to_job_relaxed = bool(pd.notna(default_max_home_to_job_min) and float(default_max_home_to_job_min) < 0)
    center_options = ["DMS", "DMS2"]
    center_index = center_options.index(default_center_type) if default_center_type in center_options else 0
    priority_options = ["A", "B", "C"]
    priority_index = priority_options.index(default_priority_group) if default_priority_group in priority_options else 1

    with st.form(f"technician_master_upsert::{subsidiary_name}::{strategic_city_name}", clear_on_submit=False):
        code_col, name_col, center_col, priority_col = st.columns([1, 1.4, 0.8, 0.8])
        employee_code = code_col.text_input("employee_code", value=default_employee_code, disabled=selected_row is not None)
        employee_name = name_col.text_input("employee_name", value=default_employee_name)
        center_type = center_col.selectbox("center_type", center_options, index=center_index)
        priority_group = priority_col.selectbox("priority_group", priority_options, index=priority_index)
        home_to_job_relaxed = st.checkbox(
            "outer technician override",
            value=default_home_to_job_relaxed,
            help="Checked stores -1. Routing treats it as relaxed home-to-job and first-leg limits.",
        )
        max_home_to_job_min_text = st.text_input(
            "max_home_to_job_min override",
            value=str(int(default_max_home_to_job_min)) if pd.notna(default_max_home_to_job_min) and not default_home_to_job_relaxed else "",
            help="Blank uses city default. Use the checkbox for outer technicians instead of typing -1.",
            disabled=home_to_job_relaxed,
        )
        home_address = st.text_input("Home Street Address", value=default_home_address)
        city_col, state_col, zip_col = st.columns([1.2, 0.6, 0.8])
        home_city = city_col.text_input("City", value=default_home_city)
        home_state = state_col.text_input("State", value=default_home_state)
        home_postal_code = zip_col.text_input("Zip", value=default_home_postal_code)
        active_flag = st.checkbox("active_flag", value=default_active_flag)
        submitted = st.form_submit_button("Save Technician", type="primary", width="stretch")
    if submitted:
        try:
            parsed_max_home_to_job_min = pd.to_numeric(pd.Series([max_home_to_job_min_text]), errors="coerce").iloc[0]
            max_home_to_job_override = (
                -1
                if home_to_job_relaxed
                else int(parsed_max_home_to_job_min)
                if pd.notna(parsed_max_home_to_job_min) and int(parsed_max_home_to_job_min) >= 0
                else None
            )
            _api_post(
                DEFAULT_COMMON_SERVER_URL,
                "/api/v1/common/engineers/upsert",
                {
                    "subsidiary_name": subsidiary_name,
                    "strategic_city_name": strategic_city_name,
                    "employee_code": employee_code,
                    "employee_name": employee_name,
                    "center_type": center_type,
                    "home_address": home_address,
                    "home_city": home_city,
                    "home_state": home_state,
                    "home_country": "USA",
                    "home_postal_code": home_postal_code,
                    "active_flag": active_flag,
                    "priority_group": priority_group,
                    "max_home_to_job_min": max_home_to_job_override,
                },
            )
            st.cache_data.clear()
            st.success("Technician master saved.")
        except Exception as exc:
            st.error(str(exc))

    if not engineer_master_df.empty:
        st.divider()
        st.subheader("Delete Technician")
        delete_options = (
            engineer_master_df["employee_name"].fillna("").astype(str)
            + " ("
            + engineer_master_df["employee_code"].fillna("").astype(str)
            + ")"
        ).tolist()
        selected_label = st.selectbox("Technician", delete_options)
        selected_code = ""
        if selected_label and "(" in selected_label and selected_label.endswith(")"):
            selected_code = selected_label.rsplit("(", 1)[-1].rstrip(")").strip()
        if st.button("Delete Selected Technician", width="stretch"):
            try:
                _api_post(
                    DEFAULT_COMMON_SERVER_URL,
                    "/api/v1/common/engineers/delete",
                    {
                        "subsidiary_name": subsidiary_name,
                        "strategic_city_name": strategic_city_name,
                        "employee_code": selected_code,
                    },
                )
                st.cache_data.clear()
                st.success(f"Deleted technician {selected_code}.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))


@st.dialog("Direct Job Input", width="large", dismissible=False)
def _direct_job_dialog(
    master_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    subsidiary_name: str,
    strategic_city_name: str,
    existing_jobs_df: pd.DataFrame,
    edit_record: pd.Series | None = None,
) -> None:
    action_container = st.container()
    engineer_labels = (engineer_master_df["employee_name"].astype(str) + " (" + engineer_master_df["employee_code"].astype(str) + ")").tolist()
    default_engineer_label = engineer_labels[0] if engineer_labels else None
    if edit_record is not None and engineer_labels:
        current_code = str(edit_record.get("svc_engineer_code", "")).strip()
        matched_engineer = engineer_master_df[engineer_master_df["employee_code"].astype(str) == current_code].head(1)
        if not matched_engineer.empty:
            matched_row = matched_engineer.iloc[0]
            candidate_label = f"{str(matched_row['employee_name']).strip()} ({str(matched_row['employee_code']).strip()})"
            if candidate_label in engineer_labels:
                default_engineer_label = candidate_label
    default_promise = str(edit_record.get("promise_date", "")) if edit_record is not None else ""
    default_promise_date = pd.to_datetime(default_promise, format="%Y%m%d", errors="coerce")
    promise_date_value = st.date_input(
        "PROMISE_DATE",
        value=default_promise_date.date() if pd.notna(default_promise_date) else None,
    )
    receipt_no = st.text_input("GSFS_RECEIPT_NO", value=str(edit_record.get("gsfs_receipt_no", "")) if edit_record is not None else "")
    fixed_col, slot_count_col = st.columns(2)
    fixed = fixed_col.checkbox("Fixed Assignment", value=_coerce_bool_value(edit_record.get("fixed", False)) if edit_record is not None else False)
    job_slot_count = slot_count_col.number_input(
        "Job Slot Count",
        min_value=1,
        step=1,
        value=_coerce_job_slot_count_value(edit_record.get("job_slot_count", 2 if _coerce_bool_value(edit_record.get("two_slot_job", False)) else 1)) if edit_record is not None else 1,
    )
    selected_engineer_label = st.selectbox(
        "Technician",
        engineer_labels,
        index=engineer_labels.index(default_engineer_label) if default_engineer_label in engineer_labels else 0,
    )
    selected_engineer_row = engineer_master_df[(
        engineer_master_df["employee_name"].astype(str) + " (" + engineer_master_df["employee_code"].astype(str) + ")"
    ) == str(selected_engineer_label)].head(1)
    if selected_engineer_row.empty:
        st.error("No technician selected.")
        return
    selected_engineer_row = selected_engineer_row.iloc[0]

    group_names = sorted(master_df["Product Group Name"].dropna().astype(str).unique().tolist())
    default_group_name = group_names[0] if group_names else None
    if edit_record is not None:
        matched_group = master_df[master_df["Product Group Code"].astype(str) == str(edit_record.get("service_product_group_code", ""))].head(1)
        if not matched_group.empty:
            candidate_group = str(matched_group.iloc[0]["Product Group Name"])
            if candidate_group in group_names:
                default_group_name = candidate_group
    selected_group_name = st.selectbox("Product Group Name", group_names, index=group_names.index(default_group_name) if default_group_name in group_names else 0)
    group_df = master_df[master_df["Product Group Name"] == selected_group_name].copy()
    product_names = sorted(group_df["Product Name"].dropna().astype(str).unique().tolist())
    default_product_name = product_names[0] if product_names else None
    if edit_record is not None:
        matched_product = group_df[group_df["Product Code"].astype(str) == str(edit_record.get("service_product_code", ""))].head(1)
        if not matched_product.empty:
            candidate_product = str(matched_product.iloc[0]["Product Name"])
            if candidate_product in product_names:
                default_product_name = candidate_product
    selected_product_name = st.selectbox("Product Name", product_names, index=product_names.index(default_product_name) if default_product_name in product_names else 0)
    product_df = group_df[group_df["Product Name"] == selected_product_name].copy()
    symptom_names = ["None"] + sorted(product_df["Symptom Name"].dropna().astype(str).unique().tolist())
    default_symptom_name = "None"
    if edit_record is not None:
        matched_detail = product_df[product_df["Detailed Symptom Code"].astype(str) == str(edit_record.get("receipt_detail_symptom_code", ""))].head(1)
        if not matched_detail.empty:
            candidate_symptom = str(matched_detail.iloc[0]["Symptom Name"])
            if candidate_symptom in symptom_names:
                default_symptom_name = candidate_symptom
    selected_symptom_name = st.selectbox("Symptom Name", symptom_names, index=symptom_names.index(default_symptom_name) if default_symptom_name in symptom_names else 0)
    selected_detail_row = None
    if selected_symptom_name == "None":
        st.selectbox("Symtom Type Name", ["None"], index=0)
        st.selectbox("Detailed Symptom Name", ["None"], index=0)
    else:
        symptom_df = product_df[product_df["Symptom Name"] == selected_symptom_name].copy()
        type_names = ["None"] + sorted(symptom_df["Symtom Type Name"].dropna().astype(str).unique().tolist())
        default_type_name = "None"
        if edit_record is not None:
            matched_detail = symptom_df[symptom_df["Detailed Symptom Code"].astype(str) == str(edit_record.get("receipt_detail_symptom_code", ""))].head(1)
            if not matched_detail.empty:
                candidate_type = str(matched_detail.iloc[0]["Symtom Type Name"])
                if candidate_type in type_names:
                    default_type_name = candidate_type
        selected_type_name = st.selectbox("Symtom Type Name", type_names, index=type_names.index(default_type_name) if default_type_name in type_names else 0)
        if selected_type_name == "None":
            st.selectbox("Detailed Symptom Name", ["None"], index=0)
        else:
            detail_df = symptom_df[symptom_df["Symtom Type Name"] == selected_type_name].copy()
            detail_names = ["None"] + sorted(detail_df["Detailed Symptom Name"].dropna().astype(str).unique().tolist())
            default_detail_name = "None"
            if edit_record is not None:
                matched_detail = detail_df[detail_df["Detailed Symptom Code"].astype(str) == str(edit_record.get("receipt_detail_symptom_code", ""))].head(1)
                if not matched_detail.empty:
                    candidate_detail = str(matched_detail.iloc[0]["Detailed Symptom Name"])
                    if candidate_detail in detail_names:
                        default_detail_name = candidate_detail
            selected_detail_name = st.selectbox("Detailed Symptom Name", detail_names, index=detail_names.index(default_detail_name) if default_detail_name in detail_names else 0)
            if selected_detail_name != "None":
                selected_detail_row = detail_df[detail_df["Detailed Symptom Name"] == selected_detail_name].head(1).iloc[0]

    city_name = st.text_input("CITY_NAME", value=str(edit_record.get("city_name", "")) if edit_record is not None else "")
    postal_code = st.text_input("POSTAL_CODE", value=str(edit_record.get("postal_code", "")) if edit_record is not None else "")
    address_line1 = st.text_input("ADDRESS_LINE1_INFO", value=str(edit_record.get("address_line1_info", "")) if edit_record is not None else "")

    with action_container:
        _, _, save_col, close_col = st.columns([4, 2, 1, 1])
        save_clicked = save_col.button("Save Job", type="primary", width="stretch")
        close_clicked = close_col.button("Close", width="stretch")

    if save_clicked:
        candidate_df = pd.DataFrame(
            [
                {
                    "SVC_ENGINEER_CODE": str(selected_engineer_row["employee_code"]).strip(),
                    "SVC_ENGINEER_NAME": str(selected_engineer_row["employee_name"]).strip(),
                    "SERVICE_PRODUCT_GROUP_CODE": str(group_df["Product Group Code"].iloc[0]).strip(),
                    "SERVICE_PRODUCT_CODE": str(product_df["Product Code"].iloc[0]).strip(),
                    "RECEIPT_DETAIL_SYMPTOM_CODE": str(selected_detail_row["Detailed Symptom Code"]).strip() if selected_detail_row is not None else "",
                    "GSFS_RECEIPT_NO": str(receipt_no).strip(),
                    "PROMISE_DATE": pd.Timestamp(promise_date_value).strftime("%Y%m%d"),
                    "CITY_NAME": str(city_name).strip(),
                    "POSTAL_CODE": str(postal_code).strip(),
                    "ADDRESS_LINE1_INFO": str(address_line1).strip(),
                    "fixed": bool(fixed),
                    "reschedule": _coerce_bool_value(edit_record.get("reschedule", False)) if edit_record is not None else False,
                    "job_slot_count": int(job_slot_count),
                }
            ]
        )
        try:
            prepared_df, duplicate_receipts = _prepare_jobs_df(
                candidate_df,
                subsidiary_name,
                strategic_city_name,
                existing_jobs_df,
                allow_existing_receipt=str(edit_record.get("gsfs_receipt_no", "")) if edit_record is not None else "",
            )
            if duplicate_receipts:
                st.error(f"Duplicate GSFS_RECEIPT_NO already exists: {', '.join(duplicate_receipts)}")
                return
            success_df, failed_df = _geocode_jobs_df(prepared_df)
            if not failed_df.empty:
                st.error("Address error. Geocoding failed.")
                return
            if edit_record is not None and not success_df.empty:
                success_df.loc[:, "record_id"] = str(edit_record["record_id"])
            _save_local_jobs(subsidiary_name, strategic_city_name, _job_rows_to_df(_build_job_upsert_rows(success_df)))
            _close_common_job_dialog()
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
    if close_clicked:
        _close_common_job_dialog()
        st.rerun()


def _render_jobs_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    master_df = _load_master_df(str(MASTER_PATH))
    engineer_master_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/engineers",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    jobs_df = _load_local_jobs(subsidiary_name, strategic_city_name)
    source_mode = st.radio("Job Input Source", ["Upload CSV", "Direct Input"], horizontal=True)
    if source_mode == "Upload CSV":
        sample_col, upload_col, save_col = st.columns([1, 2.2, 1])
        sample_data = (
            JOB_SAMPLE_PATH.read_bytes().replace(b"fixed,2slot_job", b"fixed,reschedule,job_slot_count")
            if JOB_SAMPLE_PATH.exists()
            else (",".join(JOB_REQUIRED_COLUMNS + ["fixed", "reschedule", "job_slot_count"]) + "\n").encode("utf-8-sig")
        )
        sample_col.download_button(
            "Sample CSV",
            data=sample_data,
            file_name=JOB_SAMPLE_PATH.name,
            mime="text/csv",
        )
        with upload_col:
            uploaded_file = st.file_uploader(
                "Upload Job CSV",
                type=["csv"],
                label_visibility="collapsed",
                key=f"job_csv::{subsidiary_name}::{strategic_city_name}",
            )
        if save_col.button("Upload Jobs", type="secondary", width="stretch"):
            if uploaded_file is None:
                st.warning("Upload a CSV file first.")
            else:
                raw_df = _read_uploaded_service_csv(uploaded_file)
                prepared_df, duplicate_receipts = _prepare_jobs_df(
                    raw_df,
                    subsidiary_name,
                    strategic_city_name,
                    jobs_df,
                    replace_existing_receipts=True,
                )
                success_df, failed_df = _geocode_jobs_df(prepared_df)
                if not success_df.empty:
                    _save_local_jobs(subsidiary_name, strategic_city_name, _job_rows_to_df(_build_job_upsert_rows(success_df)))
                    uploaded_dates = sorted(success_df["PROMISE_DATE"].dropna().astype(str).unique().tolist(), reverse=True)
                    if uploaded_dates:
                        st.session_state[f"common_jobs_filter_date::{subsidiary_name}::{strategic_city_name}"] = uploaded_dates[0]
                    st.session_state[f"common_jobs_upload_message::{subsidiary_name}::{strategic_city_name}"] = f"Upload completed. {len(success_df)} jobs were saved."
                    st.rerun()
                if duplicate_receipts:
                    st.warning(f"Duplicate rows in upload kept first: {', '.join(duplicate_receipts)}")
                if not failed_df.empty:
                    st.error(f"Address error rows exist. Geocoding failed for {len(failed_df)} rows.")
                    st.dataframe(
                        failed_df[
                            [
                                col
                                for col in [
                                    "GSFS_RECEIPT_NO",
                                    "PROMISE_DATE",
                                    "CITY_NAME",
                                    "STATE_NAME",
                                    "POSTAL_CODE",
                                    "ADDRESS_LINE1_INFO",
                                ]
                                if col in failed_df.columns
                            ]
                        ].head(20),
                        width="stretch",
                        hide_index=True,
                    )
                if success_df.empty and not failed_df.empty:
                    st.warning("No jobs were saved because every uploaded row failed geocoding.")
    else:
        if st.button("Open Direct Job Input", type="primary", width="stretch"):
            st.session_state["common_job_dialog_record_id"] = "__new__"
            st.session_state["common_job_dialog_open"] = True
    dialog_record_id = st.session_state.get("common_job_dialog_record_id")
    dialog_open = bool(st.session_state.get("common_job_dialog_open", False))
    if dialog_open and dialog_record_id is not None:
        edit_record = None
        if str(dialog_record_id) != "__new__":
            matched = jobs_df[jobs_df["record_id"].astype(str) == str(dialog_record_id)].head(1)
            if not matched.empty:
                edit_record = matched.iloc[0]
        _direct_job_dialog(master_df, engineer_master_df, subsidiary_name, strategic_city_name, jobs_df, edit_record)

    jobs_df = _load_local_jobs(subsidiary_name, strategic_city_name)
    upload_message_key = f"common_jobs_upload_message::{subsidiary_name}::{strategic_city_name}"
    if st.session_state.get(upload_message_key):
        st.success(str(st.session_state.pop(upload_message_key)))
    if jobs_df.empty:
        st.info("No saved jobs.")
    else:
        promise_dates = sorted(jobs_df["promise_date"].dropna().astype(str).unique().tolist(), reverse=True)
        filter_key = f"common_jobs_filter_date::{subsidiary_name}::{strategic_city_name}"
        filter_options = ["ALL"] + promise_dates
        selected_filter = st.session_state.get(filter_key, "ALL")
        filter_index = filter_options.index(selected_filter) if selected_filter in filter_options else 0
        selected_date = st.selectbox("Filter by PROMISE_DATE", filter_options, index=filter_index, key=filter_key)
        if selected_date != "ALL":
            jobs_df = jobs_df[jobs_df["promise_date"].astype(str) == str(selected_date)].copy()
            delete_date_col, date_count_col = st.columns([1, 2])
            if delete_date_col.button("Delete All Jobs For Date", width="stretch"):
                record_ids = jobs_df["record_id"].dropna().astype(str).tolist()
                for record_id in record_ids:
                    _delete_local_job(subsidiary_name, strategic_city_name, record_id)
                st.success(f"Deleted {len(record_ids)} jobs for {selected_date}.")
                st.rerun()
            date_count_col.caption(f"Jobs for {selected_date}: {len(jobs_df)}")
        _render_fixed_slot_capacity_warnings(jobs_df, engineer_master_df)
        jobs_df = jobs_df.sort_values(
            ["promise_date", "svc_engineer_name", "gsfs_receipt_no"],
            ascending=[False, True, True],
            kind="mergesort",
        ).reset_index(drop=True)
        display_cols = [
            "svc_engineer_name",
            "svc_engineer_code",
            "gsfs_receipt_no",
            "fixed",
            "reschedule",
            "promise_date",
            "service_product_group_code",
            "service_product_code",
            "job_slot_count",
            "receipt_detail_symptom_code",
            "city_name",
            "postal_code",
            "address_line1_info",
        ]
        editor_source_df = jobs_df[["record_id"] + display_cols].copy()
        editor_source_df.insert(0, "select", False)
        editor_source_df["fixed"] = _coerce_bool_series(editor_source_df["fixed"])
        editor_source_df["reschedule"] = _coerce_bool_series(editor_source_df["reschedule"])
        editor_source_df["job_slot_count"] = _coerce_job_slot_count_series(editor_source_df["job_slot_count"])
        editor_display_cols = ["select"] + display_cols
        editor_display_df = editor_source_df[editor_display_cols].rename(
            columns={
                "svc_engineer_name": "technician_name",
                "svc_engineer_code": "technician_code",
            }
        )
        edited_jobs_df = st.data_editor(
            editor_display_df,
            width="stretch",
            hide_index=True,
            num_rows="fixed",
            disabled=[col for col in editor_display_df.columns if col not in {"select", "fixed", "job_slot_count"}],
            column_config={
                "select": st.column_config.CheckboxColumn("select"),
                "fixed": st.column_config.CheckboxColumn("fixed"),
                "reschedule": st.column_config.CheckboxColumn("reschedule"),
                "job_slot_count": st.column_config.NumberColumn("job_slot_count", min_value=1, step=1),
            },
            key=f"common_jobs_fixed_editor::{subsidiary_name}::{strategic_city_name}::{selected_date}",
        )
        edited_fixed = _coerce_bool_series(edited_jobs_df["fixed"])
        original_fixed = editor_source_df["fixed"].reset_index(drop=True)
        edited_job_slot_count = _coerce_job_slot_count_series(edited_jobs_df["job_slot_count"])
        original_job_slot_count = editor_source_df["job_slot_count"].reset_index(drop=True)
        if (
            not edited_fixed.reset_index(drop=True).equals(original_fixed)
            or not edited_job_slot_count.reset_index(drop=True).equals(original_job_slot_count)
        ):
            updated_jobs_df = jobs_df.copy().reset_index(drop=True)
            updated_jobs_df["fixed"] = edited_fixed.values
            updated_jobs_df["job_slot_count"] = edited_job_slot_count.values
            _save_local_jobs(subsidiary_name, strategic_city_name, updated_jobs_df)
            st.rerun()
        selected_rows = edited_jobs_df.index[_coerce_bool_series(edited_jobs_df["select"])].tolist()
        if selected_rows:
            selected_indices = [int(idx) for idx in selected_rows if 0 <= int(idx) < len(jobs_df)]
            selected_records_df = jobs_df.iloc[selected_indices].copy()
            selected_record_ids = selected_records_df["record_id"].dropna().astype(str).tolist()
            selected_receipts = selected_records_df["gsfs_receipt_no"].dropna().astype(str).tolist()
            st.caption(f"Selected jobs: {len(selected_record_ids)}")
            edit_col, delete_col = st.columns(2)
            if len(selected_record_ids) == 1:
                selected_record_id = selected_record_ids[0]
                selected_receipt = selected_receipts[0] if selected_receipts else selected_record_id
                if edit_col.button("Edit Selected Row", width="stretch"):
                    st.session_state["common_job_dialog_record_id"] = selected_record_id
                    st.session_state["common_job_dialog_open"] = True
                    st.rerun()
            else:
                edit_col.button("Edit Selected Row", width="stretch", disabled=True)
            if delete_col.button("Delete Selected Rows", width="stretch"):
                for record_id in selected_record_ids:
                    _delete_local_job(subsidiary_name, strategic_city_name, record_id)
                st.success(f"Deleted {len(selected_record_ids)} selected jobs.")
                st.rerun()


def _render_technicians_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    jobs_df = _load_local_jobs(subsidiary_name, strategic_city_name)
    engineer_master_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/engineers",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    if jobs_df.empty:
        st.info("No saved jobs.")
        return
    promise_dates = sorted(jobs_df["promise_date"].dropna().astype(str).unique().tolist(), reverse=True)
    selected_date = st.selectbox("PROMISE_DATE for Technician List", promise_dates, index=0 if promise_dates else None)
    selected_jobs_df = jobs_df[jobs_df["promise_date"].astype(str) == str(selected_date)].copy() if selected_date else jobs_df.head(0).copy()
    draft_df = _load_technician_draft(subsidiary_name, strategic_city_name, str(selected_date))
    technicians_df = draft_df.copy() if not draft_df.empty else _load_local_technicians(subsidiary_name, strategic_city_name, str(selected_date))
    if technicians_df.empty and not selected_jobs_df.empty:
        default_rows = _build_default_technician_rows_from_jobs(
            selected_jobs_df,
            engineer_master_df,
            subsidiary_name,
            strategic_city_name,
        )
        if default_rows:
            technicians_df = pd.DataFrame(default_rows)
            _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), technicians_df.to_dict("records"))
    technicians_df = _normalize_technician_rows(
        technicians_df,
        engineer_master_df,
        subsidiary_name,
        strategic_city_name,
        str(selected_date),
        default_source="manual_input",
    )

    source_col, manage_col = st.columns([3, 1])
    with source_col:
        add_mode = st.radio("Technician Source", ["Same As Jobs", "All Technicians"], horizontal=True)
    if manage_col.button("Technician Management", type="primary", width="stretch"):
        _technician_master_dialog(subsidiary_name, strategic_city_name)
    if add_mode == "Same As Jobs":
        if st.button("Add Technicians From Jobs", width="stretch"):
            rows = _build_default_technician_rows_from_jobs(selected_jobs_df, engineer_master_df, subsidiary_name, strategic_city_name)
            if rows:
                technicians_df = _normalize_technician_rows(
                    pd.DataFrame(rows),
                    engineer_master_df,
                    subsidiary_name,
                    strategic_city_name,
                    str(selected_date),
                    default_source="same_as_jobs",
                )
                _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), technicians_df.to_dict("records"))
                st.success(f"Loaded {len(technicians_df)} technicians from jobs.")
            else:
                st.warning("No technicians found in saved jobs.")
    else:
        master_rows = _build_default_technician_rows_from_master(engineer_master_df, subsidiary_name, strategic_city_name)
        if master_rows:
            master_options = {
                f"{row['employee_name']} ({row['employee_code']})": row
                for row in master_rows
            }
            add_one_col, add_all_col = st.columns([2.2, 1])
            selected_master_label = add_one_col.selectbox(
                "Technician From Master",
                list(master_options.keys()),
                key=f"master_technician_select::{subsidiary_name}::{strategic_city_name}::{selected_date}",
            )
            if add_all_col.button("Add Selected Technician", width="stretch"):
                selected_row = master_options.get(selected_master_label)
                if selected_row:
                    merged_df = _merge_technician_rows(technicians_df, [selected_row])
                    technicians_df = _normalize_technician_rows(
                        merged_df,
                        engineer_master_df,
                        subsidiary_name,
                        strategic_city_name,
                        str(selected_date),
                        default_source="master_selected",
                    )
                    _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), technicians_df.to_dict("records"))
                    st.success(f"Added technician {selected_row['employee_code']}.")
                    st.rerun()
        else:
            st.warning("No technician master rows for the selected city.")
        if st.button("Add All Technicians In City", width="stretch", disabled=not bool(master_rows)):
            rows = master_rows
            if rows:
                technicians_df = _normalize_technician_rows(
                    pd.DataFrame(rows),
                    engineer_master_df,
                    subsidiary_name,
                    strategic_city_name,
                    str(selected_date),
                    default_source="all_technicians",
                )
                _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), technicians_df.to_dict("records"))
                st.success(f"Loaded {len(technicians_df)} technicians from city master.")
                st.rerun()
            else:
                st.warning("No technician master rows for the selected city.")

    sample_col, upload_col, import_col = st.columns([1, 2.2, 1])
    if TECHNICIAN_SAMPLE_PATH.exists():
        sample_col.download_button(
            "Sample CSV",
            data=TECHNICIAN_SAMPLE_PATH.read_bytes(),
            file_name=TECHNICIAN_SAMPLE_PATH.name,
            mime="text/csv",
        )
    with upload_col:
        uploaded_technician_file = st.file_uploader(
            "Upload Technician CSV",
            type=["csv"],
            label_visibility="collapsed",
            key=f"technician_csv::{subsidiary_name}::{strategic_city_name}::{selected_date}",
        )
    technician_message_key = f"common_technicians_message::{subsidiary_name}::{strategic_city_name}::{selected_date}"
    if st.session_state.get(technician_message_key):
        st.success(str(st.session_state.pop(technician_message_key)))
    if import_col.button("Upload Tech CSV", type="secondary", width="stretch"):
        if uploaded_technician_file is None:
            st.warning("Upload a technician CSV file first.")
        else:
            try:
                uploaded_df = _read_uploaded_technician_csv(uploaded_technician_file)
                normalized_upload_df = _normalize_technician_rows(
                    uploaded_df,
                    engineer_master_df,
                    subsidiary_name,
                    strategic_city_name,
                    str(selected_date),
                    default_source="csv_upload",
                )
                if normalized_upload_df.empty:
                    raise ValueError("Uploaded technician CSV does not contain any valid employee_code rows.")
                _save_local_technicians(subsidiary_name, strategic_city_name, str(selected_date), normalized_upload_df)
                _save_technician_draft(
                    subsidiary_name,
                    strategic_city_name,
                    str(selected_date),
                    normalized_upload_df.to_dict("records"),
                )
                st.session_state[technician_message_key] = f"Upload completed. Please review the available column for {len(normalized_upload_df)} technicians."
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    if technicians_df.empty:
        st.info("No technicians for selected date.")
        return
    editable_cols = [
        "employee_name",
        "employee_code",
        "center_type",
        "available",
        "shift_start",
        "shift_end",
        "slot_count",
        "max_minutes",
        "priority_group",
        "start_location_type",
        "start_location_address",
    ]
    editor_key = f"common_technician_editor::{subsidiary_name}::{strategic_city_name}::{selected_date}"
    edited_df = st.data_editor(
        technicians_df[["subsidiary_name", "strategic_city_name"] + editable_cols],
        width="stretch",
        hide_index=True,
        num_rows="fixed",
        column_config={
            "employee_name": st.column_config.TextColumn("technician_name"),
            "employee_code": st.column_config.TextColumn("technician_code"),
            "available": st.column_config.CheckboxColumn("available"),
            "slot_count": st.column_config.NumberColumn("slot_count", min_value=0, step=1),
            "max_minutes": st.column_config.NumberColumn("max_minutes", min_value=1, step=10),
            "priority_group": st.column_config.SelectboxColumn(
                "priority_group",
                options=["A", "B", "C"],
                help="A target 8 jobs, B target 6 jobs, C target 4 jobs. All priorities have max 8 jobs.",
                required=True,
            ),
            "start_location_type": st.column_config.SelectboxColumn(
                "start_location_type",
                options=["Home", "Custom Address"],
                required=True,
            ),
        },
        key=editor_key,
    )
    if st.button("Save Technician List", type="primary", width="stretch"):
        edited_rows_df = edited_df.copy()
        metadata_lookup = {
            _clean_text(row.get("employee_code")): {
                "record_id": _clean_text(row.get("record_id")) or uuid.uuid4().hex,
                "source": _clean_text(row.get("source")) or "manual_input",
            }
            for _, row in technicians_df.iterrows()
            if _clean_text(row.get("employee_code"))
        }
        saved_df = _normalize_technician_rows(
            edited_rows_df,
            engineer_master_df,
            subsidiary_name,
            strategic_city_name,
            str(selected_date),
            default_source="manual_input",
        )
        if saved_df.empty:
            st.error("No valid technician rows to save.")
            return
        saved_df["record_id"] = saved_df["employee_code"].map(
            lambda code: metadata_lookup.get(str(code), {}).get("record_id", uuid.uuid4().hex)
        )
        saved_df["source"] = saved_df["employee_code"].map(
            lambda code: metadata_lookup.get(str(code), {}).get("source", "manual_input")
        )
        _save_local_technicians(subsidiary_name, strategic_city_name, str(selected_date), saved_df)
        _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), saved_df.to_dict("records"))
        st.session_state[technician_message_key] = "Technician list saved successfully."
        st.rerun()


def _render_payload_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    source_city_name = _context_source_city_name(strategic_city_name)
    source_jobs_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/jobs",
            subsidiary_name=subsidiary_name,
            strategic_city_name=source_city_name,
        ).get("rows", [])
    )
    if source_jobs_df.empty or "promise_date" not in source_jobs_df.columns:
        st.info("No API source jobs for the selected context.")
        return
    promise_dates = sorted(source_jobs_df["promise_date"].dropna().astype(str).unique().tolist(), reverse=True)
    selected_date = st.selectbox("PROMISE_DATE to Build Payload", promise_dates, index=0 if promise_dates else None)
    source_city_name, selected_jobs_df, technicians_df = _load_payload_source_rows(
        subsidiary_name, strategic_city_name, str(selected_date)
    )
    engineer_master_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/engineers",
            subsidiary_name=subsidiary_name,
            strategic_city_name=source_city_name,
        ).get("rows", [])
    )
    technicians_df = _normalize_technician_rows(
        technicians_df,
        engineer_master_df,
        subsidiary_name,
        strategic_city_name,
        str(selected_date),
        default_source="manual_input",
    )
    capability_rows = _build_capability_rows_for_payload(strategic_city_name, technicians_df, selected_jobs_df)
    st.caption(f"API source context: {source_city_name} → routing context: {strategic_city_name}")
    st.caption(f"API source jobs for selected date: {len(selected_jobs_df)}")
    available_tech_count = int(technicians_df["available"].fillna(False).astype(bool).sum()) if not technicians_df.empty and "available" in technicians_df.columns else 0
    st.caption(f"Available API source technicians: {available_tech_count}")
    st.caption(f"Capability rows prepared: {len(capability_rows)}")

    payload_date = str(st.session_state.get("common_vrp_payload_date", "")).strip()
    if payload_date and payload_date != str(selected_date):
        for key in [
            "common_vrp_payload",
            "common_vrp_payload_date",
            "common_vrp_payload_debug",
            "common_vrp_request_id",
            "common_vrp_job_id",
            "common_vrp_job_status",
            "common_vrp_job_result",
        ]:
            st.session_state.pop(key, None)

    if st.button("Build Payload", type="primary", width="stretch"):
        try:
            with st.spinner("Building payload..."):
                response = _api_post(
                    DEFAULT_COMMON_SERVER_URL,
                    "/api/v1/common/routing/build-payload",
                    _build_payload_request(
                        subsidiary_name,
                        strategic_city_name,
                        str(selected_date),
                        selected_jobs_df,
                        technicians_df,
                        capability_rows,
                    ),
                )
            payload = response.get("payload")
            st.session_state["common_vrp_payload"] = payload
            st.session_state["common_vrp_payload_date"] = str(selected_date)
            st.session_state["common_vrp_payload_debug"] = response.get("debug") or {}
            st.session_state["common_result_date"] = _promise_date_to_service_date_key(str(selected_date))
            st.session_state["common_vrp_request_id"] = ""
            st.session_state["common_vrp_job_id"] = ""
            st.session_state["common_vrp_job_status"] = None
            st.session_state["common_vrp_job_result"] = None
            jobs_count = len(list(payload.get("jobs", []))) if payload else 0
            tech_count = len(list(payload.get("technicians", []))) if payload else 0
            capability_count = len(list(payload.get("capabilities", []))) if payload else 0
            st.success(
                f"Payload ready: technicians={tech_count}, jobs={jobs_count}, capabilities={capability_count}"
            )
        except Exception as exc:
            st.error(str(exc))

    payload = st.session_state.get("common_vrp_payload")
    if payload:
        payload_date = str(st.session_state.get("common_vrp_payload_date", "")).strip()
        if payload_date and payload_date != str(selected_date):
            st.warning("Selected date changed. Build the payload again before requesting routing.")
            return
        jobs_list = list(payload.get("jobs", []))
        st.caption(
            f"Payload date: {payload_date or selected_date} / technicians={len(payload.get('technicians', []))}, jobs={len(jobs_list)}, capabilities={len(payload.get('capabilities', []))}"
        )
        region_plan = ((payload.get("options") or {}).get("region_plan") or {}) if isinstance(payload, dict) else {}
        policy_version = str(region_plan.get("policy_version", "")).strip()
        if policy_version:
            st.caption(f"Active region policy: {policy_version} (distance: km; duration: minutes; slots: jobs)")
        payload_debug = st.session_state.get("common_vrp_payload_debug") or {}
        for message in payload_debug.get("precheck_messages", []) or []:
            message_text = str(message).strip()
            if message_text:
                st.warning(message_text)
        with st.expander("Payload Preview", expanded=False):
            st.json(payload)
        if payload_debug:
            unmatched_total = int(payload_debug.get("unmatched_job_product_total", 0) or 0)
            if unmatched_total:
                st.warning(f"Payload capability warning: {unmatched_total} job(s) have no matching product capability.")
            with st.expander("Payload Debug", expanded=False):
                st.json(payload_debug)
        _req_col, _chk_col = st.columns(2)
        if _req_col.button("Request Routing", width="stretch"):
            try:
                with st.spinner("Submitting routing job..."):
                    response = _api_post(
                        DEFAULT_COMMON_SERVER_URL,
                        "/api/v1/common/routing/submit",
                        {
                            "subsidiary_name": subsidiary_name,
                            "strategic_city_name": strategic_city_name,
                            "promise_date": str(selected_date),
                            "payload": payload,
                        },
                    )
                st.session_state["common_vrp_request_id"] = response.get("request_id", "")
                st.session_state["common_vrp_job_id"] = response.get("routing_job_id", "")
                st.session_state["common_vrp_job_status"] = {"status": response.get("status", ""), "routing_job_id": response.get("routing_job_id", "")}
                st.session_state["common_vrp_job_result"] = None
                _reset_common_result_view()
                st.success(f"Submitted job {st.session_state['common_vrp_job_id']}")
            except Exception as exc:
                st.error(str(exc))

        if _chk_col.button("Check Routing Result", width="stretch"):
            request_id = str(st.session_state.get("common_vrp_request_id", "")).strip()
            if not request_id:
                st.warning("Submit a job first.")
            else:
                try:
                    snapshot = _api_post(
                        DEFAULT_COMMON_SERVER_URL,
                        "/api/v1/common/routing/check",
                        {"request_id": request_id},
                    )
                    st.session_state["common_vrp_job_status"] = snapshot.get("status")
                    st.session_state["common_vrp_job_result"] = snapshot.get("result")
                    latest_status = str((snapshot.get("status") or {}).get("status", "")).strip().lower()
                    if latest_status == "completed" and snapshot.get("result"):
                        _reset_common_result_view()
                    st.success("Routing status updated.")
                except Exception as exc:
                    st.error(str(exc))

        status_payload = st.session_state.get("common_vrp_job_status")
        status_value = status_payload.get("status", "") if status_payload else ""
        normalized_status_value = "queued" if str(status_value).strip().lower() == "submitted" else status_value
        progress_value, progress_text = _routing_status_progress(normalized_status_value)
        st.progress(progress_value)
        st.caption(progress_text)
        _auto_poll_common_routing_status()
        if status_payload:
            st.caption(f"Routing job status: {str(status_payload.get('status', '')).strip().lower()}")
            if status_payload.get("error_message"):
                st.error(str(status_payload.get("error_message")))
        result_payload = st.session_state.get("common_vrp_job_result")
        if result_payload:
            with st.expander("Result JSON Preview", expanded=False):
                st.json(result_payload)


def _render_routing_result_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    history_dates = _api_get(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/routing/history-dates",
        subsidiary_name=subsidiary_name,
        strategic_city_name=strategic_city_name,
    ).get("rows", [])
    history_dates = [str(value).strip() for value in history_dates if str(value).strip()]
    if not history_dates:
        st.info("No saved routing results for the selected city.")
        return
    current_payload = st.session_state.get("common_vrp_payload") or {}
    current_planning_date = ""
    if isinstance(current_payload, dict):
        current_planning_date = str(current_payload.get("planning_date", "")).strip().replace("-", "")
    default_index = history_dates.index(current_planning_date) if current_planning_date in history_dates else 0
    selected_history_date = st.selectbox(
        "PROMISE_DATE with Routing History",
        history_dates,
        index=default_index,
    )
    load_col, info_col = st.columns([1, 1.3])
    if load_col.button("Load Routing Result", type="primary", width="stretch"):
        try:
            if _load_latest_routing_result_for_date(subsidiary_name, strategic_city_name, str(selected_history_date)):
                st.success(f"Loaded latest routing result for {selected_history_date}.")
                st.rerun()
            st.warning("No routing snapshot found for the selected date.")
        except Exception as exc:
            st.error(str(exc))
    current_status = st.session_state.get("common_vrp_job_status") or {}
    if st.session_state.get("common_result_date"):
        info_col.caption(f"Loaded result date: {st.session_state.get('common_result_date')}")
    if current_status:
        info_col.caption(f"Current routing status: {str(current_status.get('status', '')).strip().lower()}")
    result_payload = st.session_state.get("common_vrp_job_result")
    if result_payload:
        with st.expander("Result JSON Preview", expanded=False):
            st.json(result_payload)


def main() -> None:
    st.title("Smart Routing Client")
    try:
        contexts = _api_get(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/contexts")
        _register_context_city_metadata(contexts)
        subsidiaries = contexts.get("subsidiaries", []) or [DEFAULT_SUBSIDIARY_NAME]
    except Exception:
        contexts = {}
        _register_context_city_metadata(contexts)
        subsidiaries = [DEFAULT_SUBSIDIARY_NAME]

    left_col, right_col = st.columns([1.2, 1.5])
    with left_col:
        top_col1, top_col2 = st.columns(2)
        subsidiary_name = top_col1.selectbox(
            "SUBSIDIARY_NAME",
            subsidiaries,
            index=subsidiaries.index(DEFAULT_SUBSIDIARY_NAME) if DEFAULT_SUBSIDIARY_NAME in subsidiaries else 0,
        )
        cities = _city_options_for_subsidiary(contexts, subsidiary_name)
        strategic_city_name = top_col2.selectbox(
            "STRATEGIC_CITY_NAME",
            cities,
            index=cities.index(DEFAULT_STRATEGIC_CITY_NAME) if DEFAULT_STRATEGIC_CITY_NAME in cities else 0,
        )
        _ensure_common_runtime_context(subsidiary_name, strategic_city_name)
        jobs_tab, technicians_tab, avoid_area_tab, payload_tab, routing_result_tab, statistics_tab = st.tabs(
            ["Jobs", "Technicians", "Avoid Areas", "Routing Request", "Routing Result", "Statistics"]
        )
        with jobs_tab:
            _render_jobs_tab(subsidiary_name, strategic_city_name)
        with technicians_tab:
            _render_technicians_tab(subsidiary_name, strategic_city_name)
        with avoid_area_tab:
            _render_avoid_area_tab(subsidiary_name, strategic_city_name)
        with payload_tab:
            _render_payload_tab(subsidiary_name, strategic_city_name)
        with routing_result_tab:
            _render_routing_result_tab(subsidiary_name, strategic_city_name)
        with statistics_tab:
            _render_statistics_tab(subsidiary_name, strategic_city_name)
        _render_result_summary(subsidiary_name, strategic_city_name)
    with right_col:
        if st.session_state.get("server_common_statistics_view_active", False):
            _render_statistics_panel()
        else:
            _render_result_detail(subsidiary_name, strategic_city_name)


if __name__ == "__main__":
    main()
