from __future__ import annotations

import colorsys
import json
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit.components.v1 import html

from smart_routing.area_map import (
    EXPLORER_CITIES,
    get_latest_geocoded_service_file,
    load_city_map_data,
    load_region_count_options,
    load_region_count_stats,
    load_route_explorer_data,
)
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient


st.set_page_config(page_title="North America Routing Map", layout="wide")

CONFIG_FILE = Path("config.json")
CURRENT_REGION_LABEL = "Current Region"


def _load_config(config_file: Path = CONFIG_FILE) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def get_route_explorer_data(city_name: str, region_count: int | None):
    return load_route_explorer_data(city_name=city_name, region_count=region_count)


@st.cache_data(show_spinner=False)
def get_region_stats(city_name: str):
    return load_region_count_stats(city_name)


@st.cache_resource(show_spinner=False)
def get_clients():
    routing_cfg = _load_config().get("routing", {})
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
    text = str(center_type).strip().upper()
    return text if text in {"DMS", "DMS2"} else "ASC"


def _apply_center_bucket_rules(service_df: pd.DataFrame, region_count: int | None) -> pd.DataFrame:
    service_df = service_df.copy()
    per_sm_day_count = (
        service_df.groupby(["service_date_key", "assigned_sm_code"])["GSFS_RECEIPT_NO"]
        .transform(lambda s: s.dropna().astype(str).nunique())
        if not service_df.empty
        else pd.Series(dtype="float64")
    )

    if region_count is None:
        if "SVC_CENTER_TYPE" in service_df.columns:
            service_df["CENTER_BUCKET"] = service_df["SVC_CENTER_TYPE"].map(_normalize_center_bucket)
        else:
            service_df["CENTER_BUCKET"] = "ASC"
        if not service_df.empty:
            service_df.loc[per_sm_day_count <= 1, "CENTER_BUCKET"] = "ASC"
    else:
        service_df["CENTER_BUCKET"] = "DMS"
        if not service_df.empty:
            service_df.loc[per_sm_day_count <= 1, "CENTER_BUCKET"] = "ASC"
    return service_df


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


@st.cache_data(show_spinner=False)
def get_route_payload(city_name: str, sm_code: str, date_key: str, coords: tuple[tuple[float, float], ...]):
    if not coords:
        return {"ordered_coords": [], "distance_km": 0.0, "duration_min": 0.0, "geometry": []}
    client_map, default_client = get_clients()
    client = client_map.get(city_name, default_client)
    return client.build_ordered_route(coords)


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
    for (service_date, sm_code), group_df in route_df.groupby(["service_date_key", "assigned_sm_code"], sort=True):
        coords = tuple(
            group_df[["longitude", "latitude"]]
            .dropna()
            .drop_duplicates()
            .apply(lambda r: (float(r["longitude"]), float(r["latitude"])), axis=1)
            .tolist()
        )
        route_payload = get_route_payload(city_name, str(sm_code), str(service_date), coords)
        groups.append(
            {
                "service_date_key": str(service_date),
                "assigned_sm_code": str(sm_code),
                "route_payload": route_payload,
                "service_count": int(group_df["GSFS_RECEIPT_NO"].astype(str).nunique()),
            }
        )
    return groups


def _build_stop_order_lookup(route_groups: list[dict]) -> dict[tuple[str, tuple[float, float]], int]:
    lookup: dict[tuple[str, tuple[float, float]], int] = {}
    for group in route_groups:
        sm_code = str(group.get("assigned_sm_code", "")).strip()
        for seq, coord in enumerate(group["route_payload"].get("ordered_coords", []), start=1):
            key = (sm_code, (round(float(coord[0]), 6), round(float(coord[1]), 6)))
            lookup[key] = seq
    return lookup


def build_map(
    city_name: str,
    region_count: int | None,
    area_name: str,
    selected_date: str,
    selected_sm: str,
):
    explorer_data = get_route_explorer_data(city_name, region_count)
    zip_layer, area_layer, service_df = _get_selected_frames(explorer_data, region_count)
    area_col = _get_area_column_name(region_count, zip_layer)

    service_df["service_date_key"] = pd.to_datetime(service_df["service_date"]).dt.strftime("%Y-%m-%d")
    if area_name != "ALL":
        area_layer = area_layer[area_layer["AREA_NAME"] == area_name].copy()
        zip_layer = zip_layer[zip_layer[area_col] == area_name].copy()
        service_df = service_df[service_df["AREA_NAME"] == area_name].copy()
    if selected_date != "ALL":
        service_df = service_df[service_df["service_date_key"] == selected_date].copy()
    if selected_sm != "ALL":
        service_df = service_df[service_df["assigned_sm_code"] == selected_sm].copy()
    service_df = _apply_center_bucket_rules(service_df, region_count)

    center_lat, center_lon = _center_from_layers(area_layer, service_df)
    zoom_start = 9 if area_name == "ALL" else 11
    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_start, tiles="cartodbpositron")
    area_color_map = _generate_color_map(area_layer["AREA_NAME"].astype(str).tolist())

    if not zip_layer.empty:
        if region_count is None and "service_count" in zip_layer.columns:
            zero_service_zip = zip_layer[zip_layer["service_count"].fillna(0).astype(int) == 0].copy()
            positive_service_zip = zip_layer[zip_layer["service_count"].fillna(0).astype(int) > 0].copy()

            if not zero_service_zip.empty:
                folium.GeoJson(
                    zero_service_zip,
                    name="ZIP Coverage (No Service)",
                    style_function=lambda feat: {
                        "fillColor": "#d1d5db",
                        "color": "#9ca3af",
                        "weight": 0.55,
                        "fillOpacity": 0.12,
                    },
                    tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "service_count"], aliases=["ZIP", "Service Count"], localize=True),
                ).add_to(map_obj)

            if not positive_service_zip.empty:
                folium.GeoJson(
                    positive_service_zip,
                    name="ZIP Coverage (With Service)",
                    style_function=lambda feat: {
                        "fillColor": "#ffffff",
                        "color": "#7a7a7a",
                        "weight": 0.55,
                        "fillOpacity": 0.03,
                    },
                    tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "service_count"], aliases=["ZIP", "Service Count"], localize=True),
                ).add_to(map_obj)
        else:
            folium.GeoJson(
                zip_layer,
                name="ZIP Boundary",
                style_function=lambda feat: {
                    "fillColor": "#ffffff",
                    "color": "#7a7a7a",
                    "weight": 0.45,
                    "fillOpacity": 0.03,
                },
                tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "service_count"], aliases=["ZIP", "Service Count"], localize=True),
            ).add_to(map_obj)

    if region_count is None:
        missing_geometry_df = get_missing_geometry_zip_df(city_name)
        if area_name != "ALL":
            missing_geometry_df = missing_geometry_df[missing_geometry_df["AREA_NAME"] == area_name].copy()
        missing_geometry_points = missing_geometry_df[missing_geometry_df["has_point"]].copy()
        if not missing_geometry_points.empty:
            missing_fg = folium.FeatureGroup(name="Coverage ZIPs Without Geometry", show=True)
            for _, row in missing_geometry_points.iterrows():
                popup_html = (
                    f"<b>ZIP</b>: {row.get('POSTAL_CODE', '')}<br>"
                    f"<b>Area</b>: {row.get('AREA_NAME', '')}<br>"
                    f"<b>Service Count</b>: {row.get('service_count', 0)}<br>"
                    f"<b>Note</b>: ZIP exists in coverage but no polygon geometry is available."
                )
                folium.CircleMarker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    radius=6,
                    color="#111111",
                    weight=2,
                    fill=True,
                    fill_color="#f59e0b",
                    fill_opacity=0.85,
                    popup=folium.Popup(popup_html, max_width=320),
                    tooltip=f"{row.get('POSTAL_CODE', '')} | no geometry",
                ).add_to(missing_fg)
            missing_fg.add_to(map_obj)

    if not area_layer.empty:
        area_fields = ["AREA_NAME", "postal_count", "service_count"]
        area_aliases = ["Area", "Postal Count", "Service Count"]
        if "avg_daily_assigned_sm_count" in area_layer.columns:
            area_fields.extend(["avg_daily_service_count", "avg_daily_assigned_sm_count"])
            area_aliases.extend(["Avg Daily Service", "Avg Daily Assigned SM"])
        folium.GeoJson(
            area_layer,
            name="Area",
            style_function=lambda feat: {
                "fillColor": area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                "color": area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                "weight": 3.0,
                "fillOpacity": 0.28,
            },
            highlight_function=lambda feat: {
                "fillColor": area_color_map.get(feat["properties"].get("AREA_NAME", ""), "#0f766e"),
                "color": "#111111",
                "weight": 4.0,
                "fillOpacity": 0.40,
            },
            tooltip=folium.GeoJsonTooltip(fields=area_fields, aliases=area_aliases, localize=True),
            popup=folium.GeoJsonPopup(fields=area_fields, aliases=area_aliases, localize=True),
        ).add_to(map_obj)

    route_groups = _build_route_groups(service_df, city_name, selected_date, selected_sm)
    stop_order_lookup = _build_stop_order_lookup(route_groups)
    route_color_map = _generate_color_map([group["assigned_sm_code"] for group in route_groups])

    if not service_df.empty:
        if selected_date == "ALL":
            cluster = MarkerCluster(name="Service Points")
            for _, row in service_df.iterrows():
                popup_html = (
                    f"<b>Date</b>: {row.get('service_date_key', '')}<br>"
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')}<br>"
                    f"<b>Region</b>: {row.get('AREA_NAME', '')}<br>"
                    f"<b>Center Type</b>: {row.get('CENTER_BUCKET', '')}<br>"
                    f"<b>Assigned SM</b>: {row.get('assigned_sm_code', '')}<br>"
                    f"<b>Current SM</b>: {row.get('SVC_ENGINEER_CODE', '')} / {row.get('SVC_ENGINEER_NAME', '')}<br>"
                    f"<b>Postal</b>: {row.get('POSTAL_CODE', '')}<br>"
                    f"<b>Address</b>: {row.get('ADDRESS_LINE1_INFO', '')}"
                )
                folium.CircleMarker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    radius=3.2,
                    color="#14532d",
                    weight=1,
                    fill=True,
                    fill_color="#22c55e",
                    fill_opacity=0.72,
                    popup=folium.Popup(popup_html, max_width=360),
                ).add_to(cluster)
            cluster.add_to(map_obj)
        else:
            service_fg = folium.FeatureGroup(name="Numbered Service Points", show=True)
            for _, row in service_df.iterrows():
                sm_code = str(row.get("assigned_sm_code", "")).strip()
                coord_key = (sm_code, (round(float(row["longitude"]), 6), round(float(row["latitude"]), 6)))
                seq = stop_order_lookup.get(coord_key)
                seq_label = str(seq) if seq is not None else "?"
                marker_color = route_color_map.get(sm_code, "#dc2626")
                popup_html = (
                    f"<b>Seq</b>: {seq_label}<br>"
                    f"<b>Date</b>: {row.get('service_date_key', '')}<br>"
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')}<br>"
                    f"<b>Region</b>: {row.get('AREA_NAME', '')}<br>"
                    f"<b>Center Type</b>: {row.get('CENTER_BUCKET', '')}<br>"
                    f"<b>Assigned SM</b>: {row.get('assigned_sm_code', '')}<br>"
                    f"<b>Current SM</b>: {row.get('SVC_ENGINEER_CODE', '')} / {row.get('SVC_ENGINEER_NAME', '')}<br>"
                    f"<b>Postal</b>: {row.get('POSTAL_CODE', '')}<br>"
                    f"<b>Address</b>: {row.get('ADDRESS_LINE1_INFO', '')}"
                )
                is_asc = row.get("CENTER_BUCKET", "") == "ASC"
                border_width = "3px" if is_asc else "2px"
                border_color = "#111111" if is_asc else "#ffffff"
                icon_html = (
                    f"<div style=\"background:{marker_color};color:#fff;border:{border_width} solid {border_color};"
                    f"border-radius:50%;width:24px;height:24px;line-height:20px;text-align:center;"
                    f"font-size:11px;font-weight:700;box-shadow:0 1px 6px rgba(0,0,0,0.35);\">{seq_label}</div>"
                )
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.DivIcon(html=icon_html, icon_size=(24, 24), icon_anchor=(12, 12)),
                    popup=folium.Popup(popup_html, max_width=360),
                    tooltip=f"{sm_code} | Seq {seq_label}",
                ).add_to(service_fg)
            service_fg.add_to(map_obj)

    for group in route_groups:
        geometry = group["route_payload"].get("geometry", [])
        if not geometry:
            continue
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
        ).add_to(map_obj)

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


def main():
    st.title("North America Routing Map")
    latest_service = get_latest_geocoded_service_file()
    if latest_service is not None:
        st.caption(f"Service source: `{latest_service}`")

    with st.sidebar:
        st.header("Filters")
        city_name = st.selectbox("City", EXPLORER_CITIES, index=0)
        candidate_counts = load_region_count_options(city_name)
        region_options = [CURRENT_REGION_LABEL] + [f"New Region {count}" for count in candidate_counts]
        region_option = st.selectbox("Region Type", region_options, index=0)
        selected_region_count = _parse_region_option(region_option)

        explorer_data = get_route_explorer_data(city_name, selected_region_count)
        _, area_layer, service_df = _get_selected_frames(explorer_data, selected_region_count)
        service_df = service_df.copy()
        service_df["service_date_key"] = pd.to_datetime(service_df["service_date"]).dt.strftime("%Y-%m-%d")
        service_df = _apply_center_bucket_rules(service_df, selected_region_count)

        date_options = ["ALL"] + sorted(service_df["service_date_key"].dropna().unique().tolist())
        selected_date = st.selectbox("Date", date_options, index=0)

        area_options = ["ALL"] + sorted(area_layer["AREA_NAME"].astype(str).unique().tolist())
        area_name = st.selectbox("AREA NAME", area_options, index=0)

        sm_df = service_df.copy()
        if selected_date != "ALL":
            sm_df = sm_df[sm_df["service_date_key"] == selected_date].copy()
        if area_name != "ALL":
            sm_df = sm_df[sm_df["AREA_NAME"] == area_name].copy()
        sm_options = ["ALL"] + sorted(sm_df["assigned_sm_code"].astype(str).unique().tolist())
        selected_sm = st.selectbox("Assigned SM Code", sm_options, index=0)
        missing_geometry_df = get_missing_geometry_zip_df(city_name) if selected_region_count is None else pd.DataFrame(columns=["POSTAL_CODE", "AREA_NAME", "service_count", "latitude", "longitude", "has_point"])

        total_service_count = int(service_df["GSFS_RECEIPT_NO"].astype(str).nunique()) if not service_df.empty else 0
        date_service_df = service_df[service_df["service_date_key"] == selected_date].copy() if selected_date != "ALL" else service_df.copy()
        area_service_df = date_service_df[date_service_df["AREA_NAME"] == area_name].copy() if area_name != "ALL" else date_service_df.copy()
        sm_service_df = area_service_df[area_service_df["assigned_sm_code"] == selected_sm].copy() if selected_sm != "ALL" else area_service_df.copy()
        sm_count_df = (
            area_service_df.groupby("assigned_sm_code")
            .agg(service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()))
            .reset_index()
            .sort_values(["service_count", "assigned_sm_code"], ascending=[False, True])
            if not area_service_df.empty
            else pd.DataFrame(columns=["assigned_sm_code", "service_count"])
        )
        if selected_date != "ALL" and not area_service_df.empty:
            sm_route_stats_df = pd.DataFrame(
                [
                    {
                        "assigned_sm_code": str(group["assigned_sm_code"]),
                        "distance_km": round(float(group["route_payload"]["distance_km"]), 2),
                    }
                    for group in _build_route_groups(area_service_df, city_name, selected_date, "ALL")
                ]
            )
            if not sm_route_stats_df.empty:
                sm_count_df = sm_count_df.merge(sm_route_stats_df, on="assigned_sm_code", how="left")
            else:
                sm_count_df["distance_km"] = pd.NA
        else:
            sm_count_df["distance_km"] = pd.NA
        sm_count_df["distance_km"] = sm_count_df["distance_km"].map(lambda v: round(float(v), 2) if pd.notna(v) else pd.NA)
        scope_service_count = int(sm_service_df["GSFS_RECEIPT_NO"].astype(str).nunique()) if not sm_service_df.empty else 0
        scope_assigned_sm_count = int(sm_service_df["assigned_sm_code"].astype(str).nunique()) if not sm_service_df.empty else 0
        scope_center_counts = (
            sm_service_df.groupby("CENTER_BUCKET")
            .agg(service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()))
            .to_dict("index")
            if not sm_service_df.empty and "CENTER_BUCKET" in sm_service_df.columns
            else {}
        )
        dms_count = int(scope_center_counts.get("DMS", {}).get("service_count", 0))
        dms2_count = int(scope_center_counts.get("DMS2", {}).get("service_count", 0))
        asc_count = int(scope_center_counts.get("ASC", {}).get("service_count", 0))
        scope_sm_bucket_counts = (
            sm_service_df.groupby(["service_date_key", "assigned_sm_code"])["CENTER_BUCKET"]
            .first()
            .reset_index()
            .groupby("CENTER_BUCKET")
            .size()
            .to_dict()
            if not sm_service_df.empty and "CENTER_BUCKET" in sm_service_df.columns
            else {}
        )
        dms_sm_count = int(scope_sm_bucket_counts.get("DMS", 0))
        dms2_sm_count = int(scope_sm_bucket_counts.get("DMS2", 0))
        asc_sm_count = int(scope_sm_bucket_counts.get("ASC", 0))
        scope_route_groups = _build_route_groups(sm_service_df, city_name, selected_date, selected_sm)
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
            st.caption(f"Service Count: {scope_service_count} (DMS {dms_count}, DMS2 {dms2_count}, ASC {asc_count})")
            st.caption(
                f"Assigned SM Count: {scope_assigned_sm_count} "
                f"(DMS {dms_sm_count}, DMS2 {dms2_sm_count}, ASC {asc_sm_count})"
            )
        else:
            st.caption(f"Service Count: {scope_service_count} (DMS {dms_count}, ASC {asc_count})")
            st.caption(f"Assigned SM Count: {scope_assigned_sm_count} (DMS {dms_sm_count}, ASC {asc_sm_count})")
        st.caption(f"Average Distance: {avg_distance:.2f} km")
        st.caption(f"Average Duration: {avg_duration:.2f} min")
        if selected_region_count is None:
            missing_total = int(len(missing_geometry_df))
            missing_with_points = int(missing_geometry_df["has_point"].sum()) if not missing_geometry_df.empty else 0
            st.caption(f"ZIPs without geometry: {missing_total} (mapped as points: {missing_with_points})")
        st.subheader("Service Count by SM")
        st.dataframe(sm_count_df, width="stretch", height=220)
        if selected_region_count is None and not missing_geometry_df.empty:
            st.subheader("Coverage ZIPs Without Geometry")
            st.dataframe(
                missing_geometry_df[["POSTAL_CODE", "AREA_NAME", "service_count", "has_point"]],
                width="stretch",
                height=180,
            )

    map_obj, filtered_service_df, filtered_area_layer, route_groups = build_map(
        city_name=city_name,
        region_count=selected_region_count,
        area_name=area_name,
        selected_date=selected_date,
        selected_sm=selected_sm,
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Area Count", int(filtered_area_layer["AREA_NAME"].nunique()) if not filtered_area_layer.empty else 0)
    metric_cols[1].metric("Service Count", int(filtered_service_df["GSFS_RECEIPT_NO"].astype(str).nunique()) if not filtered_service_df.empty else 0)
    metric_cols[2].metric("Assigned SM Count", int(filtered_service_df["assigned_sm_code"].astype(str).nunique()) if not filtered_service_df.empty else 0)
    metric_cols[3].metric("Visible Routes", len(route_groups))

    html(map_obj._repr_html_(), height=780)

    candidate_col, summary_col, detail_col = st.columns([1.3, 1.0, 1.2], gap="medium")
    with candidate_col:
        st.subheader("Candidate Region Summary")
        candidate_df = _build_candidate_display_df(city_name)
        if candidate_df.empty:
            st.caption("No candidate summary data.")
        else:
            st.dataframe(candidate_df, width="stretch", height=300)

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
            sm_summary_df = (
                filtered_service_df.groupby(["assigned_sm_code", "service_date_key"])
                .agg(
                    region_name=("AREA_NAME", "first"),
                    service_count=("GSFS_RECEIPT_NO", lambda s: s.dropna().astype(str).nunique()),
                    postal_count=("POSTAL_CODE", "nunique"),
                )
                .reset_index()
                .sort_values(["assigned_sm_code", "service_date_key"])
            )
            st.dataframe(sm_summary_df, width="stretch", height=300)


if __name__ == "__main__":
    main()
