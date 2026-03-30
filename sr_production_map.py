from __future__ import annotations

import colorsys
import json
import math
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit.components.v1 import html

from smart_routing.area_map import load_city_map_data
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient
import smart_routing.production_assign_atlanta as production_assign_atlanta


st.set_page_config(page_title="Atlanta Production Routing", layout="wide")

CONFIG_FILE = Path("config.json")
REGION_ZIP_PATH = Path("260310/production_input/atlanta_fixed_region_zip_3.csv")
ENGINEER_REGION_PATH = Path("260310/production_input/atlanta_engineer_region_assignment.csv")
HOME_GEOCODE_PATH = Path("260310/production_input/atlanta_engineer_home_geocoded.csv")
SERVICE_PATH = Path("260310/production_input/atlanta_service_enriched.csv")
PROFILE_COPY_PATH = Path("260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx")
ASSIGNMENT_PATH = Path("260310/production_output/atlanta_assignment_result.csv")
ENGINEER_DAY_SUMMARY_PATH = Path("260310/production_output/atlanta_engineer_day_summary.csv")
SCHEDULE_PATH = Path("260310/production_output/atlanta_schedule.csv")
LINE_ACTUAL_ASSIGNMENT_PATH = Path("260310/production_output/atlanta_assignment_result_actual_attendance.csv")
LINE_ACTUAL_ENGINEER_DAY_SUMMARY_PATH = Path("260310/production_output/atlanta_engineer_day_summary_actual_attendance.csv")
LINE_ACTUAL_SCHEDULE_PATH = Path("260310/production_output/atlanta_schedule_actual_attendance.csv")
OSRM_ASSIGNMENT_PATH = Path("260310/production_output/atlanta_assignment_result_osrm.csv")
OSRM_ENGINEER_DAY_SUMMARY_PATH = Path("260310/production_output/atlanta_engineer_day_summary_osrm.csv")
OSRM_SCHEDULE_PATH = Path("260310/production_output/atlanta_schedule_osrm.csv")
OSRM_ACTUAL_ASSIGNMENT_PATH = Path("260310/production_output/atlanta_assignment_result_osrm_actual_attendance.csv")
OSRM_ACTUAL_ENGINEER_DAY_SUMMARY_PATH = Path("260310/production_output/atlanta_engineer_day_summary_osrm_actual_attendance.csv")
OSRM_ACTUAL_SCHEDULE_PATH = Path("260310/production_output/atlanta_schedule_osrm_actual_attendance.csv")
DAILY_COMPARE_PATH = Path("260310/production_output/atlanta_daily_compare_line_vs_osrm.csv")
DAILY_COMPARE_ACTUAL_PATH = Path("260310/production_output/atlanta_daily_compare_line_actual_vs_osrm_actual.csv")


def _load_config(config_file: Path = CONFIG_FILE) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


@st.cache_resource(show_spinner=False)
def get_route_client() -> OSRMTripClient:
    routing_cfg = _load_config().get("routing", {})
    return OSRMTripClient(
        OSRMConfig(
            osrm_url=str(routing_cfg.get("city_osrm_urls", {}).get("Atlanta, GA", routing_cfg.get("osrm_url", "http://20.51.244.68:5002"))).rstrip("/"),
            mode="osrm",
            osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
            cache_file=Path("data/cache/osrm_trip_cache_atlanta_production_map.csv"),
            fallback_osrm_url=str(routing_cfg.get("osrm_url", "http://20.51.244.68:5000")).rstrip("/"),
        )
    )


def _generate_color_map(labels: list[str]) -> dict[str, str]:
    color_map: dict[str, str] = {}
    hue = 0.11
    golden_ratio = 0.618033988749895
    for label in sorted({str(v).strip() for v in labels if str(v).strip()}):
        hue = (hue + golden_ratio) % 1.0
        rgb = colorsys.hsv_to_rgb(hue, 0.68, 0.92)
        color_map[label] = "#{:02x}{:02x}{:02x}".format(int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))
    return color_map


@st.cache_data(show_spinner=False)
def load_inputs():
    region_zip_df = pd.read_csv(REGION_ZIP_PATH, encoding="utf-8-sig")
    engineer_region_df = pd.read_csv(ENGINEER_REGION_PATH, encoding="utf-8-sig")
    home_df = pd.read_csv(HOME_GEOCODE_PATH, encoding="utf-8-sig")
    base_service_df = pd.read_csv(SERVICE_PATH, encoding="utf-8-sig", low_memory=False)

    assignment_df = pd.read_csv(ASSIGNMENT_PATH, encoding="utf-8-sig", low_memory=False) if ASSIGNMENT_PATH.exists() else pd.DataFrame()
    engineer_day_summary_df = (
        pd.read_csv(ENGINEER_DAY_SUMMARY_PATH, encoding="utf-8-sig", low_memory=False) if ENGINEER_DAY_SUMMARY_PATH.exists() else pd.DataFrame()
    )
    schedule_df = pd.read_csv(SCHEDULE_PATH, encoding="utf-8-sig", low_memory=False) if SCHEDULE_PATH.exists() else pd.DataFrame()
    line_actual_assignment_df = pd.read_csv(LINE_ACTUAL_ASSIGNMENT_PATH, encoding="utf-8-sig", low_memory=False) if LINE_ACTUAL_ASSIGNMENT_PATH.exists() else pd.DataFrame()
    line_actual_engineer_day_summary_df = (
        pd.read_csv(LINE_ACTUAL_ENGINEER_DAY_SUMMARY_PATH, encoding="utf-8-sig", low_memory=False)
        if LINE_ACTUAL_ENGINEER_DAY_SUMMARY_PATH.exists()
        else pd.DataFrame()
    )
    line_actual_schedule_df = pd.read_csv(LINE_ACTUAL_SCHEDULE_PATH, encoding="utf-8-sig", low_memory=False) if LINE_ACTUAL_SCHEDULE_PATH.exists() else pd.DataFrame()
    osrm_assignment_df = pd.read_csv(OSRM_ASSIGNMENT_PATH, encoding="utf-8-sig", low_memory=False) if OSRM_ASSIGNMENT_PATH.exists() else pd.DataFrame()
    osrm_engineer_day_summary_df = (
        pd.read_csv(OSRM_ENGINEER_DAY_SUMMARY_PATH, encoding="utf-8-sig", low_memory=False)
        if OSRM_ENGINEER_DAY_SUMMARY_PATH.exists()
        else pd.DataFrame()
    )
    osrm_schedule_df = pd.read_csv(OSRM_SCHEDULE_PATH, encoding="utf-8-sig", low_memory=False) if OSRM_SCHEDULE_PATH.exists() else pd.DataFrame()
    osrm_actual_assignment_df = pd.read_csv(OSRM_ACTUAL_ASSIGNMENT_PATH, encoding="utf-8-sig", low_memory=False) if OSRM_ACTUAL_ASSIGNMENT_PATH.exists() else pd.DataFrame()
    osrm_actual_engineer_day_summary_df = (
        pd.read_csv(OSRM_ACTUAL_ENGINEER_DAY_SUMMARY_PATH, encoding="utf-8-sig", low_memory=False)
        if OSRM_ACTUAL_ENGINEER_DAY_SUMMARY_PATH.exists()
        else pd.DataFrame()
    )
    osrm_actual_schedule_df = pd.read_csv(OSRM_ACTUAL_SCHEDULE_PATH, encoding="utf-8-sig", low_memory=False) if OSRM_ACTUAL_SCHEDULE_PATH.exists() else pd.DataFrame()
    daily_compare_df = pd.read_csv(DAILY_COMPARE_PATH, encoding="utf-8-sig", low_memory=False) if DAILY_COMPARE_PATH.exists() else pd.DataFrame()
    daily_compare_actual_df = (
        pd.read_csv(DAILY_COMPARE_ACTUAL_PATH, encoding="utf-8-sig", low_memory=False)
        if DAILY_COMPARE_ACTUAL_PATH.exists()
        else pd.DataFrame()
    )

    region_zip_df["POSTAL_CODE"] = region_zip_df["POSTAL_CODE"].astype(str).str.zfill(5)
    engineer_region_df["engineer_label"] = engineer_region_df["SVC_ENGINEER_CODE"].astype(str) + " | " + engineer_region_df["Name"].astype(str)
    if "POSTAL_CODE" in base_service_df.columns:
        base_service_df["POSTAL_CODE"] = base_service_df["POSTAL_CODE"].astype(str).str.zfill(5)
    if "new_region_name" not in base_service_df.columns or "region_seq" not in base_service_df.columns:
        base_service_df = base_service_df.merge(
            region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates(),
            on="POSTAL_CODE",
            how="left",
        )

    for df in [
        base_service_df,
        assignment_df,
        schedule_df,
        line_actual_assignment_df,
        line_actual_schedule_df,
        osrm_assignment_df,
        osrm_schedule_df,
        osrm_actual_assignment_df,
        osrm_actual_schedule_df,
    ]:
        if df.empty:
            continue
        df["POSTAL_CODE"] = df["POSTAL_CODE"].astype(str).str.zfill(5)
        if "service_date" in df.columns:
            df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")
        if "service_date_key" not in df.columns and "service_date" in df.columns:
            df["service_date_key"] = df["service_date"].dt.strftime("%Y-%m-%d")
        if "latitude" in df.columns:
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        if "longitude" in df.columns:
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        if "service_time_min" in df.columns:
            df["service_time_min"] = pd.to_numeric(df["service_time_min"], errors="coerce").fillna(45)

    for df in [
        engineer_day_summary_df,
        line_actual_engineer_day_summary_df,
        osrm_engineer_day_summary_df,
        osrm_actual_engineer_day_summary_df,
        daily_compare_df,
        daily_compare_actual_df,
    ]:
        if df.empty:
            continue
        if "service_date_key" not in df.columns and "service_date" in df.columns:
            service_date = pd.to_datetime(df["service_date"], errors="coerce")
            df["service_date_key"] = service_date.dt.strftime("%Y-%m-%d")
        if "service_date_key" in df.columns:
            df["service_date_key"] = df["service_date_key"].astype(str)

    return (
        region_zip_df,
        engineer_region_df,
        home_df,
        base_service_df,
        assignment_df,
        engineer_day_summary_df,
        schedule_df,
        line_actual_assignment_df,
        line_actual_engineer_day_summary_df,
        line_actual_schedule_df,
        osrm_assignment_df,
        osrm_engineer_day_summary_df,
        osrm_schedule_df,
        osrm_actual_assignment_df,
        osrm_actual_engineer_day_summary_df,
        osrm_actual_schedule_df,
        daily_compare_df,
        daily_compare_actual_df,
    )


@st.cache_data(show_spinner=False)
def build_region_layers():
    city_data = load_city_map_data("Atlanta, GA")
    region_zip_df = load_inputs()[0].copy()
    coverage_df = pd.read_excel(PROFILE_COPY_PATH, sheet_name="1. Zip Coverage", dtype={"POSTAL_CODE": str})
    coverage_df = coverage_df[coverage_df["STRATEGIC_CITY_NAME"].astype(str).eq("Atlanta, GA")].copy()
    coverage_df["POSTAL_CODE"] = coverage_df["POSTAL_CODE"].astype(str).str.zfill(5)
    coverage_df = coverage_df[["POSTAL_CODE"]].drop_duplicates().copy()
    zip_layer = city_data.zip_layer.copy()
    zip_layer["POSTAL_CODE"] = zip_layer["POSTAL_CODE"].astype(str).str.zfill(5)
    coverage_layer = zip_layer.merge(coverage_df, on="POSTAL_CODE", how="inner")
    assigned_projected = zip_layer.merge(
        region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]], on="POSTAL_CODE", how="inner"
    ).to_crs(epsg=3857)
    region_center_lookup: dict[int, tuple[float, float, str]] = {}
    for (region_seq, region_name), group in assigned_projected.groupby(["region_seq", "new_region_name"], dropna=False):
        centroid = group.geometry.union_all().centroid
        region_center_lookup[int(region_seq)] = (float(centroid.x), float(centroid.y), str(region_name))

    merged = coverage_layer.merge(region_zip_df, on="POSTAL_CODE", how="left")
    unassigned_mask = merged["region_seq"].isna()
    if unassigned_mask.any() and region_center_lookup:
        projected = merged.to_crs(epsg=3857)
        for idx, geom in projected.loc[unassigned_mask, "geometry"].items():
            centroid = geom.centroid
            best_region = None
            best_distance = None
            for region_seq, (cx, cy, region_name) in region_center_lookup.items():
                distance = math.hypot(float(centroid.x) - cx, float(centroid.y) - cy)
                if best_distance is None or distance < best_distance:
                    best_distance = distance
                    best_region = (region_seq, region_name)
            if best_region is not None:
                merged.loc[idx, "region_seq"] = int(best_region[0])
                merged.loc[idx, "new_region_name"] = str(best_region[1])
    merged["service_count"] = merged["POSTAL_CODE"].map(
        load_inputs()[3]["POSTAL_CODE"].astype(str).str.zfill(5).value_counts()
    ).fillna(0).astype(int)
    region_layer = (
        merged.dissolve(by="new_region_name", as_index=False, aggfunc="first")[["new_region_name", "region_seq", "geometry"]]
        .sort_values("region_seq")
        .reset_index(drop=True)
    )
    return merged, region_layer


def _build_actual_mode_frames(
    service_df: pd.DataFrame,
    home_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if service_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    actual_df = service_df.copy()
    actual_df["SVC_CENTER_TYPE"] = actual_df["SVC_CENTER_TYPE"].astype(str).str.upper()
    actual_df = actual_df[actual_df["SVC_CENTER_TYPE"].isin(["DMS", "DMS2"])].copy()
    if actual_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    home_lookup = (
        home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]]
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        .rename(columns={"latitude": "home_latitude", "longitude": "home_longitude"})
    )
    labeled_df = actual_df.merge(home_lookup, on="SVC_ENGINEER_CODE", how="left")
    labeled_df["assigned_sm_code"] = labeled_df["SVC_ENGINEER_CODE"].astype(str)
    labeled_df["assigned_sm_name"] = labeled_df["SVC_ENGINEER_NAME"].astype(str)
    labeled_df["assigned_center_type"] = labeled_df["SVC_CENTER_TYPE"].astype(str)
    labeled_df["home_start_longitude"] = labeled_df["home_longitude"]
    labeled_df["home_start_latitude"] = labeled_df["home_latitude"]

    route_client = get_route_client()
    schedule_frames: list[pd.DataFrame] = []
    for _, group_df in labeled_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False):
        schedule_df, route_payload = production_assign_atlanta._build_schedule_for_group(group_df.copy(), route_client)
        if schedule_df.empty:
            continue
        schedule_df["route_distance_km"] = round(float(route_payload["distance_km"]), 2)
        schedule_df["route_duration_min"] = round(float(route_payload["duration_min"]), 2)
        schedule_frames.append(schedule_df)

    schedule_result_df = pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame()

    service_counts = (
        labeled_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False)["GSFS_RECEIPT_NO"]
        .nunique()
        .rename("job_count")
        .reset_index()
    )
    service_time = (
        labeled_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False)["service_time_min"]
        .sum()
        .rename("service_time_min")
        .reset_index()
    )
    meta = (
        labeled_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False)
        .agg(
            SVC_ENGINEER_NAME=("assigned_sm_name", "first"),
            assigned_center_type=("assigned_center_type", "first"),
            assigned_region_seq=("region_seq", lambda s: pd.to_numeric(s, errors="coerce").mode().iloc[0] if not pd.to_numeric(s, errors="coerce").dropna().empty else pd.NA),
        )
        .reset_index()
        .rename(columns={"assigned_sm_code": "SVC_ENGINEER_CODE"})
    )
    route_summary = pd.DataFrame()
    if not schedule_result_df.empty:
        route_summary = (
            schedule_result_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False)
            .agg(route_distance_km=("route_distance_km", "max"), route_duration_min=("route_duration_min", "max"))
            .reset_index()
            .rename(columns={"assigned_sm_code": "SVC_ENGINEER_CODE"})
        )

    summary_df = (
        meta.merge(service_counts.rename(columns={"assigned_sm_code": "SVC_ENGINEER_CODE"}), on=["service_date_key", "SVC_ENGINEER_CODE"], how="left")
        .merge(service_time.rename(columns={"assigned_sm_code": "SVC_ENGINEER_CODE"}), on=["service_date_key", "SVC_ENGINEER_CODE"], how="left")
        .merge(route_summary, on=["service_date_key", "SVC_ENGINEER_CODE"], how="left")
    )
    summary_df["job_count"] = pd.to_numeric(summary_df["job_count"], errors="coerce").fillna(0).astype(int)
    summary_df["service_time_min"] = pd.to_numeric(summary_df["service_time_min"], errors="coerce").fillna(0)
    summary_df["route_distance_km"] = pd.to_numeric(summary_df.get("route_distance_km"), errors="coerce").fillna(0)
    summary_df["route_duration_min"] = pd.to_numeric(summary_df.get("route_duration_min"), errors="coerce").fillna(0)
    summary_df["travel_time_min"] = summary_df["route_duration_min"]
    summary_df["travel_distance_km"] = summary_df["route_distance_km"]
    summary_df["total_work_min"] = (summary_df["service_time_min"] + summary_df["travel_time_min"]).round(2)
    summary_df["overflow_480"] = summary_df["total_work_min"] > 480
    summary_df = summary_df.rename(columns={"SVC_ENGINEER_NAME": "SVC_ENGINEER_NAME"})

    return labeled_df, summary_df, schedule_result_df


def _build_actual_summary_only(service_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if service_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    actual_df = service_df.copy()
    actual_df["SVC_CENTER_TYPE"] = actual_df["SVC_CENTER_TYPE"].astype(str).str.upper()
    actual_df = actual_df[actual_df["SVC_CENTER_TYPE"].isin(["DMS", "DMS2"])].copy()
    if actual_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    actual_df["assigned_sm_code"] = actual_df["SVC_ENGINEER_CODE"].astype(str)
    actual_df["assigned_sm_name"] = actual_df["SVC_ENGINEER_NAME"].astype(str)
    actual_df["assigned_center_type"] = actual_df["SVC_CENTER_TYPE"].astype(str)
    summary_df = (
        actual_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False)
        .agg(
            SVC_ENGINEER_NAME=("assigned_sm_name", "first"),
            assigned_center_type=("assigned_center_type", "first"),
            assigned_region_seq=("region_seq", lambda s: pd.to_numeric(s, errors="coerce").mode().iloc[0] if not pd.to_numeric(s, errors="coerce").dropna().empty else pd.NA),
            job_count=("GSFS_RECEIPT_NO", "nunique"),
            service_time_min=("service_time_min", "sum"),
        )
        .reset_index()
        .rename(columns={"assigned_sm_code": "SVC_ENGINEER_CODE"})
    )
    summary_df["travel_time_min"] = 0.0
    summary_df["travel_distance_km"] = 0.0
    summary_df["route_distance_km"] = 0.0
    summary_df["route_duration_min"] = 0.0
    summary_df["total_work_min"] = pd.to_numeric(summary_df["service_time_min"], errors="coerce").fillna(0)
    summary_df["overflow_480"] = summary_df["total_work_min"] > 480
    return actual_df, summary_df


@st.cache_data(show_spinner=False)
def get_route_payload(coords: tuple[tuple[float, float], ...]):
    return get_route_client().build_ordered_route(coords, preserve_first=len(coords) > 1)


def _region_color_map() -> dict[str, str]:
    return {
        "Atlanta New Region 1": "#db4437",
        "Atlanta New Region 2": "#0f9d58",
        "Atlanta New Region 3": "#4285f4",
    }


def _popup(content: str, width: int = 360) -> folium.Popup:
    wrapped = (
        f"<div style='min-width:{width}px;max-width:{width}px;white-space:normal;"
        "line-height:1.4;font-size:13px;'>"
        f"{content}</div>"
    )
    return folium.Popup(wrapped, max_width=width + 40)


def _build_route_groups(schedule_df: pd.DataFrame):
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
        route_payload = get_route_payload(tuple(coord_chain))
        route_groups.append(
            {
                "engineer_code": str(engineer_code),
                "engineer_name": str(group["assigned_sm_name"].iloc[0]),
                "route_payload": route_payload,
                "scheduled_rows": group.to_dict("records"),
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "service_time_min": float(group["service_time_min"].sum()),
                "home_coord": start_coord,
                "center_type": str(group["assigned_center_type"].iloc[0]),
            }
        )
    return route_groups


def build_map(region_name: str, display_service_df: pd.DataFrame, home_df: pd.DataFrame, route_groups: list[dict]):
    zip_layer, region_layer = build_region_layers()
    region_colors = _region_color_map()
    engineer_colors = _generate_color_map([group["engineer_code"] for group in route_groups])

    if region_name != "ALL":
        zip_layer = zip_layer[zip_layer["new_region_name"] == region_name].copy()
        region_layer = region_layer[region_layer["new_region_name"] == region_name].copy()
        display_service_df = display_service_df[display_service_df["new_region_name"] == region_name].copy()
        home_df = home_df[home_df["assigned_region_name"] == region_name].copy()

    if not region_layer.empty:
        center_points = region_layer.to_crs(epsg=3857).geometry.centroid.to_crs(epsg=4326)
        center_lat = float(center_points.y.mean())
        center_lon = float(center_points.x.mean())
    else:
        center_lat, center_lon = 33.7490, -84.3880

    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=9, tiles="CartoDB positron")

    folium.GeoJson(
        data=zip_layer.to_json(),
        name="ZIP Coverage",
        style_function=lambda feature: {
            "color": "#c5c9cf" if int(feature["properties"].get("service_count", 0) or 0) == 0 else "#9aa0a6",
            "weight": 0.5 if int(feature["properties"].get("service_count", 0) or 0) == 0 else 0.8,
            "fillColor": "#eceff3" if int(feature["properties"].get("service_count", 0) or 0) == 0 else region_colors.get(feature["properties"].get("new_region_name", ""), "#dddddd"),
            "fillOpacity": 0.05 if int(feature["properties"].get("service_count", 0) or 0) == 0 else 0.12,
        },
        tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "new_region_name", "service_count"], aliases=["ZIP", "Region", "Service Count"]),
    ).add_to(fmap)

    folium.GeoJson(
        data=region_layer.to_json(),
        name="Production Regions",
        style_function=lambda feature: {
            "color": region_colors.get(feature["properties"].get("new_region_name", ""), "#333333"),
            "weight": 3,
            "fillColor": region_colors.get(feature["properties"].get("new_region_name", ""), "#dddddd"),
            "fillOpacity": 0.15,
        },
        tooltip=folium.GeoJsonTooltip(fields=["new_region_name"], aliases=["Region"]),
    ).add_to(fmap)

    if route_groups:
        route_layer = folium.FeatureGroup(name="Assigned Routes").add_to(fmap)
        for group in route_groups:
            engineer_color = engineer_colors.get(group["engineer_code"], "#111827")
            geometry = group["route_payload"]["geometry"]
            if geometry:
                folium.PolyLine(
                    locations=geometry,
                    color=engineer_color,
                    weight=3,
                    opacity=0.85,
                    popup=_popup(
                        f"<b>Engineer</b>: {group['engineer_code']} | {group['engineer_name']}<br>"
                        f"<b>Service Count</b>: {group['service_count']} | "
                        f"<b>Distance</b>: {group['route_payload']['distance_km']:.2f} km | "
                        f"<b>Duration</b>: {group['route_payload']['duration_min']:.2f} min",
                        width=420,
                    ),
                ).add_to(route_layer)

            if group["home_coord"] is not None:
                home_lon, home_lat = group["home_coord"]
                folium.Marker(
                    location=[home_lat, home_lon],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:10px;font-weight:700;color:{engineer_color};"
                            f"background:#fff;border:2px solid {engineer_color};border-radius:12px;"
                            "padding:2px 6px;text-align:center;white-space:nowrap;\">"
                            "Home</div>"
                        )
                    ),
                    popup=_popup(f"<b>Home Start</b>: {group['engineer_code']}", width=260),
                ).add_to(route_layer)

            for row in group["scheduled_rows"]:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                seq = int(row.get("visit_seq", 0))
                folium.Marker(
                    location=[lat, lon],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:11px;font-weight:700;color:{engineer_color};"
                            f"background:#fff;border:2px solid {engineer_color};border-radius:12px;"
                            "width:22px;height:22px;line-height:18px;text-align:center;\">"
                            f"{seq}</div>"
                        )
                    ),
                    popup=_popup(
                        f"<b>Engineer</b>: {row.get('assigned_sm_code', '')} | "
                        f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')} | "
                        f"<b>Seq</b>: {seq}<br>"
                        f"<b>Start</b>: {row.get('visit_start_time', '')} | "
                        f"<b>End</b>: {row.get('visit_end_time', '')} | "
                        f"<b>Travel</b>: {row.get('travel_time_from_prev_min', 0)} min | "
                        f"<b>Service</b>: {int(float(row.get('service_time_min', 45)))} min",
                        width=460,
                    ),
                ).add_to(route_layer)
    else:
        point_cluster = MarkerCluster(name="Service Points").add_to(fmap)
        for _, row in display_service_df.iterrows():
            if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
                continue
            popup = _popup(
                f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')} | "
                f"<b>Region</b>: {row.get('new_region_name', '')}<br>"
                f"<b>Product Group</b>: {row.get('SERVICE_PRODUCT_GROUP_CODE', '')} | "
                f"<b>Heavy Repair</b>: {'Y' if bool(row.get('is_heavy_repair')) else 'N'} | "
                f"<b>Service Time</b>: {int(float(row.get('service_time_min', 45)))} min",
                width=420,
            )
            folium.CircleMarker(
                location=[float(row["latitude"]), float(row["longitude"])],
                radius=4,
                color=region_colors.get(str(row.get("new_region_name", "")), "#555555"),
                weight=1,
                fill=True,
                fill_color=region_colors.get(str(row.get("new_region_name", "")), "#555555"),
                fill_opacity=0.75,
                popup=popup,
            ).add_to(point_cluster)

    home_group = folium.FeatureGroup(name="Engineer Homes").add_to(fmap)
    for _, row in home_df.iterrows():
        if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
            continue
        code = str(row.get("SVC_ENGINEER_CODE", ""))
        border_color = engineer_colors.get(code, "#000000" if str(row.get("SVC_CENTER_TYPE", "")).strip().upper() == "DMS2" else "#444444")
        popup = (
            f"<b>Engineer</b>: {row.get('SVC_ENGINEER_CODE', '')} | "
            f"<b>Name</b>: {row.get('Name', '')}<br>"
            f"<b>Center Type</b>: {row.get('SVC_CENTER_TYPE', '')} | "
            f"<b>Assigned Region</b>: {row.get('assigned_region_name', '')}<br>"
            f"<b>REF Heavy Repair</b>: {row.get('REF_HEAVY_REPAIR_FLAG', '')}"
        )
        folium.Marker(
            location=[float(row["latitude"]), float(row["longitude"])],
            icon=folium.DivIcon(
                html=(
                    f"<div style=\"font-size:10px;font-weight:700;color:{border_color};"
                    f"background:#fff;border:2px solid {border_color};border-radius:12px;"
                    "padding:2px 6px;text-align:center;white-space:nowrap;\">"
                    "Home</div>"
                )
            ),
            popup=_popup(popup, width=440),
        ).add_to(home_group)

    folium.LayerControl(collapsed=False).add_to(fmap)
    return fmap


def main():
    st.title("Atlanta Production Routing")

    (
        region_zip_df,
        engineer_region_df,
        home_df,
        base_service_df,
        assignment_df,
        engineer_day_summary_df,
        schedule_df,
        line_actual_assignment_df,
        line_actual_engineer_day_summary_df,
        line_actual_schedule_df,
        osrm_assignment_df,
        osrm_engineer_day_summary_df,
        osrm_schedule_df,
        osrm_actual_assignment_df,
        osrm_actual_engineer_day_summary_df,
        osrm_actual_schedule_df,
        daily_compare_df,
        daily_compare_actual_df,
    ) = load_inputs()

    line_service_df = assignment_df.copy() if not assignment_df.empty else base_service_df.copy()
    date_options = ["ALL"] + sorted(line_service_df["service_date_key"].dropna().unique().tolist())
    region_options = ["ALL"] + sorted(region_zip_df["new_region_name"].dropna().unique().tolist())
    engineer_label_parts = []
    if "engineer_label" in engineer_region_df.columns:
        engineer_label_parts.extend(engineer_region_df["engineer_label"].dropna().astype(str).tolist())
    actual_engineer_source = base_service_df.copy()
    if not actual_engineer_source.empty:
        actual_engineer_source["SVC_CENTER_TYPE"] = actual_engineer_source["SVC_CENTER_TYPE"].astype(str).str.upper()
        actual_engineer_source = actual_engineer_source[actual_engineer_source["SVC_CENTER_TYPE"].isin(["DMS", "DMS2"])].copy()
    if not actual_engineer_source.empty:
        actual_labels = (
            actual_engineer_source[["SVC_ENGINEER_CODE", "SVC_ENGINEER_NAME"]]
            .drop_duplicates()
            .astype(str)
            .assign(engineer_label=lambda df: df["SVC_ENGINEER_CODE"] + " | " + df["SVC_ENGINEER_NAME"])
        )
        engineer_label_parts.extend(actual_labels["engineer_label"].tolist())
    engineer_options = ["ALL"] + sorted({label for label in engineer_label_parts if str(label).strip()})
    assignment_mode_options = []
    if not actual_engineer_source.empty:
        assignment_mode_options.append("Actual Routes")
    assignment_mode_options.append("Line Assign")
    if not line_actual_assignment_df.empty:
        assignment_mode_options.append("Line Assign (Actual Attendance)")
    if not osrm_assignment_df.empty:
        assignment_mode_options.append("OSRM Assign")
    if not osrm_actual_assignment_df.empty:
        assignment_mode_options.append("OSRM Assign (Actual Attendance)")

    left, right = st.columns([1, 2.25])
    with left:
        selected_mode = st.selectbox("Assignment Mode", assignment_mode_options, index=0)
        selected_date = st.selectbox("Date", date_options, index=0)
        selected_region = st.selectbox("Production Region", region_options, index=0)
        selected_engineer = st.selectbox("Engineer", engineer_options, index=0)

        display_date = selected_date
        if selected_mode == "Actual Routes" and not actual_engineer_source.empty:
            active_service_df, active_summary_df = _build_actual_summary_only(base_service_df)
            active_schedule_df = pd.DataFrame()
        elif selected_mode == "Line Assign (Actual Attendance)" and not line_actual_assignment_df.empty:
            active_service_df = line_actual_assignment_df.copy()
            active_summary_df = line_actual_engineer_day_summary_df.copy()
            active_schedule_df = line_actual_schedule_df.copy()
        elif selected_mode == "OSRM Assign" and not osrm_assignment_df.empty:
            active_service_df = osrm_assignment_df.copy()
            active_summary_df = osrm_engineer_day_summary_df.copy()
            active_schedule_df = osrm_schedule_df.copy()
        elif selected_mode == "OSRM Assign (Actual Attendance)" and not osrm_actual_assignment_df.empty:
            active_service_df = osrm_actual_assignment_df.copy()
            active_summary_df = osrm_actual_engineer_day_summary_df.copy()
            active_schedule_df = osrm_actual_schedule_df.copy()
        else:
            active_service_df = line_service_df.copy()
            active_summary_df = engineer_day_summary_df.copy()
            active_schedule_df = schedule_df.copy()

        filtered_service = active_service_df.copy()
        filtered_home = home_df.copy()
        filtered_schedule = active_schedule_df.copy()
        filtered_summary = active_summary_df.copy()

        if display_date != "ALL":
            filtered_service = filtered_service[filtered_service["service_date_key"] == display_date].copy()
            if not filtered_schedule.empty:
                filtered_schedule = filtered_schedule[filtered_schedule["service_date_key"] == display_date].copy()
            if not filtered_summary.empty and "service_date_key" in filtered_summary.columns:
                filtered_summary = filtered_summary[filtered_summary["service_date_key"] == display_date].copy()
            if selected_mode == "Actual Routes":
                filtered_service, filtered_summary, filtered_schedule = _build_actual_mode_frames(filtered_service, filtered_home)
        if selected_region != "ALL":
            filtered_service = filtered_service[filtered_service["new_region_name"] == selected_region].copy()
            filtered_home = filtered_home[filtered_home["assigned_region_name"] == selected_region].copy()
            if not filtered_schedule.empty:
                filtered_schedule = filtered_schedule[filtered_schedule["new_region_name"] == selected_region].copy()
            if not filtered_summary.empty and "assigned_region_seq" in filtered_summary.columns:
                region_seq = int(selected_region.split()[-1])
                filtered_summary = filtered_summary[
                    (filtered_summary["assigned_region_seq"].isna())
                    | (pd.to_numeric(filtered_summary["assigned_region_seq"], errors="coerce") == region_seq)
                ].copy()
        if selected_engineer != "ALL":
            engineer_code = selected_engineer.split("|", 1)[0].strip()
            code_str = str(engineer_code)
            service_engineer_col = "assigned_sm_code" if "assigned_sm_code" in filtered_service.columns else "SVC_ENGINEER_CODE"
            filtered_service = filtered_service[filtered_service[service_engineer_col].astype(str) == code_str].copy()
            filtered_home = filtered_home[filtered_home["SVC_ENGINEER_CODE"].astype(str) == code_str].copy()
            if not filtered_schedule.empty:
                schedule_engineer_col = "assigned_sm_code" if "assigned_sm_code" in filtered_schedule.columns else "SVC_ENGINEER_CODE"
                filtered_schedule = filtered_schedule[filtered_schedule[schedule_engineer_col].astype(str) == code_str].copy()
            if not filtered_summary.empty and "SVC_ENGINEER_CODE" in filtered_summary.columns:
                filtered_summary = filtered_summary[filtered_summary["SVC_ENGINEER_CODE"].astype(str) == code_str].copy()

        route_groups = _build_route_groups(filtered_schedule) if display_date != "ALL" else []
        service_count = int(filtered_service["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) if not filtered_service.empty else 0
        heavy_count = int(filtered_service["is_heavy_repair"].fillna(False).astype(bool).sum()) if not filtered_service.empty else 0
        total_service_time = int(filtered_service["service_time_min"].sum()) if not filtered_service.empty else 0
        tv_count = int(filtered_service["is_tv_job"].fillna(False).astype(bool).sum()) if not filtered_service.empty else 0
        active_summary = filtered_summary.copy()
        if not active_summary.empty and "job_count" in active_summary.columns:
            active_summary = active_summary[pd.to_numeric(active_summary["job_count"], errors="coerce").fillna(0) > 0].copy()
        engineer_count = int(len(active_summary)) if not active_summary.empty else int(
            filtered_service["assigned_sm_code"].dropna().astype(str).nunique()
            if "assigned_sm_code" in filtered_service.columns and not filtered_service.empty
            else filtered_service["SVC_ENGINEER_CODE"].dropna().astype(str).nunique() if not filtered_service.empty else 0
        )
        dms_count = 0
        dms2_count = 0
        if not active_summary.empty and "assigned_center_type" in active_summary.columns:
            center_counts = active_summary["assigned_center_type"].astype(str).str.upper().value_counts()
            dms_count = int(center_counts.get("DMS", 0))
            dms2_count = int(center_counts.get("DMS2", 0))
        if display_date != "ALL" and route_groups:
            avg_distance = sum(group["route_payload"]["distance_km"] for group in route_groups) / len(route_groups)
            avg_duration = sum(group["route_payload"]["duration_min"] for group in route_groups) / len(route_groups)
        elif not active_summary.empty:
            distance_series = pd.to_numeric(active_summary.get("route_distance_km"), errors="coerce")
            duration_series = pd.to_numeric(active_summary.get("route_duration_min"), errors="coerce")
            distance_series = distance_series[distance_series.notna()]
            duration_series = duration_series[duration_series.notna()]
            avg_distance = float(distance_series.mean()) if not distance_series.empty else 0.0
            avg_duration = float(duration_series.mean()) if not duration_series.empty else 0.0
        else:
            avg_distance = 0.0
            avg_duration = 0.0
        jobs_std = 0.0
        if not filtered_service.empty:
            service_engineer_col = "assigned_sm_code" if "assigned_sm_code" in filtered_service.columns else "SVC_ENGINEER_CODE"
            weighted_service = filtered_service.copy()
            weighted_service["weighted_job_unit"] = weighted_service["is_heavy_repair"].fillna(False).astype(bool).map(lambda flag: 2.0 if flag else 1.0)
            weighted_jobs = (
                weighted_service.groupby(weighted_service[service_engineer_col].astype(str))["weighted_job_unit"]
                .sum()
            )
            weighted_jobs = weighted_jobs[weighted_jobs > 0]
            jobs_std = float(weighted_jobs.std(ddof=0)) if not weighted_jobs.empty else 0.0

        st.metric("Service Count", service_count)
        st.metric("Assigned Engineer Count", f"{engineer_count} (DMS {dms_count}, DMS2 {dms2_count})")
        st.metric("Heavy Repair Count", heavy_count)
        st.metric("TV Job Count", tv_count)
        st.metric("Total Service Time (min)", total_service_time)
        st.metric("Average Distance (km)", f"{avg_distance:.2f}")
        st.metric("Average Duration (min)", f"{avg_duration:.2f}")
        st.metric("Jobs per Engineer Std", f"{jobs_std:.2f}")

        compare_df = daily_compare_actual_df.copy() if "Actual Attendance" in selected_mode else daily_compare_df.copy()
        if selected_mode != "Actual Routes" and selected_date != "ALL" and not compare_df.empty:
            compare_row = compare_df[compare_df["service_date_key"] == selected_date].copy()
            if not compare_row.empty:
                compare_row = compare_row.iloc[0]
                compare_view = pd.DataFrame(
                    [
                        {
                            "mode": "Line Assign",
                            "distance_km": compare_row.get("line_total_distance_km"),
                            "duration_min": compare_row.get("line_total_duration_min"),
                            "service_count": compare_row.get("line_service_count"),
                            "assigned_engineers": compare_row.get("line_assigned_engineer_count"),
                            "jobs_std": compare_row.get("line_jobs_std"),
                        },
                        {
                            "mode": "OSRM Assign",
                            "distance_km": compare_row.get("osrm_total_distance_km"),
                            "duration_min": compare_row.get("osrm_total_duration_min"),
                            "service_count": compare_row.get("osrm_service_count"),
                            "assigned_engineers": compare_row.get("osrm_assigned_engineer_count"),
                            "jobs_std": compare_row.get("osrm_jobs_std"),
                        },
                    ]
                )
                st.markdown("**Daily Compare**")
                st.dataframe(compare_view, width="stretch", hide_index=True)

        st.markdown("**Assigned Service Count by Engineer**")
        if not filtered_summary.empty:
            summary_cols = [
                "SVC_ENGINEER_CODE",
                "SVC_ENGINEER_NAME",
                "assigned_center_type",
                "job_count",
                "service_time_min",
                "travel_time_min",
                "route_distance_km",
                "route_duration_min",
                "total_work_min",
                "overflow_480",
            ]
            use_cols = [col for col in summary_cols if col in filtered_summary.columns]
            st.dataframe(filtered_summary[use_cols].sort_values(["job_count", "SVC_ENGINEER_CODE"], ascending=[False, True]), width="stretch", hide_index=True)
        else:
            st.info("No assignment summary for the current selection.")

    with right:
        map_obj = build_map(selected_region, filtered_service, filtered_home, route_groups)
        html(map_obj._repr_html_(), height=860)


if __name__ == "__main__":
    main()
