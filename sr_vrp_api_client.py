from __future__ import annotations

import colorsys
import json
from datetime import date
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit.components.v1 import html

from smart_routing.area_map import load_city_map_data
from smart_routing.bigquery_runtime import query_service_data
from smart_routing.live_atlanta_runtime import build_runtime_atlanta_inputs
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient
from smart_routing.vrp_api_client import (
    build_payload_from_service_frame,
    get_routing_job_result,
    get_routing_job_status,
    submit_routing_job,
)


st.set_page_config(page_title="Smart Routing API Client", layout="wide")

ROUTING_MODE_OPTIONS = [
    ("na_general", "North America General"),
    ("weekday_general", "Weekday General"),
    ("z_weekday", "Z Weekday"),
    ("z_weekend", "Z Weekend"),
]


def _read_uploaded_service_csv(uploaded_file) -> pd.DataFrame:
    return pd.read_csv(uploaded_file, encoding="utf-8-sig", low_memory=False)


@st.cache_resource(show_spinner=False)
def get_route_client() -> OSRMTripClient:
    return OSRMTripClient(
        OSRMConfig(
            osrm_url="http://20.51.244.68:5002",
            mode="osrm",
            osrm_profile="driving",
            cache_file=Path("data/cache/osrm_trip_cache_atlanta_vrp_api_client.csv"),
            fallback_osrm_url="http://20.51.244.68:5000",
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


def _region_color_map() -> dict[str, str]:
    return {
        "Atlanta New Region 1": "#db4437",
        "Atlanta New Region 2": "#0f9d58",
        "Atlanta New Region 3": "#4285f4",
    }


def _build_region_layers(region_zip_df: pd.DataFrame, service_df: pd.DataFrame):
    city_data = load_city_map_data("Atlanta, GA")
    zip_layer = city_data.zip_layer.copy()
    zip_layer["POSTAL_CODE"] = zip_layer["POSTAL_CODE"].astype(str).str.zfill(5)
    coverage_df = region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates().copy()
    coverage_df["POSTAL_CODE"] = coverage_df["POSTAL_CODE"].astype(str).str.zfill(5)
    merged = zip_layer.merge(coverage_df, on="POSTAL_CODE", how="inner")
    if service_df.empty or "POSTAL_CODE" not in service_df.columns:
        postal_counts = pd.Series(dtype=int)
    else:
        postal_counts = service_df["POSTAL_CODE"].astype(str).str.zfill(5).value_counts()
    merged["service_count"] = merged["POSTAL_CODE"].map(postal_counts).fillna(0).astype(int)
    region_layer = (
        merged.dropna(subset=["new_region_name"])
        .dissolve(by="new_region_name", as_index=False, aggfunc="first")[["new_region_name", "region_seq", "geometry"]]
        .sort_values("region_seq")
        .reset_index(drop=True)
    )
    return merged, region_layer


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
        route_payload = get_route_client().build_ordered_route(tuple(coord_chain), preserve_first=start_coord is not None)
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
    required_cols = {"new_region_name", "assigned_sm_code", "assigned_center_type", "GSFS_RECEIPT_NO"}
    if service_df.empty or not required_cols.issubset(service_df.columns):
        return pd.DataFrame(columns=["region", "dms_count", "dms2_count", "dms_service_count", "dms2_service_count", "service_count"])
    staffing_df = service_df[["new_region_name", "assigned_sm_code", "assigned_center_type", "GSFS_RECEIPT_NO"]].dropna(
        subset=["new_region_name", "assigned_sm_code"]
    ).copy()
    staffing_df["assigned_center_type"] = staffing_df["assigned_center_type"].astype(str).str.upper()
    rows: list[dict[str, object]] = []
    for region_name, group in staffing_df.groupby("new_region_name", dropna=False):
        rows.append(
            {
                "region": str(region_name),
                "dms_count": int(group.loc[group["assigned_center_type"] == "DMS", "assigned_sm_code"].astype(str).nunique()),
                "dms2_count": int(group.loc[group["assigned_center_type"] == "DMS2", "assigned_sm_code"].astype(str).nunique()),
                "dms_service_count": int(group.loc[group["assigned_center_type"] == "DMS", "GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "dms2_service_count": int(group.loc[group["assigned_center_type"] == "DMS2", "GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
            }
        )
    return pd.DataFrame(rows).sort_values("region").reset_index(drop=True)


def _build_pre_result_service_view(service_df: pd.DataFrame) -> pd.DataFrame:
    if service_df.empty:
        return pd.DataFrame()
    preview_df = service_df.copy()
    preview_df["assigned_sm_code"] = preview_df.get("SVC_ENGINEER_CODE", pd.Series(index=preview_df.index)).astype(str)
    preview_df["assigned_sm_name"] = preview_df.get("SVC_ENGINEER_NAME", pd.Series(index=preview_df.index)).astype(str)
    preview_df["assigned_center_type"] = preview_df.get("SVC_CENTER_TYPE", pd.Series(index=preview_df.index)).astype(str)
    return preview_df


def _build_preview_route_groups(service_df: pd.DataFrame, home_df: pd.DataFrame):
    if service_df.empty:
        return []
    preview_df = _build_pre_result_service_view(service_df)
    home_lookup = (
        home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]]
        .drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        .rename(columns={"SVC_ENGINEER_CODE": "assigned_sm_code", "longitude": "home_start_longitude", "latitude": "home_start_latitude"})
    )
    preview_df = preview_df.merge(home_lookup, on="assigned_sm_code", how="left")
    preview_df["visit_seq"] = (
        preview_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False).cumcount() + 1
        if {"service_date_key", "assigned_sm_code"}.issubset(preview_df.columns)
        else range(1, len(preview_df) + 1)
    )
    preview_df["visit_start_time"] = ""
    preview_df["visit_end_time"] = ""
    preview_df["assigned_region_name"] = preview_df.get("new_region_name", pd.Series(index=preview_df.index))
    return _build_route_groups(preview_df)


def build_map(region_name: str, display_service_df: pd.DataFrame, home_df: pd.DataFrame, route_groups: list[dict], region_zip_df: pd.DataFrame):
    zip_layer, region_layer = _build_region_layers(region_zip_df, display_service_df)
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
            "fillColor": "none",
            "fillOpacity": 0.0,
        },
        tooltip=folium.GeoJsonTooltip(fields=["new_region_name"], aliases=["Region"]),
    ).add_to(fmap)

    if route_groups:
        route_layer = folium.FeatureGroup(name="Assigned Routes").add_to(fmap)
        for group in route_groups:
            engineer_color = engineer_colors.get(group["engineer_code"], "#111827")
            group_center_type = str(group.get("center_type", "")).upper()
            geometry = group["route_payload"]["geometry"]
            if geometry:
                folium.PolyLine(
                    locations=geometry,
                    color=engineer_color,
                    weight=3,
                    opacity=0.85,
                    popup=_popup(
                        f"<b>Engineer</b>: {group['engineer_name']}<br>"
                        f"<b>Engineer Code</b>: {group['engineer_code']}<br>"
                        f"<b>Service Count</b>: {group['service_count']} | "
                        f"<b>Distance</b>: {group['route_payload']['distance_km']:.2f} km | "
                        f"<b>Duration</b>: {group['route_payload']['duration_min']:.2f} min",
                        width=420,
                    ),
                ).add_to(route_layer)
            if group["home_coord"] is not None:
                home_lon, home_lat = group["home_coord"]
                home_bg = "#111111" if group_center_type == "DMS2" else "#ffffff"
                home_fg = "#ffffff" if group_center_type == "DMS2" else engineer_color
                folium.Marker(
                    location=[home_lat, home_lon],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:10px;font-weight:700;color:{home_fg};"
                            f"background:{home_bg};border:2px solid {engineer_color};border-radius:12px;"
                            "padding:2px 6px;text-align:center;white-space:nowrap;\">Home</div>"
                        )
                    ),
                    popup=_popup(f"<b>Home Start</b>: {group['engineer_name']}<br><b>Engineer Code</b>: {group['engineer_code']}", width=280),
                ).add_to(route_layer)
            for row in group["scheduled_rows"]:
                seq = int(row.get("visit_seq", 0))
                center_type = str(row.get("assigned_center_type", "")).strip().upper()
                marker_bg = "#111111" if center_type == "DMS2" else "#ffffff"
                marker_fg = "#ffffff" if center_type == "DMS2" else engineer_color
                changed_text = ""
                if "changed" in row:
                    changed_text = f"<b>Changed</b>: {'Y' if bool(row.get('changed', False)) else 'N'}<br>"
                popup_html = (
                    f"<b>Engineer</b>: {row.get('assigned_sm_name', '')}<br>"
                    f"<b>Engineer Code</b>: {row.get('assigned_sm_code', '')} | "
                    f"<b>Center Type</b>: {center_type} | "
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')} | "
                    f"<b>Seq</b>: {seq}<br>"
                    f"{changed_text}"
                    f"<b>Home Region</b>: {row.get('assigned_region_name', '')}<br>"
                    f"<b>Product Group</b>: {row.get('SERVICE_PRODUCT_GROUP_CODE', '')}<br>"
                    f"<b>Start</b>: {row.get('visit_start_time', '')} | "
                    f"<b>End</b>: {row.get('visit_end_time', '')}"
                )
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:11px;font-weight:700;color:{marker_fg};"
                            f"background:{marker_bg};border:2px solid {engineer_color};border-radius:12px;"
                            "width:22px;height:22px;line-height:18px;text-align:center;\">"
                            f"{seq}</div>"
                        )
                    ),
                    popup=_popup(popup_html, width=460),
                ).add_to(route_layer)
    else:
        point_cluster = MarkerCluster(name="Service Points").add_to(fmap)
        for _, row in display_service_df.iterrows():
            if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
                continue
            folium.CircleMarker(
                location=[float(row["latitude"]), float(row["longitude"])],
                radius=4,
                color=region_colors.get(str(row.get("new_region_name", "")), "#555555"),
                weight=1,
                fill=True,
                fill_color=region_colors.get(str(row.get("new_region_name", "")), "#555555"),
                fill_opacity=0.75,
                popup=_popup(
                    f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')} | "
                    f"<b>Region</b>: {row.get('new_region_name', '')}<br>"
                    f"<b>Product Group</b>: {row.get('SERVICE_PRODUCT_GROUP_CODE', '')}",
                    width=420,
                ),
            ).add_to(point_cluster)

    home_group = folium.FeatureGroup(name="Engineer Homes").add_to(fmap)
    for _, row in home_df.iterrows():
        if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
            continue
        code = str(row.get("SVC_ENGINEER_CODE", ""))
        border_color = engineer_colors.get(code, "#444444")
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
                f"<b>Engineer</b>: {row.get('Name', '')}<br>"
                f"<b>Engineer Code</b>: {row.get('SVC_ENGINEER_CODE', '')}<br>"
                f"<b>Assigned Region</b>: {row.get('assigned_region_name', '')}",
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


def _build_result_frames(result_payload: dict, runtime_state: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    assignments_df = pd.DataFrame(result_payload.get("assignments", []))
    if assignments_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    service_df = runtime_state["service_df"].copy()
    home_df = runtime_state["home_df"].copy()
    engineers_df = runtime_state["engineer_region_df"].copy()
    assignment_cols = ["salesforce_id", "receipt_no", "employee_code", "sequence", "planned_start", "planned_end", "changed"]
    merged = service_df.merge(
        assignments_df[assignment_cols].rename(columns={"receipt_no": "GSFS_RECEIPT_NO"}),
        on="GSFS_RECEIPT_NO",
        how="inner",
    )
    engineer_lookup = engineers_df[["SVC_ENGINEER_CODE", "Name", "SVC_CENTER_TYPE", "assigned_region_seq", "assigned_region_name"]].drop_duplicates(
        subset=["SVC_ENGINEER_CODE"]
    )
    merged = merged.merge(
        engineer_lookup.rename(
            columns={
                "SVC_ENGINEER_CODE": "assigned_sm_code",
                "Name": "assigned_sm_name",
                "SVC_CENTER_TYPE": "assigned_center_type",
                "assigned_region_seq": "assigned_region_seq",
                "assigned_region_name": "assigned_region_name",
            }
        ),
        left_on="employee_code",
        right_on="assigned_sm_code",
        how="left",
    )
    home_lookup = home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]].drop_duplicates(subset=["SVC_ENGINEER_CODE"]).rename(
        columns={"SVC_ENGINEER_CODE": "assigned_sm_code", "longitude": "home_start_longitude", "latitude": "home_start_latitude"}
    )
    merged = merged.merge(home_lookup, on="assigned_sm_code", how="left")
    merged["visit_seq"] = pd.to_numeric(merged["sequence"], errors="coerce").fillna(0).astype(int)
    merged["visit_start_time"] = pd.to_datetime(merged["planned_start"], errors="coerce").dt.strftime("%H:%M").fillna("")
    merged["visit_end_time"] = pd.to_datetime(merged["planned_end"], errors="coerce").dt.strftime("%H:%M").fillna("")
    merged["travel_time_from_prev_min"] = pd.NA
    merged["assigned_sm_name"] = merged["assigned_sm_name"].fillna(merged["employee_code"])
    merged["changed"] = merged.get("changed", False).fillna(False)
    schedule_df = merged.sort_values(["service_date_key", "assigned_sm_code", "visit_seq"]).reset_index(drop=True)
    assignment_df = schedule_df.copy()
    return assignment_df, schedule_df


def _build_actual_frames(runtime_state: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    service_df = runtime_state["service_df"].copy()
    home_df = runtime_state["home_df"].copy()
    if service_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    actual_df = service_df.copy()
    actual_df["assigned_sm_code"] = actual_df.get("SVC_ENGINEER_CODE", pd.Series(index=actual_df.index)).astype(str)
    actual_df["assigned_sm_name"] = actual_df.get("SVC_ENGINEER_NAME", pd.Series(index=actual_df.index)).astype(str)
    actual_df["assigned_center_type"] = actual_df.get("SVC_CENTER_TYPE", pd.Series(index=actual_df.index)).astype(str)
    home_lookup = home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]].drop_duplicates(subset=["SVC_ENGINEER_CODE"]).rename(
        columns={"SVC_ENGINEER_CODE": "assigned_sm_code", "longitude": "home_start_longitude", "latitude": "home_start_latitude"}
    )
    actual_df = actual_df.merge(home_lookup, on="assigned_sm_code", how="left")
    actual_df["visit_seq"] = actual_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False).cumcount() + 1
    actual_df["visit_start_time"] = ""
    actual_df["visit_end_time"] = ""
    actual_df["travel_time_from_prev_min"] = pd.NA
    schedule_df = actual_df.sort_values(["service_date_key", "assigned_sm_code", "visit_seq"]).reset_index(drop=True)
    assignment_df = schedule_df.copy()
    return assignment_df, schedule_df


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


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


@st.fragment(run_every="5s")
def _auto_poll_routing_status() -> None:
    server_url = str(st.session_state.get("smart_routing_server_url", "")).strip()
    job_id = str(st.session_state.get("vrp_job_id", "")).strip()
    current_status_payload = st.session_state.get("vrp_job_status") or {}
    current_status = str(current_status_payload.get("status", "")).strip().lower()
    if not server_url or not job_id or current_status not in {"queued", "running"}:
        return
    try:
        latest_status = get_routing_job_status(server_url, job_id)
        st.session_state["vrp_job_status"] = latest_status
        latest_state = str(latest_status.get("status", "")).strip().lower()
        if latest_state == "completed":
            st.session_state["vrp_job_result"] = get_routing_job_result(server_url, job_id)
            st.rerun()
        if latest_state == "failed":
            st.session_state["vrp_job_result"] = None
            st.rerun()
    except Exception:
        return


def main() -> None:
    st.title("Smart Routing API Client")
    left_col, right_col = st.columns([1, 2.2])

    with left_col:
        mode_labels = {value: label for value, label in ROUTING_MODE_OPTIONS}
        routing_mode = st.selectbox(
            "Routing Mode",
            options=[value for value, _ in ROUTING_MODE_OPTIONS],
            format_func=lambda value: mode_labels.get(value, value),
            index=0,
        )
        if routing_mode != "na_general":
            st.caption("현재 서버 구현은 `na_general`만 동작합니다. 나머지 모드는 공용 인터페이스만 반영된 상태입니다.")
        source_type = st.radio("Input Source", ["BigQuery", "CSV Upload"], horizontal=True)
        server_url = st.text_input("Smart Routing Server URL", value="http://20.51.244.68:8055")
        st.session_state["smart_routing_server_url"] = server_url

        if source_type == "BigQuery":
            start_date = st.date_input("Start Date", value=date.today())
            end_date = st.date_input("End Date", value=date.today())
            uploaded_file = None
        else:
            uploaded_file = st.file_uploader("Upload Service CSV", type=["csv"])
            start_date = None
            end_date = None

        if st.button("Build Payload", width="stretch", type="primary"):
            if source_type == "CSV Upload" and uploaded_file is None:
                st.warning("Upload a CSV file first.")
            else:
                with st.spinner("Preparing routing payload..."):
                    if source_type == "BigQuery":
                        queried_service_df, rendered_sql = query_service_data(start_date, end_date, st.secrets)
                        input_label = f"BigQuery {start_date.isoformat()} to {end_date.isoformat()}"
                    else:
                        queried_service_df = _read_uploaded_service_csv(uploaded_file)
                        rendered_sql = ""
                        input_label = f"CSV upload: {uploaded_file.name}"
                    runtime = build_runtime_atlanta_inputs(queried_service_df)
                    planning_date = (
                        str(runtime.service_enriched_df["service_date_key"].dropna().astype(str).min())
                        if not runtime.service_enriched_df.empty and "service_date_key" in runtime.service_enriched_df.columns
                        else str(date.today())
                    )
                    payload = build_payload_from_service_frame(
                        runtime.service_enriched_df,
                        runtime.engineer_region_df,
                        runtime.home_geocode_df,
                        planning_date=planning_date,
                        request_id=f"ROUTE-{planning_date}",
                        mode=routing_mode,
                    )
                    st.session_state["vrp_payload"] = payload
                    st.session_state["vrp_runtime"] = {
                        "input_label": input_label,
                        "rendered_sql": rendered_sql,
                        "service_df": runtime.service_enriched_df,
                        "region_zip_df": runtime.region_zip_df,
                        "engineer_region_df": runtime.engineer_region_df,
                        "home_df": runtime.home_geocode_df,
                    }
                    st.session_state["vrp_job_id"] = ""
                    st.session_state["vrp_job_submit"] = None
                    st.session_state["vrp_job_status"] = None
                    st.session_state["vrp_job_result"] = None
                st.success("Payload prepared.")

        if st.button("Request Routing", width="stretch"):
            payload = st.session_state.get("vrp_payload")
            if not payload:
                st.warning("Build the payload first.")
            else:
                with st.spinner("Submitting Smart Routing job..."):
                    response = submit_routing_job(server_url, payload)
                    st.session_state["vrp_job_submit"] = response
                    st.session_state["vrp_job_id"] = response.get("job_id", "")
                    st.session_state["vrp_job_status"] = {
                        "job_id": response.get("job_id", ""),
                        "status": str(response.get("status", "queued")).strip().lower(),
                    }
                    st.session_state["vrp_job_result"] = None
                st.success(f"Submitted job {st.session_state.get('vrp_job_id', '')}")

        if st.button("Check Routing Result", width="stretch"):
            job_id = str(st.session_state.get("vrp_job_id", "")).strip()
            if not job_id:
                st.warning("Submit a job first.")
            else:
                with st.spinner("Fetching job status..."):
                    latest_status = get_routing_job_status(server_url, job_id)
                    st.session_state["vrp_job_status"] = latest_status
                    latest_state = str(latest_status.get("status", "")).strip().lower()
                    if latest_state == "completed":
                        st.session_state["vrp_job_result"] = get_routing_job_result(server_url, job_id)
                        st.success("Smart Routing completed. Displaying the latest result.")
                        st.rerun()
                    elif latest_state == "failed":
                        st.session_state["vrp_job_result"] = None
                st.success("Status updated.")

        payload = st.session_state.get("vrp_payload")
        if payload:
            st.caption(
                f"Prepared payload with {len(payload.get('technicians', []))} technicians and {len(payload.get('jobs', []))} jobs."
            )
            current_job_id = str(st.session_state.get("vrp_job_id", "")).strip()
            if current_job_id:
                st.caption(f"Current Job ID: {current_job_id}")
            view_options = ["Actual", "Smart Routing"]
            current_view = st.session_state.get("vrp_compare_mode", "Actual")
            default_index = view_options.index(current_view) if current_view in view_options else 0
            st.radio("Assignment View", view_options, index=default_index, horizontal=True, key="vrp_compare_mode")
            with st.expander("Payload Preview", expanded=False):
                st.json(payload)

        status_payload = st.session_state.get("vrp_job_status")
        progress_value, progress_text = _routing_status_progress(
            status_payload.get("status", "") if status_payload else ""
        )
        st.progress(progress_value)
        st.caption(progress_text)
        _auto_poll_routing_status()
        if status_payload:
            current_status = str(status_payload.get("status", "")).strip().lower()
            if current_status and current_status != "completed":
                st.caption(f"Smart Routing job status: {current_status}. Auto-checking every 5 seconds.")
            elif current_status == "completed":
                st.caption("Smart Routing job completed.")

    with right_col:
        runtime_state = st.session_state.get("vrp_runtime")
        if runtime_state is None:
            st.info("Build a payload first, then submit and refresh the Smart Routing job.")
            return

        current_job_id = str(st.session_state.get("vrp_job_id", "")).strip()
        latest_status_payload = st.session_state.get("vrp_job_status") or {}
        latest_status_value = str(latest_status_payload.get("status", "")).strip().lower()
        if current_job_id and (not latest_status_payload or latest_status_value in {"queued", "running"}):
            try:
                refreshed_status = get_routing_job_status(server_url, current_job_id)
                st.session_state["vrp_job_status"] = refreshed_status
                latest_status_payload = refreshed_status
                latest_status_value = str(refreshed_status.get("status", "")).strip().lower()
                if latest_status_value == "completed" and not st.session_state.get("vrp_job_result"):
                    st.session_state["vrp_job_result"] = get_routing_job_result(server_url, current_job_id)
            except Exception:
                pass

        if runtime_state.get("rendered_sql"):
            with st.expander("Executed SQL", expanded=False):
                st.code(runtime_state["rendered_sql"], language="sql")

        current_status_payload = st.session_state.get("vrp_job_status") or {}
        current_status = str(current_status_payload.get("status", "")).strip().lower()
        result_payload = st.session_state.get("vrp_job_result")
        if current_job_id and current_status == "completed" and not result_payload:
            try:
                st.session_state["vrp_job_result"] = get_routing_job_result(server_url, current_job_id)
                result_payload = st.session_state.get("vrp_job_result")
            except Exception:
                result_payload = None

        if not result_payload:
            preview_df = _build_pre_result_service_view(runtime_state["service_df"])
            compare_mode = st.session_state.get("vrp_compare_mode", "Actual")
            if compare_mode == "Smart Routing":
                st.info("Smart Routing result is not ready yet. The current map shows the loaded service points until the job completes.")
            else:
                st.info("The current map shows the loaded service points.")
            available_dates = sorted(preview_df["service_date_key"].dropna().astype(str).unique().tolist()) if "service_date_key" in preview_df.columns else []
            available_regions = ["ALL"] + sorted(preview_df["new_region_name"].dropna().astype(str).unique().tolist()) if "new_region_name" in preview_df.columns else ["ALL"]
            preview_engineer_options, preview_engineer_label_to_code = _build_engineer_options(preview_df)
            preview_col1, preview_col2, preview_col3 = st.columns(3)
            preview_date = preview_col1.selectbox("Date", options=available_dates, index=0 if available_dates else None, key="preview_date")
            preview_region = preview_col2.selectbox("Region", options=available_regions, index=0, key="preview_region")
            preview_engineer_label = preview_col3.selectbox("Engineer", options=preview_engineer_options, index=0, key="preview_engineer")
            preview_engineer_code = preview_engineer_label_to_code.get(preview_engineer_label, "ALL")
            if preview_date:
                preview_df = preview_df[preview_df["service_date_key"].astype(str) == str(preview_date)].copy()
            preview_home = runtime_state["home_df"].copy()
            if preview_region != "ALL":
                preview_df = preview_df[preview_df["new_region_name"].astype(str) == str(preview_region)].copy()
                preview_home = preview_home[preview_home["assigned_region_name"].astype(str) == str(preview_region)].copy()
            if preview_engineer_code != "ALL":
                preview_df = preview_df[preview_df["SVC_ENGINEER_CODE"].astype(str) == str(preview_engineer_code)].copy()
                preview_home = preview_home[preview_home["SVC_ENGINEER_CODE"].astype(str) == str(preview_engineer_code)].copy()
            preview_route_groups = _build_preview_route_groups(preview_df, preview_home)
            preview_map = build_map(preview_region, preview_df, preview_home, preview_route_groups, runtime_state["region_zip_df"])
            html(preview_map._repr_html_(), height=760, scrolling=False)
            preview_schedule_df = pd.DataFrame()
            for group in preview_route_groups:
                preview_schedule_df = pd.concat([preview_schedule_df, pd.DataFrame(group["scheduled_rows"])], ignore_index=True)
            preview_cols = [
                "service_date_key",
                "SVC_ENGINEER_NAME",
                "SVC_ENGINEER_CODE",
                "GSFS_RECEIPT_NO",
                "visit_seq",
                "visit_start_time",
                "visit_end_time",
                "SERVICE_PRODUCT_GROUP_CODE",
                "SERVICE_PRODUCT_CODE",
                "SVC_CENTER_TYPE",
                "new_region_name",
            ]
            preview_source_df = preview_schedule_df if not preview_schedule_df.empty else preview_df
            preview_cols = [col for col in preview_cols if col in preview_source_df.columns]
            if preview_cols:
                st.subheader("Loaded Service Points")
                st.dataframe(preview_source_df[preview_cols], width="stretch", hide_index=True)
                st.download_button(
                    "Download Loaded Service CSV",
                    data=_to_csv_bytes(preview_source_df),
                    file_name="loaded_service_points.csv",
                    mime="text/csv",
                    width="stretch",
                )
            return

        assignment_df, schedule_df = _build_result_frames(result_payload, runtime_state)
        if assignment_df.empty or schedule_df.empty:
            st.warning("The job completed but returned no routed assignments.")
            return

        compare_mode = st.session_state.get("vrp_compare_mode", "Actual")
        if compare_mode == "Actual":
            assignment_df, schedule_df = _build_actual_frames(runtime_state)

        if compare_mode == "Smart Routing":
            changed_count = int(pd.to_numeric(assignment_df.get("changed", False), errors="coerce").fillna(0).astype(bool).sum())
            total_count = int(len(assignment_df))
            st.caption(f"Changed assignments: {changed_count} / {total_count}")

        available_dates = sorted(schedule_df["service_date_key"].dropna().astype(str).unique().tolist())
        available_regions = ["ALL"] + sorted(schedule_df["new_region_name"].dropna().astype(str).unique().tolist())
        engineer_options, engineer_label_to_code = _build_engineer_options(assignment_df)

        filter_col1, filter_col2, filter_col3 = st.columns(3)
        selected_date = filter_col1.selectbox("Date", options=available_dates, index=0)
        selected_region = filter_col2.selectbox("Region", options=available_regions, index=0)
        selected_engineer_label = filter_col3.selectbox("Engineer", options=engineer_options, index=0)
        selected_engineer_code = engineer_label_to_code.get(selected_engineer_label, "ALL")

        filtered_assignment = assignment_df[assignment_df["service_date_key"].astype(str) == str(selected_date)].copy()
        filtered_schedule = schedule_df[schedule_df["service_date_key"].astype(str) == str(selected_date)].copy()
        filtered_home = runtime_state["home_df"].copy()
        if selected_region != "ALL":
            filtered_assignment = filtered_assignment[filtered_assignment["new_region_name"].astype(str) == str(selected_region)].copy()
            filtered_schedule = filtered_schedule[filtered_schedule["new_region_name"].astype(str) == str(selected_region)].copy()
            filtered_home = filtered_home[filtered_home["assigned_region_name"].astype(str) == str(selected_region)].copy()
        if selected_engineer_code != "ALL":
            filtered_assignment = filtered_assignment[filtered_assignment["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
            filtered_schedule = filtered_schedule[filtered_schedule["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
            filtered_home = filtered_home[filtered_home["SVC_ENGINEER_CODE"].astype(str) == str(selected_engineer_code)].copy()

        route_groups = _build_route_groups(filtered_schedule)
        service_count = int(filtered_assignment["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
        engineer_count = int(filtered_assignment["assigned_sm_code"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
        dms_engineer_count = 0
        dms2_engineer_count = 0
        if not filtered_assignment.empty and "assigned_center_type" in filtered_assignment.columns:
            center_types = filtered_assignment["assigned_center_type"].astype(str).str.upper()
            dms_engineer_count = int(filtered_assignment.loc[center_types == "DMS", "assigned_sm_code"].astype(str).nunique())
            dms2_engineer_count = int(filtered_assignment.loc[center_types == "DMS2", "assigned_sm_code"].astype(str).nunique())

        route_distance_series = pd.Series([float(group["route_payload"]["distance_km"]) for group in route_groups], dtype=float)
        route_duration_series = pd.Series([float(group["route_payload"]["duration_min"]) for group in route_groups], dtype=float)
        avg_distance = float(route_distance_series.mean()) if not route_distance_series.empty else 0.0
        avg_duration = float(route_duration_series.mean()) if not route_duration_series.empty else 0.0

        jobs_per_engineer = (
            filtered_assignment.groupby("assigned_sm_code", dropna=True)["GSFS_RECEIPT_NO"].nunique()
            if not filtered_assignment.empty
            else pd.Series(dtype=float)
        )
        jobs_std = float(jobs_per_engineer.std(ddof=0)) if not jobs_per_engineer.empty else 0.0

        staffing_df = _build_region_staffing_view(filtered_assignment)
        engineer_summary_rows: list[dict[str, object]] = []
        route_group_by_code = {str(group["engineer_code"]): group for group in route_groups}
        if not filtered_assignment.empty:
            for engineer_code, group in filtered_assignment.groupby("assigned_sm_code", dropna=True):
                route_group = route_group_by_code.get(str(engineer_code))
                engineer_summary_rows.append(
                    {
                        "Engineer": str(group["assigned_sm_name"].iloc[0]) if "assigned_sm_name" in group.columns and not group.empty else str(engineer_code),
                        "job_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                        "route_distance_km": round(float(route_group["route_payload"]["distance_km"]), 2) if route_group else 0.0,
                        "route_duration_min": round(float(route_group["route_payload"]["duration_min"]), 2) if route_group else 0.0,
                    }
                )
        engineer_summary_df = pd.DataFrame(engineer_summary_rows).sort_values(["job_count", "Engineer"], ascending=[False, True]) if engineer_summary_rows else pd.DataFrame()
        with left_col:
            metric_col1, metric_col2 = st.columns(2)
            metric_col1.metric("Service Count", service_count)
            metric_col2.metric("Assigned Engineer Count", f"{engineer_count} (DMS {dms_engineer_count}, DMS2 {dms2_engineer_count})")
            metric_col3, metric_col4 = st.columns(2)
            metric_col3.metric("Average Distance (km)", f"{avg_distance:.2f}")
            metric_col4.metric("Average Duration (min)", f"{avg_duration:.2f}")
            st.metric("Jobs per Engineer Std", f"{jobs_std:.2f}")
            if not staffing_df.empty:
                st.markdown("**Regional Staffing / Jobs**")
                st.dataframe(staffing_df, width="stretch", hide_index=True)
            if not engineer_summary_df.empty:
                st.markdown("**Engineer Summary**")
                st.dataframe(engineer_summary_df, width="stretch", hide_index=True)

        map_obj = build_map(selected_region, filtered_assignment, filtered_home, route_groups, runtime_state["region_zip_df"])
        html(map_obj._repr_html_(), height=760, scrolling=False)

        st.subheader("Selected Schedule")
        display_cols = [
            "service_date_key",
            "assigned_sm_name",
            "assigned_sm_code",
            "GSFS_RECEIPT_NO",
            "changed",
            "visit_seq",
            "visit_start_time",
            "visit_end_time",
            "SERVICE_PRODUCT_GROUP_CODE",
            "SERVICE_PRODUCT_CODE",
            "assigned_center_type",
            "new_region_name",
        ]
        display_cols = [col for col in display_cols if col in filtered_schedule.columns]
        st.dataframe(filtered_schedule[display_cols], width="stretch", hide_index=True)
        st.download_button(
            "Download Assignment CSV",
            data=_to_csv_bytes(filtered_schedule),
            file_name=f"{st.session_state.get('vrp_job_id', 'vrp_job')}_schedule.csv",
            mime="text/csv",
            width="stretch",
        )

        unassigned_df = pd.DataFrame(result_payload.get("unassigned", []))
        if not unassigned_df.empty:
            st.subheader("Unassigned")
            st.dataframe(unassigned_df, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
