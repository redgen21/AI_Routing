from __future__ import annotations

import colorsys
import json
import time
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
from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm_from_frames


st.set_page_config(page_title="Atlanta Live Routing", layout="wide")

CONFIG_FILE = Path("config.json")
PROFILE_FILE = Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx")

LIVE_STAGE_LABELS = [
    "Querying BigQuery",
    "Merging geocode cache",
    "Preparing Atlanta runtime inputs",
    "Running OSRM iteration assignment",
    "Finalizing schedules and map data",
]


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
            cache_file=Path("data/cache/osrm_trip_cache_atlanta_live_map.csv"),
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


def _build_region_layers(region_zip_df: pd.DataFrame, service_df: pd.DataFrame):
    city_data = load_city_map_data("Atlanta, GA")
    coverage_df = pd.read_excel(PROFILE_FILE, sheet_name="1. Zip Coverage", dtype={"POSTAL_CODE": str})
    coverage_df = coverage_df[coverage_df["STRATEGIC_CITY_NAME"].astype(str).eq("Atlanta, GA")].copy()
    coverage_df["POSTAL_CODE"] = coverage_df["POSTAL_CODE"].astype(str).str.zfill(5)
    coverage_df = coverage_df[["POSTAL_CODE"]].drop_duplicates().copy()

    zip_layer = city_data.zip_layer.copy()
    zip_layer["POSTAL_CODE"] = zip_layer["POSTAL_CODE"].astype(str).str.zfill(5)
    coverage_layer = zip_layer.merge(coverage_df, on="POSTAL_CODE", how="inner")

    merged = coverage_layer.merge(region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]], on="POSTAL_CODE", how="left")
    merged["service_count"] = merged["POSTAL_CODE"].map(
        service_df["POSTAL_CODE"].astype(str).str.zfill(5).value_counts()
    ).fillna(0).astype(int)
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
                "route_payload": route_payload,
                "scheduled_rows": group.to_dict("records"),
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
                "home_coord": start_coord,
            }
        )
    return route_groups


def _dedupe_schedule_receipts(schedule_df: pd.DataFrame) -> pd.DataFrame:
    if schedule_df.empty:
        return schedule_df.copy()
    deduped = schedule_df.copy()
    sort_cols = [col for col in ["service_date_key", "assigned_sm_code", "visit_seq", "GSFS_RECEIPT_NO"] if col in deduped.columns]
    if sort_cols:
        deduped = deduped.sort_values(sort_cols).reset_index(drop=True)
    receipt_keys = [col for col in ["service_date_key", "assigned_sm_code", "GSFS_RECEIPT_NO"] if col in deduped.columns]
    if receipt_keys:
        deduped = deduped.drop_duplicates(subset=receipt_keys, keep="first").reset_index(drop=True)
    return deduped


def _build_region_staffing_view(service_df: pd.DataFrame) -> pd.DataFrame:
    if service_df.empty:
        return pd.DataFrame(columns=["region", "dms_count", "dms_service_count", "service_count"])
    staffing_df = service_df[["new_region_name", "assigned_sm_code", "assigned_center_type", "GSFS_RECEIPT_NO"]].dropna(subset=["new_region_name", "assigned_sm_code"]).copy()
    staffing_df["assigned_center_type"] = staffing_df["assigned_center_type"].astype(str).str.upper()
    rows: list[dict[str, object]] = []
    for region_name, group in staffing_df.groupby("new_region_name", dropna=False):
        dms_count = int(group.loc[group["assigned_center_type"] == "DMS", "assigned_sm_code"].astype(str).nunique())
        dms_service_count = int(group.loc[group["assigned_center_type"] == "DMS", "GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
        rows.append(
            {
                "region": str(region_name),
                "dms_count": dms_count,
                "dms_service_count": dms_service_count,
                "service_count": int(group["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()),
            }
        )
    return pd.DataFrame(rows).sort_values("region").reset_index(drop=True)


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
                            "padding:2px 6px;text-align:center;white-space:nowrap;\">Home</div>"
                        )
                    ),
                    popup=_popup(f"<b>Home Start</b>: {group['engineer_code']}", width=260),
                ).add_to(route_layer)
            for row in group["scheduled_rows"]:
                seq = int(row.get("visit_seq", 0))
                home_region_name = str(row.get("assigned_region_name", "")).strip()
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.DivIcon(
                        html=(
                            f"<div style=\"font-size:11px;font-weight:700;color:{engineer_color};"
                            f"background:#ffffff;border:2px solid {engineer_color};border-radius:12px;"
                            "width:22px;height:22px;line-height:18px;text-align:center;\">"
                            f"{seq}</div>"
                        )
                    ),
                    popup=_popup(
                        f"<b>Engineer</b>: {row.get('assigned_sm_code', '')} | "
                        f"<b>Receipt</b>: {row.get('GSFS_RECEIPT_NO', '')} | "
                        f"<b>Seq</b>: {seq}<br>"
                        f"<b>Home Region</b>: {home_region_name}<br>"
                        f"<b>Product Group</b>: {row.get('SERVICE_PRODUCT_GROUP_CODE', '')}"
                        + (
                            f" | <b>REF Heavy</b>: {'Y' if bool(row.get('is_heavy_repair')) else 'N'}"
                            if str(row.get('SERVICE_PRODUCT_GROUP_CODE', '')).strip().upper() == 'REF'
                            else ""
                        )
                        + "<br>"
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
                    f"<b>Product Group</b>: {row.get('SERVICE_PRODUCT_GROUP_CODE', '')} | "
                    f"<b>Heavy Repair</b>: {'Y' if bool(row.get('is_heavy_repair')) else 'N'} | "
                    f"<b>Service Time</b>: {int(float(row.get('service_time_min', 45)))} min",
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
                f"<b>Engineer</b>: {row.get('SVC_ENGINEER_CODE', '')} | "
                f"<b>Name</b>: {row.get('Name', '')}<br>"
                f"<b>Assigned Region</b>: {row.get('assigned_region_name', '')}<br>"
                f"<b>REF Heavy Repair</b>: {row.get('REF_HEAVY_REPAIR_FLAG', '')}",
                width=440,
            ),
        ).add_to(home_group)

    folium.LayerControl(collapsed=False).add_to(fmap)
    return fmap


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _render_stage_status(stage_box, progress_bar, current_step: int, lines: list[str], state: str = "running") -> None:
    progress_bar.progress(min(max(current_step / len(LIVE_STAGE_LABELS), 0.0), 1.0))
    body = "\n".join(f"- {line}" for line in lines)
    stage_box.markdown(f"**Status:** {state.capitalize()}\n\n{body}")


def main():
    st.title("Atlanta Live Routing")
    st.caption("BigQuery query -> geocode/cache merge -> Atlanta preprocessing -> OSRM iteration assignment")

    sidebar_left, sidebar_right = st.columns([1, 2.3])
    with sidebar_left:
        start_date = st.date_input("Start Date", value=date(2026, 1, 1))
        end_date = st.date_input("End Date", value=date(2026, 1, 31))
        progress_placeholder = st.empty()
        progress_bar_placeholder = st.empty()
        run_clicked = st.button("Load And Route", type="primary", use_container_width=True)

        if run_clicked:
            if start_date > end_date:
                st.error("Start Date must be on or before End Date.")
            else:
                stage_box = progress_placeholder.container()
                progress_bar = progress_bar_placeholder.progress(0.0)
                timeline: list[str] = []
                run_start = time.perf_counter()
                _render_stage_status(
                    stage_box,
                    progress_bar,
                    0,
                    ["Preparing runtime request."],
                    state="running",
                )
                try:
                    step_start = time.perf_counter()
                    _render_stage_status(
                        stage_box,
                        progress_bar,
                        0,
                        timeline + [f"{LIVE_STAGE_LABELS[0]}..."],
                        state="running",
                    )
                    queried_service_df, rendered_sql = query_service_data(start_date, end_date, st.secrets)
                    timeline.append(
                        f"{LIVE_STAGE_LABELS[0]} completed in {time.perf_counter() - step_start:.1f}s "
                        f"({len(queried_service_df):,} rows)."
                    )

                    step_start = time.perf_counter()
                    _render_stage_status(
                        stage_box,
                        progress_bar,
                        1,
                        timeline + [f"{LIVE_STAGE_LABELS[1]} and {LIVE_STAGE_LABELS[2]}..."],
                        state="running",
                    )
                    runtime = build_runtime_atlanta_inputs(queried_service_df)
                    prep_elapsed = time.perf_counter() - step_start
                    timeline.append(
                        f"{LIVE_STAGE_LABELS[1]} and {LIVE_STAGE_LABELS[2]} completed in {prep_elapsed:.1f}s "
                        f"({len(runtime.service_enriched_df):,} Atlanta service rows)."
                    )

                    step_start = time.perf_counter()
                    _render_stage_status(
                        stage_box,
                        progress_bar,
                        3,
                        timeline + [f"{LIVE_STAGE_LABELS[3]}..."],
                        state="running",
                    )
                    assignment_df, summary_df, schedule_df = build_atlanta_production_assignment_osrm_from_frames(
                        engineer_region_df=runtime.engineer_region_df,
                        home_df=runtime.home_geocode_df,
                        service_df=runtime.service_enriched_df,
                        attendance_limited=True,
                        assignment_strategy="iteration",
                    )
                    timeline.append(
                        f"{LIVE_STAGE_LABELS[3]} completed in {time.perf_counter() - step_start:.1f}s "
                        f"({len(assignment_df):,} assigned rows, {len(summary_df):,} engineer summaries)."
                    )

                    step_start = time.perf_counter()
                    _render_stage_status(
                        stage_box,
                        progress_bar,
                        4,
                        timeline + [f"{LIVE_STAGE_LABELS[4]}..."],
                        state="running",
                    )
                    st.session_state["live_runtime"] = {
                        "queried_service_df": queried_service_df,
                        "rendered_sql": rendered_sql,
                        "geocoded_service_df": runtime.geocoded_service_df,
                        "region_zip_df": runtime.region_zip_df,
                        "engineer_region_df": runtime.engineer_region_df,
                        "home_df": runtime.home_geocode_df,
                        "service_df": runtime.service_enriched_df,
                        "assignment_df": assignment_df,
                        "summary_df": summary_df,
                        "schedule_df": schedule_df,
                    }
                    timeline.append(
                        f"{LIVE_STAGE_LABELS[4]} completed in {time.perf_counter() - step_start:.1f}s "
                        f"({len(schedule_df):,} schedule rows)."
                    )
                    total_elapsed = time.perf_counter() - run_start
                    _render_stage_status(
                        stage_box,
                        progress_bar,
                        len(LIVE_STAGE_LABELS),
                        timeline + [f"Total runtime: {total_elapsed:.1f}s"],
                        state="complete",
                    )
                except Exception as exc:
                    total_elapsed = time.perf_counter() - run_start
                    _render_stage_status(
                        stage_box,
                        progress_bar,
                        max(len(timeline), 1),
                        timeline + [f"Failed after {total_elapsed:.1f}s: {exc}"],
                        state="error",
                    )
                    raise

    state = st.session_state.get("live_runtime")
    if not state:
        st.info("Select a start date and end date, then click `Load And Route` to query BigQuery and build OSRM iteration routes.")
        return

    assignment_df = state["assignment_df"].copy()
    summary_df = state["summary_df"].copy()
    schedule_df = state["schedule_df"].copy()
    service_df = state["service_df"].copy()
    home_df = state["home_df"].copy()
    region_zip_df = state["region_zip_df"].copy()

    date_options = ["ALL"] + sorted(assignment_df["service_date_key"].dropna().astype(str).unique().tolist()) if not assignment_df.empty else ["ALL"]
    region_options = ["ALL"] + sorted(region_zip_df["new_region_name"].dropna().astype(str).unique().tolist())
    engineer_labels = (
        assignment_df[["assigned_sm_code", "assigned_sm_name"]]
        .drop_duplicates()
        .astype(str)
        .assign(engineer_label=lambda df: df["assigned_sm_code"] + " | " + df["assigned_sm_name"])
    ) if not assignment_df.empty else pd.DataFrame(columns=["engineer_label"])
    engineer_options = ["ALL"] + sorted(engineer_labels["engineer_label"].tolist())

    left, right = st.columns([1, 2.25])
    with left:
        selected_date = st.selectbox("Date", date_options, index=0)
        selected_region = st.selectbox("Production Region", region_options, index=0)
        selected_engineer = st.selectbox("Engineer", engineer_options, index=0)

        filtered_assignment = assignment_df.copy()
        filtered_summary = summary_df.copy()
        filtered_schedule = schedule_df.copy()
        filtered_home = home_df.copy()

        if selected_date != "ALL":
            filtered_assignment = filtered_assignment[filtered_assignment["service_date_key"] == selected_date].copy()
            filtered_summary = filtered_summary[filtered_summary["service_date_key"] == selected_date].copy()
            filtered_schedule = filtered_schedule[filtered_schedule["service_date_key"] == selected_date].copy()
        if selected_region != "ALL":
            filtered_assignment = filtered_assignment[filtered_assignment["new_region_name"] == selected_region].copy()
            filtered_schedule = filtered_schedule[filtered_schedule["new_region_name"] == selected_region].copy()
            if "assigned_region_seq" in filtered_summary.columns:
                region_seq = int(selected_region.split()[-1])
                filtered_summary = filtered_summary[pd.to_numeric(filtered_summary["assigned_region_seq"], errors="coerce") == region_seq].copy()
            filtered_home = filtered_home[filtered_home["assigned_region_name"] == selected_region].copy()
        if selected_engineer != "ALL":
            engineer_code = selected_engineer.split("|", 1)[0].strip()
            filtered_assignment = filtered_assignment[filtered_assignment["assigned_sm_code"].astype(str) == engineer_code].copy()
            filtered_summary = filtered_summary[filtered_summary["SVC_ENGINEER_CODE"].astype(str) == engineer_code].copy()
            filtered_schedule = filtered_schedule[filtered_schedule["assigned_sm_code"].astype(str) == engineer_code].copy()
            filtered_home = filtered_home[filtered_home["SVC_ENGINEER_CODE"].astype(str) == engineer_code].copy()

        filtered_schedule = _dedupe_schedule_receipts(filtered_schedule)
        route_groups = _build_route_groups(filtered_schedule) if selected_date != "ALL" else []

        service_count = int(filtered_assignment["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
        engineer_count = int(filtered_summary["SVC_ENGINEER_CODE"].astype(str).nunique()) if not filtered_summary.empty else 0
        avg_distance = float(pd.to_numeric(filtered_summary.get("route_distance_km"), errors="coerce").dropna().mean()) if not filtered_summary.empty else 0.0
        avg_duration = float(pd.to_numeric(filtered_summary.get("route_duration_min"), errors="coerce").dropna().mean()) if not filtered_summary.empty else 0.0
        jobs_std = float(pd.to_numeric(filtered_summary.get("job_count"), errors="coerce").fillna(0).std(ddof=0)) if not filtered_summary.empty else 0.0

        st.metric("Service Count", service_count)
        st.metric("Assigned Engineer Count", f"{engineer_count} (DMS {engineer_count})")
        st.metric("Average Distance (km)", f"{avg_distance:.2f}")
        st.metric("Average Duration (min)", f"{avg_duration:.2f}")
        st.metric("Jobs per Engineer Std", f"{jobs_std:.2f}")

        region_staffing_view = _build_region_staffing_view(filtered_assignment)
        if not region_staffing_view.empty:
            st.markdown("**Regional Staffing / Jobs**")
            st.dataframe(region_staffing_view, width="stretch", hide_index=True)

        st.markdown("**Engineer Summary**")
        if not filtered_summary.empty:
            st.dataframe(
                filtered_summary[
                    [
                        "SVC_ENGINEER_CODE",
                        "SVC_ENGINEER_NAME",
                        "job_count",
                        "service_time_min",
                        "travel_time_min",
                        "route_distance_km",
                        "route_duration_min",
                        "total_work_min",
                        "overflow_480",
                    ]
                ].sort_values(["job_count", "SVC_ENGINEER_CODE"], ascending=[False, True]),
                width="stretch",
                hide_index=True,
            )

        st.markdown("**Downloads**")
        st.download_button("Download Queried Service CSV", data=_to_csv_bytes(state["queried_service_df"]), file_name="queried_service.csv", mime="text/csv", use_container_width=True)
        st.download_button("Download Geocoded Service CSV", data=_to_csv_bytes(state["geocoded_service_df"]), file_name="queried_service_geocoded.csv", mime="text/csv", use_container_width=True)
        st.download_button("Download Assignment CSV", data=_to_csv_bytes(state["assignment_df"]), file_name="atlanta_live_assignment_osrm_iteration.csv", mime="text/csv", use_container_width=True)
        st.download_button("Download Schedule CSV", data=_to_csv_bytes(state["schedule_df"]), file_name="atlanta_live_schedule_osrm_iteration.csv", mime="text/csv", use_container_width=True)

    with right:
        map_obj = build_map(selected_region, filtered_assignment, filtered_home, route_groups, region_zip_df)
        html(map_obj._repr_html_(), height=860)
        if not filtered_schedule.empty:
            st.markdown("**Selected Schedule**")
            st.dataframe(
                filtered_schedule[
                    [
                        "service_date_key",
                        "assigned_sm_code",
                        "assigned_sm_name",
                        "GSFS_RECEIPT_NO",
                        "visit_seq",
                        "visit_start_time",
                        "visit_end_time",
                        "travel_time_from_prev_min",
                        "service_time_min",
                        "new_region_name",
                    ]
                ].sort_values(["assigned_sm_code", "visit_seq"]),
                width="stretch",
                hide_index=True,
            )

        with st.expander("Executed SQL", expanded=False):
            st.code(state["rendered_sql"], language="sql")


if __name__ == "__main__":
    main()
