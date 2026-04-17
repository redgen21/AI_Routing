from __future__ import annotations

import colorsys
import json
import uuid
from pathlib import Path
from urllib.error import HTTPError
from urllib import parse, request as urllib_request

import folium
import pandas as pd
import streamlit as st
from folium.plugins import MarkerCluster

from smart_routing.area_map import load_city_map_data
from smart_routing.live_atlanta_runtime import _load_config as _load_runtime_config
from smart_routing.live_atlanta_runtime import _merge_service_geocodes
from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient


st.set_page_config(page_title="Common VRP Client", layout="wide")

CONFIG_COMMON_PATH = Path("config_common_vrp.json")


DEFAULT_COMMON_SERVER_URL = "http://20.51.244.68:8065"
MASTER_PATH = Path("data/All_In_One_Master.xlsx")
COMMON_JOB_STORE_PATH = Path("data/common_vrp_job_input.parquet")
COMMON_TECHNICIAN_STORE_PATH = Path("data/common_vrp_technician_input.parquet")
DEFAULT_SUBSIDIARY_NAME = "LGEAI"
DEFAULT_STRATEGIC_CITY_NAME = "Atlanta, GA"

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
    return bool(text)


def _coerce_bool_series(series: pd.Series) -> pd.Series:
    return series.map(_coerce_bool_value).astype(bool)


# ---------------------------------------------------------------------------
# Map / UI helpers (independent copy, no dependency on sr_vrp_api_client)
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def _load_common_client_config(config_path: str = str(CONFIG_COMMON_PATH)) -> dict:
    path = Path(config_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _resolve_city_osrm_url(strategic_city_name: str) -> str:
    cfg = _load_common_client_config()
    routing_seed = cfg.get("routing_seed", {}) if isinstance(cfg.get("routing_seed", {}), dict) else {}
    city_urls = routing_seed.get("city_osrm_urls", {}) if isinstance(routing_seed.get("city_osrm_urls", {}), dict) else {}
    city_url = str(city_urls.get(str(strategic_city_name).strip(), "")).strip()
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


def _region_color_map() -> dict[str, str]:
    return {
        "Atlanta New Region 1": "#db4437",
        "Atlanta New Region 2": "#0f9d58",
        "Atlanta New Region 3": "#4285f4",
    }


def _build_region_layers(strategic_city_name: str, region_zip_df: pd.DataFrame, service_df: pd.DataFrame):
    city_data = load_city_map_data(strategic_city_name)
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


def _render_folium_map(map_obj: folium.Map, height: int = 760) -> None:
    st.iframe(map_obj.get_root().render(), height=height)


def _build_route_groups(schedule_df: pd.DataFrame, strategic_city_name: str):
    route_groups: list[dict] = []
    if schedule_df.empty:
        return route_groups
    for engineer_code, group in schedule_df.groupby("assigned_sm_code", dropna=True):
        group = group.sort_values("visit_seq").reset_index(drop=True)
        start_coord = None
        return_to_home = _coerce_bool_value(group.iloc[0].get("return_to_home", False))
        if pd.notna(group.iloc[0].get("home_start_longitude")) and pd.notna(group.iloc[0].get("home_start_latitude")):
            start_coord = (float(group.iloc[0]["home_start_longitude"]), float(group.iloc[0]["home_start_latitude"]))
        stop_coords = [(float(row["longitude"]), float(row["latitude"])) for _, row in group.iterrows()]
        coord_chain = [start_coord] + stop_coords if start_coord is not None else stop_coords
        if return_to_home and start_coord is not None and stop_coords:
            coord_chain = coord_chain + [start_coord]
        route_payload = get_route_client(strategic_city_name).build_ordered_route(tuple(coord_chain), preserve_first=start_coord is not None)
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


def build_map(
    strategic_city_name: str,
    region_name: str,
    display_service_df: pd.DataFrame,
    home_df: pd.DataFrame,
    route_groups: list[dict],
    region_zip_df: pd.DataFrame,
):
    zip_layer, region_layer = _build_region_layers(strategic_city_name, region_zip_df, display_service_df)
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
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _write_local_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _load_local_jobs(subsidiary_name: str, strategic_city_name: str) -> pd.DataFrame:
    df = _read_local_parquet(COMMON_JOB_STORE_PATH)
    if df.empty:
        return df
    required = {"subsidiary_name", "strategic_city_name"}
    if not required.issubset(df.columns):
        return pd.DataFrame()
    filtered = df[
        (df["subsidiary_name"].astype(str) == str(subsidiary_name))
        & (df["strategic_city_name"].astype(str) == str(strategic_city_name))
    ].copy()
    if filtered.empty:
        return filtered
    if "fixed" not in filtered.columns:
        filtered["fixed"] = False
    filtered["fixed"] = _coerce_bool_series(filtered["fixed"])
    return filtered


def _save_local_jobs(subsidiary_name: str, strategic_city_name: str, new_rows_df: pd.DataFrame) -> None:
    working_new_rows_df = new_rows_df.copy()
    if "fixed" not in working_new_rows_df.columns:
        working_new_rows_df["fixed"] = False
    working_new_rows_df["fixed"] = _coerce_bool_series(working_new_rows_df["fixed"])
    all_df = _read_local_parquet(COMMON_JOB_STORE_PATH)
    if all_df.empty:
        base_df = pd.DataFrame(columns=working_new_rows_df.columns)
    else:
        if "fixed" not in all_df.columns:
            all_df["fixed"] = False
        base_df = all_df[
            ~(
                (all_df["subsidiary_name"].astype(str) == str(subsidiary_name))
                & (all_df["strategic_city_name"].astype(str) == str(strategic_city_name))
                & (all_df["gsfs_receipt_no"].astype(str).isin(working_new_rows_df["gsfs_receipt_no"].astype(str)))
            )
        ].copy()
    merged_df = pd.concat([base_df, working_new_rows_df], ignore_index=True)
    merged_df["fixed"] = _coerce_bool_series(merged_df["fixed"])
    _write_local_parquet(COMMON_JOB_STORE_PATH, merged_df)


def _load_local_technicians(subsidiary_name: str, strategic_city_name: str, promise_date: str) -> pd.DataFrame:
    df = _read_local_parquet(COMMON_TECHNICIAN_STORE_PATH)
    if df.empty:
        return df
    required = {"subsidiary_name", "strategic_city_name", "promise_date"}
    if not required.issubset(df.columns):
        return pd.DataFrame()
    filtered = df[
        (df["subsidiary_name"].astype(str) == str(subsidiary_name))
        & (df["strategic_city_name"].astype(str) == str(strategic_city_name))
        & (df["promise_date"].astype(str) == str(promise_date))
    ].copy()
    if filtered.empty:
        return filtered
    if "available" in filtered.columns:
        filtered["available"] = (
            filtered["available"]
            .map(lambda v: str(v).strip().lower() if pd.notna(v) else "")
            .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
            .fillna(filtered["available"])
            .astype(bool)
        )
    return filtered


def _save_local_technicians(subsidiary_name: str, strategic_city_name: str, promise_date: str, rows_df: pd.DataFrame) -> None:
    working_df = rows_df.copy()
    if "available" in working_df.columns:
        working_df["available"] = working_df["available"].astype(bool)
    if "promise_date" in working_df.columns:
        working_df["promise_date"] = working_df["promise_date"].astype(str)
    all_df = _read_local_parquet(COMMON_TECHNICIAN_STORE_PATH)
    if all_df.empty:
        base_df = pd.DataFrame(columns=working_df.columns)
    else:
        base_df = all_df[
            ~(
                (all_df["subsidiary_name"].astype(str) == str(subsidiary_name))
                & (all_df["strategic_city_name"].astype(str) == str(strategic_city_name))
                & (all_df["promise_date"].astype(str) == str(promise_date))
            )
        ].copy()
    merged_df = pd.concat([base_df, working_df], ignore_index=True)
    _write_local_parquet(COMMON_TECHNICIAN_STORE_PATH, merged_df)


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
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    return digits if len(digits) == 8 else ""


def _read_uploaded_service_csv(uploaded_file) -> pd.DataFrame:
    uploaded_file.seek(0)
    return pd.read_csv(
        uploaded_file,
        encoding="utf-8-sig",
        keep_default_na=False,
        sep=None,
        engine="python",
    )


def _prepare_jobs_df(
    raw_df: pd.DataFrame,
    subsidiary_name: str,
    strategic_city_name: str,
    existing_df: pd.DataFrame,
    *,
    allow_existing_receipt: str = "",
) -> tuple[pd.DataFrame, list[str]]:
    working = raw_df.copy()
    missing = [col for col in JOB_REQUIRED_COLUMNS if col not in working.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    working = working[JOB_REQUIRED_COLUMNS].copy()
    fixed_series = _coerce_bool_series(
        raw_df["fixed"] if "fixed" in raw_df.columns else pd.Series(False, index=raw_df.index)
    )
    for col in JOB_REQUIRED_COLUMNS:
        working[col] = working[col].astype(str).str.strip().replace(
            {"nan": "", "None": "", "none": "", "NaN": "", "NAN": "", "NaT": "", "nat": ""}
        )
    working["PROMISE_DATE"] = working["PROMISE_DATE"].map(_normalize_promise_date)
    if working["PROMISE_DATE"].eq("").any():
        raise ValueError("PROMISE_DATE must be YYYYMMDD.")
    working["POSTAL_CODE"] = working["POSTAL_CODE"].str.replace(r"\.0+$", "", regex=True).str.zfill(5)
    dup_mask = working["GSFS_RECEIPT_NO"].duplicated(keep=False)
    if dup_mask.any():
        dup_vals = sorted(working.loc[dup_mask, "GSFS_RECEIPT_NO"].astype(str).unique().tolist())
        raise ValueError(f"Duplicate GSFS_RECEIPT_NO in upload: {', '.join(dup_vals)}")
    existing_receipts = existing_df["gsfs_receipt_no"].astype(str).tolist() if not existing_df.empty and "gsfs_receipt_no" in existing_df.columns else []
    if allow_existing_receipt:
        existing_receipts = [value for value in existing_receipts if value != str(allow_existing_receipt)]
    duplicate_mask = working["GSFS_RECEIPT_NO"].astype(str).isin(existing_receipts)
    duplicate_receipts = sorted(working.loc[duplicate_mask, "GSFS_RECEIPT_NO"].astype(str).unique().tolist())
    working = working.loc[~duplicate_mask].copy()
    if working.empty:
        return pd.DataFrame(), duplicate_receipts
    working["SUBSIDIARY_NAME"] = subsidiary_name
    working["STRATEGIC_CITY_NAME"] = strategic_city_name
    working["fixed"] = fixed_series.loc[working.index].astype(bool)
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


def _build_common_result_frames(
    result_payload: dict,
    jobs_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_zip_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    assignments_df = pd.DataFrame(result_payload.get("assignments", []))
    if assignments_df.empty or jobs_df.empty:
        return pd.DataFrame(), pd.DataFrame()

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
    if not region_zip_df.empty:
        region_lookup = region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates().copy()
        region_lookup["POSTAL_CODE"] = region_lookup["POSTAL_CODE"].astype(str).str.zfill(5)
        merged["POSTAL_CODE"] = merged["POSTAL_CODE"].astype(str).str.zfill(5)
        merged = merged.merge(region_lookup, on="POSTAL_CODE", how="left")
    else:
        merged["region_seq"] = pd.NA
        merged["new_region_name"] = pd.NA
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
    merged["return_to_home"] = bool((st.session_state.get("common_vrp_payload") or {}).get("options", {}).get("return_to_home", False))
    merged["assigned_region_name"] = pd.NA
    merged["travel_time_from_prev_min"] = pd.NA
    schedule_df = merged.sort_values(["assigned_sm_code", "visit_seq", "GSFS_RECEIPT_NO"]).reset_index(drop=True)
    assignment_df = schedule_df.copy()
    return assignment_df, schedule_df


def _build_common_actual_frames(
    jobs_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_zip_df: pd.DataFrame,
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
    if not region_zip_df.empty:
        region_lookup = region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates().copy()
        region_lookup["POSTAL_CODE"] = region_lookup["POSTAL_CODE"].astype(str).str.zfill(5)
        actual_df["POSTAL_CODE"] = actual_df["POSTAL_CODE"].astype(str).str.zfill(5)
        actual_df = actual_df.merge(region_lookup, on="POSTAL_CODE", how="left")
    else:
        actual_df["region_seq"] = pd.NA
        actual_df["new_region_name"] = pd.NA
    actual_df["assigned_sm_name"] = actual_df["assigned_sm_name"].replace("", pd.NA).fillna(actual_df["assigned_sm_name_master"])
    actual_df["visit_seq"] = actual_df.groupby(["service_date_key", "assigned_sm_code"], dropna=False).cumcount() + 1
    actual_df["visit_start_time"] = ""
    actual_df["visit_end_time"] = ""
    actual_df["travel_time_from_prev_min"] = pd.NA
    actual_df["assigned_region_name"] = pd.NA
    actual_df["return_to_home"] = bool((st.session_state.get("common_vrp_payload") or {}).get("options", {}).get("return_to_home", False))
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
        return pd.DataFrame(columns=["POSTAL_CODE", "region_seq", "new_region_name"])
    return region_df.rename(columns={"postal_code": "POSTAL_CODE", "region_name": "new_region_name"})[
        ["POSTAL_CODE", "region_seq", "new_region_name"]
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

    compare_mode = st.session_state.get("common_vrp_compare_mode", "Actual")
    region_zip_df = _build_common_region_zip_df(subsidiary_name, strategic_city_name)
    assignment_df, schedule_df = _build_common_result_frames(result_payload, jobs_df, engineer_master_df, region_zip_df)
    if compare_mode == "Actual":
        assignment_df, schedule_df = _build_common_actual_frames(jobs_df, engineer_master_df, region_zip_df)
    home_df = _build_common_home_df(engineer_master_df)

    available_dates = sorted(schedule_df["service_date_key"].dropna().astype(str).unique().tolist()) if not schedule_df.empty else []
    available_regions = ["ALL"] + sorted(schedule_df["new_region_name"].dropna().astype(str).unique().tolist()) if "new_region_name" in schedule_df.columns else ["ALL"]
    engineer_options, engineer_label_to_code = _build_engineer_options(assignment_df)
    filtered_assignment = assignment_df.copy()
    filtered_schedule = schedule_df.copy()
    filtered_home = home_df.copy()
    selected_date = st.session_state.get("common_result_date") if st.session_state.get("common_result_date") in available_dates else (available_dates[0] if available_dates else None)
    selected_region = st.session_state.get("common_result_region") if st.session_state.get("common_result_region") in available_regions else "ALL"
    selected_engineer_label = st.session_state.get("common_result_engineer") if st.session_state.get("common_result_engineer") in engineer_options else "ALL"
    selected_engineer_code = engineer_label_to_code.get(selected_engineer_label, "ALL")

    if selected_date:
        filtered_assignment = filtered_assignment[filtered_assignment["service_date_key"].astype(str) == str(selected_date)].copy()
        filtered_schedule = filtered_schedule[filtered_schedule["service_date_key"].astype(str) == str(selected_date)].copy()
    if selected_region != "ALL" and "new_region_name" in filtered_schedule.columns:
        filtered_assignment = filtered_assignment[filtered_assignment["new_region_name"].astype(str) == str(selected_region)].copy()
        filtered_schedule = filtered_schedule[filtered_schedule["new_region_name"].astype(str) == str(selected_region)].copy()
    if selected_engineer_code != "ALL":
        filtered_assignment = filtered_assignment[filtered_assignment["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
        filtered_schedule = filtered_schedule[filtered_schedule["assigned_sm_code"].astype(str) == str(selected_engineer_code)].copy()
        filtered_home = filtered_home[filtered_home["SVC_ENGINEER_CODE"].astype(str) == str(selected_engineer_code)].copy()

    route_groups = _build_route_groups(filtered_schedule, strategic_city_name)
    service_count = int(filtered_assignment["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
    engineer_count = int(filtered_assignment["assigned_sm_code"].dropna().astype(str).nunique()) if not filtered_assignment.empty else 0
    dms_engineer_count = int(filtered_assignment.loc[filtered_assignment["assigned_center_type"].astype(str).str.upper() == "DMS", "assigned_sm_code"].astype(str).nunique()) if not filtered_assignment.empty and "assigned_center_type" in filtered_assignment.columns else 0
    dms2_engineer_count = int(filtered_assignment.loc[filtered_assignment["assigned_center_type"].astype(str).str.upper() == "DMS2", "assigned_sm_code"].astype(str).nunique()) if not filtered_assignment.empty and "assigned_center_type" in filtered_assignment.columns else 0
    route_distance_series = pd.Series([float(group["route_payload"]["distance_km"]) for group in route_groups], dtype=float)
    route_duration_series = pd.Series([float(group["route_payload"]["duration_min"]) for group in route_groups], dtype=float)
    avg_distance = float(route_distance_series.mean()) if not route_distance_series.empty else 0.0
    avg_duration = float(route_duration_series.mean()) if not route_duration_series.empty else 0.0
    jobs_per_engineer = filtered_assignment.groupby("assigned_sm_code", dropna=True)["GSFS_RECEIPT_NO"].nunique() if not filtered_assignment.empty else pd.Series(dtype=float)
    jobs_std = float(jobs_per_engineer.std(ddof=0)) if not jobs_per_engineer.empty else 0.0
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
    staffing_df = _build_region_staffing_view(filtered_assignment)
    return {
        "payload": payload,
        "status_payload": status_payload,
        "result_payload": result_payload,
        "available_dates": available_dates,
        "available_regions": available_regions,
        "engineer_options": engineer_options,
        "service_count": service_count,
        "engineer_count": engineer_count,
        "dms_engineer_count": dms_engineer_count,
        "dms2_engineer_count": dms2_engineer_count,
        "avg_distance": avg_distance,
        "avg_duration": avg_duration,
        "jobs_std": jobs_std,
        "staffing_df": staffing_df,
        "engineer_summary_df": engineer_summary_df,
        "region_zip_df": region_zip_df,
        "filtered_assignment": filtered_assignment,
        "filtered_schedule": filtered_schedule,
        "filtered_home": filtered_home,
        "selected_region": selected_region,
    }


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
        st.radio("Assignment View", view_options, horizontal=True, key="common_vrp_compare_mode")
    st.metric("Service Count", state["service_count"])
    st.metric("Assigned Engineer Count", f"{state['engineer_count']} (DMS {state['dms_engineer_count']}, DMS2 {state['dms2_engineer_count']})")
    st.metric("Average Distance (km)", f"{state['avg_distance']:.2f}")
    st.metric("Average Duration (min)", f"{state['avg_duration']:.2f}")
    st.metric("Jobs per Engineer Std", f"{state['jobs_std']:.2f}")
    if not state["staffing_df"].empty:
        st.markdown("**Regional Staffing / Jobs**")
        st.dataframe(state["staffing_df"], width="stretch", hide_index=True)
    if not state["engineer_summary_df"].empty:
        st.markdown("**Engineer Summary**")
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
    selected_date_col, selected_region_col, selected_engineer_col = st.columns(3)
    selected_date_col.selectbox("Date", options=available_dates, index=0 if available_dates else None, key="common_result_date")
    selected_region_col.selectbox("Region", options=available_regions, index=0, key="common_result_region")
    selected_engineer_col.selectbox("Engineer", options=engineer_options, index=0, key="common_result_engineer")
    state = _build_result_view_state(subsidiary_name, strategic_city_name)
    filtered_assignment = state["filtered_assignment"]
    filtered_schedule = state["filtered_schedule"]
    filtered_home = state["filtered_home"]
    selected_region = state["selected_region"]
    region_zip_df = state["region_zip_df"]
    if not filtered_assignment.empty:
        route_groups = _build_route_groups(filtered_schedule, strategic_city_name)
        map_obj = build_map(
            strategic_city_name,
            selected_region,
            filtered_assignment,
            filtered_home,
            route_groups,
            region_zip_df,
        )
        _render_folium_map(map_obj, height=760)
        st.subheader("Selected Schedule")
        display_cols = [
            "service_date_key",
            "assigned_sm_name",
            "assigned_sm_code",
            "GSFS_RECEIPT_NO",
            "fixed",
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
            file_name=f"{st.session_state.get('common_vrp_job_id', 'common_vrp_job')}_schedule.csv",
            mime="text/csv",
            width="stretch",
        )


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
    rows: list[dict] = []
    for _, row in unique_jobs.iterrows():
        code = str(row["svc_engineer_code"]).strip()
        master_row = engineer_master_df[engineer_master_df["employee_code"].astype(str) == code].head(1)
        center_type = str(master_row.iloc[0]["center_type"]) if not master_row.empty else "DMS"
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
                "max_jobs": 8,
                "available": True,
                "start_location_type": "Home",
                "start_location_address": "",
                "start_latitude": None,
                "start_longitude": None,
                "source": "same_as_jobs",
            }
        )
    return rows


def _build_default_technician_rows_from_master(engineer_master_df: pd.DataFrame, subsidiary_name: str, strategic_city_name: str) -> list[dict]:
    if engineer_master_df.empty:
        return []
    rows: list[dict] = []
    for _, row in engineer_master_df.drop_duplicates(subset=["employee_code"]).iterrows():
        rows.append(
            {
                "record_id": uuid.uuid4().hex,
                "subsidiary_name": subsidiary_name,
                "strategic_city_name": strategic_city_name,
                "employee_code": str(row["employee_code"]).strip(),
                "employee_name": str(row["employee_name"]).strip(),
                "center_type": str(row.get("center_type", "DMS")).strip().upper() or "DMS",
                "shift_start": "09:00",
                "shift_end": "18:00",
                "slot_count": 8,
                "max_jobs": 8,
                "available": True,
                "start_location_type": "Home",
                "start_location_address": "",
                "start_latitude": None,
                "start_longitude": None,
                "source": "all_technicians",
            }
        )
    return rows


@st.dialog("Direct Job Input", width="large")
def _direct_job_dialog(
    master_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    subsidiary_name: str,
    strategic_city_name: str,
    existing_jobs_df: pd.DataFrame,
    edit_record: pd.Series | None = None,
) -> None:
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
    selected_engineer_label = st.selectbox(
        "SVC_ENGINEER_NAME",
        engineer_labels,
        index=engineer_labels.index(default_engineer_label) if default_engineer_label in engineer_labels else 0,
    )
    selected_engineer_row = engineer_master_df[(
        engineer_master_df["employee_name"].astype(str) + " (" + engineer_master_df["employee_code"].astype(str) + ")"
    ) == str(selected_engineer_label)].head(1)
    if selected_engineer_row.empty:
        st.error("No engineer selected.")
        return
    selected_engineer_row = selected_engineer_row.iloc[0]

    default_promise = str(edit_record.get("promise_date", "")) if edit_record is not None else ""
    default_promise_date = pd.to_datetime(default_promise, format="%Y%m%d", errors="coerce")
    promise_date_value = st.date_input(
        "PROMISE_DATE",
        value=default_promise_date.date() if pd.notna(default_promise_date) else None,
    )
    receipt_no = st.text_input("GSFS_RECEIPT_NO", value=str(edit_record.get("gsfs_receipt_no", "")) if edit_record is not None else "")
    fixed = st.checkbox("Fixed Assignment", value=_coerce_bool_value(edit_record.get("fixed", False)) if edit_record is not None else False)

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

    if st.button("Save Job", type="primary", width="stretch"):
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
            st.session_state["common_job_dialog_record_id"] = None
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
    if st.button("Close", width="stretch"):
        st.session_state["common_job_dialog_record_id"] = None
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
        uploaded_file = st.file_uploader("Upload Job CSV", type=["csv"])
        if st.button("Save Uploaded Jobs", type="primary", width="stretch"):
            if uploaded_file is None:
                st.warning("Upload a CSV file first.")
            else:
                raw_df = _read_uploaded_service_csv(uploaded_file)
                prepared_df, duplicate_receipts = _prepare_jobs_df(raw_df, subsidiary_name, strategic_city_name, jobs_df)
                success_df, failed_df = _geocode_jobs_df(prepared_df)
                if not success_df.empty:
                    _save_local_jobs(subsidiary_name, strategic_city_name, _job_rows_to_df(_build_job_upsert_rows(success_df)))
                    st.success(f"Saved {len(success_df)} jobs.")
                    st.rerun()
                if duplicate_receipts:
                    st.warning(f"Skipped duplicates: {', '.join(duplicate_receipts)}")
                if not failed_df.empty:
                    st.error("Address error rows exist.")
    else:
        if st.button("Open Direct Job Input", type="primary", width="stretch"):
            st.session_state["common_job_dialog_record_id"] = "__new__"
    dialog_record_id = st.session_state.get("common_job_dialog_record_id")
    if dialog_record_id is not None:
        edit_record = None
        if str(dialog_record_id) != "__new__":
            matched = jobs_df[jobs_df["record_id"].astype(str) == str(dialog_record_id)].head(1)
            if not matched.empty:
                edit_record = matched.iloc[0]
        _direct_job_dialog(master_df, engineer_master_df, subsidiary_name, strategic_city_name, jobs_df, edit_record)

    jobs_df = _load_local_jobs(subsidiary_name, strategic_city_name)
    if jobs_df.empty:
        st.info("No saved jobs.")
    else:
        promise_dates = sorted(jobs_df["promise_date"].dropna().astype(str).unique().tolist(), reverse=True)
        selected_date = st.selectbox("Filter by PROMISE_DATE", ["ALL"] + promise_dates, index=0)
        if selected_date != "ALL":
            jobs_df = jobs_df[jobs_df["promise_date"].astype(str) == str(selected_date)].copy()
        display_cols = [
            "svc_engineer_name",
            "svc_engineer_code",
            "gsfs_receipt_no",
            "fixed",
            "promise_date",
            "service_product_group_code",
            "service_product_code",
            "receipt_detail_symptom_code",
            "city_name",
            "postal_code",
            "address_line1_info",
        ]
        dataframe_state = st.dataframe(
            jobs_df[display_cols],
            width="stretch",
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="common_saved_jobs_table",
        )
        selected_rows = list(getattr(getattr(dataframe_state, "selection", None), "rows", []) or [])
        if selected_rows:
            selected_idx = int(selected_rows[0])
            if 0 <= selected_idx < len(jobs_df):
                selected_record_id = str(jobs_df.iloc[selected_idx]["record_id"])
                selected_receipt = str(jobs_df.iloc[selected_idx]["gsfs_receipt_no"])
                st.caption(f"Selected receipt: {selected_receipt}")
                if st.button("Edit Selected Row", width="stretch"):
                    st.session_state["common_job_dialog_record_id"] = selected_record_id
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

    add_mode = st.radio("Technician Source", ["Same As Jobs", "All Technicians"], horizontal=True)
    if add_mode == "Same As Jobs":
        if st.button("Add Technicians From Jobs", width="stretch"):
            rows = _build_default_technician_rows_from_jobs(selected_jobs_df, engineer_master_df, subsidiary_name, strategic_city_name)
            if rows:
                merged_df = _merge_technician_rows(technicians_df, rows)
                technicians_df = merged_df
                _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), technicians_df.to_dict("records"))
                st.success(f"Added {len(rows)} technicians from jobs.")
            else:
                st.warning("No technicians found in saved jobs.")
    else:
        if st.button("Add All Technicians In City", width="stretch"):
            rows = _build_default_technician_rows_from_master(engineer_master_df, subsidiary_name, strategic_city_name)
            if rows:
                merged_df = _merge_technician_rows(technicians_df, rows)
                technicians_df = merged_df
                _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), technicians_df.to_dict("records"))
                st.success(f"Added {len(rows)} technicians from city master.")
            else:
                st.warning("No engineer master rows for the selected city.")

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
        "max_jobs",
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
            "available": st.column_config.CheckboxColumn("available"),
            "slot_count": st.column_config.NumberColumn("slot_count", min_value=0, step=1),
            "max_jobs": st.column_config.NumberColumn("max_jobs", min_value=0, step=1),
            "start_location_type": st.column_config.SelectboxColumn(
                "start_location_type",
                options=["Home", "Custom Address"],
                required=True,
            ),
        },
        key=editor_key,
    )
    if st.button("Save Technician List", type="primary", width="stretch"):
        rows = edited_df.to_dict("records")
        source_series = technicians_df["source"] if "source" in technicians_df.columns else pd.Series(["manual_input"] * len(technicians_df))
        record_id_series = technicians_df["record_id"] if "record_id" in technicians_df.columns else pd.Series([uuid.uuid4().hex for _ in range(len(technicians_df))])
        enriched_rows: list[dict] = []
        for idx, row in enumerate(rows):
            enriched = dict(row)
            enriched["record_id"] = str(record_id_series.iloc[idx]) if idx < len(record_id_series) else uuid.uuid4().hex
            enriched["source"] = str(source_series.iloc[idx]) if idx < len(source_series) else "manual_input"
            enriched["subsidiary_name"] = subsidiary_name
            enriched["strategic_city_name"] = strategic_city_name
            enriched["promise_date"] = str(selected_date)
            enriched_rows.append(enriched)
        saved_df = pd.DataFrame(enriched_rows)
        _save_local_technicians(subsidiary_name, strategic_city_name, str(selected_date), saved_df)
        _save_technician_draft(subsidiary_name, strategic_city_name, str(selected_date), enriched_rows)
        st.success("Technician list saved.")
        st.rerun()


def _render_payload_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    jobs_df = _load_local_jobs(subsidiary_name, strategic_city_name)
    if jobs_df.empty:
        st.info("No saved jobs.")
        return
    promise_dates = sorted(jobs_df["promise_date"].dropna().astype(str).unique().tolist(), reverse=True)
    selected_date = st.selectbox("PROMISE_DATE to Build Payload", promise_dates, index=0 if promise_dates else None)
    selected_jobs_df = jobs_df[jobs_df["promise_date"].astype(str) == str(selected_date)].copy() if selected_date else jobs_df.head(0).copy()
    technicians_df = _load_local_technicians(subsidiary_name, strategic_city_name, str(selected_date))
    st.caption(f"Jobs for selected date: {len(selected_jobs_df)}")
    available_tech_count = int(technicians_df["available"].fillna(False).astype(bool).sum()) if not technicians_df.empty and "available" in technicians_df.columns else 0
    st.caption(f"Available technicians saved: {available_tech_count}")
    return_to_home = st.checkbox("Return To Home", value=bool(st.session_state.get("common_vrp_return_to_home", True)), key="common_vrp_return_to_home")

    if st.button("Build Payload", type="primary", width="stretch"):
        try:
            with st.spinner("Building payload..."):
                response = _api_post(
                    DEFAULT_COMMON_SERVER_URL,
                    "/api/v1/common/routing/build-payload",
                    {
                        "subsidiary_name": subsidiary_name,
                        "strategic_city_name": strategic_city_name,
                        "promise_date": str(selected_date),
                        "jobs": selected_jobs_df.to_dict("records"),
                        "technicians": technicians_df.to_dict("records"),
                        "mode": "na_general",
                        "return_to_home": bool(return_to_home),
                    },
                )
            payload = response.get("payload")
            st.session_state["common_vrp_payload"] = payload
            st.session_state["common_vrp_request_id"] = ""
            st.session_state["common_vrp_job_id"] = ""
            st.session_state["common_vrp_job_status"] = None
            st.session_state["common_vrp_job_result"] = None
            jobs_count = len(list(payload.get("jobs", []))) if payload else 0
            tech_count = len(list(payload.get("technicians", []))) if payload else 0
            st.success(
                f"Payload ready: technicians={tech_count}, jobs={jobs_count}"
            )
        except Exception as exc:
            st.error(str(exc))

    payload = st.session_state.get("common_vrp_payload")
    if payload:
        jobs_list = list(payload.get("jobs", []))
        st.caption(
            f"Payload: technicians={len(payload.get('technicians', []))}, jobs={len(jobs_list)}"
        )
        with st.expander("Payload Preview", expanded=False):
            st.json(payload)
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


def _render_routing_config_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    row = _api_get(
        DEFAULT_COMMON_SERVER_URL,
        "/api/v1/common/routing-config",
        subsidiary_name=subsidiary_name,
        strategic_city_name=strategic_city_name,
    ).get("row")
    if not row:
        st.info("No routing config for selected city.")
        return
    edit_df = pd.DataFrame([row])
    editable_cols = [
        "distance_backend",
        "assignment_distance_backend",
        "osrm_url",
        "osrm_profile",
        "effective_service_per_sm",
        "target_sm_per_region",
        "service_time_per_job_min",
        "max_work_min_per_sm_day",
        "max_travel_min_per_sm_day",
        "max_travel_km_per_sm_day",
        "timezone_offset",
    ]
    edited = st.data_editor(edit_df[["subsidiary_name", "strategic_city_name"] + editable_cols], width="stretch", hide_index=True, num_rows="fixed", key="common_routing_config_editor")
    if st.button("Save Routing Config", width="stretch"):
        _api_post(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/routing-config/upsert", edited.iloc[0].to_dict())
        st.success("Routing config saved.")


def _render_masters_tab(subsidiary_name: str, strategic_city_name: str) -> None:
    region_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/regions",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    capability_df = pd.DataFrame(
        _api_get(
            DEFAULT_COMMON_SERVER_URL,
            "/api/v1/common/capabilities",
            subsidiary_name=subsidiary_name,
            strategic_city_name=strategic_city_name,
        ).get("rows", [])
    )
    st.markdown("**Region Master**")
    if region_df.empty:
        st.info("No region rows.")
    else:
        st.dataframe(region_df, width="stretch", hide_index=True)
    st.markdown("**Technician Capability Master**")
    if capability_df.empty:
        st.info("No capability rows.")
    else:
        st.dataframe(capability_df, width="stretch", hide_index=True)


def main() -> None:
    st.title("Common VRP Client")
    try:
        contexts = _api_get(DEFAULT_COMMON_SERVER_URL, "/api/v1/common/contexts")
        subsidiaries = contexts.get("subsidiaries", []) or [DEFAULT_SUBSIDIARY_NAME]
        cities = contexts.get("cities", []) or [DEFAULT_STRATEGIC_CITY_NAME]
    except Exception:
        subsidiaries = [DEFAULT_SUBSIDIARY_NAME]
        cities = [DEFAULT_STRATEGIC_CITY_NAME]

    left_col, right_col = st.columns([1, 1.7])
    with left_col:
        top_col1, top_col2 = st.columns(2)
        subsidiary_name = top_col1.selectbox(
            "SUBSIDIARY_NAME",
            subsidiaries,
            index=subsidiaries.index(DEFAULT_SUBSIDIARY_NAME) if DEFAULT_SUBSIDIARY_NAME in subsidiaries else 0,
        )
        strategic_city_name = top_col2.selectbox(
            "STRATEGIC_CITY_NAME",
            cities,
            index=cities.index(DEFAULT_STRATEGIC_CITY_NAME) if DEFAULT_STRATEGIC_CITY_NAME in cities else 0,
        )
        jobs_tab, technicians_tab, payload_tab, routing_config_tab, masters_tab = st.tabs(
            ["Jobs", "Technicians", "Payload", "Routing Config", "Masters"]
        )
        with jobs_tab:
            _render_jobs_tab(subsidiary_name, strategic_city_name)
        with technicians_tab:
            _render_technicians_tab(subsidiary_name, strategic_city_name)
        with payload_tab:
            _render_payload_tab(subsidiary_name, strategic_city_name)
        with routing_config_tab:
            _render_routing_config_tab(subsidiary_name, strategic_city_name)
        with masters_tab:
            _render_masters_tab(subsidiary_name, strategic_city_name)
        _render_result_summary(subsidiary_name, strategic_city_name)
    with right_col:
        _render_result_detail(subsidiary_name, strategic_city_name)


if __name__ == "__main__":
    main()
