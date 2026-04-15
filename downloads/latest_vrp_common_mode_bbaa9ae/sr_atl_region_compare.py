from __future__ import annotations

import math
from pathlib import Path

import folium
import pandas as pd
import streamlit as st

from smart_routing.area_map import load_city_map_data


st.set_page_config(page_title="Atlanta Region Compare", layout="wide")

PROFILE_COPY_PATH = Path("260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx")
OUR_REGION_PATH = Path("260310/production_input/atlanta_fixed_region_zip_3_manual320.csv")
MANUAL_REGION_PATH = Path("260310/ATL Three Markets.xlsx")

OUR_COLORS = {
    "Atlanta New Region 1": "#db4437",
    "Atlanta New Region 2": "#0f9d58",
    "Atlanta New Region 3": "#4285f4",
}
MANUAL_COLORS = {
    "ATL West": "#db4437",
    "ATL East": "#0f9d58",
    "ATL South": "#4285f4",
}
BEST_BUCKET_TO_REGION = {
    "ATL West": 1,
    "ATL East": 2,
    "ATL South": 3,
}


@st.cache_data(show_spinner=False)
def load_compare_data():
    city_data = load_city_map_data("Atlanta, GA")
    zip_layer = city_data.zip_layer.copy()
    zip_layer["POSTAL_CODE"] = zip_layer["POSTAL_CODE"].astype(str).str.zfill(5)

    coverage_df = pd.read_excel(PROFILE_COPY_PATH, sheet_name="1. Zip Coverage", dtype={"POSTAL_CODE": str})
    coverage_df = coverage_df[coverage_df["STRATEGIC_CITY_NAME"].astype(str).eq("Atlanta, GA")].copy()
    coverage_df["POSTAL_CODE"] = coverage_df["POSTAL_CODE"].astype(str).str.zfill(5)
    coverage_df = coverage_df[["POSTAL_CODE"]].drop_duplicates().copy()

    our_df = pd.read_csv(OUR_REGION_PATH, dtype={"POSTAL_CODE": str})
    our_df["POSTAL_CODE"] = our_df["POSTAL_CODE"].astype(str).str.zfill(5)
    our_df = our_df[["POSTAL_CODE", "region_seq", "new_region_name"]].drop_duplicates().copy()

    manual_df = pd.read_excel(MANUAL_REGION_PATH, sheet_name=0, dtype={"Zip Code": str})
    manual_df.columns = [str(c).strip() for c in manual_df.columns]
    manual_df = manual_df.rename(columns={"Zip Code": "POSTAL_CODE", "Bucket": "manual_bucket"})
    manual_df["POSTAL_CODE"] = manual_df["POSTAL_CODE"].astype(str).str.zfill(5)
    manual_df = manual_df[["POSTAL_CODE", "manual_bucket"]].drop_duplicates().copy()

    coverage_layer = zip_layer.merge(coverage_df, on="POSTAL_CODE", how="inner")

    assigned_projected = zip_layer.merge(
        our_df[["POSTAL_CODE", "region_seq", "new_region_name"]], on="POSTAL_CODE", how="inner"
    ).to_crs(epsg=3857)
    region_center_lookup: dict[int, tuple[float, float, str]] = {}
    for (region_seq, region_name), group in assigned_projected.groupby(["region_seq", "new_region_name"], dropna=False):
        centroid = group.geometry.unary_union.centroid
        region_center_lookup[int(region_seq)] = (float(centroid.x), float(centroid.y), str(region_name))

    our_layer = coverage_layer.merge(our_df, on="POSTAL_CODE", how="left")
    unassigned_mask = our_layer["region_seq"].isna()
    if unassigned_mask.any():
        projected = our_layer.to_crs(epsg=3857)
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
                our_layer.loc[idx, "region_seq"] = int(best_region[0])
                our_layer.loc[idx, "new_region_name"] = str(best_region[1])

    manual_layer = coverage_layer.merge(manual_df, on="POSTAL_CODE", how="left")
    compare_df = (
        coverage_df.merge(our_layer[["POSTAL_CODE", "region_seq", "new_region_name"]], on="POSTAL_CODE", how="left")
        .merge(manual_df, on="POSTAL_CODE", how="left")
    )
    compare_df["matched_region_seq"] = compare_df["manual_bucket"].map(BEST_BUCKET_TO_REGION)
    compare_df["is_match"] = compare_df["matched_region_seq"].fillna(-1).astype(int) == pd.to_numeric(compare_df["region_seq"], errors="coerce").fillna(-2).astype(int)
    return coverage_layer, our_layer, manual_layer, compare_df


def _map_center(layer) -> tuple[float, float]:
    if layer.empty:
        return 33.7490, -84.3880
    center_points = layer.to_crs(epsg=3857).geometry.centroid.to_crs(epsg=4326)
    return float(center_points.y.mean()), float(center_points.x.mean())


def _build_map(layer, scheme: str):
    center_lat, center_lon = _map_center(layer)
    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=9, tiles="CartoDB positron")
    if scheme == "our":
        folium.GeoJson(
            data=layer.to_json(),
            style_function=lambda feature: {
                "color": OUR_COLORS.get(feature["properties"].get("new_region_name", ""), "#9aa0a6"),
                "weight": 1.0,
                "fillColor": OUR_COLORS.get(feature["properties"].get("new_region_name", ""), "#dddddd"),
                "fillOpacity": 0.18,
            },
            tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "new_region_name"], aliases=["ZIP", "Our Region"]),
        ).add_to(fmap)
    else:
        folium.GeoJson(
            data=layer.to_json(),
            style_function=lambda feature: {
                "color": MANUAL_COLORS.get(feature["properties"].get("manual_bucket", ""), "#b8bec7"),
                "weight": 1.0,
                "fillColor": MANUAL_COLORS.get(feature["properties"].get("manual_bucket", ""), "#eceff3"),
                "fillOpacity": 0.18,
            },
            tooltip=folium.GeoJsonTooltip(fields=["POSTAL_CODE", "manual_bucket"], aliases=["ZIP", "Manual Bucket"]),
        ).add_to(fmap)
    return fmap


def main():
    st.title("Atlanta Region Compare")
    coverage_layer, our_layer, manual_layer, compare_df = load_compare_data()

    overlap_df = compare_df[compare_df["manual_bucket"].notna() & compare_df["region_seq"].notna()].copy()
    overlap_count = int(overlap_df["POSTAL_CODE"].nunique())
    match_count = int(overlap_df["is_match"].sum())
    coverage_count = int(compare_df["POSTAL_CODE"].nunique())
    manual_count = int(compare_df["manual_bucket"].notna().sum())
    our_count = int(compare_df["region_seq"].notna().sum())

    a, b, c, d = st.columns(4)
    a.metric("Coverage ZIPs", coverage_count)
    b.metric("Manual ZIPs On Map", manual_count)
    c.metric("Our ZIPs On Map", our_count)
    d.metric("Matched ZIPs", f"{match_count} / {overlap_count}")
    st.caption("Left map uses the 320-ZIP merged region assignment. Only ZIPs with geometry are visible on the map.")

    left, right = st.columns(2)
    with left:
        st.markdown("**Our Regions**")
        st.iframe(_build_map(our_layer, "our").get_root().render(), height=760)
    with right:
        st.markdown("**Manual Regions**")
        st.iframe(_build_map(manual_layer, "manual").get_root().render(), height=760)

    stats_rows = []
    for bucket, region_seq in BEST_BUCKET_TO_REGION.items():
        bucket_set = set(compare_df.loc[compare_df["manual_bucket"] == bucket, "POSTAL_CODE"])
        region_set = set(compare_df.loc[pd.to_numeric(compare_df["region_seq"], errors="coerce") == int(region_seq), "POSTAL_CODE"])
        overlap = bucket_set & region_set
        stats_rows.append(
            {
                "manual_bucket": bucket,
                "matched_region_seq": region_seq,
                "manual_zip_count": len(bucket_set),
                "region_zip_count": len(region_set),
                "overlap_zip_count": len(overlap),
                "manual_match_ratio": round(len(overlap) / len(bucket_set), 4) if bucket_set else 0.0,
                "region_match_ratio": round(len(overlap) / len(region_set), 4) if region_set else 0.0,
                "manual_only_zip_count": len(bucket_set - region_set),
                "region_only_zip_count": len(region_set - bucket_set),
            }
        )
    stats_df = pd.DataFrame(stats_rows)

    mismatch_df = overlap_df[~overlap_df["is_match"]].copy()
    mismatch_df = mismatch_df[["POSTAL_CODE", "manual_bucket", "region_seq", "new_region_name"]].sort_values(["manual_bucket", "POSTAL_CODE"]).reset_index(drop=True)

    st.markdown("**Bucket Mapping Stats**")
    st.dataframe(stats_df, width="stretch", hide_index=True)
    st.markdown("**Mismatched ZIPs**")
    st.dataframe(mismatch_df, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
