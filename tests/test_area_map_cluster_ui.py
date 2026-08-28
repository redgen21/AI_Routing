from __future__ import annotations

import unittest
from pathlib import Path
import tempfile
from unittest import mock

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from streamlit.testing.v1 import AppTest


class AreaMapClusterUiTests(unittest.TestCase):
    def test_candidate_cluster_panel_exposes_required_controls(self) -> None:
        def panel() -> None:
            import streamlit as st
            import sr_area_map as area_map

            st.session_state["area-plan-cluster-editor"] = {
                "selected_city": "Atlanta, GA",
                "selected_subsidiary": "LGEAI",
            }
            area_map._render_candidate_cluster_editor("LGEAI", "Atlanta, GA")

        rendered = AppTest.from_function(panel).run(timeout=20)
        self.assertTrue(any(item.value == "New Cluster" for item in rendered.subheader))
        self.assertTrue(any(item.label == "Region count" for item in rendered.number_input))
        self.assertTrue(any(item.label == "Clustering method" for item in rendered.selectbox))
        self.assertTrue(any(item.label == "Create cluster candidate" for item in rendered.button))

    def test_candidate_cluster_map_renders_zip_and_home_layers(self) -> None:
        import sr_area_map as area_map

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            postal_path = root / "candidate_postals.csv"
            summary_path = root / "summary.csv"
            evidence_path = root / "evidence.csv"
            pd.DataFrame([
                {"POSTAL_CODE": "10001", "AREA_NAME": "Region 1", "region_seq": 1, "region_id": "candidate_r01"},
            ]).to_csv(postal_path, index=False, encoding="utf-8-sig")
            pd.DataFrame([
                {"region_seq": 1, "AREA_NAME": "Region 1", "centroid_latitude": 40.0, "centroid_longitude": -74.0, "postal_count": 1, "annual_service_count": 10, "avg_daily_jobs": 1.5, "assigned_technician_count": 1, "assigned_technician_names": "Tech One"},
            ]).to_csv(summary_path, index=False, encoding="utf-8-sig")
            pd.DataFrame([
                {"SVC_ENGINEER_CODE": "T1", "AREA_NAME": "Region 1", "latitude": 40.01, "longitude": -74.01},
            ]).to_csv(evidence_path, index=False, encoding="utf-8-sig")
            geometry = gpd.GeoDataFrame(
                {"POSTAL_CODE": ["10001"]},
                geometry=[Polygon([(-74.1, 39.9), (-73.9, 39.9), (-73.9, 40.1), (-74.1, 40.1)])],
                crs="EPSG:4326",
            )
            with mock.patch.object(area_map, "_area_map_load_zcta_geometry", return_value=geometry):
                map_obj, layer, missing = area_map._build_candidate_cluster_map({
                    "region_postals": str(postal_path),
                    "region_summary": str(summary_path),
                    "evidence": str(evidence_path),
                })
            self.assertEqual(1, len(layer))
            self.assertEqual(0, missing)
            rendered_map = map_obj.get_root().render()
            self.assertIn("Candidate ZIP Regions", rendered_map)
            self.assertIn("Tech One", rendered_map)
            self.assertIn("daily average", rendered_map)

    def test_selected_area_plan_uses_candidate_style_summary_and_technicians(self) -> None:
        import sr_area_map as area_map

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "normalized").mkdir()
            pd.DataFrame([
                {"technician_id": "T1", "region_code": "r01", "active": "true", "policy_mode": "area_plan"},
            ]).to_csv(root / "normalized" / "technician.csv", index=False)
            area_layer = gpd.GeoDataFrame(
                [{"AREA_NAME": "Region 1", "region_id": "r01", "region_seq": 1, "postal_count": 2}],
                geometry=[Polygon([(-74.1, 39.9), (-73.9, 39.9), (-73.9, 40.1), (-74.1, 40.1)])],
                crs="EPSG:4326",
            )
            service = pd.DataFrame([
                {"AREA_NAME": "Region 1", "GSFS_RECEIPT_NO": "A", "service_date_key": "2026-01-01"},
                {"AREA_NAME": "Region 1", "GSFS_RECEIPT_NO": "B", "service_date_key": "2026-01-01"},
                {"AREA_NAME": "Region 1", "GSFS_RECEIPT_NO": "C", "service_date_key": "2026-01-02"},
            ])
            profile = pd.DataFrame([{"SVC_ENGINEER_CODE": "T1", "Name": "Tech One", "latitude": 40.0, "longitude": -74.0}])
            homes = {"T1": {"coord": (-74.0, 40.0)}}
            with mock.patch.object(area_map, "_load_profile_home_geocode_df", return_value=profile), mock.patch.object(area_map, "get_home_location_lookup", return_value=homes):
                summary, technicians = area_map._build_selected_area_plan_preview({"path": root}, area_layer, service, "Test City, TS")
                map_obj = area_map._build_selected_area_plan_preview_map(area_layer, summary, technicians)
            self.assertEqual(3, int(summary.loc[0, "annual_service_count"]))
            self.assertEqual(1.5, float(summary.loc[0, "avg_daily_jobs"]))
            self.assertEqual("Tech One", technicians.loc[0, "SVC_ENGINEER_NAME"])
            self.assertIn("Tech One", map_obj.get_root().render())


if __name__ == "__main__":
    unittest.main()
