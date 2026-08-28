from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pandas as pd

from smart_routing.region_candidate_planner import _center_shared_radial_labels, _connected_components, _contiguous_nearest_growth, _fair_staffing_targets, build_city_region_candidate
from tools.data.region_plan_area_map import build_area_map_region_plan


class RegionCandidatePlannerTests(unittest.TestCase):
    def test_extra_technicians_follow_workload_quota_before_home_distance(self) -> None:
        required = pd.Series([1, 1, 1, 1, 1])
        workload = pd.Series([2328, 2401, 2335, 1990, 2444])
        targets = _fair_staffing_targets(required, 14, workload)
        self.assertEqual([3, 3, 3, 2, 3], targets.tolist())

    def test_capacity_shortfall_uses_workload_to_break_equal_peak_ties(self) -> None:
        required = pd.Series([3, 3, 3, 3, 3])
        workload = pd.Series([2328, 2401, 2335, 1990, 2444])
        targets = _fair_staffing_targets(required, 14, workload)
        self.assertEqual([3, 3, 3, 2, 3], targets.tolist())

    def test_shared_centre_radial_uses_continuous_angular_sectors(self) -> None:
        postals = pd.DataFrame([
            {"POSTAL_CODE": "10001", "latitude": 1.0, "longitude": 0.0, "service_count": 10},
            {"POSTAL_CODE": "10002", "latitude": 1.0, "longitude": 1.0, "service_count": 10},
            {"POSTAL_CODE": "10003", "latitude": 0.0, "longitude": 1.0, "service_count": 10},
            {"POSTAL_CODE": "10004", "latitude": -1.0, "longitude": 1.0, "service_count": 10},
            {"POSTAL_CODE": "10005", "latitude": -1.0, "longitude": 0.0, "service_count": 10},
            {"POSTAL_CODE": "10006", "latitude": -1.0, "longitude": -1.0, "service_count": 10},
            {"POSTAL_CODE": "10007", "latitude": 0.0, "longitude": -1.0, "service_count": 10},
            {"POSTAL_CODE": "10008", "latitude": 1.0, "longitude": -1.0, "service_count": 10},
        ])
        labels = _center_shared_radial_labels(postals, 4)
        self.assertEqual({0, 1, 2, 3}, set(labels.tolist()))
        self.assertLessEqual(max(int((labels == label).sum()) for label in range(4)), 2)

    def test_contiguous_growth_never_uses_demand_to_jump_a_zip_boundary(self) -> None:
        postals = pd.DataFrame(
            [
                {"POSTAL_CODE": "10001", "latitude": 40.00, "longitude": -74.00, "service_count": 100},
                {"POSTAL_CODE": "10002", "latitude": 40.01, "longitude": -74.00, "service_count": 0},
                {"POSTAL_CODE": "10003", "latitude": 40.02, "longitude": -74.00, "service_count": 0},
                {"POSTAL_CODE": "10004", "latitude": 40.03, "longitude": -74.00, "service_count": 0},
                {"POSTAL_CODE": "10005", "latitude": 40.04, "longitude": -74.00, "service_count": 100},
            ]
        )
        graph = {
            "10001": {"10002"},
            "10002": {"10001", "10003"},
            "10003": {"10002", "10004"},
            "10004": {"10003", "10005"},
            "10005": {"10004"},
        }
        with mock.patch("smart_routing.region_candidate_planner._adjacency_graph", return_value=graph):
            result, islands = _contiguous_nearest_growth(postals, Path("unused.zip"), [0, 4])
        self.assertEqual(0, islands)
        for _, group in result.groupby("region_seq"):
            components = _connected_components(set(group["POSTAL_CODE"]), graph)
            self.assertEqual(1, len(components))

    def test_creates_area_map_and_technician_candidate_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service_path = root / "service.csv"
            profile_path = root / "profile.xlsx"
            pd.DataFrame(
                [
                    {"STRATEGIC_CITY_NAME": "Test City, TS", "POSTAL_CODE": "10001", "latitude": 40.0, "longitude": -74.0, "GSFS_RECEIPT_NO": "A", "PROMISE_DATE": "2026-01-01"},
                    {"STRATEGIC_CITY_NAME": "Test City, TS", "POSTAL_CODE": "10001", "latitude": 40.0, "longitude": -74.0, "GSFS_RECEIPT_NO": "B", "PROMISE_DATE": "2026-01-01"},
                    {"STRATEGIC_CITY_NAME": "Test City, TS", "POSTAL_CODE": "10002", "latitude": 40.2, "longitude": -73.8, "GSFS_RECEIPT_NO": "C", "PROMISE_DATE": "2026-01-02"},
                    {"STRATEGIC_CITY_NAME": "Test City, TS", "POSTAL_CODE": "19999", "latitude": 41.0, "longitude": -72.0, "GSFS_RECEIPT_NO": "OUTSIDE", "PROMISE_DATE": "2026-01-02"},
                    {"STRATEGIC_CITY_NAME": "Other City, TS", "POSTAL_CODE": "20001", "latitude": 38.9, "longitude": -77.0, "GSFS_RECEIPT_NO": "D", "PROMISE_DATE": "2026-01-01"},
                ]
            ).to_csv(service_path, index=False, encoding="utf-8-sig")
            with pd.ExcelWriter(profile_path) as writer:
                pd.DataFrame(
                    [
                        {"POSTAL_CODE": "10001", "STRATEGIC_CITY_NAME": "Test City, TS", "SVC_CENTER_TYPE": "DMS"},
                        {"POSTAL_CODE": "10002", "STRATEGIC_CITY_NAME": "Test City, TS", "SVC_CENTER_TYPE": "DMS2"},
                    ]
                ).to_excel(writer, sheet_name="1. Zip Coverage", index=False)
                pd.DataFrame(
                    [
                        {"SVC_ENGINEER_CODE": "T1", "Name": "Tech One", "City ": "Home One", "State": "Test City, TS", "latitude": 40.01, "longitude": -74.01},
                        {"SVC_ENGINEER_CODE": "T2", "Name": "Tech Two", "City ": "Home Two", "State": "Test City, TS", "latitude": 40.19, "longitude": -73.81},
                    ]
                ).to_excel(writer, sheet_name="4. Address", index=False)

            result = build_city_region_candidate(
                service_file=service_path,
                profile_file=profile_path,
                city_name="Test City, TS",
                region_count=2,
                algorithm="balanced",
                output_root=root / "candidates",
            )

            postal = pd.read_csv(result.region_postals_path, dtype={"POSTAL_CODE": str})
            technicians = pd.read_csv(result.technician_assignments_path)
            summary = pd.read_csv(result.region_summary_path)
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(["POSTAL_CODE", "STRATEGIC_CITY_NAME", "region_id", "region_seq", "AREA_NAME", "area_type"], list(postal.columns))
            self.assertEqual(2, postal["region_seq"].nunique())
            self.assertEqual({"DMS", "DMS2"}, set(postal["area_type"]))
            self.assertEqual({"T1", "T2"}, set(technicians["employee_code"]))
            self.assertEqual({"Tech One", "Tech Two"}, set(technicians["SVC_ENGINEER_NAME"]))
            self.assertTrue((summary["required_technician_count"] >= 1).all())
            self.assertEqual(2, int(summary["candidate_technician_target"].sum()))
            self.assertTrue((summary["assigned_technician_count"] >= 1).all())
            self.assertEqual(2, manifest["row_accounting"]["assigned_technicians"])
            self.assertEqual(1, manifest["row_accounting"]["service_jobs_excluded_outside_profile_zip_coverage"])
            self.assertEqual("not_applied", manifest["radius_constraint"])
            export = build_area_map_region_plan(
                result.region_postals_path.name,
                result.region_postals_path.read_bytes(),
                result.technician_assignments_path.name,
                result.technician_assignments_path.read_bytes(),
                subsidiary_id="LGEAI",
                source_city_id="Test City, TS",
                target_city_id="Test_City_TS",
                policy_version="active_roster_area_type_fallback_region_soft/v1",
                plan_display_name="Test candidate",
            )
            self.assertEqual(2, len(export.area_df))
            self.assertEqual({"DMS", "DMS2"}, set(export.area_df["area_type"]))
            self.assertEqual({"T1", "T2"}, set(export.technician_df["technician_id"]))
