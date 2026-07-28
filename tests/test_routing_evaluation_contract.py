from __future__ import annotations

import ast
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from smart_routing import export_daily_stats, production_atlanta, routing_compare, vrp_mode_z_weekend


class RegionPlanEvaluationContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.current_routes = pd.DataFrame(
            [
                {
                    "scenario": "current",
                    "STRATEGIC_CITY_NAME": "Atlanta, GA",
                    "service_date": pd.Timestamp("2026-03-01"),
                    "assignment_unit_id": "ATL_CURRENT",
                    "job_count": 2,
                    "unique_stop_count": 2,
                    "distance_km": 12.0,
                    "duration_min": 20.0,
                },
                {
                    "scenario": "current",
                    "STRATEGIC_CITY_NAME": "Los Angeles, CA",
                    "service_date": pd.Timestamp("2026-03-01"),
                    "assignment_unit_id": "LA_CURRENT",
                    "job_count": 9,
                    "unique_stop_count": 9,
                    "distance_km": 99.0,
                    "duration_min": 120.0,
                },
            ]
        )
        self.integrated_routes = pd.DataFrame(
            [
                {
                    "scenario": "integrated",
                    "STRATEGIC_CITY_NAME": "Atlanta, GA",
                    "service_date": pd.Timestamp("2026-03-01"),
                    "assignment_unit_id": "atlanta_ga_r01_sm01",
                    "region_id": "atlanta_ga_r01",
                    "job_count": 2,
                    "unique_stop_count": 2,
                    "distance_km": 10.0,
                    "duration_min": 18.0,
                }
            ]
        )
        self.service_df = pd.DataFrame(
            {
                "GSFS_RECEIPT_NO": ["A-1", "A-2", "LA-1"],
                "STRATEGIC_CITY_NAME": ["Atlanta, GA", "Atlanta, GA", "Los Angeles, CA"],
                "service_date": [
                    pd.Timestamp("2026-03-01"),
                    pd.Timestamp("2026-03-02"),
                    pd.Timestamp("2026-03-01"),
                ],
                "latitude": [33.75, 33.76, 34.05],
                "longitude": [-84.39, -84.38, -118.24],
            }
        )
        self.region_service_df = pd.DataFrame(
            {
                "GSFS_RECEIPT_NO": ["A-1", "A-2"],
                "STRATEGIC_CITY_NAME": ["Atlanta, GA", "Atlanta, GA"],
                "service_date": [pd.Timestamp("2026-03-01")] * 2,
                "region_id": ["atlanta_ga_r01"] * 2,
                "latitude": [33.75, 33.76],
                "longitude": [-84.39, -84.38],
            }
        )

    def _context(self) -> routing_compare.RegionPlanEvaluationContext:
        return routing_compare.RegionPlanEvaluationContext(
            service_df=self.service_df,
            current_route_df=self.current_routes,
            routing_config={
                "effective_service_per_sm": 4,
                "service_time_per_job_min": 55,
                "max_work_min_per_sm_day": 450,
                "max_travel_min_per_sm_day": 90,
                "max_travel_km_per_sm_day": 80,
                "assignment_distance_backend": "haversine",
            },
            client_map={},
            default_client=object(),  # type: ignore[arg-type]
        )

    def test_public_adapter_preserves_legacy_route_and_summary_results(self) -> None:
        with patch.object(
            routing_compare,
            "_build_integrated_routes",
            return_value=self.integrated_routes,
        ) as build_integrated:
            result = routing_compare.evaluate_region_plan(self._context(), self.region_service_df)

        expected_routes = pd.concat(
            [self.current_routes.iloc[[0]].copy(), self.integrated_routes],
            ignore_index=True,
        )
        expected_daily = routing_compare._build_daily_summary(expected_routes)
        expected_city = routing_compare._build_city_summary(expected_daily)
        assert_frame_equal(result.route_detail_df, expected_routes)
        assert_frame_equal(result.daily_summary_df, expected_daily)
        assert_frame_equal(result.city_summary_df, expected_city)

        call = build_integrated.call_args.kwargs
        self.assertEqual(call["effective_service_per_sm"], 4.0)
        self.assertEqual(call["service_time_per_job_min"], 55.0)
        self.assertEqual(call["max_work_min_per_sm_day"], 450.0)
        self.assertEqual(call["max_travel_min_per_sm_day"], 90.0)
        self.assertEqual(call["max_travel_km_per_sm_day"], 80.0)
        self.assertEqual(call["assignment_distance_backend"], "haversine")

    def test_public_adapter_rejects_uncovered_service_rows(self) -> None:
        invalid_df = self.region_service_df.copy()
        invalid_df.loc[1, "region_id"] = None
        with self.assertRaisesRegex(ValueError, "1 service rows without region_id"):
            routing_compare.evaluate_region_plan(self._context(), invalid_df)

    def test_public_adapter_rejects_missing_baseline_job(self) -> None:
        incomplete_df = self.region_service_df.iloc[[0]].copy()
        with self.assertRaisesRegex(ValueError, r"missing 1 baseline jobs \(A-2\)"):
            routing_compare.evaluate_region_plan(self._context(), incomplete_df)

    def test_public_adapter_rejects_duplicate_candidate_job(self) -> None:
        duplicate_df = self.region_service_df.copy()
        duplicate_df.loc[1, "GSFS_RECEIPT_NO"] = "A-1"
        with self.assertRaisesRegex(ValueError, "duplicate GSFS_RECEIPT_NO values: A-1"):
            routing_compare.evaluate_region_plan(self._context(), duplicate_df)

    def test_public_adapter_rejects_extra_candidate_job(self) -> None:
        extra_row = self.region_service_df.iloc[[0]].copy()
        extra_row["GSFS_RECEIPT_NO"] = "A-3"
        extra_df = pd.concat([self.region_service_df, extra_row], ignore_index=True)
        with self.assertRaisesRegex(ValueError, r"contains 1 extra jobs \(A-3\)"):
            routing_compare.evaluate_region_plan(self._context(), extra_df)

    def test_public_adapter_rejects_null_candidate_job_id(self) -> None:
        null_id_df = self.region_service_df.copy()
        null_id_df.loc[1, "GSFS_RECEIPT_NO"] = None
        with self.assertRaisesRegex(ValueError, "1 null GSFS_RECEIPT_NO values"):
            routing_compare.evaluate_region_plan(self._context(), null_id_df)

    def test_candidate_coordinates_and_dates_cannot_change_routing_input(self) -> None:
        manipulated_df = self.region_service_df.copy()
        manipulated_df["service_date"] = pd.Timestamp("2035-12-31")
        manipulated_df["latitude"] = [0.0, 0.1]
        manipulated_df["longitude"] = [0.0, 0.1]
        with patch.object(
            routing_compare,
            "_build_integrated_routes",
            return_value=self.integrated_routes,
        ) as build_integrated:
            routing_compare.evaluate_region_plan(self._context(), manipulated_df)

        routing_input = build_integrated.call_args.kwargs["region_service_df"]
        expected_baseline = self.service_df.iloc[:2].reset_index(drop=True)
        self.assertEqual(routing_input["service_date"].tolist(), expected_baseline["service_date"].tolist())
        self.assertEqual(routing_input["latitude"].tolist(), expected_baseline["latitude"].tolist())
        self.assertEqual(routing_input["longitude"].tolist(), expected_baseline["longitude"].tolist())

    def test_candidate_mapping_merge_preserves_one_to_one_baseline_cardinality(self) -> None:
        mapping_only_df = self.region_service_df[
            ["GSFS_RECEIPT_NO", "STRATEGIC_CITY_NAME", "region_id"]
        ].copy()
        mapping_only_df.loc[1, "region_id"] = "atlanta_ga_r02"
        with patch.object(
            routing_compare,
            "_build_integrated_routes",
            return_value=self.integrated_routes,
        ) as build_integrated:
            routing_compare.evaluate_region_plan(self._context(), mapping_only_df)

        routing_input = build_integrated.call_args.kwargs["region_service_df"]
        self.assertEqual(len(routing_input), 2)
        self.assertEqual(routing_input["GSFS_RECEIPT_NO"].nunique(), 2)
        self.assertEqual(
            dict(zip(routing_input["GSFS_RECEIPT_NO"], routing_input["region_id"])),
            {"A-1": "atlanta_ga_r01", "A-2": "atlanta_ga_r02"},
        )

    def test_region_sweep_imports_only_public_routing_compare_names(self) -> None:
        source = Path("smart_routing/region_sweep.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_names = [
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "routing_compare"
            for alias in node.names
        ]
        self.assertEqual(imported_names, ["evaluate_region_plan", "prepare_region_plan_evaluation"])
        self.assertTrue(all(not name.startswith("_") for name in imported_names))


class KoreanCompatibilityRegressionTest(unittest.TestCase):
    def _weekend_payload(self, *, skills: list[dict], job_count: int = 1) -> dict:
        return {
            "request_id": "weekend-regression",
            "planning_date": "2026-05-22",
            "city": "Korea",
            "technicians": [
                {
                    "employee_code": "T1",
                    "employee_name": "Tech 1",
                    "start_location": {"lat": 37.5, "lng": 127.0},
                    "max_jobs": 1,
                    "skills": skills,
                }
            ],
            "jobs": [
                {
                    "salesforce_id": f"J{idx}",
                    "receipt_no": f"J{idx}",
                    "product": "P1",
                    "location": {"lat": 37.5 + idx / 1000, "lng": 127.0 + idx / 1000},
                }
                for idx in range(1, job_count + 1)
            ],
        }

    def test_functional_korean_values_match_head_contract(self) -> None:
        self.assertEqual(production_atlanta.HEAVY_REPAIR_SHEET, "3depth 기준 중수리 증상")
        empty = pd.DataFrame()
        row = export_daily_stats._build_city_overall_row(empty, empty, empty, "기존지역", None)
        self.assertEqual(row["region_count"], "기존")

        export_source = Path("smart_routing/export_daily_stats.py").read_text(encoding="utf-8")
        area_map_source = Path("smart_routing/area_map.py").read_text(encoding="utf-8")
        self.assertIn("날짜별 최대 총업무 SM", export_source)
        self.assertIn('summary_sheet = f"{city_name[:10]}_전체통계"', export_source)
        self.assertIn("LA의 특수 Area View는 일반 region count와 별도의 내부 식별값을 사용한다.", area_map_source)

    def test_weekend_mode_accepts_korean_technician_aliases(self) -> None:
        request_payload = {
            "request_id": "alias-regression",
            "planning_date": "2026-03-01",
            "technicians": [
                {
                    "사번": "1001",
                    "위도": 37.5,
                    "경도": 127.0,
                    "max_jobs": 1,
                }
            ],
            "jobs": [],
        }
        with patch.object(vrp_mode_z_weekend, "_solve_jobs", return_value=([], [], [])) as solve_jobs:
            vrp_mode_z_weekend.run_mode(request_payload)

        tech_state = solve_jobs.call_args.kwargs["tech_states"][0]
        self.assertEqual(tech_state["employee_code"], "1001")
        self.assertEqual(tech_state["start_coord"], (37.5, 127.0))

    @patch.object(vrp_mode_z_weekend, "_osrm_route_distance_km", return_value=1.0)
    def test_weekend_capacity_shortage_assigns_feasible_subset(self, _route_distance) -> None:
        result = vrp_mode_z_weekend.run_mode(
            self._weekend_payload(skills=[{"product": "P1"}], job_count=2)
        )

        self.assertEqual(result["summary"], {"total_jobs": 2, "assigned_jobs": 1, "unassigned_jobs": 1})
        self.assertEqual(len(result["assignments"]), 1)
        self.assertEqual(len(result["unassigned"]), 1)

    @patch.object(vrp_mode_z_weekend, "_osrm_route_distance_km", return_value=1.0)
    def test_weekend_empty_skills_cannot_receive_product(self, _route_distance) -> None:
        result = vrp_mode_z_weekend.run_mode(self._weekend_payload(skills=[], job_count=1))

        self.assertEqual(result["summary"], {"total_jobs": 1, "assigned_jobs": 0, "unassigned_jobs": 1})
        self.assertEqual(result["unassigned"][0]["reason"], "NO_ELIGIBLE_TECHNICIAN")


if __name__ == "__main__":
    unittest.main()
