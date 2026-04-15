from __future__ import annotations

import inspect
import math
import sys

import pandas as pd

import smart_routing.production_assign_atlanta as base
from smart_routing.production_assign_atlanta_osrm import (
    _calculate_route_distance_km,
    _calculate_total_assignment_cost,
    _matrix_grow_assign_jobs,
    _matrix_seed_assign_jobs,
    _savings_algorithm_assign,
    build_atlanta_production_assignment_osrm_from_frames,
)


class FakeRouteClient:
    def pair_distance(self, a: tuple[float, float], b: tuple[float, float]) -> tuple[float, float]:
        ax, ay = a
        bx, by = b
        distance = math.hypot(ax - bx, ay - by)
        return float(distance), 0.0

    def get_distance_duration_matrix(self, coords: list[tuple[float, float]]) -> tuple[list[list[float]], list[list[float]]]:
        distance_mat: list[list[float]] = []
        duration_mat: list[list[float]] = []
        for origin in coords:
            distance_row: list[float] = []
            duration_row: list[float] = []
            for destination in coords:
                distance, duration = self.pair_distance(origin, destination)
                distance_row.append(distance)
                duration_row.append(duration)
            distance_mat.append(distance_row)
            duration_mat.append(duration_row)
        return distance_mat, duration_mat

    def build_ordered_route(self, coords: list[tuple[float, float]], preserve_first: bool = True) -> dict[str, object]:
        distance = 0.0
        for idx in range(len(coords) - 1):
            leg_distance, _ = self.pair_distance(coords[idx], coords[idx + 1])
            distance += leg_distance
        return {
            "distance_km": float(distance),
            "duration_min": 0.0,
            "ordered_coords": coords,
            "geometry": [],
        }


def _print_result(name: str, ok: bool, detail: str) -> bool:
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}")
    print(f"  {detail}")
    return ok


def test_imports() -> tuple[bool, str]:
    return True, "Core VRP-level modules imported successfully."


def test_distance_and_cost_signatures() -> tuple[bool, str]:
    distance_params = list(inspect.signature(_calculate_route_distance_km).parameters.keys())
    cost_params = list(inspect.signature(_calculate_total_assignment_cost).parameters.keys())
    if distance_params != ["jobs_df", "start_coord", "route_client"]:
        return False, f"Unexpected route-distance signature: {distance_params}"
    if cost_params != [
        "assignment_df",
        "engineer_master_df",
        "route_client",
        "region_centers",
        "weight_distance",
        "weight_balance",
    ]:
        return False, f"Unexpected assignment-cost signature: {cost_params}"
    return True, "Route-distance and assignment-cost signatures are stable."


def test_savings_prefers_lower_pair_cost_engineer() -> tuple[bool, str]:
    route_client = FakeRouteClient()
    service_day_df = pd.DataFrame(
        [
            {
                "GSFS_RECEIPT_NO": "J1",
                "longitude": 0.0,
                "latitude": 0.0,
                "service_time_min": 45,
                "is_tv_job": False,
                "is_heavy_repair": False,
                "SERVICE_PRODUCT_GROUP_CODE": "ETC",
            },
            {
                "GSFS_RECEIPT_NO": "J2",
                "longitude": 0.1,
                "latitude": 0.0,
                "service_time_min": 45,
                "is_tv_job": False,
                "is_heavy_repair": False,
                "SERVICE_PRODUCT_GROUP_CODE": "ETC",
            },
        ]
    )
    engineer_master_df = pd.DataFrame(
        [
            {
                "SVC_ENGINEER_CODE": "NEAR",
                "Name": "Near Engineer",
                "SVC_CENTER_TYPE": base.DMS_CENTER_TYPE,
                "longitude": 0.0,
                "latitude": 0.0,
            },
            {
                "SVC_ENGINEER_CODE": "FAR",
                "Name": "Far Engineer",
                "SVC_CENTER_TYPE": base.DMS_CENTER_TYPE,
                "longitude": 100.0,
                "latitude": 100.0,
            },
        ]
    )

    result = _savings_algorithm_assign(service_day_df, engineer_master_df, route_client, {})
    assigned_codes = result["assigned_sm_code"].astype(str).tolist()
    ok = assigned_codes == ["NEAR", "NEAR"]
    return ok, f"Assigned codes: {assigned_codes}"


def test_savings_respects_mixed_feasibility() -> tuple[bool, str]:
    route_client = FakeRouteClient()
    service_day_df = pd.DataFrame(
        [
            {
                "GSFS_RECEIPT_NO": "N1",
                "longitude": 0.0,
                "latitude": 0.0,
                "service_time_min": 45,
                "is_tv_job": False,
                "is_heavy_repair": False,
                "SERVICE_PRODUCT_GROUP_CODE": "ETC",
            },
            {
                "GSFS_RECEIPT_NO": "T1",
                "longitude": 10.0,
                "latitude": 0.0,
                "service_time_min": 45,
                "is_tv_job": True,
                "is_heavy_repair": False,
                "SERVICE_PRODUCT_GROUP_CODE": base.TV_PRODUCT_GROUP,
            },
        ]
    )
    engineer_master_df = pd.DataFrame(
        [
            {
                "SVC_ENGINEER_CODE": "DMS_NEAR",
                "Name": "DMS Near",
                "SVC_CENTER_TYPE": base.DMS_CENTER_TYPE,
                "longitude": 0.0,
                "latitude": 0.0,
            },
            {
                "SVC_ENGINEER_CODE": "DMS2_NEAR",
                "Name": "DMS2 Near",
                "SVC_CENTER_TYPE": base.DMS2_CENTER_TYPE,
                "longitude": 10.0,
                "latitude": 0.0,
            },
        ]
    )

    result = _savings_algorithm_assign(service_day_df, engineer_master_df, route_client, {})
    assigned = result.set_index("GSFS_RECEIPT_NO")["assigned_sm_code"].astype(str).to_dict()
    ok = assigned == {"N1": "DMS_NEAR", "T1": "DMS2_NEAR"}
    return ok, f"Assignments: {assigned}"


def test_matrix_assigners_respect_candidate_engineers() -> tuple[bool, str]:
    route_client = FakeRouteClient()
    remaining_df = pd.DataFrame(
        [
            {
                "GSFS_RECEIPT_NO": "TV1",
                "longitude": 0.0,
                "latitude": 0.0,
                "service_time_min": 45,
                "is_tv_job": True,
                "is_heavy_repair": False,
                "SERVICE_PRODUCT_GROUP_CODE": base.TV_PRODUCT_GROUP,
                "region_seq": 1,
                "service_date_key": "2026-01-01",
            }
        ]
    )
    engineer_master_df = pd.DataFrame(
        [
            {
                "SVC_ENGINEER_CODE": "DMS_CLOSE",
                "Name": "DMS Close",
                "SVC_CENTER_TYPE": base.DMS_CENTER_TYPE,
                "longitude": 0.0,
                "latitude": 0.0,
                "assigned_region_seq": 1,
                "anchor_region_seq": 1,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            },
            {
                "SVC_ENGINEER_CODE": "DMS2_FAR",
                "Name": "DMS2 Far",
                "SVC_CENTER_TYPE": base.DMS2_CENTER_TYPE,
                "longitude": 5.0,
                "latitude": 0.0,
                "assigned_region_seq": 1,
                "anchor_region_seq": 1,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            },
        ]
    )
    start_lookup = {
        "DMS_CLOSE": (0.0, 0.0),
        "DMS2_FAR": (5.0, 0.0),
    }
    states = {
        "DMS_CLOSE": {
            "engineer_code": "DMS_CLOSE",
            "engineer_name": "DMS Close",
            "center_type": base.DMS_CENTER_TYPE,
            "assigned_region_seq": 1,
            "anchor_region_seq": 1,
            "current_coord": (0.0, 0.0),
            "service_time_min": 0.0,
            "travel_time_min": 0.0,
            "travel_distance_km": 0.0,
            "job_count": 0,
            "assigned_rows": [],
            "start_coord": (0.0, 0.0),
        },
        "DMS2_FAR": {
            "engineer_code": "DMS2_FAR",
            "engineer_name": "DMS2 Far",
            "center_type": base.DMS2_CENTER_TYPE,
            "assigned_region_seq": 1,
            "anchor_region_seq": 1,
            "current_coord": (5.0, 0.0),
            "service_time_min": 0.0,
            "travel_time_min": 0.0,
            "travel_distance_km": 0.0,
            "job_count": 0,
            "assigned_rows": [],
            "start_coord": (5.0, 0.0),
        },
    }

    _, seeded = _matrix_seed_assign_jobs(
        remaining_df,
        engineer_master_df,
        states,
        start_lookup,
        route_client,
        None,
    )
    if [row["assigned_sm_code"] for row in seeded] != ["DMS2_FAR"]:
        return False, f"Seed assigned {seeded}"

    states = {
        code: {
            **state,
            "service_time_min": 0.0,
            "travel_time_min": 0.0,
            "travel_distance_km": 0.0,
            "job_count": 0,
            "assigned_rows": [],
        }
        for code, state in states.items()
    }
    _, grown = _matrix_grow_assign_jobs(
        remaining_df,
        engineer_master_df,
        states,
        route_client,
        {"DMS_CLOSE": 4, "DMS2_FAR": 4},
    )
    assigned_codes = [row["assigned_sm_code"] for row in grown]
    ok = assigned_codes == ["DMS2_FAR"]
    return ok, f"Seed and grow assignments: seed={seeded}, grow={grown}"


def test_assign_day_falls_back_to_active_day_engineers() -> tuple[bool, str]:
    route_client = FakeRouteClient()
    service_day_df = pd.DataFrame(
        [
            {
                "GSFS_RECEIPT_NO": "R1",
                "longitude": 1.0,
                "latitude": 0.0,
                "service_time_min": 45,
                "is_tv_job": False,
                "is_heavy_repair": False,
                "SERVICE_PRODUCT_GROUP_CODE": "WM",
                "region_seq": 2,
                "service_date_key": "2026-01-03",
            }
        ]
    )
    engineer_master_df = pd.DataFrame(
        [
            {
                "SVC_ENGINEER_CODE": "OFF_REGION",
                "Name": "Off Region",
                "SVC_CENTER_TYPE": base.DMS_CENTER_TYPE,
                "assigned_region_seq": 1,
                "anchor_region_seq": 1,
                "zip_overlap_count": 0,
                "zip_overlap_ratio": 0.0,
                "REF_HEAVY_REPAIR_FLAG": "Y",
                "longitude": 0.0,
                "latitude": 0.0,
            }
        ]
    )
    assignment_df, _ = base._assign_day(
        service_day_df,
        engineer_master_df,
        {1: (0.0, 0.0), 2: (1.0, 0.0)},
        route_client,
        {},
    )
    assigned = assignment_df["assigned_sm_code"].astype(str).tolist() if not assignment_df.empty else []
    ok = assigned == ["OFF_REGION"]
    return ok, f"Assignments: {assigned}"


def test_vrp_level_pipeline_shape() -> tuple[bool, str]:
    source = inspect.getsource(build_atlanta_production_assignment_osrm_from_frames)
    has_assign_day = "assignment_df, _ = base._assign_day(" in source
    has_travel_pass = 'priority_mode="travel_first"' in source
    has_balance_pass = 'priority_mode="balance_first"' in source
    mentions_vrp_level = 'assignment_strategy == "vrp_level"' in source
    ok = has_assign_day and has_travel_pass and has_balance_pass and mentions_vrp_level
    detail = (
        f"vrp_level branch uses cluster seed={has_assign_day}, "
        f"travel pass={has_travel_pass}, balance pass={has_balance_pass}"
    )
    return ok, detail


def main() -> bool:
    print("=" * 72)
    print("VRP-Level Unit Tests")
    print("=" * 72)

    tests = [
        ("imports", test_imports),
        ("signatures", test_distance_and_cost_signatures),
        ("savings_prefers_near_engineer", test_savings_prefers_lower_pair_cost_engineer),
        ("savings_respects_feasibility", test_savings_respects_mixed_feasibility),
        ("matrix_assigners_respect_feasibility", test_matrix_assigners_respect_candidate_engineers),
        ("assign_day_global_fallback", test_assign_day_falls_back_to_active_day_engineers),
        ("vrp_level_pipeline_shape", test_vrp_level_pipeline_shape),
    ]

    passed = 0
    for name, test_func in tests:
        ok, detail = test_func()
        if _print_result(name, ok, detail):
            passed += 1

    total = len(tests)
    print("=" * 72)
    print(f"Total: {passed}/{total} tests passed")
    print("=" * 72)
    return passed == total


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
