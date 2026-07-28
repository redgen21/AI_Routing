import unittest
from types import SimpleNamespace

import pandas as pd

from smart_routing.production_assign_atlanta_vrp import (
    VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST,
    _preferred_region_mismatch_penalty_cost,
    _solve_vrp_day,
)
from smart_routing.common_vrp_runtime import _apply_active_region_plan
from smart_routing.vrp_mode_na_general import (
    ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1,
    ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
    LA_6AREA_CITY,
    _build_engineer_frames_from_payload,
    _build_service_frame_from_payload,
    resolve_city_routing_policy,
)


class _UnitMatrixClient:
    cfg = SimpleNamespace(osrm_url="", fallback_osrm_url="")

    def get_distance_duration_matrix(self, coords):
        size = len(coords)
        matrix = [[0.0 if row == col else 1.0 for col in range(size)] for row in range(size)]
        return matrix, matrix


class _AffinityMatrixClient:
    cfg = SimpleNamespace(osrm_url="", fallback_osrm_url="")

    def get_distance_duration_matrix(self, coords):
        size = len(coords)
        matrix = [[0.0 if row == col else 1.0 for col in range(size)] for row in range(size)]
        # With two technicians and one job, a Region 1 technician has a
        # 1.4-minute first leg while Region 3 has a 1-minute first leg.
        if size >= 3:
            matrix[0][2] = matrix[2][0] = 1.4
            matrix[1][2] = matrix[2][1] = 1.0
        return matrix, matrix


def _engineers() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "SVC_ENGINEER_CODE": "DMS_R1",
                "Name": "DMS Region 1",
                "SVC_CENTER_TYPE": "DMS",
                "assigned_region_name": "Region 1",
                "preferred_region_name": "Region 1",
                "latitude": 34.05,
                "longitude": -118.24,
                "max_slots": 8,
                "max_minutes": 540,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            },
            {
                "SVC_ENGINEER_CODE": "DMS_R3",
                "Name": "DMS Region 3",
                "SVC_CENTER_TYPE": "DMS",
                "assigned_region_name": "Region 3",
                "preferred_region_name": "Region 3",
                "latitude": 34.05,
                "longitude": -118.25,
                "max_slots": 8,
                "max_minutes": 540,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            },
            {
                "SVC_ENGINEER_CODE": "DMS2_R2",
                "Name": "DMS2 Region 2",
                "SVC_CENTER_TYPE": "DMS2",
                "assigned_region_name": "Region 2",
                "preferred_region_name": "Region 2",
                "latitude": 34.05,
                "longitude": -118.26,
                "max_slots": 8,
                "max_minutes": 540,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            },
        ]
    )


def _jobs(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "service_date_key": "2026-07-23",
        "longitude": -118.24,
        "latitude": 34.05,
        "service_time_min": 45,
        "job_slot_count": 1,
        "SERVICE_PRODUCT_GROUP_CODE": "HA",
        "is_heavy_repair": False,
        "fixed": False,
        "reschedule": False,
        "current_employee_code": "",
        "enforce_area_type_center_match": True,
        "area_type_dms_fallback_allowed": True,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


class LA6SolverPolicyTests(unittest.TestCase):
    def test_runtime_soft_plan_overlay_preserves_region_preference_for_solver(self) -> None:
        payload = {
            "city": LA_6AREA_CITY,
            "planning_date": "2026-07-23",
            "technicians": [
                {"employee_code": "DMS_R1", "center_type": "DMS"},
                {"employee_code": "DMS_R3", "center_type": "DMS"},
                {"employee_code": "DMS2_R2", "center_type": "DMS2"},
            ],
            "jobs": [
                {
                    "receipt_no": "J1",
                    "postal_code": "90001",
                    "location": {"lat": 34.05, "lng": -118.24},
                }
            ],
        }
        snapshot = {
            "plan_id": "la6-active",
            "checksum": "test-checksum",
            "policy_version": ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
            "regions": [
                {"region_seq": 1, "region_name": "Region 1"},
                {"region_seq": 2, "region_name": "Region 2"},
            ],
            "postals": [
                {"postal_code": "90001", "region_seq": 1, "region_name": "Region 1", "area_type": "DMS"},
                {"postal_code": "90002", "region_seq": 2, "region_name": "Region 2", "area_type": "DMS2"},
            ],
            "technicians": [
                {"employee_code": "DMS_R1", "assigned_region_seq": 1, "assigned_region_name": "Region 1"},
                {"employee_code": "DMS_R3", "assigned_region_seq": 3, "assigned_region_name": "Region 3"},
                {"employee_code": "DMS2_R2", "assigned_region_seq": 2, "assigned_region_name": "Region 2"},
            ],
        }

        overlaid = _apply_active_region_plan(payload, snapshot)
        service = _build_service_frame_from_payload(overlaid, {})

        self.assertEqual("Region 1", overlaid["jobs"][0]["region_preference"]["region_name"])
        self.assertEqual("Region 1", service.iloc[0]["new_region_name"])
        self.assertEqual(0, _preferred_region_mismatch_penalty_cost("Region 1", service.iloc[0]["new_region_name"]))
        self.assertEqual(
            VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST,
            _preferred_region_mismatch_penalty_cost("Region 3", service.iloc[0]["new_region_name"]),
        )

    def test_la6_payload_uses_assigned_region_as_soft_preference(self) -> None:
        payload = {
            "city": LA_6AREA_CITY,
            "planning_date": "2026-07-23",
            "options": {
                "region_plan": {
                    "plan_id": "la6-active",
                    "policy_version": ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
                }
            },
            "technicians": [
                {
                    "employee_code": "DMS_R1",
                    "employee_name": "DMS Region 1",
                    "center_type": "DMS",
                    "assigned_region_name": "Region 1",
                    "preferred_region_name": "Stale legacy area",
                    "start_location": {"lat": 34.05, "lng": -118.24},
                }
            ],
            "jobs": [
                {
                    "receipt_no": "J1",
                    "location": {"lat": 34.05, "lng": -118.24},
                    "region_name": "Region 3",
                    "region_preference": {
                        "mode": "soft",
                        "region_seq": 3,
                        "region_name": "Region 3",
                    },
                    "region_seq": 3,
                    "area_type": "DMS",
                }
            ],
        }

        self.assertEqual(
            ACTIVE_ROSTER_AREA_TYPE_FALLBACK_REGION_SOFT_V1,
            resolve_city_routing_policy(payload),
        )
        engineers, _ = _build_engineer_frames_from_payload(
            payload,
            pd.DataFrame(columns=["SVC_ENGINEER_CODE"]),
            pd.DataFrame(columns=["SVC_ENGINEER_CODE"]),
            {1: (-118.24, 34.05)},
        )
        service = _build_service_frame_from_payload(payload, {})

        self.assertEqual("Region 1", engineers.iloc[0]["preferred_region_name"])
        self.assertEqual("Region 3", service.iloc[0]["new_region_name"])
        self.assertTrue(service.iloc[0]["enforce_area_type_center_match"])
        self.assertEqual(
            0,
            _preferred_region_mismatch_penalty_cost("Region 3", service.iloc[0]["new_region_name"]),
        )
        self.assertEqual(
            VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST,
            _preferred_region_mismatch_penalty_cost("Region 1", service.iloc[0]["new_region_name"]),
        )

    def test_dms_prefers_dms_and_dms2_remains_dms2_only(self) -> None:
        assignments, _, _ = _solve_vrp_day(
            _jobs(
                [
                    {
                        "GSFS_RECEIPT_NO": "DMS_JOB",
                        "new_region_name": "Region 1",
                        "area_type": "DMS",
                        "eligible_employee_codes": ["DMS_R1", "DMS_R3", "DMS2_R2"],
                    },
                    {
                        "GSFS_RECEIPT_NO": "DMS2_JOB",
                        "new_region_name": "Region 2",
                        "area_type": "DMS2",
                        "eligible_employee_codes": ["DMS_R1", "DMS_R3", "DMS2_R2"],
                    },
                ]
            ),
            _engineers(),
            _UnitMatrixClient(),
            {1: (-118.24, 34.05), 2: (-118.26, 34.05), 3: (-118.25, 34.05)},
            time_limit_seconds=1,
        )

        assigned = assignments.set_index("GSFS_RECEIPT_NO")["assigned_sm_code"].to_dict()
        self.assertIn(assigned["DMS_JOB"], {"DMS_R1", "DMS_R3"})
        self.assertEqual("DMS2_R2", assigned["DMS2_JOB"])

    def test_dms2_covers_dms_only_when_dms_capacity_is_unavailable(self) -> None:
        engineers = _engineers().copy()
        engineers.loc[engineers["SVC_CENTER_TYPE"] == "DMS", "max_slots"] = 0

        assignments, _, _ = _solve_vrp_day(
            _jobs(
                [
                    {
                        "GSFS_RECEIPT_NO": "DMS_FALLBACK",
                        "new_region_name": "Region 1",
                        "area_type": "DMS",
                        "eligible_employee_codes": ["DMS_R1", "DMS_R3", "DMS2_R2"],
                    }
                ]
            ),
            engineers,
            _UnitMatrixClient(),
            {1: (-118.24, 34.05), 2: (-118.26, 34.05), 3: (-118.25, 34.05)},
            time_limit_seconds=1,
        )

        self.assertEqual("DMS2_R2", assignments.iloc[0]["assigned_sm_code"])

    def test_legacy_type_hard_policy_does_not_enable_dms2_dms_fallback(self) -> None:
        payload = {
            "city": LA_6AREA_CITY,
            "planning_date": "2026-07-23",
            "options": {"region_plan": {"policy_version": ACTIVE_ROSTER_TYPE_HARD_REGION_SOFT_V1}},
            "jobs": [{
                "receipt_no": "J1",
                "location": {"lat": 34.05, "lng": -118.24},
                "region_name": "Region 1",
                "area_type": "DMS",
            }],
        }

        service = _build_service_frame_from_payload(payload, {})

        self.assertTrue(service.iloc[0]["enforce_area_type_center_match"])
        self.assertFalse(service.iloc[0]["area_type_dms_fallback_allowed"])

    def test_same_type_cross_region_is_feasible_but_equal_cost_prefers_assigned_region(self) -> None:
        assignments, _, _ = _solve_vrp_day(
            _jobs(
                [
                    {
                        "GSFS_RECEIPT_NO": "OWN_REGION",
                        "new_region_name": "Region 1",
                        "area_type": "DMS",
                        "eligible_employee_codes": ["DMS_R1", "DMS_R3"],
                    },
                    {
                        "GSFS_RECEIPT_NO": "CROSS_REGION",
                        "new_region_name": "Region 6",
                        "area_type": "DMS",
                        "eligible_employee_codes": ["DMS_R3"],
                    },
                ]
            ),
            _engineers(),
            _UnitMatrixClient(),
            {1: (-118.24, 34.05), 2: (-118.26, 34.05), 3: (-118.25, 34.05)},
            time_limit_seconds=1,
        )

        assigned = assignments.set_index("GSFS_RECEIPT_NO")["assigned_sm_code"].to_dict()
        self.assertEqual("DMS_R1", assigned["OWN_REGION"])
        self.assertEqual("DMS_R3", assigned["CROSS_REGION"])
        self.assertEqual(4_500, VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST)

    def test_4500_affinity_penalty_beats_a_0_4_minute_travel_advantage(self) -> None:
        jobs = _jobs(
            [
                {
                    "GSFS_RECEIPT_NO": "REGION_1_JOB",
                    "new_region_name": "Region 1",
                    "area_type": "DMS",
                    "eligible_employee_codes": ["DMS_R1", "DMS_R3"],
                }
            ]
        )
        engineers = _engineers().iloc[:2].copy()
        baseline_engineers = engineers.copy()
        baseline_engineers["preferred_region_name"] = ""

        baseline, _, _ = _solve_vrp_day(
            jobs, baseline_engineers, _AffinityMatrixClient(), {1: (-118.24, 34.05)}, time_limit_seconds=1
        )
        preferred, _, _ = _solve_vrp_day(
            jobs, engineers, _AffinityMatrixClient(), {1: (-118.24, 34.05)}, time_limit_seconds=1
        )

        self.assertEqual("DMS_R3", baseline.iloc[0]["assigned_sm_code"])
        self.assertEqual("DMS_R1", preferred.iloc[0]["assigned_sm_code"])
        self.assertGreater(
            VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST,
            int(round((1.4 - 1.0) * 100 * 100)),
        )


if __name__ == "__main__":
    unittest.main()
