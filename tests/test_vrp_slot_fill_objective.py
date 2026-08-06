"""Slot-fill objective regression tests (open-issues doc sections 1, 2, 4).

These tests pin the primary-solver behavior that maximizes assigned work:

1. Slot repacking: one-slot jobs must not fragment technician capacity in a
   way that leaves a two-slot job unassigned when a packing exists.
2. Lexicographic drop preference: when a drop is unavoidable, the solver
   keeps the larger-slot job (assigned slots are maximized after the
   unassigned-job count).
3. Unassigned candidate diagnosis: jobs left unassigned must carry
   per-candidate rejection reasons instead of a bare NO_FEASIBLE_ROUTE.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace

import pandas as pd

from smart_routing.production_assign_atlanta_vrp import (
    VRP_ADAPTIVE_CAPACITY_HIGH_THRESHOLD,
    VRP_ADAPTIVE_CAPACITY_LOW_THRESHOLD,
    VRP_ADAPTIVE_FIXED_LOAD_MULTIPLIER,
    VRP_ADAPTIVE_TARGET_MULTIPLIER_HIGH,
    _select_adaptive_objective_policy,
    _solve_vrp_day,
)


class _UnitMatrixClient:
    cfg = SimpleNamespace(osrm_url="", fallback_osrm_url="")

    def get_distance_duration_matrix(self, coords):
        size = len(coords)
        matrix = [
            [0.0 if row == col else 1.0 for col in range(size)]
            for row in range(size)
        ]
        return matrix, matrix


def _engineers(max_slots: int) -> pd.DataFrame:
    rows = []
    for idx, (code, lon) in enumerate([("T1", -84.39), ("T2", -84.41)]):
        rows.append(
            {
                "SVC_ENGINEER_CODE": code,
                "Name": f"Tech {idx + 1}",
                "SVC_CENTER_TYPE": "DMS",
                "latitude": 33.75,
                "longitude": lon,
                "max_slots": max_slots,
                "max_minutes": 540,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            }
        )
    return pd.DataFrame(rows)


def _job(receipt: str, slots: int, lon: float, lat: float, eligible: list[str]) -> dict:
    return {
        "GSFS_RECEIPT_NO": receipt,
        "service_date_key": "2026-07-30",
        "longitude": lon,
        "latitude": lat,
        "service_time_min": 45,
        "job_slot_count": slots,
        "SERVICE_PRODUCT_GROUP_CODE": "HA",
        "is_heavy_repair": False,
        "new_region_name": "Zone 2",
        "fixed": False,
        "reschedule": False,
        "current_employee_code": "",
        "eligible_employee_codes": eligible,
        "boundary_overflow_employee_codes": [],
    }


class SlotFillObjectiveTests(unittest.TestCase):
    def test_one_slot_jobs_repack_so_two_slot_job_is_assigned(self) -> None:
        """Two techs x 2 slots; 1+1+2 slots requested.

        A fragmented split (one 1-slot job per technician) leaves no room for
        the 2-slot job.  The primary solver must pack both 1-slot jobs on one
        technician so every job is assigned.
        """
        jobs = pd.DataFrame(
            [
                _job("J-1A", 1, -84.380, 33.760, ["T1", "T2"]),
                _job("J-1B", 1, -84.382, 33.762, ["T1", "T2"]),
                _job("J-2A", 2, -84.384, 33.764, ["T1", "T2"]),
            ]
        )
        client = _UnitMatrixClient()
        assignment_df, _, _ = _solve_vrp_day(
            jobs,
            _engineers(max_slots=2),
            client,
            {},
            time_limit_seconds=2,
        )
        assigned = set(assignment_df["GSFS_RECEIPT_NO"].astype(str))
        self.assertEqual(assigned, {"J-1A", "J-1B", "J-2A"})
        two_slot_tech = assignment_df.loc[
            assignment_df["GSFS_RECEIPT_NO"].eq("J-2A"), "assigned_sm_code"
        ].iloc[0]
        one_slot_techs = set(
            assignment_df.loc[
                assignment_df["GSFS_RECEIPT_NO"].isin(["J-1A", "J-1B"]),
                "assigned_sm_code",
            ]
        )
        self.assertEqual(len(one_slot_techs), 1)
        self.assertNotIn(two_slot_tech, one_slot_techs)

    def test_unavoidable_drop_keeps_the_larger_slot_job(self) -> None:
        """One tech x 2 slots; a 2-slot and a 1-slot job compete.

        The unassigned-job count is one either way, so the lexicographic
        objective must keep the 2-slot job and drop the 1-slot job.
        """
        jobs = pd.DataFrame(
            [
                _job("J-2SLOT", 2, -84.380, 33.760, ["T1"]),
                _job("J-1SLOT", 1, -84.382, 33.762, ["T1"]),
            ]
        )
        engineers = _engineers(max_slots=2).iloc[[0]].reset_index(drop=True)
        client = _UnitMatrixClient()
        assignment_df, _, _ = _solve_vrp_day(
            jobs,
            engineers,
            client,
            {},
            time_limit_seconds=2,
        )
        assigned = set(assignment_df["GSFS_RECEIPT_NO"].astype(str))
        self.assertIn("J-2SLOT", assigned)
        self.assertNotIn("J-1SLOT", assigned)

        analysis = getattr(client, "_vrp_unassigned_candidate_analysis", None)
        self.assertIsInstance(analysis, list)
        entries = {entry["receipt_no"]: entry for entry in analysis}
        self.assertIn("J-1SLOT", entries)
        candidates = entries["J-1SLOT"]["candidates"]
        self.assertTrue(candidates)
        t1_record = next(
            record for record in candidates if record["technician_code"] == "T1"
        )
        self.assertEqual(t1_record["rejection_reason"], "SLOT_CAPACITY_EXCEEDED")
        self.assertEqual(t1_record["current_slots"], 2)
        self.assertEqual(t1_record["max_slots"], 2)
        self.assertEqual(t1_record["remaining_slots"], 0)


class FixedVehicleWorkCapTests(unittest.TestCase):
    def test_fixed_vehicle_general_job_respects_600_minute_cap(self) -> None:
        """A fixed job does not lift the vehicle's normal work-time cap.

        The fixed job (400 min) must be assigned to its fixed technician, but
        the optional job (250 min) would push the route past the 600-minute
        hard limit and must stay unassigned with a WORK_LIMIT_EXCEEDED
        candidate diagnosis.  The legacy behavior gave fixed-job vehicles an
        unrestricted 1440-minute limit and would have assigned both.
        """
        jobs = pd.DataFrame(
            [
                {
                    **_job("J-FIXED", 1, -84.380, 33.760, ["T1"]),
                    "fixed": True,
                    "current_employee_code": "T1",
                    "service_time_min": 400,
                },
                {
                    **_job("J-OPTIONAL", 1, -84.382, 33.762, ["T1"]),
                    "service_time_min": 250,
                },
            ]
        )
        engineers = _engineers(max_slots=8).iloc[[0]].reset_index(drop=True)
        client = _UnitMatrixClient()
        assignment_df, _, _ = _solve_vrp_day(
            jobs,
            engineers,
            client,
            {},
            time_limit_seconds=2,
        )
        assigned = set(assignment_df["GSFS_RECEIPT_NO"].astype(str))
        self.assertIn("J-FIXED", assigned)
        self.assertNotIn("J-OPTIONAL", assigned)
        self.assertEqual(
            assignment_df.loc[
                assignment_df["GSFS_RECEIPT_NO"].eq("J-FIXED"), "assigned_sm_code"
            ].iloc[0],
            "T1",
        )
        analysis = getattr(client, "_vrp_unassigned_candidate_analysis", [])
        entries = {entry["receipt_no"]: entry for entry in analysis}
        self.assertIn("J-OPTIONAL", entries)
        t1_record = next(
            record
            for record in entries["J-OPTIONAL"]["candidates"]
            if record["technician_code"] == "T1"
        )
        self.assertEqual(t1_record["rejection_reason"], "WORK_LIMIT_EXCEEDED")
        self.assertGreater(t1_record["candidate_total_work_min"], 600)


class AdaptiveObjectivePolicyTests(unittest.TestCase):
    def _policy(self, **overrides):
        params = {
            "total_slot_count": 50,
            "total_capacity": 100,
            "group_sizes": [],
            "fixed_slots_by_vehicle": {},
            "max_jobs_by_vehicle": [8, 8],
        }
        params.update(overrides)
        return _select_adaptive_objective_policy(**params)

    def test_large_co_location_group_selects_co_location_first(self) -> None:
        policy = self._policy(group_sizes=[8], total_slot_count=90, total_capacity=100)
        self.assertEqual(policy["mode"], "co_location_first")
        self.assertFalse(policy["priority_load_objective_enabled"])
        self.assertEqual(policy["target_penalty_multiplier"], 0.0)

    def test_low_utilization_selects_capacity_surplus(self) -> None:
        utilization = VRP_ADAPTIVE_CAPACITY_LOW_THRESHOLD - 0.10
        policy = self._policy(total_slot_count=int(utilization * 100))
        self.assertEqual(policy["mode"], "capacity_surplus")
        self.assertFalse(policy["priority_load_objective_enabled"])

    def test_mid_utilization_selects_balanced_load(self) -> None:
        utilization = (
            VRP_ADAPTIVE_CAPACITY_LOW_THRESHOLD + VRP_ADAPTIVE_CAPACITY_HIGH_THRESHOLD
        ) / 2
        policy = self._policy(total_slot_count=int(utilization * 100))
        self.assertEqual(policy["mode"], "balanced_load")
        self.assertTrue(policy["priority_load_objective_enabled"])

    def test_high_utilization_selects_capacity_tight(self) -> None:
        utilization = VRP_ADAPTIVE_CAPACITY_HIGH_THRESHOLD + 0.05
        policy = self._policy(total_slot_count=int(utilization * 100))
        self.assertEqual(policy["mode"], "capacity_tight")
        self.assertTrue(policy["priority_load_objective_enabled"])

    def test_heavy_fixed_load_raises_target_multiplier(self) -> None:
        utilization = VRP_ADAPTIVE_CAPACITY_HIGH_THRESHOLD + 0.05
        policy = self._policy(
            total_slot_count=int(utilization * 100),
            fixed_slots_by_vehicle={0: 6},
            max_jobs_by_vehicle=[8, 8],
        )
        expected = round(
            VRP_ADAPTIVE_TARGET_MULTIPLIER_HIGH * VRP_ADAPTIVE_FIXED_LOAD_MULTIPLIER, 4
        )
        self.assertEqual(policy["target_penalty_multiplier"], expected)


if __name__ == "__main__":
    unittest.main()
