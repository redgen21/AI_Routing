import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from ortools.constraint_solver import pywrapcp

from smart_routing.production_assign_atlanta_vrp import (
    EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
    VRP_APPROVED_BOUNDARY_OVERFLOW_PENALTY_COST,
    _approved_boundary_overflow_employee_codes,
    _approved_boundary_overflow_penalty_cost,
    _fixed_technician_outside_active_plan,
    _hard_eligible_employee_codes,
    _solve_vrp_day,
)
from smart_routing.vrp_mode_na_general import (
    _build_response_payload,
    _build_routing_diagnostics,
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


class _AddOnLimitMatrixClient:
    cfg = SimpleNamespace(osrm_url="", fallback_osrm_url="")

    def get_distance_duration_matrix(self, coords):
        size = len(coords)
        # Every route leg is 70 minutes.  The first add-on remains under the
        # 600-minute hard limit; adding the second one after it does not.
        matrix = [
            [0.0 if row == col else 70.0 for col in range(size)]
            for row in range(size)
        ]
        return matrix, matrix


def _engineers() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "SVC_ENGINEER_CODE": "T1",
                "Name": "Tech 1",
                "SVC_CENTER_TYPE": "DMS",
                "latitude": 33.75,
                "longitude": -84.39,
                "max_slots": 8,
                "max_minutes": 540,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            }
        ]
    )


def _two_engineers() -> pd.DataFrame:
    second = _engineers().iloc[0].to_dict()
    second.update(
        {
            "SVC_ENGINEER_CODE": "T2",
            "Name": "Tech 2",
            "longitude": -84.40,
        }
    )
    return pd.concat([_engineers(), pd.DataFrame([second])], ignore_index=True)


def _job(**overrides) -> pd.DataFrame:
    row = {
        "GSFS_RECEIPT_NO": "J1",
        "service_date_key": "2026-07-21",
        "longitude": -84.38,
        "latitude": 33.76,
        "service_time_min": 45,
        "job_slot_count": 1,
        "SERVICE_PRODUCT_GROUP_CODE": "HA",
        "is_heavy_repair": False,
        "new_region_name": "Zone 2",
        "fixed": False,
        "reschedule": False,
        "current_employee_code": "",
        "eligible_employee_codes": ["T1"],
        "boundary_overflow_employee_codes": [],
        "region_policy": OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class Atlanta6SolverPolicyTests(unittest.TestCase):
    def test_boundary_candidates_are_intersected_with_hard_candidates(self) -> None:
        row = pd.Series(
            {
                "eligible_employee_codes": ["T1", "T2"],
                "boundary_overflow_employee_codes": ["T2", "T3"],
            }
        )
        self.assertEqual(_hard_eligible_employee_codes(row), {"T1", "T2"})
        self.assertEqual(_approved_boundary_overflow_employee_codes(row), {"T2"})

        empty_row = pd.Series(
            {
                "eligible_employee_codes": [],
                "boundary_overflow_employee_codes": ["T2"],
            }
        )
        self.assertEqual(_hard_eligible_employee_codes(empty_row), set())
        self.assertEqual(_approved_boundary_overflow_employee_codes(empty_row), set())

    def test_4500_penalty_applies_only_to_explicit_boundary_candidate(self) -> None:
        approved = {"T2"}
        for policy in (
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
            EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
        ):
            with self.subTest(policy=policy):
                self.assertEqual(
                    _approved_boundary_overflow_penalty_cost(policy, "T2", approved),
                    VRP_APPROVED_BOUNDARY_OVERFLOW_PENALTY_COST,
                )
        self.assertEqual(
            _approved_boundary_overflow_penalty_cost(
                OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1, "T1", approved
            ),
            0,
        )
        self.assertEqual(
            _approved_boundary_overflow_penalty_cost("preferred_region_soft", "T2", approved),
            0,
        )

    def test_region_plan_membership_fixed_outside_plan_semantics_match_legacy_v1(self) -> None:
        for policy in (
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
            EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
        ):
            with self.subTest(policy=policy):
                job = _job(
                    fixed=True,
                    current_employee_code="T1",
                    eligible_employee_codes=["T2"],
                    region_policy=policy,
                ).iloc[0]
                self.assertTrue(_fixed_technician_outside_active_plan(job, "T1"))

    def test_empty_hard_candidate_set_is_explicitly_unassigned(self) -> None:
        assignment_df, _, schedule_df = _solve_vrp_day(
            _job(eligible_employee_codes=[]),
            _engineers(),
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
        )
        self.assertTrue(assignment_df.empty)
        self.assertTrue(schedule_df.empty)

    def test_add_on_inserts_first_job_but_rejects_next_job_over_600_minutes(self) -> None:
        engineer = _engineers().copy()
        engineer.loc[0, "SVC_ENGINEER_CODE"] = "AI105115"
        engineer.loc[0, "Name"] = "Jason Patterson"
        engineer.loc[0, "SVC_CENTER_TYPE"] = "DMS2"
        jobs = pd.concat(
            [
                _job(
                    GSFS_RECEIPT_NO="RNN260725044485",
                    eligible_employee_codes=["AI105115"],
                ),
                _job(
                    GSFS_RECEIPT_NO="RNN260727054150",
                    eligible_employee_codes=["AI105115"],
                ),
                # These two jobs are made optional to the primary DMS2 route
                # so the test exercises the post-solver insertion pass.
                _job(
                    GSFS_RECEIPT_NO="RNN260725043216",
                    eligible_employee_codes=["AI105115"],
                    service_time_min=90,
                    area_type="DMS",
                    enforce_area_type_center_match=True,
                ),
                _job(
                    GSFS_RECEIPT_NO="RNN260725043692",
                    eligible_employee_codes=["AI105115"],
                    service_time_min=100,
                    area_type="DMS",
                    enforce_area_type_center_match=True,
                ),
            ],
            ignore_index=True,
        )
        assignment_df, _, _ = _solve_vrp_day(
            jobs,
            engineer,
            _AddOnLimitMatrixClient(),
            {},
            time_limit_seconds=1,
        )

        assigned_receipts = set(assignment_df["GSFS_RECEIPT_NO"].astype(str))
        self.assertIn("RNN260725043216", assigned_receipts)
        self.assertNotIn("RNN260725043692", assigned_receipts)
        self.assertEqual(
            assignment_df.loc[
                assignment_df["GSFS_RECEIPT_NO"].eq("RNN260725043216"),
                "assigned_sm_code",
            ].iloc[0],
            "AI105115",
        )

    def test_equal_travel_prefers_primary_candidate_over_penalized_overflow(self) -> None:
        assignment_df, _, _ = _solve_vrp_day(
            _job(
                eligible_employee_codes=["T1", "T2"],
                boundary_overflow_employee_codes=["T2"],
            ),
            _two_engineers(),
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
        )
        self.assertEqual(assignment_df.iloc[0]["assigned_sm_code"], "T1")

    def test_unavailable_fixed_technician_is_never_reassigned(self) -> None:
        assignment_df, _, schedule_df = _solve_vrp_day(
            _job(fixed=True, current_employee_code="MISSING", eligible_employee_codes=["T1"]),
            _engineers(),
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
        )
        self.assertTrue(assignment_df.empty)
        self.assertTrue(schedule_df.empty)

    def test_fixed_technician_outside_active_plan_releases_only_fixed_constraint(self) -> None:
        job_df = _job(
            fixed=True,
            current_employee_code="T1",
            eligible_employee_codes=["T2"],
        )
        self.assertTrue(_fixed_technician_outside_active_plan(job_df.iloc[0], "T1"))

        assignment_df, _, schedule_df = _solve_vrp_day(
            job_df,
            _two_engineers(),
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
        )
        self.assertEqual(assignment_df.iloc[0]["assigned_sm_code"], "T2")
        self.assertTrue(assignment_df.iloc[0]["fixed_technician_outside_active_plan_relaxed"])

        payload = {
            "request_id": "r1",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": "2026-07-21",
            "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}],
            "jobs": [{"receipt_no": "J1", "fixed": True, "current_employee_code": "T1", "eligible_employee_codes": ["T2"], "region_name": "Zone 2"}],
        }
        result = _build_response_payload(payload, pd.DataFrame(), schedule_df, diagnostics={})
        self.assertTrue(result["assignments"][0]["changed"])
        self.assertTrue(result["assignments"][0]["fixed_technician_outside_active_plan_relaxed"])
        self.assertTrue(result["diagnostics"]["relaxations_applied"]["fixed_technician_outside_active_plan_relaxed"])
        self.assertIn("Fixed technician outside active plan relaxed", result["diagnostics"]["routing_condition_messages"][0])

    def test_eligible_fixed_technician_remains_fixed(self) -> None:
        assignment_df, _, _ = _solve_vrp_day(
            _job(fixed=True, current_employee_code="T1", eligible_employee_codes=["T1", "T2"]),
            _two_engineers(),
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
        )
        self.assertEqual(assignment_df.iloc[0]["assigned_sm_code"], "T1")
        self.assertFalse(assignment_df.iloc[0]["fixed_technician_outside_active_plan_relaxed"])

    def test_released_fixed_jobs_use_reschedule_priority_and_may_drop(self) -> None:
        jobs = pd.concat(
            [
                _job(fixed=True, current_employee_code="T1", eligible_employee_codes=["T2"]),
                _job(
                    GSFS_RECEIPT_NO="J2",
                    fixed=True,
                    current_employee_code="T1",
                    eligible_employee_codes=["T2"],
                ),
            ],
            ignore_index=True,
        )
        engineers = _two_engineers()
        engineers.loc[engineers["SVC_ENGINEER_CODE"] == "T2", "max_slots"] = 1

        assignment_df, _, schedule_df = _solve_vrp_day(
            jobs,
            engineers,
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
        )

        self.assertEqual(len(assignment_df), 1)
        self.assertEqual(len(schedule_df), 1)
        self.assertEqual(assignment_df.iloc[0]["assigned_sm_code"], "T2")
        self.assertTrue(assignment_df.iloc[0]["fixed_technician_outside_active_plan_relaxed"])
        payload = {
            "request_id": "r1",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": "2026-07-21",
            "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}],
            "jobs": [
                {"receipt_no": receipt, "fixed": True, "current_employee_code": "T1", "eligible_employee_codes": ["T2"], "region_name": "Zone 2"}
                for receipt in ("J1", "J2")
            ],
        }
        result = _build_response_payload(payload, pd.DataFrame(), schedule_df, diagnostics={})
        self.assertEqual(len(result["assignments"]), 1)
        self.assertTrue(result["assignments"][0]["changed"])
        self.assertTrue(result["assignments"][0]["fixed_technician_outside_active_plan_relaxed"])
        self.assertEqual(
            [item["reason"] for item in result["unassigned"]],
            ["NO_FEASIBLE_MANDATORY_ROUTE"],
        )
        self.assertTrue(result["unassigned"][0]["fixed_technician_outside_active_plan_relaxed"])

    def test_released_fixed_priority_survives_reschedule_retry(self) -> None:
        jobs = pd.concat(
            [
                _job(
                    fixed=True,
                    current_employee_code="T1",
                    eligible_employee_codes=["T2"],
                    service_time_min=90,
                ),
                _job(
                    GSFS_RECEIPT_NO="J2",
                    eligible_employee_codes=["T2"],
                    service_time_min=45,
                ),
            ],
            ignore_index=True,
        )
        engineers = _two_engineers()
        engineers.loc[engineers["SVC_ENGINEER_CODE"] == "T2", "max_slots"] = 1

        optional_assignment_df, _, _ = _solve_vrp_day(
            jobs,
            engineers,
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
            respect_fixed_jobs=False,
            enforce_reschedule_jobs=False,
        )
        self.assertEqual(optional_assignment_df.iloc[0]["GSFS_RECEIPT_NO"], "J2")

        original_solve = pywrapcp.RoutingModel.SolveWithParameters
        solve_attempts = 0

        def force_first_attempt_retry(routing_model, search_parameters):
            nonlocal solve_attempts
            solve_attempts += 1
            if solve_attempts == 1:
                return None
            return original_solve(routing_model, search_parameters)

        with patch.object(
            pywrapcp.RoutingModel,
            "SolveWithParameters",
            new=force_first_attempt_retry,
        ):
            retry_assignment_df, _, retry_schedule_df = _solve_vrp_day(
                jobs,
                engineers,
                _UnitMatrixClient(),
                {2: (-84.38, 33.76)},
                time_limit_seconds=1,
            )

        self.assertGreaterEqual(solve_attempts, 2)
        self.assertEqual(len(retry_assignment_df), 1)
        self.assertEqual(len(retry_schedule_df), 1)
        self.assertEqual(retry_assignment_df.iloc[0]["GSFS_RECEIPT_NO"], "J1")
        self.assertEqual(retry_assignment_df.iloc[0]["assigned_sm_code"], "T2")
        self.assertTrue(
            retry_assignment_df.iloc[0]["fixed_technician_outside_active_plan_relaxed"]
        )
        self.assertTrue(retry_assignment_df.iloc[0]["reschedule_mandatory_relaxed"])

    def test_respect_fixed_jobs_false_does_not_report_release(self) -> None:
        assignment_df, _, schedule_df = _solve_vrp_day(
            _job(fixed=True, current_employee_code="T1", eligible_employee_codes=["T2"]),
            _two_engineers(),
            _UnitMatrixClient(),
            {2: (-84.38, 33.76)},
            time_limit_seconds=1,
            respect_fixed_jobs=False,
        )
        self.assertFalse(assignment_df.iloc[0]["fixed_technician_outside_active_plan_relaxed"])
        payload = {
            "request_id": "r1",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": "2026-07-21",
            "options": {"respect_fixed_jobs": False},
            "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}],
            "jobs": [{"receipt_no": "J1", "fixed": True, "current_employee_code": "T1", "eligible_employee_codes": ["T2"], "region_name": "Zone 2"}],
        }
        result = _build_response_payload(payload, pd.DataFrame(), schedule_df, diagnostics={})
        diagnostics = _build_routing_diagnostics(
            payload,
            pd.DataFrame([{"GSFS_RECEIPT_NO": "J1"}]),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        self.assertFalse(result["assignments"][0]["fixed_technician_outside_active_plan_relaxed"])
        self.assertFalse(result["diagnostics"]["relaxations_applied"]["fixed_technician_outside_active_plan_relaxed"])
        self.assertEqual(diagnostics["fixed_technician_outside_active_plan_relaxed_job_count"], 0)

    def test_respect_fixed_jobs_false_unassigned_is_not_mandatory(self) -> None:
        payload = {
            "request_id": "r1",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": "2026-07-21",
            "options": {"respect_fixed_jobs": False},
            "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}],
            "jobs": [{"receipt_no": "J1", "fixed": True, "current_employee_code": "T1", "eligible_employee_codes": ["T2"], "region_name": "Zone 2"}],
        }
        result = _build_response_payload(
            payload,
            pd.DataFrame(),
            pd.DataFrame(),
            diagnostics={"invalid_location_receipts": []},
        )
        self.assertEqual(result["unassigned"][0]["reason"], "NO_FEASIBLE_ROUTE")
        self.assertFalse(result["unassigned"][0]["fixed_technician_outside_active_plan_relaxed"])

    def test_invalid_location_does_not_report_fixed_release(self) -> None:
        payload = {
            "request_id": "r1",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": "2026-07-21",
            "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}],
            "jobs": [{"receipt_no": "J1", "fixed": True, "current_employee_code": "T1", "eligible_employee_codes": ["T2"], "region_name": "Zone 2"}],
        }
        result = _build_response_payload(
            payload,
            pd.DataFrame(),
            pd.DataFrame(),
            diagnostics={"invalid_location_receipts": ["J1"]},
        )
        diagnostics = _build_routing_diagnostics(
            payload,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        self.assertEqual(result["unassigned"][0]["reason"], "INVALID_LOCATION")
        self.assertFalse(result["unassigned"][0]["fixed_technician_outside_active_plan_relaxed"])
        self.assertFalse(result["diagnostics"]["relaxations_applied"]["fixed_technician_outside_active_plan_relaxed"])
        self.assertEqual(diagnostics["fixed_technician_outside_active_plan_relaxed_job_count"], 0)

    def test_missing_or_unavailable_fixed_code_has_stable_reason(self) -> None:
        for current_code in ("", "MISSING"):
            with self.subTest(current_code=current_code):
                payload = {
                    "request_id": "r1",
                    "mode": "na_general",
                    "city": "Atlanta_6area",
                    "planning_date": "2026-07-21",
                    "technicians": [{"employee_code": "T1"}],
                    "jobs": [
                        {
                            "receipt_no": "J1",
                            "fixed": True,
                            "current_employee_code": current_code,
                            "eligible_employee_codes": ["T1"],
                            "region_name": "Zone 2",
                        }
                    ],
                }
                result = _build_response_payload(
                    payload,
                    pd.DataFrame(),
                    pd.DataFrame(),
                    diagnostics={"invalid_location_receipts": []},
                )
                self.assertEqual(
                    result["unassigned"][0]["reason"],
                    "FIXED_TECHNICIAN_NOT_AVAILABLE",
                )

    def test_fixed_code_outside_hard_candidates_with_no_eligible_remains_explicit(self) -> None:
        payload = {
            "request_id": "r1",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": "2026-07-21",
            "technicians": [{"employee_code": "T1", "slot_count": 8}],
            "jobs": [
                {
                    "receipt_no": "J1",
                    "fixed": True,
                    "current_employee_code": "T1",
                    "eligible_employee_codes": [],
                    "job_slot_count": 1,
                    "region_name": "Zone 2",
                }
            ],
        }
        result = _build_response_payload(
            payload,
            pd.DataFrame(),
            pd.DataFrame(),
            diagnostics={"invalid_location_receipts": []},
        )
        diagnostics = _build_routing_diagnostics(
            payload,
            pd.DataFrame([{"GSFS_RECEIPT_NO": "J1"}]),
            pd.DataFrame(),
            pd.DataFrame(),
        )

        self.assertEqual(
            result["unassigned"][0]["reason"],
            "NO_ELIGIBLE_TECHNICIAN",
        )
        self.assertTrue(result["unassigned"][0]["fixed_technician_outside_active_plan_relaxed"])
        self.assertTrue(result["diagnostics"]["relaxations_applied"]["fixed_technician_outside_active_plan_relaxed"])
        self.assertEqual(diagnostics["fixed_outside_active_plan_job_count"], 1)
        self.assertEqual(diagnostics["fixed_outside_active_plan_job_sample"], ["J1"])
        self.assertEqual(diagnostics["fixed_technician_outside_active_plan_relaxed_job_count"], 1)
        self.assertEqual(diagnostics["unavailable_fixed_job_count"], 0)

    def test_unmapped_postal_has_stable_unassigned_reason_and_diagnostic(self) -> None:
        payload = {
            "request_id": "r1",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": "2026-07-21",
            "technicians": [{"employee_code": "T1", "slot_count": 8}],
            "jobs": [
                {
                    "receipt_no": "J1",
                    "postal_code": "99999",
                    "eligible_employee_codes": ["T1"],
                    "job_slot_count": 1,
                }
            ],
        }
        result = _build_response_payload(
            payload,
            pd.DataFrame(),
            pd.DataFrame(),
            diagnostics={"invalid_location_receipts": []},
        )
        diagnostics = _build_routing_diagnostics(
            payload,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        )

        self.assertEqual(result["unassigned"][0]["reason"], "POSTAL_NOT_IN_ACTIVE_PLAN")
        self.assertEqual(diagnostics["postal_not_in_active_plan_job_count"], 1)
        self.assertEqual(diagnostics["postal_not_in_active_plan_job_sample"], ["J1"])


if __name__ == "__main__":
    unittest.main()
