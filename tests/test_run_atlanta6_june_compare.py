from __future__ import annotations

import importlib.util
from tempfile import TemporaryDirectory
import unittest
from pathlib import Path

import pandas as pd
from tools.data.atlanta_6area_plan import POLICY_VERSION


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "tools" / "operations" / "run_atlanta6_june_compare.py"
SPEC = importlib.util.spec_from_file_location("atlanta6_compare", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
compare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compare)


class Atlanta6ConstraintDiagnosticTest(unittest.TestCase):
    def test_comparison_uses_canonical_v2_policy_and_new_checkpoint_namespace(self) -> None:
        self.assertEqual(compare.POLICY, POLICY_VERSION)
        self.assertEqual(compare.POLICY, "own_region_with_approved_boundary_overflow/v2")
        self.assertIn("policy_v2", compare.CHECKPOINT_VERSION)

    def test_integrity_classifies_current_input_archive_and_unexpected_directories(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in (
                "_constraint_probe_checkpoints_v3_policy_v2",
                "daily_inputs",
                "_archive_prior_run",
                "surprise_directory",
            ):
                (root / name).mkdir()
            result = compare.classify_top_level_directories(root)
        by_path = {row["path"]: row["classification"] for row in result["directories"]}
        self.assertEqual(
            by_path["_constraint_probe_checkpoints_v3_policy_v2"],
            "current_execution_evidence",
        )
        self.assertEqual(by_path["daily_inputs"], "input_derivatives")
        self.assertEqual(by_path["_archive_prior_run"], "excluded_archive_non_current")
        self.assertEqual(result["unexpected_top_level_directories"], ["surprise_directory"])
        self.assertEqual(result["status"], "unexpected_directories_present")

    def test_slot_shortage_requires_probe_and_remaining_slot_evidence(self) -> None:
        payload = {
            "technicians": [
                {"employee_code": "T1", "max_slots": 2},
                {"employee_code": "T2", "max_slots": 1},
            ]
        }
        baseline = {
            "assignments": [
                {"receipt_no": "A", "employee_code": "T1", "job_slot_count": 2},
                {"receipt_no": "B", "employee_code": "T2", "job_slot_count": 1},
            ]
        }
        evidence = compare._eligible_remaining_slot_evidence(
            {"receipt_no": "J", "job_slot_count": 2, "eligible_employee_codes": ["T1", "T2"]},
            payload,
            baseline,
        )
        self.assertEqual(evidence["eligible_remaining_slots"], 0)
        self.assertTrue(evidence["eligible_remaining_slot_shortage"])
        self.assertEqual(
            compare._classify_no_feasible_route(
                baseline_replay_matches=True,
                slot_evidence=evidence,
                capacity_probe_assigned=True,
                work_probe_assigned=False,
                travel_probe_assigned=None,
            ),
            ("CAPACITY_SLOT_SHORTAGE", "capacity_probe_assigned_with_remaining_slot_shortage"),
        )

    def test_probe_without_isolating_evidence_is_undetermined_or_multiple(self) -> None:
        evidence = {"eligible_remaining_slot_shortage": False}
        self.assertEqual(
            compare._classify_no_feasible_route(
                baseline_replay_matches=True,
                slot_evidence=evidence,
                capacity_probe_assigned=True,
                work_probe_assigned=False,
                travel_probe_assigned=False,
            ),
            ("UNDETERMINED", "capacity_probe_assigned_without_remaining_slot_shortage"),
        )
        self.assertEqual(
            compare._classify_no_feasible_route(
                baseline_replay_matches=True,
                slot_evidence={"eligible_remaining_slot_shortage": True},
                capacity_probe_assigned=True,
                work_probe_assigned=True,
                travel_probe_assigned=False,
            ),
            ("MULTIPLE_CONSTRAINTS", "capacity_slot;work_time"),
        )

    def test_result_type_slot_counts_preserve_reason_and_slot_count(self) -> None:
        rows = pd.DataFrame(
            [
                {"promise_date": 20260601, "job_area": "A", "result_type": "assigned", "receipt_no": "A1", "job_slot_count": 1},
                {"promise_date": 20260601, "job_area": "A", "result_type": "unassigned", "receipt_no": "U1", "job_slot_count": 2, "raw_reason": "NO_FEASIBLE_ROUTE", "diagnostic_classification": "WORK_TIME_LIMIT"},
                {"promise_date": 20260601, "job_area": "A", "result_type": "unassigned", "receipt_no": "U2", "job_slot_count": 2, "raw_reason": "NO_FEASIBLE_ROUTE", "diagnostic_classification": "WORK_TIME_LIMIT"},
            ]
        )
        counts = compare._result_type_slot_counts(rows)
        work = counts[counts["diagnostic_classification"].eq("WORK_TIME_LIMIT")].iloc[0]
        self.assertEqual(int(work["jobs"]), 2)
        self.assertEqual(int(work["slots"]), 4)
        self.assertEqual(int(work["job_slot_count"]), 2)

    def test_response_keys_produce_assigned_and_unassigned_detail_rows(self) -> None:
        payload = {
            "technicians": [{"employee_code": "T1", "max_slots": 8}],
            "jobs": [
                {"receipt_no": "A", "region_name": "Zone A", "job_slot_count": 1, "eligible_employee_codes": ["T1"]},
                {"receipt_no": "U", "region_name": "Zone A", "job_slot_count": 2, "eligible_employee_codes": ["T1"]},
            ],
            "options": {},
        }
        result = {
            "assignments": [{"receipt_no": "A", "employee_code": "T1", "job_slot_count": 1}],
            "unassigned": [{"receipt_no": "U", "reason": "NO_FEASIBLE_ROUTE", "job_slot_count": 2}],
        }
        probes = {
            "baseline": {"assigned_receipts": {"A"}},
            "capacity": {"assigned_receipts": {"A", "U"}},
            "work_time": {"assigned_receipts": {"A"}},
            "travel_distance": {"assigned_receipts": set()},
        }
        detail, diagnostics = compare._add_area_and_diagnostic_detail(20260601, result, payload, probes)
        self.assertEqual({row["result_type"] for row in detail}, {"assigned", "unassigned"})
        self.assertEqual({row["receipt_no"] for row in detail}, {"A", "U"})
        self.assertEqual(len(diagnostics), 1)

    def test_slot_count_comparison_requires_and_reports_both_scenarios(self) -> None:
        jobs = pd.DataFrame(
            [
                {"promise_date": 20260601, "gsfs_receipt_no": "A", "job_slot_count": 1},
                {"promise_date": 20260601, "gsfs_receipt_no": "B", "job_slot_count": 2},
            ]
        )
        existing = pd.DataFrame(
            [
                {"promise_date": 20260601, "receipt_no": "A", "result_type": "assigned"},
                {"promise_date": 20260601, "receipt_no": "B", "result_type": "unassigned"},
            ]
        )
        candidate = pd.DataFrame(
            [
                {"promise_date": 20260601, "receipt_no": "A", "result_type": "unassigned"},
                {"promise_date": 20260601, "receipt_no": "B", "result_type": "assigned"},
            ]
        )
        summary, long = compare.build_slot_count_comparison(jobs, existing, candidate)
        self.assertEqual(set(summary["scenario"]), {"Existing", "Atlanta_6area"})
        atlanta_two_slot = summary[(summary["scenario"] == "Atlanta_6area") & (summary["job_slot_count"] == 2)].iloc[0]
        self.assertEqual(int(atlanta_two_slot["assigned_jobs"]), 1)
        self.assertEqual(int(atlanta_two_slot["assigned_slots"]), 2)
        self.assertEqual(set(long["result_type"]), {"assigned", "unassigned"})

    def test_capacity_roster_includes_available_plan_technician_with_zero_assignments(self) -> None:
        # AI105115 is the Zone 6 technician.  Capacity accounting must retain
        # them when available even if a no-demand zone yields no assignments.
        technicians = pd.DataFrame([
            {"promise_date": 20260601, "employee_code": "AI105115", "available": "t", "slot_count": 8},
            {"promise_date": 20260601, "employee_code": "AI102087", "available": "t", "slot_count": 4},
            {"promise_date": 20260601, "employee_code": "AI103317", "available": "f", "slot_count": 8},
            {"promise_date": 20260601, "employee_code": "OUTSIDE_PLAN", "available": "t", "slot_count": 9},
        ])
        roster, summary = compare.technician_input_capacity_roster(
            technicians,
            scenario="Atlanta_6area",
            dates=[20260601],
            plan_only=True,
        )
        self.assertIn("AI105115", set(roster["employee_code"]))
        self.assertEqual(2, int(summary.iloc[0]["solver_input_available_technicians"]))
        self.assertEqual(12, int(summary.iloc[0]["solver_input_available_slots"]))

    def test_existing_capacity_is_rebuilt_from_raw_available_input_not_legacy_stats(self) -> None:
        stats = pd.DataFrame([{
            "promise_date": 20260601, "total_jobs": 2, "assigned_jobs": 1,
            "used_slot_count": 2, "unassigned_jobs": 1, "job_fill_rate_pct": 50.0,
            "capacity_fill_rate_pct": 999.0, "total_travel_distance_km": 10.0,
        }])
        technicians = pd.DataFrame([
            {"promise_date": 20260601, "employee_code": "T1", "available": "t", "slot_count": 8},
            {"promise_date": 20260601, "employee_code": "T2", "available": "t", "slot_count": 4},
            {"promise_date": 20260601, "employee_code": "T3", "available": "f", "slot_count": 8},
        ])
        results = pd.DataFrame([
            {"promise_date": 20260601, "result_type": "assigned", "employee_code": "T1"},
            {"promise_date": 20260601, "result_type": "unassigned", "employee_code": ""},
        ])
        metrics, roster = compare.baseline_metrics(stats, technicians, results)
        row = metrics.iloc[0]
        self.assertEqual(2, int(row["active_technicians"]))
        self.assertEqual(12, int(row["available_slots"]))
        self.assertEqual(1, int(row["dispatched_technicians"]))
        self.assertAlmostEqual(100 * 2 / 12, float(row["fill_rate_pct"]))
        self.assertEqual(3, len(roster))

    def test_integrated_workbook_rejects_stale_daily_capacity(self) -> None:
        canonical = pd.DataFrame([{
            "promise_date": 20260601,
            "active_technicians_existing": 12,
            "available_slots_existing": 92,
            "active_technicians_atlanta6": 12,
            "available_slots_atlanta6": 92,
        }])
        with TemporaryDirectory() as temporary:
            workbook = Path(temporary) / "integrated.xlsx"
            stale = canonical.copy()
            stale.loc[0, "active_technicians_existing"] = 11
            stale.loc[0, "available_slots_existing"] = 85
            with pd.ExcelWriter(workbook) as writer:
                stale.to_excel(writer, sheet_name="daily_comparison", index=False)
            with self.assertRaisesRegex(RuntimeError, "does not match canonical CSV"):
                compare.validate_integrated_statistics_workbook(workbook, canonical)
            with pd.ExcelWriter(workbook) as writer:
                canonical.to_excel(writer, sheet_name="daily_comparison", index=False)
            compare.validate_integrated_statistics_workbook(workbook, canonical)

    def test_area_daily_technician_validation_rejects_blank_or_noncanonical_rows(self) -> None:
        canonical = pd.DataFrame([{
            "promise_date": 20260630,
            "technician_area": "Atlanta_6area Zone 5",
            "dispatched_technician_count": 2,
            "dispatch_jobs": 12,
            "used_slots": 16,
            "total_travel_distance_km": 238.77,
            "total_travel_duration_min": 245.88,
            "total_travel_distance_miles": 148.364754,
            "avg_travel_distance_miles_per_dispatched_tech": 74.182377,
        }])
        corrupt = pd.concat([canonical, pd.DataFrame([{
            "promise_date": pd.NA,
            "technician_area": pd.NA,
            "dispatched_technician_count": pd.NA,
            "dispatch_jobs": pd.NA,
            "used_slots": 16,
            "total_travel_distance_km": pd.NA,
            "total_travel_duration_min": pd.NA,
            "total_travel_distance_miles": pd.NA,
            "avg_travel_distance_miles_per_dispatched_tech": pd.NA,
        }])], ignore_index=True)
        with self.assertRaisesRegex(RuntimeError, "blank keys"):
            compare.validate_area_daily_technician_statistics(corrupt, canonical)
        compare.validate_area_daily_technician_statistics(canonical, canonical)

    def test_fixed_policy_accounts_authorized_release_and_rejects_unflagged_reassignment(self) -> None:
        jobs = pd.DataFrame([
            {"promise_date": 20260601, "gsfs_receipt_no": "A", "fixed": True, "svc_engineer_code": "T1", "job_slot_count": 1},
            {"promise_date": 20260601, "gsfs_receipt_no": "B", "fixed": True, "svc_engineer_code": "T1", "job_slot_count": 1},
        ])
        detail = pd.DataFrame([
            {"promise_date": 20260601, "receipt_no": "A", "result_type": "assigned", "employee_code": "T1", "raw_reason": "", "fixed_technician_outside_active_plan_relaxed": False},
            {"promise_date": 20260601, "receipt_no": "B", "result_type": "assigned", "employee_code": "T2", "raw_reason": "", "fixed_technician_outside_active_plan_relaxed": True},
        ])
        _, summary = compare.build_fixed_job_policy_accounting(jobs, detail)
        self.assertEqual(summary["preserved_original_technician"], 1)
        self.assertEqual(summary["authorized_outside_plan_reassignments"], 1)
        self.assertEqual(summary["unauthorized_non_flagged_reassignments"], 0)
        detail.loc[1, "fixed_technician_outside_active_plan_relaxed"] = False
        with self.assertRaisesRegex(RuntimeError, "Unauthorized fixed-job reassignment"):
            compare.build_fixed_job_policy_accounting(jobs, detail)

    def test_weekday_reporting_uses_iso_monday_one_and_reconciles_1506(self) -> None:
        receipts = [f"J{i:04d}" for i in range(1506)]
        jobs = pd.DataFrame({
            "promise_date": [20260601] * 1506,
            "gsfs_receipt_no": receipts,
            "job_slot_count": [1] * 1506,
        })
        existing = pd.DataFrame({
            "promise_date": [20260601] * 1506,
            "receipt_no": receipts,
            "result_type": ["assigned"] * 1506,
        })
        candidate = existing.copy()
        candidate.loc[0, "result_type"] = "unassigned"
        metrics = pd.DataFrame([
            {"promise_date": 20260601, "scenario": "Existing", "active_technicians": 10, "travel_distance_miles": 100},
            {"promise_date": 20260601, "scenario": "Atlanta_6area", "active_technicians": 10, "travel_distance_miles": 80},
        ])
        diagnostics = pd.DataFrame([
            {"promise_date": 20260601, "receipt_no": receipts[0], "raw_reason": "NO_FEASIBLE_ROUTE", "diagnostic_classification": "UNDETERMINED", "job_slot_count": 1}
        ])
        weekday, weekday_diag = compare.build_weekday_reporting(
            jobs, existing, candidate, metrics, diagnostics
        )
        self.assertEqual(set(weekday["weekday_number"]), {1})
        self.assertEqual(set(weekday["weekday_name"]), {"Monday"})
        self.assertEqual(weekday.groupby("scenario")["total_jobs"].sum().to_dict(), {"Atlanta_6area": 1506, "Existing": 1506})
        self.assertEqual(int(weekday_diag["jobs"].sum()), 1)


if __name__ == "__main__":
    unittest.main()
