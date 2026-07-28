from __future__ import annotations

import unittest

import pandas as pd

from tools.operations import run_atlanta_fair_13tech_compare as fair


class AtlantaFair13TechRunnerTests(unittest.TestCase):
    def test_authorized_input_contract(self) -> None:
        jobs, technicians, dates = fair.load_and_validate_inputs()
        self.assertEqual(1506, len(jobs))
        self.assertEqual(22, len(dates))
        self.assertEqual(13, technicians["employee_code"].nunique())
        self.assertTrue(set(technicians["employee_code"]).isdisjoint(fair.EXCLUDED_CODES))

    def test_capacity_roster_counts_only_available_slots(self) -> None:
        technicians = pd.DataFrame([
            {"promise_date": 20260601, "employee_code": "A", "available": "t", "slot_count": 8},
            {"promise_date": 20260601, "employee_code": "B", "available": "f", "slot_count": 9},
        ])
        _roster, summary = fair.capacity_roster(technicians, [20260601], fair.FAIR_SCENARIO)
        self.assertEqual(1, int(summary.loc[0, "solver_input_available_technicians"]))
        self.assertEqual(8, int(summary.loc[0, "solver_input_available_slots"]))


if __name__ == "__main__":
    unittest.main()
