from __future__ import annotations

import unittest

from tools.operations.run_atlanta_four_scenario_compare import load_snapshot


class AtlantaThreeAreaDbOverrideSnapshotTests(unittest.TestCase):
    def test_ai103146_uses_verified_east_assignment(self) -> None:
        snapshot, evidence = load_snapshot("Atlanta_3area")
        technician = next(
            row for row in snapshot["technicians"] if row["employee_code"] == "AI103146"
        )
        self.assertEqual(1, technician["assigned_region_seq"])
        self.assertEqual("Atlanta_3area ATL East", technician["assigned_region_name"])
        self.assertIn("db_override_provenance.json", evidence)

    def test_region_staffing_is_five_three_five(self) -> None:
        snapshot, _ = load_snapshot("Atlanta_3area")
        counts = {
            seq: sum(row["assigned_region_seq"] == seq for row in snapshot["technicians"])
            for seq in (1, 2, 3)
        }
        self.assertEqual({1: 5, 2: 3, 3: 5}, counts)


if __name__ == "__main__":
    unittest.main()
