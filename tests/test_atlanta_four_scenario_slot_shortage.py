from __future__ import annotations

import unittest

from tools.operations.run_atlanta_four_scenario_compare import remaining_cross_region_slots


class CrossRegionRemainingSlotTests(unittest.TestCase):
    def test_counts_each_other_region_technician_once_per_date(self) -> None:
        active = {
            20260601: {
                "A": ("Zone 1", 8),
                "B": ("Zone 2", 8),
                "C": ("Zone 3", 6),
            }
        }
        assigned = {(20260601, "A"): 8, (20260601, "B"): 3, (20260601, "C"): 2}
        shortages = {20260601: {"Zone 1", "Zone 2"}}
        self.assertEqual(4, remaining_cross_region_slots(active, assigned, shortages))

    def test_clamps_over_capacity_and_sums_dates_independently(self) -> None:
        active = {
            20260601: {"A": ("Zone 1", 8), "B": ("Zone 2", 8)},
            20260602: {"A": ("Zone 1", 8), "B": ("Zone 2", 8)},
        }
        assigned = {
            (20260601, "A"): 2, (20260601, "B"): 9,
            (20260602, "A"): 7, (20260602, "B"): 3,
        }
        shortages = {20260601: {"Zone 1"}, 20260602: {"Zone 2"}}
        self.assertEqual(1, remaining_cross_region_slots(active, assigned, shortages))


if __name__ == "__main__":
    unittest.main()
