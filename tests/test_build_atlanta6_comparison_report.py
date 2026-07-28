from __future__ import annotations

import unittest

from tools.operations import build_atlanta6_comparison_report as report


class Atlanta6ArchivedReportTest(unittest.TestCase):
    def test_archival_report_has_legacy_structure_and_distinct_path(self) -> None:
        self.assertEqual("atlanta_6area_comparison_report_legacy_ko.html", report.DEFAULT_OUTPUT.name)
        html = report.DEFAULT_OUTPUT.read_text(encoding="utf-8")
        report.validate_html(html)
        self.assertIn("slot-paired", html)
        self.assertIn("NO_FEASIBLE_ROUTE", html)


if __name__ == "__main__":
    unittest.main()
