from __future__ import annotations

import shutil
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from tools.operations import build_atlanta13_fair_comparison_report as report
from tools.operations import build_atlanta6_comparison_report as legacy_report


class AtlantaFair13ComparisonReportTests(unittest.TestCase):
    def test_contract_and_output_do_not_collide_with_archival_report(self) -> None:
        self.assertIn("run_manifest.json", report.REQUIRED)
        self.assertNotEqual(report.DEFAULT_OUTPUT.resolve(), legacy_report.DEFAULT_OUTPUT.resolve())
        self.assertEqual("atlanta_13tech_fair_comparison_report_ko.html", report.DEFAULT_OUTPUT.name)

    def test_fresh_generation_never_requires_canonical_output(self) -> None:
        model = report.build_model(report.DEFAULT_DIR)
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "missing" / "canonical.html"
            self.assertFalse(target.exists())
            fresh = report.generate_exact_legacy_format(model, "fair")
            report.validate(fresh, model, fresh)
            target.parent.mkdir(); target.write_text(fresh, encoding="utf-8")
            report.validate(target.read_text(encoding="utf-8"), model, fresh)

    def test_mutated_copied_fair_input_changes_rendering(self) -> None:
        # Copy sibling immutable regional inputs too: they are evidence, not report output.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "atlanta 2606_test"
            shutil.copytree(report.DEFAULT_DIR, root / "atlanta_13tech_fair_comparison")
            shutil.copytree(report.DEFAULT_DIR.parent / "atlanta_6area_comparison" / "daily_inputs", root / "atlanta_6area_comparison" / "daily_inputs")
            copied = root / "atlanta_13tech_fair_comparison"
            before = report.generate_exact_legacy_format(report.build_model(copied))
            overall = copied / "overall_comparison.csv"
            text = overall.read_text(encoding="utf-8")
            overall.write_text(text.replace("1220,1576,286", "1219,1576,287", 1), encoding="utf-8")
            after = report.generate_exact_legacy_format(report.build_model(copied))
            self.assertNotEqual(before, after)

    def test_cross_region_capacity_excludes_every_shortage_region(self) -> None:
        active = {"20260601": {"a": ("Zone 1", 8), "b": ("Zone 2", 8), "c": ("Zone 3", 8)}}
        assigned = Counter({("20260601", "a"): 3, ("20260601", "b"): 2, ("20260601", "c"): 1})
        self.assertEqual((7, 1), report._remaining_cross_region_slots(active, assigned, {"20260601": {"Zone 1", "Zone 2"}}))

    def test_current_immutable_region_evidence_is_274_slots(self) -> None:
        model = report.build_model(report.DEFAULT_DIR)
        facts = report._fair_cross_region_slots(model)
        self.assertEqual(274, facts["remaining_slots"])
        rendered = report.generate_exact_legacy_format(model)
        self.assertIn(">274<", rendered)

    def test_legacy_format_has_exact_kpis_tables_and_headings(self) -> None:
        html = report.generate_exact_legacy_format(report.build_model(report.DEFAULT_DIR))
        self.assertEqual(8, html.count('class="kpi"'))
        self.assertEqual(7, html.count("<table"))
        for heading in ("요약 및 비교 해석", "슬롯 수별 결과", "요일별 비교", "권역별 결과 (6Area만)", "미배치 사유 및 진단", "fixed jobs 배정 결과", "데이터 출처"):
            self.assertIn(heading, html)
        self.assertIn("slot-paired", html)
        self.assertIn("NO_FEASIBLE_ROUTE", html)

    def test_incomplete_directory_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "incomplete"):
                report.build_model(Path(temporary))


if __name__ == "__main__":
    unittest.main()
