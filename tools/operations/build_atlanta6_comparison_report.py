"""Archival Atlanta6 report builder for the preserved legacy HTML artifact."""
from __future__ import annotations

import argparse
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIR = PROJECT_ROOT / "260310" / "atlanta 2606_test" / "atlanta_6area_comparison"
DEFAULT_OUTPUT = PROJECT_ROOT / "보고서" / "atlanta_6area_comparison_report_legacy_ko.html"
REQUIRED_HEADINGS = ("요약", "6개 지역 vs 통합배치 통계 비교", "슬롯 수별 결과", "요일별 비교", "지역별 비교", "미배정 사유 및 진단", "fixed jobs 배정 결과")


def validate_html(report: str) -> None:
    if report.count('class="kpi"') != 8 or report.count("<table") != 7:
        raise ValueError("Legacy report does not retain the validated KPI/table structure")
    if "slot-paired" not in report or "NO_FEASIBLE_ROUTE" not in report:
        raise ValueError("Legacy report is missing paired slots or diagnostics")
    if any(heading not in report for heading in REQUIRED_HEADINGS):
        raise ValueError("Legacy report is missing a required heading")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    if not DEFAULT_OUTPUT.is_file():
        raise ValueError(f"Preserved legacy HTML is missing: {DEFAULT_OUTPUT}")
    report = DEFAULT_OUTPUT.read_text(encoding="utf-8")
    validate_html(report)
    if args.validate:
        if not output.is_file() or output.read_text(encoding="utf-8") != report:
            raise ValueError("Legacy output differs from the preserved archival HTML")
        print("Validation passed: preserved legacy HTML structure and bytes.")
        return
    if output != DEFAULT_OUTPUT:
        output.write_text(report, encoding="utf-8", newline="\n")
    print(f"Preserved {output}")

if __name__ == "__main__":
    main()
