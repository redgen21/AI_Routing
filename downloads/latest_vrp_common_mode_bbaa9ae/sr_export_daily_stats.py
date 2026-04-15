from __future__ import annotations

from smart_routing.export_daily_stats import export_daily_stats_workbook


def main():
    result = export_daily_stats_workbook()
    print(f"output_path={result.output_path}")
    print(f"sheet_count={len(result.workbook_sheets)}")
    for sheet_name in result.workbook_sheets:
        print(f"sheet={sheet_name}")


if __name__ == "__main__":
    main()
