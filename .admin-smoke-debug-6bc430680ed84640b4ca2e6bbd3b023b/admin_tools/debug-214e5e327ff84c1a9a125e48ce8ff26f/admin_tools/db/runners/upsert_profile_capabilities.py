"""Upsert city-level Technician capabilities from the DMS/DMS2 profile workbook.

This intentionally writes only ``common_technician_capability_master``.  It
does not create Plan-specific copies and does not alter Region Plan technician
assignments, Technician master rows, ZIP coverage, or routing configuration.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from admin_tools.db.common_vrp import _execute_values_upsert, verify_admin_schema
from admin_tools.db.guard import require_db_write_allowed


DEFAULT_CONFIG_PATH = Path("config/common_vrp.dev.json")
DEFAULT_WORKBOOK_PATH = Path("Top 10_DMS_DMS2_Profile_20260317.xlsx")
REQUIRED_COLUMNS = (
    "STRATEGIC_CITY_NAME",
    "SVC_ENGINEER_CODE",
    "SERVICE_PRODUCT_GROUP_CODE",
    "SERVICE_PRODUCT_CODE",
    "REPAIR_FLAG",
    "AREA_PRODUCT_FLAG",
)


def _text(value: Any) -> str:
    if pd.isna(value):
        return ""
    result = " ".join(str(value).strip().split())
    return "" if result.casefold() in {"nan", "none", "nat", "<na>", "<null>"} else result


def _capability_rows(workbook_path: Path, cities: set[str]) -> tuple[list[tuple[Any, ...]], dict[str, int]]:
    if not workbook_path.is_file():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")
    product = pd.read_excel(workbook_path, sheet_name="3. Product", dtype=object)
    missing = [column for column in REQUIRED_COLUMNS if column not in product.columns]
    if missing:
        raise ValueError("Product sheet is missing required columns: " + ", ".join(missing))

    by_key: dict[tuple[str, str, str, str], tuple[bool, bool]] = {}
    counters: dict[str, int] = defaultdict(int)
    for _, source in product.iterrows():
        city = _text(source["STRATEGIC_CITY_NAME"])
        employee = _text(source["SVC_ENGINEER_CODE"])
        group = _text(source["SERVICE_PRODUCT_GROUP_CODE"])
        product_code = _text(source["SERVICE_PRODUCT_CODE"])
        if cities and city not in cities:
            continue
        if not city or not employee or not group:
            counters["skipped_missing_identity"] += 1
            continue
        key = (city, employee, group, product_code)
        value = (
            _text(source["REPAIR_FLAG"]).upper() == "T",
            not (group.upper() == "REF" and _text(source["AREA_PRODUCT_FLAG"]).upper() == "N"),
        )
        previous = by_key.get(key)
        if previous is not None and previous != value:
            raise ValueError(
                "PRODUCT_CAPABILITY_CONFLICT: identical city/employee/product key has different "
                f"flags in workbook: {key}"
            )
        if previous is not None:
            counters["duplicate_source_rows"] += 1
        by_key[key] = value

    rows = [
        ("LGEAI", city, employee, group, product_code, repair_allowed, heavy_repair_allowed, 100, None, None)
        for (city, employee, group, product_code), (repair_allowed, heavy_repair_allowed) in sorted(by_key.items())
    ]
    counters["capability_keys"] = len(rows)
    counters["cities"] = len({row[1] for row in rows})
    return rows, dict(counters)


def main() -> int:
    parser = argparse.ArgumentParser(description="Upsert city-level capabilities from the DMS/DMS2 Product workbook.")
    parser.add_argument("--runtime-root", default="", help="Application root used to resolve relative paths.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK_PATH)
    parser.add_argument("--city", action="append", default=[], help="Limit to one operational city; repeat as needed.")
    parser.add_argument("--apply", action="store_true", help="Write to Development DB. Default is preview only.")
    args = parser.parse_args()

    if args.runtime_root:
        os.chdir(Path(args.runtime_root).expanduser().resolve())
    cities = {_text(city) for city in args.city if _text(city)}
    rows, counters = _capability_rows(Path(args.workbook), cities)
    scope = ", ".join(sorted(cities)) if cities else "ALL cities in workbook"
    print(f"scope={scope}")
    for key in ("cities", "capability_keys", "duplicate_source_rows", "skipped_missing_identity"):
        print(f"{key}={counters.get(key, 0)}")
    if not args.apply:
        print("dry_run=true; no database rows were written. Re-run with --apply after reviewing the counts.")
        return 0

    require_db_write_allowed(args.config)
    verify_admin_schema(args.config, operation="la_seed")
    written = _execute_values_upsert(
        "common_technician_capability_master",
        [
            "subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code",
            "repair_allowed", "heavy_repair_allowed", "priority_score", "effective_start_date", "effective_end_date",
        ],
        rows,
        ["subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code"],
        ["repair_allowed", "heavy_repair_allowed", "priority_score", "effective_start_date", "effective_end_date"],
        args.config,
    )
    print(f"upserted_capability_keys={written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
