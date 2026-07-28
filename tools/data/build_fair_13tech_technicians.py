"""Create the June 2026 fair-comparison technician input with two people excluded.

The source CSV is never modified.  This stdlib-only command validates its
canonical schema and produces a deterministic CSV plus a JSON lineage/quality
report beside it.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


EXCLUDED = {
    "AI102933": "Richardo Brooks",
    "AI105115": "Jason Patterson",
}
EXPECTED_COLUMNS = [
    "record_id", "subsidiary_name", "strategic_city_name", "promise_date",
    "employee_code", "employee_name", "center_type", "shift_start", "shift_end",
    "slot_count", "max_jobs", "available", "start_location_type",
    "start_location_address", "source", "created_at", "updated_at",
    "priority_group", "max_minutes", "preferred_region_name",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def employee_summary(rows: list[dict[str, str]]) -> dict[str, object]:
    return {
        "rows": len(rows),
        "available_days": sum(r["available"].strip().lower() in {"t", "true", "y", "yes", "1"} for r in rows),
        "slots": sum(int(r["slot_count"] or 0) for r in rows),
        "date_count": len({r["promise_date"] for r in rows}),
    }


def duplicate_count(rows: list[dict[str, str]]) -> int:
    counts = Counter((r["promise_date"], r["employee_code"]) for r in rows)
    return sum(count - 1 for count in counts.values() if count > 1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.source.resolve() == args.output.resolve():
        raise SystemExit("output must differ from source")
    with args.source.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise SystemExit("source schema does not match expected canonical technician CSV")
        source_rows = list(reader)
    if len(source_rows) != 329:
        raise SystemExit(f"expected 329 source rows, got {len(source_rows)}")
    exclusions: dict[str, list[dict[str, str]]] = {}
    accepted: list[dict[str, str]] = []
    for row in source_rows:
        code = row["employee_code"]
        if code in EXCLUDED:
            if row["employee_name"].strip().casefold() != EXCLUDED[code].casefold():
                raise SystemExit(f"excluded employee name mismatch for {code}")
            exclusions.setdefault(code, []).append(row)
        else:
            accepted.append(row)
    if set(exclusions) != set(EXCLUDED):
        raise SystemExit("one or more required exclusions were not found")
    if duplicate_count(source_rows) != 0 or duplicate_count(accepted) != 0:
        raise SystemExit("duplicate promise_date+employee_code found")
    dates = sorted({r["promise_date"] for r in source_rows})
    if len(dates) != 22 or {r["promise_date"] for r in accepted} != set(dates):
        raise SystemExit("date coverage must remain exactly 22 dates")
    if len(source_rows) != len(accepted) + sum(len(rows) for rows in exclusions.values()):
        raise SystemExit("row accounting failed")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPECTED_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(accepted)
    report = {
        "schema_version": "atlanta_technician_csv/v1",
        "parent_source": str(args.source),
        "source_sha256": sha256(args.source),
        "output": str(args.output),
        "output_sha256": sha256(args.output),
        "schema_columns": EXPECTED_COLUMNS,
        "row_accounting": {"input_rows": len(source_rows), "accepted_rows": len(accepted), "rejected_rows": sum(len(rows) for rows in exclusions.values()), "rejection_reason": "FAIR_COMPARISON_EXCLUDED_TECHNICIAN"},
        "exclusions": {code: {"employee_name": EXCLUDED[code], **employee_summary(rows)} for code, rows in sorted(exclusions.items())},
        "date_coverage": {"count": len(dates), "dates": dates},
        "duplicate_promise_date_employee_code": 0,
        "null_policy": "Values are preserved verbatim from the canonical source; this transform removes only the explicitly named employees.",
        "stable_identifier": "record_id is preserved unchanged; employee_code identifies an employee within a promise_date.",
    }
    args.output.with_suffix(".report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
