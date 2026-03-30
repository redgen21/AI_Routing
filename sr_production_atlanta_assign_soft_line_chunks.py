from __future__ import annotations

from pathlib import Path

import pandas as pd

from smart_routing.production_assign_atlanta import _load_inputs, build_atlanta_production_assignment


OUT_DIR = Path("260310/production_output")


def main() -> None:
    _, _, _, service_df = _load_inputs()
    dates = sorted(service_df["service_date_key"].dropna().astype(str).unique().tolist())
    chunk_size = 6
    chunks = [dates[i : i + chunk_size] for i in range(0, len(dates), chunk_size)]

    for idx, chunk in enumerate(chunks, start=1):
        suffix = f"soft_line_chunk_{idx:02d}"
        assignment_chunk_path = OUT_DIR / f"atlanta_assignment_result_{suffix}.csv"
        summary_chunk_path = OUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv"
        schedule_chunk_path = OUT_DIR / f"atlanta_schedule_{suffix}.csv"
        if assignment_chunk_path.exists() and summary_chunk_path.exists() and schedule_chunk_path.exists():
            print(f"chunk={idx} skipped_existing={assignment_chunk_path.name}")
            continue
        result = build_atlanta_production_assignment(
            output_suffix=suffix,
            attendance_limited=False,
            date_keys=chunk,
        )
        print(f"chunk={idx} start={chunk[0]} end={chunk[-1]} assignment={result.assignment_path}")

    assignment_parts = []
    summary_parts = []
    schedule_parts = []
    for idx in range(1, len(chunks) + 1):
        suffix = f"soft_line_chunk_{idx:02d}"
        assignment_parts.append(pd.read_csv(OUT_DIR / f"atlanta_assignment_result_{suffix}.csv", low_memory=False))
        summary_parts.append(pd.read_csv(OUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv", low_memory=False))
        schedule_parts.append(pd.read_csv(OUT_DIR / f"atlanta_schedule_{suffix}.csv", low_memory=False))

    assignment_df = pd.concat(assignment_parts, ignore_index=True)
    engineer_day_summary_df = pd.concat(summary_parts, ignore_index=True)
    schedule_df = pd.concat(schedule_parts, ignore_index=True)

    assignment_df.to_csv(OUT_DIR / "atlanta_assignment_result.csv", index=False, encoding="utf-8-sig")
    engineer_day_summary_df.to_csv(OUT_DIR / "atlanta_engineer_day_summary.csv", index=False, encoding="utf-8-sig")
    schedule_df.to_csv(OUT_DIR / "atlanta_schedule.csv", index=False, encoding="utf-8-sig")

    print("merged_soft_line_outputs_ready")


if __name__ == "__main__":
    main()
