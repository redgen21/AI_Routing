from __future__ import annotations

from pathlib import Path

import pandas as pd

from smart_routing.production_assign_atlanta import _load_inputs
from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm


OUT_DIR = Path("260310/production_output")


def main() -> None:
    _, _, _, service_df = _load_inputs()
    dates = sorted(service_df["service_date_key"].dropna().astype(str).unique().tolist())
    chunk_size = 6
    chunks = [dates[i : i + chunk_size] for i in range(0, len(dates), chunk_size)]

    for idx, chunk in enumerate(chunks, start=1):
        suffix = f"osrm_actual_attendance_iteration_chunk_{idx:02d}"
        assignment_chunk_path = OUT_DIR / f"atlanta_assignment_result_{suffix}.csv"
        summary_chunk_path = OUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv"
        schedule_chunk_path = OUT_DIR / f"atlanta_schedule_{suffix}.csv"
        if assignment_chunk_path.exists() and summary_chunk_path.exists() and schedule_chunk_path.exists():
            print(f"chunk={idx} skipped_existing={assignment_chunk_path.name}")
            continue
        result = build_atlanta_production_assignment_osrm(
            date_keys=chunk,
            output_suffix=suffix,
            include_daily_compare=False,
            attendance_limited=True,
            assignment_strategy="iteration",
        )
        print(f"chunk={idx} start={chunk[0]} end={chunk[-1]} assignment={result.assignment_path}")

    assignment_parts = []
    summary_parts = []
    schedule_parts = []
    for idx in range(1, len(chunks) + 1):
        suffix = f"osrm_actual_attendance_iteration_chunk_{idx:02d}"
        assignment_parts.append(pd.read_csv(OUT_DIR / f"atlanta_assignment_result_{suffix}.csv", low_memory=False))
        summary_parts.append(pd.read_csv(OUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv", low_memory=False))
        schedule_parts.append(pd.read_csv(OUT_DIR / f"atlanta_schedule_{suffix}.csv", low_memory=False))

    assignment_df = pd.concat(assignment_parts, ignore_index=True)
    engineer_day_summary_df = pd.concat(summary_parts, ignore_index=True)
    schedule_df = pd.concat(schedule_parts, ignore_index=True)

    assignment_df.to_csv(OUT_DIR / "atlanta_assignment_result_osrm_actual_attendance_iteration.csv", index=False, encoding="utf-8-sig")
    engineer_day_summary_df.to_csv(OUT_DIR / "atlanta_engineer_day_summary_osrm_actual_attendance_iteration.csv", index=False, encoding="utf-8-sig")
    schedule_df.to_csv(OUT_DIR / "atlanta_schedule_osrm_actual_attendance_iteration.csv", index=False, encoding="utf-8-sig")

    print("merged_osrm_actual_attendance_iteration_outputs_ready")


if __name__ == "__main__":
    main()
