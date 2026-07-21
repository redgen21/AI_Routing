from __future__ import annotations

from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm


def main() -> None:
    result = build_atlanta_production_assignment_osrm(
        date_keys=["2026-01-12", "2026-01-19", "2026-01-20"],
        output_suffix="osrm_actual_3days",
        include_daily_compare=False,
        attendance_limited=True,
        assignment_strategy="routing",
    )
    print(f"assignment={result.assignment_path}")
    print(f"summary={result.engineer_day_summary_path}")
    print(f"schedule={result.schedule_path}")
    print("merged_osrm_actual_3days_outputs_ready")


if __name__ == "__main__":
    main()
