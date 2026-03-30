from __future__ import annotations

from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm


def main() -> None:
    result = build_atlanta_production_assignment_osrm(
        output_suffix="osrm_actual_attendance",
        include_daily_compare=False,
        attendance_limited=True,
    )
    print(f"assignment_path={result.assignment_path}")
    print(f"engineer_day_summary_path={result.engineer_day_summary_path}")
    print(f"schedule_path={result.schedule_path}")


if __name__ == "__main__":
    main()
