from __future__ import annotations

from smart_routing.production_assign_atlanta import build_atlanta_production_assignment


def main() -> None:
    result = build_atlanta_production_assignment(
        output_suffix="actual_attendance",
        attendance_limited=True,
    )
    print(f"assignment_path={result.assignment_path}")
    print(f"engineer_day_summary_path={result.engineer_day_summary_path}")
    print(f"schedule_path={result.schedule_path}")


if __name__ == "__main__":
    main()
