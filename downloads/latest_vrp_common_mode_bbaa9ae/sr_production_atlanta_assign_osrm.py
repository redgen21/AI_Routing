from __future__ import annotations

from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm


def main() -> None:
    result = build_atlanta_production_assignment_osrm()
    print(f"assignment_path={result.assignment_path}")
    print(f"engineer_day_summary_path={result.engineer_day_summary_path}")
    print(f"schedule_path={result.schedule_path}")
    print(f"daily_compare_path={result.daily_compare_path}")


if __name__ == "__main__":
    main()
