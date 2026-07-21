from __future__ import annotations

import pandas as pd

import smart_routing.production_assign_atlanta as base
from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm_from_frames
from smart_routing.production_assign_atlanta_vrp import build_atlanta_production_assignment_vrp


TARGET_DATES = ["2026-01-12", "2026-01-19", "2026-01-20"]
OUT_DIR = "260310/production_output"


def _dedupe_service(service_df: pd.DataFrame) -> pd.DataFrame:
    deduped = service_df.copy()
    deduped = deduped.sort_values(
        [col for col in ["service_date_key", "GSFS_RECEIPT_NO", "service_time_min"] if col in deduped.columns],
        ascending=[True, True, False] if "service_date_key" in deduped.columns else [True, False],
    ).reset_index(drop=True)
    if "GSFS_RECEIPT_NO" in deduped.columns:
        deduped = deduped.drop_duplicates(subset=["service_date_key", "GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
    return deduped


def main() -> None:
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    wanted = {str(v) for v in TARGET_DATES}
    service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()
    service_df = _dedupe_service(service_df)

    osrm_assignment_df, osrm_summary_df, osrm_schedule_df = build_atlanta_production_assignment_osrm_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=True,
        assignment_strategy="routing",
    )
    osrm_assignment_df.to_csv(f"{OUT_DIR}/atlanta_assignment_result_osrm_actual_3days.csv", index=False, encoding="utf-8-sig")
    osrm_summary_df.to_csv(f"{OUT_DIR}/atlanta_engineer_day_summary_osrm_actual_3days.csv", index=False, encoding="utf-8-sig")
    osrm_schedule_df.to_csv(f"{OUT_DIR}/atlanta_schedule_osrm_actual_3days.csv", index=False, encoding="utf-8-sig")

    iteration_assignment_df, iteration_summary_df, iteration_schedule_df = build_atlanta_production_assignment_osrm_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=True,
        assignment_strategy="iteration",
    )
    iteration_assignment_df.to_csv(f"{OUT_DIR}/atlanta_assignment_result_osrm_iteration_actual_3days.csv", index=False, encoding="utf-8-sig")
    iteration_summary_df.to_csv(f"{OUT_DIR}/atlanta_engineer_day_summary_osrm_iteration_actual_3days.csv", index=False, encoding="utf-8-sig")
    iteration_schedule_df.to_csv(f"{OUT_DIR}/atlanta_schedule_osrm_iteration_actual_3days.csv", index=False, encoding="utf-8-sig")

    build_atlanta_production_assignment_vrp(
        date_keys=TARGET_DATES,
        output_suffix="vrp_actual_3days",
        attendance_limited=True,
    )
    print("built actual, vrp, osrm, osrm_iteration comparison outputs for", ", ".join(TARGET_DATES))


if __name__ == "__main__":
    main()
