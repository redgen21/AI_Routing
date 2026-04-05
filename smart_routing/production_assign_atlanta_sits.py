from __future__ import annotations

import pandas as pd

import smart_routing.production_assign_atlanta as base
from smart_routing.production_assign_atlanta_csi import (
    AtlantaProductionSequentialAssignmentResult,
    _build_assignment_from_frames,
    _output_paths,
)


def build_atlanta_production_assignment_sits_from_frames(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    attendance_limited: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return _build_assignment_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=attendance_limited,
        enable_targeted_swap=True,
    )


def build_atlanta_production_assignment_sits(
    date_keys: list[str] | None = None,
    output_suffix: str = "sits_actual",
    attendance_limited: bool = True,
) -> AtlantaProductionSequentialAssignmentResult:
    assignment_path, summary_path, schedule_path = _output_paths(output_suffix)
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    if date_keys:
        wanted = {str(value) for value in date_keys}
        service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()

    assignment_df, summary_df, schedule_df = build_atlanta_production_assignment_sits_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=attendance_limited,
    )
    assignment_df.to_csv(assignment_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    schedule_df.to_csv(schedule_path, index=False, encoding="utf-8-sig")
    return AtlantaProductionSequentialAssignmentResult(
        assignment_path=assignment_path,
        engineer_day_summary_path=summary_path,
        schedule_path=schedule_path,
    )
