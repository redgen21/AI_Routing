from __future__ import annotations

import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

import smart_routing.production_assign_atlanta as base


PRODUCTION_OUTPUT_DIR = Path("260310/production_output")
VRP_SOFT_WORK_MIN = base.MAX_WORK_MIN
VRP_ABSOLUTE_WORK_MIN = 600
VRP_OVERTIME_ALLOWANCE_MIN = 60
VRP_FIXED_WORK_BUFFER_MIN = 30
VRP_OVERTIME_PENALTY_PER_UNIT = 500
VRP_TARGET_LOAD_PENALTY_PER_JOB = 4_000
VRP_PRIORITY_JOB_BIAS = {3: 0, 2: 500, 1: 1_500}
VRP_PRIORITY_FIXED_COST = {3: 0, 2: 0, 1: 0}
VRP_PRIORITY_LOWER_TARGET_PENALTY = {3: 2_500, 2: 1_500, 1: 1_000}
VRP_PRIORITY_UPPER_TARGET_PENALTY = 1_500
VRP_PRIORITY_SERVICE_TIME_MULTIPLIER = {3: 1.0, 2: 1.0, 1: 1.0}
VRP_REQUIRE_ALL_AVAILABLE_TECHNICIANS = False
VRP_USE_HARD_PRIORITY_MINIMUMS = False
VRP_ROUTE_OUTLIER_FACTOR = 1.6
VRP_ROUTE_OUTLIER_MIN_KM = 240.0
VRP_ROUTE_RELIEF_MAX_TOTAL_INCREASE_KM = 40.0
VRP_RETURN_HOME_FREE_MIN = 45.0
VRP_RETURN_HOME_SOFT_MIN = 75.0
VRP_RETURN_HOME_PENALTY_PER_MIN = 30
VRP_RETURN_HOME_EXTRA_PENALTY_PER_MIN = 90
VRP_ROUTE_REORDER_MAX_JOBS = 8
VRP_OPTIONAL_JOB_DROP_PENALTY = 1_000_000_000
VRP_RESCHEDULE_JOB_DROP_PENALTY = VRP_OPTIONAL_JOB_DROP_PENALTY * 100
VRP_FIXED_JOB_DROP_PENALTY = VRP_OPTIONAL_JOB_DROP_PENALTY * 1_000
VRP_OVERLAP_DMS2_PENALTY_COST = 10_000_000
VRP_DMS_AREA_DMS2_FALLBACK_PENALTY_COST = 50_000_000
# 45 minutes of travel-cost equivalent. This keeps Bucket Sim Draft jobs in
# their technician's preferred area unless a cross-area assignment is useful.
VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST = 4_500
VRP_UNRESTRICTED_DMS2_WORK_MIN = 24 * 60


@dataclass
class AtlantaProductionVRPAssignmentResult:
    assignment_path: Path
    engineer_day_summary_path: Path
    schedule_path: Path


def _output_paths(output_suffix: str) -> tuple[Path, Path, Path]:
    suffix = str(output_suffix).strip()
    if not suffix:
        suffix = "vrp"
    return (
        PRODUCTION_OUTPUT_DIR / f"atlanta_assignment_result_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_schedule_{suffix}.csv",
    )


def _dedupe_day_jobs(service_day_df: pd.DataFrame) -> pd.DataFrame:
    if service_day_df.empty:
        return service_day_df.copy()
    deduped = service_day_df.copy()
    deduped = deduped.sort_values(
        [col for col in ["service_date_key", "GSFS_RECEIPT_NO", "service_time_min"] if col in deduped.columns],
        ascending=[True, True, False] if "service_date_key" in deduped.columns else [True, False],
    ).reset_index(drop=True)
    if "GSFS_RECEIPT_NO" in deduped.columns:
        deduped = deduped.drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
    return deduped


def _coerce_bool_value(value: object, default: bool = False) -> bool:
    if pd.isna(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "y", "yes", "t"}:
        return True
    if text in {"false", "0", "n", "no", "f", ""}:
        return False
    return default


def _allocate_priority_targets(priority_groups: list[int], max_jobs_by_vehicle: list[int], total_jobs: int) -> list[int]:
    if not priority_groups:
        return []
    available = [idx for idx, max_jobs in enumerate(max_jobs_by_vehicle) if int(max_jobs) > 0]
    targets = [0] * len(priority_groups)
    if not available or int(total_jobs) <= 0:
        return targets

    capped_total_jobs = min(int(total_jobs), sum(int(max_jobs_by_vehicle[idx]) for idx in available))
    base_target = capped_total_jobs // len(available)
    if base_target <= 0:
        for idx in sorted(available, key=lambda i: (-int(priority_groups[i]), i))[:capped_total_jobs]:
            targets[idx] = 1
        return targets

    priority_offset = {3: 1, 2: 0, 1: -1}
    balanced_target_cap = base_target + 1
    for idx in available:
        raw_target = base_target + priority_offset.get(int(priority_groups[idx]), 0)
        targets[idx] = min(max(0, raw_target), int(max_jobs_by_vehicle[idx]), balanced_target_cap)

    while sum(targets) < capped_total_jobs:
        candidates = [
            idx
            for idx in available
            if targets[idx] < min(int(max_jobs_by_vehicle[idx]), balanced_target_cap)
        ]
        if not candidates:
            candidates = [
                idx
                for idx in available
                if targets[idx] < int(max_jobs_by_vehicle[idx])
            ]
        if not candidates:
            break
        idx = sorted(candidates, key=lambda i: (-int(priority_groups[i]), targets[i], i))[0]
        targets[idx] += 1

    while sum(targets) > capped_total_jobs:
        candidates = [idx for idx in available if targets[idx] > 0]
        if not candidates:
            break
        idx = sorted(candidates, key=lambda i: (-int(priority_groups[i]), -targets[i], i))[0]
        targets[idx] -= 1
    return targets


def _allocate_priority_minimums(priority_groups: list[int], max_jobs_by_vehicle: list[int], total_jobs: int) -> list[int]:
    if not priority_groups:
        return []
    available = [idx for idx, max_jobs in enumerate(max_jobs_by_vehicle) if int(max_jobs) > 0]
    minimums = [0] * len(priority_groups)
    if not available or int(total_jobs) <= 0:
        return minimums

    capped_total_jobs = min(int(total_jobs), sum(int(max_jobs_by_vehicle[idx]) for idx in available))
    average_jobs = capped_total_jobs // len(available)
    if average_jobs <= 0:
        for idx in sorted(available, key=lambda i: (-int(priority_groups[i]), i))[:capped_total_jobs]:
            minimums[idx] = 1
        return minimums

    priority_floor_offset = {3: 0, 2: -1, 1: -2}
    for idx in available:
        raw_minimum = average_jobs + priority_floor_offset.get(int(priority_groups[idx]), -1)
        minimums[idx] = min(max(1, raw_minimum), int(max_jobs_by_vehicle[idx]))

    while sum(minimums) > capped_total_jobs:
        candidates = [idx for idx in available if minimums[idx] > 0]
        if not candidates:
            break
        idx = sorted(candidates, key=lambda i: (int(priority_groups[i]), -minimums[i], i))[0]
        minimums[idx] -= 1
    return minimums


def _priority_group_score(value: object) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        return min(max(int(numeric), 1), 3)
    text = str(value or "").strip().upper()
    if text in {"A", "HIGH", "P3", "PRIORITY 3"}:
        return 3
    if text in {"C", "LOW", "P1", "PRIORITY 1"}:
        return 1
    return 2


def _priority_group_label(value: object) -> str:
    return {3: "A", 2: "B", 1: "C"}.get(_priority_group_score(value), "B")


def _priority_service_time_multiplier(value: object) -> float:
    return float(VRP_PRIORITY_SERVICE_TIME_MULTIPLIER.get(_priority_group_score(value), 1.0))


def _build_route_geometry(route_client, coord_chain: list[tuple[float, float]]) -> tuple[float, float, list[list[float]]]:
    if len(coord_chain) <= 1:
        geometry = [[float(lat), float(lon)] for lon, lat in coord_chain] if coord_chain else []
        return 0.0, 0.0, geometry
    try:
        distance_km, duration_min, geometry = route_client._request_route_geometry(route_client.cfg.osrm_url, coord_chain)
        return float(distance_km), float(duration_min), geometry
    except Exception:
        if route_client.cfg.fallback_osrm_url:
            try:
                distance_km, duration_min, geometry = route_client._request_route_geometry(route_client.cfg.fallback_osrm_url, coord_chain)
                return float(distance_km), float(duration_min), geometry
            except Exception:
                pass
    distance_mat, duration_mat = route_client.get_distance_duration_matrix(coord_chain)
    total_km = 0.0
    total_min = 0.0
    for i in range(len(coord_chain) - 1):
        total_km += float(distance_mat[i][i + 1])
        total_min += float(duration_mat[i][i + 1])
    geometry = [[float(lat), float(lon)] for lon, lat in coord_chain]
    return round(total_km, 2), round(total_min, 2), geometry


def _build_schedule_for_ordered_group(group_df: pd.DataFrame, route_client) -> tuple[pd.DataFrame, dict[str, object]]:
    if group_df.empty:
        return pd.DataFrame(), {"distance_km": 0.0, "duration_min": 0.0, "geometry": [], "ordered_coords": []}

    ordered_group = group_df.sort_values("vrp_visit_seq").reset_index(drop=True) if "vrp_visit_seq" in group_df.columns else group_df.reset_index(drop=True)
    first = ordered_group.iloc[0]
    start_coord = None
    if pd.notna(first.get("home_start_longitude")) and pd.notna(first.get("home_start_latitude")):
        start_coord = (float(first["home_start_longitude"]), float(first["home_start_latitude"]))

    stop_coords = [(float(row["longitude"]), float(row["latitude"])) for _, row in ordered_group.iterrows()]
    coord_chain = [start_coord] + stop_coords if start_coord is not None else stop_coords
    distance_mat, duration_mat = route_client.get_distance_duration_matrix(coord_chain)
    route_distance_km, route_duration_min, geometry = _build_route_geometry(route_client, coord_chain)
    if start_coord is not None and len(coord_chain) > 1:
        route_distance_km = max(float(route_distance_km) - float(distance_mat[0][1]), 0.0)
        route_duration_min = max(float(route_duration_min) - float(duration_mat[0][1]), 0.0)

    base_date = pd.to_datetime(str(ordered_group["service_date_key"].iloc[0]), errors="coerce")
    if pd.isna(base_date):
        base_date = pd.Timestamp("2026-01-01")
    current_time = base_date.replace(hour=base.DAY_START_HOUR, minute=0, second=0, microsecond=0)
    lunch_taken = False
    lunch_start_window = base_date.replace(
        hour=base.LUNCH_WINDOW_START_HOUR,
        minute=base.LUNCH_WINDOW_START_MIN,
        second=0,
        microsecond=0,
    )
    lunch_end_window = base_date.replace(
        hour=base.LUNCH_WINDOW_END_HOUR,
        minute=base.LUNCH_WINDOW_END_MIN,
        second=0,
        microsecond=0,
    )

    schedule_rows: list[dict[str, object]] = []
    for idx, row in enumerate(ordered_group.to_dict("records"), start=1):
        matrix_from = idx - 1 if start_coord is not None else max(idx - 1, 0)
        matrix_to = idx if start_coord is not None else idx - 1
        travel_min = 0.0 if idx == 1 and start_coord is None else float(duration_mat[matrix_from][matrix_to])
        if idx == 1 and start_coord is not None:
            travel_min = 0.0
        arrival = current_time + pd.Timedelta(minutes=travel_min)
        lunch_flag = False
        if not lunch_taken and lunch_start_window <= arrival <= lunch_end_window:
            arrival = arrival + pd.Timedelta(minutes=base.LUNCH_DURATION_MIN)
            lunch_taken = True
            lunch_flag = True
        start_time = arrival
        end_time = start_time + pd.Timedelta(minutes=float(row.get("service_time_min", 45)))
        if not lunch_taken and lunch_start_window <= end_time <= lunch_end_window:
            current_time = end_time + pd.Timedelta(minutes=base.LUNCH_DURATION_MIN)
            lunch_taken = True
            lunch_flag = True
        else:
            current_time = end_time

        schedule_row = dict(row)
        schedule_row["visit_seq"] = idx
        schedule_row["travel_time_from_prev_min"] = round(travel_min, 2)
        schedule_row["visit_start_time"] = base._fmt_dt(start_time)
        schedule_row["visit_end_time"] = base._fmt_dt(end_time)
        schedule_row["lunch_applied"] = lunch_flag
        schedule_row["route_distance_km"] = round(float(route_distance_km), 2)
        schedule_row["route_duration_min"] = round(float(route_duration_min), 2)
        schedule_rows.append(schedule_row)

    payload = {
        "distance_km": round(float(route_distance_km), 2),
        "duration_min": round(float(route_duration_min), 2),
        "geometry": geometry,
        "ordered_coords": coord_chain,
    }
    return pd.DataFrame(schedule_rows), payload


def _solve_vrp_day(
    service_day_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    route_client,
    region_centers: dict[int, tuple[float, float]],
    time_limit_seconds: int = 20,
    respect_fixed_jobs: bool = True,
    enforce_priority_minimums: bool = True,
    max_travel_min_per_sm_day: float | None = None,
    max_travel_km_per_sm_day: float | None = None,
    max_single_leg_min: float | None = None,
    max_home_to_job_min: float | None = None,
    long_leg_penalty_start_min: float | None = None,
    long_leg_penalty_multiplier: float | None = None,
    enforce_reschedule_jobs: bool = True,
    relax_distance_caps_for_feasibility: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    job_df = _dedupe_day_jobs(service_day_df)
    if job_df.empty or engineer_master_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    engineer_df = engineer_master_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).copy().reset_index(drop=True)
    engineer_df["start_coord"] = engineer_df.apply(lambda row: base._get_engineer_start_coord(row, region_centers), axis=1)
    engineer_df = engineer_df[engineer_df["start_coord"].notna()].copy().reset_index(drop=True)
    if engineer_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    vehicle_codes = engineer_df["SVC_ENGINEER_CODE"].astype(str).str.strip().tolist()
    vehicle_count = len(vehicle_codes)
    vehicle_index_by_code = {str(code).strip(): idx for idx, code in enumerate(vehicle_codes)}

    if respect_fixed_jobs and "fixed" in job_df.columns:
        fixed_mask = job_df["fixed"].map(_coerce_bool_value)
        fixed_code_series = job_df.get("current_employee_code", pd.Series("", index=job_df.index)).fillna("").astype(str).str.strip()
        unavailable_fixed_mask = fixed_mask & fixed_code_series.ne("") & ~fixed_code_series.isin(vehicle_codes)
        if unavailable_fixed_mask.any():
            job_df.loc[unavailable_fixed_mask, "fixed"] = False

    job_count = len(job_df)

    start_coords = [tuple(coord) for coord in engineer_df["start_coord"].tolist()]
    job_coords = [(float(row["longitude"]), float(row["latitude"])) for _, row in job_df.iterrows()]
    matrix_coords = start_coords + job_coords
    distance_mat_km, duration_mat_min = route_client.get_distance_duration_matrix(matrix_coords)

    manager = pywrapcp.RoutingIndexManager(job_count + (2 * vehicle_count), vehicle_count, list(range(job_count, job_count + vehicle_count)), list(range(job_count + vehicle_count, job_count + (2 * vehicle_count))))
    routing = pywrapcp.RoutingModel(manager)
    end_nodes = set(range(job_count + vehicle_count, job_count + (2 * vehicle_count)))
    start_nodes = set(range(job_count, job_count + vehicle_count))
    service_times = pd.to_numeric(job_df["service_time_min"], errors="coerce").fillna(45).tolist()
    job_slots = pd.to_numeric(job_df.get("job_slot_count", pd.Series(1, index=job_df.index)), errors="coerce").fillna(1).astype(int).clip(lower=1).tolist()
    total_slot_capacity = max(1, int(sum(job_slots)))
    area_types_by_job = (
        job_df.get("area_type", pd.Series("", index=job_df.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .tolist()
    )
    region_names_by_job = (
        job_df.get("new_region_name", pd.Series("", index=job_df.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .tolist()
    )
    priority_group_by_vehicle: list[int] = []
    service_time_multiplier_by_vehicle: list[float] = []
    center_type_by_vehicle: list[str] = []
    max_jobs_by_vehicle: list[int] = []
    max_minutes_by_vehicle: list[int] = []
    max_home_to_job_min_by_vehicle: list[float | None] = []
    max_single_leg_min_by_vehicle: list[float | None] = []
    preferred_region_by_vehicle: list[str] = []
    for _, engineer in engineer_df.iterrows():
        priority_group = _priority_group_score(engineer.get("priority_group", 2))
        center_type = str(engineer.get("SVC_CENTER_TYPE", "")).strip().upper()
        max_jobs = pd.to_numeric(
            pd.Series([engineer.get("max_slots", engineer.get("slot_count", engineer.get("max_jobs", 8)))]),
            errors="coerce",
        ).fillna(8).iloc[0]
        max_minutes = pd.to_numeric(
            pd.Series([engineer.get("max_minutes", VRP_SOFT_WORK_MIN)]),
            errors="coerce",
        ).fillna(VRP_SOFT_WORK_MIN).iloc[0]
        priority_group_by_vehicle.append(priority_group)
        service_time_multiplier_by_vehicle.append(_priority_service_time_multiplier(priority_group))
        center_type_by_vehicle.append(center_type)
        preferred_region_by_vehicle.append(str(engineer.get("preferred_region_name", "")).strip())
        max_jobs_by_vehicle.append(
            total_slot_capacity
            if center_type == base.DMS2_CENTER_TYPE
            else max(0, int(max_jobs))
        )
        max_minutes_by_vehicle.append(
            VRP_UNRESTRICTED_DMS2_WORK_MIN
            if center_type == base.DMS2_CENTER_TYPE
            else max(1, int(max_minutes))
        )
        home_to_job_override = pd.to_numeric(pd.Series([engineer.get("max_home_to_job_min")]), errors="coerce").iloc[0]
        if center_type == base.DMS2_CENTER_TYPE or (pd.notna(home_to_job_override) and float(home_to_job_override) < 0):
            max_home_to_job_min_by_vehicle.append(None)
            max_single_leg_min_by_vehicle.append(None)
        elif pd.notna(home_to_job_override):
            max_home_to_job_min_by_vehicle.append(float(home_to_job_override))
            max_single_leg_min_by_vehicle.append(float(max_single_leg_min) if max_single_leg_min is not None and float(max_single_leg_min) > 0 else None)
        elif not relax_distance_caps_for_feasibility and max_home_to_job_min is not None and float(max_home_to_job_min) > 0:
            max_home_to_job_min_by_vehicle.append(float(max_home_to_job_min))
            max_single_leg_min_by_vehicle.append(float(max_single_leg_min) if max_single_leg_min is not None and float(max_single_leg_min) > 0 else None)
        else:
            max_home_to_job_min_by_vehicle.append(None)
            max_single_leg_min_by_vehicle.append(None if relax_distance_caps_for_feasibility else float(max_single_leg_min) if max_single_leg_min is not None and float(max_single_leg_min) > 0 else None)
    priority_load_enabled = any("priority_group" in col or col in {"target_jobs"} for col in engineer_df.columns)
    total_slot_count = int(sum(job_slots))
    target_slots_by_vehicle = _allocate_priority_targets(priority_group_by_vehicle, max_jobs_by_vehicle, total_slot_count)
    minimum_slots_by_vehicle = (
        _allocate_priority_minimums(priority_group_by_vehicle, max_jobs_by_vehicle, total_slot_count)
        if priority_load_enabled
        else [0] * vehicle_count
    )
    for vehicle_idx, center_type in enumerate(center_type_by_vehicle):
        if center_type == base.DMS2_CENTER_TYPE:
            target_slots_by_vehicle[vehicle_idx] = 0
            minimum_slots_by_vehicle[vehicle_idx] = 0
    has_area_type_routing = any(area_type in {"DMS", "DMS_CORE", "DMS_ONLY", "OVERLAP", "OVERLAB", "DMS2", "DMS2_EXCLUSIVE", "DMS2_ONLY"} for area_type in area_types_by_job)
    fixed_job_nodes: set[int] = set()
    reschedule_job_nodes: set[int] = set()
    fixed_vehicle_indices: set[int] = set()
    fixed_slots_by_vehicle: dict[int, int] = {}
    fixed_service_min_by_vehicle: dict[int, float] = {}
    fixed_job_indices_by_vehicle: dict[int, list[int]] = {}
    if respect_fixed_jobs and "fixed" in job_df.columns:
        for fixed_job_idx, (_, fixed_row) in enumerate(job_df.iterrows()):
            fixed_employee_code = str(fixed_row.get("current_employee_code", "")).strip()
            fixed_vehicle_idx = vehicle_index_by_code.get(fixed_employee_code) if fixed_employee_code else None
            if _coerce_bool_value(fixed_row.get("fixed", False)) and fixed_vehicle_idx is not None:
                fixed_job_nodes.add(int(fixed_job_idx))
                fixed_vehicle_indices.add(int(fixed_vehicle_idx))
                fixed_slots_by_vehicle[int(fixed_vehicle_idx)] = (
                    fixed_slots_by_vehicle.get(int(fixed_vehicle_idx), 0)
                    + int(job_slots[int(fixed_job_idx)])
                )
                fixed_job_indices_by_vehicle.setdefault(int(fixed_vehicle_idx), []).append(int(fixed_job_idx))
                fixed_service_min_by_vehicle[int(fixed_vehicle_idx)] = (
                    fixed_service_min_by_vehicle.get(int(fixed_vehicle_idx), 0.0)
                    + float(service_times[int(fixed_job_idx)])
                )
    for fixed_vehicle_idx, fixed_slots in fixed_slots_by_vehicle.items():
        if 0 <= int(fixed_vehicle_idx) < len(max_jobs_by_vehicle):
            max_jobs_by_vehicle[int(fixed_vehicle_idx)] = max(
                int(max_jobs_by_vehicle[int(fixed_vehicle_idx)]),
                int(fixed_slots),
            ) 
    for fixed_vehicle_idx, fixed_service_min in fixed_service_min_by_vehicle.items():
        if 0 <= int(fixed_vehicle_idx) < len(max_minutes_by_vehicle):
            max_minutes_by_vehicle[int(fixed_vehicle_idx)] = max(
                int(max_minutes_by_vehicle[int(fixed_vehicle_idx)]),
                int(round(float(fixed_service_min))),
            )
    if "reschedule" in job_df.columns:
        for reschedule_job_idx, (_, reschedule_row) in enumerate(job_df.iterrows()):
            if (
                enforce_reschedule_jobs
                and
                _coerce_bool_value(reschedule_row.get("reschedule", False))
                and not _coerce_bool_value(reschedule_row.get("fixed", False))
            ):
                reschedule_job_nodes.add(int(reschedule_job_idx))
    protected_job_nodes = fixed_job_nodes | reschedule_job_nodes

    def _travel_minutes(from_node: int, to_node: int) -> float:
        if from_node in end_nodes:
            return 10_000_000.0
        if to_node in start_nodes:
            return 10_000_000.0
        if from_node in start_nodes and to_node in end_nodes:
            return 0.0
        if to_node in end_nodes:
            return 0.0
        if from_node in start_nodes and to_node < job_count:
            vehicle_idx = from_node - job_count
            return float(duration_mat_min[vehicle_idx][vehicle_count + to_node])
        if from_node < job_count and to_node < job_count:
            return float(duration_mat_min[vehicle_count + from_node][vehicle_count + to_node])
        return 10_000_000.0

    def _travel_km(from_node: int, to_node: int) -> float:
        if from_node in end_nodes:
            return 10_000_000.0
        if to_node in start_nodes:
            return 10_000_000.0
        if from_node in start_nodes and to_node in end_nodes:
            return 0.0
        if to_node in end_nodes:
            return 0.0
        if from_node in start_nodes and to_node < job_count:
            vehicle_idx = from_node - job_count
            return float(distance_mat_km[vehicle_idx][vehicle_count + to_node])
        if from_node < job_count and to_node < job_count:
            return float(distance_mat_km[vehicle_count + from_node][vehicle_count + to_node])
        return 10_000_000.0

    def _return_home_minutes(vehicle_idx: int, from_node: int) -> float:
        if from_node >= job_count or vehicle_idx < 0 or vehicle_idx >= vehicle_count:
            return 0.0
        return float(duration_mat_min[vehicle_count + from_node][vehicle_idx])

    def _return_home_penalty_cost(vehicle_idx: int, from_node: int, to_node: int) -> int:
        if to_node not in end_nodes or from_node >= job_count:
            return 0
        if vehicle_idx in fixed_vehicle_indices:
            return 0
        return_home_min = _return_home_minutes(vehicle_idx, from_node)
        soft_over_min = max(0.0, return_home_min - VRP_RETURN_HOME_FREE_MIN)
        extra_over_min = max(0.0, return_home_min - VRP_RETURN_HOME_SOFT_MIN)
        return int(round(
            soft_over_min * VRP_RETURN_HOME_PENALTY_PER_MIN
            + extra_over_min * VRP_RETURN_HOME_EXTRA_PENALTY_PER_MIN
        ))

    def _is_forward_service_leg(from_node: int, to_node: int) -> bool:
        return to_node < job_count and (from_node in start_nodes or from_node < job_count)

    def _leg_touches_respected_fixed_job(from_node: int, to_node: int) -> bool:
        return (from_node < job_count and from_node in protected_job_nodes) or (to_node < job_count and to_node in protected_job_nodes)

    def _long_leg_penalty_cost(from_node: int, to_node: int) -> int:
        if not _is_forward_service_leg(from_node, to_node):
            return 0
        if _leg_touches_respected_fixed_job(from_node, to_node):
            return 0
        if long_leg_penalty_start_min is None or long_leg_penalty_multiplier is None:
            return 0
        start_min = float(long_leg_penalty_start_min)
        multiplier = float(long_leg_penalty_multiplier)
        if start_min <= 0 or multiplier <= 1:
            return 0
        over_min = max(0.0, _travel_minutes(from_node, to_node) - start_min)
        return int(round(over_min * multiplier * 100))

    def transit_cost_callback(from_index: int, to_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        vehicle_idx = to_node - (job_count + vehicle_count) if to_node in end_nodes else -1
        return (
            int(round(_travel_minutes(from_node, to_node) * 100))
            + _long_leg_penalty_cost(from_node, to_node)
            + _return_home_penalty_cost(vehicle_idx, from_node, to_node)
        )

    def _vehicle_priority_job_bias(vehicle_idx: int, to_node: int) -> int:
        if to_node >= job_count:
            return 0
        priority_group = int(priority_group_by_vehicle[vehicle_idx]) if vehicle_idx < len(priority_group_by_vehicle) else 2
        return VRP_PRIORITY_JOB_BIAS.get(priority_group, 500)

    def _area_type_vehicle_penalty(vehicle_idx: int, to_node: int) -> int:
        if to_node >= job_count:
            return 0
        area_type = area_types_by_job[to_node] if 0 <= to_node < len(area_types_by_job) else ""
        center_type = center_type_by_vehicle[vehicle_idx] if 0 <= vehicle_idx < len(center_type_by_vehicle) else ""
        if area_type in {"DMS", "DMS_CORE", "DMS_ONLY"} and center_type == base.DMS2_CENTER_TYPE:
            return VRP_DMS_AREA_DMS2_FALLBACK_PENALTY_COST
        if area_type in {"OVERLAP", "OVERLAB"} and center_type == base.DMS2_CENTER_TYPE:
            return VRP_OVERLAP_DMS2_PENALTY_COST
        return 0

    def _preferred_region_vehicle_penalty(vehicle_idx: int, to_node: int) -> int:
        if to_node >= job_count:
            return 0
        preferred_region = (
            preferred_region_by_vehicle[vehicle_idx]
            if 0 <= vehicle_idx < len(preferred_region_by_vehicle)
            else ""
        )
        job_region = region_names_by_job[to_node] if 0 <= to_node < len(region_names_by_job) else ""
        if preferred_region and job_region and preferred_region != job_region:
            return VRP_PREFERRED_REGION_MISMATCH_PENALTY_COST
        return 0

    def _is_unrestricted_dms2_vehicle(vehicle_idx: int) -> bool:
        return (
            0 <= int(vehicle_idx) < len(center_type_by_vehicle)
            and center_type_by_vehicle[int(vehicle_idx)] == base.DMS2_CENTER_TYPE
        )

    def _adjusted_service_time(from_node: int, vehicle_idx: int) -> float:
        if from_node >= job_count:
            return 0.0
        multiplier = (
            float(service_time_multiplier_by_vehicle[vehicle_idx])
            if 0 <= vehicle_idx < len(service_time_multiplier_by_vehicle)
            else 1.0
        )
        return float(service_times[from_node]) * multiplier

    def _make_vehicle_cost_callback(vehicle_idx: int):
        def vehicle_cost_callback(from_index: int, to_index: int) -> int:
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return (
                int(round(_travel_minutes(from_node, to_node) * 100))
                + _long_leg_penalty_cost(from_node, to_node)
                + _vehicle_priority_job_bias(vehicle_idx, to_node)
                + _area_type_vehicle_penalty(vehicle_idx, to_node)
                + _preferred_region_vehicle_penalty(vehicle_idx, to_node)
                + _return_home_penalty_cost(vehicle_idx, from_node, to_node)
            )

        return vehicle_cost_callback

    def _make_time_callback(vehicle_idx: int):
        def time_callback(from_index: int, to_index: int) -> int:
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            service_min = _adjusted_service_time(from_node, vehicle_idx)
            return int(round((_travel_minutes(from_node, to_node) + service_min) * 100))

        return time_callback

    time_callback_indices = [
        routing.RegisterTransitCallback(_make_time_callback(vehicle_idx))
        for vehicle_idx in range(vehicle_count)
    ]
    if priority_load_enabled:
        for vehicle_idx in range(vehicle_count):
            routing.SetArcCostEvaluatorOfVehicle(
                routing.RegisterTransitCallback(_make_vehicle_cost_callback(vehicle_idx)),
                vehicle_idx,
            )
            priority_group = int(priority_group_by_vehicle[vehicle_idx]) if vehicle_idx < len(priority_group_by_vehicle) else 2
            routing.SetFixedCostOfVehicle(VRP_PRIORITY_FIXED_COST.get(priority_group, 0), vehicle_idx)
    else:
        transit_callback_index = routing.RegisterTransitCallback(transit_cost_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    soft_work_limit = int(VRP_SOFT_WORK_MIN * 100)
    def _fixed_required_work_floor_min(vehicle_idx: int) -> float:
        fixed_job_indices = list(fixed_job_indices_by_vehicle.get(int(vehicle_idx), []))
        if not fixed_job_indices:
            return 0.0
        remaining = set(fixed_job_indices)
        current_node = job_count + int(vehicle_idx)
        travel_min = 0.0
        while remaining:
            next_job = min(remaining, key=lambda job_idx: _travel_minutes(current_node, int(job_idx)))
            travel_min += float(_travel_minutes(current_node, int(next_job)))
            current_node = int(next_job)
            remaining.remove(int(next_job))
        service_min = sum(float(service_times[int(job_idx)]) for job_idx in fixed_job_indices)
        return float(travel_min + service_min)

    vehicle_hard_work_limits_min: list[float] = []
    for vehicle_idx in range(vehicle_count):
        if _is_unrestricted_dms2_vehicle(vehicle_idx):
            vehicle_hard_work_limits_min.append(float(VRP_UNRESTRICTED_DMS2_WORK_MIN))
            continue
        if vehicle_idx in fixed_vehicle_indices:
            vehicle_hard_work_limits_min.append(float(VRP_UNRESTRICTED_DMS2_WORK_MIN))
            continue
        configured_max = float(max_minutes_by_vehicle[vehicle_idx])
        standard_hard_limit = max(
            configured_max,
            min(configured_max + float(VRP_OVERTIME_ALLOWANCE_MIN), float(VRP_ABSOLUTE_WORK_MIN)),
        )
        fixed_floor = _fixed_required_work_floor_min(vehicle_idx)
        if fixed_floor > 0:
            standard_hard_limit = max(
                standard_hard_limit,
                fixed_floor + float(VRP_FIXED_WORK_BUFFER_MIN),
            )
        vehicle_hard_work_limits_min.append(float(standard_hard_limit))

    absolute_work_limit_min = max(max(vehicle_hard_work_limits_min or [VRP_ABSOLUTE_WORK_MIN]), VRP_SOFT_WORK_MIN)
    absolute_work_limit = int(float(absolute_work_limit_min) * 100)
    routing.AddDimensionWithVehicleTransits(time_callback_indices, 0, absolute_work_limit, True, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")
    time_dimension.SetGlobalSpanCostCoefficient(10 if priority_load_enabled else 100)
    for vehicle_idx in range(vehicle_count):
        hard_vehicle_work_limit = int(round(float(vehicle_hard_work_limits_min[vehicle_idx]) * 100))
        time_dimension.CumulVar(routing.End(vehicle_idx)).SetMax(hard_vehicle_work_limit)
        if _is_unrestricted_dms2_vehicle(vehicle_idx):
            continue
        time_dimension.SetCumulVarSoftUpperBound(
            routing.End(vehicle_idx),
            min(soft_work_limit, int(max_minutes_by_vehicle[vehicle_idx]) * 100),
            VRP_OVERTIME_PENALTY_PER_UNIT,
        )

    if (
        not has_area_type_routing
        and not relax_distance_caps_for_feasibility
        and max_travel_min_per_sm_day is not None
        and float(max_travel_min_per_sm_day) > 0
    ):
        def _make_travel_time_callback(vehicle_idx: int):
            def travel_time_callback(from_index: int, to_index: int) -> int:
                if vehicle_idx in fixed_vehicle_indices or _is_unrestricted_dms2_vehicle(vehicle_idx):
                    return 0
                from_node = manager.IndexToNode(from_index)
                to_node = manager.IndexToNode(to_index)
                return int(round(_travel_minutes(from_node, to_node) * 100))

            return travel_time_callback

        travel_time_callback_indices = [
            routing.RegisterTransitCallback(_make_travel_time_callback(vehicle_idx))
            for vehicle_idx in range(vehicle_count)
        ]
        travel_time_limit = max(1, int(round(float(max_travel_min_per_sm_day) * 100)))
        routing.AddDimensionWithVehicleTransits(travel_time_callback_indices, 0, travel_time_limit, True, "TravelTime")

    if (
        not has_area_type_routing
        and not relax_distance_caps_for_feasibility
        and max_travel_km_per_sm_day is not None
        and float(max_travel_km_per_sm_day) > 0
    ):
        def _make_travel_distance_callback(vehicle_idx: int):
            def travel_distance_callback(from_index: int, to_index: int) -> int:
                if vehicle_idx in fixed_vehicle_indices or _is_unrestricted_dms2_vehicle(vehicle_idx):
                    return 0
                from_node = manager.IndexToNode(from_index)
                to_node = manager.IndexToNode(to_index)
                return int(round(_travel_km(from_node, to_node) * 1000))

            return travel_distance_callback

        travel_distance_callback_indices = [
            routing.RegisterTransitCallback(_make_travel_distance_callback(vehicle_idx))
            for vehicle_idx in range(vehicle_count)
        ]
        travel_distance_limit = max(1, int(round(float(max_travel_km_per_sm_day) * 1000)))
        routing.AddDimensionWithVehicleTransits(travel_distance_callback_indices, 0, travel_distance_limit, True, "TravelDistance")

    if not relax_distance_caps_for_feasibility and max_single_leg_min is not None and float(max_single_leg_min) > 0:
        solver = routing.solver()
        single_leg_limit = float(max_single_leg_min)
        has_unrestricted_dms2 = any(_is_unrestricted_dms2_vehicle(vehicle_idx) for vehicle_idx in range(vehicle_count))
        for vehicle_idx in range(vehicle_count):
            if _is_unrestricted_dms2_vehicle(vehicle_idx):
                continue
            vehicle_single_leg_limit = max_single_leg_min_by_vehicle[vehicle_idx]
            if vehicle_single_leg_limit is None:
                continue
            start_index = routing.Start(vehicle_idx)
            start_node = manager.IndexToNode(start_index)
            for job_idx in range(job_count):
                if area_types_by_job[job_idx] in {"OVERLAP", "OVERLAB"}:
                    continue
                if _leg_touches_respected_fixed_job(start_node, job_idx):
                    continue
                if _travel_minutes(start_node, job_idx) > float(vehicle_single_leg_limit):
                    solver.Add(routing.NextVar(start_index) != manager.NodeToIndex(job_idx))
        if not has_unrestricted_dms2:
            for from_job_idx in range(job_count):
                from_index = manager.NodeToIndex(from_job_idx)
                for to_job_idx in range(job_count):
                    if from_job_idx == to_job_idx:
                        continue
                    if _leg_touches_respected_fixed_job(from_job_idx, to_job_idx):
                        continue
                    if _travel_minutes(from_job_idx, to_job_idx) > single_leg_limit:
                        solver.Add(routing.NextVar(from_index) != manager.NodeToIndex(to_job_idx))

    def slot_count_callback(from_index: int, to_index: int) -> int:
        to_node = manager.IndexToNode(to_index)
        return int(job_slots[to_node]) if to_node < job_count else 0

    slot_count_callback_index = routing.RegisterTransitCallback(slot_count_callback)
    max_route_slots = max(max_jobs_by_vehicle or [8])
    routing.AddDimension(slot_count_callback_index, 0, int(max_route_slots), True, "SlotCount")
    slot_count_dimension = routing.GetDimensionOrDie("SlotCount")
    for vehicle_idx in range(vehicle_count):
        end_var = slot_count_dimension.CumulVar(routing.End(vehicle_idx))
        end_var.SetMax(int(max_jobs_by_vehicle[vehicle_idx]))
        if enforce_priority_minimums and VRP_USE_HARD_PRIORITY_MINIMUMS and int(minimum_slots_by_vehicle[vehicle_idx]) > 0:
            end_var.SetMin(min(int(minimum_slots_by_vehicle[vehicle_idx]), int(max_jobs_by_vehicle[vehicle_idx])))
        target_slots = min(int(target_slots_by_vehicle[vehicle_idx]), int(max_jobs_by_vehicle[vehicle_idx]))
        lower_penalty = VRP_PRIORITY_LOWER_TARGET_PENALTY.get(
            int(priority_group_by_vehicle[vehicle_idx]),
            VRP_TARGET_LOAD_PENALTY_PER_JOB,
        )
        slot_count_dimension.SetCumulVarSoftLowerBound(
            routing.End(vehicle_idx),
            target_slots,
            lower_penalty,
        )
        slot_count_dimension.SetCumulVarSoftUpperBound(
            routing.End(vehicle_idx),
            target_slots,
            VRP_PRIORITY_UPPER_TARGET_PENALTY,
        )

    engineer_lookup = {str(row["SVC_ENGINEER_CODE"]): row for _, row in engineer_df.iterrows()}

    vehicle_has_candidate_job = [False] * vehicle_count
    for job_idx, (_, row) in enumerate(job_df.iterrows()):
        fixed_employee_code = str(row.get("current_employee_code", "")).strip()
        fixed_vehicle_idx = vehicle_index_by_code.get(fixed_employee_code) if fixed_employee_code else None
        is_respected_fixed_job = respect_fixed_jobs and _coerce_bool_value(row.get("fixed", False)) and fixed_vehicle_idx is not None
        is_reschedule_job = enforce_reschedule_jobs and _coerce_bool_value(row.get("reschedule", False)) and not _coerce_bool_value(row.get("fixed", False))
        is_hard_mandatory_job = is_respected_fixed_job
        is_distance_protected_job = is_respected_fixed_job or is_reschedule_job
        if is_respected_fixed_job:
            allowed_vehicle_indices = [fixed_vehicle_idx]
        else:
            candidates_df = base._candidate_engineers(row, engineer_df)
            allowed_codes = set(candidates_df["SVC_ENGINEER_CODE"].astype(str).tolist())
            allowed_vehicle_indices = [int(vehicle_idx) for vehicle_idx, code in enumerate(vehicle_codes) if code in allowed_codes]
            if is_reschedule_job and not allowed_vehicle_indices:
                allowed_vehicle_indices = list(range(vehicle_count))
        had_allowed_vehicle_before_distance_caps = bool(allowed_vehicle_indices)
        if not is_distance_protected_job:
            allowed_vehicle_indices = [
                int(vehicle_idx)
                for vehicle_idx in allowed_vehicle_indices
                if area_types_by_job[job_idx] in {"OVERLAP", "OVERLAB"}
                or max_home_to_job_min_by_vehicle[int(vehicle_idx)] is None
                or float(duration_mat_min[vehicle_idx][vehicle_count + job_idx]) <= float(max_home_to_job_min_by_vehicle[int(vehicle_idx)])
            ]
        elif is_reschedule_job and not is_respected_fixed_job:
            allowed_vehicle_indices = [
                int(vehicle_idx)
                for vehicle_idx in allowed_vehicle_indices
                if 0 <= int(vehicle_idx) < vehicle_count
            ]
        if not allowed_vehicle_indices:
            if had_allowed_vehicle_before_distance_caps:
                routing.AddDisjunction([manager.NodeToIndex(job_idx)], 0)
            continue
        for vehicle_idx in allowed_vehicle_indices:
            vehicle_has_candidate_job[int(vehicle_idx)] = True
        node_index = manager.NodeToIndex(job_idx)
        if is_hard_mandatory_job:
            routing.VehicleVar(node_index).SetValues([int(vehicle_idx) for vehicle_idx in allowed_vehicle_indices])
        elif is_reschedule_job:
            routing.VehicleVar(node_index).SetValues([-1] + [int(vehicle_idx) for vehicle_idx in allowed_vehicle_indices])
            routing.AddDisjunction([manager.NodeToIndex(job_idx)], VRP_RESCHEDULE_JOB_DROP_PENALTY)
        else:
            routing.VehicleVar(node_index).SetValues([-1] + [int(vehicle_idx) for vehicle_idx in allowed_vehicle_indices])
            routing.AddDisjunction([manager.NodeToIndex(job_idx)], VRP_OPTIONAL_JOB_DROP_PENALTY)

    required_vehicle_indices = [
        vehicle_idx
        for vehicle_idx, has_candidate in enumerate(vehicle_has_candidate_job)
        if has_candidate
    ]
    if VRP_REQUIRE_ALL_AVAILABLE_TECHNICIANS and not priority_load_enabled and job_count >= len(required_vehicle_indices):
        solver = routing.solver()
        for vehicle_idx in required_vehicle_indices:
            solver.Add(routing.NextVar(routing.Start(vehicle_idx)) != routing.End(vehicle_idx))

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
    search_params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_params.time_limit.FromSeconds(int(time_limit_seconds))
    solution = routing.SolveWithParameters(search_params)
    if solution is None:
        if enforce_priority_minimums and any(int(value) > 0 for value in minimum_slots_by_vehicle):
            return _solve_vrp_day(
                service_day_df,
                engineer_master_df,
                route_client,
                region_centers,
                time_limit_seconds=time_limit_seconds,
                respect_fixed_jobs=respect_fixed_jobs,
                enforce_priority_minimums=False,
                max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                max_single_leg_min=max_single_leg_min,
                max_home_to_job_min=max_home_to_job_min,
                long_leg_penalty_start_min=long_leg_penalty_start_min,
                long_leg_penalty_multiplier=long_leg_penalty_multiplier,
                enforce_reschedule_jobs=enforce_reschedule_jobs,
                relax_distance_caps_for_feasibility=relax_distance_caps_for_feasibility,
            )
        if enforce_reschedule_jobs:
            return _solve_vrp_day(
                service_day_df,
                engineer_master_df,
                route_client,
                region_centers,
                time_limit_seconds=time_limit_seconds,
                respect_fixed_jobs=respect_fixed_jobs,
                enforce_priority_minimums=False,
                max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                max_single_leg_min=max_single_leg_min,
                max_home_to_job_min=max_home_to_job_min,
                long_leg_penalty_start_min=long_leg_penalty_start_min,
                long_leg_penalty_multiplier=long_leg_penalty_multiplier,
                enforce_reschedule_jobs=False,
                relax_distance_caps_for_feasibility=relax_distance_caps_for_feasibility,
            )
        if not relax_distance_caps_for_feasibility:
            return _solve_vrp_day(
                service_day_df,
                engineer_master_df,
                route_client,
                region_centers,
                time_limit_seconds=time_limit_seconds,
                respect_fixed_jobs=respect_fixed_jobs,
                enforce_priority_minimums=False,
                max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                max_single_leg_min=max_single_leg_min,
                max_home_to_job_min=max_home_to_job_min,
                long_leg_penalty_start_min=long_leg_penalty_start_min,
                long_leg_penalty_multiplier=long_leg_penalty_multiplier,
                enforce_reschedule_jobs=False,
                relax_distance_caps_for_feasibility=True,
            )
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    assignment_rows: list[dict[str, object]] = []
    for vehicle_idx, engineer_code in enumerate(vehicle_codes):
        index = routing.Start(vehicle_idx)
        visit_seq = 0
        while not routing.IsEnd(index):
            next_index = solution.Value(routing.NextVar(index))
            if routing.IsEnd(next_index):
                break
            node = manager.IndexToNode(next_index)
            if node < job_count:
                job_row = job_df.iloc[node]
                engineer_row = engineer_lookup[engineer_code]
                start_coord = engineer_row["start_coord"]
                visit_seq += 1
                row_dict = job_row.to_dict()
                row_dict["assigned_sm_code"] = engineer_code
                row_dict["assigned_sm_name"] = str(engineer_row.get("Name", ""))
                row_dict["assigned_center_type"] = str(engineer_row.get("SVC_CENTER_TYPE", ""))
                row_dict["home_start_longitude"] = start_coord[0] if start_coord is not None else pd.NA
                row_dict["home_start_latitude"] = start_coord[1] if start_coord is not None else pd.NA
                row_dict["vrp_visit_seq"] = visit_seq
                row_dict["_vrp_job_idx"] = int(node)
                row_dict["base_service_time_min"] = round(float(service_times[node]), 2)
                row_dict["service_time_multiplier"] = round(float(service_time_multiplier_by_vehicle[vehicle_idx]), 3)
                row_dict["service_time_min"] = round(_adjusted_service_time(int(node), vehicle_idx), 2)
                row_dict["priority_minimums_relaxed"] = not bool(enforce_priority_minimums)
                row_dict["fixed_capacity_forced"] = False
                row_dict["reschedule_mandatory_relaxed"] = not bool(enforce_reschedule_jobs)
                row_dict["distance_caps_relaxed"] = bool(relax_distance_caps_for_feasibility)
                assignment_rows.append(row_dict)
            index = next_index

    assignment_df = pd.DataFrame(assignment_rows)
    if assignment_df.empty:
        if enforce_priority_minimums and any(int(value) > 0 for value in minimum_slots_by_vehicle):
            return _solve_vrp_day(
                service_day_df,
                engineer_master_df,
                route_client,
                region_centers,
                time_limit_seconds=time_limit_seconds,
                respect_fixed_jobs=respect_fixed_jobs,
                enforce_priority_minimums=False,
                max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                max_single_leg_min=max_single_leg_min,
                max_home_to_job_min=max_home_to_job_min,
                long_leg_penalty_start_min=long_leg_penalty_start_min,
                long_leg_penalty_multiplier=long_leg_penalty_multiplier,
                enforce_reschedule_jobs=enforce_reschedule_jobs,
                relax_distance_caps_for_feasibility=relax_distance_caps_for_feasibility,
            )
        if enforce_reschedule_jobs:
            return _solve_vrp_day(
                service_day_df,
                engineer_master_df,
                route_client,
                region_centers,
                time_limit_seconds=time_limit_seconds,
                respect_fixed_jobs=respect_fixed_jobs,
                enforce_priority_minimums=False,
                max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                max_single_leg_min=max_single_leg_min,
                max_home_to_job_min=max_home_to_job_min,
                long_leg_penalty_start_min=long_leg_penalty_start_min,
                long_leg_penalty_multiplier=long_leg_penalty_multiplier,
                enforce_reschedule_jobs=False,
                relax_distance_caps_for_feasibility=relax_distance_caps_for_feasibility,
            )
        if not relax_distance_caps_for_feasibility:
            return _solve_vrp_day(
                service_day_df,
                engineer_master_df,
                route_client,
                region_centers,
                time_limit_seconds=time_limit_seconds,
                respect_fixed_jobs=respect_fixed_jobs,
                enforce_priority_minimums=False,
                max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                max_single_leg_min=max_single_leg_min,
                max_home_to_job_min=max_home_to_job_min,
                long_leg_penalty_start_min=long_leg_penalty_start_min,
                long_leg_penalty_multiplier=long_leg_penalty_multiplier,
                enforce_reschedule_jobs=False,
                relax_distance_caps_for_feasibility=True,
            )
        return assignment_df, pd.DataFrame(), pd.DataFrame()

    if (
        enforce_priority_minimums
        and any(int(value) > 0 for value in minimum_slots_by_vehicle)
        and int(assignment_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) < int(job_count)
    ):
        relaxed_assignment_df, relaxed_summary_df, relaxed_schedule_df = _solve_vrp_day(
            service_day_df,
            engineer_master_df,
            route_client,
            region_centers,
            time_limit_seconds=time_limit_seconds,
            respect_fixed_jobs=respect_fixed_jobs,
            enforce_priority_minimums=False,
            max_travel_min_per_sm_day=max_travel_min_per_sm_day,
            max_travel_km_per_sm_day=max_travel_km_per_sm_day,
            max_single_leg_min=max_single_leg_min,
            max_home_to_job_min=max_home_to_job_min,
            long_leg_penalty_start_min=long_leg_penalty_start_min,
            long_leg_penalty_multiplier=long_leg_penalty_multiplier,
            enforce_reschedule_jobs=enforce_reschedule_jobs,
            relax_distance_caps_for_feasibility=relax_distance_caps_for_feasibility,
        )
        relaxed_assigned_count = (
            int(relaxed_assignment_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
            if not relaxed_assignment_df.empty and "GSFS_RECEIPT_NO" in relaxed_assignment_df.columns
            else 0
        )
        current_assigned_count = int(assignment_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
        if relaxed_assigned_count > current_assigned_count:
            return relaxed_assignment_df, relaxed_summary_df, relaxed_schedule_df

    if priority_load_enabled:
        engineer_config = {
            str(row["SVC_ENGINEER_CODE"]): {
                "priority_group": int(priority_group_by_vehicle[idx]),
                "priority_group_label": _priority_group_label(priority_group_by_vehicle[idx]),
                "target_slots": int(target_slots_by_vehicle[idx]),
                "minimum_slots": int(minimum_slots_by_vehicle[idx]),
                "max_jobs": int(max_jobs_by_vehicle[idx]),
                "max_slots": int(max_jobs_by_vehicle[idx]),
                "max_minutes": int(max_minutes_by_vehicle[idx]),
                "hard_work_limit_min": round(float(vehicle_hard_work_limits_min[idx]), 2),
                "row": row,
            }
            for idx, (_, row) in enumerate(engineer_df.iterrows())
        }

        def _move_assignment(move_idx: int, target_code: str, target_cfg: dict[str, object]) -> None:
            target_engineer = target_cfg["row"]
            start_coord = target_engineer["start_coord"]
            target_vehicle_idx = vehicle_index_by_code.get(str(target_code).strip(), -1)
            job_idx = pd.to_numeric(pd.Series([assignment_df.at[move_idx, "_vrp_job_idx"]]), errors="coerce").iloc[0]
            assignment_df.at[move_idx, "assigned_sm_code"] = target_code
            assignment_df.at[move_idx, "assigned_sm_name"] = str(target_engineer.get("Name", ""))
            assignment_df.at[move_idx, "assigned_center_type"] = str(target_engineer.get("SVC_CENTER_TYPE", ""))
            assignment_df.at[move_idx, "home_start_longitude"] = start_coord[0] if start_coord is not None else pd.NA
            assignment_df.at[move_idx, "home_start_latitude"] = start_coord[1] if start_coord is not None else pd.NA
            if pd.notna(job_idx) and 0 <= int(job_idx) < len(service_times) and target_vehicle_idx is not None and target_vehicle_idx >= 0:
                assignment_df.at[move_idx, "base_service_time_min"] = round(float(service_times[int(job_idx)]), 2)
                assignment_df.at[move_idx, "service_time_multiplier"] = round(float(service_time_multiplier_by_vehicle[target_vehicle_idx]), 3)
                assignment_df.at[move_idx, "service_time_min"] = round(_adjusted_service_time(int(job_idx), target_vehicle_idx), 2)

        def _row_slot_count(row: pd.Series) -> int:
            value = pd.to_numeric(pd.Series([row.get("job_slot_count", 1)]), errors="coerce").fillna(1).iloc[0]
            return max(1, int(value))

        def _assigned_slots(engineer_code: str) -> int:
            assigned_rows = assignment_df[assignment_df["assigned_sm_code"].astype(str) == engineer_code]
            if assigned_rows.empty:
                return 0
            return int(pd.to_numeric(assigned_rows.get("job_slot_count", pd.Series(1, index=assigned_rows.index)), errors="coerce").fillna(1).astype(int).clip(lower=1).sum())

        def _adjust_row_for_engineer(row: pd.Series, engineer_code: str, visit_seq: int | None = None) -> pd.Series:
            adjusted = row.copy()
            vehicle_idx = vehicle_index_by_code.get(str(engineer_code).strip(), -1)
            job_idx = pd.to_numeric(pd.Series([adjusted.get("_vrp_job_idx")]), errors="coerce").iloc[0]
            adjusted["assigned_sm_code"] = str(engineer_code).strip()
            if visit_seq is not None:
                adjusted["vrp_visit_seq"] = int(visit_seq)
            if pd.notna(job_idx) and 0 <= int(job_idx) < len(service_times) and vehicle_idx is not None and vehicle_idx >= 0:
                adjusted["base_service_time_min"] = round(float(service_times[int(job_idx)]), 2)
                adjusted["service_time_multiplier"] = round(float(service_time_multiplier_by_vehicle[vehicle_idx]), 3)
                adjusted["service_time_min"] = round(_adjusted_service_time(int(job_idx), vehicle_idx), 2)
            return adjusted

        def _working_minutes_for_rows(engineer_code: str, rows_df: pd.DataFrame) -> float:
            if rows_df.empty or "_vrp_job_idx" not in rows_df.columns:
                return 0.0
            vehicle_idx = vehicle_index_by_code.get(str(engineer_code).strip())
            if vehicle_idx is None:
                return 10_000_000.0
            ordered_rows = rows_df.sort_values(["vrp_visit_seq", "GSFS_RECEIPT_NO"])
            job_indices = [
                int(value)
                for value in pd.to_numeric(ordered_rows["_vrp_job_idx"], errors="coerce").dropna().tolist()
            ]
            travel_min = 0.0
            if job_indices:
                travel_min += float(duration_mat_min[vehicle_idx][vehicle_count + job_indices[0]])
                for prev_job_idx, next_job_idx in zip(job_indices[:-1], job_indices[1:]):
                    travel_min += float(duration_mat_min[vehicle_count + prev_job_idx][vehicle_count + next_job_idx])
            service_min = float(pd.to_numeric(ordered_rows.get("service_time_min", pd.Series(0, index=ordered_rows.index)), errors="coerce").fillna(0).sum())
            return float(travel_min + service_min)

        def _fits_work_limit(engineer_code: str, rows_df: pd.DataFrame) -> bool:
            cfg = engineer_config.get(str(engineer_code).strip(), {})
            max_minutes = float(pd.to_numeric(pd.Series([cfg.get("max_minutes", VRP_SOFT_WORK_MIN)]), errors="coerce").fillna(VRP_SOFT_WORK_MIN).iloc[0])
            return _working_minutes_for_rows(str(engineer_code).strip(), rows_df) <= max_minutes + 1e-6

        def _ordered_job_indices_for_rows(rows_df: pd.DataFrame) -> list[int]:
            if rows_df.empty or "_vrp_job_idx" not in rows_df.columns:
                return []
            ordered_rows = rows_df.sort_values(["vrp_visit_seq", "GSFS_RECEIPT_NO"])
            return [
                int(value)
                for value in pd.to_numeric(ordered_rows["_vrp_job_idx"], errors="coerce").dropna().tolist()
            ]

        def _rows_have_respected_fixed_job(rows_df: pd.DataFrame) -> bool:
            if rows_df.empty or "_vrp_job_idx" not in rows_df.columns:
                return False
            return any(job_idx in fixed_job_nodes for job_idx in _ordered_job_indices_for_rows(rows_df))

        def _fits_distance_limits(engineer_code: str, rows_df: pd.DataFrame) -> bool:
            vehicle_idx = vehicle_index_by_code.get(str(engineer_code).strip())
            if vehicle_idx is None:
                return False
            if _rows_have_respected_fixed_job(rows_df):
                return True
            job_indices = _ordered_job_indices_for_rows(rows_df)
            if not job_indices:
                return True
            home_to_job_limit = max_home_to_job_min_by_vehicle[vehicle_idx]
            if home_to_job_limit is not None:
                for job_idx in job_indices:
                    if float(duration_mat_min[vehicle_idx][vehicle_count + job_idx]) > float(home_to_job_limit):
                        return False
            travel_min = float(duration_mat_min[vehicle_idx][vehicle_count + job_indices[0]])
            travel_km = float(distance_mat_km[vehicle_idx][vehicle_count + job_indices[0]])
            vehicle_single_leg_limit = max_single_leg_min_by_vehicle[vehicle_idx]
            if vehicle_single_leg_limit is not None and travel_min > float(vehicle_single_leg_limit):
                return False
            for prev_job_idx, next_job_idx in zip(job_indices[:-1], job_indices[1:]):
                leg_min = float(duration_mat_min[vehicle_count + prev_job_idx][vehicle_count + next_job_idx])
                if max_single_leg_min is not None and float(max_single_leg_min) > 0 and leg_min > float(max_single_leg_min):
                    return False
                travel_min += leg_min
                travel_km += float(distance_mat_km[vehicle_count + prev_job_idx][vehicle_count + next_job_idx])
            if max_travel_min_per_sm_day is not None and float(max_travel_min_per_sm_day) > 0 and travel_min > float(max_travel_min_per_sm_day):
                return False
            if max_travel_km_per_sm_day is not None and float(max_travel_km_per_sm_day) > 0 and travel_km > float(max_travel_km_per_sm_day):
                return False
            return True

        def _fits_route_limits(engineer_code: str, rows_df: pd.DataFrame) -> bool:
            return _fits_work_limit(engineer_code, rows_df) and _fits_distance_limits(engineer_code, rows_df)

        def _best_receiver_visit_seq(receiver_code: str) -> int:
            receiver_rows = assignment_df[assignment_df["assigned_sm_code"].astype(str) == receiver_code]
            if receiver_rows.empty:
                return 1
            return int(pd.to_numeric(receiver_rows["vrp_visit_seq"], errors="coerce").fillna(0).max()) + 1

        def _rows_after_receiving(engineer_code: str, move_row: pd.Series) -> pd.DataFrame:
            receiver_rows = assignment_df[assignment_df["assigned_sm_code"].astype(str) == str(engineer_code).strip()].copy()
            moved_row = _adjust_row_for_engineer(move_row, str(engineer_code).strip(), _best_receiver_visit_seq(str(engineer_code).strip()))
            return pd.concat([receiver_rows, pd.DataFrame([moved_row])], ignore_index=False)

        if VRP_REQUIRE_ALL_AVAILABLE_TECHNICIANS and job_count >= len(required_vehicle_indices):
            required_codes = [vehicle_codes[idx] for idx in required_vehicle_indices]
            fixed_series = (
                assignment_df["fixed"].fillna(False).astype(bool)
                if "fixed" in assignment_df.columns
                else pd.Series(False, index=assignment_df.index)
            )
            for empty_code in required_codes:
                if int((assignment_df["assigned_sm_code"].astype(str) == empty_code).sum()) > 0:
                    continue
                empty_cfg = engineer_config.get(empty_code)
                if not empty_cfg:
                    continue
                donor_codes = [
                    code
                    for code, cfg in sorted(
                        engineer_config.items(),
                        key=lambda item: (
                            -int((assignment_df["assigned_sm_code"].astype(str) == item[0]).sum()),
                            int(item[1]["priority_group"]),
                            str(item[0]),
                        ),
                    )
                    if code != empty_code
                    and _assigned_slots(code) > max(1, int(cfg["minimum_slots"]))
                ]
                moved = False
                for donor_code in donor_codes:
                    donor_mask = assignment_df["assigned_sm_code"].astype(str) == donor_code
                    movable_df = assignment_df[donor_mask & ~fixed_series].copy()
                    if movable_df.empty:
                        continue
                    for move_idx, move_row in movable_df.sort_values("vrp_visit_seq", ascending=False).iterrows():
                        if _assigned_slots(donor_code) - _row_slot_count(move_row) < max(1, int(engineer_config[donor_code]["minimum_slots"])):
                            continue
                        if _assigned_slots(empty_code) + _row_slot_count(move_row) > int(empty_cfg["max_slots"]):
                            continue
                        candidates_df = base._candidate_engineers(move_row, engineer_df)
                        if empty_code not in set(candidates_df["SVC_ENGINEER_CODE"].astype(str)):
                            continue
                        if not _fits_route_limits(empty_code, _rows_after_receiving(empty_code, move_row)):
                            continue
                        _move_assignment(int(move_idx), empty_code, empty_cfg)
                        moved = True
                        break
                    if moved:
                        break

        fixed_series = (
            assignment_df["fixed"].fillna(False).astype(bool)
            if "fixed" in assignment_df.columns
            else pd.Series(False, index=assignment_df.index)
        )

        for low_code, low_cfg in sorted(
            engineer_config.items(),
            key=lambda item: (
                _assigned_slots(item[0]) - int(item[1]["minimum_slots"]),
                int(item[1]["priority_group"]),
                str(item[0]),
            ),
        ):
            while _assigned_slots(low_code) < int(low_cfg["minimum_slots"]):
                if _assigned_slots(low_code) >= int(low_cfg["max_slots"]):
                    break
                donor_codes = [
                    code
                    for code, cfg in sorted(
                        engineer_config.items(),
                        key=lambda item: (
                            -(_assigned_slots(item[0]) - int(item[1]["minimum_slots"])),
                            int(item[1]["priority_group"]),
                            str(item[0]),
                        ),
                    )
                    if code != low_code
                    and _assigned_slots(code) > max(1, int(cfg["minimum_slots"]))
                ]
                moved = False
                for donor_code in donor_codes:
                    donor_mask = assignment_df["assigned_sm_code"].astype(str) == donor_code
                    movable_df = assignment_df[donor_mask & ~fixed_series].copy()
                    if movable_df.empty:
                        continue
                    for move_idx, move_row in movable_df.sort_values("vrp_visit_seq", ascending=False).iterrows():
                        if _assigned_slots(donor_code) - _row_slot_count(move_row) < max(1, int(engineer_config[donor_code]["minimum_slots"])):
                            continue
                        if _assigned_slots(low_code) + _row_slot_count(move_row) > int(low_cfg["max_slots"]):
                            continue
                        candidates_df = base._candidate_engineers(move_row, engineer_df)
                        if low_code not in set(candidates_df["SVC_ENGINEER_CODE"].astype(str)):
                            continue
                        if not _fits_route_limits(low_code, _rows_after_receiving(low_code, move_row)):
                            continue
                        _move_assignment(int(move_idx), low_code, low_cfg)
                        moved = True
                        break
                    if moved:
                        break
                if not moved:
                    break

        for high_code, high_cfg in sorted(
            engineer_config.items(),
            key=lambda item: (-int(item[1]["priority_group"]), str(item[0])),
        ):
            while _assigned_slots(high_code) < int(high_cfg["target_slots"]):
                if _assigned_slots(high_code) >= int(high_cfg["max_slots"]):
                    break
                donor_codes = [
                    code
                    for code, cfg in sorted(engineer_config.items(), key=lambda item: (int(item[1]["priority_group"]), str(item[0])))
                    if int(cfg["priority_group"]) < int(high_cfg["priority_group"])
                    and _assigned_slots(code) > int(cfg["target_slots"])
                ]
                moved = False
                for donor_code in donor_codes:
                    donor_mask = assignment_df["assigned_sm_code"].astype(str) == donor_code
                    fixed_series = (
                        assignment_df["fixed"].fillna(False).astype(bool)
                        if "fixed" in assignment_df.columns
                        else pd.Series(False, index=assignment_df.index)
                    )
                    movable_df = assignment_df[donor_mask & ~fixed_series].copy()
                    if movable_df.empty:
                        continue
                    for move_idx, move_row in movable_df.sort_values("vrp_visit_seq", ascending=False).iterrows():
                        if _assigned_slots(donor_code) - _row_slot_count(move_row) < max(1, int(engineer_config[donor_code]["minimum_slots"])):
                            continue
                        if _assigned_slots(high_code) + _row_slot_count(move_row) > int(high_cfg["max_slots"]):
                            continue
                        candidates_df = base._candidate_engineers(move_row, engineer_df)
                        if high_code not in set(candidates_df["SVC_ENGINEER_CODE"].astype(str)):
                            continue
                        if not _fits_route_limits(high_code, _rows_after_receiving(high_code, move_row)):
                            continue
                        _move_assignment(int(move_idx), high_code, high_cfg)
                        moved = True
                        break
                    if moved:
                        break
                if not moved:
                    break

        def _route_km_for_rows(rows_df: pd.DataFrame) -> float:
            if rows_df.empty or "_vrp_job_idx" not in rows_df.columns:
                return 0.0
            ordered_rows = rows_df.sort_values("vrp_visit_seq")
            job_indices = [
                int(value)
                for value in pd.to_numeric(ordered_rows["_vrp_job_idx"], errors="coerce").dropna().tolist()
            ]
            if len(job_indices) <= 1:
                return 0.0
            total_km = 0.0
            for prev_job_idx, next_job_idx in zip(job_indices[:-1], job_indices[1:]):
                total_km += float(distance_mat_km[vehicle_count + prev_job_idx][vehicle_count + next_job_idx])
            return float(total_km)

        def _best_receiver_visit_seq(receiver_code: str) -> int:
            receiver_rows = assignment_df[assignment_df["assigned_sm_code"].astype(str) == receiver_code]
            if receiver_rows.empty:
                return 1
            return int(pd.to_numeric(receiver_rows["vrp_visit_seq"], errors="coerce").fillna(0).max()) + 1

        for _ in range(vehicle_count):
            route_by_code = {
                code: _route_km_for_rows(assignment_df[assignment_df["assigned_sm_code"].astype(str) == code])
                for code in engineer_config
            }
            positive_routes = [value for value in route_by_code.values() if value > 0]
            if not positive_routes:
                break
            median_route_km = float(pd.Series(positive_routes).median())
            outlier_threshold_km = max(VRP_ROUTE_OUTLIER_MIN_KM, median_route_km * VRP_ROUTE_OUTLIER_FACTOR)
            long_codes = [
                code
                for code, route_km in sorted(route_by_code.items(), key=lambda item: -item[1])
                if route_km > outlier_threshold_km
                and _assigned_slots(code) > max(1, int(engineer_config[code]["minimum_slots"]) - 1)
            ]
            if not long_codes:
                break

            best_move: tuple[float, float, int, str, str] | None = None
            for long_code in long_codes:
                long_mask = assignment_df["assigned_sm_code"].astype(str) == long_code
                donor_rows = assignment_df[long_mask].copy()
                movable_df = donor_rows[~fixed_series.loc[donor_rows.index]].copy()
                if movable_df.empty:
                    continue
                donor_before_km = route_by_code.get(long_code, 0.0)
                for move_idx, move_row in movable_df.sort_values("vrp_visit_seq", ascending=False).iterrows():
                    donor_after_km = _route_km_for_rows(donor_rows.drop(index=move_idx))
                    donor_reduction_km = donor_before_km - donor_after_km
                    if donor_reduction_km <= 0:
                        continue
                    move_slots = _row_slot_count(move_row)
                    if _assigned_slots(long_code) - move_slots < max(1, int(engineer_config[long_code]["minimum_slots"]) - 1):
                        continue
                    candidates_df = base._candidate_engineers(move_row, engineer_df)
                    eligible_codes = set(candidates_df["SVC_ENGINEER_CODE"].astype(str))
                    for receiver_code, receiver_cfg in engineer_config.items():
                        if receiver_code == long_code or receiver_code not in eligible_codes:
                            continue
                        receiver_slots = _assigned_slots(receiver_code)
                        if receiver_slots + move_slots > int(receiver_cfg["max_slots"]):
                            continue
                        if receiver_slots >= int(receiver_cfg["target_slots"]):
                            continue
                        receiver_rows = assignment_df[assignment_df["assigned_sm_code"].astype(str) == receiver_code].copy()
                        receiver_before_km = route_by_code.get(receiver_code, 0.0)
                        moved_row = _adjust_row_for_engineer(move_row, receiver_code, _best_receiver_visit_seq(receiver_code))
                        receiver_candidate_rows = pd.concat([receiver_rows, pd.DataFrame([moved_row])], ignore_index=False)
                        if not _fits_route_limits(receiver_code, receiver_candidate_rows):
                            continue
                        receiver_after_km = _route_km_for_rows(receiver_candidate_rows)
                        total_delta_km = (donor_after_km + receiver_after_km) - (donor_before_km + receiver_before_km)
                        if total_delta_km > VRP_ROUTE_RELIEF_MAX_TOTAL_INCREASE_KM:
                            continue
                        score = total_delta_km - donor_reduction_km
                        candidate = (score, total_delta_km, int(move_idx), long_code, receiver_code)
                        if best_move is None or candidate < best_move:
                            best_move = candidate

            if best_move is None:
                break
            _, _, move_idx, _, receiver_code = best_move
            _move_assignment(int(move_idx), receiver_code, engineer_config[receiver_code])
            assignment_df.at[int(move_idx), "vrp_visit_seq"] = _best_receiver_visit_seq(receiver_code)

    def _route_order_cost(vehicle_idx: int, job_indices: list[int]) -> float:
        if not job_indices:
            return 0.0
        cost = float(duration_mat_min[vehicle_idx][vehicle_count + job_indices[0]])
        for prev_job_idx, next_job_idx in zip(job_indices[:-1], job_indices[1:]):
            cost += float(duration_mat_min[vehicle_count + prev_job_idx][vehicle_count + next_job_idx])
        end_node = job_count + vehicle_count + vehicle_idx
        cost += _return_home_penalty_cost(vehicle_idx, job_indices[-1], end_node) / 100.0
        return float(cost)

    def _reorder_group_for_return_home(group_df: pd.DataFrame) -> pd.DataFrame:
        if group_df.empty or "_vrp_job_idx" not in group_df.columns:
            return group_df
        code = str(group_df["assigned_sm_code"].iloc[0]).strip()
        vehicle_idx = vehicle_index_by_code.get(code)
        if vehicle_idx is None:
            return group_df
        ordered = group_df.sort_values(["vrp_visit_seq", "GSFS_RECEIPT_NO"]).copy()
        job_indices = [
            int(value)
            for value in pd.to_numeric(ordered["_vrp_job_idx"], errors="coerce").dropna().tolist()
        ]
        if len(job_indices) <= 1 or len(job_indices) > VRP_ROUTE_REORDER_MAX_JOBS:
            return ordered
        best_order = job_indices
        best_cost = _route_order_cost(vehicle_idx, job_indices)
        for candidate in itertools.permutations(job_indices):
            candidate_list = list(candidate)
            candidate_cost = _route_order_cost(vehicle_idx, candidate_list)
            if candidate_cost < best_cost:
                best_cost = candidate_cost
                best_order = candidate_list
        order_rank = {job_idx: rank for rank, job_idx in enumerate(best_order)}
        ordered["_route_reorder_rank"] = ordered["_vrp_job_idx"].map(lambda value: order_rank.get(int(value), len(order_rank)))
        ordered = ordered.sort_values(["_route_reorder_rank", "GSFS_RECEIPT_NO"]).drop(columns=["_route_reorder_rank"])
        ordered["vrp_visit_seq"] = range(1, len(ordered) + 1)
        return ordered

    reordered_frames: list[pd.DataFrame] = []
    for _, group_df in assignment_df.groupby("assigned_sm_code", dropna=True):
        reordered_frames.append(_reorder_group_for_return_home(group_df))
    if reordered_frames:
        assignment_df = pd.concat(reordered_frames, ignore_index=True)

    schedule_frames: list[pd.DataFrame] = []
    assignment_df = assignment_df.sort_values(["assigned_sm_code", "vrp_visit_seq", "GSFS_RECEIPT_NO"]).reset_index(drop=True)
    assignment_df["vrp_visit_seq"] = assignment_df.groupby("assigned_sm_code", dropna=False).cumcount() + 1
    return_home_rows: list[dict[str, object]] = []
    for engineer_code, ordered_group_df in assignment_df.groupby("assigned_sm_code", dropna=True):
        code = str(engineer_code).strip()
        vehicle_idx = vehicle_index_by_code.get(code)
        if vehicle_idx is None or ordered_group_df.empty:
            continue
        last_row = ordered_group_df.sort_values("vrp_visit_seq").iloc[-1]
        last_job_idx = pd.to_numeric(pd.Series([last_row.get("_vrp_job_idx")]), errors="coerce").iloc[0]
        if pd.isna(last_job_idx):
            continue
        last_job_idx = int(last_job_idx)
        return_home_rows.append(
            {
                "service_date_key": str(last_row.get("service_date_key", service_day_df["service_date_key"].iloc[0])),
                "assigned_sm_code": code,
                "return_home_duration_min": round(float(duration_mat_min[vehicle_count + last_job_idx][vehicle_idx]), 2),
                "return_home_distance_km": round(float(distance_mat_km[vehicle_count + last_job_idx][vehicle_idx]), 2),
            }
        )
    for _, ordered_group_df in assignment_df.groupby("assigned_sm_code", dropna=True):
        schedule_df, _ = _build_schedule_for_ordered_group(ordered_group_df.copy(), route_client)
        if not schedule_df.empty:
            schedule_frames.append(schedule_df)

    summary_df = base._build_summary_from_assignment(
        assignment_df,
        engineer_df.copy(),
        region_centers,
        str(service_day_df["service_date_key"].iloc[0]),
    )
    if not summary_df.empty:
        load_config_df = pd.DataFrame(
            [
                {
                    "SVC_ENGINEER_CODE": str(row["SVC_ENGINEER_CODE"]),
                    "priority_group": int(priority_group_by_vehicle[idx]),
                    "priority_group_label": _priority_group_label(priority_group_by_vehicle[idx]),
                    "target_slots": int(target_slots_by_vehicle[idx]),
                    "minimum_slots": int(minimum_slots_by_vehicle[idx]),
                    "max_slots": int(max_jobs_by_vehicle[idx]),
                    "max_minutes": int(max_minutes_by_vehicle[idx]),
                    "hard_work_limit_min": round(float(vehicle_hard_work_limits_min[idx]), 2),
                    "service_time_multiplier": round(float(service_time_multiplier_by_vehicle[idx]), 3),
                }
                for idx, (_, row) in enumerate(engineer_df.iterrows())
            ]
        )
        if not load_config_df.empty:
            summary_df = summary_df.merge(load_config_df, on="SVC_ENGINEER_CODE", how="left")
    return_home_df = pd.DataFrame(return_home_rows)
    if not return_home_df.empty:
        summary_df = summary_df.merge(
            return_home_df,
            left_on=["service_date_key", "SVC_ENGINEER_CODE"],
            right_on=["service_date_key", "assigned_sm_code"],
            how="left",
        ).drop(columns=["assigned_sm_code"], errors="ignore")
    schedule_result_df = pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame()
    if not schedule_result_df.empty:
        route_summary_df = (
            schedule_result_df.groupby(["service_date_key", "assigned_sm_code"])
            .agg(route_distance_km=("route_distance_km", "max"), route_duration_min=("route_duration_min", "max"))
            .reset_index()
        )
        summary_df = summary_df.merge(
            route_summary_df,
            left_on=["service_date_key", "SVC_ENGINEER_CODE"],
            right_on=["service_date_key", "assigned_sm_code"],
            how="left",
        ).drop(columns=["assigned_sm_code"], errors="ignore")
        if "route_duration_min" in summary_df.columns:
            summary_df["travel_time_min"] = pd.to_numeric(summary_df["route_duration_min"], errors="coerce").fillna(
                pd.to_numeric(summary_df["travel_time_min"], errors="coerce").fillna(0)
            )
        if "route_distance_km" in summary_df.columns:
            summary_df["travel_distance_km"] = pd.to_numeric(summary_df["route_distance_km"], errors="coerce").fillna(
                pd.to_numeric(summary_df["travel_distance_km"], errors="coerce").fillna(0)
            )
        summary_df["total_work_min"] = (
            pd.to_numeric(summary_df["service_time_min"], errors="coerce").fillna(0)
            + pd.to_numeric(summary_df["travel_time_min"], errors="coerce").fillna(0)
        ).round(2)
        summary_df["return_home_duration_min"] = pd.to_numeric(
            summary_df.get("return_home_duration_min", pd.Series(0, index=summary_df.index)),
            errors="coerce",
        ).fillna(0).round(2)
        summary_df["return_home_distance_km"] = pd.to_numeric(
            summary_df.get("return_home_distance_km", pd.Series(0, index=summary_df.index)),
            errors="coerce",
        ).fillna(0).round(2)
        summary_df["total_day_duration_with_return_min"] = (
            pd.to_numeric(summary_df["total_work_min"], errors="coerce").fillna(0)
            + summary_df["return_home_duration_min"]
        ).round(2)
        summary_df["total_day_distance_with_return_km"] = (
            pd.to_numeric(summary_df["travel_distance_km"], errors="coerce").fillna(0)
            + summary_df["return_home_distance_km"]
        ).round(2)
        summary_df["overtime_min"] = (
            pd.to_numeric(summary_df["total_work_min"], errors="coerce").fillna(0)
            - float(VRP_SOFT_WORK_MIN)
        ).clip(lower=0).round(2)
        summary_df["over_soft_limit"] = summary_df["overtime_min"] > 0
        summary_df["over_absolute_limit"] = summary_df["total_work_min"] > float(VRP_ABSOLUTE_WORK_MIN)
        summary_df["overflow_480"] = summary_df["over_soft_limit"]
    return assignment_df, summary_df, schedule_result_df


def build_atlanta_production_assignment_vrp(
    date_keys: list[str] | None = None,
    output_suffix: str = "vrp_actual_3days",
    attendance_limited: bool = True,
    respect_fixed_jobs: bool = True,
) -> AtlantaProductionVRPAssignmentResult:
    assignment_path, summary_path, schedule_path = _output_paths(output_suffix)
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    if date_keys:
        wanted = {str(v) for v in date_keys}
        service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()

    has_dms2_engineers = (
        "SVC_CENTER_TYPE" in engineer_region_df.columns
        and engineer_region_df["SVC_CENTER_TYPE"].astype(str).str.upper().eq(base.DMS2_CENTER_TYPE).any()
    )
    previous_enable_dms2 = bool(base.ENABLE_DMS2)
    if has_dms2_engineers:
        base.ENABLE_DMS2 = True
    try:
        engineer_master_df = base._build_engineer_master(engineer_region_df.copy(), home_df.copy())
    finally:
        base.ENABLE_DMS2 = previous_enable_dms2
    region_centers = base._region_centers(service_df)
    attendance_master_df, attendance_by_date = base._build_actual_attendance_master(service_df, engineer_master_df)
    route_client = base._build_route_client()

    assignment_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    schedule_frames: list[pd.DataFrame] = []
    for service_date_key, service_day_df in service_df.groupby("service_date_key"):
        day_engineer_master_df = engineer_master_df.copy()
        if attendance_limited:
            allowed_codes = attendance_by_date.get(str(service_date_key), set())
            day_engineer_master_df = attendance_master_df[
                attendance_master_df["SVC_ENGINEER_CODE"].astype(str).isin(allowed_codes)
            ].copy()
            if day_engineer_master_df.empty:
                continue
        assignment_df, summary_df, schedule_df = _solve_vrp_day(
            service_day_df.copy(),
            day_engineer_master_df.copy(),
            route_client,
            region_centers,
            respect_fixed_jobs=respect_fixed_jobs,
        )
        if assignment_df.empty:
            continue
        assignment_frames.append(assignment_df)
        summary_frames.append(summary_df)
        if not schedule_df.empty:
            schedule_frames.append(schedule_df)

    assignment_result_df = pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame()
    summary_result_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()
    schedule_result_df = pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame()

    assignment_result_df.to_csv(assignment_path, index=False, encoding="utf-8-sig")
    summary_result_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    schedule_result_df.to_csv(schedule_path, index=False, encoding="utf-8-sig")
    return AtlantaProductionVRPAssignmentResult(
        assignment_path=assignment_path,
        engineer_day_summary_path=summary_path,
        schedule_path=schedule_path,
    )


def build_atlanta_production_assignment_vrp_from_frames(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    attendance_limited: bool = True,
    time_limit_seconds: int = 20,
    respect_fixed_jobs: bool = True,
    avoid_polygons: list[dict[str, Any]] | None = None,
    avoid_penalty_multiplier: float = 4.0,
    max_work_min_per_sm_day: float | None = None,
    max_travel_min_per_sm_day: float | None = None,
    max_travel_km_per_sm_day: float | None = None,
    max_single_leg_min: float | None = None,
    max_home_to_job_min: float | None = None,
    long_leg_penalty_start_min: float | None = None,
    long_leg_penalty_multiplier: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    working_service_df = service_df.copy()
    if not working_service_df.empty:
        if "service_date" in working_service_df.columns:
            working_service_df["service_date"] = pd.to_datetime(working_service_df["service_date"], errors="coerce")
        if "service_date_key" not in working_service_df.columns and "service_date" in working_service_df.columns:
            working_service_df["service_date_key"] = working_service_df["service_date"].dt.strftime("%Y-%m-%d")
        working_service_df["latitude"] = pd.to_numeric(working_service_df["latitude"], errors="coerce")
        working_service_df["longitude"] = pd.to_numeric(working_service_df["longitude"], errors="coerce")
        working_service_df["service_time_min"] = pd.to_numeric(working_service_df["service_time_min"], errors="coerce").fillna(45)
        working_service_df["is_heavy_repair"] = working_service_df["is_heavy_repair"].map(_coerce_bool_value)
        working_service_df["fixed"] = working_service_df.get("fixed", False)
        working_service_df["fixed"] = working_service_df["fixed"].map(_coerce_bool_value)
        working_service_df["reschedule"] = working_service_df.get("reschedule", False)
        working_service_df["reschedule"] = (
            working_service_df["reschedule"].map(_coerce_bool_value)
            & ~working_service_df["fixed"].map(_coerce_bool_value)
        )
        working_service_df["current_employee_code"] = working_service_df.get(
            "current_employee_code",
            pd.Series("", index=working_service_df.index),
        ).fillna("").astype(str).str.strip()
        working_service_df["is_tv_job"] = working_service_df.get(
            "is_tv_job",
            pd.Series(False, index=working_service_df.index),
        ).fillna(False).astype(bool)

    has_dms2_engineers = (
        "SVC_CENTER_TYPE" in engineer_region_df.columns
        and engineer_region_df["SVC_CENTER_TYPE"].astype(str).str.upper().eq(base.DMS2_CENTER_TYPE).any()
    )
    previous_enable_dms2 = bool(base.ENABLE_DMS2)
    if has_dms2_engineers:
        base.ENABLE_DMS2 = True
    try:
        engineer_master_df = base._build_engineer_master(engineer_region_df.copy(), home_df.copy())
    finally:
        base.ENABLE_DMS2 = previous_enable_dms2
    region_centers = base._region_centers(working_service_df)
    attendance_master_df, attendance_by_date = base._build_actual_attendance_master(working_service_df, engineer_master_df)
    route_client = base._build_route_client()
    route_client.cfg.avoid_polygons = avoid_polygons or []
    route_client.cfg.avoid_penalty_multiplier = float(avoid_penalty_multiplier or 4.0)

    assignment_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    schedule_frames: list[pd.DataFrame] = []
    for service_date_key, service_day_df in working_service_df.groupby("service_date_key"):
        day_engineer_master_df = engineer_master_df.copy()
        if attendance_limited:
            allowed_codes = attendance_by_date.get(str(service_date_key), set())
            day_engineer_master_df = attendance_master_df[
                attendance_master_df["SVC_ENGINEER_CODE"].astype(str).isin(allowed_codes)
            ].copy()
            if day_engineer_master_df.empty:
                continue
        assignment_df, summary_df, schedule_df = _solve_vrp_day(
            service_day_df.copy(),
            day_engineer_master_df.copy(),
            route_client,
            region_centers,
            time_limit_seconds=time_limit_seconds,
            respect_fixed_jobs=respect_fixed_jobs,
            max_travel_min_per_sm_day=max_travel_min_per_sm_day,
            max_travel_km_per_sm_day=max_travel_km_per_sm_day,
            max_single_leg_min=max_single_leg_min,
            max_home_to_job_min=max_home_to_job_min,
            long_leg_penalty_start_min=long_leg_penalty_start_min,
            long_leg_penalty_multiplier=long_leg_penalty_multiplier,
        )
        if schedule_df.empty and not assignment_df.empty:
            schedule_df = assignment_df.copy()
            if "visit_seq" not in schedule_df.columns:
                schedule_df["visit_seq"] = schedule_df.get("vrp_visit_seq", 0)
            schedule_df["visit_seq"] = pd.to_numeric(schedule_df["visit_seq"], errors="coerce").fillna(0).astype(int)
            schedule_df["visit_start_time"] = schedule_df.get("visit_start_time", "")
            schedule_df["visit_end_time"] = schedule_df.get("visit_end_time", "")
            schedule_df["travel_time_from_prev_min"] = pd.to_numeric(
                schedule_df.get("travel_time_from_prev_min", pd.Series(0, index=schedule_df.index)),
                errors="coerce",
            ).fillna(0)
            schedule_df["route_distance_km"] = pd.to_numeric(
                schedule_df.get("route_distance_km", pd.Series(0, index=schedule_df.index)),
                errors="coerce",
            ).fillna(0)
            schedule_df["route_duration_min"] = pd.to_numeric(
                schedule_df.get("route_duration_min", pd.Series(0, index=schedule_df.index)),
                errors="coerce",
            ).fillna(0)
        day_job_count = int(service_day_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
        assigned_count = (
            int(assignment_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
            if not assignment_df.empty and "GSFS_RECEIPT_NO" in assignment_df.columns
            else 0
        )
        fallback_work_min = pd.to_numeric(pd.Series([max_work_min_per_sm_day]), errors="coerce").iloc[0]
        current_max_minutes = pd.to_numeric(
            day_engineer_master_df.get("max_minutes", pd.Series(VRP_SOFT_WORK_MIN, index=day_engineer_master_df.index)),
            errors="coerce",
        ).fillna(VRP_SOFT_WORK_MIN)
        if (
            assigned_count < day_job_count
            and pd.notna(fallback_work_min)
            and float(fallback_work_min) > float(current_max_minutes.max())
        ):
            relaxed_engineer_master_df = day_engineer_master_df.copy()
            relaxed_engineer_master_df["max_minutes"] = current_max_minutes.clip(lower=float(fallback_work_min))
            relaxed_assignment_df, relaxed_summary_df, relaxed_schedule_df = _solve_vrp_day(
                service_day_df.copy(),
                relaxed_engineer_master_df,
                route_client,
                region_centers,
                time_limit_seconds=time_limit_seconds,
                respect_fixed_jobs=respect_fixed_jobs,
                max_travel_min_per_sm_day=max_travel_min_per_sm_day,
                max_travel_km_per_sm_day=max_travel_km_per_sm_day,
                max_single_leg_min=max_single_leg_min,
                max_home_to_job_min=max_home_to_job_min,
                long_leg_penalty_start_min=long_leg_penalty_start_min,
                long_leg_penalty_multiplier=long_leg_penalty_multiplier,
            )
            relaxed_assigned_count = (
                int(relaxed_assignment_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
                if not relaxed_assignment_df.empty and "GSFS_RECEIPT_NO" in relaxed_assignment_df.columns
                else 0
            )
            if relaxed_assigned_count > assigned_count:
                assignment_df, summary_df, schedule_df = relaxed_assignment_df, relaxed_summary_df, relaxed_schedule_df
                if schedule_df.empty and not assignment_df.empty:
                    schedule_df = assignment_df.copy()
                    if "visit_seq" not in schedule_df.columns:
                        schedule_df["visit_seq"] = schedule_df.get("vrp_visit_seq", 0)
                    schedule_df["visit_seq"] = pd.to_numeric(schedule_df["visit_seq"], errors="coerce").fillna(0).astype(int)
                    schedule_df["visit_start_time"] = schedule_df.get("visit_start_time", "")
                    schedule_df["visit_end_time"] = schedule_df.get("visit_end_time", "")
                    schedule_df["travel_time_from_prev_min"] = pd.to_numeric(
                        schedule_df.get("travel_time_from_prev_min", pd.Series(0, index=schedule_df.index)),
                        errors="coerce",
                    ).fillna(0)
                    schedule_df["route_distance_km"] = pd.to_numeric(
                        schedule_df.get("route_distance_km", pd.Series(0, index=schedule_df.index)),
                        errors="coerce",
                    ).fillna(0)
                    schedule_df["route_duration_min"] = pd.to_numeric(
                        schedule_df.get("route_duration_min", pd.Series(0, index=schedule_df.index)),
                        errors="coerce",
                    ).fillna(0)
                if not summary_df.empty:
                    summary_df["work_limit_relaxed_to_min"] = float(fallback_work_min)
        if assignment_df.empty:
            continue
        assignment_frames.append(assignment_df)
        summary_frames.append(summary_df)
        if not schedule_df.empty:
            schedule_frames.append(schedule_df)

    assignment_result_df = pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame()
    summary_result_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()
    schedule_result_df = pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame()
    return assignment_result_df, summary_result_df, schedule_result_df
