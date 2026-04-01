from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

import smart_routing.production_assign_atlanta as base


PRODUCTION_OUTPUT_DIR = Path("260310/production_output")


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
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    job_df = _dedupe_day_jobs(service_day_df)
    if job_df.empty or engineer_master_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    engineer_df = engineer_master_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).copy().reset_index(drop=True)
    engineer_df["start_coord"] = engineer_df.apply(lambda row: base._get_engineer_start_coord(row, region_centers), axis=1)
    engineer_df = engineer_df[engineer_df["start_coord"].notna()].copy().reset_index(drop=True)
    if engineer_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    vehicle_codes = engineer_df["SVC_ENGINEER_CODE"].astype(str).tolist()
    vehicle_count = len(vehicle_codes)
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

    def transit_cost_callback(from_index: int, to_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return int(round(_travel_minutes(from_node, to_node) * 100))

    def time_callback(from_index: int, to_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        service_min = float(service_times[from_node]) if from_node < job_count else 0.0
        return int(round((_travel_minutes(from_node, to_node) + service_min) * 100))

    transit_callback_index = routing.RegisterTransitCallback(transit_cost_callback)
    time_callback_index = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    routing.AddDimension(time_callback_index, 0, int(base.MAX_WORK_MIN * 100), True, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")
    time_dimension.SetGlobalSpanCostCoefficient(100)

    engineer_lookup = {str(row["SVC_ENGINEER_CODE"]): row for _, row in engineer_df.iterrows()}
    for job_idx, (_, row) in enumerate(job_df.iterrows()):
        candidates_df = base._candidate_engineers(row, engineer_df)
        allowed_codes = set(candidates_df["SVC_ENGINEER_CODE"].astype(str).tolist())
        allowed_vehicle_indices = [vehicle_idx for vehicle_idx, code in enumerate(vehicle_codes) if code in allowed_codes]
        if not allowed_vehicle_indices:
            continue
        routing.SetAllowedVehiclesForIndex(allowed_vehicle_indices, manager.NodeToIndex(job_idx))
        routing.AddDisjunction([manager.NodeToIndex(job_idx)], 10_000_000)

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_params.time_limit.FromSeconds(20)
    solution = routing.SolveWithParameters(search_params)
    if solution is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    assignment_rows: list[dict[str, object]] = []
    schedule_frames: list[pd.DataFrame] = []
    for vehicle_idx, engineer_code in enumerate(vehicle_codes):
        index = routing.Start(vehicle_idx)
        visit_seq = 0
        ordered_rows: list[dict[str, object]] = []
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
                ordered_rows.append(row_dict)
                assignment_rows.append(row_dict)
            index = next_index

        if ordered_rows:
            ordered_group_df = pd.DataFrame(ordered_rows)
            schedule_df, _ = _build_schedule_for_ordered_group(ordered_group_df, route_client)
            if not schedule_df.empty:
                schedule_frames.append(schedule_df)

    assignment_df = pd.DataFrame(assignment_rows)
    if assignment_df.empty:
        return assignment_df, pd.DataFrame(), pd.DataFrame()

    summary_df = base._build_summary_from_assignment(
        assignment_df,
        engineer_df.copy(),
        region_centers,
        str(service_day_df["service_date_key"].iloc[0]),
    )
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
        summary_df["overflow_480"] = summary_df["total_work_min"] > base.MAX_WORK_MIN
    return assignment_df, summary_df, schedule_result_df


def build_atlanta_production_assignment_vrp(
    date_keys: list[str] | None = None,
    output_suffix: str = "vrp_actual_3days",
    attendance_limited: bool = True,
) -> AtlantaProductionVRPAssignmentResult:
    assignment_path, summary_path, schedule_path = _output_paths(output_suffix)
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    if date_keys:
        wanted = {str(v) for v in date_keys}
        service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()

    engineer_master_df = base._build_engineer_master(engineer_region_df.copy(), home_df.copy())
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
        working_service_df["is_heavy_repair"] = working_service_df["is_heavy_repair"].fillna(False).astype(bool)
        working_service_df["is_tv_job"] = working_service_df["is_tv_job"].fillna(False).astype(bool)

    engineer_master_df = base._build_engineer_master(engineer_region_df.copy(), home_df.copy())
    region_centers = base._region_centers(working_service_df)
    attendance_master_df, attendance_by_date = base._build_actual_attendance_master(working_service_df, engineer_master_df)
    route_client = base._build_route_client()

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
    return assignment_result_df, summary_result_df, schedule_result_df
