from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

import smart_routing.production_assign_atlanta as base
from smart_routing.routing_compare import _build_region_day_cluster_labels


PRODUCTION_OUTPUT_DIR = Path("260310/production_output")
CLUSTER_PRIMARY_PENALTY_KM = 0.0
CLUSTER_SECONDARY_PENALTY_KM = 8.0
CLUSTER_OUTSIDE_PENALTY_KM = 28.0


@dataclass
class AtlantaProductionOSRMAssignmentResult:
    assignment_path: Path
    engineer_day_summary_path: Path
    schedule_path: Path
    daily_compare_path: Path


def _preference_penalty_km(job_row: pd.Series, engineer_code: str) -> float:
    preferred_code = str(job_row.get("preferred_engineer_code", "")).strip()
    secondary_code = str(job_row.get("secondary_engineer_code", "")).strip()
    engineer_code = str(engineer_code).strip()
    if preferred_code and engineer_code == preferred_code:
        return CLUSTER_PRIMARY_PENALTY_KM
    if secondary_code and engineer_code == secondary_code:
        return CLUSTER_SECONDARY_PENALTY_KM
    if preferred_code or secondary_code:
        return CLUSTER_OUTSIDE_PENALTY_KM
    return 0.0


def _apply_micro_cluster_preferences(
    service_day_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
) -> pd.DataFrame:
    if service_day_df.empty or engineer_master_df.empty:
        return service_day_df.copy()

    working_df = service_day_df.copy()
    working_df["micro_cluster_id"] = ""
    working_df["preferred_engineer_code"] = ""
    working_df["secondary_engineer_code"] = ""

    for region_seq, group_df in working_df.groupby("region_seq", dropna=False):
        region_engineers = engineer_master_df[
            pd.to_numeric(engineer_master_df["assigned_region_seq"], errors="coerce") == pd.to_numeric(pd.Series([region_seq]), errors="coerce").iloc[0]
        ].copy()
        if region_engineers.empty:
            region_engineers = engineer_master_df.copy()
        if region_engineers.empty:
            continue

        cluster_count = max(1, min(len(group_df), len(region_engineers)))
        labels = _build_region_day_cluster_labels(group_df, cluster_count)
        temp_group = group_df.copy()
        temp_group["_micro_cluster_seq"] = labels.astype(int)

        for cluster_seq, cluster_df in temp_group.groupby("_micro_cluster_seq", dropna=False):
            centroid_lon = pd.to_numeric(cluster_df["longitude"], errors="coerce").mean()
            centroid_lat = pd.to_numeric(cluster_df["latitude"], errors="coerce").mean()
            if pd.isna(centroid_lon) or pd.isna(centroid_lat):
                continue
            cluster_coord = (float(centroid_lon), float(centroid_lat))
            ranked_rows: list[tuple[tuple[int, float, str], str]] = []
            for _, engineer in engineer_master_df.iterrows():
                engineer_code = str(engineer["SVC_ENGINEER_CODE"])
                start_coord = base._get_engineer_start_coord(engineer, region_centers)
                if start_coord is None:
                    continue
                home_km = base._haversine_distance_km(start_coord, cluster_coord)
                same_region_rank = 0 if engineer_code in set(region_engineers["SVC_ENGINEER_CODE"].astype(str)) else 1
                ranked_rows.append(((same_region_rank, round(float(home_km), 4), engineer_code), engineer_code))
            if not ranked_rows:
                continue
            ranked_rows.sort(key=lambda item: item[0])
            preferred_code = ranked_rows[0][1]
            secondary_code = ranked_rows[1][1] if len(ranked_rows) > 1 else ""
            cluster_id = f"R{int(pd.to_numeric(pd.Series([region_seq]), errors='coerce').fillna(0).iloc[0]):02d}_C{int(cluster_seq) + 1:02d}"
            working_df.loc[cluster_df.index, "micro_cluster_id"] = cluster_id
            working_df.loc[cluster_df.index, "preferred_engineer_code"] = preferred_code
            working_df.loc[cluster_df.index, "secondary_engineer_code"] = secondary_code

    return working_df


def _matrix_seed_assign_jobs(
    remaining_df: pd.DataFrame,
    active_engineers_df: pd.DataFrame,
    states: dict[str, dict[str, Any]],
    start_lookup: dict[str, tuple[float, float] | None],
    route_client,
    default_anchor_coord: tuple[float, float] | None = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    assignments: list[dict[str, Any]] = []
    unassigned = remaining_df.copy().reset_index(drop=True)
    if unassigned.empty or active_engineers_df.empty:
        return unassigned, assignments

    engineer_rows: list[tuple[pd.Series, tuple[float, float] | None]] = []
    for _, engineer in active_engineers_df.iterrows():
        engineer_code = str(engineer["SVC_ENGINEER_CODE"])
        start_coord = start_lookup.get(engineer_code)
        if start_coord is None and str(engineer.get("SVC_CENTER_TYPE", "")).upper() == base.DMS2_CENTER_TYPE:
            start_coord = default_anchor_coord
        engineer_rows.append((engineer, start_coord))

    candidate_records = list(unassigned.iterrows())
    if not candidate_records:
        return unassigned, assignments

    valid_engineers = [(engineer, coord) for engineer, coord in engineer_rows if coord is not None]
    distance_mat_km: list[list[float]] = []
    if valid_engineers:
        coords = [coord for _, coord in valid_engineers] + [
            (float(row["longitude"]), float(row["latitude"])) for _, row in candidate_records
        ]
        distance_mat_km, _ = route_client.get_distance_duration_matrix(coords)

    used_indices: set[int] = set()
    for engineer, start_coord in engineer_rows:
        if len(used_indices) >= len(candidate_records):
            break
        engineer_code = str(engineer["SVC_ENGINEER_CODE"])
        state = states[engineer_code]
        available_candidates = [(idx, row) for idx, row in candidate_records if idx not in used_indices]
        if not available_candidates:
            break

        chosen_idx: int
        chosen_row: pd.Series
        if start_coord is not None and valid_engineers:
            engineer_center_type = str(engineer.get("SVC_CENTER_TYPE", "")).upper()
            engineer_region = pd.to_numeric(pd.Series([engineer.get("assigned_region_seq")]), errors="coerce").iloc[0]
            engineer_matrix_idx = next(
                i for i, (candidate_engineer, _) in enumerate(valid_engineers)
                if str(candidate_engineer["SVC_ENGINEER_CODE"]) == engineer_code
            )
            scored_candidates: list[tuple[tuple[float, float, str], int, pd.Series]] = []
            for candidate_offset, (idx, row) in enumerate(available_candidates):
                global_candidate_idx = next(i for i, (row_idx, _) in enumerate(candidate_records) if row_idx == idx)
                matrix_col = len(valid_engineers) + global_candidate_idx
                seed_km = float(distance_mat_km[engineer_matrix_idx][matrix_col])
                job_region = pd.to_numeric(pd.Series([row.get("region_seq")]), errors="coerce").iloc[0]
                region_penalty = 0.0
                if pd.notna(engineer_region) and pd.notna(job_region) and int(engineer_region) != int(job_region):
                    region_penalty = (
                        base.SOFT_REGION_DMS2_PENALTY_KM
                        if engineer_center_type == base.DMS2_CENTER_TYPE
                        else base.SOFT_REGION_DMS_PENALTY_KM
                    )
                cluster_penalty = _preference_penalty_km(row, engineer_code)
                score = (
                    round(seed_km + region_penalty + cluster_penalty, 4),
                    -float(pd.to_numeric(pd.Series([row.get("service_time_min")]), errors="coerce").fillna(0).iloc[0]),
                    str(row.get("GSFS_RECEIPT_NO", "")),
                )
                scored_candidates.append((score, idx, row))
            _, chosen_idx, chosen_row = min(scored_candidates, key=lambda item: item[0])
        else:
            fallback_candidates = sorted(
                available_candidates,
                key=lambda item: (
                    -float(pd.to_numeric(pd.Series([item[1].get("service_time_min")]), errors="coerce").fillna(0).iloc[0]),
                    str(item[1].get("GSFS_RECEIPT_NO", "")),
                ),
            )
            chosen_idx, chosen_row = fallback_candidates[0]

        job_coord = (float(chosen_row["longitude"]), float(chosen_row["latitude"]))
        state["current_coord"] = job_coord
        state["service_time_min"] += float(chosen_row["service_time_min"])
        state["job_count"] += 1
        if state["start_coord"] is None:
            state["start_coord"] = start_coord
        if start_coord is not None:
            inc_km, inc_min = route_client.pair_distance(start_coord, job_coord)
            state["travel_distance_km"] += float(inc_km)
            state["travel_time_min"] += float(inc_min)
        job_dict = chosen_row.to_dict()
        job_dict["assigned_sm_code"] = engineer_code
        job_dict["assigned_sm_name"] = state["engineer_name"]
        job_dict["assigned_center_type"] = state["center_type"]
        job_dict["home_start_longitude"] = state["start_coord"][0] if state["start_coord"] is not None else pd.NA
        job_dict["home_start_latitude"] = state["start_coord"][1] if state["start_coord"] is not None else pd.NA
        state["assigned_rows"].append(job_dict)
        assignments.append(job_dict)
        used_indices.add(chosen_idx)

    if used_indices:
        unassigned = unassigned.drop(index=list(used_indices)).reset_index(drop=True)
    return unassigned, assignments


def _state_ordered_route_coords(
    state: dict[str, Any],
    route_client,
) -> list[tuple[float, float]]:
    stop_coords = [
        (float(row["longitude"]), float(row["latitude"]))
        for row in state.get("assigned_rows", [])
        if pd.notna(row.get("longitude")) and pd.notna(row.get("latitude"))
    ]
    start_coord = state.get("start_coord")
    if not stop_coords:
        return [start_coord] if start_coord is not None else []
    coord_chain = [start_coord] + stop_coords if start_coord is not None else stop_coords
    payload = route_client.build_ordered_route(coord_chain, preserve_first=start_coord is not None)
    return [(float(lon), float(lat)) for lon, lat in payload.get("ordered_coords", [])]


def _route_insertion_delta_from_matrix(
    route_count: int,
    job_idx: int,
    distance_mat_km: list[list[float]],
    duration_mat_min: list[list[float]],
) -> tuple[float, float]:
    if route_count <= 0:
        return 0.0, 0.0
    if route_count == 1:
        return float(distance_mat_km[0][job_idx]), float(duration_mat_min[0][job_idx])

    best_delta_km = None
    best_delta_min = None

    for insert_pos in range(1, route_count + 1):
        prev_idx = insert_pos - 1
        next_idx = insert_pos if insert_pos < route_count else None
        delta_km = float(distance_mat_km[prev_idx][job_idx])
        delta_min = float(duration_mat_min[prev_idx][job_idx])
        if next_idx is not None:
            delta_km += float(distance_mat_km[job_idx][next_idx]) - float(distance_mat_km[prev_idx][next_idx])
            delta_min += float(duration_mat_min[job_idx][next_idx]) - float(duration_mat_min[prev_idx][next_idx])
        if best_delta_km is None or delta_km < best_delta_km:
            best_delta_km = delta_km
            best_delta_min = delta_min

    return max(float(best_delta_km or 0.0), 0.0), max(float(best_delta_min or 0.0), 0.0)


def _matrix_grow_assign_jobs(
    remaining_df: pd.DataFrame,
    active_engineers_df: pd.DataFrame,
    states: dict[str, dict[str, Any]],
    route_client,
    target_jobs_per_engineer: dict[str, int],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    assignments: list[dict[str, Any]] = []
    unassigned = remaining_df.copy().reset_index(drop=True)

    while not unassigned.empty and not active_engineers_df.empty:
        active_codes = [str(row["SVC_ENGINEER_CODE"]) for _, row in active_engineers_df.iterrows()]
        current_job_counts = {code: int(states[code]["job_count"]) for code in active_codes}
        min_job_count = min(current_job_counts.values()) if current_job_counts else 0

        route_coords_by_engineer: dict[str, list[tuple[float, float]]] = {}
        for _, engineer in active_engineers_df.iterrows():
            engineer_code = str(engineer["SVC_ENGINEER_CODE"])
            route_coords = _state_ordered_route_coords(states[engineer_code], route_client)
            if route_coords:
                route_coords_by_engineer[engineer_code] = route_coords
        if not route_coords_by_engineer:
            break

        best_move: dict[str, Any] | None = None
        candidate_rows = list(unassigned.iterrows())
        for _, engineer in active_engineers_df.iterrows():
            engineer_code = str(engineer["SVC_ENGINEER_CODE"])
            ordered_route_coords = route_coords_by_engineer.get(engineer_code)
            if not ordered_route_coords:
                continue

            state = states[engineer_code]
            engineer_region = pd.to_numeric(pd.Series([engineer.get("assigned_region_seq")]), errors="coerce").iloc[0]
            engineer_center_type = str(engineer.get("SVC_CENTER_TYPE", "")).upper()
            candidate_coords = [(float(row["longitude"]), float(row["latitude"])) for _, row in candidate_rows]
            coords = ordered_route_coords + candidate_coords
            distance_mat_km, duration_mat_min = route_client.get_distance_duration_matrix(coords)
            route_count = len(ordered_route_coords)

            scored_candidates: list[tuple[tuple[float, float, float, int], int, pd.Series, float, float]] = []
            for candidate_offset, (idx, row) in enumerate(candidate_rows):
                job_idx = route_count + candidate_offset
                inc_km, inc_min = _route_insertion_delta_from_matrix(
                    route_count,
                    job_idx,
                    distance_mat_km,
                    duration_mat_min,
                )

                projected_service = state["service_time_min"] + float(row["service_time_min"])
                projected_travel = state["travel_time_min"] + inc_min
                projected_total = projected_service + projected_travel
                target_jobs = int(target_jobs_per_engineer.get(engineer_code, 4))
                projected_jobs = int(state["job_count"]) + 1
                job_region = pd.to_numeric(pd.Series([row.get("region_seq")]), errors="coerce").iloc[0]
                region_penalty = 0.0
                if pd.notna(engineer_region) and pd.notna(job_region) and int(engineer_region) != int(job_region):
                    region_penalty = (
                        base.SOFT_REGION_DMS2_PENALTY_KM
                        if engineer_center_type == base.DMS2_CENTER_TYPE
                        else base.SOFT_REGION_DMS_PENALTY_KM
                    )
                cluster_penalty = _preference_penalty_km(row, engineer_code)
                over_target_penalty = 0.0
                if projected_jobs > target_jobs:
                    over_target_penalty += 500.0 + ((projected_jobs - target_jobs) * 250.0)
                    if state["job_count"] > min_job_count:
                        over_target_penalty += 1000.0
                fairness_penalty = 0.0
                if projected_jobs > target_jobs:
                    fairness_penalty += max(int(state["job_count"]) - min_job_count, 0) * 120.0
                elif int(state["job_count"]) > min_job_count:
                    fairness_penalty += max(int(state["job_count"]) - min_job_count, 0) * 8.0
                overflow_penalty = max(projected_total - base.MAX_WORK_MIN, 0.0) * 10.0
                score = (
                    round(float(inc_km) + region_penalty + cluster_penalty + over_target_penalty + fairness_penalty + overflow_penalty, 4),
                    round(float(inc_min), 4),
                    round(float(projected_total), 4),
                    projected_jobs,
                )
                scored_candidates.append((score, idx, row, float(inc_km), float(inc_min)))

            if not scored_candidates:
                continue

            score, nearest_idx, job, inc_km, inc_min = min(scored_candidates, key=lambda item: item[0])
            if best_move is None or score < best_move["score"]:
                best_move = {
                    "engineer_code": engineer_code,
                    "job_index": int(nearest_idx),
                    "inc_km": float(inc_km),
                    "inc_min": float(inc_min),
                    "score": score,
                }

        if best_move is None:
            break

        engineer_code = best_move["engineer_code"]
        state = states[engineer_code]
        job = unassigned.loc[best_move["job_index"]]
        job_coord = (float(job["longitude"]), float(job["latitude"]))
        state["travel_distance_km"] += best_move["inc_km"]
        state["travel_time_min"] += best_move["inc_min"]
        state["service_time_min"] += float(job["service_time_min"])
        state["job_count"] += 1
        state["current_coord"] = job_coord
        job_dict = job.to_dict()
        job_dict["assigned_sm_code"] = engineer_code
        job_dict["assigned_sm_name"] = state["engineer_name"]
        job_dict["assigned_center_type"] = state["center_type"]
        job_dict["home_start_longitude"] = state["start_coord"][0] if state["start_coord"] is not None else pd.NA
        job_dict["home_start_latitude"] = state["start_coord"][1] if state["start_coord"] is not None else pd.NA
        state["assigned_rows"].append(job_dict)
        assignments.append(job_dict)
        unassigned = unassigned.drop(index=[best_move["job_index"]]).reset_index(drop=True)

    return unassigned, assignments


def _output_paths(output_suffix: str) -> tuple[Path, Path, Path, Path]:
    suffix = str(output_suffix).strip()
    if not suffix:
        suffix = "osrm"
    return (
        PRODUCTION_OUTPUT_DIR / f"atlanta_assignment_result_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_schedule_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_daily_compare_line_vs_{suffix}.csv",
    )


def _weighted_jobs_std(assignment_df: pd.DataFrame) -> float:
    if assignment_df.empty:
        return 0.0
    weighted_df = assignment_df.copy()
    weighted_df["weighted_job_unit"] = weighted_df["is_heavy_repair"].fillna(False).astype(bool).map(lambda flag: 2.0 if flag else 1.0)
    weighted_jobs = weighted_df.groupby(weighted_df["assigned_sm_code"].astype(str))["weighted_job_unit"].sum()
    weighted_jobs = weighted_jobs[weighted_jobs > 0]
    return float(weighted_jobs.std(ddof=0)) if not weighted_jobs.empty else 0.0


def _daily_metrics(assignment_df: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    if assignment_df.empty or summary_df.empty:
        return pd.DataFrame()
    service_counts = (
        assignment_df.groupby("service_date_key")["GSFS_RECEIPT_NO"]
        .nunique()
        .rename("service_count")
        .reset_index()
    )
    heavy_counts = (
        assignment_df.groupby("service_date_key")["is_heavy_repair"]
        .sum()
        .rename("heavy_repair_count")
        .reset_index()
    )
    tv_counts = (
        assignment_df.groupby("service_date_key")["is_tv_job"]
        .sum()
        .rename("tv_job_count")
        .reset_index()
    )
    service_time = (
        assignment_df.groupby("service_date_key")["service_time_min"]
        .sum()
        .rename("total_service_time_min")
        .reset_index()
    )
    summary = (
        summary_df.groupby("service_date_key")
        .agg(
            assigned_engineer_count=("SVC_ENGINEER_CODE", "nunique"),
            total_distance_km=("route_distance_km", "sum"),
            total_duration_min=("route_duration_min", "sum"),
            avg_distance_km=("route_distance_km", "mean"),
            avg_duration_min=("route_duration_min", "mean"),
            jobs_std=("job_count", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).std(ddof=0))),
            max_total_work_min=("total_work_min", "max"),
            overflow_480_count=("overflow_480", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
        )
        .reset_index()
    )
    weighted_std_rows: list[dict[str, object]] = []
    for service_date_key, group in assignment_df.groupby("service_date_key"):
        weighted_std_rows.append(
            {
                "service_date_key": str(service_date_key),
                "weighted_jobs_std": _weighted_jobs_std(group),
            }
        )
    weighted_std_df = pd.DataFrame(weighted_std_rows)
    return (
        summary.merge(service_counts, on="service_date_key", how="left")
        .merge(heavy_counts, on="service_date_key", how="left")
        .merge(tv_counts, on="service_date_key", how="left")
        .merge(service_time, on="service_date_key", how="left")
        .merge(weighted_std_df, on="service_date_key", how="left")
        .sort_values("service_date_key")
        .reset_index(drop=True)
    )


def build_atlanta_production_assignment_osrm_from_frames(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    service_df: pd.DataFrame,
    attendance_limited: bool = True,
    assignment_strategy: str = "iteration",
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
    border_expansion_zip_map = base._build_border_expansion_zip_map(working_service_df, region_centers)
    attendance_master_df, attendance_by_date = base._build_actual_attendance_master(working_service_df, engineer_master_df)
    route_client = base._build_route_client()

    orig_estimate = base._estimate_incremental_travel
    orig_seed_assign = base._seed_assign_jobs
    orig_grow_assign = base._grow_assign_jobs

    def osrm_estimate(prev_coord, next_coord, route_client_unused=None):
        if prev_coord is None or next_coord is None:
            return 0.0, 0.0
        return route_client.pair_distance(prev_coord, next_coord)

    base._estimate_incremental_travel = osrm_estimate
    base._seed_assign_jobs = _matrix_seed_assign_jobs
    base._grow_assign_jobs = _matrix_grow_assign_jobs

    assignment_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    schedule_frames: list[pd.DataFrame] = []

    try:
        for _, service_day_df in working_service_df.groupby("service_date_key"):
            day_engineer_master_df = engineer_master_df.copy()
            if attendance_limited:
                allowed_codes = attendance_by_date.get(str(service_day_df["service_date_key"].iloc[0]), set())
                day_engineer_master_df = attendance_master_df[
                    attendance_master_df["SVC_ENGINEER_CODE"].astype(str).isin(allowed_codes)
                ].copy()
                if day_engineer_master_df.empty:
                    continue
            day_service_df = service_day_df.copy()
            if assignment_strategy == "cluster_iteration":
                day_service_df = _apply_micro_cluster_preferences(
                    day_service_df,
                    day_engineer_master_df.copy(),
                    region_centers,
                )
            if assignment_strategy == "sequence":
                assignment_df, summary_df = base._assign_day_sequence(
                    day_service_df.copy(),
                    day_engineer_master_df.copy(),
                    region_centers,
                )
            else:
                assignment_df, summary_df = base._assign_day(
                    day_service_df.copy(),
                    day_engineer_master_df.copy(),
                    region_centers,
                    route_client,
                    border_expansion_zip_map,
                )
                if assignment_strategy in {"iteration", "cluster_iteration"}:
                    assignment_df = base._iterative_improve_assignment_df(
                        assignment_df,
                        day_engineer_master_df.copy(),
                        region_centers,
                        route_client=route_client,
                        iterations=4,
                        priority_mode="travel_first",
                    )
                    summary_df = base._build_summary_from_assignment(
                        assignment_df,
                        day_engineer_master_df.copy(),
                        region_centers,
                        str(day_service_df["service_date_key"].iloc[0]),
                        route_client=route_client,
                    )
            if assignment_df.empty:
                continue
            assignment_frames.append(assignment_df)
            summary_frames.append(summary_df)

            for _, group_df in assignment_df.groupby("assigned_sm_code"):
                schedule_df, route_payload = base._build_schedule_for_group(group_df.copy(), route_client)
                if schedule_df.empty:
                    continue
                schedule_df["route_distance_km"] = round(float(route_payload["distance_km"]), 2)
                schedule_df["route_duration_min"] = round(float(route_payload["duration_min"]), 2)
                schedule_frames.append(schedule_df)
    finally:
        base._estimate_incremental_travel = orig_estimate
        base._seed_assign_jobs = orig_seed_assign
        base._grow_assign_jobs = orig_grow_assign

    assignment_result_df = pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame()
    engineer_day_summary_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()
    schedule_result_df = pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame()

    if not schedule_result_df.empty:
        route_summary_df = (
            schedule_result_df.groupby(["service_date_key", "assigned_sm_code"])
            .agg(route_distance_km=("route_distance_km", "max"), route_duration_min=("route_duration_min", "max"))
            .reset_index()
        )
        engineer_day_summary_df = engineer_day_summary_df.merge(
            route_summary_df,
            left_on=["service_date_key", "SVC_ENGINEER_CODE"],
            right_on=["service_date_key", "assigned_sm_code"],
            how="left",
        ).drop(columns=["assigned_sm_code"], errors="ignore")
        if "route_duration_min" in engineer_day_summary_df.columns:
            engineer_day_summary_df["travel_time_min"] = pd.to_numeric(
                engineer_day_summary_df["route_duration_min"], errors="coerce"
            ).fillna(pd.to_numeric(engineer_day_summary_df["travel_time_min"], errors="coerce").fillna(0))
        if "route_distance_km" in engineer_day_summary_df.columns:
            engineer_day_summary_df["travel_distance_km"] = pd.to_numeric(
                engineer_day_summary_df["route_distance_km"], errors="coerce"
            ).fillna(pd.to_numeric(engineer_day_summary_df["travel_distance_km"], errors="coerce").fillna(0))
        engineer_day_summary_df["total_work_min"] = (
            pd.to_numeric(engineer_day_summary_df["service_time_min"], errors="coerce").fillna(0)
            + pd.to_numeric(engineer_day_summary_df["travel_time_min"], errors="coerce").fillna(0)
        ).round(2)
        engineer_day_summary_df["overflow_480"] = engineer_day_summary_df["total_work_min"] > base.MAX_WORK_MIN

    return assignment_result_df, engineer_day_summary_df, schedule_result_df


def build_atlanta_production_assignment_osrm(
    date_keys: list[str] | None = None,
    output_suffix: str = "osrm",
    include_daily_compare: bool = True,
    attendance_limited: bool = False,
    assignment_strategy: str = "grow",
) -> AtlantaProductionOSRMAssignmentResult:
    assignment_path, summary_path, schedule_path, daily_compare_path = _output_paths(output_suffix)
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    engineer_master_df = base._build_engineer_master(engineer_region_df, home_df)
    region_centers = base._region_centers(service_df)
    border_expansion_zip_map = base._build_border_expansion_zip_map(service_df, region_centers)
    attendance_master_df, attendance_by_date = base._build_actual_attendance_master(service_df, engineer_master_df)
    route_client = base._build_route_client()
    if date_keys:
        wanted = {str(v) for v in date_keys}
        service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()

    orig_estimate = base._estimate_incremental_travel
    orig_seed_assign = base._seed_assign_jobs
    orig_grow_assign = base._grow_assign_jobs

    def osrm_estimate(prev_coord, next_coord, route_client_unused=None):
        if prev_coord is None or next_coord is None:
            return 0.0, 0.0
        return route_client.pair_distance(prev_coord, next_coord)

    base._estimate_incremental_travel = osrm_estimate
    base._seed_assign_jobs = _matrix_seed_assign_jobs
    base._grow_assign_jobs = _matrix_grow_assign_jobs

    assignment_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    schedule_frames: list[pd.DataFrame] = []

    try:
        for _, service_day_df in service_df.groupby("service_date_key"):
            day_engineer_master_df = engineer_master_df.copy()
            if attendance_limited:
                allowed_codes = attendance_by_date.get(str(service_day_df["service_date_key"].iloc[0]), set())
                day_engineer_master_df = attendance_master_df[
                    attendance_master_df["SVC_ENGINEER_CODE"].astype(str).isin(allowed_codes)
                ].copy()
                if day_engineer_master_df.empty:
                    continue
            day_service_df = service_day_df.copy()
            if assignment_strategy == "cluster_iteration":
                day_service_df = _apply_micro_cluster_preferences(
                    day_service_df,
                    day_engineer_master_df.copy(),
                    region_centers,
                )
            if assignment_strategy == "sequence":
                assignment_df, summary_df = base._assign_day_sequence(
                    day_service_df.copy(),
                    day_engineer_master_df.copy(),
                    region_centers,
                )
            else:
                assignment_df, summary_df = base._assign_day(
                    day_service_df.copy(),
                    day_engineer_master_df.copy(),
                    region_centers,
                    route_client,
                    border_expansion_zip_map,
                )
                if assignment_strategy in {"iteration", "cluster_iteration"}:
                    assignment_df = base._iterative_improve_assignment_df(
                        assignment_df,
                        day_engineer_master_df.copy(),
                        region_centers,
                        route_client=route_client,
                        iterations=4,
                        priority_mode="travel_first",
                    )
                    summary_df = base._build_summary_from_assignment(
                        assignment_df,
                        day_engineer_master_df.copy(),
                        region_centers,
                        str(day_service_df["service_date_key"].iloc[0]),
                        route_client=route_client,
                    )
            if assignment_df.empty:
                continue
            assignment_frames.append(assignment_df)
            summary_frames.append(summary_df)

            for _, group_df in assignment_df.groupby("assigned_sm_code"):
                schedule_df, route_payload = base._build_schedule_for_group(group_df.copy(), route_client)
                if schedule_df.empty:
                    continue
                schedule_df["route_distance_km"] = round(float(route_payload["distance_km"]), 2)
                schedule_df["route_duration_min"] = round(float(route_payload["duration_min"]), 2)
                schedule_frames.append(schedule_df)
    finally:
        base._estimate_incremental_travel = orig_estimate
        base._seed_assign_jobs = orig_seed_assign
        base._grow_assign_jobs = orig_grow_assign

    assignment_result_df = pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame()
    engineer_day_summary_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()
    schedule_result_df = pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame()

    if not schedule_result_df.empty:
        route_summary_df = (
            schedule_result_df.groupby(["service_date_key", "assigned_sm_code"])
            .agg(route_distance_km=("route_distance_km", "max"), route_duration_min=("route_duration_min", "max"))
            .reset_index()
        )
        engineer_day_summary_df = engineer_day_summary_df.merge(
            route_summary_df,
            left_on=["service_date_key", "SVC_ENGINEER_CODE"],
            right_on=["service_date_key", "assigned_sm_code"],
            how="left",
        ).drop(columns=["assigned_sm_code"], errors="ignore")
        if "route_duration_min" in engineer_day_summary_df.columns:
            engineer_day_summary_df["travel_time_min"] = pd.to_numeric(
                engineer_day_summary_df["route_duration_min"], errors="coerce"
            ).fillna(pd.to_numeric(engineer_day_summary_df["travel_time_min"], errors="coerce").fillna(0))
        if "route_distance_km" in engineer_day_summary_df.columns:
            engineer_day_summary_df["travel_distance_km"] = pd.to_numeric(
                engineer_day_summary_df["route_distance_km"], errors="coerce"
            ).fillna(pd.to_numeric(engineer_day_summary_df["travel_distance_km"], errors="coerce").fillna(0))
        engineer_day_summary_df["total_work_min"] = (
            pd.to_numeric(engineer_day_summary_df["service_time_min"], errors="coerce").fillna(0)
            + pd.to_numeric(engineer_day_summary_df["travel_time_min"], errors="coerce").fillna(0)
        ).round(2)
        engineer_day_summary_df["overflow_480"] = engineer_day_summary_df["total_work_min"] > base.MAX_WORK_MIN

    daily_compare_df = pd.DataFrame()
    if include_daily_compare:
        line_assignment_df = pd.read_csv(base.ASSIGNMENT_PATH, encoding="utf-8-sig", low_memory=False) if base.ASSIGNMENT_PATH.exists() else pd.DataFrame()
        line_summary_df = pd.read_csv(base.ENGINEER_DAY_SUMMARY_PATH, encoding="utf-8-sig", low_memory=False) if base.ENGINEER_DAY_SUMMARY_PATH.exists() else pd.DataFrame()
        for df in [line_assignment_df, engineer_day_summary_df, assignment_result_df, line_summary_df]:
            if not df.empty and "service_date_key" in df.columns:
                df["service_date_key"] = df["service_date_key"].astype(str)
        if date_keys:
            wanted = {str(v) for v in date_keys}
            if not line_assignment_df.empty:
                line_assignment_df = line_assignment_df[line_assignment_df["service_date_key"].astype(str).isin(wanted)].copy()
            if not line_summary_df.empty:
                line_summary_df = line_summary_df[line_summary_df["service_date_key"].astype(str).isin(wanted)].copy()
        line_daily = _daily_metrics(line_assignment_df, line_summary_df)
        osrm_daily = _daily_metrics(assignment_result_df, engineer_day_summary_df)
        if not line_daily.empty or not osrm_daily.empty:
            daily_compare_df = line_daily.merge(osrm_daily, on="service_date_key", how="outer", suffixes=("_line", "_osrm"))
            rename_map = {}
            for col in daily_compare_df.columns:
                if col.endswith("_line"):
                    rename_map[col] = f"line_{col[:-5]}"
                elif col.endswith("_osrm"):
                    rename_map[col] = f"osrm_{col[:-5]}"
            daily_compare_df = daily_compare_df.rename(columns=rename_map).sort_values("service_date_key").reset_index(drop=True)

    PRODUCTION_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    assignment_result_df.to_csv(assignment_path, index=False, encoding="utf-8-sig")
    engineer_day_summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    schedule_result_df.to_csv(schedule_path, index=False, encoding="utf-8-sig")
    daily_compare_df.to_csv(daily_compare_path, index=False, encoding="utf-8-sig")
    return AtlantaProductionOSRMAssignmentResult(
        assignment_path=assignment_path,
        engineer_day_summary_path=summary_path,
        schedule_path=schedule_path,
        daily_compare_path=daily_compare_path,
    )
