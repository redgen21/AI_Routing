from __future__ import annotations

import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from scipy.cluster.vq import kmeans2
from scipy.optimize import linear_sum_assignment

import smart_routing.production_assign_atlanta as base


PRODUCTION_OUTPUT_DIR = Path("260310/production_output")
REGION_PENALTY_MIN = float(base.SOFT_REGION_DMS_PENALTY_KM) / 50.0 * 60.0
CSI_MAX_WORK_WEIGHT = 1.9
CSI_STD_WORK_WEIGHT = 2.7
HYBRID_RELOCATION_SPAN_WEIGHT = 1.3
HYBRID_TRAVEL_BUDGET_RATIO = 0.03
HYBRID_RELOCATION_PASSES = 15
SITS_RELOCATION_SPAN_WEIGHT = 2.0
SITS_RELOCATION_PASSES = 20
RELOCATION_IMPROVEMENT_EPS = 0.01


@dataclass
class AtlantaProductionSequentialAssignmentResult:
    assignment_path: Path
    engineer_day_summary_path: Path
    schedule_path: Path


def _output_paths(output_suffix: str) -> tuple[Path, Path, Path]:
    suffix = str(output_suffix).strip() or "csi"
    return (
        PRODUCTION_OUTPUT_DIR / f"atlanta_assignment_result_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_schedule_{suffix}.csv",
    )


def _dedupe_day_jobs(service_day_df: pd.DataFrame) -> pd.DataFrame:
    if service_day_df.empty:
        return service_day_df.copy()
    df = service_day_df.copy()
    cols = [col for col in ["service_date_key", "GSFS_RECEIPT_NO", "service_time_min"] if col in df.columns]
    df = df.sort_values(cols, ascending=[True, True, False][: len(cols)]).reset_index(drop=True)
    if "GSFS_RECEIPT_NO" in df.columns:
        df = df.drop_duplicates(subset=["GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
    return df


def _prepare_service_df(service_df: pd.DataFrame) -> pd.DataFrame:
    df = service_df.copy()
    if df.empty:
        return df
    if "service_date" in df.columns:
        df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")
    if "service_date_key" not in df.columns and "service_date" in df.columns:
        df["service_date_key"] = df["service_date"].dt.strftime("%Y-%m-%d")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["region_seq"] = pd.to_numeric(df.get("region_seq"), errors="coerce")
    df["service_time_min"] = pd.to_numeric(df.get("service_time_min"), errors="coerce").fillna(45.0)
    if "is_heavy_repair" not in df.columns:
        df["is_heavy_repair"] = df["service_time_min"] >= 100.0
    else:
        df["is_heavy_repair"] = df["is_heavy_repair"].fillna(False).astype(bool)
    df["is_tv_job"] = False
    return df


def _build_day_engineer_master(engineer_master_df, attendance_master_df, attendance_by_date, service_date_key, attendance_limited):
    if not attendance_limited:
        return engineer_master_df.copy().reset_index(drop=True)
    allowed_codes = attendance_by_date.get(str(service_date_key), set())
    if not allowed_codes:
        return attendance_master_df.head(0).copy()
    return attendance_master_df[attendance_master_df["SVC_ENGINEER_CODE"].astype(str).isin(allowed_codes)].copy().reset_index(drop=True)


def _job_coord(job_row) -> tuple[float, float]:
    return (float(job_row["longitude"]), float(job_row["latitude"]))


def _engineer_home_coords(engineer_df: pd.DataFrame, region_centers):
    df = engineer_df.copy()
    df["start_coord"] = df.apply(lambda row: base._get_engineer_start_coord(row, region_centers), axis=1)
    df = df[df["start_coord"].notna()].copy().reset_index(drop=True)
    return df, [tuple(coord) for coord in df["start_coord"].tolist()]


def _build_engineer_lookup(engineer_df: pd.DataFrame) -> dict[str, pd.Series]:
    return {str(row["SVC_ENGINEER_CODE"]): row for _, row in engineer_df.iterrows()}


def _kmeans_cluster_jobs(jobs_df: pd.DataFrame, n_clusters: int):
    if jobs_df.empty or n_clusters <= 0:
        return pd.Series(dtype=int), []
    k = min(int(n_clusters), len(jobs_df))
    if k <= 1:
        centroid = (
            float(pd.to_numeric(jobs_df["longitude"], errors="coerce").mean()),
            float(pd.to_numeric(jobs_df["latitude"], errors="coerce").mean()),
        )
        return pd.Series([0] * len(jobs_df), index=jobs_df.index, dtype=int), [centroid]
    features = jobs_df[["latitude", "longitude"]].to_numpy(dtype=float)
    centroids_array, labels_array = kmeans2(features, k, iter=20, minit="points", seed=1)
    return pd.Series(labels_array, index=jobs_df.index, dtype=int), [(float(lon), float(lat)) for lat, lon in centroids_array]


def _hungarian_match_engineers_to_clusters(engineer_home_coords, cluster_centroids, route_client):
    if not engineer_home_coords or not cluster_centroids:
        return {}
    coords = engineer_home_coords + cluster_centroids
    dist_matrix, _ = route_client.get_distance_duration_matrix(coords)
    n = len(engineer_home_coords)
    cost = [[float(dist_matrix[e][n + c]) for c in range(len(cluster_centroids))] for e in range(n)]
    engineer_indices, cluster_indices = linear_sum_assignment(cost)
    return {int(e): int(c) for e, c in zip(engineer_indices.tolist(), cluster_indices.tolist())}


def _build_job_queue(jobs_df, engineer_cluster_match, cluster_labels, engineer_home_coords):
    if jobs_df.empty:
        return []
    queue, seen = [], set()
    for engineer_idx, cluster_id in sorted(engineer_cluster_match.items(), key=lambda item: item[0]):
        home_coord = engineer_home_coords[int(engineer_idx)]
        cluster_jobs = cluster_labels[cluster_labels == int(cluster_id)].index.tolist()
        ranked = sorted(cluster_jobs, key=lambda idx: (base._haversine_distance_km(home_coord, _job_coord(jobs_df.loc[int(idx)])), int(idx)))
        for idx in ranked:
            idx = int(idx)
            if idx not in seen:
                seen.add(idx)
                queue.append(idx)
    for idx in jobs_df.index.tolist():
        idx = int(idx)
        if idx not in seen:
            seen.add(idx)
            queue.append(idx)
    return queue


def _compute_insertion_delta(route_coords, job_coord, position, dist_matrix, dur_matrix):
    route_len = len(route_coords)
    job_idx = route_len
    prev_idx = position - 1
    if position < route_len:
        next_idx = position
        return (
            float(dist_matrix[prev_idx][job_idx]) + float(dist_matrix[job_idx][next_idx]) - float(dist_matrix[prev_idx][next_idx]),
            float(dur_matrix[prev_idx][job_idx]) + float(dur_matrix[job_idx][next_idx]) - float(dur_matrix[prev_idx][next_idx]),
        )
    return float(dist_matrix[prev_idx][job_idx]), float(dur_matrix[prev_idx][job_idx])


def _compute_removal_delta(route_coords, position, dist_matrix, dur_matrix):
    route_len = len(route_coords)
    prev_idx = position - 1
    if route_len <= 1:
        return 0.0, 0.0
    if position < route_len - 1:
        next_idx = position + 1
        return (
            float(dist_matrix[prev_idx][position]) + float(dist_matrix[position][next_idx]) - float(dist_matrix[prev_idx][next_idx]),
            float(dur_matrix[prev_idx][position]) + float(dur_matrix[position][next_idx]) - float(dur_matrix[prev_idx][next_idx]),
        )
    return float(dist_matrix[prev_idx][position]), float(dur_matrix[prev_idx][position])


def _region_penalty_min(job_row, engineer_row) -> float:
    job_region = pd.to_numeric(pd.Series([job_row.get("region_seq")]), errors="coerce").iloc[0]
    engineer_region = pd.to_numeric(pd.Series([engineer_row.get("assigned_region_seq")]), errors="coerce").iloc[0]
    if pd.isna(job_region) or pd.isna(engineer_region):
        return 0.0
    return REGION_PENALTY_MIN if int(job_region) != int(engineer_region) else 0.0


def _compute_span(work_list) -> float:
    return float(max(work_list) - min(work_list)) if work_list else 0.0


def _work_list(engineer_codes, states):
    return [float(states[str(code)]["total_work_min"]) for code in engineer_codes]


def _compute_std(work_list) -> float:
    return float(statistics.pstdev(work_list)) if len(work_list) > 1 else 0.0


def _global_work_score_delta(work_list, engineer_idx, delta_work_min, delta_travel_min, *, max_weight, std_weight):
    old_max = max(work_list) if work_list else 0.0
    old_std = _compute_std(work_list)
    new_work_list = list(work_list)
    new_work_list[int(engineer_idx)] += float(delta_work_min)
    new_max = max(new_work_list) if new_work_list else 0.0
    new_std = _compute_std(new_work_list)
    return float(delta_travel_min) + float(max_weight) * float(new_max - old_max) + float(std_weight) * float(new_std - old_std)


def _build_states(engineer_df):
    states = {}
    for idx, (_, row) in enumerate(engineer_df.iterrows()):
        code = str(row["SVC_ENGINEER_CODE"])
        home = tuple(row["start_coord"])
        states[code] = {
            "engineer_code": code,
            "engineer_name": str(row.get("Name", "")),
            "center_type": str(row.get("SVC_CENTER_TYPE", "")),
            "assigned_region_seq": row.get("assigned_region_seq"),
            "home_coord": home,
            "home_node": int(idx),
            "route_nodes": [int(idx)],
            "route_coords": [home],
            "job_indices": [],
            "service_time_min": 0.0,
            "travel_time_min": 0.0,
            "travel_distance_km": 0.0,
            "total_work_min": 0.0,
        }
    return states


def _find_best_insertion_for_engineer(
    job_row,
    engineer_row,
    state,
    route_client,
    *,
    enforce_max_work,
    global_dist_matrix=None,
    global_dur_matrix=None,
    job_node=None,
):
    route_coords = list(state["route_coords"])
    route_nodes = list(state.get("route_nodes", []))
    service_time_min = float(job_row.get("service_time_min", 45.0))
    use_global = (
        global_dist_matrix is not None
        and global_dur_matrix is not None
        and job_node is not None
        and len(route_nodes) == len(route_coords)
    )
    if not use_global:
        coords = route_coords + [_job_coord(job_row)]
        dist_matrix, dur_matrix = route_client.get_distance_duration_matrix(coords)
    penalty_min = _region_penalty_min(job_row, engineer_row)
    best = None
    for position in range(1, len(route_coords) + 1):
        if use_global:
            prev_node = route_nodes[position - 1]
            if position < len(route_nodes):
                next_node = route_nodes[position]
                delta_km = float(global_dist_matrix[prev_node][job_node]) + float(global_dist_matrix[job_node][next_node]) - float(global_dist_matrix[prev_node][next_node])
                delta_min = float(global_dur_matrix[prev_node][job_node]) + float(global_dur_matrix[job_node][next_node]) - float(global_dur_matrix[prev_node][next_node])
            else:
                delta_km = float(global_dist_matrix[prev_node][job_node])
                delta_min = float(global_dur_matrix[prev_node][job_node])
        else:
            delta_km, delta_min = _compute_insertion_delta(route_coords, _job_coord(job_row), position, dist_matrix, dur_matrix)
        delta_work_min = float(delta_min) + service_time_min
        new_total = float(state["total_work_min"]) + delta_work_min
        if enforce_max_work and new_total >= float(base.MAX_WORK_MIN):
            continue
        move = {
            "position": int(position),
            "delta_travel_km": float(delta_km),
            "delta_travel_min": float(delta_min),
            "delta_work_min": float(delta_work_min),
            "region_penalty_min": float(penalty_min),
            "new_total_work_min": float(new_total),
        }
        if best is None or (
            move["delta_travel_min"] + move["region_penalty_min"],
            move["new_total_work_min"],
            move["position"],
        ) < (
            best["delta_travel_min"] + best["region_penalty_min"],
            best["new_total_work_min"],
            best["position"],
        ):
            best = move
    return best


def _select_best_insertion(
    job_row,
    engineer_df,
    engineer_lookup,
    states,
    route_client,
    span_weight,
    *,
    global_dist_matrix=None,
    global_dur_matrix=None,
    job_node=None,
    allowed_codes=None,
):
    engineer_codes = engineer_df["SVC_ENGINEER_CODE"].astype(str).tolist()
    work_list = _work_list(engineer_codes, states)
    code_to_idx = {code: idx for idx, code in enumerate(engineer_codes)}
    allowed = set(allowed_codes or [])
    if not allowed:
        allowed = set(base._candidate_engineers(job_row, engineer_df)["SVC_ENGINEER_CODE"].astype(str).tolist()) or set(engineer_codes)
    for enforce in (True, False):
        best = None
        old_span = _compute_span(work_list)
        for code in engineer_codes:
            if code not in allowed:
                continue
            move = _find_best_insertion_for_engineer(
                job_row,
                engineer_lookup[code],
                states[code],
                route_client,
                enforce_max_work=enforce,
                global_dist_matrix=global_dist_matrix,
                global_dur_matrix=global_dur_matrix,
                job_node=job_node,
            )
            if move is None:
                continue
            new_work_list = list(work_list)
            new_work_list[code_to_idx[code]] += float(move["delta_work_min"])
            score = float(move["delta_travel_min"]) + float(move["region_penalty_min"]) + float(span_weight) * (_compute_span(new_work_list) - old_span)
            cand = (code, move | {"score_min": float(score)})
            if best is None or (
                cand[1]["score_min"],
                cand[1]["delta_travel_min"] + cand[1]["region_penalty_min"],
                cand[1]["new_total_work_min"],
                cand[0],
                cand[1]["position"],
            ) < (
                best[1]["score_min"],
                best[1]["delta_travel_min"] + best[1]["region_penalty_min"],
                best[1]["new_total_work_min"],
                best[0],
                best[1]["position"],
            ):
                best = cand
        if best is not None:
            return best
    return None


def _select_best_global_insertion(
    remaining_job_indices,
    job_df,
    engineer_df,
    engineer_lookup,
    states,
    route_client,
    *,
    global_dist_matrix,
    global_dur_matrix,
    job_node_lookup,
    allowed_codes_by_job,
    max_weight,
    std_weight,
):
    engineer_codes = engineer_df["SVC_ENGINEER_CODE"].astype(str).tolist()
    code_to_idx = {code: idx for idx, code in enumerate(engineer_codes)}
    work_list = _work_list(engineer_codes, states)
    best = None
    for job_index in remaining_job_indices:
        job_row = job_df.loc[int(job_index)]
        allowed_codes = set(allowed_codes_by_job.get(int(job_index), set())) or set(engineer_codes)
        for code in engineer_codes:
            if code not in allowed_codes:
                continue
            move = _find_best_insertion_for_engineer(
                job_row,
                engineer_lookup[code],
                states[code],
                route_client,
                enforce_max_work=True,
                global_dist_matrix=global_dist_matrix,
                global_dur_matrix=global_dur_matrix,
                job_node=job_node_lookup[int(job_index)],
            )
            if move is None:
                continue
            score = _global_work_score_delta(
                work_list,
                code_to_idx[code],
                float(move["delta_work_min"]),
                float(move["delta_travel_min"]) + float(move["region_penalty_min"]),
                max_weight=max_weight,
                std_weight=std_weight,
            )
            cand = (
                float(score),
                float(move["delta_travel_min"]) + float(move["region_penalty_min"]),
                float(move["new_total_work_min"]),
                int(job_index),
                str(code),
                int(move["position"]),
            )
            if best is None or cand < best[0]:
                best = (cand, int(job_index), str(code), move | {"score_min": float(score)})
    if best is None:
        return None
    return best[1], best[2], best[3]


def _insert_job(state, job_index, job_row, position, delta_travel_km, delta_travel_min, *, job_node=None):
    state["route_coords"].insert(int(position), _job_coord(job_row))
    if "route_nodes" in state and job_node is not None:
        state["route_nodes"].insert(int(position), int(job_node))
    state["job_indices"].insert(int(position) - 1, int(job_index))
    state["travel_distance_km"] += float(delta_travel_km)
    state["travel_time_min"] += float(delta_travel_min)
    state["service_time_min"] += float(job_row.get("service_time_min", 45.0))
    state["total_work_min"] = float(state["service_time_min"]) + float(state["travel_time_min"])


def _remove_job(state, job_position, service_time_min, removal_km, removal_min):
    removed_job_index = int(state["job_indices"].pop(int(job_position) - 1))
    state["route_coords"].pop(int(job_position))
    if "route_nodes" in state and len(state["route_nodes"]) > int(job_position):
        state["route_nodes"].pop(int(job_position))
    state["travel_distance_km"] -= float(removal_km)
    state["travel_time_min"] -= float(removal_min)
    state["service_time_min"] -= float(service_time_min)
    state["total_work_min"] = float(state["service_time_min"]) + float(state["travel_time_min"])
    return removed_job_index


def _route_cost_from_nodes(home_node, order_nodes, cost_matrix):
    if not order_nodes:
        return 0.0
    total = float(cost_matrix[home_node][order_nodes[0]])
    for idx in range(len(order_nodes) - 1):
        total += float(cost_matrix[order_nodes[idx]][order_nodes[idx + 1]])
    return float(total)


def _route_totals_from_nodes(route_nodes, dist_matrix, dur_matrix):
    total_km = 0.0
    total_min = 0.0
    for idx in range(len(route_nodes) - 1):
        total_km += float(dist_matrix[route_nodes[idx]][route_nodes[idx + 1]])
        total_min += float(dur_matrix[route_nodes[idx]][route_nodes[idx + 1]])
    return float(total_km), float(total_min)


def _total_travel_distance_km(states) -> float:
    return float(sum(float(state.get("travel_distance_km", 0.0)) for state in states.values()))


def _route_cost(order, dist_matrix):
    if not order:
        return 0.0
    total = float(dist_matrix[0][order[0]])
    for idx in range(len(order) - 1):
        total += float(dist_matrix[order[idx]][order[idx + 1]])
    return float(total)


def _optimize_route_order(state, jobs_df, route_client, max_iterations=8, *, global_cost_matrix=None):
    job_indices = [int(job_idx) for job_idx in state["job_indices"]]
    if len(job_indices) <= 2:
        state["route_coords"] = [tuple(state["home_coord"])] + [_job_coord(jobs_df.loc[j]) for j in job_indices]
        return
    use_global = global_cost_matrix is not None and len(state.get("route_nodes", [])) == len(job_indices) + 1
    if use_global:
        home_node = int(state["home_node"])
        order = [int(node) for node in state["route_nodes"][1:]]
        node_to_job = {int(node): int(job_idx) for node, job_idx in zip(state["route_nodes"][1:], job_indices)}
        best_cost = _route_cost_from_nodes(home_node, order, global_cost_matrix)
    else:
        coords = [tuple(state["home_coord"])] + [_job_coord(jobs_df.loc[j]) for j in job_indices]
        _, cost_matrix = route_client.get_distance_duration_matrix(coords)
        order = list(range(1, len(coords)))
        best_cost = _route_cost(order, cost_matrix)
    for _ in range(max_iterations):
        improved = False
        best_order = list(order)
        candidate_cost = best_cost
        for src in range(len(order)):
            node = order[src]
            reduced = order[:src] + order[src + 1 :]
            for dst in range(len(reduced) + 1):
                candidate = reduced[:dst] + [node] + reduced[dst:]
                cost = _route_cost_from_nodes(home_node, candidate, global_cost_matrix) if use_global else _route_cost(candidate, cost_matrix)
                if cost + 1e-9 < candidate_cost:
                    candidate_cost = cost
                    best_order = candidate
                    improved = True
        for start in range(len(order)):
            for end in range(start + 1, len(order)):
                candidate = order[:start] + list(reversed(order[start : end + 1])) + order[end + 1 :]
                cost = _route_cost_from_nodes(home_node, candidate, global_cost_matrix) if use_global else _route_cost(candidate, cost_matrix)
                if cost + 1e-9 < candidate_cost:
                    candidate_cost = cost
                    best_order = candidate
                    improved = True
        if not improved:
            break
        order, best_cost = best_order, candidate_cost
    if use_global:
        state["route_nodes"] = [home_node] + [int(node) for node in order]
        state["job_indices"] = [node_to_job[int(node)] for node in order]
    else:
        state["job_indices"] = [job_indices[node_idx - 1] for node_idx in order]
    state["route_coords"] = [tuple(state["home_coord"])] + [_job_coord(jobs_df.loc[j]) for j in state["job_indices"]]


def _refresh_state(state, jobs_df, route_client, *, global_dist_matrix=None, global_dur_matrix=None):
    state["route_coords"] = [tuple(state["home_coord"])] + [_job_coord(jobs_df.loc[j]) for j in state["job_indices"]]
    use_global = global_dist_matrix is not None and global_dur_matrix is not None and len(state.get("route_nodes", [])) == len(state["route_coords"])
    if use_global and len(state["route_nodes"]) > 1:
        travel_km, travel_min = _route_totals_from_nodes(state["route_nodes"], global_dist_matrix, global_dur_matrix)
        state["travel_distance_km"] = round(float(travel_km), 2)
        state["travel_time_min"] = round(float(travel_min), 2)
    elif len(state["route_coords"]) <= 1:
        state["travel_distance_km"] = 0.0
        state["travel_time_min"] = 0.0
    else:
        dist_matrix, dur_matrix = route_client.get_distance_duration_matrix(list(state["route_coords"]))
        state["travel_distance_km"] = round(sum(float(dist_matrix[i][i + 1]) for i in range(len(state["route_coords"]) - 1)), 2)
        state["travel_time_min"] = round(sum(float(dur_matrix[i][i + 1]) for i in range(len(state["route_coords"]) - 1)), 2)
    state["service_time_min"] = round(float(pd.to_numeric(jobs_df.loc[state["job_indices"], "service_time_min"], errors="coerce").fillna(45.0).sum()) if state["job_indices"] else 0.0, 2)
    state["total_work_min"] = round(float(state["service_time_min"]) + float(state["travel_time_min"]), 2)


def _relocation_pass(
    states,
    jobs_df,
    engineer_df,
    route_client,
    span_weight,
    *,
    global_dist_matrix=None,
    global_dur_matrix=None,
    job_node_lookup=None,
    allowed_codes_by_job=None,
    baseline_total_travel_km=None,
    max_travel_budget_ratio=None,
):
    engineer_codes = engineer_df["SVC_ENGINEER_CODE"].astype(str).tolist()
    engineer_lookup = _build_engineer_lookup(engineer_df)
    code_to_idx = {code: idx for idx, code in enumerate(engineer_codes)}
    work_list = _work_list(engineer_codes, states)
    old_span = _compute_span(work_list)
    best = None
    use_global = global_dist_matrix is not None and global_dur_matrix is not None and job_node_lookup is not None
    current_total_travel_km = _total_travel_distance_km(states)
    max_allowed_travel_km = None
    if baseline_total_travel_km is not None and max_travel_budget_ratio is not None:
        max_allowed_travel_km = float(baseline_total_travel_km) * (1.0 + float(max_travel_budget_ratio))
    for source_code in sorted(engineer_codes, key=lambda code: float(states[code]["total_work_min"]), reverse=True):
        source_state = states[source_code]
        if not source_state["job_indices"]:
            continue
        source_dist = None
        source_dur = None
        if not (use_global and len(source_state.get("route_nodes", [])) == len(source_state["job_indices"]) + 1):
            source_dist, source_dur = route_client.get_distance_duration_matrix(list(source_state["route_coords"]))
        jobs = []
        for pos, job_index in enumerate(source_state["job_indices"], start=1):
            if use_global and len(source_state.get("route_nodes", [])) == len(source_state["job_indices"]) + 1:
                route_nodes = source_state["route_nodes"]
                current_node = int(route_nodes[pos])
                prev_node = int(route_nodes[pos - 1])
                if pos < len(route_nodes) - 1:
                    next_node = int(route_nodes[pos + 1])
                    removal_km = float(global_dist_matrix[prev_node][current_node]) + float(global_dist_matrix[current_node][next_node]) - float(global_dist_matrix[prev_node][next_node])
                    removal_min = float(global_dur_matrix[prev_node][current_node]) + float(global_dur_matrix[current_node][next_node]) - float(global_dur_matrix[prev_node][next_node])
                else:
                    removal_km = float(global_dist_matrix[prev_node][current_node])
                    removal_min = float(global_dur_matrix[prev_node][current_node])
            else:
                removal_km, removal_min = _compute_removal_delta(source_state["route_coords"], pos, source_dist, source_dur)
            job_row = jobs_df.loc[int(job_index)]
            service_time_min = float(job_row.get("service_time_min", 45.0))
            jobs.append({
                "job_index": int(job_index),
                "job_position": int(pos),
                "job_row": job_row,
                "removal_km": float(removal_km),
                "removal_min": float(removal_min),
                "removal_work_min": float(removal_min) + service_time_min,
                "contribution_min": float(removal_min) + service_time_min,
            })
        jobs.sort(key=lambda item: (item["contribution_min"], item["removal_min"]), reverse=True)
        for job in jobs:
            allowed = set(allowed_codes_by_job.get(int(job["job_index"]), set())) if allowed_codes_by_job is not None else set(base._candidate_engineers(job["job_row"], engineer_df)["SVC_ENGINEER_CODE"].astype(str).tolist())
            if not allowed:
                allowed = set(engineer_codes)
            for target_code in engineer_codes:
                if target_code == source_code or target_code not in allowed:
                    continue
                move = _find_best_insertion_for_engineer(
                    job["job_row"],
                    engineer_lookup[target_code],
                    states[target_code],
                    route_client,
                    enforce_max_work=True,
                    global_dist_matrix=global_dist_matrix,
                    global_dur_matrix=global_dur_matrix,
                    job_node=job_node_lookup.get(int(job["job_index"])) if use_global else None,
                )
                if move is None:
                    continue
                new_work_list = list(work_list)
                new_work_list[code_to_idx[source_code]] = max(0.0, new_work_list[code_to_idx[source_code]] - float(job["removal_work_min"]))
                new_work_list[code_to_idx[target_code]] += float(move["delta_work_min"])
                candidate_total_travel_km = float(current_total_travel_km) - float(job["removal_km"]) + float(move["delta_travel_km"])
                if max_allowed_travel_km is not None and candidate_total_travel_km > float(max_allowed_travel_km) + 1e-9:
                    continue
                score = float(move["delta_travel_min"]) + float(move["region_penalty_min"]) - float(job["removal_min"]) + float(span_weight) * (_compute_span(new_work_list) - old_span)
                if score >= -RELOCATION_IMPROVEMENT_EPS:
                    continue
                cand = {
                    "score": float(score),
                    "candidate_total_travel_km": float(candidate_total_travel_km),
                    "source_code": str(source_code),
                    "target_code": str(target_code),
                    "job_index": int(job["job_index"]),
                    "job_position": int(job["job_position"]),
                    "job_row": job["job_row"],
                    "removal_km": float(job["removal_km"]),
                    "removal_min": float(job["removal_min"]),
                    "target_position": int(move["position"]),
                    "delta_travel_km": float(move["delta_travel_km"]),
                    "delta_travel_min": float(move["delta_travel_min"]),
                }
                if best is None or (
                    cand["score"],
                    cand["candidate_total_travel_km"],
                    cand["delta_travel_min"],
                    cand["target_code"],
                ) < (
                    best["score"],
                    best["candidate_total_travel_km"],
                    best["delta_travel_min"],
                    best["target_code"],
                ):
                    best = cand
    if best is None:
        return False
    source_state = states[best["source_code"]]
    removed_job_index = _remove_job(source_state, best["job_position"], float(best["job_row"].get("service_time_min", 45.0)), best["removal_km"], best["removal_min"])
    _insert_job(
        states[best["target_code"]],
        removed_job_index,
        best["job_row"],
        best["target_position"],
        best["delta_travel_km"],
        best["delta_travel_min"],
        job_node=job_node_lookup.get(int(removed_job_index)) if use_global else None,
    )
    _optimize_route_order(states[best["source_code"]], jobs_df, route_client, global_cost_matrix=global_dur_matrix)
    _optimize_route_order(states[best["target_code"]], jobs_df, route_client, global_cost_matrix=global_dur_matrix)
    _refresh_state(states[best["source_code"]], jobs_df, route_client, global_dist_matrix=global_dist_matrix, global_dur_matrix=global_dur_matrix)
    _refresh_state(states[best["target_code"]], jobs_df, route_client, global_dist_matrix=global_dist_matrix, global_dur_matrix=global_dur_matrix)
    return True


def _build_assignment_df(jobs_df, engineer_df, states):
    rows = []
    for _, engineer_row in engineer_df.iterrows():
        code = str(engineer_row["SVC_ENGINEER_CODE"])
        state = states[code]
        home = tuple(state["home_coord"])
        for visit_seq, job_index in enumerate(state["job_indices"], start=1):
            row = jobs_df.loc[int(job_index)].to_dict()
            row["assigned_sm_code"] = code
            row["assigned_sm_name"] = state["engineer_name"]
            row["assigned_center_type"] = state["center_type"]
            row["home_start_longitude"] = home[0]
            row["home_start_latitude"] = home[1]
            row["route_visit_seq"] = int(visit_seq)
            rows.append(row)
    return pd.DataFrame(rows)


def _build_summary_df(engineer_df, states, service_date_key):
    rows = []
    for _, engineer_row in engineer_df.iterrows():
        code = str(engineer_row["SVC_ENGINEER_CODE"])
        state = states[code]
        rows.append({
            "service_date_key": str(service_date_key),
            "SVC_ENGINEER_CODE": code,
            "SVC_ENGINEER_NAME": state["engineer_name"],
            "assigned_center_type": state["center_type"],
            "assigned_region_seq": engineer_row.get("assigned_region_seq"),
            "job_count": int(len(state["job_indices"])),
            "service_time_min": round(float(state["service_time_min"]), 2),
            "travel_time_min": round(float(state["travel_time_min"]), 2),
            "travel_distance_km": round(float(state["travel_distance_km"]), 2),
            "total_work_min": round(float(state["total_work_min"]), 2),
            "overflow_480": bool(float(state["total_work_min"]) > float(base.MAX_WORK_MIN)),
        })
    return pd.DataFrame(rows)


def _build_schedule_from_assignment(assignment_df, engineer_df, route_client):
    if assignment_df.empty:
        return pd.DataFrame()
    frames = []
    for _, engineer_row in engineer_df.iterrows():
        code = str(engineer_row["SVC_ENGINEER_CODE"])
        group_df = assignment_df[assignment_df["assigned_sm_code"].astype(str) == code].copy()
        if group_df.empty:
            continue
        group_df = group_df.sort_values("route_visit_seq").reset_index(drop=True)
        first = group_df.iloc[0]
        start_coord = (float(first["home_start_longitude"]), float(first["home_start_latitude"]))
        coord_chain = [start_coord] + [(float(row["longitude"]), float(row["latitude"])) for _, row in group_df.iterrows()]
        dist_matrix, dur_matrix = route_client.get_distance_duration_matrix(coord_chain)
        route_distance_km = round(sum(float(dist_matrix[i][i + 1]) for i in range(len(coord_chain) - 1)), 2)
        route_duration_min = round(sum(float(dur_matrix[i][i + 1]) for i in range(len(coord_chain) - 1)), 2)
        base_date = pd.to_datetime(str(group_df["service_date_key"].iloc[0]), errors="coerce")
        if pd.isna(base_date):
            base_date = pd.Timestamp("2026-01-01")
        current_time = base_date.replace(hour=base.DAY_START_HOUR, minute=0, second=0, microsecond=0)
        lunch_taken = False
        lunch_start = base_date.replace(hour=base.LUNCH_WINDOW_START_HOUR, minute=base.LUNCH_WINDOW_START_MIN, second=0, microsecond=0)
        lunch_end = base_date.replace(hour=base.LUNCH_WINDOW_END_HOUR, minute=base.LUNCH_WINDOW_END_MIN, second=0, microsecond=0)
        rows = []
        for idx, row in enumerate(group_df.to_dict("records"), start=1):
            travel_min = float(dur_matrix[idx - 1][idx])
            arrival = current_time + pd.Timedelta(minutes=travel_min)
            lunch_flag = False
            if not lunch_taken and lunch_start <= arrival <= lunch_end:
                arrival += pd.Timedelta(minutes=base.LUNCH_DURATION_MIN)
                lunch_taken = True
                lunch_flag = True
            start_time = arrival
            end_time = start_time + pd.Timedelta(minutes=float(row.get("service_time_min", 45.0)))
            if not lunch_taken and lunch_start <= end_time <= lunch_end:
                current_time = end_time + pd.Timedelta(minutes=base.LUNCH_DURATION_MIN)
                lunch_taken = True
                lunch_flag = True
            else:
                current_time = end_time
            row["visit_seq"] = int(idx)
            row["travel_time_from_prev_min"] = round(travel_min, 2)
            row["visit_start_time"] = base._fmt_dt(start_time)
            row["visit_end_time"] = base._fmt_dt(end_time)
            row["lunch_applied"] = lunch_flag
            row["route_distance_km"] = route_distance_km
            row["route_duration_min"] = route_duration_min
            rows.append(row)
        frames.append(pd.DataFrame(rows))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _solve_day_assignment(
    service_day_df,
    engineer_master_df,
    route_client,
    region_centers,
    *,
    enable_targeted_swap=False,
    relocation_span_weight=None,
    relocation_passes=None,
    max_travel_budget_ratio=None,
):
    job_df = _dedupe_day_jobs(service_day_df)
    if job_df.empty or engineer_master_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    engineer_df, engineer_home_coords = _engineer_home_coords(engineer_master_df, region_centers)
    if engineer_df.empty or not engineer_home_coords:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    engineer_lookup = _build_engineer_lookup(engineer_df)
    states = _build_states(engineer_df)
    cluster_labels, centroids = _kmeans_cluster_jobs(job_df, len(engineer_df))
    engineer_cluster_match = _hungarian_match_engineers_to_clusters(engineer_home_coords, centroids, route_client)
    job_queue = _build_job_queue(job_df, engineer_cluster_match, cluster_labels, engineer_home_coords)
    global_coords = list(engineer_home_coords) + [_job_coord(job_df.loc[int(job_index)]) for job_index in job_df.index.tolist()]
    global_dist_matrix, global_dur_matrix = route_client.get_distance_duration_matrix(global_coords)
    job_node_lookup = {int(job_index): len(engineer_home_coords) + offset for offset, job_index in enumerate(job_df.index.tolist())}
    engineer_codes = engineer_df["SVC_ENGINEER_CODE"].astype(str).tolist()
    allowed_codes_by_job = {}
    for job_index in job_df.index.tolist():
        allowed = set(base._candidate_engineers(job_df.loc[int(job_index)], engineer_df)["SVC_ENGINEER_CODE"].astype(str).tolist())
        allowed_codes_by_job[int(job_index)] = allowed or set(engineer_codes)
    remaining_job_indices = list(job_queue)
    while remaining_job_indices:
        best_choice = _select_best_global_insertion(
            remaining_job_indices,
            job_df,
            engineer_df,
            engineer_lookup,
            states,
            route_client,
            global_dist_matrix=global_dist_matrix,
            global_dur_matrix=global_dur_matrix,
            job_node_lookup=job_node_lookup,
            allowed_codes_by_job=allowed_codes_by_job,
            max_weight=CSI_MAX_WORK_WEIGHT,
            std_weight=CSI_STD_WORK_WEIGHT,
        )
        if best_choice is None:
            break
        job_index, code, move = best_choice
        job_row = job_df.loc[int(job_index)]
        _insert_job(
            states[code],
            int(job_index),
            job_row,
            int(move["position"]),
            float(move["delta_travel_km"]),
            float(move["delta_travel_min"]),
            job_node=job_node_lookup[int(job_index)],
        )
        _optimize_route_order(states[code], job_df, route_client, global_cost_matrix=global_dur_matrix)
        _refresh_state(states[code], job_df, route_client, global_dist_matrix=global_dist_matrix, global_dur_matrix=global_dur_matrix)
        remaining_job_indices.remove(int(job_index))
    for state in states.values():
        _optimize_route_order(state, job_df, route_client, global_cost_matrix=global_dur_matrix)
        _refresh_state(state, job_df, route_client, global_dist_matrix=global_dist_matrix, global_dur_matrix=global_dur_matrix)
    if enable_targeted_swap:
        baseline_total_travel_km = _total_travel_distance_km(states)
        effective_span_weight = float(SITS_RELOCATION_SPAN_WEIGHT if relocation_span_weight is None else relocation_span_weight)
        effective_relocation_passes = int(SITS_RELOCATION_PASSES if relocation_passes is None else relocation_passes)
        for _ in range(effective_relocation_passes):
            if not _relocation_pass(
                states,
                job_df,
                engineer_df,
                route_client,
                effective_span_weight,
                global_dist_matrix=global_dist_matrix,
                global_dur_matrix=global_dur_matrix,
                job_node_lookup=job_node_lookup,
                allowed_codes_by_job=allowed_codes_by_job,
                baseline_total_travel_km=baseline_total_travel_km,
                max_travel_budget_ratio=max_travel_budget_ratio,
            ):
                break
    for state in states.values():
        _optimize_route_order(state, job_df, route_client, global_cost_matrix=global_dur_matrix)
        _refresh_state(state, job_df, route_client, global_dist_matrix=global_dist_matrix, global_dur_matrix=global_dur_matrix)
    assignment_df = _build_assignment_df(job_df, engineer_df, states)
    summary_df = _build_summary_df(engineer_df, states, str(job_df["service_date_key"].iloc[0]))
    schedule_df = _build_schedule_from_assignment(assignment_df, engineer_df, route_client)
    if not schedule_df.empty:
        route_summary_df = (
            schedule_df.groupby(["service_date_key", "assigned_sm_code"])
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
                pd.to_numeric(summary_df["travel_time_min"], errors="coerce").fillna(0.0)
            )
        if "route_distance_km" in summary_df.columns:
            summary_df["travel_distance_km"] = pd.to_numeric(summary_df["route_distance_km"], errors="coerce").fillna(
                pd.to_numeric(summary_df["travel_distance_km"], errors="coerce").fillna(0.0)
            )
        summary_df["total_work_min"] = (
            pd.to_numeric(summary_df["service_time_min"], errors="coerce").fillna(0.0)
            + pd.to_numeric(summary_df["travel_time_min"], errors="coerce").fillna(0.0)
        ).round(2)
        summary_df["overflow_480"] = pd.to_numeric(summary_df["total_work_min"], errors="coerce").fillna(0.0) > float(base.MAX_WORK_MIN)
    return assignment_df, summary_df, schedule_df


def _build_assignment_from_frames(
    engineer_region_df,
    home_df,
    service_df,
    *,
    attendance_limited=True,
    enable_targeted_swap=False,
    relocation_span_weight=None,
    relocation_passes=None,
    max_travel_budget_ratio=None,
):
    working_service_df = _prepare_service_df(service_df)
    engineer_master_df = base._build_engineer_master(engineer_region_df.copy(), home_df.copy())
    region_centers = base._region_centers(working_service_df)
    attendance_master_df, attendance_by_date = base._build_actual_attendance_master(working_service_df, engineer_master_df)
    route_client = base._build_route_client()
    assignment_frames, summary_frames, schedule_frames = [], [], []
    for service_date_key, service_day_df in working_service_df.groupby("service_date_key"):
        day_engineer_df = _build_day_engineer_master(engineer_master_df, attendance_master_df, attendance_by_date, str(service_date_key), attendance_limited)
        if day_engineer_df.empty:
            continue
        assignment_df, summary_df, schedule_df = _solve_day_assignment(
            service_day_df.copy(),
            day_engineer_df.copy(),
            route_client,
            region_centers,
            enable_targeted_swap=enable_targeted_swap,
            relocation_span_weight=relocation_span_weight,
            relocation_passes=relocation_passes,
            max_travel_budget_ratio=max_travel_budget_ratio,
        )
        if assignment_df.empty:
            continue
        assignment_frames.append(assignment_df)
        summary_frames.append(summary_df)
        if not schedule_df.empty:
            schedule_frames.append(schedule_df)
    return (
        pd.concat(assignment_frames, ignore_index=True) if assignment_frames else pd.DataFrame(),
        pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame(),
        pd.concat(schedule_frames, ignore_index=True) if schedule_frames else pd.DataFrame(),
    )


def build_atlanta_production_assignment_csi_from_frames(engineer_region_df, home_df, service_df, attendance_limited=True):
    return _build_assignment_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=attendance_limited,
        enable_targeted_swap=False,
    )


def build_atlanta_production_assignment_csi(date_keys: list[str] | None = None, output_suffix: str = "csi_actual", attendance_limited: bool = True):
    assignment_path, summary_path, schedule_path = _output_paths(output_suffix)
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    if date_keys:
        wanted = {str(value) for value in date_keys}
        service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()
    assignment_df, summary_df, schedule_df = build_atlanta_production_assignment_csi_from_frames(
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
