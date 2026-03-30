from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import math

from .osrm_routing import OSRMConfig, OSRMTripClient


ATLANTA_CITY = "Atlanta, GA"
PRODUCTION_INPUT_DIR = Path("260310/production_input")
PRODUCTION_OUTPUT_DIR = Path("260310/production_output")
ASSIGNMENT_PATH = PRODUCTION_OUTPUT_DIR / "atlanta_assignment_result.csv"
ENGINEER_DAY_SUMMARY_PATH = PRODUCTION_OUTPUT_DIR / "atlanta_engineer_day_summary.csv"
SCHEDULE_PATH = PRODUCTION_OUTPUT_DIR / "atlanta_schedule.csv"
REGION_ZIP_PATH = PRODUCTION_INPUT_DIR / "atlanta_fixed_region_zip_3.csv"
ENGINEER_REGION_PATH = PRODUCTION_INPUT_DIR / "atlanta_engineer_region_assignment.csv"
HOME_GEOCODE_PATH = PRODUCTION_INPUT_DIR / "atlanta_engineer_home_geocoded.csv"
SERVICE_PATH = PRODUCTION_INPUT_DIR / "atlanta_service_enriched.csv"
DAY_START_HOUR = 9
LUNCH_WINDOW_START_HOUR = 11
LUNCH_WINDOW_START_MIN = 30
LUNCH_WINDOW_END_HOUR = 13
LUNCH_WINDOW_END_MIN = 30
LUNCH_DURATION_MIN = 60
MAX_WORK_MIN = 480
TV_PRODUCT_GROUP = "TV"
REF_PRODUCT_GROUP = "REF"
DMS_CENTER_TYPE = "DMS"
DMS2_CENTER_TYPE = "DMS2"
EXPANSION_SOURCE_REGION = 3
EXPANSION_TARGET_REGIONS = {1, 2}
EXPANSION_BORDER_EXTRA_KM = 15.0
EXPANSION_BORDER_RATIO = 1.35
SOFT_REGION_DMS_PENALTY_KM = 18.0
SOFT_REGION_DMS2_PENALTY_KM = 8.0


@dataclass
class AtlantaProductionAssignmentResult:
    assignment_path: Path
    engineer_day_summary_path: Path
    schedule_path: Path


def _output_paths(output_suffix: str = "") -> tuple[Path, Path, Path]:
    suffix = str(output_suffix).strip()
    if not suffix:
        return ASSIGNMENT_PATH, ENGINEER_DAY_SUMMARY_PATH, SCHEDULE_PATH
    return (
        PRODUCTION_OUTPUT_DIR / f"atlanta_assignment_result_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_engineer_day_summary_{suffix}.csv",
        PRODUCTION_OUTPUT_DIR / f"atlanta_schedule_{suffix}.csv",
    )


def _load_config(config_path: Path = Path("config.json")) -> dict[str, Any]:
    import json

    if not config_path.exists():
        return {}
    return json.loads(config_path.read_text(encoding="utf-8"))


def _build_route_client() -> OSRMTripClient:
    routing_cfg = _load_config().get("routing", {})
    return OSRMTripClient(
        OSRMConfig(
            osrm_url=str(routing_cfg.get("city_osrm_urls", {}).get(ATLANTA_CITY, routing_cfg.get("osrm_url", "http://20.51.244.68:5002"))).rstrip("/"),
            mode="osrm",
            osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
            cache_file=Path("data/cache/osrm_trip_cache_atlanta_production_assignment.csv"),
            fallback_osrm_url=str(routing_cfg.get("osrm_url", "http://20.51.244.68:5000")).rstrip("/"),
        )
    )


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    region_zip_df = pd.read_csv(REGION_ZIP_PATH, encoding="utf-8-sig")
    engineer_region_df = pd.read_csv(ENGINEER_REGION_PATH, encoding="utf-8-sig")
    home_df = pd.read_csv(HOME_GEOCODE_PATH, encoding="utf-8-sig")
    service_df = pd.read_csv(SERVICE_PATH, encoding="utf-8-sig", low_memory=False)

    region_zip_df["POSTAL_CODE"] = region_zip_df["POSTAL_CODE"].astype(str).str.zfill(5)
    service_df["POSTAL_CODE"] = service_df["POSTAL_CODE"].astype(str).str.zfill(5)
    service_df["service_date"] = pd.to_datetime(service_df["service_date"], errors="coerce")
    service_df["service_date_key"] = service_df["service_date"].dt.strftime("%Y-%m-%d")
    service_df["latitude"] = pd.to_numeric(service_df["latitude"], errors="coerce")
    service_df["longitude"] = pd.to_numeric(service_df["longitude"], errors="coerce")
    service_df["service_time_min"] = pd.to_numeric(service_df["service_time_min"], errors="coerce").fillna(45)
    service_df["is_heavy_repair"] = service_df["is_heavy_repair"].fillna(False).astype(bool)
    service_df["is_tv_job"] = service_df["is_tv_job"].fillna(False).astype(bool)

    service_df = service_df.merge(
        region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]],
        on="POSTAL_CODE",
        how="left",
    )
    service_df = service_df[service_df["region_seq"].notna()].copy()
    service_df["region_seq"] = service_df["region_seq"].astype(int)
    return region_zip_df, engineer_region_df, home_df, service_df


def _region_centers(service_df: pd.DataFrame) -> dict[int, tuple[float, float]]:
    centers = (
        service_df.groupby("region_seq")
        .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"))
        .reset_index()
    )
    return {
        int(row["region_seq"]): (float(row["longitude"]), float(row["latitude"]))
        for _, row in centers.iterrows()
        if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
    }


def _build_engineer_master(engineer_region_df: pd.DataFrame, home_df: pd.DataFrame) -> pd.DataFrame:
    home_lookup = home_df[["SVC_ENGINEER_CODE", "latitude", "longitude"]].drop_duplicates(subset=["SVC_ENGINEER_CODE"])
    master_df = engineer_region_df.merge(home_lookup, on="SVC_ENGINEER_CODE", how="left")
    master_df["assigned_region_seq"] = pd.to_numeric(master_df["assigned_region_seq"], errors="coerce")
    if "anchor_region_seq" in master_df.columns:
        master_df["anchor_region_seq"] = pd.to_numeric(master_df["anchor_region_seq"], errors="coerce")
    master_df["normalized_slot"] = pd.to_numeric(master_df["normalized_slot"], errors="coerce").fillna(8)
    master_df["REF_HEAVY_REPAIR_FLAG"] = master_df["REF_HEAVY_REPAIR_FLAG"].fillna("Y").astype(str).str.upper()
    master_df["SVC_CENTER_TYPE"] = master_df["SVC_CENTER_TYPE"].astype(str).str.upper()
    return master_df


def _haversine_distance_km(coord_a: tuple[float, float] | None, coord_b: tuple[float, float] | None) -> float:
    if coord_a is None or coord_b is None:
        return 0.0
    lon1, lat1 = coord_a
    lon2, lat2 = coord_b
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    hav = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2
    )
    return float(2.0 * r * math.asin(math.sqrt(hav)))


def _build_border_expansion_zip_map(
    service_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
) -> dict[int, set[str]]:
    zip_centers = (
        service_df.groupby(["POSTAL_CODE", "region_seq"], dropna=False)
        .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"))
        .reset_index()
    )
    border_zip_map: dict[int, set[str]] = {region_seq: set() for region_seq in EXPANSION_TARGET_REGIONS}
    source_center = region_centers.get(EXPANSION_SOURCE_REGION)
    if source_center is None:
        return border_zip_map

    for _, row in zip_centers.iterrows():
        region_seq = int(row["region_seq"])
        if region_seq not in EXPANSION_TARGET_REGIONS:
            continue
        if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
            continue
        zip_coord = (float(row["longitude"]), float(row["latitude"]))
        own_center = region_centers.get(region_seq)
        if own_center is None:
            continue
        own_km = _haversine_distance_km(zip_coord, own_center)
        source_km = _haversine_distance_km(zip_coord, source_center)
        if source_km <= own_km + EXPANSION_BORDER_EXTRA_KM and source_km <= max(own_km * EXPANSION_BORDER_RATIO, own_km + 1.0):
            border_zip_map[region_seq].add(str(row["POSTAL_CODE"]).zfill(5))

    return border_zip_map


def _first_mode(series: pd.Series, default: str = "") -> str:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return default
    modes = cleaned.mode()
    if not modes.empty:
        return str(modes.iloc[0])
    return str(cleaned.iloc[0])


def _build_actual_attendance_master(
    service_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, set[str]]]:
    actual_df = service_df[service_df["SVC_CENTER_TYPE"].astype(str).str.upper().isin([DMS_CENTER_TYPE, DMS2_CENTER_TYPE])].copy()
    if actual_df.empty:
        empty = engineer_master_df.head(0).copy()
        return empty, {}

    actual_df["SVC_ENGINEER_CODE"] = actual_df["SVC_ENGINEER_CODE"].astype(str)
    actual_df["SVC_CENTER_TYPE"] = actual_df["SVC_CENTER_TYPE"].astype(str).str.upper()
    actual_df["region_seq"] = pd.to_numeric(actual_df["region_seq"], errors="coerce")
    actual_df["latitude"] = pd.to_numeric(actual_df["latitude"], errors="coerce")
    actual_df["longitude"] = pd.to_numeric(actual_df["longitude"], errors="coerce")

    roster_codes = set(engineer_master_df["SVC_ENGINEER_CODE"].astype(str))
    attendance_by_date = {
        str(service_date_key): set(group["SVC_ENGINEER_CODE"].astype(str).tolist())
        for service_date_key, group in actual_df.groupby("service_date_key")
    }

    supplemental_rows: list[dict[str, Any]] = []
    for engineer_code, group in actual_df.groupby("SVC_ENGINEER_CODE"):
        engineer_code = str(engineer_code)
        if engineer_code in roster_codes:
            continue
        center_type = _first_mode(group["SVC_CENTER_TYPE"], DMS_CENTER_TYPE)
        assigned_region_seq = pd.to_numeric(group["region_seq"], errors="coerce").dropna()
        assigned_region = int(assigned_region_seq.mode().iloc[0]) if not assigned_region_seq.empty else pd.NA
        supplemental_rows.append(
            {
                "SVC_ENGINEER_CODE": engineer_code,
                "assigned_region_seq": assigned_region,
                "zip_overlap_count": 0,
                "zip_overlap_ratio": 0.0,
                "AREA_NAME": f"{engineer_code}_ACTUAL",
                "SVC_CENTER_TYPE": center_type,
                "assigned_region_name": f"Atlanta New Region {int(assigned_region)}" if pd.notna(assigned_region) else "Atlanta Floating",
                "preferred_region_rank_1": pd.NA,
                "preferred_region_rank_2": pd.NA,
                "preferred_region_rank_3": pd.NA,
                "anchor_region_seq": assigned_region,
                "anchor_region_name": f"Atlanta New Region {int(assigned_region)}" if pd.notna(assigned_region) else "Atlanta Floating",
                "Name": _first_mode(group["SVC_ENGINEER_NAME"], engineer_code),
                "normalized_slot": 8,
                "REF_HEAVY_REPAIR_FLAG": "Y",
                "latitude": pd.to_numeric(group["latitude"], errors="coerce").median(),
                "longitude": pd.to_numeric(group["longitude"], errors="coerce").median(),
            }
        )

    attendance_master_df = engineer_master_df.copy()
    if supplemental_rows:
        supplemental_df = pd.DataFrame(supplemental_rows)
        for col in attendance_master_df.columns:
            if col not in supplemental_df.columns:
                supplemental_df[col] = pd.NA
        supplemental_df = supplemental_df[attendance_master_df.columns]
        attendance_master_df = pd.concat([attendance_master_df, supplemental_df], ignore_index=True)

    attendance_master_df = attendance_master_df.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).reset_index(drop=True)
    return attendance_master_df, attendance_by_date


def _job_priority(df: pd.DataFrame) -> pd.DataFrame:
    prioritized = df.copy()
    prioritized["tv_priority"] = prioritized["is_tv_job"].astype(int)
    prioritized["heavy_priority"] = prioritized["is_heavy_repair"].astype(int)
    prioritized["service_time_priority"] = prioritized["service_time_min"]
    prioritized = prioritized.sort_values(
        ["tv_priority", "heavy_priority", "service_time_priority", "GSFS_RECEIPT_NO"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    return prioritized


def _compute_active_count(job_df: pd.DataFrame, max_engineers: int, baseline_jobs_per_engineer: int = 4) -> int:
    if job_df.empty or max_engineers <= 0:
        return 0
    job_count = int(job_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique())
    return max(1, min(int(max_engineers), math.ceil(job_count / float(baseline_jobs_per_engineer))))


def _pick_active_engineers(
    candidates_df: pd.DataFrame,
    target_count: int,
    anchor_coord: tuple[float, float] | None,
) -> pd.DataFrame:
    if candidates_df.empty or target_count <= 0:
        return candidates_df.head(0).copy()
    ranked = candidates_df.copy()
    if anchor_coord is not None:
        ranked["anchor_distance"] = ranked.apply(
            lambda row: _estimate_incremental_travel(
                (
                    float(row["longitude"]),
                    float(row["latitude"]),
                )
                if pd.notna(row.get("longitude")) and pd.notna(row.get("latitude"))
                else None,
                anchor_coord,
                None,
            )[0],
            axis=1,
        )
    else:
        ranked["anchor_distance"] = 0.0
    ranked = ranked.sort_values(
        ["anchor_distance", "zip_overlap_count", "zip_overlap_ratio", "SVC_ENGINEER_CODE"],
        ascending=[True, False, False, True],
    )
    return ranked.head(target_count).copy()


def _seed_assign_jobs(
    remaining_df: pd.DataFrame,
    active_engineers_df: pd.DataFrame,
    states: dict[str, dict[str, Any]],
    start_lookup: dict[str, tuple[float, float] | None],
    route_client: OSRMTripClient,
    default_anchor_coord: tuple[float, float] | None = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    assignments: list[dict[str, Any]] = []
    if remaining_df.empty or active_engineers_df.empty:
        return remaining_df.copy(), assignments

    unassigned = remaining_df.copy().reset_index(drop=True)
    used_indices: set[int] = set()
    for _, engineer in active_engineers_df.iterrows():
        engineer_code = str(engineer["SVC_ENGINEER_CODE"])
        if unassigned.empty:
            break
        start_coord = start_lookup.get(engineer_code)
        candidates = unassigned.copy()
        if candidates.empty:
            break
        if start_coord is None and str(engineer.get("SVC_CENTER_TYPE", "")).upper() == DMS2_CENTER_TYPE:
            start_coord = default_anchor_coord
        if start_coord is not None:
            candidates["_seed_km"] = candidates.apply(
                lambda row: _estimate_incremental_travel(
                    start_coord,
                    (float(row["longitude"]), float(row["latitude"])),
                    route_client,
                )[0],
                axis=1,
            )
            engineer_region = pd.to_numeric(pd.Series([engineer.get("assigned_region_seq")]), errors="coerce").iloc[0]
            engineer_center_type = str(engineer.get("SVC_CENTER_TYPE", "")).upper()
            if pd.notna(engineer_region):
                def _seed_penalty(row: pd.Series) -> float:
                    job_region = pd.to_numeric(pd.Series([row.get("region_seq")]), errors="coerce").iloc[0]
                    if pd.isna(job_region) or int(job_region) == int(engineer_region):
                        return 0.0
                    return SOFT_REGION_DMS2_PENALTY_KM if engineer_center_type == DMS2_CENTER_TYPE else SOFT_REGION_DMS_PENALTY_KM
                candidates["_region_penalty"] = candidates.apply(_seed_penalty, axis=1)
            else:
                candidates["_region_penalty"] = 0.0
            candidates["_seed_score"] = candidates["_seed_km"] + candidates["_region_penalty"]
            chosen_idx = int(candidates.sort_values(["_seed_score", "service_time_min", "GSFS_RECEIPT_NO"], ascending=[True, False, True]).index[0])
        else:
            chosen_idx = int(candidates.sort_values(["service_time_min", "GSFS_RECEIPT_NO"], ascending=[False, True]).index[0])

        if chosen_idx in used_indices:
            continue
        job = unassigned.loc[chosen_idx]
        job_coord = (float(job["longitude"]), float(job["latitude"]))
        state = states[engineer_code]
        state["current_coord"] = job_coord
        state["service_time_min"] += float(job["service_time_min"])
        state["job_count"] += 1
        if state["start_coord"] is None:
            state["start_coord"] = start_coord
        if start_coord is not None:
            inc_km, inc_min = _estimate_incremental_travel(start_coord, job_coord, route_client)
            state["travel_distance_km"] += inc_km
            state["travel_time_min"] += inc_min
        job_dict = job.to_dict()
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


def _grow_assign_jobs(
    remaining_df: pd.DataFrame,
    active_engineers_df: pd.DataFrame,
    states: dict[str, dict[str, Any]],
    route_client: OSRMTripClient,
    target_jobs_per_engineer: dict[str, int],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    assignments: list[dict[str, Any]] = []
    unassigned = remaining_df.copy().reset_index(drop=True)

    def _state_anchor_coords(state: dict[str, Any]) -> list[tuple[float, float]]:
        anchors: list[tuple[float, float]] = []
        start_coord = state.get("start_coord")
        if start_coord is not None:
            anchors.append((float(start_coord[0]), float(start_coord[1])))
        for row in state.get("assigned_rows", []):
            if pd.notna(row.get("longitude")) and pd.notna(row.get("latitude")):
                anchors.append((float(row["longitude"]), float(row["latitude"])))
        return anchors

    def _best_anchor_distance(
        state: dict[str, Any],
        job_coord: tuple[float, float],
        route_client: OSRMTripClient,
    ) -> tuple[float, float]:
        anchors = _state_anchor_coords(state)
        if not anchors:
            return 0.0, 0.0
        best_km = None
        best_min = None
        for anchor in anchors:
            inc_km, inc_min = _estimate_incremental_travel(anchor, job_coord, route_client)
            if best_km is None or inc_km < best_km:
                best_km = float(inc_km)
                best_min = float(inc_min)
        return float(best_km or 0.0), float(best_min or 0.0)

    while not unassigned.empty and not active_engineers_df.empty:
        best_move: dict[str, Any] | None = None
        active_codes = [str(row["SVC_ENGINEER_CODE"]) for _, row in active_engineers_df.iterrows()]
        current_job_counts = {code: int(states[code]["job_count"]) for code in active_codes}
        min_job_count = min(current_job_counts.values()) if current_job_counts else 0
        for _, engineer in active_engineers_df.iterrows():
            engineer_code = str(engineer["SVC_ENGINEER_CODE"])
            state = states[engineer_code]
            anchor_coords = _state_anchor_coords(state)
            if not anchor_coords:
                continue
            eligible = unassigned.copy()
            if eligible.empty:
                continue
            eligible["_inc_km"] = eligible.apply(
                lambda row: _best_anchor_distance(
                    state,
                    (float(row["longitude"]), float(row["latitude"])),
                    route_client,
                )[0],
                axis=1,
            )
            nearest_idx = int(eligible.sort_values(["_inc_km", "service_time_min", "GSFS_RECEIPT_NO"], ascending=[True, False, True]).index[0])
            job = eligible.loc[nearest_idx]
            inc_km, inc_min = _best_anchor_distance(
                state,
                (float(job["longitude"]), float(job["latitude"])),
                route_client,
            )
            projected_service = state["service_time_min"] + float(job["service_time_min"])
            projected_travel = state["travel_time_min"] + inc_min
            projected_total = projected_service + projected_travel
            target_jobs = int(target_jobs_per_engineer.get(engineer_code, 4))
            projected_jobs = int(state["job_count"]) + 1
            engineer_region = pd.to_numeric(pd.Series([engineer.get("assigned_region_seq")]), errors="coerce").iloc[0]
            job_region = pd.to_numeric(pd.Series([job.get("region_seq")]), errors="coerce").iloc[0]
            engineer_center_type = str(engineer.get("SVC_CENTER_TYPE", "")).upper()
            region_penalty = 0.0
            if pd.notna(engineer_region) and pd.notna(job_region) and int(engineer_region) != int(job_region):
                region_penalty = SOFT_REGION_DMS2_PENALTY_KM if engineer_center_type == DMS2_CENTER_TYPE else SOFT_REGION_DMS_PENALTY_KM
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
            overflow_penalty = max(projected_total - MAX_WORK_MIN, 0.0) * 10.0
            score = (
                round(float(inc_km) + region_penalty + over_target_penalty + fairness_penalty + overflow_penalty, 4),
                round(float(inc_min), 4),
                round(float(projected_total), 4),
                projected_jobs,
            )
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


def _candidate_engineers(job_row: pd.Series, engineer_master_df: pd.DataFrame) -> pd.DataFrame:
    is_tv = bool(job_row.get("is_tv_job", False))
    is_heavy = bool(job_row.get("is_heavy_repair", False))
    product_group = str(job_row.get("SERVICE_PRODUCT_GROUP_CODE", "")).strip().upper()

    if is_tv:
        candidates = engineer_master_df[engineer_master_df["SVC_CENTER_TYPE"] == DMS2_CENTER_TYPE].copy()
    else:
        region_dms = engineer_master_df[
            engineer_master_df["SVC_CENTER_TYPE"] == DMS_CENTER_TYPE
        ].copy()
        floating_dms2 = engineer_master_df[engineer_master_df["SVC_CENTER_TYPE"] == DMS2_CENTER_TYPE].copy()
        candidates = pd.concat([region_dms, floating_dms2], ignore_index=True)

    if is_heavy and product_group == REF_PRODUCT_GROUP:
        candidates = candidates[candidates["REF_HEAVY_REPAIR_FLAG"] == "Y"].copy()
    return candidates.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).reset_index(drop=True)


def _estimate_incremental_travel(last_coord: tuple[float, float] | None, next_coord: tuple[float, float], route_client: OSRMTripClient) -> tuple[float, float]:
    if last_coord is None:
        return 0.0, 0.0
    distance_km = _haversine_distance_km(last_coord, next_coord)
    duration_min = (distance_km / 50.0) * 60.0
    return float(distance_km), float(duration_min)


def _get_engineer_start_coord(engineer_row: pd.Series, region_centers: dict[int, tuple[float, float]]) -> tuple[float, float] | None:
    if pd.notna(engineer_row.get("latitude")) and pd.notna(engineer_row.get("longitude")):
        return (float(engineer_row["longitude"]), float(engineer_row["latitude"]))
    anchor_region_seq = pd.to_numeric(pd.Series([engineer_row.get("anchor_region_seq")]), errors="coerce").iloc[0]
    if pd.notna(anchor_region_seq):
        return region_centers.get(int(anchor_region_seq))
    return None


def _estimate_group_metrics(group_df: pd.DataFrame, start_coord: tuple[float, float] | None) -> tuple[float, float]:
    if group_df.empty:
        return 0.0, 0.0
    remaining = [
        (int(idx), (float(row["longitude"]), float(row["latitude"])))
        for idx, row in group_df.iterrows()
        if pd.notna(row.get("longitude")) and pd.notna(row.get("latitude"))
    ]
    if not remaining:
        return 0.0, 0.0
    current = start_coord
    total_km = 0.0
    total_min = 0.0
    while remaining:
        if current is None:
            chosen_pos = 0
            chosen_coord = remaining[chosen_pos][1]
            current = chosen_coord
            remaining.pop(chosen_pos)
            continue
        best_pos = 0
        best_km = None
        best_min = None
        for pos, (_, coord) in enumerate(remaining):
            inc_km, inc_min = _estimate_incremental_travel(current, coord, None)  # route_client unused in current haversine estimate
            if best_km is None or inc_km < best_km:
                best_km = inc_km
                best_min = inc_min
                best_pos = pos
        _, chosen_coord = remaining.pop(best_pos)
        total_km += float(best_km or 0.0)
        total_min += float(best_min or 0.0)
        current = chosen_coord
    return round(total_km, 2), round(total_min, 2)


def _estimate_group_metrics_osrm(
    group_df: pd.DataFrame,
    start_coord: tuple[float, float] | None,
    route_client: OSRMTripClient,
) -> tuple[float, float]:
    if group_df.empty:
        return 0.0, 0.0
    stop_coords = [
        (float(row["longitude"]), float(row["latitude"]))
        for _, row in group_df.iterrows()
        if pd.notna(row.get("longitude")) and pd.notna(row.get("latitude"))
    ]
    if not stop_coords:
        return 0.0, 0.0
    coord_chain = [start_coord] + stop_coords if start_coord is not None else stop_coords
    payload = route_client.build_ordered_route(coord_chain, preserve_first=start_coord is not None)
    return round(float(payload.get("distance_km", 0.0)), 2), round(float(payload.get("duration_min", 0.0)), 2)


def _build_summary_from_assignment(
    assignment_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
    service_date_key: str,
) -> pd.DataFrame:
    summary_rows: list[dict[str, Any]] = []
    for _, engineer in engineer_master_df.iterrows():
        code = str(engineer["SVC_ENGINEER_CODE"])
        group_df = assignment_df[assignment_df["assigned_sm_code"].astype(str) == code].copy()
        start_coord = _get_engineer_start_coord(engineer, region_centers)
        travel_distance_km, travel_time_min = _estimate_group_metrics(group_df, start_coord)
        service_time_min = float(pd.to_numeric(group_df.get("service_time_min"), errors="coerce").fillna(0).sum()) if not group_df.empty else 0.0
        total_work = service_time_min + travel_time_min
        summary_rows.append(
            {
                "service_date_key": service_date_key,
                "SVC_ENGINEER_CODE": code,
                "SVC_ENGINEER_NAME": str(engineer.get("Name", "")),
                "assigned_center_type": str(engineer.get("SVC_CENTER_TYPE", "")),
                "assigned_region_seq": engineer.get("assigned_region_seq"),
                "job_count": int(group_df["GSFS_RECEIPT_NO"].dropna().astype(str).nunique()) if not group_df.empty else 0,
                "service_time_min": round(service_time_min, 2),
                "travel_time_min": round(travel_time_min, 2),
                "travel_distance_km": round(travel_distance_km, 2),
                "total_work_min": round(total_work, 2),
                "overflow_480": bool(total_work > MAX_WORK_MIN),
            }
        )
    return pd.DataFrame(summary_rows)


def _local_rebalance_assignment_df(
    assignment_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
) -> pd.DataFrame:
    if assignment_df.empty:
        return assignment_df

    rebalanced_df = assignment_df.copy().reset_index(drop=True)
    engineer_lookup = {
        str(row["SVC_ENGINEER_CODE"]): row
        for _, row in engineer_master_df.iterrows()
    }

    for _ in range(3):
        changed = False
        job_counts = rebalanced_df.groupby("assigned_sm_code")["GSFS_RECEIPT_NO"].nunique().to_dict()
        for region_seq in sorted(rebalanced_df["region_seq"].dropna().astype(int).unique().tolist()):
            region_mask = pd.to_numeric(rebalanced_df["region_seq"], errors="coerce") == int(region_seq)
            region_df = rebalanced_df[region_mask].copy()
            if region_df.empty:
                continue
            for idx, job_row in region_df.iterrows():
                current_code = str(job_row["assigned_sm_code"])
                current_count = int(job_counts.get(current_code, 0))
                if current_count <= 1:
                    continue
                current_engineer = engineer_lookup.get(current_code)
                if current_engineer is None:
                    continue
                current_start = _get_engineer_start_coord(current_engineer, region_centers)
                job_coord = (float(job_row["longitude"]), float(job_row["latitude"]))
                current_home_km, _ = _estimate_incremental_travel(current_start, job_coord, None)

                candidates_df = _candidate_engineers(job_row, engineer_master_df)
                if candidates_df.empty:
                    continue

                source_group = rebalanced_df[rebalanced_df["assigned_sm_code"].astype(str) == current_code].copy()
                source_old_km, source_old_min = _estimate_group_metrics(source_group, current_start)
                source_old_total = float(pd.to_numeric(source_group["service_time_min"], errors="coerce").fillna(0).sum()) + source_old_min
                source_group_new = source_group[source_group["GSFS_RECEIPT_NO"].astype(str) != str(job_row["GSFS_RECEIPT_NO"])].copy()
                source_new_km, source_new_min = _estimate_group_metrics(source_group_new, current_start)
                source_new_total = float(pd.to_numeric(source_group_new.get("service_time_min"), errors="coerce").fillna(0).sum()) + source_new_min

                best_move: dict[str, Any] | None = None
                for _, candidate in candidates_df.iterrows():
                    candidate_code = str(candidate["SVC_ENGINEER_CODE"])
                    if candidate_code == current_code:
                        continue
                    candidate_count = int(job_counts.get(candidate_code, 0))
                    candidate_start = _get_engineer_start_coord(candidate, region_centers)
                    if candidate_start is None:
                        continue
                    candidate_home_km, _ = _estimate_incremental_travel(candidate_start, job_coord, None)
                    if candidate_count >= 5 and candidate_count >= current_count:
                        continue
                    target_group = rebalanced_df[rebalanced_df["assigned_sm_code"].astype(str) == candidate_code].copy()
                    target_old_km, target_old_min = _estimate_group_metrics(target_group, candidate_start)
                    target_old_total = float(pd.to_numeric(target_group.get("service_time_min"), errors="coerce").fillna(0).sum()) + target_old_min
                    target_group_new = pd.concat([target_group, pd.DataFrame([job_row])], ignore_index=True)
                    target_new_km, target_new_min = _estimate_group_metrics(target_group_new, candidate_start)
                    target_new_total = float(pd.to_numeric(target_group_new.get("service_time_min"), errors="coerce").fillna(0).sum()) + target_new_min

                    old_combined = source_old_total + target_old_total
                    new_combined = source_new_total + target_new_total
                    old_gap = abs(source_old_total - target_old_total)
                    new_gap = abs(source_new_total - target_new_total)
                    home_gain_km = current_home_km - candidate_home_km
                    score = (
                        round(new_combined - old_combined, 4),
                        round(new_gap - old_gap, 4),
                        round(-home_gain_km, 4),
                        candidate_count,
                    )
                    should_move = False
                    if candidate_home_km + 5.0 < current_home_km and new_combined <= old_combined + 12.0:
                        should_move = True
                    if new_combined + 5.0 < old_combined:
                        should_move = True
                    if new_gap + 15.0 < old_gap and new_combined <= old_combined + 20.0:
                        should_move = True
                    if not should_move:
                        continue
                    if best_move is None or score < best_move["score"]:
                        best_move = {
                            "candidate_code": candidate_code,
                            "candidate_name": str(candidate.get("Name", "")),
                            "candidate_center_type": str(candidate.get("SVC_CENTER_TYPE", "")),
                            "candidate_start": candidate_start,
                            "score": score,
                        }

                if best_move is None:
                    continue

                rebalanced_df.loc[idx, "assigned_sm_code"] = best_move["candidate_code"]
                rebalanced_df.loc[idx, "assigned_sm_name"] = best_move["candidate_name"]
                rebalanced_df.loc[idx, "assigned_center_type"] = best_move["candidate_center_type"]
                rebalanced_df.loc[idx, "home_start_longitude"] = best_move["candidate_start"][0]
                rebalanced_df.loc[idx, "home_start_latitude"] = best_move["candidate_start"][1]
                job_counts[current_code] = max(current_count - 1, 0)
                job_counts[best_move["candidate_code"]] = int(job_counts.get(best_move["candidate_code"], 0)) + 1
                changed = True
        if not changed:
            break

    return rebalanced_df


def _targeted_region_worst_move_rebalance(
    assignment_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
    route_client: OSRMTripClient,
) -> pd.DataFrame:
    if assignment_df.empty:
        return assignment_df

    rebalanced_df = assignment_df.copy().reset_index(drop=True)
    engineer_lookup = {str(row["SVC_ENGINEER_CODE"]): row for _, row in engineer_master_df.iterrows()}

    for _ in range(4):
        changed = False
        summary_df = _build_summary_from_assignment(
            rebalanced_df,
            engineer_master_df,
            region_centers,
            str(rebalanced_df["service_date_key"].iloc[0]),
        )
        job_counts = summary_df.set_index("SVC_ENGINEER_CODE")["job_count"].to_dict()
        total_work = summary_df.set_index("SVC_ENGINEER_CODE")["total_work_min"].to_dict()

        for region_seq in sorted(rebalanced_df["region_seq"].dropna().astype(int).unique().tolist()):
            region_engineers = summary_df[
                pd.to_numeric(summary_df["assigned_region_seq"], errors="coerce") == int(region_seq)
            ].copy()
            if region_engineers.empty:
                continue
            worst_row = region_engineers.sort_values(
                ["total_work_min", "job_count", "travel_distance_km"],
                ascending=[False, False, False],
            ).iloc[0]
            worst_code = str(worst_row["SVC_ENGINEER_CODE"])
            worst_group = rebalanced_df[rebalanced_df["assigned_sm_code"].astype(str) == worst_code].copy()
            if worst_group.empty or int(worst_row["job_count"]) <= 1:
                continue

            worst_engineer = engineer_lookup.get(worst_code)
            if worst_engineer is None:
                continue
            worst_start = _get_engineer_start_coord(worst_engineer, region_centers)
            worst_group["_home_km"] = worst_group.apply(
                lambda r: _estimate_incremental_travel(
                    worst_start,
                    (float(r["longitude"]), float(r["latitude"])),
                    None,
                )[0],
                axis=1,
            )
            # 먼 건부터 검토
            for _, job_row in worst_group.sort_values(["_home_km", "service_time_min"], ascending=[False, False]).iterrows():
                candidates_df = _candidate_engineers(job_row, engineer_master_df)
                if candidates_df.empty:
                    continue

                source_old_km, source_old_min = _estimate_group_metrics_osrm(
                    worst_group.drop(columns=["_home_km"], errors="ignore"),
                    worst_start,
                    route_client,
                )
                source_old_service = float(pd.to_numeric(worst_group["service_time_min"], errors="coerce").fillna(0).sum())
                source_new_group = worst_group[worst_group["GSFS_RECEIPT_NO"].astype(str) != str(job_row["GSFS_RECEIPT_NO"])].copy()
                source_new_km, source_new_min = _estimate_group_metrics_osrm(
                    source_new_group.drop(columns=["_home_km"], errors="ignore"),
                    worst_start,
                    route_client,
                )
                source_new_service = float(pd.to_numeric(source_new_group.get("service_time_min"), errors="coerce").fillna(0).sum()) if not source_new_group.empty else 0.0
                source_old_total = source_old_service + source_old_min
                source_new_total = source_new_service + source_new_min

                best_move: dict[str, Any] | None = None
                for _, candidate in candidates_df.iterrows():
                    candidate_code = str(candidate["SVC_ENGINEER_CODE"])
                    if candidate_code == worst_code:
                        continue
                    candidate_jobs = int(job_counts.get(candidate_code, 0))
                    if candidate_jobs >= int(worst_row["job_count"]):
                        continue

                    candidate_start = _get_engineer_start_coord(candidate, region_centers)
                    if candidate_start is None:
                        continue
                    target_group = rebalanced_df[rebalanced_df["assigned_sm_code"].astype(str) == candidate_code].copy()
                    target_old_km, target_old_min = _estimate_group_metrics_osrm(
                        target_group,
                        candidate_start,
                        route_client,
                    )
                    target_old_service = float(pd.to_numeric(target_group.get("service_time_min"), errors="coerce").fillna(0).sum()) if not target_group.empty else 0.0
                    target_new_group = pd.concat([target_group, pd.DataFrame([job_row.drop(labels=['_home_km'], errors='ignore')])], ignore_index=True)
                    target_new_km, target_new_min = _estimate_group_metrics_osrm(
                        target_new_group,
                        candidate_start,
                        route_client,
                    )
                    target_new_service = float(pd.to_numeric(target_new_group["service_time_min"], errors="coerce").fillna(0).sum())
                    target_old_total = target_old_service + target_old_min
                    target_new_total = target_new_service + target_new_min
                    if target_new_total > MAX_WORK_MIN:
                        continue

                    old_max = max(float(total_work.get(worst_code, 0.0)), float(total_work.get(candidate_code, 0.0)))
                    new_max = max(source_new_total, target_new_total)
                    old_gap = abs(float(total_work.get(worst_code, 0.0)) - float(total_work.get(candidate_code, 0.0)))
                    new_gap = abs(source_new_total - target_new_total)
                    candidate_home_km, _ = _estimate_incremental_travel(
                        candidate_start,
                        (float(job_row["longitude"]), float(job_row["latitude"])),
                        None,
                    )
                    move_score = (
                        round(new_max - old_max, 4),
                        round(new_gap - old_gap, 4),
                        round(candidate_home_km, 4),
                        candidate_jobs,
                    )
                    should_move = False
                    if new_max + 5.0 < old_max:
                        should_move = True
                    elif new_gap + 20.0 < old_gap and new_max <= old_max + 5.0:
                        should_move = True
                    if not should_move:
                        continue
                    if best_move is None or move_score < best_move["score"]:
                        best_move = {
                            "candidate_code": candidate_code,
                            "candidate_name": str(candidate.get("Name", "")),
                            "candidate_center_type": str(candidate.get("SVC_CENTER_TYPE", "")),
                            "candidate_start": candidate_start,
                            "score": move_score,
                            "job_receipt": str(job_row["GSFS_RECEIPT_NO"]),
                        }

                if best_move is None:
                    continue

                move_mask = (
                    (rebalanced_df["assigned_sm_code"].astype(str) == worst_code)
                    & (rebalanced_df["GSFS_RECEIPT_NO"].astype(str) == best_move["job_receipt"])
                )
                rebalanced_df.loc[move_mask, "assigned_sm_code"] = best_move["candidate_code"]
                rebalanced_df.loc[move_mask, "assigned_sm_name"] = best_move["candidate_name"]
                rebalanced_df.loc[move_mask, "assigned_center_type"] = best_move["candidate_center_type"]
                rebalanced_df.loc[move_mask, "home_start_longitude"] = best_move["candidate_start"][0]
                rebalanced_df.loc[move_mask, "home_start_latitude"] = best_move["candidate_start"][1]
                changed = True
                break
            if changed:
                break
        if not changed:
            break

    return rebalanced_df


def _assign_day(
    service_day_df: pd.DataFrame,
    engineer_master_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
    route_client: OSRMTripClient,
    border_expansion_zip_map: dict[int, set[str]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    states: dict[str, dict[str, Any]] = {}
    for _, row in engineer_master_df.iterrows():
        code = str(row["SVC_ENGINEER_CODE"])
        home_coord = None
        if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
            home_coord = (float(row["longitude"]), float(row["latitude"]))
        elif pd.notna(row.get("anchor_region_seq")):
            home_coord = region_centers.get(int(row["anchor_region_seq"]))
        states[code] = {
            "engineer_code": code,
            "engineer_name": str(row.get("Name", "")),
            "center_type": str(row.get("SVC_CENTER_TYPE", "")),
            "assigned_region_seq": row.get("assigned_region_seq"),
            "anchor_region_seq": row.get("anchor_region_seq"),
            "current_coord": home_coord,
            "service_time_min": 0.0,
            "travel_time_min": 0.0,
            "travel_distance_km": 0.0,
            "job_count": 0,
            "assigned_rows": [],
            "start_coord": home_coord,
        }

    assignments: list[dict[str, Any]] = []
    start_lookup: dict[str, tuple[float, float] | None] = {}
    for code, state in states.items():
        start_lookup[code] = state["start_coord"]
    for region_seq, center in region_centers.items():
        start_lookup[f"region::{int(region_seq)}"] = center

    source_region_dms_df = engineer_master_df[
        (engineer_master_df["SVC_CENTER_TYPE"] == DMS_CENTER_TYPE)
        & (pd.to_numeric(engineer_master_df["assigned_region_seq"], errors="coerce") == EXPANSION_SOURCE_REGION)
    ].copy()
    source_region_day_df = service_day_df[pd.to_numeric(service_day_df["region_seq"], errors="coerce") == EXPANSION_SOURCE_REGION].copy()
    source_region_spare_dms = max(
        0,
        len(source_region_dms_df) - _compute_active_count(source_region_day_df[~source_region_day_df["is_tv_job"]].copy(), len(source_region_dms_df)),
    )
    reserved_support_codes: set[str] = set()

    for region_seq, region_day_df in service_day_df.groupby("region_seq"):
        region_seq = int(region_seq)
        region_dms_df = engineer_master_df[
            (engineer_master_df["SVC_CENTER_TYPE"] == DMS_CENTER_TYPE)
            & (pd.to_numeric(engineer_master_df["assigned_region_seq"], errors="coerce") == region_seq)
        ].copy()
        if region_seq == EXPANSION_SOURCE_REGION and reserved_support_codes:
            region_dms_df = region_dms_df[~region_dms_df["SVC_ENGINEER_CODE"].astype(str).isin(reserved_support_codes)].copy()
        dms2_df = engineer_master_df[engineer_master_df["SVC_CENTER_TYPE"] == DMS2_CENTER_TYPE].copy()
        if not dms2_df.empty and "anchor_region_seq" in dms2_df.columns:
            for idx, row in dms2_df.iterrows():
                if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
                    anchor_region_seq = pd.to_numeric(pd.Series([row.get("anchor_region_seq")]), errors="coerce").iloc[0]
                    if pd.notna(anchor_region_seq):
                        anchor_coord = region_centers.get(int(anchor_region_seq))
                        if anchor_coord is not None:
                            dms2_df.at[idx, "longitude"] = float(anchor_coord[0])
                            dms2_df.at[idx, "latitude"] = float(anchor_coord[1])

        tv_jobs_df = _job_priority(region_day_df[region_day_df["is_tv_job"]].copy())
        non_tv_jobs_df = _job_priority(region_day_df[~region_day_df["is_tv_job"]].copy())
        if region_seq in EXPANSION_TARGET_REGIONS:
            border_non_tv_df = non_tv_jobs_df.copy()
            core_non_tv_df = non_tv_jobs_df.head(0).copy()
        else:
            border_non_tv_df = non_tv_jobs_df.head(0).copy()
            core_non_tv_df = non_tv_jobs_df.copy()

        tv_anchor = region_centers.get(region_seq)
        dms_anchor = region_centers.get(region_seq)
        active_dms2_tv = _pick_active_engineers(dms2_df, _compute_active_count(tv_jobs_df, len(dms2_df)), tv_anchor)
        active_dms = _pick_active_engineers(region_dms_df, _compute_active_count(non_tv_jobs_df, len(region_dms_df)), dms_anchor)

        remaining_tv_df, seeded_tv_assignments = _seed_assign_jobs(
            tv_jobs_df,
            active_dms2_tv,
            states,
            start_lookup,
            route_client,
            tv_anchor,
        )
        assignments.extend(seeded_tv_assignments)
        tv_target_jobs = {str(row["SVC_ENGINEER_CODE"]): 4 for _, row in active_dms2_tv.iterrows()}
        remaining_tv_df, grown_tv_assignments = _grow_assign_jobs(remaining_tv_df, active_dms2_tv, states, route_client, tv_target_jobs)
        assignments.extend(grown_tv_assignments)

        extra_dms2_count = 0
        if not non_tv_jobs_df.empty and len(dms2_df) > len(active_dms2_tv):
            total_capacity_gap = len(non_tv_jobs_df) - (max(len(active_dms), 1) * 4)
            extra_dms2_count = max(0, min(len(dms2_df) - len(active_dms2_tv), math.ceil(max(total_capacity_gap, 0) / 4)))
        inactive_dms2 = dms2_df[~dms2_df["SVC_ENGINEER_CODE"].isin(active_dms2_tv["SVC_ENGINEER_CODE"])].copy()
        extra_dms2 = _pick_active_engineers(inactive_dms2, extra_dms2_count, dms_anchor)

        support_dms = region_dms_df.head(0).copy()
        if region_seq in EXPANSION_TARGET_REGIONS and not border_non_tv_df.empty and source_region_spare_dms > 0:
            support_candidates = source_region_dms_df[
                ~source_region_dms_df["SVC_ENGINEER_CODE"].astype(str).isin(reserved_support_codes)
            ].copy()
            support_gap = max(
                len(border_non_tv_df),
                max(len(non_tv_jobs_df) - ((len(active_dms) + len(active_dms2_tv) + len(extra_dms2)) * 4), 0),
            )
            support_count = min(source_region_spare_dms, max(0, math.ceil(support_gap / 4)))
            support_dms = _pick_active_engineers(support_candidates, support_count, dms_anchor)
            reserved_support_codes.update(support_dms["SVC_ENGINEER_CODE"].astype(str).tolist())
            source_region_spare_dms = max(0, source_region_spare_dms - len(support_dms))

        local_non_tv_engineers = pd.concat([active_dms, active_dms2_tv, extra_dms2], ignore_index=True).drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        remaining_core_non_tv_df, seeded_non_tv_assignments = _seed_assign_jobs(
            core_non_tv_df,
            local_non_tv_engineers,
            states,
            start_lookup,
            route_client,
            dms_anchor,
        )
        assignments.extend(seeded_non_tv_assignments)

        target_jobs_per_engineer = {str(row["SVC_ENGINEER_CODE"]): 4 for _, row in active_dms.iterrows()}
        for _, row in pd.concat([active_dms2_tv, extra_dms2], ignore_index=True).drop_duplicates(subset=["SVC_ENGINEER_CODE"]).iterrows():
            target_jobs_per_engineer[str(row["SVC_ENGINEER_CODE"])] = 4
        remaining_core_non_tv_df, grown_non_tv_assignments = _grow_assign_jobs(
            remaining_core_non_tv_df,
            local_non_tv_engineers,
            states,
            route_client,
            target_jobs_per_engineer,
        )
        assignments.extend(grown_non_tv_assignments)

        border_non_tv_engineers = pd.concat([local_non_tv_engineers, support_dms], ignore_index=True).drop_duplicates(subset=["SVC_ENGINEER_CODE"])
        remaining_border_non_tv_df, seeded_border_assignments = _seed_assign_jobs(
            border_non_tv_df,
            border_non_tv_engineers,
            states,
            start_lookup,
            route_client,
            dms_anchor,
        )
        assignments.extend(seeded_border_assignments)

        border_target_jobs = target_jobs_per_engineer.copy()
        for _, row in support_dms.iterrows():
            border_target_jobs[str(row["SVC_ENGINEER_CODE"])] = 4
        remaining_border_non_tv_df, grown_border_assignments = _grow_assign_jobs(
            remaining_border_non_tv_df,
            border_non_tv_engineers,
            states,
            route_client,
            border_target_jobs,
        )
        assignments.extend(grown_border_assignments)

        if not remaining_tv_df.empty or not remaining_core_non_tv_df.empty or not remaining_border_non_tv_df.empty:
            leftover_df = pd.concat([remaining_tv_df, remaining_core_non_tv_df, remaining_border_non_tv_df], ignore_index=True)
            leftover_df = _job_priority(leftover_df)
            fallback_engineers = pd.concat([region_dms_df, dms2_df, support_dms], ignore_index=True).drop_duplicates(subset=["SVC_ENGINEER_CODE"])
            fallback_targets = {str(row["SVC_ENGINEER_CODE"]): 5 for _, row in fallback_engineers.iterrows()}
            _, fallback_assignments = _grow_assign_jobs(leftover_df, fallback_engineers, states, route_client, fallback_targets)
            assignments.extend(fallback_assignments)

    assignment_df = pd.DataFrame(assignments)
    assignment_df = _local_rebalance_assignment_df(
        assignment_df,
        engineer_master_df,
        region_centers,
    )
    assignment_df = _targeted_region_worst_move_rebalance(
        assignment_df,
        engineer_master_df,
        region_centers,
        route_client,
    )
    summary_df = _build_summary_from_assignment(
        assignment_df,
        engineer_master_df,
        region_centers,
        str(service_day_df["service_date_key"].iloc[0]),
    )
    return assignment_df, summary_df


def _fmt_dt(value: pd.Timestamp) -> str:
    return value.strftime("%H:%M")


def _build_schedule_for_group(group_df: pd.DataFrame, route_client: OSRMTripClient) -> tuple[pd.DataFrame, dict[str, Any]]:
    if group_df.empty:
        return pd.DataFrame(), {"distance_km": 0.0, "duration_min": 0.0, "geometry": []}

    start_coord = None
    first = group_df.iloc[0]
    if pd.notna(first.get("home_start_longitude")) and pd.notna(first.get("home_start_latitude")):
        start_coord = (float(first["home_start_longitude"]), float(first["home_start_latitude"]))

    stop_coords = [(float(row["longitude"]), float(row["latitude"])) for _, row in group_df.iterrows()]
    all_coords = [start_coord] + stop_coords if start_coord is not None else stop_coords
    route_payload = route_client.build_ordered_route(all_coords, preserve_first=start_coord is not None)
    ordered_coords = route_payload["ordered_coords"]
    ordered_stop_coords = ordered_coords[1:] if start_coord is not None and len(ordered_coords) > 1 else ordered_coords

    buckets: dict[tuple[float, float], list[dict[str, Any]]] = {}
    for _, row in group_df.iterrows():
        key = (round(float(row["latitude"]), 6), round(float(row["longitude"]), 6))
        buckets.setdefault(key, []).append(row.to_dict())

    ordered_rows: list[dict[str, Any]] = []
    for lon, lat in ordered_stop_coords:
        key = (round(float(lat), 6), round(float(lon), 6))
        row_list = buckets.get(key, [])
        if row_list:
            ordered_rows.append(row_list.pop(0))

    coord_chain = [start_coord] + [(float(row["longitude"]), float(row["latitude"])) for row in ordered_rows] if start_coord is not None else [(float(row["longitude"]), float(row["latitude"])) for row in ordered_rows]
    _, duration_mat = route_client.get_distance_duration_matrix(coord_chain)

    base_date = pd.to_datetime(str(group_df["service_date_key"].iloc[0]), errors="coerce")
    if pd.isna(base_date):
        base_date = pd.Timestamp("2026-01-01")
    current_time = base_date.replace(hour=DAY_START_HOUR, minute=0, second=0, microsecond=0)
    lunch_taken = False
    lunch_start_window = base_date.replace(hour=LUNCH_WINDOW_START_HOUR, minute=LUNCH_WINDOW_START_MIN, second=0, microsecond=0)
    lunch_end_window = base_date.replace(hour=LUNCH_WINDOW_END_HOUR, minute=LUNCH_WINDOW_END_MIN, second=0, microsecond=0)

    schedule_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(ordered_rows, start=1):
        matrix_from = idx - 1 if start_coord is not None else max(idx - 1, 0)
        matrix_to = idx if start_coord is not None else idx - 1
        travel_min = 0.0 if idx == 1 and start_coord is None else float(duration_mat[matrix_from][matrix_to])
        arrival = current_time + pd.Timedelta(minutes=travel_min)
        lunch_flag = False
        if not lunch_taken and lunch_start_window <= arrival <= lunch_end_window:
            arrival = arrival + pd.Timedelta(minutes=LUNCH_DURATION_MIN)
            lunch_taken = True
            lunch_flag = True
        start_time = arrival
        end_time = start_time + pd.Timedelta(minutes=float(row["service_time_min"]))
        if not lunch_taken and lunch_start_window <= end_time <= lunch_end_window:
            current_time = end_time + pd.Timedelta(minutes=LUNCH_DURATION_MIN)
            lunch_taken = True
            lunch_flag = True
        else:
            current_time = end_time

        schedule_row = dict(row)
        schedule_row["visit_seq"] = idx
        schedule_row["travel_time_from_prev_min"] = round(travel_min, 2)
        schedule_row["visit_start_time"] = _fmt_dt(start_time)
        schedule_row["visit_end_time"] = _fmt_dt(end_time)
        schedule_row["lunch_applied"] = lunch_flag
        schedule_rows.append(schedule_row)

    schedule_df = pd.DataFrame(schedule_rows)
    return schedule_df, route_payload


def build_atlanta_production_assignment(
    output_suffix: str = "",
    attendance_limited: bool = False,
    date_keys: list[str] | None = None,
) -> AtlantaProductionAssignmentResult:
    _, engineer_region_df, home_df, service_df = _load_inputs()
    if date_keys:
        wanted = {str(v) for v in date_keys}
        service_df = service_df[service_df["service_date_key"].astype(str).isin(wanted)].copy()
    engineer_master_df = _build_engineer_master(engineer_region_df, home_df)
    region_centers = _region_centers(service_df)
    border_expansion_zip_map = _build_border_expansion_zip_map(service_df, region_centers)
    attendance_master_df, attendance_by_date = _build_actual_attendance_master(service_df, engineer_master_df)
    route_client = _build_route_client()
    assignment_path, summary_path, schedule_path = _output_paths(output_suffix)

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
        assignment_df, summary_df = _assign_day(
            service_day_df.copy(),
            day_engineer_master_df.copy(),
            region_centers,
            route_client,
            border_expansion_zip_map,
        )
        if assignment_df.empty:
            continue
        assignment_frames.append(assignment_df)
        summary_frames.append(summary_df)

        for engineer_code, group_df in assignment_df.groupby("assigned_sm_code"):
            schedule_df, route_payload = _build_schedule_for_group(group_df.copy(), route_client)
            if schedule_df.empty:
                continue
            schedule_df["route_distance_km"] = round(float(route_payload["distance_km"]), 2)
            schedule_df["route_duration_min"] = round(float(route_payload["duration_min"]), 2)
            schedule_frames.append(schedule_df)

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
            engineer_day_summary_df["overflow_480"] = engineer_day_summary_df["total_work_min"] > MAX_WORK_MIN

    PRODUCTION_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    assignment_result_df.to_csv(assignment_path, index=False, encoding="utf-8-sig")
    engineer_day_summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    schedule_result_df.to_csv(schedule_path, index=False, encoding="utf-8-sig")
    return AtlantaProductionAssignmentResult(
        assignment_path=assignment_path,
        engineer_day_summary_path=summary_path,
        schedule_path=schedule_path,
    )
