from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any
from urllib import request as urllib_request

import pandas as pd
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from .vrp_api_common import format_planned_timestamp


CONFIG_JSON_PATH = Path("config.json")
FIXED_Z_WEEKEND_SLOTS = ["09:00", "10:00", "11:00", "12:00"]


def _load_runtime_config() -> dict[str, Any]:
    if not CONFIG_JSON_PATH.exists():
        return {}
    return json.loads(CONFIG_JSON_PATH.read_text(encoding="utf-8"))


def _parse_clock_minutes(value: str | None, fallback: int) -> int:
    raw = str(value or "").strip()
    if len(raw) == 5 and raw[2] == ":":
        try:
            return int(raw[:2]) * 60 + int(raw[3:])
        except Exception:
            return fallback
    return fallback


def _clock_text_from_minutes(total_minutes: int) -> str:
    mins = max(0, int(total_minutes))
    hour = mins // 60
    minute = mins % 60
    return f"{hour:02d}:{minute:02d}"


def _slot_sort_minutes(value: str) -> int:
    return _parse_clock_minutes(value, 99 * 60)


def _normalize_available_slots(slots: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for slot in sorted([str(slot).strip() for slot in slots if str(slot).strip()], key=_slot_sort_minutes):
        if slot not in seen:
            cleaned.append(slot)
            seen.add(slot)
    return cleaned or list(FIXED_Z_WEEKEND_SLOTS)


def _next_slot(available_slots: list[str], next_sequence: int) -> str:
    slot_pool = available_slots or FIXED_Z_WEEKEND_SLOTS
    if 1 <= int(next_sequence) <= len(slot_pool):
        return slot_pool[int(next_sequence) - 1]
    base_minutes = _parse_clock_minutes(slot_pool[0] if slot_pool else "09:00", 9 * 60)
    return _clock_text_from_minutes(base_minutes + max(0, int(next_sequence) - 1) * 60)


def _haversine_distance_km(origin: tuple[float, float], dest: tuple[float, float]) -> float:
    lat1, lon1 = origin
    lat2, lon2 = dest
    radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * radius_km * math.asin(math.sqrt(a))


def _resolve_osrm_url(city: str) -> str:
    config = _load_runtime_config()
    routing_cfg = config.get("routing", {}) if isinstance(config, dict) else {}
    city_map = routing_cfg.get("city_osrm_urls", {}) if isinstance(routing_cfg, dict) else {}
    raw_city = str(city or "").strip()
    if raw_city and raw_city in city_map:
        return str(city_map[raw_city]).rstrip("/")
    return str(routing_cfg.get("osrm_url", "http://20.51.244.68:5000")).rstrip("/")


def _resolve_runtime_city(request_payload: dict[str, Any]) -> str:
    raw_city = str(request_payload.get("city", "")).strip()
    if raw_city:
        return raw_city
    return "Korea"


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text[:-2] if text.endswith(".0") else text


def _text_value(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "nan":
            return text
    return ""


def _float_value(*values: Any) -> float | None:
    for value in values:
        num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.notna(num):
            return float(num)
    return None


def _osrm_route_distance_km(origin: tuple[float, float], dest: tuple[float, float], city: str) -> float:
    osrm_url = _resolve_osrm_url(city)
    coord_text = f"{origin[1]},{origin[0]};{dest[1]},{dest[0]}"
    url = f"{osrm_url}/route/v1/driving/{coord_text}?overview=false"
    try:
        with urllib_request.urlopen(url, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        routes = payload.get("routes", [])
        if routes:
            return float(routes[0].get("distance", 0.0)) / 1000.0
    except Exception:
        pass
    return _haversine_distance_km(origin, dest)


def _normalize_skill_priorities(skills: list[dict[str, Any]]) -> dict[str, float]:
    priorities: dict[str, float] = {}
    for skill in skills:
        product = _text_value(skill.get("product"), skill.get("SERVICE_PRODUCT_CODE"), skill.get("역량제품명"))
        if product:
            priority = float(
                pd.to_numeric(
                    pd.Series([skill.get("repair_priority", skill.get("REPAIR_PRIORITY", 0.0))]),
                    errors="coerce",
                ).fillna(0.0).iloc[0]
            )
            priorities[product] = priority
    return priorities


def _solve_jobs(
    jobs: list[dict[str, Any]],
    tech_states: list[dict[str, Any]],
    planning_date: str,
    timezone_offset: str,
    city: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    vehicle_count = len(tech_states)
    if vehicle_count == 0:
        return [], [], [
            {
                "salesforce_id": str(job.get("salesforce_id", "")).strip(),
                "receipt_no": str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(),
                "reason": "NO_ELIGIBLE_TECHNICIAN",
            }
            for job in jobs
        ]

    valid_jobs: list[dict[str, Any]] = []
    invalid_jobs: list[dict[str, Any]] = []
    for job in jobs:
        receipt_no = _text_value(job.get("receipt_no"), job.get("salesforce_id"), job.get("접수번호"), job.get("GSFS_RECEIPT_NO"))
        location = job.get("location") or {}
        job_lat = _float_value(location.get("lat"), location.get("latitude"), job.get("lat"), job.get("latitude"), job.get("LATITUDE"), job.get("위도"))
        job_lng = _float_value(location.get("lng"), location.get("longitude"), job.get("lng"), job.get("longitude"), job.get("LONGITUDE"), job.get("경도"))
        if not receipt_no or job_lat is None or job_lng is None:
            invalid_jobs.append(
                {
                    "salesforce_id": str(job.get("salesforce_id", "")).strip(),
                    "receipt_no": receipt_no,
                    "reason": "INVALID_JOB_LOCATION",
                }
            )
            continue
        valid_jobs.append(
            {
                "salesforce_id": _text_value(job.get("salesforce_id"), receipt_no),
                "receipt_no": receipt_no,
                "job_lat": float(job_lat),
                "job_lng": float(job_lng),
                "product": _text_value(job.get("product"), job.get("SERVICE_PRODUCT_CODE"), job.get("역량제품명"), job.get("접수제품명")),
                "service_minutes": int(
                    pd.to_numeric(
                        pd.Series([job.get("service_minutes", job.get("SERVICE_MINUTES", 45))]),
                        errors="coerce",
                    ).fillna(45).iloc[0]
                ),
            }
        )

    if not valid_jobs:
        return [], [], invalid_jobs

    node_coords: list[tuple[float, float]] = []
    starts: list[int] = []
    ends: list[int] = []
    for tech_state in tech_states:
        starts.append(len(node_coords))
        ends.append(len(node_coords))
        node_coords.append((float(tech_state["start_coord"][0]), float(tech_state["start_coord"][1])))
    job_node_offset = len(node_coords)
    for job in valid_jobs:
        node_coords.append((job["job_lat"], job["job_lng"]))

    matrix_size = len(node_coords)
    distance_matrix: list[list[int]] = [[0] * matrix_size for _ in range(matrix_size)]
    for i in range(matrix_size):
        for j in range(matrix_size):
            if i == j:
                continue
            dist_km = _osrm_route_distance_km(node_coords[i], node_coords[j], city)
            distance_matrix[i][j] = max(0, int(round(dist_km * 1000)))

    manager = pywrapcp.RoutingIndexManager(matrix_size, vehicle_count, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    def raw_distance_callback(from_index: int, to_index: int) -> int:
        return distance_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]

    raw_transit_index = routing.RegisterTransitCallback(raw_distance_callback)

    for vehicle_idx, tech in enumerate(tech_states):
        skill_priorities = dict(tech.get("skill_priorities", {}))

        def distance_callback(from_index: int, to_index: int, *, _skill_priorities: dict[str, float] = skill_priorities) -> int:
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            base_cost = distance_matrix[from_node][to_node]
            if to_node >= job_node_offset:
                job = valid_jobs[to_node - job_node_offset]
                bonus = float(_skill_priorities.get(str(job.get("product", "")).strip(), 0.0))
                if bonus > 0:
                    base_cost = max(0, base_cost - int(round(bonus * 1500)))
            return base_cost

        transit_idx = routing.RegisterTransitCallback(distance_callback)
        routing.SetArcCostEvaluatorOfVehicle(transit_idx, vehicle_idx)

    def demand_callback(from_index: int) -> int:
        return 0 if manager.IndexToNode(from_index) < job_node_offset else 1

    demand_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_index,
        0,
        [int(max(0, tech["max_jobs"])) for tech in tech_states],
        True,
        "job_count",
    )
    count_dimension = routing.GetDimensionOrDie("job_count")
    count_dimension.SetGlobalSpanCostCoefficient(12000)

    total_valid_jobs = len(valid_jobs)
    target_floor = total_valid_jobs // vehicle_count
    target_ceil = math.ceil(total_valid_jobs / vehicle_count)
    for vehicle_idx, tech in enumerate(tech_states):
        end_index = routing.End(vehicle_idx)
        max_jobs = int(max(0, tech["max_jobs"]))
        soft_lower = min(target_floor, max_jobs)
        soft_upper = min(target_ceil, max_jobs)
        if soft_lower > 0:
            count_dimension.SetCumulVarSoftLowerBound(end_index, soft_lower, 9000)
        count_dimension.SetCumulVarSoftUpperBound(end_index, soft_upper, 12000)

    routing.AddDimension(
        raw_transit_index,
        0,
        max(1, int(sum(sum(row) for row in distance_matrix) + 1)),
        True,
        "distance",
    )
    routing.GetDimensionOrDie("distance").SetGlobalSpanCostCoefficient(4000)

    for job_idx, job in enumerate(valid_jobs):
        node = job_node_offset + job_idx
        index = manager.NodeToIndex(node)
        allowed_vehicles = [
            vehicle_idx
            for vehicle_idx, tech in enumerate(tech_states)
            if not tech["eligible_products"] or not job["product"] or job["product"] in tech["eligible_products"]
        ]
        if not allowed_vehicles:
            invalid_jobs.append(
                {
                    "salesforce_id": job["salesforce_id"],
                    "receipt_no": job["receipt_no"],
                    "reason": "NO_ELIGIBLE_TECHNICIAN",
                }
            )
            routing.AddDisjunction([index], 0)
            continue
        for vehicle_idx in range(vehicle_count):
            if vehicle_idx not in allowed_vehicles:
                routing.VehicleVar(index).RemoveValue(vehicle_idx)

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.seconds = 10

    solution = routing.SolveWithParameters(search_parameters)
    if solution is None:
        return [], [], invalid_jobs + [
            {
                "salesforce_id": job["salesforce_id"],
                "receipt_no": job["receipt_no"],
                "reason": "NO_FEASIBLE_SOLUTION",
            }
            for job in valid_jobs
        ]

    assignments: list[dict[str, Any]] = []
    engineer_summary_rows: list[dict[str, Any]] = []
    assigned_receipts: set[str] = set()
    for vehicle_idx, tech_state in enumerate(tech_states):
        index = routing.Start(vehicle_idx)
        sequence = 0
        route_distance_km = 0.0
        while not routing.IsEnd(index):
            next_index = solution.Value(routing.NextVar(index))
            node = manager.IndexToNode(index)
            next_node = manager.IndexToNode(next_index)
            if node >= job_node_offset:
                job = valid_jobs[node - job_node_offset]
                sequence += 1
                slot_time = _next_slot(tech_state["available_slots"], sequence)
                start_minutes = _parse_clock_minutes(slot_time, 9 * 60)
                end_minutes = start_minutes + int(job["service_minutes"])
                assignments.append(
                    {
                        "salesforce_id": job["salesforce_id"],
                        "receipt_no": job["receipt_no"],
                        "employee_code": tech_state["employee_code"],
                        "sequence": sequence,
                        "planned_start": format_planned_timestamp(planning_date, _clock_text_from_minutes(start_minutes), timezone_offset),
                        "planned_end": format_planned_timestamp(planning_date, _clock_text_from_minutes(end_minutes), timezone_offset),
                        "changed": False,
                    }
                )
                assigned_receipts.add(job["receipt_no"])
            if not routing.IsEnd(next_index):
                route_distance_km += distance_matrix[node][next_node] / 1000.0
            index = next_index
        engineer_summary_rows.append(
            {
                "employee_code": tech_state["employee_code"],
                "employee_name": tech_state["employee_name"],
                "assigned_jobs": sequence,
                "route_distance_km": round(route_distance_km, 2),
                "max_jobs": tech_state["max_jobs"],
            }
        )

    unassigned = list(invalid_jobs)
    invalid_receipts = {str(item.get("receipt_no", "")).strip() for item in invalid_jobs}
    for job in valid_jobs:
        if job["receipt_no"] not in assigned_receipts and job["receipt_no"] not in invalid_receipts:
            unassigned.append(
                {
                    "salesforce_id": job["salesforce_id"],
                    "receipt_no": job["receipt_no"],
                    "reason": "NO_FEASIBLE_SOLUTION",
                }
            )
    return assignments, engineer_summary_rows, unassigned


def run_mode(request_payload: dict[str, Any]) -> dict[str, Any]:
    planning_date = str(request_payload.get("planning_date", "")).strip()
    timezone_offset = str(request_payload.get("options", {}).get("timezone_offset", "+09:00")).strip() or "+09:00"
    city = _resolve_runtime_city(request_payload)
    technicians = list(request_payload.get("technicians", []))
    jobs = list(request_payload.get("jobs", []))
    default_max_jobs_per_sm = int(
        pd.to_numeric(pd.Series([request_payload.get("options", {}).get("max_jobs_per_sm", 4)]), errors="coerce")
        .fillna(4)
        .iloc[0]
    )

    tech_states: list[dict[str, Any]] = []
    for tech in technicians:
        employee_code = _normalize_code(_text_value(tech.get("employee_code"), tech.get("사번"), tech.get("SVC_ENGINEER_CODE")))
        start = tech.get("start_location") or {}
        start_lat = _float_value(start.get("lat"), start.get("latitude"), tech.get("latitude"), tech.get("위도"))
        start_lng = _float_value(start.get("lng"), start.get("longitude"), tech.get("longitude"), tech.get("경도"))
        if not employee_code or start_lat is None or start_lng is None:
            continue
        available_slots = _normalize_available_slots(list(tech.get("available_slots", [])))
        max_jobs = int(
            pd.to_numeric(pd.Series([tech.get("max_jobs", default_max_jobs_per_sm)]), errors="coerce")
            .fillna(default_max_jobs_per_sm)
            .iloc[0]
        )
        if max_jobs <= 0:
            continue
        skill_priorities = _normalize_skill_priorities(list(tech.get("skills", [])))
        tech_states.append(
            {
                "employee_code": employee_code,
                "employee_name": _text_value(tech.get("employee_name"), tech.get("이름"), employee_code),
                "center_type": _text_value(tech.get("center_type"), tech.get("센터명"), "KOREA_SM"),
                "skill_priorities": skill_priorities,
                "eligible_products": set(skill_priorities),
                "start_coord": (float(start_lat), float(start_lng)),
                "available_slots": available_slots,
                "max_jobs": max_jobs,
            }
        )

    assignments, engineer_summary_rows, unassigned = _solve_jobs(
        jobs=jobs,
        tech_states=tech_states,
        planning_date=planning_date,
        timezone_offset=timezone_offset,
        city=city,
    )
    return {
        "request_id": str(request_payload.get("request_id", "")).strip(),
        "mode": "z_weekend",
        "city": city,
        "status": "completed",
        "summary": {
            "total_jobs": len(jobs),
            "assigned_jobs": len(assignments),
            "unassigned_jobs": len(unassigned),
        },
        "assignments": assignments,
        "unassigned": unassigned,
        "engineer_summary": engineer_summary_rows,
    }
