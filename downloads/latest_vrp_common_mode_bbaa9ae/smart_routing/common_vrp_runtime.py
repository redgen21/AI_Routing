from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from .common_vrp_db import (
    COMMON_CONFIG_PATH,
    get_latest_routing_request,
    get_routing_config,
    get_routing_request,
    get_routing_result,
    list_engineers,
    list_heavy_repair_rules,
    load_common_config,
    replace_request_technicians,
    upsert_jobs,
    upsert_routing_request,
    upsert_routing_result,
)
from .live_atlanta_runtime import _load_config as _load_runtime_config
from .live_atlanta_runtime import _merge_service_geocodes
from .vrp_api_client import get_routing_job_result, get_routing_job_status, submit_routing_job


def _routing_server_url(config_path: Path = COMMON_CONFIG_PATH) -> str:
    cfg = load_common_config(config_path)
    url = str(cfg.get("routing_api_url", "http://20.51.244.68:8055")).strip()
    return url or "http://20.51.244.68:8055"


def _build_payload_from_dataframes(
    jobs_df: pd.DataFrame,
    technicians_df: pd.DataFrame,
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    if jobs_df.empty:
        raise ValueError("No jobs found for the selected PROMISE_DATE.")
    if technicians_df.empty:
        raise ValueError("No technician list found for the selected PROMISE_DATE.")

    active_technicians = technicians_df[technicians_df["available"].fillna(False).astype(bool)].copy()
    if active_technicians.empty:
        raise ValueError("No available technicians selected.")

    engineer_master_df = list_engineers(subsidiary_name, strategic_city_name, config_path=config_path)
    heavy_repair_rule_df = list_heavy_repair_rules(config_path=config_path)
    routing_config = get_routing_config(subsidiary_name, strategic_city_name, config_path=config_path) or {}
    heavy_repair_exact_key = {
        (
            str(row.get("product_group_code", "")).strip().upper(),
            str(row.get("product_code", "")).strip().upper(),
            str(row.get("detailed_symptom_code", "")).strip().upper(),
        )
        for _, row in heavy_repair_rule_df.iterrows()
        if str(row.get("product_group_code", "")).strip() and str(row.get("product_code", "")).strip()
    }
    heavy_repair_group_key = {
        (
            str(row.get("product_group_code", "")).strip().upper(),
            str(row.get("detailed_symptom_code", "")).strip().upper(),
        )
        for _, row in heavy_repair_rule_df.iterrows()
        if str(row.get("product_group_code", "")).strip() and str(row.get("detailed_symptom_code", "")).strip()
    }

    state_value = str(jobs_df["state_name"].dropna().astype(str).iloc[0]).strip() if "state_name" in jobs_df.columns else ""
    custom_geo_rows: list[dict[str, Any]] = []
    tech_location_lookup: dict[str, tuple[float, float]] = {}

    for _, tech in active_technicians.iterrows():
        employee_code = str(tech["employee_code"]).strip()
        master_row = engineer_master_df[engineer_master_df["employee_code"].astype(str) == employee_code].head(1)
        if master_row.empty:
            raise ValueError(f"Missing technician master row for {employee_code}")
        master_row = master_row.iloc[0]

        start_type = str(tech.get("start_location_type", "Home")).strip() or "Home"
        if start_type == "Custom Address":
            address_line = str(tech.get("start_location_address", "")).strip()
            if not address_line:
                raise ValueError(f"Custom Address is selected but empty for technician {employee_code}")
            custom_geo_rows.append(
                {
                    "GSFS_RECEIPT_NO": employee_code,
                    "ADDRESS_LINE1_INFO": address_line,
                    "CITY_NAME": str(master_row.get("home_city", "")).strip(),
                    "STATE_NAME": str(master_row.get("home_state", state_value)).strip() or state_value,
                    "COUNTRY_NAME": str(master_row.get("home_country", "USA")).strip() or "USA",
                    "POSTAL_CODE": str(master_row.get("home_postal_code", "")).strip().replace(".0", ""),
                }
            )
            continue

        home_lat = pd.to_numeric(master_row.get("home_latitude"), errors="coerce")
        home_lng = pd.to_numeric(master_row.get("home_longitude"), errors="coerce")
        if pd.isna(home_lat) or pd.isna(home_lng):
            raise ValueError(f"Missing home coordinates for technician {employee_code}")
        tech_location_lookup[employee_code] = (float(home_lat), float(home_lng))

    if custom_geo_rows:
        geocoded_custom_df = _merge_service_geocodes(pd.DataFrame(custom_geo_rows), _load_runtime_config())
        geocoded_custom_df["latitude"] = pd.to_numeric(geocoded_custom_df.get("latitude"), errors="coerce")
        geocoded_custom_df["longitude"] = pd.to_numeric(geocoded_custom_df.get("longitude"), errors="coerce")
        failed_df = geocoded_custom_df[geocoded_custom_df["latitude"].isna() | geocoded_custom_df["longitude"].isna()].copy()
        if not failed_df.empty:
            failed_codes = ", ".join(failed_df["GSFS_RECEIPT_NO"].astype(str).tolist())
            raise ValueError(f"Failed to geocode technician start locations: {failed_codes}")
        tech_location_lookup.update(
            {
                str(row["GSFS_RECEIPT_NO"]).strip(): (float(row["latitude"]), float(row["longitude"]))
                for _, row in geocoded_custom_df.iterrows()
            }
        )

    planning_date = f"{str(promise_date)[:4]}-{str(promise_date)[4:6]}-{str(promise_date)[6:8]}"

    technicians_payload: list[dict[str, Any]] = []
    for _, tech in active_technicians.iterrows():
        code = str(tech["employee_code"]).strip()
        if code not in tech_location_lookup:
            raise ValueError(f"Missing start location for technician {code}")
        lat, lng = tech_location_lookup[code]
        technicians_payload.append(
            {
                "employee_code": code,
                "employee_name": str(tech.get("employee_name", code)).strip() or code,
                "center_type": str(tech.get("center_type", "DMS")).strip().upper() or "DMS",
                "start_location": {"lat": float(lat), "lng": float(lng)},
                "end_location": {"lat": float(lat), "lng": float(lng)},
                "shift_start": str(tech.get("shift_start", "08:00")).strip() or "08:00",
                "shift_end": str(tech.get("shift_end", "18:00")).strip() or "18:00",
                "slot_count": int(pd.to_numeric(tech.get("slot_count", 8), errors="coerce")) if pd.notna(pd.to_numeric(tech.get("slot_count", 8), errors="coerce")) else 8,
                "max_jobs": int(pd.to_numeric(tech.get("max_jobs", 8), errors="coerce")) if pd.notna(pd.to_numeric(tech.get("max_jobs", 8), errors="coerce")) else 8,
            }
        )

    jobs_payload: list[dict[str, Any]] = []
    timezone_offset = str(routing_config.get("timezone_offset", "-04:00")).strip() or "-04:00"
    for _, row in jobs_df.iterrows():
        product_group = str(row.get("service_product_group_code", "")).strip().upper()
        product_code = str(row.get("service_product_code", "")).strip().upper()
        symptom = str(row.get("receipt_detail_symptom_code", "")).strip().upper()
        current_employee_code = str(row.get("svc_engineer_code", "")).strip()
        symptom_candidates = [symptom]
        if symptom:
            symptom_candidates.append(symptom[:5])
            symptom_candidates.append(symptom[:3])
        is_heavy_repair = any(
            (
                (product_group, product_code, candidate) in heavy_repair_exact_key
                or (product_group, candidate) in heavy_repair_group_key
            )
            for candidate in symptom_candidates
            if candidate
        )
        jobs_payload.append(
            {
                "salesforce_id": str(row.get("gsfs_receipt_no", "")).strip(),
                "receipt_no": str(row.get("gsfs_receipt_no", "")).strip(),
                "product_group": product_group,
                "product": product_code,
                "symptom": symptom,
                "address": str(row.get("address_line1_info", "")).strip(),
                "city_name": str(row.get("city_name", "")).strip(),
                "state_name": str(row.get("state_name", "")).strip(),
                "country_name": str(row.get("country_name", "USA")).strip() or "USA",
                "postal_code": str(row.get("postal_code", "")).strip(),
                "location": {"lat": float(row["latitude"]), "lng": float(row["longitude"])},
                "service_minutes": 100 if is_heavy_repair else 45,
                "time_window": [],
                "priority": 0,
                "fixed": False,
                "current_employee_code": current_employee_code,
                "current_center_type": "DMS",
                "is_heavy_repair": is_heavy_repair,
            }
        )

    return {
        "request_id": uuid.uuid4().hex,
        "mode": "na_general",
        "city": strategic_city_name,
        "planning_date": planning_date,
        "options": {
            "respect_fixed_jobs": True,
            "objective": "min_total_travel_time",
            "time_limit_seconds": 30,
            "timezone_offset": timezone_offset,
        },
        "technicians": technicians_payload,
        "jobs": jobs_payload,
    }


def build_payload_from_inputs(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    job_rows: list[dict[str, Any]],
    technician_rows: list[dict[str, Any]],
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    jobs_df = pd.DataFrame(job_rows)
    technicians_df = pd.DataFrame(technician_rows)
    return _build_payload_from_dataframes(jobs_df, technicians_df, subsidiary_name, strategic_city_name, promise_date, config_path=config_path)


def submit_routing_from_inputs(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    job_rows: list[dict[str, Any]],
    technician_rows: list[dict[str, Any]],
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    upsert_jobs(job_rows, config_path=config_path)
    replace_request_technicians(
        subsidiary_name,
        strategic_city_name,
        str(promise_date),
        technician_rows,
        config_path=config_path,
    )
    payload = build_payload_from_inputs(
        subsidiary_name,
        strategic_city_name,
        promise_date,
        job_rows,
        technician_rows,
        config_path=config_path,
    )
    response = submit_routing_job(_routing_server_url(config_path), payload)
    request_row = {
        "request_id": payload["request_id"],
        "subsidiary_name": subsidiary_name,
        "strategic_city_name": strategic_city_name,
        "promise_date": str(promise_date),
        "routing_job_id": str(response.get("job_id", "")).strip(),
        "routing_status": str(response.get("status", "submitted")).strip() or "submitted",
        "payload_json": json.dumps(payload, ensure_ascii=False),
        "status_json": json.dumps(response, ensure_ascii=False),
    }
    upsert_routing_request(request_row, config_path=config_path)
    return {
        "request_id": payload["request_id"],
        "routing_job_id": str(response.get("job_id", "")).strip(),
        "status": str(response.get("status", "submitted")).strip() or "submitted",
        "payload": payload,
    }


def refresh_routing_result(
    request_id: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any]:
    request_row = get_routing_request(request_id, config_path=config_path)
    if not request_row:
        raise ValueError(f"Unknown request_id: {request_id}")
    routing_job_id = str(request_row.get("routing_job_id", "")).strip()
    if not routing_job_id:
        raise ValueError("Missing routing_job_id for request.")

    status_payload = get_routing_job_status(_routing_server_url(config_path), routing_job_id)
    request_row["routing_status"] = str(status_payload.get("status", "")).strip()
    request_row["status_json"] = json.dumps(status_payload, ensure_ascii=False)
    upsert_routing_request(request_row, config_path=config_path)

    result_payload: dict[str, Any] | None = None
    if str(status_payload.get("status", "")).strip().lower() == "completed":
        result_payload = get_routing_job_result(_routing_server_url(config_path), routing_job_id)
        upsert_routing_result(
            {
                "request_id": request_id,
                "routing_job_id": routing_job_id,
                "result_json": json.dumps(result_payload, ensure_ascii=False),
            },
            config_path=config_path,
        )
    else:
        saved_result = get_routing_result(request_id, config_path=config_path)
        if saved_result and saved_result.get("result_json"):
            result_payload = json.loads(str(saved_result["result_json"]))

    return {
        "request_id": request_id,
        "routing_job_id": routing_job_id,
        "status": status_payload,
        "result": result_payload,
    }


def get_latest_routing_snapshot(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str,
    config_path: Path = COMMON_CONFIG_PATH,
) -> dict[str, Any] | None:
    request_row = get_latest_routing_request(subsidiary_name, strategic_city_name, promise_date, config_path=config_path)
    if not request_row:
        return None
    result_row = get_routing_result(str(request_row["request_id"]), config_path=config_path)
    return {
        "request": request_row,
        "result": json.loads(str(result_row["result_json"])) if result_row and result_row.get("result_json") else None,
    }
