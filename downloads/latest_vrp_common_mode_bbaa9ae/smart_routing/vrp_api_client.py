from __future__ import annotations

import json
from pathlib import Path
from urllib import request as urllib_request

import pandas as pd


def _http_json(method: str, url: str, payload: dict | None = None, timeout_sec: int = 60) -> dict:
    data = None
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib_request.Request(url=url, method=method.upper(), data=data, headers=headers)
    with urllib_request.urlopen(req, timeout=timeout_sec) as resp:
        return json.loads(resp.read().decode("utf-8"))


def submit_routing_job(server_url: str, payload: dict, timeout_sec: int = 60) -> dict:
    return _http_json("POST", f"{server_url.rstrip('/')}/api/v1/routing/jobs", payload=payload, timeout_sec=timeout_sec)


def get_routing_job_status(server_url: str, job_id: str, timeout_sec: int = 30) -> dict:
    return _http_json("GET", f"{server_url.rstrip('/')}/api/v1/routing/jobs/{job_id}", timeout_sec=timeout_sec)


def get_routing_job_result(server_url: str, job_id: str, timeout_sec: int = 60) -> dict:
    return _http_json("GET", f"{server_url.rstrip('/')}/api/v1/routing/jobs/{job_id}/result", timeout_sec=timeout_sec)


def _infer_city_from_service_frame(service_df: pd.DataFrame, fallback: str = "Atlanta, GA") -> str:
    if service_df.empty or "STRATEGIC_CITY_NAME" not in service_df.columns:
        return str(fallback).strip() or "Atlanta, GA"
    city_series = (
        service_df["STRATEGIC_CITY_NAME"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    city_series = city_series[city_series != ""]
    if city_series.empty:
        return str(fallback).strip() or "Atlanta, GA"
    return str(city_series.iloc[0]).strip()


def build_payload_from_service_frame(
    service_df: pd.DataFrame,
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    planning_date: str,
    request_id: str,
    mode: str = "na_general",
    city: str = "",
    respect_fixed_jobs: bool = True,
    objective: str = "min_total_travel_time",
    time_limit_seconds: int = 30,
) -> dict:
    service_working = service_df.copy()
    resolved_city = _infer_city_from_service_frame(service_working, fallback=city)
    service_working["SVC_ENGINEER_CODE"] = service_working["SVC_ENGINEER_CODE"].astype(str).str.strip()
    engineer_working = engineer_region_df.copy()
    engineer_working["SVC_ENGINEER_CODE"] = engineer_working["SVC_ENGINEER_CODE"].astype(str).str.strip()
    home_working = home_df.copy()
    home_working["SVC_ENGINEER_CODE"] = home_working["SVC_ENGINEER_CODE"].astype(str).str.strip()
    home_lookup = home_working.drop_duplicates(subset=["SVC_ENGINEER_CODE"])

    technicians: list[dict] = []
    for _, engineer in engineer_working.drop_duplicates(subset=["SVC_ENGINEER_CODE"]).iterrows():
        code = str(engineer["SVC_ENGINEER_CODE"]).strip()
        home_row = home_lookup[home_lookup["SVC_ENGINEER_CODE"] == code].head(1)
        if home_row.empty:
            continue
        home_row = home_row.iloc[0]
        if pd.isna(home_row.get("latitude")) or pd.isna(home_row.get("longitude")):
            continue
        technicians.append(
            {
                "employee_code": code,
                "employee_name": str(engineer.get("Name", code)).strip() or code,
                "center_type": str(engineer.get("SVC_CENTER_TYPE", "DMS")).strip().upper() or "DMS",
                "start_location": {
                    "lat": float(home_row["latitude"]),
                    "lng": float(home_row["longitude"]),
                },
                "end_location": {
                    "lat": float(home_row["latitude"]),
                    "lng": float(home_row["longitude"]),
                },
                "shift_start": "08:00",
                "shift_end": "18:00",
            }
        )

    jobs: list[dict] = []
    for _, row in service_working.iterrows():
        if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
            continue
        jobs.append(
            {
                "salesforce_id": str(row.get("salesforce_id", row.get("GSFS_RECEIPT_NO", ""))).strip(),
                "receipt_no": str(row.get("GSFS_RECEIPT_NO", "")).strip(),
                "product": str(row.get("SERVICE_PRODUCT_CODE", "")).strip().upper(),
                "product_group": str(row.get("SERVICE_PRODUCT_GROUP_CODE", "")).strip().upper(),
                "symptom": str(row.get("RECEIPT_DETAIL_SYMPTOM_CODE", "")).strip().upper(),
                "address": str(row.get("ADDRESS_LINE1_INFO", "")).strip(),
                "postal_code": str(row.get("POSTAL_CODE", "")).strip(),
                "location": {"lat": float(row["latitude"]), "lng": float(row["longitude"])},
                "service_minutes": int(pd.to_numeric(pd.Series([row.get("service_time_min", 45)]), errors="coerce").fillna(45).iloc[0]),
                "time_window": [],
                "priority": int(pd.to_numeric(pd.Series([row.get("priority", 0)]), errors="coerce").fillna(0).iloc[0]),
                "fixed": False,
                "current_employee_code": str(row.get("SVC_ENGINEER_CODE", "")).strip(),
                "current_center_type": str(row.get("SVC_CENTER_TYPE", "")).strip().upper(),
                "is_heavy_repair": bool(row.get("is_heavy_repair", False)),
            }
        )

    return {
        "request_id": request_id,
        "mode": str(mode).strip() or "na_general",
        "city": resolved_city,
        "planning_date": planning_date,
        "options": {
            "respect_fixed_jobs": bool(respect_fixed_jobs),
            "time_limit_seconds": int(time_limit_seconds),
            "objective": objective,
            "timezone_offset": "-04:00",
        },
        "technicians": technicians,
        "jobs": jobs,
    }


def result_to_schedule_df(result_payload: dict) -> pd.DataFrame:
    assignments = list(result_payload.get("assignments", []))
    if not assignments:
        return pd.DataFrame()
    return pd.DataFrame(assignments)


def save_result_json(result_payload: dict, output_path: str | Path) -> None:
    Path(output_path).write_text(json.dumps(result_payload, ensure_ascii=False, indent=2), encoding="utf-8")
