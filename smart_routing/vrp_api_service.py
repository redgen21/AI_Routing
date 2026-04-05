from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

import smart_routing.production_assign_atlanta as base
import smart_routing.production_atlanta as prod
from smart_routing.production_assign_atlanta_vrp import build_atlanta_production_assignment_vrp_from_frames


JOB_ROOT = Path("260310/vrp_api_jobs")
DEFAULT_TIMEZONE_OFFSET = "-04:00"
_JOB_LOCK = threading.Lock()


@dataclass
class JobPaths:
    job_dir: Path
    request_path: Path
    status_path: Path
    result_path: Path
    error_path: Path


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _ensure_job_root() -> Path:
    JOB_ROOT.mkdir(parents=True, exist_ok=True)
    return JOB_ROOT


def build_job_paths(job_id: str) -> JobPaths:
    job_dir = _ensure_job_root() / str(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    return JobPaths(
        job_dir=job_dir,
        request_path=job_dir / "request.json",
        status_path=job_dir / "status.json",
        result_path=job_dir / "result.json",
        error_path=job_dir / "error.txt",
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def create_job_id(request_id: str | None = None) -> str:
    base_id = str(request_id).strip() if request_id else ""
    if base_id:
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in base_id)
        return f"{safe}_{uuid.uuid4().hex[:8]}"
    return f"vrp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"


def save_new_job(job_id: str, request_payload: dict[str, Any]) -> None:
    paths = build_job_paths(job_id)
    _write_json(paths.request_path, request_payload)
    _write_json(
        paths.status_path,
        {
            "job_id": job_id,
            "status": "queued",
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        },
    )


def load_status(job_id: str) -> dict[str, Any]:
    paths = build_job_paths(job_id)
    if not paths.status_path.exists():
        raise FileNotFoundError(job_id)
    status = _read_json(paths.status_path)
    if paths.error_path.exists():
        status["error_message"] = paths.error_path.read_text(encoding="utf-8").strip()
    return status


def load_result(job_id: str) -> dict[str, Any]:
    paths = build_job_paths(job_id)
    if not paths.result_path.exists():
        raise FileNotFoundError(job_id)
    return _read_json(paths.result_path)


def _update_status(job_id: str, **fields: Any) -> None:
    with _JOB_LOCK:
        paths = build_job_paths(job_id)
        status = _read_json(paths.status_path) if paths.status_path.exists() else {"job_id": job_id}
        status.update(fields)
        status["updated_at"] = _utc_now_iso()
        _write_json(paths.status_path, status)


def _load_reference_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    region_zip_df = pd.read_csv(base.REGION_ZIP_PATH, encoding="utf-8-sig")
    engineer_region_df = pd.read_csv(base.ENGINEER_REGION_PATH, encoding="utf-8-sig")
    home_df = pd.read_csv(base.HOME_GEOCODE_PATH, encoding="utf-8-sig")
    region_zip_df["POSTAL_CODE"] = region_zip_df["POSTAL_CODE"].astype(str).str.zfill(5)
    return region_zip_df, engineer_region_df, home_df


def _normalize_shift_time(value: str | None, fallback: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    if len(raw) == 5 and raw[2] == ":":
        return raw
    return fallback


def _build_region_lookup(region_zip_df: pd.DataFrame) -> dict[str, tuple[int, str]]:
    lookup_df = region_zip_df[["POSTAL_CODE", "region_seq", "new_region_name"]].dropna(subset=["POSTAL_CODE"]).drop_duplicates()
    lookup_df["POSTAL_CODE"] = lookup_df["POSTAL_CODE"].astype(str).str.zfill(5)
    return {
        str(row["POSTAL_CODE"]).zfill(5): (int(row["region_seq"]), str(row["new_region_name"]))
        for _, row in lookup_df.iterrows()
        if pd.notna(row["region_seq"])
    }


def _build_region_centers_from_service_df(service_df: pd.DataFrame) -> dict[int, tuple[float, float]]:
    if service_df.empty or "region_seq" not in service_df.columns:
        return {}
    working = service_df.copy()
    working["region_seq"] = pd.to_numeric(working["region_seq"], errors="coerce")
    working["latitude"] = pd.to_numeric(working["latitude"], errors="coerce")
    working["longitude"] = pd.to_numeric(working["longitude"], errors="coerce")
    working = working.dropna(subset=["region_seq", "latitude", "longitude"]).copy()
    if working.empty:
        return {}
    centers = (
        working.groupby("region_seq")
        .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"))
        .reset_index()
    )
    return {
        int(row["region_seq"]): (float(row["longitude"]), float(row["latitude"]))
        for _, row in centers.iterrows()
    }


def _nearest_region(
    lon: float,
    lat: float,
    region_centers: dict[int, tuple[float, float]],
) -> tuple[int, str]:
    best_region = 1
    best_km = None
    for region_seq, center in region_centers.items():
        km = base._haversine_distance_km((float(lon), float(lat)), center)
        if best_km is None or km < best_km:
            best_km = km
            best_region = int(region_seq)
    return best_region, f"Atlanta New Region {best_region}"


def _build_engineer_frames_from_payload(
    request_payload: dict[str, Any],
    reference_engineer_region_df: pd.DataFrame,
    reference_home_df: pd.DataFrame,
    region_centers: dict[int, tuple[float, float]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    technicians = list(request_payload.get("technicians", []))
    ref_engineer = reference_engineer_region_df.copy()
    ref_engineer["SVC_ENGINEER_CODE"] = ref_engineer["SVC_ENGINEER_CODE"].astype(str)
    ref_home = reference_home_df.copy()
    ref_home["SVC_ENGINEER_CODE"] = ref_home["SVC_ENGINEER_CODE"].astype(str)

    requested_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
    }
    if not requested_codes:
        requested_codes = {
            str(job.get("current_employee_code", "")).strip()
            for job in request_payload.get("jobs", [])
            if str(job.get("current_employee_code", "")).strip()
        }
    if requested_codes:
        ref_engineer = ref_engineer[ref_engineer["SVC_ENGINEER_CODE"].astype(str).isin(requested_codes)].copy()
        ref_home = ref_home[ref_home["SVC_ENGINEER_CODE"].astype(str).isin(requested_codes)].copy()

    engineer_rows: list[dict[str, Any]] = []
    home_rows: list[dict[str, Any]] = []
    for tech in technicians:
        code = str(tech.get("employee_code", "")).strip()
        if not code:
            continue
        name = str(tech.get("employee_name", code)).strip() or code
        start_location = tech.get("start_location") or {}
        start_lat = pd.to_numeric(pd.Series([start_location.get("lat")]), errors="coerce").iloc[0]
        start_lng = pd.to_numeric(pd.Series([start_location.get("lng")]), errors="coerce").iloc[0]

        matched_region = ref_engineer[ref_engineer["SVC_ENGINEER_CODE"].astype(str) == code].head(1)
        matched_home = ref_home[ref_home["SVC_ENGINEER_CODE"].astype(str) == code].head(1)
        if not matched_region.empty:
            region_row = matched_region.iloc[0].to_dict()
        else:
            if pd.notna(start_lat) and pd.notna(start_lng):
                region_seq, region_name = _nearest_region(float(start_lng), float(start_lat), region_centers)
            else:
                region_seq, region_name = 1, "Atlanta New Region 1"
            region_row = {
                "SVC_ENGINEER_CODE": code,
                "assigned_region_seq": int(region_seq),
                "zip_overlap_count": 0,
                "zip_overlap_ratio": 0.0,
                "AREA_NAME": f"{code}_{name}",
                "SVC_CENTER_TYPE": str(tech.get("center_type", "DMS")).strip().upper() or "DMS",
                "assigned_region_name": region_name,
                "preferred_region_rank_1": pd.NA,
                "preferred_region_rank_2": pd.NA,
                "preferred_region_rank_3": pd.NA,
                "anchor_region_seq": int(region_seq),
                "anchor_region_name": region_name,
                "Name": name,
                "normalized_slot": 8,
                "REF_HEAVY_REPAIR_FLAG": "Y",
            }
        region_row["SVC_ENGINEER_CODE"] = code
        region_row["Name"] = name
        region_row["SVC_CENTER_TYPE"] = str(tech.get("center_type", region_row.get("SVC_CENTER_TYPE", "DMS"))).strip().upper() or "DMS"
        if "ref_heavy_repair_flag" in tech:
            region_row["REF_HEAVY_REPAIR_FLAG"] = str(tech.get("ref_heavy_repair_flag", "Y")).strip().upper() or "Y"
        engineer_rows.append(region_row)

        if not matched_home.empty:
            home_row = matched_home.iloc[0].to_dict()
        else:
            home_row = {
                "SVC_ENGINEER_CODE": code,
                "Name": name,
                "Home Street Address": "",
                "City ": "",
                "State": "",
                "Zip": "",
                "matched_address": "",
                "match_indicator": "",
                "match_type": "",
                "source": "api_payload",
                "SVC_CENTER_TYPE": region_row["SVC_CENTER_TYPE"],
                "normalized_slot": region_row.get("normalized_slot", 8),
                "REF_HEAVY_REPAIR_FLAG": region_row.get("REF_HEAVY_REPAIR_FLAG", "Y"),
                "assigned_region_seq": region_row.get("assigned_region_seq"),
                "assigned_region_name": region_row.get("assigned_region_name"),
            }
        home_row["SVC_ENGINEER_CODE"] = code
        home_row["Name"] = name
        home_row["SVC_CENTER_TYPE"] = region_row["SVC_CENTER_TYPE"]
        home_row["latitude"] = float(start_lat) if pd.notna(start_lat) else pd.NA
        home_row["longitude"] = float(start_lng) if pd.notna(start_lng) else pd.NA
        home_rows.append(home_row)

    if not engineer_rows and requested_codes:
        engineer_rows = ref_engineer.to_dict("records")
    if not home_rows and requested_codes:
        home_rows = ref_home.to_dict("records")
    return pd.DataFrame(engineer_rows), pd.DataFrame(home_rows)


def _build_service_frame_from_payload(
    request_payload: dict[str, Any],
    region_lookup: dict[str, tuple[int, str]],
) -> pd.DataFrame:
    planning_date = str(request_payload.get("planning_date", "")).strip()
    jobs = list(request_payload.get("jobs", []))
    rows: list[dict[str, Any]] = []
    for job in jobs:
        receipt_no = str(job.get("receipt_no", "")).strip()
        salesforce_id = str(job.get("salesforce_id", "")).strip()
        location = job.get("location") or {}
        lat = pd.to_numeric(pd.Series([location.get("lat")]), errors="coerce").iloc[0]
        lng = pd.to_numeric(pd.Series([location.get("lng")]), errors="coerce").iloc[0]
        if pd.isna(lat) or pd.isna(lng):
            continue
        postal_code = str(job.get("postal_code", "") or job.get("zip_code", "")).strip()
        if postal_code:
            postal_code = postal_code.zfill(5)
        if postal_code and postal_code in region_lookup:
            region_seq, region_name = region_lookup[postal_code]
        else:
            region_seq, region_name = pd.NA, ""
        time_window = job.get("time_window") or []
        row = {
            "salesforce_id": salesforce_id,
            "GSFS_RECEIPT_NO": receipt_no or salesforce_id,
            "SVC_ENGINEER_CODE": str(job.get("current_employee_code", "")).strip(),
            "SVC_ENGINEER_NAME": str(job.get("current_employee_name", job.get("current_employee_code", ""))).strip(),
            "SERVICE_PRODUCT_GROUP_CODE": str(job.get("product_group", "") or job.get("product", "")).strip().upper(),
            "SERVICE_PRODUCT_CODE": str(job.get("product", "") or job.get("service_product_code", "")).strip().upper(),
            "RECEIPT_DETAIL_SYMPTOM_CODE": str(job.get("symptom", "") or job.get("symptom_code", "")).strip().upper(),
            "ADDRESS_LINE1_INFO": str(job.get("address", "")).strip(),
            "CITY_NAME": str(job.get("city_name", "")).strip(),
            "STATE_NAME": str(job.get("state_name", "")).strip(),
            "COUNTRY_NAME": str(job.get("country_name", "USA")).strip() or "USA",
            "POSTAL_CODE": postal_code,
            "latitude": float(lat),
            "longitude": float(lng),
            "service_date": pd.to_datetime(planning_date, errors="coerce"),
            "service_date_key": str(planning_date),
            "PROMISE_DATE": planning_date.replace("-", ""),
            "PROMISE_TIMESTAMP": f"{planning_date}T{str(time_window[0]).strip() if len(time_window) >= 1 else '09:00'}:00",
            "time_window_start": str(time_window[0]).strip() if len(time_window) >= 1 else "",
            "time_window_end": str(time_window[1]).strip() if len(time_window) >= 2 else "",
            "priority": int(pd.to_numeric(pd.Series([job.get('priority', 0)]), errors='coerce').fillna(0).iloc[0]),
            "fixed": bool(job.get("fixed", False)),
            "current_employee_code": str(job.get("current_employee_code", "")).strip(),
            "region_seq": int(region_seq),
            "new_region_name": region_name,
            "SVC_CENTER_TYPE": str(job.get("current_center_type", "")).strip().upper() or "DMS",
            "is_tv_job": bool(job.get("is_tv_job", False)),
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    if not df.empty:
        region_centers = _build_region_centers_from_service_df(df)
        missing_region_mask = df["region_seq"].isna() | (df["new_region_name"].astype(str).str.strip() == "")
        if missing_region_mask.any():
            for idx in df.index[missing_region_mask]:
                row = df.loc[idx]
                if region_centers:
                    region_seq, region_name = _nearest_region(float(row["longitude"]), float(row["latitude"]), region_centers)
                else:
                    region_seq, region_name = 1, "Atlanta New Region 1"
                df.at[idx, "region_seq"] = int(region_seq)
                df.at[idx, "new_region_name"] = region_name
        df = df.sort_values(["service_date_key", "GSFS_RECEIPT_NO"]).reset_index(drop=True)
        heavy_lookup_df = prod._build_heavy_repair_lookup(prod.DEFAULT_SYMPTOM_FILE)
        df = prod._enrich_service_df(df, heavy_lookup_df)
    return df


def _format_planned_timestamp(service_date_key: str, time_text: str, timezone_offset: str) -> str:
    clean_time = str(time_text or "").strip() or "09:00"
    return f"{service_date_key}T{clean_time}:00{timezone_offset}"


def _build_response_payload(
    request_payload: dict[str, Any],
    assignment_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    schedule_df: pd.DataFrame,
) -> dict[str, Any]:
    planning_date = str(request_payload.get("planning_date", "")).strip()
    timezone_offset = str(request_payload.get("options", {}).get("timezone_offset", DEFAULT_TIMEZONE_OFFSET)).strip() or DEFAULT_TIMEZONE_OFFSET
    jobs = list(request_payload.get("jobs", []))
    job_lookup = {
        str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(): job
        for job in jobs
    }
    assigned_receipts = set()
    assignments: list[dict[str, Any]] = []
    if not schedule_df.empty:
        for _, row in schedule_df.iterrows():
            receipt_no = str(row.get("GSFS_RECEIPT_NO", "")).strip()
            payload_job = job_lookup.get(receipt_no, {})
            assigned_receipts.add(receipt_no)
            current_employee_code = str(payload_job.get("current_employee_code", "")).strip()
            assignments.append(
                {
                    "salesforce_id": str(payload_job.get("salesforce_id", row.get("salesforce_id", ""))).strip(),
                    "receipt_no": receipt_no,
                    "employee_code": str(row.get("assigned_sm_code", "")).strip(),
                    "sequence": int(pd.to_numeric(pd.Series([row.get("visit_seq", 0)]), errors="coerce").fillna(0).iloc[0]),
                    "planned_start": _format_planned_timestamp(str(row.get("service_date_key", planning_date)), str(row.get("visit_start_time", "")), timezone_offset),
                    "planned_end": _format_planned_timestamp(str(row.get("service_date_key", planning_date)), str(row.get("visit_end_time", "")), timezone_offset),
                    "changed": bool(current_employee_code and current_employee_code != str(row.get("assigned_sm_code", "")).strip()),
                }
            )

    unassigned: list[dict[str, Any]] = []
    for job in jobs:
        receipt_no = str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
        if receipt_no in assigned_receipts:
            continue
        unassigned.append(
            {
                "salesforce_id": str(job.get("salesforce_id", "")).strip(),
                "receipt_no": receipt_no,
                "reason": "NO_FEASIBLE_ROUTE",
            }
        )

    total_jobs = len(jobs)
    return {
        "request_id": str(request_payload.get("request_id", "")).strip(),
        "status": "completed",
        "summary": {
            "total_jobs": total_jobs,
            "assigned_jobs": len(assignments),
            "unassigned_jobs": len(unassigned),
        },
        "assignments": assignments,
        "unassigned": unassigned,
        "engineer_summary": summary_df.to_dict("records") if not summary_df.empty else [],
    }


def run_vrp_request(request_payload: dict[str, Any]) -> dict[str, Any]:
    region_zip_df, reference_engineer_region_df, reference_home_df = _load_reference_inputs()
    region_lookup = _build_region_lookup(region_zip_df)
    service_df = _build_service_frame_from_payload(request_payload, region_lookup)
    region_centers = _build_region_centers_from_service_df(service_df)
    engineer_region_df, home_df = _build_engineer_frames_from_payload(
        request_payload,
        reference_engineer_region_df,
        reference_home_df,
        region_centers,
    )
    if engineer_region_df.empty or home_df.empty or service_df.empty:
        return {
            "request_id": str(request_payload.get("request_id", "")).strip(),
            "status": "completed",
            "summary": {"total_jobs": len(request_payload.get("jobs", [])), "assigned_jobs": 0, "unassigned_jobs": len(request_payload.get("jobs", []))},
            "assignments": [],
            "unassigned": [
                {
                    "salesforce_id": str(job.get("salesforce_id", "")).strip(),
                    "receipt_no": str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(),
                    "reason": "INVALID_INPUT_DATA",
                }
                for job in request_payload.get("jobs", [])
            ],
        }

    assignment_df, summary_df, schedule_df = build_atlanta_production_assignment_vrp_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=True,
    )

    return _build_response_payload(request_payload, assignment_df, summary_df, schedule_df)


def process_job(job_id: str) -> None:
    paths = build_job_paths(job_id)
    request_payload = _read_json(paths.request_path)
    _update_status(job_id, status="running", started_at=_utc_now_iso())
    try:
        result_payload = run_vrp_request(request_payload)
        _write_json(paths.result_path, result_payload)
        _update_status(
            job_id,
            status="completed",
            completed_at=_utc_now_iso(),
            request_id=str(request_payload.get("request_id", "")).strip(),
            summary=result_payload.get("summary", {}),
        )
    except Exception as exc:
        paths.error_path.write_text(str(exc), encoding="utf-8")
        _update_status(job_id, status="failed", completed_at=_utc_now_iso())
        raise
