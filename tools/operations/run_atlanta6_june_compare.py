from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smart_routing.common_vrp_runtime import _apply_active_region_plan, _apply_job_capabilities
from smart_routing.data_catalog import na_data_path
from smart_routing.production_assign_atlanta_vrp import build_atlanta_production_assignment_vrp_from_frames
from smart_routing.vrp_api_service import run_routing_request
from smart_routing import vrp_mode_na_general as na_general_mode
from tools.data.atlanta_6area_plan import POLICY_VERSION


INPUT_DIR = ROOT / "260310" / "atlanta 2606_test"
OUTPUT_DIR = INPUT_DIR / "atlanta_6area_comparison"
CHECKPOINT_DIR = OUTPUT_DIR / "_constraint_probe_checkpoints_v3_policy_v2"
JOBS_FILE = INPUT_DIR / "atlanta_jobs_20260601_20260630.csv"
TECH_FILE = INPUT_DIR / "atlanta_technicians_20260601_20260630.csv"
BASELINE_STATS_FILE = INPUT_DIR / "atlanta_routing_daily_stats_20260601_20260630.csv"
BASELINE_RESULTS_FILE = INPUT_DIR / "atlanta_routing_results_20260601_20260630.csv"
REGION_FILE = ROOT / "data" / "north_america" / "reviewed" / "regions" / "fixed_region_postal_atlanta_6area_atlanta_6area_new_atl_buckets_20260721_v2.csv"
HOME_FILE = ROOT / "data" / "north_america" / "db_input" / "technicians" / "atlanta_engineer_home_geocoded.csv"
PROFILE_FILE = na_data_path("profile_production")
PLAN_ID = "atlanta_6area_v2_51a40fc9ddf29b6b2e095050ecc24a72e874b9f93f383b33f55360f21f3202d2"
PLAN_CHECKSUM = "94869889f2e8ea60ca528aa59f0562c9f4b38d655c50782bd57cbcc833c2047b"
POLICY = POLICY_VERSION
OVERFLOW_POSTALS = {"30028", "30040", "30041", "30107"}
ASSIGNMENTS = {
    "AI105115": 6, "AI102448": 1, "AI102087": 1, "AI105116": 1,
    "AI103146": 2, "AI102608": 2, "AI102977": 2, "AI103128": 3,
    "AI103261": 4, "AI103317": 4, "AI005576": 4, "AI103264": 5,
    "AI102961": 5, "AI102315": 5,
}
REGION_NAMES = {
    1: "Atlanta_6area Zone 1", 2: "Atlanta_6area Zone 2",
    3: "Atlanta_6area Zone 3", 4: "Atlanta_6area Zone 4",
    5: "Atlanta_6area Zone 5", 6: "Atlanta_6area ATL Outer Area",
}

KM_TO_MILES = 0.621371
WORK_PROBE_MAX_MINUTES = 600
CONSTRAINT_PROBE_TIME_LIMIT_SECONDS = 3
DIAGNOSTIC_CLASSES = (
    "CAPACITY_SLOT_SHORTAGE",
    "WORK_TIME_LIMIT",
    "TRAVEL_DISTANCE_CONSTRAINT",
    "MULTIPLE_CONSTRAINTS",
    "UNDETERMINED",
)
CHECKPOINT_VERSION = "atlanta6_constraint_probe_v3_policy_v2"
ARTIFACT_INTEGRITY_FILE = "artifact_integrity_manifest.json"
INTEGRATED_STATISTICS_WORKBOOK = "atlanta_6area_integrated_statistics.xlsx"
INTEGRATED_WORKBOOK_CAPACITY_COLUMNS = (
    "promise_date",
    "active_technicians_existing",
    "available_slots_existing",
    "active_technicians_atlanta6",
    "available_slots_atlanta6",
)
TOP_LEVEL_DIRECTORY_CLASSIFICATION = {
    "_constraint_probe_checkpoints_v3_policy_v2": "current_execution_evidence",
    "daily_inputs": "input_derivatives",
    "_archive_prior_run": "excluded_archive_non_current",
}
CURRENT_ARTIFACT_ALLOWLIST = (
    "area_assignment_flow.csv",
    "area_daily_demand_stats.csv",
    "area_daily_technician_stats.csv",
    "area_overall_demand_stats.csv",
    "area_overall_technician_stats.csv",
    "area_technician_route_distance_detail.csv",
    "area_unassigned_reasons.csv",
    "atlanta_6area_area_statistics.xlsx",
    "atlanta_6area_daily_metrics.csv",
    "atlanta_6area_integrated_area_statistics.csv",
    "atlanta_6area_integrated_statistics.xlsx",
    "atlanta_6area_result_type_slot_counts.csv",
    "atlanta_6area_routing_results.csv",
    "atlanta_6area_unassigned_diagnostics.csv",
    "atlanta_6area_unassigned_reasons.csv",
    "atlanta_6area_vs_existing_summary.xlsx",
    "constraint_probe_run_status.csv",
    "daily_comparison.csv",
    "daily_metrics_all_scenarios.csv",
    "executive_comparison.csv",
    "fixed_job_policy_accounting.csv",
    "jobs_excluded_from_aligned_comparison.csv",
    "overall_comparison.csv",
    "run_manifest.json",
    "run_status.csv",
    "slot_count_comparison.csv",
    "slot_count_result_type_comparison.csv",
    "solver_diagnostics_by_day.json",
    "technician_input_capacity_roster.csv",
    "weekday_comparison.csv",
    "weekday_unassigned_diagnostics.csv",
)


def clean(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "y", "yes"}


def number(value: object, default: int) -> int:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return default if pd.isna(parsed) else int(parsed)


def postal(value: object) -> str:
    text = clean(value).removesuffix(".0")
    return text.zfill(5) if text else ""


def load_snapshot() -> dict[str, Any]:
    region = pd.read_csv(REGION_FILE, dtype={"POSTAL_CODE": str})
    region["POSTAL_CODE"] = region["POSTAL_CODE"].map(postal)
    if len(region) != 297 or region["POSTAL_CODE"].nunique() != 297:
        raise RuntimeError("Atlanta_6area region contract is not 297 unique ZIPs")
    postals = [
        {
            "postal_code": row.POSTAL_CODE,
            "region_seq": int(row.region_seq),
            "region_name": clean(row.new_region_name),
            "area_type": clean(row.area_type) or "DMS",
        }
        for row in region.itertuples(index=False)
    ]
    technicians = [
        {
            "employee_code": code,
            "assigned_region_seq": seq,
            "assigned_region_name": REGION_NAMES[seq],
            "active_flag": True,
        }
        for code, seq in sorted(ASSIGNMENTS.items())
    ]
    return {
        "enabled": True,
        "status": "active",
        "context_status": "active",
        "plan_id": PLAN_ID,
        "revision": 2,
        "policy_version": POLICY,
        "checksum": PLAN_CHECKSUM,
        "activation_revision": 1,
        "regions": [
            {"region_seq": seq, "region_id": f"atlanta_6area_r{seq:02d}", "region_name": name}
            for seq, name in REGION_NAMES.items()
        ],
        "postals": postals,
        "technicians": technicians,
        "boundary_overflow": [
            {
                "postal_code": code,
                "primary_region_seq": 3,
                "alternate_region_seq": 2,
                "allow_overflow": True,
                "penalty_cost": 4500,
            }
            for code in sorted(OVERFLOW_POSTALS)
        ],
    }


def load_capabilities(codes: set[str]) -> list[dict[str, Any]]:
    product = pd.read_excel(PROFILE_FILE, sheet_name="3. Product", dtype=object)
    product.columns = [str(col).strip() for col in product.columns]
    product["SVC_ENGINEER_CODE"] = product["SVC_ENGINEER_CODE"].map(clean)
    product = product[
        product["SVC_ENGINEER_CODE"].isin(codes)
        & product["STRATEGIC_CITY_NAME"].map(clean).eq("Atlanta, GA")
        & product["REPAIR_FLAG"].map(clean).str.upper().eq("T")
    ].copy()
    rows: list[dict[str, Any]] = []
    for row in product.itertuples(index=False):
        values = row._asdict()
        group = clean(values.get("SERVICE_PRODUCT_GROUP_CODE")).upper()
        code = clean(values.get("SERVICE_PRODUCT_CODE")).upper()
        employee = clean(values.get("SVC_ENGINEER_CODE"))
        if employee and group:
            rows.append({
                "employee_code": employee,
                "product_group_code": group,
                "product_code": code,
                "heavy_repair_allowed": not (
                    group == "REF" and clean(values.get("AREA_PRODUCT_FLAG")).upper() == "N"
                ),
            })
    unique = {(r["employee_code"], r["product_group_code"], r["product_code"]): r for r in rows}
    return list(unique.values())


def technician_payload(day: pd.DataFrame, homes: pd.DataFrame) -> list[dict[str, Any]]:
    homes_by_code = homes.set_index("SVC_ENGINEER_CODE").to_dict("index")
    rows: list[dict[str, Any]] = []
    for row in day.itertuples(index=False):
        values = row._asdict()
        code = clean(values.get("employee_code"))
        if code not in ASSIGNMENTS or not truthy(values.get("available")):
            continue
        home = homes_by_code.get(code)
        if not home or pd.isna(home.get("latitude")) or pd.isna(home.get("longitude")):
            raise RuntimeError(f"Missing technician home coordinate: {code}")
        slots = max(0, number(values.get("slot_count"), 8))
        max_jobs = max(0, number(values.get("max_jobs"), slots))
        max_minutes = max(0, number(values.get("max_minutes"), 540))
        rows.append({
            "employee_code": code,
            "employee_name": clean(values.get("employee_name")) or code,
            "center_type": clean(values.get("center_type")) or "DMS",
            "available": True,
            "shift_start": clean(values.get("shift_start")) or "09:00",
            "shift_end": clean(values.get("shift_end")) or "18:00",
            "slot_count": slots,
            "max_slots": slots,
            "max_jobs": max_jobs or slots,
            "max_minutes": max_minutes or 540,
            "priority_group": clean(values.get("priority_group")) or "B",
            "start_location": {"lat": float(home["latitude"]), "lng": float(home["longitude"])},
        })
    return rows


def job_payload(day: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in day.itertuples(index=False):
        v = row._asdict()
        receipt = clean(v.get("gsfs_receipt_no"))
        if not receipt:
            continue
        rows.append({
            "salesforce_id": receipt,
            "receipt_no": receipt,
            "product_group": clean(v.get("service_product_group_code")).upper(),
            "product": clean(v.get("service_product_code")).upper(),
            "symptom": clean(v.get("receipt_detail_symptom_code")).upper(),
            "address": clean(v.get("address_line1_info")),
            "city_name": clean(v.get("city_name")),
            "state_name": clean(v.get("state_name")),
            "country_name": clean(v.get("country_name")) or "USA",
            "postal_code": postal(v.get("postal_code")),
            "location": {"lat": float(v.get("latitude")), "lng": float(v.get("longitude"))},
            "priority": 0,
            "fixed": truthy(v.get("fixed")),
            "reschedule": truthy(v.get("reschedule")),
            "job_slot_count": max(1, number(v.get("job_slot_count"), 1)),
            "current_employee_code": clean(v.get("svc_engineer_code")),
            "current_employee_name": clean(v.get("svc_engineer_name")),
            "current_center_type": "DMS",
            "area_type": "DMS",
        })
    return rows


def build_daily_metrics(
    date: int, jobs: pd.DataFrame, techs: list[dict[str, Any]], result: dict[str, Any], elapsed: float
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    slot_by_receipt = {
        clean(row.gsfs_receipt_no): max(1, number(row.job_slot_count, 1))
        for row in jobs.itertuples(index=False)
    }
    assignments = list(result.get("assignments", []))
    unassigned = list(result.get("unassigned", []))
    assigned_receipts = {clean(row.get("receipt_no") or row.get("salesforce_id")) for row in assignments}
    used_slots = sum(slot_by_receipt.get(receipt, 1) for receipt in assigned_receipts)
    capacity = sum(int(row["slot_count"]) for row in techs)
    dispatched_techs = len({clean(row.get("employee_code")) for row in assignments if clean(row.get("employee_code"))})
    active_techs = len(techs)
    total_jobs = len(jobs)
    assigned_jobs = len(assigned_receipts)
    metrics = {
        "promise_date": int(date),
        "scenario": "Atlanta_6area",
        "total_jobs": total_jobs,
        "dispatch_jobs": assigned_jobs,
        "dispatch_slots": used_slots,
        "not_dispatch_jobs": total_jobs - assigned_jobs,
        "active_technicians": active_techs,
        "dispatched_technicians": dispatched_techs,
        "available_slots": capacity,
        "avg_jobs": assigned_jobs / active_techs if active_techs else 0.0,
        "avg_slots": used_slots / active_techs if active_techs else 0.0,
        "job_fill_rate_pct": assigned_jobs / total_jobs * 100.0 if total_jobs else 0.0,
        "fill_rate_pct": used_slots / capacity * 100.0 if capacity else 0.0,
        "runtime_seconds": elapsed,
    }
    detail = []
    for row in assignments:
        receipt = clean(row.get("receipt_no") or row.get("salesforce_id"))
        detail.append({"promise_date": int(date), "result_type": "assigned", **row, "job_slot_count": slot_by_receipt.get(receipt, 1)})
    for row in unassigned:
        receipt = clean(row.get("receipt_no") or row.get("salesforce_id"))
        detail.append({"promise_date": int(date), "result_type": "unassigned", **row, "job_slot_count": slot_by_receipt.get(receipt, 1)})
    reasons = pd.DataFrame(unassigned)
    reason_rows = []
    if not reasons.empty:
        key = "reason" if "reason" in reasons.columns else "unassigned_reason"
        for reason, group in reasons.groupby(key, dropna=False):
            reason_rows.append({"promise_date": int(date), "unassigned_reason": clean(reason) or "UNKNOWN", "jobs": len(group)})
    return metrics, detail, reason_rows


def technician_input_capacity_roster(
    technicians: pd.DataFrame,
    *,
    scenario: str,
    dates: list[int],
    plan_only: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize the daily technician input that defines available capacity.

    This is deliberately independent of assignment/result records.  A
    technician contributes capacity iff their daily input row is available;
    an available technician with zero assigned jobs remains in the roster and
    in the daily capacity denominator.  Atlanta_6area first filters the raw
    daily input to the immutable plan, while Existing retains the original
    routing input roster.
    """

    required = {"promise_date", "employee_code", "available", "slot_count"}
    missing = required - set(technicians.columns)
    if missing:
        raise RuntimeError(f"Technician input is missing required columns: {sorted(missing)}")
    expected_dates = {int(date) for date in dates}
    source = technicians.copy()
    source["promise_date"] = pd.to_numeric(source["promise_date"], errors="raise").astype(int)
    source["employee_code"] = source["employee_code"].map(clean)
    if source["employee_code"].eq("").any():
        raise RuntimeError("Technician input contains an empty employee_code")
    if plan_only:
        source = source[source["employee_code"].isin(ASSIGNMENTS)].copy()
    source = source[source["promise_date"].isin(expected_dates)].copy()
    if source.duplicated(["promise_date", "employee_code"]).any():
        raise RuntimeError(f"{scenario} technician input has duplicate date+employee rows")
    actual_dates = set(source["promise_date"])
    if actual_dates != expected_dates:
        raise RuntimeError(
            f"{scenario} technician input dates mismatch: "
            f"missing={sorted(expected_dates - actual_dates)} extra={sorted(actual_dates - expected_dates)}"
        )

    # Match technician_payload's effective slot policy exactly: a blank slot
    # defaults to 8 and any negative input is clipped to zero.
    source["slot_count"] = source["slot_count"].map(lambda value: max(0, number(value, 8)))
    source["available"] = source["available"].map(truthy)
    source["solver_input_eligible"] = source["available"]
    source["scenario"] = scenario
    source["capacity_source"] = (
        "raw_daily_input_filtered_to_immutable_atlanta6_plan"
        if plan_only else "raw_daily_input"
    )
    source["assigned_region_seq"] = source["employee_code"].map(ASSIGNMENTS) if plan_only else pd.NA
    source["assigned_region_name"] = source["assigned_region_seq"].map(REGION_NAMES) if plan_only else pd.NA
    roster = source.reindex(columns=[
        "promise_date", "scenario", "capacity_source", "employee_code", "available",
        "solver_input_eligible", "slot_count", "assigned_region_seq", "assigned_region_name",
    ]).sort_values(["scenario", "promise_date", "employee_code"]).reset_index(drop=True)
    summary = roster.groupby(["promise_date", "scenario", "capacity_source"], as_index=False).agg(
        technician_input_rows=("employee_code", "nunique"),
        solver_input_available_technicians=("solver_input_eligible", "sum"),
        solver_input_available_slots=("slot_count", lambda slots: int(slots[roster.loc[slots.index, "solver_input_eligible"]].sum())),
    )
    return roster, summary.sort_values("promise_date").reset_index(drop=True)


def _apply_input_capacity_metrics(
    metrics: pd.DataFrame,
    capacity_summary: pd.DataFrame,
    *,
    scenario: str,
) -> pd.DataFrame:
    """Set metric denominators from the authoritative solver-input roster."""

    out = metrics.copy()
    capacity = capacity_summary[[
        "promise_date", "solver_input_available_technicians", "solver_input_available_slots",
    ]].copy()
    out["promise_date"] = out["promise_date"].astype(int)
    if set(out["promise_date"]) != set(capacity["promise_date"]):
        raise RuntimeError(f"{scenario} metric dates do not match technician input capacity dates")
    out = out.drop(columns=[
        "active_technicians", "available_slots", "input_available_technicians",
        "input_available_slots",
    ], errors="ignore").merge(capacity, on="promise_date", how="left", validate="one_to_one")
    out["active_technicians"] = out["solver_input_available_technicians"].astype(int)
    out["available_slots"] = out["solver_input_available_slots"].astype(int)
    out["input_available_technicians"] = out["active_technicians"]
    out["input_available_slots"] = out["available_slots"]
    out["avg_jobs"] = out["dispatch_jobs"] / out["active_technicians"].replace(0, pd.NA)
    out["avg_slots"] = out["dispatch_slots"] / out["active_technicians"].replace(0, pd.NA)
    out["fill_rate_pct"] = out["dispatch_slots"] / out["available_slots"].replace(0, pd.NA) * 100.0
    out["fill_rate_pct"] = out["fill_rate_pct"].fillna(0.0)
    if "travel_distance_miles" in out:
        out["avg_travel_miles_per_active_tech"] = (
            out["travel_distance_miles"] / out["active_technicians"].replace(0, pd.NA)
        )
    return out


def baseline_metrics(
    stats: pd.DataFrame,
    technician_inputs: pd.DataFrame,
    baseline_results: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rebuild Existing capacity from raw daily input, never result statistics."""

    dates = stats["promise_date"].astype(int).tolist()
    roster, capacity_summary = technician_input_capacity_roster(
        technician_inputs, scenario="Existing", dates=dates, plan_only=False
    )
    assigned = baseline_results.copy()
    assigned["promise_date"] = assigned["promise_date"].astype(int)
    assigned = assigned[
        assigned["promise_date"].isin(dates)
        & assigned["result_type"].map(clean).str.lower().eq("assigned")
    ]
    dispatched = assigned.groupby("promise_date", as_index=False).agg(
        dispatched_technicians=("employee_code", lambda values: values.map(clean).replace("", pd.NA).nunique())
    )
    out = pd.DataFrame({
        "promise_date": stats["promise_date"].astype(int),
        "scenario": "Existing",
        "total_jobs": stats["total_jobs"].astype(int),
        "dispatch_jobs": stats["assigned_jobs"].astype(int),
        "dispatch_slots": stats["used_slot_count"].fillna(0).astype(int),
        "not_dispatch_jobs": stats["unassigned_jobs"].astype(int),
    })
    out = out.merge(dispatched, on="promise_date", how="left", validate="one_to_one")
    out["dispatched_technicians"] = out["dispatched_technicians"].fillna(0).astype(int)
    out["job_fill_rate_pct"] = stats["job_fill_rate_pct"].astype(float)
    out["runtime_seconds"] = pd.NA
    travel_km = pd.to_numeric(stats.get("total_travel_distance_km"), errors="coerce").fillna(0.0)
    out["travel_distance_km"] = travel_km
    out["travel_distance_miles"] = travel_km * KM_TO_MILES
    out = _apply_input_capacity_metrics(out, capacity_summary, scenario="Existing")
    return out, roster


def build_executive_comparison(overall: pd.DataFrame) -> pd.DataFrame:
    """Build executive values from the current daily-metric aggregation."""

    existing = overall[overall["scenario"].eq("Existing")]
    candidate = overall[overall["scenario"].eq("Atlanta_6area")]
    if existing.empty or candidate.empty:
        return pd.DataFrame(columns=["metric", "existing", "atlanta_6area", "delta_atlanta6_minus_existing"])
    rows = []
    for label, column in (
        ("Total jobs", "total_jobs"), ("Dispatch jobs", "dispatch_jobs"),
        ("Dispatch slots", "dispatch_slots"), ("Not dispatch jobs", "not_dispatch_jobs"),
        ("Daily avg job fill rate (%)", "avg_job_fill_rate_pct"),
        ("Daily avg slot fill rate (%)", "avg_fill_rate_pct"),
        ("Total travel miles", "travel_distance_miles"),
        ("Avg travel miles / active tech-day", "avg_travel_miles_per_active_tech"),
    ):
        base_value = float(existing.iloc[0][column])
        candidate_value = float(candidate.iloc[0][column])
        rows.append({
            "metric": label,
            "existing": base_value,
            "atlanta_6area": candidate_value,
            "delta_atlanta6_minus_existing": candidate_value - base_value,
        })
    return pd.DataFrame(rows)


def validate_integrated_statistics_workbook(
    workbook_path: Path,
    daily_comparison: pd.DataFrame,
) -> None:
    """Reject a workbook whose daily capacity differs from canonical CSV data."""

    required = list(INTEGRATED_WORKBOOK_CAPACITY_COLUMNS)
    missing = set(required) - set(daily_comparison.columns)
    if missing:
        raise RuntimeError(f"Canonical daily comparison is missing capacity columns: {sorted(missing)}")
    try:
        workbook_daily = pd.read_excel(workbook_path, sheet_name="daily_comparison")
    except Exception as exc:
        raise RuntimeError("Integrated statistics workbook lacks a readable daily_comparison sheet") from exc
    missing = set(required) - set(workbook_daily.columns)
    if missing:
        raise RuntimeError(f"Integrated workbook is missing capacity columns: {sorted(missing)}")
    expected = daily_comparison[required].copy()
    actual = workbook_daily[required].copy()
    for frame in (expected, actual):
        frame["promise_date"] = pd.to_numeric(frame["promise_date"], errors="raise").astype(int)
        for column in required[1:]:
            frame[column] = pd.to_numeric(frame[column], errors="raise")
    expected = expected.sort_values("promise_date").reset_index(drop=True)
    actual = actual.sort_values("promise_date").reset_index(drop=True)
    if len(actual) != len(expected) or not actual.equals(expected):
        raise RuntimeError(
            "Integrated statistics workbook daily_comparison capacity does not match canonical CSV"
        )


def rebuild_integrated_statistics_workbook() -> None:
    """Rebuild only the integrated-statistics workbook from current CSV artifacts."""

    daily_comparison = pd.read_csv(OUTPUT_DIR / "daily_comparison.csv")
    sheets = {
        "daily_comparison": daily_comparison,
        "area_statistics": pd.read_csv(OUTPUT_DIR / "atlanta_6area_integrated_area_statistics.csv"),
        "executive": pd.read_csv(OUTPUT_DIR / "executive_comparison.csv"),
        "unassigned_diagnostics": pd.read_csv(OUTPUT_DIR / "atlanta_6area_unassigned_diagnostics.csv"),
        "result_type_slots": pd.read_csv(OUTPUT_DIR / "atlanta_6area_result_type_slot_counts.csv"),
        "slot_count_comparison": pd.read_csv(OUTPUT_DIR / "slot_count_comparison.csv"),
        "slot_count_result_type": pd.read_csv(OUTPUT_DIR / "slot_count_result_type_comparison.csv"),
    }
    workbook_path = OUTPUT_DIR / INTEGRATED_STATISTICS_WORKBOOK
    with pd.ExcelWriter(workbook_path) as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=sheet_name, index=False)
    validate_integrated_statistics_workbook(workbook_path, daily_comparison)


def _positive_number(value: object) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(parsed) if pd.notna(parsed) and float(parsed) > 0 else None


def _receipt(row: dict[str, Any]) -> str:
    return clean(row.get("receipt_no") or row.get("salesforce_id"))


def _assigned_receipts(result: dict[str, Any]) -> set[str]:
    return {_receipt(row) for row in result.get("assignments", []) if _receipt(row)}


def _payload_job_lookup(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {_receipt(job): job for job in payload.get("jobs", []) if _receipt(job)}


def _baseline_slot_usage(result: dict[str, Any]) -> dict[str, int]:
    usage: dict[str, int] = {}
    for assignment in result.get("assignments", []):
        employee_code = clean(assignment.get("employee_code"))
        if not employee_code:
            continue
        usage[employee_code] = usage.get(employee_code, 0) + max(
            1, number(assignment.get("job_slot_count"), 1)
        )
    return usage


def _eligible_remaining_slot_evidence(
    job: dict[str, Any],
    payload: dict[str, Any],
    baseline_result: dict[str, Any],
) -> dict[str, Any]:
    """Return an auditable *current assignment* slot-balance observation.

    It is deliberately not used as a heuristic by itself: the capacity probe
    must also assign the same receipt before the result can be called a slot
    shortage.  The observation lets the report distinguish that conclusion
    from a route reordering artefact.
    """

    capacities = {
        clean(technician.get("employee_code")): max(
            0,
            number(
                technician.get(
                    "max_slots",
                    technician.get("slot_count", technician.get("max_jobs", 0)),
                ),
                0,
            ),
        )
        for technician in payload.get("technicians", [])
        if clean(technician.get("employee_code"))
    }
    eligible_raw = job.get("eligible_employee_codes")
    eligible = (
        sorted({clean(code) for code in eligible_raw if clean(code)})
        if isinstance(eligible_raw, list)
        else sorted(capacities)
    )
    usage = _baseline_slot_usage(baseline_result)
    remaining = sum(max(0, capacities.get(code, 0) - usage.get(code, 0)) for code in eligible)
    job_slots = max(1, number(job.get("job_slot_count"), 1))
    return {
        "eligible_candidate_count": len(eligible),
        "eligible_candidate_codes": ";".join(eligible),
        "eligible_slot_capacity": sum(capacities.get(code, 0) for code in eligible),
        "eligible_assigned_slots": sum(usage.get(code, 0) for code in eligible),
        "eligible_remaining_slots": remaining,
        "job_slot_count": job_slots,
        "eligible_remaining_slot_shortage": remaining < job_slots,
    }


def _has_explicit_travel_constraint(options: dict[str, Any]) -> bool:
    return any(
        _positive_number(options.get(name)) is not None
        for name in (
            "max_travel_min_per_sm_day",
            "max_travel_km_per_sm_day",
            "max_single_leg_min",
            "max_home_to_job_min",
        )
    )


def _build_probe_frames(payload: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Any]:
    """Build the exact NA-general solver inputs without persisting a job.

    `run_routing_request` is the source result.  Probes rebuild the same
    frames and call the same production solver directly so just one hard
    limit can be changed while fixed jobs, capabilities, region policy and
    every other eligibility restriction remain intact.
    """

    region_zip_df, reference_engineer_region_df, reference_home_df = (
        na_general_mode._load_reference_inputs(payload)
    )
    service_df = na_general_mode._build_service_frame_from_payload(
        payload,
        na_general_mode._build_region_lookup(region_zip_df),
    )
    engineer_df, home_df = na_general_mode._build_engineer_frames_from_payload(
        payload,
        reference_engineer_region_df,
        reference_home_df,
        na_general_mode._build_region_centers_from_service_df(service_df),
    )
    return service_df, engineer_df, home_df, na_general_mode._build_city_route_client(payload)


def _run_constraint_probe(payload: dict[str, Any], probe: str) -> dict[str, Any]:
    """Run a deterministic full-day counterfactual and return its assignments.

    Capacity preserves each technician's baseline effective work-minute limit;
    work-time preserves slot capacity; travel removes only explicit distance
    caps.  A probe does not widen fixed, capability, postal or region rules.
    """

    options = dict(payload.get("options") or {})
    service_df, engineer_df, home_df, route_client = _build_probe_frames(payload)
    if service_df.empty or engineer_df.empty or home_df.empty:
        return {"status": "invalid_input", "assigned_receipts": set(), "summary": pd.DataFrame()}

    max_job_slots = max(
        1,
        sum(max(1, number(job.get("job_slot_count"), 1)) for job in payload.get("jobs", [])),
    )
    if probe == "capacity":
        # `_build_engineer_frames_from_payload` has already normalized the
        # effective max_minutes.  Keep that value unchanged while opening
        # only the SlotCount dimension.
        engineer_df = engineer_df.copy()
        engineer_df["max_jobs"] = max_job_slots
        engineer_df["max_slots"] = max_job_slots
    elif probe == "work_time":
        engineer_df = engineer_df.copy()
        # 600 is the production solver's documented absolute work ceiling;
        # it relaxes configured/slot-derived work limits without inventing an
        # unsupported unlimited workday.
        engineer_df["max_minutes"] = WORK_PROBE_MAX_MINUTES
    elif probe == "travel_distance":
        for name in (
            "max_travel_min_per_sm_day",
            "max_travel_km_per_sm_day",
            "max_single_leg_min",
            "max_home_to_job_min",
        ):
            options.pop(name, None)
    elif probe != "baseline":
        raise ValueError(f"Unsupported constraint probe: {probe}")

    started = time.perf_counter()
    assignment_df, summary_df, schedule_df = build_atlanta_production_assignment_vrp_from_frames(
        engineer_region_df=engineer_df,
        home_df=home_df,
        service_df=service_df,
        attendance_limited=False,
        # Probes are counterfactual diagnostics, not the authoritative route.
        # The main response retains its 10-second budget.  A non-isolating
        # 3-second probe is reported as UNDETERMINED rather than guessed.
        time_limit_seconds=CONSTRAINT_PROBE_TIME_LIMIT_SECONDS,
        respect_fixed_jobs=truthy(options.get("respect_fixed_jobs", True)),
        max_work_min_per_sm_day=(
            _positive_number(options.get("max_work_min_per_sm_day"))
            if probe != "work_time"
            else None
        ),
        max_travel_min_per_sm_day=_positive_number(options.get("max_travel_min_per_sm_day")),
        max_travel_km_per_sm_day=_positive_number(options.get("max_travel_km_per_sm_day")),
        max_single_leg_min=_positive_number(options.get("max_single_leg_min")),
        max_home_to_job_min=_positive_number(options.get("max_home_to_job_min")),
        long_leg_penalty_start_min=_positive_number(options.get("long_leg_penalty_start_min")),
        long_leg_penalty_multiplier=_positive_number(options.get("long_leg_penalty_multiplier")),
        route_client=route_client,
    )
    result_df = schedule_df if not schedule_df.empty else assignment_df
    assigned = (
        {clean(value) for value in result_df.get("GSFS_RECEIPT_NO", pd.Series(dtype=object)).tolist() if clean(value)}
        if not result_df.empty
        else set()
    )
    return {
        "status": "completed",
        "assigned_receipts": assigned,
        "summary": summary_df,
        "runtime_seconds": time.perf_counter() - started,
    }


def _no_feasible_receipts_with_eligible_candidates(
    result: dict[str, Any], payload: dict[str, Any]
) -> set[str]:
    jobs = _payload_job_lookup(payload)
    receipts: set[str] = set()
    for row in result.get("unassigned", []):
        reason = clean(row.get("reason") or row.get("unassigned_reason"))
        receipt = _receipt(row)
        eligible = jobs.get(receipt, {}).get("eligible_employee_codes")
        if (
            receipt
            and reason in {"NO_FEASIBLE_ROUTE", "NO_FEASIBLE_MANDATORY_ROUTE"}
            and isinstance(eligible, list)
            and any(clean(code) for code in eligible)
        ):
            receipts.add(receipt)
    return receipts


def _classify_no_feasible_route(
    *,
    baseline_replay_matches: bool,
    slot_evidence: dict[str, Any],
    capacity_probe_assigned: bool,
    work_probe_assigned: bool,
    travel_probe_assigned: bool | None,
) -> tuple[str, str]:
    """Classify only observed counterfactual effects; never infer a cause."""

    if not baseline_replay_matches:
        return "UNDETERMINED", "baseline_replay_mismatch"
    triggers: list[str] = []
    if capacity_probe_assigned and bool(slot_evidence["eligible_remaining_slot_shortage"]):
        triggers.append("capacity_slot")
    if work_probe_assigned:
        triggers.append("work_time")
    if travel_probe_assigned:
        triggers.append("travel_distance")
    if len(triggers) > 1:
        return "MULTIPLE_CONSTRAINTS", ";".join(triggers)
    if triggers == ["capacity_slot"]:
        return "CAPACITY_SLOT_SHORTAGE", "capacity_probe_assigned_with_remaining_slot_shortage"
    if triggers == ["work_time"]:
        return "WORK_TIME_LIMIT", "work_time_probe_assigned"
    if triggers == ["travel_distance"]:
        return "TRAVEL_DISTANCE_CONSTRAINT", "travel_distance_probe_assigned"
    if capacity_probe_assigned:
        return "UNDETERMINED", "capacity_probe_assigned_without_remaining_slot_shortage"
    return "UNDETERMINED", "no_isolating_probe_assigned"


def _route_detail_rows(
    date: int,
    result: dict[str, Any],
    summary_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    assignments = pd.DataFrame(result.get("assignments", []))
    if assignments.empty:
        return []
    assignments["employee_code"] = assignments["employee_code"].map(clean)
    assignments["job_slot_count"] = pd.to_numeric(
        assignments.get("job_slot_count", pd.Series(1, index=assignments.index)), errors="coerce"
    ).fillna(1).astype(int).clip(lower=1)
    loads = assignments.groupby("employee_code", as_index=False).agg(
        dispatch_jobs=("receipt_no", "nunique"),
        used_slots=("job_slot_count", "sum"),
    )
    summary = summary_df.copy() if isinstance(summary_df, pd.DataFrame) else pd.DataFrame()
    if not summary.empty:
        summary["employee_code"] = summary.get("SVC_ENGINEER_CODE", pd.Series("", index=summary.index)).map(clean)
        summary["travel_distance_km"] = pd.to_numeric(
            summary.get("travel_distance_km", summary.get("route_distance_km", 0)), errors="coerce"
        ).fillna(0.0)
        summary["travel_duration_min"] = pd.to_numeric(
            summary.get("travel_time_min", summary.get("route_duration_min", 0)), errors="coerce"
        ).fillna(0.0)
        summary = summary[["employee_code", "travel_distance_km", "travel_duration_min"]].drop_duplicates("employee_code")
        loads = loads.merge(summary, on="employee_code", how="left")
    else:
        loads["travel_distance_km"] = 0.0
        loads["travel_duration_min"] = 0.0
    loads["technician_area"] = loads["employee_code"].map(ASSIGNMENTS).map(REGION_NAMES).fillna("UNASSIGNED_TECHNICIAN_AREA")
    loads["travel_distance_km"] = pd.to_numeric(loads["travel_distance_km"], errors="coerce").fillna(0.0)
    loads["travel_duration_min"] = pd.to_numeric(loads["travel_duration_min"], errors="coerce").fillna(0.0)
    loads["travel_distance_miles"] = loads["travel_distance_km"] * KM_TO_MILES
    loads.insert(0, "promise_date", int(date))
    loads["distance_source"] = "solver_route_summary_home_to_jobs_no_return"
    return loads.to_dict("records")


def _add_area_and_diagnostic_detail(
    date: int,
    result: dict[str, Any],
    payload: dict[str, Any],
    probe_results: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    jobs = _payload_job_lookup(payload)
    response_assigned = _assigned_receipts(result)
    replay_assigned = probe_results["baseline"]["assigned_receipts"]
    replay_matches = response_assigned == replay_assigned
    travel_enabled = _has_explicit_travel_constraint(dict(payload.get("options") or {}))
    details: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    for result_type, response_key in (("assigned", "assignments"), ("unassigned", "unassigned")):
        for row in result.get(response_key, []):
            receipt = _receipt(row)
            job = jobs.get(receipt, {})
            job_slots = max(1, number(row.get("job_slot_count", job.get("job_slot_count", 1)), 1))
            detail = {
                "promise_date": int(date),
                "result_type": result_type,
                "job_area": clean(job.get("region_name")) or "OUTSIDE_ACTIVE_PLAN",
                **row,
                "job_slot_count": job_slots,
            }
            details.append(detail)
            if result_type != "unassigned":
                continue

            raw_reason = clean(row.get("reason") or row.get("unassigned_reason")) or "UNKNOWN"
            evidence = _eligible_remaining_slot_evidence(job, payload, result)
            is_no_feasible = raw_reason in {"NO_FEASIBLE_ROUTE", "NO_FEASIBLE_MANDATORY_ROUTE"}
            capacity_assigned = receipt in probe_results["capacity"]["assigned_receipts"] if is_no_feasible else False
            work_assigned = receipt in probe_results["work_time"]["assigned_receipts"] if is_no_feasible else False
            travel_assigned: bool | None = (
                receipt in probe_results["travel_distance"]["assigned_receipts"]
                if is_no_feasible and travel_enabled and "travel_distance" in probe_results
                else None
            )
            classification, classification_evidence = (
                _classify_no_feasible_route(
                    baseline_replay_matches=replay_matches,
                    slot_evidence=evidence,
                    capacity_probe_assigned=capacity_assigned,
                    work_probe_assigned=work_assigned,
                    travel_probe_assigned=travel_assigned,
                )
                if is_no_feasible
                else ("NOT_APPLICABLE_RAW_REASON", "raw_reason_not_no_feasible")
            )
            diagnostics.append(
                {
                    "promise_date": int(date),
                    "receipt_no": receipt,
                    "job_area": detail["job_area"],
                    "raw_reason": raw_reason,
                    "diagnostic_classification": classification,
                    "classification_evidence": classification_evidence,
                    "baseline_replay_matches_response": replay_matches,
                    "baseline_source": "main_response",
                    "baseline_response_assigned_count": len(response_assigned),
                    "baseline_replay_assigned_count": len(replay_assigned),
                    "capacity_probe_assigned": capacity_assigned,
                    "work_time_probe_assigned": work_assigned,
                    "travel_distance_probe_configured": travel_enabled,
                    "travel_distance_probe_assigned": travel_assigned,
                    "job_slot_count": job_slots,
                    **evidence,
                }
            )
    return details, diagnostics


def _result_type_slot_counts(detail_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "promise_date", "job_area", "result_type", "raw_reason",
        "diagnostic_classification", "job_slot_count", "jobs", "slots",
    ]
    if detail_df.empty:
        return pd.DataFrame(columns=columns)
    detail = detail_df.copy()
    diagnostic_columns = ["receipt_no", "raw_reason", "diagnostic_classification"]
    if "raw_reason" not in detail:
        detail["raw_reason"] = detail.get("reason", "")
    if "diagnostic_classification" not in detail:
        detail["diagnostic_classification"] = "NOT_APPLICABLE"
    detail["raw_reason"] = detail["raw_reason"].fillna("").map(clean)
    detail["diagnostic_classification"] = detail["diagnostic_classification"].fillna("NOT_APPLICABLE")
    grouped = detail.groupby(
        ["promise_date", "job_area", "result_type", "raw_reason", "diagnostic_classification", "job_slot_count"],
        dropna=False,
        as_index=False,
    ).agg(jobs=("receipt_no", "nunique"), slots=("job_slot_count", "sum"))
    return grouped.reindex(columns=columns)


def _area_daily_statistics(
    detail_df: pd.DataFrame,
    tech_rows: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Produce area demand, technician and flow views with explicit miles."""

    if detail_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    detail = detail_df.copy()
    assigned = detail[detail["result_type"].eq("assigned")].copy()
    demand = detail.groupby(["promise_date", "job_area"], as_index=False).agg(
        total_jobs=("receipt_no", "nunique"),
        requested_slots=("job_slot_count", "sum"),
    )
    if not assigned.empty:
        assigned_area = assigned.groupby(["promise_date", "job_area"], as_index=False).agg(
            dispatch_jobs=("receipt_no", "nunique"), dispatch_slots=("job_slot_count", "sum"),
        )
        demand = demand.merge(assigned_area, on=["promise_date", "job_area"], how="left")
    demand[["dispatch_jobs", "dispatch_slots"]] = demand[["dispatch_jobs", "dispatch_slots"]].fillna(0).astype(int)
    demand["not_dispatch_jobs"] = demand["total_jobs"] - demand["dispatch_jobs"]
    demand["job_fill_rate_pct"] = demand["dispatch_jobs"] / demand["total_jobs"].replace(0, pd.NA) * 100.0
    demand["requested_slot_completion_pct"] = demand["dispatch_slots"] / demand["requested_slots"].replace(0, pd.NA) * 100.0

    tech = pd.DataFrame(tech_rows)
    if tech.empty:
        return demand, pd.DataFrame(), pd.DataFrame()
    tech_daily = tech.groupby(["promise_date", "technician_area"], as_index=False).agg(
        dispatched_technician_count=("employee_code", "nunique"),
        dispatch_jobs=("dispatch_jobs", "sum"), used_slots=("used_slots", "sum"),
        total_travel_distance_km=("travel_distance_km", "sum"),
        total_travel_duration_min=("travel_duration_min", "sum"),
        total_travel_distance_miles=("travel_distance_miles", "sum"),
    )
    tech_daily["avg_travel_distance_miles_per_dispatched_tech"] = (
        tech_daily["total_travel_distance_miles"] / tech_daily["dispatched_technician_count"].replace(0, pd.NA)
    )
    flow = assigned.groupby(["promise_date", "job_area", "employee_code"], as_index=False).agg(
        dispatch_jobs=("receipt_no", "nunique"), dispatch_slots=("job_slot_count", "sum"),
    )
    flow["technician_area"] = flow["employee_code"].map(ASSIGNMENTS).map(REGION_NAMES).fillna("UNASSIGNED_TECHNICIAN_AREA")
    flow = flow.groupby(["promise_date", "job_area", "technician_area"], as_index=False).agg(
        dispatch_jobs=("dispatch_jobs", "sum"), dispatch_slots=("dispatch_slots", "sum"),
    )
    return demand, tech_daily, flow


def _normalize_area_daily_technician_statistics(frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "promise_date", "technician_area", "dispatched_technician_count",
        "dispatch_jobs", "used_slots", "total_travel_distance_km",
        "total_travel_duration_min", "total_travel_distance_miles",
        "avg_travel_distance_miles_per_dispatched_tech",
    ]
    if list(frame.columns) != required:
        raise RuntimeError("Area daily technician statistics have an unexpected schema")
    if frame[["promise_date", "technician_area"]].isna().any().any():
        raise RuntimeError("Area daily technician statistics contain blank keys")
    out = frame.copy()
    out["promise_date"] = pd.to_numeric(out["promise_date"], errors="raise").astype(int)
    out["technician_area"] = out["technician_area"].map(clean)
    if out["technician_area"].eq("").any() or out.duplicated(["promise_date", "technician_area"]).any():
        raise RuntimeError("Area daily technician statistics contain duplicate or blank keys")
    for column in required[2:]:
        out[column] = pd.to_numeric(out[column], errors="raise")
    return out.sort_values(["promise_date", "technician_area"]).reset_index(drop=True)


def validate_area_daily_technician_statistics(
    candidate: pd.DataFrame,
    canonical: pd.DataFrame,
) -> None:
    """Ensure area technician rows are keyed, non-duplicated, and route-derived."""

    actual = _normalize_area_daily_technician_statistics(candidate)
    expected = _normalize_area_daily_technician_statistics(canonical)
    if len(actual) != len(expected) or not actual.equals(expected):
        raise RuntimeError("Area daily technician statistics do not match route-derived canonical rows")


def _atomic_csv_write(frame: pd.DataFrame, path: Path) -> None:
    """Write a generated CSV atomically; a locked target remains untouched."""

    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8-sig")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def rebuild_area_daily_technician_statistics_from_persisted_outputs() -> pd.DataFrame:
    """Repair only the area daily technician CSV from immutable persisted outputs.

    No solve, route, or demand source is changed.  Replacement occurs only
    after the route-derived canonical table proves the current CSV invalid;
    os.replace leaves a locked/user-open target intact.
    """

    detail = pd.read_csv(OUTPUT_DIR / "atlanta_6area_routing_results.csv", low_memory=False)
    routes = pd.read_csv(OUTPUT_DIR / "area_technician_route_distance_detail.csv", low_memory=False)
    _, canonical, _ = _area_daily_statistics(detail, routes.to_dict("records"))
    canonical = _normalize_area_daily_technician_statistics(canonical)
    path = OUTPUT_DIR / "area_daily_technician_stats.csv"
    current = pd.read_csv(path, low_memory=False)
    try:
        validate_area_daily_technician_statistics(current, canonical)
    except RuntimeError:
        _atomic_csv_write(canonical, path)
    repaired = pd.read_csv(path, low_memory=False)
    validate_area_daily_technician_statistics(repaired, canonical)

    workbook_path = OUTPUT_DIR / "atlanta_6area_area_statistics.xlsx"
    try:
        workbook_rows = pd.read_excel(workbook_path, sheet_name="daily_technician")
    except Exception as exc:
        raise RuntimeError("Area statistics workbook lacks a readable daily_technician sheet") from exc
    validate_area_daily_technician_statistics(workbook_rows, canonical)
    return canonical


def build_slot_count_comparison(
    jobs: pd.DataFrame,
    baseline_results: pd.DataFrame,
    candidate_detail: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare Existing and Atlanta_6area outcomes by requested job slots.

    Every aligned date+receipt is required exactly once in both scenarios.  A
    separate long result-type table makes the assigned/unassigned basis
    explicit while the compact comparison table remains executive-friendly.
    """

    requested = jobs[["promise_date", "gsfs_receipt_no", "job_slot_count"]].copy()
    requested["promise_date"] = requested["promise_date"].astype(int)
    requested["receipt_no"] = requested["gsfs_receipt_no"].map(clean)
    requested["job_slot_count"] = pd.to_numeric(requested["job_slot_count"], errors="coerce").fillna(1).astype(int).clip(lower=1)
    requested = requested[["promise_date", "receipt_no", "job_slot_count"]]
    if requested.duplicated(["promise_date", "receipt_no"]).any():
        raise RuntimeError("Slot-count comparison input contains duplicate date+receipt keys")

    scenario_outcomes = {
        "Existing": baseline_results.rename(columns={"result_type": "result_type"})[
            ["promise_date", "receipt_no", "result_type"]
        ].copy(),
        "Atlanta_6area": candidate_detail[["promise_date", "receipt_no", "result_type"]].copy(),
    }
    compact_rows: list[dict[str, Any]] = []
    long_rows: list[dict[str, Any]] = []
    expected_keys = set(zip(requested["promise_date"], requested["receipt_no"]))
    for scenario, outcomes in scenario_outcomes.items():
        outcomes["promise_date"] = outcomes["promise_date"].astype(int)
        outcomes["receipt_no"] = outcomes["receipt_no"].map(clean)
        if outcomes.duplicated(["promise_date", "receipt_no"]).any():
            raise RuntimeError(f"{scenario} result has duplicate date+receipt keys")
        actual_keys = set(zip(outcomes["promise_date"], outcomes["receipt_no"]))
        if actual_keys != expected_keys:
            raise RuntimeError(
                f"{scenario} slot-count comparison keys mismatch: "
                f"missing={sorted(expected_keys - actual_keys)[:5]} extra={sorted(actual_keys - expected_keys)[:5]}"
            )
        merged = requested.merge(outcomes, on=["promise_date", "receipt_no"], how="inner", validate="one_to_one")
        merged["result_type"] = merged["result_type"].map(clean).str.lower()
        if not merged["result_type"].isin({"assigned", "unassigned"}).all():
            raise RuntimeError(f"{scenario} has invalid result_type for slot-count comparison")
        for slot_count, group in merged.groupby("job_slot_count", sort=True):
            assigned = group[group["result_type"].eq("assigned")]
            compact_rows.append({
                "scenario": scenario,
                "job_slot_count": int(slot_count),
                "total_jobs": int(len(group)),
                "assigned_jobs": int(len(assigned)),
                "unassigned_jobs": int(len(group) - len(assigned)),
                "requested_slots": int(group["job_slot_count"].sum()),
                "assigned_slots": int(assigned["job_slot_count"].sum()),
                "job_fill_rate_pct": float(len(assigned) / len(group) * 100.0) if len(group) else 0.0,
            })
        for (slot_count, result_type), group in merged.groupby(["job_slot_count", "result_type"], sort=True):
            long_rows.append({
                "scenario": scenario,
                "job_slot_count": int(slot_count),
                "result_type": result_type,
                "jobs": int(len(group)),
                "slots": int(group["job_slot_count"].sum()),
            })
    return pd.DataFrame(compact_rows), pd.DataFrame(long_rows)


def _add_weekday_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    dates = pd.to_datetime(out["promise_date"].astype(str), format="%Y%m%d", errors="raise")
    out["weekday_number"] = dates.dt.weekday + 1
    out["weekday_name"] = dates.dt.day_name()
    return out


def build_weekday_reporting(
    jobs: pd.DataFrame,
    baseline_results: pd.DataFrame,
    candidate_detail: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    diagnostics: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    requested = jobs[["promise_date", "gsfs_receipt_no", "job_slot_count"]].copy()
    requested["promise_date"] = requested["promise_date"].astype(int)
    requested["receipt_no"] = requested["gsfs_receipt_no"].map(clean)
    requested["job_slot_count"] = pd.to_numeric(requested["job_slot_count"], errors="coerce").fillna(1).astype(int).clip(lower=1)
    requested = requested[["promise_date", "receipt_no", "job_slot_count"]]
    if len(requested) != 1506 or requested.duplicated(["promise_date", "receipt_no"]).any():
        raise RuntimeError("Weekday source must contain 1,506 unique aligned date+receipt jobs")

    outcomes = {
        "Existing": baseline_results[["promise_date", "receipt_no", "result_type"]].copy(),
        "Atlanta_6area": candidate_detail[["promise_date", "receipt_no", "result_type"]].copy(),
    }
    metric_frame = daily_metrics.copy()
    metric_frame["promise_date"] = metric_frame["promise_date"].astype(int)
    rows: list[dict[str, Any]] = []
    expected_keys = set(zip(requested["promise_date"], requested["receipt_no"]))
    for scenario, result in outcomes.items():
        result["promise_date"] = result["promise_date"].astype(int)
        result["receipt_no"] = result["receipt_no"].map(clean)
        if result.duplicated(["promise_date", "receipt_no"]).any():
            raise RuntimeError(f"{scenario} weekday results contain duplicate keys")
        actual_keys = set(zip(result["promise_date"], result["receipt_no"]))
        if actual_keys != expected_keys:
            raise RuntimeError(f"{scenario} weekday result keys do not match 1,506 aligned source jobs")
        merged = _add_weekday_columns(
            requested.merge(result, on=["promise_date", "receipt_no"], how="inner", validate="one_to_one")
        )
        scenario_metrics = _add_weekday_columns(metric_frame[metric_frame["scenario"].eq(scenario)])
        for (weekday_number, weekday_name), group in merged.groupby(
            ["weekday_number", "weekday_name"], sort=True
        ):
            assigned = group[group["result_type"].map(clean).str.lower().eq("assigned")]
            day_metrics = scenario_metrics[scenario_metrics["weekday_number"].eq(weekday_number)]
            observed_days = int(group["promise_date"].nunique())
            active_tech_days = pd.to_numeric(
                day_metrics.get("active_technicians", pd.Series(dtype=float)), errors="coerce"
            ).fillna(0).sum()
            travel_miles = pd.to_numeric(
                day_metrics.get("travel_distance_miles", pd.Series(dtype=float)), errors="coerce"
            ).fillna(0).sum()
            rows.append({
                "scenario": scenario,
                "weekday_number": int(weekday_number),
                "weekday_name": str(weekday_name),
                "observed_days": observed_days,
                "total_jobs": int(len(group)),
                "assigned_jobs": int(len(assigned)),
                "unassigned_jobs": int(len(group) - len(assigned)),
                "requested_slots": int(group["job_slot_count"].sum()),
                "assigned_slots": int(assigned["job_slot_count"].sum()),
                "job_fill_rate_pct": float(len(assigned) / len(group) * 100.0) if len(group) else 0.0,
                "slot_fill_rate_pct": float(
                    assigned["job_slot_count"].sum() / group["job_slot_count"].sum() * 100.0
                ) if group["job_slot_count"].sum() else 0.0,
                "active_technician_days": float(active_tech_days),
                "avg_jobs_per_active_tech_day": float(len(assigned) / active_tech_days) if active_tech_days else pd.NA,
                "avg_slots_per_active_tech_day": float(assigned["job_slot_count"].sum() / active_tech_days) if active_tech_days else pd.NA,
                "total_travel_miles": float(travel_miles),
                "avg_travel_miles_per_observed_day": float(travel_miles / observed_days) if observed_days else 0.0,
            })
    comparison = pd.DataFrame(rows).sort_values(["weekday_number", "scenario"]).reset_index(drop=True)
    if comparison.groupby("scenario")["total_jobs"].sum().to_dict() != {
        "Atlanta_6area": 1506, "Existing": 1506
    }:
        raise RuntimeError("Weekday comparison does not reconcile to 1,506 jobs per scenario")

    diagnostic = _add_weekday_columns(diagnostics.copy())
    diagnostic["job_slot_count"] = pd.to_numeric(diagnostic["job_slot_count"], errors="coerce").fillna(1).astype(int)
    diagnostic_summary = diagnostic.groupby(
        ["weekday_number", "weekday_name", "raw_reason", "diagnostic_classification", "job_slot_count"],
        as_index=False,
        dropna=False,
    ).agg(jobs=("receipt_no", "size"), slots=("job_slot_count", "sum"))
    if int(diagnostic_summary["jobs"].sum()) != int(len(diagnostics)):
        raise RuntimeError("Weekday unassigned diagnostics do not reconcile to source diagnostics")
    return comparison, diagnostic_summary


def build_fixed_job_policy_accounting(
    jobs: pd.DataFrame,
    candidate_detail: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Account every aligned fixed job under the authorized release policy."""

    source = jobs.copy()
    source["promise_date"] = source["promise_date"].astype(int)
    source["receipt_no"] = source["gsfs_receipt_no"].map(clean)
    source["source_fixed"] = source["fixed"].map(truthy)
    source = source[source["source_fixed"]].copy()
    source["current_employee_code"] = source["svc_engineer_code"].map(clean)
    source = source[["promise_date", "receipt_no", "current_employee_code", "job_slot_count"]]
    if source.duplicated(["promise_date", "receipt_no"]).any():
        raise RuntimeError("Fixed-job source contains duplicate date+receipt keys")

    result = candidate_detail.copy()
    result["promise_date"] = result["promise_date"].astype(int)
    result["receipt_no"] = result["receipt_no"].map(clean)
    result = result[[
        "promise_date", "receipt_no", "result_type", "employee_code", "raw_reason",
        "fixed_technician_outside_active_plan_relaxed",
    ]].copy()
    if result.duplicated(["promise_date", "receipt_no"]).any():
        raise RuntimeError("Candidate detail contains duplicate date+receipt keys")
    accounting = source.merge(result, on=["promise_date", "receipt_no"], how="left", validate="one_to_one")
    if accounting["result_type"].isna().any():
        missing = accounting.loc[accounting["result_type"].isna(), "receipt_no"].head(5).tolist()
        raise RuntimeError(f"Fixed-job accounting is missing result rows: {missing}")
    accounting["assigned_employee_code"] = accounting["employee_code"].map(clean)
    accounting["raw_reason"] = accounting["raw_reason"].fillna("").map(clean)
    accounting["release_flag"] = accounting["fixed_technician_outside_active_plan_relaxed"].map(truthy)

    outcomes: list[str] = []
    authorized: list[bool] = []
    for row in accounting.itertuples(index=False):
        if row.result_type == "assigned" and row.assigned_employee_code == row.current_employee_code:
            outcomes.append("PRESERVED_ORIGINAL_TECHNICIAN")
            authorized.append(True)
        elif row.result_type == "assigned" and row.release_flag:
            outcomes.append("AUTHORIZED_OUTSIDE_PLAN_REASSIGNMENT")
            authorized.append(True)
        elif row.result_type == "assigned":
            outcomes.append("UNAUTHORIZED_NON_FLAGGED_REASSIGNMENT")
            authorized.append(False)
        else:
            outcomes.append(f"UNASSIGNED_{row.raw_reason or 'UNKNOWN'}")
            authorized.append(True)
    accounting["policy_outcome"] = outcomes
    accounting["authorized"] = authorized

    reassigned_mask = (
        accounting["result_type"].eq("assigned")
        & accounting["assigned_employee_code"].ne(accounting["current_employee_code"])
    )
    unassigned = accounting[accounting["result_type"].eq("unassigned")]
    reason_counts = unassigned["raw_reason"].value_counts().to_dict()
    summary = {
        "aligned_fixed_jobs": int(len(accounting)),
        "preserved_original_technician": int(
            accounting["policy_outcome"].eq("PRESERVED_ORIGINAL_TECHNICIAN").sum()
        ),
        "authorized_outside_plan_reassignments": int(
            accounting["policy_outcome"].eq("AUTHORIZED_OUTSIDE_PLAN_REASSIGNMENT").sum()
        ),
        "unassigned_fixed_jobs": int(len(unassigned)),
        "unassigned_fixed_technician_not_available": int(reason_counts.get("FIXED_TECHNICIAN_NOT_AVAILABLE", 0)),
        "unassigned_postal_not_in_active_plan": int(reason_counts.get("POSTAL_NOT_IN_ACTIVE_PLAN", 0)),
        "unassigned_no_eligible_technician": int(reason_counts.get("NO_ELIGIBLE_TECHNICIAN", 0)),
        "released_outside_plan_unassigned": int(unassigned["release_flag"].sum()),
        "unauthorized_non_flagged_reassignments": int((reassigned_mask & ~accounting["release_flag"]).sum()),
    }
    if summary["unauthorized_non_flagged_reassignments"] != 0:
        sample = accounting.loc[
            reassigned_mask & ~accounting["release_flag"],
            ["promise_date", "receipt_no", "current_employee_code", "assigned_employee_code"],
        ].head(10).to_dict("records")
        raise RuntimeError(f"Unauthorized fixed-job reassignment detected: {sample}")

    job_rows = accounting[[
        "promise_date", "receipt_no", "current_employee_code", "result_type",
        "assigned_employee_code", "raw_reason", "release_flag", "policy_outcome", "authorized",
    ]].copy()
    job_rows.insert(0, "row_type", "job")
    job_rows["job_count"] = 1
    summary_rows = pd.DataFrame([
        {
            "row_type": "summary", "promise_date": pd.NA, "receipt_no": "",
            "current_employee_code": "", "result_type": "", "assigned_employee_code": "",
            "raw_reason": "", "release_flag": pd.NA, "policy_outcome": key,
            "authorized": key != "unauthorized_non_flagged_reassignments" or value == 0,
            "job_count": value,
        }
        for key, value in summary.items()
    ])
    return pd.concat([job_rows, summary_rows], ignore_index=True), summary


def assert_atlanta6_fixed_job_contract(summary: dict[str, Any]) -> None:
    expected = {
        "aligned_fixed_jobs": 348,
        "preserved_original_technician": 176,
        "authorized_outside_plan_reassignments": 144,
        "unassigned_fixed_jobs": 28,
        "unassigned_fixed_technician_not_available": 20,
        "unassigned_postal_not_in_active_plan": 6,
        "unassigned_no_eligible_technician": 2,
        "released_outside_plan_unassigned": 8,
        "unauthorized_non_flagged_reassignments": 0,
    }
    mismatches = {
        key: {"expected": expected_value, "actual": summary.get(key)}
        for key, expected_value in expected.items()
        if summary.get(key) != expected_value
    }
    if mismatches:
        raise RuntimeError(f"Fixed-job policy accounting contract failed: {mismatches}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_top_level_directories(root: Path) -> dict[str, Any]:
    classified = []
    unexpected = []
    for path in sorted((item for item in root.iterdir() if item.is_dir()), key=lambda item: item.name):
        classification = TOP_LEVEL_DIRECTORY_CLASSIFICATION.get(path.name)
        if classification is None:
            classification = "unexpected_top_level_directory"
            unexpected.append(path.name)
        classified.append({"path": path.name, "classification": classification})
    return {
        "directories": classified,
        "unexpected_top_level_directories": unexpected,
        "status": "unexpected_directories_present" if unexpected else "clean",
    }


def write_artifact_integrity_manifest() -> dict[str, Any]:
    missing = [name for name in CURRENT_ARTIFACT_ALLOWLIST if not (OUTPUT_DIR / name).is_file()]
    if missing:
        raise RuntimeError(f"Missing allowlisted comparison artifacts: {missing}")
    entries = [
        {
            "path": name,
            "size_bytes": int((OUTPUT_DIR / name).stat().st_size),
            "sha256": _sha256(OUTPUT_DIR / name),
        }
        for name in CURRENT_ARTIFACT_ALLOWLIST
    ]
    ignored = sorted(path.name for path in OUTPUT_DIR.glob("~$*") if path.is_file())
    all_files = {path.name for path in OUTPUT_DIR.iterdir() if path.is_file()}
    non_allowlisted = sorted(
        all_files - set(CURRENT_ARTIFACT_ALLOWLIST) - set(ignored) - {ARTIFACT_INTEGRITY_FILE}
    )
    directory_classification = classify_top_level_directories(OUTPUT_DIR)
    payload = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "algorithm": "sha256",
        "allowlisted_artifact_count": len(entries),
        "allowlisted_artifacts": entries,
        "ignored_non_artifact_patterns": [
            {
                "pattern": "~$*",
                "reason": "Microsoft Excel owner/lock files are transient non-artifacts",
                "warning": "A detected lock file may mean an Excel workbook is currently open; it was not deleted or checksummed.",
            }
        ],
        "detected_ignored_non_artifacts": ignored,
        "non_allowlisted_files": non_allowlisted,
        "top_level_directory_classification": directory_classification,
        "integrity_manifest_self_excluded": ARTIFACT_INTEGRITY_FILE,
    }
    (OUTPUT_DIR / ARTIFACT_INTEGRITY_FILE).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return payload


def _validate_result_accounting(payload: dict[str, Any], result: dict[str, Any]) -> None:
    requested = set(_payload_job_lookup(payload))
    assigned = _assigned_receipts(result)
    unassigned = {_receipt(row) for row in result.get("unassigned", []) if _receipt(row)}
    duplicate = assigned & unassigned
    missing = requested - assigned - unassigned
    extra = (assigned | unassigned) - requested
    if duplicate or missing or extra:
        raise RuntimeError(
            "Routing accounting failed: "
            f"duplicate={sorted(duplicate)[:5]} missing={sorted(missing)[:5]} extra={sorted(extra)[:5]}"
        )


def _checkpoint_path(date: int) -> Path:
    return CHECKPOINT_DIR / f"{int(date)}.json"


def _write_day_checkpoint(date: int, payload: dict[str, Any]) -> None:
    """Atomically persist enough raw response/probe evidence to resume safely."""

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    target = _checkpoint_path(date)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(target)


def _load_day_checkpoint(date: int) -> dict[str, Any] | None:
    path = _checkpoint_path(date)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        payload.get("checkpoint_version") != CHECKPOINT_VERSION
        or int(payload.get("promise_date", 0) or 0) != int(date)
        or payload.get("plan_checksum") != PLAN_CHECKSUM
    ):
        return None
    required = {"metrics", "detail", "diagnostics", "route_detail", "status", "probe_status"}
    return payload if required.issubset(payload) else None


def run() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    jobs = pd.read_csv(JOBS_FILE, low_memory=False)
    tech = pd.read_csv(TECH_FILE, low_memory=False)
    baseline = pd.read_csv(BASELINE_STATS_FILE, low_memory=False)
    baseline_results = pd.read_csv(BASELINE_RESULTS_FILE, low_memory=False)
    baseline_keys = set(zip(
        baseline_results["promise_date"].astype(int),
        baseline_results["receipt_no"].astype(str),
    ))
    aligned_mask = [
        (int(date), str(receipt)) in baseline_keys
        for date, receipt in zip(jobs["promise_date"], jobs["gsfs_receipt_no"])
    ]
    excluded = jobs.loc[[not value for value in aligned_mask]].copy()
    jobs = jobs.loc[aligned_mask].copy()
    excluded.to_csv(OUTPUT_DIR / "jobs_excluded_from_aligned_comparison.csv", index=False, encoding="utf-8-sig")
    homes = pd.read_csv(HOME_FILE, low_memory=False)
    region_snapshot = load_snapshot()
    capabilities = load_capabilities(set(ASSIGNMENTS))
    dates = sorted(set(jobs["promise_date"].astype(int)) & set(tech["promise_date"].astype(int)) & set(baseline["promise_date"].astype(int)))
    requested_dates = {
        int(value.strip())
        for value in os.environ.get("ATLANTA6_COMPARE_DATES", "").split(",")
        if value.strip()
    }
    if requested_dates:
        dates = [date for date in dates if date in requested_dates]
        if not dates:
            raise RuntimeError("ATLANTA6_COMPARE_DATES does not select an available comparison date")
    daily_input_dir = OUTPUT_DIR / "daily_inputs"
    daily_input_dir.mkdir(parents=True, exist_ok=True)
    for date in dates:
        jobs_out = jobs[jobs["promise_date"].astype(int).eq(date)].copy()
        jobs_out["strategic_city_name"] = "Atlanta_6area"
        tech_out = tech[
            tech["promise_date"].astype(int).eq(date)
            & tech["employee_code"].astype(str).isin(ASSIGNMENTS)
        ].copy()
        tech_out["strategic_city_name"] = "Atlanta_6area"
        tech_out["assigned_region_seq"] = tech_out["employee_code"].map(ASSIGNMENTS)
        tech_out["assigned_region_name"] = tech_out["assigned_region_seq"].map(REGION_NAMES)
        jobs_out.to_csv(daily_input_dir / f"jobs_{date}_atlanta6.csv", index=False, encoding="utf-8-sig")
        tech_out.to_csv(daily_input_dir / f"technicians_{date}_atlanta6.csv", index=False, encoding="utf-8-sig")
    metrics_rows: list[dict[str, Any]] = []
    result_rows: list[dict[str, Any]] = []
    diagnostic_rows: list[dict[str, Any]] = []
    route_rows: list[dict[str, Any]] = []
    reason_rows: list[dict[str, Any]] = []
    status_rows: list[dict[str, Any]] = []
    probe_status_rows: list[dict[str, Any]] = []
    solver_diagnostics_by_day: dict[str, Any] = {}
    for date in dates:
        checkpoint = _load_day_checkpoint(date)
        if checkpoint is not None:
            metrics_rows.append(checkpoint["metrics"])
            result_rows.extend(checkpoint["detail"])
            diagnostic_rows.extend(checkpoint["diagnostics"])
            route_rows.extend(checkpoint["route_detail"])
            reason_rows.extend(checkpoint.get("reasons", []))
            status_rows.append(checkpoint["status"])
            probe_status_rows.extend(checkpoint["probe_status"])
            solver_diagnostics_by_day[str(date)] = checkpoint.get("solver_diagnostics", {})
            print(f"{date}: resumed checkpoint", flush=True)
            continue
        day_jobs = jobs[jobs["promise_date"].astype(int).eq(date)].copy()
        day_tech = tech[tech["promise_date"].astype(int).eq(date)].copy()
        technicians = technician_payload(day_tech, homes)
        payload = {
            "request_id": f"atlanta6_compare_{date}",
            "mode": "na_general",
            "city": "Atlanta_6area",
            "planning_date": f"{str(date)[:4]}-{str(date)[4:6]}-{str(date)[6:8]}",
            "options": {
                "respect_fixed_jobs": True,
                "objective": "min_total_travel_time",
                "time_limit_seconds": 10,
                "osrm_url": "http://20.51.244.68:5002",
                "distance_backend": "city_osrm_else_haversine",
            },
            "technicians": technicians,
            "jobs": _apply_job_capabilities(job_payload(day_jobs), capabilities, capability_policy_present=True),
            "capabilities": capabilities,
        }
        payload = _apply_active_region_plan(payload, region_snapshot)
        started = time.perf_counter()
        try:
            result = run_routing_request(payload)
            elapsed = time.perf_counter() - started
            _validate_result_accounting(payload, result)
            # The main routing response is the authoritative baseline.  Do
            # not spend another full solver budget replaying it merely for a
            # diagnostic.  Probes are limited to relevant no-feasible calls.
            probe_results = {
                "baseline": {
                    "status": "main_response",
                    "assigned_receipts": _assigned_receipts(result),
                    "summary": pd.DataFrame(result.get("engineer_summary", [])),
                    "runtime_seconds": elapsed,
                },
            }
            probe_receipts = _no_feasible_receipts_with_eligible_candidates(result, payload)
            if probe_receipts:
                probe_results["capacity"] = _run_constraint_probe(payload, "capacity")
                probe_results["work_time"] = _run_constraint_probe(payload, "work_time")
            else:
                for probe_name in ("capacity", "work_time"):
                    probe_results[probe_name] = {
                        "status": "not_needed",
                        "assigned_receipts": set(),
                        "summary": pd.DataFrame(),
                    }
            if _has_explicit_travel_constraint(dict(payload.get("options") or {})) and probe_receipts:
                probe_results["travel_distance"] = _run_constraint_probe(payload, "travel_distance")
            else:
                probe_results["travel_distance"] = {
                    "status": "not_configured" if not _has_explicit_travel_constraint(dict(payload.get("options") or {})) else "not_needed",
                    "assigned_receipts": set(),
                    "summary": pd.DataFrame(),
                }
            metrics, detail, reasons = build_daily_metrics(date, day_jobs, technicians, result, elapsed)
            day_detail, day_diagnostics = _add_area_and_diagnostic_detail(
                date, result, payload, probe_results
            )
            # `day_detail` is constructed from the response and includes the
            # same raw result records as `detail`, plus the active plan area.
            # Use it as the canonical output row set.
            detail = day_detail
            route_detail = _route_detail_rows(date, result, probe_results["baseline"]["summary"])
            route_rows.extend(route_detail)
            metrics["travel_distance_km"] = sum(float(row["travel_distance_km"]) for row in route_detail)
            metrics["travel_distance_miles"] = metrics["travel_distance_km"] * KM_TO_MILES
            metrics["avg_travel_miles_per_active_tech"] = (
                metrics["travel_distance_miles"] / metrics["active_technicians"]
                if metrics["active_technicians"] else 0.0
            )
            metrics_rows.append(metrics)
            result_rows.extend(detail)
            diagnostic_rows.extend(day_diagnostics)
            reason_rows.extend(reasons)
            solver_diagnostics_by_day[str(date)] = result.get("diagnostics", {})
            status_rows.append({
                "promise_date": date, "status": "completed", "runtime_seconds": elapsed,
                "baseline_replay_matches_response": (
                    _assigned_receipts(result) == probe_results["baseline"]["assigned_receipts"]
                ),
                "baseline_source": "main_response",
                "probe_candidate_no_feasible_jobs": len(probe_receipts),
                "error": "",
            })
            for probe_name, probe_result in probe_results.items():
                probe_status_rows.append({
                    "promise_date": date,
                    "probe": probe_name,
                    "status": probe_result.get("status", "unknown"),
                    "runtime_seconds": probe_result.get("runtime_seconds", pd.NA),
                    "assigned_jobs": len(probe_result.get("assigned_receipts", set())),
                    "note": (
                        "travel caps not configured; no travel-distance probe was needed"
                        if probe_name == "travel_distance" and not _has_explicit_travel_constraint(dict(payload.get("options") or {}))
                        else ""
                    ),
                })
            _write_day_checkpoint(date, {
                "checkpoint_version": CHECKPOINT_VERSION,
                "promise_date": int(date),
                "plan_checksum": PLAN_CHECKSUM,
                # Raw solver response remains available for audit/rebuild;
                # probe assignment sets and statuses record the exact limited
                # counterfactual evidence without serialising DataFrames.
                "result": result,
                "probe_results": {
                    name: {
                        "status": value.get("status"),
                        "runtime_seconds": value.get("runtime_seconds"),
                        "assigned_receipts": sorted(value.get("assigned_receipts", set())),
                    }
                    for name, value in probe_results.items()
                },
                "metrics": metrics,
                "detail": detail,
                "diagnostics": day_diagnostics,
                "route_detail": route_detail,
                "reasons": reasons,
                "status": status_rows[-1],
                "probe_status": probe_status_rows[-len(probe_results):],
                "solver_diagnostics": result.get("diagnostics", {}),
            })
            print(f"{date}: assigned={metrics['dispatch_jobs']} unassigned={metrics['not_dispatch_jobs']} slots={metrics['dispatch_slots']} runtime={elapsed:.1f}s", flush=True)
        except Exception as exc:
            elapsed = time.perf_counter() - started
            status_rows.append({
                "promise_date": date, "status": "failed", "runtime_seconds": elapsed,
                "baseline_replay_matches_response": pd.NA,
                "error": f"{type(exc).__name__}: {exc}",
            })
            print(f"{date}: FAILED {type(exc).__name__}: {exc}", flush=True)

    new = pd.DataFrame(metrics_rows).sort_values("promise_date")
    completed_dates = set(new["promise_date"].astype(int)) if not new.empty else set()
    completed_date_list = sorted(completed_dates)
    base, existing_capacity_roster = baseline_metrics(
        baseline[baseline["promise_date"].astype(int).isin(completed_dates)],
        tech,
        baseline_results,
    )
    atlanta_capacity_roster, atlanta_capacity_summary = technician_input_capacity_roster(
        tech,
        scenario="Atlanta_6area",
        dates=completed_date_list,
        plan_only=True,
    )
    new = _apply_input_capacity_metrics(
        new, atlanta_capacity_summary, scenario="Atlanta_6area"
    )
    base = base.sort_values("promise_date")
    capacity_roster = pd.concat(
        [existing_capacity_roster, atlanta_capacity_roster], ignore_index=True
    ).sort_values(["scenario", "promise_date", "employee_code"])
    combined = pd.concat([base, new], ignore_index=True).sort_values(["promise_date", "scenario"])
    compare = base.merge(new, on="promise_date", suffixes=("_existing", "_atlanta6"))
    for metric in [
        "dispatch_jobs", "dispatch_slots", "not_dispatch_jobs", "avg_jobs", "avg_slots",
        "job_fill_rate_pct", "fill_rate_pct", "travel_distance_km", "travel_distance_miles",
        "avg_travel_miles_per_active_tech",
    ]:
        compare[f"{metric}_delta"] = compare[f"{metric}_atlanta6"] - compare[f"{metric}_existing"]
    overall = combined.groupby("scenario", as_index=False).agg(
        days=("promise_date", "nunique"), total_jobs=("total_jobs", "sum"),
        dispatch_jobs=("dispatch_jobs", "sum"), dispatch_slots=("dispatch_slots", "sum"),
        not_dispatch_jobs=("not_dispatch_jobs", "sum"), avg_jobs=("avg_jobs", "mean"),
        avg_slots=("avg_slots", "mean"), avg_job_fill_rate_pct=("job_fill_rate_pct", "mean"),
        avg_fill_rate_pct=("fill_rate_pct", "mean"), travel_distance_km=("travel_distance_km", "sum"),
        travel_distance_miles=("travel_distance_miles", "sum"),
        avg_travel_miles_per_active_tech=("avg_travel_miles_per_active_tech", "mean"),
    )
    detail_df = pd.DataFrame(result_rows)
    diagnostics_df = pd.DataFrame(diagnostic_rows)
    expected_detail_keys = {
        (int(row.promise_date), clean(row.gsfs_receipt_no))
        for row in jobs[jobs["promise_date"].astype(int).isin(completed_dates)].itertuples(index=False)
        if clean(row.gsfs_receipt_no)
    }
    actual_detail_keys = (
        {
            (int(row.promise_date), clean(row.receipt_no))
            for row in detail_df.itertuples(index=False)
            if clean(row.receipt_no)
        }
        if not detail_df.empty and {"promise_date", "receipt_no"}.issubset(detail_df.columns)
        else set()
    )
    if len(detail_df) != len(expected_detail_keys) or actual_detail_keys != expected_detail_keys:
        missing = sorted(expected_detail_keys - actual_detail_keys)[:5]
        extra = sorted(actual_detail_keys - expected_detail_keys)[:5]
        raise RuntimeError(
            "Output detail accounting failed before overwrite: "
            f"rows={len(detail_df)} expected={len(expected_detail_keys)} "
            f"missing={missing} extra={extra}"
        )
    if not detail_df.empty:
        detail_df["raw_reason"] = detail_df.get("reason", "").fillna("").map(clean)
        if not diagnostics_df.empty:
            detail_df = detail_df.merge(
                diagnostics_df[["promise_date", "receipt_no", "diagnostic_classification", "classification_evidence"]],
                on=["promise_date", "receipt_no"], how="left",
            )
        detail_df["diagnostic_classification"] = detail_df.get(
            "diagnostic_classification", "NOT_APPLICABLE"
        ).fillna("NOT_APPLICABLE")
        detail_df["classification_evidence"] = detail_df.get(
            "classification_evidence", ""
        ).fillna("")
    slot_counts = _result_type_slot_counts(detail_df)
    slot_count_comparison, slot_count_result_type_comparison = build_slot_count_comparison(
        jobs, baseline_results, detail_df
    )
    area_demand, area_tech, assignment_flow = _area_daily_statistics(detail_df, route_rows)
    area_unassigned = (
        diagnostics_df.groupby(
            ["job_area", "raw_reason", "diagnostic_classification", "job_slot_count"],
            as_index=False,
        ).agg(jobs=("receipt_no", "nunique"), slots=("job_slot_count", "sum"))
        if not diagnostics_df.empty
        else pd.DataFrame()
    )
    if not area_demand.empty:
        area_overall_demand = area_demand.groupby("job_area", as_index=False).agg(
            total_jobs=("total_jobs", "sum"), requested_slots=("requested_slots", "sum"),
            dispatch_jobs=("dispatch_jobs", "sum"), dispatch_slots=("dispatch_slots", "sum"),
            not_dispatch_jobs=("not_dispatch_jobs", "sum"),
        )
        area_overall_demand["job_fill_rate_pct"] = (
            area_overall_demand["dispatch_jobs"] / area_overall_demand["total_jobs"].replace(0, pd.NA) * 100.0
        )
        area_overall_demand["requested_slot_completion_pct"] = (
            area_overall_demand["dispatch_slots"] / area_overall_demand["requested_slots"].replace(0, pd.NA) * 100.0
        )
    else:
        area_overall_demand = pd.DataFrame()
    if not area_tech.empty:
        area_overall_tech = area_tech.groupby("technician_area", as_index=False).agg(
            dispatched_technician_days=("dispatched_technician_count", "sum"),
            dispatch_jobs=("dispatch_jobs", "sum"), used_slots=("used_slots", "sum"),
            total_travel_distance_km=("total_travel_distance_km", "sum"),
            total_travel_distance_miles=("total_travel_distance_miles", "sum"),
            total_travel_duration_min=("total_travel_duration_min", "sum"),
        )
        area_overall_tech["avg_travel_miles_per_dispatched_tech_day"] = (
            area_overall_tech["total_travel_distance_miles"] /
            area_overall_tech["dispatched_technician_days"].replace(0, pd.NA)
        )
    else:
        area_overall_tech = pd.DataFrame()
    integrated_area = area_overall_demand.merge(
        area_overall_tech, left_on="job_area", right_on="technician_area", how="outer"
    ) if not area_overall_demand.empty or not area_overall_tech.empty else pd.DataFrame()
    executive_rows = []
    if not overall.empty:
        existing = overall[overall["scenario"].eq("Existing")]
        candidate = overall[overall["scenario"].eq("Atlanta_6area")]
        if not existing.empty and not candidate.empty:
            for label, column in (
                ("Total jobs", "total_jobs"), ("Dispatch jobs", "dispatch_jobs"),
                ("Dispatch slots", "dispatch_slots"), ("Not dispatch jobs", "not_dispatch_jobs"),
                ("Daily avg job fill rate (%)", "avg_job_fill_rate_pct"),
                ("Daily avg slot fill rate (%)", "avg_fill_rate_pct"),
                ("Total travel miles", "travel_distance_miles"),
                ("Avg travel miles / active tech-day", "avg_travel_miles_per_active_tech"),
            ):
                base_value = float(existing.iloc[0][column])
                candidate_value = float(candidate.iloc[0][column])
                executive_rows.append({
                    "metric": label, "existing": base_value, "atlanta_6area": candidate_value,
                    "delta_atlanta6_minus_existing": candidate_value - base_value,
                })
    executive = pd.DataFrame(executive_rows)
    executive = build_executive_comparison(overall)
    combined.to_csv(OUTPUT_DIR / "daily_metrics_all_scenarios.csv", index=False, encoding="utf-8-sig")
    new.to_csv(OUTPUT_DIR / "atlanta_6area_daily_metrics.csv", index=False, encoding="utf-8-sig")
    capacity_roster.to_csv(
        OUTPUT_DIR / "technician_input_capacity_roster.csv", index=False, encoding="utf-8-sig"
    )
    compare.to_csv(OUTPUT_DIR / "daily_comparison.csv", index=False, encoding="utf-8-sig")
    overall.to_csv(OUTPUT_DIR / "overall_comparison.csv", index=False, encoding="utf-8-sig")
    detail_df.to_csv(OUTPUT_DIR / "atlanta_6area_routing_results.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(reason_rows).to_csv(OUTPUT_DIR / "atlanta_6area_unassigned_reasons.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(status_rows).to_csv(OUTPUT_DIR / "run_status.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(probe_status_rows).to_csv(OUTPUT_DIR / "constraint_probe_run_status.csv", index=False, encoding="utf-8-sig")
    diagnostics_df.to_csv(OUTPUT_DIR / "atlanta_6area_unassigned_diagnostics.csv", index=False, encoding="utf-8-sig")
    slot_counts.to_csv(OUTPUT_DIR / "atlanta_6area_result_type_slot_counts.csv", index=False, encoding="utf-8-sig")
    slot_count_comparison.to_csv(OUTPUT_DIR / "slot_count_comparison.csv", index=False, encoding="utf-8-sig")
    slot_count_result_type_comparison.to_csv(
        OUTPUT_DIR / "slot_count_result_type_comparison.csv", index=False, encoding="utf-8-sig"
    )
    area_unassigned.to_csv(OUTPUT_DIR / "area_unassigned_reasons.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(route_rows).to_csv(OUTPUT_DIR / "area_technician_route_distance_detail.csv", index=False, encoding="utf-8-sig")
    area_demand.to_csv(OUTPUT_DIR / "area_daily_demand_stats.csv", index=False, encoding="utf-8-sig")
    area_tech.to_csv(OUTPUT_DIR / "area_daily_technician_stats.csv", index=False, encoding="utf-8-sig")
    assignment_flow.to_csv(OUTPUT_DIR / "area_assignment_flow.csv", index=False, encoding="utf-8-sig")
    area_overall_demand.to_csv(OUTPUT_DIR / "area_overall_demand_stats.csv", index=False, encoding="utf-8-sig")
    area_overall_tech.to_csv(OUTPUT_DIR / "area_overall_technician_stats.csv", index=False, encoding="utf-8-sig")
    integrated_area.to_csv(OUTPUT_DIR / "atlanta_6area_integrated_area_statistics.csv", index=False, encoding="utf-8-sig")
    executive.to_csv(OUTPUT_DIR / "executive_comparison.csv", index=False, encoding="utf-8-sig")
    with pd.ExcelWriter(OUTPUT_DIR / "atlanta_6area_integrated_statistics.xlsx") as writer:
        compare.to_excel(writer, sheet_name="daily_comparison", index=False)
        integrated_area.to_excel(writer, sheet_name="area_statistics", index=False)
        executive.to_excel(writer, sheet_name="executive", index=False)
        diagnostics_df.to_excel(writer, sheet_name="unassigned_diagnostics", index=False)
        slot_counts.to_excel(writer, sheet_name="result_type_slots", index=False)
        slot_count_comparison.to_excel(writer, sheet_name="slot_count_comparison", index=False)
        slot_count_result_type_comparison.to_excel(writer, sheet_name="slot_count_result_type", index=False)
    with pd.ExcelWriter(OUTPUT_DIR / "atlanta_6area_area_statistics.xlsx") as writer:
        area_demand.to_excel(writer, sheet_name="daily_demand", index=False)
        area_tech.to_excel(writer, sheet_name="daily_technician", index=False)
        assignment_flow.to_excel(writer, sheet_name="assignment_flow", index=False)
        pd.DataFrame(route_rows).to_excel(writer, sheet_name="route_miles", index=False)
        area_unassigned.to_excel(writer, sheet_name="unassigned_constraints", index=False)
    (OUTPUT_DIR / "solver_diagnostics_by_day.json").write_text(
        json.dumps(solver_diagnostics_by_day, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (OUTPUT_DIR / "run_manifest.json").write_text(json.dumps({
        "plan_id": PLAN_ID, "plan_checksum": PLAN_CHECKSUM,
        "policy": POLICY, "dates": dates, "job_rows": len(jobs),
        "comparison_universe": "date+receipt keys present in atlanta_routing_results_20260601_20260630.csv",
        "excluded_job_rows": len(excluded),
        "technician_input_rows": len(tech), "plan_technicians": sorted(ASSIGNMENTS),
        "technician_capacity_basis": {
            "artifact": "technician_input_capacity_roster.csv",
            "definition": "available=true daily solver-input technicians; includes zero-assignment technicians",
            "existing": "all raw Atlanta daily technician input rows",
            "atlanta_6area": "raw daily technician input filtered to immutable 14-technician plan",
            "active_technicians": "alias retained for compatibility; equals solver_input_available_technicians",
            "available_slots": "sum(slot_count) for solver_input_available_technicians",
            "dispatched_technicians": "distinct technicians with at least one assigned job; not used as a capacity denominator",
        },
        "overflow_postals": sorted(OVERFLOW_POSTALS),
        "fixed_job_policy": {
            "authorization": "A fixed job remains with its original technician unless that technician is outside the active plan; only rows explicitly flagged fixed_technician_outside_active_plan_relaxed may be reassigned within remaining hard eligibility.",
            "accounting_artifact": "fixed_job_policy_accounting.csv",
            "hard_assertion": "zero unauthorized non-flagged fixed-job reassignments",
        },
        "time_limit_seconds_per_day": 10,
        "solver": {
            "engine": "OR-Tools na_general / production_assign_atlanta_vrp",
            "first_solution_strategy": "PARALLEL_CHEAPEST_INSERTION",
            "local_search": "GUIDED_LOCAL_SEARCH",
            "objective": "min_total_travel_time",
        },
        "units": {
            "matrix_distance": "km",
            "matrix_duration": "minutes",
            "service_duration": "minutes (45 per slot unless input override)",
            "reported_route_distance": "km and miles (km * 0.621371)",
            "slot_capacity": "job slots",
        },
        "matrix": {
            "source": "OSRM table/route via city_osrm_else_haversine",
            "shape": "(technicians + jobs) square directed matrix per daily solve",
            "coordinate_order": "longitude,latitude",
            "fallback": "haversine_on_osrm_error",
        },
        "constraint_probe_method": {
            "scope": "NO_FEASIBLE_ROUTE and NO_FEASIBLE_MANDATORY_ROUTE with explicit eligible candidates only; raw reason is preserved for every job",
            "baseline": "authoritative main routing response (no duplicate replay solve)",
            "probe_time_limit_seconds": CONSTRAINT_PROBE_TIME_LIMIT_SECONDS,
            "capacity": "increase max_slots/max_jobs only while retaining each normalized effective max_minutes",
            "work_time": f"increase max_minutes only to solver absolute ceiling ({WORK_PROBE_MAX_MINUTES} min), retaining slots",
            "travel_distance": "remove only explicit daily/leg/home travel caps; not run when no cap is configured",
            "classification": "single observed isolating probe; MULTIPLE_CONSTRAINTS or UNDETERMINED otherwise (including a 3-second probe that does not isolate a job)",
        },
        "comparison_artifacts": {
            "slot_count_comparison.csv": "Existing and Atlanta_6area requested/assigned/unassigned jobs and slots by job_slot_count",
            "slot_count_result_type_comparison.csv": "Long assigned/unassigned result-type counts and slots by scenario and job_slot_count",
        },
        "formulas": {
            "avg_jobs": "dispatch_jobs / solver_input_available_technicians",
            "avg_slots": "dispatch_slots / solver_input_available_technicians",
            "job_fill_rate_pct": "dispatch_jobs / total_jobs * 100",
            "fill_rate_pct": "dispatch_slots / solver_input_available_slots * 100",
            "travel_distance_miles": "travel_distance_km * 0.621371",
        },
        "completed_dates": sorted(completed_dates),
        "failed_dates": [row["promise_date"] for row in status_rows if row["status"] == "failed"],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    regenerate_qa_integrity_artifacts()


def regenerate_slot_count_comparison_artifacts() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Refresh only comparison reporting from persisted inputs/results.

    This is intentionally read/report-only: it makes no routing, API, DB or
    server call and is suitable after a completed run when a reporting field
    is added.
    """

    jobs = pd.read_csv(JOBS_FILE, low_memory=False)
    baseline_results = pd.read_csv(BASELINE_RESULTS_FILE, low_memory=False)
    baseline_keys = set(zip(
        baseline_results["promise_date"].astype(int),
        baseline_results["receipt_no"].astype(str),
    ))
    aligned = [
        (int(date), str(receipt)) in baseline_keys
        for date, receipt in zip(jobs["promise_date"], jobs["gsfs_receipt_no"])
    ]
    jobs = jobs.loc[aligned].copy()
    detail = pd.read_csv(OUTPUT_DIR / "atlanta_6area_routing_results.csv", low_memory=False)
    summary, long = build_slot_count_comparison(jobs, baseline_results, detail)
    summary.to_csv(OUTPUT_DIR / "slot_count_comparison.csv", index=False, encoding="utf-8-sig")
    long.to_csv(OUTPUT_DIR / "slot_count_result_type_comparison.csv", index=False, encoding="utf-8-sig")
    with pd.ExcelWriter(
        OUTPUT_DIR / "atlanta_6area_integrated_statistics.xlsx",
        mode="a",
        if_sheet_exists="replace",
        engine="openpyxl",
    ) as writer:
        summary.to_excel(writer, sheet_name="slot_count_comparison", index=False)
        long.to_excel(writer, sheet_name="slot_count_result_type", index=False)
    regenerate_qa_integrity_artifacts()
    return summary, long


def regenerate_qa_integrity_artifacts() -> dict[str, Any]:
    """Regenerate QA reporting from current CSV/checkpoint-derived outputs only."""

    jobs = pd.read_csv(JOBS_FILE, low_memory=False)
    baseline_results = pd.read_csv(BASELINE_RESULTS_FILE, low_memory=False)
    baseline_keys = set(zip(
        baseline_results["promise_date"].astype(int),
        baseline_results["receipt_no"].astype(str),
    ))
    aligned = [
        (int(date), str(receipt)) in baseline_keys
        for date, receipt in zip(jobs["promise_date"], jobs["gsfs_receipt_no"])
    ]
    jobs = jobs.loc[aligned].copy()
    if len(jobs) != 1506:
        raise RuntimeError(f"Expected 1,506 aligned jobs for QA regeneration, found {len(jobs)}")

    detail = pd.read_csv(OUTPUT_DIR / "atlanta_6area_routing_results.csv", low_memory=False)
    diagnostics = pd.read_csv(OUTPUT_DIR / "atlanta_6area_unassigned_diagnostics.csv", low_memory=False)
    daily_metrics = pd.read_csv(OUTPUT_DIR / "daily_metrics_all_scenarios.csv", low_memory=False)
    if len(detail) != 1506 or detail[["promise_date", "receipt_no"]].drop_duplicates().shape[0] != 1506:
        raise RuntimeError("Current Atlanta_6area detail does not contain 1,506 unique aligned results")

    fixed_accounting, fixed_summary = build_fixed_job_policy_accounting(jobs, detail)
    assert_atlanta6_fixed_job_contract(fixed_summary)
    fixed_accounting.to_csv(
        OUTPUT_DIR / "fixed_job_policy_accounting.csv", index=False, encoding="utf-8-sig"
    )

    weekday_comparison, weekday_diagnostics = build_weekday_reporting(
        jobs, baseline_results, detail, daily_metrics, diagnostics
    )
    weekday_comparison.to_csv(
        OUTPUT_DIR / "weekday_comparison.csv", index=False, encoding="utf-8-sig"
    )
    weekday_diagnostics.to_csv(
        OUTPUT_DIR / "weekday_unassigned_diagnostics.csv", index=False, encoding="utf-8-sig"
    )

    overall = pd.read_csv(OUTPUT_DIR / "overall_comparison.csv")
    build_executive_comparison(overall).to_csv(
        OUTPUT_DIR / "executive_comparison.csv", index=False, encoding="utf-8-sig"
    )
    daily_comparison = pd.read_csv(OUTPUT_DIR / "daily_comparison.csv")
    atlanta_daily = pd.read_csv(OUTPUT_DIR / "atlanta_6area_daily_metrics.csv")
    executive = pd.read_csv(OUTPUT_DIR / "executive_comparison.csv")
    unassigned_reasons = pd.read_csv(OUTPUT_DIR / "atlanta_6area_unassigned_reasons.csv")
    run_status = pd.read_csv(OUTPUT_DIR / "run_status.csv")
    excluded = pd.read_csv(OUTPUT_DIR / "jobs_excluded_from_aligned_comparison.csv")
    slot_comparison = pd.read_csv(OUTPUT_DIR / "slot_count_comparison.csv")
    slot_result_type = pd.read_csv(OUTPUT_DIR / "slot_count_result_type_comparison.csv")
    rebuild_integrated_statistics_workbook()
    with pd.ExcelWriter(OUTPUT_DIR / "atlanta_6area_vs_existing_summary.xlsx") as writer:
        overall.to_excel(writer, sheet_name="Overall", index=False)
        executive.to_excel(writer, sheet_name="Executive", index=False)
        daily_comparison.to_excel(writer, sheet_name="Daily Comparison", index=False)
        atlanta_daily.to_excel(writer, sheet_name="Atlanta6 Daily", index=False)
        slot_comparison.to_excel(writer, sheet_name="Slot Count", index=False)
        slot_result_type.to_excel(writer, sheet_name="Slot Result Type", index=False)
        weekday_comparison.to_excel(writer, sheet_name="Weekday Comparison", index=False)
        weekday_diagnostics.to_excel(writer, sheet_name="Weekday Unassigned", index=False)
        unassigned_reasons.to_excel(writer, sheet_name="Unassigned Reasons", index=False)
        diagnostics.to_excel(writer, sheet_name="Unassigned Diagnostics", index=False)
        fixed_accounting.to_excel(writer, sheet_name="Fixed Job Policy", index=False)
        run_status.to_excel(writer, sheet_name="Run Status", index=False)
        excluded.to_excel(writer, sheet_name="Excluded Jobs", index=False)

    manifest_path = OUTPUT_DIR / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("fixed_jobs_preserved", None)
    manifest["fixed_job_policy"] = {
        "authorization": "A fixed job remains with its original technician unless that technician is outside the active plan; only rows explicitly flagged fixed_technician_outside_active_plan_relaxed may be reassigned within remaining hard eligibility.",
        "accounting_artifact": "fixed_job_policy_accounting.csv",
        "hard_assertion": "zero unauthorized non-flagged fixed-job reassignments",
        **fixed_summary,
    }
    manifest["weekday_reporting"] = {
        "weekday_number_contract": "ISO weekday: Monday=1, Tuesday=2, Wednesday=3, Thursday=4, Friday=5, Saturday=6, Sunday=7",
        "comparison_artifact": "weekday_comparison.csv",
        "unassigned_diagnostic_artifact": "weekday_unassigned_diagnostics.csv",
        "source_aligned_job_count_per_scenario": 1506,
    }
    manifest["atlanta_6area_vs_existing_summary"] = {
        "path": "atlanta_6area_vs_existing_summary.xlsx",
        "regenerated_without_reroute": True,
        "atlanta_6area_dispatch_jobs": 1220,
        "atlanta_6area_unassigned_jobs": 286,
        "atlanta_6area_dispatch_slots": 1576,
    }
    # This is a semantic repair/validation, not a route recomputation.  It
    # must complete before integrity hashes are refreshed.
    rebuild_area_daily_technician_statistics_from_persisted_outputs()
    detected_locks = sorted(path.name for path in OUTPUT_DIR.glob("~$*") if path.is_file())
    manifest["artifact_integrity"] = {
        "manifest_path": ARTIFACT_INTEGRITY_FILE,
        "algorithm": "sha256",
        "allowlisted_artifact_count": len(CURRENT_ARTIFACT_ALLOWLIST),
        "ignored_non_artifact_patterns": ["~$*"],
        "detected_excel_lock_files": detected_locks,
        "warning": "Excel owner/lock files were not deleted; their presence may mean a workbook is currently open.",
        "integrity_manifest_self_excluded": True,
    }
    manifest["comparison_artifacts"].update({
        "fixed_job_policy_accounting.csv": "Per-fixed-job authorized policy outcome plus verified summary rows",
        "weekday_comparison.csv": "Existing and Atlanta_6area performance by ISO weekday (Monday=1)",
        "weekday_unassigned_diagnostics.csv": "Atlanta unassigned reason/class/slot counts by ISO weekday",
    })
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    integrity = write_artifact_integrity_manifest()
    return {
        "fixed_job_policy": fixed_summary,
        "weekday_rows": len(weekday_comparison),
        "weekday_diagnostic_rows": len(weekday_diagnostics),
        "integrity": integrity,
    }


if __name__ == "__main__":
    run()
