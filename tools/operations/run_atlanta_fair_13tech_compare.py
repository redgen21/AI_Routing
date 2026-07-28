"""Run the isolated June 2026 13-technician fair comparison.

This deliberately does not alter the reviewed Atlanta6 comparison.  It solves
the integrated/no-region counterfactual using the supplied 13-technician input,
then copies the already-reviewed Atlanta6 result byte-for-byte after verifying
that the excluded outer-area technician received no assignments.  All generated
files live below ``atlanta_13tech_fair_comparison`` and daily solver responses
are checkpointed there for safe resume.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smart_routing.common_vrp_runtime import _apply_job_capabilities
from smart_routing.vrp_api_service import run_routing_request
from tools.operations import run_atlanta6_june_compare as atlanta6


INPUT_DIR = ROOT / "260310" / "atlanta 2606_test"
OUTPUT_DIR = INPUT_DIR / "atlanta_13tech_fair_comparison"
CHECKPOINT_DIR = OUTPUT_DIR / "integrated_checkpoints_v1"
JOBS_FILE = INPUT_DIR / "atlanta_jobs_20260601_20260630.csv"
FAIR_TECH_FILE = INPUT_DIR / "atlanta_technicians_20260601_20260630_fair_13tech.csv"
BASELINE_RESULTS_FILE = INPUT_DIR / "atlanta_routing_results_20260601_20260630.csv"
ATLANTA6_DIR = INPUT_DIR / "atlanta_6area_comparison"
ATLANTA6_RESULTS_FILE = ATLANTA6_DIR / "atlanta_6area_routing_results.csv"
ATLANTA6_DIAGNOSTICS_FILE = ATLANTA6_DIR / "atlanta_6area_unassigned_diagnostics.csv"
ATLANTA6_ROUTE_FILE = ATLANTA6_DIR / "area_technician_route_distance_detail.csv"
ATLANTA6_FIXED_ACCOUNTING_FILE = ATLANTA6_DIR / "fixed_job_policy_accounting.csv"
HOME_FILE = atlanta6.HOME_FILE

FAIR_TECH_SHA256 = "63e2bed2cadfe5bda5a7ec9b6fe5331e31328373f08adcd766fcc86331258cf5"
EXCLUDED_CODES = {"AI102933", "AI105115"}
FAIR_SCENARIO = "Integrated_13tech"
ATLANTA6_SCENARIO = "Atlanta_6area_13tech"
CHECKPOINT_VERSION = "atlanta_fair_13tech_integrated_v1"
OSRM_OPTIONS = {
    "respect_fixed_jobs": True,
    "objective": "min_total_travel_time",
    "time_limit_seconds": 10,
    "osrm_url": "http://20.51.244.68:5002",
    "distance_backend": "city_osrm_else_haversine",
}


def clean(value: object) -> str:
    return atlanta6.clean(value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8-sig")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_json(value: dict[str, Any], path: Path) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_and_validate_inputs() -> tuple[pd.DataFrame, pd.DataFrame, list[int]]:
    """Load the locked fair input and the 1,506-job comparison universe."""

    if _sha256(FAIR_TECH_FILE) != FAIR_TECH_SHA256:
        raise RuntimeError("Fair technician input SHA-256 does not match the authorized file")
    jobs = pd.read_csv(JOBS_FILE, low_memory=False)
    fair_tech = pd.read_csv(FAIR_TECH_FILE, low_memory=False)
    baseline = pd.read_csv(BASELINE_RESULTS_FILE, low_memory=False)
    for frame, field in ((jobs, "promise_date"), (fair_tech, "promise_date"), (baseline, "promise_date")):
        frame[field] = pd.to_numeric(frame[field], errors="raise").astype(int)
    fair_tech["employee_code"] = fair_tech["employee_code"].map(clean)
    actual_codes = set(fair_tech["employee_code"])
    if len(actual_codes) != 13 or actual_codes & EXCLUDED_CODES:
        raise RuntimeError(f"Fair technician roster is not the authorized 13-tech population: {sorted(actual_codes)}")
    if fair_tech.duplicated(["promise_date", "employee_code"]).any():
        raise RuntimeError("Fair technician input has duplicate date+employee keys")
    baseline_keys = set(zip(baseline["promise_date"], baseline["receipt_no"].map(clean)))
    jobs["receipt_no"] = jobs["gsfs_receipt_no"].map(clean)
    jobs = jobs[[key in baseline_keys for key in zip(jobs["promise_date"], jobs["receipt_no"])]].copy()
    if len(jobs) != 1506 or jobs.duplicated(["promise_date", "receipt_no"]).any():
        raise RuntimeError("Fair comparison requires exactly 1,506 unique aligned date+receipt jobs")
    dates = sorted(jobs["promise_date"].unique().tolist())
    if len(dates) != 22 or set(fair_tech["promise_date"]) != set(dates):
        raise RuntimeError("Fair technician input does not cover the 22 aligned comparison dates")
    return jobs, fair_tech, dates


def _requested_dates(dates: list[int]) -> list[int]:
    requested = {
        int(value.strip())
        for value in os.environ.get("ATLANTA_FAIR_COMPARE_DATES", "").split(",")
        if value.strip()
    }
    selected = [date for date in dates if not requested or date in requested]
    if not selected:
        raise RuntimeError("ATLANTA_FAIR_COMPARE_DATES selected no available comparison dates")
    return selected


def capacity_roster(technicians: pd.DataFrame, dates: list[int], scenario: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return daily input capacity, including available zero-assignment people."""

    source = technicians[technicians["promise_date"].isin(dates)].copy()
    if set(source["promise_date"]) != set(dates):
        raise RuntimeError(f"{scenario} capacity roster date coverage is incomplete")
    source["available"] = source["available"].map(atlanta6.truthy)
    source["slot_count"] = source["slot_count"].map(lambda value: max(0, atlanta6.number(value, 8)))
    source["solver_input_eligible"] = source["available"]
    source["scenario"] = scenario
    source["capacity_source"] = "authorized_fair_13tech_daily_input"
    roster = source[[
        "promise_date", "scenario", "capacity_source", "employee_code", "available",
        "solver_input_eligible", "slot_count",
    ]].sort_values(["promise_date", "employee_code"]).reset_index(drop=True)
    eligible_slots = roster["slot_count"].where(roster["solver_input_eligible"], 0)
    roster = roster.assign(_eligible_slots=eligible_slots)
    summary = roster.groupby(["promise_date", "scenario", "capacity_source"], as_index=False).agg(
        technician_input_rows=("employee_code", "nunique"),
        solver_input_available_technicians=("solver_input_eligible", "sum"),
        solver_input_available_slots=("_eligible_slots", "sum"),
    )
    return roster.drop(columns="_eligible_slots"), summary.sort_values("promise_date").reset_index(drop=True)


def _keys(frame: pd.DataFrame) -> set[tuple[int, str]]:
    return set(zip(frame["promise_date"].astype(int), frame["receipt_no"].map(clean)))


def validate_candidate_reuse(jobs: pd.DataFrame, dates: list[int]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Validate, then only read/copy, the reviewed Atlanta6 routing outcome."""

    detail = pd.read_csv(ATLANTA6_RESULTS_FILE, low_memory=False)
    diagnostics = pd.read_csv(ATLANTA6_DIAGNOSTICS_FILE, low_memory=False)
    routes = pd.read_csv(ATLANTA6_ROUTE_FILE, low_memory=False)
    for frame in (detail, diagnostics, routes):
        frame["promise_date"] = pd.to_numeric(frame["promise_date"], errors="raise").astype(int)
    detail = detail[detail["promise_date"].isin(dates)].copy()
    diagnostics = diagnostics[diagnostics["promise_date"].isin(dates)].copy()
    routes = routes[routes["promise_date"].isin(dates)].copy()
    expected = _keys(jobs[jobs["promise_date"].isin(dates)])
    if len(detail) != len(expected) or detail.duplicated(["promise_date", "receipt_no"]).any() or _keys(detail) != expected:
        raise RuntimeError("Reviewed Atlanta6 routing result does not match the requested comparison universe")
    ai_assignments = detail[
        detail["employee_code"].map(clean).eq("AI105115")
        & detail["result_type"].map(clean).str.lower().eq("assigned")
    ]
    if not ai_assignments.empty:
        raise RuntimeError("Cannot reuse Atlanta6 result: AI105115 has assignments")
    return detail, diagnostics, routes


def _checkpoint_path(date: int) -> Path:
    return CHECKPOINT_DIR / f"{date}.json"


def _load_checkpoint(date: int) -> dict[str, Any] | None:
    path = _checkpoint_path(date)
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    required = {"metrics", "detail", "diagnostics", "routes", "status", "solver_diagnostics"}
    if value.get("checkpoint_version") != CHECKPOINT_VERSION or value.get("fair_tech_sha256") != FAIR_TECH_SHA256:
        return None
    return value if int(value.get("promise_date", 0)) == date and required.issubset(value) else None


def _write_checkpoint(date: int, value: dict[str, Any]) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    _atomic_json(value, _checkpoint_path(date))


def _detail_and_diagnostics(
    date: int, result: dict[str, Any], payload: dict[str, Any], probes: dict[str, dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    detail, diagnostics = atlanta6._add_area_and_diagnostic_detail(date, result, payload, probes)
    for row in detail:
        row["job_area"] = "INTEGRATED_NO_REGION"
    for row in diagnostics:
        row["job_area"] = "INTEGRATED_NO_REGION"
    return detail, diagnostics


def _solve_integrated_day(
    date: int, day_jobs: pd.DataFrame, day_tech: pd.DataFrame, homes: pd.DataFrame, capabilities: list[dict[str, Any]]
) -> dict[str, Any]:
    technicians = atlanta6.technician_payload(day_tech, homes)
    payload = {
        "request_id": f"atlanta_fair_13tech_integrated_{date}",
        "mode": "na_general",
        "city": "Atlanta, GA",
        "planning_date": f"{str(date)[:4]}-{str(date)[4:6]}-{str(date)[6:8]}",
        "options": dict(OSRM_OPTIONS),
        "technicians": technicians,
        "jobs": _apply_job_capabilities(
            atlanta6.job_payload(day_jobs), capabilities, capability_policy_present=True
        ),
        "capabilities": capabilities,
    }
    started = time.perf_counter()
    result = run_routing_request(payload)
    elapsed = time.perf_counter() - started
    atlanta6._validate_result_accounting(payload, result)
    probes: dict[str, dict[str, Any]] = {
        "baseline": {
            "status": "main_response",
            "assigned_receipts": atlanta6._assigned_receipts(result),
            "summary": pd.DataFrame(result.get("engineer_summary", [])),
            "runtime_seconds": elapsed,
        }
    }
    no_feasible = atlanta6._no_feasible_receipts_with_eligible_candidates(result, payload)
    if no_feasible:
        probes["capacity"] = atlanta6._run_constraint_probe(payload, "capacity")
        probes["work_time"] = atlanta6._run_constraint_probe(payload, "work_time")
    else:
        probes["capacity"] = {"status": "not_needed", "assigned_receipts": set(), "summary": pd.DataFrame()}
        probes["work_time"] = {"status": "not_needed", "assigned_receipts": set(), "summary": pd.DataFrame()}
    probes["travel_distance"] = {
        "status": "not_configured", "assigned_receipts": set(), "summary": pd.DataFrame()
    }
    metrics, _unused_detail, reasons = atlanta6.build_daily_metrics(date, day_jobs, technicians, result, elapsed)
    metrics["scenario"] = FAIR_SCENARIO
    detail, diagnostics = _detail_and_diagnostics(date, result, payload, probes)
    routes = atlanta6._route_detail_rows(date, result, probes["baseline"]["summary"])
    for row in routes:
        row["technician_area"] = "INTEGRATED_NO_REGION"
    metrics["travel_distance_km"] = sum(float(row["travel_distance_km"]) for row in routes)
    metrics["travel_distance_miles"] = metrics["travel_distance_km"] * atlanta6.KM_TO_MILES
    status = {
        "promise_date": date, "status": "completed", "runtime_seconds": elapsed,
        "assigned_jobs": metrics["dispatch_jobs"], "unassigned_jobs": metrics["not_dispatch_jobs"],
        "probe_candidate_no_feasible_jobs": len(no_feasible), "error": "",
    }
    return {
        "checkpoint_version": CHECKPOINT_VERSION, "fair_tech_sha256": FAIR_TECH_SHA256,
        "promise_date": date, "metrics": metrics, "detail": detail, "diagnostics": diagnostics,
        "routes": routes, "reasons": reasons, "status": status,
        "solver_diagnostics": result.get("diagnostics", {}),
        "probe_status": [{
            "promise_date": date, "probe": name, "status": probe.get("status", "unknown"),
            "runtime_seconds": probe.get("runtime_seconds", pd.NA),
            "assigned_jobs": len(probe.get("assigned_receipts", set())),
        } for name, probe in probes.items()],
    }


def metrics_from_detail(
    jobs: pd.DataFrame, detail: pd.DataFrame, routes: pd.DataFrame, capacity: pd.DataFrame,
    scenario: str, runtime: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Recalculate all capacity-sensitive metrics from result keys and fair roster."""

    requested = jobs[["promise_date", "receipt_no", "job_slot_count"]].copy()
    requested["job_slot_count"] = pd.to_numeric(requested["job_slot_count"], errors="coerce").fillna(1).astype(int).clip(lower=1)
    outcomes = detail[["promise_date", "receipt_no", "result_type", "employee_code"]].copy()
    merged = requested.merge(outcomes, on=["promise_date", "receipt_no"], how="left", validate="one_to_one")
    if merged["result_type"].isna().any():
        raise RuntimeError(f"{scenario} is missing result rows")
    merged["assigned"] = merged["result_type"].map(clean).str.lower().eq("assigned")
    grouped = merged.groupby("promise_date", as_index=False).agg(
        total_jobs=("receipt_no", "size"), dispatch_jobs=("assigned", "sum"),
        dispatch_slots=("job_slot_count", lambda values: int(values[merged.loc[values.index, "assigned"]].sum())),
    )
    grouped["not_dispatch_jobs"] = grouped["total_jobs"] - grouped["dispatch_jobs"]
    dispatched = merged[merged["assigned"]].groupby("promise_date", as_index=False).agg(
        dispatched_technicians=("employee_code", lambda values: values.map(clean).replace("", pd.NA).nunique())
    )
    travel = routes.groupby("promise_date", as_index=False).agg(
        travel_distance_km=("travel_distance_km", "sum"), travel_distance_miles=("travel_distance_miles", "sum")
    ) if not routes.empty else pd.DataFrame(columns=["promise_date", "travel_distance_km", "travel_distance_miles"])
    out = grouped.merge(dispatched, on="promise_date", how="left").merge(travel, on="promise_date", how="left")
    out = out.merge(capacity[["promise_date", "solver_input_available_technicians", "solver_input_available_slots"]], on="promise_date", how="left", validate="one_to_one")
    out["scenario"] = scenario
    out["dispatched_technicians"] = out["dispatched_technicians"].fillna(0).astype(int)
    out[["travel_distance_km", "travel_distance_miles"]] = out[["travel_distance_km", "travel_distance_miles"]].fillna(0.0)
    out["active_technicians"] = out["solver_input_available_technicians"].astype(int)
    out["available_slots"] = out["solver_input_available_slots"].astype(int)
    out["input_available_technicians"] = out["active_technicians"]
    out["input_available_slots"] = out["available_slots"]
    out["avg_jobs"] = out["dispatch_jobs"] / out["active_technicians"].replace(0, pd.NA)
    out["avg_slots"] = out["dispatch_slots"] / out["active_technicians"].replace(0, pd.NA)
    out["job_fill_rate_pct"] = out["dispatch_jobs"] / out["total_jobs"] * 100.0
    out["fill_rate_pct"] = out["dispatch_slots"] / out["available_slots"].replace(0, pd.NA) * 100.0
    out["avg_travel_miles_per_active_tech"] = out["travel_distance_miles"] / out["active_technicians"].replace(0, pd.NA)
    out["runtime_seconds"] = pd.NA
    if runtime is not None and not runtime.empty:
        out = out.drop(columns="runtime_seconds").merge(runtime[["promise_date", "runtime_seconds"]], on="promise_date", how="left")
    return out.sort_values("promise_date").reset_index(drop=True)


def _slot_reporting(jobs: pd.DataFrame, outcomes: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    requested = jobs[["promise_date", "receipt_no", "job_slot_count"]].copy()
    requested["job_slot_count"] = pd.to_numeric(requested["job_slot_count"], errors="coerce").fillna(1).astype(int).clip(lower=1)
    summary: list[dict[str, Any]] = []
    long: list[dict[str, Any]] = []
    expected = _keys(requested)
    for scenario, detail in outcomes.items():
        result = detail[["promise_date", "receipt_no", "result_type"]].copy()
        if result.duplicated(["promise_date", "receipt_no"]).any() or _keys(result) != expected:
            raise RuntimeError(f"{scenario} cannot be used for slot reporting")
        result["assigned"] = result["result_type"].map(clean).str.lower().eq("assigned")
        merged = requested.merge(result, on=["promise_date", "receipt_no"], validate="one_to_one")
        for slots, group in merged.groupby("job_slot_count", sort=True):
            assigned = group[group["assigned"]]
            summary.append({"scenario": scenario, "job_slot_count": int(slots), "total_jobs": len(group), "assigned_jobs": len(assigned), "unassigned_jobs": len(group) - len(assigned), "requested_slots": int(group["job_slot_count"].sum()), "assigned_slots": int(assigned["job_slot_count"].sum()), "job_fill_rate_pct": len(assigned) / len(group) * 100.0})
        for (slots, result_type), group in merged.groupby(["job_slot_count", "result_type"], sort=True):
            long.append({"scenario": scenario, "job_slot_count": int(slots), "result_type": clean(result_type).lower(), "jobs": len(group), "slots": int(group["job_slot_count"].sum())})
    return pd.DataFrame(summary), pd.DataFrame(long)


def _weekday_reporting(jobs: pd.DataFrame, outcomes: dict[str, pd.DataFrame], daily: pd.DataFrame) -> pd.DataFrame:
    requested = jobs[["promise_date", "receipt_no", "job_slot_count"]].copy()
    requested["weekday_number"] = pd.to_datetime(requested["promise_date"].astype(str), format="%Y%m%d").dt.weekday + 1
    requested["weekday_name"] = pd.to_datetime(requested["promise_date"].astype(str), format="%Y%m%d").dt.day_name()
    rows: list[dict[str, Any]] = []
    for scenario, detail in outcomes.items():
        merged = requested.merge(detail[["promise_date", "receipt_no", "result_type"]], on=["promise_date", "receipt_no"], validate="one_to_one")
        for (number, name), group in merged.groupby(["weekday_number", "weekday_name"], sort=True):
            assigned = group[group["result_type"].map(clean).str.lower().eq("assigned")]
            metrics = daily[(daily["scenario"] == scenario) & (pd.to_datetime(daily["promise_date"].astype(str), format="%Y%m%d").dt.weekday + 1 == number)]
            active = float(metrics["active_technicians"].sum())
            miles = float(metrics["travel_distance_miles"].sum())
            rows.append({"scenario": scenario, "weekday_number": int(number), "weekday_name": name, "observed_days": int(group["promise_date"].nunique()), "total_jobs": len(group), "assigned_jobs": len(assigned), "unassigned_jobs": len(group) - len(assigned), "requested_slots": int(group["job_slot_count"].sum()), "assigned_slots": int(assigned["job_slot_count"].sum()), "job_fill_rate_pct": len(assigned) / len(group) * 100.0, "slot_fill_rate_pct": assigned["job_slot_count"].sum() / group["job_slot_count"].sum() * 100.0, "active_technician_days": active, "avg_jobs_per_active_tech_day": len(assigned) / active if active else pd.NA, "avg_slots_per_active_tech_day": assigned["job_slot_count"].sum() / active if active else pd.NA, "total_travel_miles": miles})
    return pd.DataFrame(rows).sort_values(["weekday_number", "scenario"]).reset_index(drop=True)


def _overall(daily: pd.DataFrame) -> pd.DataFrame:
    return daily.groupby("scenario", as_index=False).agg(days=("promise_date", "nunique"), total_jobs=("total_jobs", "sum"), dispatch_jobs=("dispatch_jobs", "sum"), dispatch_slots=("dispatch_slots", "sum"), not_dispatch_jobs=("not_dispatch_jobs", "sum"), avg_jobs=("avg_jobs", "mean"), avg_slots=("avg_slots", "mean"), avg_job_fill_rate_pct=("job_fill_rate_pct", "mean"), avg_fill_rate_pct=("fill_rate_pct", "mean"), travel_distance_km=("travel_distance_km", "sum"), travel_distance_miles=("travel_distance_miles", "sum"), avg_travel_miles_per_active_tech=("avg_travel_miles_per_active_tech", "mean")).sort_values("scenario")


def _executive(overall: pd.DataFrame) -> pd.DataFrame:
    integrated = overall[overall["scenario"].eq(FAIR_SCENARIO)].iloc[0]
    candidate = overall[overall["scenario"].eq(ATLANTA6_SCENARIO)].iloc[0]
    rows = []
    for label, column in (("Total jobs", "total_jobs"), ("Dispatch jobs", "dispatch_jobs"), ("Dispatch slots", "dispatch_slots"), ("Not dispatch jobs", "not_dispatch_jobs"), ("Daily avg job fill rate (%)", "avg_job_fill_rate_pct"), ("Daily avg slot fill rate (%)", "avg_fill_rate_pct"), ("Total travel miles", "travel_distance_miles"), ("Avg travel miles / active tech-day", "avg_travel_miles_per_active_tech")):
        rows.append({"metric": label, "integrated_13tech": float(integrated[column]), "atlanta_6area_13tech": float(candidate[column]), "delta_atlanta6_minus_integrated": float(candidate[column] - integrated[column])})
    return pd.DataFrame(rows)


def _integrated_fixed_policy(jobs: pd.DataFrame, detail: pd.DataFrame) -> pd.DataFrame:
    fixed = jobs[jobs["fixed"].map(atlanta6.truthy)][["promise_date", "receipt_no", "svc_engineer_code"]].copy()
    fixed["current_employee_code"] = fixed.pop("svc_engineer_code").map(clean)
    result = detail[["promise_date", "receipt_no", "result_type", "employee_code", "raw_reason"]].copy()
    output = fixed.merge(result, on=["promise_date", "receipt_no"], how="left", validate="one_to_one")
    output["employee_code"] = output["employee_code"].fillna("").map(clean)
    bad = output[output["result_type"].map(clean).str.lower().eq("assigned") & output["employee_code"].ne(output["current_employee_code"])]
    if not bad.empty:
        raise RuntimeError(f"Integrated fair run violated hard fixed-job policy: {bad[['receipt_no']].head().to_dict('records')}")
    output["policy_outcome"] = output.apply(lambda row: "PRESERVED_ORIGINAL_TECHNICIAN" if clean(row.get("result_type")).lower() == "assigned" else f"UNASSIGNED_{clean(row.get('raw_reason')) or 'UNKNOWN'}", axis=1)
    return output


def run() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    jobs, fair_tech, all_dates = load_and_validate_inputs()
    dates = _requested_dates(all_dates)
    jobs = jobs[jobs["promise_date"].isin(dates)].copy()
    candidate_detail, candidate_diagnostics, candidate_routes = validate_candidate_reuse(jobs, dates)
    roster_integrated, capacity_integrated = capacity_roster(fair_tech, dates, FAIR_SCENARIO)
    roster_candidate, capacity_candidate = capacity_roster(fair_tech, dates, ATLANTA6_SCENARIO)
    homes = pd.read_csv(HOME_FILE, low_memory=False)
    capabilities = atlanta6.load_capabilities(set(fair_tech["employee_code"]))
    checkpoints: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index, date in enumerate(dates, start=1):
        checkpoint = _load_checkpoint(date)
        if checkpoint is None:
            try:
                checkpoint = _solve_integrated_day(date, jobs[jobs["promise_date"].eq(date)], fair_tech[fair_tech["promise_date"].eq(date)], homes, capabilities)
                _write_checkpoint(date, checkpoint)
                print(f"{date}: completed integrated assigned={checkpoint['metrics']['dispatch_jobs']} unassigned={checkpoint['metrics']['not_dispatch_jobs']} ({index}/{len(dates)})", flush=True)
            except Exception as exc:
                failure = {"promise_date": date, "status": "failed", "error": f"{type(exc).__name__}: {exc}"}
                failures.append(failure)
                print(f"{date}: FAILED {failure['error']}", flush=True)
                continue
        else:
            print(f"{date}: resumed checkpoint ({index}/{len(dates)})", flush=True)
        checkpoints.append(checkpoint)
    if failures or len(checkpoints) != len(dates):
        _atomic_csv(pd.DataFrame(failures), OUTPUT_DIR / "run_failures.csv")
        raise RuntimeError(f"Integrated fair execution incomplete; completed={len(checkpoints)} failed={len(failures)}. Resume is safe.")

    integrated_detail = pd.DataFrame([row for checkpoint in checkpoints for row in checkpoint["detail"]])
    integrated_diagnostics = pd.DataFrame([row for checkpoint in checkpoints for row in checkpoint["diagnostics"]])
    integrated_routes = pd.DataFrame([row for checkpoint in checkpoints for row in checkpoint["routes"]])
    status = pd.DataFrame([checkpoint["status"] for checkpoint in checkpoints])
    probes = pd.DataFrame([row for checkpoint in checkpoints for row in checkpoint["probe_status"]])
    expected = _keys(jobs)
    if len(integrated_detail) != len(expected) or integrated_detail.duplicated(["promise_date", "receipt_no"]).any() or _keys(integrated_detail) != expected:
        raise RuntimeError("Integrated fair output does not assign each job once or explicitly unassign it")
    integrated_detail["raw_reason"] = integrated_detail.get("reason", "").fillna("").map(clean)
    if not integrated_diagnostics.empty:
        integrated_detail = integrated_detail.merge(integrated_diagnostics[["promise_date", "receipt_no", "diagnostic_classification", "classification_evidence"]], on=["promise_date", "receipt_no"], how="left")
    integrated_detail["diagnostic_classification"] = integrated_detail.get("diagnostic_classification", "NOT_APPLICABLE").fillna("NOT_APPLICABLE")
    integrated_daily = metrics_from_detail(jobs, integrated_detail, integrated_routes, capacity_integrated, FAIR_SCENARIO, status)
    candidate_daily = metrics_from_detail(jobs, candidate_detail, candidate_routes, capacity_candidate, ATLANTA6_SCENARIO)
    daily = pd.concat([integrated_daily, candidate_daily], ignore_index=True).sort_values(["promise_date", "scenario"])
    overall = _overall(daily)
    executive = _executive(overall)
    slot_summary, slot_long = _slot_reporting(jobs, {FAIR_SCENARIO: integrated_detail, ATLANTA6_SCENARIO: candidate_detail})
    weekday = _weekday_reporting(jobs, {FAIR_SCENARIO: integrated_detail, ATLANTA6_SCENARIO: candidate_detail}, daily)
    fixed = _integrated_fixed_policy(jobs, integrated_detail)
    # The candidate routing result is copied byte-for-byte after its zero-AI105115 assertion above.
    shutil.copy2(ATLANTA6_RESULTS_FILE, OUTPUT_DIR / "atlanta_6area_13tech_routing_results_20260601_20260630.csv")
    shutil.copy2(ATLANTA6_FIXED_ACCOUNTING_FILE, OUTPUT_DIR / "atlanta_6area_13tech_fixed_job_policy_accounting.csv")
    _atomic_csv(integrated_detail, OUTPUT_DIR / "atlanta_integrated_13tech_routing_results_20260601_20260630.csv")
    _atomic_csv(candidate_diagnostics, OUTPUT_DIR / "atlanta_6area_13tech_unassigned_diagnostics.csv")
    _atomic_csv(integrated_diagnostics, OUTPUT_DIR / "atlanta_integrated_13tech_unassigned_diagnostics.csv")
    _atomic_csv(candidate_routes, OUTPUT_DIR / "atlanta_6area_13tech_route_distance_detail.csv")
    _atomic_csv(integrated_routes, OUTPUT_DIR / "atlanta_integrated_13tech_route_distance_detail.csv")
    _atomic_csv(integrated_daily, OUTPUT_DIR / "atlanta_integrated_13tech_daily_metrics.csv")
    _atomic_csv(candidate_daily, OUTPUT_DIR / "atlanta_6area_13tech_daily_metrics.csv")
    _atomic_csv(pd.concat([roster_integrated, roster_candidate], ignore_index=True), OUTPUT_DIR / "technician_input_capacity_roster.csv")
    daily_comparison = integrated_daily.merge(
        candidate_daily, on="promise_date", suffixes=("_integrated_13tech", "_atlanta6_13tech"),
        validate="one_to_one",
    )
    for metric in ("dispatch_jobs", "dispatch_slots", "not_dispatch_jobs", "job_fill_rate_pct", "fill_rate_pct", "travel_distance_miles"):
        daily_comparison[f"{metric}_delta_atlanta6_minus_integrated"] = (
            daily_comparison[f"{metric}_atlanta6_13tech"]
            - daily_comparison[f"{metric}_integrated_13tech"]
        )
    _atomic_csv(daily, OUTPUT_DIR / "daily_metrics_all_scenarios.csv")
    _atomic_csv(daily_comparison, OUTPUT_DIR / "daily_comparison.csv")
    _atomic_csv(overall, OUTPUT_DIR / "overall_comparison.csv")
    _atomic_csv(executive, OUTPUT_DIR / "executive_comparison.csv")
    _atomic_csv(slot_summary, OUTPUT_DIR / "slot_count_comparison.csv")
    _atomic_csv(slot_long, OUTPUT_DIR / "slot_count_result_type_comparison.csv")
    _atomic_csv(weekday, OUTPUT_DIR / "weekday_comparison.csv")
    _atomic_csv(status, OUTPUT_DIR / "run_status.csv")
    _atomic_csv(probes, OUTPUT_DIR / "constraint_probe_run_status.csv")
    _atomic_csv(fixed, OUTPUT_DIR / "integrated_fixed_job_policy_accounting.csv")
    _atomic_json({
        "schema_version": "atlanta_fair_13tech_comparison/v1", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "period": {"calendar_start": "2026-06-01", "calendar_end": "2026-06-30", "execution_scope": "all 22 available June 2026 business dates"},
        "dates": dates, "completed_date_count": len(dates), "aligned_jobs": len(jobs), "fair_technician_input": {"path": str(FAIR_TECH_FILE.relative_to(ROOT)), "sha256": FAIR_TECH_SHA256, "excluded_codes": sorted(EXCLUDED_CODES), "unique_employee_codes": sorted(set(fair_tech["employee_code"]))},
        "scenarios": {FAIR_SCENARIO: "new integrated/no-region OR-Tools solve", ATLANTA6_SCENARIO: "reviewed Atlanta6 result copied unchanged; capacity recalculated from fair 13-tech input"},
        "candidate_reuse": {"source": str(ATLANTA6_RESULTS_FILE.relative_to(ROOT)), "source_sha256": _sha256(ATLANTA6_RESULTS_FILE), "copied_result": "atlanta_6area_13tech_routing_results_20260601_20260630.csv", "ai105115_assigned_jobs": 0},
        "fixed_job_policy": {"integrated": "hard original-technician preservation; no active-plan release", "integrated_accounting": "integrated_fixed_job_policy_accounting.csv", "atlanta6": "reused reviewed result and its original fixed-job policy evidence", "atlanta6_reused_accounting": "atlanta_6area_13tech_fixed_job_policy_accounting.csv"},
        "solver": {"engine": "OR-Tools na_general / production_assign_atlanta_vrp", "objective": "min_total_travel_time", "time_limit_seconds_per_day": 10, "first_solution_strategy": "PARALLEL_CHEAPEST_INSERTION", "local_search": "GUIDED_LOCAL_SEARCH"},
        "matrix": {"source": "OSRM table/route via city_osrm_else_haversine", "shape": "(technicians + jobs) square directed matrix per daily solve", "coordinate_order": "longitude,latitude", "fallback": "haversine_on_osrm_error", "units": {"distance": "km", "duration": "minutes", "service_duration": "minutes (45 per slot unless input override)", "slot_capacity": "job slots"}},
        "result_accounting": {"integrated_unique_result_keys": len(_keys(integrated_detail)), "candidate_unique_result_keys": len(_keys(candidate_detail)), "every_job_assigned_or_explicitly_unassigned": True},
    }, OUTPUT_DIR / "run_manifest.json")


if __name__ == "__main__":
    run()
