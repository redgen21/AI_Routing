"""Checkpointed June 2026 comparison of one integrated and three region plans."""
from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smart_routing.common_vrp_runtime import _apply_active_region_plan, _apply_job_capabilities
from smart_routing.vrp_api_service import run_routing_request
from tools.operations import run_atlanta6_june_compare as atlanta6
from tools.operations import run_atlanta_fair_13tech_compare as fair

INPUT = ROOT / "260310" / "atlanta 2606_test"
OUTPUT = INPUT / "atlanta_four_scenario_comparison"
BUNDLES = ROOT / "data" / "north_america" / "db_input" / "atlanta_region_imports"
SCENARIO_BUNDLE_OVERRIDES = {
    "Atlanta_3area": ROOT / "260310" / "atlanta 2606_test" / "atlanta_3area_active_db_snapshot_20260724" / "Atlanta_3area",
}
FAIR_DIR = INPUT / "atlanta_13tech_fair_comparison"
SCENARIOS = ("Integrated_13tech", "Atlanta_3area", "Atlanta_6area_new", "Atlanta_6area_overlab")
NEW_SCENARIOS = SCENARIOS[1:]
POLICY = "explicit_workbook_membership/v1"
CHECKPOINT_VERSION = "atlanta_four_scenario_v2_strict_osrm"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name("." + path.name + ".tmp")
    frame.to_csv(temp, index=False, encoding="utf-8-sig")
    os.replace(temp, path)


def atomic_json(value: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name("." + path.name + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temp, path)


def load_snapshot(city: str) -> tuple[dict[str, Any], dict[str, str]]:
    directory = SCENARIO_BUNDLE_OVERRIDES.get(city, BUNDLES / city)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    postals = pd.read_csv(directory / "region_postals.csv", dtype={"postal_code": str})
    technicians = pd.read_csv(directory / "technician_assignments.csv", dtype=str)
    overflow = pd.read_csv(directory / "boundary_overflow.csv", dtype=str)
    regions = []
    for row in manifest["territories"]:
        seq = int(row["seq"])
        territory = str(row["source_territory"])
        regions.append({"region_seq": seq, "region_id": f"{city.lower()}_r{seq:02d}", "region_name": f"{city} {territory}"})
    region_names = {int(row["region_seq"]): row["region_name"] for row in regions}
    postal_rows = []
    for row in postals.to_dict("records"):
        seq = int(row["primary_region_seq"])
        postal_rows.append({
            "postal_code": str(row["postal_code"]).zfill(5), "region_seq": seq,
            "region_name": region_names[seq], "area_type": str(row["area_type"]),
            "source_membership_count": int(row["source_membership_count"]),
        })
    technician_rows = []
    for row in technicians.to_dict("records"):
        seq = int(row["assigned_region_seq"])
        technician_rows.append({
            "employee_code": str(row["employee_code"]), "assigned_region_seq": seq,
            "assigned_region_name": region_names[seq], "active_flag": True,
        })
    override_path = directory / "technician_assignment_override.json"
    if override_path.is_file():
        override = json.loads(override_path.read_text(encoding="utf-8"))
        if (
            override.get("schema") != "region-technician-assignment-override/v1"
            or override.get("environment") != "development"
            or override.get("database") != "vrp_db_dev"
            or override.get("strategic_city_name") != city
            or override.get("plan_id") != manifest.get("plan_id")
        ):
            raise ValueError(f"Invalid technician assignment override contract: {override_path}")
        by_employee = {row["employee_code"]: row for row in technician_rows}
        for change in override.get("assignments", []):
            employee_code = str(change.get("employee_code", "")).strip()
            row = by_employee.get(employee_code)
            from_seq = int(change["from_region_seq"])
            to_seq = int(change["to_region_seq"])
            if row is None or int(row["assigned_region_seq"]) != from_seq or to_seq not in region_names:
                raise ValueError(f"Technician override precondition failed: {employee_code}")
            if str(change.get("to_territory", "")).strip() not in region_names[to_seq]:
                raise ValueError(f"Technician override territory mismatch: {employee_code}")
            row["assigned_region_seq"] = to_seq
            row["assigned_region_name"] = region_names[to_seq]
    overflow_rows = []
    for row in overflow.to_dict("records"):
        if not str(row.get("postal_code", "")).strip():
            continue
        overflow_rows.append({
            "postal_code": str(row["postal_code"]).zfill(5),
            "primary_region_seq": int(row["primary_region_seq"]),
            "alternate_region_seq": int(row["alternate_region_seq"]),
            "allow_overflow": str(row["allow_overflow"]).strip().lower() == "true",
            "penalty_cost": int(row["penalty_cost"]),
        })
    evidence = {
        name: sha256(directory / name)
        for name in ("manifest.json", "region_postals.csv", "technician_assignments.csv", "boundary_overflow.csv")
    }
    if override_path.is_file():
        evidence[override_path.name] = sha256(override_path)
    provenance_path = directory / "db_override_provenance.json"
    if provenance_path.is_file():
        evidence[provenance_path.name] = sha256(provenance_path)
    bundle_hash = hashlib.sha256("".join(evidence[key] for key in sorted(evidence)).encode()).hexdigest()
    snapshot = {
        "enabled": True, "status": "active", "context_status": "active",
        "plan_id": str(manifest["plan_id"]), "revision": 1, "policy_version": POLICY,
        "checksum": bundle_hash, "activation_revision": 1, "regions": regions,
        "postals": postal_rows, "technicians": technician_rows,
        "boundary_overflow": overflow_rows,
    }
    evidence["bundle_sha256"] = bundle_hash
    return snapshot, evidence


def checkpoint_path(scenario: str, date: int) -> Path:
    return OUTPUT / "checkpoints" / scenario / f"{date}.json"


def solve_day(
    scenario: str, snapshot: dict[str, Any] | None, date: int, jobs: pd.DataFrame,
    technicians: pd.DataFrame, homes: pd.DataFrame, capabilities: list[dict[str, Any]],
) -> dict[str, Any]:
    tech_payload = atlanta6.technician_payload(technicians, homes)
    payload = {
        "request_id": f"atlanta_four_{scenario}_{date}", "mode": "na_general", "city": scenario,
        "planning_date": f"{str(date)[:4]}-{str(date)[4:6]}-{str(date)[6:8]}",
        "options": {**fair.OSRM_OPTIONS, "fail_closed_on_osrm_error": True}, "technicians": tech_payload,
        "jobs": _apply_job_capabilities(atlanta6.job_payload(jobs), capabilities, capability_policy_present=True),
        "capabilities": capabilities,
    }
    if snapshot is not None:
        payload = _apply_active_region_plan(payload, snapshot)
    started = time.perf_counter()
    result = run_routing_request(payload)
    elapsed = time.perf_counter() - started
    atlanta6._validate_result_accounting(payload, result)
    matrix_telemetry = result.get("diagnostics", {}).get("matrix_telemetry", {})
    if matrix_telemetry.get("fallback_used") or matrix_telemetry.get("matrix_source") != "osrm_primary":
        raise RuntimeError(f"Strict OSRM solve lacked primary-only evidence: {matrix_telemetry}")
    assigned = atlanta6._assigned_receipts(result)
    empty_probe = {"assigned_receipts": set(), "status": "not_run", "summary": pd.DataFrame()}
    probes = {
        "baseline": {"assigned_receipts": assigned, "status": "main_response", "summary": pd.DataFrame(result.get("engineer_summary", []))},
        "capacity": empty_probe, "work_time": empty_probe, "travel_distance": empty_probe,
    }
    detail, diagnostics = atlanta6._add_area_and_diagnostic_detail(date, result, payload, probes)
    routes = atlanta6._route_detail_rows(date, result, probes["baseline"]["summary"])
    for row in detail:
        row["scenario"] = scenario
        row["raw_reason"] = atlanta6.clean(row.get("reason") or row.get("unassigned_reason"))
    for row in diagnostics:
        row["scenario"] = scenario
    for row in routes:
        row["scenario"] = scenario
    return {
        "checkpoint_version": CHECKPOINT_VERSION, "scenario": scenario, "promise_date": date,
        "runtime_seconds": elapsed, "detail": detail, "diagnostics": diagnostics, "routes": routes,
        "solver_diagnostics": result.get("diagnostics", {}),
    }


def load_checkpoint(scenario: str, date: int, bundle_hash: str) -> dict[str, Any] | None:
    path = checkpoint_path(scenario, date)
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("checkpoint_version") != CHECKPOINT_VERSION or value.get("bundle_sha256") != bundle_hash:
        return None
    return value


def write_checkpoint(value: dict[str, Any], bundle_hash: str) -> None:
    payload = dict(value)
    payload["bundle_sha256"] = bundle_hash
    atomic_json(payload, checkpoint_path(str(value["scenario"]), int(value["promise_date"])))


def status_rows(dates: list[int], new_checkpoints: dict[str, list[dict[str, Any]]]) -> pd.DataFrame:
    rows = []
    for scenario, values in new_checkpoints.items():
        rows.extend({
            "scenario": scenario, "promise_date": int(item["promise_date"]), "status": "completed",
            "runtime_seconds": float(item["runtime_seconds"]),
        } for item in values)
    return pd.DataFrame(rows).sort_values(["scenario", "promise_date"])


def add_travel_minutes(daily: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
    if routes.empty:
        daily["travel_duration_min"] = 0.0
        return daily
    travel = routes.groupby("promise_date", as_index=False)["travel_duration_min"].sum()
    return daily.merge(travel, on="promise_date", how="left").fillna({"travel_duration_min": 0.0})


def remaining_cross_region_slots(
    active: dict[int, dict[str, tuple[str, int]]],
    assigned_slots: dict[tuple[int, str], int],
    shortage_regions: dict[int, set[str]],
) -> int:
    """Count unused slots outside shortage regions once per date/technician."""
    total = 0
    for promise_date, regions in shortage_regions.items():
        for employee_code, (region, capacity) in active.get(promise_date, {}).items():
            if region not in regions:
                total += max(0, capacity - int(assigned_slots.get((promise_date, employee_code), 0)))
    return total


def regional_slot_shortage_summary(
    details: dict[str, pd.DataFrame],
    diagnostics: dict[str, pd.DataFrame],
    fair_tech: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize regional slot shortage and capacity stranded in other regions.

    Other-region capacity follows the established report rule: for each date,
    exclude every primary job region with at least one slot-shortage job, then
    sum max(0, input slot capacity - assigned slots) once for technicians whose
    assigned region is outside that set.  It is capacity evidence, not proof
    that product, time, travel, or overflow policy would permit reassignment.
    """
    output: list[dict[str, Any]] = []
    available = fair_tech["available"].astype(str).str.strip().str.lower().isin({"true", "t", "1", "yes"})
    roster = fair_tech.loc[available].copy()
    roster["promise_date"] = pd.to_numeric(roster["promise_date"], errors="raise").astype(int)
    roster["slot_count"] = pd.to_numeric(roster["slot_count"], errors="coerce").fillna(0).clip(lower=0).astype(int)

    for scenario in NEW_SCENARIOS:
        directory = SCENARIO_BUNDLE_OVERRIDES.get(scenario, BUNDLES / scenario)
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        region_names = {
            int(item["seq"]): f"{scenario} {item['source_territory']}"
            for item in manifest["territories"]
        }
        assignment = pd.read_csv(directory / "technician_assignments.csv", dtype=str)
        technician_region = {
            str(row.employee_code).strip(): region_names[int(row.assigned_region_seq)]
            for row in assignment.itertuples(index=False)
        }
        result = details[scenario].copy()
        result["promise_date"] = pd.to_numeric(result["promise_date"], errors="raise").astype(int)
        result["job_slot_count"] = pd.to_numeric(result["job_slot_count"], errors="coerce").fillna(1).clip(lower=1).astype(int)
        assigned_slots = (
            result[result["result_type"].astype(str).str.lower().eq("assigned")]
            .groupby(["promise_date", "employee_code"])["job_slot_count"].sum().to_dict()
        )
        diag = diagnostics[scenario].copy()
        diag["promise_date"] = pd.to_numeric(diag["promise_date"], errors="raise").astype(int)
        diag["job_slot_count"] = pd.to_numeric(diag["job_slot_count"], errors="coerce").fillna(1).clip(lower=1).astype(int)
        no_feasible = diag[diag["raw_reason"].astype(str).eq("NO_FEASIBLE_ROUTE")].copy()
        remaining_shortage = no_feasible["eligible_remaining_slot_shortage"].astype(str).str.strip().str.lower().isin({"true", "t", "1", "yes"})
        shortage_flag = remaining_shortage
        shortage = no_feasible.loc[shortage_flag].copy()
        active_by_date: dict[int, dict[str, tuple[str, int]]] = defaultdict(dict)
        for row in roster.itertuples(index=False):
            employee_code = str(row.employee_code).strip()
            region = technician_region.get(employee_code)
            if region:
                active_by_date[int(row.promise_date)][employee_code] = (region, int(row.slot_count))
        shortage_regions_by_date: dict[int, set[str]] = {}
        shortage_date_region_groups = 0
        for promise_date, group in shortage.groupby("promise_date"):
            shortage_regions = set(group["job_area"].fillna("").astype(str).str.strip()) - {""}
            shortage_regions_by_date[int(promise_date)] = shortage_regions
            shortage_date_region_groups += len(shortage_regions)
        other_region_remaining_slots = remaining_cross_region_slots(
            active_by_date, assigned_slots, shortage_regions_by_date
        )
        output.append({
            "scenario": scenario,
            "no_feasible_route_jobs": len(no_feasible),
            "no_feasible_route_slots": int(no_feasible["job_slot_count"].sum()),
            "slot_shortage_jobs": len(shortage),
            "slot_shortage_slots": int(shortage["job_slot_count"].sum()),
            "non_slot_shortage_jobs": len(no_feasible) - len(shortage),
            "non_slot_shortage_slots": int(no_feasible["job_slot_count"].sum() - shortage["job_slot_count"].sum()),
            "shortage_dates": int(shortage["promise_date"].nunique()),
            "shortage_date_region_groups": shortage_date_region_groups,
            "other_region_remaining_slots": other_region_remaining_slots,
            "classification_method": "observed eligible-candidate remaining slots < requested job slots",
        })
    return pd.DataFrame(output)


def build_outputs(
    jobs: pd.DataFrame, fair_tech: pd.DataFrame, dates: list[int],
    new_checkpoints: dict[str, list[dict[str, Any]]], evidence: dict[str, dict[str, str]],
) -> None:
    details: dict[str, pd.DataFrame] = {}
    routes: dict[str, pd.DataFrame] = {}
    diagnostics: dict[str, pd.DataFrame] = {}
    daily_frames = []
    for scenario in SCENARIOS:
        _, capacity = fair.capacity_roster(fair_tech, dates, scenario)
        values = new_checkpoints[scenario]
        details[scenario] = pd.DataFrame([row for value in values for row in value["detail"]])
        routes[scenario] = pd.DataFrame([row for value in values for row in value["routes"]])
        diagnostics[scenario] = pd.DataFrame([row for value in values for row in value["diagnostics"]])
        runtime = pd.DataFrame({"promise_date": [int(v["promise_date"]) for v in values], "runtime_seconds": [float(v["runtime_seconds"]) for v in values]})
        daily = fair.metrics_from_detail(jobs, details[scenario], routes[scenario], capacity, scenario, runtime)
        daily = add_travel_minutes(daily, routes[scenario])
        daily_frames.append(daily)
        atomic_csv(details[scenario], OUTPUT / f"{scenario}_routing_results.csv")
        atomic_csv(routes[scenario], OUTPUT / f"{scenario}_route_distance_detail.csv")
        atomic_csv(diagnostics[scenario], OUTPUT / f"{scenario}_unassigned_diagnostics.csv")
    daily_all = pd.concat(daily_frames, ignore_index=True).sort_values(["scenario", "promise_date"])
    overall = daily_all.groupby("scenario", as_index=False).agg(
        days=("promise_date", "nunique"), total_jobs=("total_jobs", "sum"),
        dispatch_jobs=("dispatch_jobs", "sum"), dispatch_slots=("dispatch_slots", "sum"),
        not_dispatch_jobs=("not_dispatch_jobs", "sum"), technician_days=("active_technicians", "sum"),
        total_travel_miles=("travel_distance_miles", "sum"), total_travel_minutes=("travel_duration_min", "sum"),
    )
    total_requested_slots = int(pd.to_numeric(jobs["job_slot_count"], errors="coerce").fillna(1).clip(lower=1).sum())
    overall["not_dispatch_slots"] = total_requested_slots - overall["dispatch_slots"]
    overall["jobs_per_technician"] = overall["dispatch_jobs"] / overall["technician_days"].replace(0, pd.NA)
    overall["slots_per_technician"] = overall["dispatch_slots"] / overall["technician_days"].replace(0, pd.NA)
    overall["job_fill_rate_pct"] = overall["dispatch_jobs"] / overall["total_jobs"] * 100.0
    overall["slot_fill_rate_pct"] = overall["dispatch_slots"] / total_requested_slots * 100.0
    overall["miles_per_assigned_job"] = overall["total_travel_miles"] / overall["dispatch_jobs"].replace(0, pd.NA)
    metric_map = {
        "Total jobs": "total_jobs", "Dispatch jobs": "dispatch_jobs",
        "Not dispatch jobs": "not_dispatch_jobs", "Dispatch slots": "dispatch_slots",
        "Not dispatch slots": "not_dispatch_slots", "Jobs per technician": "jobs_per_technician",
        "Total travel miles": "total_travel_miles", "Total travel minutes": "total_travel_minutes",
        "Job fill rate (%)": "job_fill_rate_pct", "Slot fill rate (%)": "slot_fill_rate_pct",
        "Miles per assigned job": "miles_per_assigned_job",
    }
    indexed = overall.set_index("scenario")
    executive = pd.DataFrame([
        {"metric": label, **{scenario: float(indexed.loc[scenario, column]) for scenario in SCENARIOS}}
        for label, column in metric_map.items()
    ])
    reasons = []
    for scenario, detail in details.items():
        unassigned = detail[detail["result_type"].astype(str).str.lower().eq("unassigned")].copy()
        unassigned["raw_reason"] = unassigned.get("raw_reason", unassigned.get("reason", "UNKNOWN")).fillna("UNKNOWN").replace("", "UNKNOWN")
        unassigned["job_slot_count"] = pd.to_numeric(unassigned["job_slot_count"], errors="coerce").fillna(1).astype(int)
        grouped = unassigned.groupby("raw_reason", as_index=False).agg(unassigned_jobs=("receipt_no", "size"), unassigned_slots=("job_slot_count", "sum"))
        grouped.insert(0, "scenario", scenario)
        reasons.append(grouped)
    reason_frame = pd.concat(reasons, ignore_index=True)
    policy_relaxations = []
    for scenario, detail in details.items():
        policy_detail = detail.merge(
            jobs[["promise_date", "receipt_no", "svc_engineer_code"]],
            on=["promise_date", "receipt_no"], how="left", validate="many_to_one",
            suffixes=("", "_source"),
        )
        relaxed = policy_detail.get(
            "fixed_technician_outside_active_plan_relaxed",
            pd.Series(False, index=policy_detail.index),
        ).astype(str).str.strip().str.lower().isin({"true", "t", "1", "yes"})
        assigned = policy_detail["result_type"].astype(str).str.lower().eq("assigned")
        slot_count = pd.to_numeric(policy_detail["job_slot_count"], errors="coerce").fillna(1).clip(lower=1).astype(int)
        current = policy_detail["svc_engineer_code"].fillna("").astype(str).str.strip()
        actual = policy_detail.get("employee_code", pd.Series("", index=policy_detail.index)).fillna("").astype(str).str.strip()
        reassigned = relaxed & assigned & current.ne("") & actual.ne(current)
        policy_relaxations.append({
            "scenario": scenario,
            "policy_version": "fixed_outside_active_plan_soft_priority/v1" if scenario != SCENARIOS[0] else "hard_fixed_integrated/v1",
            "relaxed_jobs": int(relaxed.sum()),
            "relaxed_slots": int(slot_count[relaxed].sum()),
            "reassigned_to_other_technician_jobs": int(reassigned.sum()),
            "reassigned_to_other_technician_slots": int(slot_count[reassigned].sum()),
            "relaxed_unassigned_jobs": int((relaxed & ~assigned).sum()),
            "relaxed_unassigned_slots": int(slot_count[relaxed & ~assigned].sum()),
        })
    policy_relaxation_frame = pd.DataFrame(policy_relaxations)
    regional_slot_shortage = regional_slot_shortage_summary(details, diagnostics, fair_tech)
    slot_summary, slot_long = fair._slot_reporting(jobs, details)
    weekday = fair._weekday_reporting(jobs, details, daily_all)
    region_rows = []
    for scenario in NEW_SCENARIOS:
        frame = details[scenario].copy()
        frame["job_slot_count"] = pd.to_numeric(frame["job_slot_count"], errors="coerce").fillna(1).astype(int)
        frame["assigned"] = frame["result_type"].astype(str).str.lower().eq("assigned")
        for region, group in frame.groupby("job_area", dropna=False):
            assigned = group[group["assigned"]]
            region_rows.append({
                "scenario": scenario, "region": str(region), "jobs": len(group),
                "requested_slots": int(group["job_slot_count"].sum()), "assigned_jobs": len(assigned),
                "unassigned_jobs": len(group) - len(assigned), "assigned_slots": int(assigned["job_slot_count"].sum()),
            })
    run_status = status_rows(dates, new_checkpoints)
    matrix_telemetry = [
        value["solver_diagnostics"]["matrix_telemetry"]
        for scenario in SCENARIOS for value in new_checkpoints[scenario]
    ]
    if len(matrix_telemetry) != 88 or any(
        item.get("matrix_source") != "osrm_primary"
        or item.get("fallback_count") != 0
        or item.get("failure_count") != 0
        for item in matrix_telemetry
    ):
        raise RuntimeError("Strict OSRM telemetry is incomplete or contains a fallback/failure")
    atomic_csv(daily_all, OUTPUT / "daily_metrics_all_scenarios.csv")
    atomic_csv(overall, OUTPUT / "overall_comparison.csv")
    atomic_csv(executive, OUTPUT / "executive_comparison.csv")
    atomic_csv(reason_frame, OUTPUT / "unassigned_reason_diagnostics.csv")
    atomic_csv(policy_relaxation_frame, OUTPUT / "policy_relaxation_comparison.csv")
    atomic_csv(regional_slot_shortage, OUTPUT / "regional_slot_shortage_comparison.csv")
    atomic_csv(slot_summary, OUTPUT / "slot_count_comparison.csv")
    atomic_csv(slot_long, OUTPUT / "slot_count_result_type_comparison.csv")
    atomic_csv(weekday, OUTPUT / "weekday_comparison.csv")
    atomic_csv(pd.DataFrame(region_rows), OUTPUT / "region_comparison.csv")
    atomic_csv(run_status, OUTPUT / "run_status.csv")
    manifest = {
        "schema_version": "atlanta_four_scenario_comparison/v1", "status": "completed", "completed": True,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(), "scenarios": list(SCENARIOS),
        "dates": dates, "completed_date_count_per_scenario": 22, "aligned_jobs": len(jobs),
        "fair_technician_input_sha256": fair.FAIR_TECH_SHA256, "plan_evidence": evidence,
        "solver": {"engine": "OR-Tools na_general", "objective": "min_total_travel_time", "time_limit_seconds_per_day": 10},
        "matrix": {"endpoint": fair.OSRM_OPTIONS["osrm_url"], "profile": "driving", "coordinate_order": "longitude,latitude", "table_direction": "source_to_destination", "raw_units": "metres/seconds", "normalized_units": "km/min", "fallback_policy": "fail_closed_on_osrm_error", "matrix_request_count": sum(int(item["request_count"]) for item in matrix_telemetry), "actual_fallback_count": 0, "failure_count": 0, "telemetry_evidence": "per-day checkpoint solver_diagnostics.matrix_telemetry", "map_version": "unavailable"},
        "distance_metric": "solver route summary, job-to-job/no-return; home-to-first leg excluded",
        "fixed_job_policy": {
            "Integrated_13tech": "hard_fixed_integrated/v1",
            "regional_scenarios": "fixed_outside_active_plan_soft_priority/v1",
            "approval_basis": "user-approved: fixed jobs outside their technician region use reschedule-like priority, not a hard assignment",
            "aggregate_file": "policy_relaxation_comparison.csv",
        },
        "regional_slot_shortage": {
            "aggregate_file": "regional_slot_shortage_comparison.csv",
            "classification": "observed eligible-candidate remaining slots < requested job slots; not an isolating causal probe",
            "other_region_rule": "per date, exclude all primary job regions with slot-shortage jobs; sum positive input capacity minus assigned slots once for technicians in every other assigned region",
            "caveat": "remaining slots do not prove product, time, travel, fixed-job, or overflow-policy feasibility",
        },
        "result_accounting": {"each_scenario_jobs": {scenario: len(details[scenario]) for scenario in SCENARIOS}, "every_job_assigned_or_explicitly_unassigned": True},
    }
    atomic_json(manifest, OUTPUT / "run_manifest.json")


def run() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    jobs, fair_tech, dates = fair.load_and_validate_inputs()
    homes = pd.read_csv(atlanta6.HOME_FILE, low_memory=False)
    capabilities = atlanta6.load_capabilities(set(fair_tech["employee_code"]))
    snapshots: dict[str, dict[str, Any] | None] = {}
    evidence: dict[str, dict[str, str]] = {}
    snapshots[SCENARIOS[0]] = None
    integrated_inputs = {
        "jobs.csv": sha256(fair.JOBS_FILE),
        "fair_technicians.csv": sha256(fair.FAIR_TECH_FILE),
        "technician_homes.csv": sha256(atlanta6.HOME_FILE),
        "capability_profile.xlsx": sha256(Path(atlanta6.PROFILE_FILE)),
    }
    integrated_inputs["bundle_sha256"] = hashlib.sha256(
        "".join(integrated_inputs[key] for key in sorted(integrated_inputs)).encode()
    ).hexdigest()
    evidence[SCENARIOS[0]] = integrated_inputs
    for scenario in NEW_SCENARIOS:
        snapshots[scenario], evidence[scenario] = load_snapshot(scenario)
    requested = tuple(
        value.strip() for value in os.environ.get("ATLANTA_FOUR_SCENARIOS", "").split(",")
        if value.strip()
    )
    selected_scenarios = requested or SCENARIOS
    unknown = set(selected_scenarios) - set(SCENARIOS)
    if unknown:
        raise ValueError(f"Unknown ATLANTA_FOUR_SCENARIOS: {sorted(unknown)}")
    completed_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    pending: list[tuple[str, int]] = []
    for scenario in selected_scenarios:
        for date in dates:
            checkpoint = load_checkpoint(scenario, date, evidence[scenario]["bundle_sha256"])
            if checkpoint is None:
                pending.append((scenario, date))
            else:
                completed_by_key[(scenario, date)] = checkpoint
                print(f"{scenario} {date}: resumed", flush=True)

    workers = max(1, min(int(os.environ.get("ATLANTA_FOUR_WORKERS", "1")), 8))
    failures: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="atlanta-four") as pool:
        futures = {
            pool.submit(
                solve_day, scenario, snapshots[scenario], date,
                jobs[jobs["promise_date"].eq(date)].copy(),
                fair_tech[fair_tech["promise_date"].eq(date)].copy(), homes, capabilities,
            ): (scenario, date)
            for scenario, date in pending
        }
        for future in as_completed(futures):
            scenario, date = futures[future]
            try:
                checkpoint = future.result()
                write_checkpoint(checkpoint, evidence[scenario]["bundle_sha256"])
                completed_by_key[(scenario, date)] = checkpoint
                count = sum(1 for key in completed_by_key if key[0] == scenario)
                print(f"{scenario} {date}: completed ({count}/{len(dates)})", flush=True)
            except Exception as exc:
                failures.append({"scenario": scenario, "promise_date": date, "error": f"{type(exc).__name__}: {exc}"})
                print(f"{scenario} {date}: FAILED {type(exc).__name__}: {exc}", flush=True)
    if failures:
        atomic_csv(pd.DataFrame(failures), OUTPUT / "run_failures.csv")
        raise RuntimeError(f"Four-scenario execution incomplete; failures={len(failures)}. Resume is safe.")
    if requested:
        print(f"Completed selected scenario checkpoints: {', '.join(selected_scenarios)}", flush=True)
        return
    completed = {
        scenario: [completed_by_key[(scenario, date)] for date in dates]
        for scenario in SCENARIOS
    }
    build_outputs(jobs, fair_tech, dates, completed, evidence)
    print(f"Completed four-scenario comparison: {OUTPUT}", flush=True)


if __name__ == "__main__":
    run()
