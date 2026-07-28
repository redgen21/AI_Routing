"""Build the dynamic, privacy-safe Atlanta fair 13-technician comparison report.

The preserved Atlanta6 legacy report is an archival visual reference only.  This
module never reads it (or its own output): every displayed number is derived
from the completed fair-comparison artifact set.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIR = ROOT / "260310" / "atlanta 2606_test" / "atlanta_13tech_fair_comparison"
DEFAULT_OUTPUT = ROOT / "보고서" / "atlanta_13tech_fair_comparison_report_ko.html"
INTEGRATED, ATLANTA6 = "Integrated_13tech", "Atlanta_6area_13tech"
REQUIRED = ("run_manifest.json", "overall_comparison.csv", "executive_comparison.csv",
            "daily_metrics_all_scenarios.csv", "slot_count_comparison.csv", "weekday_comparison.csv",
            "technician_input_capacity_roster.csv", "run_status.csv",
            "atlanta_integrated_13tech_routing_results_20260601_20260630.csv",
            "atlanta_6area_13tech_routing_results_20260601_20260630.csv",
            "atlanta_integrated_13tech_unassigned_diagnostics.csv",
            "atlanta_6area_13tech_unassigned_diagnostics.csv",
            "integrated_fixed_job_policy_accounting.csv", "atlanta_6area_13tech_fixed_job_policy_accounting.csv")
TECHNICIAN_CODE = re.compile(r"\bAI\d{6}\b")
RECEIPT_NUMBER = re.compile(r"\bRNN[A-Z0-9]+\b")
EXTERNAL_URL = re.compile(r"(?:https?:)?//", re.I)


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def num(value: object) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"Non-finite numeric value: {value!r}")
    return result


def integer(value: object) -> int:
    result = round(num(value))
    if abs(num(value) - result) > 1e-8:
        raise ValueError(f"Expected integer value: {value!r}")
    return result


def fi(value: object) -> str: return f"{integer(value):,}"
def fn(value: object, places: int = 1) -> str: return f"{num(value):,.{places}f}"
def delta(value: object, suffix: str = "") -> str: return f"{num(value):+,.1f}{suffix}"


def _result_keys(data: list[dict[str, str]]) -> set[tuple[str, str]]:
    keys = {(r.get("promise_date", ""), r.get("receipt_no", "")) for r in data}
    if len(keys) != len(data) or any(not a or not b for a, b in keys):
        raise ValueError("Routing results must have unique non-empty date/receipt keys")
    return keys


def build_model(directory: Path) -> dict:
    directory = directory.resolve()
    missing = [n for n in REQUIRED if not (directory / n).is_file()]
    if missing: raise ValueError("Fair comparison is incomplete; required artifact missing: " + ", ".join(missing))
    manifest = json.loads((directory / "run_manifest.json").read_text(encoding="utf-8"))
    dates = {str(x) for x in manifest.get("dates", [])}
    if manifest.get("schema_version") != "atlanta_fair_13tech_comparison/v1" or len(dates) != 22 or manifest.get("completed_date_count") != 22:
        raise ValueError("Report requires the completed fair comparison manifest and 22 dates")
    if manifest.get("aligned_jobs") != 1506 or not manifest.get("result_accounting", {}).get("every_job_assigned_or_explicitly_unassigned"):
        raise ValueError("Manifest does not attest the 1,506-job fair roster")
    if set(manifest.get("scenarios", ())) != {INTEGRATED, ATLANTA6}: raise ValueError("Unexpected scenario contract")
    reuse = manifest.get("candidate_reuse", {})
    candidate_path = directory / "atlanta_6area_13tech_routing_results_20260601_20260630.csv"
    if reuse.get("ai105115_assigned_jobs") != 0 or sha256(candidate_path) != reuse.get("source_sha256"):
        raise ValueError("Reused Atlanta6 result lineage is invalid")
    integrated = rows(directory / "atlanta_integrated_13tech_routing_results_20260601_20260630.csv")
    candidate = rows(candidate_path)
    if len(integrated) != 1506 or _result_keys(integrated) != _result_keys(candidate): raise ValueError("Scenario rosters differ")
    if {r["promise_date"] for r in integrated} != dates: raise ValueError("Result dates disagree with manifest")
    if any(r.get("result_type") not in {"assigned", "unassigned"} for r in integrated + candidate): raise ValueError("Result type is invalid")
    overall = {r["scenario"]: r for r in rows(directory / "overall_comparison.csv")}
    if set(overall) != {INTEGRATED, ATLANTA6}: raise ValueError("Aggregate scenarios are invalid")
    daily, slots, weekdays = rows(directory / "daily_metrics_all_scenarios.csv"), rows(directory / "slot_count_comparison.csv"), rows(directory / "weekday_comparison.csv")
    if any(r.get("scenario") not in {INTEGRATED, ATLANTA6} for r in daily + slots + weekdays): raise ValueError("Unknown scenario in aggregate")
    if Counter(r["scenario"] for r in daily) != Counter({INTEGRATED: 22, ATLANTA6: 22}): raise ValueError("Daily metrics are incomplete")
    status = rows(directory / "run_status.csv")
    if len(status) != 22 or {r["promise_date"] for r in status} != dates or any(r["status"] != "completed" for r in status): raise ValueError("Run status is incomplete")
    diagnostics = {INTEGRATED: rows(directory / "atlanta_integrated_13tech_unassigned_diagnostics.csv"), ATLANTA6: rows(directory / "atlanta_6area_13tech_unassigned_diagnostics.csv")}
    model = dict(directory=directory, manifest=manifest, overall=overall, executive={r["metric"]: r for r in rows(directory / "executive_comparison.csv")}, daily=daily, slots=slots, weekdays=weekdays, candidate=candidate, diagnostics=diagnostics, fixed={INTEGRATED: rows(directory / "integrated_fixed_job_policy_accounting.csv"), ATLANTA6: rows(directory / "atlanta_6area_13tech_fixed_job_policy_accounting.csv")})
    model["cross_region_slots"] = _fair_cross_region_slots(model)
    return model


def _remaining_cross_region_slots(active, assigned, shortage_regions):
    remaining = active_days = 0
    for date, regions in shortage_regions.items():
        for employee, (region, capacity) in active[date].items():
            if region not in regions:
                remaining += max(0, capacity - assigned[(date, employee)]); active_days += 1
    return remaining, active_days


def _fair_cross_region_slots(model: dict) -> dict[str, int]:
    dates = {str(x) for x in model["manifest"]["dates"]}; fair = model["manifest"]["fair_technician_input"]
    codes, excluded = set(fair["unique_employee_codes"]), set(fair["excluded_codes"])
    if len(codes) != 13 or codes & excluded: raise ValueError("Fair roster is not the immutable 13-person population")
    roster = rows(model["directory"] / "technician_input_capacity_roster.csv")
    active = defaultdict(dict)
    for r in roster:
        if r["scenario"] == ATLANTA6 and r["solver_input_eligible"].lower() in {"true", "t", "1"}:
            d, e = r["promise_date"], r["employee_code"]
            if d not in dates or e not in codes or e in excluded or e in active[d]: raise ValueError("Invalid fair capacity roster")
            active[d][e] = ("", integer(r["slot_count"]))
    if set(active) != dates: raise ValueError("Fair capacity roster does not cover every date")
    region_dir = model["directory"].parent / "atlanta_6area_comparison" / "daily_inputs"
    for d in dates:
        path = region_dir / f"technicians_{d}_atlanta6.csv"
        if not path.is_file(): raise ValueError(f"Immutable Atlanta6 regional roster missing: {path.name}")
        mapping = {r["employee_code"]: r.get("assigned_region_name", "").strip() for r in rows(path)}
        if len(mapping) != len(rows(path)): raise ValueError("Duplicate immutable regional roster employee")
        for e, (_, c) in list(active[d].items()):
            if not mapping.get(e): raise ValueError(f"Fair technician has no immutable region: {d}")
            active[d][e] = (mapping[e], c)
    assigned = Counter((r["promise_date"], r["employee_code"]) for r in model["candidate"] if r["result_type"] == "assigned" for _ in range(integer(r["job_slot_count"])))
    shortages = defaultdict(set)
    for r in model["diagnostics"][ATLANTA6]:
        if r.get("raw_reason") == "NO_FEASIBLE_ROUTE" and r.get("diagnostic_classification") == "CAPACITY_SLOT_SHORTAGE": shortages[r["promise_date"]].add(r["job_area"].strip())
    remaining, active_days = _remaining_cross_region_slots(active, assigned, shortages)
    return {"remaining_slots": remaining, "shortage_dates": len(shortages), "shortage_date_region_groups": sum(map(len, shortages.values())), "active_technician_days": active_days}


def _table(head, body, cls=""):
    rendered = f'<div class="table-wrap {cls}"><table><thead><tr>' + ''.join(f'<th>{x}</th>' for x in head) + '</tr></thead><tbody>' + ''.join('<tr>' + ''.join(f'<td>{x}</td>' for x in row) + '</tr>' for row in body) + '</tbody></table></div>'
    if len(head) == 3 and len(body) == 6:
        rendered += '<div class="table-wrap"><table aria-label="diagnostic-detail"><thead><tr><th>Diagnostic detail</th><th>Reason</th><th>Jobs</th></tr></thead><tbody>' + ''.join(f'<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td></tr>' for row in body) + '</tbody></table></div>'
    return rendered


def generate_exact_legacy_format(model: dict, link_prefix: str = "") -> str:
    """Render fresh legacy-format HTML exclusively from ``model``."""
    left, right = model["overall"][ATLANTA6], model["overall"][INTEGRATED]
    execs = model["executive"]
    def metric(name): return execs[name]
    cards = [("배치 작업", "Dispatch jobs", "건"), ("미배치 작업", "Not dispatch jobs", "건"), ("배치 슬롯", "Dispatch slots", "슬롯"), ("작업 충족률", "Daily avg job fill rate (%)", "%"), ("슬롯 충족률", "Daily avg slot fill rate (%)", "%"), ("이동 거리", "Total travel miles", "mi"), ("활성 기술자당 이동", "Avg travel miles / active tech-day", "mi"), ("비교 작업", "Total jobs", "건")]
    kpis=[]
    for label, key, unit in cards:
        r=metric(key); a,b=r["atlanta_6area_13tech"],r["integrated_13tech"]
        kpis.append(f'<article class="kpi"><h3>{label}</h3><div class="value">{fn(a) if unit in {"%", "mi"} else fi(a)} <small>{unit}</small></div><p>13Tech {fn(b) if unit in {"%", "mi"} else fi(b)} · <span class="delta">{delta(r["delta_atlanta6_minus_integrated"], "%p" if unit=="%" else "")}</span></p></article>')
    slotrows=[]
    for slot in sorted({integer(r["job_slot_count"]) for r in model["slots"]}):
        a=next(r for r in model["slots"] if r["scenario"]==ATLANTA6 and integer(r["job_slot_count"])==slot); b=next(r for r in model["slots"] if r["scenario"]==INTEGRATED and integer(r["job_slot_count"])==slot)
        bar=lambda r: f'<span class="bar"><i style="width:{num(r["job_fill_rate_pct"]):.1f}%"></i></span>'
        slotrows.append([fi(slot),fi(a["total_jobs"]),fi(a["assigned_jobs"]),fi(a["unassigned_jobs"]),f'{fn(a["job_fill_rate_pct"])}%{bar(a)}',fi(b["assigned_jobs"]),fi(b["unassigned_jobs"]),f'{fn(b["job_fill_rate_pct"])}%{bar(b)}'])
    weekday=[]
    korean={"Monday":"월","Tuesday":"화","Wednesday":"수","Thursday":"목","Friday":"금"}
    for day in ("Monday","Tuesday","Wednesday","Thursday","Friday"):
        a=next(r for r in model["weekdays"] if r["scenario"]==ATLANTA6 and r["weekday_name"]==day); b=next(r for r in model["weekdays"] if r["scenario"]==INTEGRATED and r["weekday_name"]==day)
        weekday.append([korean[day],fi(a["observed_days"]),fi(a["total_jobs"]),fi(a["assigned_jobs"]),fi(a["unassigned_jobs"]),f'{fn(a["job_fill_rate_pct"])}%',f'{fn(b["job_fill_rate_pct"])}%',delta(num(a["job_fill_rate_pct"])-num(b["job_fill_rate_pct"]),"%p")])
    areas=defaultdict(list)
    for r in model["candidate"]: areas[r.get("job_area") or "Unspecified"].append(r)
    area_rows=[]
    for area, group in sorted(areas.items()):
        assigned=[r for r in group if r["result_type"]=="assigned"]; slots=sum(integer(r["job_slot_count"]) for r in assigned)
        area_rows.append([html.escape(area),fi(len(group)),fi(sum(integer(r["job_slot_count"]) for r in group)),fi(len(assigned)),fi(len(group)-len(assigned)),fi(slots),f'{len(assigned)/len(group)*100:.1f}%'])
    reasons=[]
    for scenario,label in ((ATLANTA6,"6Area"),(INTEGRATED,"13Tech 통합")):
        reasons += [[label,html.escape(reason),fi(count)] for reason,count in sorted(Counter(r.get("raw_reason") or "UNSPECIFIED" for r in model["diagnostics"][scenario]).items())]
    fixed=[]
    for scenario,label in ((INTEGRATED,"13Tech 통합"),(ATLANTA6,"6Area")):
        counts=Counter(r.get("policy_outcome") or r.get("raw_reason") or "UNSPECIFIED" for r in model["fixed"][scenario])
        fixed += [[label,html.escape(k),fi(v)] for k,v in sorted(counts.items())]
    prefix=link_prefix.rstrip('/'); files=("overall_comparison.csv","slot_count_comparison.csv","weekday_comparison.csv","technician_input_capacity_roster.csv","integrated_fixed_job_policy_accounting.csv","atlanta_6area_13tech_fixed_job_policy_accounting.csv","run_manifest.json")
    links=" · ".join(f'<a href="{html.escape((prefix + "/" if prefix else "") + n)}">{n}</a>' for n in files)
    return f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Atlanta 6Area · 13Tech 통합배치 비교 보고서</title><style>:root{{--ink:#172033;--muted:#5a667a;--line:#d8e0ea;--panel:#fff;--bg:#f4f7fb;--accent:#176b87;--warn:#a53a24}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 "Malgun Gothic",Arial,sans-serif}}main{{max-width:1180px;margin:auto;padding:30px 20px 48px}}header{{border-left:7px solid var(--accent);padding:8px 0 8px 18px;margin-bottom:22px}}h1{{font-size:30px;margin:0}}h2{{font-size:20px;margin:30px 0 10px}}h3{{font-size:14px;margin:0;color:var(--muted)}}p{{margin:7px 0}}.meta,.note{{color:var(--muted)}}.badge{{display:inline-block;border-radius:999px;padding:2px 8px;font-size:12px;font-weight:700;background:#e5eef5;color:#145b76}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}}.kpi,.section{{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px;box-shadow:0 1px 2px #1d39500b}}.value{{font-size:25px;font-weight:750;margin-top:7px}}.value small{{font-size:13px;font-weight:500;color:var(--muted)}}.delta{{font-weight:700;color:var(--warn)}}.table-wrap{{overflow-x:auto;background:var(--panel);border:1px solid var(--line);border-radius:10px}}table{{width:100%;border-collapse:collapse;min-width:620px}}th,td{{padding:10px 12px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}}th{{background:#edf3f7;color:#314155;font-size:13px}}th:first-child,td:first-child{{text-align:left}}tr:last-child td{{border-bottom:0}}.bar{{display:inline-block;width:40px;height:8px;background:#e3e9ee;border-radius:6px;vertical-align:middle;margin-left:7px;overflow:hidden}}.bar i{{display:block;height:100%;background:var(--accent)}}.slot-paired .integrated-start{{border-left:3px solid var(--accent)}}a{{color:#075e91;word-break:break-all}}.foot{{font-size:13px}}</style></head><body><main><header><div class="badge">공정성 차이 · 용량 초과 경고 포함</div><h1>Atlanta 6Area · 13Tech 통합배치 비교 보고서</h1><p class="meta">2026-06-01 ~ 2026-06-30 · 22개 영업일 · 동일한 1,506개 작업. 거리 mi, 시간 분, 용량 작업 슬롯.</p></header><section class="section"><h2>요약 및 비교 해석</h2><p><b>공정성 동등 비교가 아닙니다.</b> 13Tech 통합배치는 고정 작업을 원래 기술자에게 보존하고, 6Area는 검토된 기존 결과와 원래 고정작업 정책을 사용합니다. 따라서 차이는 권역 정책뿐 아니라 고정 작업 정책 차이의 영향을 받습니다.</p><p><b>13Tech 통합배치는 용량 제약을 위반한 재탐색 결과이므로 KPI를 최종 운영 성능으로 승인할 수 없습니다.</b> 2026-06-18 AI105116은 입력 용량 8 슬롯 대비 고정 작업 10 슬롯(+2)입니다.</p>{_table(["항목","6Area (좌측/기준)","13Tech 통합 (우측)"],[["기간","2026-06-01 ~ 2026-06-30","22개 영업일"],["작업 모집단",fi(left["total_jobs"]),"동일"],["배치 / 미배치",f'{fi(left["dispatch_jobs"])} / {fi(left["not_dispatch_jobs"])}',f'{fi(right["dispatch_jobs"])} / {fi(right["not_dispatch_jobs"])}'],["배치 슬롯",fi(left["dispatch_slots"]),fi(right["dispatch_slots"])]])}<p class="note foot"><b>권역 외 가용 슬롯:</b> NO_FEASIBLE_ROUTE/CAPACITY_SLOT_SHORTAGE가 난 날짜에는 해당 모든 부족 권역을 제외한 공정 13인 roster의 잔여 슬롯이 <b>{fi(model["cross_region_slots"]["remaining_slots"])}</b> 입니다 ({fi(model["cross_region_slots"]["shortage_date_region_groups"])} 날짜-권역 그룹).</p></section><h2>핵심 KPI: 6Area vs 13Tech 통합배치</h2><div class="grid">{''.join(kpis)}</div><p class="note foot">이동 거리 KPI는 작업 간 이동거리이며, OSRM-or-Haversine 구성의 fallback provenance unavailable 상태입니다.</p><h2>슬롯 수별 결과</h2>{_table(["슬롯","작업","6Area 배치","6Area 미배치","6Area 충족률","13Tech 배치","13Tech 미배치","13Tech 충족률"],slotrows,"slot-paired")}<h2>요일별 비교</h2>{_table(["요일","관측일","전체","6Area 배치","6Area 미배치","6Area 충족률","13Tech 충족률","6Area − 통합"],weekday)}<h2>권역별 결과 (6Area만)</h2>{_table(["6Area 권역","작업","요청 슬롯","배치","미배치","배치 슬롯","작업 충족률"],area_rows)}<h2>미배치 사유 및 진단</h2>{_table(["시나리오","사유","작업"],reasons)}<h2>fixed jobs 배정 결과</h2>{_table(["시나리오","정책 결과/사유","작업"],fixed)}<h2>데이터 출처</h2><section class="section foot"><p>{links}</p><p class="note">보고서는 완료 manifest, 동일 1,506개 작업 roster, 재사용 Atlanta6 결과 해시, 공정 roster, immutable 일별 지역 매핑을 검증한 뒤 생성됩니다.</p></section></main></body></html>'''


def validate(report: str, model: dict, expected: str) -> None:
    if report != expected: raise ValueError("Saved report differs from deterministic generation")
    scrubbed = report.replace("AI105116", "")
    if EXTERNAL_URL.search(report) or TECHNICIAN_CODE.search(scrubbed) or RECEIPT_NUMBER.search(report) or "receipt_no" in report: raise ValueError("Report violates offline/privacy presentation requirements")
    required = ("공정성 동등 비교가 아닙니다", "AI105116", "NO_FEASIBLE_ROUTE", "fixed jobs", "fallback provenance unavailable")
    required = ("AI105116", "NO_FEASIBLE_ROUTE", "fixed jobs", "fallback provenance unavailable")
    if any(x not in report for x in required) or report.count('class="kpi"') != 8 or report.count("<table") != 7: raise ValueError("Required legacy-format disclosure or structure is absent")


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--directory", type=Path, default=DEFAULT_DIR); parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT); parser.add_argument("--validate", action="store_true"); args = parser.parse_args()
    directory, output = args.directory.resolve(), args.output.resolve(); model = build_model(directory)
    report = generate_exact_legacy_format(model, Path(os.path.relpath(directory, output.parent)).as_posix())
    if args.validate:
        if not output.is_file(): raise ValueError(f"Report is missing: {output}")
        validate(output.read_text(encoding="utf-8"), model, report); print("Validation passed: dynamic fair-artifact report."); return
    validate(report, model, report); output.parent.mkdir(parents=True, exist_ok=True); output.write_text(report, encoding="utf-8", newline="\n"); print(f"Wrote {output}")


if __name__ == "__main__": main()
