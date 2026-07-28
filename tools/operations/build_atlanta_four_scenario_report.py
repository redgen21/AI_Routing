"""Build the Korean Atlanta four-scenario routing comparison report."""
from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIRECTORY = ROOT / "260310" / "atlanta 2606_test" / "atlanta_four_scenario_comparison"
DEFAULT_OUTPUT = ROOT / "보고서" / "atlanta_four_scenario_efficiency_report_ko.html"
SCENARIOS = (
    "Integrated_13tech",
    "Atlanta_3area",
    "Atlanta_6area_new",
    "Atlanta_6area_overlab",
)
DISPLAY_NAMES = {
    "Integrated_13tech": "13인 통합배치",
    "Atlanta_3area": "3개 지역 배치",
    "Atlanta_6area_new": "6개 지역 배치(신규)",
    "Atlanta_6area_overlab": "6개 지역 배치(중복)",
}
SHORT_NAMES = {
    "Integrated_13tech": "통합 13인",
    "Atlanta_3area": "3지역",
    "Atlanta_6area_new": "6지역 신규",
    "Atlanta_6area_overlab": "6지역 중복",
}
REQUIRED_FILES = (
    "run_manifest.json",
    "overall_comparison.csv",
    "daily_metrics_all_scenarios.csv",
    "slot_count_result_type_comparison.csv",
    "weekday_comparison.csv",
    "region_comparison.csv",
    "unassigned_reason_diagnostics.csv",
    "policy_relaxation_comparison.csv",
    "regional_slot_shortage_comparison.csv",
    "run_status.csv",
)


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def number(value: object, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric: {value!r}") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field} must be finite: {value!r}")
    return result


def integer(value: object, field: str) -> int:
    result = number(value, field)
    rounded = round(result)
    if abs(result - rounded) > 1e-8:
        raise ValueError(f"{field} must be an integer: {value!r}")
    return rounded


def fi(value: object) -> str:
    return f"{integer(value, 'display'):,}"


def fn(value: object, places: int = 1) -> str:
    return f"{number(value, 'display'):,.{places}f}"


def pct(value: object) -> str:
    return f"{number(value, 'display'):.1f}%"


def load_model(directory: Path) -> dict:
    directory = directory.resolve()
    missing = [name for name in REQUIRED_FILES if not (directory / name).is_file()]
    if missing:
        raise ValueError("완료된 비교 산출물이 아닙니다. 누락: " + ", ".join(missing))

    manifest = json.loads((directory / "run_manifest.json").read_text(encoding="utf-8-sig"))
    if manifest.get("status") != "completed" or manifest.get("completed") is not True:
        raise ValueError("run_manifest.json이 완료 상태가 아닙니다")
    if set(manifest.get("scenarios", [])) != set(SCENARIOS):
        raise ValueError("승인된 네 시나리오가 모두 포함되지 않았습니다")
    dates = manifest.get("dates", [])
    if len(dates) != 22 or len(set(dates)) != 22:
        raise ValueError("22개 영업일이 모두 포함되어야 합니다")

    overall_rows = rows(directory / "overall_comparison.csv")
    overall = {row["scenario"]: {k: number(v, f"{row['scenario']}/{k}") for k, v in row.items() if k != "scenario"} for row in overall_rows}
    if set(overall) != set(SCENARIOS):
        raise ValueError("overall_comparison.csv 시나리오 구성이 잘못되었습니다")
    for scenario, item in overall.items():
        if integer(item["total_jobs"], scenario) != 1506:
            raise ValueError(f"{scenario}: 공통 1,506건 모집단이 아닙니다")
        if integer(item["dispatch_jobs"], scenario) + integer(item["not_dispatch_jobs"], scenario) != 1506:
            raise ValueError(f"{scenario}: 배정/미배정 건수 합계가 맞지 않습니다")
        if integer(item["dispatch_slots"], scenario) + integer(item["not_dispatch_slots"], scenario) != 1998:
            raise ValueError(f"{scenario}: 배정/미배정 슬롯 합계가 맞지 않습니다")

    status_rows = rows(directory / "run_status.csv")
    for scenario in SCENARIOS:
        completed = {r["promise_date"] for r in status_rows if r["scenario"] == scenario and r["status"] == "completed"}
        if len(completed) != 22:
            raise ValueError(f"{scenario}: 완료된 날짜가 22일이 아닙니다")

    daily = rows(directory / "daily_metrics_all_scenarios.csv")
    daily_by_scenario = defaultdict(list)
    for row in daily:
        daily_by_scenario[row["scenario"]].append(row)
    capacity = {}
    for scenario in SCENARIOS:
        scenario_rows = daily_by_scenario[scenario]
        if len(scenario_rows) != 22:
            raise ValueError(f"{scenario}: 일별 KPI가 22일이 아닙니다")
        capacity[scenario] = {
            "technician_days": sum(integer(r["input_available_technicians"], "technicians") for r in scenario_rows),
            "available_slots": sum(integer(r["input_available_slots"], "slots") for r in scenario_rows),
        }

    slot_rows = rows(directory / "slot_count_result_type_comparison.csv")
    slots = defaultdict(lambda: defaultdict(dict))
    for row in slot_rows:
        slots[row["scenario"]][integer(row["job_slot_count"], "job_slot_count")][row["result_type"]] = {
            "jobs": integer(row["jobs"], "jobs"), "slots": integer(row["slots"], "slots")
        }
    weekdays = rows(directory / "weekday_comparison.csv")
    regions = rows(directory / "region_comparison.csv")
    reason_rows = rows(directory / "unassigned_reason_diagnostics.csv")
    reasons = defaultdict(list)
    for row in reason_rows:
        reasons[row["scenario"]].append(row)
    for scenario in SCENARIOS:
        if sum(integer(r["unassigned_jobs"], "unassigned jobs") for r in reasons[scenario]) != integer(overall[scenario]["not_dispatch_jobs"], scenario):
            raise ValueError(f"{scenario}: 미배정 사유 합계가 KPI와 다릅니다")

    policy_rows = rows(directory / "policy_relaxation_comparison.csv")
    policies = {row["scenario"]: row for row in policy_rows}
    if set(policies) != set(SCENARIOS):
        raise ValueError("고정 작업 완화 정책 집계에 네 시나리오가 모두 필요합니다")
    shortage_rows = rows(directory / "regional_slot_shortage_comparison.csv")
    slot_shortages = {row["scenario"]: row for row in shortage_rows}
    if set(slot_shortages) != set(SCENARIOS[1:]):
        raise ValueError("지역 슬롯 부족 집계에는 세 지역 시나리오가 필요합니다")
    for scenario, item in slot_shortages.items():
        if integer(item["slot_shortage_jobs"], scenario) > integer(item["no_feasible_route_jobs"], scenario):
            raise ValueError(f"{scenario}: 슬롯 부족 작업이 NO_FEASIBLE_ROUTE보다 많습니다")

    return {
        "directory": directory,
        "manifest": manifest,
        "overall": overall,
        "capacity": capacity,
        "slots": slots,
        "weekdays": weekdays,
        "regions": regions,
        "reasons": reasons,
        "policies": policies,
        "slot_shortages": slot_shortages,
    }


def table(headers: list[str], body: list[list[str]], css_class: str = "") -> str:
    return '<div class="table-wrap"><table class="' + css_class + '"><thead><tr>' + "".join(
        f"<th>{html.escape(cell)}</th>" for cell in headers
    ) + "</tr></thead><tbody>" + "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in body
    ) + "</tbody></table></div>"


def bar(value: float) -> str:
    width = max(0.0, min(100.0, value))
    return f'{value:.1f}% <span class="bar"><i style="width:{width:.1f}%"></i></span>'


def render(model: dict, source_prefix: str) -> str:
    overall = model["overall"]
    integrated = overall["Integrated_13tech"]
    overlap = overall["Atlanta_6area_overlab"]
    new6 = overall["Atlanta_6area_new"]
    three = overall["Atlanta_3area"]
    manual_override_note = ""
    if "db_override_provenance.json" in model["manifest"].get("plan_evidence", {}).get("Atlanta_3area", {}):
        manual_override_note = (
            '<p class="note"><b>Atlanta_3area 입력 변경:</b> 기사 1명의 활성 DB 배정이 ATL West(region 3)에서 '
            'ATL East(region 1)로 직접 수정된 상태를 반영했습니다. 현재 workbook에서 재생성한 East 5 / South 3 / West 5 '
            '스냅샷과 별도 DB override provenance를 사용했습니다. 활성 plan의 저장 checksum은 수동 DB 변경 전 값을 유지하므로, '
            '정식 운영 전 새 immutable plan으로 교체해야 합니다.</p>'
        )

    overview = (
        f"동일한 13명, 22개 영업일, 1,506건을 비교했을 때 <b>종합 운영 효율은 13인 통합배치가 가장 높습니다.</b> "
        f"배정 {fi(integrated['dispatch_jobs'])}건, 미배정 {fi(integrated['not_dispatch_jobs'])}건으로 서비스 충족과 기사일당 배정 건수에서 1위입니다. "
        f"지역 책임제를 유지해야 한다면 <b>6개 지역 중복배치가 최선의 절충안</b>입니다. 신규 6지역보다 "
        f"{fi(overlap['dispatch_jobs'] - new6['dispatch_jobs'])}건을 더 배정하고, 통합배치와의 차이를 "
        f"{fi(integrated['dispatch_jobs'] - overlap['dispatch_jobs'])}건까지 줄였습니다. "
        f"순수 이동 효율은 <b>6개 지역 신규배치</b>가 배정 건당 {fn(new6['miles_per_assigned_job'], 2)}mi로 가장 좋지만 "
        f"미배정이 {fi(new6['not_dispatch_jobs'])}건이므로 종합 최적안은 아닙니다. "
        f"3개 지역은 신규 6지역보다 {fi(three['dispatch_jobs'] - new6['dispatch_jobs'])}건을 더 배정하지만 "
        f"총 이동거리는 {fn(three['total_travel_miles'] - new6['total_travel_miles'])}mi 더 깁니다. "
        f"중복 6지역과 비교하면 배정은 {fi(overlap['dispatch_jobs'] - three['dispatch_jobs'])}건 적고 총 이동거리는 "
        f"{fn(overlap['total_travel_miles'] - three['total_travel_miles'])}mi 짧아 서비스 충족과 거리 사이의 중간안입니다."
    )

    common_rows = [
        ["기간", "2026-06-01 ~ 2026-06-30"],
        ["완료 영업일", "22일"],
        ["총 작업", "1,506건"],
        ["총 요청 슬롯", "1,998슬롯 (1슬롯 1,032 / 2슬롯 914 / 3슬롯 48 / 4슬롯 4)"],
        ["투입 인원", "13명 (각 방식 동일 입력)"],
        ["투입 가능 기사일 / 총 가용 슬롯", "244 기사일 / 1,862슬롯 (각 방식 동일)"],
        ["지역 정책", "통합: 경계 없음 / 3지역: overflow 없음 / 신규 6지역: 4 ZIP overflow / 중복 6지역: 101 ZIP overflow"],
    ]

    metrics = [
        ("배정 작업", "dispatch_jobs", fi),
        ("미배정 작업", "not_dispatch_jobs", fi),
        ("배정 슬롯", "dispatch_slots", fi),
        ("미배정 슬롯", "not_dispatch_slots", fi),
        ("작업 충족률", "job_fill_rate_pct", pct),
        ("슬롯 충족률", "slot_fill_rate_pct", pct),
        ("기사일당 배정 작업", "jobs_per_technician", lambda v: fn(v, 2)),
        ("기사일당 배정 슬롯", "slots_per_technician", lambda v: fn(v, 2)),
        ("총 이동거리 (mi)", "total_travel_miles", fn),
        ("배정 건당 이동거리 (mi)", "miles_per_assigned_job", lambda v: fn(v, 2)),
        ("기사일당 이동거리 (mi)", None, lambda v: fn(v, 1)),
        ("총 이동시간 (분)", "total_travel_minutes", fn),
    ]
    metric_body = []
    for label, key, formatter in metrics:
        values = []
        for scenario in SCENARIOS:
            value = overall[scenario][key] if key else overall[scenario]["total_travel_miles"] / model["capacity"][scenario]["technician_days"]
            values.append(formatter(value))
        metric_body.append([label, *values])

    slot_body = []
    for slot_count in range(1, 5):
        row = [f"{slot_count}슬롯"]
        for scenario in SCENARIOS:
            assigned = model["slots"][scenario][slot_count].get("assigned", {"jobs": 0, "slots": 0})
            unassigned = model["slots"][scenario][slot_count].get("unassigned", {"jobs": 0, "slots": 0})
            total = assigned["jobs"] + unassigned["jobs"]
            fill = assigned["jobs"] / total * 100 if total else 0
            row.append(f"{assigned['jobs']:,} / {unassigned['jobs']:,}<br><small>{assigned['slots']:,} / {unassigned['slots']:,}슬롯</small><br>{bar(fill)}")
        slot_body.append(row)

    weekday_order = {"Monday": "월", "Tuesday": "화", "Wednesday": "수", "Thursday": "목", "Friday": "금"}
    weekday_index = {(r["weekday_name"], r["scenario"]): r for r in model["weekdays"]}
    weekday_body = []
    for english, korean in weekday_order.items():
        sample = weekday_index[(english, SCENARIOS[0])]
        row = [korean, fi(sample["observed_days"]), fi(sample["total_jobs"])]
        for scenario in SCENARIOS:
            item = weekday_index[(english, scenario)]
            row.append(f"{fi(item['assigned_jobs'])} / {fi(item['unassigned_jobs'])}<br><small>{pct(item['job_fill_rate_pct'])} · {fn(item['total_travel_miles'])}mi</small>")
        weekday_body.append(row)

    region_body = []
    for item in model["regions"]:
        scenario = item["scenario"]
        jobs = integer(item["jobs"], "region jobs")
        assigned = integer(item["assigned_jobs"], "region assigned")
        requested_slots = integer(item["requested_slots"], "region slots")
        assigned_slots = integer(item["assigned_slots"], "region assigned slots")
        region_body.append([
            DISPLAY_NAMES[scenario], html.escape(item["region"]), fi(jobs), fi(assigned), fi(item["unassigned_jobs"]),
            pct(assigned / jobs * 100 if jobs else 0), pct(assigned_slots / requested_slots * 100 if requested_slots else 0),
        ])

    reason_labels = {
        "FIXED_TECHNICIAN_NOT_AVAILABLE": "고정 기사 미가용",
        "NO_ELIGIBLE_TECHNICIAN": "적격 기사 없음",
        "NO_FEASIBLE_MANDATORY_ROUTE": "우선배정 경로 불가",
        "NO_FEASIBLE_ROUTE": "용량·시간·이동 제약으로 경로 불가",
        "POSTAL_NOT_IN_ACTIVE_PLAN": "활성 계획 외 우편번호",
    }
    reason_body = []
    for scenario in SCENARIOS:
        for item in model["reasons"][scenario]:
            raw = item["raw_reason"]
            reason_body.append([
                DISPLAY_NAMES[scenario], reason_labels.get(raw, html.escape(raw)), fi(item["unassigned_jobs"]), fi(item["unassigned_slots"])
            ])

    policy_body = []
    for scenario in SCENARIOS:
        item = model["policies"][scenario]
        policy_body.append([
            DISPLAY_NAMES[scenario], html.escape(item["policy_version"]), fi(item["relaxed_jobs"]),
            fi(item["reassigned_to_other_technician_jobs"]), fi(item["relaxed_unassigned_jobs"]),
        ])

    shortage_body = []
    for scenario in SCENARIOS[1:]:
        item = model["slot_shortages"][scenario]
        shortage_body.append([
            DISPLAY_NAMES[scenario],
            f"{fi(item['no_feasible_route_jobs'])} / {fi(item['no_feasible_route_slots'])}",
            f"{fi(item['slot_shortage_jobs'])} / {fi(item['slot_shortage_slots'])}",
            f"{fi(item['non_slot_shortage_jobs'])} / {fi(item['non_slot_shortage_slots'])}",
            fi(item["shortage_dates"]), fi(item["shortage_date_region_groups"]),
            fi(item["other_region_remaining_slots"]),
        ])

    links = " · ".join(
        f'<a href="{html.escape(source_prefix + "/" + name)}">{html.escape(name)}</a>'
        for name in REQUIRED_FILES
    )
    return f'''<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Atlanta 4개 배치 방식 효율 비교 보고서</title>
<style>
:root{{--ink:#172033;--muted:#5a667a;--line:#d8e0ea;--panel:#fff;--bg:#f4f7fb;--accent:#176b87;--warn:#a53a24;--good:#17633f}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 "Malgun Gothic","Apple SD Gothic Neo",Arial,sans-serif}}main{{max-width:1280px;margin:auto;padding:30px 20px 48px}}header{{border-left:7px solid var(--accent);padding:8px 0 8px 18px;margin-bottom:22px}}h1{{font-size:30px;margin:0}}h2{{font-size:20px;margin:30px 0 10px}}p{{margin:7px 0}}.meta,.note,small{{color:var(--muted)}}.badge{{display:inline-block;border-radius:999px;padding:2px 8px;font-size:12px;font-weight:700;background:#e5eef5;color:#145b76}}.section{{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px;box-shadow:0 1px 2px #1d39500b}}.conclusion{{border-left:5px solid var(--good)}}.table-wrap{{overflow-x:auto;background:var(--panel);border:1px solid var(--line);border-radius:10px}}table{{width:100%;border-collapse:collapse;min-width:860px}}th,td{{padding:10px 12px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}}th{{background:#edf3f7;color:#314155;font-size:13px}}th:first-child,td:first-child{{text-align:left}}tr:last-child td{{border-bottom:0}}.bar{{display:inline-block;width:42px;height:8px;background:#e3e9ee;border-radius:6px;vertical-align:middle;margin-left:6px;overflow:hidden}}.bar i{{display:block;height:100%;background:var(--accent)}}a{{color:#075e91;word-break:break-all}}.foot{{font-size:13px}}@media(max-width:620px){{main{{padding:20px 12px}}h1{{font-size:24px}}}}@media print{{body{{background:#fff;font-size:10px}}main{{max-width:none;padding:0}}.section,.table-wrap{{box-shadow:none;break-inside:avoid}}a{{color:inherit;text-decoration:none}}}}
</style></head><body><main>
<header><div class="badge">동일 입력 · 4개 정책 · 22일 완료</div><h1>Atlanta 4개 배치 방식 효율 비교 보고서</h1><p class="meta">기간: 2026-06-01 ~ 2026-06-30 · 완료 영업일 22일 · 총 1,506건 · 비교: 13인 통합 / 3지역 / 신규 6지역 / 중복 6지역</p><p class="meta">기사·고객 식별정보는 포함하지 않습니다. 거리 단위는 mile, 시간 단위는 minute입니다.</p></header>
<section class="section conclusion"><h2>개요 및 권고</h2><p>{overview}</p><p class="note">권고: 서비스 충족을 최우선으로 하면 13인 통합배치, 지역 책임과 충족률을 함께 고려하면 6개 지역 중복배치, 이동거리 최소화가 우선이면 신규 6개 지역 배치를 선택하는 것이 타당합니다.</p></section>
<h2>비교 조건</h2>{table(["항목", "공통 조건"], common_rows)}
<h2>전체 KPI 비교</h2>{table(["지표", *(DISPLAY_NAMES[s] for s in SCENARIOS)], metric_body)}
<p class="note foot">이동거리는 OSRM driving 경로의 작업 간 이동 합계이며 집→첫 작업과 마지막 작업→집은 제외합니다. 따라서 배정 건수가 적을수록 총거리가 낮아질 수 있어 배정 건당 거리도 함께 비교해야 합니다.</p>
<h2>슬롯 수별 결과</h2>{table(["요청 슬롯", *(DISPLAY_NAMES[s] + " (배정/미배정 작업)" for s in SCENARIOS)], slot_body)}
<h2>요일별 비교</h2>{table(["요일", "관측일", "전체 작업", *(SHORT_NAMES[s] + " 배정/미배정" for s in SCENARIOS)], weekday_body)}
<h2>지역별 비교</h2>{table(["방식", "지역", "총 작업", "배정", "미배정", "작업 충족률", "슬롯 충족률"], region_body)}
<p class="note foot">통합배치는 지역 경계가 없으므로 지역별 표에서 제외했습니다. OUTSIDE_ACTIVE_PLAN 작업은 전체 통계에는 포함됩니다. 두 6지역 계획의 Zone 6은 PO Box 56 ZIP으로 구성되고 담당 기사가 0명입니다. 이번 1,506건에는 해당 지역 작업이 없어 손실은 없었으며 지역별 표에도 나타나지 않습니다. 향후 해당 ZIP에 수요가 생기면 별도 담당 또는 overflow 정책 없이는 배정할 수 없습니다.</p>
<h2>미배정 사유</h2>{table(["방식", "사유", "작업", "슬롯"], reason_body)}
<h2>NO_FEASIBLE_ROUTE 상세: 슬롯 부족과 다른 제약</h2><section class="section"><p><code>NO_FEASIBLE_ROUTE</code>는 <b>용량·근무시간·시간창·이동시간/거리·경로 순서 등의 제약 때문에 배정 가능한 경로를 만들지 못했다</b>는 상위 미배정 사유입니다.</p><p><b>슬롯 부족 확인</b>: 해당 작업을 처리할 수 있는 후보 기사들의 남은 슬롯을 모두 합해도 작업의 요청 슬롯보다 적습니다. 슬롯 부족은 확인됐지만 시간·이동 등의 다른 제약도 함께 존재할 수 있습니다.</p><p><b>슬롯은 남아 있으나 다른 제약으로 미배정</b>: 후보 기사에게 요청량 이상의 슬롯은 남아 있지만 배정되지 않았습니다. 근무시간, 시간창, 이동시간/거리, 최대 작업 수 또는 경로 순서 등이 원인일 수 있으며, 현재 결과만으로 어느 한 제약인지 확정할 수 없습니다.</p></section>{table(["방식", "전체 NO_FEASIBLE_ROUTE 작업/슬롯", "슬롯 부족 확인 작업/슬롯", "슬롯은 남아 있으나 다른 제약으로 미배정 작업/슬롯", "슬롯 부족 발생일", "날짜-지역 그룹", "타 지역 잔여 슬롯"], shortage_body)}
<p class="note foot">타 지역 잔여 슬롯은 날짜별로 슬롯 부족이 발생한 모든 작업 지역을 제외한 뒤, 다른 지역의 출근 기사에 대해 <code>입력 slot_count − 실제 배정 슬롯</code>의 양수만 한 번 합산했습니다. 이 숫자는 다른 지역에 남은 명목상 용량이며, Product 자격·시간창·이동시간·고정작업·승인된 overflow 조건까지 만족하여 실제 재배정할 수 있다는 뜻은 아닙니다. 통합배치는 지역 경계가 없어 표에서 제외했습니다.</p>
<h2>고정 작업 정책 완화</h2>{table(["방식", "정책 버전", "지역 밖 고정 완화", "다른 기사 배정", "완화 후 미배정"], policy_body)}
<p class="note foot">지역 분할 3개 방식은 사용자가 승인한 정책에 따라 고정 기사가 해당 작업 지역의 적격 후보가 아니면 원 기사 고정을 하드 제약으로 유지하지 않고, reschedule과 같은 우선순위로 해당 지역의 다른 적격 기사에게 배정할 수 있습니다. 통합배치는 이 지역 밖 완화를 사용하지 않습니다. 따라서 두 정책군의 고정 작업 조건은 완전히 동일하지 않습니다.</p>
<h2>실행 및 산출물</h2><section class="section">{manual_override_note}<p>Solver: OR-Tools na_general · 일별 제한 10초 · 목적함수: 총 이동시간 최소화. OSRM Table은 longitude,latitude 순서, source→destination 방향, 원본 metre/second를 km/min으로 정규화했습니다.</p><p>이번 실행은 OSRM 실패 시 대체 직선거리로 진행하지 않는 fail-closed 정책을 사용했습니다. 88개 날짜-시나리오의 총 {fi(model['manifest']['matrix']['matrix_request_count'])}회 matrix 요청은 모두 <code>osrm_primary</code>였고 fallback과 실패는 각각 0회입니다.</p><p>{links}</p><p class="note foot">OSRM map build version은 서버 응답에서 제공되지 않아 기록하지 못했습니다. 정책 효과와 solver의 일별 10초 제한 영향이 함께 포함된 시뮬레이션 결과이므로 실제 운영 전 소규모 병행 검증이 필요합니다.</p></section>
</main></body></html>'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    model = load_model(args.directory)
    output = args.output.resolve()
    prefix = Path(os.path.relpath(model["directory"], output.parent)).as_posix()
    expected = render(model, prefix)
    if args.validate:
        if not output.is_file() or output.read_text(encoding="utf-8") != expected:
            raise ValueError("보고서가 없거나 검증된 산출물과 일치하지 않습니다")
        print("Validation passed: four-scenario report")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(expected, encoding="utf-8", newline="\n")
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
