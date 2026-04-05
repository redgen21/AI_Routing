from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

import smart_routing.production_assign_atlanta as base
import smart_routing.production_assign_atlanta_csi as csi
from smart_routing.production_assign_atlanta_hybrid import build_atlanta_production_assignment_hybrid_from_frames
from smart_routing.production_assign_atlanta_sits import build_atlanta_production_assignment_sits_from_frames
from smart_routing.production_assign_atlanta_vrp import build_atlanta_production_assignment_vrp_from_frames


DEFAULT_DATE_FROM = "2026-01-01"
DEFAULT_DATE_TO = "2026-01-12"


def _dedupe_service(service_df: pd.DataFrame) -> pd.DataFrame:
    deduped = csi._prepare_service_df(service_df)
    sort_cols = [col for col in ["service_date_key", "GSFS_RECEIPT_NO", "service_time_min"] if col in deduped.columns]
    ascending = [True, True, False][: len(sort_cols)]
    deduped = deduped.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)
    if {"service_date_key", "GSFS_RECEIPT_NO"}.issubset(deduped.columns):
        deduped = deduped.drop_duplicates(subset=["service_date_key", "GSFS_RECEIPT_NO"], keep="first").reset_index(drop=True)
    return deduped


def _metrics(summary_df: pd.DataFrame) -> dict[str, float]:
    if summary_df.empty:
        return {
            "engineers": 0.0,
            "travel_km": 0.0,
            "work_std": 0.0,
            "max_work": 0.0,
            "overflow_480": 0.0,
        }
    work_series = pd.to_numeric(summary_df["total_work_min"], errors="coerce").fillna(0.0)
    return {
        "engineers": float(summary_df["SVC_ENGINEER_CODE"].nunique()),
        "travel_km": float(pd.to_numeric(summary_df["travel_distance_km"], errors="coerce").fillna(0.0).sum()),
        "work_std": float(work_series.std(ddof=0)),
        "max_work": float(work_series.max()),
        "overflow_480": float(pd.to_numeric(summary_df["overflow_480"], errors="coerce").fillna(0.0).astype(int).sum()),
    }


def _gap_pct(actual: float, baseline: float) -> float:
    if baseline == 0:
        return 0.0
    return ((actual - baseline) / baseline) * 100.0


def _compare_to_vrp(metrics: dict[str, float], vrp_metrics: dict[str, float]) -> dict[str, float]:
    return {
        "travel_gap_pct": _gap_pct(metrics["travel_km"], vrp_metrics["travel_km"]),
        "work_std_gap_pct": _gap_pct(metrics["work_std"], vrp_metrics["work_std"]),
        "max_work_gap_pct": _gap_pct(metrics["max_work"], vrp_metrics["max_work"]),
        "overflow_delta": metrics["overflow_480"] - vrp_metrics["overflow_480"],
    }


def _benchmark_single_date(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    filtered_service_df: pd.DataFrame,
    target_date: str,
) -> dict[str, Any]:
    csi_assignment_df, csi_summary_df, _ = csi.build_atlanta_production_assignment_csi_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=filtered_service_df,
        attendance_limited=True,
    )
    hybrid_assignment_df, hybrid_summary_df, _ = build_atlanta_production_assignment_hybrid_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=filtered_service_df,
        attendance_limited=True,
    )
    sits_assignment_df, sits_summary_df, _ = build_atlanta_production_assignment_sits_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=filtered_service_df,
        attendance_limited=True,
    )
    vrp_assignment_df, vrp_summary_df, _ = build_atlanta_production_assignment_vrp_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=filtered_service_df,
        attendance_limited=True,
    )

    csi_metrics = _metrics(csi_summary_df)
    hybrid_metrics = _metrics(hybrid_summary_df)
    sits_metrics = _metrics(sits_summary_df)
    vrp_metrics = _metrics(vrp_summary_df)
    return {
        "date": str(target_date),
        "csi": csi_metrics,
        "hybrid": hybrid_metrics,
        "sits": sits_metrics,
        "vrp": vrp_metrics,
        "csi_gaps": _compare_to_vrp(csi_metrics, vrp_metrics),
        "hybrid_gaps": _compare_to_vrp(hybrid_metrics, vrp_metrics),
        "sits_gaps": _compare_to_vrp(sits_metrics, vrp_metrics),
        "rows": {
            "csi_assignment_rows": float(len(csi_assignment_df)),
            "hybrid_assignment_rows": float(len(hybrid_assignment_df)),
            "sits_assignment_rows": float(len(sits_assignment_df)),
            "vrp_assignment_rows": float(len(vrp_assignment_df)),
        },
    }


def _available_dates(service_df: pd.DataFrame, date_from: str, date_to: str) -> list[str]:
    date_series = service_df["service_date_key"].astype(str)
    return sorted(date_series[(date_series >= str(date_from)) & (date_series <= str(date_to))].dropna().unique().tolist())


def _single_date_lines(result: dict[str, Any]) -> list[str]:
    csi_metrics = result["csi"]
    hybrid_metrics = result["hybrid"]
    sits_metrics = result["sits"]
    vrp_metrics = result["vrp"]
    csi_gaps = result["csi_gaps"]
    hybrid_gaps = result["hybrid_gaps"]
    sits_gaps = result["sits_gaps"]
    target_date = result["date"]
    return [
        f"# CSI/Hybrid/SITS Benchmark {target_date}",
        "",
        "## Metrics",
        "",
        "| Metric | CSI | Hybrid | SITS | VRP | CSI Gap | Hybrid Gap | SITS Gap |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        f"| Engineers | {csi_metrics['engineers']:.0f} | {hybrid_metrics['engineers']:.0f} | {sits_metrics['engineers']:.0f} | {vrp_metrics['engineers']:.0f} | 0.00% | 0.00% | 0.00% |",
        f"| Travel Distance (km) | {csi_metrics['travel_km']:.2f} | {hybrid_metrics['travel_km']:.2f} | {sits_metrics['travel_km']:.2f} | {vrp_metrics['travel_km']:.2f} | {csi_gaps['travel_gap_pct']:.2f}% | {hybrid_gaps['travel_gap_pct']:.2f}% | {sits_gaps['travel_gap_pct']:.2f}% |",
        f"| Work Std Dev (min) | {csi_metrics['work_std']:.2f} | {hybrid_metrics['work_std']:.2f} | {sits_metrics['work_std']:.2f} | {vrp_metrics['work_std']:.2f} | {csi_gaps['work_std_gap_pct']:.2f}% | {hybrid_gaps['work_std_gap_pct']:.2f}% | {sits_gaps['work_std_gap_pct']:.2f}% |",
        f"| Max Work (min) | {csi_metrics['max_work']:.2f} | {hybrid_metrics['max_work']:.2f} | {sits_metrics['max_work']:.2f} | {vrp_metrics['max_work']:.2f} | {csi_gaps['max_work_gap_pct']:.2f}% | {hybrid_gaps['max_work_gap_pct']:.2f}% | {sits_gaps['max_work_gap_pct']:.2f}% |",
        f"| Overflow 480 | {csi_metrics['overflow_480']:.0f} | {hybrid_metrics['overflow_480']:.0f} | {sits_metrics['overflow_480']:.0f} | {vrp_metrics['overflow_480']:.0f} | {csi_gaps['overflow_delta']:.0f} | {hybrid_gaps['overflow_delta']:.0f} | {sits_gaps['overflow_delta']:.0f} |",
        "",
        "## Notes",
        "",
        f"- `csi` uses non-OR-Tools global insertion with `max_weight={csi.CSI_MAX_WORK_WEIGHT}` and `std_weight={csi.CSI_STD_WORK_WEIGHT}`.",
        f"- `hybrid` uses `csi` insertion, then capped relocation with `span_weight={csi.HYBRID_RELOCATION_SPAN_WEIGHT}`, `travel_budget=+{csi.HYBRID_TRAVEL_BUDGET_RATIO * 100:.0f}%`, and `{csi.HYBRID_RELOCATION_PASSES}` passes.",
        f"- `sits` starts from `csi` and applies relocation with `span_weight={csi.SITS_RELOCATION_SPAN_WEIGHT}` for up to `{csi.SITS_RELOCATION_PASSES}` passes.",
        "- `vrp` uses the OR-Tools baseline solver with the default span coefficient.",
    ]


def _range_summary_rows(results: list[dict[str, Any]], gap_key: str) -> tuple[float, float, str]:
    values = [item[gap_key] for item in results]
    avg_gap = sum(values) / len(values)
    worst_index = max(range(len(values)), key=lambda idx: values[idx])
    worst_gap = values[worst_index]
    worst_date = str(results[worst_index]["date"])
    return avg_gap, worst_gap, worst_date


def _range_lines(date_from: str, date_to: str, results: list[dict[str, Any]]) -> list[str]:
    if not results:
        return [
            f"# CSI/Hybrid/SITS Benchmark {date_from} to {date_to}",
            "",
            "No service dates were available in this range.",
        ]

    csi_rows = [item["csi_gaps"] | {"date": item["date"]} for item in results]
    hybrid_rows = [item["hybrid_gaps"] | {"date": item["date"]} for item in results]
    sits_rows = [item["sits_gaps"] | {"date": item["date"]} for item in results]
    csi_travel_avg, csi_travel_worst, csi_travel_date = _range_summary_rows(csi_rows, "travel_gap_pct")
    csi_std_avg, csi_std_worst, csi_std_date = _range_summary_rows(csi_rows, "work_std_gap_pct")
    csi_max_avg, csi_max_worst, csi_max_date = _range_summary_rows(csi_rows, "max_work_gap_pct")
    hybrid_travel_avg, hybrid_travel_worst, hybrid_travel_date = _range_summary_rows(hybrid_rows, "travel_gap_pct")
    hybrid_std_avg, hybrid_std_worst, hybrid_std_date = _range_summary_rows(hybrid_rows, "work_std_gap_pct")
    hybrid_max_avg, hybrid_max_worst, hybrid_max_date = _range_summary_rows(hybrid_rows, "max_work_gap_pct")
    sits_travel_avg, sits_travel_worst, sits_travel_date = _range_summary_rows(sits_rows, "travel_gap_pct")
    sits_std_avg, sits_std_worst, sits_std_date = _range_summary_rows(sits_rows, "work_std_gap_pct")
    sits_max_avg, sits_max_worst, sits_max_date = _range_summary_rows(sits_rows, "max_work_gap_pct")

    lines = [
        f"# CSI/Hybrid/SITS Benchmark {date_from} to {date_to}",
        "",
        "## Scope",
        "",
        "- Validation window is restricted to `2026-01-01` through `2026-01-12` for the active benchmark loop.",
        f"- Available service dates in this run: {', '.join(item['date'] for item in results)}",
        "",
        "## Summary",
        "",
        "| Algorithm | Metric | Average Gap | Worst Gap | Worst Date |",
        "|---|---|---:|---:|---|",
        f"| CSI | Travel Distance (km) | {csi_travel_avg:.2f}% | {csi_travel_worst:.2f}% | {csi_travel_date} |",
        f"| CSI | Work Std Dev (min) | {csi_std_avg:.2f}% | {csi_std_worst:.2f}% | {csi_std_date} |",
        f"| CSI | Max Work (min) | {csi_max_avg:.2f}% | {csi_max_worst:.2f}% | {csi_max_date} |",
        f"| Hybrid | Travel Distance (km) | {hybrid_travel_avg:.2f}% | {hybrid_travel_worst:.2f}% | {hybrid_travel_date} |",
        f"| Hybrid | Work Std Dev (min) | {hybrid_std_avg:.2f}% | {hybrid_std_worst:.2f}% | {hybrid_std_date} |",
        f"| Hybrid | Max Work (min) | {hybrid_max_avg:.2f}% | {hybrid_max_worst:.2f}% | {hybrid_max_date} |",
        f"| SITS | Travel Distance (km) | {sits_travel_avg:.2f}% | {sits_travel_worst:.2f}% | {sits_travel_date} |",
        f"| SITS | Work Std Dev (min) | {sits_std_avg:.2f}% | {sits_std_worst:.2f}% | {sits_std_date} |",
        f"| SITS | Max Work (min) | {sits_max_avg:.2f}% | {sits_max_worst:.2f}% | {sits_max_date} |",
        "",
        "## Per-Date Results",
        "",
        "| Date | CSI km | Hybrid km | SITS km | VRP km | CSI Travel Gap | Hybrid Travel Gap | SITS Travel Gap | CSI Std Gap | Hybrid Std Gap | SITS Std Gap | CSI Max Gap | Hybrid Max Gap | SITS Max Gap |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in results:
        lines.append(
            f"| {item['date']} | {item['csi']['travel_km']:.2f} | {item['hybrid']['travel_km']:.2f} | {item['sits']['travel_km']:.2f} | {item['vrp']['travel_km']:.2f} | "
            f"{item['csi_gaps']['travel_gap_pct']:.2f}% | {item['hybrid_gaps']['travel_gap_pct']:.2f}% | {item['sits_gaps']['travel_gap_pct']:.2f}% | "
            f"{item['csi_gaps']['work_std_gap_pct']:.2f}% | {item['hybrid_gaps']['work_std_gap_pct']:.2f}% | {item['sits_gaps']['work_std_gap_pct']:.2f}% | "
            f"{item['csi_gaps']['max_work_gap_pct']:.2f}% | {item['hybrid_gaps']['max_work_gap_pct']:.2f}% | {item['sits_gaps']['max_work_gap_pct']:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            f"- `csi` uses non-OR-Tools global insertion with `max_weight={csi.CSI_MAX_WORK_WEIGHT}` and `std_weight={csi.CSI_STD_WORK_WEIGHT}`.",
            f"- `hybrid` uses `csi` insertion, then capped relocation with `span_weight={csi.HYBRID_RELOCATION_SPAN_WEIGHT}`, `travel_budget=+{csi.HYBRID_TRAVEL_BUDGET_RATIO * 100:.0f}%`, and `{csi.HYBRID_RELOCATION_PASSES}` passes.",
            f"- `sits` starts from `csi` and applies relocation with `span_weight={csi.SITS_RELOCATION_SPAN_WEIGHT}` for up to `{csi.SITS_RELOCATION_PASSES}` passes.",
            "- `vrp` uses the OR-Tools baseline solver with the default span coefficient.",
        ]
    )
    return lines


def build_report(target_date: str = "", date_from: str = DEFAULT_DATE_FROM, date_to: str = DEFAULT_DATE_TO) -> tuple[str, dict[str, Any]]:
    _, engineer_region_df, home_df, service_df = base._load_inputs()
    service_df = _dedupe_service(service_df)

    if target_date:
        filtered_service_df = service_df[service_df["service_date_key"].astype(str) == str(target_date)].copy()
        result = _benchmark_single_date(
            engineer_region_df=engineer_region_df,
            home_df=home_df,
            filtered_service_df=filtered_service_df,
            target_date=str(target_date),
        )
        return "\n".join(_single_date_lines(result)) + "\n", result

    available_dates = _available_dates(service_df, date_from, date_to)
    results: list[dict[str, Any]] = []
    for current_date in available_dates:
        filtered_service_df = service_df[service_df["service_date_key"].astype(str) == current_date].copy()
        print(f"Benchmarking {current_date}...")
        results.append(
            _benchmark_single_date(
                engineer_region_df=engineer_region_df,
                home_df=home_df,
                filtered_service_df=filtered_service_df,
                target_date=current_date,
            )
        )
    return "\n".join(_range_lines(date_from, date_to, results)) + "\n", {"date_from": date_from, "date_to": date_to, "results": results}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="")
    parser.add_argument("--date-from", default=DEFAULT_DATE_FROM)
    parser.add_argument("--date-to", default=DEFAULT_DATE_TO)
    parser.add_argument("--write", default="")
    args = parser.parse_args()

    report_text, _ = build_report(args.date, args.date_from, args.date_to)
    print(report_text)
    if args.write:
        output_path = Path(args.write)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report_text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
