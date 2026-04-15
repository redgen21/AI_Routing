from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

import smart_routing.production_assign_atlanta as base
from smart_routing.production_assign_atlanta_osrm import build_atlanta_production_assignment_osrm_from_frames
from smart_routing.production_assign_atlanta_vrp import build_atlanta_production_assignment_vrp_from_frames


DEFAULT_DATE_FROM = "2026-01-01"
DEFAULT_DATE_TO = "2026-01-12"


def _dedupe_service(service_df: pd.DataFrame) -> pd.DataFrame:
    deduped = service_df.copy()
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
    work_series = pd.to_numeric(summary_df["total_work_min"], errors="coerce").fillna(0)
    return {
        "engineers": float(summary_df["SVC_ENGINEER_CODE"].nunique()),
        "travel_km": float(pd.to_numeric(summary_df["travel_distance_km"], errors="coerce").fillna(0).sum()),
        "work_std": float(work_series.std(ddof=0)),
        "max_work": float(work_series.max()),
        "overflow_480": float(pd.to_numeric(summary_df["overflow_480"], errors="coerce").fillna(0).astype(int).sum()),
    }


def _gap_pct(actual: float, baseline: float) -> float:
    if baseline == 0:
        return 0.0
    return ((actual - baseline) / baseline) * 100.0


def _benchmark_single_date(
    engineer_region_df: pd.DataFrame,
    home_df: pd.DataFrame,
    filtered_service_df: pd.DataFrame,
    target_date: str,
) -> dict[str, Any]:
    vrp_level_assignment_df, vrp_level_summary_df, _ = build_atlanta_production_assignment_osrm_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=filtered_service_df,
        attendance_limited=True,
        assignment_strategy="vrp_level",
    )
    vrp_assignment_df, vrp_summary_df, _ = build_atlanta_production_assignment_vrp_from_frames(
        engineer_region_df=engineer_region_df,
        home_df=home_df,
        service_df=filtered_service_df,
        attendance_limited=True,
    )

    vrp_level_metrics = _metrics(vrp_level_summary_df)
    vrp_metrics = _metrics(vrp_summary_df)
    gaps = {
        "travel_gap_pct": _gap_pct(vrp_level_metrics["travel_km"], vrp_metrics["travel_km"]),
        "work_std_gap_pct": _gap_pct(vrp_level_metrics["work_std"], vrp_metrics["work_std"]),
        "max_work_gap_pct": _gap_pct(vrp_level_metrics["max_work"], vrp_metrics["max_work"]),
    }
    return {
        "date": str(target_date),
        "vrp_level": vrp_level_metrics,
        "vrp": vrp_metrics,
        "gaps": gaps,
        "rows": {
            "vrp_level_assignment_rows": float(len(vrp_level_assignment_df)),
            "vrp_assignment_rows": float(len(vrp_assignment_df)),
        },
    }


def _available_dates(service_df: pd.DataFrame, date_from: str, date_to: str) -> list[str]:
    date_series = service_df["service_date_key"].astype(str)
    return sorted(date_series[(date_series >= str(date_from)) & (date_series <= str(date_to))].dropna().unique().tolist())


def _single_date_lines(result: dict[str, Any]) -> list[str]:
    vrp_level_metrics = result["vrp_level"]
    vrp_metrics = result["vrp"]
    gaps = result["gaps"]
    target_date = result["date"]
    return [
        f"# VRP-Level Benchmark {target_date}",
        "",
        "## Metrics",
        "",
        "| Metric | VRP-Level | VRP | Gap |",
        "|---|---:|---:|---:|",
        f"| Engineers | {vrp_level_metrics['engineers']:.0f} | {vrp_metrics['engineers']:.0f} | 0.00% |",
        f"| Travel Distance (km) | {vrp_level_metrics['travel_km']:.2f} | {vrp_metrics['travel_km']:.2f} | {gaps['travel_gap_pct']:.2f}% |",
        f"| Work Std Dev (min) | {vrp_level_metrics['work_std']:.2f} | {vrp_metrics['work_std']:.2f} | {gaps['work_std_gap_pct']:.2f}% |",
        f"| Max Work (min) | {vrp_level_metrics['max_work']:.2f} | {vrp_metrics['max_work']:.2f} | {gaps['max_work_gap_pct']:.2f}% |",
        f"| Overflow 480 | {vrp_level_metrics['overflow_480']:.0f} | {vrp_metrics['overflow_480']:.0f} | {vrp_level_metrics['overflow_480'] - vrp_metrics['overflow_480']:.0f} |",
        "",
        "## Notes",
        "",
        "- `vrp_level` uses the current OSRM heuristic pipeline.",
        "- `vrp` uses the OR-Tools baseline solver.",
    ]


def _range_lines(date_from: str, date_to: str, results: list[dict[str, Any]]) -> list[str]:
    if not results:
        return [
            f"# VRP-Level Benchmark {date_from} to {date_to}",
            "",
            "No service dates were available in this range.",
        ]

    avg_travel_gap = sum(item["gaps"]["travel_gap_pct"] for item in results) / len(results)
    avg_work_std_gap = sum(item["gaps"]["work_std_gap_pct"] for item in results) / len(results)
    avg_max_work_gap = sum(item["gaps"]["max_work_gap_pct"] for item in results) / len(results)
    worst_travel = max(results, key=lambda item: item["gaps"]["travel_gap_pct"])
    worst_work_std = max(results, key=lambda item: item["gaps"]["work_std_gap_pct"])
    worst_max_work = max(results, key=lambda item: item["gaps"]["max_work_gap_pct"])

    lines = [
        f"# VRP-Level Benchmark {date_from} to {date_to}",
        "",
        "## Scope",
        "",
        "- Validation window is restricted to `2026-01-01` through `2026-01-12`.",
        f"- Available service dates in this run: {', '.join(item['date'] for item in results)}",
        "",
        "## Summary",
        "",
        "| Metric | Average Gap | Worst Gap | Worst Date |",
        "|---|---:|---:|---|",
        f"| Travel Distance (km) | {avg_travel_gap:.2f}% | {worst_travel['gaps']['travel_gap_pct']:.2f}% | {worst_travel['date']} |",
        f"| Work Std Dev (min) | {avg_work_std_gap:.2f}% | {worst_work_std['gaps']['work_std_gap_pct']:.2f}% | {worst_work_std['date']} |",
        f"| Max Work (min) | {avg_max_work_gap:.2f}% | {worst_max_work['gaps']['max_work_gap_pct']:.2f}% | {worst_max_work['date']} |",
        "",
        "## Per-Date Results",
        "",
        "| Date | VRP-Level km | VRP km | Travel Gap | VRP-Level Std | VRP Std | Std Gap | VRP-Level Max | VRP Max | Max Gap | Overflow Delta |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in results:
        vrp_level_metrics = item["vrp_level"]
        vrp_metrics = item["vrp"]
        gaps = item["gaps"]
        overflow_delta = vrp_level_metrics["overflow_480"] - vrp_metrics["overflow_480"]
        lines.append(
            f"| {item['date']} | {vrp_level_metrics['travel_km']:.2f} | {vrp_metrics['travel_km']:.2f} | {gaps['travel_gap_pct']:.2f}% | "
            f"{vrp_level_metrics['work_std']:.2f} | {vrp_metrics['work_std']:.2f} | {gaps['work_std_gap_pct']:.2f}% | "
            f"{vrp_level_metrics['max_work']:.2f} | {vrp_metrics['max_work']:.2f} | {gaps['max_work_gap_pct']:.2f}% | {overflow_delta:.0f} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `vrp_level` uses the current OSRM heuristic pipeline.",
            "- `vrp` uses the OR-Tools baseline solver.",
            "- This report should be treated as the shared Codex/Claude benchmark artifact for the allowed January window.",
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
    report_text = "\n".join(_range_lines(date_from, date_to, results)) + "\n"
    return report_text, {"date_from": date_from, "date_to": date_to, "results": results}


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
