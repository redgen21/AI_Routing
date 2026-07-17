#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import math
import statistics
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_BASE_URL = "http://20.51.244.68:5002"
DEFAULT_ORIGIN = (-84.3880, 33.7490)
DEFAULT_SIZES = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]


@dataclass
class RequestResult:
    size: int
    request_id: int
    ok: bool
    status: int | None
    elapsed_ms: float
    response_bytes: int
    osrm_code: str
    error: str


def build_destinations(count: int, origin: tuple[float, float]) -> list[tuple[float, float]]:
    lon0, lat0 = origin
    points: list[tuple[float, float]] = []
    # Deterministic spiral around Atlanta. The radius grows slowly enough to keep
    # points in the Georgia OSRM extract while still avoiding 1000 identical snaps.
    for idx in range(count):
        angle = idx * 2.399963229728653
        radius = 0.01 + 0.00028 * math.sqrt(idx)
        lon = lon0 + math.cos(angle) * radius * 1.25
        lat = lat0 + math.sin(angle) * radius
        points.append((round(lon, 6), round(lat, 6)))
    return points


def build_table_url(
    base_url: str,
    profile: str,
    origin: tuple[float, float],
    destinations: list[tuple[float, float]],
    annotations: str,
) -> str:
    coords = [origin] + destinations
    coord_text = ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)
    dest_indices = ";".join(str(idx) for idx in range(1, len(coords)))
    return (
        f"{base_url.rstrip('/')}/table/v1/{profile}/{coord_text}"
        f"?sources=0&destinations={dest_indices}&annotations={annotations}"
    )


def request_once(url: str, size: int, request_id: int, timeout_sec: float) -> RequestResult:
    started = time.perf_counter()
    status: int | None = None
    payload_bytes = b""
    osrm_code = ""
    error = ""
    ok = False
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "osrm-table-load-test/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            status = int(resp.status)
            payload_bytes = resp.read()
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
            osrm_code = str(payload.get("code", ""))
            ok = status == 200 and osrm_code == "Ok"
            if not ok:
                error = str(payload.get("message", ""))[:500]
        except Exception as exc:  # noqa: BLE001
            ok = False
            error = f"invalid-json: {exc}"
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        payload_bytes = exc.read()
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
            osrm_code = str(payload.get("code", ""))
            error = str(payload.get("message", ""))[:500]
        except Exception:
            error = str(exc)[:500]
    except Exception as exc:  # noqa: BLE001
        error = str(exc)[:500]
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return RequestResult(
        size=size,
        request_id=request_id,
        ok=ok,
        status=status,
        elapsed_ms=elapsed_ms,
        response_bytes=len(payload_bytes),
        osrm_code=osrm_code,
        error=error,
    )


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[int(index)]
    return ordered[lower] * (upper - index) + ordered[upper] * (index - lower)


def summarize(size: int, results: list[RequestResult], scenario_elapsed_sec: float) -> dict[str, Any]:
    ok_results = [result for result in results if result.ok]
    latencies = [result.elapsed_ms for result in ok_results]
    errors: dict[str, int] = {}
    for result in results:
        if result.ok:
            continue
        key = result.osrm_code or result.error or f"HTTP_{result.status}"
        errors[key[:120]] = errors.get(key[:120], 0) + 1
    return {
        "size": size,
        "requests": len(results),
        "success": len(ok_results),
        "failed": len(results) - len(ok_results),
        "success_rate": len(ok_results) / len(results) if results else 0.0,
        "scenario_elapsed_sec": scenario_elapsed_sec,
        "throughput_rps": len(results) / scenario_elapsed_sec if scenario_elapsed_sec > 0 else 0.0,
        "latency_avg_ms": statistics.mean(latencies) if latencies else 0.0,
        "latency_p50_ms": percentile(latencies, 0.50),
        "latency_p95_ms": percentile(latencies, 0.95),
        "latency_max_ms": max(latencies) if latencies else 0.0,
        "response_bytes_avg": statistics.mean([r.response_bytes for r in ok_results]) if ok_results else 0.0,
        "errors": errors,
    }


def run_scenario(
    base_url: str,
    profile: str,
    origin: tuple[float, float],
    annotations: str,
    size: int,
    users: int,
    timeout_sec: float,
) -> tuple[dict[str, Any], list[RequestResult]]:
    destinations = build_destinations(size, origin)
    url = build_table_url(base_url, profile, origin, destinations, annotations)
    print(f"\n=== size=1x{size}, users={users}, url_chars={len(url)} ===", flush=True)
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=users) as executor:
        futures = [
            executor.submit(request_once, url, size, request_id + 1, timeout_sec)
            for request_id in range(users)
        ]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    elapsed = time.perf_counter() - started
    summary = summarize(size, results, elapsed)
    print(
        "success={success}/{requests} p50={latency_p50_ms:.0f}ms "
        "p95={latency_p95_ms:.0f}ms max={latency_max_ms:.0f}ms rps={throughput_rps:.2f}".format(
            **summary
        ),
        flush=True,
    )
    if summary["errors"]:
        print(f"errors={summary['errors']}", flush=True)
    return summary, results


def write_outputs(output_dir: Path, summaries: list[dict[str, Any]], results: list[RequestResult]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "osrm_table_load_summary.csv"
    detail_path = output_dir / "osrm_table_load_detail.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as fh:
        fieldnames = [
            "size",
            "requests",
            "success",
            "failed",
            "success_rate",
            "scenario_elapsed_sec",
            "throughput_rps",
            "latency_avg_ms",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_max_ms",
            "response_bytes_avg",
            "errors",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in summaries:
            serializable = dict(row)
            serializable["errors"] = json.dumps(serializable["errors"], ensure_ascii=False)
            writer.writerow(serializable)
    with detail_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(RequestResult.__dataclass_fields__.keys()))
        writer.writeheader()
        for result in results:
            writer.writerow(result.__dict__)
    print(f"\nWrote {summary_path}")
    print(f"Wrote {detail_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Concurrent OSRM table 1xN load test")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--profile", default="driving")
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--sizes", default=",".join(str(size) for size in DEFAULT_SIZES))
    parser.add_argument("--timeout-sec", type=float, default=120.0)
    parser.add_argument("--annotations", default="duration,distance")
    parser.add_argument("--origin-lon", type=float, default=DEFAULT_ORIGIN[0])
    parser.add_argument("--origin-lat", type=float, default=DEFAULT_ORIGIN[1])
    parser.add_argument("--output-dir", default="log")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sizes = [int(part.strip()) for part in str(args.sizes).split(",") if part.strip()]
    origin = (float(args.origin_lon), float(args.origin_lat))
    all_summaries: list[dict[str, Any]] = []
    all_results: list[RequestResult] = []
    total_started = time.perf_counter()
    for size in sizes:
        summary, results = run_scenario(
            base_url=str(args.base_url),
            profile=str(args.profile),
            origin=origin,
            annotations=str(args.annotations),
            size=size,
            users=int(args.users),
            timeout_sec=float(args.timeout_sec),
        )
        all_summaries.append(summary)
        all_results.extend(results)
    print(f"\nTotal elapsed: {time.perf_counter() - total_started:.1f}s")
    write_outputs(Path(args.output_dir), all_summaries, all_results)


if __name__ == "__main__":
    main()
