"""Build a privacy-safe Korean Current Coverage report from the UI service source.

The report deliberately does not borrow persisted route-explorer distances: those
artifacts were built from a different service extract.  It uses the same Current
Coverage grouping as ``sr_area_map`` (service date + assigned SM) and only emits
aggregate counts.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sr_area_map import (
    _apply_center_bucket_rules,
    _classify_assignment_group_bucket,
    get_latest_geocoded_service_file,
)
from smart_routing.area_map import load_service_points

DEFAULT_OUTPUT = ROOT / "보고서/current_coverage_report_ko.html"
DEFAULT_ROUTE_DETAIL = ROOT / "exports/current_coverage_june_2026_osrm/route_detail_privacy_safe.csv"
DEFAULT_ROUTE_METADATA = ROOT / "exports/current_coverage_june_2026_osrm/metadata.json"
KM_TO_MILE = 0.621371
EXPECTED_OSRM_FINGERPRINT = "141efa82b369a742413a1bfaa7a5eb2406056e1f53f7e89e1067e56735f9dee5"
CITIES = (
    "Atlanta, GA",
    "Los Angeles, CA",
    "North Jersey, NJ",
    "Philadelphia, PA",
    "San Diego, CA",
    "Washington, DC",
)
EXPECTED_SERVICES = {
    "Atlanta, GA": 981,
    "Los Angeles, CA": 3438,
    "North Jersey, NJ": 2602,
    "Philadelphia, PA": 714,
    "San Diego, CA": 683,
    "Washington, DC": 862,
}


def _load_current_coverage(service_file: Path) -> pd.DataFrame:
    frame = load_service_points(service_file)
    required = {"STRATEGIC_CITY_NAME", "GSFS_RECEIPT_NO", "SVC_ENGINEER_CODE", "service_date"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Current Coverage source missing fields: {sorted(missing)}")
    frame = frame[frame["STRATEGIC_CITY_NAME"].isin(CITIES)].copy()
    frame["service_date_key"] = pd.to_datetime(frame["service_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if frame["service_date_key"].isna().any():
        raise ValueError("Current Coverage source has invalid service dates")
    frame["assigned_sm_code"] = frame["SVC_ENGINEER_CODE"].astype(str).str.strip()
    if frame["assigned_sm_code"].eq("").any():
        raise ValueError("Current Coverage source has blank assigned SM codes")
    return _apply_center_bucket_rules(frame, None)


def _ui_normalize(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).replace("\r", " ").replace("\n", " ").split()).strip()


def _route_group_id(city: object, service_date: object, assigned_sm_code: object) -> str:
    value = "\x1f".join((_ui_normalize(city), str(service_date), _ui_normalize(assigned_sm_code)))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _source_groups(frame: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        frame.groupby(["STRATEGIC_CITY_NAME", "service_date_key", "assigned_sm_code"], as_index=False)
        .agg(
            bucket=("CENTER_BUCKET", _classify_assignment_group_bucket),
            jobs=("GSFS_RECEIPT_NO", lambda values: values.dropna().astype(str).nunique()),
        )
        .rename(columns={"STRATEGIC_CITY_NAME": "city"})
    )
    grouped["route_group_id"] = grouped.apply(
        lambda row: _route_group_id(row["city"], row["service_date_key"], row["assigned_sm_code"]), axis=1
    )
    return grouped


def _load_route_detail(route_file: Path, metadata_file: Path, service_file: Path, groups: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    source_sha = hashlib.sha256(service_file.resolve().read_bytes()).hexdigest()
    expected_dates = (str(groups["service_date_key"].min()), str(groups["service_date_key"].max()))
    if metadata.get("schema") != "current-coverage-osrm-result/v2":
        raise ValueError("Unsupported route-detail schema")
    computation = metadata.get("computation", {})
    fingerprint = computation.get("fingerprint", {}).get("sha256")
    if fingerprint != EXPECTED_OSRM_FINGERPRINT:
        raise ValueError("Route-detail computation fingerprint does not match approved artifact")
    if computation.get("single_uninterrupted_result_set") is not True:
        raise ValueError("Route-detail must be one uninterrupted result set")
    if int(metadata.get("checkpoint", {}).get("resumed_group_count", -1)) != 0:
        raise ValueError("Route-detail must not include resumed checkpoint groups")
    if int(metadata.get("counts", {}).get("cache_hit_group_count", -1)) != 0:
        raise ValueError("Route-detail must not include cache hits")
    if int(metadata.get("counts", {}).get("invalid_zero_success_count", -1)) != 0:
        raise ValueError("Route-detail contains invalid zero-distance successes")
    if metadata.get("source", {}).get("sha256") != source_sha:
        raise ValueError("Route-detail source SHA-256 does not match Current Coverage source")
    if (metadata.get("source", {}).get("service_date_min"), metadata.get("source", {}).get("service_date_max")) != expected_dates:
        raise ValueError("Route-detail service date provenance does not match")
    if metadata.get("route_semantics", {}).get("distance_unit") != "km" or metadata.get("route_semantics", {}).get("duration_unit") != "min":
        raise ValueError("Route-detail units must be km and min")
    routes = pd.read_csv(route_file, low_memory=False)
    required = {"city", "service_date_key", "route_group_id", "route_bucket", "service_count", "status", "failure_reason", "distance_km", "duration_min"}
    missing = required - set(routes.columns)
    if missing:
        raise ValueError(f"Route-detail fields missing: {sorted(missing)}")
    keys = ["city", "service_date_key", "route_group_id"]
    if routes.duplicated(keys).any():
        raise ValueError("Route-detail contains duplicate group keys")
    if not routes["status"].isin(["success", "failed"]).all():
        raise ValueError("Route-detail contains unsupported status")
    counts = metadata.get("counts", {})
    actual_counts = {
        "expected_group_count": len(groups),
        "detail_row_count": len(routes),
        "success_group_count": int(routes["status"].eq("success").sum()),
        "failed_group_count": int(routes["status"].eq("failed").sum()),
    }
    if any(int(counts.get(key, -1)) != value for key, value in actual_counts.items()):
        raise ValueError("Route-detail metadata accounting does not match source/detail rows")
    routes["distance_km"] = pd.to_numeric(routes["distance_km"], errors="coerce")
    routes["duration_min"] = pd.to_numeric(routes["duration_min"], errors="coerce")
    success = routes["status"].eq("success")
    if routes.loc[success, ["distance_km", "duration_min"]].isna().any().any() or (routes.loc[success, ["distance_km", "duration_min"]] < 0).any().any():
        raise ValueError("Successful route-detail rows require nonnegative metrics")
    if routes.loc[~success, ["distance_km", "duration_min"]].notna().any().any() or routes.loc[~success, "failure_reason"].fillna("").astype(str).str.strip().eq("").any():
        raise ValueError("Failed route-detail rows require null metrics and a reason")
    expected = groups[["city", "service_date_key", "route_group_id", "bucket", "jobs"]]
    joined = expected.merge(routes, on=keys, how="outer", indicator=True, validate="one_to_one")
    if joined["_merge"].eq("right_only").any():
        raise ValueError("Route-detail contains groups outside the Current Coverage source")
    matched = joined["_merge"].eq("both")
    if not joined.loc[matched, "route_bucket"].eq(joined.loc[matched, "bucket"]).all() or not pd.to_numeric(joined.loc[matched, "service_count"]).eq(joined.loc[matched, "jobs"]).all():
        raise ValueError("Route-detail bucket/service accounting does not match source groups")
    return joined, metadata


def _summarize(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for city in CITIES:
        scoped = frame[frame["STRATEGIC_CITY_NAME"].eq(city)].copy()
        services = int(scoped["GSFS_RECEIPT_NO"].astype(str).nunique())
        daily_groups = (
            scoped.groupby(["service_date_key", "assigned_sm_code"], as_index=False)["CENTER_BUCKET"]
            .agg(_classify_assignment_group_bucket)
            .rename(columns={"CENTER_BUCKET": "group_bucket"})
        )
        service_by_bucket = (
            scoped.groupby("CENTER_BUCKET")["GSFS_RECEIPT_NO"].nunique().to_dict()
        )
        group_by_bucket = daily_groups["group_bucket"].value_counts().to_dict()
        rows.append({
            "city": city,
            "services": services,
            "assigned_sm_groups": int(len(daily_groups)),
            "avg_services_per_group": services / len(daily_groups) if len(daily_groups) else None,
            "service_dms": int(service_by_bucket.get("DMS", 0)),
            "service_dms2": int(service_by_bucket.get("DMS2", 0)),
            "service_asc": int(service_by_bucket.get("ASC", 0)),
            "group_dms": int(group_by_bucket.get("DMS", 0)),
            "group_dms2": int(group_by_bucket.get("DMS2", 0)),
            "group_asc": int(group_by_bucket.get("ASC", 0)),
            "group_mixed": int(group_by_bucket.get("MIXED", 0)),
            "date_start": scoped["service_date_key"].min(),
            "date_end": scoped["service_date_key"].max(),
        })
    return pd.DataFrame(rows)


def _add_city_route_metrics(summary: pd.DataFrame, route_joined: pd.DataFrame) -> pd.DataFrame:
    output = summary.copy()
    successful = route_joined[route_joined["status"].eq("success")].copy()
    city_metrics = successful.groupby("city").agg(
        avg_distance_km=("distance_km", "mean"),
        avg_duration_min=("duration_min", "mean"),
    )
    output["avg_distance_mile"] = output["city"].map(city_metrics["avg_distance_km"]) * KM_TO_MILE
    output["avg_duration_min"] = output["city"].map(city_metrics["avg_duration_min"])
    return output


def _bucket_detail(frame: pd.DataFrame, route_joined: pd.DataFrame | None = None) -> pd.DataFrame:
    """Return city × UI bucket metrics without inferring unavailable routes."""
    grouped = _source_groups(frame)
    rows: list[dict[str, object]] = []
    for city in CITIES:
        city_rows = frame[frame["STRATEGIC_CITY_NAME"].eq(city)]
        city_total = int(city_rows["GSFS_RECEIPT_NO"].astype(str).nunique())
        service_by_bucket = city_rows.groupby("CENTER_BUCKET")["GSFS_RECEIPT_NO"].nunique()
        city_groups = grouped[grouped["city"].eq(city)]
        for bucket in ("DMS", "DMS2", "ASC", "MIXED"):
            group_rows = city_groups[city_groups["bucket"].eq(bucket)]
            if bucket == "MIXED" and group_rows.empty:
                continue
            service_count: int | None = (
                None if bucket == "MIXED" else int(service_by_bucket.get(bucket, 0))
            )
            route_rows = route_joined[(route_joined["city"].eq(city)) & (route_joined["bucket"].eq(bucket))] if route_joined is not None else pd.DataFrame()
            routed = route_rows[route_rows["status"].eq("success")] if not route_rows.empty else route_rows
            failed = int(route_rows["status"].eq("failed").sum()) if not route_rows.empty else 0
            unmatched = int(route_rows["_merge"].eq("left_only").sum()) if not route_rows.empty else int(len(group_rows))
            rows.append(
                {
                    "city": city,
                    "bucket": bucket,
                    "services": service_count,
                    "service_share": (
                        service_count / city_total
                        if service_count is not None and city_total
                        else None
                    ),
                    "daily_sm_groups": int(len(group_rows)),
                    "avg_jobs_per_group": float(group_rows["jobs"].mean()) if len(group_rows) else None,
                    "jobs_stddev": float(group_rows["jobs"].std(ddof=0)) if len(group_rows) else None,
                    "routed_groups": int(len(routed)),
                    "failed_groups": failed,
                    "unmatched_groups": unmatched,
                    "route_coverage": len(routed) / len(group_rows) if len(group_rows) else None,
                    "avg_distance_km": float(routed["distance_km"].mean()) if len(routed) else None,
                    "total_distance_km": float(routed["distance_km"].sum()) if len(routed) else None,
                    "avg_distance_mile": float(routed["distance_km"].mean()) * KM_TO_MILE if len(routed) else None,
                    "total_distance_mile": float(routed["distance_km"].sum()) * KM_TO_MILE if len(routed) else None,
                    "avg_duration_min": float(routed["duration_min"].mean()) if len(routed) else None,
                    "total_duration_min": float(routed["duration_min"].sum()) if len(routed) else None,
                }
            )
    return pd.DataFrame(rows)


def _fmt(value: object) -> str:
    if value is None or pd.isna(value):
        return "사용 불가"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{int(value):,}" if isinstance(value, (int,)) else str(value)


def _table(summary: pd.DataFrame) -> str:
    headers = ["도시", "서비스", "일별 배정 SM 그룹", "평균 서비스/그룹", "평균 거리 (mile)", "평균 시간 (분)", "DMS 서비스", "DMS2 서비스", "ASC 서비스", "DMS 그룹", "DMS2 그룹", "ASC 그룹", "MIXED 그룹"]
    parts = ["<table><thead><tr>", *(f"<th>{html.escape(header)}</th>" for header in headers), "</tr></thead><tbody>"]
    for row in summary.itertuples(index=False):
        values = [row.city, _fmt(row.services), _fmt(row.assigned_sm_groups), _fmt(row.avg_services_per_group), _fmt(row.avg_distance_mile), _fmt(row.avg_duration_min), _fmt(row.service_dms), _fmt(row.service_dms2), _fmt(row.service_asc), _fmt(row.group_dms), _fmt(row.group_dms2), _fmt(row.group_asc), _fmt(row.group_mixed)]
        parts.append("<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in values) + "</tr>")
    return "".join(parts) + "</tbody></table>"


def _bucket_table(detail: pd.DataFrame) -> str:
    headers = [
        "도시", "버킷", "서비스", "서비스 비중", "일별 SM 그룹",
        "평균 작업/그룹", "작업 표준편차 (ddof=0)",
        "경로 성공",
        "평균 km", "합계 km", "평균 mile", "합계 mile", "평균 분", "합계 분",
    ]
    parts = ["<table><thead><tr>", *(f"<th>{html.escape(header)}</th>" for header in headers), "</tr></thead><tbody>"]
    for row in detail.itertuples(index=False):
        values = [
            row.city, row.bucket,
            "N/A" if row.services is None or pd.isna(row.services) else _fmt(row.services),
            "N/A" if row.service_share is None or pd.isna(row.service_share) else f"{float(row.service_share) * 100:.1f}%",
            _fmt(row.daily_sm_groups), _fmt(row.avg_jobs_per_group), _fmt(row.jobs_stddev),
            _fmt(row.routed_groups),
            _fmt(row.avg_distance_km), _fmt(row.total_distance_km),
            _fmt(row.avg_distance_mile), _fmt(row.total_distance_mile),
            _fmt(row.avg_duration_min), _fmt(row.total_duration_min),
        ]
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in values)
        row_class = f"bucket-row bucket-{str(row.bucket).lower()}"
        parts.append(f"<tr class='{row_class}'>" + cells + "</tr>")
    return "".join(parts) + "</tbody></table>"


def _render(summary: pd.DataFrame, detail: pd.DataFrame, service_file: Path, route_joined: pd.DataFrame, route_metadata: dict) -> str:
    total = int(summary["services"].sum())
    groups = int(summary["assigned_sm_groups"].sum())
    date_range = f"{summary['date_start'].min()} ~ {summary['date_end'].max()}"
    routed = int(route_joined["status"].eq("success").sum())
    failed = int(route_joined["status"].eq("failed").sum())
    unmatched = int(route_joined["_merge"].eq("left_only").sum())
    home_found = int(route_metadata["counts"]["home_found_group_count"])
    return f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Current Coverage 현황 보고서</title>
<style>body{{font-family:system-ui,-apple-system,'Segoe UI',sans-serif;margin:0;background:#f5f7fa;color:#18212f}}main{{max-width:1400px;margin:auto;padding:32px}}section,.card{{background:#fff;border:1px solid #d7dee8;border-radius:8px;padding:20px;margin:18px 0}}.cards{{display:flex;gap:12px;flex-wrap:wrap}}.n{{font-size:1.5rem;font-weight:700}}.table-head{{display:flex;align-items:baseline;justify-content:space-between;gap:16px}}.date-range{{color:#52606d;font-size:.9rem;white-space:nowrap}}table{{border-collapse:collapse;width:100%;font-size:.86rem}}th,td{{border-bottom:1px solid #e5eaf0;padding:8px;text-align:right;white-space:nowrap}}th{{background:#eef3f8;text-align:center}}td:first-child{{text-align:left}}.bucket-row td:nth-child(2){{font-weight:700;text-align:center}}.bucket-dms>td{{background:#e8f3ff;color:#174a78}}.bucket-dms2>td{{background:#eee9ff;color:#4a3580}}.bucket-asc>td{{background:#e8f7ed;color:#25633a}}.bucket-mixed>td{{background:#fff2d9;color:#74510a}}</style></head>
<body><main><h1>Current Coverage 현황</h1><p>Atlanta · Los Angeles · North Jersey · Philadelphia · San Diego · Washington DC</p>
<div class="cards"><div class="card">서비스 고유 건수<div class="n">{total:,}</div></div><div class="card">일별 배정 SM 그룹<div class="n">{groups:,}</div></div><div class="card">OSRM 성공<div class="n">{routed:,}</div></div><div class="card">실패 / 미매칭<div class="n">{failed:,} / {unmatched:,}</div></div><div class="card">경로 커버리지<div class="n">{routed / groups:.1%}</div></div><div class="card">홈 좌표 확보<div class="n">{home_found:,} / {groups:,}</div></div></div>
<section><div class="table-head"><h2>도시별 집계</h2><span class="date-range">서비스 날짜: {date_range}</span></div>{_table(summary)}</section>
<section><h2>도시 × 센터/그룹 버킷 비교</h2><p>서비스는 row-level 센터 버킷의 고유 접수 건수이며, 그룹은 UI와 동일한 일자+SM 기준입니다. MIXED는 그룹 버킷만 있으므로 서비스/비중은 N/A입니다.</p>{_bucket_table(detail)}</section>
</main></body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service-file", type=Path)
    parser.add_argument("--route-detail", type=Path, default=DEFAULT_ROUTE_DETAIL)
    parser.add_argument("--route-metadata", type=Path, default=DEFAULT_ROUTE_METADATA)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    service_file = args.service_file or get_latest_geocoded_service_file()
    if service_file is None:
        raise ValueError("No configured Current Coverage service source is available")
    frame = _load_current_coverage(service_file)
    summary = _summarize(frame)
    groups = _source_groups(frame)
    route_joined, route_metadata = _load_route_detail(args.route_detail, args.route_metadata, service_file, groups)
    summary = _add_city_route_metrics(summary, route_joined)
    detail = _bucket_detail(frame, route_joined)
    actual = summary.set_index("city")["services"].to_dict()
    if actual != EXPECTED_SERVICES:
        raise ValueError(f"Current Coverage totals changed: {actual}")
    if int(summary["services"].sum()) != 9280:
        raise ValueError("Current Coverage total must be 9280")
    for city_row in summary.itertuples(index=False):
        city_detail = detail[detail["city"].eq(city_row.city)]
        service_total = city_detail[city_detail["bucket"].isin(["DMS", "DMS2", "ASC"])]["services"].sum()
        if int(service_total) != int(city_row.services):
            raise ValueError(f"{city_row.city}: service bucket accounting changed")
        if int(city_detail["daily_sm_groups"].sum()) != int(city_row.assigned_sm_groups):
            raise ValueError(f"{city_row.city}: group bucket accounting changed")
    routed_count = int(detail["routed_groups"].sum())
    failed_count = int(detail["failed_groups"].sum())
    unmatched_count = int(detail["unmatched_groups"].sum())
    metadata_counts = route_metadata["counts"]
    if routed_count != int(metadata_counts["success_group_count"]) or failed_count != int(metadata_counts["failed_group_count"]):
        raise ValueError("Route-detail aggregates disagree with metadata counts")
    if routed_count + failed_count != len(groups) or unmatched_count != 0:
        raise ValueError("Route-detail success/failure/unmatched accounting changed")
    document = _render(summary, detail, service_file, route_joined, route_metadata)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(document, encoding="utf-8")
    print(f"validated: services={int(summary['services'].sum())}, groups={int(summary['assigned_sm_groups'].sum())}")
    print(f"report: {args.output}")


if __name__ == "__main__":
    main()
