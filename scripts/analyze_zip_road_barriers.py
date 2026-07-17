from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import time

import pandas as pd
import requests


DEFAULT_CITY = "North Jersey, NJ"
DEFAULT_REGION_DIR = Path("260310/input/fixed_region_maps")
DEFAULT_OUTPUT_DIR = Path("260310/output")
DEFAULT_CONFIG_FILE = Path("config.json")


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def slugify_city_name(city_name: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in city_name)
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    dlat = lat2_r - lat1_r
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2.0) ** 2
    return float(2.0 * r * math.asin(math.sqrt(max(0.0, min(1.0, a)))))


def load_config(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def route_city_key(city_name: str) -> str:
    aliases = {
        "North Jersey, NJ": "Northeast",
        "Philadelphia, PA": "Northeast",
    }
    return aliases.get(city_name, city_name)


def resolve_osrm_url(config: dict, city_name: str) -> str:
    routing_cfg = config.get("routing", {})
    city_urls = routing_cfg.get("city_osrm_urls", {})
    key = route_city_key(city_name)
    return str(city_urls.get(key) or routing_cfg.get("osrm_url") or "").rstrip("/")


def fixed_region_path(region_dir: Path, city_name: str, region_count: int) -> Path:
    return region_dir / f"fixed_region_postal_{slugify_city_name(city_name)}_{int(region_count)}.csv"


def load_region_map(path: Path, city_name: str, region_count: int) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype={"POSTAL_CODE": str}, low_memory=False)
    df["POSTAL_CODE"] = df["POSTAL_CODE"].map(clean_text).str.zfill(5)
    df["STRATEGIC_CITY_NAME"] = df["STRATEGIC_CITY_NAME"].map(clean_text)
    df = df[df["STRATEGIC_CITY_NAME"].eq(city_name)].copy()
    df["region_seq"] = pd.to_numeric(df["region_seq"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["service_count"] = pd.to_numeric(df.get("service_count"), errors="coerce").fillna(0.0)
    df = df[df["region_seq"].notna() & df["latitude"].notna() & df["longitude"].notna()].copy()
    df["region_seq"] = df["region_seq"].astype(int)
    df["candidate_region_count"] = int(region_count)
    return df.drop_duplicates(subset=["POSTAL_CODE"], keep="first").reset_index(drop=True)


def build_candidate_pairs(region_df: pd.DataFrame, nearest_count: int, max_haversine_km: float) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for region_seq, group_df in region_df.groupby("region_seq", sort=True):
        points = group_df[["POSTAL_CODE", "latitude", "longitude", "service_count"]].to_dict("records")
        for source in points:
            distances: list[tuple[float, dict[str, object]]] = []
            for target in points:
                if source["POSTAL_CODE"] == target["POSTAL_CODE"]:
                    continue
                straight_km = haversine_km(
                    float(source["longitude"]),
                    float(source["latitude"]),
                    float(target["longitude"]),
                    float(target["latitude"]),
                )
                if 0.5 <= straight_km <= max_haversine_km:
                    distances.append((straight_km, target))
            for straight_km, target in sorted(distances, key=lambda item: item[0])[:nearest_count]:
                from_zip, to_zip = sorted([str(source["POSTAL_CODE"]), str(target["POSTAL_CODE"])])
                rows.append(
                    {
                        "region_seq": int(region_seq),
                        "from_zip": from_zip,
                        "to_zip": to_zip,
                        "from_latitude": float(source["latitude"]) if str(source["POSTAL_CODE"]) == from_zip else float(target["latitude"]),
                        "from_longitude": float(source["longitude"]) if str(source["POSTAL_CODE"]) == from_zip else float(target["longitude"]),
                        "to_latitude": float(target["latitude"]) if str(target["POSTAL_CODE"]) == to_zip else float(source["latitude"]),
                        "to_longitude": float(target["longitude"]) if str(target["POSTAL_CODE"]) == to_zip else float(source["longitude"]),
                        "haversine_km": round(float(straight_km), 4),
                    }
                )
    return pd.DataFrame(rows).drop_duplicates(subset=["from_zip", "to_zip"]).reset_index(drop=True)


def read_cache(cache_path: Path) -> pd.DataFrame:
    columns = [
        "from_zip",
        "to_zip",
        "from_latitude",
        "from_longitude",
        "to_latitude",
        "to_longitude",
        "osrm_km",
        "osrm_min",
        "osrm_status",
        "error_message",
        "updated_at",
    ]
    if not cache_path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(cache_path, encoding="utf-8-sig", dtype={"from_zip": str, "to_zip": str}, low_memory=False)
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    df["from_zip"] = df["from_zip"].map(clean_text).str.zfill(5)
    df["to_zip"] = df["to_zip"].map(clean_text).str.zfill(5)
    return df[columns].drop_duplicates(subset=["from_zip", "to_zip"], keep="last").reset_index(drop=True)


def save_cache(cache_path: Path, cache_df: pd.DataFrame) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_df.to_csv(cache_path, index=False, encoding="utf-8-sig")


def osrm_route(session: requests.Session, osrm_url: str, profile: str, row: pd.Series, timeout: int) -> dict[str, object]:
    coord_str = f"{row['from_longitude']},{row['from_latitude']};{row['to_longitude']},{row['to_latitude']}"
    url = f"{osrm_url}/route/v1/{profile}/{coord_str}?overview=false&steps=false&alternatives=false"
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return {
            "osrm_km": "",
            "osrm_min": "",
            "osrm_status": "ERROR",
            "error_message": str(exc)[:300],
        }
    routes = data.get("routes") or []
    if data.get("code") != "Ok" or not routes:
        return {
            "osrm_km": "",
            "osrm_min": "",
            "osrm_status": clean_text(data.get("code")) or "NO_ROUTE",
            "error_message": json.dumps(data, ensure_ascii=False)[:300],
        }
    route = routes[0]
    return {
        "osrm_km": round(float(route.get("distance", 0.0)) / 1000.0, 4),
        "osrm_min": round(float(route.get("duration", 0.0)) / 60.0, 4),
        "osrm_status": "OK",
        "error_message": "",
    }


def fill_osrm_cache(
    pairs_df: pd.DataFrame,
    cache_path: Path,
    osrm_url: str,
    profile: str,
    timeout: int,
    sleep_sec: float,
    limit: int | None,
) -> pd.DataFrame:
    cache_df = read_cache(cache_path)
    cached_keys = set(zip(cache_df["from_zip"].astype(str), cache_df["to_zip"].astype(str)))
    pending = pairs_df[~pairs_df.apply(lambda row: (str(row["from_zip"]), str(row["to_zip"])) in cached_keys, axis=1)].copy()
    if limit is not None and limit >= 0:
        pending = pending.head(limit).copy()
    print(f"osrm_cache_rows={len(cache_df)} pending_pairs={len(pending)}", flush=True)
    if pending.empty:
        return cache_df

    session = requests.Session()
    buffer: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(pending.iterrows(), start=1):
        result = osrm_route(session, osrm_url, profile, row, timeout)
        buffer.append(
            {
                "from_zip": str(row["from_zip"]).zfill(5),
                "to_zip": str(row["to_zip"]).zfill(5),
                "from_latitude": row["from_latitude"],
                "from_longitude": row["from_longitude"],
                "to_latitude": row["to_latitude"],
                "to_longitude": row["to_longitude"],
                **result,
                "updated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            }
        )
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        if idx % 200 == 0 or idx == len(pending):
            cache_df = pd.concat([cache_df, pd.DataFrame(buffer)], ignore_index=True)
            cache_df = cache_df.drop_duplicates(subset=["from_zip", "to_zip"], keep="last").reset_index(drop=True)
            save_cache(cache_path, cache_df)
            buffer = []
            print(f"osrm progress={idx}/{len(pending)}", flush=True)
    return read_cache(cache_path)


def build_report(
    region_df: pd.DataFrame,
    pairs_df: pd.DataFrame,
    cache_df: pd.DataFrame,
    region_count: int,
    detour_ratio_threshold: float,
    extra_km_threshold: float,
    output_dir: Path,
    city_slug: str,
) -> tuple[Path, pd.DataFrame, pd.DataFrame]:
    report = pairs_df.merge(cache_df, on=["from_zip", "to_zip"], how="left", suffixes=("", "_cache"))
    report["osrm_km"] = pd.to_numeric(report["osrm_km"], errors="coerce")
    report["osrm_min"] = pd.to_numeric(report["osrm_min"], errors="coerce")
    report["detour_ratio"] = report["osrm_km"] / report["haversine_km"].replace(0, pd.NA)
    report["extra_km"] = report["osrm_km"] - report["haversine_km"]
    report["barrier_flag"] = (
        report["osrm_status"].astype(str).ne("OK")
        | report["detour_ratio"].ge(detour_ratio_threshold)
        | report["extra_km"].ge(extra_km_threshold)
    )
    report = report.sort_values(["barrier_flag", "detour_ratio", "extra_km"], ascending=[False, False, False]).reset_index(drop=True)
    report_path = output_dir / f"{city_slug}_region_barrier_report_{region_count}.csv"
    report.to_csv(report_path, index=False, encoding="utf-8-sig")

    summary = (
        report.groupby("region_seq")
        .agg(
            candidate_pairs=("from_zip", "count"),
            barrier_pairs=("barrier_flag", "sum"),
            max_detour_ratio=("detour_ratio", "max"),
            avg_detour_ratio=("detour_ratio", "mean"),
            max_extra_km=("extra_km", "max"),
            avg_extra_km=("extra_km", "mean"),
        )
        .reset_index()
    )
    summary.insert(0, "candidate_region_count", int(region_count))
    region_stats = (
        region_df.groupby("region_seq")
        .agg(region_postals=("POSTAL_CODE", "nunique"), region_service_count=("service_count", "sum"))
        .reset_index()
    )
    summary = summary.merge(region_stats, on="region_seq", how="left")
    summary["barrier_pair_ratio"] = (summary["barrier_pairs"] / summary["candidate_pairs"].replace(0, pd.NA) * 100.0).round(2)
    for col in ["max_detour_ratio", "avg_detour_ratio", "max_extra_km", "avg_extra_km", "region_service_count"]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").round(2)
    return report_path, report, summary


def run(args: argparse.Namespace) -> None:
    config = load_config(Path(args.config_file))
    city_name = str(args.city)
    city_slug = slugify_city_name(city_name)
    osrm_url = str(args.osrm_url or resolve_osrm_url(config, city_name)).rstrip("/")
    if not osrm_url:
        raise ValueError("OSRM URL is missing. Set config routing.city_osrm_urls or pass --osrm-url.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = Path(args.cache_file) if args.cache_file else output_dir / f"{city_slug}_zip_road_barrier_matrix.csv"

    all_pairs: list[pd.DataFrame] = []
    region_frames: dict[int, pd.DataFrame] = {}
    for region_count in args.region_counts:
        path = fixed_region_path(Path(args.region_dir), city_name, int(region_count))
        if not path.exists():
            raise FileNotFoundError(path)
        region_df = load_region_map(path, city_name, int(region_count))
        region_frames[int(region_count)] = region_df
        pairs = build_candidate_pairs(region_df, int(args.nearest_count), float(args.max_haversine_km))
        pairs["candidate_region_count"] = int(region_count)
        all_pairs.append(pairs)
        print(f"region_count={region_count} candidate_pairs={len(pairs)}", flush=True)

    combined_pairs = pd.concat(all_pairs, ignore_index=True)
    unique_pairs = combined_pairs.drop_duplicates(subset=["from_zip", "to_zip"]).reset_index(drop=True)
    print(f"unique_osrm_pairs={len(unique_pairs)} osrm_url={osrm_url}", flush=True)
    cache_df = fill_osrm_cache(
        unique_pairs,
        cache_path,
        osrm_url,
        str(config.get("routing", {}).get("osrm_profile", "driving")),
        int(args.timeout_sec),
        float(args.sleep_sec),
        args.limit,
    )

    summaries: list[pd.DataFrame] = []
    for region_count, region_df in region_frames.items():
        pairs = combined_pairs[combined_pairs["candidate_region_count"].eq(region_count)].drop(columns=["candidate_region_count"])
        report_path, report, summary = build_report(
            region_df,
            pairs,
            cache_df,
            region_count,
            float(args.detour_ratio_threshold),
            float(args.extra_km_threshold),
            output_dir,
            city_slug,
        )
        summaries.append(summary)
        print(f"report={report_path} barrier_pairs={int(report['barrier_flag'].sum())}/{len(report)}", flush=True)
        top_cols = ["region_seq", "from_zip", "to_zip", "haversine_km", "osrm_km", "detour_ratio", "extra_km", "osrm_min"]
        print(report[report["barrier_flag"]].head(10)[top_cols].round(2).to_string(index=False), flush=True)

    summary_df = pd.concat(summaries, ignore_index=True)
    summary_path = output_dir / f"{city_slug}_region_barrier_summary.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    print(f"cache_file={cache_path}", flush=True)
    print(f"summary_file={summary_path}", flush=True)
    print(summary_df.to_string(index=False), flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze road-network barrier pairs inside fixed ZIP regions.")
    parser.add_argument("--city", default=DEFAULT_CITY)
    parser.add_argument("--region-counts", type=int, nargs="+", default=[4, 5, 6])
    parser.add_argument("--region-dir", default=str(DEFAULT_REGION_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--config-file", default=str(DEFAULT_CONFIG_FILE))
    parser.add_argument("--cache-file", default="")
    parser.add_argument("--osrm-url", default="")
    parser.add_argument("--nearest-count", type=int, default=8)
    parser.add_argument("--max-haversine-km", type=float, default=35.0)
    parser.add_argument("--detour-ratio-threshold", type=float, default=2.5)
    parser.add_argument("--extra-km-threshold", type=float, default=12.0)
    parser.add_argument("--timeout-sec", type=int, default=20)
    parser.add_argument("--sleep-sec", type=float, default=0.0)
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
