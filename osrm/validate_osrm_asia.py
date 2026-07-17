from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pandas as pd


CITY_CONFIG = {
    "BANGKOK": {
        "url": "http://127.0.0.1:5003",
        "health_coord": (100.5018, 13.7563),
        "lat_range": (13.0, 14.5),
        "lon_range": (99.5, 101.5),
    },
    "JAKARTA": {
        "url": "http://127.0.0.1:5004",
        "health_coord": (106.8456, -6.2088),
        "lat_range": (-7.0, -5.5),
        "lon_range": (106.0, 107.5),
    },
    "KUALA LUMPUR": {
        "url": "http://127.0.0.1:5005",
        "health_coord": (101.6869, 3.1390),
        "lat_range": (2.5, 4.0),
        "lon_range": (101.0, 102.5),
    },
}


def _request_json(url: str) -> dict:
    with urlopen(url, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _validate_endpoint(city: str, config: dict) -> bool:
    lon, lat = config["health_coord"]
    base_url = str(config["url"]).rstrip("/")
    nearest_url = f"{base_url}/nearest/v1/driving/{lon},{lat}"
    route_url = f"{base_url}/route/v1/driving/{lon},{lat};{lon + 0.02},{lat + 0.02}?overview=false"
    try:
        nearest = _request_json(nearest_url)
        route = _request_json(route_url)
    except (OSError, URLError, ValueError) as exc:
        print(f"[FAIL] {city} endpoint: {exc}")
        return False
    ok = nearest.get("code") == "Ok" and route.get("code") == "Ok" and bool(route.get("routes"))
    print(f"[{'OK' if ok else 'FAIL'}] {city} endpoint: {base_url}")
    return ok


def _validate_service_file(path: Path) -> bool:
    if not path.exists():
        print(f"[FAIL] Service file not found: {path}")
        return False
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    required = {"STRATEGIC_CITY_NAME", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        print(f"[FAIL] Missing service columns: {sorted(missing)}")
        return False

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    all_ok = True
    for city, config in CITY_CONFIG.items():
        city_df = df[df["STRATEGIC_CITY_NAME"].astype(str).str.strip().str.upper() == city].copy()
        valid = city_df["latitude"].between(-90, 90) & city_df["longitude"].between(-180, 180)
        valid &= ~(city_df["latitude"].eq(0) & city_df["longitude"].eq(0))
        valid_df = city_df[valid]
        success_rate = (len(valid_df) / len(city_df) * 100.0) if len(city_df) else 0.0
        lat_min, lat_max = config["lat_range"]
        lon_min, lon_max = config["lon_range"]
        in_city_range = valid_df["latitude"].between(lat_min, lat_max) & valid_df["longitude"].between(lon_min, lon_max)
        in_range_rate = (int(in_city_range.sum()) / len(valid_df) * 100.0) if len(valid_df) else 0.0
        print(
            f"[{'OK' if success_rate >= 95.0 else 'FAIL'}] {city}: "
            f"rows={len(city_df)}, valid_coords={len(valid_df)} ({success_rate:.2f}%), "
            f"rough_city_range={int(in_city_range.sum())} ({in_range_rate:.2f}%)"
        )
        if success_rate < 95.0:
            all_ok = False
    return all_ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Southeast Asia service coordinates and OSRM endpoints.")
    parser.add_argument(
        "--service-file",
        default="260310/input/Service_202606161433_asia_census_only_geocoded.csv",
    )
    parser.add_argument("--skip-endpoints", action="store_true")
    args = parser.parse_args()

    service_ok = _validate_service_file(Path(args.service_file))
    endpoint_ok = True
    if not args.skip_endpoints:
        endpoint_ok = all(_validate_endpoint(city, config) for city, config in CITY_CONFIG.items())
    return 0 if service_ok and endpoint_ok else 1


if __name__ == "__main__":
    sys.exit(main())
