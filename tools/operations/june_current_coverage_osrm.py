"""Calculate June 2026 Current Coverage road-route metrics without exporting PII.

This reproduces the route construction used by ``sr_area_map.py`` for the
Current Coverage view, with one deliberate operational difference: an OSRM
failure stays a failed route with null metrics.  It is never replaced with the
UI client's Haversine fallback, which would make a road-network aggregate look
complete when it is not.

The detail output intentionally contains no customer/technician identifiers,
addresses, or coordinates.  A report consumer can reproduce ``route_group_id``
from the source using the documented SHA-256 canonicalisation in metadata.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from smart_routing.census_geocoder import load_geocode_cache, merge_service_with_geocodes
from smart_routing.data_catalog import na_data_path


SOURCE_FILE = PROJECT_ROOT / "260310/input/Service_202607071543_normalized_geocoded.csv"
CONFIG_FILE = PROJECT_ROOT / "config/config.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "exports/current_coverage_june_2026_osrm"
DEFAULT_CACHE_FILE = PROJECT_ROOT / "data/cache/current_coverage_june_2026_osrm_cache.json"
CHECKPOINT_FILENAME = "route_checkpoint.json"
ALGORITHM_VERSION = "current-coverage-osrm/v2-zero-route-guard-fingerprint"
TABLE_OPTIONS = {"annotations": "distance,duration"}
ROUTE_OPTIONS = {"overview": "full", "geometries": "geojson", "steps": False, "alternatives": True}
DIRECT_VALIDATION_OPTIONS = {"overview": False, "steps": False, "alternatives": False}

TARGET_CITIES = (
    "Atlanta, GA",
    "Los Angeles, CA",
    "North Jersey, NJ",
    "Philadelphia, PA",
    "San Diego, CA",
    "Washington, DC",
)
ROUTE_CITY_ALIASES = {
    "North Jersey, NJ": "Northeast",
    "Philadelphia, PA": "Northeast",
}
DEFAULT_CITY_OSRM_URLS = {
    "Los Angeles, CA": "http://20.51.244.68:5001",
    "Atlanta, GA": "http://20.51.244.68:5002",
}

DATE_CANDIDATES = (
    "PROMISE_DATE",
    "REPAIR_END_DATE_YYYYMMDD",
    "REPAIR_RECEIPT_DATE_YYYYMMDD",
    "GERP_INPUT_DATE_YYYYMMDD_ID_LAST",
)
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
_THREAD_LOCAL = threading.local()


@dataclass(frozen=True)
class Endpoint:
    city: str
    route_city_key: str
    endpoint_url: str
    endpoint_id: str
    profile: str


@dataclass(frozen=True)
class RouteTask:
    city: str
    service_date_key: str
    assigned_sm_code: str
    route_group_id: str
    route_bucket: str
    input_row_count: int
    service_count: int
    unique_service_stop_count: int
    coords: tuple[tuple[float, float], ...]
    home_found: bool
    endpoint: Endpoint


class OSRMFailure(RuntimeError):
    """A safe-to-export OSRM failure category (never contains coordinates)."""


def _ui_normalize_text(value: object) -> str:
    """Mirror smart_routing.area_map._normalize_text for group identity."""
    text = "" if pd.isna(value) else str(value)
    return " ".join(text.replace("\r", " ").replace("\n", " ").split()).strip()


def _route_group_id(city: str, service_date_key: str, assigned_sm_code: str) -> str:
    canonical = "\x1f".join(
        (_ui_normalize_text(city), str(service_date_key).strip(), _ui_normalize_text(assigned_sm_code))
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sha256_file(path: Path, retries: int = 5) -> str:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except OSError as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(0.5 * (2**attempt))
    raise RuntimeError(f"Unable to read source for SHA-256 after {retries} attempts: {last_error}")


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(resolved)


def _file_dependency(path: Path, role: str) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.exists():
        return {"role": role, "path": _display_path(resolved), "exists": False, "size": None, "mtime_ns": None, "sha256": None}
    stat = resolved.stat()
    return {
        "role": role,
        "path": _display_path(resolved),
        "exists": True,
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": _sha256_file(resolved),
    }


def _canonical_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_config() -> dict[str, Any]:
    return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))


def _build_endpoints(routing_cfg: dict[str, Any]) -> dict[str, Endpoint]:
    configured = routing_cfg.get("city_osrm_urls", {})
    urls = dict(DEFAULT_CITY_OSRM_URLS)
    if isinstance(configured, dict):
        urls.update({str(key): str(value).rstrip("/") for key, value in configured.items() if str(value).strip()})
    profile = str(routing_cfg.get("osrm_profile", "driving")).strip() or "driving"
    endpoints: dict[str, Endpoint] = {}
    for city in TARGET_CITIES:
        route_city_key = ROUTE_CITY_ALIASES.get(city, city)
        endpoint_url = str(urls.get(route_city_key, "")).rstrip("/")
        if not endpoint_url:
            raise ValueError(f"No city OSRM endpoint configured for {city!r} (route key {route_city_key!r})")
        endpoint_id = route_city_key.lower().replace(",", "").replace(" ", "_")
        endpoints[city] = Endpoint(city, route_city_key, endpoint_url, endpoint_id, profile)
    return endpoints


def _load_current_coverage_service(source_file: Path) -> pd.DataFrame:
    """Mirror area_map.load_service_points for the configured Current Coverage source."""
    df = pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)
    required = {
        "STRATEGIC_CITY_NAME",
        "POSTAL_CODE",
        "GSFS_RECEIPT_NO",
        "SVC_ENGINEER_CODE",
        "latitude",
        "longitude",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Source is not a valid Current Coverage service file; missing columns: {missing}")
    if "SVC_CENTER_TYPE" not in df.columns and "SERVICE_CENTER_TYPE" in df.columns:
        df["SVC_CENTER_TYPE"] = df["SERVICE_CENTER_TYPE"]
    for column in (
        "SVC_ENGINEER_CODE",
        "SVC_ENGINEER_NAME",
        "STRATEGIC_CITY_NAME",
        "GSFS_RECEIPT_NO",
        "POSTAL_CODE",
        "ADDRESS_LINE1_INFO",
        "SVC_CENTER_TYPE",
        "SVC_RECEIPT_TYPE",
        "SVC_TYPE_CODE",
        "source",
    ):
        if column in df.columns:
            df[column] = df[column].map(_ui_normalize_text)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    for date_column in DATE_CANDIDATES:
        if date_column not in df.columns:
            continue
        date_text = df[date_column].astype(str).str.replace(r"\.0+$", "", regex=True).str.strip()
        df["service_date"] = pd.to_datetime(date_text, format="%Y%m%d", errors="coerce")
        if df["service_date"].notna().any():
            break
    if "service_date" not in df.columns:
        raise ValueError(f"None of the UI date columns exist in source: {DATE_CANDIDATES}")
    if "source" in df.columns:
        df = df[df["source"].astype(str).str.strip().ne("failed")].copy()
    df = df[df["latitude"].notna() & df["longitude"].notna() & df["service_date"].notna()].copy()
    df = df[df["STRATEGIC_CITY_NAME"].isin(TARGET_CITIES)].copy()
    df["service_date_key"] = df["service_date"].dt.strftime("%Y-%m-%d")
    if df.empty:
        raise ValueError("No valid target-city Current Coverage service rows remain after UI-equivalent filters")
    return df


def _normalise_center_bucket(center_type: object) -> str:
    value = _ui_normalize_text(center_type).upper()
    return value if value in {"DMS", "DMS2"} else "ASC"


def _classify_group_bucket(values: Iterable[object]) -> str:
    buckets = sorted({_normalise_center_bucket(value) for value in values})
    return buckets[0] if len(buckets) == 1 else ("MIXED" if buckets else "UNKNOWN")


def _home_lookup_from_frame(home_df: pd.DataFrame) -> dict[str, tuple[float, float]]:
    if home_df.empty:
        return {}
    code_column = "SVC_ENGINEER_CODE" if "SVC_ENGINEER_CODE" in home_df.columns else "GSFS_RECEIPT_NO"
    working = home_df.copy()
    working["latitude"] = pd.to_numeric(working.get("latitude"), errors="coerce")
    working["longitude"] = pd.to_numeric(working.get("longitude"), errors="coerce")
    working = working.dropna(subset=["latitude", "longitude"])
    lookup: dict[str, tuple[float, float]] = {}
    for _, row in working.iterrows():
        code = _ui_normalize_text(row.get(code_column, ""))
        if code:
            # Dict assignment deliberately follows the UI comprehension: last row wins.
            lookup[code] = (float(row["longitude"]), float(row["latitude"]))
    return lookup


def _saved_home_geocode_path(city: str) -> Path:
    region_seed_dir = na_data_path("region_seed_dir")
    city_slug = city.split(",", 1)[0].lower().strip().replace(" ", "_")
    return (region_seed_dir / f"{city_slug}_engineer_home_geocoded.csv").resolve()


def _load_saved_home_geocodes(city: str) -> pd.DataFrame:
    path = _saved_home_geocode_path(city)
    if not path.exists():
        return pd.DataFrame()
    home_df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    rename_map = {
        "GSFS_RECEIPT_NO": "SVC_ENGINEER_CODE",
        "ADDRESS_LINE1_INFO": "Home Street Address",
        "CITY_NAME": "City ",
        "STATE_NAME": "State",
        "POSTAL_CODE": "Zip",
    }
    home_df = home_df.rename(columns={key: value for key, value in rename_map.items() if key in home_df.columns})
    required = {"SVC_ENGINEER_CODE", "Home Street Address", "latitude", "longitude"}
    if not required.issubset(home_df.columns):
        return pd.DataFrame()
    home_df["SVC_ENGINEER_CODE"] = home_df["SVC_ENGINEER_CODE"].astype(str).str.strip()
    return home_df


def _load_home_lookup_by_city(service_df: pd.DataFrame, config: dict[str, Any]) -> tuple[dict[str, dict[str, tuple[float, float]]], dict[str, Any]]:
    """Mirror sr_area_map.get_home_location_lookup without any geocoding write/call."""
    profile_file = Path(str(config["area_map_usa"]["profile_file"]))
    if not profile_file.is_absolute():
        profile_file = PROJECT_ROOT / profile_file
    profile_file = profile_file.resolve()
    address_df = pd.read_excel(profile_file, sheet_name="4. Address")
    required = {"SVC_ENGINEER_CODE", "Home Street Address", "latitude", "longitude"}
    if not required.issubset(address_df.columns):
        raise ValueError(f"Profile home sheet lacks required columns: {sorted(required - set(address_df.columns))}")
    address_df = address_df.copy()
    address_df["SVC_ENGINEER_CODE"] = address_df["SVC_ENGINEER_CODE"].astype(str).str.strip()

    # The UI uses this exact cache precedence for missing home coordinates.
    geocoding_cfg = config.get("geocoding", {})
    census_cache = (PROJECT_ROOT / str(geocoding_cfg.get("census_cache_file", "data/geocode_cache_us_census.csv"))).resolve()
    google_cache = (PROJECT_ROOT / str(geocoding_cfg.get("google_cache_file", "data/geocode_cache_google.csv"))).resolve()
    cache_frames = [load_geocode_cache(census_cache)]
    if google_cache.exists():
        cache_frames.append(load_geocode_cache(google_cache))
    cached_geocodes = pd.concat(cache_frames, ignore_index=True).drop_duplicates(subset=["address_key"], keep="first")

    output: dict[str, dict[str, tuple[float, float]]] = {}
    home_metadata: dict[str, Any] = {
        "profile_file": _display_path(profile_file),
        "profile_sha256": _sha256_file(profile_file),
        "home_lookup_precedence": ["profile_sheet_4_address", "saved_city_home_geocode", "census_then_google_geocode_cache"],
        "remote_geocoding_called": False,
        "dependencies": [
            _file_dependency(profile_file, "home_profile"),
            _file_dependency(census_cache, "census_geocode_cache"),
            _file_dependency(google_cache, "google_geocode_cache"),
            _file_dependency(PROJECT_ROOT / "config/data_catalog.json", "north_america_data_catalog"),
            *[
                _file_dependency(_saved_home_geocode_path(city), f"saved_city_home_geocode:{city}")
                for city in TARGET_CITIES
            ],
        ],
    }
    for city in TARGET_CITIES:
        city_codes = {
            _ui_normalize_text(value)
            for value in service_df.loc[service_df["STRATEGIC_CITY_NAME"].eq(city), "SVC_ENGINEER_CODE"]
            if _ui_normalize_text(value)
        }
        profile_lookup = _home_lookup_from_frame(
            address_df[address_df["SVC_ENGINEER_CODE"].isin(city_codes)].copy()
        )
        lookup = dict(profile_lookup)
        saved_lookup = _home_lookup_from_frame(_load_saved_home_geocodes(city))
        for code, coord in saved_lookup.items():
            lookup.setdefault(code, coord)
        missing_codes = city_codes - set(lookup)
        if missing_codes:
            address_required = {"SVC_ENGINEER_CODE", "Home Street Address", "City ", "State", "Zip"}
            if address_required.issubset(address_df.columns):
                home_input = address_df[address_df["SVC_ENGINEER_CODE"].isin(missing_codes)].copy()
                home_input = home_input.rename(
                    columns={
                        "SVC_ENGINEER_CODE": "GSFS_RECEIPT_NO",
                        "Home Street Address": "ADDRESS_LINE1_INFO",
                        "City ": "CITY_NAME",
                        "State": "STATE_NAME",
                        "Zip": "POSTAL_CODE",
                    }
                )
                if not home_input.empty:
                    home_input["CITY_NAME"] = home_input["CITY_NAME"].fillna("").astype(str).str.strip()
                    city_state_mask = home_input["CITY_NAME"].eq("") & home_input["STATE_NAME"].astype(str).str.contains(",", na=False)
                    if city_state_mask.any():
                        split = home_input.loc[city_state_mask, "STATE_NAME"].map(
                            lambda value: tuple(str(value).strip().split(",", 1))
                        )
                        home_input.loc[city_state_mask, "CITY_NAME"] = split.map(lambda value: value[0].strip())
                        home_input.loc[city_state_mask, "STATE_NAME"] = split.map(lambda value: value[1].strip())
                    home_input["COUNTRY_NAME"] = "USA"
                    merged = merge_service_with_geocodes(home_input, cached_geocodes)
                    lookup.update(_home_lookup_from_frame(merged))
        output[city] = lookup
    return output, home_metadata


def _build_computation_fingerprint(
    *,
    source_file: Path,
    endpoints: dict[str, Endpoint],
    home_metadata: dict[str, Any],
    map_version: str,
) -> dict[str, Any]:
    """Bind resumable state to every input and semantic that can change results."""
    payload: dict[str, Any] = {
        "algorithm_version": ALGORITHM_VERSION,
        "script": _file_dependency(Path(__file__), "calculation_script"),
        "config": _file_dependency(CONFIG_FILE, "runtime_config"),
        "source": _file_dependency(source_file, "service_source"),
        "home_dependencies": home_metadata["dependencies"],
        "endpoints": [
            {
                "city": endpoint.city.strip(),
                "route_city_key": endpoint.route_city_key.strip(),
                "endpoint_url": endpoint.endpoint_url.rstrip("/"),
                "endpoint_id": endpoint.endpoint_id,
                "profile": endpoint.profile.strip(),
            }
            for endpoint in sorted(endpoints.values(), key=lambda item: item.city)
        ],
        "map_version": str(map_version).strip(),
        "request_options": {
            "table": TABLE_OPTIONS,
            "route": ROUTE_OPTIONS,
            "direct_zero_validation": DIRECT_VALIDATION_OPTIONS,
        },
        "semantics": {
            "coordinate_order": "longitude,latitude",
            "distance_unit": "km",
            "duration_unit": "min",
            "ordering": "directed OSRM Table distance nearest-neighbor; fixed index 0 when home exists",
            "return_home": False,
            "fallback": "none",
            "single_stop_zero_rule": "0/0 success only for exactly one unique service stop and no home",
            "date_candidates": list(DATE_CANDIDATES),
            "grouping": ["STRATEGIC_CITY_NAME", "service_date_key", "SVC_ENGINEER_CODE"],
            "home_lookup_precedence": home_metadata["home_lookup_precedence"],
        },
    }
    return {"sha256": _canonical_sha256(payload), "payload": payload}


def _build_tasks(service_df: pd.DataFrame, endpoints: dict[str, Endpoint], homes: dict[str, dict[str, tuple[float, float]]]) -> list[RouteTask]:
    tasks: list[RouteTask] = []
    grouped = service_df.groupby(["STRATEGIC_CITY_NAME", "service_date_key", "SVC_ENGINEER_CODE"], sort=True, dropna=False)
    for (city, date_key, sm_code), group_df in grouped:
        city_text = _ui_normalize_text(city)
        sm_text = _ui_normalize_text(sm_code)
        # UI: select lon/lat, drop nulls, drop duplicate pairs, preserve source order.
        service_coords = tuple(
            (float(row.longitude), float(row.latitude))
            for row in group_df[["longitude", "latitude"]].dropna().drop_duplicates().itertuples(index=False)
        )
        if not service_coords:
            # This cannot occur after load_service_points filtering; preserve a visible failure if invariants change.
            raise ValueError(f"No valid coordinates for generated group {city_text}/{date_key}")
        home_coord = homes.get(city_text, {}).get(sm_text)
        coords = ((home_coord,) + service_coords) if home_coord is not None else service_coords
        service_count = int(group_df["GSFS_RECEIPT_NO"].astype(str).nunique())
        tasks.append(
            RouteTask(
                city=city_text,
                service_date_key=str(date_key),
                assigned_sm_code=sm_text,
                route_group_id=_route_group_id(city_text, str(date_key), sm_text),
                route_bucket=_classify_group_bucket(group_df.get("SVC_CENTER_TYPE", pd.Series("", index=group_df.index))),
                input_row_count=int(len(group_df)),
                service_count=service_count,
                unique_service_stop_count=len(service_coords),
                coords=coords,
                home_found=home_coord is not None,
                endpoint=endpoints[city_text],
            )
        )
    return tasks


def _session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": "smart-routing-current-coverage-audit/1.0"})
        _THREAD_LOCAL.session = session
    return session


def _request_json(url: str, timeout_seconds: float, max_attempts: int) -> tuple[dict[str, Any], int, float]:
    start = time.perf_counter()
    last_error: str = "request_not_attempted"
    for attempt in range(1, max_attempts + 1):
        try:
            response = _session().get(url, timeout=timeout_seconds)
            if response.status_code in RETRYABLE_HTTP_STATUS:
                last_error = f"http_{response.status_code}"
                if attempt < max_attempts:
                    time.sleep(0.35 * (2 ** (attempt - 1)))
                    continue
                raise OSRMFailure(last_error)
            if not response.ok:
                try:
                    engine_code = _ui_normalize_text(response.json().get("code", "")).lower()
                except ValueError:
                    engine_code = ""
                raise OSRMFailure(f"http_{response.status_code}" + (f"_{engine_code}" if engine_code else ""))
            try:
                payload = response.json()
            except ValueError as exc:
                raise OSRMFailure("invalid_json") from exc
            return payload, attempt, (time.perf_counter() - start) * 1000.0
        except requests.Timeout:
            last_error = "timeout"
        except requests.RequestException:
            last_error = "connection_error"
        except OSRMFailure:
            raise
        if attempt < max_attempts:
            time.sleep(0.35 * (2 ** (attempt - 1)))
    raise OSRMFailure(last_error)


def _coord_string(coords: tuple[tuple[float, float], ...]) -> str:
    # OSRM uses longitude,latitude; this is deliberately never exported.
    return ";".join(f"{longitude},{latitude}" for longitude, latitude in coords)


def _validate_matrix(matrix: Any, expected_size: int) -> list[list[float]]:
    if not isinstance(matrix, list) or len(matrix) != expected_size:
        raise OSRMFailure("table_invalid_shape")
    output: list[list[float]] = []
    for row in matrix:
        if not isinstance(row, list) or len(row) != expected_size:
            raise OSRMFailure("table_invalid_shape")
        values: list[float] = []
        for value in row:
            if value is None:
                raise OSRMFailure("table_unroutable")
            try:
                numeric = float(value)
            except (TypeError, ValueError) as exc:
                raise OSRMFailure("table_non_numeric") from exc
            if not math.isfinite(numeric) or numeric < 0:
                raise OSRMFailure("table_invalid_value")
            values.append(numeric)
        output.append(values)
    return output


def _nearest_neighbor_order(distance_m: list[list[float]], fixed_start_idx: int | None) -> list[int]:
    """Exactly match OSRMTripClient._nearest_neighbor_order."""
    size = len(distance_m)
    if size <= 2:
        return list(range(size))
    if fixed_start_idx is not None and 0 <= fixed_start_idx < size:
        remaining = set(range(size))
        remaining.remove(fixed_start_idx)
        order = [fixed_start_idx]
        while remaining:
            last = order[-1]
            next_idx = min(remaining, key=lambda idx: float(distance_m[last][idx]))
            order.append(next_idx)
            remaining.remove(next_idx)
        return order
    best_order: list[int] | None = None
    best_total = float("inf")
    for start_idx in range(size):
        remaining = set(range(size))
        remaining.remove(start_idx)
        order = [start_idx]
        total = 0.0
        while remaining:
            last = order[-1]
            next_idx = min(remaining, key=lambda idx: float(distance_m[last][idx]))
            total += float(distance_m[last][next_idx])
            order.append(next_idx)
            remaining.remove(next_idx)
        if total < best_total:
            best_total = total
            best_order = order
    return best_order or list(range(size))


def _cache_identity(task: RouteTask, map_version: str, computation_fingerprint_sha256: str) -> str:
    payload = {
        "algorithm_version": ALGORITHM_VERSION,
        "computation_fingerprint_sha256": computation_fingerprint_sha256,
        "coordinates_lon_lat": [[lon, lat] for lon, lat in task.coords],
        "endpoint_url": task.endpoint.endpoint_url,
        "map_version": map_version,
        "profile": task.endpoint.profile,
        "table_options": TABLE_OPTIONS,
        "route_options": ROUTE_OPTIONS,
        "preserve_first": task.home_found,
        "distance_unit": "km",
        "duration_unit": "min",
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _zero_route_success_allowed(task: RouteTask) -> bool:
    return task.unique_service_stop_count == 1 and not task.home_found


def _is_zero_route(distance_km: float, duration_min: float) -> bool:
    return distance_km == 0.0 and duration_min == 0.0


def _result_base(task: RouteTask, cache_identity: str) -> dict[str, Any]:
    return {
        "city": task.city,
        "service_date_key": task.service_date_key,
        "route_group_id": task.route_group_id,
        "route_bucket": task.route_bucket,
        "input_row_count": task.input_row_count,
        "service_count": task.service_count,
        "unique_service_stop_count": task.unique_service_stop_count,
        "route_coordinate_count": len(task.coords),
        "home_found": task.home_found,
        "start_policy": "home_fixed_start" if task.home_found else "nearest_neighbor_best_start",
        "end_policy": "last_service_stop_no_home_return",
        "backend": "osrm_road_network",
        "endpoint_id": task.endpoint.endpoint_id,
        "profile": task.endpoint.profile,
        "cache_identity": cache_identity,
    }


def _route_task(
    task: RouteTask,
    *,
    map_version: str,
    computation_fingerprint_sha256: str,
    timeout_seconds: float,
    max_attempts: int,
    cache_entries: dict[str, Any],
    allow_cache_read: bool,
) -> dict[str, Any]:
    identity = _cache_identity(task, map_version, computation_fingerprint_sha256)
    base = _result_base(task, identity)
    cached = cache_entries.get(identity) if allow_cache_read else None
    if isinstance(cached, dict) and cached.get("status") == "success":
        cached_distance = float(cached["distance_km"])
        cached_duration = float(cached["duration_min"])
        if _is_zero_route(cached_distance, cached_duration) and not _zero_route_success_allowed(task):
            cached = None
    if isinstance(cached, dict) and cached.get("status") == "success":
        return {
            **base,
            "status": "success",
            "failure_reason": "",
            "distance_km": cached_distance,
            "duration_min": cached_duration,
            "cache_hit": True,
            "request_attempts": 0,
            "request_elapsed_ms": 0.0,
        }

    # UI behavior for a sole service coordinate with no home: no network request and zero route length.
    if len(task.coords) == 1 and _zero_route_success_allowed(task):
        return {
            **base,
            "status": "success",
            "failure_reason": "",
            "distance_km": 0.0,
            "duration_min": 0.0,
            "cache_hit": False,
            "request_attempts": 0,
            "request_elapsed_ms": 0.0,
        }

    try:
        coord_str = _coord_string(task.coords)
        table_url = (
            f"{task.endpoint.endpoint_url}/table/v1/{task.endpoint.profile}/{coord_str}"
            "?annotations=distance,duration"
        )
        table_payload, table_attempts, table_elapsed = _request_json(table_url, timeout_seconds, max_attempts)
        table_code = str(table_payload.get("code", ""))
        if table_code != "Ok":
            raise OSRMFailure(f"table_{table_code.lower() or 'unknown'}")
        distance_m = _validate_matrix(table_payload.get("distances"), len(task.coords))
        _validate_matrix(table_payload.get("durations"), len(task.coords))
        order = _nearest_neighbor_order(distance_m, fixed_start_idx=0 if task.home_found else None)
        ordered_coords = tuple(task.coords[index] for index in order)
        route_url = (
            f"{task.endpoint.endpoint_url}/route/v1/{task.endpoint.profile}/{_coord_string(ordered_coords)}"
            "?overview=full&geometries=geojson&steps=false&alternatives=true"
        )
        route_payload, route_attempts, route_elapsed = _request_json(route_url, timeout_seconds, max_attempts)
        route_code = str(route_payload.get("code", ""))
        routes = route_payload.get("routes", [])
        if route_code != "Ok" or not isinstance(routes, list) or not routes:
            raise OSRMFailure(f"route_{route_code.lower() or 'unknown'}")
        selected = routes[0]  # UI selects the first alternative unless configured avoid polygons are active.
        distance_km = float(selected.get("distance", float("nan"))) / 1000.0
        duration_min = float(selected.get("duration", float("nan"))) / 60.0
        if not (math.isfinite(distance_km) and distance_km >= 0 and math.isfinite(duration_min) and duration_min >= 0):
            raise OSRMFailure("route_invalid_metric")
        if _is_zero_route(distance_km, duration_min) and not _zero_route_success_allowed(task):
            validation_url = (
                f"{task.endpoint.endpoint_url}/route/v1/{task.endpoint.profile}/{_coord_string(ordered_coords)}"
                "?overview=false&steps=false&alternatives=false"
            )
            try:
                validation_payload, validation_attempts, validation_elapsed = _request_json(
                    validation_url, timeout_seconds, max_attempts
                )
            except OSRMFailure as exc:
                raise OSRMFailure(f"zero_route_validation_{exc}") from exc
            validation_code = str(validation_payload.get("code", ""))
            validation_routes = validation_payload.get("routes", [])
            if validation_code != "Ok" or not isinstance(validation_routes, list) or not validation_routes:
                raise OSRMFailure(f"zero_route_validation_{validation_code.lower() or 'unknown'}")
            validation_route = validation_routes[0]
            distance_km = float(validation_route.get("distance", float("nan"))) / 1000.0
            duration_min = float(validation_route.get("duration", float("nan"))) / 60.0
            if not (math.isfinite(distance_km) and distance_km >= 0 and math.isfinite(duration_min) and duration_min >= 0):
                raise OSRMFailure("zero_route_validation_invalid_metric")
            if _is_zero_route(distance_km, duration_min):
                raise OSRMFailure("zero_route_for_distinct_coordinates")
            route_attempts += validation_attempts
            route_elapsed += validation_elapsed
        return {
            **base,
            "status": "success",
            "failure_reason": "",
            "distance_km": distance_km,
            "duration_min": duration_min,
            "cache_hit": False,
            "request_attempts": table_attempts + route_attempts,
            "request_elapsed_ms": table_elapsed + route_elapsed,
        }
    except OSRMFailure as exc:
        return {
            **base,
            "status": "failed",
            "failure_reason": str(exc),
            "distance_km": None,
            "duration_min": None,
            "cache_hit": False,
            "request_attempts": None,
            "request_elapsed_ms": None,
        }
    except (TypeError, ValueError):
        return {
            **base,
            "status": "failed",
            "failure_reason": "response_parse_error",
            "distance_km": None,
            "duration_min": None,
            "cache_hit": False,
            "request_attempts": None,
            "request_elapsed_ms": None,
        }


def _load_cache(cache_file: Path) -> dict[str, Any]:
    if not cache_file.exists():
        return {}
    try:
        payload = json.loads(cache_file.read_text(encoding="utf-8"))
        entries = payload.get("entries", {})
        return entries if isinstance(entries, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_cache(cache_file: Path, entries: dict[str, Any]) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    safe_entries = {
        key: {
            "status": row["status"],
            "distance_km": row["distance_km"],
            "duration_min": row["duration_min"],
        }
        for key, row in entries.items()
        if row.get("status") == "success"
    }
    temp_file = cache_file.with_suffix(f"{cache_file.suffix}.tmp")
    temp_file.write_text(json.dumps({"schema": "current-coverage-osrm-cache/v1", "entries": safe_entries}, sort_keys=True), encoding="utf-8")
    os.replace(temp_file, cache_file)


def _load_checkpoint(checkpoint_file: Path, computation_fingerprint: dict[str, Any]) -> list[dict[str, Any]]:
    """Load a deliberately requested checkpoint after strict provenance checks."""
    if not checkpoint_file.exists():
        raise FileNotFoundError(f"Checkpoint requested but absent: {checkpoint_file}")
    payload = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    if payload.get("schema") != "current-coverage-osrm-checkpoint/v2":
        raise ValueError("Unsupported checkpoint schema")
    if payload.get("computation_fingerprint") != computation_fingerprint:
        raise ValueError("Checkpoint computation fingerprint mismatch")
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError("Checkpoint rows are invalid")
    return [row for row in rows if isinstance(row, dict)]


def _save_checkpoint(checkpoint_file: Path, computation_fingerprint: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "current-coverage-osrm-checkpoint/v2",
        "computation_fingerprint": computation_fingerprint,
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
    }
    temporary = checkpoint_file.with_suffix(f"{checkpoint_file.suffix}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    os.replace(temporary, checkpoint_file)


def _publish_artifact_directory(staging_dir: Path, output_dir: Path) -> None:
    """Publish a complete generation as one directory swap, restoring on failure."""
    parent = output_dir.parent.resolve()
    staging = staging_dir.resolve()
    output = output_dir.resolve()
    staging.relative_to(parent)
    output.relative_to(parent)
    backup = parent / f".{output.name}.backup-{os.getpid()}-{time.time_ns()}"
    moved_existing = False
    try:
        if output.exists():
            os.replace(output, backup)
            moved_existing = True
        os.replace(staging, output)
    except Exception:
        if moved_existing and backup.exists() and not output.exists():
            os.replace(backup, output)
        raise
    if backup.exists():
        backup.relative_to(parent)
        shutil.rmtree(backup)


def _summarise(detail_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    def summarise(group: pd.DataFrame) -> pd.Series:
        success = group[group["status"].eq("success")].copy()
        return pd.Series(
            {
                "group_count": int(len(group)),
                "success_group_count": int(len(success)),
                "failed_group_count": int(len(group) - len(success)),
                "success_rate": float(len(success) / len(group)) if len(group) else float("nan"),
                "input_row_count": int(group["input_row_count"].sum()),
                "service_count": int(group["service_count"].sum()),
                "total_distance_km_success_only": success["distance_km"].sum(min_count=1),
                "total_duration_min_success_only": success["duration_min"].sum(min_count=1),
                "avg_distance_km_per_successful_group": success["distance_km"].mean(),
                "avg_duration_min_per_successful_group": success["duration_min"].mean(),
                "home_found_group_count": int(group["home_found"].sum()),
                "cache_hit_group_count": int(group["cache_hit"].sum()),
            }
        )

    city_summary = detail_df.groupby("city", sort=True, dropna=False).apply(summarise, include_groups=False).reset_index()
    overall = summarise(detail_df).to_frame().T
    overall.insert(0, "city", "ALL_CITIES")
    city_summary = pd.concat([city_summary, overall], ignore_index=True)
    bucket_summary = (
        detail_df.groupby(["city", "route_bucket"], sort=True, dropna=False)
        .apply(summarise, include_groups=False)
        .reset_index()
    )
    return city_summary, bucket_summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--cache-file", type=Path, default=DEFAULT_CACHE_FILE)
    parser.add_argument("--resume-checkpoint", action="store_true", help="Resume from the provenance-matching local checkpoint in output-dir.")
    parser.add_argument("--retry-failed", action="store_true", help="With --resume-checkpoint, rerun only checkpoint rows whose status is failed.")
    parser.add_argument("--map-version", default="unavailable", help="Immutable deployed graph/map version; required to reuse cache.")
    parser.add_argument("--reuse-cache", action="store_true", help="Reuse only a cache tagged with an explicit --map-version.")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_file = args.source.resolve()
    output_dir = args.output_dir.resolve()
    cache_file = args.cache_file.resolve()
    if not source_file.exists():
        raise FileNotFoundError(source_file)
    if args.reuse_cache and str(args.map_version).strip().lower() in {"", "unavailable", "unknown"}:
        raise ValueError("--reuse-cache requires an explicit immutable --map-version; refusing stale-graph reuse")
    if args.retry_failed and not args.resume_checkpoint:
        raise ValueError("--retry-failed requires --resume-checkpoint")
    final_names = {"route_detail_privacy_safe.csv", "city_summary.csv", "city_bucket_summary.csv", "metadata.json"}
    if output_dir.exists() and any((output_dir / name).exists() for name in final_names) and not args.overwrite:
        raise FileExistsError(f"Output directory already contains final artifacts: {output_dir}; pass --overwrite to replace artifacts")
    if args.workers < 1 or args.workers > 8:
        raise ValueError("--workers must be in [1, 8]")
    if args.max_attempts < 1:
        raise ValueError("--max-attempts must be >= 1")

    config = _load_config()
    routing_cfg = config.get("routing", {})
    if str(routing_cfg.get("distance_backend", "")).strip().lower() != "city_osrm_else_haversine":
        raise ValueError("This audit expects routing.distance_backend=city_osrm_else_haversine to match Current Coverage city endpoints")
    source_sha256 = _sha256_file(source_file)
    source_row_count = int(len(pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)))
    service_df = _load_current_coverage_service(source_file)
    endpoints = _build_endpoints(routing_cfg)
    homes, home_metadata = _load_home_lookup_by_city(service_df, config)
    computation_fingerprint = _build_computation_fingerprint(
        source_file=source_file,
        endpoints=endpoints,
        home_metadata=home_metadata,
        map_version=str(args.map_version),
    )
    tasks = _build_tasks(service_df, endpoints, homes)
    if len(tasks) != 2853:
        raise ValueError(f"Expected 2,853 June daily-SM groups for this source; found {len(tasks)}")
    if len({task.route_group_id for task in tasks}) != len(tasks):
        raise ValueError("route_group_id collision detected")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_file = output_dir.parent / f".{output_dir.name}.{CHECKPOINT_FILENAME}"
    cache_entries = _load_cache(cache_file) if args.reuse_cache else {}
    resumed_rows = _load_checkpoint(checkpoint_file, computation_fingerprint) if args.resume_checkpoint else []
    expected_group_ids = {task.route_group_id for task in tasks}
    if args.retry_failed:
        resumed_rows = [row for row in resumed_rows if str(row.get("status", "")) != "failed"]
    resume_by_group_id = {str(row.get("route_group_id", "")): row for row in resumed_rows if str(row.get("route_group_id", "")) in expected_group_ids}
    if len(resume_by_group_id) != len(resumed_rows):
        raise ValueError("Checkpoint has an unknown or duplicate route_group_id")
    pending_tasks = [task for task in tasks if task.route_group_id not in resume_by_group_id]
    started = time.perf_counter()
    rows: list[dict[str, Any]] = list(resume_by_group_id.values())
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                _route_task,
                task,
                map_version=str(args.map_version),
                computation_fingerprint_sha256=str(computation_fingerprint["sha256"]),
                timeout_seconds=float(args.timeout_seconds),
                max_attempts=int(args.max_attempts),
                cache_entries=cache_entries,
                allow_cache_read=bool(args.reuse_cache),
            )
            for task in pending_tasks
        ]
        for completed, future in enumerate(as_completed(futures), start=1):
            rows.append(future.result())
            if completed % 250 == 0 or completed == len(pending_tasks):
                _save_checkpoint(checkpoint_file, computation_fingerprint, rows)
                print(f"completed_groups={len(rows)}/{len(tasks)} pending={len(tasks) - len(rows)} checkpoint={checkpoint_file}")
    elapsed_seconds = time.perf_counter() - started
    detail_df = pd.DataFrame(rows).sort_values(["city", "service_date_key", "route_group_id"]).reset_index(drop=True)
    if detail_df.duplicated(["city", "service_date_key", "route_group_id"]).any() or len(detail_df) != len(tasks):
        raise RuntimeError("Output detail key uniqueness/cardinality invariant failed")
    success_mask = detail_df["status"].eq("success")
    failed_mask = ~success_mask
    if not detail_df.loc[success_mask, ["distance_km", "duration_min"]].applymap(lambda value: pd.notna(value) and math.isfinite(float(value)) and float(value) >= 0).all().all():
        raise RuntimeError("Successful route has an invalid metric")
    if detail_df.loc[failed_mask, ["distance_km", "duration_min"]].notna().any().any() or detail_df.loc[failed_mask, "failure_reason"].eq("").any():
        raise RuntimeError("Failed route must have null metrics and a failure reason")
    invalid_zero_success = (
        success_mask
        & detail_df["distance_km"].eq(0.0)
        & detail_df["duration_min"].eq(0.0)
        & ~(detail_df["unique_service_stop_count"].eq(1) & ~detail_df["home_found"])
    )
    if invalid_zero_success.any():
        raise RuntimeError(f"Invalid zero-distance/time successes remain: {int(invalid_zero_success.sum())}")

    city_summary_df, bucket_summary_df = _summarise(detail_df)
    staging_dir = output_dir.parent / f".{output_dir.name}.staging-{os.getpid()}-{time.time_ns()}"
    staging_dir.mkdir(parents=False, exist_ok=False)
    detail_path = staging_dir / "route_detail_privacy_safe.csv"
    city_summary_path = staging_dir / "city_summary.csv"
    bucket_summary_path = staging_dir / "city_bucket_summary.csv"
    detail_df.to_csv(detail_path, index=False, encoding="utf-8")
    city_summary_df.to_csv(city_summary_path, index=False, encoding="utf-8")
    bucket_summary_df.to_csv(bucket_summary_path, index=False, encoding="utf-8")

    cache_to_save = {row["cache_identity"]: row for row in rows}
    _save_cache(cache_file, cache_to_save)
    metadata = {
        "schema": "current-coverage-osrm-result/v2",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "computation": {
            "algorithm_version": ALGORITHM_VERSION,
            "fingerprint": computation_fingerprint,
            "single_uninterrupted_result_set": len(resume_by_group_id) == 0 and int(detail_df["cache_hit"].sum()) == 0,
        },
        "source": {
            "path": str(source_file.relative_to(PROJECT_ROOT)),
            "sha256": source_sha256,
            "row_count_read": source_row_count,
            "row_count_after_ui_validity_filters": int(len(service_df)),
            "service_date_min": str(service_df["service_date_key"].min()),
            "service_date_max": str(service_df["service_date_key"].max()),
            "ui_date_column_selected": "PROMISE_DATE",
        },
        "scope": {"cities": list(TARGET_CITIES), "scenario": "Current Coverage"},
        "group_identity": {
            "detail_key": ["city", "service_date_key", "route_group_id"],
            "route_group_id": "lowercase sha256 hex of UTF-8(ui_normalize(city) + U+001F + YYYY-MM-DD + U+001F + ui_normalize(assigned_sm_code))",
            "ui_normalize": "null to empty; CR/LF to space; collapse all whitespace to one ASCII space; trim; no Unicode normalization or case-folding",
            "raw_assigned_sm_code_exported": False,
        },
        "route_semantics": {
            "coordinate_order": "longitude,latitude",
            "road_network_backend": "OSRM Route/Table",
            "distance_unit": "km",
            "duration_unit": "min",
            "ordering": "OSRM Table distance matrix followed by OSRMTripClient nearest-neighbor ordering",
            "home_start": "fixed first coordinate if a UI-priority home lookup succeeds",
            "return_home": False,
            "single_service_stop_without_home": "success with 0 km / 0 min and no OSRM request, matching UI client behavior",
            "zero_route_guard": "0 km / 0 min is rejected unless there is exactly one unique service stop and no home; rejected routes receive null metrics",
            "zero_route_direct_validation": DIRECT_VALIDATION_OPTIONS,
            "snapping": "OSRM Table and Route implicitly snap request coordinates; no snapped coordinates are exported",
            "fallback": "disabled for this audit; OSRM failure remains failed/null rather than Haversine fallback",
            "traffic": "No live traffic input is supplied by this OSRM API call; metrics are static graph/profile estimates",
        },
        "osrm": {
            "profile": str(routing_cfg.get("osrm_profile", "driving")),
            "distance_backend_config": str(routing_cfg.get("distance_backend")),
            "map_version": str(args.map_version),
            "map_version_status": "not_exposed_by_configured_OSRM_HTTP_API" if str(args.map_version) == "unavailable" else "caller_supplied",
            "engine_version": "not_exposed_by_configured_OSRM_HTTP_API",
            "endpoints": [
                {"city": endpoint.city, "route_city_key": endpoint.route_city_key, "endpoint_id": endpoint.endpoint_id, "url": endpoint.endpoint_url, "profile": endpoint.profile}
                for endpoint in endpoints.values()
            ],
            "table_options": TABLE_OPTIONS,
            "route_options": ROUTE_OPTIONS,
            "timeout_seconds": float(args.timeout_seconds),
            "max_attempts": int(args.max_attempts),
            "workers": int(args.workers),
        },
        "cache": {
            "cache_file": str(cache_file.relative_to(PROJECT_ROOT)),
            "cache_identity_fields": ["algorithm_version", "computation_fingerprint_sha256", "coordinates_lon_lat", "endpoint_url", "map_version", "profile", "table_options", "route_options", "preserve_first", "distance_unit", "duration_unit"],
            "reuse_enabled": bool(args.reuse_cache),
            "reuse_blocked_without_immutable_map_version": not bool(args.reuse_cache),
        },
        "checkpoint": {
            "file": _display_path(checkpoint_file),
            "computation_fingerprint_sha256": computation_fingerprint["sha256"],
            "resumed_group_count": len(resume_by_group_id),
            "resume_requires_explicit_flag": "--resume-checkpoint",
        },
        "home_lookup": home_metadata,
        "counts": {
            "expected_group_count": len(tasks),
            "detail_row_count": len(detail_df),
            "success_group_count": int(success_mask.sum()),
            "failed_group_count": int(failed_mask.sum()),
            "cache_hit_group_count": int(detail_df["cache_hit"].sum()),
            "home_found_group_count": int(detail_df["home_found"].sum()),
            "failure_reasons": {str(key): int(value) for key, value in detail_df.loc[failed_mask, "failure_reason"].value_counts().items()},
            "invalid_zero_success_count": int(invalid_zero_success.sum()),
        },
        "performance": {
            "wall_clock_seconds": elapsed_seconds,
            "successful_request_elapsed_ms_sum": float(detail_df.loc[success_mask, "request_elapsed_ms"].fillna(0).sum()),
            "successful_request_attempts_sum": int(detail_df.loc[success_mask, "request_attempts"].fillna(0).sum()),
        },
        "files": {
            "route_detail_privacy_safe": detail_path.name,
            "city_summary": city_summary_path.name,
            "city_bucket_summary": bucket_summary_path.name,
        },
    }
    (staging_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    _publish_artifact_directory(staging_dir, output_dir)
    print(f"output_dir={output_dir}")
    print(f"success_groups={int(success_mask.sum())} failed_groups={int(failed_mask.sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
