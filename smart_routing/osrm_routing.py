from __future__ import annotations

import csv
import hashlib
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import requests


Coord = tuple[float, float]  # (lon, lat)


@dataclass
class OSRMConfig:
    osrm_url: str
    mode: str = "osrm"
    osrm_profile: str = "driving"
    cache_file: Path = Path("data/cache/osrm_trip_cache.csv")
    route_cache_file: Path | None = None
    fallback_osrm_url: str | None = None


class OSRMTripClient:
    MAX_ROUTE_CACHE_PRELOAD_BYTES = 100 * 1024 * 1024

    def __init__(self, cfg: OSRMConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.cache: dict[str, tuple[float, float]] = {}
        self.route_cache: dict[str, dict[str, object]] = {}
        self._lock = threading.Lock()
        self._load_cache()
        self._load_route_cache()

    def _load_cache(self) -> None:
        if not self.cfg.cache_file.exists():
            return
        with self.cfg.cache_file.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    cache_key = row.get("cache_key")
                    distance_km = row.get("distance_km")
                    duration_min = row.get("duration_min")
                    if not cache_key or distance_km in (None, "") or duration_min in (None, ""):
                        continue
                    self.cache[cache_key] = (float(distance_km), float(duration_min))
                except Exception:
                    continue

    def _append_cache(self, cache_key: str, distance_km: float, duration_min: float, stop_count: int) -> None:
        self.cfg.cache_file.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.cfg.cache_file.exists()
        with self.cfg.cache_file.open("a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["cache_key", "distance_km", "duration_min", "stop_count"])
            if not file_exists:
                writer.writeheader()
            writer.writerow(
                {
                    "cache_key": cache_key,
                    "distance_km": round(distance_km, 6),
                    "duration_min": round(duration_min, 6),
                    "stop_count": int(stop_count),
                }
            )

    def _resolved_route_cache_file(self) -> Path:
        if self.cfg.route_cache_file is not None:
            return self.cfg.route_cache_file
        return self.cfg.cache_file.with_name(f"{self.cfg.cache_file.stem}_route.jsonl")

    def _load_route_cache(self) -> None:
        route_cache_file = self._resolved_route_cache_file()
        if not route_cache_file.exists():
            return
        if route_cache_file.stat().st_size > self.MAX_ROUTE_CACHE_PRELOAD_BYTES:
            return
        with route_cache_file.open("r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    cache_key = row.get("cache_key")
                    if not cache_key:
                        continue
                    self.route_cache[cache_key] = {
                        "ordered_coords": row.get("ordered_coords", []),
                        "distance_km": float(row.get("distance_km", 0.0)),
                        "duration_min": float(row.get("duration_min", 0.0)),
                        "geometry": row.get("geometry", []),
                    }
                except Exception:
                    continue

    def _append_route_cache(self, cache_key: str, payload: dict[str, object], stop_count: int) -> None:
        route_cache_file = self._resolved_route_cache_file()
        route_cache_file.parent.mkdir(parents=True, exist_ok=True)
        with route_cache_file.open("a", encoding="utf-8-sig") as f:
            f.write(
                json.dumps(
                    {
                        "cache_key": cache_key,
                        "distance_km": round(float(payload.get("distance_km", 0.0)), 6),
                        "duration_min": round(float(payload.get("duration_min", 0.0)), 6),
                        "stop_count": int(stop_count),
                        "ordered_coords": payload.get("ordered_coords", []),
                        "geometry": payload.get("geometry", []),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    def _canonical_key(self, coords: Sequence[Coord]) -> str:
        canonical = sorted(f"{lon:.6f},{lat:.6f}" for lon, lat in coords)
        joined = "|".join(canonical)
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()

    def get_trip(self, coords: Sequence[Coord]) -> tuple[float, float]:
        unique_coords = [(float(lon), float(lat)) for lon, lat in coords]
        if len(unique_coords) < 2:
            return 0.0, 0.0
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_haversine_trip(unique_coords)

        cache_key = self._canonical_key(unique_coords)
        with self._lock:
            if cache_key in self.cache:
                return self.cache[cache_key]

        coord_str = ";".join(f"{lon},{lat}" for lon, lat in unique_coords)
        distance_km = 0.0
        duration_min = 0.0
        try:
            distance_km, duration_min = self._request_trip(self.cfg.osrm_url, coord_str)
        except Exception:
            if self.cfg.fallback_osrm_url:
                try:
                    distance_km, duration_min = self._request_trip(self.cfg.fallback_osrm_url, coord_str)
                except Exception:
                    distance_km, duration_min = self._request_route_nn_with_fallback(unique_coords)
            else:
                distance_km, duration_min = self._request_route_nn_with_fallback(unique_coords)

        with self._lock:
            self.cache[cache_key] = (distance_km, duration_min)
            self._append_cache(cache_key, distance_km, duration_min, len(unique_coords))
        return distance_km, duration_min

    def pair_distance(self, a: Coord, b: Coord) -> tuple[float, float]:
        distance_km, duration_min = self.get_trip([a, b])
        return float(distance_km), float(duration_min)

    def get_distance_duration_matrix(self, coords: Sequence[Coord]) -> tuple[list[list[float]], list[list[float]]]:
        normalized = [(float(lon), float(lat)) for lon, lat in coords]
        if len(normalized) <= 1:
            base = [[0.0] * len(normalized) for _ in range(len(normalized))]
            return base, base
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_matrix(normalized)
        try:
            distances_m, durations_s = self._request_table(self.cfg.osrm_url, normalized)
            return (
                [[float(v) / 1000.0 for v in row] for row in distances_m],
                [[float(v) / 60.0 for v in row] for row in durations_s],
            )
        except Exception:
            if self.cfg.fallback_osrm_url:
                try:
                    distances_m, durations_s = self._request_table(self.cfg.fallback_osrm_url, normalized)
                    return (
                        [[float(v) / 1000.0 for v in row] for row in distances_m],
                        [[float(v) / 60.0 for v in row] for row in durations_s],
                    )
                except Exception:
                    pass
            return self._fallback_matrix(normalized)

    def build_ordered_route(self, coords: Sequence[Coord], preserve_first: bool = False) -> dict[str, object]:
        normalized = [(float(lon), float(lat)) for lon, lat in coords]
        if not normalized:
            return {"ordered_coords": [], "distance_km": 0.0, "duration_min": 0.0, "geometry": []}
        if len(normalized) == 1:
            lon, lat = normalized[0]
            return {
                "ordered_coords": normalized,
                "distance_km": 0.0,
                "duration_min": 0.0,
                "geometry": [[lat, lon]],
            }
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_ordered_route(normalized)

        base_cache_key = self._canonical_key(normalized)
        cache_key = f"{base_cache_key}|preserve_first_v2=1" if preserve_first else base_cache_key
        with self._lock:
            cached_payload = self.route_cache.get(cache_key)
            if cached_payload is not None:
                return cached_payload

        for base_url in [self.cfg.osrm_url, self.cfg.fallback_osrm_url]:
            if not base_url:
                continue
            try:
                if preserve_first and len(normalized) > 1:
                    distances_m, _ = self._request_table(base_url, normalized)
                    order = self._nearest_neighbor_order(distances_m, fixed_start_idx=0)
                    ordered_coords = [normalized[idx] for idx in order]
                else:
                    distances_m, _ = self._request_table(base_url, normalized)
                    order = self._nearest_neighbor_order(distances_m)
                    ordered_coords = [normalized[idx] for idx in order]
                distance_km, duration_min, geometry = self._request_route_geometry(base_url, ordered_coords)
                payload = {
                    "ordered_coords": ordered_coords,
                    "distance_km": distance_km,
                    "duration_min": duration_min,
                    "geometry": geometry,
                }
                with self._lock:
                    self.route_cache[cache_key] = payload
                    self._append_route_cache(cache_key, payload, len(normalized))
                return payload
            except Exception:
                continue
        return self._fallback_ordered_route(normalized)

    def _request_trip(self, base_url: str, coord_str: str) -> tuple[float, float]:
        url = (
            f"{base_url}/trip/v1/{self.cfg.osrm_profile}/{coord_str}"
            "?source=any&destination=any&roundtrip=false&steps=false&overview=false"
        )
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        trips = data.get("trips", [])
        if data.get("code") == "Ok" and trips:
            distance_km = float(trips[0].get("distance", 0.0)) / 1000.0
            duration_min = float(trips[0].get("duration", 0.0)) / 60.0
            return distance_km, duration_min
        raise ValueError(json.dumps(data)[:300])

    def _request_route_nn_with_fallback(self, coords: Sequence[Coord]) -> tuple[float, float]:
        try:
            return self._request_route_nn(self.cfg.osrm_url, coords)
        except Exception:
            if self.cfg.fallback_osrm_url:
                try:
                    return self._request_route_nn(self.cfg.fallback_osrm_url, coords)
                except Exception:
                    pass
        return self._fallback_haversine_trip(coords)

    def _request_route_nn(self, base_url: str, coords: Sequence[Coord]) -> tuple[float, float]:
        if len(coords) < 2:
            return 0.0, 0.0
        distance_mat, duration_mat = self._request_table(base_url, coords)
        order = self._nearest_neighbor_order(distance_mat)
        ordered_coords = [coords[idx] for idx in order]
        try:
            return self._request_route(base_url, ordered_coords)
        except Exception:
            total_km = 0.0
            total_min = 0.0
            for i in range(len(order) - 1):
                total_km += float(distance_mat[order[i]][order[i + 1]]) / 1000.0
                total_min += float(duration_mat[order[i]][order[i + 1]]) / 60.0
            return total_km, total_min

    def _request_table(self, base_url: str, coords: Sequence[Coord]) -> tuple[list[list[float]], list[list[float]]]:
        coord_str = ";".join(f"{lon},{lat}" for lon, lat in coords)
        url = f"{base_url}/table/v1/{self.cfg.osrm_profile}/{coord_str}?annotations=distance,duration"
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        if data.get("code") != "Ok":
            raise ValueError(json.dumps(data)[:300])
        distances = data.get("distances", [])
        durations = data.get("durations", [])
        if not distances or not durations:
            raise ValueError("Empty table result")
        return distances, durations

    def _fallback_matrix(self, coords: Sequence[Coord]) -> tuple[list[list[float]], list[list[float]]]:
        distances_km: list[list[float]] = []
        durations_min: list[list[float]] = []
        for src in coords:
            dist_row: list[float] = []
            dur_row: list[float] = []
            for dst in coords:
                d = self._haversine_km(src, dst)
                dist_row.append(d)
                dur_row.append((d / 50.0) * 60.0)
            distances_km.append(dist_row)
            durations_min.append(dur_row)
        return distances_km, durations_min

    def _request_route(self, base_url: str, coords: Sequence[Coord]) -> tuple[float, float]:
        coord_str = ";".join(f"{lon},{lat}" for lon, lat in coords)
        url = f"{base_url}/route/v1/{self.cfg.osrm_profile}/{coord_str}?overview=false&steps=false&alternatives=false"
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        routes = data.get("routes", [])
        if data.get("code") == "Ok" and routes:
            return float(routes[0].get("distance", 0.0)) / 1000.0, float(routes[0].get("duration", 0.0)) / 60.0
        raise ValueError(json.dumps(data)[:300])

    def _request_route_geometry(self, base_url: str, coords: Sequence[Coord]) -> tuple[float, float, list[list[float]]]:
        coord_str = ";".join(f"{lon},{lat}" for lon, lat in coords)
        url = (
            f"{base_url}/route/v1/{self.cfg.osrm_profile}/{coord_str}"
            "?overview=full&geometries=geojson&steps=false&alternatives=false"
        )
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        routes = data.get("routes", [])
        if data.get("code") != "Ok" or not routes:
            raise ValueError(json.dumps(data)[:300])
        route = routes[0]
        geometry_coords = route.get("geometry", {}).get("coordinates", [])
        geometry = [[float(lat), float(lon)] for lon, lat in geometry_coords]
        return float(route.get("distance", 0.0)) / 1000.0, float(route.get("duration", 0.0)) / 60.0, geometry

    def _nearest_neighbor_order(self, distance_mat: Sequence[Sequence[float]], fixed_start_idx: int | None = None) -> list[int]:
        size = len(distance_mat)
        if size <= 2:
            return list(range(size))
        if fixed_start_idx is not None and 0 <= int(fixed_start_idx) < size:
            start_idx = int(fixed_start_idx)
            remaining = set(range(size))
            remaining.remove(start_idx)
            order = [start_idx]
            while remaining:
                last = order[-1]
                next_idx = min(remaining, key=lambda idx: float(distance_mat[last][idx]))
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
                next_idx = min(remaining, key=lambda idx: float(distance_mat[last][idx]))
                total += float(distance_mat[last][next_idx])
                order.append(next_idx)
                remaining.remove(next_idx)
            if total < best_total:
                best_total = total
                best_order = order
        return best_order or list(range(size))

    def _fallback_haversine_trip(self, coords: Sequence[Coord]) -> tuple[float, float]:
        if len(coords) < 2:
            return 0.0, 0.0
        remaining = list(coords[1:])
        ordered = [coords[0]]
        while remaining:
            last = ordered[-1]
            next_coord = min(remaining, key=lambda c: self._haversine_km(last, c))
            ordered.append(next_coord)
            remaining.remove(next_coord)
        total_km = 0.0
        for idx in range(len(ordered) - 1):
            total_km += self._haversine_km(ordered[idx], ordered[idx + 1])
        return total_km, (total_km / 50.0) * 60.0

    def _fallback_ordered_route(self, coords: Sequence[Coord]) -> dict[str, object]:
        if len(coords) < 2:
            lon, lat = coords[0]
            return {"ordered_coords": coords, "distance_km": 0.0, "duration_min": 0.0, "geometry": [[lat, lon]]}
        remaining = list(coords[1:])
        ordered = [coords[0]]
        while remaining:
            last = ordered[-1]
            next_coord = min(remaining, key=lambda c: self._haversine_km(last, c))
            ordered.append(next_coord)
            remaining.remove(next_coord)
        total_km = 0.0
        for idx in range(len(ordered) - 1):
            total_km += self._haversine_km(ordered[idx], ordered[idx + 1])
        geometry = [[lat, lon] for lon, lat in ordered]
        return {
            "ordered_coords": ordered,
            "distance_km": total_km,
            "duration_min": (total_km / 50.0) * 60.0,
            "geometry": geometry,
        }

    def _haversine_km(self, a: Coord, b: Coord) -> float:
        import math

        lon1, lat1 = a
        lon2, lat2 = b
        r = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        hav = (
            math.sin(dlat / 2.0) ** 2
            + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2
        )
        return 2.0 * r * math.asin(math.sqrt(hav))
