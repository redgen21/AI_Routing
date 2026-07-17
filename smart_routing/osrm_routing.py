from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import requests
from shapely.geometry import LineString, shape


Coord = tuple[float, float]  # (lon, lat)


@dataclass
class OSRMConfig:
    osrm_url: str
    mode: str = "osrm"
    osrm_profile: str = "driving"
    cache_file: Path = Path("data/cache/osrm_trip_cache.csv")
    route_cache_file: Path | None = None
    fallback_osrm_url: str | None = None
    avoid_polygons: list[dict[str, Any]] | None = None
    avoid_penalty_multiplier: float = 4.0


class OSRMTripClient:
    def __init__(self, cfg: OSRMConfig):
        self.cfg = cfg
        self.session = requests.Session()

    def get_trip(self, coords: Sequence[Coord]) -> tuple[float, float]:
        unique_coords = [(float(lon), float(lat)) for lon, lat in coords]
        if len(unique_coords) < 2:
            return 0.0, 0.0
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_haversine_trip(unique_coords)

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
            return self._apply_avoid_penalty_to_matrix(normalized, distances_m, durations_s)
        except Exception:
            if self.cfg.fallback_osrm_url:
                try:
                    distances_m, durations_s = self._request_table(self.cfg.fallback_osrm_url, normalized)
                    return self._apply_avoid_penalty_to_matrix(normalized, distances_m, durations_s)
                except Exception:
                    pass
            return self._fallback_matrix(normalized)

    def build_ordered_route(self, coords: Sequence[Coord], preserve_first: bool = False) -> dict[str, object]:
        normalized = [(float(lon), float(lat)) for lon, lat in coords]
        if not normalized:
            return {"ordered_coords": [], "distance_km": 0.0, "duration_min": 0.0, "geometry": []}
        if len(normalized) == 1:
            return self._single_coord_route(normalized[0])
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_ordered_route(normalized)

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
                return payload
            except Exception:
                continue
        return self._fallback_ordered_route(normalized)

    def build_route_in_order(self, coords: Sequence[Coord]) -> dict[str, object]:
        normalized = [(float(lon), float(lat)) for lon, lat in coords]
        if not normalized:
            return {"ordered_coords": [], "distance_km": 0.0, "duration_min": 0.0, "geometry": []}
        if len(normalized) == 1:
            return self._single_coord_route(normalized[0])
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_route_in_order(normalized)

        for base_url in [self.cfg.osrm_url, self.cfg.fallback_osrm_url]:
            if not base_url:
                continue
            try:
                distance_km, duration_min, geometry = self._request_route_geometry(base_url, normalized)
                return {
                    "ordered_coords": normalized,
                    "distance_km": distance_km,
                    "duration_min": duration_min,
                    "geometry": geometry,
                }
            except Exception:
                continue
        return self._fallback_route_in_order(normalized)

    def _single_coord_route(self, coord: Coord) -> dict[str, object]:
        lon, lat = coord
        return {
            "ordered_coords": [coord],
            "distance_km": 0.0,
            "duration_min": 0.0,
            "geometry": [[lat, lon]],
        }

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
            "?overview=full&geometries=geojson&steps=false&alternatives=true"
        )
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        routes = data.get("routes", [])
        if data.get("code") != "Ok" or not routes:
            raise ValueError(json.dumps(data)[:300])
        route = self._choose_route_avoiding_polygons(routes)
        geometry_coords = route.get("geometry", {}).get("coordinates", [])
        geometry = [[float(lat), float(lon)] for lon, lat in geometry_coords]
        distance_km = float(route.get("distance", 0.0)) / 1000.0
        duration_min = float(route.get("duration", 0.0)) / 60.0
        if self._route_hits_avoid_polygon(geometry):
            multiplier = self._avoid_penalty_multiplier()
            distance_km *= multiplier
            duration_min *= multiplier
        return distance_km, duration_min, geometry

    def _avoid_penalty_multiplier(self) -> float:
        try:
            return max(1.0, float(self.cfg.avoid_penalty_multiplier or 1.0))
        except Exception:
            return 1.0

    def _active_avoid_shapes(self):
        shapes = []
        for polygon in self.cfg.avoid_polygons or []:
            geometry = polygon.get("geometry") if isinstance(polygon, dict) else None
            if not geometry:
                continue
            try:
                geom = shape(geometry)
            except Exception:
                continue
            if not geom.is_empty:
                shapes.append(geom)
        return shapes

    def _route_hits_avoid_polygon(self, geometry: Sequence[Sequence[float]]) -> bool:
        shapes = self._active_avoid_shapes()
        if not shapes or len(geometry) < 2:
            return False
        try:
            line = LineString([(float(lon), float(lat)) for lat, lon in geometry])
        except Exception:
            return False
        return any(line.intersects(geom) for geom in shapes)

    def _segment_hits_avoid_polygon(self, source: Coord, target: Coord) -> bool:
        shapes = self._active_avoid_shapes()
        if not shapes:
            return False
        try:
            line = LineString([(float(source[0]), float(source[1])), (float(target[0]), float(target[1]))])
        except Exception:
            return False
        return any(line.intersects(geom) for geom in shapes)

    def _choose_route_avoiding_polygons(self, routes: Sequence[dict[str, Any]]) -> dict[str, Any]:
        if not self.cfg.avoid_polygons:
            return routes[0]
        best_route = routes[0]
        best_duration = float(best_route.get("duration", 0.0) or 0.0)
        for route in routes:
            geometry_coords = route.get("geometry", {}).get("coordinates", [])
            geometry = [[float(lat), float(lon)] for lon, lat in geometry_coords]
            if self._route_hits_avoid_polygon(geometry):
                continue
            duration = float(route.get("duration", 0.0) or 0.0)
            if best_route is routes[0] or duration < best_duration:
                best_route = route
                best_duration = duration
        return best_route

    def _apply_avoid_penalty_to_matrix(
        self,
        coords: Sequence[Coord],
        distances_m: Sequence[Sequence[float]],
        durations_s: Sequence[Sequence[float]],
    ) -> tuple[list[list[float]], list[list[float]]]:
        multiplier = self._avoid_penalty_multiplier()
        distance_rows: list[list[float]] = []
        duration_rows: list[list[float]] = []
        for src_idx, (distance_row, duration_row) in enumerate(zip(distances_m, durations_s)):
            out_distance_row: list[float] = []
            out_duration_row: list[float] = []
            for dst_idx, (distance_value, duration_value) in enumerate(zip(distance_row, duration_row)):
                penalty = multiplier if src_idx != dst_idx and self._segment_hits_avoid_polygon(coords[src_idx], coords[dst_idx]) else 1.0
                out_distance_row.append((float(distance_value) / 1000.0) * penalty)
                out_duration_row.append((float(duration_value) / 60.0) * penalty)
            distance_rows.append(out_distance_row)
            duration_rows.append(out_duration_row)
        return distance_rows, duration_rows

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
            return self._single_coord_route(coords[0])
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

    def _fallback_route_in_order(self, coords: Sequence[Coord]) -> dict[str, object]:
        if len(coords) < 2:
            return self._single_coord_route(coords[0])
        total_km = 0.0
        for idx in range(len(coords) - 1):
            total_km += self._haversine_km(coords[idx], coords[idx + 1])
        geometry = [[lat, lon] for lon, lat in coords]
        return {
            "ordered_coords": coords,
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
