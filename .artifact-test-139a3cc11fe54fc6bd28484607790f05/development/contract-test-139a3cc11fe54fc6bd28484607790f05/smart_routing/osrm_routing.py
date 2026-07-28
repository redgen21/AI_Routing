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
    # Keep the historical Haversine fallback unless a caller explicitly opts in
    # to fail-closed routing for an OSRM-backed request.
    fail_closed_on_osrm_error: bool = False


class OSRMUnavailableError(RuntimeError):
    """Raised when fail-closed OSRM routing cannot obtain a road-network result."""


class OSRMTripClient:
    def __init__(self, cfg: OSRMConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.matrix_telemetry: dict[str, Any] = {}
        self._matrix_source_counts: dict[str, int] = {}
        self._matrix_request_count = 0
        self._matrix_failure_count = 0

    def get_matrix_telemetry(self) -> dict[str, Any]:
        """Return the latest matrix source plus cumulative fallback/error counters.

        Matrix values always use ``distance_km`` and ``duration_min``.  ``matrix_source``
        distinguishes road-network results from Haversine estimates; Haversine is not a
        road-network distance and does not incorporate live traffic.
        """
        fallback_count = sum(
            count
            for source, count in self._matrix_source_counts.items()
            if source in {"osrm_fallback", "haversine_fallback"}
        )
        telemetry = dict(self.matrix_telemetry)
        telemetry.update(
            {
                "request_count": self._matrix_request_count,
                "failure_count": self._matrix_failure_count,
                "source_counts": dict(self._matrix_source_counts),
                "fallback_count": fallback_count,
                "fallback_rate": fallback_count / self._matrix_request_count if self._matrix_request_count else 0.0,
            }
        )
        return telemetry

    def _record_matrix_telemetry(
        self,
        matrix_source: str,
        *,
        fallback_attempted: bool = False,
        fallback_used: bool = False,
        error: Exception | None = None,
    ) -> None:
        self._matrix_request_count += 1
        self._matrix_source_counts[matrix_source] = self._matrix_source_counts.get(matrix_source, 0) + 1
        if matrix_source == "error":
            self._matrix_failure_count += 1
        self.matrix_telemetry = {
            "matrix_source": matrix_source,
            "fallback_attempted": fallback_attempted,
            "fallback_used": fallback_used,
            "distance_unit": "km",
            "duration_unit": "min",
            "osrm_profile": self.cfg.osrm_profile,
            "fail_closed_on_osrm_error": bool(self.cfg.fail_closed_on_osrm_error),
        }
        if error is not None:
            self.matrix_telemetry["error"] = f"{type(error).__name__}: {error}"

    def _raise_if_fail_closed(self, operation: str, error: Exception) -> None:
        if self.cfg.fail_closed_on_osrm_error:
            raise OSRMUnavailableError(
                f"OSRM {operation} failed and fail_closed_on_osrm_error is enabled"
            ) from error

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
            self._record_matrix_telemetry("trivial")
            return base, base
        if str(self.cfg.mode).strip().lower() != "osrm":
            self._record_matrix_telemetry("haversine_configured")
            return self._fallback_matrix(normalized)
        try:
            distances_m, durations_s = self._request_table(self.cfg.osrm_url, normalized)
            self._record_matrix_telemetry("osrm_primary")
            return self._apply_avoid_penalty_to_matrix(normalized, distances_m, durations_s)
        except Exception as primary_error:
            last_error: Exception = primary_error
            if self.cfg.fallback_osrm_url:
                try:
                    distances_m, durations_s = self._request_table(self.cfg.fallback_osrm_url, normalized)
                    self._record_matrix_telemetry(
                        "osrm_fallback", fallback_attempted=True, fallback_used=True
                    )
                    return self._apply_avoid_penalty_to_matrix(normalized, distances_m, durations_s)
                except Exception as fallback_error:
                    last_error = fallback_error
            if self.cfg.fail_closed_on_osrm_error:
                self._record_matrix_telemetry(
                    "error",
                    fallback_attempted=bool(self.cfg.fallback_osrm_url),
                    error=last_error,
                )
                self._raise_if_fail_closed("table request", last_error)
            self._record_matrix_telemetry(
                "haversine_fallback",
                fallback_attempted=bool(self.cfg.fallback_osrm_url),
                fallback_used=True,
                error=last_error,
            )
            return self._fallback_matrix(normalized)

    def build_ordered_route(self, coords: Sequence[Coord], preserve_first: bool = False) -> dict[str, object]:
        normalized = [(float(lon), float(lat)) for lon, lat in coords]
        if not normalized:
            return {"ordered_coords": [], "distance_km": 0.0, "duration_min": 0.0, "geometry": []}
        if len(normalized) == 1:
            return self._single_coord_route(normalized[0])
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_ordered_route(normalized)

        last_error: Exception | None = None
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
            except Exception as error:
                last_error = error
                continue
        if last_error is not None:
            self._raise_if_fail_closed("ordered-route request", last_error)
        return self._fallback_ordered_route(normalized)

    def build_route_in_order(self, coords: Sequence[Coord]) -> dict[str, object]:
        normalized = [(float(lon), float(lat)) for lon, lat in coords]
        if not normalized:
            return {"ordered_coords": [], "distance_km": 0.0, "duration_min": 0.0, "geometry": []}
        if len(normalized) == 1:
            return self._single_coord_route(normalized[0])
        if str(self.cfg.mode).strip().lower() != "osrm":
            return self._fallback_route_in_order(normalized)

        last_error: Exception | None = None
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
            except Exception as error:
                last_error = error
                continue
        if last_error is not None:
            self._raise_if_fail_closed("route request", last_error)
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
        except Exception as primary_error:
            last_error: Exception = primary_error
            if self.cfg.fallback_osrm_url:
                try:
                    return self._request_route_nn(self.cfg.fallback_osrm_url, coords)
                except Exception as fallback_error:
                    last_error = fallback_error
        self._raise_if_fail_closed("route fallback request", last_error)
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
        """Return a normalized Haversine matrix in km/min.

        The OSRM Table API uses metres/seconds.  Build the Haversine estimate in
        that same raw contract, then normalize through the single common
        conversion point so it can never be converted a second time.
        """
        distances_m, durations_s = self._fallback_matrix_raw(coords)
        return self._apply_avoid_penalty_to_matrix(coords, distances_m, durations_s)

    def _fallback_matrix_raw(self, coords: Sequence[Coord]) -> tuple[list[list[float]], list[list[float]]]:
        """Return Haversine estimates in the raw OSRM Table units: metres/seconds."""
        distances_m: list[list[float]] = []
        durations_s: list[list[float]] = []
        for src in coords:
            dist_row: list[float] = []
            dur_row: list[float] = []
            for dst in coords:
                distance_km = self._haversine_km(src, dst)
                dist_row.append(distance_km * 1000.0)
                dur_row.append((distance_km / 50.0) * 60.0 * 60.0)
            distances_m.append(dist_row)
            durations_s.append(dur_row)
        return distances_m, durations_s

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
        """Apply avoid penalties and normalize raw OSRM Table metres/seconds to km/min."""
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
