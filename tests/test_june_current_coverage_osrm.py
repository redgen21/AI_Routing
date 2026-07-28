from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.operations import june_current_coverage_osrm as current_osrm


def _task(*, coords: tuple[tuple[float, float], ...], unique_service_stops: int, home_found: bool) -> current_osrm.RouteTask:
    endpoint = current_osrm.Endpoint(
        city="Washington, DC",
        route_city_key="Washington, DC",
        endpoint_url="http://osrm.test",
        endpoint_id="washington_dc",
        profile="driving",
    )
    return current_osrm.RouteTask(
        city="Washington, DC",
        service_date_key="2026-06-01",
        assigned_sm_code="TEST_ONLY",
        route_group_id="offline-test-group",
        route_bucket="DMS",
        input_row_count=unique_service_stops,
        service_count=unique_service_stops,
        unique_service_stop_count=unique_service_stops,
        coords=coords,
        home_found=home_found,
        endpoint=endpoint,
    )


class JuneCurrentCoverageOSRMTests(unittest.TestCase):
    def test_zero_route_for_distinct_stops_is_retried_then_rejected(self) -> None:
        calls: list[str] = []

        def fake_request(url: str, timeout_seconds: float, max_attempts: int):
            calls.append(url)
            if "/table/v1/" in url:
                return {"code": "Ok", "distances": [[0, 100], [100, 0]], "durations": [[0, 10], [10, 0]]}, 1, 1.0
            return {"code": "Ok", "routes": [{"distance": 0, "duration": 0}]}, 1, 1.0

        with patch.object(current_osrm, "_request_json", side_effect=fake_request):
            result = current_osrm._route_task(
                _task(coords=((-77.0, 38.9), (-76.0, 39.5)), unique_service_stops=2, home_found=False),
                map_version="offline-test-map",
                computation_fingerprint_sha256="offline-test-fingerprint",
                timeout_seconds=1,
                max_attempts=1,
                cache_entries={},
                allow_cache_read=False,
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failure_reason"], "zero_route_for_distinct_coordinates")
        self.assertIsNone(result["distance_km"])
        self.assertIsNone(result["duration_min"])
        self.assertEqual(sum("/route/v1/" in call for call in calls), 2)
        self.assertTrue(any("alternatives=true" in call for call in calls))
        self.assertTrue(any("alternatives=false" in call for call in calls))

    def test_single_service_stop_without_home_may_be_zero_without_network(self) -> None:
        with patch.object(current_osrm, "_request_json", side_effect=AssertionError("OSRM must not be called")):
            result = current_osrm._route_task(
                _task(coords=((-77.0, 38.9),), unique_service_stops=1, home_found=False),
                map_version="offline-test-map",
                computation_fingerprint_sha256="offline-test-fingerprint",
                timeout_seconds=1,
                max_attempts=1,
                cache_entries={},
                allow_cache_read=False,
            )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["distance_km"], 0.0)
        self.assertEqual(result["duration_min"], 0.0)

    def test_checkpoint_rejects_computation_fingerprint_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint = Path(temp_dir) / "route_checkpoint.json"
            written = {"sha256": "fingerprint-a", "payload": {"algorithm_version": "a"}}
            current_osrm._save_checkpoint(checkpoint, written, [{"route_group_id": "one"}])

            self.assertEqual(current_osrm._load_checkpoint(checkpoint, written), [{"route_group_id": "one"}])
            mismatched = {"sha256": "fingerprint-b", "payload": {"algorithm_version": "b"}}
            with self.assertRaisesRegex(ValueError, "fingerprint mismatch"):
                current_osrm._load_checkpoint(checkpoint, mismatched)


if __name__ == "__main__":
    unittest.main()
