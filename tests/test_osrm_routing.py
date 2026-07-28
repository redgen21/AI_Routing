import unittest
from pathlib import Path
from unittest.mock import patch

from smart_routing.osrm_routing import OSRMConfig, OSRMTripClient, OSRMUnavailableError


class OSRMMatrixFallbackTests(unittest.TestCase):
    def _client(self, **config_overrides):
        config = {
            "osrm_url": "http://primary-osrm.invalid",
            "cache_file": Path("data/cache/test_osrm_trip_cache.csv"),
        }
        config.update(config_overrides)
        return OSRMTripClient(OSRMConfig(**config))

    def test_osrm_table_failure_returns_haversine_matrix_in_km_and_min_once(self) -> None:
        client = self._client()
        coords = [(-84.39, 33.75), (-84.39, 34.75)]  # one latitude degree, lon/lat order

        with patch.object(client, "_request_table", side_effect=ConnectionError("primary down")):
            distances_km, durations_min = client.get_distance_duration_matrix(coords)

        expected_km = client._haversine_km(coords[0], coords[1])
        self.assertAlmostEqual(distances_km[0][1], expected_km, places=6)
        self.assertAlmostEqual(durations_min[0][1], expected_km / 50.0 * 60.0, places=6)
        self.assertGreater(distances_km[0][1], 100.0)
        telemetry = client.get_matrix_telemetry()
        self.assertEqual(telemetry["matrix_source"], "haversine_fallback")
        self.assertTrue(telemetry["fallback_used"])
        self.assertEqual(telemetry["distance_unit"], "km")
        self.assertEqual(telemetry["duration_unit"], "min")

    def test_fail_closed_matrix_does_not_substitute_haversine_after_osrm_failure(self) -> None:
        client = self._client(fail_closed_on_osrm_error=True)

        with patch.object(client, "_request_table", side_effect=ConnectionError("primary down")):
            with self.assertRaisesRegex(OSRMUnavailableError, "fail_closed_on_osrm_error"):
                client.get_distance_duration_matrix([(-84.39, 33.75), (-84.38, 33.76)])

        telemetry = client.get_matrix_telemetry()
        self.assertEqual(telemetry["matrix_source"], "error")
        self.assertFalse(telemetry["fallback_used"])
        self.assertEqual(telemetry["failure_count"], 1)

    def test_secondary_osrm_matrix_is_recorded_as_road_network_fallback(self) -> None:
        client = self._client(fallback_osrm_url="http://secondary-osrm.invalid")
        with patch.object(
            client,
            "_request_table",
            side_effect=[ConnectionError("primary down"), ([[0.0, 1500.0], [1400.0, 0.0]], [[0.0, 180.0], [170.0, 0.0]])],
        ):
            distances_km, durations_min = client.get_distance_duration_matrix(
                [(-84.39, 33.75), (-84.38, 33.76)]
            )

        self.assertEqual(distances_km, [[0.0, 1.5], [1.4, 0.0]])
        self.assertEqual(durations_min, [[0.0, 3.0], [170.0 / 60.0, 0.0]])
        telemetry = client.get_matrix_telemetry()
        self.assertEqual(telemetry["matrix_source"], "osrm_fallback")
        self.assertTrue(telemetry["fallback_attempted"])
        self.assertTrue(telemetry["fallback_used"])
        self.assertEqual(telemetry["fallback_rate"], 1.0)


if __name__ == "__main__":
    unittest.main()
