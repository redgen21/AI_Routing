import unittest

import pandas as pd

from smart_routing.vrp_mode_na_general import (
    HOME_DISTANCE_ONLY,
    PREFERRED_REGION_SOFT,
    _build_city_route_client,
    _build_engineer_frames_from_payload,
    _load_reference_inputs,
    resolve_city_routing_policy,
)


def _empty_reference_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    engineers = pd.DataFrame(columns=["SVC_ENGINEER_CODE"])
    homes = pd.DataFrame(columns=["SVC_ENGINEER_CODE"])
    return engineers, homes


def _payload(city: str) -> dict:
    return {
        "city": city,
        "technicians": [
            {
                "employee_code": "T1",
                "employee_name": "Tech 1",
                "center_type": "DMS",
                "start_location": {"lat": 33.75, "lng": -84.39},
                "preferred_region_name": "Region A",
                "slot_count": 8,
            }
        ],
        "jobs": [],
        "options": {},
    }


class CityRoutingPolicyTests(unittest.TestCase):
    def test_common_mode_does_not_require_legacy_atlanta_reference_files(self) -> None:
        region, engineers, homes = _load_reference_inputs({"options": {}})
        self.assertTrue(region.empty)
        self.assertTrue(engineers.empty)
        self.assertTrue(homes.empty)
        self.assertIn("POSTAL_CODE", region.columns)

    def test_city_route_client_uses_server_supplied_osrm_endpoint(self) -> None:
        payload = _payload("Los Angeles, CA")
        payload["options"] = {
            "osrm_url": "http://127.0.0.1:5001",
            "osrm_profile": "driving",
            "distance_backend": "city_osrm_else_haversine",
        }
        client = _build_city_route_client(payload)
        self.assertEqual(client.cfg.osrm_url, "http://127.0.0.1:5001")
        self.assertEqual(client.cfg.mode, "osrm")

    def test_city_policy_is_explicit_for_atlanta_and_la_contexts(self) -> None:
        self.assertEqual(resolve_city_routing_policy(_payload("Atlanta, GA")), HOME_DISTANCE_ONLY)
        self.assertEqual(resolve_city_routing_policy(_payload("Los Angeles, CA")), PREFERRED_REGION_SOFT)
        self.assertEqual(
            resolve_city_routing_policy(_payload("Los Angeles, CA - Area Type Clusters")),
            PREFERRED_REGION_SOFT,
        )
        self.assertEqual(
            resolve_city_routing_policy(_payload("Los Angeles, CA - Bucket Sim Draft")),
            PREFERRED_REGION_SOFT,
        )
        self.assertEqual(resolve_city_routing_policy(_payload("Unknown City")), HOME_DISTANCE_ONLY)

    def test_atlanta_drops_preferred_region_but_la_preserves_it(self) -> None:
        ref_engineers, ref_homes = _empty_reference_frames()
        atl_engineers, _ = _build_engineer_frames_from_payload(
            _payload("Atlanta, GA"), ref_engineers, ref_homes, {1: (-84.39, 33.75)}
        )
        la_engineers, _ = _build_engineer_frames_from_payload(
            _payload("Los Angeles, CA"), ref_engineers, ref_homes, {1: (-118.24, 34.05)}
        )
        self.assertEqual(atl_engineers.iloc[0]["preferred_region_name"], "")
        self.assertEqual(la_engineers.iloc[0]["preferred_region_name"], "Region A")


if __name__ == "__main__":
    unittest.main()
