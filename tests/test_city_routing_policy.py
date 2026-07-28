import unittest
from unittest.mock import Mock, patch

import pandas as pd

from smart_routing.vrp_mode_na_general import (
    EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
    HOME_DISTANCE_ONLY,
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
    OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
    PREFERRED_REGION_SOFT,
    _approved_boundary_overflow_employee_codes,
    _build_city_route_client,
    _build_engineer_frames_from_payload,
    _build_service_frame_from_payload,
    _hard_eligible_employee_codes,
    _load_reference_inputs,
    _technician_assigned_region_name,
    run_mode,
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
        self.assertFalse(client.cfg.fail_closed_on_osrm_error)

    def test_city_route_client_passes_fail_closed_osrm_option(self) -> None:
        payload = _payload("Los Angeles, CA")
        payload["options"] = {
            "osrm_url": "http://127.0.0.1:5001",
            "fail_closed_on_osrm_error": "true",
        }

        client = _build_city_route_client(payload)

        self.assertTrue(client.cfg.fail_closed_on_osrm_error)

    @patch("smart_routing.production_assign_atlanta_vrp.build_atlanta_production_assignment_vrp_from_frames")
    @patch("smart_routing.vrp_mode_na_general._build_city_route_client")
    def test_run_mode_exposes_matrix_telemetry(
        self,
        build_route_client: Mock,
        build_assignment: Mock,
    ) -> None:
        payload = _payload("Los Angeles, CA")
        payload["planning_date"] = "2026-07-21"
        payload["options"] = {"osrm_url": "http://127.0.0.1:5001"}
        payload["jobs"] = [{
            "receipt_no": "J1",
            "location": {"lat": 34.05, "lng": -118.24},
            "service_minutes": 45,
        }]
        route_client = Mock()
        route_client.cfg.osrm_url = "http://127.0.0.1:5001"
        route_client.cfg.osrm_profile = "driving"
        route_client.cfg.mode = "osrm"
        route_client.cfg.fail_closed_on_osrm_error = False
        route_client.get_matrix_telemetry.return_value = {
            "matrix_source": "osrm_primary",
            "distance_unit": "km",
            "duration_unit": "min",
            "request_count": 1,
        }
        build_route_client.return_value = route_client
        build_assignment.return_value = (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

        result = run_mode(payload)

        self.assertEqual(result["diagnostics"]["matrix_telemetry"], route_client.get_matrix_telemetry.return_value)
        self.assertEqual(result["diagnostics"]["matrix_fallback"], "haversine_on_osrm_error")
        build_assignment.assert_called_once()

    def test_city_policy_is_explicit_for_atlanta_and_la_contexts(self) -> None:
        self.assertEqual(resolve_city_routing_policy(_payload("Atlanta, GA")), HOME_DISTANCE_ONLY)
        self.assertEqual(
            resolve_city_routing_policy(_payload("Atlanta_6area")),
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
        )
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

    def test_atlanta_6area_rejects_request_policy_downgrade(self) -> None:
        for requested_policy in (HOME_DISTANCE_ONLY, PREFERRED_REGION_SOFT):
            with self.subTest(requested_policy=requested_policy):
                payload = _payload("Atlanta_6area")
                payload["options"]["region_policy"] = requested_policy
                with self.assertRaisesRegex(ValueError, "explicitly supported active-plan policy"):
                    resolve_city_routing_policy(payload)

        legacy_payload = _payload("Atlanta, GA")
        legacy_payload["options"]["region_policy"] = PREFERRED_REGION_SOFT
        self.assertEqual(resolve_city_routing_policy(legacy_payload), PREFERRED_REGION_SOFT)

    def test_atlanta_6area_honors_only_explicit_v1_or_v2_snapshot_policy(self) -> None:
        for policy in (
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V1,
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
        ):
            with self.subTest(policy=policy):
                payload = _payload("Atlanta_6area")
                payload["options"]["region_policy"] = policy
                self.assertEqual(resolve_city_routing_policy(payload), policy)
        invalid = _payload("Atlanta_6area")
        invalid["options"]["region_policy"] = "own_region_with_approved_boundary_overflow/v999"
        with self.assertRaisesRegex(ValueError, "v999"):
            resolve_city_routing_policy(invalid)

    def test_explicit_workbook_membership_policy_is_city_independent(self) -> None:
        for city_name in ("Atlanta_3area", "Atlanta_6area_new", "Any Workbook City"):
            with self.subTest(city_name=city_name):
                payload = _payload(city_name)
                payload["options"]["region_policy"] = EXPLICIT_WORKBOOK_MEMBERSHIP_V1
                self.assertEqual(
                    resolve_city_routing_policy(payload),
                    EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
                )

        active_plan_payload = _payload("Unlisted City")
        active_plan_payload["options"] = {
            "region_plan": {"plan_id": "immutable-plan-1", "policy_version": EXPLICIT_WORKBOOK_MEMBERSHIP_V1}
        }
        self.assertEqual(
            resolve_city_routing_policy(active_plan_payload),
            EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
        )

        conflict_payload = _payload("Unlisted City")
        conflict_payload["options"] = {
            "region_policy": EXPLICIT_WORKBOOK_MEMBERSHIP_V1,
            "region_plan": {"plan_id": "immutable-plan-1", "policy_version": OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2},
        }
        with self.assertRaisesRegex(ValueError, "conflicts with immutable active-plan"):
            resolve_city_routing_policy(conflict_payload)

    def test_explicit_workbook_overflow_is_job_scoped_without_widening_candidates(self) -> None:
        def service_row(
            city_name: str,
            eligible_codes: list[str],
            boundary_codes: list[str],
        ) -> pd.Series:
            payload = _payload(city_name)
            payload["planning_date"] = "2026-07-21"
            payload["options"]["region_policy"] = EXPLICIT_WORKBOOK_MEMBERSHIP_V1
            payload["jobs"] = [{
                "receipt_no": "J1",
                "location": {"lat": 33.75, "lng": -84.39},
                "postal_code": "30028",
                "region_seq": 2,
                "region_name": "Zone 2",
                "eligible_employee_codes": eligible_codes,
                "boundary_overflow_employee_codes": boundary_codes,
            }]
            return _build_service_frame_from_payload(payload, {}).iloc[0]

        no_overflow = service_row("Any Workbook City", ["T1"], [])
        self.assertEqual(no_overflow["eligible_employee_codes"], ["T1"])
        self.assertEqual(no_overflow["boundary_overflow_employee_codes"], [])

        overflow = service_row("Completely Different City", ["T1", "T2"], ["T2"])
        self.assertEqual(overflow["eligible_employee_codes"], ["T1", "T2"])
        self.assertEqual(overflow["boundary_overflow_employee_codes"], ["T2"])

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

    def test_atlanta_6area_uses_assigned_region_without_enabling_legacy_soft_preference(self) -> None:
        payload = _payload("Atlanta_6area")
        payload["technicians"][0]["assigned_region_name"] = "Zone 2"
        ref_engineers, ref_homes = _empty_reference_frames()

        engineers, _ = _build_engineer_frames_from_payload(
            payload, ref_engineers, ref_homes, {2: (-84.39, 33.75)}
        )

        self.assertEqual(engineers.iloc[0]["assigned_region_name"], "Zone 2")
        self.assertEqual(engineers.iloc[0]["preferred_region_name"], "")

    def test_hard_and_boundary_candidate_helpers_never_widen_candidates(self) -> None:
        job = {
            "eligible_employee_codes": ["T1", "T2", "T2", ""],
            "boundary_overflow_employee_codes": ["T2", "T3"],
        }
        self.assertEqual(_hard_eligible_employee_codes(job), ["T1", "T2"])
        self.assertEqual(_approved_boundary_overflow_employee_codes(job), ["T2"])
        self.assertEqual(_hard_eligible_employee_codes({"eligible_employee_codes": []}), [])
        self.assertEqual(
            _approved_boundary_overflow_employee_codes(
                {"boundary_overflow_employee_codes": ["T2"]}
            ),
            [],
        )

    def test_assigned_region_uses_legacy_preferred_region_as_blank_safe_fallback(self) -> None:
        self.assertEqual(
            _technician_assigned_region_name(
                {"assigned_region_name": "", "preferred_region_name": "Zone 3"}
            ),
            "Zone 3",
        )

    def test_service_frame_preserves_empty_hard_candidate_set(self) -> None:
        payload = _payload("Atlanta_6area")
        payload["planning_date"] = "2026-07-21"
        payload["jobs"] = [
            {
                "receipt_no": "J1",
                "location": {"lat": 33.75, "lng": -84.39},
                "postal_code": "30028",
                "region_seq": 2,
                "region_name": "Zone 2",
                "eligible_employee_codes": [],
                "boundary_overflow_employee_codes": ["T1"],
            }
        ]

        service_df = _build_service_frame_from_payload(payload, {})

        self.assertEqual(service_df.iloc[0]["eligible_employee_codes"], [])
        self.assertEqual(service_df.iloc[0]["boundary_overflow_employee_codes"], [])
        self.assertEqual(
            service_df.iloc[0]["region_policy"],
            OWN_REGION_WITH_APPROVED_BOUNDARY_OVERFLOW_V2,
        )

    def test_atlanta_6area_does_not_nearest_fill_unmapped_postal(self) -> None:
        payload = _payload("Atlanta_6area")
        payload["planning_date"] = "2026-07-21"
        payload["jobs"] = [
            {
                "receipt_no": "MAPPED",
                "location": {"lat": 33.75, "lng": -84.39},
                "postal_code": "30028",
                "region_seq": 2,
                "region_name": "Zone 2",
                "eligible_employee_codes": ["T1"],
            },
            {
                "receipt_no": "UNMAPPED",
                "location": {"lat": 33.76, "lng": -84.38},
                "postal_code": "99999",
                "eligible_employee_codes": ["T1"],
            },
        ]

        service_df = _build_service_frame_from_payload(payload, {})
        unmapped = service_df[service_df["GSFS_RECEIPT_NO"] == "UNMAPPED"].iloc[0]

        self.assertTrue(pd.isna(unmapped["region_seq"]))
        self.assertEqual(unmapped["new_region_name"], "")
        self.assertEqual(unmapped["eligible_employee_codes"], [])

    def test_legacy_city_keeps_nearest_region_fallback(self) -> None:
        payload = _payload("Atlanta, GA")
        payload["planning_date"] = "2026-07-21"
        payload["jobs"] = [
            {
                "receipt_no": "J1",
                "location": {"lat": 33.75, "lng": -84.39},
                "postal_code": "99999",
            }
        ]

        service_df = _build_service_frame_from_payload(payload, {})

        self.assertEqual(int(service_df.iloc[0]["region_seq"]), 1)
        self.assertEqual(service_df.iloc[0]["new_region_name"], "Atlanta New Region 1")


if __name__ == "__main__":
    unittest.main()
