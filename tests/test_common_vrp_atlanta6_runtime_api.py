from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from smart_routing import common_vrp_api_server as api
from smart_routing import common_vrp_runtime as runtime
from smart_routing.vrp_mode_na_general import _fixed_technician_outside_active_plan


SNAPSHOT = {
    "enabled": True,
    "status": "active",
    "context_status": "active",
    "plan_id": "plan-1",
    "revision": 2,
    "policy_version": "own_region_with_approved_boundary_overflow/v2",
    "checksum": "a" * 64,
    "activation_revision": 3,
    "postals": [{"postal_code": "30301", "region_seq": 1, "region_name": "One"}],
    "technicians": [
        {"employee_code": "T1", "assigned_region_seq": 1, "active_flag": True},
        {"employee_code": "T2", "assigned_region_seq": 2, "active_flag": True},
    ],
    "boundary_overflow": [{"postal_code": "30301", "alternate_region_seq": 2, "allow_overflow": True}],
}


class Atlanta6RuntimeApiTests(unittest.TestCase):
    def test_source_city_resolution_preserves_subsidiary_and_config(self) -> None:
        config_path = runtime.Path("custom-development.json")
        snapshot = dict(SNAPSHOT, source_strategic_city_name="Source City")
        with patch.object(runtime, "_active_atlanta6_plan", return_value=snapshot) as active_plan:
            result = runtime._runtime_source_city("ACME", "Plan City", config_path)
        self.assertEqual("Source City", result)
        active_plan.assert_called_once_with("ACME", "Plan City", config_path)

    def test_arbitrary_active_plan_context_uses_declared_source_city(self) -> None:
        snapshot = dict(SNAPSHOT, policy_version="explicit_workbook_membership/v1", source_strategic_city_name="Base City")
        with patch.object(runtime, "_active_atlanta6_plan", return_value=snapshot), patch.object(runtime, "list_engineers", side_effect=[pd.DataFrame(), pd.DataFrame([{"employee_code": "T1"}])]) as engineers:
            result = runtime._runtime_engineer_master("ACME", "Any Region Plan")
        self.assertEqual(["Any Region Plan", "Base City"], [call.args[1] for call in engineers.call_args_list])
        self.assertEqual("T1", result.iloc[0]["employee_code"])

    def test_runtime_context_contract_exposes_arbitrary_region_plan_metadata(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="explicit_workbook_membership/v1",
            source_strategic_city_name="Chicago, IL",
        )
        with patch.object(runtime, "list_contexts", return_value={"cities": [], "cities_by_subsidiary": {}, "subsidiaries": []}), patch.object(
            runtime,
            "list_active_region_plan_contexts",
            return_value=[{"subsidiary_name": "ACME", "strategic_city_name": "Chicago_plan_v1"}],
        ), patch.object(runtime, "_active_atlanta6_plan", return_value=snapshot):
            contexts = runtime.list_runtime_contexts()
        self.assertEqual(
            [{
                "subsidiary_name": "ACME",
                "strategic_city_name": "Chicago_plan_v1",
                "source_strategic_city_name": "Chicago, IL",
                "geometry_city_name": "Chicago, IL",
                "profile_city_name": "Chicago, IL",
            }],
            contexts["region_plan_cities"],
        )

    def test_arbitrary_plan_dms2_membership_excludes_dms_technician(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="explicit_workbook_membership/v1",
            postals=[{"postal_code": "30301", "region_seq": 1, "area_type": "DMS2"}],
            technicians=[
                {"employee_code": "DMS1", "assigned_region_seq": 1},
                {"employee_code": "DMS2A", "assigned_region_seq": 1},
            ],
            boundary_overflow=[],
        )
        payload = {
            "technicians": [
                {"employee_code": "DMS1", "center_type": "DMS"},
                {"employee_code": "DMS2A", "center_type": "DMS2"},
            ],
            "jobs": [{"postal_code": "30301"}],
        }
        result = runtime._apply_active_region_plan(payload, snapshot)
        self.assertEqual("DMS2", result["jobs"][0]["area_type"])
        self.assertEqual(["DMS2A"], result["jobs"][0]["eligible_employee_codes"])
        prepared = runtime._apply_job_area_type_rules(
            result["jobs"], result["technicians"], "ACME", "Any Plan City"
        )
        self.assertEqual("DMS2", prepared[0]["area_type"])
        self.assertEqual(["DMS2A"], prepared[0]["eligible_employee_codes"])

    def test_arbitrary_plan_dms2_explicit_empty_stays_unassigned(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="explicit_workbook_membership/v1",
            postals=[{"postal_code": "30301", "region_seq": 1, "area_type": "DMS2"}],
            technicians=[{"employee_code": "DMS2A", "assigned_region_seq": 1}],
            boundary_overflow=[],
        )
        result = runtime._apply_active_region_plan(
            {
                "technicians": [{"employee_code": "DMS2A", "center_type": "DMS2"}],
                "jobs": [{"postal_code": "30301", "eligible_employee_codes": []}],
            },
            snapshot,
        )
        self.assertEqual([], result["jobs"][0]["eligible_employee_codes"])
        self.assertEqual("NO_ELIGIBLE_TECHNICIAN", result["jobs"][0]["region_plan_unassigned_marker"])

    def test_snapshot_makes_own_region_hard_and_overflow_explicit(self) -> None:
        payload = {
            "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}, {"employee_code": "T3"}],
            "jobs": [{"postal_code": "30301", "eligible_employee_codes": ["T1", "T2"]}],
        }
        result = runtime._apply_active_region_plan(payload, SNAPSHOT)
        self.assertEqual(["T1", "T2"], [row["employee_code"] for row in result["technicians"]])
        self.assertEqual(["T1"], result["jobs"][0]["hard_eligible_employee_codes"])
        self.assertEqual(["T1", "T2"], result["jobs"][0]["eligible_employee_codes"])
        self.assertEqual(["T2"], result["jobs"][0]["boundary_overflow_employee_codes"])
        self.assertEqual(1, result["technicians"][0]["assigned_region_seq"])
        self.assertEqual("plan-1", result["options"]["region_plan"]["plan_id"])
        self.assertEqual("own_region_with_approved_boundary_overflow/v2", result["options"]["region_policy"])
        self.assertEqual("own_region_with_approved_boundary_overflow/v2", result["region_plan"]["policy_version"])

    def test_snapshot_policy_version_is_required_and_supported(self) -> None:
        missing = dict(SNAPSHOT)
        missing.pop("policy_version")
        with self.assertRaisesRegex(ValueError, "ATLANTA_6AREA_PLAN_POLICY_VERSION_REQUIRED"):
            runtime._apply_active_region_plan({}, missing)

        unsupported = dict(SNAPSHOT, policy_version="own_region_with_approved_boundary_overflow/v99")
        with self.assertRaisesRegex(ValueError, "ATLANTA_6AREA_PLAN_POLICY_VERSION_UNSUPPORTED"):
            runtime._apply_active_region_plan({}, unsupported)

    def test_active_roster_type_hard_region_soft_uses_type_not_same_region(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="active_roster_type_hard_region_soft/v1",
            postals=[
                {"postal_code": "30301", "region_seq": 1, "region_name": "DMS Region", "area_type": "DMS"},
                {"postal_code": "30302", "region_seq": 2, "region_name": "DMS2 Region", "area_type": "DMS2"},
            ],
            technicians=[
                {"employee_code": "DMS_A", "assigned_region_seq": 1},
                {"employee_code": "DMS_B", "assigned_region_seq": 2},
                {"employee_code": "DMS2_A", "assigned_region_seq": 1},
            ],
        )
        result = runtime._apply_active_region_plan(
            {
                "technicians": [
                    {"employee_code": "DMS_A", "center_type": "DMS"},
                    {"employee_code": "DMS_B", "center_type": "DMS"},
                    {"employee_code": "DMS2_A", "center_type": "DMS2"},
                ],
                "jobs": [{"postal_code": "30301", "eligible_employee_codes": ["DMS_B", "DMS2_A"]}],
            }, snapshot,
        )
        job = result["jobs"][0]
        self.assertEqual(["DMS_B"], job["eligible_employee_codes"])
        self.assertEqual([], job["boundary_overflow_employee_codes"])
        self.assertEqual(1, job["region_preference"]["region_seq"])
        self.assertEqual("DMS", job["area_type"])

    def test_active_roster_area_type_fallback_allows_dms2_for_dms_area(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="active_roster_area_type_fallback_region_soft/v1",
            postals=[{"postal_code": "30301", "region_seq": 1, "region_name": "DMS Region", "area_type": "DMS"}],
            technicians=[
                {"employee_code": "DMS_A", "assigned_region_seq": 1},
                {"employee_code": "DMS2_A", "assigned_region_seq": 2},
            ],
        )
        result = runtime._apply_active_region_plan(
            {
                "technicians": [
                    {"employee_code": "DMS_A", "center_type": "DMS"},
                    {"employee_code": "DMS2_A", "center_type": "DMS2"},
                ],
                "jobs": [{"postal_code": "30301", "eligible_employee_codes": ["DMS_A", "DMS2_A"]}],
            }, snapshot,
        )
        self.assertEqual(["DMS2_A", "DMS_A"], result["jobs"][0]["eligible_employee_codes"])
        self.assertEqual("DMS", result["jobs"][0]["area_type"])

    def test_active_roster_soft_policy_keeps_dms2_area_exclusive(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="active_roster_area_type_fallback_region_soft/v1",
            postals=[{"postal_code": "30302", "region_seq": 2, "region_name": "DMS2 Region", "area_type": "DMS2"}],
            technicians=[
                {"employee_code": "DMS_A", "assigned_region_seq": 2},
                {"employee_code": "DMS2_A", "assigned_region_seq": 1},
            ],
        )
        result = runtime._apply_active_region_plan(
            {
                "technicians": [
                    {"employee_code": "DMS_A", "center_type": "DMS"},
                    {"employee_code": "DMS2_A", "center_type": "DMS2"},
                ],
                "jobs": [{"postal_code": "30302", "eligible_employee_codes": ["DMS_A", "DMS2_A"]}],
            }, snapshot,
        )
        job = result["jobs"][0]
        self.assertEqual(["DMS2_A"], job["eligible_employee_codes"])
        self.assertEqual("DMS2", job["area_type"])
        self.assertEqual(2, job["region_preference"]["region_seq"])

    def test_active_roster_policy_keeps_unmapped_postal_type_feasible(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="active_roster_type_hard_region_soft/v1",
            postals=[{"postal_code": "30301", "region_seq": 1, "area_type": "DMS"}],
        )
        result = runtime._apply_active_region_plan(
            {"technicians": [{"employee_code": "T1", "center_type": "DMS"}, {"employee_code": "T2", "center_type": "DMS2"}],
             "jobs": [{"postal_code": "99999"}]}, snapshot,
        )
        job = result["jobs"][0]
        self.assertEqual(["T1", "T2"], job["eligible_employee_codes"])
        self.assertEqual("REGION_PREFERENCE_UNRESOLVED", job["region_preference_diagnostic"])
        self.assertNotIn("region_plan_unassigned_marker", job)

    def test_active_roster_policy_rejects_mixed_region_area_types(self) -> None:
        snapshot = dict(
            SNAPSHOT,
            policy_version="active_roster_type_hard_region_soft/v1",
            postals=[
                {"postal_code": "30301", "region_seq": 1, "area_type": "DMS"},
                {"postal_code": "30302", "region_seq": 1, "area_type": "DMS2"},
            ],
        )
        with self.assertRaisesRegex(ValueError, "REGION_AREA_TYPE_NOT_UNIFORM"):
            runtime._apply_active_region_plan({"technicians": [{"employee_code": "T1", "center_type": "DMS"}]}, snapshot)

    def test_explicit_legacy_v1_snapshot_remains_executable_without_defaulting(self) -> None:
        legacy = dict(SNAPSHOT, policy_version="own_region_with_approved_boundary_overflow/v1")
        result = runtime._apply_active_region_plan({"technicians": [], "jobs": []}, legacy)
        self.assertEqual("own_region_with_approved_boundary_overflow/v1", result["options"]["region_policy"])
        self.assertEqual("own_region_with_approved_boundary_overflow/v1", result["region_plan"]["policy_version"])

    def test_queued_snapshot_cannot_fall_back_to_server_region_policy(self) -> None:
        payload = {"options": {"region_plan": {"plan_id": "plan-1"}}}
        with patch.object(runtime, "_build_server_routing_options", return_value={"region_policy": "own_region_with_approved_boundary_overflow/v1"}):
            with self.assertRaisesRegex(ValueError, "ATLANTA_6AREA_PLAN_POLICY_VERSION_REQUIRED"):
                runtime._with_server_routing_options(payload, "LGEAI", "Atlanta_6area")

    def test_context_is_hidden_without_active_enabled_snapshot(self) -> None:
        with patch.object(runtime, "list_contexts", return_value={"subsidiaries": ["LGEAI"], "cities": ["Atlanta, GA"], "cities_by_subsidiary": {"LGEAI": ["Atlanta, GA"]}}), patch.object(runtime, "_active_atlanta6_plan", side_effect=ValueError("required")):
            self.assertNotIn("Atlanta_6area", runtime.list_runtime_contexts()["cities"])

    def test_activation_requires_all_concurrency_guards(self) -> None:
        with patch.object(api, "_region_plan_write_allowed", return_value=True):
            status, body = api._region_plan_request("activate", {"idempotency_key": "x"})
        self.assertEqual(400, status)
        self.assertEqual("ACTIVATION_GUARD_REQUIRED", body["error"])

    def test_unmapped_postal_is_preserved_for_solver_reason(self) -> None:
        result = runtime._apply_active_region_plan(
            {"technicians": [{"employee_code": "T1"}], "jobs": [{"postal_code": "99999", "fixed": True}]},
            SNAPSHOT,
        )
        job = result["jobs"][0]
        self.assertEqual([], job["eligible_employee_codes"])
        self.assertEqual("POSTAL_NOT_IN_ACTIVE_PLAN", job["region_plan_unassigned_marker"])
        self.assertNotIn("region_name", job)

    def test_rest_paths_match_ui_contract(self) -> None:
        operation, payload = api._region_plan_route("/region-plans/plan-1/ambiguity-resolutions", {}) or (None, {})
        self.assertEqual("resolution", operation)
        self.assertEqual("plan-1", payload["plan_id"])
        self.assertEqual(("list", {}), api._region_plan_route("/region-plans", {}))

    def test_explicit_empty_eligibility_is_never_widened(self) -> None:
        result = runtime._apply_active_region_plan(
            {"technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}], "jobs": [{"postal_code": "30301", "eligible_employee_codes": []}]},
            SNAPSHOT,
        )
        job = result["jobs"][0]
        self.assertEqual([], job["eligible_employee_codes"])
        self.assertEqual([], job["boundary_overflow_employee_codes"])
        self.assertEqual("NO_ELIGIBLE_TECHNICIAN", job["region_plan_unassigned_marker"])

    def test_list_returns_active_plan_metadata(self) -> None:
        with patch("smart_routing.common_vrp_db.load_common_config", return_value={"environment": "development"}), patch("smart_routing.common_vrp_db.get_active_region_plan_snapshot", return_value=SNAPSHOT):
            from smart_routing.common_vrp_db import region_plan_operation
            result = region_plan_operation("list", {"city_key": "Atlanta_6area"})
        self.assertEqual("plan-1", result["plans"][0]["plan_id"])

    def test_production_denies_active_region_plan_even_when_rows_exist(self) -> None:
        from smart_routing import common_vrp_db

        with patch.object(
            common_vrp_db,
            "load_common_config",
            return_value={"environment": "production", "region_plan_production_enabled": True},
        ), patch.object(common_vrp_db, "_fetch_df") as fetch:
            with self.assertRaisesRegex(RuntimeError, "REGION_PLAN_RUNTIME_DISABLED_IN_PRODUCTION"):
                common_vrp_db.get_active_region_plan_snapshot("LGEAI", "Atlanta_6area")
        fetch.assert_not_called()

    def test_queued_atlanta6_keeps_plan_candidates_and_policy(self) -> None:
        queued_payload = runtime._set_capability_snapshot(runtime._apply_active_region_plan(
            {
                "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}, {"employee_code": "T3"}],
                "jobs": [{"postal_code": "30301", "product_group": "A", "product": "P"}],
                "capabilities": [
                    {"employee_code": "T1", "product_group_code": "A", "product_code": "P"},
                    {"employee_code": "T2", "product_group_code": "A", "product_code": "P"},
                    {"employee_code": "T3", "product_group_code": "A", "product_code": "P"},
                ],
            },
            SNAPSHOT,
        ), [
            {"employee_code": "T1", "product_group_code": "A", "product_code": "P"},
            {"employee_code": "T2", "product_group_code": "A", "product_code": "P"},
            {"employee_code": "T3", "product_group_code": "A", "product_code": "P"},
        ], managed=True)
        # Exercise the exact second preparation pass used by queued jobs.
        with patch.object(runtime, "_build_server_routing_options", return_value={"region_policy": "home_distance_only"}), patch.object(runtime, "_enrich_jobs_heavy_repair", side_effect=lambda jobs, **_: jobs), patch.object(runtime, "list_engineers", return_value=pd.DataFrame([{"employee_code": "T1"}])):
            result = runtime._with_server_routing_options(queued_payload, "LGEAI", "Atlanta_6area")
        job = result["jobs"][0]
        self.assertEqual(["T1"], job["hard_eligible_employee_codes"])
        self.assertEqual(["T1", "T2"], job["eligible_employee_codes"])
        self.assertEqual(["T2"], job["boundary_overflow_employee_codes"])
        self.assertNotIn("T3", job["eligible_employee_codes"])
        self.assertEqual(SNAPSHOT["policy_version"], result["options"]["region_policy"])

    def test_queued_explicit_empty_remains_no_eligible_technician(self) -> None:
        queued_payload = runtime._set_capability_snapshot(runtime._apply_active_region_plan(
            {"technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}], "jobs": [{"postal_code": "30301", "eligible_employee_codes": []}]},
            SNAPSHOT,
        ), [], managed=True)
        with patch.object(runtime, "_build_server_routing_options", return_value={}), patch.object(runtime, "_enrich_jobs_heavy_repair", side_effect=lambda jobs, **_: jobs), patch.object(runtime, "list_engineers", return_value=pd.DataFrame([{"employee_code": "T1"}])):
            result = runtime._with_server_routing_options(queued_payload, "LGEAI", "Atlanta_6area")
        job = result["jobs"][0]
        self.assertEqual([], job["eligible_employee_codes"])
        self.assertEqual("NO_ELIGIBLE_TECHNICIAN", job["region_plan_unassigned_marker"])

    def test_queued_fixed_job_outside_plan_stays_safely_unassignable(self) -> None:
        snapshot = dict(SNAPSHOT)
        snapshot["technicians"] = [*SNAPSHOT["technicians"], {"employee_code": "T3", "assigned_region_seq": 3, "active_flag": True}]
        queued_payload = runtime._set_capability_snapshot(runtime._apply_active_region_plan(
            {
                "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}, {"employee_code": "T3"}],
                "jobs": [{"postal_code": "30301", "fixed": True, "current_employee_code": "T3"}],
            },
            snapshot,
        ), [], managed=True)
        with patch.object(runtime, "_build_server_routing_options", return_value={}), patch.object(runtime, "_enrich_jobs_heavy_repair", side_effect=lambda jobs, **_: jobs), patch.object(runtime, "list_engineers", return_value=pd.DataFrame([{"employee_code": "T1"}])):
            result = runtime._with_server_routing_options(queued_payload, "LGEAI", "Atlanta_6area")
        job = result["jobs"][0]
        self.assertEqual("T3", job["current_employee_code"])
        self.assertTrue(_fixed_technician_outside_active_plan(job, {"T1", "T2", "T3"}, result["options"]["region_policy"]))

    def test_managed_capabilities_replace_client_capabilities_and_snapshot(self) -> None:
        payload = {
            "technicians": [{"employee_code": "T1"}, {"employee_code": "T2"}],
            "jobs": [{"product_group": "A", "product": "P"}],
            "capabilities": [{"employee_code": "T2", "product_group_code": "A", "product_code": "P"}],
        }
        managed = pd.DataFrame([{"employee_code": "T1", "product_group_code": "A", "product_code": "P", "repair_allowed": True}])
        with patch.object(runtime, "_build_server_routing_options", return_value={}), patch.object(runtime, "_enrich_jobs_heavy_repair", side_effect=lambda jobs, **_: jobs), patch.object(runtime, "list_capabilities", return_value=managed), patch.object(runtime, "list_engineers", return_value=pd.DataFrame()):
            result = runtime._with_server_routing_options(payload, "LGEAI", "Dallas, TX")
        self.assertEqual(["T1"], result["jobs"][0]["eligible_employee_codes"])
        self.assertEqual(["T1"], [row["employee_code"] for row in result["capabilities"]])
        self.assertEqual("managed_master", result["options"]["capability_snapshot"]["source"])
        self.assertEqual(1, result["options"]["capability_snapshot"]["count"])

    def test_atlanta6_scenario_master_is_preferred_for_runtime_limits(self) -> None:
        scenario_master = pd.DataFrame([{"employee_code": "T1", "max_home_to_job_min": 23}])
        payload = {
            "technicians": [{"employee_code": "T1"}],
            "jobs": [],
            "capabilities": [],
            "options": {"region_plan": {"plan_id": "plan-1", "policy_version": "own_region_with_approved_boundary_overflow/v2"}},
        }
        with patch.object(runtime, "_build_server_routing_options", return_value={}), patch.object(runtime, "_enrich_jobs_heavy_repair", side_effect=lambda jobs, **_: jobs), patch.object(runtime, "list_capabilities", return_value=pd.DataFrame([{"employee_code": "T1", "product_group_code": "A", "product_code": "P"}])), patch.object(runtime, "list_engineers", return_value=scenario_master) as engineers:
            result = runtime._with_server_routing_options(payload, "LGEAI", "Atlanta_6area")
        engineers.assert_called_once_with("LGEAI", "Atlanta_6area", config_path=runtime.COMMON_CONFIG_PATH)
        self.assertEqual(23, result["technicians"][0]["max_home_to_job_min"])

    def test_atlanta6_empty_managed_master_fails_without_base_master_query(self) -> None:
        with patch.object(runtime, "list_engineers", return_value=pd.DataFrame()) as engineers:
            with self.assertRaisesRegex(ValueError, runtime.ATLANTA_6AREA_MANAGED_MASTER_REQUIRED):
                runtime._runtime_engineer_master("LGEAI", "Atlanta_6area")
        engineers.assert_called_once_with("LGEAI", "Atlanta_6area", config_path=runtime.COMMON_CONFIG_PATH)

    def test_atlanta6_empty_managed_master_is_visible_during_execution_preparation(self) -> None:
        payload = {
            "technicians": [{"employee_code": "T1"}],
            "jobs": [],
            "options": {"region_plan": {"plan_id": "plan-1", "policy_version": "own_region_with_approved_boundary_overflow/v2"}},
        }
        managed_capabilities = pd.DataFrame([{"employee_code": "T1", "product_group_code": "A", "product_code": "P"}])
        with patch.object(runtime, "_build_server_routing_options", return_value={}), patch.object(runtime, "_enrich_jobs_heavy_repair", side_effect=lambda jobs, **_: jobs), patch.object(runtime, "list_capabilities", return_value=managed_capabilities), patch.object(runtime, "list_engineers", return_value=pd.DataFrame()) as engineers:
            with self.assertRaisesRegex(ValueError, runtime.ATLANTA_6AREA_MANAGED_MASTER_REQUIRED):
                runtime._with_server_routing_options(payload, "LGEAI", "Atlanta_6area")
        engineers.assert_called_once_with("LGEAI", "Atlanta_6area", config_path=runtime.COMMON_CONFIG_PATH)

    def test_atlanta6_rejects_client_capability_fallback_when_managed_rows_missing(self) -> None:
        payload = {
            "technicians": [{"employee_code": "T1"}],
            "jobs": [],
            "capabilities": [{"employee_code": "T1", "product_group_code": "A", "product_code": "P"}],
            "options": {"region_plan": {"plan_id": "plan-1", "policy_version": "own_region_with_approved_boundary_overflow/v2"}},
        }
        with patch.object(runtime, "_build_server_routing_options", return_value={}), patch.object(runtime, "list_capabilities", return_value=pd.DataFrame()):
            with self.assertRaisesRegex(ValueError, "ATLANTA_6AREA_MANAGED_CAPABILITIES_REQUIRED"):
                runtime._with_server_routing_options(payload, "LGEAI", "Atlanta_6area")
