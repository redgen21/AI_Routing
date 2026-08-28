from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

import pandas as pd
from streamlit.testing.v1 import AppTest


ATLANTA_REGION_CITY_NAMES = (
    "Atlanta_6area",
    "Atlanta_3area",
    "Atlanta_6area_new",
    "Atlanta_6area_overlab",
)


class AtlantaRegionCityUiTests(unittest.TestCase):
    def test_common_clients_use_configured_slot_policy_for_ui_defaults(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"COMMON_VRP_CONFIG_PATH": "config/common_vrp.dev.json"},
        ):
            import sr_common_vrp_client as client
            import sr_common_vrp_client_server as server

        for module in (client, server):
            self.assertEqual(module._slot_minutes(), 40)
            self.assertEqual(module._default_technician_slot_count(), 9)
            self.assertEqual(module._estimate_service_time_min(pd.Series({"job_slot_count": 1})), 40.0)
            self.assertEqual(module._estimate_service_time_min(pd.Series({"job_slot_count": 2})), 80.0)
            self.assertEqual(module._estimate_service_time_min(pd.Series({"service_time_min": 55})), 55.0)
            self.assertEqual(
                module._estimate_service_time_min(pd.Series({"is_heavy_repair": True, "service_time_min": 55})),
                100.0,
            )
            normalized = module._normalize_technician_rows(
                pd.DataFrame([{"employee_code": "TECH-1", "slot_count": 6}]),
                pd.DataFrame(),
                "LGEAI",
                "Atlanta, GA",
                "2026-08-28",
                default_source="test",
            )
            self.assertEqual(int(normalized.iloc[0]["slot_count"]), 6)

    def test_common_clients_expose_city_options_and_reuse_atlanta_runtime_truth(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"COMMON_VRP_CONFIG_PATH": "config/common_vrp.dev.json"},
        ):
            import sr_common_vrp_client as client
            import sr_common_vrp_client_server as server

        for module in (client, server):
            for city_name in ATLANTA_REGION_CITY_NAMES:
                self.assertIn(city_name, module.DEFAULT_STRATEGIC_CITY_OPTIONS)
                self.assertEqual(module._geometry_city_name(city_name), "Atlanta, GA")
                self.assertEqual(module._profile_city_name(city_name), "Atlanta, GA")
            original = module._load_common_client_config
            try:
                module._load_common_client_config = lambda: {
                    "routing_seed": {"city_osrm_urls": {"Atlanta, GA": "http://127.0.0.1:5002"}}
                }
                for city_name in ATLANTA_REGION_CITY_NAMES:
                    self.assertEqual(module._resolve_city_osrm_url(city_name), "http://127.0.0.1:5002")
            finally:
                module._load_common_client_config = original

    def test_map_city_options_filter_base_atlanta_service_rows(self) -> None:
        import sr_area_map as area_map

        source = pd.DataFrame(
            {
                "SUBSIDIARY_NAME": ["LGEAI", "LGEAI"],
                "STRATEGIC_CITY_NAME": ["Atlanta, GA", "Los Angeles, CA"],
            }
        )
        for city_name in ATLANTA_REGION_CITY_NAMES:
            filtered = area_map._apply_service_scope_filters(source, "LGEAI", city_name)
            self.assertEqual(filtered["STRATEGIC_CITY_NAME"].tolist(), ["Atlanta, GA"])
            self.assertEqual(area_map._base_city_name(city_name), "Atlanta, GA")
            self.assertEqual(
                area_map.DEFAULT_CITY_OSRM_URLS[city_name],
                area_map.DEFAULT_CITY_OSRM_URLS["Atlanta, GA"],
            )

        with tempfile.NamedTemporaryFile(suffix=".csv") as service_file, mock.patch(
            "pandas.read_csv", return_value=source
        ):
            _, city_options = area_map.get_service_scope_options(service_file.name)
        self.assertEqual(
            city_options["LGEAI"],
            ["ALL", "Atlanta, GA", *ATLANTA_REGION_CITY_NAMES, "Los Angeles, CA"],
        )

    def test_arbitrary_api_context_city_uses_declared_metadata(self) -> None:
        contexts = {
            "cities_by_subsidiary": {"LGEAI": ["Chicago_plan_v1"]},
            "city_metadata": {
                "Chicago_plan_v1": {
                    "source_strategic_city_name": "Chicago, IL",
                    "profile_city_name": "Chicago Profile",
                    "osrm_url": "http://127.0.0.1:5999",
                }
            },
        }
        with mock.patch.dict(
            os.environ,
            {"COMMON_VRP_CONFIG_PATH": "config/common_vrp.dev.json"},
        ):
            import sr_common_vrp_client as client
            import sr_common_vrp_client_server as server

        for module in (client, server):
            module._register_context_city_metadata(contexts)
            self.assertIn("Chicago_plan_v1", module._city_options_for_subsidiary(contexts, "LGEAI"))
            self.assertEqual(module._geometry_city_name("Chicago_plan_v1"), "Chicago, IL")
            self.assertEqual(module._profile_city_name("Chicago_plan_v1"), "Chicago Profile")
            self.assertEqual(module._resolve_city_osrm_url("Chicago_plan_v1"), "http://127.0.0.1:5999")

    def test_la6area_active_context_uses_api_source_rows_and_routing_payload(self) -> None:
        contexts = {
            "cities_by_subsidiary": {"LGEAI": ["LA_6area"]},
            "region_plan_cities": [
                {
                    "subsidiary_name": "LGEAI",
                    "strategic_city_name": "LA_6area",
                    "source_strategic_city_name": "Los Angeles, CA",
                }
            ],
        }
        with mock.patch.dict(
            os.environ,
            {"COMMON_VRP_CONFIG_PATH": "config/common_vrp.dev.json"},
        ):
            import sr_common_vrp_client as client
            import sr_common_vrp_client_server as server

        for module in (client, server):
            module._register_context_city_metadata(contexts)
            self.assertEqual(module._city_options_for_subsidiary(contexts, "LGEAI"), ["LA_6area"])
            calls: list[tuple[str, str]] = []

            def fake_api_get(_origin, path, **query):
                calls.append((path, query["strategic_city_name"]))
                if path.endswith("/jobs"):
                    return {"rows": [{"receipt_no": "LA-1", "promise_date": "20260724"}]}
                return {"rows": [{"employee_code": "LA-TECH", "promise_date": "20260724"}]}

            with mock.patch.object(module, "_api_get", side_effect=fake_api_get):
                source_city, jobs_df, technicians_df = module._load_payload_source_rows(
                    "LGEAI", "LA_6area", "20260724"
                )
            self.assertEqual(source_city, "Los Angeles, CA")
            self.assertEqual(calls, [
                ("/api/v1/common/jobs", "Los Angeles, CA"),
                ("/api/v1/common/technicians", "Los Angeles, CA"),
            ])
            request = module._build_payload_request(
                "LGEAI", "LA_6area", "20260724", jobs_df, technicians_df, []
            )
            self.assertEqual(request["strategic_city_name"], "LA_6area")
            self.assertEqual(request["jobs"][0]["receipt_no"], "LA-1")
            self.assertEqual(request["technicians"][0]["employee_code"], "LA-TECH")

    def test_map_plan_status_is_read_only_with_no_authoring_or_download_controls(self) -> None:
        def panel() -> None:
            import sr_area_map as area_map

            area_map._region_plan_api_origin = lambda: ""
            area_map._render_atlanta_plan_comparison("Atlanta_6area")

        rendered = AppTest.from_function(panel).run(timeout=10)
        self.assertTrue(any("visualization/debug only" in item.value for item in rendered.info))
        self.assertEqual(rendered.file_uploader, [])
        self.assertEqual(rendered.get("download_button"), [])
        self.assertEqual(rendered.button, [])


if __name__ == "__main__":
    unittest.main()
