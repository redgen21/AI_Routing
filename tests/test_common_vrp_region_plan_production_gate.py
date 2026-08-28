from __future__ import annotations

import io
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from services.api.common_vrp_config import normalize_region_plan_runtime
from smart_routing import common_vrp_api_server as api
from smart_routing import common_vrp_db as db
from smart_routing import common_vrp_runtime as runtime


class RegionPlanProductionGateTests(unittest.TestCase):
    def test_config_defaults_to_deny_and_requires_boolean(self) -> None:
        self.assertEqual({"production_enabled": False}, normalize_region_plan_runtime({}))
        with self.assertRaisesRegex(ValueError, "must be a boolean"):
            normalize_region_plan_runtime({"region_plan_runtime": {"production_enabled": "true"}})

    def test_production_gate_off_skips_all_plan_reads(self) -> None:
        with patch.object(runtime, "load_common_config", return_value={"environment": "production"}), patch.object(runtime, "region_plan_production_enabled", return_value=False), patch.object(runtime, "get_configured_region_plan_snapshot") as configured, patch.object(runtime, "get_active_region_plan_snapshot") as active:
            self.assertIsNone(runtime._active_atlanta6_plan("LGEAI", "Atlanta, GA", runtime.COMMON_CONFIG_PATH))
        configured.assert_not_called()
        active.assert_not_called()

    def test_production_gate_uses_pinned_plan_without_activation_fallback(self) -> None:
        snapshot = {"enabled": True, "status": "active", "context_status": "active", "plan_id": "p1", "checksum": "a" * 64}
        with patch.object(runtime, "load_common_config", return_value={"environment": "production"}), patch.object(runtime, "region_plan_production_enabled", return_value=True), patch.object(runtime, "get_configured_region_plan_snapshot", return_value=snapshot), patch.object(runtime, "get_active_region_plan_snapshot") as active:
            actual = runtime._active_atlanta6_plan("LGEAI", "Atlanta, GA", runtime.COMMON_CONFIG_PATH)
        self.assertEqual("p1", actual["plan_id"])
        active.assert_not_called()

    def test_readiness_reports_missing_schema_without_plan_reads(self) -> None:
        missing = pd.DataFrame([{relation: None for relation in db.REGION_PLAN_RUNTIME_RELATIONS}])
        with patch.object(db, "load_common_config", return_value={"environment": "production", "region_plan_runtime": {"production_enabled": True}}), patch.object(db, "region_plan_production_enabled", return_value=True), patch.object(db, "_fetch_df", return_value=missing), patch.object(db, "get_configured_region_plan_snapshot") as selected:
            readiness = db.region_plan_runtime_readiness()
        self.assertFalse(readiness["ready"])
        self.assertIn("REGION_PLAN_RUNTIME_SCHEMA_UNAVAILABLE", readiness["errors"])
        selected.assert_not_called()

    def test_readiness_endpoint_returns_503_for_failed_gate(self) -> None:
        handler = object.__new__(api.CommonVRPRequestHandler)
        handler.path = "/api/v1/common/readiness"
        handler.wfile = io.BytesIO()
        handler.send_response = lambda status: setattr(handler, "status", status)
        handler.send_header = lambda *_: None
        handler.end_headers = lambda: None
        with patch.object(api, "region_plan_runtime_readiness", return_value={"ready": False, "errors": ["REGION_PLAN_RUNTIME_SCHEMA_UNAVAILABLE"]}):
            handler.do_GET()
        self.assertEqual(503, int(handler.status))
        self.assertFalse(json.loads(handler.wfile.getvalue())["ready"])


if __name__ == "__main__":
    unittest.main()
