from __future__ import annotations

import base64
import json
import unittest
from unittest import mock

from services.deploy import console_backend


class RegionPlanV2ConsoleBackendTests(unittest.TestCase):
    def test_upload_posts_one_workbook_to_v2_api(self) -> None:
        response = {"contract_version": "region-plan/v2", "status": "accepted", "data": {"plan_id": "rp2_LA_6area_abc", "workbook_sha256": "a" * 64, "lifecycle": "candidate"}}
        with mock.patch.object(console_backend, "_region_plan_v2_request", return_value=response) as request:
            result = console_backend.import_region_plan_v2_workbook(
                workbook_name="LA Area Technician.xlsx", workbook_bytes=b"xlsx-bytes",
                metadata={"subsidiary_id": "LGEAI", "target_city_id": "LA_6area", "source_city_id": "Los Angeles, CA", "policy_version": "explicit_workbook_membership/v1", "technician_policy_mode": "assigned_region_boundary_spillover", "overlap_policy": "registry_default", "activation_intent": "review_only"},
            )
        self.assertEqual("rp2_LA_6area_abc", result["data"]["plan_id"])
        self.assertEqual("POST", request.call_args.args[0])
        self.assertEqual("/imports", request.call_args.args[1])
        body = json.loads(request.call_args.kwargs["body"])
        self.assertEqual(b"xlsx-bytes", base64.b64decode(body["workbook_base64"]))
        self.assertEqual("assigned_region_boundary_spillover", body["city_metadata"]["technician_policy_mode"])
        self.assertEqual("application/json", request.call_args.kwargs["headers"]["Content-Type"])
        self.assertIn("Idempotency-Key", request.call_args.kwargs["headers"])
        self.assertNotIn("X-Authenticated-Principal", request.call_args.kwargs["headers"])
        self.assertNotIn("source_sha256", body)

    def test_la_candidate_adopt_review_preview_activate_uses_only_api_contract(self) -> None:
        calls: list[tuple[str, str, dict]] = []
        responses = iter((
            {"status": "completed", "data": {"plans": [{"plan_id": "rp2_LA_6area_abc", "plan_revision": 1, "lifecycle": "candidate"}]}},
            {"status": "completed", "data": {"plan": {"plan_id": "rp2_LA_6area_abc", "plan_revision": 1, "activation_revision": 0, "lifecycle": "candidate"}}},
            {"status": "completed", "data": {"plan_id": "rp2_LA_6area_abc", "plan_revision": 2, "lifecycle": "reviewed"}},
            {"status": "completed", "data": {"plan_id": "rp2_LA_6area_abc", "plan_revision": 2, "activation_revision": 0, "preview_token": "p" * 64}},
            {"status": "completed", "data": {"plan_id": "rp2_LA_6area_abc", "activation_revision": 1, "lifecycle": "active"}},
        ))

        def api(method: str, path: str, **kwargs: object) -> dict:
            calls.append((method, path, dict(kwargs)))
            return next(responses)

        with mock.patch.object(console_backend, "_region_plan_v2_request", side_effect=api):
            listed = console_backend.list_region_plan_v2_candidates(subsidiary_id="LGEAI", target_city_id="LA_6area")
            adopted = console_backend.adopt_region_plan_v2_candidate(subsidiary_id="LGEAI", target_city_id="LA_6area", plan_id="rp2_LA_6area_abc")
            reviewed = console_backend.review_region_plan_v2(subsidiary_id="LGEAI", target_city_id="LA_6area", plan_id="rp2_LA_6area_abc", plan_revision=1, activation_revision=0)
            preview = console_backend.preview_region_plan_v2_activation(subsidiary_id="LGEAI", target_city_id="LA_6area", plan_id="rp2_LA_6area_abc", plan_revision=2, activation_revision=0)
            active = console_backend.activate_region_plan_v2(subsidiary_id="LGEAI", target_city_id="LA_6area", plan_id="rp2_LA_6area_abc", plan_revision=2, activation_revision=0, preview_token="p" * 64, activation_reference="LA validation")

        self.assertEqual("active", active["data"]["lifecycle"])
        self.assertEqual("rp2_LA_6area_abc", listed["data"]["plans"][0]["plan_id"])
        self.assertEqual(["/plans/list", "/adopt", "/plans/rp2_LA_6area_abc/review", "/plans/rp2_LA_6area_abc/activation-preview", "/plans/rp2_LA_6area_abc/activate"], [path for _, path, _ in calls])
        self.assertEqual("1", calls[2][2]["headers"]["If-Match"])
        self.assertEqual("2", calls[4][2]["headers"]["If-Match"])
        self.assertNotIn("X-Authenticated-Principal", calls[4][2]["headers"])
        activate_body = json.loads(calls[4][2]["body"])
        self.assertEqual(0, activate_body["activation_revision"])
        self.assertEqual("LGEAI", activate_body["subsidiary_id"])
        self.assertEqual("LA_6area", activate_body["target_city_id"])
        self.assertIn("preview_token", activate_body)
        self.assertNotIn("bundle_sha256", activate_body)
        self.assertEqual("reviewed", reviewed["data"]["lifecycle"])
        self.assertTrue(preview["data"]["preview_token"])


if __name__ == "__main__":
    unittest.main()
