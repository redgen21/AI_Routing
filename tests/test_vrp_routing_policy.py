from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from smart_routing import common_vrp_runtime as runtime
from smart_routing import vrp_mode_na_general as na_general


class RoutingSlotPolicyTests(unittest.TestCase):
    def _config_path(self, payload: dict) -> tuple[tempfile.TemporaryDirectory, Path]:
        temp_dir = tempfile.TemporaryDirectory()
        path = Path(temp_dir.name) / "common_vrp.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return temp_dir, path

    def test_legacy_config_uses_legacy_slot_defaults(self) -> None:
        temp_dir, path = self._config_path({})
        self.addCleanup(temp_dir.cleanup)

        self.assertEqual(
            runtime._routing_slot_policy(path),
            {
                "slot_minutes": 45,
                "default_technician_slot_count": 8,
                "heavy_job_min_service_minutes": 100,
            },
        )

    def test_top_level_routing_policy_is_loaded(self) -> None:
        temp_dir, path = self._config_path(
            {
                "routing_policy": {
                    "slot_minutes": 40,
                    "default_technician_slot_count": 9,
                    "heavy_job_min_service_minutes": 100,
                }
            }
        )
        self.addCleanup(temp_dir.cleanup)

        self.assertEqual(runtime._routing_slot_policy(path)["slot_minutes"], 40)
        self.assertEqual(
            runtime._routing_slot_policy(path)["default_technician_slot_count"], 9
        )

    def test_explicit_service_minutes_win_over_computed_policy(self) -> None:
        rules = pd.DataFrame(
            [
                {
                    "product_group_code": "PG",
                    "product_code": "HEAVY",
                    "detailed_symptom_code": "SYM",
                }
            ]
        )
        jobs = [
            {"product_group": "OTHER", "product": "NORMAL", "symptom": "SYM", "job_slot_count": 1},
            {"product_group": "PG", "product": "HEAVY", "symptom": "SYM", "job_slot_count": 1},
            {
                "product_group": "PG",
                "product": "HEAVY",
                "symptom": "SYM",
                "job_slot_count": 1,
                "service_minutes": 55,
            },
        ]

        with patch.object(runtime, "list_heavy_repair_rules", return_value=rules):
            enriched = runtime._enrich_jobs_heavy_repair(
                jobs,
                slot_minutes=40,
                heavy_job_min_service_minutes=100,
            )

        self.assertEqual(enriched[0]["service_minutes"], 40)
        self.assertEqual(enriched[1]["job_slot_count"], 2)
        self.assertEqual(enriched[1]["service_minutes"], 100)
        self.assertEqual(enriched[2]["job_slot_count"], 2)
        self.assertEqual(enriched[2]["service_minutes"], 55)

    def test_na_general_uses_policy_defaults_when_payload_values_are_missing(self) -> None:
        payload = {
            "planning_date": "2026-08-28",
            "options": {
                "region_policy": "home_distance_only",
                "slot_minutes": 40,
                "default_technician_slot_count": 9,
                "heavy_job_min_service_minutes": 100,
            },
            "technicians": [
                {
                    "employee_code": "T1",
                    "employee_name": "Tech 1",
                    "start_location": {"lat": 33.75, "lng": -84.39},
                }
            ],
            "jobs": [
                {
                    "receipt_no": "J1",
                    "location": {"lat": 33.76, "lng": -84.38},
                    "job_slot_count": 1,
                }
            ],
        }
        empty_reference = pd.DataFrame({"SVC_ENGINEER_CODE": pd.Series(dtype=str)})
        engineer_df, _ = na_general._build_engineer_frames_from_payload(
            payload,
            empty_reference,
            empty_reference,
            {1: (-84.39, 33.75)},
        )
        with patch(
            "smart_routing.production_atlanta._build_heavy_repair_lookup",
            return_value=pd.DataFrame(),
        ), patch(
            "smart_routing.production_atlanta._enrich_service_df",
            side_effect=lambda frame, _lookup: frame,
        ):
            service_df = na_general._build_service_frame_from_payload(payload, {})

        self.assertEqual(int(engineer_df.iloc[0]["max_slots"]), 9)
        self.assertEqual(int(service_df.iloc[0]["service_time_min"]), 40)


if __name__ == "__main__":
    unittest.main()
