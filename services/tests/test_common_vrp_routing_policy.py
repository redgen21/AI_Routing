from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.api.common_vrp_config import load_and_validate_common_config


def _config() -> dict:
    return {
        "environment": "development",
        "api": {"host": "0.0.0.0", "port": 8066},
        "routing_api_url": "http://127.0.0.1:8066",
        "database": {
            "host": "localhost", "port": 5432, "dbname": "vrp_db_dev",
            "user": "vrp_agent", "password": "test-only-password",
        },
    }


class RoutingPolicyConfigTests(unittest.TestCase):
    def _write(self, config: dict) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "config.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        return path

    def test_legacy_config_receives_historical_policy(self) -> None:
        loaded = load_and_validate_common_config(self._write(_config()))
        self.assertEqual(
            {
                "slot_minutes": 45,
                "default_technician_slot_count": 8,
                "heavy_job_min_service_minutes": 100,
            },
            loaded["routing_policy"],
        )

    def test_complete_policy_is_validated_and_normalized(self) -> None:
        config = _config()
        config["routing_policy"] = {
            "slot_minutes": 40,
            "default_technician_slot_count": 9,
            "heavy_job_min_service_minutes": 100,
        }
        loaded = load_and_validate_common_config(self._write(config))
        self.assertEqual(40, loaded["routing_policy"]["slot_minutes"])
        self.assertEqual(9, loaded["routing_policy"]["default_technician_slot_count"])

    def test_partial_or_non_integral_policy_is_rejected(self) -> None:
        config = _config()
        config["routing_policy"] = {"slot_minutes": 40}
        with self.assertRaisesRegex(ValueError, "missing required settings"):
            load_and_validate_common_config(self._write(config))
        config["routing_policy"] = {
            "slot_minutes": 40.5,
            "default_technician_slot_count": 9,
            "heavy_job_min_service_minutes": 100,
        }
        with self.assertRaisesRegex(ValueError, "slot_minutes must be a positive integer"):
            load_and_validate_common_config(self._write(config))


if __name__ == "__main__":
    unittest.main()
