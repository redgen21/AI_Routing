from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from admin_tools.db.guard import require_db_write_allowed


def _write_config(path: Path, environment: str | None) -> Path:
    database_names = {
        "development": "vrp_db_dev",
        "production": "vrp_db",
    }
    payload = {} if environment is None else {
        "environment": environment,
        "database": {"dbname": database_names.get(environment, "unknown_db")},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class DbAdminGuardTests(unittest.TestCase):
    def test_development_write_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _write_config(Path(temp_dir) / "dev.json", "development")

            require_db_write_allowed(config_path)

    def test_production_write_requires_explicit_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _write_config(Path(temp_dir) / "prod.json", "production")

            with self.assertRaisesRegex(ValueError, "confirm-production"):
                require_db_write_allowed(config_path)

            require_db_write_allowed(config_path, confirm_production=True)

    def test_unknown_or_missing_environment_is_blocked(self) -> None:
        for environment in [None, "", "staging", "prod"]:
            with self.subTest(environment=environment), tempfile.TemporaryDirectory() as temp_dir:
                config_path = _write_config(Path(temp_dir) / "unsafe.json", environment)

                with self.assertRaisesRegex(ValueError, "exactly development or production"):
                    require_db_write_allowed(config_path)

    def test_development_label_cannot_target_production_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _write_config(Path(temp_dir) / "mismatch.json", "development")
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            payload["database"]["dbname"] = "vrp_db"
            config_path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "vrp_db_dev"):
                require_db_write_allowed(config_path)

    def test_production_label_cannot_target_development_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = _write_config(Path(temp_dir) / "mismatch.json", "production")
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            payload["database"]["dbname"] = "vrp_db_dev"
            config_path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "database.dbname=vrp_db"):
                require_db_write_allowed(config_path, confirm_production=True)


if __name__ == "__main__":
    unittest.main()
