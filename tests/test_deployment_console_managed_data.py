from __future__ import annotations

import contextlib
import hashlib
import json
import os
import stat
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from services.deploy import console_backend


class _ManagedRemote:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
        self.uploads: list[str] = []
        self.locks: list[tuple[str, str]] = []

    def __enter__(self) -> "_ManagedRemote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    @contextlib.contextmanager
    def deployment_lock(self, root: str, deployment_id: str):
        self.locks.append((root, deployment_id))
        yield

    def exists(self, target: str) -> bool:
        return target in self.files

    def sha256(self, target: str) -> str | None:
        payload = self.files.get(target)
        return hashlib.sha256(payload).hexdigest() if payload is not None else None

    def mode(self, target: str) -> int | None:
        return self.modes.get(target)

    def upload_bytes_atomic(
        self, payload: bytes, target: str, backup: str | None = None
    ) -> None:
        if backup is not None:
            raise AssertionError("immutable managed-data uploads must not request backups")
        self.uploads.append(target)
        self.files[target] = payload
        self.modes[target] = 0o600


class ManagedDataBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.managed_root = self.root / "managed_data"
        self.profile = {
            "host": "deploy.example.internal",
            "port": 22,
            "username": "deployer",
            "password": "not-used-by-fake",
            "remote_root": "/home/csda/AI_Routing",
            "allow_upload": True,
        }
        self.remote = _ManagedRemote()
        self.history: list[dict[str, object]] = []
        self.heavy_csv = (
            "product_group_code,product_code,detailed_symptom_code\n"
            "tv,oled,s1\n"
        ).encode("utf-8")
        self.version = hashlib.sha256(self.heavy_csv).hexdigest()
        self.patches = (
            mock.patch.object(console_backend, "MANAGED_DATA_ROOT", self.managed_root),
            mock.patch.object(
                console_backend, "_load_remote_profile", return_value=self.profile
            ),
            mock.patch.object(
                console_backend, "_remote_session_factory", return_value=self.remote
            ),
            mock.patch.object(
                console_backend, "_append_history", side_effect=self.history.append
            ),
        )
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        with console_backend._MANAGED_DATA_DB_PREVIEW_LOCK:
            console_backend._MANAGED_DATA_DB_PREVIEWS.clear()
        with console_backend._MASTER_PREVIEW_CONFIRMATION_LOCK:
            console_backend._MASTER_PREVIEW_CONFIRMATIONS.clear()

    def _upload_heavy(self) -> dict[str, object]:
        return console_backend.upload_managed_data_file(
            scope="common",
            dataset_id="heavy_repair_rules",
            file_name="rules.csv",
            file_bytes=self.heavy_csv,
            expected_sha256=self.version,
            confirm=True,
            config_path="config/server_deploy.local.json",
        )

    def test_registry_list_and_preview_are_safe_and_do_not_write(self) -> None:
        listing = console_backend.list_managed_data_sets(scope="common")
        heavy = next(
            item for item in listing["datasets"] if item["dataset_id"] == "heavy_repair_rules"
        )
        preview = console_backend.preview_managed_data_upload(
            scope="common",
            dataset_id="heavy_repair_rules",
            file_name="customer_jane_doe_rules.csv",
            file_bytes=self.heavy_csv,
        )

        self.assertTrue(heavy["enabled"])
        self.assertTrue(heavy["db_sync_supported"])
        self.assertEqual(preview["status"], "ready")
        self.assertEqual(preview["sha256"], self.version)
        self.assertEqual(preview["version"], self.version)
        self.assertEqual(preview["file_type"], ".csv")
        self.assertFalse(self.managed_root.exists())
        self.assertEqual(self.remote.uploads, [])
        rendered = json.dumps(preview, sort_keys=True)
        self.assertNotIn("/home/", rendered)
        self.assertNotIn(str(self.managed_root), rendered)
        self.assertNotIn("customer_jane_doe", rendered)
        self.assertNotIn("filename", preview["summary"])
        self.assertNotIn("file_name", preview)

    def test_upload_is_immutable_scoped_and_idempotent(self) -> None:
        first = self._upload_heavy()
        second = self._upload_heavy()

        target = (
            "/home/csda/AI_Routing/shared/north_america/managed/"
            f"heavy_repair_rules/{self.version}/payload.csv"
        )
        version_root = (
            self.managed_root / "common" / "heavy_repair_rules" / self.version
        )
        self.assertEqual(first["status"], "uploaded")
        self.assertEqual(second["status"], "already_exists")
        self.assertEqual(self.remote.uploads, [target])
        self.assertEqual(self.remote.modes[target], 0o600)
        self.assertEqual((version_root / "payload.csv").read_bytes(), self.heavy_csv)
        self.assertEqual(
            {item.name for item in version_root.iterdir()},
            {"payload.csv", "metadata.json"},
        )
        self.assertEqual([row["status"] for row in self.history], ["uploaded", "already_exists"])
        self.assertTrue(all(row["remote_verified"] is True for row in self.history))
        self.assertNotIn("target", self.history[0])
        if os.name != "nt":
            self.assertEqual(stat.S_IMODE(version_root.stat().st_mode), 0o700)
            self.assertEqual(
                stat.S_IMODE((version_root / "payload.csv").stat().st_mode), 0o600
            )
            self.assertEqual(
                stat.S_IMODE((version_root / "metadata.json").stat().st_mode), 0o600
            )

        versions = console_backend.list_managed_data_versions(
            scope="common", dataset_id="heavy_repair_rules"
        )
        detail = console_backend.preview_managed_data_version(
            scope="common", dataset_id="heavy_repair_rules", version=self.version
        )
        self.assertEqual(len(versions["versions"]), 1)
        self.assertEqual(detail["version"], self.version)
        self.assertNotIn("path", json.dumps(detail).lower())

    def test_environment_scope_uses_separate_state_root(self) -> None:
        payload = (
            "STRATEGIC_CITY_NAME,GSFS_RECEIPT_NO,POSTAL_CODE,latitude,longitude\n"
            "Atlanta,R1,30301,33.7,-84.3\n"
        ).encode("utf-8")
        version = hashlib.sha256(payload).hexdigest()
        result = console_backend.upload_managed_data_file(
            scope="development",
            dataset_id="service_geocoded",
            file_name="services.csv",
            file_bytes=payload,
            expected_sha256=version,
            confirm=True,
            config_path="config/server_deploy.local.json",
        )

        self.assertEqual(result["scope"], "development")
        self.assertEqual(
            self.remote.uploads[-1],
            "/home/csda/AI_Routing/state/development/managed_data/"
            f"service_geocoded/{version}/payload.csv",
        )
        with self.assertRaisesRegex(ValueError, "registered for this scope"):
            console_backend.preview_managed_data_upload(
                scope="common",
                dataset_id="service_geocoded",
                file_name="services.csv",
                file_bytes=payload,
            )

    def test_confirmation_checksum_and_local_collision_fail_closed(self) -> None:
        with self.assertRaisesRegex(PermissionError, "explicit confirmation"):
            console_backend.upload_managed_data_file(
                scope="common",
                dataset_id="heavy_repair_rules",
                file_name="rules.csv",
                file_bytes=self.heavy_csv,
                expected_sha256=self.version,
                confirm=False,
                config_path="unused",
            )
        with self.assertRaisesRegex(ValueError, "checksum"):
            console_backend.upload_managed_data_file(
                scope="common",
                dataset_id="heavy_repair_rules",
                file_name="rules.csv",
                file_bytes=self.heavy_csv,
                expected_sha256="0" * 64,
                confirm=True,
                config_path="unused",
            )
        self.assertEqual(self.remote.uploads, [])

        self._upload_heavy()
        payload_path = (
            self.managed_root
            / "common"
            / "heavy_repair_rules"
            / self.version
            / "payload.csv"
        )
        payload_path.write_bytes(b"tampered")
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            self._upload_heavy()
        self.assertEqual(payload_path.read_bytes(), b"tampered")

    def test_db_sync_is_bound_to_common_heavy_version_and_development(self) -> None:
        self._upload_heavy()
        preview_id = str(uuid.uuid4())
        preview_digest = "a" * 64
        captured: dict[str, object] = {}

        def preview(**kwargs: object) -> dict[str, object]:
            captured.update(kwargs)
            return {
                "status": "ready",
                "preview_id": preview_id,
                "preview_digest": preview_digest,
                "create_count": 1,
                "update_count": 0,
                "unchanged_count": 0,
                "masked_samples": [],
                "table_name": "must-not-leak",
            }

        with mock.patch.object(
            console_backend, "preview_master_csv_upsert", side_effect=preview
        ):
            result = console_backend.preview_managed_data_db_sync(
                dataset_id="heavy_repair_rules",
                version=self.version,
                target_environment="development",
            )

        self.assertEqual(captured["table_id"], "common_heavy_repair_rule_master")
        self.assertEqual(captured["environment"], "development")
        self.assertTrue(bytes(captured["csv_bytes"]).startswith(b"\xef\xbb\xbf"))
        self.assertNotIn("table_name", result)
        self.assertEqual(result["dataset_id"], "heavy_repair_rules")
        self.assertEqual(result["version"], self.version)

        applied = {
            "status": "applied",
            "operation_id": str(uuid.uuid4()),
            "table_name": "must-not-leak",
        }
        with mock.patch.object(
            console_backend, "apply_master_csv_upsert", return_value=applied
        ) as apply:
            response = console_backend.apply_managed_data_db_sync(
                preview_id=preview_id,
                preview_digest=preview_digest,
                idempotency_key=str(uuid.uuid4()),
                target_environment="development",
                confirm=True,
            )
        self.assertEqual(response["status"], "applied")
        self.assertNotIn("table_name", response)
        self.assertEqual(apply.call_args.kwargs["environment"], "development")
        self.assertTrue(apply.call_args.kwargs["confirm"])

        with self.assertRaisesRegex(PermissionError, "does not match"):
            console_backend.apply_managed_data_db_sync(
                preview_id=preview_id,
                preview_digest="b" * 64,
                idempotency_key=str(uuid.uuid4()),
                target_environment="development",
                confirm=True,
            )
        with self.assertRaisesRegex(PermissionError, "Production"):
            console_backend.preview_managed_data_db_sync(
                dataset_id="heavy_repair_rules",
                version=self.version,
                target_environment="production",
            )
        with self.assertRaisesRegex(PermissionError, "Production"):
            console_backend.apply_managed_data_db_sync(
                preview_id=preview_id,
                preview_digest=preview_digest,
                idempotency_key=str(uuid.uuid4()),
                target_environment="production",
                confirm=True,
            )


if __name__ == "__main__":
    unittest.main()
