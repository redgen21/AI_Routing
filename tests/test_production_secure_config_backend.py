from __future__ import annotations

import contextlib
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.deploy import console_backend


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class _SecureConfigRemote:
    def __init__(self, *, fail_second_upload: bool = False) -> None:
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
        self.uploads: list[str] = []
        self.remote_reads = 0
        self.fail_second_upload = fail_second_upload

    def __enter__(self) -> "_SecureConfigRemote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    @contextlib.contextmanager
    def deployment_lock(self, _base: str, _deployment_id: str):
        yield

    def exists(self, path: str) -> bool:
        return path in self.files or path in self.modes

    def sha256(self, path: str) -> str | None:
        self.remote_reads += 1
        return _sha256(self.files[path]) if path in self.files else None

    def read_bytes(self, path: str, *, maximum_bytes: int) -> bytes:
        self.remote_reads += 1
        if path not in self.files:
            raise FileNotFoundError("remote control file is missing")
        payload = self.files[path]
        if len(payload) > maximum_bytes:
            raise ValueError("remote payload too large")
        return payload

    def mode(self, path: str) -> int | None:
        return self.modes.get(path)

    def mkdirs(self, path: str, mode: int = 0o750) -> None:
        self.modes.setdefault(path, mode)

    def chmod(self, path: str, mode: int) -> None:
        self.modes[path] = mode

    def copy(self, source: str, target: str) -> None:
        self.files[target] = self.files[source]
        self.modes[target] = self.modes.get(source, 0o600)

    def remove(self, path: str) -> None:
        self.files.pop(path, None)
        self.modes.pop(path, None)

    def upload_bytes_atomic(self, payload: bytes, target: str, backup: str | None) -> None:
        if backup and target in self.files:
            self.copy(target, backup)
            self.chmod(backup, 0o600)
        self.files[target] = payload
        self.modes[target] = 0o600
        self.uploads.append(target)
        if self.fail_second_upload and len(self.uploads) == 2:
            raise RuntimeError("simulated production second upload failure")


class ProductionSecureConfigBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config_root = self.root / "config"
        self.config_root.mkdir()
        self.history_path = self.root / "history.json"
        self.profile = {
            "host": "example.internal",
            "port": 22,
            "username": "deployer",
            "remote_root": "/home/csda/AI_Routing",
            "allow_upload": True,
            "allow_development_secure_config_upload": True,
            "allow_production_secure_config_upload": True,
            "allow_service_control": False,
        }
        self._write_valid_configs()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_valid_configs(self) -> None:
        (self.config_root / "common_vrp.prod.json").write_text(
            json.dumps(
                {
                    "environment": "production",
                    "api": {"port": 8065},
                    "routing_api_url": "http://127.0.0.1:8065",
                    "database": {
                        "host": "localhost",
                        "port": 5432,
                        "dbname": "vrp_db",
                        "user": "vrp_agent",
                        "password": "PRODUCTION_SECRET",
                    },
                    "storage": {"job_archive_root": "data/north_america/runtime/production"},
                }
            ),
            encoding="utf-8",
        )

        (self.config_root / "config.json").write_text(
            json.dumps(
                {
                    "geocoding": {"google_api_key": "PRODUCTION_SECRET"},
                    "routing": {
                        "distance_backend": "city_osrm_else_haversine",
                        "assignment_distance_backend": "haversine",
                        "osrm_url": "http://127.0.0.1:5000",
                        "osrm_profile": "driving",
                        "city_osrm_urls": {"Atlanta, GA": "http://127.0.0.1:5002"},
                    },
                    "area_map_usa": {
                        "service_file": "data/north_america/processed/service/service.csv",
                        "profile_file": "data/north_america/processed/profile/profile.xlsx",
                        "zcta_zip_file": "data/north_america/reference/geospatial/zcta.zip",
                    },
                    "area_map": {
                        "service_file": "data/north_america/processed/service/service.csv",
                        "profile_file": "data/north_america/processed/profile/profile.xlsx",
                    },
                }
            ),
            encoding="utf-8",
        )
        (self.config_root / "data_catalog.json").write_text(
            json.dumps(
                {
                    "data_root": "data/north_america",
                    "active": {
                        "service_geocoded": "processed/service/service.csv",
                        "profile_production": "processed/profile/profile.xlsx",
                        "zcta_geometry": "reference/geospatial/zcta.zip",
                    },
                }
            ),
            encoding="utf-8",
        )

    def _server_catalog_payload(self) -> bytes:
        return json.dumps(
            {
                "schema": "north-america-routing-data-catalog/v1",
                "data_root": "/home/csda/AI_Routing/shared/north_america",
                "state_root": "/home/csda/AI_Routing/state/production",
                "active": {
                    "service_geocoded": "processed/service/service.csv",
                    "profile_production": "processed/profile/profile.xlsx",
                    "zcta_geometry": "reference/geospatial/zcta.zip",
                },
            }
        ).encode("utf-8")

    @contextlib.contextmanager
    def _backend(self, remote: _SecureConfigRemote):
        remote.files.setdefault(
            console_backend.PRODUCTION_REMOTE_DATA_CATALOG,
            self._server_catalog_payload(),
        )
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", self.config_root),
            mock.patch.object(console_backend, "HISTORY_PATH", self.history_path),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
        ):
            yield

    def _preview(self, remote: _SecureConfigRemote) -> dict[str, object]:
        with self._backend(remote):
            return console_backend.preview_production_secure_config_upload(
                environment="production", config_path="config/server_deploy.local.json"
            )

    def test_preview_is_redacted_and_rewrites_only_fixed_production_targets(self) -> None:
        remote = _SecureConfigRemote()
        preview = self._preview(remote)
        self.assertNotIn("PRODUCTION_SECRET", json.dumps(preview))
        self.assertEqual(
            [row["target"] for row in preview["files"]],
            [target for _, target in console_backend.PRODUCTION_SECURE_CONFIG_TARGETS],
        )
        self.assertEqual(
            preview["target_upload_path"], "/home/csda/AI_Routing/production/config"
        )
        with self._backend(remote):
            prepared = console_backend._prepare_production_secure_config_payloads()
        contents = {item["filename"]: json.loads(item["payload"]) for item in prepared}
        self.assertEqual(
            contents["common_vrp.prod.json"]["storage"]["job_archive_root"],
            "/home/csda/AI_Routing/state/production/common_vrp_jobs",
        )
        self.assertEqual(
            contents["config.json"]["area_map"]["service_file"],
            "/home/csda/AI_Routing/shared/north_america/processed/service/service.csv",
        )
        self.assertEqual(
            contents["config.json"]["area_map_usa"]["zcta_zip_file"],
            "/home/csda/AI_Routing/shared/north_america/reference/geospatial/zcta.zip",
        )

    def test_policy_off_never_opens_a_remote_session(self) -> None:
        remote = _SecureConfigRemote()
        self.profile["allow_production_secure_config_upload"] = False
        with self._backend(remote):
            preview = console_backend.preview_production_secure_config_upload(
                environment="production", config_path="config/server_deploy.local.json"
            )
            self.assertFalse(preview["upload_allowed"])
            self.assertEqual(remote.remote_reads, 0)
            with self.assertRaisesRegex(PermissionError, "Production secure-config upload is disabled"):
                console_backend.upload_production_secure_config(
                    environment="production",
                    config_path="config/server_deploy.local.json",
                    expected_fingerprint=str(preview["fingerprint"]),
                    dry_run=False,
                )

    def test_rejects_wrong_routing_port_before_remote_access(self) -> None:
        common_path = self.config_root / "common_vrp.prod.json"
        common = json.loads(common_path.read_text(encoding="utf-8"))
        common["routing_api_url"] = "http://127.0.0.1:8066"
        common_path.write_text(json.dumps(common), encoding="utf-8")
        remote = _SecureConfigRemote()
        with self._backend(remote), self.assertRaisesRegex(ValueError, "port must match API port 8065"):
            console_backend.preview_production_secure_config_upload(
                environment="production", config_path="config/server_deploy.local.json"
            )
        self.assertEqual(remote.remote_reads, 0)

    def test_remote_catalog_missing_or_mismatched_blocks_before_config_upload(self) -> None:
        remote = _SecureConfigRemote()
        with self._backend(remote):
            remote.files.pop(console_backend.PRODUCTION_REMOTE_DATA_CATALOG)
            with self.assertRaisesRegex(FileNotFoundError, "control file is missing"):
                console_backend.preview_production_secure_config_upload(
                    environment="production", config_path="config/server_deploy.local.json"
                )
        self.assertEqual(remote.uploads, [])

        remote = _SecureConfigRemote()
        mismatched_catalog = json.loads(self._server_catalog_payload())
        mismatched_catalog["active"]["service_geocoded"] = "processed/service/other.csv"
        with self._backend(remote):
            remote.files[console_backend.PRODUCTION_REMOTE_DATA_CATALOG] = json.dumps(
                mismatched_catalog
            ).encode("utf-8")
            with self.assertRaisesRegex(ValueError, "must match the active data catalog") as raised:
                console_backend.preview_production_secure_config_upload(
                    environment="production", config_path="config/server_deploy.local.json"
                )
        self.assertNotIn("other.csv", str(raised.exception))
        self.assertEqual(remote.uploads, [])

    def test_upload_uses_0600_records_redacted_history_and_detects_stale_preview(self) -> None:
        remote = _SecureConfigRemote()
        targets = [target for _, target in console_backend.PRODUCTION_SECURE_CONFIG_TARGETS]
        remote.files.update({targets[0]: b"old-common", targets[1]: b"old-general"})
        remote.modes.update({targets[0]: 0o640, targets[1]: 0o600})
        with self._backend(remote):
            preview = console_backend.preview_production_secure_config_upload(
                environment="production", config_path="config/server_deploy.local.json"
            )
            recorded: list[dict[str, object]] = []
            with mock.patch.object(console_backend, "_append_history", side_effect=recorded.append):
                receipt = console_backend.upload_production_secure_config(
                    environment="production",
                    config_path="config/server_deploy.local.json",
                    expected_fingerprint=str(preview["fingerprint"]),
                    dry_run=False,
                )
        self.assertEqual(receipt["kind"], "production-secure-config")
        self.assertEqual({remote.modes[target] for target in targets}, {0o600})
        self.assertEqual(recorded[0]["kind"], "production-secure-config")
        self.assertEqual(
            recorded[0]["production_catalog_sha256"],
            _sha256(self._server_catalog_payload()),
        )
        self.assertNotIn("PRODUCTION_SECRET", json.dumps(recorded))

        stale_preview = self._preview(remote)
        remote.files[targets[0]] = b"changed-after-preview"
        with self._backend(remote), self.assertRaisesRegex(RuntimeError, "preview is stale"):
            console_backend.upload_production_secure_config(
                environment="production",
                config_path="config/server_deploy.local.json",
                expected_fingerprint=str(stale_preview["fingerprint"]),
                dry_run=False,
            )

    def test_remote_catalog_drift_invalidates_preview_without_upload(self) -> None:
        remote = _SecureConfigRemote()
        preview = self._preview(remote)
        catalog = json.loads(self._server_catalog_payload())
        # A valid catalog metadata/version change preserves the active paths but
        # must still force a fresh operator review.
        catalog["version"] = "server-data-v2"
        remote.files[console_backend.PRODUCTION_REMOTE_DATA_CATALOG] = json.dumps(
            catalog, separators=(",", ":")
        ).encode("utf-8")
        with self._backend(remote), self.assertRaisesRegex(RuntimeError, "preview is stale"):
            console_backend.upload_production_secure_config(
                environment="production",
                config_path="config/server_deploy.local.json",
                expected_fingerprint=str(preview["fingerprint"]),
                dry_run=False,
            )
        self.assertEqual(remote.uploads, [])

    def test_second_upload_failure_compensates_production_targets(self) -> None:
        remote = _SecureConfigRemote(fail_second_upload=True)
        targets = [target for _, target in console_backend.PRODUCTION_SECURE_CONFIG_TARGETS]
        originals = {targets[0]: b"old-common", targets[1]: b"old-general"}
        remote.files.update(originals)
        remote.modes.update({targets[0]: 0o640, targets[1]: 0o600})
        preview = self._preview(remote)
        with self._backend(remote), self.assertRaisesRegex(RuntimeError, "production second upload failure"):
            console_backend.upload_production_secure_config(
                environment="production",
                config_path="config/server_deploy.local.json",
                expected_fingerprint=str(preview["fingerprint"]),
                dry_run=False,
            )
        self.assertEqual({target: remote.files[target] for target in targets}, originals)
        self.assertEqual(remote.modes[targets[0]], 0o640)


if __name__ == "__main__":
    unittest.main()
