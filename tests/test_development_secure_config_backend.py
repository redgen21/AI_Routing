from __future__ import annotations

import contextlib
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.deploy import console_backend


def _sha256(value: bytes | Path) -> str:
    data = value.read_bytes() if isinstance(value, Path) else value
    return hashlib.sha256(data).hexdigest()


class _SecureConfigRemote:
    def __init__(self, *, fail_second_upload: bool = False) -> None:
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
        self.mkdirs_calls: list[tuple[str, int]] = []
        self.chmod_calls: list[tuple[str, int]] = []
        self.uploads: list[str] = []
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
        data = self.files.get(path)
        return _sha256(data) if data is not None else None

    def mode(self, path: str) -> int | None:
        return self.modes.get(path)

    def mkdirs(self, path: str, mode: int = 0o750) -> None:
        self.mkdirs_calls.append((path, mode))
        self.modes.setdefault(path, mode)

    def chmod(self, path: str, mode: int) -> None:
        self.chmod_calls.append((path, mode))
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
            raise RuntimeError("simulated second secure-config failure")


class DevelopmentSecureConfigBackendTests(unittest.TestCase):
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
            "allow_service_control": False,
        }
        self._write_valid_configs()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_valid_configs(self) -> None:
        (self.config_root / "common_vrp.dev.json").write_text(
            json.dumps(
                {
                    "environment": "development",
                    "api": {"port": 8066},
                    "database": {"dbname": "vrp_db_dev", "password": "TOP_SECRET"},
                    "storage": {"job_archive_root": "data/north_america/runtime/development"},
                }
            ),
            encoding="utf-8",
        )
        (self.config_root / "config.json").write_text(
            json.dumps(
                {
                    "geocoding": {"google_api_key": "TOP_SECRET"},
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

    @contextlib.contextmanager
    def _backend(self, remote: _SecureConfigRemote):
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", self.config_root),
            mock.patch.object(console_backend, "HISTORY_PATH", self.history_path),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
        ):
            yield

    def _preview(self, remote: _SecureConfigRemote) -> dict[str, object]:
        with self._backend(remote):
            return console_backend.preview_development_secure_config_upload(
                environment="development", config_path="config/server_deploy.local.json"
            )

    def test_preview_is_secret_free_and_uses_only_fixed_server_paths(self) -> None:
        remote = _SecureConfigRemote()
        preview = self._preview(remote)
        serialized = json.dumps(preview)
        self.assertNotIn("TOP_SECRET", serialized)
        self.assertEqual(
            set(preview),
            {
                "status",
                "upload_allowed",
                "mutation_required",
                "target_upload_path",
                "files",
                "fingerprint",
            },
        )
        rows = preview["files"]
        self.assertEqual(
            [row["target"] for row in rows],
            [target for _, target in console_backend.DEVELOPMENT_SECURE_CONFIG_TARGETS],
        )
        self.assertTrue(all(row["mode"] == "0600" and row["changed"] for row in rows))
        self.assertTrue(all(set(row) <= {
            "filename", "target", "local_sha256", "remote_sha256", "size_bytes", "mode", "status", "changed"
        } for row in rows))

    def test_policy_off_blocks_mutation_without_opening_remote_session(self) -> None:
        remote = _SecureConfigRemote()
        self.profile["allow_development_secure_config_upload"] = False
        with self._backend(remote):
            preview = console_backend.preview_development_secure_config_upload(
                environment="development", config_path="config/server_deploy.local.json"
            )
            self.assertFalse(preview["upload_allowed"])
            self.assertEqual(remote.mkdirs_calls, [])
            with self.assertRaisesRegex(PermissionError, "disabled"):
                console_backend.upload_development_secure_config(
                    environment="development",
                    config_path="config/server_deploy.local.json",
                    expected_fingerprint=str(preview["fingerprint"]),
                    dry_run=False,
                )

    def test_source_validation_rejects_wrong_development_contract_before_remote(self) -> None:
        common_path = self.config_root / "common_vrp.dev.json"
        broken = json.loads(common_path.read_text(encoding="utf-8"))
        broken["api"]["port"] = 9999
        common_path.write_text(json.dumps(broken), encoding="utf-8")
        remote = _SecureConfigRemote()
        with self._backend(remote), self.assertRaisesRegex(ValueError, "port must be 8066"):
            console_backend.preview_development_secure_config_upload(
                environment="development", config_path="config/server_deploy.local.json"
            )
        self.assertEqual(remote.mkdirs_calls, [])

    def test_upload_sets_private_modes_and_records_no_secret(self) -> None:
        remote = _SecureConfigRemote()
        common_target, general_target = [target for _, target in console_backend.DEVELOPMENT_SECURE_CONFIG_TARGETS]
        remote.files[common_target] = b"previous-common"
        remote.files[general_target] = b"previous-general"
        remote.modes[common_target] = 0o640
        remote.modes[general_target] = 0o600
        with self._backend(remote):
            preview = console_backend.preview_development_secure_config_upload(
                environment="development", config_path="config/server_deploy.local.json"
            )
            recorded: list[dict[str, object]] = []
            with mock.patch.object(console_backend, "_append_history", side_effect=recorded.append):
                receipt = console_backend.upload_development_secure_config(
                    environment="development",
                    config_path="config/server_deploy.local.json",
                    expected_fingerprint=str(preview["fingerprint"]),
                    dry_run=False,
                )
        self.assertEqual(receipt["status"], "uploaded")
        self.assertFalse(receipt["restart_performed"])
        self.assertTrue(receipt["restart_required"])
        self.assertEqual(remote.modes[common_target], 0o600)
        self.assertEqual(remote.modes[general_target], 0o600)
        self.assertTrue(any(mode == 0o700 for _, mode in remote.mkdirs_calls))
        backup_file_modes = [
            mode
            for path, mode in remote.chmod_calls
            if ".deployment_backups" in path
            and path.rsplit("/", 1)[-1] in {"common_vrp.dev.json", "config.json"}
        ]
        self.assertEqual(backup_file_modes, [0o600, 0o600])
        self.assertNotIn("TOP_SECRET", json.dumps(recorded))
        self.assertEqual(recorded[0]["kind"], "development-secure-config")

    def test_fingerprint_drift_prevents_mutation(self) -> None:
        remote = _SecureConfigRemote()
        preview = self._preview(remote)
        target = console_backend.DEVELOPMENT_SECURE_CONFIG_TARGETS[0][1]
        remote.files[target] = b"changed-after-preview"
        remote.modes[target] = 0o600
        with self._backend(remote), self.assertRaisesRegex(RuntimeError, "preview is stale"):
            console_backend.upload_development_secure_config(
                environment="development",
                config_path="config/server_deploy.local.json",
                expected_fingerprint=str(preview["fingerprint"]),
                dry_run=False,
            )
        self.assertEqual(remote.uploads, [])

    def test_identical_contents_and_permissions_refuse_mutation(self) -> None:
        remote = _SecureConfigRemote()
        with self._backend(remote):
            prepared = console_backend._prepare_development_secure_config_payloads()
            for item in prepared:
                remote.files[str(item["target"])] = bytes(item["payload"])
                remote.modes[str(item["target"])] = 0o600
            preview = console_backend.preview_development_secure_config_upload(
                environment="development", config_path="config/server_deploy.local.json"
            )
            self.assertFalse(preview["mutation_required"])
            with self.assertRaisesRegex(ValueError, "already unchanged"):
                console_backend.upload_development_secure_config(
                    environment="development",
                    config_path="config/server_deploy.local.json",
                    expected_fingerprint=str(preview["fingerprint"]),
                    dry_run=False,
                )
        self.assertEqual(remote.uploads, [])

    def test_second_file_failure_compensates_both_targets(self) -> None:
        remote = _SecureConfigRemote(fail_second_upload=True)
        targets = [target for _, target in console_backend.DEVELOPMENT_SECURE_CONFIG_TARGETS]
        originals = {targets[0]: b"old-common", targets[1]: b"old-general"}
        remote.files.update(originals)
        remote.modes.update({targets[0]: 0o640, targets[1]: 0o600})
        preview = self._preview(remote)
        with self._backend(remote), self.assertRaisesRegex(RuntimeError, "second secure-config"):
            console_backend.upload_development_secure_config(
                environment="development",
                config_path="config/server_deploy.local.json",
                expected_fingerprint=str(preview["fingerprint"]),
                dry_run=False,
            )
        self.assertEqual({target: remote.files[target] for target in targets}, originals)
        self.assertEqual(remote.modes[targets[0]], 0o640)
        self.assertEqual(remote.modes[targets[1]], 0o600)

    def test_production_is_structurally_rejected(self) -> None:
        remote = _SecureConfigRemote()
        with self._backend(remote), self.assertRaisesRegex(PermissionError, "development-only"):
            console_backend.preview_development_secure_config_upload(
                environment="production", config_path="config/server_deploy.local.json"
            )

    def test_resolver_accepts_legacy_naive_created_at_and_prefers_newest(self) -> None:
        deployment_root = self.root / "deployment"
        for version, created_at in (("old", "2026-07-20T00:14:05"), ("new", "2026-07-20T00:14:06+00:00")):
            stage = deployment_root / "development" / version
            source = stage / "smart_routing" / "api.py"
            source.parent.mkdir(parents=True)
            source.write_text(version, encoding="utf-8")
            (stage / "deploy_manifest.json").write_text(
                json.dumps(
                    {
                        "artifact_type": "server-runtime",
                        "target_environment": "development",
                        "target_root": "/home/csda/AI_Routing/development",
                        "source_dirty": True,
                        "source_mode": "worktree",
                        "promotable": False,
                        "created_at": created_at,
                        "files": [{"path": "smart_routing/api.py", "sha256": _sha256(source)}],
                    }
                ),
                encoding="utf-8",
            )
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", deployment_root):
            result = console_backend.resolve_latest_runtime_artifact(environment="development")
        self.assertIsNotNone(result)
        self.assertEqual(result.version, "new")

    def test_manifest_timestamp_parser_accepts_powershell_ticks_and_rejects_malformed_values(self) -> None:
        self.assertEqual(
            console_backend._manifest_created_at(
                {"created_at": "2026-07-20T01:33:35.5206073Z"}
            ).isoformat(),
            "2026-07-20T01:33:35.520607+00:00",
        )
        self.assertEqual(
            console_backend._manifest_created_at(
                {"created_at": "2026-07-20T10:33:35.5+09:00"}
            ).isoformat(),
            "2026-07-20T01:33:35.500000+00:00",
        )
        self.assertEqual(
            console_backend._manifest_created_at(
                {"created_at": "2026-07-20T01:33:35"}
            ).isoformat(),
            "2026-07-20T01:33:35+00:00",
        )
        for value in (
            "2026-07-20 01:33:35Z",
            "2026-07-20T01:33:35.12345678Z",
            "2026-07-20T01:33:35+24:00",
            "2026-07-20T01:33:35Z trailing",
        ):
            with self.assertRaisesRegex(ValueError, "created_at is invalid"):
                console_backend._manifest_created_at({"created_at": value})

    def test_future_build_manifest_uses_timezone_aware_utc(self) -> None:
        source = console_backend._BUILD_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("yyyy-MM-ddTHH:mm:ss.ffffff'Z'", source)


if __name__ == "__main__":
    unittest.main()
