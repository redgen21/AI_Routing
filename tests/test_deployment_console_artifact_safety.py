from __future__ import annotations

import contextlib
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.deploy import console_backend


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _UploadRemote:
    def __init__(self) -> None:
        self.checksums: dict[str, str] = {}
        self.uploads: list[str] = []
        self.lock_calls = 0

    def __enter__(self) -> "_UploadRemote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    @contextlib.contextmanager
    def deployment_lock(self, _base: str, _deployment_id: str):
        self.lock_calls += 1
        yield

    def exists(self, target: str) -> bool:
        return target in self.checksums

    def upload_atomic(self, local: Path, target: str, backup: str | None) -> None:
        if backup and target in self.checksums:
            self.checksums[backup] = self.checksums[target]
        self.uploads.append(target)
        self.checksums[target] = _sha256(local)

    def sha256(self, target: str) -> str | None:
        return self.checksums.get(target)

    def inventory_files(self, directory: str) -> list[str]:
        prefix = directory.rstrip("/") + "/"
        return sorted(
            target[len(prefix) :]
            for target in self.checksums
            if target.startswith(prefix)
        )

    def size(self, target: str) -> int | None:
        return 123 if target in self.checksums else None

    def copy(self, source: str, target: str) -> None:
        self.checksums[target] = self.checksums[source]

    def remove(self, target: str) -> None:
        self.checksums.pop(target, None)


class _FailingUploadRemote(_UploadRemote):
    def upload_atomic(self, local: Path, target: str, backup: str | None) -> None:
        super().upload_atomic(local, target, backup)
        if len(self.uploads) == 2:
            raise RuntimeError("simulated second-file failure")


class _PartialBackupFailureRemote(_UploadRemote):
    def upload_atomic(self, local: Path, target: str, backup: str | None) -> None:
        self.uploads.append(target)
        if backup:
            self.checksums[backup] = "PARTIAL-BACKUP"
        raise RuntimeError("simulated partial backup failure")


class _UnselectedDriftRemote(_UploadRemote):
    def __init__(self, drift_target: str) -> None:
        super().__init__()
        self.drift_target = drift_target

    def upload_atomic(self, local: Path, target: str, backup: str | None) -> None:
        super().upload_atomic(local, target, backup)
        self.checksums[self.drift_target] = "d" * 64


class DeploymentConsoleArtifactSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.stage = self.root / "development" / "v1"
        self.stage.mkdir(parents=True)
        self.source = self.stage / "smart_routing" / "api.py"
        self.source.parent.mkdir()
        self.source.write_text("print('safe')\n", encoding="utf-8")
        self.manifest = {
            "files": [{"path": "smart_routing/api.py", "sha256": _sha256(self.source)}],
            "artifact_type": "server-runtime",
            "target_environment": "development",
            "target_root": "/home/csda/AI_Routing/development",
            "source_dirty": True,
            "source_mode": "worktree",
            "promotable": False,
        }
        self.inspection = console_backend.ArtifactInspection(
            path=str(self.stage),
            kind="runtime",
            environment="development",
            version="v1",
            manifest=self.manifest,
            archive_sha256="a" * 64,
            target_upload_path="/home/csda/AI_Routing/development",
            required_confirmation="DEPLOY development v1",
            restricted_data=False,
        )
        self.profile = {
            "host": "server.example.internal",
            "port": 22,
            "username": "deployer",
            "remote_root": "/home/csda/AI_Routing",
            "allow_upload": True,
            "allow_service_control": False,
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _admin_tools_inspection(
        self, *, file_count: int = 2, clean: bool = False
    ) -> console_backend.ArtifactInspection:
        stage = self.root / "admin_tools" / "admin-v1"
        files: list[dict[str, str]] = []
        for index in range(file_count):
            relative = f"admin_tools/tool_{index:03d}.py"
            source = stage / relative
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text(f"VALUE = {index}\n", encoding="utf-8")
            files.append({"path": relative, "sha256": _sha256(source)})
        manifest = {
            "artifact_type": "db-admin-tools",
            "target_root": "/home/csda/AI_Routing/admin_tools/releases/admin-v1",
            "source_dirty": not clean,
            "promotable": clean,
            "contains_secrets": False,
            "contains_data": False,
            # The release manifest is deliberately absent: it cannot hash itself.
            "files": files,
        }
        (stage / "deploy_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root):
            return console_backend.inspect_artifact(
                path=str(stage), kind="admin-tools", environment="development"
            )

    def _admin_pin_config(self) -> Path:
        config = self.root / "config"
        config.mkdir(exist_ok=True)
        (config / "server_deploy.local.json").write_text(
            json.dumps(
                {
                    "credentials_file": "config/server_ftp.local.json",
                    "remote_root": "/home/csda/AI_Routing",
                    "allow_upload": True,
                    "preserved_policy_extension": {"keep": True},
                }
            ),
            encoding="utf-8",
        )
        # The pin operation must not read or rewrite credentials.
        (config / "server_ftp.local.json").write_text(
            json.dumps({"password": "x" * 23}), encoding="utf-8"
        )
        return config

    def test_selection_outside_manifest_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "not in the manifest"):
            console_backend._selected(self.inspection, ["smart_routing/other.py"])

    def test_remote_diff_returns_sorted_full_paths_checksums_status_and_sizes(self) -> None:
        remote = _UploadRemote()
        target = "/home/csda/AI_Routing/development/smart_routing/api.py"
        remote.checksums[target] = _sha256(self.source)
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
        ):
            rows = console_backend.preview_remote_diff(
                inspection=self.inspection,
                selected_files=["smart_routing/api.py"],
                config_path="config/server_deploy.local.json",
            )
        self.assertEqual(rows[0]["local_path"], str(self.source.resolve()))
        self.assertEqual(rows[0]["remote_path"], target)
        self.assertEqual(rows[0]["status"], "unchanged")
        self.assertEqual(rows[0]["local_sha256"], _sha256(self.source))
        self.assertEqual(rows[0]["remote_sha256"], _sha256(self.source))
        self.assertEqual(rows[0]["local_size_bytes"], self.source.stat().st_size)
        self.assertEqual(rows[0]["remote_size_bytes"], 123)
        self.assertEqual(remote.lock_calls, 0)

    def test_deployment_policy_exposes_flags_and_target_id_only(self) -> None:
        with mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile):
            policy = console_backend.deployment_policy(
                environment="development",
                config_path="config/server_deploy.local.json",
            )
        self.assertEqual(
            set(policy), {"allow_upload", "allow_service_control", "target_id"}
        )
        self.assertTrue(policy["allow_upload"])
        self.assertEqual(len(policy["target_id"]), 64)

    def test_local_checksum_change_is_rejected(self) -> None:
        self.source.write_text("changed\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "changed after inspection"):
            console_backend._selected(self.inspection, ["smart_routing/api.py"])

    def test_remote_target_escape_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsafe manifest path"):
            console_backend._remote_target(self.inspection, "../outside.py")

    def test_artifact_path_outside_environment_root_is_rejected(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root):
            with self.assertRaisesRegex(ValueError, "escapes allowed root"):
                console_backend.inspect_artifact(
                    path=str(outside), kind="runtime", environment="development"
                )

    def test_dry_run_never_opens_remote_session(self) -> None:
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(
                console_backend,
                "_remote_session_factory",
                side_effect=AssertionError("remote session must not open"),
            ),
        ):
            result = console_backend.upload_artifact(
                inspection=self.inspection,
                selected_files=["smart_routing/api.py"],
                config_path="config/server_deploy.local.json",
                typed_confirmation="DEPLOY development v1",
                dry_run=True,
            )
        self.assertEqual(result["status"], "dry_run")

    def test_upload_policy_blocks_before_remote_session(self) -> None:
        self.profile["allow_upload"] = False
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(
                console_backend,
                "_remote_session_factory",
                side_effect=AssertionError("remote session must not open"),
            ),
        ):
            with self.assertRaisesRegex(PermissionError, "disabled"):
                console_backend.upload_artifact(
                    inspection=self.inspection,
                    selected_files=["smart_routing/api.py"],
                    config_path="config/server_deploy.local.json",
                    typed_confirmation="DEPLOY development v1",
                    dry_run=False,
                )

    def test_restricted_artifact_is_always_blocked(self) -> None:
        restricted = console_backend.ArtifactInspection(
            **{**self.inspection.__dict__, "restricted_data": True}
        )
        with self.assertRaisesRegex(ValueError, "forbidden"):
            console_backend.upload_artifact(
                inspection=restricted,
                selected_files=["smart_routing/api.py"],
                config_path="config/server_deploy.local.json",
                typed_confirmation="DEPLOY development v1",
                dry_run=True,
            )
        with mock.patch.object(
            console_backend,
            "_remote_session_factory",
            side_effect=AssertionError("restricted diff must not open a remote session"),
        ):
            with self.assertRaisesRegex(ValueError, "forbidden"):
                console_backend.preview_remote_diff(
                    inspection=restricted,
                    selected_files=["smart_routing/api.py"],
                    config_path="config/server_deploy.local.json",
                )

    def test_production_dirty_artifact_is_rejected_during_inspection(self) -> None:
        stage = self.root / "production" / "v2"
        source = stage / "smart_routing" / "api.py"
        source.parent.mkdir(parents=True)
        source.write_text("production\n", encoding="utf-8")
        manifest = {
            "artifact_type": "server-runtime",
            "target_environment": "production",
            "target_root": "/home/csda/AI_Routing/production",
            "source_dirty": True,
            "promotable": False,
            "files": [{"path": "smart_routing/api.py", "sha256": _sha256(source)}],
        }
        (stage / "deploy_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root):
            with self.assertRaisesRegex(ValueError, "clean promotable"):
                console_backend.inspect_artifact(
                    path=str(stage), kind="runtime", environment="production"
                )

    def test_production_clean_artifact_requires_immutable_source_mode(self) -> None:
        stage = self.root / "production" / "v3"
        source = stage / "smart_routing" / "api.py"
        source.parent.mkdir(parents=True)
        source.write_text("production\n", encoding="utf-8")
        manifest = {
            "artifact_type": "server-runtime",
            "target_environment": "production",
            "target_root": "/home/csda/AI_Routing/production",
            "source_dirty": False,
            "source_mode": "worktree",
            "promotable": True,
            "files": [{"path": "smart_routing/api.py", "sha256": _sha256(source)}],
        }
        (stage / "deploy_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root):
            with self.assertRaisesRegex(ValueError, "source_mode"):
                console_backend.inspect_artifact(
                    path=str(stage), kind="runtime", environment="production"
                )

            manifest["source_mode"] = "immutable-git-archive"
            (stage / "deploy_manifest.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )
            inspected = console_backend.inspect_artifact(
                path=str(stage), kind="runtime", environment="production"
            )
        self.assertEqual(inspected.version, "v3")

    def test_local_secret_file_is_marked_restricted(self) -> None:
        secret = self.stage / "config" / "server_ftp.local.json"
        secret.parent.mkdir()
        secret.write_text("{}", encoding="utf-8")
        self.manifest["files"].append(
            {"path": "config/server_ftp.local.json", "sha256": _sha256(secret)}
        )
        (self.stage / "deploy_manifest.json").write_text(
            json.dumps(self.manifest), encoding="utf-8"
        )
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root):
            inspected = console_backend.inspect_artifact(
                path=str(self.stage), kind="runtime", environment="development"
            )
        self.assertTrue(inspected.restricted_data)

    def test_development_artifact_must_be_explicitly_non_promotable(self) -> None:
        self.manifest["promotable"] = True
        (self.stage / "deploy_manifest.json").write_text(
            json.dumps(self.manifest), encoding="utf-8"
        )
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root):
            with self.assertRaisesRegex(ValueError, "non-promotable"):
                console_backend.inspect_artifact(
                    path=str(self.stage), kind="runtime", environment="development"
                )

    def test_admin_manifest_only_retry_is_validated_and_verified(self) -> None:
        inspection = self._admin_tools_inspection()
        config = self._admin_pin_config()
        release_files = console_backend._release_file_map(inspection)
        self.assertNotIn("deploy_manifest.json", console_backend._manifest_files(inspection.manifest))
        remote = _UploadRemote()
        for relative, (checksum, _) in release_files.items():
            if relative != "deploy_manifest.json":
                remote.checksums[console_backend._remote_target(inspection, relative)] = checksum
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", config),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            result = console_backend.upload_artifact(
                inspection=inspection,
                selected_files=["deploy_manifest.json", "deploy_manifest.json"],
                config_path="config/server_deploy.local.json",
                typed_confirmation=inspection.required_confirmation,
                dry_run=False,
            )
        self.assertFalse(result["selected_full_manifest"])
        self.assertTrue(result["remote_manifest_verified"])
        self.assertEqual(
            remote.uploads,
            [
                "/home/csda/AI_Routing/admin_tools/releases/admin-v1/"
                "deploy_manifest.json"
            ],
        )
        self.assertEqual([change["path"] for change in recorded[0]["changes"]], ["deploy_manifest.json"])
        self.assertEqual(
            {row["path"] for row in recorded[0]["verified_files"]}, set(release_files)
        )

    def test_admin_full_upload_includes_manifest_once_in_53_path_receipt(self) -> None:
        inspection = self._admin_tools_inspection(file_count=52)
        config = self._admin_pin_config()
        manifest_files = console_backend._manifest_files(inspection.manifest)
        release_files = console_backend._release_file_map(inspection)
        self.assertEqual(len(manifest_files), 52)
        self.assertNotIn("deploy_manifest.json", manifest_files)
        self.assertEqual(len(release_files), 53)
        remote = _UploadRemote()
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", config),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            result = console_backend.upload_artifact(
                inspection=inspection,
                selected_files=sorted(manifest_files),
                config_path="config/server_deploy.local.json",
                typed_confirmation=inspection.required_confirmation,
                dry_run=False,
            )
        self.assertTrue(result["selected_full_manifest"])
        self.assertTrue(result["complete_manifest"])
        self.assertEqual(len(remote.uploads), 53)
        self.assertEqual(remote.uploads.count(
            "/home/csda/AI_Routing/admin_tools/releases/admin-v1/deploy_manifest.json"
        ), 1)
        self.assertEqual(len(recorded[0]["changes"]), 53)
        self.assertEqual(len(recorded[0]["verified_files"]), 53)
        self.assertEqual(
            result["admin_tools_pin"],
            {
                "status": "pinned_development_verification",
                "version": "admin-v1",
            },
        )
        self.assertEqual(recorded[0]["admin_tools_pin"], result["admin_tools_pin"])
        self.assertEqual(
            {row["path"] for row in recorded[0]["verified_files"]}, set(release_files)
        )
        saved = json.loads((config / "server_deploy.local.json").read_text(encoding="utf-8"))
        self.assertEqual(saved["admin_tools_development_release_version"], "admin-v1")
        self.assertNotIn("admin_tools_release_version", saved)

    def test_non_admin_manifest_self_entry_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "only for admin-tools"):
            console_backend._selected_release_files(
                self.inspection, ["deploy_manifest.json"]
            )

    def test_legacy_admin_artifact_with_smart_routing_paths_is_not_selectable(self) -> None:
        stage = self.root / "admin_tools" / "legacy-admin"
        legacy_source = stage / "smart_routing" / "common_vrp_db.py"
        legacy_source.parent.mkdir(parents=True)
        legacy_source.write_text("LEGACY = True\n", encoding="utf-8")
        manifest = {
            "artifact_type": "db-admin-tools",
            "target_root": "/home/csda/AI_Routing/admin_tools/releases/legacy-admin",
            "files": [{"path": "smart_routing/common_vrp_db.py", "sha256": _sha256(legacy_source)}],
        }
        (stage / "deploy_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        with mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root):
            self.assertEqual(
                console_backend.list_artifacts(
                    environment="development", kind="admin-tools"
                ),
                [],
            )
            with self.assertRaisesRegex(ValueError, "must not contain smart_routing"):
                console_backend.inspect_artifact(
                    path=str(stage), kind="admin-tools", environment="development"
                )

    def test_verified_clean_admin_upload_pins_shared_profile_without_credentials(self) -> None:
        inspection = self._admin_tools_inspection(clean=True)
        config = self._admin_pin_config()
        credential_path = config / "server_ftp.local.json"
        credential_before = credential_path.read_bytes()
        remote = _UploadRemote()
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", config),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            result = console_backend.upload_artifact(
                inspection=inspection,
                selected_files=sorted(console_backend._manifest_files(inspection.manifest)),
                config_path="config/server_deploy.local.json",
                typed_confirmation=inspection.required_confirmation,
                dry_run=False,
            )
        profile = json.loads((config / "server_deploy.local.json").read_text(encoding="utf-8"))
        self.assertEqual(result["status"], "uploaded")
        self.assertEqual(
            result["admin_tools_pin"],
            {"status": "pinned_common_and_development", "version": "admin-v1"},
        )
        self.assertEqual(recorded[0]["status"], "uploaded")
        self.assertEqual(recorded[0]["admin_tools_pin"], result["admin_tools_pin"])
        self.assertTrue(recorded[0]["complete_manifest"])
        self.assertTrue(recorded[0]["remote_manifest_verified"])
        self.assertEqual(profile["admin_tools_release_version"], "admin-v1")
        self.assertEqual(
            profile["admin_tools_development_release_version"], "admin-v1"
        )
        self.assertEqual(profile["preserved_policy_extension"], {"keep": True})
        self.assertEqual(credential_path.read_bytes(), credential_before)
        self.assertEqual(
            console_backend._master_admin_context(profile, "production")["release_version"],
            "admin-v1",
        )

    def test_local_pin_failure_does_not_reclassify_verified_remote_upload(self) -> None:
        inspection = self._admin_tools_inspection(clean=True)
        config = self._admin_pin_config()
        profile_path = config / "server_deploy.local.json"
        profile_before = profile_path.read_bytes()
        remote = _UploadRemote()
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", config),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
            mock.patch.object(
                console_backend,
                "_commit_connection_settings_transaction",
                side_effect=OSError("simulated local profile failure"),
            ),
        ):
            result = console_backend.upload_artifact(
                inspection=inspection,
                selected_files=sorted(console_backend._manifest_files(inspection.manifest)),
                config_path="config/server_deploy.local.json",
                typed_confirmation=inspection.required_confirmation,
                dry_run=False,
            )
        self.assertEqual(result["status"], "uploaded")
        self.assertEqual(
            result["admin_tools_pin"],
            {"status": "pin_failed", "error_code": "local_profile_write_failed"},
        )
        self.assertEqual(recorded[0]["status"], "uploaded")
        self.assertEqual(recorded[0]["admin_tools_pin"], result["admin_tools_pin"])
        self.assertTrue(recorded[0]["complete_manifest"])
        self.assertTrue(recorded[0]["remote_manifest_verified"])
        self.assertEqual(profile_path.read_bytes(), profile_before)

    def test_dirty_admin_tools_activation_is_development_only_and_rechecks_remote(self) -> None:
        inspection = self._admin_tools_inspection()
        config = self._admin_pin_config()
        profile = {
            **self.profile,
            "password": "not-used-by-fake",
            "admin_tools_release_version": "",
            "admin_tools_development_release_version": "",
        }
        release_files = console_backend._release_file_map(inspection)
        remote = _UploadRemote()
        verified_files = []
        for relative, (checksum, _) in release_files.items():
            target = console_backend._remote_target(inspection, relative)
            remote.checksums[target] = checksum
            verified_files.append(
                {"path": relative, "target": target, "sha256": checksum}
            )
        receipt = {
            "kind": "admin-tools",
            "environment": "development",
            "version": inspection.version,
            "status": "uploaded",
            "target_id": console_backend._target_id(profile, "development"),
            "complete_manifest": True,
            "remote_manifest_verified": True,
            "sha256": inspection.archive_sha256,
            "verified_files": verified_files,
        }
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", config),
            mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root),
            mock.patch.object(console_backend, "_master_admin_profile", return_value=profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_load_history", return_value=[receipt]),
        ):
            preview = console_backend.preview_admin_tools_development_activation(
                version=inspection.version
            )
            self.assertEqual(
                preview,
                {
                    "status": "ready",
                    "version": "admin-v1",
                    "eligible": True,
                    "mode": "development-verification",
                },
            )
            with self.assertRaisesRegex(PermissionError, "explicit confirmation"):
                console_backend.activate_admin_tools_development_release(
                    version=inspection.version
                )
            result = console_backend.activate_admin_tools_development_release(
                version=inspection.version, confirm=True
            )

        saved = json.loads((config / "server_deploy.local.json").read_text(encoding="utf-8"))
        self.assertEqual(
            result,
            {
                "status": "activated",
                "version": "admin-v1",
                "mode": "development-verification",
            },
        )
        self.assertEqual(saved["admin_tools_development_release_version"], "admin-v1")
        self.assertNotIn("admin_tools_release_version", saved)
        self.assertEqual(saved["preserved_policy_extension"], {"keep": True})

        remote.checksums[verified_files[0]["target"]] = "0" * 64
        with (
            mock.patch.object(console_backend, "CONFIG_ROOT", config),
            mock.patch.object(console_backend, "DEPLOYMENT_ROOT", self.root),
            mock.patch.object(console_backend, "_master_admin_profile", return_value=profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_load_history", return_value=[receipt]),
        ):
            with self.assertRaisesRegex(RuntimeError, "hash verification failed"):
                console_backend.preview_admin_tools_development_activation(
                    version=inspection.version
                )

    def test_successful_full_upload_records_service_eligible_receipt(self) -> None:
        remote = _UploadRemote()
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            result = console_backend.upload_artifact(
                inspection=self.inspection,
                selected_files=["smart_routing/api.py"],
                config_path="config/server_deploy.local.json",
                typed_confirmation="DEPLOY development v1",
                dry_run=False,
            )
        self.assertEqual(result["status"], "uploaded")
        self.assertTrue(result["selected_full_manifest"])
        self.assertTrue(result["remote_manifest_verified"])
        self.assertTrue(result["complete_manifest"])
        self.assertTrue(result["service_eligible"])
        self.assertEqual(len(remote.uploads), 1)
        self.assertEqual(recorded[0]["target_id"], console_backend._target_id(self.profile, "development"))
        self.assertEqual(len(recorded[0]["verified_files"]), 1)

    def test_incremental_upload_is_service_eligible_after_full_remote_verification(self) -> None:
        second = self.stage / "smart_routing" / "worker.py"
        second.write_text("print('worker')\n", encoding="utf-8")
        self.manifest["files"].append(
            {"path": "smart_routing/worker.py", "sha256": _sha256(second)}
        )
        inspection = console_backend.ArtifactInspection(
            **{**self.inspection.__dict__, "manifest": self.manifest}
        )
        remote = _UploadRemote()
        worker_target = "/home/csda/AI_Routing/development/smart_routing/worker.py"
        remote.checksums[worker_target] = _sha256(second)
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            result = console_backend.upload_artifact(
                inspection=inspection,
                selected_files=["smart_routing/api.py"],
                config_path="config/server_deploy.local.json",
                typed_confirmation="DEPLOY development v1",
                dry_run=False,
            )
        self.assertFalse(result["selected_full_manifest"])
        self.assertTrue(result["remote_manifest_verified"])
        self.assertTrue(result["complete_manifest"])
        self.assertTrue(result["service_eligible"])
        self.assertEqual(
            remote.uploads,
            ["/home/csda/AI_Routing/development/smart_routing/api.py"],
        )
        self.assertEqual(len(recorded[0]["changes"]), 1)
        self.assertEqual(len(recorded[0]["verified_files"]), 2)

    def test_incremental_upload_fails_before_write_when_unselected_file_differs(self) -> None:
        second = self.stage / "smart_routing" / "worker.py"
        second.write_text("print('worker')\n", encoding="utf-8")
        self.manifest["files"].append(
            {"path": "smart_routing/worker.py", "sha256": _sha256(second)}
        )
        inspection = console_backend.ArtifactInspection(
            **{**self.inspection.__dict__, "manifest": self.manifest}
        )
        remote = _UploadRemote()
        worker_target = "/home/csda/AI_Routing/development/smart_routing/worker.py"
        remote.checksums[worker_target] = "c" * 64
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            with self.assertRaisesRegex(RuntimeError, "Unselected remote manifest file"):
                console_backend.upload_artifact(
                    inspection=inspection,
                    selected_files=["smart_routing/api.py"],
                    config_path="config/server_deploy.local.json",
                    typed_confirmation="DEPLOY development v1",
                    dry_run=False,
                )
        self.assertEqual(remote.uploads, [])
        self.assertFalse(recorded[0]["remote_manifest_verified"])
        self.assertFalse(recorded[0]["service_eligible"])

    def test_post_upload_manifest_drift_compensates_selected_file(self) -> None:
        second = self.stage / "smart_routing" / "worker.py"
        second.write_text("print('worker')\n", encoding="utf-8")
        self.manifest["files"].append(
            {"path": "smart_routing/worker.py", "sha256": _sha256(second)}
        )
        inspection = console_backend.ArtifactInspection(
            **{**self.inspection.__dict__, "manifest": self.manifest}
        )
        worker_target = "/home/csda/AI_Routing/development/smart_routing/worker.py"
        api_target = "/home/csda/AI_Routing/development/smart_routing/api.py"
        remote = _UnselectedDriftRemote(worker_target)
        remote.checksums[worker_target] = _sha256(second)
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            with self.assertRaisesRegex(RuntimeError, "Remote manifest verification failed"):
                console_backend.upload_artifact(
                    inspection=inspection,
                    selected_files=["smart_routing/api.py"],
                    config_path="config/server_deploy.local.json",
                    typed_confirmation="DEPLOY development v1",
                    dry_run=False,
                )
        self.assertNotIn(api_target, remote.checksums)
        self.assertTrue(recorded[0]["compensated"])
        self.assertFalse(recorded[0]["service_eligible"])

    def test_multi_file_failure_compensates_and_records_failed_receipt(self) -> None:
        second = self.stage / "smart_routing" / "worker.py"
        second.write_text("print('worker')\n", encoding="utf-8")
        self.manifest["files"].append(
            {"path": "smart_routing/worker.py", "sha256": _sha256(second)}
        )
        inspection = console_backend.ArtifactInspection(
            **{**self.inspection.__dict__, "manifest": self.manifest}
        )
        remote = _FailingUploadRemote()
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated second-file failure"):
                console_backend.upload_artifact(
                    inspection=inspection,
                    selected_files=["smart_routing/api.py", "smart_routing/worker.py"],
                    config_path="config/server_deploy.local.json",
                    typed_confirmation="DEPLOY development v1",
                    dry_run=False,
                )
        self.assertFalse(
            any(path.startswith("/home/csda/AI_Routing/development/") for path in remote.checksums)
        )
        self.assertEqual(recorded[0]["status"], "upload_failed")
        self.assertTrue(recorded[0]["compensated"])

    def test_partial_backup_is_never_copied_over_unchanged_original(self) -> None:
        remote = _PartialBackupFailureRemote()
        target = "/home/csda/AI_Routing/development/smart_routing/api.py"
        original_checksum = "c" * 64
        remote.checksums[target] = original_checksum
        recorded: list[dict[str, object]] = []
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history", side_effect=recorded.append),
        ):
            with self.assertRaisesRegex(RuntimeError, "partial backup failure"):
                console_backend.upload_artifact(
                    inspection=self.inspection,
                    selected_files=["smart_routing/api.py"],
                    config_path="config/server_deploy.local.json",
                    typed_confirmation="DEPLOY development v1",
                    dry_run=False,
                )
        self.assertEqual(remote.checksums[target], original_checksum)
        self.assertEqual(recorded[0]["status"], "upload_failed")
        self.assertTrue(recorded[0]["compensated"])


if __name__ == "__main__":
    unittest.main()
