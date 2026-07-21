from __future__ import annotations

import contextlib
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.deploy import console_backend


class DeploymentConsoleConnectionSettingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.config = self.root / "config"
        self.config.mkdir()
        self.ssh_secret = "x" * 23
        self.dev_secret = "y" * 23
        self.prod_secret = "z" * 23
        self._write(
            "server_deploy.local.json",
            {
                "credentials_file": "config/server_ftp.local.json",
                "remote_root": "/srv/ai-routing",
                "allow_upload": False,
                "allow_service_control": False,
                "preserved_policy_extension": {"enabled": True},
            },
        )
        self._write(
            "server_ftp.local.json",
            {
                "protocol": "sftp",
                "host": "deploy.example.internal",
                "sftp_port": 22,
                "username": "deployer",
                "password": self.ssh_secret,
                "preserved_credential_extension": "keep",
            },
        )
        self._write_environment("development", "db-dev.example.internal", self.dev_secret)
        self._write_environment("production", "db-prod.example.internal", self.prod_secret)
        self.patches = (
            mock.patch.object(console_backend, "PROJECT_ROOT", self.root),
            mock.patch.object(console_backend, "CONFIG_ROOT", self.config),
        )
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)

    def _write(self, name: str, payload: dict[str, object]) -> None:
        (self.config / name).write_text(json.dumps(payload), encoding="utf-8")

    def _read(self, name: str) -> dict[str, object]:
        return json.loads((self.config / name).read_text(encoding="utf-8"))

    def _write_environment(self, environment: str, host: str, password: str) -> None:
        name = "common_vrp.dev.json" if environment == "development" else "common_vrp.prod.json"
        dbname = "vrp_db_dev" if environment == "development" else "vrp_db"
        self._write(
            name,
            {
                "environment": environment,
                "database": {
                    "host": host,
                    "port": 5432,
                    "dbname": dbname,
                    "user": "vrp_agent",
                    "password": password,
                },
                "preserved_environment_extension": ["keep"],
            },
        )

    def _database_update(self, environment: str, host: str) -> dict[str, object]:
        return {
            environment: {
                "host": host,
                "port": 5432,
                "dbname": "vrp_db_dev" if environment == "development" else "vrp_db",
                "user": "vrp_agent",
                "password": "   ",
            }
        }

    def test_get_returns_redacted_metadata_only(self) -> None:
        result = console_backend.get_connection_settings()

        rendered = json.dumps(result, sort_keys=True)
        self.assertEqual(result["status"], "configured")
        self.assertEqual(result["connection"]["host"], "deploy.example.internal")
        self.assertTrue(result["connection"]["password_configured"])
        self.assertFalse(result["connection"]["admin_tools_release_configured"])
        self.assertEqual(result["connection"]["admin_tools_release_version"], "")
        self.assertFalse(
            result["connection"]["admin_tools_development_release_configured"]
        )
        self.assertEqual(
            result["connection"]["admin_tools_development_release_version"], ""
        )
        self.assertEqual(
            result["connection"]["admin_tools_development_release_mode"], ""
        )
        self.assertTrue(
            result["environments"]["development"]["database"]["password_configured"]
        )
        self.assertNotIn(self.ssh_secret, rendered)
        self.assertNotIn(self.dev_secret, rendered)
        self.assertNotIn(self.prod_secret, rendered)

    def test_get_exposes_development_admin_tools_pin_without_credentials(self) -> None:
        profile = self._read("server_deploy.local.json")
        profile["admin_tools_release_version"] = "admin-clean"
        profile["admin_tools_development_release_version"] = "admin-dev-check"
        self._write("server_deploy.local.json", profile)

        result = console_backend.get_connection_settings()

        connection = result["connection"]
        self.assertEqual(connection["admin_tools_release_version"], "admin-clean")
        self.assertEqual(
            connection["admin_tools_development_release_version"], "admin-dev-check"
        )
        self.assertTrue(connection["admin_tools_development_release_configured"])
        self.assertEqual(
            connection["admin_tools_development_release_mode"],
            "development-verification",
        )

    def test_atomic_write_preserves_secrets_and_unknown_fields_for_blank_password(self) -> None:
        with mock.patch.object(
            console_backend,
            "_replace_connection_target",
            wraps=console_backend._replace_connection_target,
        ) as replace_target:
            result = console_backend.update_connection_settings(
                databases=self._database_update("development", "db-dev-new.example.internal")
            )

        saved = self._read("common_vrp.dev.json")
        self.assertEqual(result["status"], "configured")
        self.assertEqual(saved["database"]["host"], "db-dev-new.example.internal")
        self.assertEqual(saved["database"]["password"], self.dev_secret)
        self.assertEqual(saved["preserved_environment_extension"], ["keep"])
        self.assertEqual(replace_target.call_count, 1)
        temporary_path, final_path = replace_target.call_args.args
        self.assertEqual(
            Path(final_path).resolve(), (self.config / "common_vrp.dev.json").resolve()
        )
        self.assertTrue(Path(temporary_path).name.startswith(".common_vrp.dev.json."))
        self.assertEqual(list(self.config.glob("*.tmp")), [])

    def test_database_request_must_target_exactly_one_environment(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one environment"):
            console_backend.update_connection_settings(
                databases={
                    **self._database_update("development", "db-dev-new.example.internal"),
                    **self._database_update("production", "db-prod-new.example.internal"),
                },
                confirm_production=True,
            )

    def test_ssh_transaction_rolls_back_after_each_replacement_failpoint(self) -> None:
        originals = {
            name: (self.config / name).read_bytes()
            for name in ("server_ftp.local.json", "server_deploy.local.json")
        }
        submitted_secret = "n" * 23
        request = {
            "host": "new-deploy.example.internal",
            "port": 2222,
            "username": "new_deployer",
            "remote_root": "/srv/ai-routing-next",
            "password": submitted_secret,
        }
        real_replace = console_backend._replace_connection_target
        for fail_after in (1, 2):
            with self.subTest(fail_after=fail_after):
                calls = 0

                def failpoint(staged: Path, target: Path) -> None:
                    nonlocal calls
                    real_replace(staged, target)
                    calls += 1
                    if calls == fail_after:
                        raise OSError("simulated local replacement failure")

                with mock.patch.object(
                    console_backend, "_replace_connection_target", side_effect=failpoint
                ):
                    with self.assertRaisesRegex(RuntimeError, "no changes were kept") as error:
                        console_backend.update_connection_settings(
                            ssh_sftp=request,
                            confirm_production=True,
                        )
                self.assertNotIn(submitted_secret, str(error.exception))
                for name, original in originals.items():
                    self.assertEqual((self.config / name).read_bytes(), original)
                self.assertFalse(
                    (self.config / ".connection-settings-transaction.local.json").exists()
                )
                self.assertEqual(list(self.config.glob(".connection-settings-*.bak")), [])

    def test_interrupted_ssh_transaction_is_recovered_before_next_update(self) -> None:
        originals = {
            name: (self.config / name).read_bytes()
            for name in ("server_ftp.local.json", "server_deploy.local.json")
        }
        real_replace = console_backend._replace_connection_target

        def interrupted(staged: Path, target: Path) -> None:
            real_replace(staged, target)
            raise KeyboardInterrupt("simulated process interruption")

        with mock.patch.object(
            console_backend, "_replace_connection_target", side_effect=interrupted
        ):
            with self.assertRaises(KeyboardInterrupt):
                console_backend.update_connection_settings(
                    ssh_sftp={
                        "host": "new-deploy.example.internal",
                        "port": 2222,
                        "username": "new_deployer",
                        "remote_root": "/srv/ai-routing-next",
                        "password": "n" * 23,
                    },
                    confirm_production=True,
                )

        self.assertTrue(
            (self.config / ".connection-settings-transaction.local.json").is_file()
        )
        self.assertEqual(console_backend.get_connection_settings()["status"], "unavailable")
        console_backend.update_connection_settings(
            databases=self._database_update("development", "db-dev-new.example.internal")
        )
        for name, original in originals.items():
            self.assertEqual((self.config / name).read_bytes(), original)
        self.assertFalse(
            (self.config / ".connection-settings-transaction.local.json").exists()
        )

    def test_target_guards_reject_wrong_database_and_credentials_path(self) -> None:
        with self.assertRaisesRegex(ValueError, "not allowed"):
            console_backend.update_connection_settings(
                databases={
                    "development": {
                        "host": "db-dev.example.internal",
                        "port": 5432,
                        "dbname": "vrp_db",
                        "user": "vrp_agent",
                    }
                }
            )
        profile = self._read("server_deploy.local.json")
        profile["credentials_file"] = "config/not-allowed.local.json"
        self._write("server_deploy.local.json", profile)
        with self.assertRaisesRegex(ValueError, "fixed local path"):
            console_backend.update_connection_settings(
                databases=self._database_update("development", "db-dev.example.internal")
            )

    def test_production_changes_require_explicit_confirmation(self) -> None:
        before = self._read("common_vrp.prod.json")
        with self.assertRaisesRegex(PermissionError, "explicit confirmation"):
            console_backend.update_connection_settings(
                databases=self._database_update("production", "db-prod-new.example.internal")
            )
        self.assertEqual(self._read("common_vrp.prod.json"), before)

        result = console_backend.update_connection_settings(
            databases=self._database_update("production", "db-prod-new.example.internal"),
            confirm_production=True,
        )
        saved = self._read("common_vrp.prod.json")
        self.assertEqual(saved["database"]["host"], "db-prod-new.example.internal")
        self.assertEqual(saved["database"]["password"], self.prod_secret)
        self.assertTrue(
            result["environments"]["production"]["database"]["password_configured"]
        )

    def test_ssh_update_requires_confirmation_and_never_returns_new_password(self) -> None:
        submitted_secret = "n" * 23
        with self.assertRaisesRegex(PermissionError, "explicit confirmation"):
            console_backend.update_connection_settings(
                ssh_sftp={
                    "host": "new-deploy.example.internal",
                    "port": 2222,
                    "username": "new_deployer",
                    "remote_root": "/srv/ai-routing-next",
                    "password": submitted_secret,
                }
            )
        result = console_backend.update_connection_settings(
            ssh_sftp={
                "host": "new-deploy.example.internal",
                "port": 2222,
                "username": "new_deployer",
                "remote_root": "/srv/ai-routing-next",
                "password": submitted_secret,
            },
            confirm_production=True,
        )
        self.assertEqual(self._read("server_ftp.local.json")["password"], submitted_secret)
        self.assertNotIn(submitted_secret, json.dumps(result, sort_keys=True))
        self.assertEqual(
            self._read("server_deploy.local.json")["preserved_policy_extension"],
            {"enabled": True},
        )
        self.assertEqual(
            self._read("server_ftp.local.json")["preserved_credential_extension"], "keep"
        )


class DeploymentConsoleUploadProgressTests(unittest.TestCase):
    def test_callback_reports_only_verified_manifest_relative_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "worker.py"
            source.write_text("print('safe')\n", encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            inspection = console_backend.ArtifactInspection(
                path=str(root),
                kind="runtime",
                environment="development",
                version="v1",
                manifest={"files": [{"path": "worker.py", "sha256": checksum}]},
                archive_sha256="a" * 64,
                target_upload_path="/safe/development",
                required_confirmation="DEPLOY development v1",
                restricted_data=False,
            )

            class FakeRemote:
                def __init__(self) -> None:
                    self.checksums: dict[str, str] = {}

                def __enter__(self) -> "FakeRemote":
                    return self

                def __exit__(self, *_: object) -> None:
                    return None

                @contextlib.contextmanager
                def deployment_lock(self, *_: object):
                    yield

                def exists(self, path: str) -> bool:
                    return path in self.checksums

                def upload_atomic(self, local: Path, target: str, _backup: str | None) -> None:
                    self.checksums[target] = hashlib.sha256(local.read_bytes()).hexdigest()

                def sha256(self, path: str) -> str | None:
                    return self.checksums.get(path)

            profile = {
                "host": "deploy.example.internal",
                "port": 22,
                "username": "deployer",
                "remote_root": "/safe",
                "allow_upload": True,
            }
            events: list[tuple[int, int, str, str]] = []
            with (
                mock.patch.object(console_backend, "_load_remote_profile", return_value=profile),
                mock.patch.object(
                    console_backend, "_remote_session_factory", return_value=FakeRemote()
                ),
                mock.patch.object(console_backend, "_append_history"),
            ):
                result = console_backend.upload_artifact(
                    inspection=inspection,
                    selected_files=["worker.py"],
                    config_path="config/server_deploy.local.json",
                    typed_confirmation="DEPLOY development v1",
                    dry_run=False,
                    progress_callback=lambda completed, total, path, status: events.append(
                        (completed, total, path, status)
                    ),
                )
        self.assertEqual(result["status"], "uploaded")
        self.assertEqual(events, [(1, 1, "worker.py", "verified")])


if __name__ == "__main__":
    unittest.main()
