from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.deploy import console_backend


class _Remote:
    def __init__(self, checksum: str, target: str) -> None:
        self.checksum = checksum
        self.target = target
        self.calls = 0

    def __enter__(self) -> "_Remote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def sha256(self, path: str) -> str | None:
        self.calls += 1
        return self.checksum if path == self.target else None

    def size(self, path: str) -> int | None:
        return 17 if path == self.target else None


class RemoteManifestCacheTests(unittest.TestCase):
    def test_preview_uses_db_after_first_remote_observation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "app.py"
            source.write_text("APP = True\n", encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            inspection = console_backend.ArtifactInspection(
                path=str(root),
                kind="runtime",
                environment="development",
                version="v1",
                manifest={"files": [{"path": "app.py", "sha256": checksum}]},
                archive_sha256="a" * 64,
                target_upload_path="/home/csda/AI_Routing/development",
                required_confirmation="DEPLOY development v1",
                restricted_data=False,
            )
            profile = {
                "host": "server.example.internal",
                "port": 22,
                "username": "deployer",
                "remote_root": "/home/csda/AI_Routing",
            }
            target = "/home/csda/AI_Routing/development/app.py"
            remote = _Remote(checksum, target)
            db_path = root / "manifest.sqlite3"

            with (
                mock.patch.object(console_backend, "REMOTE_MANIFEST_DB_PATH", db_path),
                mock.patch.object(console_backend, "_load_remote_profile", return_value=profile),
                mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            ):
                first = console_backend.preview_remote_diff(
                    inspection=inspection,
                    selected_files=["app.py"],
                    config_path="config/server_deploy.local.json",
                )
                first_calls = remote.calls
                with mock.patch.object(
                    console_backend,
                    "_remote_session_factory",
                    side_effect=AssertionError("cached preview must not connect remotely"),
                ):
                    second = console_backend.preview_remote_diff(
                        inspection=inspection,
                        selected_files=["app.py"],
                        config_path="config/server_deploy.local.json",
                    )

            self.assertEqual(first[0]["status"], "unchanged")
            self.assertEqual(second[0]["status"], "unchanged")
            self.assertGreater(first_calls, 0)
            self.assertEqual(second[0]["remote_state_source"], "manifest_db")

    def test_refresh_remote_bypasses_db_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "app.py"
            source.write_text("APP = True\n", encoding="utf-8")
            checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            inspection = console_backend.ArtifactInspection(
                path=str(root), kind="runtime", environment="development", version="v1",
                manifest={"files": [{"path": "app.py", "sha256": checksum}]},
                archive_sha256="a" * 64, target_upload_path="/home/csda/AI_Routing/development",
                required_confirmation="DEPLOY development v1", restricted_data=False,
            )
            profile = {"host": "server", "port": 22, "username": "user", "remote_root": "/home/csda/AI_Routing"}
            target = "/home/csda/AI_Routing/development/app.py"
            remote = _Remote(checksum, target)
            db_path = root / "manifest.sqlite3"
            with (
                mock.patch.object(console_backend, "REMOTE_MANIFEST_DB_PATH", db_path),
                mock.patch.object(console_backend, "_load_remote_profile", return_value=profile),
                mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            ):
                console_backend.preview_remote_diff(
                    inspection=inspection, selected_files=["app.py"], config_path="config/server_deploy.local.json"
                )
                before = remote.calls
                refreshed = console_backend.preview_remote_diff(
                    inspection=inspection, selected_files=["app.py"],
                    config_path="config/server_deploy.local.json", refresh_remote=True,
                )
            self.assertGreater(remote.calls, before)
            self.assertEqual(refreshed[0]["remote_state_source"], "remote_sftp")

    def test_refresh_remote_replaces_stale_db_checksum(self) -> None:
        """A manual refresh is the recovery path after an interrupted upload."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "app.py"
            source.write_text("APP = True\n", encoding="utf-8")
            local_checksum = hashlib.sha256(source.read_bytes()).hexdigest()
            inspection = console_backend.ArtifactInspection(
                path=str(root), kind="runtime", environment="development", version="v1",
                manifest={"files": [{"path": "app.py", "sha256": local_checksum}]},
                archive_sha256="a" * 64, target_upload_path="/home/csda/AI_Routing/development",
                required_confirmation="DEPLOY development v1", restricted_data=False,
            )
            profile = {"host": "server", "port": 22, "username": "user", "remote_root": "/home/csda/AI_Routing"}
            target = "/home/csda/AI_Routing/development/app.py"
            remote = _Remote("b" * 64, target)
            db_path = root / "manifest.sqlite3"
            with (
                mock.patch.object(console_backend, "REMOTE_MANIFEST_DB_PATH", db_path),
                mock.patch.object(console_backend, "_load_remote_profile", return_value=profile),
                mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            ):
                console_backend.preview_remote_diff(
                    inspection=inspection, selected_files=["app.py"], config_path="config/server_deploy.local.json"
                )
                remote.checksum = local_checksum
                console_backend.preview_remote_diff(
                    inspection=inspection, selected_files=["app.py"], config_path="config/server_deploy.local.json", refresh_remote=True,
                )
                cached = console_backend.preview_remote_diff(
                    inspection=inspection, selected_files=["app.py"], config_path="config/server_deploy.local.json"
                )
            self.assertEqual(cached[0]["status"], "unchanged")
            self.assertEqual(cached[0]["remote_state_source"], "manifest_db")


if __name__ == "__main__":
    unittest.main()
