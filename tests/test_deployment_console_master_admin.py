"""Platform contract tests for the SSH-only master-data console bridge."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from services.deploy import console_backend


class _Channel:
    def __init__(self, code: int = 0) -> None:
        self.code = code
        self.closed = False

    def recv_exit_status(self) -> int:
        return self.code

    def close(self) -> None:
        self.closed = True


class _Stream:
    def __init__(self, payload: bytes, channel: _Channel) -> None:
        self.payload = payload
        self.channel = channel

    def read(self, size: int = -1) -> bytes:
        return self.payload if size < 0 else self.payload[:size]


class _Client:
    def __init__(self, stdout: bytes, stderr: bytes = b"", code: int = 0) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.code = code
        self.commands: list[tuple[str, int]] = []

    def exec_command(self, command: str, timeout: int = 45):
        self.commands.append((command, timeout))
        channel = _Channel(self.code)
        return None, _Stream(self.stdout, channel), _Stream(self.stderr, channel)


class _Remote:
    def __init__(self, payload: dict[str, object] | None = None) -> None:
        self.payload = payload or _payload()
        self.commands: list[tuple[str, int]] = []
        self.uploads: list[tuple[bytes, str, object]] = []
        self.removed: list[str] = []
        self.files: set[str] = set()
        self.exit_code = 0
        self.stderr = ""
        self.cleanup_error: Exception | None = None
        self.default_execute_calls = 0
        self.checksums: dict[str, str] = {}
        self.inventory: list[str] = []

    def __enter__(self) -> "_Remote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, command: str, timeout: int = 45) -> tuple[int, str, str]:
        self.default_execute_calls += 1
        redacted = dict(self.payload)
        if "confirmation_token" in redacted:
            redacted["confirmation_token"] = "[REDACTED]"
        return self.exit_code, json.dumps(redacted), self.stderr

    def execute_master_json(self, command: str, timeout: int = 45) -> tuple[int, str, str]:
        self.commands.append((command, timeout))
        return self.exit_code, json.dumps(self.payload), self.stderr

    def upload_bytes_atomic(self, payload: bytes, target: str, backup: object = None) -> None:
        self.uploads.append((payload, target, backup))
        self.files.add(target)

    def exists(self, target: str) -> bool:
        return target in self.files

    def remove(self, target: str) -> None:
        if self.cleanup_error is not None:
            raise self.cleanup_error
        self.files.discard(target)
        self.removed.append(target)

    def sha256(self, target: str) -> str | None:
        return self.checksums.get(target)

    def inventory_files(self, _directory: str) -> list[str]:
        return list(self.inventory)


def _payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "contract_version": "db-admin/v1",
        "environment": "development",
        "dbname": "vrp_db_dev",
        "target_id": "development:vrp_db_dev",
        "status": "ok",
    }
    payload.update(overrides)
    return payload


class MasterAdminBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        with console_backend._MASTER_PREVIEW_CONFIRMATION_LOCK:
            console_backend._MASTER_PREVIEW_CONFIRMATIONS.clear()
        self.profile = {
            "host": "example.test",
            "port": 22,
            "username": "operator",
            "password": "not-used-by-fake",
            "remote_root": "/home/csda/AI_Routing",
            "admin_tools_release_version": "admin-v20260720",
        }

    def _call(self, remote: _Remote, callback: object) -> object:
        with mock.patch.object(
            console_backend, "_master_admin_profile", return_value=self.profile
        ), mock.patch.object(
            console_backend, "_remote_session_factory", return_value=remote
        ), mock.patch.object(console_backend, "_verify_master_admin_release"):
            return callback()

    def test_overview_is_ssh_only_and_pins_remote_release_runtime_and_config(self) -> None:
        remote = _Remote()
        result = self._call(
            remote, lambda: console_backend.get_database_overview(environment="development")
        )

        self.assertEqual(result["status"], "ok")
        command, timeout = remote.commands[0]
        self.assertEqual(timeout, console_backend._MASTER_ADMIN_TIMEOUT_SECONDS)
        self.assertIn("/admin_tools/releases/admin-v20260720", command)
        self.assertIn("/development/.venv/bin/python", command)
        self.assertIn(" -B -m admin_tools.db.master_data_backend", command)
        self.assertIn("/development/config_common_vrp.dev.json", command)
        self.assertIn("admin_tools.db.master_data_backend", command)
        self.assertNotIn("localhost", command)
        self.assertNotIn("127.0.0.1", command)

    def test_paramiko_master_json_path_preserves_token_but_default_executor_redacts(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        command = console_backend._master_admin_command(
            context,
            "preview",
            ("--table", "common_technician_master", "--csv", "/tmp/input.csv"),
        )
        payload = json.dumps(
            _payload(
                preview_id="p1",
                preview_digest="a" * 64,
                confirmation_token="real-one-time-token",
            )
        ).encode()
        raw_remote = console_backend.ParamikoRemote(self.profile)
        raw_remote.client = _Client(payload)
        _, raw_stdout, _ = raw_remote.execute_master_json(command)
        self.assertIn("real-one-time-token", raw_stdout)

        default_remote = console_backend.ParamikoRemote(self.profile)
        default_remote.client = _Client(payload)
        _, redacted_stdout, _ = default_remote.execute(command)
        self.assertNotIn("real-one-time-token", redacted_stdout)
        self.assertIn("[REDACTED]", redacted_stdout)
        with self.assertRaisesRegex(ValueError, "not an allowlisted"):
            raw_remote.execute_master_json("uname -a")

    def test_master_command_requires_exact_bytecode_disabled_flag(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        command = console_backend._master_admin_command(context, "overview")
        remote = console_backend.ParamikoRemote(self.profile)
        remote.client = _Client(json.dumps(_payload()).encode())

        self.assertIn(" -B -m ", command)
        for malformed in (
            command.replace(" -B -m ", " -m "),
            command.replace(" -B -m ", " -B -X dev -m "),
        ):
            with self.subTest(command=malformed):
                with self.assertRaisesRegex(ValueError, "allowlisted|canonically"):
                    remote.execute_master_json(malformed)

    def test_python_bytecode_flag_preserves_exact_release_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = root / "admin_tools" / "db"
            package.mkdir(parents=True)
            (root / "admin_tools" / "__init__.py").write_text("", encoding="utf-8")
            (package / "__init__.py").write_text("", encoding="utf-8")
            (package / "master_data_backend.py").write_text(
                "print('{\\\"status\\\":\\\"ok\\\"}')\n", encoding="utf-8"
            )
            expected = {
                path.relative_to(root).as_posix()
                for path in root.rglob("*")
                if path.is_file()
            }
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "admin_tools.db.master_data_backend",
                ],
                cwd=root,
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
            )
            actual = {
                path.relative_to(root).as_posix()
                for path in root.rglob("*")
                if path.is_file()
            }

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), '{"status":"ok"}')
        self.assertEqual(actual, expected)

    def test_preview_uploads_only_bounded_csv_and_removes_remote_stage(self) -> None:
        remote = _Remote(
            _payload(
                preview_id="p1",
                preview_digest="a" * 64,
                confirmation_token="backend-issued-token",
            )
        )
        result = self._call(
            remote,
            lambda: console_backend.preview_master_csv_upsert(
                environment="development",
                table_id="common_technician_master",
                file_name="technicians.csv",
                csv_bytes=b"employee_code,name\nT1,Ada\n",
            ),
        )

        self.assertEqual(result["preview_id"], "p1")
        self.assertNotIn("confirmation_token", result)
        self.assertNotIn("_private_confirmation_token", result)
        with console_backend._MASTER_PREVIEW_CONFIRMATION_LOCK:
            self.assertEqual(
                console_backend._MASTER_PREVIEW_CONFIRMATIONS["p1"][3],
                "backend-issued-token",
            )
        self.assertEqual(remote.default_execute_calls, 0)
        self.assertEqual(len(remote.uploads), 1)
        data, target, backup = remote.uploads[0]
        self.assertEqual(data, b"employee_code,name\nT1,Ada\n")
        self.assertIsNone(backup)
        self.assertIn("/.deployment-console/master-csv/development/preview-", target)
        self.assertEqual(remote.removed, [target])
        self.assertNotIn(target, remote.files)
        command, _ = remote.commands[0]
        self.assertIn("--table common_technician_master", command)
        self.assertIn("--csv", command)
        self.assertNotIn("technicians.csv", command)

    def test_preview_cleanup_failure_withholds_result(self) -> None:
        remote = _Remote(
            _payload(
                preview_id="p1",
                preview_digest="a" * 64,
                confirmation_token="backend-issued-token",
            )
        )
        remote.cleanup_error = OSError("denied")
        with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
            self._call(
                remote,
                lambda: console_backend.preview_master_csv_upsert(
                    environment="development",
                    table_id="common_technician_master",
                    file_name="technicians.csv",
                    csv_bytes=b"employee_code,name\nT1,Ada\n",
                ),
            )

    def test_apply_requires_second_confirmation_and_never_accepts_csv_or_table(self) -> None:
        remote = _Remote(_payload(status="applied", operation_id=str(uuid.uuid4())))
        key = str(uuid.uuid4())
        with console_backend._MASTER_PREVIEW_CONFIRMATION_LOCK:
            console_backend._MASTER_PREVIEW_CONFIRMATIONS["preview-1"] = (
                "a" * 64,
                "development",
                console_backend._target_id(self.profile, "development"),
                "backend-issued-token",
                console_backend.datetime.now(console_backend.timezone.utc),
            )
        with self.assertRaises(PermissionError):
            self._call(
                remote,
                lambda: console_backend.apply_master_csv_upsert(
                    environment="development",
                    preview_id="preview-1",
                    preview_digest="a" * 64,
                    idempotency_key=key,
                ),
            )
        self.assertEqual(remote.commands, [])

        result = self._call(
            remote,
            lambda: console_backend.apply_master_csv_upsert(
                environment="development",
                preview_id="preview-1",
                preview_digest="a" * 64,
                idempotency_key=key,
                confirm=True,
            ),
        )
        self.assertEqual(result["status"], "applied")
        command, _ = remote.commands[0]
        self.assertIn("--preview-id preview-1", command)
        self.assertIn("--preview-digest", command)
        self.assertIn("--idempotency-key", command)
        self.assertIn("backend-issued-token", command)
        self.assertNotIn("--confirm-production", command)
        self.assertNotIn("--csv", command)
        self.assertNotIn("--table", command)

    def test_production_master_preview_and_apply_stop_before_profile_or_remote(self) -> None:
        with mock.patch.object(console_backend, "_master_admin_profile") as profile, mock.patch.object(
            console_backend, "_remote_session_factory"
        ) as remote_factory:
            with self.assertRaisesRegex(PermissionError, "Production master CSV preview"):
                console_backend.preview_master_csv_upsert(
                    environment="production",
                    table_id="common_technician_master",
                    file_name="technicians.csv",
                    csv_bytes=b"employee_code\nT1\n",
                )
            with self.assertRaisesRegex(PermissionError, "Production master CSV apply"):
                console_backend.apply_master_csv_upsert(
                    environment="production",
                    preview_id="preview-1",
                    preview_digest="a" * 64,
                    idempotency_key=str(uuid.uuid4()),
                    confirm=True,
                )
        profile.assert_not_called()
        remote_factory.assert_not_called()

    def test_remote_response_must_match_contract_and_selected_target(self) -> None:
        remote = _Remote(_payload(target_id="production:vrp_db"))
        with self.assertRaisesRegex(RuntimeError, "target does not match"):
            self._call(
                remote,
                lambda: console_backend.get_database_overview(environment="development"),
            )

    def test_structured_remote_rejection_survives_nonzero_cli_exit(self) -> None:
        remote = _Remote(_payload(status="preview_stale", error_code="PREVIEW_STALE"))
        remote.exit_code = 3
        result = self._call(
            remote, lambda: console_backend.get_database_overview(environment="development")
        )
        self.assertEqual(result["error_code"], "PREVIEW_STALE")

    def test_remote_non_json_and_csv_limits_fail_closed(self) -> None:
        remote = _Remote()
        remote.execute_master_json = lambda *_args, **_kwargs: (0, "not-json", "")  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
            self._call(
                remote,
                lambda: console_backend.get_database_overview(environment="development"),
            )
        with self.assertRaisesRegex(ValueError, "UTF-8"):
            self._call(
                _Remote(),
                lambda: console_backend.preview_master_csv_upsert(
                    environment="development",
                    table_id="common_technician_master",
                    file_name="bad.csv",
                    csv_bytes=b"\xff",
                ),
            )

    def test_profile_requires_pinned_immutable_release(self) -> None:
        profile = dict(self.profile)
        profile["admin_tools_release_version"] = ""
        with self.assertRaisesRegex(ValueError, "admin_tools_release_version"):
            console_backend._master_admin_context(profile, "development")

    def test_development_pin_precedes_common_pin_but_production_never_uses_it(self) -> None:
        profile = {
            **self.profile,
            "admin_tools_development_release_version": "admin-dev-verification",
        }
        development = console_backend._master_admin_context(profile, "development")
        production = console_backend._master_admin_context(profile, "production")
        self.assertEqual(development["release_version"], "admin-dev-verification")
        self.assertEqual(development["pin_scope"], "development")
        self.assertEqual(production["release_version"], "admin-v20260720")
        self.assertEqual(production["pin_scope"], "common")

    def test_release_trust_requires_clean_target_receipt_and_remote_hashes(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        inspection = console_backend.ArtifactInspection(
            path="C:/artifact/admin-v20260720",
            kind="admin-tools",
            environment="development",
            version="admin-v20260720",
            manifest={"source_dirty": False, "promotable": True},
            archive_sha256="a" * 64,
            target_upload_path=context["release_root"],
            required_confirmation="unused",
            restricted_data=False,
        )
        release_files = {
            "deploy_manifest.json": ("d" * 64, Path("deploy_manifest.json")),
            "admin_tools/db/master_data_backend.py": ("m" * 64, Path("master.py")),
            "admin_tools/__init__.py": ("i" * 64, Path("__init__.py")),
        }
        verified_files = [
            {
                "path": relative,
                "target": f"{context['release_root']}/{relative}",
                "sha256": checksum,
            }
            for relative, (checksum, _) in release_files.items()
        ]
        receipt = {
            "kind": "admin-tools",
            "environment": "development",
            "version": "admin-v20260720",
            "status": "uploaded",
            "target_id": context["remote_target_id"],
            "complete_manifest": True,
            "remote_manifest_verified": True,
            "sha256": "a" * 64,
            "verified_files": verified_files,
        }
        remote = _Remote()
        remote.checksums = {
            row["target"]: row["sha256"] for row in verified_files
        }
        remote.inventory = list(release_files)

        def verify(*, item=inspection, history=(receipt,), target=remote) -> None:
            with mock.patch.object(
                console_backend, "inspect_artifact", return_value=item
            ), mock.patch.object(
                console_backend, "_release_file_map", return_value=release_files
            ), mock.patch.object(
                console_backend, "_load_history", return_value=list(history)
            ):
                console_backend._verify_master_admin_release(target, context)

        verify()
        with self.assertRaisesRegex(PermissionError, "no local upload receipt"):
            verify(history=())
        dirty = console_backend.ArtifactInspection(
            **{
                **inspection.__dict__,
                "manifest": {"source_dirty": True, "promotable": False},
            }
        )
        with self.assertRaisesRegex(PermissionError, "clean promotable"):
            verify(item=dirty)
        dirty_development_profile = {
            **self.profile,
            "admin_tools_development_release_version": "admin-v20260720",
        }
        dirty_development_context = console_backend._master_admin_context(
            dirty_development_profile, "development"
        )
        with mock.patch.object(
            console_backend, "inspect_artifact", return_value=dirty
        ), mock.patch.object(
            console_backend, "_release_file_map", return_value=release_files
        ), mock.patch.object(
            console_backend, "_load_history", return_value=[receipt]
        ):
            console_backend._verify_master_admin_release(remote, dirty_development_context)
        wrong_target = {**receipt, "target_id": "wrong-target"}
        with self.assertRaisesRegex(PermissionError, "does not match"):
            verify(history=(wrong_target,))
        drifted = _Remote()
        drifted.checksums = dict(remote.checksums)
        drifted.inventory = list(release_files)
        init_target = next(
            row["target"]
            for row in verified_files
            if row["path"] == "admin_tools/__init__.py"
        )
        drifted.checksums[init_target] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "hash verification failed"):
            verify(target=drifted)
        shadowed = _Remote()
        shadowed.checksums = dict(remote.checksums)
        shadowed.inventory = [*release_files, "admin_tools/shadow.py"]
        with self.assertRaisesRegex(RuntimeError, "inventory differs"):
            verify(target=shadowed)

    def test_legacy_local_migration_and_seed_entrypoints_are_fail_closed(self) -> None:
        self.assertEqual(
            console_backend.list_migrations(
                environment="development", config_path="C:/not-read.json"
            ),
            [],
        )
        self.assertEqual(
            console_backend.list_seed_actions(
                environment="development", config_path="C:/not-read.json"
            ),
            [],
        )
        migration = console_backend.execute_migration(
            environment="development",
            migration_id="V001__not_run",
            config_path="C:/not-read.json",
            typed_confirmation="internal-only",
            dry_run=False,
        )
        seed = console_backend.run_seed_action(
            environment="development",
            action_id="not-run",
            config_path="C:/not-read.json",
            typed_confirmation="internal-only",
            dry_run=False,
        )
        self.assertEqual(migration["error_code"], "REMOTE_ADMIN_CLI_REQUIRED")
        self.assertEqual(seed["error_code"], "REMOTE_ADMIN_CLI_REQUIRED")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
