from __future__ import annotations

import contextlib
import unittest
from unittest import mock

from services.deploy import console_backend


class _FakeRemote:
    def __init__(self, checksums: dict[str, str] | None = None) -> None:
        self.commands: list[tuple[str, int]] = []
        self.lock_events: list[tuple[str, str, str]] = []
        self.checksums = checksums or {}

    def __enter__(self) -> "_FakeRemote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, command: str, timeout: int = 45) -> tuple[int, str, str]:
        self.commands.append((command, timeout))
        if command.startswith("systemctl is-active "):
            return 0, "active\n", ""
        if command.startswith("systemctl is-enabled "):
            return 0, "enabled\n", ""
        if command.startswith("journalctl "):
            return 0, "healthy", ""
        return 0, "", ""

    def sha256(self, path: str) -> str | None:
        return self.checksums.get(path, "a" * 64)

    @contextlib.contextmanager
    def deployment_lock(self, base: str, deployment_id: str):
        self.lock_events.append(("enter", base, deployment_id))
        try:
            yield
        finally:
            self.lock_events.append(("exit", base, deployment_id))


class DeploymentConsoleServiceActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release_id = "development-runtime-v1-abc123"
        self.profile = {
            "host": "deploy.example.internal",
            "port": 22,
            "username": "deploy-user",
            "remote_root": "/home/csda/AI_Routing",
            "allow_service_control": True,
        }
        self.history = [
            {
                "id": self.release_id,
                "environment": "development",
                "kind": "runtime",
                "status": "uploaded",
                "sha256": "a" * 64,
                "complete_manifest": True,
                "service_eligible": True,
                "target_id": console_backend._target_id(
                    self.profile, "development"
                ),
                "changes": [
                    {
                        "path": "sr_vrp_api_server.py",
                        "target": (
                            "/home/csda/AI_Routing/development/sr_vrp_api_server.py"
                        ),
                        "sha256": "a" * 64,
                    }
                ],
            }
        ]

    def _run(self, **overrides: object) -> tuple[dict[str, object], _FakeRemote]:
        remote = _FakeRemote(getattr(self, "remote_checksums", None))
        kwargs: dict[str, object] = {
            "environment": "development",
            "action": "start",
            "units": ["common-vrp-dev.service", "smart-routing-dev.service"],
            "release_id": self.release_id,
            "config_path": "config/server_deploy.local.json",
            "typed_confirmation": (
                "START development development-runtime-v1-abc123 "
                "common-vrp-dev.service,smart-routing-dev.service"
            ),
        }
        kwargs.update(overrides)
        with (
            mock.patch.object(console_backend, "_load_history", return_value=self.history),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_append_history") as append_history,
        ):
            result = console_backend.run_service_action(**kwargs)
        self.last_audit = append_history
        return result, remote

    def test_start_is_bound_to_latest_runtime_release_and_allowlisted_units(self) -> None:
        result, remote = self._run()
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["release_id"], self.release_id)
        self.assertNotIn(("sudo -n systemctl daemon-reload", 45), remote.commands)
        self.assertIn(("sudo -n systemctl start common-vrp-dev.service", 90), remote.commands)
        self.assertIn(("sudo -n systemctl start smart-routing-dev.service", 90), remote.commands)
        self.assertNotIn("common-vrp.service", "\n".join(command for command, _ in remote.commands))

    def test_restart_uses_the_same_release_and_unit_guards(self) -> None:
        result, remote = self._run(
            action="restart",
            typed_confirmation=(
                "RESTART development development-runtime-v1-abc123 "
                "common-vrp-dev.service,smart-routing-dev.service"
            ),
        )
        self.assertEqual(result["action"], "restart")
        self.assertIn(("sudo -n systemctl restart common-vrp-dev.service", 90), remote.commands)
        self.assertEqual([event[0] for event in remote.lock_events], ["enter", "exit"])
        self.assertEqual(remote.lock_events[0][1], "/home/csda/AI_Routing")
        self.assertEqual(remote.lock_events[0][2], result["action_id"])
        audit = self.last_audit.call_args.args[0]
        self.assertEqual(audit["status"], "healthy")
        self.assertEqual(audit["release_id"], self.release_id)

    def test_cross_environment_unit_is_rejected_before_remote_access(self) -> None:
        with self.assertRaisesRegex(ValueError, "not allowlisted"):
            self._run(
                units=["common-vrp.service"],
                typed_confirmation=(
                    "START development development-runtime-v1-abc123 common-vrp.service"
                ),
            )

    def test_confirmation_must_include_exact_release_and_ordered_units(self) -> None:
        with self.assertRaisesRegex(ValueError, "confirmation mismatch"):
            self._run(typed_confirmation="START development common-vrp-dev.service")

    def test_old_or_non_runtime_release_cannot_authorize_service_action(self) -> None:
        self.history = [
            {
                "id": "development-runtime-old",
                "environment": "development",
                "kind": "runtime",
                "status": "uploaded",
                "sha256": "a" * 64,
                "complete_manifest": True,
                "service_eligible": True,
                "target_id": console_backend._target_id(
                    self.profile, "development"
                ),
                "changes": self.history[0]["changes"],
            },
            {
                "id": "development-data-new",
                "environment": "development",
                "kind": "server-data",
                "status": "uploaded",
            },
            {
                "id": "development-runtime-new",
                "environment": "development",
                "kind": "runtime",
                "status": "uploaded",
                "sha256": "b" * 64,
                "complete_manifest": True,
                "service_eligible": True,
                "target_id": console_backend._target_id(
                    self.profile, "development"
                ),
                "changes": self.history[0]["changes"],
            },
        ]
        with self.assertRaisesRegex(PermissionError, "latest complete runtime upload receipt"):
            self._run(
                release_id="development-runtime-old",
                typed_confirmation=(
                    "START development development-runtime-old "
                    "common-vrp-dev.service,smart-routing-dev.service"
                ),
            )

    def test_string_units_are_rejected_instead_of_becoming_characters(self) -> None:
        with self.assertRaisesRegex(ValueError, "iterable of unit names"):
            self._run(units="common-vrp-dev.service")

    def test_service_control_policy_is_fail_closed(self) -> None:
        self.profile["allow_service_control"] = False
        with self.assertRaisesRegex(PermissionError, "disabled"):
            self._run()

    def test_receipt_for_another_host_cannot_authorize_action(self) -> None:
        self.profile["host"] = "other.example.internal"
        with self.assertRaisesRegex(PermissionError, "for this target"):
            self._run()

    def test_partial_runtime_upload_cannot_authorize_action(self) -> None:
        self.history[0]["complete_manifest"] = False
        self.history[0]["service_eligible"] = False
        with self.assertRaisesRegex(PermissionError, "complete runtime upload"):
            self._run()

    def test_remote_file_drift_blocks_service_start(self) -> None:
        self.history[0]["changes"][0]["sha256"] = "b" * 64
        with self.assertRaisesRegex(RuntimeError, "no longer match"):
            self._run()

    def test_incremental_receipt_rechecks_unselected_verified_file_before_start(self) -> None:
        extra_target = "/home/csda/AI_Routing/development/smart_routing/worker.py"
        self.history[0]["verified_files"] = [
            *self.history[0]["changes"],
            {
                "path": "smart_routing/worker.py",
                "target": extra_target,
                "sha256": "a" * 64,
            },
        ]
        self.remote_checksums = {extra_target: "b" * 64}
        with self.assertRaisesRegex(RuntimeError, "no longer match"):
            self._run()

    def test_new_receipt_without_verified_files_cannot_use_legacy_fallback(self) -> None:
        self.history[0]["selected_full_manifest"] = False
        with self.assertRaisesRegex(PermissionError, "no verifiable remote files"):
            self._run()


class DeploymentConsoleRollbackTargetTests(unittest.TestCase):
    def test_rollback_receipt_for_another_target_is_rejected(self) -> None:
        profile = {
            "host": "server-b.example.internal",
            "port": 22,
            "username": "deploy-user",
            "remote_root": "/home/csda/AI_Routing",
            "allow_upload": True,
        }
        history = [
            {
                "id": "release-a",
                "environment": "development",
                "kind": "runtime",
                "status": "uploaded",
                "target_id": "0" * 64,
                "changes": [
                    {
                        "target": "/home/csda/AI_Routing/development/app.py",
                        "backup": "/home/csda/AI_Routing/.deployment_backups/release-a/app.py",
                    }
                ],
            }
        ]
        with (
            mock.patch.object(console_backend, "_load_history", return_value=history),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=profile),
        ):
            with self.assertRaisesRegex(PermissionError, "selected remote target"):
                console_backend.rollback_release(
                    environment="development",
                    kind="runtime",
                    release_id="release-a",
                    config_path="config/server_deploy.local.json",
                    typed_confirmation="ROLLBACK development release-a",
                )


class _RollbackRemote:
    def __init__(self, checksums: dict[str, str]) -> None:
        self.checksums = checksums

    def __enter__(self) -> "_RollbackRemote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    @contextlib.contextmanager
    def deployment_lock(self, _base: str, _deployment_id: str):
        yield

    def sha256(self, path: str) -> str | None:
        return self.checksums.get(path)

    def copy(self, source: str, target: str) -> None:
        self.checksums[target] = self.checksums[source]


class DeploymentConsoleRollbackIntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.profile = {
            "host": "server.example.internal",
            "port": 22,
            "username": "deploy-user",
            "remote_root": "/home/csda/AI_Routing",
            "allow_upload": True,
        }
        self.release_id = "development-runtime-v1-release"
        self.target = "/home/csda/AI_Routing/development/app.py"
        self.backup = (
            f"/home/csda/AI_Routing/.deployment_backups/{self.release_id}/app.py"
        )
        self.entry = {
            "id": self.release_id,
            "version": "v1",
            "environment": "development",
            "kind": "runtime",
            "status": "uploaded",
            "target_id": console_backend._target_id(self.profile, "development"),
            "changes": [
                {
                    "path": "app.py",
                    "target": self.target,
                    "sha256": "a" * 64,
                    "backup": self.backup,
                    "backup_sha256": "b" * 64,
                    "created": False,
                }
            ],
        }

    def _rollback(self, history: list[dict[str, object]], remote: _RollbackRemote):
        with (
            mock.patch.object(console_backend, "_load_history", return_value=history),
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
            mock.patch.object(console_backend, "_save_history"),
        ):
            return console_backend.rollback_release(
                environment="development",
                kind="runtime",
                release_id=self.release_id,
                config_path="config/server_deploy.local.json",
                typed_confirmation=f"ROLLBACK development {self.release_id}",
            )

    def test_latest_release_rollback_verifies_and_restores_checksums(self) -> None:
        remote = _RollbackRemote({self.target: "a" * 64, self.backup: "b" * 64})
        result = self._rollback([self.entry], remote)
        self.assertEqual(result["status"], "rolled_back")
        self.assertEqual(remote.sha256(self.target), "b" * 64)
        self.assertEqual(self.entry["status"], "rolled_back")

    def test_remote_drift_blocks_rollback(self) -> None:
        remote = _RollbackRemote({self.target: "c" * 64, self.backup: "b" * 64})
        with self.assertRaisesRegex(RuntimeError, "no longer match"):
            self._rollback([self.entry], remote)

    def test_stale_release_cannot_overwrite_newer_release(self) -> None:
        newer = {
            **self.entry,
            "id": "development-runtime-v2-release",
            "version": "v2",
            "changes": [],
        }
        remote = _RollbackRemote({self.target: "a" * 64, self.backup: "b" * 64})
        with self.assertRaisesRegex(PermissionError, "latest release"):
            self._rollback([self.entry, newer], remote)


if __name__ == "__main__":
    unittest.main()
