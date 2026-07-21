from __future__ import annotations

import unittest
from unittest import mock

from services.deploy import console_backend


class _MonitorRemote:
    def __init__(self, failed_port: int | None = None) -> None:
        self.failed_port = failed_port
        self.commands: list[str] = []

    def __enter__(self) -> "_MonitorRemote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, command: str, timeout: int = 45) -> tuple[int, str, str]:
        self.commands.append(command)
        if command.startswith("systemctl is-active "):
            return 0, "active\n", ""
        if command.startswith("systemctl is-enabled "):
            return 0, "enabled\n", ""
        if command.startswith("journalctl "):
            return 0, "normal journal", ""
        if command.startswith("curl "):
            if self.failed_port is not None and f":{self.failed_port}/" in command:
                return 22, "", "unavailable"
            return 0, "", ""
        raise AssertionError(f"Unexpected monitor command: {command}")


class DeploymentConsoleMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.profile = {
            "host": "server.example.internal",
            "port": 22,
            "username": "deployer",
            "remote_root": "/home/csda/AI_Routing",
            "allow_upload": False,
            "allow_service_control": False,
        }

    def _observe(self, remote: _MonitorRemote) -> dict[str, object]:
        with (
            mock.patch.object(console_backend, "_load_remote_profile", return_value=self.profile),
            mock.patch.object(console_backend, "_remote_session_factory", return_value=remote),
        ):
            return console_backend.observe_platform(
                config_path="config/server_deploy.local.json"
            )

    def test_snapshot_contains_both_environments_and_shared_osrm(self) -> None:
        remote = _MonitorRemote()
        report = self._observe(remote)
        rows = report["services"]
        self.assertEqual(report["total"], 9)
        self.assertEqual(report["healthy"], 9)
        self.assertEqual({row["scope"] for row in rows}, {"production", "development", "shared"})
        self.assertEqual(
            {row["port"] for row in rows if row["component_type"] == "osrm"},
            {5000, 5001, 5002},
        )
        self.assertEqual(
            {
                row["health_endpoint"]
                for row in rows
                if row["component_type"] == "osrm"
            },
            {
                spec[2]
                for spec in console_backend.OSRM_MONITOR_SPECS
            },
        )
        self.assertTrue(
            all(str(row["health_endpoint"]).startswith("http://127.0.0.1:") for row in rows)
        )
        self.assertFalse(any("sudo" in command for command in remote.commands))
        self.assertFalse(any(" start " in command or " restart " in command for command in remote.commands))

    def test_failed_endpoint_is_reported_even_when_systemd_is_active(self) -> None:
        report = self._observe(_MonitorRemote(failed_port=5001))
        failed = [row for row in report["services"] if row["port"] == 5001][0]
        self.assertTrue(failed["active"])
        self.assertFalse(failed["health_ok"])
        self.assertEqual(failed["status"], "unhealthy")
        self.assertEqual(report["healthy"], 8)

    def test_backend_redacts_json_and_database_uri_credentials(self) -> None:
        source = (
            '{"password":"json-secret"} '
            "postgresql://dbuser:db-secret@localhost/vrp Bearer auth-secret"
        )
        redacted = console_backend._redact(source)
        self.assertNotIn("json-secret", redacted)
        self.assertNotIn("db-secret", redacted)
        self.assertNotIn("auth-secret", redacted)


if __name__ == "__main__":
    unittest.main()
