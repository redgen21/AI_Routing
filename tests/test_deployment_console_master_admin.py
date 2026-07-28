"""Platform contract tests for the SSH-only master-data console bridge."""

from __future__ import annotations

import contextlib
import hashlib
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


class _RegionPlanRemote:
    """In-memory remote for the fixed region-plan Admin Tools bridge."""

    def __init__(self, payloads: dict[str, dict[str, object]]) -> None:
        self.payloads = payloads
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
        self.commands: list[str] = []
        self.uploads: list[str] = []
        self.locks: list[tuple[str, str]] = []

    def __enter__(self) -> "_RegionPlanRemote":
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

    def upload_bytes_atomic(self, payload: bytes, target: str, backup: object = None) -> None:
        if backup is not None:
            raise AssertionError("region-plan requests are immutable")
        self.files[target] = payload
        self.modes[target] = 0o600
        self.uploads.append(target)

    def execute_region_plan_json(self, command: str, timeout: int = 45):
        self.commands.append(command)
        for operation, payload in self.payloads.items():
            if f"--json {operation} " in command:
                return 0, json.dumps(payload), ""
        raise AssertionError(f"unexpected command: {command}")


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


def _region_payload(operation: str, **overrides: object) -> dict[str, object]:
    version = "a" * 64
    migration = console_backend._region_plan_migration_spec(
        "V001__atlanta_6area_region_plan"
    )
    base: dict[str, object] = {
        "environment": "development",
        "plan_id": f"atlanta_6area_v2_{version}",
    }
    if operation in {"migration-preview", "install-schema"}:
        base.update(
            {
                "contract_version": "region-plan-migration/v1",
                "status": "ready" if operation == "migration-preview" else "applied",
                "migration_id": "V001__atlanta_6area_region_plan",
                "checksum_sha256": migration.checksum_sha256,
                "statement_count": 7,
            }
        )
        if operation == "migration-preview":
            base.update(
                {
                    "required_confirmation": "APPLY V001__atlanta_6area_region_plan TO DEVELOPMENT vrp_db_dev",
                    "statement_types": ["CREATE TABLE", "CREATE INDEX"],
                    "rollback_instructions": "Disable feature and retain audit tables.",
                }
            )
    else:
        base["contract_version"] = "region-plan-workflow/v1"
        if operation == "resolve":
            base.update(
                {
                    "status": "candidate_imported",
                    "revision": 2,
                    "checksum": version,
                    "resolution_digest": version,
                    "lifecycle_stage": "candidate_resolved",
                }
            )
        elif operation == "review":
            base.update({"status": "reviewed", "revision": 3})
        elif operation == "activation-preview":
            base.update(
                {
                    "status": "ready",
                    "preview_id": version,
                    "preview_digest": version,
                    "checksum": "b" * 64,
                    "plan_revision": 3,
                    "expected_activation_revision": 4,
                    "region_count": 6,
                    "postal_count": 297,
                    "technician_count": 14,
                    "boundary_resolution_count": 4,
                }
            )
        elif operation == "activate":
            base.update(
                {
                    "status": "activated",
                    "activation_revision": 5,
                    "preview_digest": version,
                }
            )
    base.update(overrides)
    return base


def _region_schema_payload(status: str = "ready") -> dict[str, object]:
    return {
        "contract_version": "region-plan-schema/v2",
        "status": status,
        "environment": "development",
        "dbname": "vrp_db_dev",
        "target_id": "development:vrp_db_dev",
        "schema_id": "common_region_plan_schema_v2",
        "checksum_sha256": "f" * 64,
        "requires_confirmation": (
            "RECONCILE COMMON REGION PLAN SCHEMA V2 TO DEVELOPMENT vrp_db_dev"
        ),
    }


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

    def _region_call(self, remote: _RegionPlanRemote, callback: object) -> object:
        with mock.patch.object(
            console_backend, "_master_admin_profile", return_value=self.profile
        ), mock.patch.object(
            console_backend, "_remote_session_factory", return_value=remote
        ), mock.patch.object(console_backend, "_verify_region_plan_admin_release"):
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

    def test_region_candidate_command_is_fixed_to_development_admin_tools_bridge(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        version = "a" * 64
        source = (
            "/home/csda/AI_Routing/state/development/managed_data/"
            f"territory_plan_workbook/{version}/payload.xlsx"
        )
        command = console_backend._region_plan_admin_command(
            context, source_path=source, version=version
        )
        self.assertIn(" -B -m admin_tools.db.region_plan_backend", command)
        self.assertIn(" stage-candidate ", command)
        self.assertIn("--source-sha256", command)
        self.assertIn("--managed-version", command)
        self.assertNotIn("--table", command)
        self.assertNotIn("--destination", command)
        self.assertNotIn("--confirm-production", command)

        raw_remote = console_backend.ParamikoRemote(self.profile)
        raw_remote.client = _Client(
            json.dumps(
                {
                    "contract_version": "region-plan/v1",
                    "environment": "development",
                    "source_sha256": version,
                    "managed_version": version,
                    "status": "candidate_staged",
                    "plan_id": "atlanta_6area_new_atl_buckets_20260721_v1",
                    "lifecycle_stage": "candidate_plan",
                    "approval_status": "pending_boundary_resolutions",
                    "promotable": False,
                    "promotion_required": True,
                    "direct_db_upsert": False,
                }
            ).encode()
        )
        _, stdout, _ = raw_remote.execute_region_plan_json(command)
        result = console_backend._parse_region_plan_candidate_json(
            stdout, context=context, version=version
        )
        self.assertEqual(result["plan_id"], "atlanta_6area_new_atl_buckets_20260721_v1")
        self.assertFalse(result["promotable"])
        for malformed in (
            command.replace("stage-candidate", "apply"),
            command.replace("--managed-version", "--destination"),
            command.replace(" -B -m ", " -m "),
            command.replace(source, "/etc/passwd"),
        ):
            with self.subTest(command=malformed):
                with self.assertRaisesRegex(ValueError, "allowlisted|invalid"):
                    raw_remote.execute_region_plan_json(malformed)
        with self.assertRaisesRegex(PermissionError, "development-only"):
            console_backend._region_plan_admin_command(
                console_backend._master_admin_context(self.profile, "production"),
                source_path=source,
                version=version,
            )

    def test_fixed_region_bundle_transport_is_path_free_on_import(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        version = "a" * 64
        bundle_path = (
            "/home/csda/AI_Routing/state/development/managed_data/"
            f"fixed_region_plan_bundle/{version}/payload.zip"
        )
        preview = console_backend._fixed_region_plan_bundle_cli_command(
            context,
            "stage-bundle",
            (
                "--source", bundle_path,
                "--bundle-sha256", version,
                "--managed-version", version,
            ),
        )
        request_path = (
            "/home/csda/AI_Routing/state/development/region_plan_bundle_requests/"
            f"{version}.json"
        )
        apply = console_backend._fixed_region_plan_bundle_cli_command(
            context,
            "import-bundle",
            ("--request", request_path, "--request-sha256", version),
        )
        raw_remote = console_backend.ParamikoRemote(self.profile)
        raw_remote.client = _Client(b"{}")
        raw_remote.execute_region_plan_json(preview)
        raw_remote.execute_region_plan_json(apply)
        self.assertIn(" stage-bundle ", preview)
        self.assertIn(" import-bundle ", apply)
        self.assertNotIn("--source", apply)
        self.assertNotIn("--bundle", apply)
        for malformed in (
            preview.replace(bundle_path, "/tmp/bundle.zip"),
            preview.replace("--managed-version", "--destination"),
            apply.replace(request_path, "/tmp/request.json"),
            apply + " --source /tmp/bundle.zip",
        ):
            with self.subTest(command=malformed):
                with self.assertRaisesRegex(ValueError, "allowlisted|invalid|binding"):
                    raw_remote.execute_region_plan_json(malformed)

    def test_fixed_region_bundle_preview_apply_use_only_pinned_operations(self) -> None:
        version = "b" * 64
        remote = _RegionPlanRemote(
            {
                "stage-bundle": {
                    "contract_version": "region-plan-bundle-import/v1",
                    "environment": "development",
                    "managed_version": version,
                    "bundle_sha256": version,
                    "status": "ready",
                    "write_allowed": False,
                    "target_environment": "development",
                    "lifecycle_stage": "resolved_candidate",
                    "verification_only": True,
                    "promotable": False,
                    "plan_id": f"atlanta_6area_v2_{version}",
                    "region_count": 6,
                },
                "status-bundle": {
                    "contract_version": "region-plan-bundle-import/v1",
                    "environment": "development",
                    "managed_version": version,
                    "bundle_sha256": version,
                    "status": "reviewed",
                    "plan_id": f"atlanta_6area_v2_{version}",
                    "revision": 1,
                    "checksum": version,
                    "lifecycle_stage": "reviewed",
                    "verification_only": True,
                    "promotable": False,
                },
                "import-bundle": {
                    "contract_version": "region-plan-bundle-import/v1",
                    "environment": "development",
                    "managed_version": version,
                    "status": "candidate_imported",
                    "plan_id": f"atlanta_6area_v2_{version}",
                    "revision": 1,
                    "checksum": version,
                    "lifecycle_stage": "candidate",
                    "verification_only": True,
                    "promotable": False,
                },
            }
        )
        with mock.patch.object(
            console_backend, "_master_admin_profile", return_value=self.profile
        ), mock.patch.object(
            console_backend, "_remote_session_factory", return_value=remote
        ), mock.patch.object(
            console_backend,
            "_load_managed_data_version",
            return_value=({"payload_name": "payload.zip"}, b"opaque-zip"),
        ), mock.patch.object(console_backend, "_verify_fixed_region_plan_bundle_admin_release"):
            preview = console_backend.preview_fixed_region_plan_bundle_import(
                environment="development", version=version
            )
            status = console_backend.get_fixed_region_plan_bundle_status(
                environment="development", version=version
            )
            applied = console_backend.apply_fixed_region_plan_bundle_import(
                environment="development",
                version=version,
                imported_by="operator-1",
                idempotency_key=str(uuid.uuid4()),
                confirm=True,
            )
        self.assertEqual(preview["status"], "ready")
        self.assertEqual(status["status"], "reviewed")
        self.assertEqual(status["resolution_digest"], version)
        self.assertEqual(applied["status"], "candidate_imported")
        self.assertEqual(len(remote.uploads), 1)
        self.assertIn("/region_plan_bundle_requests/", remote.uploads[0])
        commands = "\n".join(remote.commands)
        self.assertIn("--json stage-bundle", commands)
        self.assertIn("--json status-bundle", commands)
        self.assertIn("--json import-bundle", commands)
        self.assertNotIn("--source", commands.split("--json import-bundle", 1)[1])
        self.assertNotIn("/home/", json.dumps({"preview": preview, "applied": applied}))

    def test_region_workflow_is_fixed_request_bound_and_development_only(self) -> None:
        with console_backend._REGION_PLAN_SCHEMA_CONFIRMATION_LOCK:
            console_backend._REGION_PLAN_SCHEMA_CONFIRMATIONS.clear()
        with console_backend._REGION_PLAN_ACTIVATION_PREVIEW_LOCK:
            console_backend._REGION_PLAN_ACTIVATION_PREVIEWS.clear()
        remote = _RegionPlanRemote(
            {
                "preview": _region_schema_payload(),
                "reconcile": _region_schema_payload("reconciled"),
                "resolve": _region_payload("resolve"),
                "review": _region_payload("review"),
                "activation-preview": _region_payload("activation-preview"),
                "activate": _region_payload("activate"),
            }
        )
        version = "c" * 64
        key = str(uuid.uuid4())
        resolutions = {
            postal: {"primary_region": "Zone 2", "allow_overflow": False, "rationale": "approved"}
            for postal in ("30028", "30040", "30041", "30107")
        }
        with mock.patch.object(console_backend, "_load_managed_data_version", return_value=({}, b"source")):
            schema = self._region_call(
                remote, lambda: console_backend.preview_region_plan_schema(environment="development")
            )
            self.assertNotIn("required_confirmation", schema)
            self.assertEqual(
                self._region_call(
                    remote,
                    lambda: console_backend.install_region_plan_schema(
                        environment="development", confirm=True
                    ),
                )["status"],
                "reconciled",
            )
            preview = self._region_call(
                remote,
                lambda: console_backend.preview_region_plan_resolutions(
                    environment="development", source_version=version,
                    boundary_resolutions=resolutions, imported_by="operator-1", idempotency_key=key,
                ),
            )
            resolved = self._region_call(
                remote,
                lambda: console_backend.apply_region_plan_resolutions(
                    environment="development", source_version=version,
                    boundary_resolutions=resolutions, imported_by="operator-1", idempotency_key=key,
                    expected_request_sha256=preview["request_sha256"], confirm=True,
                ),
            )
            self.assertEqual(resolved["status"], "candidate_imported")
            reviewed = self._region_call(
                remote,
                lambda: console_backend.review_region_plan(
                    environment="development", expected_revision=2, reviewed_by="reviewer-1",
                    review_reference="review-1",
                    resolution_digest=resolved["resolution_digest"], confirm=True,
                ),
            )
            self.assertEqual(reviewed["revision"], 3)
            activation = self._region_call(
                remote, lambda: console_backend.preview_region_plan_activation(
                    environment="development",
                    resolution_digest=resolved["resolution_digest"],
                )
            )
            activated = self._region_call(
                remote,
                lambda: console_backend.apply_region_plan_activation(
                    environment="development", preview_id=activation["preview_id"],
                    preview_digest=activation["preview_digest"], activated_by="operator-1",
                    activation_reference="activate-1", idempotency_key=str(uuid.uuid4()), confirm=True,
                ),
            )
        self.assertEqual(activated["activation_revision"], 5)
        self.assertTrue(any("/region_plan_requests/" in path for path in remote.uploads))
        self.assertTrue(all(remote.modes[path] == 0o600 for path in remote.uploads))
        commands = "\n".join(remote.commands)
        for operation in ("preview", "reconcile", "resolve", "review", "activation-preview", "activate"):
            self.assertIn(f"--json {operation}", commands)
        self.assertNotIn("--table", commands)
        self.assertNotIn("--destination", commands)

    def test_region_schema_previews_single_common_v2_reconciler(self) -> None:
        remote = _RegionPlanRemote({"preview": _region_schema_payload()})
        result = self._region_call(
            remote,
            lambda: console_backend.preview_region_plan_schema(environment="development"),
        )
        self.assertEqual(result["schema_id"], "common_region_plan_schema_v2")
        self.assertIn("admin_tools.db.region_plan_schema_backend", remote.commands[0])
        self.assertNotIn("--migration-id", remote.commands[0])

    def test_region_command_verifier_allows_only_fixed_schema_and_request_shapes(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        remote = console_backend.ParamikoRemote(self.profile)
        remote.client = _Client(json.dumps(_region_schema_payload()).encode())
        schema_preview = console_backend._region_plan_schema_cli_command(context, "preview")
        self.assertIn("--json preview --config", schema_preview)
        remote.execute_region_plan_json(schema_preview)

        digest = "e" * 64
        request_path = (
            "/home/csda/AI_Routing/state/development/region_plan_requests/"
            f"{digest}.json"
        )
        workflow = console_backend._region_plan_cli_command(
            context, "review", ("--request", request_path, "--request-sha256", digest)
        )
        remote.execute_region_plan_json(workflow)
        for malformed in (
            workflow.replace(" review ", " arbitrary "),
            workflow.replace(request_path, "/tmp/request.json"),
            schema_preview + " --sql 'drop table'",
        ):
            with self.subTest(command=malformed):
                with self.assertRaisesRegex(ValueError, "allowlisted|invalid|arguments"):
                    remote.execute_region_plan_json(malformed)

    def test_region_operations_reject_production_before_profile_or_remote(self) -> None:
        with mock.patch.object(console_backend, "_master_admin_profile") as profile, mock.patch.object(
            console_backend, "_remote_session_factory"
        ) as remote:
            with self.assertRaisesRegex(PermissionError, "development-only"):
                console_backend.preview_region_plan_schema(environment="production")
            with self.assertRaisesRegex(PermissionError, "development-only"):
                console_backend.apply_region_plan_activation(
                    environment="production", preview_id="a" * 64, preview_digest="a" * 64,
                    activated_by="operator", activation_reference="ref", idempotency_key=str(uuid.uuid4()), confirm=True,
                )
            with self.assertRaisesRegex(PermissionError, "development-only"):
                console_backend.apply_region_plan_resolutions(
                    environment="production", source_version="a" * 64, boundary_resolutions={},
                    imported_by="operator", idempotency_key=str(uuid.uuid4()), confirm=True,
                )
        profile.assert_not_called()
        remote.assert_not_called()

    def test_region_artifact_download_returns_bytes_only_on_explicit_bound_request(self) -> None:
        from tools.data.atlanta_6area_plan import (
            BOUNDARY_POLICY_FILENAME,
            FIXED_REGION_FILENAME,
            MANIFEST_FILENAME,
            TECHNICIAN_POLICY_FILENAME,
        )

        with console_backend._REGION_PLAN_RESOLUTION_DOWNLOAD_LOCK:
            console_backend._REGION_PLAN_RESOLUTION_DOWNLOADS.clear()
        version = "d" * 64
        resolutions = {
            postal: {"primary_region": "Zone 2", "allow_overflow": False, "rationale": "reviewed"}
            for postal in ("30028", "30040", "30041", "30107")
        }
        artifacts = {
            FIXED_REGION_FILENAME: b"POSTAL_CODE\n30028\n",
            BOUNDARY_POLICY_FILENAME: b"POSTAL_CODE\n30028\n",
            TECHNICIAN_POLICY_FILENAME: b"SVC_ENGINEER_CODE\nAI100001\n",
            MANIFEST_FILENAME: b"{}\n",
        }
        bundle = type("Bundle", (), {"artifacts": artifacts})()
        with mock.patch.object(console_backend, "_master_admin_profile", return_value=self.profile), mock.patch.object(
            console_backend, "_load_managed_data_version", return_value=({}, b"immutable-source")
        ), mock.patch(
            "tools.data.atlanta_6area_plan.build_atlanta_6area_bundle", return_value=bundle
        ):
            preview = console_backend.preview_region_plan_resolutions(
                environment="development", source_version=version,
                boundary_resolutions=resolutions, imported_by="operator-1", idempotency_key=str(uuid.uuid4()),
            )
            self.assertNotIn("content", json.dumps(preview))
            result = console_backend.download_region_plan_resolution_artifact(
                environment="development", resolution_digest=preview["resolution_digest"],
                artifact_id="technician_policy_csv",
            )
        self.assertEqual(result["content"], b"SVC_ENGINEER_CODE\nAI100001\n")
        public = {key: value for key, value in result.items() if key != "content"}
        self.assertNotIn("SVC_ENGINEER_NAME", json.dumps(public))
        self.assertEqual(result["content_type"], "text/csv")

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
