from __future__ import annotations

import contextlib
import hashlib
import unittest
import uuid
from unittest import mock

from services.deploy import console_backend


class _Remote:
    def __init__(self, source_path: str, version: str) -> None:
        self.source_path = source_path
        self.version = version

    def __enter__(self) -> "_Remote":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def exists(self, path: str) -> bool:
        return path == self.source_path

    def sha256(self, path: str) -> str | None:
        return self.version if path == self.source_path else None

    def mode(self, path: str) -> int | None:
        return 0o600 if path == self.source_path else None


class TechnicianProfileConsoleBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.version = hashlib.sha256(b"immutable technician workbook").hexdigest()
        self.profile = {
            "host": "deploy.example.test",
            "port": 22,
            "username": "operator",
            "password": "not-used",
            "remote_root": "/home/csda/AI_Routing",
            "admin_tools_release_version": "admin-clean-v1",
            "admin_tools_development_release_version": "admin-dev-v1",
        }
        self.source_path = (
            "/home/csda/AI_Routing/state/development/managed_data/"
            f"technician_data_workbook/{self.version}/payload.xlsx"
        )
        self.remote = _Remote(self.source_path, self.version)
        with console_backend._TECHNICIAN_PROFILE_PREVIEW_LOCK:
            console_backend._TECHNICIAN_PROFILE_PREVIEWS.clear()
        with console_backend._REGION_PLAN_FINAL_BINDING_LOCK:
            console_backend._REGION_PLAN_FINAL_BINDINGS.clear()

    @contextlib.contextmanager
    def _bridge(self, run_result: dict[str, object]):
        with (
            mock.patch.object(
                console_backend,
                "_master_admin_profile",
                return_value=self.profile,
            ),
            mock.patch.object(
                console_backend,
                "_remote_session_factory",
                return_value=self.remote,
            ),
            mock.patch.object(
                console_backend,
                "_load_managed_data_version",
                return_value=({"payload_name": "payload.xlsx"}, b"workbook"),
            ),
            mock.patch.object(
                console_backend,
                "_managed_data_validation",
                return_value=(
                    {"db_sync_supported": True},
                    "payload.xlsx",
                    {},
                ),
            ),
            mock.patch.object(
                console_backend,
                "_run_technician_profile_admin_command",
                return_value=run_result,
            ) as run,
        ):
            yield run

    def _preview_payload(self, preview_id: str, digest: str) -> dict[str, object]:
        return {
            "status": "ready",
            "preview_id": preview_id,
            "preview_digest": digest,
            "_private_confirmation_token": "remote-one-time-confirmation",
            "technician_create_count": 2,
            "capability_create_count": 3,
            "capability_delete_count": 1,
            "region_mapping_create_count": 4,
            "plan_id": "atlanta_6area_v2_" + "b" * 64,
            "region_mapping_source": "active_region_data",
            "rejected_count": 0,
            "errors": [],
        }

    def test_preview_and_apply_are_bound_to_one_immutable_development_workbook(self) -> None:
        preview_id = "preview-1"
        digest = "a" * 64
        with self._bridge(self._preview_payload(preview_id, digest)) as preview_run:
            preview = console_backend.preview_managed_data_db_sync(
                dataset_id="technician_data_workbook",
                version=self.version,
                target_environment="development",
            )

        self.assertEqual(preview["dataset_id"], "technician_data_workbook")
        self.assertEqual(preview["version"], self.version)
        self.assertEqual(preview["capability_delete_count"], 1)
        self.assertTrue(preview["plan_id"].startswith("atlanta_6area_v2_"))
        self.assertNotIn("_private_confirmation_token", preview)
        preview_args = preview_run.call_args.kwargs["arguments"]
        self.assertIn(self.source_path, preview_args)
        self.assertIn("--source-sha256", preview_args)
        self.assertIn("--managed-version", preview_args)
        self.assertIn("--environment", preview_args)

        applied = {
            "status": "applied",
            "preview_id": preview_id,
            "preview_digest": digest,
            "operation_id": str(uuid.uuid4()),
            "technician_update_count": 2,
            "capability_update_count": 3,
            "region_mapping_update_count": 4,
            "rejected_count": 0,
        }
        with self._bridge(applied) as apply_run:
            result = console_backend.apply_managed_data_db_sync(
                preview_id=preview_id,
                preview_digest=digest,
                idempotency_key=str(uuid.uuid4()),
                target_environment="development",
                confirm=True,
            )

        self.assertEqual(result["status"], "applied")
        self.assertEqual(result["version"], self.version)
        apply_args = apply_run.call_args.kwargs["arguments"]
        self.assertNotIn(self.source_path, apply_args)
        self.assertIn("--preview-id", apply_args)
        self.assertIn("--idempotency-key", apply_args)
        self.assertIn("--confirmation", apply_args)

    def test_preview_cannot_be_reused_with_another_idempotency_key(self) -> None:
        preview_id = "preview-2"
        digest = "b" * 64
        with self._bridge(self._preview_payload(preview_id, digest)):
            console_backend.preview_managed_technician_profile_sync(
                version=self.version, target_environment="development"
            )
        applied = {
            "status": "already_applied",
            "preview_id": preview_id,
            "preview_digest": digest,
        }
        first_key = str(uuid.uuid4())
        with self._bridge(applied):
            console_backend.apply_managed_technician_profile_sync(
                preview_id=preview_id,
                preview_digest=digest,
                idempotency_key=first_key,
                target_environment="development",
                confirm=True,
            )
        with self.assertRaisesRegex(PermissionError, "another idempotency key"):
            console_backend.apply_managed_technician_profile_sync(
                preview_id=preview_id,
                preview_digest=digest,
                idempotency_key=str(uuid.uuid4()),
                target_environment="development",
                confirm=True,
            )

    def test_production_is_rejected_before_profile_or_remote_access(self) -> None:
        with mock.patch.object(console_backend, "_master_admin_profile") as profile, mock.patch.object(
            console_backend, "_remote_session_factory"
        ) as remote:
            with self.assertRaisesRegex(PermissionError, "Production technician-profile"):
                console_backend.preview_managed_technician_profile_sync(
                    version=self.version, target_environment="production"
                )
            with self.assertRaisesRegex(PermissionError, "Production managed-data"):
                console_backend.preview_managed_data_db_sync(
                    dataset_id="technician_data_workbook",
                    version=self.version,
                    target_environment="production",
                )
        profile.assert_not_called()
        remote.assert_not_called()

    def test_preview_command_has_no_table_or_caller_selected_target(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        command = console_backend._technician_profile_preview_command(
            context, source_path=self.source_path, version=self.version
        )
        self.assertIn(" -B -m admin_tools.db.technician_profile_backend", command)
        self.assertIn("--source-sha256", command)
        self.assertIn("--managed-version", command)
        self.assertIn("--environment development", command)
        self.assertNotIn("--table", command)
        self.assertNotIn("--destination", command)
        self.assertNotIn("production", command)

    def test_region_dynamic_plan_id_is_accepted_only_from_a_resolution_binding(self) -> None:
        context = console_backend._master_admin_context(self.profile, "development")
        resolution_digest = "c" * 64
        plan_id = f"atlanta_6area_v2_{resolution_digest}"
        result = console_backend._safe_region_plan_workflow_result(
            {
                "status": "candidate_imported",
                "plan_id": plan_id,
                "lifecycle_stage": "candidate_resolved",
                "revision": 2,
                "checksum": "d" * 64,
                "resolution_digest": resolution_digest,
            },
            command="resolve",
            expected_plan_id=None,
        )
        console_backend._remember_region_plan_final_binding(
            result=result, source_version=self.version, context=context
        )
        self.assertEqual(
            console_backend._bound_region_plan_id(
                context=context, resolution_digest=resolution_digest
            ),
            plan_id,
        )
        with self.assertRaisesRegex(ValueError, "allowlisted"):
            console_backend._require_region_plan_final_id("attacker_selected_plan")


if __name__ == "__main__":
    unittest.main()
