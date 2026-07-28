from __future__ import annotations

import importlib
import hashlib
import json
import unittest
import uuid
from pathlib import Path

from streamlit.testing.v1 import AppTest

from deployment_console_ui.backend_adapter import BackendAdapter, BackendCapabilityError
from deployment_console_ui.app import (
    DB_CONFIG_PATHS,
    NAVIGATION,
    PROJECT_ROOT,
    _admin_tools_pin_message,
    _changed_upload_files,
    _diff_panels,
    _load_remote_diff,
    _managed_data_preview_payload,
    _managed_dataset_section,
    _managed_dataset_ui_metadata,
    _project_relative_path,
    _result_message,
    _safe_backend_error,
    _upload_intent,
)
from deployment_console_ui.helpers import (
    classify_sql,
    confirmation_matches,
    expected_confirmation,
    public_mapping,
    redact_text,
    safe_manifest_files,
)


class DeploymentConsoleHelpersTest(unittest.TestCase):
    def test_technician_preview_keeps_safe_delete_and_region_binding_fields(self) -> None:
        preview = _managed_data_preview_payload(
            {
                "status": "ready",
                "capability_delete_count": 3,
                "plan_id": "atlanta_6area_v1_" + "a" * 64,
                "region_mapping_source": "active_region_data",
                "home_address": "must-not-leak",
            }
        )
        self.assertEqual(preview["capability_delete_count"], 3)
        self.assertTrue(preview["plan_id"].startswith("atlanta_6area_v1_"))
        self.assertEqual(preview["region_mapping_source"], "active_region_data")
        self.assertNotIn("home_address", preview)

    def test_managed_data_presentation_hides_runtime_inputs_and_marks_derived(self) -> None:
        self.assertTrue(
            _managed_dataset_ui_metadata({"dataset_id": "service_raw"})["ui_hidden"]
        )
        profile = _managed_dataset_ui_metadata({"dataset_id": "profile_raw"})
        self.assertEqual(profile["label"], "Technician profile workbook (source)")
        territory = _managed_dataset_ui_metadata({"dataset_id": "territory_plan_workbook"})
        self.assertTrue(territory["ui_hidden"])
        bundle = _managed_dataset_ui_metadata({"dataset_id": "fixed_region_plan_bundle"})
        self.assertIn("DB-input bundle", bundle["label"])
        derived = _managed_dataset_ui_metadata({"dataset_id": "atlanta_engineer_home"})
        self.assertEqual(derived["ui_role"], "derived_projection")
        self.assertFalse(derived["ui_upload_allowed"])
        self.assertEqual(_managed_dataset_section(profile), "technician")
        self.assertEqual(_managed_dataset_section(bundle), "region")
        self.assertEqual(_managed_dataset_section({"dataset_id": "heavy_repair_rules"}), "other")

    def test_admin_tools_pin_messages_are_safe_and_actionable(self) -> None:
        self.assertEqual(
            _admin_tools_pin_message("pinned", "admin-v1"),
            (
                "success",
                "Clean Admin Tools execution version is set (admin-v1) for Production and Development.",
            ),
        )
        policy = _admin_tools_pin_message("not_pinned_policy", "password=hidden")
        self.assertIsNotNone(policy)
        self.assertEqual(policy[0], "warning")
        self.assertIn("clean, promotable build", policy[1])
        self.assertNotIn("password=hidden", policy[1])
        failure = _admin_tools_pin_message("pin_failed", "token=hidden")
        self.assertIsNotNone(failure)
        self.assertEqual(failure[0], "warning")
        self.assertIn("saving the local execution-version pin failed", failure[1])
        self.assertNotIn("token=hidden", failure[1])
        development = _admin_tools_pin_message(
            "pinned_development_verification", "admin-dirty-v2"
        )
        self.assertEqual(development[0], "success")
        self.assertIn("Development DB verification", development[1])

    def test_office_navigation_exposes_all_operational_routes(self) -> None:
        self.assertEqual(
            {key for key, _, _ in NAVIGATION},
            {
                "dashboard", "monitoring", "package-development", "package-production",
                "package-admin-tools", "data", "region-plans", "settings",
            },
        )

    def test_build_output_paths_are_project_relative_for_display(self) -> None:
        staging = PROJECT_ROOT / "deployment" / "development" / "v1"
        archive = PROJECT_ROOT / "deployment" / "development" / "runtime-v1.zip"
        self.assertEqual(
            Path(_project_relative_path(staging)),
            Path("deployment/development/v1"),
        )
        self.assertEqual(
            Path(_project_relative_path(archive)),
            Path("deployment/development/runtime-v1.zip"),
        )

    def test_backend_error_is_double_redacted_and_keeps_safe_lock_reason(self) -> None:
        message = _safe_backend_error(
            RuntimeError(
                "Another deployment holds the remote lock; "
                "password=do-not-show token=abc123"
            )
        )
        self.assertIn("Another deployment holds the remote lock", message)
        self.assertNotIn("do-not-show", message)
        self.assertNotIn("abc123", message)

    def test_upload_receipt_surfaces_release_id_without_secret_fields(self) -> None:
        message = _result_message(
            {
                "status": "uploaded",
                "release_id": "development-runtime-v1-abc123",
                "password": "do-not-show",
            }
        )
        self.assertIn("development-runtime-v1-abc123", message)
        self.assertNotIn("do-not-show", message)

    def test_manifest_file_paths_are_allowlisted_and_normalized(self) -> None:
        manifest = {
            "files": [
                {"path": "smart_routing/api.py"},
                {"path": "smart_routing\\db.py"},
                {"path": "../secret.json"},
                {"path": "/etc/passwd"},
                {"path": "smart_routing/api.py"},
            ]
        }
        self.assertEqual(
            safe_manifest_files(manifest),
            ["smart_routing/api.py", "smart_routing/db.py"],
        )

    def test_confirmation_is_exact_and_case_sensitive(self) -> None:
        expected = expected_confirmation("deploy", "production", "2026.07.19")
        self.assertEqual(expected, "DEPLOY production 2026.07.19")
        self.assertTrue(confirmation_matches(expected, expected))
        self.assertFalse(confirmation_matches(expected + " ", expected))
        self.assertFalse(confirmation_matches(expected.lower(), expected))

    def test_sql_classification(self) -> None:
        result = classify_sql(
            "-- change\nCREATE TABLE t(id int); INSERT INTO t VALUES (1); SELECT * FROM t;"
        )
        self.assertEqual([item["category"] for item in result], ["DDL", "DML", "READ"])

    def test_sensitive_values_are_redacted(self) -> None:
        message = (
            'password="do-not-show" token=abc123 '
            'sftp://user:pass@example Bearer abc.def '
            '{"password":"json-secret"} '
            "postgresql://dbuser:db-secret@localhost/vrp"
        )
        redacted = redact_text(message)
        self.assertNotIn("do-not-show", redacted)
        self.assertNotIn("abc123", redacted)
        self.assertNotIn("user:pass", redacted)
        self.assertNotIn("abc.def", redacted)
        self.assertNotIn("json-secret", redacted)
        self.assertNotIn("db-secret", redacted)

    def test_public_mapping_does_not_expose_secret_fields(self) -> None:
        source = {"status": "ok", "password": "hidden", "token": "hidden"}
        self.assertEqual(public_mapping(source, ("status",)), {"status": "ok"})


class BackendAdapterTest(unittest.TestCase):
    def test_settings_route_masks_backend_secrets(self) -> None:
        class FakeBackend:
            @staticmethod
            def get_connection_settings(**_: object):
                return {
                    "status": "configured",
                    "connection": {
                        "host": "sftp.internal",
                        "port": 22,
                        "username": "operator",
                        "remote_root": "/srv/routing",
                        "admin_tools_release_version": "admin-v1",
                        "admin_tools_release_configured": True,
                        "admin_tools_development_release_version": "admin-dirty-v2",
                        "admin_tools_development_release_configured": True,
                        "admin_tools_development_release_mode": "development-verification",
                        "password": "do-not-render-this",
                        "token": "also-hidden",
                    },
                    "environments": {},
                }

        def app(injected_backend: object) -> None:
            from deployment_console_ui.app import render_app as render

            render(injected_backend)

        rendered = AppTest.from_function(app, args=(FakeBackend(),)).run(timeout=10)
        settings = next(
            item for item in rendered.button if item.label.endswith("Connection settings")
        ).click().run(timeout=10)
        visible = " ".join(
            str(item.value)
            for collection in (settings.markdown, settings.caption, settings.text_input)
            for item in collection
        )
        self.assertTrue(
            any("Production Admin Tools execution version" in item.value for item in settings.info)
        )
        self.assertTrue(
            any("admin-dirty-v2 (development-verification)" in item.value for item in settings.info)
        )
        self.assertTrue(
            any("cannot be used for Production" in item.value for item in settings.warning)
        )
        self.assertNotIn("do-not-render-this", visible)
        self.assertNotIn("also-hidden", visible)

    def test_runtime_always_uses_latest_artifact_after_build(self) -> None:
        class FakeBackend:
            built = False
            built_version = ""
            build_calls = 0
            list_calls = 0

            @staticmethod
            def preview_runtime_build(*, environment: str, version: str):
                output_exists = FakeBackend.built_version == version
                return {
                    "environment": environment,
                    "version": version,
                    "source_revision": "a" * 40,
                    "source_dirty": False,
                    "source_change_count": 0,
                    "build_allowed": not output_exists,
                    "staging_path": f"deployment/{environment}/{version}",
                    "archive_path": f"deployment/{environment}/runtime-{version}.zip",
                    "output_exists": output_exists,
                }

            @classmethod
            def build_runtime_artifact(
                cls, *, environment: str, version: str, allow_dirty_source: bool
            ):
                cls.build_calls += 1
                cls.built = True
                cls.built_version = version
                return {
                    "status": "built",
                    "environment": environment,
                    "version": version,
                    "source_revision": "a" * 40,
                    "source_dirty": False,
                    "source_mode": "worktree",
                    "staging_path": f"deployment/{environment}/{version}",
                    "archive_path": f"deployment/{environment}/runtime-{version}.zip",
                }

            @classmethod
            def list_artifacts(cls, *, environment: str, kind: str):
                cls.list_calls += 1
                artifacts = [
                    {
                        "id": "old-v1",
                        "version": "old-v1",
                        "label": "old-v1",
                        "path": f"deployment/{environment}/old-v1",
                    }
                ]
                if cls.built:
                    artifacts.insert(
                        0,
                        {
                            "id": cls.built_version,
                            "version": cls.built_version,
                            "label": cls.built_version,
                            "path": f"deployment/{environment}/{cls.built_version}",
                        }
                    )
                return artifacts

            @classmethod
            def resolve_latest_runtime_artifact(cls, *, environment: str):
                version = cls.built_version if cls.built else "old-v1"
                return {
                    "id": version,
                    "version": version,
                    "label": version,
                    "path": f"deployment/{environment}/{version}",
                }

            @staticmethod
            def inspect_artifact(**_: object):
                return {
                    "archive_sha256": "a" * 64,
                    "target_upload_path": "/home/csda/AI_Routing/development",
                    "restricted_data": False,
                    "required_confirmation": "DEPLOY development new-v1",
                    "manifest": {
                        "artifact_type": "server-runtime",
                        "source_dirty": False,
                        "source_mode": "worktree",
                        "promotable": False,
                        "files": [{"path": "app.py", "sha256": "b" * 64}],
                    },
                }

            @staticmethod
            def deployment_policy(**_: object):
                return {"allow_upload": False, "target_id": "c" * 64}

            @staticmethod
            def preview_remote_diff(**_: object):
                return []

        def artifact_panel(backend: object) -> None:
            from deployment_console_ui.app import _render_artifact_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_artifact_tab(
                Adapter(backend), "development", "config/server_deploy.local.json"
            )

        backend = FakeBackend()
        rendered = AppTest.from_function(artifact_panel, args=(backend,)).run(timeout=10)
        build_version = next(
            item for item in rendered.text_input if item.label == "Build version"
        ).value
        selector = next(
            item for item in rendered.selectbox if item.label == "Artifact version"
        )
        self.assertEqual(selector.value, "old-v1")
        self.assertTrue(
            any("Newest runtime artifact: old-v1" in item.value for item in rendered.caption)
        )

        completed = next(
            item for item in rendered.button if item.label == "Build runtime artifact"
        ).click().run(timeout=10)

        self.assertEqual(FakeBackend.build_calls, 1)
        self.assertGreaterEqual(FakeBackend.list_calls, 2)
        artifact_type = next(
            item for item in completed.selectbox if item.label == "Artifact type"
        )
        self.assertEqual(artifact_type.value, "runtime")
        version_selector = next(
            item for item in completed.selectbox if item.label == "Artifact version"
        )
        self.assertEqual(version_selector.value, build_version)
        self.assertTrue(
            any(
                f"Newest runtime artifact: {build_version}" in item.value
                for item in completed.caption
            )
        )
        next_build_version = next(
            item for item in completed.text_input if item.label == "Build version"
        ).value
        self.assertNotEqual(next_build_version, build_version)
        self.assertFalse(
            any("already has staging or ZIP output" in item.value for item in completed.error)
        )

    def test_non_runtime_keeps_version_selector(self) -> None:
        class FakeBackend:
            @staticmethod
            def resolve_latest_runtime_artifact(**_: object):
                return {"id": "runtime", "version": "runtime", "path": "stage/runtime"}

            @staticmethod
            def list_artifacts(*, kind: str, **_: object):
                if kind == "server-data":
                    return [{"id": "data-v1", "version": "data-v1", "path": "stage/data-v1"}]
                return []

            @staticmethod
            def inspect_artifact(**_: object):
                return {
                    "archive_sha256": "a" * 64,
                    "target_upload_path": "/srv/shared",
                    "restricted_data": True,
                    "manifest": {"files": []},
                }

        def artifact_panel(backend: object) -> None:
            from deployment_console_ui.app import _render_artifact_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_artifact_tab(Adapter(backend), "development", "config/server_deploy.local.json")

        rendered = AppTest.from_function(artifact_panel, args=(FakeBackend(),)).run(timeout=10)
        selected = next(item for item in rendered.selectbox if item.label == "Artifact type")
        server_data = selected.set_value("server-data").run(timeout=10)
        self.assertIn("Version", [item.label for item in server_data.selectbox])

    def test_server_data_resumes_after_interrupted_upload_and_browser_reload(self) -> None:
        class FakeBackend:
            preview_calls = 0
            remote_files: set[str] = set()

            @staticmethod
            def resolve_latest_runtime_artifact(**_: object):
                return None

            @staticmethod
            def list_artifacts(*, kind: str, **_: object):
                if kind == "server-data":
                    return [
                        {
                            "id": "retained-data-v1",
                            "version": "retained-data-v1",
                            "path": "stage/retained-data-v1",
                        }
                    ]
                return []

            @staticmethod
            def inspect_artifact(**_: object):
                return {
                    "archive_sha256": "a" * 64,
                    "target_upload_path": "/srv/shared",
                    "restricted_data": False,
                    "required_confirmation": "DEPLOY development retained-data-v1",
                    "manifest": {
                        "files": [
                            {"path": "shared/first.csv", "sha256": "b" * 64},
                            {"path": "shared/remaining.csv", "sha256": "c" * 64},
                        ]
                    },
                }

            @staticmethod
            def deployment_policy(**_: object):
                return {"allow_upload": True, "target_id": "d" * 64}

            @classmethod
            def preview_remote_diff(cls, **_: object):
                cls.preview_calls += 1
                rows = []
                for path, checksum in (
                    ("shared/first.csv", "b" * 64),
                    ("shared/remaining.csv", "c" * 64),
                ):
                    uploaded = path in cls.remote_files
                    rows.append(
                        {
                            "path": path,
                            "local_path": f"stage/retained-data-v1/{path}",
                            "remote_path": f"/srv/shared/{path}",
                            "local_sha256": checksum,
                            "remote_sha256": checksum if uploaded else None,
                            "local_size_bytes": 1,
                            "remote_size_bytes": 1 if uploaded else None,
                            "status": "unchanged" if uploaded else "create",
                        }
                    )
                return rows

            @classmethod
            def upload_artifact(cls, **_: object):
                # Model a connection loss after one atomic file upload.  This
                # test never opens an actual remote session.
                cls.remote_files.add("shared/first.csv")
                raise RuntimeError("simulated connection interruption")

        def artifact_panel(backend: object) -> None:
            from deployment_console_ui.app import _render_artifact_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_artifact_tab(Adapter(backend), "development", "config/server_deploy.local.json")

        rendered = AppTest.from_function(artifact_panel, args=(FakeBackend(),)).run(timeout=10)
        kind_picker = next(item for item in rendered.selectbox if item.label == "Artifact type")
        server_data = kind_picker.set_value("server-data").run(timeout=10)
        self.assertEqual(FakeBackend.preview_calls, 1)
        self.assertEqual(
            server_data.multiselect[0].options,
            ["shared/first.csv", "shared/remaining.csv"],
        )

        pending = next(
            item for item in server_data.button if item.label == "Upload selected files"
        ).click().run(timeout=10)
        resumed = next(
            item for item in pending.button if item.label == "Confirm upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.preview_calls, 2)
        self.assertEqual(
            resumed.multiselect[0].options, ["shared/remaining.csv"]
        )
        self.assertEqual(
            next(item for item in resumed.selectbox if item.label == "Version").value,
            "retained-data-v1",
        )

        # The UI additionally mirrors the non-secret kind/version into URL
        # query parameters for an actual browser reload.  AppTest does not
        # expose a URL/query-parameter harness, so this regression covers the
        # interruption/recomparison behavior directly.

    def test_runtime_older_selection_persists_and_missing_selection_falls_back(self) -> None:
        class FakeBackend:
            include_old = True
            inspected_paths: list[str] = []
            uploaded_paths: list[str] = []

            @classmethod
            def list_artifacts(cls, **_: object):
                entries = [
                    {"id": "new-v2", "version": "new-v2", "path": "stage/new-v2"},
                ]
                if cls.include_old:
                    entries.append({"id": "old-v1", "version": "old-v1", "path": "stage/old-v1"})
                return entries

            @staticmethod
            def resolve_latest_runtime_artifact(**_: object):
                return {"id": "new-v2", "version": "new-v2", "path": "stage/new-v2"}

            @classmethod
            def inspect_artifact(cls, *, path: str, **_: object):
                cls.inspected_paths.append(path)
                return {
                    "archive_sha256": "a" * 64,
                    "target_upload_path": "/srv/runtime",
                    "artifact_path": path,
                    "restricted_data": False,
                    "manifest": {"files": [{"path": "app.py", "sha256": "b" * 64}]},
                }

            @staticmethod
            def deployment_policy(**_: object):
                return {"allow_upload": True, "target_id": "c" * 64}

            @staticmethod
            def preview_remote_diff(**_: object):
                return [{"path": "app.py", "status": "create"}]

            @classmethod
            def upload_artifact(cls, *, inspection: object, **_: object):
                cls.uploaded_paths.append(str(inspection["artifact_path"]))
                return {"status": "uploaded", "release_id": "test-release"}

        def artifact_panel(backend: object) -> None:
            from deployment_console_ui.app import _render_artifact_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_artifact_tab(Adapter(backend), "development", "config/server_deploy.local.json")

        rendered = AppTest.from_function(artifact_panel, args=(FakeBackend(),)).run(timeout=10)
        picker = next(item for item in rendered.selectbox if item.label == "Artifact version")
        self.assertEqual(picker.value, "new-v2")
        older = picker.set_value("old-v1").run(timeout=10)
        self.assertEqual(FakeBackend.inspected_paths[-1], "stage/old-v1")
        self.assertEqual(
            next(item for item in older.selectbox if item.label == "Artifact version").value,
            "old-v1",
        )
        pending = next(
            item for item in older.button if item.label == "Upload selected files"
        ).click().run(timeout=10)
        confirmed = next(
            item for item in pending.button if item.label == "Confirm upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.uploaded_paths, ["stage/old-v1"])
        refreshed = confirmed.run(timeout=10)
        self.assertEqual(
            next(item for item in refreshed.selectbox if item.label == "Artifact version").value,
            "old-v1",
        )

        FakeBackend.include_old = False
        fallback = refreshed.run(timeout=10)
        self.assertEqual(
            next(item for item in fallback.selectbox if item.label == "Artifact version").value,
            "new-v2",
        )
        self.assertEqual(FakeBackend.inspected_paths[-1], "stage/new-v2")

    def test_development_secure_config_is_redacted_and_requires_two_clicks(self) -> None:
        class FakeBackend:
            fingerprint = "a" * 64
            upload_calls = 0
            last_fingerprint = ""

            @classmethod
            def preview_development_secure_config_upload(cls, **_: object):
                return {
                    "status": "ready",
                    "upload_allowed": True,
                    "fingerprint": cls.fingerprint,
                    "target_upload_path": "/home/csda/AI_Routing/development/config_common_vrp.dev.json",
                    "files": [{
                        "filename": "config_common_vrp.dev.json",
                        "target": "/home/csda/AI_Routing/development/config_common_vrp.dev.json",
                        "sha256": "b" * 64,
                        "size_bytes": 321,
                        "mode": "0600",
                        "status": "update",
                        "password": "do-not-show",
                    }],
                    "password": "do-not-show",
                }

            @classmethod
            def upload_development_secure_config(cls, *, expected_fingerprint: str, **_: object):
                cls.upload_calls += 1
                cls.last_fingerprint = expected_fingerprint
                return {"status": "uploaded", "password": "do-not-show"}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_development_secure_config
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_development_secure_config(Adapter(backend))

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        displayed = "\n".join(
            [item.value for item in rendered.markdown]
            + [item.value for item in rendered.caption]
            + [item.value for item in rendered.success]
            + [item.value for item in rendered.error]
        )
        self.assertNotIn("do-not-show", displayed)
        secure_table = rendered.dataframe[0].value
        self.assertEqual(list(secure_table.columns), [
            "local_file", "remote_path", "sha256_12", "size_bytes", "mode", "status"
        ])
        self.assertEqual(secure_table.iloc[0]["local_file"], "config_common_vrp.dev.json")
        self.assertEqual(
            secure_table.iloc[0]["remote_path"],
            "/home/csda/AI_Routing/development/config_common_vrp.dev.json",
        )
        self.assertNotIn(
            "/home/csda/AI_Routing/development/config_common_vrp.dev.json",
            [item.value for item in rendered.metric],
        )
        self.assertIn("Upload development secure config", [item.label for item in rendered.button])

        pending = next(
            item for item in rendered.button if item.label == "Upload development secure config"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 0)
        self.assertIn("Confirm secure config upload", [item.label for item in pending.button])
        cancelled = next(
            item for item in pending.button if item.label == "Cancel secure config upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 0)

        pending = next(
            item for item in cancelled.button if item.label == "Upload development secure config"
        ).click().run(timeout=10)
        FakeBackend.fingerprint = "c" * 64
        invalidated = pending.run(timeout=10)
        self.assertNotIn("Confirm secure config upload", [item.label for item in invalidated.button])
        pending = next(
            item for item in invalidated.button if item.label == "Upload development secure config"
        ).click().run(timeout=10)
        confirmed = next(
            item for item in pending.button if item.label == "Confirm secure config upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 1)
        self.assertEqual(FakeBackend.last_fingerprint, "c" * 64)
        self.assertTrue(any("restart is required" in item.value for item in confirmed.success))

    def test_production_secure_config_is_redacted_and_uses_production_backend(self) -> None:
        class FakeBackend:
            fingerprint = "d" * 64
            upload_calls = 0
            last_fingerprint = ""

            @classmethod
            def preview_production_secure_config_upload(cls, **kwargs: object):
                self.assertEqual(kwargs["environment"], "production")
                return {
                    "status": "ready",
                    "upload_allowed": True,
                    "fingerprint": cls.fingerprint,
                    "files": [
                        {
                            "filename": "config_common_vrp.json",
                            "target": "/home/csda/AI_Routing/production/config_common_vrp.json",
                            "sha256": "e" * 64,
                            "size_bytes": 321,
                            "mode": "0600",
                            "status": "unchanged",
                            "api_key": "do-not-show",
                        },
                        {
                            "filename": "server_deploy.local.json",
                            "target": "/home/csda/AI_Routing/production/server_deploy.local.json",
                            "sha256": "f" * 64,
                            "size_bytes": 654,
                            "mode": "0600",
                            "status": "changed",
                            "password": "do-not-show",
                        },
                    ],
                }

            @classmethod
            def upload_production_secure_config(cls, *, expected_fingerprint: str, **kwargs: object):
                self.assertEqual(kwargs["environment"], "production")
                cls.upload_calls += 1
                cls.last_fingerprint = expected_fingerprint
                return {"status": "uploaded", "api_key": "do-not-show"}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_production_secure_config
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_production_secure_config(Adapter(backend))

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        displayed = "\n".join(
            [item.value for item in rendered.markdown]
            + [item.value for item in rendered.caption]
            + [item.value for item in rendered.error]
        )
        self.assertIn("Production secure config", displayed)
        self.assertNotIn("Development secure config", displayed)
        self.assertNotIn("do-not-show", displayed)
        self.assertEqual([item.value for item in rendered.metric], [
            "2 protected local files", "2 fixed server files"
        ])
        secure_table = rendered.dataframe[0].value
        self.assertEqual(list(secure_table["status"]), ["unchanged", "changed"])
        self.assertIn("Upload production secure config", [item.label for item in rendered.button])

        pending = next(
            item for item in rendered.button if item.label == "Upload production secure config"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 0)
        confirmed = next(
            item for item in pending.button if item.label == "Confirm secure config upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 1)
        self.assertEqual(FakeBackend.last_fingerprint, "d" * 64)
        self.assertTrue(any("Production secure config uploaded" in item.value for item in confirmed.success))

    def test_production_secure_config_disables_upload_when_unchanged(self) -> None:
        class FakeBackend:
            @staticmethod
            def preview_production_secure_config_upload(**_: object):
                return {
                    "status": "unchanged",
                    "upload_allowed": True,
                    "mutation_required": False,
                    "fingerprint": "a" * 64,
                    "files": [],
                }

            @staticmethod
            def upload_production_secure_config(**_: object):
                raise AssertionError("unchanged secure config must not upload")

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_production_secure_config
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_production_secure_config(Adapter(backend))

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertTrue(
            any("already up to date" in item.value for item in rendered.success)
        )
        upload = next(
            item
            for item in rendered.button
            if item.label == "Upload production secure config"
        )
        self.assertTrue(upload.disabled)


    def test_dirty_development_build_requires_toggle_and_calls_backend_once(self) -> None:
        class FakeBackend:
            preview_calls = 0
            build_calls = 0

            @classmethod
            def preview_runtime_build(cls, *, environment: str, version: str):
                cls.preview_calls += 1
                return {
                    "environment": environment,
                    "version": version,
                    "source_revision": "a" * 40,
                    "source_dirty": True,
                    "source_change_count": 3,
                    "requires_dirty_approval": True,
                    "build_allowed": True,
                    "staging_path": f"deployment/{environment}/{version}",
                    "archive_path": f"deployment/{environment}/runtime-{version}.zip",
                    "output_exists": False,
                }

            @classmethod
            def build_runtime_artifact(
                cls, *, environment: str, version: str, allow_dirty_source: bool
            ):
                cls.build_calls += 1
                if not allow_dirty_source:
                    raise AssertionError("dirty approval was not forwarded")
                return {
                    "status": "built",
                    "environment": environment,
                    "version": version,
                    "source_dirty": True,
                    "archive_path": f"deployment/{environment}/runtime-{version}.zip",
                }

        def build_panel(backend: object) -> None:
            from deployment_console_ui.app import _render_build_artifact as render_build
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            render_build(Adapter(backend), "development")

        rendered = AppTest.from_function(build_panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertTrue(
            any("never promoted" in item.value for item in rendered.info)
        )
        button = next(item for item in rendered.button if item.label == "Build runtime artifact")
        self.assertTrue(button.disabled)
        self.assertEqual(len(rendered.checkbox), 1)

        approved = rendered.checkbox[0].set_value(True).run(timeout=10)
        button = next(item for item in approved.button if item.label == "Build runtime artifact")
        self.assertFalse(button.disabled)
        completed = button.click().run(timeout=10)
        self.assertEqual(FakeBackend.build_calls, 1)
        self.assertTrue(any("Built runtime artifact" in item.value for item in completed.success))

    def test_admin_tools_build_requires_dirty_approval_and_hands_off_version(self) -> None:
        class FakeBackend:
            build_calls = 0
            allow_dirty = False

            @staticmethod
            def preview_admin_tools_build(*, version: str):
                return {
                    "version": version,
                    "source_revision": "b" * 40,
                    "source_dirty": True,
                    "source_change_count": 2,
                    "build_allowed": True,
                    "output_exists": False,
                    "staging_path": f"deployment/admin-tools/{version}",
                    "archive_path": f"deployment/admin-tools/admin-tools-{version}.zip",
                }

            @classmethod
            def build_admin_tools_artifact(
                cls, *, version: str, allow_dirty_source: bool
            ):
                cls.build_calls += 1
                cls.allow_dirty = allow_dirty_source
                return {
                    "status": "built",
                    "version": version,
                    "source_dirty": True,
                    "promotable": False,
                    "archive_path": f"deployment/admin-tools/admin-tools-{version}.zip",
                }

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_admin_tools_build
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_admin_tools_build(Adapter(backend), "development")

        initial = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        build_button = next(
            item for item in initial.button if item.label == "Build Admin Tools artifact"
        )
        self.assertTrue(build_button.disabled)
        self.assertTrue(
            any("cannot be pinned" in item.value for item in initial.warning)
        )
        approved = initial.checkbox[0].set_value(True).run(timeout=10)
        completed_version = next(
            item for item in approved.text_input
            if item.label == "Admin Tools build version"
        ).value
        built = next(
            item for item in approved.button if item.label == "Build Admin Tools artifact"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.build_calls, 1)
        self.assertTrue(FakeBackend.allow_dirty)
        self.assertEqual(
            built.session_state["artifact-version-development-admin-tools"],
            completed_version,
        )
        self.assertTrue(
            any("Built Admin Tools artifact" in item.value for item in built.success)
        )

    def test_development_verification_activation_is_two_click_and_development_only(self) -> None:
        class FakeBackend:
            activation_calls = 0
            confirm_value = False

            @staticmethod
            def preview_admin_tools_development_activation(*, version: str):
                if version == "admin-v3":
                    return {
                        "status": "not_eligible",
                        "version": version,
                        "eligible": False,
                        "mode": "unavailable",
                    }
                return {
                    "status": "ready",
                    "version": version,
                    "eligible": True,
                    "mode": "development-verification",
                    "required_confirmation": "must-not-be-rendered",
                }

            @classmethod
            def activate_admin_tools_development_release(
                cls, *, version: str, confirm: bool
            ):
                cls.activation_calls += 1
                cls.confirm_value = confirm
                return {
                    "status": "activated",
                    "version": version,
                    "mode": "development-verification",
                    "password": "do-not-render",
                }

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_admin_tools_development_activation
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            artifacts = [{"version": "admin-v3"}, {"version": "admin-v2"}]
            _render_admin_tools_development_activation(Adapter(backend), artifacts)

        initial = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertTrue(
            any("Latest eligible verified non-promotable version: admin-v2" in item.value for item in initial.caption)
        )
        requested = next(
            item for item in initial.button if item.label == "Use for Development DB"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.activation_calls, 0)
        confirmed = next(
            item for item in requested.button
            if item.label == "Confirm Development DB activation"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.activation_calls, 1)
        self.assertTrue(FakeBackend.confirm_value)
        visible = " ".join(
            [item.value for item in confirmed.success]
            + [item.value for item in confirmed.warning]
            + [item.value for item in confirmed.error]
        )
        self.assertIn("Development DB verification execution version activated", visible)
        self.assertIn("cannot be used for Production", visible)
        self.assertNotIn("must-not-be-rendered", visible)
        self.assertNotIn("do-not-render", visible)

    def test_production_dirty_build_has_no_bypass_and_invalid_version_skips_preview(self) -> None:
        class FakeBackend:
            preview_calls = 0

            @classmethod
            def preview_runtime_build(cls, *, environment: str, version: str):
                cls.preview_calls += 1
                return {
                    "environment": environment,
                    "version": version,
                    "source_revision": "a" * 40,
                    "source_dirty": True,
                    "source_change_count": 1,
                    "build_allowed": False,
                    "output_exists": False,
                }

            @staticmethod
            def build_runtime_artifact(**_: object):
                raise AssertionError("blocked production build must not run")

        def build_panel(backend: object) -> None:
            from deployment_console_ui.app import _render_build_artifact as render_build
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            render_build(Adapter(backend), "production")

        rendered = AppTest.from_function(build_panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertEqual(len(rendered.checkbox), 0)
        button = next(item for item in rendered.button if item.label == "Build runtime artifact")
        self.assertTrue(button.disabled)
        calls_before_invalid = FakeBackend.preview_calls
        invalid = rendered.text_input[0].set_value("bad version; rm").run(timeout=10)
        self.assertEqual(FakeBackend.preview_calls, calls_before_invalid)
        self.assertTrue(any("Version must start" in item.value for item in invalid.error))

    def test_service_action_is_one_click_and_monitoring_has_no_environment_split(self) -> None:
        class FakeBackend:
            service_calls = 0
            monitor_calls = 0
            confirmation = ""

            @staticmethod
            def list_artifacts(**_: object):
                return []

            @staticmethod
            def list_migrations(**_: object):
                return []

            @staticmethod
            def list_seed_actions(**_: object):
                return []

            @staticmethod
            def observe_services(**_: object):
                raise AssertionError("Monitoring must not issue duplicate per-environment status calls")

            @staticmethod
            def list_history(*, environment: str, kind: str, **_: object):
                if kind != "runtime":
                    return []
                return [
                    {
                        "id": f"{environment}-runtime-v1",
                        "version": "v1",
                        "status": "uploaded",
                        "service_eligible": True,
                        "complete_manifest": True,
                    }
                ]

            @classmethod
            def run_service_action(cls, *, typed_confirmation: str, **_: object):
                cls.service_calls += 1
                cls.confirmation = typed_confirmation
                return {"status": "healthy", "observations": []}

            @classmethod
            def observe_platform(cls, **_: object):
                cls.monitor_calls += 1
                units = [
                    ("production", "common-vrp.service"),
                    ("production", "smart-routing.service"),
                    ("production", "common-vrp-client.service"),
                    ("development", "common-vrp-dev.service"),
                    ("development", "smart-routing-dev.service"),
                    ("development", "common-vrp-client-dev.service"),
                    ("shared", "osrm-korea.service"),
                    ("shared", "osrm-usa.service"),
                    ("shared", "osrm-usa.service"),
                ]
                services = [
                    {
                        "scope": scope,
                        "component": f"component-{index}",
                        "unit": unit,
                        "health_endpoint": f"http://127.0.0.1:{8000 + index}/health",
                        "port": 8000 + index,
                        "active": True,
                        "enabled": True,
                        "health_ok": True,
                        "status": "healthy",
                    }
                    for index, (scope, unit) in enumerate(units)
                ]
                return {"services": services, "total": 9, "healthy": 9}

        backend = FakeBackend()

        def app(injected_backend: object) -> None:
            from deployment_console_ui.app import render_app as render

            render(injected_backend)

        rendered = AppTest.from_function(app, args=(backend,)).run(timeout=10)
        self.assertEqual(len(rendered.exception), 0)
        self.assertFalse(any(button.label in {"Start", "Restart"} for button in rendered.button))
        self.assertNotIn("Services", [tab.label for tab in rendered.tabs])
        self.assertNotIn("Monitor", [tab.label for tab in rendered.tabs])

        monitor_calls_before_navigation = FakeBackend.monitor_calls
        monitored = next(
            item for item in rendered.button if item.label.endswith("Monitoring")
        ).click().run(timeout=10)
        self.assertEqual(len(monitored.exception), 0)
        self.assertNotIn(
            "Type the service action phrase exactly",
            [item.label for item in monitored.text_input],
        )
        self.assertNotIn("Environment", [item.label for item in monitored.radio])
        self.assertEqual(len(monitored.dataframe), 1)
        service_selectors = [
            item for item in monitored.multiselect if item.label == "Allowlisted services"
        ]
        self.assertEqual([len(item.value) for item in service_selectors], [3, 3])
        headings = [item.value for item in monitored.markdown]
        self.assertIn("#### Production", headings)
        self.assertIn("#### Development", headings)
        start_buttons = [button for button in monitored.button if button.label == "Start"]
        self.assertEqual(len(start_buttons), 2)
        started = start_buttons[1].click().run(timeout=10)
        self.assertEqual(FakeBackend.service_calls, 1)
        self.assertEqual(
            FakeBackend.confirmation,
            "START development development-runtime-v1 "
            "common-vrp-dev.service,smart-routing-dev.service,"
            "common-vrp-client-dev.service",
        )
        self.assertTrue(any("Start completed" in item.value for item in started.success))
        self.assertEqual(len(started.dataframe), 1)
        self.assertGreater(FakeBackend.monitor_calls, monitor_calls_before_navigation)

    def test_local_policy_explicitly_enables_allowlisted_service_control(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "config" / "server_deploy.local.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertIs(payload.get("allow_service_control"), True)

    def test_hidden_database_profiles_exist_for_both_environments(self) -> None:
        from admin_tools.db.release_backend import load_database_target

        for environment, relative_path in DB_CONFIG_PATHS.items():
            path = PROJECT_ROOT / relative_path
            self.assertTrue(path.is_file(), path)
            self.assertEqual(load_database_target(path).environment, environment)

    def test_artifact_selection_auto_fetches_full_diff_with_fake_ui_backend(self) -> None:
        class FakeBackend:
            preview_calls = 0
            upload_calls = 0
            last_confirmation = ""
            fail_upload = False
            failure_receipt = False
            remote_uploaded = False
            target_id = "c" * 64

            @staticmethod
            def list_artifacts(*, environment: str, kind: str):
                return [{"id": "v1", "version": "v1", "label": "v1", "path": "stage/v1"}]

            @staticmethod
            def inspect_artifact(**_: object):
                return {
                    "archive_sha256": "a" * 64,
                    "target_upload_path": "/home/csda/AI_Routing/development",
                    "restricted_data": False,
                    "required_confirmation": "DEPLOY development v1",
                    "manifest": {
                        "artifact_type": "server-runtime",
                        "source_dirty": True,
                        "promotable": False,
                        "files": [{"path": "app.py", "sha256": "b" * 64}],
                    },
                }

            @staticmethod
            def resolve_latest_runtime_artifact(*, environment: str):
                return {"id": "v1", "version": "v1", "label": "v1", "path": "stage/v1"}

            @classmethod
            def deployment_policy(cls, **_: object):
                return {"allow_upload": True, "target_id": cls.target_id}

            @classmethod
            def preview_remote_diff(cls, **_: object):
                cls.preview_calls += 1
                status = "unchanged" if cls.remote_uploaded else "create"
                return [
                    {
                        "path": "app.py",
                        "local_path": "C:/stage/v1/app.py",
                        "remote_path": "/home/csda/AI_Routing/development/app.py",
                        "local_sha256": "b" * 64,
                        "remote_sha256": None if status == "create" else "b" * 64,
                        "local_size_bytes": 12,
                        "remote_size_bytes": None if status == "create" else 12,
                        "status": status,
                    }
                ]

            @staticmethod
            def list_migrations(**_: object):
                return []

            @staticmethod
            def list_seed_actions(**_: object):
                return []

            @staticmethod
            def observe_services(**_: object):
                return []

            @staticmethod
            def list_history(**_: object):
                return []

            @staticmethod
            def observe_platform(**_: object):
                return {"services": [], "total": 0, "healthy": 0}

            @classmethod
            def upload_artifact(cls, *, typed_confirmation: str, **_: object):
                cls.upload_calls += 1
                cls.last_confirmation = typed_confirmation
                if cls.fail_upload:
                    raise RuntimeError("simulated upload failure password=hidden")
                if cls.failure_receipt:
                    return {
                        "status": "upload_failed",
                        "release_id": "must-not-be-treated-as-success",
                    }
                cls.remote_uploaded = True
                return {
                    "status": "uploaded",
                    "release_id": "development-runtime-v1-test",
                    "sha256": "a" * 64,
                }

        backend = FakeBackend()

        def app(injected_backend: object) -> None:
            from deployment_console_ui.app import render_app as render

            render(injected_backend)

        rendered = AppTest.from_function(app, args=(backend,)).run(timeout=10)
        rendered = next(
            item for item in rendered.button if item.label.endswith("Development")
        ).click().run(timeout=10)
        self.assertEqual(len(rendered.exception), 0)
        self.assertEqual(FakeBackend.preview_calls, 1)
        self.assertNotIn("Preview remote diff", [button.label for button in rendered.button])
        self.assertGreaterEqual(len(rendered.dataframe), 2)
        self.assertNotIn(
            "Type the phrase exactly to authorize upload",
            [item.label for item in rendered.text_input],
        )
        self.assertNotIn(
            "### Read-only validation",
            [item.value for item in rendered.markdown],
        )
        self.assertNotIn(
            "DEPLOY development v1", [item.value for item in rendered.code]
        )
        captions = [item.value for item in rendered.caption]
        self.assertFalse(any("cached read-only snapshot" in value for value in captions))
        self.assertFalse(any("file-content fingerprint" in value for value in captions))

        pending = next(
            button for button in rendered.button if button.label == "Upload selected files"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 0)
        self.assertIn("Confirm upload", [button.label for button in pending.button])
        self.assertIn("Cancel", [button.label for button in pending.button])
        self.assertIn(
            "development server: Upload 1 selected files?",
            [item.value for item in pending.warning],
        )

        cancelled = next(
            button for button in pending.button if button.label == "Cancel"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 0)

        pending = next(
            button for button in cancelled.button if button.label == "Upload selected files"
        ).click().run(timeout=10)
        invalidated = pending.multiselect[0].set_value([]).run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 0)
        restored = invalidated.multiselect[0].set_value(["app.py"]).run(timeout=10)
        pending = next(
            button for button in restored.button if button.label == "Upload selected files"
        ).click().run(timeout=10)
        confirmed = next(
            button for button in pending.button if button.label == "Confirm upload"
        ).click().run(timeout=10)
        self.assertEqual(len(confirmed.exception), 0)
        self.assertEqual(FakeBackend.upload_calls, 1)
        self.assertEqual(FakeBackend.last_confirmation, "DEPLOY development v1")
        self.assertEqual(FakeBackend.preview_calls, 2)
        self.assertNotIn("Confirm upload", [button.label for button in confirmed.button])
        self.assertTrue(
            any("Upload completed" in item.value for item in confirmed.success)
        )
        self.assertTrue(
            any("development-runtime-v1-test" in item.value for item in confirmed.success)
        )
        self.assertTrue(
            any("No changed files to upload" in item.value for item in confirmed.info)
        )
        self.assertNotIn(
            "Upload selected files", [button.label for button in confirmed.button]
        )
        FakeBackend.target_id = "d" * 64
        target_changed = confirmed.run(timeout=10)
        self.assertFalse(
            any("Upload completed" in item.value for item in target_changed.success)
        )
        FakeBackend.target_id = "c" * 64

        FakeBackend.fail_upload = True
        FakeBackend.upload_calls = 0
        FakeBackend.remote_uploaded = False
        failed_app = AppTest.from_function(app, args=(backend,)).run(timeout=10)
        failed_app = next(
            item for item in failed_app.button if item.label.endswith("Development")
        ).click().run(timeout=10)
        failed_pending = next(
            button
            for button in failed_app.button
            if button.label == "Upload selected files"
        ).click().run(timeout=10)
        failed = next(
            button
            for button in failed_pending.button
            if button.label == "Confirm upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 1)
        self.assertNotIn("Confirm upload", [button.label for button in failed.button])
        self.assertTrue(any("Upload failed" in item.value for item in failed.error))
        failure_text = " ".join(item.value for item in failed.error)
        self.assertIn("upload connection did not complete", failure_text)
        self.assertNotIn("password=hidden", failure_text)
        self.assertNotIn("simulated upload failure", failure_text)
        retry_pending = next(
            button for button in failed.button if button.label == "Upload selected files"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 1)
        self.assertIn("Confirm upload", [button.label for button in retry_pending.button])
        self.assertFalse(any("Upload failed" in item.value for item in retry_pending.error))

        FakeBackend.fail_upload = False
        FakeBackend.failure_receipt = True
        FakeBackend.upload_calls = 0
        FakeBackend.remote_uploaded = False
        receipt_app = AppTest.from_function(app, args=(backend,)).run(timeout=10)
        receipt_app = next(
            item for item in receipt_app.button if item.label.endswith("Development")
        ).click().run(timeout=10)
        receipt_pending = next(
            button
            for button in receipt_app.button
            if button.label == "Upload selected files"
        ).click().run(timeout=10)
        receipt_failed = next(
            button
            for button in receipt_pending.button
            if button.label == "Confirm upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 1)
        self.assertTrue(any("Upload failed" in item.value for item in receipt_failed.error))
        self.assertFalse(
            any("Upload completed" in item.value for item in receipt_failed.success)
        )
        self.assertGreaterEqual(FakeBackend.preview_calls, 2)
        self.assertIn(
            "Upload selected files", [button.label for button in receipt_failed.button]
        )

    def test_upload_intent_changes_with_every_bound_input(self) -> None:
        base = {
            "environment": "development",
            "kind": "runtime",
            "artifact_id": "v1",
            "archive_sha256": "a" * 64,
            "target_id": "c" * 64,
            "selected_files": ["b.py", "a.py"],
        }
        original = _upload_intent(**base)
        reordered = _upload_intent(**{**base, "selected_files": ["a.py", "b.py"]})
        self.assertEqual(original["intent_id"], reordered["intent_id"])
        self.assertEqual(original["scope_id"], reordered["scope_id"])
        for key, value in (
            ("environment", "production"),
            ("artifact_id", "v2"),
            ("archive_sha256", "b" * 64),
            ("target_id", "d" * 64),
            ("selected_files", ["a.py"]),
        ):
            changed = _upload_intent(**{**base, key: value})
            self.assertNotEqual(original["intent_id"], changed["intent_id"])
            if key == "selected_files":
                self.assertEqual(original["scope_id"], changed["scope_id"])
            else:
                self.assertNotEqual(original["scope_id"], changed["scope_id"])

    def test_changed_upload_files_hides_unchanged_rows(self) -> None:
        self.assertEqual(
            _changed_upload_files(
                [
                    {"path": "same.py", "status": "unchanged"},
                    {"path": "new.py", "status": "create"},
                    {"path": "changed.py", "status": "update"},
                ]
            ),
            ["changed.py", "new.py"],
        )

    def test_remote_diff_is_cached_and_force_refreshes_with_fake_backend(self) -> None:
        class FakeBackend:
            calls = 0

            @classmethod
            def preview_remote_diff(cls, **_: object):
                cls.calls += 1
                return [{"path": "app.py", "status": f"call-{cls.calls}"}]

        adapter = BackendAdapter(FakeBackend())
        cache: dict[str, object] = {}

        def fetch():
            return adapter.call(
                "preview_remote_diff",
                inspection={},
                selected_files=["app.py"],
                config_path="config/server_deploy.local.json",
            )

        first = _load_remote_diff(cache, "target:artifact", fetch)
        second = _load_remote_diff(cache, "target:artifact", fetch)
        refreshed = _load_remote_diff(cache, "target:artifact", fetch, force=True)
        self.assertEqual(first, second)
        self.assertEqual(FakeBackend.calls, 2)
        self.assertEqual(refreshed[0]["status"], "call-2")
        failed = _load_remote_diff(cache, "target:artifact", lambda: None, force=True)
        self.assertIsNone(failed)
        self.assertNotIn("target:artifact", cache)

    def test_diff_panels_are_sorted_and_show_only_safe_file_fields(self) -> None:
        local, remote = _diff_panels(
            [
                {
                    "path": "z.py",
                    "local_path": "C:/stage/z.py",
                    "remote_path": "/srv/z.py",
                    "local_sha256": "a" * 64,
                    "remote_sha256": None,
                    "local_size_bytes": 10,
                    "remote_size_bytes": None,
                    "status": "create",
                    "password": "hidden",
                },
                {
                    "path": "a.py",
                    "local_path": "C:/stage/a.py",
                    "remote_path": "/srv/a.py",
                    "local_sha256": "b" * 64,
                    "remote_sha256": "b" * 64,
                    "local_size_bytes": 20,
                    "remote_size_bytes": 20,
                    "status": "unchanged",
                },
            ]
        )
        self.assertEqual([row["artifact_path"] for row in local], ["a.py", "z.py"])
        self.assertEqual([row["artifact_path"] for row in remote], ["a.py", "z.py"])
        self.assertEqual(local[1]["sha256_12"], "a" * 12)
        self.assertEqual(remote[1]["sha256_12"], "-")
        self.assertTrue(all("password" not in row for row in local + remote))

    def test_all_unchanged_preview_keeps_both_panels_full_but_upload_queue_empty(
        self,
    ) -> None:
        preview = [
            {
                "path": "a.py",
                "local_path": "C:/stage/a.py",
                "remote_path": "/srv/a.py",
                "local_sha256": "a" * 64,
                "remote_sha256": "a" * 64,
                "local_size_bytes": 10,
                "remote_size_bytes": 10,
                "status": "unchanged",
            },
            {
                "path": "nested/b.py",
                "local_path": "C:/stage/nested/b.py",
                "remote_path": "/srv/nested/b.py",
                "local_sha256": "b" * 64,
                "remote_sha256": "b" * 64,
                "local_size_bytes": 20,
                "remote_size_bytes": 20,
                "status": "unchanged",
            },
        ]

        local, remote = _diff_panels(preview)

        self.assertEqual([row["artifact_path"] for row in local], ["a.py", "nested/b.py"])
        self.assertEqual([row["artifact_path"] for row in remote], ["a.py", "nested/b.py"])
        self.assertEqual(_changed_upload_files(preview), [])

    def test_local_panel_path_is_relative_to_project_root(self) -> None:
        local_file = PROJECT_ROOT / "deployment" / "development" / "v1" / "app.py"
        local, _ = _diff_panels(
            [
                {
                    "path": "app.py",
                    "local_path": str(local_file),
                    "remote_path": "/home/csda/AI_Routing/development/app.py",
                    "local_sha256": "a" * 64,
                    "remote_sha256": None,
                    "local_size_bytes": 1,
                    "remote_size_bytes": None,
                    "status": "create",
                }
            ]
        )
        self.assertEqual(
            Path(local[0]["path"]), Path("deployment/development/v1/app.py")
        )

    def test_streamlit_dataframes_use_current_stretch_width_api(self) -> None:
        source = (
            Path(__file__).resolve().parents[1] / "deployment_console_ui" / "app.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("use_container" + "_width", source)
        self.assertIn('width="stretch"', source)
        self.assertNotIn("Local SFTP/SSH profile", source)
        self.assertNotIn("Local DB target profile", source)
        self.assertNotIn("No delete operation is provided by this console.", source)
        self.assertNotIn("Development environment selected.", source)

    def test_pending_upload_warning_source_has_clear_text_without_replacement_characters(self) -> None:
        source = (PROJECT_ROOT / "deployment_console_ui" / "app.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('f"{pending.get(\'environment\')} server: Upload "', source)
        self.assertIn('selected files?"', source)
        self.assertNotIn("\ufffd", source)

    def test_root_entrypoint_import_has_no_backend_side_effect(self) -> None:
        module = importlib.import_module("sr_deployment_console")
        self.assertTrue(callable(module.main))

    def test_injected_backend_is_used_without_importing_platform(self) -> None:
        class FakeBackend:
            @staticmethod
            def list_artifacts(*, environment: str, kind: str):
                return [environment, kind]

        adapter = BackendAdapter(FakeBackend())
        self.assertEqual(
            adapter.call("list_artifacts", environment="development", kind="runtime"),
            ["development", "runtime"],
        )

    def test_missing_capability_is_explicit(self) -> None:
        adapter = BackendAdapter(object())
        with self.assertRaises(BackendCapabilityError):
            adapter.call("upload_artifact")

    def test_db_admin_unavailable_has_no_generic_master_uploader(self) -> None:
        """The DB screen must keep failed/read-only state visible without a SQL escape hatch."""

        class FakeBackend:
            @staticmethod
            def get_database_overview(*, environment: str):
                return {"status": "unavailable", "environment": environment}

            @staticmethod
            def list_migrations(**_: object):
                return []

            @staticmethod
            def list_seed_actions(**_: object):
                return []

            @staticmethod
            def list_master_table_specs(*, environment: str):
                return {
                    "tables": [
                        {
                            "id": "technicians",
                            "label": "Technician master",
                            "write_allowed": True,
                            "required_columns": ["technician_id"],
                            "primary_key": ["technician_id"],
                        },
                        {
                            "id": "routing_jobs",
                            "label": "Routing jobs (transactional)",
                            "write_allowed": False,
                        },
                    ]
                }

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_db_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_db_tab(Adapter(backend), "development", "config/common_vrp.dev.json")

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertEqual(len(rendered.exception), 0)
        self.assertTrue(any("Database overview is unavailable" in item.value for item in rendered.error))
        self.assertNotIn("Allowlisted master table", [item.label for item in rendered.selectbox])
        self.assertEqual(len(rendered.file_uploader), 0)
        self.assertNotIn("SQL", " ".join(item.label for item in rendered.text_area))

    def test_db_admin_is_locked_when_admin_tools_release_is_not_pinned(self) -> None:
        class FakeBackend:
            overview_calls = 0

            @staticmethod
            def get_connection_settings():
                return {
                    "status": "configured",
                    "connection": {
                        "admin_tools_release_configured": False,
                        "admin_tools_release_version": "",
                    },
                }

            @classmethod
            def get_database_overview(cls, **_: object):
                cls.overview_calls += 1
                raise AssertionError("DB overview must remain blocked without a pin")

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_db_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_db_tab(Adapter(backend), "development", "config/common_vrp.dev.json")

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertEqual(FakeBackend.overview_calls, 0)
        self.assertTrue(
            any("Development may use an explicitly activated verification release" in item.value for item in rendered.warning)
        )
        self.assertNotIn("Operation failed", " ".join(item.value for item in rendered.error))
        self.assertNotIn("Confirm migration", [item.label for item in rendered.button])

    def test_db_admin_pin_error_is_one_safe_actionable_message(self) -> None:
        class FakeBackend:
            @staticmethod
            def get_connection_settings():
                return {
                    "status": "configured",
                    "connection": {
                        "admin_tools_release_configured": True,
                        "admin_tools_release_version": "admin-v1",
                    },
                }

            @staticmethod
            def get_database_overview(**_: object):
                raise ValueError(
                    "admin_tools_release_version pin invalid password=do-not-render"
                )

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_db_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_db_tab(Adapter(backend), "development", "config/common_vrp.dev.json")

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        visible = " ".join(
            [item.value for item in rendered.warning]
            + [item.value for item in rendered.error]
        )
        self.assertIn("Activate a verified Admin Tools release", visible)
        self.assertNotIn("Operation failed", visible)
        self.assertNotIn("do-not-render", visible)

    def test_development_db_accepts_explicit_verification_pin_but_marks_scope(self) -> None:
        class FakeBackend:
            overview_calls = 0

            @staticmethod
            def get_connection_settings():
                return {
                    "status": "configured",
                    "connection": {
                        "admin_tools_release_configured": False,
                        "admin_tools_release_version": "",
                        "admin_tools_development_release_configured": True,
                        "admin_tools_development_release_version": "admin-dirty-v2",
                        "admin_tools_development_release_mode": "development-verification",
                    },
                }

            @classmethod
            def get_database_overview(cls, **_: object):
                cls.overview_calls += 1
                return {"status": "unavailable", "environment": "development"}

            @staticmethod
            def list_migrations(**_: object):
                return []

            @staticmethod
            def list_seed_actions(**_: object):
                return []

            @staticmethod
            def list_master_table_specs(**_: object):
                return {"tables": []}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_db_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_db_tab(Adapter(backend), "development", "config/common_vrp.dev.json")

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertEqual(FakeBackend.overview_calls, 1)
        self.assertTrue(
            any("Development verification execution version: admin-dirty-v2" in item.value for item in rendered.caption)
        )
        self.assertTrue(
            any("cannot be used for Production" in item.value for item in rendered.warning)
        )

    def test_production_db_rejects_development_verification_pin(self) -> None:
        class FakeBackend:
            overview_calls = 0

            @staticmethod
            def get_connection_settings():
                return {
                    "status": "configured",
                    "connection": {
                        "admin_tools_release_configured": False,
                        "admin_tools_release_version": "",
                        "admin_tools_development_release_configured": True,
                        "admin_tools_development_release_version": "admin-dirty-v2",
                        "admin_tools_development_release_mode": "development-verification",
                    },
                }

            @classmethod
            def get_database_overview(cls, **_: object):
                cls.overview_calls += 1
                raise AssertionError("Production must not use a Development verification pin")

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_db_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_db_tab(Adapter(backend), "production", "config/common_vrp.prod.json")

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertEqual(FakeBackend.overview_calls, 0)
        self.assertTrue(
            any("Production requires a clean, promotable release" in item.value for item in rendered.warning)
        )

    def test_db_migration_requires_review_then_confirm_without_typed_input(self) -> None:
        class FakeBackend:
            execute_calls = 0

            @staticmethod
            def get_database_overview(*, environment: str):
                return {
                    "status": "ok",
                    "environment": environment,
                    "database": "vrp_db_dev",
                    "migration_registry_exists": True,
                    "tables": [{"table_name": f"t{index}", "exists": True} for index in range(13)],
                }

            @staticmethod
            def list_migrations(**_: object):
                return [{"migration_id": "V001__test", "description": "test migration", "checksum_sha256": "a" * 64, "status": "pending", "statement_count": 1, "rollback_instructions": "manual"}]

            @staticmethod
            def preview_migration(**_: object):
                return {
                    "sql": "CREATE TABLE test(id int);",
                    "statements": ["CREATE TABLE test(id int)"],
                    "plan": {"checksum_sha256": "a" * 64, "statement_types": ["create_table"], "required_confirmation": "internal-only"},
                }

            @classmethod
            def execute_migration(cls, **_: object):
                cls.execute_calls += 1
                return {"status": "applied"}

            @staticmethod
            def list_seed_actions(**_: object):
                return []

            @staticmethod
            def list_master_table_specs(**_: object):
                return {"tables": []}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_db_tab
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_db_tab(Adapter(backend), "development", "config/common_vrp.dev.json")

        initial = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertEqual(len(initial.exception), 0)
        self.assertNotIn("Type the migration phrase exactly", [item.label for item in initial.text_input])
        reviewed = next(item for item in initial.button if item.label == "Review execution").click().run(timeout=10)
        self.assertEqual(FakeBackend.execute_calls, 0)
        self.assertIn("Confirm migration", [item.label for item in reviewed.button])
        confirmed = next(item for item in reviewed.button if item.label == "Confirm migration").click().run(timeout=10)
        self.assertEqual(len(confirmed.exception), 0)
        self.assertEqual(FakeBackend.execute_calls, 1)

    def test_managed_heavy_dataset_db_preview_then_confirm_apply(self) -> None:
        class FakeBackend:
            apply_calls = 0

            @staticmethod
            def list_managed_data_sets(*, scope: str):
                return {
                    "status": "ok",
                    "scope": scope,
                    "datasets": [{
                        "dataset_id": "historical_demand",
                        "label": "Historical demand",
                        "description": "Versioned heavy operational input",
                        "allowed_file_types": ["csv"],
                        "db_sync_supported": True,
                    }],
                }

            @staticmethod
            def list_managed_data_versions(**_: object):
                return {
                    "status": "ok",
                    "versions": [{
                        "version": "demand-v1",
                        "sha256": "a" * 64,
                        "row_count": 12,
                        "updated_at": "2026-07-21T00:00:00Z",
                    }],
                }

            @staticmethod
            def preview_managed_data_upload(**_: object):
                return {"status": "ready"}

            @staticmethod
            def upload_managed_data_file(**_: object):
                return {"status": "uploaded", "version": "demand-v2"}

            @staticmethod
            def preview_managed_data_version(**_: object):
                return {"status": "ready", "version": "demand-v1", "row_count": 12}

            @staticmethod
            def preview_managed_data_db_sync(
                *, dataset_id: str, version: str, target_environment: str
            ):
                return {
                    "status": "ready",
                    "dataset_id": dataset_id,
                    "version": version,
                    "target_environment": target_environment,
                    "preview_id": "preview-1",
                    "preview_digest": "b" * 64,
                    "create_count": 3,
                    "update_count": 2,
                    "unchanged_count": 7,
                }

            @classmethod
            def apply_managed_data_db_sync(cls, **kwargs: object):
                cls.apply_calls += 1
                cls.apply_kwargs = kwargs
                return {"status": "applied", "operation_id": "123e4567-e89b-12d3-a456-426614174000"}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_data_management
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_data_management(Adapter(backend), "config/server_deploy.local.json")

        initial = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertIn("DB-sync", " ".join(item.value for item in initial.info))
        self.assertNotIn("Master CSV", " ".join(item.value for item in initial.markdown))
        previewed = next(
            item for item in initial.button if item.label == "Preview DB update"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.apply_calls, 0)
        self.assertIn("Confirm Apply", [item.label for item in previewed.button])
        completed = next(
            item for item in previewed.button if item.label == "Confirm Apply"
        ).click().run(timeout=10)
        self.assertEqual(len(completed.exception), 0)
        self.assertEqual(FakeBackend.apply_calls, 1)
        self.assertNotIn("table_name", FakeBackend.apply_kwargs)
        self.assertTrue(FakeBackend.apply_kwargs["confirm"])

    def test_common_symptom_dataset_is_upload_only_and_scope_separated(self) -> None:
        class FakeBackend:
            scopes: list[str] = []

            @classmethod
            def list_managed_data_sets(cls, *, scope: str):
                cls.scopes.append(scope)
                datasets = []
                if scope == "common":
                    datasets = [{
                        "id": "symptom_catalog",
                        "description": "Common symptom mapping",
                        "extensions": ["csv"],
                        "allowed_targets": [],
                        "db_profile": None,
                        "PII": False,
                    }]
                return {"status": "ok", "scope": scope, "datasets": datasets}

            @staticmethod
            def list_managed_data_versions(**_: object):
                return {"status": "ok", "versions": []}

            @staticmethod
            def preview_managed_data_upload(**_: object):
                return {"status": "ready"}

            @staticmethod
            def upload_managed_data_file(**_: object):
                return {"status": "uploaded", "version": "symptom-v1"}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_data_management
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_data_management(Adapter(backend), "config/server_deploy.local.json")

        initial = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        common = next(
            item for item in initial.button_group if item.label == "Data scope"
        ).set_value("common").run(timeout=10)
        self.assertIn("common", FakeBackend.scopes)
        self.assertTrue(any("Upload-only" in item.value for item in common.info))
        self.assertNotIn("### Database", [item.value for item in common.markdown])
        self.assertNotIn("Preview DB update", [item.label for item in common.button])
        self.assertEqual(len(common.file_uploader), 1)

    def test_managed_file_upload_is_two_click_and_failure_is_secret_free(self) -> None:
        class FakeBackend:
            upload_calls = 0
            confirm_value = False
            fail = False

            @staticmethod
            def list_managed_data_sets(*, scope: str):
                return {
                    "status": "ok",
                    "scope": scope,
                    "datasets": [{
                        "dataset_id": "daily_input",
                        "label": "Daily input",
                        "allowed_file_types": ["csv"],
                        "db_sync_supported": False,
                    }],
                }

            @staticmethod
            def list_managed_data_versions(**_: object):
                return {"status": "ok", "versions": []}

            @staticmethod
            def preview_managed_data_upload(*, file_bytes: bytes, **_: object):
                digest = hashlib.sha256(file_bytes).hexdigest()
                return {
                    "status": "ready",
                    "sha256": digest,
                    "version": "daily-v1",
                    "row_count": 1,
                }

            @classmethod
            def upload_managed_data_file(cls, *, confirm: bool, **_: object):
                cls.upload_calls += 1
                cls.confirm_value = confirm
                if cls.fail:
                    raise RuntimeError("remote failure password=do-not-render token=hidden")
                return {"status": "uploaded", "version": "daily-v1"}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_data_management
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_data_management(Adapter(backend), "config/server_deploy.local.json")

        initial = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        selected = initial.file_uploader[0].set_value(
            ("daily.csv", b"id\n1\n", "text/csv")
        ).run(timeout=10)
        validated = next(
            item for item in selected.button if item.label == "Validate managed data file"
        ).click().run(timeout=10)
        pending = next(
            item for item in validated.button if item.label == "Upload reviewed file"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 0)
        completed = next(
            item for item in pending.button if item.label == "Confirm managed data upload"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.upload_calls, 1)
        self.assertTrue(FakeBackend.confirm_value)
        self.assertTrue(any("upload completed" in item.value.lower() for item in completed.success))

        FakeBackend.fail = True
        FakeBackend.upload_calls = 0
        failed_app = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        failed_app = failed_app.file_uploader[0].set_value(
            ("daily.csv", b"id\n1\n", "text/csv")
        ).run(timeout=10)
        failed_app = next(
            item for item in failed_app.button if item.label == "Validate managed data file"
        ).click().run(timeout=10)
        failed_app = next(
            item for item in failed_app.button if item.label == "Upload reviewed file"
        ).click().run(timeout=10)
        failed = next(
            item for item in failed_app.button if item.label == "Confirm managed data upload"
        ).click().run(timeout=10)
        visible = " ".join(item.value for item in failed.error)
        self.assertNotIn("do-not-render", visible)
        self.assertNotIn("token=hidden", visible)
        self.assertNotIn("remote failure", visible)

    def test_managed_data_hides_legacy_region_controls_and_keeps_technician_binding(self) -> None:
        class FakeBackend:
            @staticmethod
            def list_managed_data_sets(*, scope: str):
                return {
                    "status": "ok",
                    "scope": scope,
                    "datasets": [
                        {
                            "dataset_id": "technician_profile_workbook",
                            "label": "Technician workbook",
                            "allowed_file_types": ["xlsx"],
                            "active_region_binding": {
                                "status": "active",
                                "plan_id": "Atlanta_6area",
                                "plan_version": "region-v7",
                                "employee_name": "must-not-render",
                            },
                        },
                        {
                            "dataset_id": "territory_plan_workbook",
                            "label": "Territory workbook",
                            "allowed_file_types": ["xlsx"],
                        },
                        {
                            "dataset_id": "fixed_region_plan_bundle",
                            "label": "Fixed region plan bundle",
                            "allowed_file_types": ["zip"],
                        },
                    ],
                }

            @staticmethod
            def list_managed_data_versions(**_: object):
                return {"status": "ok", "versions": []}

            @staticmethod
            def preview_managed_data_upload(**_: object):
                return {"status": "ready"}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_data_management
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_data_management(Adapter(backend), "config/server_deploy.local.json")

        rendered = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        markdown = "\n".join(item.value for item in rendered.markdown)
        self.assertIn("Technician Data", markdown)
        self.assertIn("Region Data", markdown)
        self.assertIn("Active region binding", markdown)
        self.assertIn("Technician dataset", [item.label for item in rendered.selectbox])
        self.assertNotIn("Region dataset", [item.label for item in rendered.selectbox])
        visible = " ".join(item.value for item in rendered.info)
        self.assertIn("Region Plans v2", visible)
        self.assertNotIn("migration", visible.lower())
        public_json = " ".join(str(item.value) for item in rendered.json)
        self.assertIn("Atlanta_6area", public_json)
        self.assertNotIn("must-not-render", public_json)

    def test_region_plan_v2_schema_then_la_adopt_review_preview_activate(self) -> None:
        class FakeBackend:
            activated = False

            @staticmethod
            def preview_region_plan_schema(**_: object):
                return {"status": "ready", "schema_id": "common_region_plan_schema_v2", "target_id": "development:vrp_db_dev", "checksum_sha256": "f" * 64}

            @staticmethod
            def install_region_plan_schema(*, confirm: bool, **_: object):
                assert confirm
                return {"status": "reconciled", "schema_id": "common_region_plan_schema_v2", "checksum_sha256": "f" * 64}

            @staticmethod
            def list_region_plan_v2_cities():
                return {"status": "completed", "data": {"cities": [{
                    "subsidiary_id": "LGEAI", "source_city_id": "Los Angeles, CA",
                    "policies": [{"policy_version": "explicit_workbook_membership/v1", "technician_policy_mode": "assigned_region_boundary_spillover"}],
                }]}}

            @staticmethod
            def import_region_plan_v2_workbook(**_: object):
                return {"status": "accepted", "data": {"plan_id": "rp2_LA_6area_abc", "lifecycle": "candidate"}}

            @staticmethod
            def list_region_plan_v2_candidates(**kwargs: object):
                assert kwargs == {"subsidiary_id": "LGEAI", "target_city_id": "LA_6area"}
                return {"status": "completed", "data": {"plans": [{"plan_id": "rp2_LA_6area_abc", "plan_revision": 1, "lifecycle": "candidate"}]}}

            @staticmethod
            def adopt_region_plan_v2_candidate(**kwargs: object):
                assert kwargs["subsidiary_id"] == "LGEAI"
                return {"status": "completed", "data": {"plan": {"plan_id": "rp2_LA_6area_abc", "plan_revision": 1, "activation_revision": 0, "lifecycle": "candidate"}}}

            @staticmethod
            def review_region_plan_v2(**kwargs: object):
                assert (kwargs["subsidiary_id"], kwargs["plan_revision"], kwargs["activation_revision"]) == ("LGEAI", 1, 0)
                return {"status": "completed", "data": {"plan_id": "rp2_LA_6area_abc", "plan_revision": 2, "lifecycle": "reviewed"}}

            @staticmethod
            def preview_region_plan_v2_activation(**kwargs: object):
                assert (kwargs["plan_revision"], kwargs["activation_revision"]) == (2, 0)
                return {"status": "completed", "data": {"plan_id": "rp2_LA_6area_abc", "plan_revision": 2, "activation_revision": 0, "preview_token": "p" * 64}}

            @classmethod
            def activate_region_plan_v2(cls, **kwargs: object):
                assert kwargs["subsidiary_id"] == "LGEAI"
                cls.activated = True
                return {"status": "completed", "data": {"plan_id": "rp2_LA_6area_abc", "activation_revision": 1, "lifecycle": "active"}}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_region_plan_v2
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter
            _render_region_plan_v2(Adapter(backend))

        page = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertNotIn("Schema migration", [item.label for item in page.selectbox])
        page = next(item for item in page.button if item.label == "Prepare common Region Plan schema").click().run(timeout=10)
        page = next(item for item in page.text_input if item.label == "Target city ID").set_value("LA_6area").run(timeout=10)
        page = next(item for item in page.button if item.label == "List candidates for selected city").click().run(timeout=10)
        page = next(item for item in page.button if item.label == "Adopt selected candidate").click().run(timeout=10)
        page = next(item for item in page.button if item.label == "Review").click().run(timeout=10)
        page = next(item for item in page.button if item.label == "Preview activation").click().run(timeout=10)
        page = next(item for item in page.text_input if item.label == "Activation reference").set_value("LA rollout").run(timeout=10)
        page = next(item for item in page.button if item.label == "Activate").click().run(timeout=10)
        self.assertTrue(FakeBackend.activated)
        self.assertTrue(any("active" in str(item.value) for item in page.json))

    def test_production_managed_db_refusal_is_safe_and_disables_apply(self) -> None:
        class FakeBackend:
            @staticmethod
            def list_managed_data_sets(*, scope: str):
                return {
                    "status": "ok",
                    "scope": scope,
                    "datasets": [{
                        "id": "historical_demand",
                        "extensions": ["csv"],
                        "allowed_targets": ["development", "production"],
                        "db_profile": "heavy",
                    }],
                }

            @staticmethod
            def list_managed_data_versions(**_: object):
                return {"status": "ok", "versions": [{"version": "demand-v1"}]}

            @staticmethod
            def preview_managed_data_upload(**_: object):
                return {"status": "ready"}

            @staticmethod
            def upload_managed_data_file(**_: object):
                return {"status": "uploaded"}

            @staticmethod
            def preview_managed_data_db_sync(**_: object):
                raise PermissionError("production disabled password=do-not-render")

            @staticmethod
            def apply_managed_data_db_sync(**_: object):
                raise AssertionError("Apply must remain unavailable after refusal")

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_data_management
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_data_management(Adapter(backend), "config/server_deploy.local.json")

        initial = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        production = next(
            item for item in initial.button_group if item.label == "Database target"
        ).set_value("production").run(timeout=10)
        refused = next(
            item for item in production.button if item.label == "Preview DB update"
        ).click().run(timeout=10)
        visible = " ".join(item.value for item in refused.warning)
        self.assertIn("Production DB update is disabled", visible)
        self.assertNotIn("do-not-render", visible)
        self.assertNotIn("Confirm Apply", [item.label for item in refused.button])

    def test_package_admin_tools_route_never_loads_managed_data(self) -> None:
        class FakeBackend:
            artifact_kinds: list[str] = []

            @staticmethod
            def observe_platform(**_: object):
                return {"services": [], "total": 0, "healthy": 0}

            @classmethod
            def list_artifacts(cls, *, kind: str, **_: object):
                cls.artifact_kinds.append(kind)
                return []

            @staticmethod
            def list_managed_data_sets(**_: object):
                raise AssertionError("Admin Tools source package route must not load managed data")

        def app(backend: object) -> None:
            from deployment_console_ui.app import render_app

            render_app(backend)

        initial = AppTest.from_function(app, args=(FakeBackend(),)).run(timeout=10)
        admin = next(
            item for item in initial.button if item.label.endswith("Admin Tools")
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.artifact_kinds, ["admin-tools"])
        self.assertNotIn("Database administration", [item.label for item in admin.tabs])

    def test_region_plan_workflow_is_development_only_and_schema_is_two_step(self) -> None:
        class FakeBackend:
            schema_preview_calls = 0
            schema_install_calls = 0
            resolution_calls = 0
            review_calls = 0
            activation_calls = 0
            activation_kwargs: dict[str, object] = {}
            download_calls: list[str] = []

            @classmethod
            def preview_region_plan_schema(cls, **_: object):
                cls.schema_preview_calls += 1
                return {
                    "status": "ready",
                    "migration_id": "V001__region_plan",
                    "checksum_sha256": "a" * 64,
                    "statement_count": 3,
                    "statement_types": ["CREATE TABLE"],
                }

            @classmethod
            def install_region_plan_schema(cls, *, confirm: bool, **_: object):
                cls.schema_install_calls += 1
                if not confirm:
                    raise AssertionError("schema confirm required")
                return {
                    "status": "applied",
                    "migration_id": "V001__region_plan",
                    "checksum_sha256": "a" * 64,
                    "statement_count": 3,
                }

            @staticmethod
            def preview_region_plan_resolutions(**_: object):
                return {"status": "ready", "request_sha256": "b" * 64}

            @classmethod
            def apply_region_plan_resolutions(cls, *, confirm: bool, **_: object):
                cls.resolution_calls += 1
                if not confirm:
                    raise AssertionError("resolution confirm required")
                return {
                    "status": "candidate_imported",
                    "lifecycle_stage": "candidate_resolved",
                    "revision": 2,
                    "checksum": "c" * 64,
                    "resolution_digest": "9" * 64,
                }

            @classmethod
            def download_region_plan_resolution_artifact(
                cls, *, artifact_id: str, **_: object
            ):
                cls.download_calls.append(artifact_id)
                return {
                    "status": "ready",
                    "file_name": f"{artifact_id}.csv",
                    "content": b"safe,bounded\n",
                }

            @classmethod
            def review_region_plan(cls, *, confirm: bool, **_: object):
                cls.review_calls += 1
                if not confirm:
                    raise AssertionError("review confirm required")
                return {"status": "reviewed", "revision": 3, "lifecycle_stage": "reviewed"}

            @staticmethod
            def preview_region_plan_activation(**_: object):
                return {
                    "status": "ready",
                    "preview_id": "d" * 64,
                    "preview_digest": "d" * 64,
                    "checksum": "e" * 64,
                    "plan_revision": 3,
                    "expected_activation_revision": 1,
                    "region_count": 6,
                    "postal_count": 297,
                    "technician_count": 16,
                    "boundary_resolution_count": 4,
                }

            @classmethod
            def apply_region_plan_activation(cls, **kwargs: object):
                cls.activation_calls += 1
                cls.activation_kwargs = dict(kwargs)
                return {
                    "status": "activated",
                    "activation_revision": 1,
                    "preview_digest": "d" * 64,
                    "lifecycle_stage": "active",
                }

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_region_plan_workflow
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_region_plan_workflow(
                Adapter(backend),
                scope="development",
                dataset={"dataset_id": "territory_plan_workbook"},
                source_version="f" * 64,
            )

        app = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        self.assertTrue(any("Development verification only" in item.value for item in app.warning))
        schema_previewed = next(
            item for item in app.button if item.label == "Preview region-plan schema"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.schema_preview_calls, 1)
        self.assertEqual(FakeBackend.schema_install_calls, 0)
        schema_installed = next(
            item for item in schema_previewed.button
            if item.label == "Confirm Install Region Plan Schema"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.schema_install_calls, 1)

        working = schema_installed
        for owner in [item for item in working.selectbox if item.label == "Owner"]:
            working = owner.set_value("Zone 2").run(timeout=10)
        for rationale in [item for item in working.text_input if item.label == "Rationale"]:
            working = rationale.set_value("Development boundary verification").run(timeout=10)
        prepared = next(
            item for item in working.button if item.label == "Prepare ambiguity resolutions"
        ).click().run(timeout=10)
        resolved = next(
            item for item in prepared.button
            if item.label == "Confirm Apply Ambiguity Resolutions"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.resolution_calls, 1)
        prepare_downloads = [
            item for item in resolved.button
            if item.label in {
                "Prepare reviewed fixed-region CSV",
                "Prepare technician policy CSV",
                "Prepare boundary policy CSV",
                "Prepare reviewed plan manifest",
            }
        ]
        self.assertEqual(len(prepare_downloads), 4)
        download_ready = prepare_downloads[0].click().run(timeout=10)
        self.assertEqual(FakeBackend.download_calls, ["fixed_region_csv"])
        self.assertEqual(len(download_ready.get("download_button")), 1)

        acknowledged = next(
            item for item in download_ready.checkbox
            if item.label.startswith("I acknowledge this review")
        ).set_value(True).run(timeout=10)
        referenced = next(
            item for item in acknowledged.text_input if item.label == "Review reference"
        ).set_value("DEV-VERIFY-1").run(timeout=10)
        review_prepared = next(
            item for item in referenced.button if item.label == "Prepare Region Plan Review"
        ).click().run(timeout=10)
        reviewed = next(
            item for item in review_prepared.button if item.label == "Confirm Review Region Plan"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.review_calls, 1)
        activation_referenced = next(
            item for item in reviewed.text_input if item.label == "Activation reference"
        ).set_value("DEV-ACTIVATE-1").run(timeout=10)
        activation_previewed = next(
            item for item in activation_referenced.button
            if item.label == "Preview Region Plan Activation"
        ).click().run(timeout=10)
        activation_json = " ".join(str(item.value) for item in activation_previewed.json)
        self.assertIn("297", activation_json)
        activated = next(
            item for item in activation_previewed.button
            if item.label == "Confirm Activate Region Plan"
        ).click().run(timeout=10)
        self.assertEqual(FakeBackend.activation_calls, 1)
        self.assertTrue(FakeBackend.activation_kwargs["confirm"])
        uuid.UUID(str(FakeBackend.activation_kwargs["idempotency_key"]))
        self.assertTrue(any("Development only" in item.value for item in activated.success))

    def test_region_plan_production_and_missing_capabilities_are_fail_closed(self) -> None:
        def panel(scope: str, backend: object) -> None:
            from deployment_console_ui.app import _render_region_plan_workflow
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_region_plan_workflow(
                Adapter(backend),
                scope=scope,
                dataset={"dataset_id": "territory_plan_workbook"},
                source_version="a" * 64,
            )

        production = AppTest.from_function(
            panel, args=("production", object())
        ).run(timeout=10)
        self.assertTrue(any("Production" in item.value for item in production.error))
        self.assertTrue(all(item.disabled for item in production.button))

        unavailable = AppTest.from_function(
            panel, args=("development", object())
        ).run(timeout=10)
        self.assertTrue(any("capability is unavailable" in item.value for item in unavailable.info))

    def test_fixed_region_bundle_rehydrates_reviewed_state_after_fresh_session(self) -> None:
        digest = "a" * 64

        class FakeBackend:
            activation_preview_kwargs: dict[str, object] = {}

            @staticmethod
            def preview_fixed_region_plan_bundle_import(**_kwargs: object):
                return {"status": "ready"}

            @staticmethod
            def apply_fixed_region_plan_bundle_import(**_kwargs: object):
                return {"status": "candidate_imported"}

            @staticmethod
            def get_fixed_region_plan_bundle_status(**_kwargs: object):
                return {
                    "status": "reviewed",
                    "plan_id": f"atlanta_6area_v1_{digest}",
                    "resolution_digest": digest,
                    "revision": 1,
                    "checksum": "b" * 64,
                    "managed_version": "f" * 64,
                    "bundle_sha256": "f" * 64,
                    "verification_only": True,
                    "promotable": False,
                }

            @staticmethod
            def review_region_plan(**_kwargs: object):
                return {"status": "reviewed", "revision": 2}

            @classmethod
            def preview_region_plan_activation(cls, **kwargs: object):
                cls.activation_preview_kwargs = dict(kwargs)
                return {
                    "status": "ready",
                    "preview_id": "c" * 64,
                    "preview_digest": "d" * 64,
                }

            @staticmethod
            def apply_region_plan_activation(**_kwargs: object):
                return {"status": "activated"}

        def panel(backend: object) -> None:
            from deployment_console_ui.app import _render_fixed_region_plan_bundle_workflow
            from deployment_console_ui.backend_adapter import BackendAdapter as Adapter

            _render_fixed_region_plan_bundle_workflow(
                Adapter(backend),
                scope="development",
                dataset={"dataset_id": "fixed_region_plan_bundle"},
                source_version="f" * 64,
            )

        app = AppTest.from_function(panel, args=(FakeBackend(),)).run(timeout=10)
        button = next(
            item
            for item in app.button
            if item.label == "Preview Fixed Region Plan Activation"
        )
        self.assertFalse(button.disabled)
        button.click().run(timeout=10)
        self.assertEqual(
            FakeBackend.activation_preview_kwargs,
            {"environment": "development", "resolution_digest": digest},
        )


if __name__ == "__main__":
    unittest.main()
