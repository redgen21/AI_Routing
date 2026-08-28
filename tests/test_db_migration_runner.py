from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from admin_tools.db import migration_runner


class MigrationRunnerRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sql = self.root / "V006__reviewed_change.sql"
        self.sql.write_text(
            "create table reviewed_change (id integer primary key);\n", encoding="utf-8"
        )
        checksum = hashlib.sha256(self.sql.read_bytes()).hexdigest()
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps({
            "schema": "vrp-schema-migration-registry/v1",
            "migrations": [{
                "migration_id": "V006__reviewed_change",
                "description": "A reviewed generic change.",
                "sql_file": self.sql.name,
                "checksum_sha256": checksum,
                "rollback_instructions": "Use a reviewed forward fix.",
                "reversible": False,
                "rollback_migration_id": None,
                "target_environments": ["development", "production"],
            }],
        }), encoding="utf-8")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_registry_defaults_legacy_entries_to_development_only(self) -> None:
        payload = json.loads(self.manifest.read_text(encoding="utf-8"))
        payload["migrations"][0].pop("target_environments")
        self.manifest.write_text(json.dumps(payload), encoding="utf-8")
        registry = migration_runner.load_registry(self.manifest)
        self.assertEqual({"development"}, registry["V006__reviewed_change"].target_environments)

    def test_registry_rejects_checksum_tampering(self) -> None:
        self.sql.write_text("select 1;\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            migration_runner.load_registry(self.manifest)

    def test_artifact_manifest_rehashes_packaged_migration_files(self) -> None:
        release = self.root / "release"
        required = (
            "admin_tools/db/migration_runner.py",
            "admin_tools/db/release_backend.py",
            "admin_tools/db/common_vrp.py",
            "admin_tools/db/migrations/manifest.json",
        )
        files = []
        for relative in required:
            path = release / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(relative, encoding="utf-8")
            files.append({"path": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        manifest = release / "deploy_manifest.json"
        manifest.write_text(json.dumps({
            "artifact_type": "db-admin-tools", "promotable": True,
            "source_dirty": False, "files": files,
        }), encoding="utf-8")
        self.assertEqual(hashlib.sha256(manifest.read_bytes()).hexdigest(), migration_runner._production_artifact_manifest(manifest))
        (release / required[0]).write_text("tampered", encoding="utf-8")
        with self.assertRaisesRegex(PermissionError, "do not match"):
            migration_runner._production_artifact_manifest(manifest)

    def test_production_apply_requires_explicit_approval_before_connecting(self) -> None:
        config = self.root / "common_vrp.prod.json"
        config.write_text(
            json.dumps({"environment": "production", "database": {"dbname": "vrp_db"}}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PermissionError, "--confirm-production"):
            migration_runner.apply_migration(
                "V006__reviewed_change",
                config,
                typed_confirmation="APPLY V006__reviewed_change TO PRODUCTION vrp_db",
                manifest_path=self.manifest,
            )

    def test_production_apply_requires_a_preview_token_before_connecting(self) -> None:
        config = self.root / "common_vrp.prod.json"
        config.write_text(
            json.dumps({"environment": "production", "database": {"dbname": "vrp_db"}}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PermissionError, "packaged admin artifact manifest"):
            migration_runner.apply_migration(
                "V006__reviewed_change",
                config,
                typed_confirmation="APPLY V006__reviewed_change TO PRODUCTION vrp_db",
                confirm_production=True,
                approval_reference="CHG-1234",
                backup_reference="backup-1234",
                preview_token="a" * 32,
                manifest_path=self.manifest,
            )


if __name__ == "__main__":
    unittest.main()
