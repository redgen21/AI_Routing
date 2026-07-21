from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from admin_tools.db.release_backend import (
    DatabaseReleaseBackend,
    MigrationSpec,
    classify_sql_statement,
    list_admin_commands,
    prepare_admin_command,
    required_confirmation,
    split_sql_statements,
)


class _FakeCursor:
    def __init__(self, connection: "_FakeConnection") -> None:
        self.connection = connection
        self.description = (("id",), ("name",))

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.connection.executions.append((sql, params))
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise RuntimeError("simulated SQL failure")

    def fetchone(self):
        return self.connection.history_row

    def fetchall(self):
        return self.connection.preview_rows


class _FakeConnection:
    def __init__(
        self,
        *,
        history_row: tuple[str, str] | None = None,
        fail_on: str = "",
        preview_rows: tuple[tuple[object, ...], ...] = (),
    ) -> None:
        self.history_row = history_row
        self.fail_on = fail_on
        self.preview_rows = preview_rows
        self.executions: list[tuple[str, object]] = []
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closes += 1


class DatabaseReleaseBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = self.root / "common_vrp.dev.json"
        self.config.write_text(
            json.dumps(
                {
                    "environment": "development",
                    "database": {"dbname": "vrp_db_dev"},
                }
            ),
            encoding="utf-8",
        )
        self.sql = "create table release_example (id integer primary key);\n"
        self.sql_path = self.root / "V001__create_release_example.sql"
        self.sql_path.write_text(self.sql, encoding="utf-8")
        self.checksum = hashlib.sha256(self.sql_path.read_bytes()).hexdigest()
        self.spec = MigrationSpec(
            "V001__create_release_example",
            "Create a release example table.",
            self.sql_path,
            self.checksum,
            "Apply V002__drop_release_example after confirming no consumers remain.",
            True,
            "V002__drop_release_example",
        )
        self.backend = DatabaseReleaseBackend(
            [self.spec], migrations_root=self.root, statement_timeout_ms=5_000
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _confirmation(self) -> str:
        plan = self.backend.plan(self.spec.migration_id, self.config)
        return plan.required_confirmation

    def test_apply_uses_transaction_timeout_lock_and_history(self) -> None:
        connection = _FakeConnection()
        result = self.backend.apply(
            self.spec.migration_id,
            self.config,
            typed_confirmation=self._confirmation(),
            connection_factory=lambda target: connection,
        )

        self.assertEqual("applied", result.status)
        self.assertEqual(1, connection.commits)
        self.assertEqual(0, connection.rollbacks)
        self.assertEqual(1, connection.closes)
        statements = [sql.lower() for sql, _ in connection.executions]
        self.assertTrue(any("statement_timeout" in sql for sql in statements))
        self.assertTrue(any("pg_advisory_xact_lock" in sql for sql in statements))
        self.assertTrue(any("admin_schema_migration_history" in sql for sql in statements))
        self.assertIn("create table release_example", "\n".join(statements))

    def test_migration_preview_exposes_only_validated_allowlisted_sql(self) -> None:
        preview = self.backend.preview_migration(self.spec.migration_id, self.config)
        self.assertEqual(self.sql_path.read_bytes().decode("utf-8"), preview.sql)
        self.assertEqual(("create_table",), preview.plan.statement_types)
        self.assertEqual(1, len(preview.statements))
        self.assertEqual(self._confirmation(), preview.plan.required_confirmation)

    def test_already_applied_is_idempotent_and_checksum_protected(self) -> None:
        connection = _FakeConnection(history_row=(self.checksum, "success"))
        result = self.backend.apply(
            self.spec.migration_id,
            self.config,
            typed_confirmation=self._confirmation(),
            connection_factory=lambda target: connection,
        )
        self.assertEqual("already_applied", result.status)
        self.assertEqual(0, connection.commits)
        self.assertEqual(1, connection.rollbacks)

        mismatch = _FakeConnection(history_row=("0" * 64, "success"))
        with self.assertRaisesRegex(ValueError, "checksum differs"):
            self.backend.apply(
                self.spec.migration_id,
                self.config,
                typed_confirmation=self._confirmation(),
                connection_factory=lambda target: mismatch,
            )
        self.assertEqual(1, mismatch.rollbacks)

    def test_failed_history_requires_explicit_retry(self) -> None:
        blocked = _FakeConnection(history_row=(self.checksum, "failed"))
        with self.assertRaisesRegex(ValueError, "retry_failed=True"):
            self.backend.apply(
                self.spec.migration_id,
                self.config,
                typed_confirmation=self._confirmation(),
                connection_factory=lambda target: blocked,
            )
        self.assertEqual(1, blocked.rollbacks)

        retry = _FakeConnection(history_row=(self.checksum, "failed"))
        result = self.backend.apply(
            self.spec.migration_id,
            self.config,
            typed_confirmation=self._confirmation(),
            connection_factory=lambda target: retry,
            retry_failed=True,
        )
        self.assertEqual("applied", result.status)
        self.assertEqual(1, retry.commits)

        unknown = _FakeConnection(history_row=(self.checksum, "pending"))
        with self.assertRaisesRegex(ValueError, "Unknown migration history status"):
            self.backend.apply(
                self.spec.migration_id,
                self.config,
                typed_confirmation=self._confirmation(),
                connection_factory=lambda target: unknown,
                retry_failed=True,
            )

    def test_failure_rolls_back_without_commit(self) -> None:
        connection = _FakeConnection(fail_on="create table release_example")
        with self.assertRaisesRegex(RuntimeError, "simulated SQL failure"):
            self.backend.apply(
                self.spec.migration_id,
                self.config,
                typed_confirmation=self._confirmation(),
                connection_factory=lambda target: connection,
            )
        self.assertEqual(0, connection.commits)
        self.assertEqual(1, connection.rollbacks)
        self.assertEqual(1, connection.closes)

    def test_unregistered_or_modified_migration_is_rejected_before_connect(self) -> None:
        with self.assertRaisesRegex(ValueError, "not allowlisted"):
            self.backend.plan("V999__unknown", self.config)
        self.sql_path.write_text("select 1;", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.backend.plan(self.spec.migration_id, self.config)

    def test_forbidden_sql_and_multi_command_preview_are_rejected(self) -> None:
        dangerous = [
            "COPY x TO PROGRAM 'curl attacker';",
            "CREATE ROLE attacker;",
            "DROP DATABASE vrp_db;",
            "CREATE EXTENSION dblink;",
            "SELECT pg_read_file('/etc/passwd');",
            "DO $$ begin null; end $$;",
            "ALTER TABLE x OWNER TO attacker;",
        ]
        for sql in dangerous:
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                statements = split_sql_statements(sql)
                for statement in statements:
                    classify_sql_statement(statement)
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.backend.preview_select(
                "select 1; delete from x",
                self.config,
                typed_confirmation="unused",
                connection_factory=lambda target: _FakeConnection(),
            )
        with self.assertRaisesRegex(ValueError, "INTO"):
            self.backend.preview_select(
                "select * into copied_table from release_example",
                self.config,
                typed_confirmation="unused",
                connection_factory=lambda target: _FakeConnection(),
            )

    def test_select_preview_is_read_only_and_row_limited(self) -> None:
        connection = _FakeConnection(preview_rows=((1, "one"),))
        target = self.backend.plan(self.spec.migration_id, self.config).target
        preview = self.backend.preview_select(
            "select id, name from release_example",
            self.config,
            typed_confirmation=required_confirmation("preview", target, "SELECT"),
            connection_factory=lambda selected: connection,
            row_limit=25,
        )
        self.assertEqual(((1, "one"),), preview.rows)
        self.assertEqual(25, preview.row_limit)
        self.assertEqual(1, connection.rollbacks)
        self.assertEqual(0, connection.commits)
        sql_text = "\n".join(sql.lower() for sql, _ in connection.executions)
        self.assertIn("set transaction read only", sql_text)
        self.assertIn("limit %s", sql_text)

    def test_splitter_keeps_semicolons_inside_literals(self) -> None:
        statements = split_sql_statements("insert into x values ('a;b'); select 1;")
        self.assertEqual(2, len(statements))
        self.assertEqual("insert", classify_sql_statement(statements[0]))
        self.assertEqual("select", classify_sql_statement(statements[1]))

    def test_prepare_seed_command_returns_argv_without_execution(self) -> None:
        catalog = self.root / "catalog.json"
        catalog.write_text("{}", encoding="utf-8")
        target = self.backend.plan(self.spec.migration_id, self.config).target
        confirmation = required_confirmation("run", target, "la_bucket_apply")
        with self.assertRaisesRegex(ValueError, "requires an explicit"):
            prepare_admin_command(
                "la_bucket_apply",
                self.config,
                typed_confirmation=confirmation,
            )
        command = prepare_admin_command(
            "la_bucket_apply",
            self.config,
            typed_confirmation=confirmation,
            data_catalog_path=catalog,
        )
        self.assertIn("--update-db", command.argv)
        self.assertIn("--data-catalog", command.argv)
        self.assertNotIn("--confirm-production", command.argv)
        self.assertIn("la_bucket_apply", {item.command_id for item in list_admin_commands()})
        self.assertEqual(1, len(self.backend.list_migrations(self.config)))
        with self.assertRaisesRegex(ValueError, "not allowlisted"):
            prepare_admin_command("shell", self.config)


if __name__ == "__main__":
    unittest.main()
