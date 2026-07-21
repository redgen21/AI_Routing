"""Fail-closed backend contract for a database release console.

This module intentionally has no default database connector and no subprocess
runner. Callers must inject a connection factory for migrations/previews and may
only obtain an argv specification for allowlisted seed/import commands.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, Sequence

from admin_tools.db.guard import ENVIRONMENT_DATABASES, require_db_write_allowed


__all__ = [
    "AdminCommandSpec",
    "ConnectionFactory",
    "DatabaseReleaseBackend",
    "DatabaseTarget",
    "MigrationPlan",
    "MigrationPreview",
    "MigrationResult",
    "MigrationSpec",
    "PreparedAdminCommand",
    "SelectPreview",
    "classify_sql_statement",
    "list_admin_commands",
    "load_database_target",
    "prepare_admin_command",
    "required_confirmation",
    "split_sql_statements",
]


MIGRATION_ID_RE = re.compile(r"^V[0-9]{3,}__[a-z0-9][a-z0-9_]*$")
SAFE_STATEMENT_TYPES = {
    "select",
    "insert",
    "update",
    "delete",
    "create_table",
    "create_index",
    "alter_table",
    "drop_table",
    "drop_index",
    "comment",
}
FORBIDDEN_SQL_PATTERNS = (
    r"\bcopy\b",
    r"\bprogram\b",
    r"\b(create|alter|drop)\s+(role|user|database)\b",
    r"\b(create|alter|drop)\s+extension\b",
    r"\b(create|alter|drop)\s+(function|procedure|language)\b",
    r"\bcreate\s+(foreign\s+data\s+wrapper|server)\b",
    r"\balter\s+system\b",
    r"\b(do|call|execute|vacuum|reindex|cluster)\b",
    r"\b(dblink|lo_import|lo_export|pg_read_file|pg_read_binary_file|pg_write_file|pg_ls_dir|pg_stat_file)\s*\(",
    r"\b(pg_terminate_backend|pg_cancel_backend|pg_reload_conf|pg_rotate_logfile)\s*\(",
    r"\b(pg_execute_server_program|postgres_fdw|file_fdw|http_post|http_get)\b",
    r"\b(owner\s+to|security\s+definer|set\s+role|session\s+authorization)\b",
    r"\btruncate\b",
)
HISTORY_TABLE_SQL = """
create table if not exists admin_schema_migration_history (
    migration_id text primary key,
    description text not null,
    checksum_sha256 text not null,
    status text not null check (status in ('success', 'failed')),
    rollback_metadata jsonb not null,
    applied_at timestamptz not null default now()
)
"""
ADVISORY_LOCK_KEY = 4_215_207_701


ConnectionFactory = Callable[["DatabaseTarget"], Any]


@dataclass(frozen=True)
class DatabaseTarget:
    environment: str
    dbname: str
    config_path: Path


@dataclass(frozen=True)
class MigrationSpec:
    migration_id: str
    description: str
    sql_path: Path
    checksum_sha256: str
    rollback_instructions: str
    reversible: bool = False
    rollback_migration_id: str | None = None


@dataclass(frozen=True)
class MigrationPlan:
    migration_id: str
    description: str
    checksum_sha256: str
    statement_types: tuple[str, ...]
    statement_count: int
    rollback_instructions: str
    reversible: bool
    rollback_migration_id: str | None
    target: DatabaseTarget
    required_confirmation: str


@dataclass(frozen=True)
class MigrationResult:
    migration_id: str
    target: DatabaseTarget
    status: str
    statement_count: int
    checksum_sha256: str


@dataclass(frozen=True)
class MigrationPreview:
    plan: MigrationPlan
    sql: str
    statements: tuple[str, ...]


@dataclass(frozen=True)
class SelectPreview:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    row_limit: int
    target: DatabaseTarget


@dataclass(frozen=True)
class AdminCommandSpec:
    command_id: str
    description: str
    module: str
    fixed_args: tuple[str, ...]
    writes_database: bool
    accepts_data_catalog: bool = False
    requires_data_catalog: bool = False
    config_flag: str = "--config"
    requires_confirmation: bool = False


@dataclass(frozen=True)
class PreparedAdminCommand:
    command_id: str
    argv: tuple[str, ...]
    target: DatabaseTarget
    writes_database: bool


ADMIN_COMMANDS: Mapping[str, AdminCommandSpec] = MappingProxyType({
    "la_bucket_generate": AdminCommandSpec(
        "la_bucket_generate",
        "Generate LA bucket input and seed files without a database write.",
        "admin_tools.db.seeds.build_la_bucket_vrp_inputs",
        (),
        False,
        True,
        True,
    ),
    "la_bucket_apply": AdminCommandSpec(
        "la_bucket_apply",
        "Generate LA bucket inputs and atomically refresh approved DB masters.",
        "admin_tools.db.seeds.build_la_bucket_vrp_inputs",
        ("--update-db",),
        True,
        True,
        True,
    ),
    "asia_centroid_preview": AdminCommandSpec(
        "asia_centroid_preview",
        "Build the Asia technician centroid export without a database write.",
        "admin_tools.db.seeds.import_asia_technician_centroids",
        (),
        False,
        False,
        False,
        "--common-config",
    ),
    "asia_centroid_apply": AdminCommandSpec(
        "asia_centroid_apply",
        "Apply the Asia technician centroid import.",
        "admin_tools.db.seeds.import_asia_technician_centroids",
        ("--apply",),
        True,
        False,
        False,
        "--common-config",
    ),
    "common_vrp_reset_preview": AdminCommandSpec(
        "common_vrp_reset_preview",
        "Count scoped Common VRP transactional rows without deletion.",
        "admin_tools.db.runners.reset_common_vrp_data",
        (),
        False,
        False,
        False,
        "--config",
        True,
    ),
})


def load_database_target(config_path: Path | str) -> DatabaseTarget:
    path = Path(config_path).expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    environment = str(payload.get("environment", "")).strip().lower()
    # Reuse the project write guard for exact environment -> dbname enforcement.
    require_db_write_allowed(path, confirm_production=environment == "production")
    dbname = str(payload["database"]["dbname"]).strip().lower()
    if dbname != ENVIRONMENT_DATABASES[environment]:
        raise ValueError("Database target does not match the selected environment.")
    return DatabaseTarget(environment, dbname, path)


def required_confirmation(action: str, target: DatabaseTarget, item_id: str) -> str:
    verb = action.strip().upper()
    return f"{verb} {item_id} TO {target.environment.upper()} {target.dbname}"


def _require_confirmation(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"Typed confirmation mismatch. Required: {expected}")


def _strip_comments_and_literals(sql: str) -> str:
    output: list[str] = []
    index = 0
    state = "normal"
    while index < len(sql):
        char = sql[index]
        pair = sql[index : index + 2]
        if state == "normal":
            if pair == "--":
                state = "line_comment"
                output.extend("  ")
                index += 2
                continue
            if pair == "/*":
                state = "block_comment"
                output.extend("  ")
                index += 2
                continue
            if char == "'":
                state = "single_quote"
                output.append(" ")
            elif char == '"':
                state = "double_quote"
                output.append(" ")
            elif char == "$":
                raise ValueError("Dollar-quoted SQL is not allowed in release migrations.")
            else:
                output.append(char)
        elif state == "line_comment":
            output.append("\n" if char == "\n" else " ")
            if char == "\n":
                state = "normal"
        elif state == "block_comment":
            output.append(" ")
            if pair == "*/":
                output.append(" ")
                index += 2
                state = "normal"
                continue
        elif state == "single_quote":
            output.append(" ")
            if pair == "''":
                output.append(" ")
                index += 2
                continue
            if char == "'":
                state = "normal"
        elif state == "double_quote":
            output.append(" ")
            if pair == '""':
                output.append(" ")
                index += 2
                continue
            if char == '"':
                state = "normal"
        index += 1
    if state in {"single_quote", "double_quote", "block_comment"}:
        raise ValueError("Unterminated SQL quote or comment.")
    return "".join(output)


def split_sql_statements(sql: str) -> tuple[str, ...]:
    sanitized = _strip_comments_and_literals(sql)
    boundaries = [index for index, char in enumerate(sanitized) if char == ";"]
    statements: list[str] = []
    start = 0
    for boundary in boundaries + [len(sql)]:
        statement = sql[start:boundary].strip()
        if statement and _strip_comments_and_literals(statement).strip():
            statements.append(statement)
        start = boundary + 1
    return tuple(statements)


def classify_sql_statement(statement: str) -> str:
    normalized = " ".join(_strip_comments_and_literals(statement).lower().split())
    if not normalized:
        raise ValueError("Empty SQL statement is not allowed.")
    for pattern in FORBIDDEN_SQL_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            raise ValueError(f"Forbidden SQL primitive matched: {pattern}")
    prefixes = (
        (r"^select\b", "select"),
        (r"^insert\b", "insert"),
        (r"^update\b", "update"),
        (r"^delete\b", "delete"),
        (r"^create\s+table\b", "create_table"),
        (r"^create\s+(unique\s+)?index\b", "create_index"),
        (r"^alter\s+table\b", "alter_table"),
        (r"^drop\s+table\b", "drop_table"),
        (r"^drop\s+index\b", "drop_index"),
        (r"^comment\s+on\b", "comment"),
    )
    for pattern, statement_type in prefixes:
        if re.search(pattern, normalized):
            return statement_type
    raise ValueError("SQL statement type is not allowed by the release backend.")


def _read_and_validate_migration(spec: MigrationSpec) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    if not MIGRATION_ID_RE.fullmatch(spec.migration_id):
        raise ValueError(f"Invalid versioned migration id: {spec.migration_id}")
    if spec.sql_path.stem != spec.migration_id:
        raise ValueError("Migration file name must exactly match migration_id.")
    sql_bytes = spec.sql_path.read_bytes()
    checksum = hashlib.sha256(sql_bytes).hexdigest()
    if checksum != spec.checksum_sha256.lower():
        raise ValueError(f"Migration checksum mismatch: {spec.migration_id}")
    sql = sql_bytes.decode("utf-8")
    statements = split_sql_statements(sql)
    if not statements:
        raise ValueError("Migration contains no SQL statements.")
    statement_types = tuple(classify_sql_statement(item) for item in statements)
    if any(item not in SAFE_STATEMENT_TYPES for item in statement_types):
        raise ValueError("Migration contains a non-allowlisted statement type.")
    return sql, statements, statement_types


class DatabaseReleaseBackend:
    """Public backend used by a future CLI or UI release console."""

    def __init__(
        self,
        migrations: Iterable[MigrationSpec],
        *,
        migrations_root: Path | str | None = None,
        statement_timeout_ms: int = 30_000,
        advisory_lock_key: int = ADVISORY_LOCK_KEY,
    ) -> None:
        if not 1_000 <= int(statement_timeout_ms) <= 300_000:
            raise ValueError("statement_timeout_ms must be between 1000 and 300000.")
        migration_list = list(migrations)
        self._migrations = {spec.migration_id: spec for spec in migration_list}
        if len(self._migrations) != len(migration_list):
            raise ValueError("Duplicate migration id in allowlist.")
        default_root = Path(__file__).resolve().parent / "migrations"
        self.migrations_root = Path(migrations_root or default_root).expanduser().resolve()
        for spec in migration_list:
            if not re.fullmatch(r"[0-9a-f]{64}", spec.checksum_sha256):
                raise ValueError(f"Migration checksum must be lowercase SHA-256: {spec.migration_id}")
            if not spec.description.strip() or not spec.rollback_instructions.strip():
                raise ValueError("Migration description and rollback instructions are required.")
            if spec.rollback_migration_id and not MIGRATION_ID_RE.fullmatch(spec.rollback_migration_id):
                raise ValueError("rollback_migration_id must be a versioned migration id.")
            try:
                spec.sql_path.expanduser().resolve().relative_to(self.migrations_root)
            except ValueError as exc:
                raise ValueError(
                    f"Allowlisted migration escapes migrations_root: {spec.sql_path}"
                ) from exc
        self.statement_timeout_ms = int(statement_timeout_ms)
        self.advisory_lock_key = int(advisory_lock_key)

    def list_migrations(self, config_path: Path | str) -> tuple[MigrationPlan, ...]:
        return tuple(
            self.plan(migration_id, config_path)
            for migration_id in sorted(self._migrations)
        )

    def preview_migration(
        self,
        migration_id: str,
        config_path: Path | str,
    ) -> MigrationPreview:
        plan = self.plan(migration_id, config_path)
        spec = self._migrations[migration_id]
        sql, statements, _ = _read_and_validate_migration(spec)
        return MigrationPreview(plan=plan, sql=sql, statements=statements)

    def plan(self, migration_id: str, config_path: Path | str) -> MigrationPlan:
        try:
            spec = self._migrations[migration_id]
        except KeyError as exc:
            raise ValueError(f"Migration is not allowlisted: {migration_id}") from exc
        _, statements, statement_types = _read_and_validate_migration(spec)
        target = load_database_target(config_path)
        return MigrationPlan(
            migration_id=spec.migration_id,
            description=spec.description,
            checksum_sha256=spec.checksum_sha256,
            statement_types=statement_types,
            statement_count=len(statements),
            rollback_instructions=spec.rollback_instructions,
            reversible=spec.reversible,
            rollback_migration_id=spec.rollback_migration_id,
            target=target,
            required_confirmation=required_confirmation("apply", target, spec.migration_id),
        )

    def apply(
        self,
        migration_id: str,
        config_path: Path | str,
        *,
        typed_confirmation: str,
        connection_factory: ConnectionFactory,
        retry_failed: bool = False,
    ) -> MigrationResult:
        plan = self.plan(migration_id, config_path)
        _require_confirmation(typed_confirmation, plan.required_confirmation)
        spec = self._migrations[migration_id]
        _, statements, _ = _read_and_validate_migration(spec)
        connection = connection_factory(plan.target)
        try:
            if hasattr(connection, "autocommit"):
                connection.autocommit = False
            with connection.cursor() as cursor:
                cursor.execute(
                    "select set_config('statement_timeout', %s, true)",
                    (str(self.statement_timeout_ms),),
                )
                cursor.execute("select pg_advisory_xact_lock(%s)", (self.advisory_lock_key,))
                cursor.execute(HISTORY_TABLE_SQL)
                cursor.execute(
                    "select checksum_sha256, status from admin_schema_migration_history where migration_id = %s",
                    (migration_id,),
                )
                existing = cursor.fetchone()
                if existing:
                    existing_checksum, status = str(existing[0]), str(existing[1]).lower()
                    if existing_checksum != spec.checksum_sha256:
                        raise ValueError("Applied migration checksum differs from the allowlisted checksum.")
                    if status == "success":
                        connection.rollback()
                        return MigrationResult(
                            migration_id,
                            plan.target,
                            "already_applied",
                            0,
                            spec.checksum_sha256,
                        )
                    if status == "failed" and not retry_failed:
                        raise ValueError("Migration has failed history; explicit retry_failed=True is required.")
                    if status not in {"success", "failed"}:
                        raise ValueError(f"Unknown migration history status: {status}")
                for statement in statements:
                    cursor.execute(statement)
                rollback_metadata = json.dumps(
                    {
                        "reversible": spec.reversible,
                        "rollback_migration_id": spec.rollback_migration_id,
                        "instructions": spec.rollback_instructions,
                    },
                    ensure_ascii=False,
                )
                cursor.execute(
                    """
                    insert into admin_schema_migration_history
                        (migration_id, description, checksum_sha256, status, rollback_metadata, applied_at)
                    values (%s, %s, %s, 'success', %s::jsonb, now())
                    on conflict (migration_id) do update set
                        description = excluded.description,
                        checksum_sha256 = excluded.checksum_sha256,
                        status = excluded.status,
                        rollback_metadata = excluded.rollback_metadata,
                        applied_at = excluded.applied_at
                    """,
                    (
                        spec.migration_id,
                        spec.description,
                        spec.checksum_sha256,
                        rollback_metadata,
                    ),
                )
            connection.commit()
            return MigrationResult(
                migration_id,
                plan.target,
                "applied",
                len(statements),
                spec.checksum_sha256,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def preview_select(
        self,
        select_sql: str,
        config_path: Path | str,
        *,
        typed_confirmation: str,
        connection_factory: ConnectionFactory,
        row_limit: int = 100,
    ) -> SelectPreview:
        statements = split_sql_statements(select_sql)
        if len(statements) != 1 or classify_sql_statement(statements[0]) != "select":
            raise ValueError("Preview accepts exactly one read-only SELECT statement.")
        preview_normalized = " ".join(
            _strip_comments_and_literals(statements[0]).lower().split()
        )
        if re.search(r"\binto\b|\bfor\s+(update|share)\b", preview_normalized):
            raise ValueError("Preview SELECT may not use INTO or row-locking clauses.")
        limit = int(row_limit)
        if not 1 <= limit <= 500:
            raise ValueError("Preview row_limit must be between 1 and 500.")
        target = load_database_target(config_path)
        expected = required_confirmation("preview", target, "SELECT")
        _require_confirmation(typed_confirmation, expected)
        connection = connection_factory(target)
        try:
            if hasattr(connection, "autocommit"):
                connection.autocommit = False
            with connection.cursor() as cursor:
                cursor.execute("set transaction read only")
                cursor.execute(
                    "select set_config('statement_timeout', %s, true)",
                    (str(self.statement_timeout_ms),),
                )
                cursor.execute(f"select * from ({statements[0]}) as release_preview limit %s", (limit,))
                rows = tuple(tuple(row) for row in cursor.fetchall())
                columns = tuple(str(item[0]) for item in (cursor.description or ()))
            connection.rollback()
            return SelectPreview(columns, rows, limit, target)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def prepare_admin_command(
    command_id: str,
    config_path: Path | str,
    *,
    typed_confirmation: str = "",
    data_catalog_path: Path | str | None = None,
) -> PreparedAdminCommand:
    try:
        spec = ADMIN_COMMANDS[command_id]
    except KeyError as exc:
        raise ValueError(f"Admin command is not allowlisted: {command_id}") from exc
    target = load_database_target(config_path)
    if spec.writes_database or spec.requires_confirmation:
        expected = required_confirmation("run", target, command_id)
        _require_confirmation(typed_confirmation, expected)
    argv = [
        sys.executable,
        "-m",
        spec.module,
        *spec.fixed_args,
        spec.config_flag,
        str(target.config_path),
    ]
    if spec.requires_data_catalog and data_catalog_path is None:
        raise ValueError(f"{command_id} requires an explicit absolute data catalog path.")
    if target.environment == "production" and spec.writes_database:
        argv.append("--confirm-production")
    if data_catalog_path is not None:
        if not spec.accepts_data_catalog:
            raise ValueError(f"{command_id} does not accept a data catalog.")
        catalog = Path(data_catalog_path).expanduser().resolve()
        if not catalog.is_absolute() or not catalog.is_file():
            raise ValueError("data_catalog_path must be an existing absolute file.")
        argv.extend(["--data-catalog", str(catalog)])
    return PreparedAdminCommand(command_id, tuple(argv), target, spec.writes_database)


def list_admin_commands() -> tuple[AdminCommandSpec, ...]:
    return tuple(ADMIN_COMMANDS[key] for key in sorted(ADMIN_COMMANDS))
