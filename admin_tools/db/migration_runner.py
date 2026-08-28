"""Allowlisted database migration CLI for Admin Tools.

Operators cannot supply SQL or a filesystem path: every change must already be
registered in ``migrations/manifest.json`` with an immutable checksum. Existing
registry entries remain development-only unless they explicitly opt into the
production target.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from admin_tools.db.common_vrp import get_db_connection
from admin_tools.db.release_backend import DatabaseReleaseBackend, MigrationSpec


CONTRACT_VERSION = "db-admin-migration/v1"
MIGRATIONS_ROOT = Path(__file__).resolve().parent / "migrations"
MANIFEST_PATH = MIGRATIONS_ROOT / "manifest.json"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REFERENCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{2,199}$")
_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")
PREVIEW_TTL_SECONDS = 15 * 60


@dataclass(frozen=True)
class MigrationRegistration:
    spec: MigrationSpec
    target_environments: frozenset[str]


def _reference(value: str, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not _REFERENCE_RE.fullmatch(normalized):
        raise ValueError(f"{field} must be a non-secret release reference.")
    return normalized


def load_registry(manifest_path: Path | str | None = None) -> dict[str, MigrationRegistration]:
    """Load the reviewed allowlist without connecting to a database."""

    manifest = Path(MANIFEST_PATH if manifest_path is None else manifest_path).expanduser().resolve()
    root = manifest.parent.resolve()
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Migration registry is unreadable.") from exc
    if not isinstance(payload, Mapping) or payload.get("schema") != "vrp-schema-migration-registry/v1":
        raise ValueError("Migration registry schema is invalid.")
    entries = payload.get("migrations")
    if not isinstance(entries, list) or not entries:
        raise ValueError("Migration registry must contain migrations.")

    result: dict[str, MigrationRegistration] = {}
    previous_id = ""
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ValueError("Migration registry entry is invalid.")
        migration_id = str(entry.get("migration_id", "")).strip()
        if not migration_id or migration_id <= previous_id or migration_id in result:
            raise ValueError("Migration registry ids must be unique and ordered.")
        previous_id = migration_id
        checksum = str(entry.get("checksum_sha256", "")).strip().lower()
        sql_name = str(entry.get("sql_file", "")).strip()
        sql_path = (root / sql_name).resolve()
        try:
            sql_path.relative_to(root)
        except ValueError as exc:
            raise ValueError("Migration SQL path escapes the registry directory.") from exc
        if not sql_path.is_file() or not _SHA256_RE.fullmatch(checksum):
            raise ValueError("Migration registry checksum or SQL file is invalid.")
        if hashlib.sha256(sql_path.read_bytes()).hexdigest() != checksum:
            raise ValueError(f"Migration checksum mismatch: {migration_id}")
        raw_targets = entry.get("target_environments", ["development"])
        if not isinstance(raw_targets, list) or not raw_targets:
            raise ValueError("Migration target_environments is invalid.")
        targets = frozenset(str(item).strip().lower() for item in raw_targets)
        if not targets or not targets.issubset({"development", "production"}):
            raise ValueError("Migration target_environments is invalid.")
        result[migration_id] = MigrationRegistration(
            spec=MigrationSpec(
                migration_id=migration_id,
                description=str(entry.get("description", "")).strip(),
                sql_path=sql_path,
                checksum_sha256=checksum,
                rollback_instructions=str(entry.get("rollback_instructions", "")).strip(),
                reversible=entry.get("reversible") is True,
                rollback_migration_id=(str(entry["rollback_migration_id"]).strip() if entry.get("rollback_migration_id") else None),
            ),
            target_environments=targets,
        )
    return result


def _backend(registry: Mapping[str, MigrationRegistration]) -> DatabaseReleaseBackend:
    root = next(iter(registry.values())).spec.sql_path.parent
    return DatabaseReleaseBackend((item.spec for item in registry.values()), migrations_root=root)


def _production_artifact_manifest(manifest_path: Path | str | None = None) -> str:
    """Return the verified current admin artifact manifest hash.

    The manifest location is derived from this packaged module, never from an
    operator argument. Development previews deliberately do not require it.
    """
    manifest = (
        Path(__file__).resolve().parents[2] / "deploy_manifest.json"
        if manifest_path is None
        else Path(manifest_path).expanduser().resolve()
    )
    try:
        raw = manifest.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PermissionError("Production migration requires a packaged admin artifact manifest.") from exc
    if not isinstance(payload, Mapping) or payload.get("artifact_type") != "db-admin-tools":
        raise PermissionError("Admin artifact manifest is invalid.")
    if payload.get("promotable") is not True or payload.get("source_dirty") is not False:
        raise PermissionError("Production migration requires a clean promotable admin artifact.")
    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise PermissionError("Admin artifact manifest file inventory is invalid.")
    release_root = manifest.parent.resolve()
    seen: set[str] = set()
    required = {
        "admin_tools/db/migration_runner.py",
        "admin_tools/db/release_backend.py",
        "admin_tools/db/common_vrp.py",
        "admin_tools/db/migrations/manifest.json",
    }
    for item in files:
        if not isinstance(item, Mapping):
            raise PermissionError("Admin artifact manifest file inventory is invalid.")
        relative = item.get("path")
        expected_hash = item.get("sha256")
        if not isinstance(relative, str) or not isinstance(expected_hash, str):
            raise PermissionError("Admin artifact manifest file inventory is invalid.")
        normalized = relative.replace("\\", "/")
        if (
            not normalized or normalized.startswith("/") or ".." in Path(normalized).parts
            or normalized in seen or not _SHA256_RE.fullmatch(expected_hash.lower())
        ):
            raise PermissionError("Admin artifact manifest file inventory is invalid.")
        seen.add(normalized)
        candidate = (release_root / normalized).resolve()
        try:
            candidate.relative_to(release_root)
        except ValueError as exc:
            raise PermissionError("Admin artifact manifest path is unsafe.") from exc
        if not candidate.is_file() or hashlib.sha256(candidate.read_bytes()).hexdigest() != expected_hash.lower():
            raise PermissionError("Admin artifact files do not match the approved manifest.")
    if not required.issubset(seen):
        raise PermissionError("Admin artifact manifest is missing migration runtime files.")
    return hashlib.sha256(raw).hexdigest()


def _preview_receipt_root(config_path: Path | str) -> Path:
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    configured = config.get("admin_migration_receipt_root")
    if configured is None:
        root = Path(config_path).resolve().parent / ".admin_migration_receipts"
    elif isinstance(configured, str) and configured.strip():
        root = Path(configured).expanduser().resolve()
    else:
        raise ValueError("admin_migration_receipt_root must be a non-empty path when configured.")
    return root


def _write_preview_receipt(plan: Mapping[str, Any], config_path: Path | str, artifact_hash: str) -> str:
    root = _preview_receipt_root(config_path)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    token = secrets.token_hex(16)
    payload = {
        "token": token,
        "expires_at": int(time.time()) + PREVIEW_TTL_SECONDS,
        "migration_id": plan["migration_id"],
        "checksum_sha256": plan["checksum_sha256"],
        "target_id": plan["target_id"],
        "required_confirmation": plan["required_confirmation"],
        "artifact_manifest_sha256": artifact_hash,
    }
    path = root / f"preview-{token}.json"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
    return token


def _consume_preview_receipt(
    token: str, plan: Mapping[str, Any], config_path: Path | str, artifact_hash: str
) -> None:
    if not _TOKEN_RE.fullmatch(str(token)):
        raise PermissionError("Production migration requires a valid preview token.")
    root = _preview_receipt_root(config_path).resolve()
    path = (root / f"preview-{token}.json").resolve()
    try:
        path.relative_to(root)
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise PermissionError("Production preview receipt is unavailable.") from exc
    expected = {
        "token": token,
        "migration_id": plan["migration_id"],
        "checksum_sha256": plan["checksum_sha256"],
        "target_id": plan["target_id"],
        "required_confirmation": plan["required_confirmation"],
        "artifact_manifest_sha256": artifact_hash,
    }
    if not isinstance(payload, Mapping) or any(payload.get(key) != value for key, value in expected.items()):
        raise PermissionError("Production preview receipt does not match this migration plan.")
    if not isinstance(payload.get("expires_at"), int) or payload["expires_at"] < int(time.time()):
        raise PermissionError("Production preview receipt has expired.")
    path.unlink()


def preview_migration(
    migration_id: str, config_path: Path | str, *, manifest_path: Path | str | None = None
) -> dict[str, Any]:
    registry = load_registry(manifest_path)
    try:
        registration = registry[migration_id]
    except KeyError as exc:
        raise ValueError("Migration is not allowlisted.") from exc
    preview = _backend(registry).preview_migration(migration_id, config_path)
    plan = preview.plan
    result = {
        "contract_version": CONTRACT_VERSION, "status": "ready", "migration_id": plan.migration_id,
        "checksum_sha256": plan.checksum_sha256, "environment": plan.target.environment,
        "dbname": plan.target.dbname, "target_id": f"{plan.target.environment}:{plan.target.dbname}",
        "target_allowed": plan.target.environment in registration.target_environments,
        "statement_count": plan.statement_count, "statement_types": list(plan.statement_types),
        "required_confirmation": plan.required_confirmation,
        "rollback_instructions": plan.rollback_instructions,
    }
    if plan.target.environment == "production" and result["target_allowed"]:
        artifact_hash = _production_artifact_manifest()
        result["preview_token"] = _write_preview_receipt(result, config_path, artifact_hash)
        result["preview_expires_in_seconds"] = PREVIEW_TTL_SECONDS
        result["artifact_manifest_sha256"] = artifact_hash
    return result


def apply_migration(
    migration_id: str, config_path: Path | str, *, typed_confirmation: str,
    confirm_production: bool = False, approval_reference: str = "", backup_reference: str = "",
    retry_failed: bool = False, preview_token: str = "",
    manifest_path: Path | str | None = None,
) -> dict[str, Any]:
    registry = load_registry(manifest_path)
    try:
        registration = registry[migration_id]
    except KeyError as exc:
        raise ValueError("Migration is not allowlisted.") from exc
    backend = _backend(registry)
    plan = backend.plan(migration_id, config_path)
    if plan.target.environment not in registration.target_environments:
        raise PermissionError("Migration is not approved for this environment.")
    approval = backup = artifact_hash = ""
    if plan.target.environment == "production":
        if confirm_production is not True:
            raise PermissionError("Production migration requires --confirm-production.")
        approval = _reference(approval_reference, field="approval_reference")
        backup = _reference(backup_reference, field="backup_reference")
        receipt_plan = {
            "migration_id": plan.migration_id,
            "checksum_sha256": plan.checksum_sha256,
            "target_id": f"{plan.target.environment}:{plan.target.dbname}",
            "required_confirmation": plan.required_confirmation,
        }
        # Bind apply to the caller-supplied one-time preview receipt before
        # opening a DB connection; apply never creates an implicit preview.
        artifact_hash = _production_artifact_manifest()
        _consume_preview_receipt(preview_token, receipt_plan, config_path, artifact_hash)

    def connection_factory(target: Any) -> Any:
        connection = get_db_connection(Path(config_path))
        try:
            with connection.cursor() as cursor:
                cursor.execute("select current_database()")
                row = cursor.fetchone()
            if not row or str(row[0]).strip().lower() != target.dbname:
                raise RuntimeError("Connected database does not match the approved target.")
            return connection
        except Exception:
            connection.close()
            raise

    execution_id = secrets.token_hex(16)
    result = backend.apply(
        migration_id, config_path, typed_confirmation=typed_confirmation,
        connection_factory=connection_factory, retry_failed=retry_failed,
        receipt_metadata={
            "execution_id": execution_id,
            "approval_reference": approval or "development",
            "backup_reference": backup or "development",
            "artifact_manifest_sha256": artifact_hash or "development",
            "preview_token": preview_token or "development",
        },
    )
    return {
        "contract_version": CONTRACT_VERSION, "status": result.status,
        "migration_id": result.migration_id, "checksum_sha256": result.checksum_sha256,
        "statement_count": result.statement_count, "environment": result.target.environment,
        "dbname": result.target.dbname, "target_id": f"{result.target.environment}:{result.target.dbname}",
        "execution_id": execution_id, "approval_reference": approval or None,
        "backup_reference": backup or None,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m admin_tools.db.migration_runner")
    parser.add_argument("--json", action="store_true", dest="json_output")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list")
    listing.add_argument("--config", type=Path, required=True)
    preview = commands.add_parser("preview")
    preview.add_argument("--config", type=Path, required=True)
    preview.add_argument("--migration-id", required=True)
    apply = commands.add_parser("apply")
    apply.add_argument("--config", type=Path, required=True)
    apply.add_argument("--migration-id", required=True)
    apply.add_argument("--confirmation", required=True)
    apply.add_argument("--confirm-production", action="store_true")
    apply.add_argument("--approval-reference", default="")
    apply.add_argument("--backup-reference", default="")
    apply.add_argument("--preview-token", default="")
    apply.add_argument("--retry-failed", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "list":
            registry = load_registry()
            backend = _backend(registry)
            target = backend.plan(next(iter(registry)), args.config).target
            result: Any = {
                "contract_version": CONTRACT_VERSION, "environment": target.environment, "dbname": target.dbname,
                "migrations": [{
                    "migration_id": item.spec.migration_id, "description": item.spec.description,
                    "checksum_sha256": item.spec.checksum_sha256,
                    "target_environments": sorted(item.target_environments),
                    "target_allowed": target.environment in item.target_environments,
                } for item in registry.values()],
            }
        elif args.command == "preview":
            result = preview_migration(args.migration_id, args.config)
        else:
            result = apply_migration(
                args.migration_id, args.config, typed_confirmation=args.confirmation,
                confirm_production=args.confirm_production, approval_reference=args.approval_reference,
                backup_reference=args.backup_reference, retry_failed=args.retry_failed,
                preview_token=args.preview_token,
            )
    except Exception as exc:
        print(json.dumps({"contract_version": CONTRACT_VERSION, "status": "error", "message": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
