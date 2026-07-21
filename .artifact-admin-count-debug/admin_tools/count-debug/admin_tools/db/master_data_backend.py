"""Preview-first administration for a small allowlist of Common VRP masters.

This module is executed on the routing server by the deployment console.  The
browser never supplies SQL or an arbitrary table name.  Preview state and
before-images are stored on the server with owner-only permissions.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
import re
import secrets
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from cryptography.fernet import Fernet, InvalidToken

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:  # pragma: no cover - server dependency, replaced in tests
    psycopg2 = None
    sql = None


CONTRACT_VERSION = "db-admin/v1"
MAX_CSV_BYTES = 16 * 1024 * 1024
MAX_ROWS = 20_000
PREVIEW_TTL_SECONDS = 30 * 60
SYSTEM_COLUMNS = {"created_at", "updated_at"}


@dataclass(frozen=True)
class Column:
    name: str
    parser: Callable[[str], Any] = str
    required: bool = False
    sensitive: bool = False


@dataclass(frozen=True)
class TableSpec:
    table: str
    primary_key: tuple[str, ...]
    classification: str
    columns: tuple[Column, ...] = ()

    @property
    def writable(self) -> bool:
        return bool(self.columns)


def _text(value: str) -> str | None:
    value = value.strip()
    return value or None


def _required_text(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("required text is blank")
    return value


def _center_type(value: str) -> str:
    normalized = _required_text(value).upper()
    if normalized not in {"DMS", "DMS2"}:
        raise ValueError("center_type must be DMS or DMS2")
    return normalized


def _priority_group(value: str) -> str:
    normalized = _required_text(value).upper()
    if normalized not in {"A", "B", "C"}:
        raise ValueError("priority_group must be A, B, or C")
    return normalized


def _boolean(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError("boolean must be true or false")


def _optional_int(value: str) -> int | None:
    value = value.strip()
    return None if not value else int(value)


def _optional_float(value: str) -> float | None:
    value = value.strip()
    return None if not value else float(value)


def _latitude(value: str) -> float | None:
    result = _optional_float(value)
    if result is not None and not -90 <= result <= 90:
        raise ValueError("latitude is outside [-90, 90]")
    return result


def _longitude(value: str) -> float | None:
    result = _optional_float(value)
    if result is not None and not -180 <= result <= 180:
        raise ValueError("longitude is outside [-180, 180]")
    return result


def _optional_date(value: str) -> date | None:
    value = value.strip()
    return None if not value else date.fromisoformat(value)


TECHNICIAN_COLUMNS = (
    Column("subsidiary_name", _required_text, True),
    Column("strategic_city_name", _required_text, True),
    Column("employee_code", _required_text, True),
    Column("employee_name", _required_text, True, True),
    Column("center_type", _center_type, True),
    Column("home_address", _text, False, True),
    Column("home_city", _text),
    Column("home_state", _text),
    Column("home_country", _text),
    Column("home_postal_code", _text, False, True),
    Column("home_latitude", _latitude, False, True),
    Column("home_longitude", _longitude, False, True),
    Column("active_flag", _boolean, True),
    Column("priority_group", _priority_group, True),
    Column("max_home_to_job_min", _optional_int),
)
CAPABILITY_COLUMNS = (
    Column("subsidiary_name", _required_text, True),
    Column("strategic_city_name", _required_text, True),
    Column("employee_code", _required_text, True),
    Column("product_group_code", _required_text, True),
    Column("product_code", _required_text, True),
    Column("repair_allowed", _boolean, True),
    Column("heavy_repair_allowed", _boolean, True),
    Column("priority_score", _optional_int),
    Column("effective_start_date", _optional_date),
    Column("effective_end_date", _optional_date),
)
HEAVY_COLUMNS = (
    Column("product_group_code", _required_text, True),
    Column("product_code", _required_text, True),
    Column("detailed_symptom_code", _required_text, True),
)


TABLE_REGISTRY: dict[str, TableSpec] = {
    "common_avoid_area": TableSpec("common_avoid_area", ("avoid_area_id",), "policy_read_only"),
    "common_geocode_attempt_log": TableSpec("common_geocode_attempt_log", ("address_key", "source_bucket", "attempted_date"), "operational_log"),
    "common_geocode_cache": TableSpec("common_geocode_cache", ("address_key", "source_bucket"), "cache"),
    "common_geocode_daily_log": TableSpec("common_geocode_daily_log", ("run_date", "source_bucket"), "operational_log"),
    "common_heavy_repair_rule_master": TableSpec("common_heavy_repair_rule_master", ("product_group_code", "product_code", "detailed_symptom_code"), "master_upsert_allowed", HEAVY_COLUMNS),
    "common_job_input": TableSpec("common_job_input", ("record_id",), "transactional_input"),
    "common_region_master": TableSpec("common_region_master", ("subsidiary_name", "strategic_city_name", "postal_code"), "approved_region_only"),
    "common_request_technician_input": TableSpec("common_request_technician_input", ("record_id",), "transactional_input"),
    "common_routing_config_master": TableSpec("common_routing_config_master", ("subsidiary_name", "strategic_city_name"), "solver_policy_read_only"),
    "common_routing_request": TableSpec("common_routing_request", ("request_id",), "transactional_request"),
    "common_routing_result": TableSpec("common_routing_result", ("request_id",), "result_read_only"),
    # Capability effective dates are not yet enforced by the routing runtime.
    # Keep this table visible but read-only until that solver contract is fixed.
    "common_technician_capability_master": TableSpec("common_technician_capability_master", ("subsidiary_name", "strategic_city_name", "employee_code", "product_group_code", "product_code"), "effective_date_contract_pending"),
    "common_technician_master": TableSpec("common_technician_master", ("subsidiary_name", "strategic_city_name", "employee_code"), "master_upsert_allowed", TECHNICIAN_COLUMNS),
}
WRITABLE_TABLES = frozenset(name for name, spec in TABLE_REGISTRY.items() if spec.writable)


def _read_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("database"), dict):
        raise ValueError("configuration has no database object")
    environment = str(payload.get("environment", "")).strip().lower()
    if environment not in {"development", "production"}:
        raise ValueError("configuration environment is invalid")
    return payload


def _target(config: Mapping[str, Any]) -> dict[str, str]:
    database = config["database"]
    environment = str(config["environment"]).lower()
    dbname = str(database["dbname"])
    expected = "vrp_db_dev" if environment == "development" else "vrp_db"
    if dbname != expected:
        raise ValueError("database target does not match environment")
    return {"environment": environment, "dbname": dbname, "target_id": f"{environment}:{dbname}"}


def _connect(config: Mapping[str, Any]):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is unavailable")
    database = config["database"]
    return psycopg2.connect(
        host=database["host"], port=int(database["port"]), dbname=database["dbname"],
        user=database["user"], password=database["password"], connect_timeout=10,
    )


def _envelope(target: Mapping[str, str], **values: Any) -> dict[str, Any]:
    return {"contract_version": CONTRACT_VERSION, **target, **values}


def _state_root(config_path: Path, target: Mapping[str, str]) -> Path:
    # Server configs live at <root>/<environment>/config_common_*.json.
    parent = config_path.resolve().parent
    project_root = parent.parent if parent.name in {"development", "production"} else parent
    path = project_root / "state" / target["environment"] / "db_admin"
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    with contextlib_suppress_oserror():
        os.chmod(path, 0o700)
    return path


class contextlib_suppress_oserror:
    def __enter__(self): return self
    def __exit__(self, exc_type, _exc, _tb): return exc_type is not None and issubclass(exc_type, OSError)


def _write_private(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")
    with contextlib_suppress_oserror(): os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    with contextlib_suppress_oserror(): os.chmod(path, 0o600)


def _preview_key(config: Mapping[str, Any], preview_id: str) -> bytes:
    """Derive a preview-at-rest key without persisting another secret."""
    secret = str(config["database"].get("password", "")).encode("utf-8")
    if not secret:
        raise RuntimeError("PREVIEW_ENCRYPTION_UNAVAILABLE")
    digest = hashlib.sha256(secret + b"\0db-admin/v1\0" + preview_id.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest)


def _write_encrypted_preview(path: Path, payload: Mapping[str, Any], config: Mapping[str, Any]) -> None:
    preview_id = str(payload["preview_id"])
    plaintext = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    wrapper = {
        "schema": "db-admin-encrypted-preview/v1",
        "preview_id": preview_id,
        "ciphertext": Fernet(_preview_key(config, preview_id)).encrypt(plaintext).decode("ascii"),
    }
    _write_private(path, wrapper)


def _read_encrypted_preview(path: Path, config: Mapping[str, Any]) -> dict[str, Any]:
    wrapper = json.loads(path.read_text(encoding="utf-8"))
    preview_id = str(wrapper.get("preview_id", ""))
    try:
        plaintext = Fernet(_preview_key(config, preview_id)).decrypt(
            str(wrapper["ciphertext"]).encode("ascii")
        )
    except (InvalidToken, KeyError, ValueError) as exc:
        raise RuntimeError("PREVIEW_STATE_INVALID") from exc
    payload = json.loads(plaintext.decode("utf-8"))
    if payload.get("preview_id") != preview_id:
        raise RuntimeError("PREVIEW_STATE_INVALID")
    return payload


def parse_csv_bytes(data: bytes, table: str) -> list[dict[str, Any]]:
    if table not in WRITABLE_TABLES:
        raise ValueError("TABLE_NOT_ALLOWED")
    if not data or len(data) > MAX_CSV_BYTES or b"\x00" in data:
        raise ValueError("CSV_INVALID")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("CSV_INVALID") from exc
    spec = TABLE_REGISTRY[table]
    expected = [column.name for column in spec.columns]
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if reader.fieldnames != expected or SYSTEM_COLUMNS.intersection(reader.fieldnames or []):
        raise ValueError("CSV_HEADERS_INVALID")
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for number, raw in enumerate(reader, start=2):
        if len(rows) >= MAX_ROWS:
            raise ValueError("CSV_ROW_LIMIT")
        parsed: dict[str, Any] = {}
        for column in spec.columns:
            try:
                value = column.parser(raw.get(column.name, ""))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"CSV_VALUE_INVALID:{number}:{column.name}") from exc
            if column.required and value is None:
                raise ValueError(f"CSV_VALUE_REQUIRED:{number}:{column.name}")
            parsed[column.name] = value
        key = tuple(parsed[name] for name in spec.primary_key)
        if any(value in {None, ""} for value in key) or key in seen:
            raise ValueError(f"CSV_PRIMARY_KEY_INVALID:{number}")
        seen.add(key)
        if table == "common_technician_master":
            latitude = parsed["home_latitude"]
            longitude = parsed["home_longitude"]
            if (latitude is None) != (longitude is None):
                raise ValueError(f"CSV_VALUE_INVALID:{number}:home_coordinate_pair")
            center_type = parsed["center_type"]
            max_minutes = parsed["max_home_to_job_min"]
            if center_type == "DMS2":
                if max_minutes != -1:
                    raise ValueError(f"CSV_VALUE_INVALID:{number}:max_home_to_job_min")
            elif max_minutes is not None and not 1 <= max_minutes <= 1440:
                raise ValueError(f"CSV_VALUE_INVALID:{number}:max_home_to_job_min")
        rows.append(parsed)
    if not rows:
        raise ValueError("CSV_EMPTY")
    return rows


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)): return value.isoformat()
    if isinstance(value, dict): return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)): return [_jsonable(item) for item in value]
    return value


def _canonical_hash(value: Any) -> str:
    raw = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_current(cursor: Any, spec: TableSpec) -> dict[tuple[Any, ...], dict[str, Any]]:
    columns = [column.name for column in spec.columns]
    query = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(",").join(map(sql.Identifier, columns)), sql.Identifier(spec.table)
    )
    cursor.execute(query)
    return {
        tuple(values[columns.index(key)] for key in spec.primary_key): dict(zip(columns, values))
        for values in cursor.fetchall()
    }


def _diff(rows: list[dict[str, Any]], current: Mapping[tuple[Any, ...], Mapping[str, Any]], spec: TableSpec):
    created: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    before: dict[str, Any] = {}
    for row in rows:
        key = tuple(row[name] for name in spec.primary_key)
        old = current.get(key)
        encoded_key = json.dumps(_jsonable(key), separators=(",", ":"))
        if old is None:
            created.append(row)
        elif _canonical_hash(old) == _canonical_hash(row):
            unchanged.append(row)
        else:
            updated.append(row)
            before[encoded_key] = _jsonable(dict(old))
    return created, updated, unchanged, before


def list_specs(config_path: Path) -> dict[str, Any]:
    config = _read_config(config_path); target = _target(config)
    specs = []
    for spec in TABLE_REGISTRY.values():
        write_allowed = spec.writable and target["environment"] == "development"
        specs.append({
            "id": spec.table, "table_name": spec.table, "description": spec.classification,
            "primary_key": list(spec.primary_key), "required_columns": [c.name for c in spec.columns],
            "row_limit": MAX_ROWS, "file_size_limit": MAX_CSV_BYTES,
            "write_allowed": write_allowed, "write_capability": {"allowed": write_allowed, "mode": "csv_upsert" if write_allowed else "read_only"},
        })
    return _envelope(target, status="ok", specs=specs)


def overview(config_path: Path) -> dict[str, Any]:
    config = _read_config(config_path); target = _target(config)
    started = time.monotonic(); rows = []
    connection = _connect(config)
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            for spec in TABLE_REGISTRY.values():
                try:
                    cursor.execute("SELECT to_regclass(%s)", (f"public.{spec.table}",))
                    exists = cursor.fetchone()[0] is not None
                    count = None
                    actual_pk: list[str] = []
                    if exists:
                        cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(spec.table)))
                        count = int(cursor.fetchone()[0])
                        cursor.execute(
                            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid "
                            "AND a.attnum=ANY(i.indkey) WHERE i.indrelid=%s::regclass AND i.indisprimary "
                            "ORDER BY array_position(i.indkey,a.attnum)", (f"public.{spec.table}",),
                        )
                        actual_pk = [str(item[0]) for item in cursor.fetchall()]
                    schema_status = "missing_table" if not exists else ("compatible" if actual_pk == list(spec.primary_key) else "pk_drift")
                    write_allowed = spec.writable and target["environment"] == "development"
                    rows.append({"table": spec.table, "table_name": spec.table, "exists": exists, "row_count": count, "primary_key": actual_pk, "schema_status": schema_status, "write_allowed": write_allowed, "write_capability": "csv_upsert" if write_allowed else "read_only"})
                except Exception:
                    connection.rollback()
                    rows.append({"table": spec.table, "table_name": spec.table, "exists": None, "row_count": None, "primary_key": [], "schema_status": "unreadable", "write_allowed": False, "write_capability": "read_only"})
                    cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            connection.rollback()
    finally:
        connection.close()
    migration = {"registry_status": "legacy_unversioned_schema", "registered_count": 0, "applied_count": 0, "pending_count": 0, "history_status": "absent", "legacy_unversioned_schema": True}
    return _envelope(target, status="ok", target=target, observed_at=datetime.now(timezone.utc).isoformat(), connection={"status": "ok", "latency_ms": round((time.monotonic()-started)*1000)}, migration=migration, tables=rows)


def create_preview(config_path: Path, table: str, csv_path: Path) -> dict[str, Any]:
    config = _read_config(config_path); target = _target(config)
    if target["environment"] != "development":
        raise PermissionError("PRODUCTION_MASTER_WRITES_DISABLED")
    rows = parse_csv_bytes(csv_path.read_bytes(), table); spec = TABLE_REGISTRY[table]
    connection = _connect(config)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            current = _load_current(cursor, spec)
            connection.rollback()
    finally: connection.close()
    created, updated, unchanged, before = _diff(rows, current, spec)
    preview_id = str(uuid.uuid4()); confirmation = secrets.token_urlsafe(32)
    payload = {"preview_id": preview_id, "target": target, "table": table, "rows": _jsonable(rows), "before": before, "current_digest": _canonical_hash(current), "source_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(), "created_at": time.time(), "expires_at_epoch": time.time()+PREVIEW_TTL_SECONDS, "confirmation_token": confirmation}
    digest = _canonical_hash({key: value for key, value in payload.items() if key != "confirmation_token"})
    payload["preview_digest"] = digest
    state = _state_root(config_path, target) / "previews"; state.mkdir(parents=True, exist_ok=True, mode=0o700)
    _write_encrypted_preview(state / f"{preview_id}.json", payload, config)
    sensitive = {column.name for column in spec.columns if column.sensitive}
    samples = []
    for action, values in (("create", created), ("update", updated)):
        for row in values[:5]: samples.append({"action": action, **{key: ("***" if key in sensitive and value not in {None, ""} else _jsonable(value)) for key, value in row.items() if key in spec.primary_key or key not in sensitive}})
    return _envelope(target, status="ready", preview_id=preview_id, preview_digest=digest, confirmation_token=confirmation, expires_at=datetime.fromtimestamp(payload["expires_at_epoch"], timezone.utc).isoformat(), create_count=len(created), update_count=len(updated), unchanged_count=len(unchanged), masked_samples=samples, errors=[])


def _preview_path(config_path: Path, target: Mapping[str, str], preview_id: str) -> Path:
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", preview_id): raise ValueError("PREVIEW_ID_INVALID")
    return _state_root(config_path, target) / "previews" / f"{preview_id}.json"


def _receipt_binding_conflict(
    receipt: Mapping[str, Any], preview_id: str, preview_digest: str
) -> bool:
    return bool(receipt) and (
        receipt.get("preview_id") != preview_id
        or receipt.get("preview_digest") != preview_digest
    )


def apply_preview(config_path: Path, preview_id: str, digest: str, idempotency_key: str, confirmation: str, confirm_production: bool) -> dict[str, Any]:
    config = _read_config(config_path); target = _target(config)
    operation_id = str(uuid.UUID(idempotency_key))
    if target["environment"] != "development":
        raise PermissionError("PRODUCTION_MASTER_WRITES_DISABLED")
    receipts = _state_root(config_path, target) / "receipts"; receipts.mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt_path = receipts / f"{operation_id}.json"
    payload = _read_encrypted_preview(_preview_path(config_path, target, preview_id), config)
    if payload.get("target") != target or payload.get("preview_digest") != digest or payload.get("confirmation_token") != confirmation: raise PermissionError("PREVIEW_MISMATCH")
    if float(payload.get("expires_at_epoch", 0)) < time.time(): raise ValueError("PREVIEW_EXPIRED")
    table = str(payload["table"]); spec = TABLE_REGISTRY.get(table)
    if spec is None or not spec.writable: raise ValueError("TABLE_NOT_ALLOWED")
    rows = list(payload["rows"]); connection = _connect(config)
    pending = _envelope(target, status="pending", operation_id=operation_id, table_name=table, preview_id=preview_id, preview_digest=digest, created_at=datetime.now(timezone.utc).isoformat())
    audits = _state_root(config_path, target) / "audits"; audits.mkdir(parents=True, exist_ok=True, mode=0o700)
    audit_path = audits / f"{operation_id}.json"
    audit_payload: dict[str, Any] | None = None
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30000ms'")
            # Session lock remains held until the durable audit and public
            # receipt are written, closing the commit/receipt race.
            cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (f"db-admin:{table}",))
            if receipt_path.is_file():
                existing_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                if existing_receipt.get("preview_id") != preview_id or existing_receipt.get("preview_digest") != digest:
                    raise RuntimeError("IDEMPOTENCY_CONFLICT")
                if existing_receipt.get("status") == "applied":
                    return existing_receipt
            current = _load_current(cursor, spec)
            created, updated, unchanged, before = _diff(rows, current, spec)
            desired_already_present = not created and not updated
            if desired_already_present and receipt_path.is_file():
                recovered = {**pending, "status": "applied", "recovered": True, "applied_at": datetime.now(timezone.utc).isoformat(), "create_count": 0, "update_count": 0, "unchanged_count": len(unchanged), "digest": digest, "rollback_available": audit_path.is_file()}
                _write_private(receipt_path, recovered)
                return recovered
            if _canonical_hash(current) != payload.get("current_digest"):
                raise RuntimeError("PREVIEW_STALE")
            created_keys = [[_jsonable(row[name]) for name in spec.primary_key] for row in created]
            audit_payload = {"preview_id": preview_id, "operation_id": operation_id, "preview_digest": digest, "target": target, "table": table, "status": "pending", "before": before, "created_keys": created_keys, "recorded_at": datetime.now(timezone.utc).isoformat()}
            _write_encrypted_preview(audit_path, {**audit_payload, "preview_id": preview_id}, config)
            _write_private(receipt_path, pending)
            columns = [column.name for column in spec.columns]
            updates = [name for name in columns if name not in spec.primary_key]
            base = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) ").format(sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, columns)), sql.SQL(",").join(sql.Placeholder() for _ in columns), sql.SQL(",").join(map(sql.Identifier, spec.primary_key)))
            if updates:
                statement = base + sql.SQL("DO UPDATE SET ") + sql.SQL(",").join(sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(name), sql.Identifier(name)) for name in updates)
            else: statement = base + sql.SQL("DO NOTHING")
            for row in rows: cursor.execute(statement, tuple(row.get(name) for name in columns))
        connection.commit()
        assert audit_payload is not None
        audit_payload["status"] = "applied"
        audit_payload["applied_at"] = datetime.now(timezone.utc).isoformat()
        _write_encrypted_preview(audit_path, {**audit_payload, "preview_id": preview_id}, config)
        applied = {**pending, "status": "applied", "applied_at": audit_payload["applied_at"], "create_count": len(created), "update_count": len(updated), "unchanged_count": len(unchanged), "digest": digest, "rollback_available": True}
        _write_private(receipt_path, applied)
        return applied
    except Exception:
        connection.rollback()
        failed = {**pending, "status": "failed", "failed_at": datetime.now(timezone.utc).isoformat()}
        existing_after_error: dict[str, Any] = {}
        if receipt_path.is_file():
            with contextlib_suppress_oserror():
                existing_after_error = json.loads(receipt_path.read_text(encoding="utf-8"))
        # A conflicting reuse of an idempotency key must never corrupt any
        # durable receipt (pending, failed, or applied) from the original
        # operation.  A pending receipt may represent a commit whose process
        # died before final receipt reconciliation.
        binding_conflict = _receipt_binding_conflict(
            existing_after_error, preview_id, digest
        )
        if not binding_conflict and existing_after_error.get("status") != "applied":
            _write_private(receipt_path, failed)
        if audit_payload is not None:
            audit_payload["status"] = "failed"
            _write_encrypted_preview(audit_path, {**audit_payload, "preview_id": preview_id}, config)
        raise
    finally:
        try:
            with connection.cursor() as unlock_cursor:
                unlock_cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"db-admin:{table}",))
        except Exception:
            pass
        connection.close()


def get_receipt(config_path: Path, operation_id: str) -> dict[str, Any]:
    config = _read_config(config_path); target = _target(config); operation_id = str(uuid.UUID(operation_id))
    payload = json.loads((_state_root(config_path, target) / "receipts" / f"{operation_id}.json").read_text(encoding="utf-8"))
    payload.pop("before", None); return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(); parser.add_argument("--json", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("overview", "list-specs"):
        item = commands.add_parser(name); item.add_argument("--config", type=Path, required=True)
    item = commands.add_parser("preview"); item.add_argument("--config", type=Path, required=True); item.add_argument("--table", required=True); item.add_argument("--csv", type=Path, required=True)
    item = commands.add_parser("apply"); item.add_argument("--config", type=Path, required=True); item.add_argument("--preview-id", required=True); item.add_argument("--preview-digest", required=True); item.add_argument("--idempotency-key", required=True); item.add_argument("--confirmation", required=True); item.add_argument("--confirm-production", action="store_true")
    item = commands.add_parser("receipt"); item.add_argument("--config", type=Path, required=True); item.add_argument("--operation-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv); target = {"environment": "unknown", "dbname": "unknown", "target_id": "unknown:unknown"}
    try:
        target = _target(_read_config(args.config))
        if args.command == "overview": result = overview(args.config)
        elif args.command == "list-specs": result = list_specs(args.config)
        elif args.command == "preview": result = create_preview(args.config, args.table, args.csv)
        elif args.command == "apply": result = apply_preview(args.config, args.preview_id, args.preview_digest, args.idempotency_key, args.confirmation, args.confirm_production)
        else: result = get_receipt(args.config, args.operation_id)
        print(json.dumps(result, ensure_ascii=False, default=str)); return 0
    except Exception as exc:
        code = str(exc) if re.fullmatch(r"[A-Z][A-Z0-9_:.-]+", str(exc)) else "DB_ADMIN_FAILED"
        print(json.dumps(_envelope(target, status="error", error_code=code, message="DB administration request failed."))); return 2


if __name__ == "__main__":
    raise SystemExit(main())
