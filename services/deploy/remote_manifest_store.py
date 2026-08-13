"""Local database for the deployment console's verified remote file state.

The deployment console deliberately does not connect directly to the routing
PostgreSQL databases.  This small SQLite store is therefore a local control
plane cache, not a substitute for the routing DB.  Rows are written only from
an actual remote observation or a successful post-upload SHA-256 verification.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA = "deployment-remote-manifest/v1"
_LOCK = threading.RLock()


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS deployment_remote_manifest (
            target_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            artifact_kind TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            remote_path TEXT NOT NULL,
            sha256 TEXT,
            size_bytes INTEGER,
            exists_flag INTEGER NOT NULL,
            verified_at TEXT NOT NULL,
            release_id TEXT,
            verification_source TEXT NOT NULL,
            PRIMARY KEY (target_id, remote_path)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS deployment_remote_manifest_lookup
        ON deployment_remote_manifest(target_id, environment, artifact_kind, relative_path)
        """
    )
    connection.commit()
    return connection


def load(
    path: Path,
    *,
    target_id: str,
    environment: str,
    artifact_kind: str,
    remote_paths: Iterable[str],
) -> dict[str, dict[str, Any]]:
    """Load known rows keyed by remote path without touching the remote host."""

    paths = tuple(dict.fromkeys(str(value) for value in remote_paths))
    if not paths:
        return {}
    placeholders = ",".join("?" for _ in paths)
    with _LOCK, _connect(path) as connection:
        rows = connection.execute(
            f"""
            SELECT remote_path, relative_path, sha256, size_bytes, exists_flag,
                   verified_at, release_id, verification_source
            FROM deployment_remote_manifest
            WHERE target_id = ? AND environment = ? AND artifact_kind = ?
              AND remote_path IN ({placeholders})
            """,
            (target_id, environment, artifact_kind, *paths),
        ).fetchall()
    return {str(row["remote_path"]): dict(row) for row in rows}


def record(
    path: Path,
    *,
    target_id: str,
    environment: str,
    artifact_kind: str,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    """Atomically persist verified remote observations."""

    values = []
    for row in rows:
        remote_path = str(row.get("remote_path") or "")
        if not remote_path:
            raise ValueError("Remote manifest row requires remote_path.")
        checksum = row.get("sha256")
        if checksum is not None:
            checksum = str(checksum).lower()
        size = row.get("size_bytes")
        if size is not None:
            size = int(size)
        values.append(
            (
                target_id,
                environment,
                artifact_kind,
                str(row.get("relative_path") or ""),
                remote_path,
                checksum,
                size,
                1 if bool(row.get("exists", checksum is not None)) else 0,
                str(row.get("verified_at") or ""),
                str(row.get("release_id") or "") or None,
                str(row.get("verification_source") or "post_upload_sha256"),
            )
        )
    if not values:
        return
    with _LOCK, _connect(path) as connection:
        connection.executemany(
            """
            INSERT INTO deployment_remote_manifest(
                target_id, environment, artifact_kind, relative_path,
                remote_path, sha256, size_bytes, exists_flag, verified_at,
                release_id, verification_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(target_id, remote_path) DO UPDATE SET
                environment = excluded.environment,
                artifact_kind = excluded.artifact_kind,
                relative_path = excluded.relative_path,
                sha256 = excluded.sha256,
                size_bytes = excluded.size_bytes,
                exists_flag = excluded.exists_flag,
                verified_at = excluded.verified_at,
                release_id = excluded.release_id,
                verification_source = excluded.verification_source
            """,
            values,
        )
        connection.commit()


def clear(path: Path) -> None:
    """Test/support utility to remove the local cache database."""

    with _LOCK:
        if path.exists():
            path.unlink()
