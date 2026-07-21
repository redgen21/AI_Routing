"""Pure, security-focused helpers used by the deployment console UI."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any, Iterable, Mapping


ARTIFACT_LABELS = {
    "runtime": "Source code runtime",
    "server-data": "Server data",
    "admin-tools": "DB admin tools",
}

_SECRET_ASSIGNMENT = re.compile(
    r"(?i)([\"']?\b(?:password|passwd|pwd|secret|token|api[_-]?key|private[_-]?key)\b"
    r"[\"']?\s*[:=]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_URI_CREDENTIAL = re.compile(
    r"(?i)([a-z][a-z0-9+.-]*://)([^\s:/@]+):([^\s@]+)@"
)
_BEARER = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+")


def redact_text(value: object) -> str:
    """Apply a final presentation-layer redaction to backend messages/journals."""

    text = str(value or "")
    text = _SECRET_ASSIGNMENT.sub(r"\1[REDACTED]", text)
    text = _URI_CREDENTIAL.sub(r"\1[REDACTED]@", text)
    return _BEARER.sub(r"\1 [REDACTED]", text)


def safe_manifest_files(manifest: Mapping[str, Any] | None) -> list[str]:
    """Return normalized relative paths from a manifest, excluding unsafe paths."""

    result: list[str] = []
    for item in (manifest or {}).get("files", []):
        raw = item.get("path") if isinstance(item, Mapping) else item
        if not isinstance(raw, str) or not raw.strip():
            continue
        path = PurePosixPath(raw.replace("\\", "/"))
        if path.is_absolute() or ".." in path.parts:
            continue
        normalized = str(path)
        if normalized not in result:
            result.append(normalized)
    return sorted(result)


def expected_confirmation(action: str, environment: str, identifier: str) -> str:
    """Build the exact phrase required for a state-changing operation."""

    return f"{action.upper()} {environment.lower()} {identifier}".strip()


def confirmation_matches(actual: str, expected: str) -> bool:
    """Confirm exact, case-sensitive user intent without forgiving whitespace."""

    return bool(expected) and actual == expected


def classify_sql(sql: str) -> list[dict[str, str]]:
    """Classify statements for display only; execution policy remains in backend."""

    statements: list[dict[str, str]] = []
    cleaned = re.sub(r"/\*.*?\*/", " ", sql or "", flags=re.DOTALL)
    cleaned = re.sub(r"(?m)--.*$", " ", cleaned)
    for index, statement in enumerate(cleaned.split(";"), start=1):
        compact = " ".join(statement.split())
        if not compact:
            continue
        keyword = compact.split(maxsplit=1)[0].upper()
        if keyword in {"CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT"}:
            category = "DDL"
        elif keyword in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
            category = "DML"
        elif keyword in {"SELECT", "WITH", "EXPLAIN", "SHOW"}:
            category = "READ"
        elif keyword in {"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT"}:
            category = "TRANSACTION"
        else:
            category = "OTHER"
        statements.append(
            {"statement": str(index), "keyword": keyword, "category": category}
        )
    return statements


def public_mapping(value: object, allowed: Iterable[str]) -> dict[str, Any]:
    """Extract only explicitly allowlisted fields from a DTO or mapping."""

    output: dict[str, Any] = {}
    for key in allowed:
        if isinstance(value, Mapping):
            item = value.get(key)
        else:
            item = getattr(value, key, None)
        if item is not None:
            output[key] = item
    return output
