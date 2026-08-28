"""Server-side bridge for Region Plan v2 operations.

The local deployment console invokes this module over SSH.  The bridge then
calls the Region Plan API through the server loopback interface, where its
development-only mutation guard permits the operation.  The local browser
never receives database credentials and the public API does not need to expose
write access.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from admin_tools.db.region_plan_backend import _config_target, _require_development

REQUEST_ROOT = Path("/home/csda/AI_Routing/state/development/region_plan_requests").resolve()
MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_WORKBOOK_BYTES = 24 * 1024 * 1024
PATH_RE = re.compile(r"^/(?:cities|plans/list|imports|adopt|plans/[A-Za-z0-9._-]+(?:/review|/activation-preview|/activate|/retire|/delete)?|retire)$")


def _load_request(path: Path, expected_sha256: str) -> dict:
    resolved = path.resolve()
    if not resolved.is_file() or (REQUEST_ROOT != resolved and REQUEST_ROOT not in resolved.parents):
        raise ValueError("REGION_PLAN_REQUEST_PATH_INVALID")
    raw = resolved.read_bytes()
    if len(raw) > MAX_REQUEST_BYTES or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("REGION_PLAN_REQUEST_CHECKSUM_INVALID")
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("REGION_PLAN_REQUEST_INVALID")
    return value


def _workbook_bytes(path_value: object, expected_sha256: object) -> bytes:
    path = Path(str(path_value or "")).resolve()
    expected = str(expected_sha256 or "").strip().lower()
    if not path.is_file() or (REQUEST_ROOT != path and REQUEST_ROOT not in path.parents):
        raise ValueError("REGION_PLAN_WORKBOOK_PATH_INVALID")
    raw = path.read_bytes()
    if len(raw) > MAX_WORKBOOK_BYTES or not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise ValueError("REGION_PLAN_WORKBOOK_INVALID")
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("REGION_PLAN_WORKBOOK_CHECKSUM_INVALID")
    return raw


def _call_local_api(request: dict) -> dict:
    method = str(request.get("method") or "POST").upper()
    path = str(request.get("path") or "")
    if method not in {"GET", "POST"} or not PATH_RE.fullmatch(path.split("?", 1)[0]):
        raise ValueError("REGION_PLAN_API_PATH_INVALID")
    body = dict(request.get("body") or {})
    if path == "/imports" and "workbook_path" in body:
        raw = _workbook_bytes(body.pop("workbook_path"), body.pop("workbook_sha256", ""))
        body["workbook_base64"] = base64.b64encode(raw).decode("ascii")
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    for name, value in dict(request.get("headers") or {}).items():
        if name in {"Idempotency-Key", "If-Match"} and str(value).strip():
            headers[name] = str(value).strip()
    url = "http://127.0.0.1:8066/api/region-plans/v2" + path
    if method == "GET" and body:
        url += "?" + urlencode({str(k): str(v) for k, v in body.items()})
        encoded = None
    else:
        encoded = json.dumps(body, separators=(",", ":")).encode("utf-8") if method != "GET" else None
    try:
        with urlopen(Request(url, data=encoded, headers=headers, method=method), timeout=45) as response:
            raw = response.read()
            status = response.status
    except HTTPError as exc:
        raw, status = exc.read(), exc.code
    except URLError as exc:
        raise RuntimeError("REGION_PLAN_API_UNAVAILABLE") from exc
    try:
        result = json.loads(raw.decode("utf-8")) if raw else {}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("REGION_PLAN_API_INVALID_RESPONSE") from exc
    if not isinstance(result, dict):
        raise RuntimeError("REGION_PLAN_API_INVALID_RESPONSE")
    result.setdefault("http_status", int(status))
    return result


def run(config: Path, request_path: Path, request_sha256: str) -> dict:
    _database, environment, dbname = _config_target(config)
    _require_development(environment, dbname)
    request = _load_request(request_path, request_sha256)
    return _call_local_api(request)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m admin_tools.db.region_plan_v2_backend")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        result = run(args.config, args.request, args.request_sha256)
    except Exception as exc:
        result = {
            "contract_version": "region-plan/v2",
            "status": "failed",
            "error": {"code": str(exc), "message": "Server-side Region Plan bridge failed.", "retryable": False},
        }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") in {"completed", "accepted"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
