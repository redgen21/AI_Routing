from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .common_vrp_db import (
    get_routing_config,
    list_capabilities,
    list_contexts,
    list_engineers,
    list_regions,
    seed_default_masters,
    upsert_routing_config,
)
from .common_vrp_runtime import (
    build_payload_from_inputs,
    get_latest_routing_snapshot,
    refresh_routing_result,
    submit_routing_from_payload,
)


def _build_payload_debug(payload: dict) -> dict:
    jobs = list(payload.get("jobs", []))
    return {
        "job_count": len(jobs),
        "technician_count": len(list(payload.get("technicians", []))),
    }


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=True, default=str).encode("ascii")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
    handler.wfile.flush()


def _read_json_request(handler: BaseHTTPRequestHandler) -> dict:
    content_length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(content_length) if content_length > 0 else b"{}"
    return json.loads(raw.decode("utf-8"))


def _query_value(parsed, key: str, default: str = "") -> str:
    values = parse_qs(parsed.query).get(key, [])
    return str(values[0]).strip() if values else default


class CommonVRPRequestHandler(BaseHTTPRequestHandler):
    server_version = "CommonVRPServer/1.0"

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/v1/common/init":
                seed_default_masters()
                _json_response(self, HTTPStatus.OK, {"status": "ok"})
                return
            if parsed.path == "/api/v1/common/routing-config/upsert":
                payload = _read_json_request(self)
                saved = upsert_routing_config(payload)
                _json_response(self, HTTPStatus.OK, {"saved_rows": saved})
                return
            if parsed.path == "/api/v1/common/routing/build-payload":
                payload = _read_json_request(self)
                built = build_payload_from_inputs(
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("promise_date", "")).strip(),
                    list(payload.get("jobs", [])),
                    list(payload.get("technicians", [])),
                    mode=str(payload.get("mode", "na_general")).strip() or "na_general",
                    return_to_home=bool(payload.get("return_to_home", False)),
                )
                _json_response(self, HTTPStatus.OK, {"payload": built, "debug": _build_payload_debug(built)})
                return
            if parsed.path == "/api/v1/common/routing/submit":
                payload = _read_json_request(self)
                result = submit_routing_from_payload(
                    dict(payload.get("payload", {})),
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("promise_date", "")).strip(),
                )
                _json_response(self, HTTPStatus.OK, result)
                return
            if parsed.path == "/api/v1/common/routing/check":
                payload = _read_json_request(self)
                result = refresh_routing_result(str(payload.get("request_id", "")).strip())
                _json_response(self, HTTPStatus.OK, result)
                return
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
        except Exception as exc:
            _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "INVALID_REQUEST", "message": str(exc)})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/v1/common/contexts":
                _json_response(self, HTTPStatus.OK, list_contexts())
                return
            if parsed.path == "/api/v1/common/engineers":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                df = list_engineers(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/capabilities":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                df = list_capabilities(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/regions":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                df = list_regions(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/routing-config":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                row = get_routing_config(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"row": row})
                return
            if parsed.path == "/api/v1/common/routing/latest":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                promise_date = _query_value(parsed, "promise_date")
                snapshot = get_latest_routing_snapshot(subsidiary_name, strategic_city_name, promise_date)
                _json_response(self, HTTPStatus.OK, {"snapshot": snapshot})
                return
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
        except Exception as exc:
            _json_response(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "SERVER_ERROR", "message": str(exc)})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def run_server(host: str = "0.0.0.0", port: int = 8065) -> None:
    server = ThreadingHTTPServer((host, int(port)), CommonVRPRequestHandler)
    print(f"Common VRP API server listening on http://{host}:{port}")
    server.serve_forever()
