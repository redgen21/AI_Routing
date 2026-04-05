from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .vrp_api_service import create_job_id, load_result, load_status, process_job, save_new_job


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_json_request(handler: BaseHTTPRequestHandler) -> dict:
    content_length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(content_length) if content_length > 0 else b"{}"
    return json.loads(raw.decode("utf-8"))


class VRPRequestHandler(BaseHTTPRequestHandler):
    server_version = "VRPServer/1.0"

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/v1/routing/jobs":
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
            return
        try:
            request_payload = _read_json_request(self)
            request_id = str(request_payload.get("request_id", "")).strip()
            job_id = create_job_id(request_id)
            save_new_job(job_id, request_payload)
            thread = threading.Thread(target=process_job, args=(job_id,), daemon=True)
            thread.start()
            _json_response(
                self,
                HTTPStatus.ACCEPTED,
                {
                    "job_id": job_id,
                    "status": "queued",
                },
            )
        except Exception as exc:
            _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "INVALID_REQUEST", "message": str(exc)})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path_parts = [part for part in parsed.path.split("/") if part]
        try:
            if len(path_parts) == 5 and path_parts[:4] == ["api", "v1", "routing", "jobs"]:
                job_id = path_parts[4]
                _json_response(self, HTTPStatus.OK, load_status(job_id))
                return
            if len(path_parts) == 6 and path_parts[:4] == ["api", "v1", "routing", "jobs"] and path_parts[5] == "result":
                job_id = path_parts[4]
                status_payload = load_status(job_id)
                if status_payload.get("status") != "completed":
                    _json_response(self, HTTPStatus.CONFLICT, status_payload)
                    return
                _json_response(self, HTTPStatus.OK, load_result(job_id))
                return
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
        except FileNotFoundError:
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "JOB_NOT_FOUND"})
        except Exception as exc:
            _json_response(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "SERVER_ERROR", "message": str(exc)})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def run_server(host: str = "0.0.0.0", port: int = 8055) -> None:
    server = ThreadingHTTPServer((host, int(port)), VRPRequestHandler)
    print(f"Smart Routing API server listening on http://{host}:{port}")
    server.serve_forever()
