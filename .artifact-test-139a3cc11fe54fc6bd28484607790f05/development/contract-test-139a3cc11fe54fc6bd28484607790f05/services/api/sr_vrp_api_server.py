from __future__ import annotations

import argparse
import json
import sys
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from smart_routing.vrp_api_server import VRPRequestHandler


class ServiceVRPRequestHandler(VRPRequestHandler):
    """Add an operational liveness contract without changing routing semantics."""

    def do_GET(self) -> None:  # noqa: N802
        if urlparse(self.path).path == "/api/v1/routing/health":
            body = json.dumps({"status": "ok"}).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()


def run_service(host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, int(port)), ServiceVRPRequestHandler)
    print(f"Smart Routing API server listening on http://{host}:{port}")
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Smart Routing API server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8055)
    args = parser.parse_args()
    run_service(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
