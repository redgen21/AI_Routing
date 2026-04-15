from __future__ import annotations

import argparse

from smart_routing.common_vrp_api_server import run_server


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Common VRP API server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8065)
    args = parser.parse_args()
    run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
