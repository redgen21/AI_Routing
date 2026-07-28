from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.api.common_vrp_config import load_and_validate_common_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Common VRP API for one isolated environment.")
    parser.add_argument("--config", type=Path, required=True, help="Environment-specific Common VRP JSON config.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--expected-environment",
        choices=("development", "production"),
        help="Reject a config for the other environment.",
    )
    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument(
        "--check-config",
        action="store_true",
        help="Validate configuration without connecting to the database or starting a server.",
    )
    action_group.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Initialize schema and seed configured masters, then exit.",
    )
    parser.add_argument(
        "--confirm-production-bootstrap",
        action="store_true",
        help="Required with --bootstrap-only for production.",
    )
    args = parser.parse_args()

    if args.confirm_production_bootstrap and not args.bootstrap_only:
        parser.error("--confirm-production-bootstrap requires --bootstrap-only")

    config_path = args.config.resolve()
    config = load_and_validate_common_config(
        config_path,
        expected_port=args.port,
        expected_environment=args.expected_environment,
    )
    if args.check_config:
        print(f"Valid {config['environment']} Common VRP config: {config_path}")
        return
    os.environ["COMMON_VRP_CONFIG_PATH"] = str(config_path)

    if args.bootstrap_only:
        if config["environment"] == "production" and not args.confirm_production_bootstrap:
            raise PermissionError(
                "Production bootstrap requires --confirm-production-bootstrap."
            )
        from smart_routing.common_vrp_db import init_schema, seed_default_masters

        init_schema(config_path)
        seed_default_masters(config_path)
        print(f"Bootstrapped {config['environment']} Common VRP database.")
        return

    from smart_routing.common_vrp_api_server import run_server

    run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
