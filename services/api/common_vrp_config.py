from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ENVIRONMENT_DATABASES = {
    "development": "vrp_db_dev",
    "production": "vrp_db",
}

DEFAULT_ROUTING_POLICY = {
    "slot_minutes": 45,
    "default_technician_slot_count": 8,
    "heavy_job_min_service_minutes": 100,
}

DEFAULT_REGION_PLAN_RUNTIME = {
    "production_enabled": False,
}


def normalize_region_plan_runtime(config: dict[str, Any]) -> dict[str, bool]:
    """Return the production Region Plan runtime gate, defaulting to deny."""
    raw_runtime = config.get("region_plan_runtime")
    if raw_runtime is None:
        return dict(DEFAULT_REGION_PLAN_RUNTIME)
    if not isinstance(raw_runtime, dict):
        raise ValueError("region_plan_runtime must be a JSON object.")
    value = raw_runtime.get("production_enabled", False)
    if not isinstance(value, bool):
        raise ValueError("region_plan_runtime.production_enabled must be a boolean.")
    return {"production_enabled": value}


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"routing_policy.{name} must be a positive integer.")
    try:
        numeric = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"routing_policy.{name} must be a positive integer.") from exc
    if str(value).strip() not in {str(numeric), f"{numeric}.0"} or numeric <= 0:
        raise ValueError(f"routing_policy.{name} must be a positive integer.")
    return numeric


def normalize_routing_policy(config: dict[str, Any]) -> dict[str, int]:
    """Validate the optional policy block and preserve legacy defaults."""
    raw_policy = config.get("routing_policy")
    if raw_policy is None:
        return dict(DEFAULT_ROUTING_POLICY)
    if not isinstance(raw_policy, dict):
        raise ValueError("routing_policy must be a JSON object.")
    missing = [key for key in DEFAULT_ROUTING_POLICY if key not in raw_policy]
    if missing:
        raise ValueError("routing_policy is missing required settings: " + ", ".join(missing))
    return {
        key: _positive_integer(raw_policy[key], key)
        for key in DEFAULT_ROUTING_POLICY
    }


def load_and_validate_common_config(
    config_path: Path,
    *,
    expected_port: int | None = None,
    expected_environment: str | None = None,
) -> dict[str, Any]:
    """Load one explicit Common VRP environment config and fail closed."""
    path = config_path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Missing Common VRP config: {path}")
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in Common VRP config {path}: {exc}") from exc
    if not isinstance(config, dict):
        raise ValueError(f"Common VRP config must be a JSON object: {path}")

    environment = str(config.get("environment", "")).strip().lower()
    if environment not in ENVIRONMENT_DATABASES:
        raise ValueError("environment must be exactly 'development' or 'production'.")
    if expected_environment and environment != expected_environment.strip().lower():
        raise ValueError(
            f"Expected {expected_environment} config, but {path} declares {environment}."
        )

    api = config.get("api")
    if not isinstance(api, dict) or "port" not in api:
        raise ValueError("api.port is required.")
    try:
        configured_port = int(api["port"])
    except (TypeError, ValueError) as exc:
        raise ValueError("api.port must be an integer.") from exc
    if not 1 <= configured_port <= 65535:
        raise ValueError("api.port must be between 1 and 65535.")
    if expected_port is not None and configured_port != int(expected_port):
        raise ValueError(
            f"Requested port {expected_port} does not match {environment} api.port {configured_port}."
        )

    database = config.get("database")
    if not isinstance(database, dict):
        raise ValueError("database configuration is required.")
    missing_database_keys = [
        key for key in ("host", "port", "dbname", "user", "password")
        if not str(database.get(key, "")).strip()
    ]
    if missing_database_keys:
        raise ValueError(
            "Missing required database settings: " + ", ".join(missing_database_keys)
        )
    expected_database = ENVIRONMENT_DATABASES[environment]
    database_name = str(database["dbname"]).strip().lower()
    if database_name != expected_database:
        raise ValueError(
            f"{environment} must use database {expected_database}, not {database_name}."
        )
    password = str(database["password"]).strip()
    if password.upper() in {"<REPLACE_ME>", "REPLACE_ME", "CHANGEME"}:
        raise ValueError("Replace the database password placeholder before startup.")
    try:
        database_port = int(database["port"])
    except (TypeError, ValueError) as exc:
        raise ValueError("database.port must be an integer.") from exc
    if not 1 <= database_port <= 65535:
        raise ValueError("database.port must be between 1 and 65535.")

    routing_api_url = str(config.get("routing_api_url", "")).strip().rstrip("/")
    if not routing_api_url:
        raise ValueError("routing_api_url is required.")
    parsed_url = urlparse(routing_api_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
        raise ValueError("routing_api_url must be an absolute http(s) URL.")
    if parsed_url.username or parsed_url.password or parsed_url.query or parsed_url.fragment:
        raise ValueError("routing_api_url must not contain credentials, query, or fragment.")
    if parsed_url.path not in {"", "/"}:
        raise ValueError("routing_api_url must point to the API origin without a path.")
    try:
        routing_port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)
    except ValueError as exc:
        raise ValueError("routing_api_url contains an invalid port.") from exc
    if routing_port != configured_port:
        raise ValueError(
            f"routing_api_url port {routing_port} does not match api.port {configured_port}."
        )

    # Always provide a complete, validated policy to API startup.  Configs
    # written before this block existed retain their historical 45/8/100
    # behavior instead of becoming invalid on deployment.
    config["routing_policy"] = normalize_routing_policy(config)
    config["region_plan_runtime"] = normalize_region_plan_runtime(config)
    return config


def configured_api_url(config: dict[str, Any]) -> str:
    return str(config["routing_api_url"]).strip().rstrip("/")
