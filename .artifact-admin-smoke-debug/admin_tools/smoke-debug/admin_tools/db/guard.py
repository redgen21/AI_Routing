from __future__ import annotations

import json
from pathlib import Path


ENVIRONMENT_DATABASES = {
    "development": "vrp_db_dev",
    "production": "vrp_db",
}


def require_db_write_allowed(config_path: Path, *, confirm_production: bool = False) -> None:
    """Block accidental writes to production Common VRP data."""
    config = json.loads(config_path.read_text(encoding="utf-8"))
    environment = str(config.get("environment", "")).strip().lower()
    database = config.get("database")
    if environment not in ENVIRONMENT_DATABASES:
        raise ValueError(
            "Database write requires environment to be exactly development or production."
        )
    if not isinstance(database, dict):
        raise ValueError("Database write requires database.dbname in the selected config.")
    database_name = str(database.get("dbname", "")).strip().lower()
    expected_database = ENVIRONMENT_DATABASES[environment]
    if database_name != expected_database:
        raise ValueError(
            f"{environment} database write requires database.dbname={expected_database}, "
            f"not {database_name or '<missing>'}."
        )
    if environment == "development":
        return
    if environment == "production" and confirm_production:
        return
    raise ValueError(
        "Database write is allowed by default only for the development environment. "
        "Use --confirm-production only after verifying the production target."
    )
