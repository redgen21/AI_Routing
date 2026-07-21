from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

import pandas as pd


GeocodeBackend = Literal["file", "database"]


class GeocodeStorageConfigurationError(ValueError):
    """Raised when the geocode storage target is missing or ambiguous."""


@runtime_checkable
class GeocodeStore(Protocol):
    """Persistence boundary shared by Census, HERE, and Google geocoders."""

    backend: GeocodeBackend

    def artifact_exists(self, path: Path) -> bool: ...

    def load_cache(self, path: Path) -> pd.DataFrame: ...

    def save_cache(self, path: Path, df: pd.DataFrame) -> None: ...

    def load_attempt_log(self, path: Path) -> pd.DataFrame: ...

    def save_attempt_log(self, path: Path, df: pd.DataFrame) -> None: ...

    def load_daily_log(self, source_bucket: str, path: Path) -> dict[str, int]: ...

    def increment_daily_log(
        self,
        run_date: str,
        added_count: int,
        source_bucket: str,
        path: Path,
    ) -> None: ...


@dataclass(frozen=True)
class FileGeocodeStore:
    """CSV/JSON persistence for local tools and development runs."""

    backend: GeocodeBackend = "file"

    def artifact_exists(self, path: Path) -> bool:
        return path.exists()

    def load_cache(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    def save_cache(self, path: Path, df: pd.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8-sig")

    def load_attempt_log(self, path: Path) -> pd.DataFrame:
        return self.load_cache(path)

    def save_attempt_log(self, path: Path, df: pd.DataFrame) -> None:
        self.save_cache(path, df)

    def load_daily_log(self, source_bucket: str, path: Path) -> dict[str, int]:
        del source_bucket
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if not isinstance(raw, dict):
            raise ValueError(f"Geocode daily log must contain a JSON object: {path}")
        return {str(key): int(value) for key, value in raw.items()}

    def increment_daily_log(
        self,
        run_date: str,
        added_count: int,
        source_bucket: str,
        path: Path,
    ) -> None:
        del source_bucket
        if int(added_count) <= 0:
            return
        daily_log = self.load_daily_log("", path)
        daily_log[run_date] = int(daily_log.get(run_date, 0)) + int(added_count)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(daily_log, handle, indent=2, ensure_ascii=False)


@dataclass(frozen=True)
class DatabaseGeocodeStore:
    """PostgreSQL persistence tied to one explicitly selected config file."""

    config_path: Path
    backend: GeocodeBackend = "database"

    def __post_init__(self) -> None:
        resolved = self.config_path.expanduser().resolve()
        if not resolved.is_file():
            raise GeocodeStorageConfigurationError(
                f"Database geocode storage requires an existing config file: {resolved}"
            )
        object.__setattr__(self, "config_path", resolved)

    def artifact_exists(self, path: Path) -> bool:
        del path
        return True

    def load_cache(self, path: Path) -> pd.DataFrame:
        from .common_vrp_db import load_geocode_cache_df

        return load_geocode_cache_df(path, config_path=self.config_path)

    def save_cache(self, path: Path, df: pd.DataFrame) -> None:
        from .common_vrp_db import (
            GEOCODE_ATTEMPT_RETENTION_DAYS,
            GEOCODE_CACHE_RETENTION_DAYS,
            cleanup_geocode_cache,
            upsert_geocode_cache_df,
        )

        upsert_geocode_cache_df(path, df, config_path=self.config_path)
        cleanup_geocode_cache(
            retention_days=GEOCODE_CACHE_RETENTION_DAYS,
            config_path=self.config_path,
            attempt_retention_days=GEOCODE_ATTEMPT_RETENTION_DAYS,
        )

    def load_attempt_log(self, path: Path) -> pd.DataFrame:
        from .common_vrp_db import load_geocode_attempt_log_df

        return load_geocode_attempt_log_df(path, config_path=self.config_path)

    def save_attempt_log(self, path: Path, df: pd.DataFrame) -> None:
        from .common_vrp_db import (
            GEOCODE_ATTEMPT_RETENTION_DAYS,
            GEOCODE_CACHE_RETENTION_DAYS,
            cleanup_geocode_cache,
            upsert_geocode_attempt_log_df,
        )

        upsert_geocode_attempt_log_df(path, df, config_path=self.config_path)
        cleanup_geocode_cache(
            retention_days=GEOCODE_CACHE_RETENTION_DAYS,
            config_path=self.config_path,
            attempt_retention_days=GEOCODE_ATTEMPT_RETENTION_DAYS,
        )

    def load_daily_log(self, source_bucket: str, path: Path) -> dict[str, int]:
        del path
        from .common_vrp_db import load_geocode_daily_log

        return load_geocode_daily_log(source_bucket, config_path=self.config_path)

    def increment_daily_log(
        self,
        run_date: str,
        added_count: int,
        source_bucket: str,
        path: Path,
    ) -> None:
        del path
        from .common_vrp_db import increment_geocode_daily_log

        increment_geocode_daily_log(
            run_date,
            added_count,
            source_bucket,
            config_path=self.config_path,
        )


def resolve_geocode_store(
    backend: GeocodeBackend | str | None = None,
    database_config_path: Path | str | None = None,
) -> GeocodeStore:
    """Resolve one storage target without ever defaulting to the production DB.

    With no arguments, an explicitly set ``COMMON_VRP_CONFIG_PATH`` selects the
    database store (preserving service behavior); otherwise local file storage is
    selected. Requesting the database backend without an explicit config fails.
    """

    env_config = os.environ.get("COMMON_VRP_CONFIG_PATH", "").strip()
    requested_config = str(database_config_path).strip() if database_config_path is not None else ""
    selected_backend = str(backend).strip().lower() if backend is not None else ""

    if not selected_backend:
        selected_backend = "database" if requested_config or env_config else "file"
    if selected_backend not in {"file", "database"}:
        raise GeocodeStorageConfigurationError(
            f"Unsupported geocode storage backend {backend!r}; expected 'file' or 'database'."
        )
    if selected_backend == "file":
        if requested_config:
            raise GeocodeStorageConfigurationError(
                "database_config_path cannot be combined with the file geocode backend."
            )
        return FileGeocodeStore()

    config_value = requested_config or env_config
    if not config_value:
        raise GeocodeStorageConfigurationError(
            "Database geocode storage requires database_config_path or COMMON_VRP_CONFIG_PATH."
        )
    return DatabaseGeocodeStore(Path(config_value))
