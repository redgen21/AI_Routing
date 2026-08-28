"""Release-local North America data catalog resolver.

Administrative releases use this module instead of the application package so
that an explicit shared-data catalog is the only source of input artifacts.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ADMIN_TOOLS_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CATALOG_PATH = ADMIN_TOOLS_ROOT / "config" / "data_catalog.json"
ROLE_STAGE_PREFIXES = {
    "service_raw": "raw/service",
    "service_geocoded": ("processed/service", "curated/service"),
    "profile_raw": "raw/profile",
    "profile_runtime": ("raw/profile", "processed/profile"),
    "profile_production": ("processed/profile", "curated/profile"),
    "region_candidates_dir": "planning/regions/candidates",
    "reviewed_regions_dir": "reviewed/regions",
    "region_seed_dir": ("db_input/regions", "seeds/regions"),
    "client_master": "reference/client",
    "zcta_geometry": "reference/geospatial",
    "symptom_mapping": "reference/lookups",
    "heavy_repair_lookup": "db_input/lookups",
    "technician_list": "raw/technicians",
    "technician_map": "processed/technicians",
    "atlanta_engineer_region": "db_input/technicians",
    "atlanta_engineer_home": "db_input/technicians",
    "development_runtime_dir": "runtime/development",
    "production_runtime_dir": "runtime/production",
    "reports_dir": "reports",
    "migration_manifest": "catalog",
}
STATE_ROLES = {
    "region_candidates_dir",
    "development_runtime_dir",
    "production_runtime_dir",
    "reports_dir",
}


@dataclass(frozen=True)
class NorthAmericaDataCatalog:
    catalog_path: Path
    admin_tools_root: Path
    data_root: Path
    state_root: Path | None
    active: dict[str, str]

    def resolve(self, role: str) -> Path:
        try:
            configured = self.active[role]
        except KeyError as exc:
            raise KeyError(f"Unknown data catalog role: {role}") from exc
        path = Path(configured)
        role_root = self.state_root if role in STATE_ROLES and self.state_root is not None else self.data_root
        if not path.is_absolute():
            path = role_root / path
        resolved = path.resolve()
        try:
            relative = resolved.relative_to(role_root)
        except ValueError as exc:
            raise ValueError(f"Data catalog role escapes its configured root ({role}): {resolved}") from exc
        expected_prefix = ROLE_STAGE_PREFIXES.get(role)
        allowed_prefixes = (expected_prefix,) if isinstance(expected_prefix, str) else expected_prefix
        relative_text = relative.as_posix()
        if allowed_prefixes and not any(
            relative_text == prefix or relative_text.startswith(f"{prefix}/")
            for prefix in allowed_prefixes
        ):
            raise ValueError(
                f"Data catalog role is in the wrong lifecycle stage ({role}): "
                f"expected one of {allowed_prefixes}, got {relative_text}"
            )
        return resolved

    def require(self, role: str) -> Path:
        path = self.resolve(role)
        if not path.exists():
            raise FileNotFoundError(f"Active data artifact is missing for {role}: {path}")
        return path

    def as_dict(self) -> dict[str, Any]:
        return {
            "catalog_path": str(self.catalog_path),
            "admin_tools_root": str(self.admin_tools_root),
            "data_root": str(self.data_root),
            "state_root": str(self.state_root) if self.state_root is not None else None,
            "active": dict(self.active),
        }


def load_na_data_catalog(catalog_path: Path | str | None = None) -> NorthAmericaDataCatalog:
    configured_path = catalog_path or os.environ.get("NA_DATA_CATALOG_PATH") or DEFAULT_CATALOG_PATH
    path = Path(configured_path)
    if not path.is_absolute():
        path = ADMIN_TOOLS_ROOT / path
    path = path.resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "north-america-routing-data-catalog/v1":
        raise ValueError(f"Unsupported data catalog schema: {payload.get('schema')!r}")
    root_value = Path(str(payload.get("data_root", "data/north_america")))
    data_root = root_value if root_value.is_absolute() else ADMIN_TOOLS_ROOT / root_value
    state_value = payload.get("state_root")
    state_root: Path | None = None
    if state_value is not None and str(state_value).strip():
        configured_state_root = Path(str(state_value))
        state_root = configured_state_root if configured_state_root.is_absolute() else ADMIN_TOOLS_ROOT / configured_state_root
    active = payload.get("active")
    if not isinstance(active, dict) or not active:
        raise ValueError(f"Data catalog has no active artifacts: {path}")
    return NorthAmericaDataCatalog(
        catalog_path=path,
        admin_tools_root=ADMIN_TOOLS_ROOT,
        data_root=data_root.resolve(),
        state_root=state_root.resolve() if state_root is not None else None,
        active={str(key): str(value) for key, value in active.items()},
    )


def na_data_path(role: str, catalog_path: Path | str | None = None) -> Path:
    return load_na_data_catalog(catalog_path).resolve(role)
