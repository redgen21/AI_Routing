from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.api.common_vrp_config import load_and_validate_common_config
from smart_routing.data_catalog import load_na_data_catalog


RUNTIME_DATA_ROLES = ("service_geocoded", "profile_production")
RUNTIME_DIRECTORY_ROLES = ("reviewed_regions_dir", "region_seed_dir")
GENERAL_CONFIG_PATH = Path("config/config.json")
ZCTA_CONFIG_PATHS = ("area_map_usa.zcta_zip_file",)
AREA_MAP_CONFIG_PATHS = {
    "area_map.service_file": "service_geocoded",
    "area_map.profile_file": "profile_production",
    "area_map_usa.service_file": "service_geocoded",
    "area_map_usa.profile_file": "profile_production",
}


def verify_deployment(config_path: Path, expected_environment: str) -> dict[str, object]:
    config = load_and_validate_common_config(
        config_path,
        expected_environment=expected_environment,
    )
    catalog = load_na_data_catalog()
    checked: dict[str, str] = {}
    missing: list[str] = []
    general_payload: dict[str, object] = {}

    general_config = catalog.project_root / GENERAL_CONFIG_PATH
    checked["general_config"] = str(general_config.resolve())
    if not general_config.is_file():
        missing.append(f"general_config: {general_config.resolve()}")
    else:
        loaded_general_payload = json.loads(general_config.read_text(encoding="utf-8"))
        if not isinstance(loaded_general_payload, dict):
            missing.append(f"general_config must be a JSON object: {general_config.resolve()}")
        else:
            general_payload = loaded_general_payload
        if "<REPLACE_ME>" in json.dumps(loaded_general_payload, ensure_ascii=False):
            missing.append(f"general_config contains template placeholders: {general_config.resolve()}")

    for role in RUNTIME_DATA_ROLES:
        path = catalog.resolve(role)
        checked[role] = str(path)
        if not path.is_file():
            missing.append(f"{role}: {path}")
    for role in RUNTIME_DIRECTORY_ROLES:
        path = catalog.resolve(role)
        checked[role] = str(path)
        if not path.is_dir() or not any(path.iterdir()):
            missing.append(f"{role}: {path}")

    for seed in config.get("region_seed_files", []) or []:
        path = Path(str(seed.get("file", "")))
        if not path.is_absolute():
            project_path = catalog.project_root / path
            path = project_path if project_path.is_file() else catalog.resolve("region_seed_dir") / path.name
        role = f"region_seed:{seed.get('strategic_city_name', '')}"
        checked[role] = str(path.resolve())
        if not path.is_file():
            missing.append(f"{role}: {path.resolve()}")

    client_master = catalog.resolve("client_master")
    checked["client_master"] = str(client_master.resolve())
    if not client_master.is_file():
        missing.append(f"client_master: {client_master.resolve()}")
    default_zcta_geometry = catalog.resolve("zcta_geometry")
    checked["zcta_geometry:area_map_default"] = str(default_zcta_geometry.resolve())
    if not default_zcta_geometry.is_file():
        missing.append(f"zcta_geometry:area_map_default: {default_zcta_geometry.resolve()}")
    for config_key in ZCTA_CONFIG_PATHS:
        section_name, field_name = config_key.split(".", 1)
        section = general_payload.get(section_name, {})
        configured_path = section.get(field_name, "") if isinstance(section, dict) else ""
        if not str(configured_path).strip():
            missing.append(f"general_config missing {config_key}: {general_config.resolve()}")
            continue
        zcta_geometry = Path(str(configured_path))
        if not zcta_geometry.is_absolute():
            zcta_geometry = catalog.project_root / zcta_geometry
        checked[f"zcta_geometry:{section_name}"] = str(zcta_geometry.resolve())
        if not zcta_geometry.is_file():
            missing.append(f"zcta_geometry:{section_name}: {zcta_geometry.resolve()}")
    for config_key, default_role in AREA_MAP_CONFIG_PATHS.items():
        section_name, field_name = config_key.split(".", 1)
        section = general_payload.get(section_name, {})
        configured_path = section.get(field_name, "") if isinstance(section, dict) else ""
        data_path = Path(str(configured_path)) if str(configured_path).strip() else catalog.resolve(default_role)
        if not data_path.is_absolute():
            data_path = catalog.project_root / data_path
        checked[f"configured_data:{config_key}"] = str(data_path.resolve())
        if not data_path.is_file():
            missing.append(f"configured_data:{config_key}: {data_path.resolve()}")
    heavy_candidates = [catalog.resolve("heavy_repair_lookup"), catalog.resolve("symptom_mapping")]
    checked["heavy_repair_source"] = " | ".join(str(path.resolve()) for path in heavy_candidates)
    if not any(path.is_file() for path in heavy_candidates):
        missing.append("heavy_repair_source: " + " or ".join(str(path.resolve()) for path in heavy_candidates))

    if missing:
        raise FileNotFoundError("Deployment data hydration is incomplete:\n- " + "\n- ".join(missing))
    return {
        "status": "ok",
        "environment": config["environment"],
        "config": str(config_path.resolve()),
        "catalog": str(catalog.catalog_path),
        "checked": checked,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate config and hydrated runtime data before service start.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--expected-environment", choices=["development", "production"], required=True)
    args = parser.parse_args()
    print(json.dumps(verify_deployment(args.config, args.expected_environment), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
