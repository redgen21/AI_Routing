from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from verify_deployment import verify_deployment


class _Catalog:
    def __init__(self, project_root: Path, roles: dict[str, Path]) -> None:
        self.project_root = project_root
        self.catalog_path = project_root / "config/data_catalog.json"
        self._roles = roles

    def resolve(self, role: str) -> Path:
        return self._roles[role]


class DeploymentHydrationTests(unittest.TestCase):
    def test_zcta_gate_checks_default_and_configured_usa_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service_file = root / "shared/service.csv"
            profile_file = root / "shared/profile.xlsx"
            reviewed_dir = root / "shared/reviewed"
            seed_dir = root / "shared/seeds"
            for path in (service_file, profile_file, reviewed_dir / "region.csv", seed_dir / "seed.csv"):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"test")

            (root / "config").mkdir(parents=True, exist_ok=True)
            configured_usa = root / "shared/geo/usa.zip"
            (root / "config/config.json").write_text(
                json.dumps(
                    {
                        "area_map_usa": {"zcta_zip_file": str(configured_usa)},
                    }
                ),
                encoding="utf-8",
            )
            master = root / "shared/reference/client/All_In_One_Master.xlsx"
            heavy = root / "shared/reference/lookups/Notification_Symptom_mapping_20241120_3depth.xlsx"
            heavy_lookup = root / "shared/db_input/lookups/atlanta_heavy_repair_lookup.csv"
            default_zcta = root / "shared/reference/geospatial/tl_2024_us_zcta520.zip"
            for path in (master, heavy, heavy_lookup, default_zcta):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"test")

            catalog = _Catalog(
                root,
                {
                    "service_geocoded": service_file,
                    "profile_production": profile_file,
                    "reviewed_regions_dir": reviewed_dir,
                    "region_seed_dir": seed_dir,
                    "client_master": master,
                    "zcta_geometry": default_zcta,
                    "symptom_mapping": heavy,
                    "heavy_repair_lookup": heavy_lookup,
                },
            )
            config = {"environment": "development", "region_seed_files": []}
            with (
                mock.patch("verify_deployment.load_and_validate_common_config", return_value=config),
                mock.patch("verify_deployment.load_na_data_catalog", return_value=catalog),
            ):
                with self.assertRaisesRegex(FileNotFoundError, "shared.*geo.*usa.zip"):
                    verify_deployment(root / "config_common_vrp.dev.json", "development")

                configured_usa.parent.mkdir(parents=True, exist_ok=True)
                configured_usa.write_bytes(b"usa")
                result = verify_deployment(root / "config_common_vrp.dev.json", "development")

            checked = result["checked"]
            self.assertEqual(checked["zcta_geometry:area_map_default"], str(default_zcta.resolve()))
            self.assertEqual(checked["zcta_geometry:area_map_usa"], str(configured_usa.resolve()))

            config["environment"] = "production"
            with (
                mock.patch("verify_deployment.load_and_validate_common_config", return_value=config),
                mock.patch("verify_deployment.load_na_data_catalog", return_value=catalog),
            ):
                with self.assertRaisesRegex(FileNotFoundError, "does not match the active production data catalog"):
                    verify_deployment(root / "config_common_vrp.json", "production")

                (root / "config/config.json").write_text(
                    json.dumps({"area_map_usa": {"zcta_zip_file": str(default_zcta)}}),
                    encoding="utf-8",
                )
                result = verify_deployment(root / "config_common_vrp.json", "production")
            self.assertEqual(result["status"], "ok")


if __name__ == "__main__":
    unittest.main()
