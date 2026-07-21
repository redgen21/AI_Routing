import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from smart_routing.data_catalog import load_na_data_catalog


class DataCatalogTests(unittest.TestCase):
    def test_catalog_resolves_relative_roles_under_data_root(self) -> None:
        with TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            catalog_path = tmp_path / "catalog.json"
            catalog_path.write_text(
                json.dumps(
                    {
                        "schema": "north-america-routing-data-catalog/v1",
                        "data_root": str(tmp_path / "data"),
                        "active": {"service_raw": "raw/service/snapshot/service.csv"},
                    }
                ),
                encoding="utf-8",
            )
            catalog = load_na_data_catalog(catalog_path)
            self.assertEqual(
                catalog.resolve("service_raw"),
                (tmp_path / "data" / "raw" / "service" / "snapshot" / "service.csv").resolve(),
            )

    def test_repository_catalog_has_required_pipeline_roles(self) -> None:
        catalog = load_na_data_catalog()
        required = {
            "service_raw",
            "service_geocoded",
            "profile_raw",
            "profile_runtime",
            "profile_production",
            "region_candidates_dir",
            "reviewed_regions_dir",
            "region_seed_dir",
            "development_runtime_dir",
            "production_runtime_dir",
            "reports_dir",
            "client_master",
            "zcta_geometry",
            "symptom_mapping",
            "heavy_repair_lookup",
            "technician_list",
            "technician_map",
            "atlanta_engineer_region",
            "atlanta_engineer_home",
        }
        self.assertLessEqual(required, set(catalog.active))

    def test_catalog_rejects_role_in_wrong_lifecycle_stage(self) -> None:
        with TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            catalog_path = tmp_path / "catalog.json"
            catalog_path.write_text(
                json.dumps(
                    {
                        "schema": "north-america-routing-data-catalog/v1",
                        "data_root": str(tmp_path / "data"),
                        "active": {"service_raw": "reviewed/regions/not_raw.csv"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "wrong lifecycle stage"):
                load_na_data_catalog(catalog_path).resolve("service_raw")

    def test_server_catalog_separates_shared_data_from_environment_state(self) -> None:
        with TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            catalog_path = tmp_path / "catalog.json"
            catalog_path.write_text(
                json.dumps(
                    {
                        "schema": "north-america-routing-data-catalog/v1",
                        "data_root": str(tmp_path / "shared"),
                        "state_root": str(tmp_path / "state" / "development"),
                        "active": {
                            "profile_production": "processed/profile/profile.xlsx",
                            "reports_dir": "reports",
                        },
                    }
                ),
                encoding="utf-8",
            )
            catalog = load_na_data_catalog(catalog_path)
            self.assertEqual(catalog.resolve("profile_production"), (tmp_path / "shared/processed/profile/profile.xlsx").resolve())
            self.assertEqual(catalog.resolve("reports_dir"), (tmp_path / "state/development/reports").resolve())

    def test_legacy_v1_lifecycle_names_remain_readable(self) -> None:
        with TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            catalog_path = tmp_path / "catalog.json"
            catalog_path.write_text(
                json.dumps(
                    {
                        "schema": "north-america-routing-data-catalog/v1",
                        "data_root": str(tmp_path / "data"),
                        "active": {
                            "service_geocoded": "curated/service/service.csv",
                            "profile_production": "curated/profile/profile.xlsx",
                            "region_seed_dir": "seeds/regions",
                        },
                    }
                ),
                encoding="utf-8",
            )
            catalog = load_na_data_catalog(catalog_path)
            self.assertEqual(catalog.resolve("service_geocoded"), (tmp_path / "data/curated/service/service.csv").resolve())
            self.assertEqual(catalog.resolve("region_seed_dir"), (tmp_path / "data/seeds/regions").resolve())


if __name__ == "__main__":
    unittest.main()
