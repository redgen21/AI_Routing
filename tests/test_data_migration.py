import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from smart_routing.data_catalog import PROJECT_ROOT
from tools.data.migrate_legacy_layout import (
    _build_atlanta_reviewed_with_full_coverage,
    _build_minimized_technician_map,
    _copy_verified,
)


class AtlantaRegionMigrationTests(unittest.TestCase):
    def test_server_technician_map_removes_home_address_columns(self) -> None:
        temporary_root = PROJECT_ROOT / "data" / "_tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=temporary_root) as directory:
            root = Path(directory)
            source = root / "technicians.xlsx"
            target = root / "technicians_map.xlsx"
            pd.DataFrame(
                [
                    {
                        "Tech Market": "Atlanta",
                        "EMP_NUMBER": "001",
                        "Tech Name": "Example",
                        "ASM": "A",
                        "RSM": "R",
                        "Home Address": "private",
                        "Home Zip": "30001",
                    }
                ]
            ).to_excel(source, index=False)
            evidence = _build_minimized_technician_map(source, target, dry_run=False)
            result = pd.read_excel(target, dtype={"EMP_NUMBER": str})
            self.assertEqual(evidence["row_count"], 1)
            self.assertEqual(list(result.columns), ["Tech Market", "EMP_NUMBER", "Tech Name", "ASM", "RSM"])
            self.assertNotIn("Home Address", result.columns)

    def test_copy_dry_run_does_not_create_target_directories(self) -> None:
        temporary_root = PROJECT_ROOT / "data" / "_tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=temporary_root) as directory:
            root = Path(directory)
            source = root / "source.csv"
            target = root / "new" / "nested" / "target.csv"
            source.write_text("value\n1\n", encoding="utf-8")
            _copy_verified(source, target, dry_run=True)
            self.assertFalse(target.parent.exists())

    def test_reviewed_plan_is_extended_to_full_active_service_coverage(self) -> None:
        temporary_root = PROJECT_ROOT / "data" / "_tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=temporary_root) as directory:
            root = Path(directory)
            reviewed_source = root / "reviewed.csv"
            service_file = root / "service.csv"
            legacy_seed = root / "seed.csv"
            target = root / "extended.csv"
            pd.DataFrame(
                [
                    {
                        "baseline_service_file": "service.csv",
                        "STRATEGIC_CITY_NAME": "Atlanta, GA",
                        "candidate_region_count": 3,
                        "POSTAL_CODE": "30001",
                        "region_id": "atlanta_ga_r01",
                        "region_seq": 1,
                        "AREA_NAME": "Region 1",
                        "service_count": 1,
                        "latitude": 33.7,
                        "longitude": -84.3,
                    }
                ]
            ).to_csv(reviewed_source, index=False)
            pd.DataFrame(
                [
                    {
                        "STRATEGIC_CITY_NAME": "Atlanta, GA",
                        "POSTAL_CODE": "30001",
                        "GSFS_RECEIPT_NO": "A",
                        "latitude": 33.7,
                        "longitude": -84.3,
                    },
                    {
                        "STRATEGIC_CITY_NAME": "Atlanta, GA",
                        "POSTAL_CODE": "30002",
                        "GSFS_RECEIPT_NO": "B",
                        "latitude": 33.8,
                        "longitude": -84.2,
                    },
                ]
            ).to_csv(service_file, index=False)
            pd.DataFrame(
                [
                    {"POSTAL_CODE": "30001", "region_id": "legacy_west", "region_seq": 1},
                    {"POSTAL_CODE": "30002", "region_id": "legacy_east", "region_seq": 2},
                ]
            ).to_csv(legacy_seed, index=False)

            evidence = _build_atlanta_reviewed_with_full_coverage(
                reviewed_source,
                service_file,
                legacy_seed,
                target,
                dry_run=False,
            )
            result = pd.read_csv(target)
            self.assertEqual(evidence["missing_service_postal_count"], 0)
            self.assertEqual(evidence["added_postals"], ["30002"])
            self.assertEqual(set(result["POSTAL_CODE"].astype(str).str.zfill(5)), {"30001", "30002"})
            added = result[result["POSTAL_CODE"].astype(str).str.zfill(5).eq("30002")].iloc[0]
            self.assertEqual(int(added["region_seq"]), 2)
            self.assertEqual(added["region_id"], "atlanta_ga_r02")


if __name__ == "__main__":
    unittest.main()
