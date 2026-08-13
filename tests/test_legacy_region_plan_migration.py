import csv
import json
import tempfile
import unittest
from pathlib import Path

from tools.data.migrate_legacy_region_plans import build_city_bundle, bundle_to_workbook_bytes, migrate


class LegacyRegionPlanMigrationTests(unittest.TestCase):
    def test_builds_common_three_file_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            region_file = root / "regions.csv"
            region_file.write_text(
                "POSTAL_CODE,region_id,region_seq,new_region_name,area_type\n"
                "30001,legacy_r01,1,Zone 1,DMS\n"
                "30002,legacy_r02,2,Zone 2,DMS2\n",
                encoding="utf-8",
            )
            technician_file = root / "technicians.csv"
            technician_file.write_text(
                "SVC_ENGINEER_CODE,Name,assigned_region_seq\n"
                "AI000001,One,1\n"
                "AI000002,Two,2\n",
                encoding="utf-8",
            )
            output = root / "bundles"
            manifest = build_city_bundle(
                subsidiary="LGEAI",
                city="Example, GA",
                plan_id="legacy_example_v1",
                region_file=region_file,
                technician_file=technician_file,
                output_root=output,
                routing_policy="preferred_region_soft",
            )
            self.assertEqual("ready", manifest["status"])
            bundle = Path(manifest["bundle_path"])
            self.assertEqual(
                ["manifest.json", "region_postal.csv", "regions.csv", "rejects.csv", "technician_assignments.csv"],
                sorted(path.name for path in bundle.iterdir()),
            )
            with (bundle / "region_postal.csv").open(encoding="utf-8", newline="") as stream:
                self.assertEqual(2, len(list(csv.DictReader(stream))))
            with (bundle / "technician_assignments.csv").open(encoding="utf-8", newline="") as stream:
                self.assertEqual({"AI000001", "AI000002"}, {row["employee_code"] for row in csv.DictReader(stream)})
            self.assertTrue(bundle_to_workbook_bytes(bundle).startswith(b"PK"))

    def test_missing_assignment_source_is_needs_review_and_never_invented(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            region_file = root / "regions.csv"
            region_file.write_text(
                "POSTAL_CODE,region_id,region_seq,new_region_name,area_type\n30001,r1,1,Zone 1,DMS\n",
                encoding="utf-8",
            )
            manifest = build_city_bundle(
                subsidiary="LGEAI",
                city="No Assignment, CA",
                plan_id="legacy_no_assignment_v1",
                region_file=region_file,
                technician_file=None,
                output_root=root / "bundles",
                routing_policy="home_distance_only",
            )
            self.assertEqual("needs_review", manifest["status"])
            self.assertEqual(0, manifest["row_accounting"]["technician_assignments"])
            text = (Path(manifest["bundle_path"]) / "technician_assignments.csv").read_text(encoding="utf-8")
            self.assertEqual("employee_code", text.splitlines()[0].split(",")[3])
            self.assertIn("TECHNICIAN_REGION_SOURCE_MISSING", (Path(manifest["bundle_path"]) / "rejects.csv").read_text(encoding="utf-8"))

    def test_inventory_contains_all_configured_cities(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            region_file = root / "regions.csv"
            region_file.write_text("POSTAL_CODE,region_seq,area_type\n30001,1,DMS\n", encoding="utf-8")
            config = root / "config.json"
            config.write_text(json.dumps({
                "defaults": {"subsidiary_name": "LGEAI"},
                "region_seed_files": [{
                    "subsidiary_name": "LGEAI",
                    "strategic_city_name": "Example, GA",
                    "file": str(region_file),
                }],
            }), encoding="utf-8")
            # load_migration_specs intentionally reads the project catalog for
            # plan defaults, but the input config remains fully isolated.
            inventory = migrate(config, root / "bundles")
            self.assertEqual(1, inventory["city_count"])
            self.assertEqual("needs_review", inventory["cities"][0]["status"])
            self.assertTrue((root / "bundles" / "inventory.json").exists())


if __name__ == "__main__":
    unittest.main()
