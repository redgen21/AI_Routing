import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools.data.region_plan_area_map import (
    build_area_map_region_plan,
    list_saved_region_plan_workbooks,
    save_area_map_region_plan,
)


class AreaMapRegionPlanExportTest(unittest.TestCase):
    def _export(self):
        region = pd.DataFrame([
            {
                "POSTAL_CODE": "10001",
                "STRATEGIC_CITY_NAME": "Metro_2area",
                "region_id": "corp_metro_r01",
                "region_seq": 1,
                "AREA_NAME": "Zone 1",
                "new_region_name": "Metro_2area Zone 1",
                "area_type": "DMS",
            },
            {
                "POSTAL_CODE": "10001",
                "STRATEGIC_CITY_NAME": "Metro_2area",
                "region_id": "corp_metro_r02",
                "region_seq": 2,
                "AREA_NAME": "Zone 2",
                "new_region_name": "Metro_2area Zone 2",
                "area_type": "DMS",
            },
        ])
        technician = pd.DataFrame([
            {"Tech ID": "T001", "Tech Name": "Private Name", "Assignment": "Zone 1"},
            {"Tech ID": "T002", "Tech Name": "", "Assignment": ""},
        ])
        return build_area_map_region_plan(
            "regions.csv", region.to_csv(index=False).encode(),
            "technicians.csv", technician.to_csv(index=False).encode(),
            subsidiary_id="CORP", source_city_id="Metro, ST", target_city_id="Metro_2area",
            policy_version="explicit_workbook_membership/v1",
        )

    def test_common_city_export_preserves_region_ids_and_excludes_private_names(self):
        export = self._export()
        self.assertEqual(export.manifest["city_metadata"]["source_city_id"], "Metro, ST")
        self.assertEqual(export.area_df.iloc[0]["region_code"], "corp_metro_r01")
        self.assertEqual(int(export.area_df.iloc[1]["region_seq"]), 2)
        self.assertNotIn("Private Name", export.manifest)
        self.assertEqual(export.manifest["row_accounting"]["Technician"]["accepted_rows"], 1)

    def test_save_and_discover_candidate(self):
        export = self._export()
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = save_area_map_region_plan(export, root)
            self.assertTrue((directory / "region_plan.xlsx").is_file())
            candidates = list_saved_region_plan_workbooks(root)
            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0]["manifest"]["plan_id"], export.manifest["plan_id"])

    def test_conflicted_region_ids_are_normalized_by_region_sequence(self):
        region = pd.DataFrame([
            {
                "POSTAL_CODE": "10001", "STRATEGIC_CITY_NAME": "Metro_2area",
                "region_id": "corp_metro_r01", "region_seq": 1,
                "AREA_NAME": "Zone 1", "new_region_name": "Metro_2area Zone 1", "area_type": "DMS",
            },
            {
                "POSTAL_CODE": "10002", "STRATEGIC_CITY_NAME": "Metro_2area",
                # This row was moved to Zone 2 but retained the old ID.
                "region_id": "corp_metro_r01", "region_seq": 2,
                "AREA_NAME": "Zone 2", "new_region_name": "Metro_2area Zone 2", "area_type": "DMS",
            },
        ])
        technician = pd.DataFrame([
            {"Tech ID": "T001", "Tech Name": "Private Name", "Assignment": "Zone 1"},
            {"Tech ID": "T002", "Tech Name": "Private Name", "Assignment": "Zone 2"},
        ])
        export = build_area_map_region_plan(
            "regions.csv", region.to_csv(index=False).encode(),
            "technicians.csv", technician.to_csv(index=False).encode(),
            subsidiary_id="CORP", source_city_id="Metro, ST", target_city_id="Metro_2area",
            policy_version="explicit_workbook_membership/v1",
        )
        self.assertEqual(set(export.area_df["region_code"]), {"Metro_2area_r01", "Metro_2area_r02"})
        self.assertTrue(export.manifest["normalization_warnings"])

    def test_region_sequence_gaps_are_compacted_without_moving_postals(self):
        region = pd.DataFrame([
            {
                "POSTAL_CODE": "10001", "STRATEGIC_CITY_NAME": "Metro_2area",
                "region_id": "corp_metro_r01", "region_seq": 1,
                "AREA_NAME": "Zone 1", "new_region_name": "Metro_2area Zone 1", "area_type": "DMS",
            },
            {
                "POSTAL_CODE": "10002", "STRATEGIC_CITY_NAME": "Metro_2area",
                "region_id": "corp_metro_r04", "region_seq": 4,
                "AREA_NAME": "Zone 4", "new_region_name": "Metro_2area Zone 4", "area_type": "DMS",
            },
        ])
        technician = pd.DataFrame([
            {"Tech ID": "T001", "Tech Name": "Private Name", "Assignment": "Zone 1"},
            {"Tech ID": "T002", "Tech Name": "Private Name", "Assignment": "Zone 4"},
        ])
        export = build_area_map_region_plan(
            "regions.csv", region.to_csv(index=False).encode(),
            "technicians.csv", technician.to_csv(index=False).encode(),
            subsidiary_id="CORP", source_city_id="Metro, ST", target_city_id="Metro_2area",
            policy_version="explicit_workbook_membership/v1",
        )
        self.assertEqual(export.area_df["region_seq"].astype(int).tolist(), [1, 2])
        self.assertTrue(any("region_seq normalized" in warning for warning in export.manifest["normalization_warnings"]))

    def test_tab_delimited_csv_exports_are_supported(self):
        region = pd.DataFrame([
            {
                "POSTAL_CODE": "10001", "STRATEGIC_CITY_NAME": "Metro_2area",
                "region_id": "Zone 1", "region_seq": 1,
                "AREA_NAME": "Zone 1", "new_region_name": "Metro Zone 1", "area_type": "DMS",
            },
            {
                "POSTAL_CODE": "10002", "STRATEGIC_CITY_NAME": "Metro_2area",
                "region_id": "Zone 2", "region_seq": 2,
                "AREA_NAME": "Zone 2", "new_region_name": "Metro Zone 2", "area_type": "DMS",
            },
        ])
        technician = pd.DataFrame([
            {"Tech ID": "T001", "Tech Name": "Private Name", "Assignment": "Zone 1"},
            {"Tech ID": "T002", "Tech Name": "Private Name", "Assignment": "Zone 2"},
        ])
        export = build_area_map_region_plan(
            "regions.csv", region.to_csv(sep="\t", index=False).encode(),
            "technicians.csv", technician.to_csv(sep="\t", index=False).encode(),
            subsidiary_id="CORP", source_city_id="Metro, ST", target_city_id="Metro_2area",
            policy_version="explicit_workbook_membership/v1",
        )
        self.assertEqual(len(export.area_df), 2)
        self.assertEqual(len(export.technician_df), 2)


if __name__ == "__main__":
    unittest.main()
