import io
import unittest
from pathlib import Path

import pandas as pd

from tools.data.atlanta_6area_plan import EXPECTED_SOURCE_SHA256, build_atlanta_6area_bundle
from tools.data.managed_data_registry import (
    COMMON,
    DEVELOPMENT,
    PRODUCTION,
    ManagedDataValidationError,
    get_dataset_spec,
    get_managed_data_set,
    list_dataset_specs,
    list_managed_data_sets,
    normalize_heavy_repair_rules,
    resolve_fixed_local_canonical_path,
    validate_managed_data_file,
    validate_and_preview_bytes,
)


def _xlsx_bytes(frame: pd.DataFrame, *, sheet_name: str = "Sheet1") -> bytes:
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name=sheet_name)
    return stream.getvalue()


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TERRITORY_SOURCE = PROJECT_ROOT / "260310" / "New ATL Buckets.xlsx"
PROFILE_SOURCE = (
    PROJECT_ROOT
    / "data"
    / "north_america"
    / "raw"
    / "profile"
    / "20260317"
    / "Top 10_DMS_DMS2_Profile_20260317.xlsx"
)
PROFILE_DERIVED = (
    PROJECT_ROOT
    / "data"
    / "north_america"
    / "processed"
    / "profile"
    / "20260317"
    / "Top 10_DMS_DMS2_Profile_20260317_production.xlsx"
)


class ManagedDataRegistryTests(unittest.TestCase):
    def test_registry_exposes_only_reference_profile_and_candidate_sources(self) -> None:
        specs = list_dataset_specs()
        self.assertTrue(any(spec.id == "heavy_repair_rules" and spec.scope == COMMON for spec in specs))
        self.assertTrue(
            any(spec.id == "territory_plan_workbook" and spec.scope == DEVELOPMENT for spec in specs)
        )
        self.assertTrue(
            any(spec.id == "fixed_region_plan_bundle" and spec.scope == DEVELOPMENT for spec in specs)
        )
        self.assertTrue(
            any(spec.id == "technician_profile_workbook" and spec.scope == PRODUCTION for spec in specs)
        )
        listed_ids = {spec.id for spec in specs}
        for removed in (
            "service_raw",
            "service_geocoded",
            "profile_raw",
            "profile_production",
            "atlanta_engineer_home",
            "atlanta_engineer_region",
            "reviewed_regions",
            "region_seeds",
        ):
            self.assertNotIn(removed, listed_ids)
        self.assertEqual(
            {item["id"] for item in list_managed_data_sets(COMMON)},
            {"symptom_mapping_source", "heavy_repair_rules", "client_master"},
        )
        self.assertEqual(get_managed_data_set("client_master").id, "client_master")
        with self.assertRaisesRegex(ManagedDataValidationError, "SCOPE_REQUIRED"):
            get_managed_data_set("technician_profile_workbook")
        with self.assertRaisesRegex(ManagedDataValidationError, "DATASET_NOT_ALLOWED"):
            get_dataset_spec("service_raw", DEVELOPMENT)
        with self.assertRaisesRegex(ManagedDataValidationError, "SCOPE_NOT_ALLOWED"):
            get_dataset_spec("heavy_repair_rules", "sandbox")

    def test_canonical_paths_are_role_resolved_not_caller_paths(self) -> None:
        self.assertTrue(
            str(resolve_fixed_local_canonical_path("heavy_repair_rules", COMMON)).endswith(
                "db_input\\lookups\\atlanta_heavy_repair_lookup.csv"
            )
        )
        self.assertTrue(
            str(
                resolve_fixed_local_canonical_path(
                    "technician_profile_workbook", DEVELOPMENT
                )
            ).endswith(".xlsx")
        )
        self.assertTrue(
            str(
                resolve_fixed_local_canonical_path("territory_plan_workbook", DEVELOPMENT)
            ).endswith("planning\\regions\\candidates")
        )

    def test_extension_and_size_are_fail_closed(self) -> None:
        with self.assertRaisesRegex(ManagedDataValidationError, "FILE_EXTENSION_NOT_ALLOWED"):
            validate_and_preview_bytes("client_master", COMMON, "client.csv", b"a,b\n1,2\n")
        spec = get_dataset_spec("heavy_repair_rules", COMMON)
        with self.assertRaisesRegex(ManagedDataValidationError, "FILE_TOO_LARGE"):
            validate_and_preview_bytes(
                "heavy_repair_rules", COMMON, "rules.csv", b"x" * (spec.max_bytes + 1)
            )
        with self.assertRaisesRegex(ManagedDataValidationError, "DATASET_NOT_ALLOWED"):
            validate_and_preview_bytes("service_raw", DEVELOPMENT, "service.csv", b"a\n1\n")

    def test_xlsx_preview_and_profile_sheet_contract(self) -> None:
        client = pd.DataFrame(
            [{
                "Product Group Name": "TV", "Product Group Code": "TV", "Product Name": "OLED",
                "Product Code": "A", "Symptom Name": "No power", "Symptom Code": "S",
                "Detailed Symptom Name": "Dead", "Detailed Symptom Code": "D",
            }]
        )
        preview = validate_and_preview_bytes("client_master", COMMON, "client.xlsx", _xlsx_bytes(client))
        self.assertEqual(preview.tables[0].name, "Sheet1")
        with self.assertRaisesRegex(ManagedDataValidationError, "SCHEMA_REQUIRED_COLUMNS_MISSING"):
            validate_and_preview_bytes(
                "client_master", COMMON, "client.xlsx", _xlsx_bytes(pd.DataFrame([{"x": 1}]))
            )

        profile_preview = validate_and_preview_bytes(
            "technician_profile_workbook",
            DEVELOPMENT,
            PROFILE_SOURCE.name,
            PROFILE_SOURCE.read_bytes(),
        )
        self.assertEqual(len(profile_preview.tables), 4)
        self.assertEqual(
            [table.name for table in profile_preview.tables],
            ["1. Zip Coverage", "2. Slot", "3. Product", "4. Address"],
        )
        address = profile_preview.tables[-1]
        self.assertIn("Name", address.masked_columns)
        self.assertIn("Home Street Address", address.masked_columns)
        self.assertTrue(all(row["Name"] == "[REDACTED]" for row in address.rows))

    def test_profile_upload_rejects_derived_production_projection(self) -> None:
        with self.assertRaisesRegex(ManagedDataValidationError, "DERIVED_PROFILE_UPLOAD_NOT_ALLOWED"):
            validate_and_preview_bytes(
                "technician_profile_workbook",
                PRODUCTION,
                PROFILE_DERIVED.name,
                PROFILE_DERIVED.read_bytes(),
            )
        with self.assertRaisesRegex(ManagedDataValidationError, "DERIVED_PROFILE_UPLOAD_NOT_ALLOWED"):
            validate_and_preview_bytes(
                "technician_profile_workbook",
                PRODUCTION,
                "renamed_source.xlsx",
                PROFILE_DERIVED.read_bytes(),
            )

    def test_territory_plan_preview_is_candidate_only_and_row_accounted(self) -> None:
        public_spec = get_dataset_spec("territory_plan_workbook", DEVELOPMENT).as_dict()
        self.assertEqual(public_spec["allowed_targets"], ["file_upload", "preview"])
        self.assertEqual(public_spec["lifecycle_stage"], "candidate_plan")
        self.assertFalse(public_spec["direct_db_upsert"])
        self.assertTrue(public_spec["promotion_required"])

        preview = validate_and_preview_bytes(
            "territory_plan_workbook",
            DEVELOPMENT,
            TERRITORY_SOURCE.name,
            TERRITORY_SOURCE.read_bytes(),
        )
        normalization = preview.normalization
        self.assertEqual(normalization["membership_input_rows"], 301)
        self.assertEqual(normalization["unique_postal_count"], 297)
        self.assertEqual(normalization["technician_input_rows"], 14)
        self.assertEqual(normalization["ambiguous_postal_count"], 4)
        self.assertEqual(normalization["approval_status"], "pending_boundary_resolutions")
        self.assertFalse(normalization["promotable"])
        self.assertTrue(normalization["promotion_required"])
        self.assertEqual(
            [table.name for table in preview.tables], ["1. Area", "2. Technician"]
        )
        technician_row = preview.tables[1].rows[0]
        self.assertEqual(technician_row["Tech ID"], "[REDACTED]")
        self.assertEqual(technician_row["Tech Name"], "[REDACTED]")
        self.assertEqual(
            public_spec["preview_schema"]["sheet_columns"],
            {
                "1. Area": ["ZIPCode", "Territory"],
                "2. Technician": ["Tech ID", "Tech Name", "Assignment"],
            },
        )

        backend = validate_managed_data_file(
            scope=DEVELOPMENT,
            dataset_id="territory_plan_workbook",
            file_name=TERRITORY_SOURCE.name,
            file_bytes=TERRITORY_SOURCE.read_bytes(),
        )
        self.assertEqual(
            backend["summary"]["normalization"]["membership_input_rows"], 301
        )

    def test_territory_plan_parser_failure_is_a_safe_registry_error(self) -> None:
        invalid = pd.DataFrame(
            [{
                "ZIPCode": "30001",
                "Territory": "Zone 1",
                "Tech ID": "AI105115",
                "Tech Name": "Person",
                "Assignment": "Zone 1",
            }]
        )
        with self.assertRaisesRegex(
            ManagedDataValidationError, "XLSX_SHEETS_INVALID"
        ):
            validate_and_preview_bytes(
                "territory_plan_workbook",
                DEVELOPMENT,
                "territory.xlsx",
                _xlsx_bytes(invalid),
            )

    def test_fixed_region_bundle_is_development_only_canonical_and_pii_safe(self) -> None:
        bundle = build_atlanta_6area_bundle(
            TERRITORY_SOURCE,
            boundary_resolutions={
                "30028": {"primary_region": "Zone 2", "allow_overflow": True},
                "30040": {"primary_region": "Zone 3", "allow_overflow": False},
                "30041": {"primary_region": "Zone 2", "allow_overflow": False},
                "30107": {"primary_region": "Zone 3", "allow_overflow": True},
            },
        )
        spec = get_dataset_spec("fixed_region_plan_bundle", DEVELOPMENT).as_dict()
        self.assertEqual(spec["extensions"], [".zip"])
        self.assertEqual(spec["allowed_targets"], ["file_upload", "preview"])
        self.assertEqual(spec["lifecycle_stage"], "candidate_plan")
        self.assertTrue(spec["primary_section"])
        self.assertTrue(get_dataset_spec("territory_plan_workbook", DEVELOPMENT).ui_hidden)

        preview = validate_and_preview_bytes(
            "fixed_region_plan_bundle", DEVELOPMENT, "candidate.zip", bundle.bundle_bytes
        )
        self.assertEqual([table.name for table in preview.tables], ["fixed_region", "boundary_policy"])
        self.assertEqual(preview.normalization["canonical_region_count"], 6)
        self.assertEqual(preview.normalization["canonical_postal_count"], 297)
        self.assertEqual(preview.normalization["canonical_technician_count"], 14)
        self.assertEqual(preview.normalization["canonical_boundary_policy_count"], 2)
        self.assertEqual(preview.normalization["parent_source_sha256"], EXPECTED_SOURCE_SHA256)
        self.assertEqual(preview.normalization["privacy_classification"], "internal_pii_redacted")
        self.assertTrue(preview.normalization["technician_names_redacted"])
        rendered = str(preview.as_dict())
        self.assertNotIn("AI105115", rendered)
        self.assertNotIn("Jason Patterson", rendered)
        with self.assertRaisesRegex(ManagedDataValidationError, "SCOPE_NOT_ALLOWED|DATASET_NOT_ALLOWED"):
            validate_and_preview_bytes(
                "fixed_region_plan_bundle", PRODUCTION, "candidate.zip", bundle.bundle_bytes
            )
        with self.assertRaisesRegex(ManagedDataValidationError, "FIXED_REGION_PLAN_BUNDLE_BUNDLE_ARCHIVE_INVALID"):
            validate_and_preview_bytes(
                "fixed_region_plan_bundle", DEVELOPMENT, "candidate.zip", b"not a zip"
            )

    def test_heavy_repair_transform_accounts_for_nulls_and_duplicates(self) -> None:
        payload = (
            "SERVICE_PRODUCT_GROUP_CODE,SERVICE_PRODUCT_CODE,SYMP_CODE_THREE\n"
            "ref,ref,abc\n"
            "REF,REF,ABC\n"
            "REF,,missing\n"
            "TV,C1,xyz\n"
        ).encode("utf-8-sig")
        normalized = normalize_heavy_repair_rules("rules.csv", payload)
        self.assertEqual(normalized.input_rows, 4)
        self.assertEqual(normalized.accepted_rows, 2)
        self.assertEqual(normalized.rejected_rows, 2)
        self.assertEqual(normalized.rejected_by_reason, {"blank_key": 1, "duplicate_key": 1})
        self.assertTrue(normalized.canonical_csv.startswith(b"\xef\xbb\xbf"))
        self.assertIn(b"REF,REF,ABC", normalized.canonical_csv)
        preview = validate_and_preview_bytes("heavy_repair_rules", COMMON, "rules.csv", payload)
        self.assertFalse(preview.normalization["clean"])


if __name__ == "__main__":
    unittest.main()
