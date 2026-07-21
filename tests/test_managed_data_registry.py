import io
import unittest

import pandas as pd

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


class ManagedDataRegistryTests(unittest.TestCase):
    def test_registry_has_only_fixed_scopes_and_mvp_entries(self) -> None:
        specs = list_dataset_specs()
        self.assertTrue(any(spec.id == "heavy_repair_rules" and spec.scope == COMMON for spec in specs))
        self.assertTrue(any(spec.id == "service_raw" and spec.scope == DEVELOPMENT for spec in specs))
        self.assertTrue(any(spec.id == "profile_production" and spec.scope == PRODUCTION for spec in specs))
        self.assertFalse(get_dataset_spec("reviewed_regions", COMMON).enabled)
        self.assertEqual(list_managed_data_sets(COMMON)[0]["scope"], COMMON)
        self.assertEqual(get_managed_data_set("client_master").id, "client_master")
        with self.assertRaisesRegex(ManagedDataValidationError, "SCOPE_REQUIRED"):
            get_managed_data_set("service_geocoded")
        with self.assertRaisesRegex(ManagedDataValidationError, "SCOPE_NOT_ALLOWED"):
            get_dataset_spec("heavy_repair_rules", "sandbox")

    def test_canonical_paths_are_role_resolved_not_caller_paths(self) -> None:
        self.assertTrue(
            str(resolve_fixed_local_canonical_path("heavy_repair_rules", COMMON)).endswith(
                "db_input\\lookups\\atlanta_heavy_repair_lookup.csv"
            )
        )
        self.assertTrue(
            str(resolve_fixed_local_canonical_path("service_raw", DEVELOPMENT)).endswith(".csv")
        )

    def test_extension_and_size_are_fail_closed(self) -> None:
        with self.assertRaisesRegex(ManagedDataValidationError, "FILE_EXTENSION_NOT_ALLOWED"):
            validate_and_preview_bytes("client_master", COMMON, "client.csv", b"a,b\n1,2\n")
        spec = get_dataset_spec("heavy_repair_rules", COMMON)
        with self.assertRaisesRegex(ManagedDataValidationError, "FILE_TOO_LARGE"):
            validate_and_preview_bytes(
                "heavy_repair_rules", COMMON, "rules.csv", b"x" * (spec.max_bytes + 1)
            )
        with self.assertRaisesRegex(ManagedDataValidationError, "DATASET_DISABLED"):
            validate_and_preview_bytes("region_seeds", COMMON, "regions.csv", b"POSTAL_CODE,region_seq\n30001,1\n")

    def test_csv_preview_validates_schema_and_redacts_pii(self) -> None:
        raw = (
            "STRATEGIC_CITY_NAME,GSFS_RECEIPT_NO,POSTAL_CODE,ADDRESS_LINE1_INFO\n"
            "Atlanta, GA,R-1,30001,1 Private Street\n"
        ).encode("cp949")
        preview = validate_and_preview_bytes("service_raw", DEVELOPMENT, "service.csv", raw)
        self.assertEqual(preview.tables[0].rows[0]["ADDRESS_LINE1_INFO"], "[REDACTED]")
        backend_preview = validate_managed_data_file(
            scope=DEVELOPMENT, dataset_id="service_raw", file_name="service.csv", file_bytes=raw
        )
        self.assertEqual(backend_preview["file_type"], "csv")
        self.assertEqual(backend_preview["sample"][0]["rows"][0]["ADDRESS_LINE1_INFO"], "[REDACTED]")
        with self.assertRaisesRegex(ManagedDataValidationError, "SCHEMA_REQUIRED_COLUMNS_MISSING"):
            validate_and_preview_bytes("service_raw", DEVELOPMENT, "service.csv", b"receipt\nR-1\n")

    def test_service_raw_preview_tolerates_extra_row_fields(self) -> None:
        raw = (
            "STRATEGIC_CITY_NAME,GSFS_RECEIPT_NO,POSTAL_CODE,ADDRESS_LINE1_INFO\n"
            "Atlanta, GA,R-1,30001,Private Street,legacy-extra\n"
        ).encode("utf-8")
        preview = validate_and_preview_bytes("service_raw", DEVELOPMENT, "service.csv", raw)
        self.assertIn("TOLERANT_ROW_SHAPE", preview.tables[0].warnings)

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
            validate_and_preview_bytes("client_master", COMMON, "client.xlsx", _xlsx_bytes(pd.DataFrame([{"x": 1}])))

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
