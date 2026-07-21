from __future__ import annotations

import unittest

from admin_tools.db.master_data_backend import (
    TABLE_REGISTRY,
    WRITABLE_TABLES,
    _receipt_binding_conflict,
    parse_csv_bytes,
)


class MasterDataBackendTests(unittest.TestCase):
    def test_registry_is_exact_and_write_allowlist_is_narrow(self) -> None:
        self.assertEqual(len(TABLE_REGISTRY), 13)
        self.assertEqual(
            WRITABLE_TABLES,
            {
                "common_technician_master",
                "common_heavy_repair_rule_master",
            },
        )
        self.assertEqual(TABLE_REGISTRY["common_avoid_area"].primary_key, ("avoid_area_id",))
        self.assertEqual(TABLE_REGISTRY["common_job_input"].primary_key, ("record_id",))
        self.assertIn("common_routing_request", TABLE_REGISTRY)
        self.assertNotIn("common_service_geocode", TABLE_REGISTRY)

    def test_heavy_repair_csv_parses_and_duplicate_pk_is_rejected(self) -> None:
        header = "product_group_code,product_code,detailed_symptom_code\n"
        rows = parse_csv_bytes((header + "TV,OLED,S1\n").encode(), "common_heavy_repair_rule_master")
        self.assertEqual(rows[0]["product_code"], "OLED")
        with self.assertRaisesRegex(ValueError, "PRIMARY_KEY"):
            parse_csv_bytes((header + "TV,OLED,S1\nTV,OLED,S1\n").encode(), "common_heavy_repair_rule_master")

    def test_denied_table_and_system_header_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "TABLE_NOT_ALLOWED"):
            parse_csv_bytes(b"request_id\n1\n", "common_routing_request")
        with self.assertRaisesRegex(ValueError, "HEADERS"):
            parse_csv_bytes(
                b"product_group_code,product_code,detailed_symptom_code,created_at\nTV,O,S,now\n",
                "common_heavy_repair_rule_master",
            )

    def test_coordinate_validation(self) -> None:
        columns = [column.name for column in TABLE_REGISTRY["common_technician_master"].columns]
        values = {name: "" for name in columns}
        values.update(
            subsidiary_name="LGEAI",
            strategic_city_name="Atlanta GA",
            employee_code="E1",
            employee_name="Tech",
            center_type="DMS",
            home_latitude="91",
            active_flag="true",
            priority_group="B",
        )
        csv_data = (",".join(columns) + "\n" + ",".join(values[name] for name in columns) + "\n").encode()
        with self.assertRaisesRegex(ValueError, "home_latitude"):
            parse_csv_bytes(csv_data, "common_technician_master")

    def test_technician_routing_policy_fields_are_constrained(self) -> None:
        columns = [column.name for column in TABLE_REGISTRY["common_technician_master"].columns]

        def payload(**overrides: str) -> bytes:
            values = {name: "" for name in columns}
            values.update(
                subsidiary_name="LGEAI", strategic_city_name="Atlanta GA",
                employee_code="E1", employee_name="Tech", center_type="DMS",
                active_flag="true", priority_group="B",
            )
            values.update(overrides)
            return (",".join(columns) + "\n" + ",".join(values[name] for name in columns) + "\n").encode()

        with self.assertRaisesRegex(ValueError, "center_type"):
            parse_csv_bytes(payload(center_type="UNKNOWN"), "common_technician_master")
        with self.assertRaisesRegex(ValueError, "priority_group"):
            parse_csv_bytes(payload(priority_group="Z"), "common_technician_master")
        with self.assertRaisesRegex(ValueError, "max_home_to_job_min"):
            parse_csv_bytes(payload(max_home_to_job_min="-1"), "common_technician_master")
        dms2 = parse_csv_bytes(
            payload(center_type="DMS2", max_home_to_job_min="-1"),
            "common_technician_master",
        )
        self.assertEqual(dms2[0]["max_home_to_job_min"], -1)

    def test_idempotency_conflict_preserves_pending_and_failed_receipts(self) -> None:
        for status in ("pending", "failed", "applied"):
            receipt = {
                "status": status,
                "preview_id": "preview-a",
                "preview_digest": "a" * 64,
            }
            self.assertTrue(
                _receipt_binding_conflict(receipt, "preview-b", "b" * 64)
            )
            self.assertFalse(
                _receipt_binding_conflict(receipt, "preview-a", "a" * 64)
            )


if __name__ == "__main__":
    unittest.main()
