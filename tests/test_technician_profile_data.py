import io
import unittest
from pathlib import Path

import pandas as pd

from tools.data.atlanta_6area_plan import ZONE_TO_SEQ, parse_atlanta_6area_workbook
from tools.data.technician_profile_data import (
    TechnicianProfileDataError,
    canonicalize_technician_profile,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE = (
    ROOT
    / "data"
    / "north_america"
    / "raw"
    / "profile"
    / "20260317"
    / "Top 10_DMS_DMS2_Profile_20260317.xlsx"
)
BUCKETS = ROOT / "260310" / "New ATL Buckets.xlsx"


def _actual_assignments():
    parsed = parse_atlanta_6area_workbook(BUCKETS)
    # Region Data is intentionally PII-redacted: its parsed technician rows
    # contain only the stable employee key and territory.  Names belong to the
    # separate Technician Data source (and, in production, the source master),
    # so keep this fixture on that same boundary instead of restoring names to
    # the region bundle contract.
    selected_codes = {item.employee_code for item in parsed.technicians}
    slot = pd.read_excel(PROFILE, sheet_name="2. Slot", dtype=object)
    slot["SVC_ENGINEER_CODE"] = (
        slot["SVC_ENGINEER_CODE"].astype(str).str.strip().str.upper()
    )
    selected_slot = slot[slot["SVC_ENGINEER_CODE"].isin(selected_codes)]
    if (
        set(selected_slot["SVC_ENGINEER_CODE"]) != selected_codes
        or selected_slot["SVC_ENGINEER_CODE"].duplicated().any()
        or selected_slot["Name"].isna().any()
    ):
        raise AssertionError("actual profile fixture has no deterministic selected names")
    names = {
        str(row["SVC_ENGINEER_CODE"]): str(row["Name"]).strip()
        for _, row in selected_slot.iterrows()
    }
    return {
        item.employee_code: {
            "employee_name": names[item.employee_code],
            "assigned_region_seq": ZONE_TO_SEQ[item.territory],
            "assigned_region_name": f"Atlanta_6area {item.territory}",
            "policy_mode": "assigned_region_boundary_spillover",
            "active_flag": True,
        }
        for item in parsed.technicians
    }


def _assignment(code="AI000001", name="Target Tech"):
    return {
        code: {
            "employee_name": name,
            "assigned_region_seq": 1,
            "assigned_region_name": "Atlanta_6area Zone 1 Area",
            "policy_mode": "assigned_region_boundary_spillover",
            "active_flag": True,
        }
    }


def _workbook(*, slot=None, product=None, address=None, coverage=None):
    slot = slot or [
        {
            "SVC_ENGINEER_CODE": "AI000001",
            "Name": "Target Tech",
            "Slot": 8,
            "STRATEGIC_CITY_NAME": "Atlanta, GA",
            "SVC_CENTER_TYPE": "DMS",
        },
        {
            "SVC_ENGINEER_CODE": "AI999999",
            "Name": "Other Tech",
            "Slot": 8,
            "STRATEGIC_CITY_NAME": "Seattle, WA",
            "SVC_CENTER_TYPE": "DMS2",
        },
    ]
    product = product or [
        {
            "SVC_ENGINEER_CODE": "AI000001",
            "SERVICE_PRODUCT_GROUP_CODE": "REF",
            "SERVICE_PRODUCT_CODE": "R1",
            "REPAIR_FLAG": "T",
            "AREA_PRODUCT_FLAG": "N",
            "STRATEGIC_CITY_NAME": "Atlanta, GA",
            "SVC_CENTER_TYPE": "DMS",
        },
        {
            "SVC_ENGINEER_CODE": "AI999999",
            "SERVICE_PRODUCT_GROUP_CODE": "TV",
            "SERVICE_PRODUCT_CODE": "T1",
            "REPAIR_FLAG": "T",
            "AREA_PRODUCT_FLAG": "T",
            "STRATEGIC_CITY_NAME": "Seattle, WA",
            "SVC_CENTER_TYPE": "DMS2",
        },
    ]
    address = address or [
        {
            "SVC_ENGINEER_CODE": "AI000001",
            "Name": "Target Tech",
            "Home Street Address": "1 Main St",
            "City ": "Atlanta",
            "State": "GA",
            "Zip": "30301",
            "latitude": 33.75,
            "longitude": -84.39,
        },
        {
            "SVC_ENGINEER_CODE": "AI999999",
            "Name": "Other Tech",
            "Home Street Address": "2 Main St",
            "City ": "Seattle",
            "State": "WA",
            "Zip": "98101",
            "latitude": 47.61,
            "longitude": -122.33,
        },
    ]
    zip_coverage = coverage or [{
        "SVC_ENGINEER_CODE": "AI000001",
        "AREA_CODE": "ATL",
        "AREA_NAME": "Atlanta",
        "POSTAL_CODE": "30301",
        "STRATEGIC_CITY_NAME": "Atlanta, GA",
        "SVC_CENTER_TYPE": "DMS",
    }]
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        pd.DataFrame(zip_coverage).to_excel(writer, index=False, sheet_name="1. Zip Coverage")
        pd.DataFrame(slot).to_excel(writer, index=False, sheet_name="2. Slot")
        pd.DataFrame(product).to_excel(writer, index=False, sheet_name="3. Product")
        pd.DataFrame(address).to_excel(writer, index=False, sheet_name="4. Address")
    return stream.getvalue()


class TechnicianProfileDataTests(unittest.TestCase):
    def test_actual_profile_has_complete_deterministic_accounting(self):
        canonical = canonicalize_technician_profile(
            PROFILE, plan_id="atlanta_6area_v001", assignments=_actual_assignments()
        )
        self.assertEqual(len(canonical.technician_rows), 14)
        self.assertEqual(len(canonical.capability_rows), 518)
        self.assertEqual(len(canonical.assignment_rows), 14)
        self.assertEqual(
            canonical.source_sha256,
            "5a1738b40dbbe534a1ffccf36020384c6745c03641089c73e14e448907557adf",
        )
        self.assertEqual(
            canonical.row_accounting["zip_coverage"],
            {
                "input_rows": 29238,
                "accepted_rows": 0,
                "excluded_rows": 29238,
                "excluded_by_reason": {
                    "region_assignment_from_active_region_data": 29238
                },
                "rejected_rows": 0,
                "rejected_by_reason": {},
            },
        )
        self.assertEqual(
            canonical.row_accounting["slot"],
            {
                "input_rows": 217,
                "accepted_rows": 14,
                "excluded_rows": 203,
                "excluded_by_reason": {"not_assigned_to_target_plan": 203},
                "rejected_rows": 0,
                "rejected_by_reason": {},
            },
        )
        self.assertEqual(canonical.row_accounting["address"]["input_rows"], 111)
        self.assertEqual(canonical.row_accounting["address"]["accepted_rows"], 14)
        self.assertEqual(canonical.row_accounting["product"]["input_rows"], 8419)
        self.assertEqual(canonical.row_accounting["product"]["accepted_rows"], 518)
        self.assertEqual(
            canonical.row_accounting["product"]["excluded_by_reason"],
            {
                "not_assigned_to_target_plan": 7701,
                "duplicate_source_scope_projection": 200,
            },
        )
        for values in canonical.row_accounting.values():
            self.assertEqual(values["rejected_rows"], 0)

    def test_valid_other_city_rows_are_excluded_not_rejected(self):
        canonical = canonicalize_technician_profile(
            _workbook(), plan_id="plan", assignments=_assignment()
        )
        self.assertEqual(len(canonical.technician_rows), 1)
        self.assertEqual(len(canonical.capability_rows), 1)
        self.assertEqual(canonical.row_accounting["zip_coverage"]["accepted_rows"], 0)
        self.assertEqual(
            canonical.row_accounting["zip_coverage"]["excluded_by_reason"],
            {"region_assignment_from_active_region_data": 1},
        )
        self.assertEqual(canonical.row_accounting["slot"]["excluded_rows"], 1)
        self.assertEqual(canonical.row_accounting["product"]["excluded_rows"], 1)
        self.assertEqual(canonical.row_accounting["address"]["excluded_rows"], 1)
        self.assertTrue(all(v["rejected_rows"] == 0 for v in canonical.row_accounting.values()))

    def test_overlapping_zip_coverage_is_non_authoritative(self):
        coverage = [
            _coverage_row("AI000001", "Legacy North"),
            _coverage_row("AI999999", "Legacy South"),
        ]
        canonical = canonicalize_technician_profile(
            _workbook(coverage=coverage), plan_id="plan", assignments=_assignment()
        )
        self.assertEqual(canonical.assignment_rows[0]["assigned_region_seq"], 1)
        self.assertEqual(
            canonical.assignment_rows[0]["assigned_region_name"],
            "Atlanta_6area Zone 1 Area",
        )
        self.assertEqual(canonical.row_accounting["zip_coverage"]["accepted_rows"], 0)
        self.assertEqual(canonical.row_accounting["zip_coverage"]["excluded_rows"], 2)

    def test_name_conflict_is_rejected(self):
        with self.assertRaisesRegex(TechnicianProfileDataError, "EMPLOYEE_NAME_CONFLICT"):
            canonicalize_technician_profile(
                _workbook(), plan_id="plan", assignments=_assignment(name="Wrong Name")
            )

    def test_missing_slot_address_or_product_is_rejected(self):
        cases = (
            ({"slot": [_workbook_slot_other()]}, "SLOT_EMPLOYEE_COVERAGE_INVALID"),
            ({"address": [_workbook_address_other()]}, "ADDRESS_EMPLOYEE_COVERAGE_INVALID"),
            ({"product": [_workbook_product_other()]}, "PRODUCT_EMPLOYEE_COVERAGE_INVALID"),
        )
        for override, code in cases:
            with self.subTest(code=code), self.assertRaisesRegex(TechnicianProfileDataError, code):
                canonicalize_technician_profile(
                    _workbook(**override), plan_id="plan", assignments=_assignment()
                )

    def test_conflicting_capability_duplicate_is_rejected(self):
        products = [
            _workbook_product_target(area_flag="T"),
            _workbook_product_target(area_flag="N"),
        ]
        with self.assertRaisesRegex(TechnicianProfileDataError, "CAPABILITY_DUPLICATE_CONFLICT"):
            canonicalize_technician_profile(
                _workbook(product=products), plan_id="plan", assignments=_assignment()
            )

    def test_coordinate_pair_and_bounds_are_rejected(self):
        address = [_workbook_address_target()]
        address[0]["longitude"] = ""
        with self.assertRaisesRegex(TechnicianProfileDataError, "HOME_COORDINATE_PAIR_INVALID"):
            canonicalize_technician_profile(
                _workbook(address=address), plan_id="plan", assignments=_assignment()
            )
        address = [_workbook_address_target()]
        address[0]["latitude"] = 91
        with self.assertRaisesRegex(TechnicianProfileDataError, "HOME_COORDINATE_INVALID"):
            canonicalize_technician_profile(
                _workbook(address=address), plan_id="plan", assignments=_assignment()
            )


def _workbook_slot_other():
    return {
        "SVC_ENGINEER_CODE": "AI999999", "Name": "Other Tech", "Slot": 8,
        "STRATEGIC_CITY_NAME": "Seattle, WA", "SVC_CENTER_TYPE": "DMS2",
    }


def _coverage_row(code, area_name):
    return {
        "SVC_ENGINEER_CODE": code, "AREA_CODE": "ATL", "AREA_NAME": area_name,
        "POSTAL_CODE": "30301", "STRATEGIC_CITY_NAME": "Atlanta, GA",
        "SVC_CENTER_TYPE": "DMS",
    }


def _workbook_product_target(*, area_flag="N"):
    return {
        "SVC_ENGINEER_CODE": "AI000001", "SERVICE_PRODUCT_GROUP_CODE": "REF",
        "SERVICE_PRODUCT_CODE": "R1", "REPAIR_FLAG": "T",
        "AREA_PRODUCT_FLAG": area_flag, "STRATEGIC_CITY_NAME": "Atlanta, GA",
        "SVC_CENTER_TYPE": "DMS",
    }


def _workbook_product_other():
    row = _workbook_product_target()
    row.update(SVC_ENGINEER_CODE="AI999999", STRATEGIC_CITY_NAME="Seattle, WA")
    return row


def _workbook_address_target():
    return {
        "SVC_ENGINEER_CODE": "AI000001", "Name": "Target Tech",
        "Home Street Address": "1 Main St", "City ": "Atlanta", "State": "GA",
        "Zip": "30301", "latitude": 33.75, "longitude": -84.39,
    }


def _workbook_address_other():
    row = _workbook_address_target()
    row.update(SVC_ENGINEER_CODE="AI999999", Name="Other Tech", **{"City ": "Seattle", "State": "WA", "Zip": "98101"})
    return row


if __name__ == "__main__":
    unittest.main()
