from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from admin_tools.db import region_plan_schema_backend as schema


class RegionPlanSchemaV2Tests(TestCase):
    def test_contract_contains_common_reconciliation_and_candidate_grants(self):
        sql = schema._sql().lower()
        self.assertIn("create table if not exists common_region_plan", sql)
        self.assertIn("required_center_type", sql)
        self.assertIn("grant select, insert, update on table public.common_region_plan", sql)
        self.assertNotIn("v005__region_plan_workflow_grants", sql)

    def test_v2_adds_rerunnable_common_verification_audit_columns(self):
        sql = schema.V2_SQL.read_text(encoding="utf-8").lower()
        self.assertIn("alter table public.common_region_plan", sql)
        self.assertIn("add column if not exists verified_content_sha256 char(64)", sql)
        self.assertIn("add column if not exists verified_at timestamptz", sql)
        self.assertIn("add column if not exists verified_by text", sql)
        constraint = "common_region_plan_verified_content_sha256_v2_check"
        self.assertIn(f"drop constraint if exists {constraint}", sql)
        self.assertIn(f"add constraint {constraint}", sql)
        self.assertIn("verified_content_sha256 is null", sql)
        self.assertIn("^[0-9a-f]{64}$", sql)

    def test_v2_removes_city_specific_cardinality_limits_and_is_rerunnable(self):
        sql = schema.V2_SQL.read_text(encoding="utf-8").lower()
        self.assertNotIn("source_membership_count in (1, 2)", sql)
        self.assertNotIn("region_seq between 1 and 6", sql)
        self.assertIn("source_membership_count > 0", sql)
        self.assertIn("source_membership_count > 1", sql)
        self.assertIn("alternate_region_seq", sql)
        self.assertIn("drop constraint if exists", sql)
        self.assertIn("current_key_columns is distinct from", sql)
        self.assertIn("having count(*) > 1", sql)

    def test_v2_recreates_postal_constraints_safely_on_each_reconcile(self):
        sql = schema.V2_SQL.read_text(encoding="utf-8").lower()
        membership_drop = (
            "drop constraint if exists "
            "common_region_plan_postal_membership_count_v2_check"
        )
        membership_add = (
            "add constraint "
            "common_region_plan_postal_membership_count_v2_check"
        )
        resolution_drop = (
            "drop constraint if exists "
            "common_region_plan_postal_resolution_v2_check"
        )
        resolution_add = (
            "add constraint "
            "common_region_plan_postal_resolution_v2_check"
        )
        self.assertIn(membership_drop, sql)
        self.assertIn(resolution_drop, sql)
        self.assertLess(sql.index(membership_drop), sql.index(membership_add))
        self.assertLess(sql.index(resolution_drop), sql.index(resolution_add))

    def test_preview_is_fixed_to_development_target(self):
        with patch.object(schema, "_config_target", return_value=({}, "development", "vrp_db_dev")):
            result = schema.preview(Path("ignored.json"))
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["target_id"], "development:vrp_db_dev")

    def test_reconcile_requires_exact_confirmation(self):
        with self.assertRaisesRegex(Exception, "CONFIRMATION_REQUIRED"):
            schema.reconcile(Path("ignored.json"), confirmation="yes")
