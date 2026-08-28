import unittest
from unittest.mock import patch

import pandas as pd

from smart_routing import common_vrp_db as db


class RegionPlanActiveSelectionTests(unittest.TestCase):
    def test_client_options_query_only_active_plans(self):
        with patch.object(db, "_fetch_df", return_value=pd.DataFrame()) as fetch:
            db.list_region_plan_options("LGEAI", "Atlanta, GA")
        sql = str(fetch.call_args.args[0]).lower()
        self.assertIn("rp.plan_status = 'active'", sql)
        self.assertNotIn("'candidate','reviewed','active','superseded'", sql)

    def test_configured_non_active_plan_fails_closed(self):
        with (
            patch.object(
                db,
                "get_routing_config",
                return_value={"region_plan_id": "rp2_Atlanta_GA_example"},
            ),
            patch.object(
                db,
                "_region_plan_storage_reference",
                return_value={"plan_id": "rp2_Atlanta_GA_example", "lifecycle": "candidate"},
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "CONFIGURED_REGION_PLAN_NOT_ACTIVE"):
                db.get_configured_region_plan_snapshot("LGEAI", "Atlanta, GA")


if __name__ == "__main__":
    unittest.main()
