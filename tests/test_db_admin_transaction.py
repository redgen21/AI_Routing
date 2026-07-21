from __future__ import annotations

import json
import os
import ast
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from admin_tools.db.seeds import build_la_bucket_vrp_inputs
from admin_tools.db.seeds import import_asia_technician_centroids as asia_import
from admin_tools.db import common_vrp as common_vrp_db


def _connection() -> tuple[MagicMock, MagicMock]:
    connection = MagicMock(name="connection")
    cursor = MagicMock(name="cursor")
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


class DbAdminTransactionTests(unittest.TestCase):
    def test_schema_mismatch_fails_before_any_write_transaction(self) -> None:
        connection, cursor = _connection()
        cursor.fetchall.side_effect = [[], []]
        with patch.object(common_vrp_db, "get_db_connection", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "ADMIN_SCHEMA_INCOMPATIBLE"):
                common_vrp_db.verify_admin_schema(Path("config.json"), operation="asia_import")
        self.assertEqual("SET TRANSACTION READ ONLY", cursor.execute.call_args_list[0].args[0])
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_asia_apply_is_one_transaction_and_rolls_back_all_steps(self) -> None:
        connection = MagicMock(name="connection")
        empty = pd.DataFrame()
        with (
            patch.object(asia_import, "verify_admin_schema") as verify_schema,
            patch.object(asia_import, "get_db_connection", return_value=connection),
            patch.object(asia_import, "import_technician_rows", return_value=1) as technicians,
            patch.object(asia_import, "import_capability_rows", return_value=2) as capabilities,
            patch.object(asia_import, "import_region_rows", side_effect=RuntimeError("region failure")) as regions,
        ):
            with self.assertRaisesRegex(RuntimeError, "region failure"):
                asia_import.apply_master_imports(empty, empty, empty, Path("config.json"))
        verify_schema.assert_called_once_with(Path("config.json"), operation="asia_import")
        for helper in (technicians, capabilities, regions):
            self.assertIs(helper.call_args.kwargs["connection"], connection)
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_la_schema_preflight_failure_opens_no_write_connection(self) -> None:
        with (
            patch.object(build_la_bucket_vrp_inputs, "verify_admin_schema", side_effect=RuntimeError("ADMIN_SCHEMA_INCOMPATIBLE")),
            patch.object(build_la_bucket_vrp_inputs, "get_db_connection") as get_connection,
            patch.object(build_la_bucket_vrp_inputs.pd, "read_csv"),
        ):
            with self.assertRaisesRegex(RuntimeError, "ADMIN_SCHEMA_INCOMPATIBLE"):
                build_la_bucket_vrp_inputs._update_db(Path("config.json"), Path("technicians.csv"), ["Los Angeles, CA"])
        get_connection.assert_not_called()
    def test_admin_db_modules_do_not_import_application_package(self) -> None:
        root = Path("admin_tools/db")
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
            imports = [
                node.module or ""
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)
            ] + [
                alias.name
                for node in ast.walk(tree)
                if isinstance(node, ast.Import)
                for alias in node.names
            ]
            self.assertFalse(
                any(name == "smart_routing" or name.startswith("smart_routing.") for name in imports),
                f"application import remains in {path}",
            )

    def test_admin_data_catalog_resolves_external_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "shared"
            catalog_path = root / "catalog.json"
            catalog_path.write_text(
                json.dumps(
                    {
                        "schema": "north-america-routing-data-catalog/v1",
                        "data_root": str(data_root),
                        "active": {
                            "profile_production": "processed/profile/current/profile.xlsx",
                            "region_seed_dir": "db_input/regions",
                            "heavy_repair_lookup": "db_input/lookups/heavy.csv",
                            "symptom_mapping": "reference/lookups/symptom.xlsx",
                        },
                    }
                ),
                encoding="utf-8",
            )
            from admin_tools.db.data_catalog import na_data_path

            with patch.dict(os.environ, {"NA_DATA_CATALOG_PATH": str(catalog_path)}):
                self.assertEqual(
                    na_data_path("profile_production"),
                    (data_root / "processed/profile/current/profile.xlsx").resolve(),
                )
                self.assertEqual(
                    na_data_path("region_seed_dir") / "atlanta_fixed_region_zip_3.csv",
                    (data_root / "db_input/regions/atlanta_fixed_region_zip_3.csv").resolve(),
                )
                self.assertEqual(
                    na_data_path("heavy_repair_lookup"),
                    (data_root / "db_input/lookups/heavy.csv").resolve(),
                )
                self.assertEqual(
                    na_data_path("symptom_mapping"),
                    (data_root / "reference/lookups/symptom.xlsx").resolve(),
                )

    def test_bulk_upsert_does_not_commit_borrowed_connection(self) -> None:
        connection, cursor = _connection()
        with (
            patch.object(common_vrp_db, "get_db_connection") as get_connection,
            patch.object(common_vrp_db, "execute_values") as execute_values,
        ):
            count = common_vrp_db._execute_values_upsert(
                "common_region_master",
                ["subsidiary_name", "strategic_city_name", "postal_code"],
                [("LGEAI", "Los Angeles, CA", "90001")],
                ["subsidiary_name", "strategic_city_name", "postal_code"],
                [],
                connection=connection,
            )

        self.assertEqual(count, 1)
        get_connection.assert_not_called()
        execute_values.assert_called_once()
        self.assertIs(execute_values.call_args.args[0], cursor)
        connection.commit.assert_not_called()
        connection.rollback.assert_not_called()
        connection.close.assert_not_called()

    def test_la_update_uses_one_connection_and_commits_once(self) -> None:
        connection, cursor = _connection()
        technicians = pd.DataFrame(
            [
                {"subsidiary_name": "LGEAI", "strategic_city_name": "Los Angeles, CA", "employee_code": "T1"},
                {"subsidiary_name": "LGEAI", "strategic_city_name": "Los Angeles, CA", "employee_code": "T2"},
            ]
        )
        config_path = Path("config/common_vrp.dev.json")
        cities = ["Los Angeles, CA", "Los Angeles, CA - Area Type Clusters"]
        with (
            patch.object(build_la_bucket_vrp_inputs.pd, "read_csv", return_value=technicians),
            patch.object(build_la_bucket_vrp_inputs, "verify_admin_schema") as verify_schema,
            patch.object(build_la_bucket_vrp_inputs, "get_db_connection", return_value=connection) as get_connection,
            patch.object(build_la_bucket_vrp_inputs, "seed_default_masters") as seed_default_masters,
            patch.object(build_la_bucket_vrp_inputs, "upsert_technician_master") as upsert_technician_master,
        ):
            build_la_bucket_vrp_inputs._update_db(config_path, Path("technicians.csv"), cities)

        verify_schema.assert_called_once_with(config_path, operation="la_seed")
        get_connection.assert_called_once_with(config_path)
        seed_default_masters.assert_called_once_with(
            config_path,
            connection=connection,
        )
        self.assertEqual(upsert_technician_master.call_count, 2)
        for invocation in upsert_technician_master.call_args_list:
            self.assertIs(invocation.kwargs["connection"], connection)
        self.assertEqual(cursor.execute.call_count, len(cities) * 3)
        connection.commit.assert_called_once_with()
        connection.rollback.assert_not_called()
        connection.close.assert_called_once_with()

    def test_la_update_rolls_back_when_seed_fails(self) -> None:
        connection, _ = _connection()
        config_path = Path("config/common_vrp.dev.json")
        with (
            patch.object(build_la_bucket_vrp_inputs.pd, "read_csv", return_value=pd.DataFrame()),
            patch.object(build_la_bucket_vrp_inputs, "verify_admin_schema"),
            patch.object(build_la_bucket_vrp_inputs, "get_db_connection", return_value=connection),
            patch.object(build_la_bucket_vrp_inputs, "seed_default_masters", side_effect=RuntimeError("seed failed")),
            patch.object(build_la_bucket_vrp_inputs, "upsert_technician_master") as upsert_technician_master,
        ):
            with self.assertRaisesRegex(RuntimeError, "seed failed"):
                build_la_bucket_vrp_inputs._update_db(config_path, Path("technicians.csv"), ["Los Angeles, CA"])

        connection.commit.assert_not_called()
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()
        upsert_technician_master.assert_not_called()

    def test_seed_default_masters_propagates_borrowed_connection(self) -> None:
        connection = MagicMock(name="connection")
        with (
            patch.object(common_vrp_db, "load_common_config", return_value={"master_seed": {"technician_master": True}}),
            patch.object(common_vrp_db, "_seed_routing_config") as routing,
            patch.object(common_vrp_db, "_seed_technician_master") as technicians,
            patch.object(common_vrp_db, "_seed_technician_capabilities") as capabilities,
            patch.object(common_vrp_db, "_seed_region_master") as regions,
            patch.object(common_vrp_db, "_seed_heavy_repair_rules") as heavy,
        ):
            common_vrp_db.seed_default_masters(Path("config.json"), connection=connection)

        for helper in (routing, technicians, capabilities, regions, heavy):
            helper.assert_called_once_with(Path("config.json"), connection=connection)


if __name__ == "__main__":
    unittest.main()
