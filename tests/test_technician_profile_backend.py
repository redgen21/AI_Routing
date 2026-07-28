import contextlib
import hashlib
import io
import json
import unittest
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from admin_tools.db import master_data_backend as master
from admin_tools.db.technician_profile_backend import (
    CAPABILITY_COLUMNS,
    TECHNICIAN_COLUMNS,
    TechnicianProfileBackendError,
    _managed_source,
    apply,
    main,
    preview,
)
from tools.data.technician_profile_data import TechnicianProfileDataError


PLAN_ID = "atlanta_6area_v001"


def _profile_bytes():
    slots = []
    products = []
    addresses = []
    coverage = []
    for index in range(1, 15):
        code = f"AI{index:06d}"
        name = f"Technician {index}"
        slots.append({
            "SVC_ENGINEER_CODE": code, "Name": name, "Slot": 8,
            "STRATEGIC_CITY_NAME": "Atlanta, GA", "SVC_CENTER_TYPE": "DMS",
        })
        products.append({
            "SVC_ENGINEER_CODE": code, "SERVICE_PRODUCT_GROUP_CODE": "TV",
            "SERVICE_PRODUCT_CODE": f"P{index:02d}", "REPAIR_FLAG": "T",
            "AREA_PRODUCT_FLAG": "T", "STRATEGIC_CITY_NAME": "Atlanta, GA",
            "SVC_CENTER_TYPE": "DMS",
        })
        addresses.append({
            "SVC_ENGINEER_CODE": code, "Name": name,
            "Home Street Address": f"{index} Private Road", "City ": "Atlanta",
            "State": "GA", "Zip": f"{30300 + index}",
        })
        coverage.append({
            "SVC_ENGINEER_CODE": code, "AREA_CODE": "ATL", "AREA_NAME": "Atlanta",
            "POSTAL_CODE": f"{30300 + index}", "STRATEGIC_CITY_NAME": "Atlanta, GA",
            "SVC_CENTER_TYPE": "DMS",
        })
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        pd.DataFrame(coverage).to_excel(writer, index=False, sheet_name="1. Zip Coverage")
        pd.DataFrame(slots).to_excel(writer, index=False, sheet_name="2. Slot")
        pd.DataFrame(products).to_excel(writer, index=False, sheet_name="3. Product")
        pd.DataFrame(addresses).to_excel(writer, index=False, sheet_name="4. Address")
    return stream.getvalue()


def _assignment_rows():
    return [
        (
            PLAN_ID, f"AI{index:06d}", f"Technician {index}", True,
            ((index - 1) % 6) + 1, f"Atlanta_6area Zone {((index - 1) % 6) + 1}",
            "assigned_region_boundary_spillover", True,
        )
        for index in range(1, 15)
    ]


class _Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).lower()
        self.connection.calls.append((normalized, params))
        if "from common_region_plan_activation a" in normalized:
            self.rows = list(self.connection.assignments)
        elif "from common_technician_master" in normalized:
            self.rows = list(self.connection.technicians)
        elif "from common_technician_capability_master" in normalized:
            self.rows = list(self.connection.capabilities)
        else:
            self.rows = []

    def executemany(self, sql, rows):
        normalized = " ".join(str(sql).split()).lower()
        materialized = list(rows)
        self.connection.batches.append((normalized, materialized))
        self.rowcount = len(materialized)
        if (
            self.connection.fail_capabilities
            and normalized.startswith("insert into common_technician_capability_master")
        ):
            raise RuntimeError("simulated capability failure")

    def fetchall(self):
        return list(self.rows)


class _Connection:
    def __init__(
        self, *, technicians=(), capabilities=(), fail_capabilities=False,
        assignments=None,
    ):
        self.assignments = list(
            _assignment_rows() if assignments is None else assignments
        )
        self.technicians = technicians
        self.capabilities = capabilities
        self.fail_capabilities = fail_capabilities
        self.calls = []
        self.batches = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.autocommit = True

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


class TechnicianProfileBackendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.payload = _profile_bytes()
        cls.version = hashlib.sha256(cls.payload).hexdigest()

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.managed_root = self.root / "managed"
        self.source = (
            self.managed_root
            / "technician_data_workbook"
            / self.version
            / "payload.xlsx"
        )
        self.source.parent.mkdir(parents=True)
        self.source.write_bytes(self.payload)
        config_dir = self.root / "development"
        config_dir.mkdir()
        self.config = config_dir / "config_common_vrp.json"
        self.config.write_text(json.dumps({
            "environment": "development",
            "managed_data_root": str(self.managed_root),
            "database": {
                "host": "localhost", "port": 5432, "dbname": "vrp_db_dev",
                "user": "test", "password": "preview-encryption-secret",
            },
        }), encoding="utf-8")

    def tearDown(self):
        self.temporary.cleanup()

    def _preview(self, connection=None):
        connection = connection or _Connection()
        with patch.object(master, "_connect", return_value=connection):
            result = preview(
                self.config, self.source, self.version, self.version, "development"
            )
        return result, connection

    def _state(self, result):
        target = {
            "environment": "development", "dbname": "vrp_db_dev",
            "target_id": "development:vrp_db_dev",
        }
        path = (
            master._state_root(self.config, target)
            / "technician-profile-previews"
            / f"{result['preview_id']}.json"
        )
        config = master._read_config(self.config)
        return master._read_encrypted_preview(path, config)

    @staticmethod
    def _stale_capability():
        values = {
            "subsidiary_name": "LGEAI",
            "strategic_city_name": "Atlanta_6area",
            "employee_code": "AI000001",
            "product_group_code": "OLD",
            "product_code": "STALE",
            "repair_allowed": True,
            "heavy_repair_allowed": True,
            "priority_score": 1,
            "effective_start_date": None,
            "effective_end_date": None,
        }
        return tuple(values[column] for column in CAPABILITY_COLUMNS)

    def test_preview_is_bound_pii_free_and_row_accounted(self):
        result, connection = self._preview()
        self.assertEqual(result["contract_version"], "technician-profile/v1")
        self.assertEqual(result["target_id"], "development:vrp_db_dev")
        self.assertEqual(result["managed_version"], self.version)
        self.assertEqual(result["technician_create_count"], 14)
        self.assertEqual(result["capability_create_count"], 14)
        self.assertEqual(result["capability_delete_count"], 0)
        self.assertEqual(result["region_mapping_unchanged_count"], 14)
        self.assertEqual(result["plan_id"], PLAN_ID)
        self.assertEqual(result["region_mapping_source"], "active_region_data")
        self.assertEqual(result["rejected_count"], 0)
        self.assertEqual(result["error_count"], 0)
        self.assertEqual(result["errors"], [])
        public = json.dumps(result)
        self.assertNotIn("Technician 1", public)
        self.assertNotIn("Private Road", public)
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.closed, 1)
        calls = "\n".join(sql for sql, _ in connection.calls)
        self.assertIn("from common_region_plan_activation a", calls)
        self.assertIn("and a.active_flag", calls)
        self.assertIn("and c.context_status='active'", calls)
        self.assertIn("and p.plan_status='active'", calls)
        self.assertIn("left join common_technician_master m", calls)
        self.assertIn("m.strategic_city_name=%s", calls)
        self.assertNotIn("plan_status in ('reviewed', 'active')", calls)
        self.assertIn("order by employee_code", calls)
        self.assertIn("order by employee_code, product_group_code, product_code", calls)
        assignment_call = next(
            call for call in connection.calls
            if "from common_region_plan_activation a" in call[0]
        )
        self.assertEqual(
            assignment_call[1],
            ("LGEAI", "Atlanta, GA", "LGEAI", "Atlanta_6area"),
        )

        wrong_path = self.root / "payload.xlsx"
        wrong_path.write_bytes(self.payload)
        config = master._read_config(self.config)
        with self.assertRaisesRegex(TechnicianProfileBackendError, "SOURCE_PATH_NOT_ALLOWED"):
            _managed_source(config, wrong_path, self.version, self.version)
        with self.assertRaisesRegex(TechnicianProfileBackendError, "SOURCE_VERSION_INVALID"):
            _managed_source(config, self.source, self.version, "0" * 64)

    def test_reviewed_only_without_active_activation_is_rejected(self):
        connection = _Connection(assignments=[])
        with patch.object(master, "_connect", return_value=connection):
            with self.assertRaisesRegex(
                TechnicianProfileBackendError,
                "ACTIVE_REGION_ASSIGNMENTS_REQUIRED",
            ):
                preview(
                    self.config, self.source, self.version, self.version,
                    "development",
                )
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.closed, 1)
        sql = "\n".join(statement for statement, _ in connection.calls)
        self.assertIn("common_region_plan_activation", sql)
        self.assertNotIn("plan_status in ('reviewed', 'active')", sql)

    def test_authoritative_source_master_errors_fail_closed_without_pii(self):
        cases = {
            "TECHNICIAN_MASTER_MISSING": lambda rows: rows.__setitem__(
                0, (rows[0][0], rows[0][1], None, None, *rows[0][4:])
            ),
            "TECHNICIAN_MASTER_INACTIVE": lambda rows: rows.__setitem__(
                0, (rows[0][0], rows[0][1], rows[0][2], False, *rows[0][4:])
            ),
            "TECHNICIAN_MASTER_DUPLICATE": lambda rows: rows.append(rows[0]),
        }
        for expected, mutate in cases.items():
            with self.subTest(expected=expected):
                connection = _Connection()
                mutate(connection.assignments)
                with patch.object(master, "_connect", return_value=connection):
                    with self.assertRaisesRegex(TechnicianProfileBackendError, expected) as raised:
                        preview(
                            self.config, self.source, self.version, self.version,
                            "development",
                        )
                self.assertNotIn("Technician", str(raised.exception))
                self.assertEqual(connection.commits, 0)
                self.assertEqual(connection.rollbacks, 1)
                self.assertEqual(connection.batches, [])

    def test_authoritative_master_name_mismatch_preserves_profile_check(self):
        connection = _Connection()
        rows = list(connection.assignments)
        rows[0] = (rows[0][0], rows[0][1], "Different Name", *rows[0][3:])
        connection.assignments = rows
        with patch.object(master, "_connect", return_value=connection):
            with self.assertRaisesRegex(TechnicianProfileDataError, "EMPLOYEE_NAME_CONFLICT") as raised:
                preview(
                    self.config, self.source, self.version, self.version,
                    "development",
                )
        self.assertNotIn("Different Name", str(raised.exception))
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.batches, [])

    def test_apply_verifies_assignments_and_upserts_both_tables_atomically(self):
        result, _ = self._preview()
        connection = _Connection()
        operation = str(uuid.uuid4())
        with patch.object(master, "_connect", return_value=connection):
            applied = apply(
                self.config, result["preview_id"], result["preview_digest"],
                operation, result["confirmation_token"], "development",
            )
        self.assertEqual(applied["status"], "applied")
        self.assertEqual(applied["preview_id"], result["preview_id"])
        self.assertEqual(applied["preview_digest"], result["preview_digest"])
        self.assertEqual(applied["technician_applied_count"], 14)
        self.assertEqual(applied["capability_applied_count"], 14)
        self.assertEqual(applied["region_mapping_verified_count"], 14)
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        self.assertEqual(len(connection.batches), 2)
        self.assertIn("common_technician_master", connection.batches[0][0])
        self.assertIn("common_technician_capability_master", connection.batches[1][0])

        with patch.object(master, "_connect") as connect:
            replay = apply(
                self.config, result["preview_id"], result["preview_digest"],
                operation, result["confirmation_token"], "development",
            )
        self.assertEqual(replay["status"], "already_applied")
        connect.assert_not_called()

    def test_capability_failure_rolls_back_composite_transaction(self):
        stale = [self._stale_capability()]
        result, _ = self._preview(_Connection(capabilities=stale))
        connection = _Connection(capabilities=stale, fail_capabilities=True)
        with patch.object(master, "_connect", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "simulated capability failure"):
                apply(
                    self.config, result["preview_id"], result["preview_digest"],
                    str(uuid.uuid4()), result["confirmation_token"], "development",
                )
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(len(connection.batches), 3)
        self.assertTrue(connection.batches[0][0].startswith("delete from common_technician_capability_master"))

    def test_stale_capability_reconciliation_is_scoped_and_reported(self):
        stale = [self._stale_capability()]
        result, _ = self._preview(_Connection(capabilities=stale))
        self.assertEqual(result["capability_delete_count"], 1)
        connection = _Connection(capabilities=stale)
        operation = str(uuid.uuid4())
        with patch.object(master, "_connect", return_value=connection):
            applied = apply(
                self.config, result["preview_id"], result["preview_digest"],
                operation, result["confirmation_token"], "development",
            )
        self.assertEqual(applied["capability_delete_count"], 1)
        self.assertEqual(connection.commits, 1)
        self.assertEqual(len(connection.batches), 3)
        delete_sql, delete_rows = connection.batches[0]
        self.assertIn("delete from common_technician_capability_master", delete_sql)
        self.assertEqual(
            delete_rows,
            [("LGEAI", "Atlanta_6area", "AI000001", "OLD", "STALE")],
        )
        self.assertNotIn("not in", delete_sql)
        with patch.object(master, "_connect") as connect:
            replay = apply(
                self.config, result["preview_id"], result["preview_digest"],
                operation, result["confirmation_token"], "development",
            )
        self.assertEqual(replay["status"], "already_applied")
        self.assertEqual(replay["capability_delete_count"], 1)
        connect.assert_not_called()

    def test_stale_preview_and_region_assignment_change_fail_closed(self):
        result, _ = self._preview()
        changed = _Connection(technicians=[tuple(
            "changed" if column == "employee_name" else (
                "LGEAI" if column == "subsidiary_name" else
                "Atlanta_6area" if column == "strategic_city_name" else
                "AI000001" if column == "employee_code" else None
            )
            for column in TECHNICIAN_COLUMNS
        )])
        with patch.object(master, "_connect", return_value=changed):
            with self.assertRaisesRegex(TechnicianProfileBackendError, "PREVIEW_STALE"):
                apply(
                    self.config, result["preview_id"], result["preview_digest"],
                    str(uuid.uuid4()), result["confirmation_token"], "development",
                )
        self.assertEqual(changed.rollbacks, 1)

        result, _ = self._preview()
        changed_assignment = _Connection()
        rows = list(changed_assignment.assignments)
        rows[0] = (*rows[0][:-2], "different_policy", True)
        changed_assignment.assignments = rows
        with patch.object(master, "_connect", return_value=changed_assignment):
            with self.assertRaisesRegex(TechnicianProfileBackendError, "REGION_ASSIGNMENTS_CHANGED"):
                apply(
                    self.config, result["preview_id"], result["preview_digest"],
                    str(uuid.uuid4()), result["confirmation_token"], "development",
                )

    def test_commit_receipt_recovery_and_idempotency_conflict(self):
        result, _ = self._preview()
        state = self._state(result)
        technicians = [tuple(row.get(column) for column in TECHNICIAN_COLUMNS) for row in state["technicians"]]
        capabilities = [tuple(row.get(column) for column in CAPABILITY_COLUMNS) for row in state["capabilities"]]
        connection = _Connection(technicians=technicians, capabilities=capabilities)
        operation = str(uuid.uuid4())
        with patch.object(master, "_connect", return_value=connection):
            recovered = apply(
                self.config, result["preview_id"], result["preview_digest"],
                operation, result["confirmation_token"], "development",
            )
        self.assertTrue(recovered["recovered"])
        self.assertEqual(recovered["status"], "applied")
        self.assertEqual(connection.batches, [])
        self.assertEqual(connection.rollbacks, 1)

        other_operation = str(uuid.uuid4())
        target = state["target"]
        receipt = (
            master._state_root(self.config, target)
            / "technician-profile-receipts"
            / f"{other_operation}.json"
        )
        receipt.parent.mkdir(parents=True, exist_ok=True)
        master._write_private(receipt, {
            "status": "applied", "preview_id": str(uuid.uuid4()),
            "preview_digest": "f" * 64,
        })
        with self.assertRaisesRegex(TechnicianProfileBackendError, "IDEMPOTENCY_CONFLICT"):
            apply(
                self.config, result["preview_id"], result["preview_digest"],
                other_operation, result["confirmation_token"], "development",
            )

    def test_production_target_is_disabled(self):
        production = self.root / "production.json"
        production.write_text(json.dumps({
            "environment": "production",
            "database": {
                "host": "localhost", "port": 5432, "dbname": "vrp_db",
                "user": "test", "password": "secret",
            },
        }), encoding="utf-8")
        with self.assertRaisesRegex(TechnicianProfileBackendError, "PRODUCTION_TECHNICIAN_WRITES_DISABLED"):
            preview(production, self.source, self.version, self.version, "production")

    def test_cli_accepts_fixed_preview_contract_and_emits_json(self):
        expected = {
            "contract_version": "technician-profile/v1", "status": "ready",
            "environment": "development", "dbname": "vrp_db_dev",
            "target_id": "development:vrp_db_dev",
        }
        output = io.StringIO()
        with patch(
            "admin_tools.db.technician_profile_backend.preview", return_value=expected
        ) as call, contextlib.redirect_stdout(output):
            status = main([
                "--json", "preview", "--config", str(self.config),
                "--source", str(self.source), "--source-sha256", self.version,
                "--managed-version", self.version, "--environment", "development",
            ])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(output.getvalue()), expected)
        call.assert_called_once_with(
            self.config, self.source, self.version, self.version, "development"
        )


if __name__ == "__main__":
    unittest.main()
