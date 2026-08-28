import contextlib
import hashlib
import io
import json
import unittest
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from admin_tools.db import region_plan_backend
from smart_routing import common_vrp_runtime as runtime
from admin_tools.db.region_plan_backend import (
    Atlanta6AreaPlanRepository,
    GenericRegionPlanLifecycleRepository,
    PLAN_ID,
    RegionPlanContractError,
    _import_bundle_request_command,
    _resolve_request_command,
    _stage_bundle_command,
    _status_bundle_command,
    dispatch,
    get_active_plan_snapshot,
    install_fixed_schema,
    main,
    preview_candidate_import,
    preview_fixed_schema_migration,
    validate_region_plan_bundle,
)
from admin_tools.db.release_backend import classify_sql_statement, split_sql_statements
from tools.data.atlanta_6area_plan import EXPECTED_TECH_IDS, build_atlanta_6area_bundle


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "260310" / "New ATL Buckets.xlsx"
MIGRATION = (
    ROOT
    / "admin_tools"
    / "db"
    / "migrations"
    / "V001__atlanta_6area_region_plan.sql"
)
MIGRATION_MANIFEST = MIGRATION.with_suffix(".manifest.json")
REGION_PLAN_SCHEMA_V2 = ROOT / "admin_tools" / "db" / "region_plan_schema_v2.sql"


def _resolutions() -> dict[str, dict[str, object]]:
    return {
        "30028": {"primary_region": "Zone 2", "allow_overflow": True},
        "30040": {"primary_region": "Zone 3", "allow_overflow": False},
        "30041": {"primary_region": "Zone 2", "allow_overflow": False},
        "30107": {"primary_region": "Zone 3", "allow_overflow": True},
    }


def _bundle() -> bytes:
    return build_atlanta_6area_bundle(
        SOURCE, boundary_resolutions=_resolutions()
    ).bundle_bytes


class _Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.next_row = None
        self.rows = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).lower()
        self.connection.calls.append((normalized, params))
        self.rowcount = 0
        if "insert into common_region_plan (" in normalized:
            self.next_row = (0,)
            self.rowcount = 1
        elif "select count(*) from common_region_plan_region" in normalized:
            self.next_row = (6, 297, 14, 4)
        elif (
            normalized.startswith("select employee_code from common_region_plan_technician")
        ):
            self.rows = [(code,) for code, _name, _active in self.connection.master_rows]
        elif "from common_technician_master" in normalized:
            if "employee_name" in normalized:
                self.rows = [
                    (
                        code, name, "Service", f"{index} Main St", "Atlanta", "GA",
                        "US", f"30{index:03d}", 33.0 + index, -84.0 - index,
                        active, "B", 60,
                    )
                    for index, (code, name, active) in enumerate(self.connection.master_rows, 1)
                ]
            else:
                self.rows = [(code, active) for code, _name, active in self.connection.master_rows]
        elif "from common_technician_capability_master" in normalized:
            self.rows = [
                (code, "HA", "MODEL", True, True, 1, None, None)
                for code, _name, _active in self.connection.master_rows
            ]
        elif normalized.startswith("update common_region_plan set plan_status='reviewed'"):
            self.next_row = self.connection.review_row
            self.rowcount = 1 if self.next_row else 0
        elif normalized.startswith("select plan_status, revision"):
            self.next_row = ("reviewed", 1, "a" * 64, "b" * 64, "c" * 64)
        elif normalized.startswith("select activation_revision from common_city_context"):
            self.next_row = (0,)
        elif normalized.startswith("select plan_id from common_region_plan_activation"):
            self.next_row = None
        elif normalized.startswith("select activation_revision, plan_id, preview_digest"):
            self.next_row = None
        elif normalized.startswith("update common_city_context set activation_revision"):
            self.next_row = (1,)
            self.rowcount = 1
        elif (
            normalized.startswith("update common_region_plan set plan_status='active'")
        ):
            self.next_row = None
            self.rowcount = 1
        else:
            self.next_row = None

    def executemany(self, sql, rows):
        normalized = " ".join(str(sql).split()).lower()
        materialized = list(rows)
        self.connection.calls.append((normalized, materialized))
        self.rowcount = len(materialized)

    def fetchone(self):
        row = self.next_row
        self.next_row = None
        return row

    def fetchall(self):
        rows = list(self.rows)
        self.rows = []
        return rows


class _Connection:
    def __init__(self):
        self.calls = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.review_row = (1,)
        self.master_rows = [(code, f"Technician {index}", True) for index, code in enumerate(sorted(EXPECTED_TECH_IDS), 1)]

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


class _RosterCursor(_Cursor):
    """Small stateful DB double for activation roster synchronization."""

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).lower()
        if normalized.startswith("select employee_code from common_region_plan_technician"):
            self.connection.calls.append((normalized, params))
            self.next_row = None
            self.rows = [(code,) for code in self.connection.selected_codes]
            self.rowcount = 0
        elif (
            normalized.startswith("select employee_code, employee_name")
            and "from common_technician_master" in normalized
        ):
            self.connection.calls.append((normalized, params))
            city = params[1]
            source = (
                self.connection.source_master
                if city == region_plan_backend.SOURCE_STRATEGIC_CITY_NAME
                else self.connection.target_master
            )
            self.next_row = None
            self.rows = [source[code] for code in self.connection.selected_codes if code in source]
            self.rowcount = 0
        elif (
            normalized.startswith("select employee_code, product_group_code")
            and "from common_technician_capability_master" in normalized
        ):
            self.connection.calls.append((normalized, params))
            city = params[1]
            source = (
                self.connection.source_capabilities
                if city == region_plan_backend.SOURCE_STRATEGIC_CITY_NAME
                else self.connection.target_capabilities
            )
            self.next_row = None
            self.rows = sorted(source)
            self.rowcount = 0
        elif normalized.startswith("insert into common_technician_master"):
            self.connection.calls.append((normalized, params))
            self.connection.target_master = dict(self.connection.source_master)
            self.next_row = None
            self.rows = []
            self.rowcount = len(self.connection.selected_codes)
        elif normalized.startswith("delete from common_technician_capability_master"):
            self.connection.calls.append((normalized, params))
            selected = set(params[2])
            before = len(self.connection.target_capabilities)
            self.connection.target_capabilities = {
                row for row in self.connection.target_capabilities if row[0] not in selected
            }
            self.next_row = None
            self.rows = []
            self.rowcount = before - len(self.connection.target_capabilities)
        elif normalized.startswith("insert into common_technician_capability_master"):
            self.connection.calls.append((normalized, params))
            self.connection.target_capabilities.update(self.connection.source_capabilities)
            self.next_row = None
            self.rows = []
            self.rowcount = len(self.connection.source_capabilities)
        else:
            super().execute(sql, params)


class _RosterConnection(_Connection):
    def __init__(self):
        super().__init__()
        self.selected_codes = tuple(sorted(EXPECTED_TECH_IDS))
        self.source_master = {
            code: (
                code, f"Current Technician {index}", "Service", f"{index} Source Ave",
                "Atlanta", "GA", "US", f"30{index:03d}", 33.0 + index,
                -84.0 - index, True, "A" if index == 1 else "B", 45 + index,
            )
            for index, code in enumerate(self.selected_codes, 1)
        }
        self.source_capabilities = {
            (code, "HA", "MODEL", True, True, index, None, None)
            for index, code in enumerate(self.selected_codes, 1)
        }
        first = self.selected_codes[0]
        self.source_capabilities.add((first, "HA", "PREMIUM", True, False, 99, None, None))
        # A realistic changed source roster plus stale target: one selected
        # master is absent, all copied names/addresses are stale, and the
        # first technician has an obsolete capability that must be removed.
        self.target_master = {
            code: (
                code, f"Stale Technician {index}", "Old Center", f"{index} Old Rd",
                "Old Atlanta", "GA", "US", f"99{index:03d}", 1.0, -1.0,
                True, "B", 999,
            )
            for index, code in enumerate(self.selected_codes[:-1], 1)
        }
        self.target_capabilities = {
            (code, "HA", "MODEL", True, True, index, None, None)
            for index, code in enumerate(self.selected_codes, 1)
        }
        self.target_capabilities.remove((first, "HA", "MODEL", True, True, 1, None, None))
        self.target_capabilities.add((first, "STALE", "OLD", True, True, 0, None, None))

    def cursor(self):
        return _RosterCursor(self)


class _SequenceFactory:
    def __init__(self, *connections):
        self.connections = list(connections)
        self.targets = []

    def __call__(self, environment, dbname):
        self.targets.append((environment, dbname))
        return self.connections.pop(0)


class _Factory:
    def __init__(self):
        self.connections = []
        self.targets = []

    def __call__(self, environment, dbname):
        self.targets.append((environment, dbname))
        connection = _Connection()
        self.connections.append(connection)
        return connection


class _SnapshotCursor:
    def __init__(self):
        self.description = None
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).lower()
        if normalized == "set transaction read only":
            self.description, self.rows = None, []
        elif "from common_region_plan_activation a" in normalized:
            columns = [
                "context_status", "status", "plan_id", "revision", "policy_version", "verification_only",
                "checksum", "source_sha256", "manifest_sha256", "activation_revision",
                "activated_at", "activation_reference",
            ]
            self.description = [(name,) for name in columns]
            self.rows = [
                (
                    "active", "active", PLAN_ID, 2,
                    "own_region_with_approved_boundary_overflow/v1",
                    True, "a" * 64, "b" * 64, "c" * 64, 3, "2026-07-21", "ATL6-ACT-1",
                )
            ]
        elif "from common_region_plan_region" in normalized:
            self.description = [(name,) for name in ("region_seq", "region_id", "region_name", "source_territory")]
            self.rows = [
                (index, f"atlanta_6area_r{index:02d}", f"Region {index}", f"Zone {index}")
                for index in range(1, 7)
            ]
        elif "from common_region_plan_postal p" in normalized:
            self.description = [(name,) for name in (
                "postal_code", "region_seq", "region_id", "region_name",
                "area_type", "source_membership_count", "resolution_status",
            )]
            self.rows = [
                (f"{30000 + index:05d}", (index % 6) + 1, "r", "Region", "DMS", 1, "not_required")
                for index in range(297)
            ]
        elif "from common_region_plan_technician t" in normalized:
            self.description = [(name,) for name in (
                "employee_code", "employee_name", "assigned_region_seq", "assigned_region_id",
                "assigned_region_name", "policy_mode", "active_flag",
            )]
            self.rows = [
                (f"AI{index:06d}", "", (index % 6) + 1, "r", "Region",
                 "assigned_region_boundary_spillover", True)
                for index in range(14)
            ]
        elif "from common_region_plan_boundary_overflow" in normalized:
            self.description = [(name,) for name in (
                "postal_code", "primary_region_seq", "alternate_region_seq", "allow_overflow",
                "penalty_cost", "rationale", "policy_version",
            )]
            self.rows = [("30028", 2, 3, True, 4500, "edge", "own_region_with_approved_boundary_overflow/v1")]
        elif "from common_region_plan" in normalized:
            self.description = [("plan_id",)]
            self.rows = [(PLAN_ID,)]
        else:
            self.description, self.rows = None, []

    def fetchall(self):
        return list(self.rows)


class _SnapshotConnection:
    def __init__(self):
        self.rollbacks = 0
        self.closed = 0

    def cursor(self):
        return _SnapshotCursor()

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


class _MigrationCursor:
    def __init__(self, existing=None):
        self.existing = existing
        self.next_row = None
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).lower()
        self.calls.append((normalized, params))
        self.next_row = self.existing if "from admin_schema_migration_history" in normalized else None

    def fetchone(self):
        row = self.next_row
        self.next_row = None
        return row


class _MigrationConnection:
    def __init__(self, existing=None):
        self.cursor_value = _MigrationCursor(existing)
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.autocommit = True

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


class RegionPlanBackendTests(unittest.TestCase):
    def test_bundle_validation_is_fixed_and_preview_safe(self) -> None:
        bundle = _bundle()
        validated = validate_region_plan_bundle(bundle)
        self.assertRegex(validated.plan_id, r"^atlanta_6area_v2_[0-9a-f]{64}$")
        self.assertEqual(validated.plan_id, f"atlanta_6area_v2_{validated.plan_id.rsplit('_', 1)[1]}")
        self.assertEqual(len(validated.regions), 6)
        self.assertEqual(len(validated.postals), 297)
        self.assertTrue(all(row[2] == "DMS" for row in validated.postals))
        self.assertEqual(len(validated.technicians), 14)
        self.assertEqual(len(validated.boundary_resolutions), 4)
        preview = preview_candidate_import(bundle)
        self.assertEqual(preview["lifecycle_stage"], "resolved_candidate")
        self.assertTrue(preview["verification_only"])
        self.assertFalse(preview["promotable"])
        self.assertFalse(preview["write_allowed"])
        self.assertNotIn("employee_name", json.dumps(preview))
        self.assertNotIn("SVC_ENGINEER_NAME", json.dumps(validated.technicians))

    def test_backend_defaults_blank_fixed_region_area_type_to_dms(self) -> None:
        original_read_csv = region_plan_backend._read_csv

        def read_with_blank_area_type(payload):
            rows = original_read_csv(payload)
            if rows and "AREA_NAME" in rows[0] and "POSTAL_CODE" in rows[0]:
                for row in rows:
                    row["area_type"] = ""
            return rows

        with patch.object(region_plan_backend, "validate_atlanta_6area_bundle"), patch.object(
            region_plan_backend, "_read_csv", side_effect=read_with_blank_area_type
        ):
            validated = validate_region_plan_bundle(_bundle())
        self.assertTrue(all(row[2] == "DMS" for row in validated.postals))

    def test_candidate_import_is_transactional_idempotent_sql_without_legacy_writes(self) -> None:
        factory = _Factory()
        repository = Atlanta6AreaPlanRepository(factory)
        result = repository.import_candidate(
            _bundle(),
            environment="development",
            dbname="vrp_db_dev",
            imported_by="qa.user",
            idempotency_key="candidate:20260721",
        )
        connection = factory.connections[0]
        self.assertEqual(
            result.status, "candidate_imported_for_development_verification"
        )
        self.assertEqual(factory.targets, [("development", "vrp_db_dev")])
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        sql = " ".join(call[0] for call in connection.calls)
        self.assertNotIn("common_region_master", sql)
        self.assertNotIn(" delete ", f" {sql} ")
        self.assertIn("on conflict", sql)
        postal_call = next(call for call in connection.calls if "insert into common_region_plan_postal" in call[0])
        ambiguous = [row for row in postal_call[1] if row[6] == 2]
        self.assertEqual(len(ambiguous), 4)
        self.assertTrue(all(row[5] == "DMS" for row in postal_call[1]))
        self.assertTrue(all(row[7] == "resolved" for row in ambiguous))
        master_call = next(call for call in connection.calls if "from common_technician_master" in call[0])
        self.assertEqual(master_call[1][:2], ("LGEAI", "Atlanta, GA"))
        technician_call = next(call for call in connection.calls if "insert into common_region_plan_technician" in call[0])
        self.assertEqual(len(technician_call[1]), 14)
        self.assertTrue(all(len(row) == 6 for row in technician_call[1]))
        self.assertNotIn("employee_name", technician_call[0])

    def test_candidate_import_rejects_source_master_errors_atomically_without_pii(self) -> None:
        cases = {
            "TECHNICIAN_MASTER_MISSING": lambda rows: rows.pop(),
            "TECHNICIAN_MASTER_INACTIVE": lambda rows: rows.__setitem__(0, (rows[0][0], rows[0][1], False)),
            "TECHNICIAN_MASTER_DUPLICATE": lambda rows: rows.append(rows[0]),
        }
        for expected, mutate in cases.items():
            with self.subTest(expected=expected):
                factory = _Factory()
                connection = _Connection()
                mutate(connection.master_rows)
                factory.connections.append(connection)
                factory.targets.append(("development", "vrp_db_dev"))
                repository = Atlanta6AreaPlanRepository(lambda *_args: connection)
                with self.assertRaisesRegex(RegionPlanContractError, expected) as raised:
                    repository.import_candidate(
                        _bundle(), environment="development", dbname="vrp_db_dev",
                        imported_by="qa.user", idempotency_key="candidate:master-error",
                    )
                self.assertNotIn("Technician", str(raised.exception))
                self.assertEqual(connection.commits, 0)
                self.assertEqual(connection.rollbacks, 1)
                self.assertFalse(any("insert into common_region_plan" in sql for sql, _ in connection.calls))

    def test_dynamic_plan_id_flows_through_review_and_activation_while_legacy_remains_allowed(self) -> None:
        dynamic_plan_id = validate_region_plan_bundle(_bundle()).plan_id
        factory = _Factory()
        repository = Atlanta6AreaPlanRepository(factory)
        reviewed = repository.review_plan(
            environment="development", dbname="vrp_db_dev", plan_id=dynamic_plan_id,
            expected_revision=0, reviewed_by="qa.user", review_reference="ATL6-DYNAMIC",
        )
        self.assertEqual(reviewed.plan_id, dynamic_plan_id)
        self.assertIn(dynamic_plan_id, factory.connections[0].calls[0][1])

        preview = repository.preview_activation(
            environment="development", dbname="vrp_db_dev", plan_id=dynamic_plan_id
        )
        self.assertEqual(preview.plan_id, dynamic_plan_id)
        scoped_calls = factory.connections[1].calls
        self.assertTrue(any(dynamic_plan_id in params for _sql, params in scoped_calls))

        legacy = repository.review_plan(
            environment="development", dbname="vrp_db_dev", plan_id=PLAN_ID,
            expected_revision=0, reviewed_by="qa.user", review_reference="ATL6-LEGACY",
        )
        self.assertEqual(legacy.plan_id, PLAN_ID)

    def test_dynamic_manifest_identity_tampering_is_rejected(self) -> None:
        source = _bundle()
        with zipfile.ZipFile(io.BytesIO(source)) as archive:
            files = {name: archive.read(name) for name in archive.namelist()}
        manifest_name = next(name for name in files if name.endswith(".json"))
        manifest = json.loads(files[manifest_name].decode("utf-8"))
        manifest["plan_id"] = "atlanta_6area_v2_" + "0" * 64
        files[manifest_name] = (
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, payload in files.items():
                archive.writestr(name, payload)
        with self.assertRaisesRegex(RegionPlanContractError, "BUNDLE_PLAN_IDENTITY_INVALID"):
            validate_region_plan_bundle(stream.getvalue())

    def test_legacy_plan_id_cannot_bypass_bundle_identity_validation(self) -> None:
        source = _bundle()
        with zipfile.ZipFile(io.BytesIO(source)) as archive:
            files = {name: archive.read(name) for name in archive.namelist()}
        manifest_name = next(name for name in files if name.endswith(".json"))
        manifest = json.loads(files[manifest_name].decode("utf-8"))
        manifest["plan_id"] = PLAN_ID
        manifest["resolution_digest"] = "0" * 64
        files[manifest_name] = (
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, payload in files.items():
                archive.writestr(name, payload)
        with self.assertRaisesRegex(
            RegionPlanContractError, "BUNDLE_PLAN_IDENTITY_INVALID"
        ):
            validate_region_plan_bundle(stream.getvalue())

    def test_raw_workbook_resolution_request_is_disabled(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            managed = root / "managed"
            requests = root / "requests"
            source_bytes = SOURCE.read_bytes()
            source_sha = hashlib.sha256(source_bytes).hexdigest()
            source_path = managed / "territory_plan_workbook" / source_sha / "payload.xlsx"
            source_path.parent.mkdir(parents=True)
            source_path.write_bytes(source_bytes)
            requests.mkdir()
            config = root / "config.json"
            config.write_text(json.dumps({
                "environment": "development",
                "managed_data_root": str(managed),
                "region_plan_request_root": str(requests),
                "database": {"dbname": "vrp_db_dev"},
            }), encoding="utf-8")
            request = {
                "schema": "region-plan-resolution-request/v1",
                "source": str(source_path),
                "source_sha256": source_sha,
                "managed_version": source_sha,
                "boundary_resolutions": _resolutions(),
                "imported_by": "qa.user",
                "idempotency_key": "candidate:dynamic",
            }
            request_bytes = json.dumps(request, sort_keys=True, separators=(",", ":")).encode("utf-8")
            request_sha = hashlib.sha256(request_bytes).hexdigest()
            request_path = requests / f"{request_sha}.json"
            request_path.write_bytes(request_bytes)

            with self.assertRaisesRegex(
                RegionPlanContractError, "RAW_WORKBOOK_WORKFLOW_DISABLED"
            ):
                _resolve_request_command(config, request_path, request_sha)

    def test_production_and_wrong_database_fail_before_connection(self) -> None:
        factory = _Factory()
        repository = Atlanta6AreaPlanRepository(factory)
        for environment, dbname, code in (
            ("production", "vrp_db", "PRODUCTION_WRITE_DISABLED"),
            ("development", "vrp_db", "DATABASE_TARGET_MISMATCH"),
        ):
            with self.subTest(environment=environment, dbname=dbname):
                with self.assertRaisesRegex(RegionPlanContractError, code):
                    repository.import_candidate(
                        _bundle(),
                        environment=environment,
                        dbname=dbname,
                        imported_by="qa.user",
                        idempotency_key="candidate:20260721",
                    )
        self.assertEqual(factory.connections, [])

    def test_review_uses_optimistic_revision_and_rolls_back_conflict(self) -> None:
        factory = _Factory()
        repository = Atlanta6AreaPlanRepository(factory)
        result = repository.review_plan(
            environment="development",
            dbname="vrp_db_dev",
            plan_id=PLAN_ID,
            expected_revision=0,
            reviewed_by="qa.user",
            review_reference="ATL6-1",
        )
        self.assertEqual(result.revision, 1)
        self.assertEqual(factory.connections[0].commits, 1)

        conflict_factory = _Factory()
        connection = _Connection()
        connection.review_row = None
        conflict_factory.__call__ = lambda *_: connection  # type: ignore[method-assign]
        repository = Atlanta6AreaPlanRepository(lambda *_: connection)
        with self.assertRaisesRegex(RegionPlanContractError, "PLAN_REVIEW_REVISION_CONFLICT"):
            repository.review_plan(
                environment="development",
                dbname="vrp_db_dev",
                plan_id=PLAN_ID,
                expected_revision=99,
                reviewed_by="qa.user",
                review_reference="ATL6-1",
            )
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)

    def test_activation_preview_and_apply_are_revision_bound_and_append_audit(self) -> None:
        factory = _Factory()
        repository = Atlanta6AreaPlanRepository(factory)
        preview = repository.preview_activation(
            environment="development", dbname="vrp_db_dev", plan_id=PLAN_ID
        )
        self.assertEqual(preview.expected_activation_revision, 0)
        self.assertRegex(preview.preview_digest, r"^[0-9a-f]{64}$")
        self.assertEqual(factory.connections[0].rollbacks, 1)
        result = repository.apply_activation(
            preview,
            environment="development",
            dbname="vrp_db_dev",
            activated_by="qa.user",
            activation_reference="ATL6-ACT-1",
            idempotency_key="activate:20260721",
        )
        connection = factory.connections[1]
        self.assertEqual(result.activation_revision, 1)
        self.assertEqual(connection.commits, 1)
        sql = " ".join(call[0] for call in connection.calls)
        self.assertIn("insert into common_region_plan_activation", sql)
        self.assertNotIn("common_region_master", sql)

    def test_activation_repairs_changed_roster_and_runtime_inputs_for_selected_ids(self) -> None:
        preview_connection = _Connection()
        roster_connection = _RosterConnection()
        source_master = dict(roster_connection.source_master)
        source_capabilities = set(roster_connection.source_capabilities)
        factory = _SequenceFactory(preview_connection, roster_connection)
        repository = Atlanta6AreaPlanRepository(factory)
        preview = repository.preview_activation(
            environment="development", dbname="vrp_db_dev", plan_id=PLAN_ID
        )

        repository.apply_activation(
            preview,
            environment="development",
            dbname="vrp_db_dev",
            activated_by="qa.user",
            activation_reference="ATL6-ROSTER-SYNC",
            idempotency_key="activate:roster-sync",
        )

        self.assertEqual(roster_connection.commits, 1)
        self.assertEqual(roster_connection.rollbacks, 0)
        self.assertEqual(roster_connection.target_master, source_master)
        self.assertEqual(roster_connection.target_capabilities, source_capabilities)
        self.assertEqual(
            set(roster_connection.selected_codes), set(roster_connection.target_master)
        )
        self.assertTrue(
            all(
                any(row[0] == code for row in roster_connection.target_capabilities)
                for code in roster_connection.selected_codes
            )
        )
        # The runtime's scenario lookups receive the repaired target rows, not
        # the stale source-city or request-attendance inputs.
        master_rows = pd.DataFrame(
            [
                {"employee_code": row[0], "employee_name": row[1], "active_flag": row[10]}
                for row in roster_connection.target_master.values()
            ]
        )
        capability_rows = pd.DataFrame(
            [
                {
                    "employee_code": row[0],
                    "product_group_code": row[1],
                    "product_code": row[2],
                }
                for row in roster_connection.target_capabilities
            ]
        )
        with patch.object(runtime, "list_engineers", return_value=master_rows) as engineers, patch.object(
            runtime, "list_capabilities", return_value=capability_rows
        ):
            runtime_master = runtime._runtime_engineer_master("LGEAI", "Atlanta_6area")
            runtime_capabilities, managed = runtime._managed_capability_rows(
                "LGEAI", "Atlanta_6area", []
            )
        engineers.assert_called_once_with(
            "LGEAI", "Atlanta_6area", config_path=runtime.COMMON_CONFIG_PATH
        )
        self.assertTrue(managed)
        self.assertEqual(set(runtime_master["employee_code"]), set(roster_connection.selected_codes))
        self.assertEqual(
            set(row["employee_code"] for row in runtime_capabilities),
            set(roster_connection.selected_codes),
        )
        sql = " ".join(call[0] for call in roster_connection.calls)
        self.assertIn("insert into common_technician_master", sql)
        self.assertIn("delete from common_technician_capability_master", sql)
        self.assertIn("insert into common_technician_capability_master", sql)
        self.assertNotIn("common_request_technician_input", sql)
        self.assertNotIn("common_job_input", sql)

    def test_activation_rolls_back_when_selected_source_technician_has_no_capability(self) -> None:
        preview_connection = _Connection()
        roster_connection = _RosterConnection()
        missing_code = roster_connection.selected_codes[-1]
        target_master = dict(roster_connection.target_master)
        target_capabilities = set(roster_connection.target_capabilities)
        roster_connection.source_capabilities = {
            row for row in roster_connection.source_capabilities if row[0] != missing_code
        }
        factory = _SequenceFactory(preview_connection, roster_connection)
        repository = Atlanta6AreaPlanRepository(factory)
        preview = repository.preview_activation(
            environment="development", dbname="vrp_db_dev", plan_id=PLAN_ID
        )

        with self.assertRaisesRegex(RegionPlanContractError, "TECHNICIAN_CAPABILITY_MISSING"):
            repository.apply_activation(
                preview,
                environment="development",
                dbname="vrp_db_dev",
                activated_by="qa.user",
                activation_reference="ATL6-ROSTER-MISSING-CAP",
                idempotency_key="activate:roster-missing-cap",
            )

        self.assertEqual(roster_connection.commits, 0)
        self.assertEqual(roster_connection.rollbacks, 1)
        self.assertEqual(roster_connection.target_master, target_master)
        self.assertEqual(roster_connection.target_capabilities, target_capabilities)
        sql = " ".join(call[0] for call in roster_connection.calls)
        self.assertNotIn("insert into common_technician_master", sql)
        self.assertNotIn("delete from common_technician_capability_master", sql)

    def test_migration_is_allowlisted_safe_additive_sql_with_matching_manifest(self) -> None:
        sql_bytes = MIGRATION.read_bytes()
        manifest = json.loads(MIGRATION_MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(manifest["checksum_sha256"], hashlib.sha256(sql_bytes).hexdigest())
        self.assertTrue(manifest["data_impact"]["common_region_master_unchanged"])
        sql = sql_bytes.decode("utf-8")
        statements = split_sql_statements(sql)
        self.assertGreaterEqual(len(statements), 12)
        self.assertTrue(all(classify_sql_statement(statement) in {"create_table", "create_index"} for statement in statements))
        for table in (
            "common_city_context",
            "common_region_plan",
            "common_region_plan_region",
            "common_region_plan_postal",
            "common_region_plan_technician",
            "common_region_plan_boundary_overflow",
            "common_region_plan_activation",
        ):
            self.assertIn(f"create table if not exists {table}", sql.lower())
        self.assertIn(
            "area_type text not null check (area_type in ('dms', 'dms2'))",
            sql.lower(),
        )
        self.assertEqual(
            manifest["runtime_policy"]["postal_area_type"],
            {
                "required": True,
                "allowed_values": ["DMS", "DMS2"],
                "canonical_atlanta_default": "DMS",
            },
        )
        self.assertNotIn("common_region_master", sql.lower())
        self.assertNotIn("delete from", sql.lower())

    def test_schema_v2_grants_are_minimal_and_v005_is_absent(self) -> None:
        registry = json.loads(
            (ROOT / "admin_tools" / "db" / "migrations" / "manifest.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertNotIn(
            "V005__region_plan_workflow_grants",
            {item["migration_id"] for item in registry["migrations"]},
        )
        sql = REGION_PLAN_SCHEMA_V2.read_text(encoding="utf-8")
        normalized = " ".join(sql.lower().split())
        self.assertIn("grant select, insert, update on table public.common_city_context to vrp_agent", normalized)
        self.assertIn("grant select, insert, update, delete on table public.common_region_plan to vrp_agent", normalized)
        self.assertIn("public.common_region_plan_boundary_overflow, public.common_region_plan_activation to vrp_agent", normalized)
        self.assertNotIn("common_region_master", normalized)
        self.assertNotIn("common_technician_master", normalized)
        self.assertNotIn(" grant all ", f" {normalized} ")
        self.assertNotIn("grant truncate", normalized)

    def test_stage_candidate_cli_rejects_legacy_raw_workbook_flow(self) -> None:
        source_sha = hashlib.sha256(SOURCE.read_bytes()).hexdigest()
        with TemporaryDirectory() as directory:
            config = Path(directory) / "dev.json"
            config.write_text(
                json.dumps(
                    {
                        "environment": "development",
                        "database": {"dbname": "vrp_db_dev", "password": "SECRET"},
                    }
                ),
                encoding="utf-8",
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = main(
                    [
                        "--json",
                        "stage-candidate",
                        "--config",
                        str(config),
                        "--source",
                        str(SOURCE),
                        "--source-sha256",
                        source_sha,
                        "--managed-version",
                        source_sha,
                    ]
                )
            payload = json.loads(output.getvalue())
        self.assertEqual(status, 2)
        self.assertEqual(payload["contract_version"], "region-plan/v1")
        self.assertEqual(payload["environment"], "development")
        self.assertEqual(payload["status"], "rejected")
        self.assertEqual(payload["error_code"], "RAW_WORKBOOK_WORKFLOW_DISABLED")
        self.assertNotIn("SECRET", output.getvalue())
        self.assertNotIn(str(SOURCE), output.getvalue())

    def test_managed_bundle_stage_and_import_use_only_canonical_zip(self) -> None:
        bundle = _bundle()
        bundle_sha = hashlib.sha256(bundle).hexdigest()
        with TemporaryDirectory() as directory:
            root = Path(directory)
            managed = root / "managed"
            requests = root / "requests"
            bundle_path = (
                managed
                / "fixed_region_plan_bundle"
                / bundle_sha
                / "payload.zip"
            )
            bundle_path.parent.mkdir(parents=True)
            bundle_path.write_bytes(bundle)
            bundle_path.chmod(0o600)
            requests.mkdir()
            config = root / "dev.json"
            config.write_text(
                json.dumps(
                    {
                        "environment": "development",
                        "managed_data_root": str(managed),
                        "region_plan_request_root": str(requests),
                        "database": {"dbname": "vrp_db_dev"},
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "admin_tools.db.region_plan_backend.preview_atlanta_6area_plan",
                side_effect=AssertionError("workbook parser must not run"),
            ):
                preview = _stage_bundle_command(
                    config_path=config,
                    source_path=bundle_path,
                    bundle_sha256=bundle_sha,
                    managed_version=bundle_sha,
                )
            self.assertEqual(preview["contract_version"], "region-plan-bundle-import/v1")
            self.assertEqual(preview["status"], "ready")
            self.assertFalse(preview["write_allowed"])
            self.assertEqual(preview["unique_postal_count"], 297)
            self.assertEqual(preview["technician_count"], 14)

            request = {
                "schema": "region-plan-bundle-import-request/v1",
                "managed_version": bundle_sha,
                "bundle_sha256": bundle_sha,
                "imported_by": "qa.user",
                "idempotency_key": "bundle:20260721",
            }
            request_bytes = json.dumps(
                request, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            request_sha = hashlib.sha256(request_bytes).hexdigest()
            request_path = requests / f"{request_sha}.json"
            request_path.write_bytes(request_bytes)

            class Repository:
                received = b""

                def import_candidate(self, bundle_bytes, **_kwargs):
                    self.received = bundle_bytes
                    validated = validate_region_plan_bundle(bundle_bytes)
                    return type(
                        "Result",
                        (),
                        {
                            "plan_id": validated.plan_id,
                            "revision": 0,
                            "bundle_sha256": validated.bundle_sha256,
                        },
                    )()

            repository = Repository()
            with patch(
                "admin_tools.db.region_plan_backend._repository_from_config",
                return_value=(repository, "development", "vrp_db_dev"),
            ), patch(
                "admin_tools.db.region_plan_backend.preview_atlanta_6area_plan",
                side_effect=AssertionError("workbook parser must not run"),
            ):
                result = _import_bundle_request_command(
                    config, request_path, request_sha
                )
            self.assertEqual(repository.received, bundle)
            self.assertEqual(result["status"], "candidate_imported")
            self.assertEqual(result["checksum"], bundle_sha)

    def test_managed_bundle_rejects_arbitrary_path_and_production(self) -> None:
        bundle = _bundle()
        bundle_sha = hashlib.sha256(bundle).hexdigest()
        with TemporaryDirectory() as directory:
            root = Path(directory)
            managed = root / "managed"
            expected = (
                managed / "fixed_region_plan_bundle" / bundle_sha / "payload.zip"
            )
            expected.parent.mkdir(parents=True)
            expected.write_bytes(bundle)
            expected.chmod(0o600)
            outside = root / "payload.zip"
            outside.write_bytes(bundle)
            config = root / "dev.json"
            config.write_text(
                json.dumps(
                    {
                        "environment": "development",
                        "managed_data_root": str(managed),
                        "database": {"dbname": "vrp_db_dev"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                RegionPlanContractError, "BUNDLE_PATH_NOT_ALLOWED"
            ):
                _stage_bundle_command(
                    config_path=config,
                    source_path=outside,
                    bundle_sha256=bundle_sha,
                    managed_version=bundle_sha,
                )

            config.write_text(
                json.dumps(
                    {
                        "environment": "production",
                        "managed_data_root": str(managed),
                        "database": {"dbname": "vrp_db"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                RegionPlanContractError, "PRODUCTION_WRITE_DISABLED"
            ):
                _stage_bundle_command(
                    config_path=config,
                    source_path=expected,
                    bundle_sha256=bundle_sha,
                    managed_version=bundle_sha,
                )

    def test_managed_bundle_status_rehydrates_reviewed_plan_by_checksum(self) -> None:
        bundle = _bundle()
        validated = validate_region_plan_bundle(bundle)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            managed = root / "managed"
            bundle_path = (
                managed
                / "fixed_region_plan_bundle"
                / validated.bundle_sha256
                / "payload.zip"
            )
            bundle_path.parent.mkdir(parents=True)
            bundle_path.write_bytes(bundle)
            bundle_path.chmod(0o600)
            config = root / "dev.json"
            config.write_text(
                json.dumps(
                    {
                        "environment": "development",
                        "managed_data_root": str(managed),
                        "database": {"dbname": "vrp_db_dev"},
                    }
                ),
                encoding="utf-8",
            )
            with patch(
                "admin_tools.db.region_plan_backend.list_region_plans",
                return_value={
                    "plans": [
                        {
                            "plan_id": validated.plan_id,
                            "status": "reviewed",
                            "revision": 1,
                            "checksum": validated.bundle_sha256,
                        }
                    ]
                },
            ):
                result = _status_bundle_command(
                    config_path=config,
                    bundle_sha256=validated.bundle_sha256,
                    managed_version=validated.bundle_sha256,
                )
            with patch(
                "admin_tools.db.region_plan_backend.list_region_plans",
                return_value={
                    "plans": [
                        {
                            "plan_id": validated.plan_id,
                            "status": "reviewed",
                            "revision": 1,
                            "checksum": "0" * 64,
                        }
                    ]
                },
            ), self.assertRaisesRegex(
                RegionPlanContractError, "BUNDLE_PLAN_STATE_CHECKSUM_MISMATCH"
            ):
                _status_bundle_command(
                    config_path=config,
                    bundle_sha256=validated.bundle_sha256,
                    managed_version=validated.bundle_sha256,
                )
        self.assertEqual(result["status"], "reviewed")
        self.assertEqual(result["revision"], 1)
        self.assertEqual(result["checksum"], validated.bundle_sha256)
        self.assertEqual(result["plan_id"], validated.plan_id)

    def test_managed_bundle_rejects_resolved_path_outside_managed_root(self) -> None:
        bundle = _bundle()
        bundle_sha = hashlib.sha256(bundle).hexdigest()
        with TemporaryDirectory() as directory:
            root = Path(directory)
            managed = root / "managed"
            outside = root / "outside" / "payload.zip"
            outside.parent.mkdir(parents=True)
            outside.write_bytes(bundle)
            config = root / "dev.json"
            config.write_text(
                json.dumps(
                    {
                        "environment": "development",
                        "managed_data_root": str(managed),
                        "database": {"dbname": "vrp_db_dev"},
                    }
                ),
                encoding="utf-8",
            )
            original_resolve = Path.resolve

            def resolve_with_bundle_escape(path: Path, *args, **kwargs) -> Path:
                if path.name == "payload.zip":
                    return original_resolve(outside)
                return original_resolve(path, *args, **kwargs)

            with patch.object(Path, "resolve", autospec=True, side_effect=resolve_with_bundle_escape):
                with self.assertRaisesRegex(RegionPlanContractError, "BUNDLE_PATH_NOT_ALLOWED"):
                    _stage_bundle_command(
                        config_path=config,
                        source_path=outside,
                        bundle_sha256=bundle_sha,
                        managed_version=bundle_sha,
                    )

    def test_active_snapshot_is_read_only_and_matches_runtime_contract(self) -> None:
        connection = _SnapshotConnection()
        with patch(
            "admin_tools.db.region_plan_backend._connect_config",
            return_value=(connection, "development", "vrp_db_dev"),
        ):
            snapshot = get_active_plan_snapshot(
                "LGEAI", "Atlanta_6area", ROOT / "config" / "common_vrp.dev.json"
            )
        self.assertTrue(snapshot["enabled"])
        self.assertEqual(snapshot["status"], "active")
        self.assertEqual(snapshot["context_status"], "active")
        self.assertEqual(snapshot["plan_id"], PLAN_ID)
        self.assertEqual(
            snapshot["policy_version"],
            "own_region_with_approved_boundary_overflow/v1",
        )
        self.assertEqual(len(snapshot["postals"]), 297)
        self.assertTrue(all(row["area_type"] == "DMS" for row in snapshot["postals"]))
        self.assertEqual(len(snapshot["technicians"]), 14)
        self.assertTrue(all(row["employee_name"] == "" for row in snapshot["technicians"]))
        self.assertEqual(snapshot["boundary_overflow"][0]["alternate_region_seq"], 3)
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.closed, 1)

        production = _SnapshotConnection()
        with patch(
            "admin_tools.db.region_plan_backend._connect_config",
            return_value=(production, "production", "vrp_db"),
        ):
            disabled = get_active_plan_snapshot(
                "LGEAI", "Atlanta_6area", ROOT / "config" / "common_vrp.prod.json"
            )
        self.assertFalse(disabled["enabled"])
        self.assertTrue(disabled["verification_only"])
        self.assertEqual(production.closed, 1)

        second = _SnapshotConnection()
        with patch(
            "admin_tools.db.region_plan_backend._connect_config",
            return_value=(second, "development", "vrp_db_dev"),
        ):
            dispatched = dispatch(
                "active",
                {"subsidiary_name": "LGEAI", "strategic_city_name": "Atlanta_6area"},
                config_path=ROOT / "config" / "common_vrp.dev.json",
            )
        self.assertTrue(dispatched["enabled"])

    def test_fixed_migration_preview_and_install_use_registry_and_history(self) -> None:
        with TemporaryDirectory() as directory:
            config = Path(directory) / "dev.json"
            config.write_text(
                json.dumps(
                    {
                        "environment": "development",
                        "database": {
                            "host": "localhost", "port": 5432, "dbname": "vrp_db_dev",
                            "user": "vrp_agent", "password": "SECRET",
                        },
                    }
                ),
                encoding="utf-8",
            )
            preview = preview_fixed_schema_migration(config)
            self.assertEqual(preview["contract_version"], "region-plan-migration/v1")
            self.assertEqual(preview["migration_id"], "V001__atlanta_6area_region_plan")
            self.assertRegex(preview["checksum_sha256"], r"^[0-9a-f]{64}$")

            connection = _MigrationConnection()
            with patch(
                "admin_tools.db.region_plan_backend._connect_config",
                return_value=(connection, "development", "vrp_db_dev"),
            ):
                result = install_fixed_schema(
                    config, typed_confirmation=preview["required_confirmation"]
                )
            self.assertEqual(result["status"], "applied")
            self.assertEqual(connection.commits, 1)
            sql = " ".join(call[0] for call in connection.cursor_value.calls)
            self.assertIn("admin_schema_migration_history", sql)
            self.assertIn("create table if not exists common_region_plan", sql)

            already = _MigrationConnection((preview["checksum_sha256"], "success"))
            with patch(
                "admin_tools.db.region_plan_backend._connect_config",
                return_value=(already, "development", "vrp_db_dev"),
            ):
                result = install_fixed_schema(
                    config, typed_confirmation=preview["required_confirmation"]
                )
            self.assertEqual(result["status"], "already_applied")
            self.assertEqual(already.commits, 0)
            self.assertGreaterEqual(already.rollbacks, 1)

    def test_schema_install_is_development_only(self) -> None:
        with TemporaryDirectory() as directory:
            config = Path(directory) / "prod.json"
            config.write_text(
                json.dumps(
                    {
                        "environment": "production",
                        "database": {"dbname": "vrp_db"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RegionPlanContractError, "PRODUCTION_WRITE_DISABLED"):
                preview_fixed_schema_migration(config)


class _GenericLifecycleCursor:
    def __init__(self, connection):
        self.connection = connection
        self.next_row = None
        self.rows = []
        self.rowcount = 0

    def __enter__(self): return self
    def __exit__(self, *_): return None

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).lower()
        self.connection.calls.append((normalized, params))
        self.next_row, self.rows, self.rowcount = None, [], 0
        if normalized.startswith("select exists(select 1 from information_schema.columns"):
            self.next_row = (True,)
        elif normalized.startswith("select p.plan_status"):
            self.next_row = (
                self.connection.status, self.connection.revision, "region-workbook-import/v1",
                region_plan_backend.GENERIC_POLICY_VERSION, "a" * 64, "b" * 64, "c" * 64,
                413, 0, 413, 54, "Los Angeles, CA",
                self.connection.context_policy, self.connection.activation_revision,
            )
        elif normalized.startswith("select (select count(*) from common_region_plan_region"):
            self.next_row = (6, 413, 54, 0, 0, 0)
        elif normalized.startswith("select a.plan_id, p.policy_version"):
            self.next_row = (
                (self.connection.active_plan, self.connection.active_policy)
                if self.connection.active_plan else None
            )
        elif normalized.startswith("select activation_revision from common_city_context"):
            self.next_row = (self.connection.activation_revision,)
        elif normalized.startswith("select plan_id, activation_revision from common_region_plan_activation"):
            self.next_row = (
                (self.connection.active_plan, self.connection.activation_revision)
                if self.connection.active_plan else None
            )
        elif normalized.startswith("select m.employee_code"):
            self.rows = [
                (f"AI{i:06d}", f"Tech {i}", "DMS" if i <= 27 else "DMS2", "", "LA", "CA", "US", "90001", 34.0, -118.0, True, "B", 60)
                for i in range(1, 55)
            ]
        elif normalized.startswith("select employee_code, product_group_code"):
            self.rows = [(f"AI{i:06d}", "HA", "MODEL", True, True, 1, None, None) for i in range(1, 55)]
        elif normalized.startswith("update common_region_plan set plan_status='reviewed'"):
            self.next_row, self.rowcount = (self.connection.revision + 1,), 1
        elif normalized.startswith("select plan_id, plan_revision, preview_digest"):
            self.next_row = self.connection.existing_activation
        elif normalized.startswith("insert into common_region_master"):
            self.rowcount = 412 if self.connection.fail_projection else 413
        elif normalized.startswith("update common_region_plan set plan_status='active'"):
            self.rowcount = 1
        elif normalized.startswith("update common_city_context set policy_version"):
            self.rowcount = 1

    def executemany(self, sql, rows):
        materialized = list(rows)
        self.connection.calls.append((" ".join(str(sql).split()).lower(), materialized))
        self.rowcount = len(materialized)

    def fetchone(self):
        row, self.next_row = self.next_row, None
        return row

    def fetchall(self):
        rows, self.rows = list(self.rows), []
        return rows


class _GenericLifecycleConnection:
    def __init__(self, *, status="reviewed", revision=2, fail_projection=False,
                 active_plan="old-plan", existing_activation=None,
                 context_policy=None, active_policy=None):
        self.status, self.revision = status, revision
        self.activation_revision = 4
        self.fail_projection = fail_projection
        self.active_plan = active_plan
        self.context_policy = context_policy or region_plan_backend.GENERIC_POLICY_VERSION
        self.active_policy = active_policy or self.context_policy
        self.existing_activation = existing_activation
        self.calls, self.commits, self.rollbacks, self.closed = [], 0, 0, 0
    def cursor(self): return _GenericLifecycleCursor(self)
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): self.closed += 1


def _generic_request(**overrides):
    request = {
        "contract_version": "region-plan-lifecycle-request/v1",
        "subsidiary_name": "LGEAI", "strategic_city_name": "LA_6area",
        "source_strategic_city_name": "Los Angeles, CA", "plan_id": "la6-20260724",
        "policy_version": "active_roster_area_type_fallback_region_soft/v1",
        "source_sha256": "a" * 64, "manifest_sha256": "b" * 64, "bundle_sha256": "c" * 64,
        "region_count": 6, "postal_count": 413, "technician_count": 54,
        "boundary_resolution_count": 0, "expected_plan_revision": 2,
        "expected_activation_revision": 4,
    }
    request.update(overrides)
    return request


class GenericRegionPlanLifecycleTests(unittest.TestCase):
    def test_old_active_context_policy_does_not_block_new_candidate_review_or_preview(self):
        old_policy = "own_region_with_approved_boundary_overflow/v2"
        review_connection = _GenericLifecycleConnection(
            status="candidate", revision=1,
            context_policy=old_policy, active_policy=old_policy,
        )
        review = GenericRegionPlanLifecycleRepository(lambda *_: review_connection).review(
            _generic_request(
                expected_plan_revision=1, reviewed_by="reviewer", review_reference="LA6-R1"
            ), environment="development", dbname="vrp_db_dev"
        )
        self.assertEqual("reviewed", review.status)
        preview_connection = _GenericLifecycleConnection(
            status="reviewed", context_policy=old_policy, active_policy=old_policy,
        )
        preview = GenericRegionPlanLifecycleRepository(lambda *_: preview_connection).preview(
            _generic_request(), environment="development", dbname="vrp_db_dev"
        )
        self.assertEqual("old-plan", preview.current_active_plan_id)

    def test_review_requires_candidate_and_revision_cas(self):
        connection = _GenericLifecycleConnection(status="candidate", revision=1)
        repo = GenericRegionPlanLifecycleRepository(lambda *_: connection)
        result = repo.review(_generic_request(expected_plan_revision=1, reviewed_by="reviewer", review_reference="LA6-R1"), environment="development", dbname="vrp_db_dev")
        self.assertEqual(("reviewed", 2), (result.status, result.revision))
        self.assertEqual(1, connection.commits)
        sql = " ".join(call[0] for call in connection.calls)
        self.assertNotIn("update common_city_context set context_status", sql)
        self.assertNotIn("delete from common_region_master", sql)

    def test_preview_binds_active_plan_and_source_roster(self):
        connection = _GenericLifecycleConnection()
        repo = GenericRegionPlanLifecycleRepository(lambda *_: connection)
        preview = repo.preview(_generic_request(), environment="development", dbname="vrp_db_dev")
        self.assertEqual("old-plan", preview.current_active_plan_id)
        self.assertRegex(preview.source_roster_digest, r"^[0-9a-f]{64}$")
        self.assertRegex(preview.preview_digest, r"^[0-9a-f]{64}$")

    def test_activate_projects_exact_counts_and_failure_rolls_back_for_retry(self):
        preview_connection = _GenericLifecycleConnection()
        preview = GenericRegionPlanLifecycleRepository(lambda *_: preview_connection).preview(
            _generic_request(), environment="development", dbname="vrp_db_dev"
        )
        failed = _GenericLifecycleConnection(fail_projection=True)
        request = _generic_request(preview_digest=preview.preview_digest, activated_by="operator", activation_reference="LA6-A1", idempotency_key="la6-a1")
        with self.assertRaisesRegex(RegionPlanContractError, "ACTIVATION_REGION_PROJECTION_INVALID"):
            GenericRegionPlanLifecycleRepository(lambda *_: failed).activate(request, environment="development", dbname="vrp_db_dev")
        self.assertEqual((0, 1), (failed.commits, failed.rollbacks))
        retry = _GenericLifecycleConnection()
        result = GenericRegionPlanLifecycleRepository(lambda *_: retry).activate(request, environment="development", dbname="vrp_db_dev")
        self.assertEqual(5, result.activation_revision)
        self.assertEqual(1, retry.commits)
        context_switches = [
            params for sql, params in retry.calls
            if sql.startswith("update common_city_context set policy_version")
        ]
        self.assertEqual(region_plan_backend.GENERIC_POLICY_VERSION, context_switches[0][0])
        self.assertEqual(5, context_switches[0][1])

    def test_candidate_cannot_activate_directly(self):
        connection = _GenericLifecycleConnection(status="candidate")
        repo = GenericRegionPlanLifecycleRepository(lambda *_: connection)
        with self.assertRaisesRegex(RegionPlanContractError, "PLAN_STATUS_INVALID"):
            repo.preview(_generic_request(), environment="development", dbname="vrp_db_dev")

    def test_activate_replays_success_after_commit_response_loss(self):
        preview = GenericRegionPlanLifecycleRepository(
            lambda *_: _GenericLifecycleConnection()
        ).preview(_generic_request(), environment="development", dbname="vrp_db_dev")
        replay = _GenericLifecycleConnection(
            active_plan="la6-20260724",
            existing_activation=("la6-20260724", 2, preview.preview_digest, 5, "operator", "LA6-A1")
        )
        replay.activation_revision = 5
        request = _generic_request(
            preview_digest=preview.preview_digest, activated_by="operator",
            activation_reference="LA6-A1", idempotency_key="la6-a1",
        )
        result = GenericRegionPlanLifecycleRepository(lambda *_: replay).activate(
            request, environment="development", dbname="vrp_db_dev"
        )
        self.assertEqual(("already_active", 5), (result.status, result.activation_revision))
        self.assertEqual((0, 1), (replay.commits, replay.rollbacks))
        self.assertFalse(any("delete from common_region_master" in sql for sql, _ in replay.calls))

    def test_late_replay_is_stale_after_another_plan_becomes_active(self):
        preview = GenericRegionPlanLifecycleRepository(
            lambda *_: _GenericLifecycleConnection()
        ).preview(_generic_request(plan_id="plan-a"), environment="development", dbname="vrp_db_dev")
        late_replay = _GenericLifecycleConnection(
            active_plan="plan-b",
            existing_activation=("plan-a", 2, preview.preview_digest, 5, "operator", "A-ACT"),
        )
        late_replay.activation_revision = 6
        with self.assertRaisesRegex(RegionPlanContractError, "ACTIVATION_IDEMPOTENCY_STALE"):
            GenericRegionPlanLifecycleRepository(lambda *_: late_replay).activate(
                _generic_request(
                    plan_id="plan-a", preview_digest=preview.preview_digest,
                    activated_by="operator", activation_reference="A-ACT",
                    idempotency_key="key-a",
                ), environment="development", dbname="vrp_db_dev"
            )
        self.assertEqual((0, 1), (late_replay.commits, late_replay.rollbacks))

    def test_activate_replay_rejects_approval_metadata_mismatch(self):
        preview = GenericRegionPlanLifecycleRepository(
            lambda *_: _GenericLifecycleConnection()
        ).preview(_generic_request(), environment="development", dbname="vrp_db_dev")
        replay = _GenericLifecycleConnection(
            existing_activation=("la6-20260724", 2, preview.preview_digest, 5, "original", "LA6-A1")
        )
        with self.assertRaisesRegex(RegionPlanContractError, "ACTIVATION_IDEMPOTENCY_CONFLICT"):
            GenericRegionPlanLifecycleRepository(lambda *_: replay).activate(
                _generic_request(
                    preview_digest=preview.preview_digest, activated_by="different",
                    activation_reference="LA6-A1", idempotency_key="la6-a1",
                ), environment="development", dbname="vrp_db_dev"
            )
        self.assertEqual((0, 1), (replay.commits, replay.rollbacks))

    def test_superseded_plan_can_be_reactivated_with_fresh_preview(self):
        # A -> B
        b_preview = GenericRegionPlanLifecycleRepository(
            lambda *_: _GenericLifecycleConnection(status="reviewed", active_plan="plan-a")
        ).preview(_generic_request(plan_id="plan-b"), environment="development", dbname="vrp_db_dev")
        b_connection = _GenericLifecycleConnection(status="reviewed", active_plan="plan-a")
        b_result = GenericRegionPlanLifecycleRepository(lambda *_: b_connection).activate(
            _generic_request(
                plan_id="plan-b", preview_digest=b_preview.preview_digest,
                activated_by="operator", activation_reference="B-ACT", idempotency_key="b-act",
            ), environment="development", dbname="vrp_db_dev"
        )
        self.assertEqual("activated", b_result.status)

        # B -> A, using A's new superseded revision and a fresh roster-bound preview.
        a_preview_connection = _GenericLifecycleConnection(
            status="superseded", revision=3, active_plan="plan-b"
        )
        a_preview_connection.activation_revision = 5
        a_request = _generic_request(
            plan_id="plan-a", expected_plan_revision=3, expected_activation_revision=5
        )
        a_preview = GenericRegionPlanLifecycleRepository(
            lambda *_: a_preview_connection
        ).preview(a_request, environment="development", dbname="vrp_db_dev")
        a_connection = _GenericLifecycleConnection(
            status="superseded", revision=3, active_plan="plan-b"
        )
        a_connection.activation_revision = 5
        a_result = GenericRegionPlanLifecycleRepository(lambda *_: a_connection).activate(
            {
                **a_request, "preview_digest": a_preview.preview_digest,
                "activated_by": "operator", "activation_reference": "A-REACT",
                "idempotency_key": "a-react",
            }, environment="development", dbname="vrp_db_dev"
        )
        self.assertEqual(("activated", 6), (a_result.status, a_result.activation_revision))
        a_context_switch = next(
            params for sql, params in a_connection.calls
            if sql.startswith("update common_city_context set policy_version")
        )
        self.assertEqual(a_request["policy_version"], a_context_switch[0])
        self.assertEqual(6, a_context_switch[1])
        region_projection = [
            (sql, params) for sql, params in a_connection.calls
            if sql.startswith("insert into common_region_master")
        ]
        self.assertEqual(1, len(region_projection))
        self.assertEqual(("LGEAI", "LA_6area", "plan-a"), region_projection[0][1])
        technician_projection = [
            rows for sql, rows in a_connection.calls
            if sql.startswith("insert into common_technician_master")
        ]
        capability_projection = [
            rows for sql, rows in a_connection.calls
            if sql.startswith("insert into common_technician_capability_master")
        ]
        self.assertEqual(54, len(technician_projection[0]))
        self.assertEqual(54, len(capability_projection[0]))
        self.assertTrue(all(row[:2] == ("LGEAI", "LA_6area") for row in technician_projection[0]))
        self.assertTrue(all(row[:2] == ("LGEAI", "LA_6area") for row in capability_projection[0]))


if __name__ == "__main__":
    unittest.main()
