from __future__ import annotations

import tempfile
import unittest
import json
import hashlib
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from admin_tools.db.seeds.build_la_bucket_vrp_inputs import (
    _load_scenario_region_inputs,
    _require_safe_output_root,
    _scenario_configs,
    _update_db,
    _write_inputs,
)
from admin_tools.db import common_vrp as common_vrp_db


class _Cursor:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.connection.executions.append((sql, params))


class _Connection:
    def __init__(self) -> None:
        self.executions: list[tuple[str, object]] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


class AtomicDbSeedTests(unittest.TestCase):
    def _technician_csv(self, directory: Path) -> Path:
        path = directory / "technicians.csv"
        pd.DataFrame(
            [
                {"employee_code": "T1", "strategic_city_name": "Los Angeles, CA"},
                {"employee_code": "T2", "strategic_city_name": "Los Angeles, CA"},
            ]
        ).to_csv(path, index=False, encoding="utf-8-sig")
        return path

    def test_update_uses_one_connection_and_commits_once(self) -> None:
        connection = _Connection()
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = self._technician_csv(Path(temp_dir))
            with (
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs.verify_admin_schema") as verify_schema,
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs.get_db_connection", return_value=connection),
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs.seed_default_masters") as seed_masters,
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs.upsert_technician_master", return_value=1) as upsert,
            ):
                _update_db(Path("dev.json"), csv_path, ["Los Angeles, CA"])

        verify_schema.assert_called_once_with(Path("dev.json"), operation="la_seed")
        seed_masters.assert_called_once_with(
            Path("dev.json"), connection=connection
        )
        self.assertEqual(2, upsert.call_count)
        self.assertTrue(all(call.kwargs["connection"] is connection for call in upsert.call_args_list))
        self.assertEqual(1, connection.commit_count)
        self.assertEqual(0, connection.rollback_count)
        self.assertEqual(1, connection.close_count)

    def test_followup_failure_rolls_back_without_commit(self) -> None:
        connection = _Connection()
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = self._technician_csv(Path(temp_dir))
            with (
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs.verify_admin_schema"),
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs.get_db_connection", return_value=connection),
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs.seed_default_masters") as seed_masters,
                patch(
                    "admin_tools.db.seeds.build_la_bucket_vrp_inputs.upsert_technician_master",
                    side_effect=[1, RuntimeError("simulated upsert failure")],
                ) as upsert,
            ):
                with self.assertRaisesRegex(RuntimeError, "simulated upsert failure"):
                    _update_db(Path("dev.json"), csv_path, ["Los Angeles, CA"])

        self.assertIs(connection, seed_masters.call_args.kwargs["connection"])
        self.assertTrue(all(call.kwargs["connection"] is connection for call in upsert.call_args_list))
        self.assertEqual(0, connection.commit_count)
        self.assertEqual(1, connection.rollback_count)
        self.assertEqual(1, connection.close_count)

    def test_injected_upsert_connection_does_not_commit_or_close(self) -> None:
        connection = _Connection()
        with patch.object(common_vrp_db, "execute_values") as execute_values:
            count = common_vrp_db._execute_values_upsert(
                "example_table",
                ["id"],
                [(1,)],
                ["id"],
                [],
                connection=connection,
            )

        self.assertEqual(1, count)
        execute_values.assert_called_once()
        self.assertEqual(0, connection.commit_count)
        self.assertEqual(0, connection.close_count)

    def test_explicit_external_catalog_controls_la_region_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            data_root = root / "shared" / "north_america"
            catalog_path = root / "external_data_catalog.json"
            catalog_path.write_text(
                json.dumps(
                    {
                        "schema": "north-america-routing-data-catalog/v1",
                        "data_root": str(data_root),
                        "active": {
                            "region_seed_dir": "seeds/regions",
                            "reviewed_regions_dir": "reviewed/regions",
                        },
                    }
                ),
                encoding="utf-8",
            )

            scenarios = _scenario_configs(catalog_path.resolve())

        self.assertEqual(
            data_root.resolve() / "seeds" / "regions",
            scenarios["area_type_clusters"]["region_source"].parent,
        )
        self.assertEqual(
            data_root.resolve() / "reviewed" / "regions",
            scenarios["bucket_sim_draft"]["region_source"].parent,
        )

    def test_region_inputs_are_immutable_and_candidate_has_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reviewed = root / "reviewed" / "reviewed.csv"
            seed = root / "seed" / "seed.csv"
            reviewed.parent.mkdir()
            seed.parent.mkdir()
            source_text = "POSTAL_CODE,region_seq\n90001,1\n"
            reviewed.write_text(source_text, encoding="utf-8")
            seed.write_text("POSTAL_CODE,region_seq\n90002,2\n", encoding="utf-8")
            reviewed_before = reviewed.read_bytes()
            seed_before = seed.read_bytes()
            output_root = root / "generated" / "version-20260721"
            frames, provenance = _load_scenario_region_inputs(
                {"scenario": {"city_name": "Los Angeles, CA", "region_source": reviewed, "region_seed": seed}},
                output_root,
            )
            candidate = Path(provenance["scenario"]["derived_candidate_path"])
            self.assertEqual(reviewed_before, reviewed.read_bytes())
            self.assertEqual(seed_before, seed.read_bytes())
            self.assertTrue(candidate.is_file())
            self.assertTrue(candidate.is_relative_to(output_root.resolve()))
            self.assertEqual(str(reviewed), provenance["scenario"]["source_path"])
            self.assertEqual(hashlib.sha256(reviewed_before).hexdigest(), provenance["scenario"]["source_sha256"])
            self.assertEqual("candidate_only_no_reviewed_to_seed_promotion", provenance["scenario"]["lifecycle"])
            self.assertEqual("90001", frames["scenario"].iloc[0]["POSTAL_CODE"])

    def test_region_candidate_output_root_cannot_be_reviewed_or_seed_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reviewed = root / "reviewed" / "reviewed.csv"
            seed = root / "seed" / "seed.csv"
            reviewed.parent.mkdir()
            seed.parent.mkdir()
            scenarios = {"scenario": {"region_source": reviewed, "region_seed": seed}}
            with self.assertRaisesRegex(ValueError, "reviewed or seed"):
                _require_safe_output_root(reviewed.parent / "candidate", scenarios)
            with self.assertRaisesRegex(ValueError, "reviewed or seed"):
                _require_safe_output_root(seed.parent / "candidate", scenarios)

    def test_la_generator_has_no_hard_coded_local_draft_input(self) -> None:
        source = Path("admin_tools/db/seeds/build_la_bucket_vrp_inputs.py").read_text(encoding="utf-8-sig")
        self.assertNotIn("LA Bucket Sim_Draft.xlsx", source)
        self.assertNotIn("BUCKET_SIM_DRAFT_XLSX", source)

    def test_lineage_hashes_service_and_profile_file_contents(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = root / "service.csv"
            profile = root / "profile.xlsx"
            service.write_bytes(b"service-v1")
            profile.write_bytes(b"profile-v1")
            empty_technicians = pd.DataFrame(columns=["employee_code", "preferred_region_name"])
            empty_jobs = pd.DataFrame(columns=["PROMISE_DATE"])
            with (
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs._load_scenario_region_inputs", return_value=({}, {})),
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs._load_la_jobs", return_value=empty_jobs),
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs._load_dms_technicians", return_value=empty_technicians),
                patch("admin_tools.db.seeds.build_la_bucket_vrp_inputs._build_dms2_technicians", return_value=empty_technicians),
            ):
                first = _write_inputs(service, profile, root / "out-one", {})
                service.write_bytes(b"service-v2")
                profile.write_bytes(b"profile-v2")
                second = _write_inputs(service, profile, root / "out-two", {})
            first_lineage = json.loads(Path(first["lineage_path"]).read_text(encoding="utf-8"))
            second_lineage = json.loads(Path(second["lineage_path"]).read_text(encoding="utf-8"))
            self.assertEqual(str(service.resolve()), first_lineage["service_source"]["path"])
            self.assertEqual(hashlib.sha256(b"service-v1").hexdigest(), first_lineage["service_source"]["sha256"])
            self.assertEqual(hashlib.sha256(b"profile-v1").hexdigest(), first_lineage["profile_source"]["sha256"])
            self.assertEqual(hashlib.sha256(b"service-v2").hexdigest(), second_lineage["service_source"]["sha256"])
            self.assertEqual(hashlib.sha256(b"profile-v2").hexdigest(), second_lineage["profile_source"]["sha256"])


if __name__ == "__main__":
    unittest.main()
