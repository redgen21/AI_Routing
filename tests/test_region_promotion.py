import unittest
import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from tools.data.promote_region_plan import (
    _assert_replace_allowed,
    _load_approved_evaluation,
    promote,
    validate_region_plan,
)


class RegionPromotionTests(unittest.TestCase):
    def test_promotion_requires_evidence_and_writes_versioned_artifacts(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            candidate_dir = data_root / "planning" / "regions" / "candidates"
            candidate_dir.mkdir(parents=True)
            candidate = candidate_dir / "candidate.csv"
            pd.DataFrame([{"POSTAL_CODE": "90001", "region_id": "R1", "region_seq": 1}]).to_csv(
                candidate, index=False
            )
            candidate_hash = hashlib.sha256(candidate.read_bytes()).hexdigest()
            evidence = root / "evaluation.json"
            evidence.write_text(
                json.dumps(
                    {
                        "schema": "north-america-region-evaluation/v1",
                        "status": "passed",
                        "candidate_sha256": candidate_hash,
                        "checks": {
                            "coverage_complete": True,
                            "duplicate_postal_count": 0,
                            "empty_region_count": 0,
                            "fixed_boundaries_preserved": True,
                            "routing_evaluated": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            catalog = root / "catalog.json"
            catalog.write_text(
                json.dumps(
                    {
                        "schema": "north-america-routing-data-catalog/v1",
                        "data_root": str(data_root),
                        "active": {
                            "region_candidates_dir": "planning/regions/candidates",
                            "reviewed_regions_dir": "reviewed/regions",
                            "region_seed_dir": "db_input/regions",
                        },
                    }
                ),
                encoding="utf-8",
            )
            previous = os.environ.get("NA_DATA_CATALOG_PATH")
            os.environ["NA_DATA_CATALOG_PATH"] = str(catalog)
            try:
                result = promote(
                    candidate,
                    "la_plan-001.csv",
                    plan_id="plan-001",
                    evaluation_file=evidence,
                    approved_by="qa",
                    approval_reference="TEST-1",
                    seed_name="la_seed_plan-001.csv",
                    city="Los Angeles, CA",
                    service_file=None,
                )
            finally:
                if previous is None:
                    os.environ.pop("NA_DATA_CATALOG_PATH", None)
                else:
                    os.environ["NA_DATA_CATALOG_PATH"] = previous
            self.assertEqual(result["plan_id"], "plan-001")
            self.assertTrue((data_root / "reviewed" / "regions" / "la_plan-001.csv").is_file())
            self.assertTrue((data_root / "db_input" / "regions" / "la_seed_plan-001.csv").is_file())
            self.assertTrue((data_root / "reviewed" / "regions" / "la_plan-001.review.json").is_file())

    def test_region_plan_rejects_blank_region_id(self) -> None:
        with TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.csv"
            pd.DataFrame([{"POSTAL_CODE": "90001", "region_id": "", "region_seq": 1}]).to_csv(
                candidate, index=False
            )
            with self.assertRaisesRegex(ValueError, "blank postal codes or region IDs"):
                validate_region_plan(candidate)

    def test_evaluation_must_match_candidate_and_all_gates(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.csv"
            candidate.write_text("POSTAL_CODE,region_id,region_seq\n90001,R1,1\n", encoding="utf-8")
            candidate_hash = hashlib.sha256(candidate.read_bytes()).hexdigest()
            evidence = root / "evaluation.json"
            evidence.write_text(
                json.dumps(
                    {
                        "schema": "north-america-region-evaluation/v1",
                        "status": "passed",
                        "candidate_sha256": candidate_hash,
                        "checks": {
                            "coverage_complete": True,
                            "duplicate_postal_count": 0,
                            "empty_region_count": 0,
                            "fixed_boundaries_preserved": True,
                            "routing_evaluated": False,
                        },
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "gates did not pass"):
                _load_approved_evaluation(evidence, candidate_hash)

    def test_existing_artifact_requires_matching_compare_and_swap_hash(self) -> None:
        with TemporaryDirectory() as directory:
            target = Path(directory) / "reviewed.csv"
            target.write_text("current", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                _assert_replace_allowed(target, None)
            with self.assertRaises(RuntimeError):
                _assert_replace_allowed(target, "wrong")
            expected = hashlib.sha256(target.read_bytes()).hexdigest()
            _assert_replace_allowed(target, expected)

    def test_region_plan_rejects_duplicate_postal_assignments(self) -> None:
        with TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.csv"
            pd.DataFrame(
                [
                    {"POSTAL_CODE": "90001", "region_id": "R1", "region_seq": 1},
                    {"POSTAL_CODE": "90001", "region_id": "R2", "region_seq": 2},
                ]
            ).to_csv(candidate, index=False)
            with self.assertRaisesRegex(ValueError, "more than one region"):
                validate_region_plan(candidate)

    def test_region_plan_requires_full_service_postal_coverage(self) -> None:
        with TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            candidate = tmp_path / "candidate.csv"
            service = tmp_path / "service.csv"
            pd.DataFrame([{"POSTAL_CODE": "90001", "region_id": "R1", "region_seq": 1}]).to_csv(
                candidate, index=False
            )
            pd.DataFrame(
                [
                    {"STRATEGIC_CITY_NAME": "Los Angeles, CA", "POSTAL_CODE": "90001"},
                    {"STRATEGIC_CITY_NAME": "Los Angeles, CA", "POSTAL_CODE": "90002"},
                ]
            ).to_csv(service, index=False)
            with self.assertRaisesRegex(ValueError, "does not cover"):
                validate_region_plan(service_file=service, candidate=candidate, city="Los Angeles, CA")


if __name__ == "__main__":
    unittest.main()
