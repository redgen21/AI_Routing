"""Standalone city-level region and technician candidate planner."""
from __future__ import annotations

import argparse
from pathlib import Path

from smart_routing.data_catalog import na_data_path
from smart_routing.region_candidate_planner import build_city_region_candidate


DEFAULT_SERVICE_FILE = Path("260310/input/Service_202606151658_final_geocoded.csv")
DEFAULT_PROFILE_FILE = Path("260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a reviewable region/technician candidate for one strategic city.")
    parser.add_argument("--city", required=True, help='Exact STRATEGIC_CITY_NAME, for example "Atlanta, GA".')
    parser.add_argument("--region-count", type=int, required=True, help="Number of candidate regions to create.")
    parser.add_argument("--algorithm", choices=["contiguous_balanced", "weighted_kmeans", "weighted_kmeans_staffing", "capacity_balanced_contiguous", "center_shared_radial"], default="contiguous_balanced")
    parser.add_argument("--service-file", type=Path, default=DEFAULT_SERVICE_FILE)
    parser.add_argument("--profile-file", type=Path, default=DEFAULT_PROFILE_FILE)
    parser.add_argument("--technician-city", help="Optional Address-sheet city override when it differs from --city.")
    parser.add_argument("--max-daily-jobs-per-technician", type=int, default=8)
    parser.add_argument("--plan-id", help="Optional immutable candidate identifier.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=na_data_path("region_candidates_dir") / "home_allocation",
        help="Candidate-only output root; reviewed plans are never overwritten.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_city_region_candidate(
        service_file=args.service_file,
        profile_file=args.profile_file,
        city_name=args.city,
        region_count=args.region_count,
        algorithm=args.algorithm,
        output_root=args.output_root,
        max_daily_jobs_per_technician=args.max_daily_jobs_per_technician,
        technician_city=args.technician_city,
        plan_id=args.plan_id,
    )
    print(f"plan_id={result.plan_id}")
    print(f"output_dir={result.output_dir}")
    print(f"area_map_region_postals={result.region_postals_path}")
    print(f"technician_assignments={result.technician_assignments_path}")
    print(f"region_summary={result.region_summary_path}")
    print(f"technician_evidence={result.evidence_path}")
    print(f"rejects={result.rejects_path}")
    print(f"manifest={result.manifest_path}")


if __name__ == "__main__":
    main()
