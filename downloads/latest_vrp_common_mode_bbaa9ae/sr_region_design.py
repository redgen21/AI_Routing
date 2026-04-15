from __future__ import annotations

import argparse
from pathlib import Path

from smart_routing.region_design import build_region_design


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Design integrated routing regions for North America service data.")
    parser.add_argument("--service-file", default="260310/input/Service_202603181109_geocoded.csv")
    parser.add_argument("--slot-file", default="260310/input/Slot_updated_Service_202603181109.csv")
    parser.add_argument("--input-dir", default="260310/input")
    parser.add_argument("--output-dir", default="260310/output")
    parser.add_argument("--target-sm-per-region", type=int, default=5)
    parser.add_argument("--effective-service-per-sm", type=float, default=5.0)
    parser.add_argument("--balance-weight", type=float, default=120.0)
    parser.add_argument("--radius-weight", type=float, default=40.0)
    parser.add_argument("--algorithm", choices=["balanced", "weighted_kmeans"], default="balanced")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_region_design(
        service_file=Path(args.service_file),
        slot_file=Path(args.slot_file),
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        target_sm_per_region=args.target_sm_per_region,
        effective_service_per_sm=args.effective_service_per_sm,
        balance_weight=args.balance_weight,
        radius_weight=args.radius_weight,
        algorithm=args.algorithm,
    )
    print(f"city_summary_path={result.city_summary_path}")
    print(f"region_summary_path={result.region_summary_path}")
    print(f"postal_assignment_path={result.postal_assignment_path}")
    print(f"service_assignment_path={result.service_assignment_path}")
    print(f"city_count={len(result.city_summary_df)}")
    print(f"region_count={len(result.region_summary_df)}")
    print(f"postal_count={len(result.postal_assignment_df)}")
    print(f"service_count={len(result.service_assignment_df)}")


if __name__ == "__main__":
    main()
