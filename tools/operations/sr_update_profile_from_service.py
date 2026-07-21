from __future__ import annotations

import argparse
from pathlib import Path

from smart_routing.data_catalog import na_data_path
from smart_routing.profile_sync import build_updated_profile


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update Zip Coverage and Slot using active DMS/DMS2 service engineers.")
    parser.add_argument("--profile-file", default=str(na_data_path("profile_raw")))
    parser.add_argument("--service-file", default=str(na_data_path("service_raw")))
    parser.add_argument("--input-dir", default=str(na_data_path("region_candidates_dir") / "profile_sync"))
    parser.add_argument("--output-dir", default=str(na_data_path("reports_dir") / "profile_sync"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_updated_profile(
        profile_file=Path(args.profile_file),
        service_file=Path(args.service_file),
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
    )
    print(f"zip_output_path={result.zip_output_path}")
    print(f"slot_output_path={result.slot_output_path}")
    print(f"unmatched_output_path={result.unmatched_output_path}")
    print(f"summary_output_path={result.summary_output_path}")
    for _, row in result.summary_df.iterrows():
        print(f"{row['metric']}={row['value']}")


if __name__ == "__main__":
    main()
