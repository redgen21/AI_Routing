from __future__ import annotations

import argparse
from pathlib import Path

from smart_routing.data_catalog import na_data_path
from smart_routing.region_sweep import sweep_region_counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep candidate region counts and compare routing balance.")
    parser.add_argument("--service-file", default=str(na_data_path("service_geocoded")))
    parser.add_argument("--config-file", default="config/config.json")
    parser.add_argument("--output-dir", default=str(na_data_path("reports_dir")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = sweep_region_counts(
        service_file=Path(args.service_file),
        config_file=Path(args.config_file),
        output_dir=Path(args.output_dir),
    )
    print(f"summary_path={result.summary_path}")
    print(f"detail_path={result.detail_path}")
    print(f"summary_rows={len(result.summary_df)}")
    print(f"detail_rows={len(result.detail_df)}")


if __name__ == "__main__":
    main()

