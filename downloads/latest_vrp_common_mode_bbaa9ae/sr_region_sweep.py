from __future__ import annotations

import argparse
from pathlib import Path

from smart_routing.region_sweep import sweep_region_counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep candidate region counts and compare routing balance.")
    parser.add_argument("--service-file", default="260310/input/Service_202603181109_geocoded.csv")
    parser.add_argument("--config-file", default="config.json")
    parser.add_argument("--output-dir", default="260310/output")
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
