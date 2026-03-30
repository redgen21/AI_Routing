from __future__ import annotations

import argparse
from pathlib import Path

from smart_routing.routing_compare import build_routing_compare


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare current routing vs integrated region routing using OSRM.")
    parser.add_argument("--service-file", default="260310/input/Service_202603181109_geocoded.csv")
    parser.add_argument("--region-service-file", default="")
    parser.add_argument("--config-file", default="config.json")
    parser.add_argument("--output-dir", default="260310/output")
    parser.add_argument("--cities", nargs="*", default=[])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    region_service_file = Path(args.region_service_file) if str(args.region_service_file).strip() else None
    result = build_routing_compare(
        service_file=Path(args.service_file),
        region_service_file=region_service_file,
        config_file=Path(args.config_file),
        output_dir=Path(args.output_dir),
        cities=args.cities or None,
    )
    print(f"route_detail_path={result.route_detail_path}")
    print(f"daily_summary_path={result.daily_summary_path}")
    print(f"city_summary_path={result.city_summary_path}")
    print(f"overall_summary_path={result.overall_summary_path}")
    print(f"route_rows={len(result.route_detail_df)}")
    print(f"daily_rows={len(result.daily_summary_df)}")
    print(f"city_rows={len(result.city_summary_df)}")
    print(f"overall_rows={len(result.overall_summary_df)}")


if __name__ == "__main__":
    main()
