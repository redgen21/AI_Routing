from __future__ import annotations

import argparse
from pathlib import Path

from smart_routing.data_catalog import na_data_path
from smart_routing.service_preprocess import preprocess_service_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize raw service CSV for Smart Routing.")
    parser.add_argument(
        "--service-file",
        default=str(na_data_path("service_raw")),
        help="Raw service CSV file (defaults to the active data catalog).",
    )
    parser.add_argument(
        "--output-file",
        default=str(na_data_path("service_geocoded")),
        help="Normalized output CSV file (defaults to the active data catalog).",
    )
    parser.add_argument("--config-file", default="config/config.json", help="Config file with geocoding settings.")
    parser.add_argument("--skip-geocode", action="store_true", help="Only normalize the service file without geocoding.")
    parser.add_argument(
        "--geocode-backend",
        choices=["auto", "nominatim", "asia-fallback"],
        default="auto",
        help="Geocoding backend. auto uses Asia Nominatim->HERE->Google or USA Census->HERE->Google.",
    )
    parser.add_argument("--retry-failed", action="store_true", help="Retry addresses already cached as failed.")
    parser.add_argument("--limit", type=int, default=None, help="Optional max unique addresses to geocode.")
    args = parser.parse_args()

    summary = preprocess_service_file(
        Path(args.service_file),
        Path(args.output_file),
        config_file=Path(args.config_file),
        geocode=not args.skip_geocode,
        geocode_backend=args.geocode_backend,
        retry_failed=bool(args.retry_failed),
        limit=args.limit,
    )
    print(f"source_rows={summary.source_rows}")
    print(f"output_rows={summary.output_rows}")
    print(f"dropped_blank_address_rows={summary.dropped_blank_address_rows}")
    print(f"dropped_blank_receipt_rows={summary.dropped_blank_receipt_rows}")
    print(f"dropped_duplicate_receipt_rows={summary.dropped_duplicate_receipt_rows}")
    print(f"unique_address_rows={summary.unique_address_rows}")
    print(f"nominatim_attempted_rows={summary.nominatim_attempted_rows}")
    print(f"here_attempted_rows={summary.here_attempted_rows}")
    print(f"google_attempted_rows={summary.google_attempted_rows}")
    print(f"geocoded_rows={summary.geocoded_rows}")
    print(f"failed_geocode_rows={summary.failed_geocode_rows}")
    print(f"nominatim_remaining_rows={summary.nominatim_remaining_rows}")
    print(f"output_file={Path(args.output_file)}")


if __name__ == "__main__":
    main()

