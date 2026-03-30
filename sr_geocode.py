from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd

from smart_routing.census_geocoder import (
    CensusBatchGeocoder,
    load_geocode_cache,
    merge_service_with_geocodes,
    read_table,
)
from smart_routing.google_geocoder import GoogleGeocoder


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    parser = argparse.ArgumentParser(description="Geocode North America service addresses with US Census Geocoder.")
    parser.add_argument("--config-file", default="config.json", help="Path to JSON config file.")
    parser.add_argument("--service-file", required=True, help="Path to service CSV/XLSX file.")
    parser.add_argument(
        "--cache-file",
        default=None,
        help="Persistent cache file path.",
    )
    parser.add_argument(
        "--daily-log-file",
        default=None,
        help="Daily quota usage log path.",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help="Merged service output path. Default: <service_dir>/input/<stem>_geocoded.csv",
    )
    parser.add_argument(
        "--report-file",
        default=None,
        help="Run report path. Default: <service_dir>/output/geocode_report_<stem>.csv",
    )
    parser.add_argument("--run-date", default=None, help="Quota date in YYYY-MM-DD. Default: today.")
    parser.add_argument("--daily-limit", type=int, default=None, help="Max new addresses per day.")
    parser.add_argument("--batch-size", type=int, default=None, help="Addresses per Census batch request.")
    parser.add_argument("--google-fallback", action="store_true", help="Retry unmatched Census addresses with Google.")
    parser.add_argument("--google-api-key", default=None, help="Google Maps Geocoding API key.")
    parser.add_argument("--google-cache-file", default=None, help="Persistent Google fallback cache file path.")
    parser.add_argument("--google-attempt-log-file", default=None, help="Google fallback attempted-address log path.")
    parser.add_argument("--google-monthly-limit", type=int, default=None, help="Max Google geocoding attempts per month.")
    parser.add_argument("--google-sleep-sec", type=float, default=None, help="Sleep seconds between Google requests.")
    parser.add_argument(
        "--google-retry-previous-attempts-once",
        action="store_true",
        help="Ignore previous Google attempt log for this run only. New attempts will still be logged.",
    )
    parser.add_argument(
        "--max-new-per-run",
        type=int,
        default=None,
        help="Optional cap smaller than the daily limit for a single run.",
    )
    args = parser.parse_args()
    config = load_config(Path(args.config_file))
    geocoding_cfg = config.get("geocoding", {})

    service_path = Path(args.service_file)
    if not service_path.exists():
        raise FileNotFoundError(f"Service file not found: {service_path}")

    cache_file = args.cache_file or geocoding_cfg.get("census_cache_file", "data/geocode_cache_us_census.csv")
    daily_log_file = args.daily_log_file or geocoding_cfg.get("census_daily_log_file", "data/geocode_daily_log_us_census.json")
    google_cache_file = args.google_cache_file or geocoding_cfg.get("google_cache_file", "data/geocode_cache_google.csv")
    google_attempt_log_file = args.google_attempt_log_file or geocoding_cfg.get("google_attempt_log_file", "data/geocode_attempted_google.csv")
    google_monthly_limit = args.google_monthly_limit if args.google_monthly_limit is not None else int(geocoding_cfg.get("google_monthly_limit", 10000))
    daily_limit = args.daily_limit if args.daily_limit is not None else int(geocoding_cfg.get("daily_limit", 10000))
    batch_size = args.batch_size if args.batch_size is not None else int(geocoding_cfg.get("batch_size", 1000))
    google_sleep_sec = args.google_sleep_sec if args.google_sleep_sec is not None else float(geocoding_cfg.get("google_sleep_sec", 0.05))
    google_api_key = (
        args.google_api_key
        or os.getenv("GOOGLE_MAPS_API_KEY")
        or geocoding_cfg.get("google_api_key")
    )

    default_output = service_path.parent / "input" / f"{service_path.stem}_geocoded.csv"
    default_report = service_path.parent / "output" / f"geocode_report_{service_path.stem}.csv"
    output_path = Path(args.output_file) if args.output_file else default_output
    report_path = Path(args.report_file) if args.report_file else default_report

    def write_merged_output() -> dict[str, int]:
        service_df = read_table(service_path)
        census_cache_df = load_geocode_cache(Path(cache_file))
        cache_frames = [census_cache_df]
        google_cache_path_obj = Path(google_cache_file)
        if google_cache_path_obj.exists():
            cache_frames.append(load_geocode_cache(google_cache_path_obj))
        all_cache_df = pd.concat(cache_frames, ignore_index=True)
        all_cache_df = all_cache_df.drop_duplicates(subset=["address_key"], keep="first").reset_index(drop=True)
        merged_df = merge_service_with_geocodes(service_df, all_cache_df)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        final_failed_mask = merged_df["source"].astype(str).eq("failed")
        return {
            "final_failed_rows": int(final_failed_mask.sum()),
            "final_success_rows": int((~final_failed_mask).sum()),
            "final_unresolved_unique": int(merged_df.loc[final_failed_mask, "address_key"].nunique()),
            "final_resolved_unique": int(merged_df.loc[~final_failed_mask, "address_key"].nunique()),
        }

    geocoder = CensusBatchGeocoder(
        cache_path=Path(cache_file),
        log_path=Path(daily_log_file),
        daily_limit=daily_limit,
        batch_size=batch_size,
    )
    result = geocoder.run_for_service_file(
        service_path=service_path,
        merged_output_path=output_path,
        report_path=report_path,
        run_date=args.run_date,
        max_new_per_run=args.max_new_per_run,
    )
    final_summary = write_merged_output()

    google_result = None
    if args.google_fallback:
        if not google_api_key:
            raise ValueError("Google fallback requested but no API key was provided.")
        google = GoogleGeocoder(
            api_key=google_api_key,
            cache_path=Path(google_cache_file),
            attempt_log_path=Path(google_attempt_log_file),
            monthly_limit=google_monthly_limit,
            sleep_sec=google_sleep_sec,
        )
        google_result = google.run_for_unmatched(
            service_path=service_path,
            census_cache_path=Path(cache_file),
            run_date=args.run_date,
            ignore_attempt_log_once=args.google_retry_previous_attempts_once,
        )
        final_summary = write_merged_output()

    print(f"run_date={result.run_date}")
    print(f"total_unique_addresses={result.total_unique_addresses}")
    print(f"already_cached={result.already_cached}")
    print(f"pending_before_run={result.pending_before_run}")
    print(f"attempted_today={result.attempted_today}")
    print(f"geocoded_today={result.geocoded_today}")
    print(f"failed_today={result.failed_today}")
    print(f"remaining_after_run={result.remaining_after_run}")
    print(f"final_success_rows={final_summary['final_success_rows']}")
    print(f"final_failed_rows={final_summary['final_failed_rows']}")
    print(f"final_resolved_unique={final_summary['final_resolved_unique']}")
    print(f"final_unresolved_unique={final_summary['final_unresolved_unique']}")
    print(f"cache_path={result.cache_path}")
    print(f"merged_output_path={result.merged_output_path}")
    print(f"report_path={report_path}")
    print(f"config_file={Path(args.config_file)}")
    if google_result is not None:
        print(f"google_run_month={google_result.run_month}")
        print(f"google_monthly_limit={google_result.monthly_limit}")
        print(f"google_monthly_used_before_run={google_result.monthly_used_before_run}")
        print(f"google_monthly_remaining_before_run={google_result.monthly_remaining_before_run}")
        print(f"google_retry_previous_attempts_once={args.google_retry_previous_attempts_once}")
        print(f"google_attempted={google_result.attempted}")
        print(f"google_geocoded={google_result.geocoded}")
        print(f"google_failed={google_result.failed}")
        print(f"google_cache_path={google_result.cache_path}")
        print(f"google_attempt_log_path={google_result.attempt_log_path}")


if __name__ == "__main__":
    main()
