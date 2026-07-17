from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def merge_google_cache(source_file: Path, cache_file: Path, output_file: Path) -> pd.DataFrame:
    service_df = pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)
    cache_df = pd.read_csv(cache_file, encoding="utf-8-sig", low_memory=False)
    if "asia_address_key" not in service_df.columns or "asia_address_key" not in cache_df.columns:
        raise ValueError("Both source and cache must include asia_address_key.")

    cache_columns = [
        "asia_address_key",
        "geocode_query",
        "geocode_status",
        "latitude",
        "longitude",
        "matched_address",
        "location_type",
        "place_id",
        "source",
        "error_message",
    ]
    for col in cache_columns:
        if col not in cache_df.columns:
            cache_df[col] = ""
    cache_df = cache_df[cache_columns].drop_duplicates("asia_address_key", keep="last")

    drop_columns = [col for col in cache_columns if col != "asia_address_key" and col in service_df.columns]
    merged = service_df.drop(columns=drop_columns, errors="ignore").merge(
        cache_df,
        on="asia_address_key",
        how="left",
    )
    merged["latitude"] = pd.to_numeric(merged["latitude"], errors="coerce")
    merged["longitude"] = pd.to_numeric(merged["longitude"], errors="coerce")
    valid = (
        merged["latitude"].between(-90, 90)
        & merged["longitude"].between(-180, 180)
        & ~(merged["latitude"].eq(0) & merged["longitude"].eq(0))
        & merged["geocode_status"].astype(str).eq("OK")
    )
    merged.loc[valid, "source"] = "google_geocoding_api"
    merged.loc[~valid, "source"] = "failed"
    merged["geocoded_ok"] = valid

    output_file.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_file, index=False, encoding="utf-8-sig")
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge cached Asia Google coordinates into service data.")
    parser.add_argument(
        "--source-file",
        default="260310/input/Service_202606161433_asia_latin_cleaned.csv",
    )
    parser.add_argument(
        "--cache-file",
        default="data/asia_google_geocode_cache_202606161433.csv",
    )
    parser.add_argument(
        "--output-file",
        default="260310/input/Service_202606161433_asia_google_cached_geocoded.csv",
    )
    args = parser.parse_args()

    result = merge_google_cache(Path(args.source_file), Path(args.cache_file), Path(args.output_file))
    print(f"output_file={args.output_file}")
    print(f"rows={len(result)}")
    print(f"geocoded_ok={int(result['geocoded_ok'].sum())}")
    print(f"geocoded_failed={int((~result['geocoded_ok']).sum())}")
    print(
        result.groupby("STRATEGIC_CITY_NAME")["geocoded_ok"]
        .agg(["sum", "count"])
        .to_string()
    )


if __name__ == "__main__":
    main()
