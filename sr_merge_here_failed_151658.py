from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd

from smart_routing.here_geocoder import HereGeocoder


GEO_COLS = [
    "matched_address",
    "match_indicator",
    "match_type",
    "latitude",
    "longitude",
    "census_state_fips",
    "census_county_fips",
    "census_tract",
    "census_block",
    "geocoded_date",
    "source",
]


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).replace("\u00a0", " ").strip().split())


def clean_key(value: object) -> str:
    return clean_text(value).upper()


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def read_csv_if_exists(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df[columns].copy()


def save_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def apply_here_cache(base_df: pd.DataFrame, here_cache: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    out = base_df.copy()
    if here_cache.empty:
        return out, 0
    cache = here_cache.copy()
    cache["_merge_key"] = cache["address_key"].map(clean_key)
    cache = cache[cache["_merge_key"].ne("") & cache["latitude"].notna() & cache["longitude"].notna()].copy()
    cache = cache.drop_duplicates(subset=["_merge_key"], keep="last")
    if cache.empty:
        return out, 0

    out["_merge_key"] = out["address_key"].map(clean_key)
    failed_mask = out["source"].astype(str).eq("failed") | out["latitude"].isna() | out["longitude"].isna()
    match_mask = failed_mask & out["_merge_key"].isin(set(cache["_merge_key"]))
    lookup = cache.set_index("_merge_key")
    matched_keys = out.loc[match_mask, "_merge_key"]
    for col in GEO_COLS:
        if col in lookup.columns:
            out.loc[match_mask, col] = matched_keys.map(lookup[col]).to_numpy()
    out = out.drop(columns=["_merge_key"])
    return out, int(match_mask.sum())


def run(base_file: Path, output_file: Path, report_file: Path, config_file: Path) -> None:
    cfg = load_config(config_file)
    geocoding_cfg = cfg.get("geocoding", {})
    here_key = str(geocoding_cfg.get("here_api_key", "")).strip()
    if not here_key:
        raise ValueError("Missing geocoding.here_api_key in config.json")

    here_cache_path = Path(str(geocoding_cfg.get("here_cache_file", "data/geocode_cache_here.csv")))
    here_attempt_log_path = Path(str(geocoding_cfg.get("here_attempt_log_file", "data/geocode_attempted_here.csv")))

    base = pd.read_csv(base_file, encoding="utf-8-sig", low_memory=False)
    before_failed = int((base["source"].astype(str).eq("failed") | base["latitude"].isna() | base["longitude"].isna()).sum())

    cache_cols = [
        "address_key",
        "address_line1",
        "city",
        "state",
        "postal_code",
        "country_name",
        *GEO_COLS,
        "tiger_line_id",
        "tiger_line_side",
    ]
    attempt_cols = ["address_key", "attempted_date", "status", "source"]
    here_cache = read_csv_if_exists(here_cache_path, cache_cols)
    here_attempt_log = read_csv_if_exists(here_attempt_log_path, attempt_cols)

    base, cache_applied_before = apply_here_cache(base, here_cache)
    failed_mask = base["source"].astype(str).eq("failed") | base["latitude"].isna() | base["longitude"].isna()
    failed = base.loc[failed_mask].copy()
    failed["_merge_key"] = failed["address_key"].map(clean_key)
    unique_failed = failed[failed["_merge_key"].ne("")].drop_duplicates(subset=["_merge_key"], keep="first").copy()

    cached_keys = set(here_cache["address_key"].map(clean_key)) if not here_cache.empty else set()
    pending = unique_failed[~unique_failed["_merge_key"].isin(cached_keys)].copy()

    run_month = date.today().strftime("%Y-%m")
    monthly_limit = int(geocoding_cfg.get("here_monthly_limit", 10000))
    if not here_attempt_log.empty and "attempted_date" in here_attempt_log.columns:
        used_this_month = int(here_attempt_log["attempted_date"].astype(str).str.startswith(run_month).sum())
    else:
        used_this_month = 0
    monthly_remaining = max(monthly_limit - used_this_month, 0)
    pending = pending.head(monthly_remaining).copy()

    geocoder = HereGeocoder(
        api_key=here_key,
        cache_path=here_cache_path,
        attempt_log_path=here_attempt_log_path,
        monthly_limit=monthly_limit,
        sleep_sec=float(geocoding_cfg.get("here_sleep_sec", 0.05)),
        min_query_score=float(geocoding_cfg.get("here_min_query_score", 0.7)),
        min_field_score=float(geocoding_cfg.get("here_min_field_score", 0.7)),
    )

    new_cache_rows: list[dict[str, object]] = []
    new_attempt_rows: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(pending.iterrows(), start=1):
        result, attempt = geocoder._geocode_one(
            address_line1=clean_text(row.get("ADDRESS_LINE1_INFO", "")),
            city=clean_text(row.get("CITY_NAME", "")),
            state=clean_text(row.get("STATE_NAME", "")),
            postal_code=clean_text(row.get("POSTAL_CODE", "")),
            country_name=clean_text(row.get("COUNTRY_NAME", "")),
            address_key=clean_text(row.get("address_key", "")),
        )
        if result is not None:
            new_cache_rows.append(result)
        if attempt is not None:
            new_attempt_rows.append(attempt)
        if idx % 100 == 0 or idx == len(pending):
            if new_cache_rows:
                here_cache = pd.concat([here_cache, pd.DataFrame(new_cache_rows)], ignore_index=True)
                here_cache = here_cache.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
                save_cache(here_cache_path, here_cache)
                new_cache_rows = []
            if new_attempt_rows:
                here_attempt_log = pd.concat([here_attempt_log, pd.DataFrame(new_attempt_rows)], ignore_index=True)
                here_attempt_log = here_attempt_log.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
                save_cache(here_attempt_log_path, here_attempt_log)
                new_attempt_rows = []
            print(f"here progress={idx}/{len(pending)} cache_success={int((here_cache['source'].astype(str) == 'here_geocoding_api').sum())}", flush=True)

    here_cache = read_csv_if_exists(here_cache_path, cache_cols)
    final, cache_applied_after = apply_here_cache(base, here_cache)
    final_failed = int((final["source"].astype(str).eq("failed") | final["latitude"].isna() | final["longitude"].isna()).sum())
    output_file.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(output_file, index=False, encoding="utf-8-sig")

    report_rows = [
        {"metric": "base_file", "value": str(base_file)},
        {"metric": "output_file", "value": str(output_file)},
        {"metric": "before_failed_or_missing", "value": before_failed},
        {"metric": "here_cache_applied_before_run", "value": cache_applied_before},
        {"metric": "unique_failed_before_here_run", "value": int(len(unique_failed))},
        {"metric": "here_monthly_limit", "value": monthly_limit},
        {"metric": "here_monthly_used_before_run", "value": used_this_month},
        {"metric": "here_monthly_remaining_before_run", "value": monthly_remaining},
        {"metric": "here_attempted_this_run", "value": int(len(pending))},
        {"metric": "here_rows_applied_after_run", "value": cache_applied_after},
        {"metric": "final_failed_or_missing", "value": final_failed},
    ]
    for source, count in final["source"].fillna("").astype(str).value_counts().items():
        report_rows.append({"metric": f"final_source_{source}", "value": int(count)})
    report = pd.DataFrame(report_rows)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(report_file, index=False, encoding="utf-8-sig")

    print(f"before_failed_or_missing={before_failed}", flush=True)
    print(f"here_attempted_this_run={len(pending)}", flush=True)
    print(f"here_rows_applied_after_run={cache_applied_after}", flush=True)
    print(f"final_failed_or_missing={final_failed}", flush=True)
    print(final["source"].fillna("").astype(str).value_counts().to_string(), flush=True)
    print(f"output_file={output_file}", flush=True)
    print(f"report_file={report_file}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-file", type=Path, default=Path("260310/input/Service_202606151658_census_only_geocoded_with_151604_google.csv"))
    parser.add_argument("--output-file", type=Path, default=Path("260310/input/Service_202606151658_final_geocoded.csv"))
    parser.add_argument("--report-file", type=Path, default=Path("260310/output/here_merge_failed_151658_report.csv"))
    parser.add_argument("--config-file", type=Path, default=Path("config.json"))
    args = parser.parse_args()
    run(args.base_file, args.output_file, args.report_file, args.config_file)


if __name__ == "__main__":
    main()
