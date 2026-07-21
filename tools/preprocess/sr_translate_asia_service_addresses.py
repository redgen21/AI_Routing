from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pandas as pd

from smart_routing.asia_geocode_cleaner import (
    azure_clean_geocode_queries,
    build_default_query,
    cache_columns,
    cache_frame_to_queries,
    clean_text,
    default_country_name,
    normalize_cleaned_item,
    queries_to_cache_frame,
)


ADDRESS_COLUMNS = ["ADDRESS_LINE1_INFO", "CITY_NAME", "STATE_NAME", "COUNTRY_NAME", "POSTAL_CODE"]


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def address_key(row: pd.Series) -> str:
    return "|".join(clean_text(row.get(col, "")) for col in ADDRESS_COLUMNS)


def clean_query(value: object) -> str:
    text = clean_text(value)
    text = text.replace("\\", " ")
    parts = [clean_text(part) for part in text.split(",")]
    parts = [part for part in parts if part and part.lower() != "nan"]
    return ", ".join(parts)


def load_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=cache_columns())
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    if "asia_address_key" in df.columns and "address_key" not in df.columns:
        df = df.rename(columns={"asia_address_key": "address_key"})
    for col in cache_columns():
        if col not in df.columns:
            df[col] = ""
    return df[cache_columns()].copy()


def save_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL, escapechar="\\")


def build_unique_addresses(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    for col in ADDRESS_COLUMNS:
        if col not in working.columns:
            working[col] = ""
        working[col] = working[col].map(clean_text)
    working["asia_address_key"] = working.apply(address_key, axis=1)
    unique_df = working[ADDRESS_COLUMNS + ["asia_address_key"]].drop_duplicates("asia_address_key").reset_index(drop=True)
    unique_df["default_geocode_query"] = unique_df.apply(build_default_query, axis=1)
    return unique_df


def translate_unique_addresses(
    unique_df: pd.DataFrame,
    llm_cfg: dict,
    cache_file: Path,
    batch_size: int,
    limit: int | None,
) -> pd.DataFrame:
    cache = load_cache(cache_file)
    done = set(cache["address_key"].astype(str))
    pending = unique_df[~unique_df["asia_address_key"].astype(str).isin(done)].copy()
    pending = pending[pending["asia_address_key"].astype(str).str.strip().ne("")].copy()
    if limit is not None and limit >= 0:
        pending = pending.head(limit).copy()
    print(f"translation cached={len(cache)} pending={len(pending)}", flush=True)

    for start in range(0, len(pending), batch_size):
        chunk = pending.iloc[start : start + batch_size].copy()
        records: list[dict[str, str]] = []
        id_to_key: dict[str, str] = {}
        id_to_fallback: dict[str, str] = {}
        for idx, (_, row) in enumerate(chunk.iterrows(), start=1):
            rec_id = str(idx)
            id_to_key[rec_id] = str(row["asia_address_key"])
            id_to_fallback[rec_id] = str(row["default_geocode_query"])
            records.append(
                {
                    "id": rec_id,
                    "address": clean_text(row.get("ADDRESS_LINE1_INFO", "")),
                    "city": clean_text(row.get("CITY_NAME", "")),
                    "state": clean_text(row.get("STATE_NAME", "")),
                    "country": default_country_name(row.get("COUNTRY_NAME", "")),
                    "postal_code": clean_text(row.get("POSTAL_CODE", "")),
                }
            )
        try:
            cleaned = azure_clean_geocode_queries(records, llm_cfg)
        except Exception as exc:
            cleaned = {rec_id: {"primary_geocode_query": "", "removed_noise": str(exc)} for rec_id in id_to_key}

        queries = []
        for rec_id, key in id_to_key.items():
            item = cleaned.get(rec_id, {}) if isinstance(cleaned, dict) else {}
            queries.append(normalize_cleaned_item(key, item, fallback_query=id_to_fallback.get(rec_id, "")))

        if queries:
            cache = pd.concat([cache, queries_to_cache_frame(queries)], ignore_index=True)
            cache = cache.drop_duplicates("address_key", keep="last").reset_index(drop=True)
            save_cache(cache_file, cache)
        print(f"translation progress={min(start + batch_size, len(pending))}/{len(pending)}", flush=True)

    return load_cache(cache_file)


def apply_translations(df: pd.DataFrame, unique_df: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    query_rows = []
    for query in cache_frame_to_queries(cache):
        variants = query.variants()
        row = {
            "asia_address_key": query.address_key,
            "translated_address": query.street_address,
            "translated_city": query.city,
            "translated_state": query.state,
            "translated_country": query.country,
            "translated_postal_code": query.postal_code,
            "translated_query": clean_query(query.primary_geocode_query),
            "geocode_query": clean_query(query.primary_geocode_query),
            "translation_status": query.clean_status,
            "place_name": query.place_name,
            "removed_noise": query.removed_noise,
        }
        for idx in range(1, 5):
            row[f"fallback_{idx}"] = ""
        for name, value in variants:
            if name.startswith("fallback_"):
                row[name] = clean_query(value)
        query_rows.append(row)

    translated = pd.DataFrame(query_rows)
    if translated.empty:
        translated = pd.DataFrame(
            columns=[
                "asia_address_key",
                "translated_address",
                "translated_city",
                "translated_state",
                "translated_country",
                "translated_postal_code",
                "translated_query",
                "geocode_query",
                "translation_status",
                "place_name",
                "removed_noise",
                "fallback_1",
                "fallback_2",
                "fallback_3",
                "fallback_4",
            ]
        )
    merged_unique = unique_df.merge(translated, on="asia_address_key", how="left")
    merged_unique["default_geocode_query"] = merged_unique["default_geocode_query"].map(clean_query)
    merged_unique["geocode_query"] = merged_unique["geocode_query"].map(clean_query)
    merged_unique["translated_query"] = merged_unique["translated_query"].map(clean_query)
    bad_query = (
        merged_unique["geocode_query"].fillna("").astype(str).str.strip().eq("")
        | merged_unique["geocode_query"].fillna("").astype(str).str.strip().str.startswith(",")
    )
    merged_unique.loc[bad_query, "geocode_query"] = merged_unique.loc[bad_query, "default_geocode_query"]
    merged_unique.loc[bad_query, "translated_query"] = merged_unique.loc[bad_query, "default_geocode_query"]
    merged_unique.loc[bad_query & merged_unique["translation_status"].fillna("").astype(str).eq("OK"), "translation_status"] = "DEFAULT_QUERY"
    missing_query = merged_unique["geocode_query"].fillna("").astype(str).str.strip().eq("")
    merged_unique.loc[missing_query, "geocode_query"] = merged_unique.loc[missing_query, "default_geocode_query"]
    merged_unique.loc[missing_query, "translated_query"] = merged_unique.loc[missing_query, "default_geocode_query"]
    merged_unique.loc[missing_query, "translation_status"] = "DEFAULT_QUERY"
    missing_status = merged_unique["translation_status"].fillna("").astype(str).str.strip().eq("")
    merged_unique.loc[missing_status, "translation_status"] = "DEFAULT_QUERY"

    working = df.copy()
    for col in ADDRESS_COLUMNS:
        if col not in working.columns:
            working[col] = ""
        working[col] = working[col].map(clean_text)
    working["asia_address_key"] = working.apply(address_key, axis=1)
    keep_cols = [
        "asia_address_key",
        "default_geocode_query",
        "translated_address",
        "translated_city",
        "translated_state",
        "translated_country",
        "translated_postal_code",
        "translated_query",
        "geocode_query",
        "translation_status",
        "place_name",
        "removed_noise",
        "fallback_1",
        "fallback_2",
        "fallback_3",
        "fallback_4",
    ]
    return working.merge(merged_unique[keep_cols], on="asia_address_key", how="left")


def run(source_file: Path, output_file: Path, summary_file: Path, config_file: Path, cache_file: Path, batch_size: int, limit: int | None) -> None:
    cfg = load_config(config_file)
    llm_cfg = cfg.get("llm", {})
    if not llm_cfg.get("enabled", True) or not clean_text(llm_cfg.get("api_key", "")):
        raise ValueError("Missing enabled llm.api_key in config/config.json.")

    raw_df = pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)
    unique_df = build_unique_addresses(raw_df)
    print(f"source_rows={len(raw_df)}", flush=True)
    print(f"unique_addresses={len(unique_df)}", flush=True)

    cache = translate_unique_addresses(unique_df, llm_cfg, cache_file, batch_size, limit)
    output = apply_translations(raw_df, unique_df, cache)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_file, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL, escapechar="\\")

    summary = (
        output.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME"], dropna=False)
        .agg(
            total=("GSFS_RECEIPT_NO", "count"),
            unique_addresses=("asia_address_key", "nunique"),
            translated_ok=("translation_status", lambda s: int(s.fillna("").astype(str).eq("OK").sum())),
            default_query=("translation_status", lambda s: int(s.fillna("").astype(str).eq("DEFAULT_QUERY").sum())),
        )
        .reset_index()
    )
    total = pd.DataFrame(
        [
            {
                "COUNTRY_NAME": "TOTAL",
                "STRATEGIC_CITY_NAME": "TOTAL",
                "total": int(len(output)),
                "unique_addresses": int(output["asia_address_key"].nunique()),
                "translated_ok": int(output["translation_status"].fillna("").astype(str).eq("OK").sum()),
                "default_query": int(output["translation_status"].fillna("").astype(str).eq("DEFAULT_QUERY").sum()),
            }
        ]
    )
    summary = pd.concat([summary, total], ignore_index=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_file, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL, escapechar="\\")
    print(summary.to_string(index=False), flush=True)
    print(f"output_file={output_file}", flush=True)
    print(f"summary_file={summary_file}", flush=True)
    print(f"cache_file={cache_file}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Translate/clean Asia service addresses for later geocoding.")
    parser.add_argument(
        "--source-file",
        default="260310/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202606271712.csv",
    )
    parser.add_argument(
        "--output-file",
        default="260310/input/Service_202606271712_asia_translated.csv",
    )
    parser.add_argument(
        "--summary-file",
        default="260310/output/asia_translation_summary_202606271712.csv",
    )
    parser.add_argument("--config-file", default="config/config.json")
    parser.add_argument("--cache-file", default="data/asia_address_translation_cache_202606271712.csv")
    parser.add_argument("--batch-size", type=int, default=30)
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of uncached unique addresses to translate.")
    args = parser.parse_args()
    run(
        source_file=Path(args.source_file),
        output_file=Path(args.output_file),
        summary_file=Path(args.summary_file),
        config_file=Path(args.config_file),
        cache_file=Path(args.cache_file),
        batch_size=max(1, int(args.batch_size)),
        limit=args.limit,
    )


if __name__ == "__main__":
    main()

