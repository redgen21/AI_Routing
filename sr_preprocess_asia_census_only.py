from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from urllib import parse, request

import pandas as pd

from smart_routing.census_geocoder import CensusBatchGeocoder, normalize_census_address_text
from smart_routing.service_preprocess import normalize_service_df


COUNTRY_NAME = {
    "THA": "Thailand",
    "IDN": "Indonesia",
    "MYS": "Malaysia",
}

ADDRESS_COLUMNS = ["ADDRESS_LINE1_INFO", "CITY_NAME", "STATE_NAME", "COUNTRY_NAME", "POSTAL_CODE"]


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def _has_non_latin(value: object) -> bool:
    return any(ord(ch) > 127 for ch in str(value or ""))


def _country_display(value: object) -> str:
    text = _clean_text(value).upper()
    return COUNTRY_NAME.get(text, text)


def _address_key(row: pd.Series) -> str:
    return "|".join(_clean_text(row.get(col, "")) for col in ADDRESS_COLUMNS)


def _load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_cache(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    if "asia_address_key" not in df.columns and "address_key" in df.columns:
        df = df.rename(columns={"address_key": "asia_address_key"})
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df[columns].copy()


def _save_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _default_query(row: pd.Series) -> str:
    parts = [
        _clean_text(row.get("ADDRESS_LINE1_INFO", "")),
        _clean_text(row.get("CITY_NAME", "")),
        _clean_text(row.get("STATE_NAME", "")),
        _clean_text(row.get("POSTAL_CODE", "")),
        _country_display(row.get("COUNTRY_NAME", "")),
    ]
    return ", ".join([part for part in parts if part])


def _azure_translate_batch(records: list[dict[str, str]], llm_cfg: dict) -> dict[str, dict[str, str]]:
    base_url = str(llm_cfg["base_url"]).rstrip("/")
    deployment = str(llm_cfg.get("deployment") or llm_cfg.get("model"))
    api_version = str(llm_cfg["api_version"])
    url = f"{base_url}/openai/deployments/{parse.quote(deployment)}/chat/completions?api-version={parse.quote(api_version)}"
    payload = {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You transliterate and translate Southeast Asian postal addresses into English/Latin script for geocoding. "
                    "Return a JSON object mapping each id to fields: address, city, state, country, postal_code, query. "
                    "Preserve house numbers, unit numbers, street numbers, building/village names, and postal codes. "
                    "Do not add explanations."
                ),
            },
            {"role": "user", "content": json.dumps(records, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
        "max_completion_tokens": int(llm_cfg.get("max_completion_tokens", 12000)),
    }
    headers = {"Content-Type": "application/json", "api-key": str(llm_cfg["api_key"])}
    timeout = int(llm_cfg.get("timeout_sec", 30))
    max_retries = int(llm_cfg.get("max_retries", 2))
    backoff = float(llm_cfg.get("retry_backoff_sec", 1.0))
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        req = request.Request(url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), headers=headers, method="POST")
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return json.loads(data["choices"][0]["message"].get("content", "{}"))
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Azure translation failed: {last_error}") from last_error


def _build_unique_addresses(service_df: pd.DataFrame) -> pd.DataFrame:
    working = service_df.copy()
    for col in ADDRESS_COLUMNS:
        if col not in working.columns:
            working[col] = ""
        working[col] = working[col].map(_clean_text)
    working["asia_address_key"] = working.apply(_address_key, axis=1)
    unique_df = working[ADDRESS_COLUMNS + ["asia_address_key"]].drop_duplicates(subset=["asia_address_key"]).reset_index(drop=True)
    unique_df["needs_translation"] = (
        unique_df["ADDRESS_LINE1_INFO"] + " " + unique_df["CITY_NAME"] + " " + unique_df["STATE_NAME"]
    ).map(_has_non_latin)
    unique_df["default_query"] = unique_df.apply(_default_query, axis=1)
    return unique_df


def _translate_missing(unique_df: pd.DataFrame, translation_cache_file: Path, llm_cfg: dict, batch_size: int) -> pd.DataFrame:
    cols = [
        "asia_address_key",
        "translated_address",
        "translated_city",
        "translated_state",
        "translated_country",
        "translated_postal_code",
        "translated_query",
        "translation_status",
    ]
    cache = _read_cache(translation_cache_file, cols)
    done = set(cache["asia_address_key"].astype(str))
    pending = unique_df[unique_df["needs_translation"] & ~unique_df["asia_address_key"].isin(done)].copy()
    print(f"translation cached={len(cache)} pending={len(pending)}", flush=True)
    rows: list[dict[str, object]] = []
    for start in range(0, len(pending), batch_size):
        chunk = pending.iloc[start : start + batch_size].copy()
        id_to_key: dict[str, str] = {}
        records: list[dict[str, str]] = []
        for idx, (_, row) in enumerate(chunk.iterrows(), start=1):
            rec_id = str(idx)
            id_to_key[rec_id] = str(row["asia_address_key"])
            records.append(
                {
                    "id": rec_id,
                    "address": str(row["ADDRESS_LINE1_INFO"]),
                    "city": str(row["CITY_NAME"]),
                    "state": str(row["STATE_NAME"]),
                    "country": str(row["COUNTRY_NAME"]),
                    "postal_code": str(row["POSTAL_CODE"]),
                }
            )
        try:
            translated = _azure_translate_batch(records, llm_cfg)
        except Exception as exc:
            translated = {rec_id: {"query": "", "error": str(exc)} for rec_id in id_to_key}
        for rec_id, key in id_to_key.items():
            item = translated.get(rec_id, {}) if isinstance(translated, dict) else {}
            query = _clean_text(item.get("query", ""))
            rows.append(
                {
                    "asia_address_key": key,
                    "translated_address": _clean_text(item.get("address", "")),
                    "translated_city": _clean_text(item.get("city", "")),
                    "translated_state": _clean_text(item.get("state", "")),
                    "translated_country": _clean_text(item.get("country", "")),
                    "translated_postal_code": _clean_text(item.get("postal_code", "")),
                    "translated_query": query,
                    "translation_status": "OK" if query else "FAILED",
                }
            )
        cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
        cache = cache.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
        _save_cache(translation_cache_file, cache)
        rows = []
        print(f"translation progress={min(start + batch_size, len(pending))}/{len(pending)}", flush=True)
    return _read_cache(translation_cache_file, cols)


def _census_input_from_unique(unique_df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "address_key": unique_df["asia_address_key"].astype(str),
            "address_line1": unique_df["census_address_line1"].map(normalize_census_address_text),
            "city": unique_df["census_city"].map(normalize_census_address_text),
            "state": unique_df["census_state"].map(normalize_census_address_text),
            "postal_code": unique_df["census_postal_code"].map(_clean_text),
            "country_name": unique_df["census_country"].map(_clean_text),
        }
    )
    out = out[out["address_line1"].astype(str).str.strip().ne("")].copy()
    out = out.drop_duplicates(subset=["address_key"]).reset_index(drop=True)
    return out


def _geocode_batch_safe(geocoder: CensusBatchGeocoder, chunk: pd.DataFrame, bad_rows: list[dict[str, object]]) -> pd.DataFrame:
    if chunk.empty:
        return geocoder._empty_cache_frame()
    try:
        return geocoder._geocode_batch(chunk)
    except Exception as exc:
        if len(chunk) <= 1:
            row = chunk.iloc[0].to_dict()
            row["error"] = f"{type(exc).__name__}: {exc}"
            bad_rows.append(row)
            return geocoder._empty_cache_frame()
        sub_size = max(1, len(chunk) // 10)
        frames = []
        for start in range(0, len(chunk), sub_size):
            result = _geocode_batch_safe(geocoder, chunk.iloc[start : start + sub_size].copy(), bad_rows)
            if not result.empty:
                frames.append(result)
        if not frames:
            return geocoder._empty_cache_frame()
        return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["address_key"], keep="first").reset_index(drop=True)


def _census_only_geocode(
    census_input: pd.DataFrame,
    attempt_cache_file: Path,
    match_cache_file: Path,
    bad_rows_file: Path,
    batch_size: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    attempt_cols = ["asia_address_key", "census_status"]
    match_cols = [
        "asia_address_key",
        "matched_address",
        "match_indicator",
        "match_type",
        "longitude",
        "latitude",
        "census_state_fips",
        "census_county_fips",
        "census_tract",
        "census_block",
        "source",
    ]
    attempts = _read_cache(attempt_cache_file, attempt_cols)
    matches = _read_cache(match_cache_file, match_cols)
    done = set(attempts["asia_address_key"].astype(str))
    pending = census_input[~census_input["address_key"].astype(str).isin(done)].copy().reset_index(drop=True)
    print(f"census attempted_cached={len(attempts)} pending={len(pending)}", flush=True)

    geocoder = CensusBatchGeocoder(
        cache_path=Path("data/asia_census_internal_unused_cache.csv"),
        log_path=Path("data/asia_census_internal_unused_log.json"),
        timeout=180,
        batch_size=batch_size,
    )
    bad_rows: list[dict[str, object]] = []
    attempt_rows: list[dict[str, object]] = []
    match_rows: list[dict[str, object]] = []
    for start in range(0, len(pending), batch_size):
        chunk = pending.iloc[start : start + batch_size].copy()
        result = _geocode_batch_safe(geocoder, chunk, bad_rows)
        matched_keys = set(result["address_key"].astype(str)) if not result.empty else set()
        for key in chunk["address_key"].astype(str).tolist():
            attempt_rows.append({"asia_address_key": key, "census_status": "MATCH" if key in matched_keys else "FAILED"})
        if not result.empty:
            renamed = result.rename(columns={"address_key": "asia_address_key"})
            for col in match_cols:
                if col not in renamed.columns:
                    renamed[col] = ""
            match_rows.extend(renamed[match_cols].to_dict("records"))

        if attempt_rows:
            attempts = pd.concat([attempts, pd.DataFrame(attempt_rows)], ignore_index=True)
            attempts = attempts.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
            _save_cache(attempt_cache_file, attempts)
            attempt_rows = []
        if match_rows:
            matches = pd.concat([matches, pd.DataFrame(match_rows)], ignore_index=True)
            matches = matches.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
            _save_cache(match_cache_file, matches)
            match_rows = []
        if bad_rows:
            _save_cache(bad_rows_file, pd.DataFrame(bad_rows))
        print(f"census progress={min(start + batch_size, len(pending))}/{len(pending)} matches={len(matches)}", flush=True)
    return attempts, matches


def run(
    source_file: Path,
    output_file: Path,
    summary_file: Path,
    config_file: Path,
    translation_cache_file: Path,
    census_attempt_cache_file: Path,
    census_match_cache_file: Path,
    bad_rows_file: Path,
    translation_batch_size: int,
    census_batch_size: int,
) -> None:
    cfg = _load_config(config_file)
    llm_cfg = cfg.get("llm", {})
    raw_df = pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)
    service_df, preprocess_summary = normalize_service_df(raw_df)
    unique_df = _build_unique_addresses(service_df)
    print(f"source_rows={preprocess_summary.source_rows}", flush=True)
    print(f"service_rows_after_preprocess={len(service_df)}", flush=True)
    print(f"unique_addresses={len(unique_df)}", flush=True)
    print(f"unique_addresses_needing_translation={int(unique_df['needs_translation'].sum())}", flush=True)

    translations = _translate_missing(unique_df, translation_cache_file, llm_cfg, translation_batch_size)
    unique_df = unique_df.merge(translations, on="asia_address_key", how="left")
    translated_ok = unique_df["translated_query"].fillna("").astype(str).str.strip().ne("")
    unique_df["census_address_line1"] = unique_df["ADDRESS_LINE1_INFO"]
    unique_df["census_city"] = unique_df["CITY_NAME"]
    unique_df["census_state"] = unique_df["STATE_NAME"]
    unique_df["census_postal_code"] = unique_df["POSTAL_CODE"]
    unique_df["census_country"] = unique_df["COUNTRY_NAME"].map(_country_display)
    unique_df.loc[translated_ok, "census_address_line1"] = unique_df.loc[translated_ok, "translated_address"]
    unique_df.loc[translated_ok, "census_city"] = unique_df.loc[translated_ok, "translated_city"]
    unique_df.loc[translated_ok, "census_state"] = unique_df.loc[translated_ok, "translated_state"]
    unique_df.loc[translated_ok, "census_postal_code"] = unique_df.loc[translated_ok, "translated_postal_code"]
    unique_df.loc[translated_ok, "census_country"] = unique_df.loc[translated_ok, "translated_country"]

    census_input = _census_input_from_unique(unique_df)
    attempts, matches = _census_only_geocode(
        census_input,
        census_attempt_cache_file,
        census_match_cache_file,
        bad_rows_file,
        census_batch_size,
    )

    service_df = service_df.copy()
    for col in ADDRESS_COLUMNS:
        if col not in service_df.columns:
            service_df[col] = ""
        service_df[col] = service_df[col].map(_clean_text)
    service_df["asia_address_key"] = service_df.apply(_address_key, axis=1)
    enriched = service_df.merge(
        unique_df[
            [
                "asia_address_key",
                "needs_translation",
                "translated_address",
                "translated_city",
                "translated_state",
                "translated_country",
                "translated_postal_code",
                "translated_query",
                "translation_status",
                "census_address_line1",
                "census_city",
                "census_state",
                "census_postal_code",
                "census_country",
            ]
        ],
        on="asia_address_key",
        how="left",
    )
    enriched = enriched.merge(attempts, on="asia_address_key", how="left")
    enriched = enriched.merge(matches, on="asia_address_key", how="left")
    enriched["latitude"] = pd.to_numeric(enriched.get("latitude"), errors="coerce")
    enriched["longitude"] = pd.to_numeric(enriched.get("longitude"), errors="coerce")
    ok = enriched["latitude"].notna() & enriched["longitude"].notna()
    enriched["source"] = enriched.get("source", pd.Series("", index=enriched.index)).fillna("")
    enriched.loc[ok & enriched["source"].eq(""), "source"] = "us_census_geocoder"
    enriched.loc[~ok, "source"] = "failed"
    enriched["geocoded_ok"] = ok
    output_file.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(output_file, index=False, encoding="utf-8-sig")

    summary = (
        enriched.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME"], dropna=False)
        .agg(total=("GSFS_RECEIPT_NO", "count"), geocoded_ok=("geocoded_ok", "sum"))
        .reset_index()
    )
    summary["geocoded_failed"] = summary["total"] - summary["geocoded_ok"]
    total = pd.DataFrame(
        [
            {
                "COUNTRY_NAME": "TOTAL",
                "STRATEGIC_CITY_NAME": "TOTAL",
                "total": int(len(enriched)),
                "geocoded_ok": int(ok.sum()),
                "geocoded_failed": int((~ok).sum()),
            }
        ]
    )
    summary = pd.concat([summary, total], ignore_index=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_file, index=False, encoding="utf-8-sig")
    print(summary.to_string(index=False), flush=True)
    print(f"output_file={output_file}", flush=True)
    print(f"summary_file={summary_file}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Translate Asia service addresses and geocode with Census only.")
    parser.add_argument("--source-file", default="260310/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202606161433.csv")
    parser.add_argument("--output-file", default="260310/input/Service_202606161433_asia_census_only_geocoded.csv")
    parser.add_argument("--summary-file", default="260310/output/asia_census_only_geocode_summary_202606161433.csv")
    parser.add_argument("--config-file", default="config.json")
    parser.add_argument("--translation-cache-file", default="data/asia_address_translation_cache_202606161433.csv")
    parser.add_argument("--census-attempt-cache-file", default="data/asia_census_attempt_cache_202606161433.csv")
    parser.add_argument("--census-match-cache-file", default="data/asia_census_match_cache_202606161433.csv")
    parser.add_argument("--bad-rows-file", default="260310/output/asia_census_bad_batch_addresses_202606161433.csv")
    parser.add_argument("--translation-batch-size", type=int, default=50)
    parser.add_argument("--census-batch-size", type=int, default=1000)
    args = parser.parse_args()
    run(
        source_file=Path(args.source_file),
        output_file=Path(args.output_file),
        summary_file=Path(args.summary_file),
        config_file=Path(args.config_file),
        translation_cache_file=Path(args.translation_cache_file),
        census_attempt_cache_file=Path(args.census_attempt_cache_file),
        census_match_cache_file=Path(args.census_match_cache_file),
        bad_rows_file=Path(args.bad_rows_file),
        translation_batch_size=max(1, int(args.translation_batch_size)),
        census_batch_size=max(1, int(args.census_batch_size)),
    )


if __name__ == "__main__":
    main()
