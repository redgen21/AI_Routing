from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib import error, parse, request

import pandas as pd

from smart_routing.service_preprocess import normalize_service_df


COUNTRY_NAME = {
    "THA": "Thailand",
    "IDN": "Indonesia",
    "MYS": "Malaysia",
}

ADDRESS_COLUMNS = ["ADDRESS_LINE1_INFO", "CITY_NAME", "STATE_NAME", "COUNTRY_NAME", "POSTAL_CODE"]


def _load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def _has_non_latin(value: object) -> bool:
    return any(ord(ch) > 127 for ch in str(value or ""))


def _address_key(row: pd.Series) -> str:
    return "|".join(_clean_text(row.get(col, "")) for col in ADDRESS_COLUMNS)


def _country_display(value: object) -> str:
    text = _clean_text(value).upper()
    return COUNTRY_NAME.get(text, text)


def _default_query(row: pd.Series) -> str:
    parts = [
        _clean_text(row.get("ADDRESS_LINE1_INFO", "")),
        _clean_text(row.get("CITY_NAME", "")),
        _clean_text(row.get("STATE_NAME", "")),
        _clean_text(row.get("POSTAL_CODE", "")),
        _country_display(row.get("COUNTRY_NAME", "")),
    ]
    return ", ".join([part for part in parts if part])


def _read_cache(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df[columns].copy()


def _save_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


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


def _google_geocode(query: str, api_key: str, timeout: int = 30) -> dict[str, object]:
    params = parse.urlencode({"address": query, "key": api_key})
    url = f"https://maps.googleapis.com/maps/api/geocode/json?{params}"
    try:
        with request.urlopen(url, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        return {"status": f"HTTP_{exc.code}", "source": "google_geocoding_api"}
    except error.URLError:
        return {"status": "URL_ERROR", "source": "google_geocoding_api"}

    status = _clean_text(payload.get("status")) or "UNKNOWN"
    if status != "OK":
        return {"status": status, "source": "google_geocoding_api", "error_message": _clean_text(payload.get("error_message", ""))}
    results = payload.get("results") or []
    if not results:
        return {"status": "NO_RESULTS", "source": "google_geocoding_api"}
    top = results[0]
    location = (top.get("geometry") or {}).get("location") or {}
    try:
        lat = float(location.get("lat"))
        lng = float(location.get("lng"))
    except (TypeError, ValueError):
        return {"status": "NO_COORDS", "source": "google_geocoding_api"}
    return {
        "status": "OK",
        "source": "google_geocoding_api",
        "latitude": lat,
        "longitude": lng,
        "matched_address": _clean_text(top.get("formatted_address", "")),
        "location_type": _clean_text((top.get("geometry") or {}).get("location_type", "")),
        "place_id": _clean_text(top.get("place_id", "")),
    }


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
    unique_df["default_geocode_query"] = unique_df.apply(_default_query, axis=1)
    return unique_df


def _translate_pending(unique_df: pd.DataFrame, translation_cache_file: Path, llm_cfg: dict, batch_size: int) -> pd.DataFrame:
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
    if translation_cache_file.exists():
        cache = pd.read_csv(translation_cache_file, encoding="utf-8-sig", low_memory=False)
        if "asia_address_key" not in cache.columns and "address_key" in cache.columns:
            cache = cache.rename(columns={"address_key": "asia_address_key"})
        for col in cols:
            if col not in cache.columns:
                cache[col] = ""
        cache = cache[cols].copy()
    else:
        cache = pd.DataFrame(columns=cols)
    done = set(cache["asia_address_key"].astype(str))
    pending = unique_df[unique_df["needs_translation"] & ~unique_df["asia_address_key"].isin(done)].copy()
    print(f"translation cached={len(cache)} pending={len(pending)}", flush=True)
    buffer: list[dict[str, object]] = []
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
            buffer.append(
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
        if (start // batch_size + 1) % 5 == 0 or start + batch_size >= len(pending):
            cache = pd.concat([cache, pd.DataFrame(buffer)], ignore_index=True)
            cache = cache.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
            _save_cache(translation_cache_file, cache)
            buffer = []
        print(f"translation progress={min(start + batch_size, len(pending))}/{len(pending)}", flush=True)
    return _read_cache(translation_cache_file, cols)


def _geocode_pending(unique_df: pd.DataFrame, google_cache_file: Path, google_key: str, sleep_sec: float) -> pd.DataFrame:
    cols = [
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
    cache = _read_cache(google_cache_file, cols)
    done = set(cache["asia_address_key"].astype(str))
    pending = unique_df[~unique_df["asia_address_key"].isin(done)].copy()
    print(f"google geocode cached={len(cache)} pending={len(pending)}", flush=True)
    buffer: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(pending.iterrows(), start=1):
        query = str(row["geocode_query"]).strip()
        result = _google_geocode(query, google_key)
        buffer.append(
            {
                "asia_address_key": row["asia_address_key"],
                "geocode_query": query,
                "geocode_status": result.get("status", ""),
                "latitude": result.get("latitude", ""),
                "longitude": result.get("longitude", ""),
                "matched_address": result.get("matched_address", ""),
                "location_type": result.get("location_type", ""),
                "place_id": result.get("place_id", ""),
                "source": result.get("source", "google_geocoding_api"),
                "error_message": result.get("error_message", ""),
            }
        )
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        if idx % 500 == 0 or idx == len(pending):
            cache = pd.concat([cache, pd.DataFrame(buffer)], ignore_index=True)
            cache = cache.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
            _save_cache(google_cache_file, cache)
            buffer = []
            print(f"google geocode progress={idx}/{len(pending)}", flush=True)
    return _read_cache(google_cache_file, cols)


def run(
    source_file: Path,
    output_file: Path,
    summary_file: Path,
    config_file: Path,
    translation_cache_file: Path,
    google_cache_file: Path,
    batch_size: int,
    geocode_sleep_sec: float,
) -> None:
    cfg = _load_config(config_file)
    llm_cfg = cfg.get("llm", {})
    google_key = str(cfg.get("geocoding", {}).get("google_api_key", "")).strip()
    if not google_key:
        raise ValueError("Missing geocoding.google_api_key in config/config.json.")
    if not llm_cfg.get("enabled", True) or not str(llm_cfg.get("api_key", "")).strip():
        raise ValueError("Missing enabled llm.api_key in config/config.json.")

    raw_df = pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)
    service_df, preprocess_summary = normalize_service_df(raw_df)
    unique_df = _build_unique_addresses(service_df)
    print(f"source_rows={preprocess_summary.source_rows}", flush=True)
    print(f"service_rows_after_preprocess={len(service_df)}", flush=True)
    print(f"unique_addresses={len(unique_df)}", flush=True)
    print(f"unique_addresses_needing_translation={int(unique_df['needs_translation'].sum())}", flush=True)

    translation_cache = _translate_pending(unique_df, translation_cache_file, llm_cfg, batch_size)
    unique_df = unique_df.merge(translation_cache, on="asia_address_key", how="left")
    unique_df["geocode_query"] = unique_df["default_geocode_query"]
    translated_ok = unique_df["translated_query"].fillna("").astype(str).str.strip().ne("")
    unique_df.loc[translated_ok, "geocode_query"] = unique_df.loc[translated_ok, "translated_query"]

    google_cache = _geocode_pending(unique_df, google_cache_file, google_key, geocode_sleep_sec)

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
                "default_geocode_query",
                "translated_address",
                "translated_city",
                "translated_state",
                "translated_country",
                "translated_postal_code",
                "translated_query",
                "translation_status",
                "geocode_query",
            ]
        ],
        on="asia_address_key",
        how="left",
    )
    enriched = enriched.merge(google_cache, on="asia_address_key", how="left", suffixes=("", "_google"))
    enriched["latitude"] = pd.to_numeric(enriched["latitude"], errors="coerce")
    enriched["longitude"] = pd.to_numeric(enriched["longitude"], errors="coerce")
    enriched["source"] = enriched["source"].fillna("failed")
    ok = enriched["latitude"].notna() & enriched["longitude"].notna() & enriched["geocode_status"].astype(str).eq("OK")
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
    parser = argparse.ArgumentParser(description="Preprocess Asia service data, translate addresses, and geocode with Google.")
    parser.add_argument("--source-file", default="260310/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202606161433.csv")
    parser.add_argument("--output-file", default="260310/input/Service_202606161433_asia_google_geocoded.csv")
    parser.add_argument("--summary-file", default="260310/output/asia_google_geocode_summary_202606161433.csv")
    parser.add_argument("--config-file", default="config/config.json")
    parser.add_argument("--translation-cache-file", default="data/asia_address_translation_cache_202606161433.csv")
    parser.add_argument("--google-cache-file", default="data/asia_google_geocode_cache_202606161433.csv")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--geocode-sleep-sec", type=float, default=0.02)
    args = parser.parse_args()
    run(
        source_file=Path(args.source_file),
        output_file=Path(args.output_file),
        summary_file=Path(args.summary_file),
        config_file=Path(args.config_file),
        translation_cache_file=Path(args.translation_cache_file),
        google_cache_file=Path(args.google_cache_file),
        batch_size=max(1, int(args.batch_size)),
        geocode_sleep_sec=max(0.0, float(args.geocode_sleep_sec)),
    )


if __name__ == "__main__":
    main()

