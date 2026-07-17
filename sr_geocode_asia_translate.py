from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from urllib import error, parse, request

import pandas as pd


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
    prompt = (
        "You transliterate and translate Southeast Asian postal addresses into English/Latin script for geocoding. "
        "Return a JSON object mapping each id to fields: address, city, state, country, postal_code, query. "
        "Preserve house numbers, unit numbers, street numbers, village/building names, and postal codes. "
        "Do not add explanations."
    )
    payload = {
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(records, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
        "max_completion_tokens": int(llm_cfg.get("max_completion_tokens", 12000)),
    }
    headers = {
        "Content-Type": "application/json",
        "api-key": str(llm_cfg["api_key"]),
    }
    timeout = int(llm_cfg.get("timeout_sec", 30))
    max_retries = int(llm_cfg.get("max_retries", 2))
    backoff = float(llm_cfg.get("retry_backoff_sec", 1.0))
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        req = request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            content = data["choices"][0]["message"].get("content", "")
            parsed = json.loads(content)
            return {str(key): value for key, value in parsed.items() if isinstance(value, dict)}
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Azure translation failed: {last_error}") from last_error


def _here_geocode(query: str, api_key: str, timeout: int = 30) -> dict[str, object]:
    params = parse.urlencode({"q": query, "apiKey": api_key})
    url = f"https://geocode.search.hereapi.com/v1/geocode?{params}"
    try:
        with request.urlopen(url, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        return {"status": f"HTTP_{exc.code}", "source": "here_geocoding_api"}
    except error.URLError:
        return {"status": "URL_ERROR", "source": "here_geocoding_api"}
    items = payload.get("items") or []
    if not items:
        return {"status": "NO_RESULTS", "source": "here_geocoding_api"}
    top = items[0]
    position = top.get("position") or {}
    scoring = top.get("scoring") or {}
    try:
        lat = float(position.get("lat"))
        lng = float(position.get("lng"))
    except (TypeError, ValueError):
        return {"status": "NO_COORDS", "source": "here_geocoding_api"}
    return {
        "status": "OK",
        "source": "here_geocoding_api",
        "latitude": lat,
        "longitude": lng,
        "matched_address": _clean_text(top.get("title", "")),
        "result_type": _clean_text(top.get("resultType", "")),
        "query_score": scoring.get("queryScore", ""),
    }


def run(
    source_file: Path,
    output_file: Path,
    summary_file: Path,
    config_file: Path,
    translation_cache_file: Path,
    geocode_cache_file: Path,
    batch_size: int,
    geocode_sleep_sec: float,
) -> None:
    cfg = _load_config(config_file)
    llm_cfg = cfg.get("llm", {})
    here_key = str(cfg.get("geocoding", {}).get("here_api_key", "")).strip()
    if not here_key:
        raise ValueError("Missing geocoding.here_api_key in config.")
    if not llm_cfg.get("enabled", True) or not str(llm_cfg.get("api_key", "")).strip():
        raise ValueError("Missing enabled llm.api_key in config.")

    raw_df = pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)
    working = raw_df.copy()
    for col in ADDRESS_COLUMNS:
        if col not in working.columns:
            working[col] = ""
        working[col] = working[col].map(_clean_text)
    working["address_key"] = working.apply(_address_key, axis=1)
    unique_df = working[ADDRESS_COLUMNS + ["address_key"]].drop_duplicates(subset=["address_key"]).reset_index(drop=True)
    unique_df["needs_translation"] = (
        unique_df["ADDRESS_LINE1_INFO"]
        + " "
        + unique_df["CITY_NAME"]
        + " "
        + unique_df["STATE_NAME"]
    ).map(_has_non_latin)
    unique_df["default_query"] = unique_df.apply(_default_query, axis=1)

    translation_cols = ["address_key", "translated_address", "translated_city", "translated_state", "translated_country", "translated_postal_code", "translated_query", "translation_status"]
    translation_cache = _read_cache(translation_cache_file, translation_cols)
    translated_keys = set(translation_cache["address_key"].astype(str))
    pending_translate = unique_df[unique_df["needs_translation"] & ~unique_df["address_key"].isin(translated_keys)].copy()
    print(f"translation pending={len(pending_translate)} cached={len(translation_cache)}", flush=True)
    new_translation_rows: list[dict[str, object]] = []
    for start in range(0, len(pending_translate), batch_size):
        chunk = pending_translate.iloc[start : start + batch_size].copy()
        records = []
        id_to_key = {}
        for idx, (_, row) in enumerate(chunk.iterrows(), start=1):
            rec_id = str(idx)
            id_to_key[rec_id] = row["address_key"]
            records.append(
                {
                    "id": rec_id,
                    "address": row["ADDRESS_LINE1_INFO"],
                    "city": row["CITY_NAME"],
                    "state": row["STATE_NAME"],
                    "country": row["COUNTRY_NAME"],
                    "postal_code": row["POSTAL_CODE"],
                }
            )
        try:
            translated = _azure_translate_batch(records, llm_cfg)
        except Exception as exc:
            translated = {}
            for rec_id, key in id_to_key.items():
                translated[rec_id] = {"query": "", "error": str(exc)}
        for rec_id, key in id_to_key.items():
            item = translated.get(rec_id, {})
            new_translation_rows.append(
                {
                    "address_key": key,
                    "translated_address": _clean_text(item.get("address", "")),
                    "translated_city": _clean_text(item.get("city", "")),
                    "translated_state": _clean_text(item.get("state", "")),
                    "translated_country": _clean_text(item.get("country", "")),
                    "translated_postal_code": _clean_text(item.get("postal_code", "")),
                    "translated_query": _clean_text(item.get("query", "")),
                    "translation_status": "OK" if _clean_text(item.get("query", "")) else "FAILED",
                }
            )
        if (start // batch_size + 1) % 5 == 0 or start + batch_size >= len(pending_translate):
            translation_cache = pd.concat([translation_cache, pd.DataFrame(new_translation_rows)], ignore_index=True)
            translation_cache = translation_cache.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            _save_cache(translation_cache_file, translation_cache)
            new_translation_rows = []
        print(f"translation progress={min(start + batch_size, len(pending_translate))}/{len(pending_translate)}", flush=True)

    translation_cache = _read_cache(translation_cache_file, translation_cols)
    unique_df = unique_df.merge(translation_cache, on="address_key", how="left")
    unique_df["geocode_query"] = unique_df["default_query"]
    translated_ok = unique_df["translated_query"].fillna("").astype(str).str.strip().ne("")
    unique_df.loc[translated_ok, "geocode_query"] = unique_df.loc[translated_ok, "translated_query"]

    geocode_cols = ["address_key", "geocode_query", "status", "latitude", "longitude", "matched_address", "result_type", "query_score", "source"]
    geocode_cache = _read_cache(geocode_cache_file, geocode_cols)
    geocoded_keys = set(geocode_cache["address_key"].astype(str))
    pending_geocode = unique_df[~unique_df["address_key"].isin(geocoded_keys)].copy()
    print(f"geocode pending={len(pending_geocode)} cached={len(geocode_cache)}", flush=True)
    geocode_rows = []
    for idx, (_, row) in enumerate(pending_geocode.iterrows(), start=1):
        result = _here_geocode(str(row["geocode_query"]), here_key)
        geocode_rows.append(
            {
                "address_key": row["address_key"],
                "geocode_query": row["geocode_query"],
                "status": result.get("status", ""),
                "latitude": result.get("latitude", ""),
                "longitude": result.get("longitude", ""),
                "matched_address": result.get("matched_address", ""),
                "result_type": result.get("result_type", ""),
                "query_score": result.get("query_score", ""),
                "source": result.get("source", "here_geocoding_api"),
            }
        )
        if geocode_sleep_sec > 0:
            time.sleep(geocode_sleep_sec)
        if idx % 500 == 0 or idx == len(pending_geocode):
            geocode_cache = pd.concat([geocode_cache, pd.DataFrame(geocode_rows)], ignore_index=True)
            geocode_cache = geocode_cache.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            _save_cache(geocode_cache_file, geocode_cache)
            geocode_rows = []
            print(f"geocode progress={idx}/{len(pending_geocode)}", flush=True)

    geocode_cache = _read_cache(geocode_cache_file, geocode_cols)
    enriched = working.merge(unique_df[["address_key", "needs_translation", "geocode_query"] + translation_cols[1:]], on="address_key", how="left")
    enriched = enriched.merge(geocode_cache, on="address_key", how="left", suffixes=("", "_geo"))
    enriched["latitude"] = pd.to_numeric(enriched["latitude"], errors="coerce")
    enriched["longitude"] = pd.to_numeric(enriched["longitude"], errors="coerce")
    enriched["geocoded_ok"] = enriched["latitude"].notna() & enriched["longitude"].notna() & enriched["status"].astype(str).eq("OK")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(output_file, index=False, encoding="utf-8-sig")

    summary = (
        enriched.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME"], dropna=False)
        .agg(total=("GSFS_RECEIPT_NO", "count"), geocoded_ok=("geocoded_ok", "sum"))
        .reset_index()
    )
    summary["geocoded_failed"] = summary["total"] - summary["geocoded_ok"]
    total_row = pd.DataFrame(
        [
            {
                "COUNTRY_NAME": "TOTAL",
                "STRATEGIC_CITY_NAME": "TOTAL",
                "total": len(enriched),
                "geocoded_ok": int(enriched["geocoded_ok"].sum()),
                "geocoded_failed": int((~enriched["geocoded_ok"]).sum()),
            }
        ]
    )
    summary = pd.concat([summary, total_row], ignore_index=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_file, index=False, encoding="utf-8-sig")
    print(summary.to_string(index=False), flush=True)
    print(f"output_file={output_file}", flush=True)
    print(f"summary_file={summary_file}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Translate Asia service addresses to English and geocode with HERE.")
    parser.add_argument("--source-file", default="260310/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_T19_STRATEGIC_CITY_202606161433.csv")
    parser.add_argument("--output-file", default="260310/input/Service_202606161433_asia_translated_geocoded.csv")
    parser.add_argument("--summary-file", default="260310/output/asia_translated_geocode_summary_202606161433.csv")
    parser.add_argument("--config-file", default="config.json")
    parser.add_argument("--translation-cache-file", default="data/asia_address_translation_cache_202606161433.csv")
    parser.add_argument("--geocode-cache-file", default="data/asia_here_geocode_cache_202606161433.csv")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--geocode-sleep-sec", type=float, default=0.02)
    args = parser.parse_args()
    run(
        source_file=Path(args.source_file),
        output_file=Path(args.output_file),
        summary_file=Path(args.summary_file),
        config_file=Path(args.config_file),
        translation_cache_file=Path(args.translation_cache_file),
        geocode_cache_file=Path(args.geocode_cache_file),
        batch_size=max(1, int(args.batch_size)),
        geocode_sleep_sec=max(0.0, float(args.geocode_sleep_sec)),
    )


if __name__ == "__main__":
    main()
