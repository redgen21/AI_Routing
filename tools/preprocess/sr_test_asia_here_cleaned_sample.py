from __future__ import annotations

import json
import time
from pathlib import Path
from urllib import parse, request

import pandas as pd

from smart_routing.here_geocoder import HereGeocoder


SAMPLE_FILE = Path("260310/output/asia_here_sample_100_by_city_202606161433.csv")
CLEAN_CACHE_FILE = Path("data/asia_here_sample_cleaned_query_cache_202606161433.csv")
OUTPUT_FILE = Path("260310/output/asia_here_cleaned_sample_100_by_city_202606161433.csv")
SUMMARY_FILE = Path("260310/output/asia_here_cleaned_sample_100_by_city_summary_202606161433.csv")


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def load_config() -> dict:
    return json.loads(Path("config/config.json").read_text(encoding="utf-8"))


def read_cache() -> pd.DataFrame:
    cols = [
        "asia_address_key",
        "cleaned_geocode_query",
        "place_name",
        "street_address",
        "city",
        "state",
        "postal_code",
        "country",
        "removed_noise",
        "clean_status",
    ]
    if not CLEAN_CACHE_FILE.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(CLEAN_CACHE_FILE, encoding="utf-8-sig", low_memory=False)
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    return df[cols].drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)


def save_cache(df: pd.DataFrame) -> None:
    CLEAN_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEAN_CACHE_FILE, index=False, encoding="utf-8-sig")


def azure_clean_batch(records: list[dict[str, str]], llm_cfg: dict) -> dict[str, dict[str, str]]:
    base_url = str(llm_cfg["base_url"]).rstrip("/")
    deployment = str(llm_cfg.get("deployment") or llm_cfg.get("model"))
    api_version = str(llm_cfg["api_version"])
    url = f"{base_url}/openai/deployments/{parse.quote(deployment)}/chat/completions?api-version={parse.quote(api_version)}"
    system = (
        "You clean Southeast Asian service addresses for geocoding. "
        "Return only a JSON object mapping each id to these fields: "
        "cleaned_geocode_query, place_name, street_address, city, state, postal_code, country, removed_noise. "
        "Keep useful place/building/store names when they help identify the location. "
        "Keep house number, street/soi/jalan, district/subdistrict, city, postal code, country. "
        "Remove phone numbers, WhatsApp notes, directions, parenthetical instructions, landmarks that are only directions, "
        "repair notes, customer notes, duplicate country/city fragments, and trailing .0 from postal codes. "
        "Do not invent missing house numbers. Do not add explanations."
    )
    payload = {
        "messages": [
            {"role": "system", "content": system},
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
            content = data["choices"][0]["message"].get("content", "{}")
            parsed = json.loads(content)
            return {str(key): value for key, value in parsed.items() if isinstance(value, dict)}
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Azure clean failed: {last_error}") from last_error


def main() -> None:
    cfg = load_config()
    llm_cfg = cfg.get("llm", {})
    here_key = str(cfg.get("geocoding", {}).get("here_api_key", "")).strip()
    if not here_key:
        raise ValueError("Missing geocoding.here_api_key")
    if not str(llm_cfg.get("api_key", "")).strip():
        raise ValueError("Missing llm.api_key")

    sample = pd.read_csv(SAMPLE_FILE, encoding="utf-8-sig", low_memory=False)
    sample = sample.drop_duplicates(subset=["asia_address_key"]).reset_index(drop=True)

    cache = read_cache()
    done = set(cache["asia_address_key"].astype(str))
    pending = sample[~sample["asia_address_key"].astype(str).isin(done)].copy()
    print(f"clean_query cached={len(cache)} pending={len(pending)}", flush=True)

    batch_size = 30
    new_rows: list[dict[str, object]] = []
    for start in range(0, len(pending), batch_size):
        chunk = pending.iloc[start : start + batch_size]
        records = []
        id_to_key: dict[str, str] = {}
        for idx, (_, row) in enumerate(chunk.iterrows(), start=1):
            rec_id = str(idx)
            id_to_key[rec_id] = str(row["asia_address_key"])
            records.append(
                {
                    "id": rec_id,
                    "country_code": clean_text(row.get("COUNTRY_NAME", "")),
                    "strategic_city": clean_text(row.get("STRATEGIC_CITY_NAME", "")),
                    "current_query": clean_text(row.get("query", "")),
                }
            )
        try:
            cleaned = azure_clean_batch(records, llm_cfg)
        except Exception as exc:
            cleaned = {rec_id: {"cleaned_geocode_query": "", "removed_noise": str(exc)} for rec_id in id_to_key}
        for rec_id, key in id_to_key.items():
            item = cleaned.get(rec_id, {}) if isinstance(cleaned, dict) else {}
            query = clean_text(item.get("cleaned_geocode_query", ""))
            new_rows.append(
                {
                    "asia_address_key": key,
                    "cleaned_geocode_query": query,
                    "place_name": clean_text(item.get("place_name", "")),
                    "street_address": clean_text(item.get("street_address", "")),
                    "city": clean_text(item.get("city", "")),
                    "state": clean_text(item.get("state", "")),
                    "postal_code": clean_text(item.get("postal_code", "")),
                    "country": clean_text(item.get("country", "")),
                    "removed_noise": clean_text(item.get("removed_noise", "")),
                    "clean_status": "OK" if query else "FAILED",
                }
            )
        cache = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
        cache = cache.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)
        save_cache(cache)
        new_rows = []
        print(f"clean_query progress={min(start + batch_size, len(pending))}/{len(pending)}", flush=True)

    cache = read_cache()
    sample = sample.merge(cache, on="asia_address_key", how="left")
    sample["cleaned_geocode_query"] = sample["cleaned_geocode_query"].fillna("").map(clean_text)
    sample["here_query"] = sample["cleaned_geocode_query"]
    sample.loc[sample["here_query"].eq(""), "here_query"] = sample.loc[sample["here_query"].eq(""), "query"].map(clean_text)

    geocoder = HereGeocoder(
        api_key=here_key,
        cache_path=Path("data/_tmp_here_cleaned_sample_cache_unused.csv"),
        attempt_log_path=Path("data/_tmp_here_cleaned_sample_attempt_unused.csv"),
        monthly_limit=100000,
        timeout=30,
        sleep_sec=0.05,
        min_query_score=float(cfg.get("geocoding", {}).get("here_min_query_score", 0.7)),
        min_field_score=float(cfg.get("geocoding", {}).get("here_min_field_score", 0.7)),
    )

    rows = []
    for idx, (_, row) in enumerate(sample.iterrows(), start=1):
        query_parts = [part.strip() for part in str(row["here_query"]).split(",")]
        address_line1 = query_parts[0] if query_parts else clean_text(row.get("here_query", ""))
        country = clean_text(row.get("country", "")) or clean_text(row.get("COUNTRY_NAME", ""))
        postal_code = clean_text(row.get("postal_code", ""))
        city = clean_text(row.get("city", "")) or clean_text(row.get("STRATEGIC_CITY_NAME", ""))
        state = clean_text(row.get("state", ""))
        result, attempt = geocoder._geocode_one(
            address_line1=address_line1,
            city=city,
            state=state,
            postal_code=postal_code,
            country_name=country,
            address_key=str(row["asia_address_key"]),
        )
        rows.append(
            {
                "STRATEGIC_CITY_NAME": clean_text(row.get("STRATEGIC_CITY_NAME", "")),
                "COUNTRY_NAME": clean_text(row.get("COUNTRY_NAME", "")),
                "asia_address_key": row["asia_address_key"],
                "original_query": clean_text(row.get("query", "")),
                "cleaned_geocode_query": clean_text(row.get("here_query", "")),
                "status": (attempt or {}).get("status", "URL_ERROR"),
                "ok": result is not None,
                "latitude": "" if result is None else result.get("latitude", ""),
                "longitude": "" if result is None else result.get("longitude", ""),
                "matched_address": "" if result is None else result.get("matched_address", ""),
                "match_type": "" if result is None else result.get("match_type", ""),
                "removed_noise": clean_text(row.get("removed_noise", "")),
                "source": "here_geocoding_api",
            }
        )
        if idx % 50 == 0 or idx == len(sample):
            print(f"here progress={idx}/{len(sample)}", flush=True)

    out = pd.DataFrame(rows)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    summary = (
        out.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME"], dropna=False)
        .agg(sample_count=("asia_address_key", "count"), here_ok=("ok", "sum"))
        .reset_index()
    )
    summary["here_failed"] = summary["sample_count"] - summary["here_ok"]
    summary["success_rate_pct"] = (summary["here_ok"] / summary["sample_count"] * 100).round(1)
    summary.to_csv(SUMMARY_FILE, index=False, encoding="utf-8-sig")
    status_counts = out.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME", "status"], dropna=False).size().reset_index(name="count")
    print(summary.to_string(index=False), flush=True)
    print("status counts", flush=True)
    print(status_counts.to_string(index=False), flush=True)
    print(f"output_file={OUTPUT_FILE}", flush=True)
    print(f"summary_file={SUMMARY_FILE}", flush=True)


if __name__ == "__main__":
    main()

