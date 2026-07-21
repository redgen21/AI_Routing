from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import date
from pathlib import Path
from urllib import error, parse, request

import pandas as pd


QUERY_COLUMNS = ["geocode_query", "fallback_1", "fallback_2", "fallback_3", "fallback_4"]
CACHE_COLUMNS = [
    "asia_address_key",
    "query_variant",
    "geocode_query",
    "geocode_status",
    "latitude",
    "longitude",
    "matched_address",
    "result_type",
    "query_score",
    "source",
    "error_message",
    "geocoded_date",
]


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = " ".join(str(value).strip().split())
    return text[:-2] if text.endswith(".0") and text[:-2].isdigit() else text


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def read_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=CACHE_COLUMNS)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    for col in CACHE_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[CACHE_COLUMNS].copy()


def save_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL, escapechar="\\")


def query_variants(row: pd.Series) -> list[tuple[str, str]]:
    variants: list[tuple[str, str]] = []
    seen: set[str] = set()
    for col in QUERY_COLUMNS:
        query = clean_text(row.get(col, ""))
        key = query.casefold()
        if query and key not in seen:
            variants.append((col, query))
            seen.add(key)
    return variants


def here_geocode(query: str, api_key: str, timeout: int) -> dict[str, object]:
    params = parse.urlencode({"q": query, "apiKey": api_key, "limit": "1"})
    url = f"https://geocode.search.hereapi.com/v1/geocode?{params}"
    try:
        with request.urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        return {"geocode_status": f"HTTP_{exc.code}", "source": "here_geocoding_api", "error_message": str(exc)}
    except error.URLError as exc:
        return {"geocode_status": "URL_ERROR", "source": "here_geocoding_api", "error_message": str(exc)}
    except json.JSONDecodeError as exc:
        return {"geocode_status": "BAD_JSON", "source": "here_geocoding_api", "error_message": str(exc)}

    items = payload.get("items") or []
    if not items:
        return {"geocode_status": "NO_RESULTS", "source": "here_geocoding_api"}
    top = items[0]
    position = top.get("position") or {}
    try:
        lat = float(position.get("lat"))
        lon = float(position.get("lng"))
    except (TypeError, ValueError):
        return {"geocode_status": "NO_COORDS", "source": "here_geocoding_api"}

    scoring = top.get("scoring") or {}
    return {
        "geocode_status": "OK",
        "latitude": lat,
        "longitude": lon,
        "matched_address": clean_text(top.get("title", "")),
        "result_type": clean_text(top.get("resultType", "")),
        "query_score": scoring.get("queryScore", ""),
        "source": "here_geocoding_api",
        "error_message": "",
    }


def geocode_one(row: pd.Series, api_key: str, timeout: int) -> dict[str, object]:
    variants = query_variants(row)
    last_result = {
        "geocode_status": "NO_QUERY",
        "source": "here_geocoding_api",
        "error_message": "",
    }
    for variant_name, query in variants:
        result = here_geocode(query, api_key, timeout)
        result["query_variant"] = variant_name
        result["geocode_query"] = query
        if clean_text(result.get("geocode_status", "")) == "OK":
            return result
        last_result = result
    return last_result


def build_unique_input(df: pd.DataFrame) -> pd.DataFrame:
    if "asia_address_key" not in df.columns:
        raise ValueError("Missing required column: asia_address_key")
    for col in ["asia_address_key", *QUERY_COLUMNS]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(clean_text)
    unique_df = df[["asia_address_key", *QUERY_COLUMNS]].drop_duplicates("asia_address_key").reset_index(drop=True)
    return unique_df[unique_df["asia_address_key"].ne("")].copy()


def apply_cache(df: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    ok_cache = cache[cache["geocode_status"].eq("OK")].copy()
    if ok_cache.empty:
        output["geocoded_ok"] = False
        return output
    ok_cache = ok_cache.rename(
        columns={
            "geocode_query": "here_geocode_query",
            "query_variant": "here_query_variant",
            "geocode_status": "here_geocode_status",
            "latitude": "here_latitude",
            "longitude": "here_longitude",
            "matched_address": "here_matched_address",
            "result_type": "here_result_type",
            "query_score": "here_query_score",
            "source": "here_source",
            "error_message": "here_error_message",
            "geocoded_date": "here_geocoded_date",
        }
    )
    cols = [
        "asia_address_key",
        "here_query_variant",
        "here_geocode_query",
        "here_geocode_status",
        "here_latitude",
        "here_longitude",
        "here_matched_address",
        "here_result_type",
        "here_query_score",
        "here_source",
        "here_error_message",
        "here_geocoded_date",
    ]
    output = output.merge(ok_cache[cols], on="asia_address_key", how="left")
    output["latitude"] = pd.to_numeric(output.get("here_latitude"), errors="coerce")
    output["longitude"] = pd.to_numeric(output.get("here_longitude"), errors="coerce")
    output["matched_address"] = output.get("here_matched_address", "")
    output["location_type"] = output.get("here_result_type", "")
    output["source"] = "failed"
    ok = output["latitude"].notna() & output["longitude"].notna()
    output.loc[ok, "source"] = "here_geocoding_api"
    output["geocode_status"] = "FAILED"
    output.loc[ok, "geocode_status"] = "OK"
    output["geocoded_ok"] = ok
    return output


def run(
    input_file: Path,
    output_file: Path,
    summary_file: Path,
    config_file: Path,
    cache_file: Path,
    timeout: int,
    sleep_sec: float,
    limit: int | None,
    retry_failed: bool,
) -> None:
    cfg = load_config(config_file)
    api_key = clean_text(cfg.get("geocoding", {}).get("here_api_key", ""))
    if not api_key:
        raise ValueError("Missing geocoding.here_api_key in config/config.json.")

    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    unique_df = build_unique_input(df.copy())
    cache = read_cache(cache_file)
    done_keys = set(cache[cache["geocode_status"].eq("OK")]["asia_address_key"].astype(str))
    if not retry_failed:
        done_keys.update(cache["asia_address_key"].astype(str))
    pending = unique_df[~unique_df["asia_address_key"].astype(str).isin(done_keys)].copy()
    if limit is not None and limit >= 0:
        pending = pending.head(limit).copy()

    print(f"input_rows={len(df)}", flush=True)
    print(f"unique_addresses={len(unique_df)}", flush=True)
    print(f"here cached={len(cache)} pending={len(pending)}", flush=True)

    rows: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(pending.iterrows(), start=1):
        result = geocode_one(row, api_key, timeout)
        rows.append(
            {
                "asia_address_key": row["asia_address_key"],
                "query_variant": result.get("query_variant", ""),
                "geocode_query": result.get("geocode_query", ""),
                "geocode_status": result.get("geocode_status", ""),
                "latitude": result.get("latitude", ""),
                "longitude": result.get("longitude", ""),
                "matched_address": result.get("matched_address", ""),
                "result_type": result.get("result_type", ""),
                "query_score": result.get("query_score", ""),
                "source": result.get("source", "here_geocoding_api"),
                "error_message": result.get("error_message", ""),
                "geocoded_date": date.today().isoformat(),
            }
        )
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        if idx % 200 == 0 or idx == len(pending):
            if rows:
                cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
                cache = cache.drop_duplicates("asia_address_key", keep="last").reset_index(drop=True)
                save_csv(cache_file, cache)
                rows = []
            print(f"here progress={idx}/{len(pending)}", flush=True)

    cache = read_cache(cache_file)
    output = apply_cache(df, cache)
    save_csv(output_file, output)

    summary = (
        output.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME"], dropna=False)
        .agg(total=("GSFS_RECEIPT_NO", "count"), geocoded_ok=("geocoded_ok", "sum"))
        .reset_index()
    )
    summary["geocoded_failed"] = summary["total"] - summary["geocoded_ok"]
    total = pd.DataFrame(
        [
            {
                "COUNTRY_NAME": "TOTAL",
                "STRATEGIC_CITY_NAME": "TOTAL",
                "total": int(len(output)),
                "geocoded_ok": int(output["geocoded_ok"].sum()),
                "geocoded_failed": int((~output["geocoded_ok"]).sum()),
            }
        ]
    )
    summary = pd.concat([summary, total], ignore_index=True)
    save_csv(summary_file, summary)
    print(summary.to_string(index=False), flush=True)
    print(f"output_file={output_file}", flush=True)
    print(f"summary_file={summary_file}", flush=True)
    print(f"cache_file={cache_file}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Geocode translated Asia service addresses with HERE.")
    parser.add_argument("--input-file", default="260310/input/Service_202606271712_asia_translated.csv")
    parser.add_argument("--output-file", default="260310/input/Service_202606271712_asia_here_geocoded.csv")
    parser.add_argument("--summary-file", default="260310/output/asia_here_geocode_summary_202606271712.csv")
    parser.add_argument("--config-file", default="config/config.json")
    parser.add_argument("--cache-file", default="data/asia_here_geocode_cache_202606271712.csv")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args()
    run(
        input_file=Path(args.input_file),
        output_file=Path(args.output_file),
        summary_file=Path(args.summary_file),
        config_file=Path(args.config_file),
        cache_file=Path(args.cache_file),
        timeout=int(args.timeout),
        sleep_sec=max(0.0, float(args.sleep_sec)),
        limit=args.limit,
        retry_failed=bool(args.retry_failed),
    )


if __name__ == "__main__":
    main()

