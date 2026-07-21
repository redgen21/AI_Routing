from __future__ import annotations

import json
import time
from pathlib import Path
from urllib import error, parse, request

import pandas as pd


SAMPLE_FILE = Path("260310/output/asia_here_sample_100_by_city_202606161433.csv")
CLEAN_CACHE_FILE = Path("data/asia_here_sample_cleaned_query_cache_202606161433.csv")
RESULT_CACHE_FILE = Path("data/asia_here_query_variant_cache_202606161433.csv")
OUTPUT_FILE = Path("260310/output/asia_here_query_variant_results_202606161433.csv")
SUMMARY_FILE = Path("260310/output/asia_here_query_variant_summary_202606161433.csv")
BEST_FILE = Path("260310/output/asia_here_query_variant_best_202606161433.csv")
BEST_SUMMARY_FILE = Path("260310/output/asia_here_query_variant_best_summary_202606161433.csv")


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = " ".join(str(value).strip().split())
    return text[:-2] if text.endswith(".0") and text[:-2].isdigit() else text


def join_parts(parts: list[object]) -> str:
    return ", ".join([clean_text(part) for part in parts if clean_text(part)])


def load_config() -> dict:
    return json.loads(Path("config/config.json").read_text(encoding="utf-8"))


def build_variants(row: pd.Series) -> dict[str, str]:
    place = clean_text(row.get("place_name", ""))
    street = clean_text(row.get("street_address", ""))
    city = clean_text(row.get("city", ""))
    state = clean_text(row.get("state", ""))
    postal = clean_text(row.get("postal_code", ""))
    country = clean_text(row.get("country", ""))
    full = clean_text(row.get("cleaned_geocode_query", ""))
    original = clean_text(row.get("query", ""))

    variants = {
        "cleaned_full": full,
        "place_street_postal_country": join_parts([place, street, postal, country]),
        "street_postal_country": join_parts([street, postal, country]),
        "place_street_country": join_parts([place, street, country]),
        "street_country": join_parts([street, country]),
        "street_city_postal_country": join_parts([street, city, postal, country]),
        "original_translated": original,
    }
    return {name: query for name, query in variants.items() if query}


def empty_cache() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "asia_address_key",
            "variant",
            "query",
            "status",
            "latitude",
            "longitude",
            "matched_address",
            "result_type",
            "query_score",
            "field_score_json",
            "quality_ok",
            "score_rank",
            "source",
        ]
    )


def read_cache() -> pd.DataFrame:
    if not RESULT_CACHE_FILE.exists():
        return empty_cache()
    df = pd.read_csv(RESULT_CACHE_FILE, encoding="utf-8-sig", low_memory=False)
    base = empty_cache()
    for col in base.columns:
        if col not in df.columns:
            df[col] = ""
    return df[base.columns].drop_duplicates(subset=["asia_address_key", "variant"], keep="last").reset_index(drop=True)


def save_cache(df: pd.DataFrame) -> None:
    RESULT_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RESULT_CACHE_FILE, index=False, encoding="utf-8-sig")


def to_float(value: object) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def field_min(field_score: dict[str, object]) -> float | None:
    values: list[float] = []
    for key in ["state", "city", "postalCode", "houseNumber"]:
        value = to_float(field_score.get(key))
        if value is not None:
            values.append(value)
    streets = field_score.get("streets")
    if isinstance(streets, list):
        values.extend([value for value in (to_float(item) for item in streets) if value is not None])
    return min(values) if values else None


def quality_ok(scoring: dict[str, object], min_query_score: float, min_field_score: float) -> bool:
    query_score = to_float(scoring.get("queryScore"))
    if query_score is None or query_score < min_query_score:
        return False
    field_score = scoring.get("fieldScore") or {}
    if not isinstance(field_score, dict):
        return True
    min_score = field_min(field_score)
    return min_score is None or min_score >= min_field_score


def score_rank(row: dict[str, object]) -> float:
    if row["status"] != "OK":
        return -1.0
    query_score = to_float(row.get("query_score")) or 0.0
    field_score = {}
    try:
        field_score = json.loads(str(row.get("field_score_json") or "{}"))
    except json.JSONDecodeError:
        field_score = {}
    min_field = field_min(field_score) or 0.0
    result_type = clean_text(row.get("result_type", ""))
    type_bonus = {
        "houseNumber": 0.30,
        "place": 0.24,
        "street": 0.18,
        "locality": 0.06,
    }.get(result_type, 0.0)
    return round(query_score + min_field + type_bonus, 4)


def here_geocode(query: str, api_key: str, min_query_score: float, min_field_score: float) -> dict[str, object]:
    params = parse.urlencode({"q": query, "apiKey": api_key})
    url = f"https://geocode.search.hereapi.com/v1/geocode?{params}"
    try:
        with request.urlopen(url, timeout=30) as resp:
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
    lat = to_float(position.get("lat"))
    lng = to_float(position.get("lng"))
    scoring = top.get("scoring") or {}
    if lat is None or lng is None:
        return {"status": "NO_COORDS", "source": "here_geocoding_api"}
    ok = quality_ok(scoring if isinstance(scoring, dict) else {}, min_query_score, min_field_score)
    row = {
        "status": "OK" if ok else "LOW_QUALITY",
        "latitude": lat,
        "longitude": lng,
        "matched_address": clean_text(top.get("title", "")),
        "result_type": clean_text(top.get("resultType", "")),
        "query_score": scoring.get("queryScore", "") if isinstance(scoring, dict) else "",
        "field_score_json": json.dumps(scoring.get("fieldScore", {}) if isinstance(scoring, dict) else {}, ensure_ascii=False),
        "quality_ok": ok,
        "source": "here_geocoding_api",
    }
    row["score_rank"] = score_rank(row)
    return row


def main() -> None:
    cfg = load_config()
    here_key = str(cfg.get("geocoding", {}).get("here_api_key", "")).strip()
    if not here_key:
        raise ValueError("Missing geocoding.here_api_key")
    min_query_score = float(cfg.get("geocoding", {}).get("here_min_query_score", 0.7))
    min_field_score = float(cfg.get("geocoding", {}).get("here_min_field_score", 0.7))

    sample = pd.read_csv(SAMPLE_FILE, encoding="utf-8-sig", low_memory=False)
    clean_cache = pd.read_csv(CLEAN_CACHE_FILE, encoding="utf-8-sig", low_memory=False)
    sample = sample.merge(clean_cache, on="asia_address_key", how="left")
    sample = sample.drop_duplicates(subset=["asia_address_key"]).reset_index(drop=True)

    desired_rows = []
    for _, row in sample.iterrows():
        for variant, query in build_variants(row).items():
            desired_rows.append(
                {
                    "asia_address_key": row["asia_address_key"],
                    "COUNTRY_NAME": row["COUNTRY_NAME"],
                    "STRATEGIC_CITY_NAME": row["STRATEGIC_CITY_NAME"],
                    "variant": variant,
                    "query": query,
                }
            )
    desired = pd.DataFrame(desired_rows)
    cache = read_cache()
    done = set(zip(cache["asia_address_key"].astype(str), cache["variant"].astype(str)))
    pending = desired[~desired.apply(lambda r: (str(r["asia_address_key"]), str(r["variant"])) in done, axis=1)].copy()
    print(f"variant_queries total={len(desired)} cached={len(cache)} pending={len(pending)}", flush=True)

    new_rows: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(pending.iterrows(), start=1):
        result = here_geocode(str(row["query"]), here_key, min_query_score, min_field_score)
        result["asia_address_key"] = row["asia_address_key"]
        result["variant"] = row["variant"]
        result["query"] = row["query"]
        if "quality_ok" not in result:
            result["quality_ok"] = False
        if "score_rank" not in result:
            result["score_rank"] = -1.0
        new_rows.append(result)
        if idx % 100 == 0 or idx == len(pending):
            cache = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
            cache = cache.drop_duplicates(subset=["asia_address_key", "variant"], keep="last").reset_index(drop=True)
            save_cache(cache)
            new_rows = []
            print(f"here variant progress={idx}/{len(pending)}", flush=True)
        time.sleep(0.05)

    cache = read_cache()
    result = desired.merge(cache, on=["asia_address_key", "variant", "query"], how="left")
    result["quality_ok"] = result["quality_ok"].fillna(False).astype(bool)
    result["score_rank"] = pd.to_numeric(result["score_rank"], errors="coerce").fillna(-1.0)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    summary = (
        result.groupby(["variant"], dropna=False)
        .agg(
            attempts=("asia_address_key", "count"),
            ok=("quality_ok", "sum"),
            low_quality=("status", lambda s: int((s == "LOW_QUALITY").sum())),
            no_results=("status", lambda s: int((s == "NO_RESULTS").sum())),
            avg_score=("score_rank", lambda s: round(float(pd.to_numeric(s, errors="coerce").clip(lower=0).mean()), 4)),
        )
        .reset_index()
    )
    summary["failed"] = summary["attempts"] - summary["ok"]
    summary["success_rate_pct"] = (summary["ok"] / summary["attempts"] * 100).round(1)
    summary = summary.sort_values(["ok", "avg_score"], ascending=[False, False]).reset_index(drop=True)
    summary.to_csv(SUMMARY_FILE, index=False, encoding="utf-8-sig")

    best = result.sort_values(["asia_address_key", "quality_ok", "score_rank"], ascending=[True, False, False])
    best = best.drop_duplicates(subset=["asia_address_key"], keep="first").reset_index(drop=True)
    best.to_csv(BEST_FILE, index=False, encoding="utf-8-sig")
    best_summary = (
        best.groupby(["COUNTRY_NAME", "STRATEGIC_CITY_NAME"], dropna=False)
        .agg(sample_count=("asia_address_key", "count"), here_ok=("quality_ok", "sum"))
        .reset_index()
    )
    best_summary["here_failed"] = best_summary["sample_count"] - best_summary["here_ok"]
    best_summary["success_rate_pct"] = (best_summary["here_ok"] / best_summary["sample_count"] * 100).round(1)
    best_summary.to_csv(BEST_SUMMARY_FILE, index=False, encoding="utf-8-sig")

    print(summary.to_string(index=False), flush=True)
    print("best by address", flush=True)
    print(best_summary.to_string(index=False), flush=True)
    print(f"output_file={OUTPUT_FILE}", flush=True)
    print(f"summary_file={SUMMARY_FILE}", flush=True)
    print(f"best_file={BEST_FILE}", flush=True)
    print(f"best_summary_file={BEST_SUMMARY_FILE}", flush=True)


if __name__ == "__main__":
    main()

