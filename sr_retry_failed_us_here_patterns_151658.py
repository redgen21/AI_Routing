from __future__ import annotations

import json
import re
import time
from pathlib import Path
from urllib import parse, request

import pandas as pd


FINAL_FILE = Path("260310/input/Service_202606151658_final_geocoded.csv")
CACHE_FILE = Path("data/us_failed_here_pattern_retry_cache_202606151658.csv")
REPORT_FILE = Path("260310/output/us_failed_here_pattern_retry_151658_report.csv")


EMBEDDED_CITY_STATE_ZIP = re.compile(
    r"^(.+?)\s{2,}([A-Za-z][A-Za-z .'-]*?)\s*,?\s*([A-Z]{2})\s*,?\s*(\d{5})(?:-\d{4})?",
    re.I,
)
USA_THEN_CITY_ZIP = re.compile(r"^(.*?),?\s+USA\s+([A-Za-z][A-Za-z .'-]*?)\s+(\d{5})(?:-\d{4})?\s*$", re.I)
TRAILING_CITY_STATE_ZIP = re.compile(r"^(.+?)\s{2,}([A-Za-z][A-Za-z .'-]*?)\s*,\s*([A-Z]{2})\s*,\s*(\d{5})(?:-\d{4})?.*$", re.I)
CITY_STATE_USA = re.compile(r"^(.+?)\s+([A-Za-z][A-Za-z .'-]*?)\s+([A-Z]{2})\s+USA\s*$", re.I)


def clean(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).replace("\u00a0", " ").replace("\r", " ").replace("\n", " ").strip().split())


def raw_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\u00a0", " ").replace("\r", " ").replace("\n", " ").strip()


def dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean(value)
        key = text.upper()
        if text and key not in seen:
            out.append(text)
            seen.add(key)
    return out


def row_city(row: pd.Series) -> str:
    return clean(row.get("CITY_NAME", "")).title()


def row_state(row: pd.Series) -> str:
    return clean(row.get("STATE_NAME", "")).upper()


def row_zip(row: pd.Series) -> str:
    return clean(row.get("POSTAL_CODE", "")).split(".")[0]


def query_from_parts(street: str, city: str, state: str, postal: str) -> str:
    street = clean(street)
    city = clean(city).title()
    state = clean(state).upper()
    postal = clean(postal)
    if not street:
        return ""
    if city and state and postal:
        return f"{street}, {city}, {state} {postal}, USA"
    if city and state:
        return f"{street}, {city}, {state}, USA"
    if state and postal:
        return f"{street}, {state} {postal}, USA"
    return f"{street}, USA"


def build_queries(row: pd.Series) -> list[tuple[str, str]]:
    raw = raw_text(row.get("ADDRESS_LINE1_INFO", ""))
    addr = clean(raw)
    city = row_city(row)
    state = row_state(row)
    postal = row_zip(row)
    queries: list[tuple[str, str]] = []

    m = EMBEDDED_CITY_STATE_ZIP.search(raw)
    if m:
        queries.append(("embedded_city_state_zip", query_from_parts(m.group(1), m.group(2), m.group(3), m.group(4))))

    m = USA_THEN_CITY_ZIP.search(addr)
    if m:
        before_usa = clean(m.group(1)).rstrip(",")
        # The address before USA usually already contains street, city, and state.
        queries.append(("usa_tail_zip_rebuilt", f"{before_usa} {m.group(3)}, USA"))
        queries.append(("usa_tail_removed", f"{before_usa}, {state} {postal}, USA" if state and postal else f"{before_usa}, USA"))

    m = TRAILING_CITY_STATE_ZIP.search(raw)
    if m:
        queries.append(("trailing_city_state_zip", query_from_parts(m.group(1), m.group(2), m.group(3), m.group(4))))

    m = CITY_STATE_USA.search(addr)
    if m:
        queries.append(("city_state_usa_with_row_zip", query_from_parts(m.group(1), m.group(2), m.group(3), postal)))

    if " USA " in f" {addr.upper()} ":
        before_usa = re.split(r"\bUSA\b", addr, flags=re.I)[0].strip(" ,")
        queries.append(("trim_after_usa_with_row_zip", f"{before_usa} {postal}, USA" if postal else f"{before_usa}, USA"))

    # General fallbacks. These rescue simple rows where address_line1 lacks city/ZIP.
    queries.append(("row_city_state_zip", query_from_parts(addr, city, state, postal)))
    queries.append(("row_city_state", query_from_parts(addr, city, state, "")))
    queries.append(("raw_address", addr))
    return dedupe_query_pairs(queries)


def dedupe_query_pairs(queries: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, query in queries:
        cleaned = clean(query)
        key = cleaned.upper()
        if cleaned and key not in seen:
            out.append((name, cleaned))
            seen.add(key)
    return out


def to_float(value: object) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def passes_quality(item: dict[str, object], min_query_score: float, min_field_score: float) -> bool:
    scoring = item.get("scoring") or {}
    if not isinstance(scoring, dict):
        return False
    query_score = to_float(scoring.get("queryScore"))
    if query_score is None or query_score < min_query_score:
        return False
    field_score = scoring.get("fieldScore") or {}
    if isinstance(field_score, dict):
        for key in ["state", "city", "postalCode", "houseNumber"]:
            value = to_float(field_score.get(key))
            if value is not None and value < min_field_score:
                return False
        streets = field_score.get("streets")
        if isinstance(streets, list) and streets:
            value = to_float(streets[0])
            if value is not None and value < min_field_score:
                return False
    return True


def here_geocode(query: str, api_key: str, min_query_score: float, min_field_score: float) -> dict[str, object]:
    url = "https://geocode.search.hereapi.com/v1/geocode?" + parse.urlencode({"q": query, "apiKey": api_key})
    try:
        with request.urlopen(url, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return {"status": type(exc).__name__, "query": query}
    items = payload.get("items") or []
    if not items:
        return {"status": "NO_RESULTS", "query": query}
    top = items[0]
    position = top.get("position") or {}
    lat = to_float(position.get("lat"))
    lng = to_float(position.get("lng"))
    scoring = top.get("scoring") or {}
    query_score = to_float(scoring.get("queryScore")) if isinstance(scoring, dict) else None
    if lat is None or lng is None:
        return {"status": "NO_COORDS", "query": query}
    ok = passes_quality(top, min_query_score, min_field_score)
    return {
        "status": "OK" if ok else "LOW_QUALITY",
        "query": query,
        "latitude": lat,
        "longitude": lng,
        "matched_address": clean(top.get("title", "")),
        "match_type": f"HERE_{query_score:.2f}" if query_score is not None else "HERE",
        "query_score": "" if query_score is None else query_score,
        "field_score": json.dumps(scoring.get("fieldScore", {}) if isinstance(scoring, dict) else {}, ensure_ascii=False),
        "result_type": clean(top.get("resultType", "")),
    }


def read_cache() -> pd.DataFrame:
    cols = [
        "address_key",
        "variant",
        "query",
        "status",
        "latitude",
        "longitude",
        "matched_address",
        "match_type",
        "query_score",
        "field_score",
        "result_type",
    ]
    if not CACHE_FILE.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(CACHE_FILE, encoding="utf-8-sig", low_memory=False)
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    return df[cols].drop_duplicates(subset=["address_key", "variant"], keep="last").reset_index(drop=True)


def save_cache(df: pd.DataFrame) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")


def main() -> None:
    cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
    geocoding_cfg = cfg.get("geocoding", {})
    api_key = str(geocoding_cfg.get("here_api_key", "")).strip()
    if not api_key:
        raise ValueError("Missing geocoding.here_api_key")
    min_query_score = float(geocoding_cfg.get("here_min_query_score", 0.7))
    min_field_score = float(geocoding_cfg.get("here_min_field_score", 0.7))
    sleep_sec = float(geocoding_cfg.get("here_sleep_sec", 0.05))

    df = pd.read_csv(FINAL_FILE, encoding="utf-8-sig", low_memory=False)
    failed_mask = df["source"].astype(str).eq("failed") | df["latitude"].isna() | df["longitude"].isna()
    before_failed = int(failed_mask.sum())
    failed = df.loc[failed_mask].copy()

    candidates: list[dict[str, object]] = []
    for idx, row in failed.iterrows():
        address_key = clean(row.get("address_key", ""))
        for variant, query in build_queries(row):
            candidates.append({"row_index": idx, "address_key": address_key, "variant": variant, "query": query})
    cand_df = pd.DataFrame(candidates)
    cand_df = cand_df.drop_duplicates(subset=["address_key", "variant"]).reset_index(drop=True)

    cache = read_cache()
    done = set(zip(cache["address_key"].astype(str), cache["variant"].astype(str)))
    pending = cand_df[~cand_df.apply(lambda r: (str(r["address_key"]), str(r["variant"])) in done, axis=1)].copy()
    print(f"failed_before={before_failed}", flush=True)
    print(f"candidate_variants={len(cand_df)} pending_here={len(pending)}", flush=True)

    new_rows: list[dict[str, object]] = []
    for i, (_, row) in enumerate(pending.iterrows(), start=1):
        result = here_geocode(str(row["query"]), api_key, min_query_score, min_field_score)
        result["address_key"] = row["address_key"]
        result["variant"] = row["variant"]
        new_rows.append(result)
        if i % 100 == 0 or i == len(pending):
            cache = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
            cache = cache.drop_duplicates(subset=["address_key", "variant"], keep="last").reset_index(drop=True)
            save_cache(cache)
            new_rows = []
            print(f"here progress={i}/{len(pending)} ok={int((cache['status'].astype(str) == 'OK').sum())}", flush=True)
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    cache = read_cache()
    merged = cand_df.merge(cache, on=["address_key", "variant", "query"], how="left")
    merged["_ok"] = merged["status"].astype(str).eq("OK")
    merged["_score"] = pd.to_numeric(merged["query_score"], errors="coerce").fillna(-1.0)
    best = (
        merged[merged["_ok"]]
        .sort_values(["address_key", "_score"], ascending=[True, False])
        .drop_duplicates(subset=["address_key"], keep="first")
    )

    updated = 0
    if not best.empty:
        # Apply one geocode per address_key to every remaining failed row with that key.
        lookup = best.set_index("address_key")
        failed_mask = df["source"].astype(str).eq("failed") | df["latitude"].isna() | df["longitude"].isna()
        for idx, row in df.loc[failed_mask].iterrows():
            address_key = clean(row.get("address_key", ""))
            if address_key not in lookup.index:
                continue
            item = lookup.loc[address_key]
            df.at[idx, "matched_address"] = item.get("matched_address", "")
            df.at[idx, "match_indicator"] = "Match"
            df.at[idx, "match_type"] = item.get("match_type", "HERE")
            df.at[idx, "latitude"] = item.get("latitude", "")
            df.at[idx, "longitude"] = item.get("longitude", "")
            for col in ["census_state_fips", "census_county_fips", "census_tract", "census_block"]:
                if col in df.columns:
                    df.at[idx, col] = ""
            df.at[idx, "geocoded_date"] = pd.Timestamp.today().date().isoformat()
            df.at[idx, "source"] = "here_geocoding_api"
            updated += 1

    final_failed = int((df["source"].astype(str).eq("failed") | df["latitude"].isna() | df["longitude"].isna()).sum())
    df.to_csv(FINAL_FILE, index=False, encoding="utf-8-sig")

    report = pd.DataFrame(
        [
            {"metric": "failed_before", "value": before_failed},
            {"metric": "candidate_variants", "value": int(len(cand_df))},
            {"metric": "pending_here_this_run", "value": int(len(pending))},
            {"metric": "successful_unique_address_keys", "value": int(len(best))},
            {"metric": "updated_rows", "value": updated},
            {"metric": "failed_after", "value": final_failed},
        ]
    )
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(REPORT_FILE, index=False, encoding="utf-8-sig")
    print(f"updated_rows={updated}", flush=True)
    print(f"failed_after={final_failed}", flush=True)
    print(df["source"].fillna("").astype(str).value_counts().to_string(), flush=True)
    print(f"updated_file={FINAL_FILE}", flush=True)
    print(f"report_file={REPORT_FILE}", flush=True)


if __name__ == "__main__":
    main()
