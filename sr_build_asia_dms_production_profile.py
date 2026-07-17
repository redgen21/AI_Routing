from __future__ import annotations

import argparse
import json
import re
import time
from datetime import date
from pathlib import Path
from urllib import error, parse, request

import pandas as pd


SHEETS = ["1. Zip Coverage", "2. Slot", "3. Product", "4. Address"]
ADDRESS_COLUMNS = ["Home Street Address", "City ", "State", "Zip"]


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = " ".join(str(value).strip().split())
    text = text.replace("Indonecia", "Indonesia").replace("indonecia", "Indonesia")
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"(?:,\s*){2,}", ", ", text)
    return text.strip(" ,")


def clean_zip(value: object) -> str:
    text = clean_text(value)
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def split_product_rows(product_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, row in product_df.iterrows():
        raw_products = clean_text(row.get("SERVICE_PRODUCT_CODE", ""))
        products = [clean_text(item) for item in raw_products.split(",") if clean_text(item)]
        if not products:
            products = [clean_text(row.get("SERVICE_PRODUCT_GROUP_CODE", ""))]
        for product_group in products:
            out = row.to_dict()
            out["SERVICE_PRODUCT_GROUP_CODE"] = product_group
            out["SERVICE_PRODUCT_CODE"] = ""
            out["REPAIR_FLAG"] = clean_text(out.get("REPAIR_FLAG", "")) or "T"
            out["INSTALL_FLAG"] = clean_text(out.get("INSTALL_FLAG", "")) or "F"
            out["DEMO_FLAG"] = clean_text(out.get("DEMO_FLAG", "")) or "F"
            out["SS_FLAG"] = clean_text(out.get("SS_FLAG", "")) or "N"
            out["DEPT_SS_FLAG"] = clean_text(out.get("DEPT_SS_FLAG", "")) or "F"
            out["SKS_FLAG"] = clean_text(out.get("SKS_FLAG", "")) or "N"
            out["DEPT_SKS_FLAG"] = clean_text(out.get("DEPT_SKS_FLAG", "")) or "F"
            out["AREA_PRODUCT_FLAG"] = clean_text(out.get("AREA_PRODUCT_FLAG", "")) or "Y"
            rows.append(out)
    return pd.DataFrame(rows, columns=product_df.columns)


def address_query(row: pd.Series) -> str:
    return ", ".join(
        part
        for part in [
            clean_text(row.get("Home Street Address", "")),
            clean_text(row.get("City ", "")),
            clean_zip(row.get("Zip", "")),
            "Indonesia",
        ]
        if part
    )


def read_cache(path: Path) -> pd.DataFrame:
    cols = [
        "address_key",
        "geocode_query",
        "matched_address",
        "match_indicator",
        "match_type",
        "latitude",
        "longitude",
        "source",
        "status",
        "geocoded_date",
    ]
    if not path.exists():
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    return df[cols].copy()


def save_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def here_geocode(query: str, api_key: str, timeout: int) -> dict[str, object]:
    params = parse.urlencode({"q": query, "apiKey": api_key, "limit": "1"})
    url = f"https://geocode.search.hereapi.com/v1/geocode?{params}"
    try:
        with request.urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        return {"status": f"HTTP_{exc.code}", "source": "here_geocoding_api"}
    except error.URLError as exc:
        return {"status": "URL_ERROR", "source": "here_geocoding_api", "error_message": str(exc)}

    items = payload.get("items") or []
    if not items:
        return {"status": "NO_RESULTS", "source": "here_geocoding_api"}
    top = items[0]
    position = top.get("position") or {}
    try:
        lat = float(position.get("lat"))
        lng = float(position.get("lng"))
    except (TypeError, ValueError):
        return {"status": "NO_COORDS", "source": "here_geocoding_api"}

    scoring = top.get("scoring") or {}
    query_score = scoring.get("queryScore", "")
    return {
        "status": "OK",
        "source": "here_geocoding_api",
        "matched_address": clean_text(top.get("title", "")),
        "match_indicator": "Match",
        "match_type": f"HERE_{float(query_score):.2f}" if isinstance(query_score, (int, float)) else "HERE",
        "latitude": lat,
        "longitude": lng,
    }


def geocode_addresses(address_df: pd.DataFrame, api_key: str, cache_file: Path, timeout: int, sleep_sec: float) -> pd.DataFrame:
    output = address_df.copy()
    for col in ["SVC_ENGINEER_CODE", "Name", *ADDRESS_COLUMNS]:
        if col in output.columns:
            output[col] = output[col].map(clean_zip if col == "Zip" else clean_text)
    output["State"] = output["State"].map(lambda value: clean_text(value) or "Indonesia")
    output["geocode_query"] = output.apply(address_query, axis=1)
    output["address_key"] = output["geocode_query"].str.upper()

    cache = read_cache(cache_file)
    done = set(cache["address_key"].astype(str))
    pending = output[~output["address_key"].isin(done)][["address_key", "geocode_query"]].drop_duplicates()
    print(f"address_rows={len(output)} unique_queries={output['address_key'].nunique()} here_pending={len(pending)}", flush=True)

    rows: list[dict[str, object]] = []
    for idx, (_, row) in enumerate(pending.iterrows(), start=1):
        result = here_geocode(str(row["geocode_query"]), api_key, timeout)
        rows.append(
            {
                "address_key": row["address_key"],
                "geocode_query": row["geocode_query"],
                "matched_address": result.get("matched_address", ""),
                "match_indicator": result.get("match_indicator", ""),
                "match_type": result.get("match_type", ""),
                "latitude": result.get("latitude", ""),
                "longitude": result.get("longitude", ""),
                "source": result.get("source", "here_geocoding_api"),
                "status": result.get("status", ""),
                "geocoded_date": date.today().isoformat(),
            }
        )
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        if idx % 25 == 0 or idx == len(pending):
            cache = pd.concat([cache, pd.DataFrame(rows)], ignore_index=True)
            cache = cache.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            save_cache(cache_file, cache)
            rows = []
            print(f"here progress={idx}/{len(pending)}", flush=True)

    cache = read_cache(cache_file)
    ok_cache = cache[cache["status"].eq("OK")].copy()
    output = output.merge(
        ok_cache[
            [
                "address_key",
                "matched_address",
                "match_indicator",
                "match_type",
                "latitude",
                "longitude",
                "source",
            ]
        ],
        on="address_key",
        how="left",
    )
    output = output.drop(columns=["address_key", "geocode_query"])
    return output


def build_profile(source_file: Path, output_file: Path, config_file: Path, cache_file: Path, timeout: int, sleep_sec: float) -> None:
    cfg = load_config(config_file)
    here_key = clean_text(cfg.get("geocoding", {}).get("here_api_key", ""))
    if not here_key:
        raise ValueError("Missing geocoding.here_api_key in config.json")

    sheets = pd.read_excel(source_file, sheet_name=SHEETS)
    zip_df = sheets["1. Zip Coverage"].copy()
    slot_df = sheets["2. Slot"].copy()
    product_df = split_product_rows(sheets["3. Product"].copy())
    address_df = geocode_addresses(sheets["4. Address"].copy(), here_key, cache_file, timeout, sleep_sec)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        zip_df.to_excel(writer, sheet_name="1. Zip Coverage", index=False)
        slot_df.to_excel(writer, sheet_name="2. Slot", index=False)
        product_df.to_excel(writer, sheet_name="3. Product", index=False)
        address_df.to_excel(writer, sheet_name="4. Address", index=False)

    geocoded_ok = pd.to_numeric(address_df["latitude"], errors="coerce").notna() & pd.to_numeric(address_df["longitude"], errors="coerce").notna()
    print(f"output_file={output_file}", flush=True)
    print(f"zip_rows={len(zip_df)} slot_rows={len(slot_df)} product_rows={len(product_df)} address_rows={len(address_df)}", flush=True)
    print(f"address_geocoded_ok={int(geocoded_ok.sum())}/{len(address_df)}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Asia DMS production technician profile workbook.")
    parser.add_argument("--source-file", default="260310/Asia_DMS_Profile_20260627.xlsx")
    parser.add_argument(
        "--output-file",
        default="260310/production_input/Asia_DMS_Profile_20260627_production.xlsx",
    )
    parser.add_argument("--config-file", default="config.json")
    parser.add_argument("--cache-file", default="data/asia_dms_profile_here_geocode_cache_20260627.csv")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    args = parser.parse_args()

    build_profile(
        source_file=Path(args.source_file),
        output_file=Path(args.output_file),
        config_file=Path(args.config_file),
        cache_file=Path(args.cache_file),
        timeout=int(args.timeout),
        sleep_sec=max(0.0, float(args.sleep_sec)),
    )


if __name__ == "__main__":
    main()
