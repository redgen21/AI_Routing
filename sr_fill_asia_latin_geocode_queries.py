from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


COUNTRY_NAMES = {
    "THA": "Thailand",
    "IDN": "Indonesia",
    "MYS": "Malaysia",
}


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = " ".join(str(value).strip().split())
    text = re.sub(r"\s*[,;|]+\s*", ", ", text)
    text = re.sub(r"(?:,\s*){2,}", ", ", text)
    return text.strip(" ,")


def _clean_postal_code(value: object) -> str:
    text = _clean_text(value)
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _country_name(value: object) -> str:
    text = _clean_text(value)
    return COUNTRY_NAMES.get(text.upper(), text)


def _deduplicated_query(parts: list[str]) -> str:
    output: list[str] = []
    seen: set[str] = set()
    for part in parts:
        cleaned = _clean_text(part)
        key = re.sub(r"[^a-z0-9]+", "", cleaned.lower())
        combined_key = re.sub(r"[^a-z0-9]+", "", " ".join(output).lower())
        if not cleaned or not key or key in seen or key in combined_key:
            continue
        seen.add(key)
        output.append(cleaned)
    return ", ".join(output)


def fill_latin_geocode_queries(source_file: Path, output_file: Path) -> pd.DataFrame:
    df = pd.read_csv(source_file, encoding="utf-8-sig", low_memory=False)
    required = {
        "ADDRESS_LINE1_INFO",
        "CITY_NAME",
        "STATE_NAME",
        "COUNTRY_NAME",
        "POSTAL_CODE",
        "needs_translation",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    output = df.copy()
    for col in [
        "translated_address",
        "translated_city",
        "translated_state",
        "translated_country",
        "translated_postal_code",
        "translated_query",
        "translation_status",
    ]:
        if col not in output.columns:
            output[col] = ""
        output[col] = output[col].astype("object")

    needs_translation = (
        output["needs_translation"]
        .fillna(False)
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "1", "y", "yes"})
    )
    has_translated_query = output["translated_query"].fillna("").astype(str).str.strip().ne("")
    fill_mask = ~needs_translation & ~has_translated_query

    output.loc[fill_mask, "translated_address"] = output.loc[fill_mask, "ADDRESS_LINE1_INFO"].map(_clean_text)
    output.loc[fill_mask, "translated_city"] = output.loc[fill_mask, "CITY_NAME"].map(_clean_text)
    output.loc[fill_mask, "translated_state"] = output.loc[fill_mask, "STATE_NAME"].map(_clean_text)
    output.loc[fill_mask, "translated_country"] = output.loc[fill_mask, "COUNTRY_NAME"].map(_country_name)
    output.loc[fill_mask, "translated_postal_code"] = output.loc[fill_mask, "POSTAL_CODE"].map(_clean_postal_code)
    output.loc[fill_mask, "translated_query"] = output.loc[fill_mask].apply(
        lambda row: _deduplicated_query(
            [
                row["translated_address"],
                row["translated_city"],
                row["translated_state"],
                row["translated_postal_code"],
                row["translated_country"],
            ]
        ),
        axis=1,
    )
    output.loc[fill_mask, "translation_status"] = "LATIN_CLEANED"

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_file, index=False, encoding="utf-8-sig")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill Latin-script Asia addresses with cleaned geocoding queries.")
    parser.add_argument(
        "--source-file",
        default="260310/input/Service_202606161433_asia_census_only_geocoded.csv",
    )
    parser.add_argument(
        "--output-file",
        default="260310/input/Service_202606161433_asia_latin_cleaned.csv",
    )
    args = parser.parse_args()

    result = fill_latin_geocode_queries(Path(args.source_file), Path(args.output_file))
    translated_query = result["translated_query"].fillna("").astype(str).str.strip()
    print(f"output_file={args.output_file}")
    print(f"rows={len(result)}")
    print(f"translated_query_filled={int(translated_query.ne('').sum())}")
    print(result["translation_status"].fillna("UNSET").astype(str).value_counts().to_string())


if __name__ == "__main__":
    main()
