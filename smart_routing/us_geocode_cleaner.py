from __future__ import annotations

import re

import pandas as pd


EMBEDDED_CITY_STATE_ZIP = re.compile(
    r"^(.+?)\s{2,}([A-Za-z][A-Za-z .'-]*?)\s*,?\s*([A-Z]{2})\s*,?\s*(\d{5})(?:-\d{4})?",
    re.I,
)
USA_THEN_CITY_ZIP = re.compile(r"^(.*?),?\s+USA\s+([A-Za-z][A-Za-z .'-]*?)\s+(\d{5})(?:-\d{4})?\s*$", re.I)
TRAILING_CITY_STATE_ZIP = re.compile(r"^(.+?)\s{2,}([A-Za-z][A-Za-z .'-]*?)\s*,\s*([A-Z]{2})\s*,\s*(\d{5})(?:-\d{4})?.*$", re.I)
CITY_STATE_USA = re.compile(r"^(.+?)\s+([A-Za-z][A-Za-z .'-]*?)\s+([A-Z]{2})\s+USA\s*$", re.I)


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).replace("\u00a0", " ").replace("\r", " ").replace("\n", " ").strip().split())


def raw_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\u00a0", " ").replace("\r", " ").replace("\n", " ").strip()


def _row_city(row: pd.Series) -> str:
    return clean_text(row.get("CITY_NAME", "")).title()


def _row_state(row: pd.Series) -> str:
    return clean_text(row.get("STATE_NAME", "")).upper()


def _row_zip(row: pd.Series) -> str:
    return clean_text(row.get("POSTAL_CODE", "")).split(".")[0]


def _query_from_parts(street: str, city: str, state: str, postal_code: str) -> str:
    street = clean_text(street)
    city = clean_text(city).title()
    state = clean_text(state).upper()
    postal_code = clean_text(postal_code)
    if not street:
        return ""
    if city and state and postal_code:
        return f"{street}, {city}, {state} {postal_code}, USA"
    if city and state:
        return f"{street}, {city}, {state}, USA"
    if state and postal_code:
        return f"{street}, {state} {postal_code}, USA"
    return f"{street}, USA"


def _dedupe_query_pairs(queries: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, query in queries:
        cleaned = clean_text(query)
        key = cleaned.upper()
        if cleaned and key not in seen:
            out.append((name, cleaned))
            seen.add(key)
    return out


def build_us_geocode_query_variants(row: pd.Series) -> list[tuple[str, str]]:
    raw = raw_text(row.get("ADDRESS_LINE1_INFO", ""))
    address = clean_text(raw)
    city = _row_city(row)
    state = _row_state(row)
    postal_code = _row_zip(row)
    queries: list[tuple[str, str]] = []

    embedded_match = EMBEDDED_CITY_STATE_ZIP.search(raw)
    if embedded_match:
        queries.append(
            (
                "embedded_city_state_zip",
                _query_from_parts(
                    embedded_match.group(1),
                    embedded_match.group(2),
                    embedded_match.group(3),
                    embedded_match.group(4),
                ),
            )
        )

    usa_tail_match = USA_THEN_CITY_ZIP.search(address)
    if usa_tail_match:
        before_usa = clean_text(usa_tail_match.group(1)).rstrip(",")
        queries.append(("usa_tail_zip_rebuilt", f"{before_usa} {usa_tail_match.group(3)}, USA"))
        if state and postal_code:
            if before_usa.upper().endswith(f", {state}") or before_usa.upper().endswith(f" {state}"):
                queries.append(("usa_tail_removed", f"{before_usa} {postal_code}, USA"))
            else:
                queries.append(("usa_tail_removed", f"{before_usa}, {state} {postal_code}, USA"))
        else:
            queries.append(("usa_tail_removed", f"{before_usa}, USA"))

    trailing_match = TRAILING_CITY_STATE_ZIP.search(raw)
    if trailing_match:
        queries.append(
            (
                "trailing_city_state_zip",
                _query_from_parts(
                    trailing_match.group(1),
                    trailing_match.group(2),
                    trailing_match.group(3),
                    trailing_match.group(4),
                ),
            )
        )

    city_state_usa_match = CITY_STATE_USA.search(address)
    if city_state_usa_match:
        queries.append(
            (
                "city_state_usa_with_row_zip",
                _query_from_parts(
                    city_state_usa_match.group(1),
                    city_state_usa_match.group(2),
                    city_state_usa_match.group(3),
                    postal_code,
                ),
            )
        )

    if " USA " in f" {address.upper()} ":
        before_usa = re.split(r"\bUSA\b", address, flags=re.I)[0].strip(" ,")
        if postal_code:
            queries.append(("trim_after_usa_with_row_zip", f"{before_usa} {postal_code}, USA"))
        else:
            queries.append(("trim_after_usa", f"{before_usa}, USA"))

    queries.append(("row_city_state_zip", _query_from_parts(address, city, state, postal_code)))
    queries.append(("row_city_state", _query_from_parts(address, city, state, "")))
    queries.append(("raw_address", address))
    return _dedupe_query_pairs(queries)
