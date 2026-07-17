from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from urllib import parse, request

import pandas as pd


DEFAULT_PROMPT_FILE = Path("prompts/asia_geocode_query_cleaning.md")
ASIA_COUNTRY_CODES = {"THA", "IDN", "MYS"}
ASIA_COUNTRY_NAMES = {
    "THA": "Thailand",
    "IDN": "Indonesia",
    "MYS": "Malaysia",
}


@dataclass(frozen=True)
class AsiaGeocodeQuery:
    address_key: str
    primary_geocode_query: str
    fallback_geocode_queries: tuple[str, ...]
    place_name: str = ""
    street_address: str = ""
    city: str = ""
    state: str = ""
    postal_code: str = ""
    country: str = ""
    removed_noise: str = ""
    clean_status: str = "OK"

    def variants(self) -> list[tuple[str, str]]:
        rows = [("primary_geocode_query", self.primary_geocode_query)]
        rows.extend((f"fallback_{idx}", query) for idx, query in enumerate(self.fallback_geocode_queries, start=1))
        return [(name, query) for name, query in rows if query]


def clean_text(value: object) -> str:
    if isinstance(value, (list, tuple, set)):
        return " | ".join(clean_text(item) for item in value if clean_text(item))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if pd.isna(value):
        return ""
    text = " ".join(str(value).strip().split())
    return text[:-2] if text.endswith(".0") and text[:-2].isdigit() else text


def load_prompt(prompt_file: Path = DEFAULT_PROMPT_FILE) -> str:
    return prompt_file.read_text(encoding="utf-8").strip()


def is_asia_country(value: object) -> bool:
    text = clean_text(value).upper()
    return text in ASIA_COUNTRY_CODES or text in {name.upper() for name in ASIA_COUNTRY_NAMES.values()}


def default_country_name(value: object) -> str:
    text = clean_text(value)
    return ASIA_COUNTRY_NAMES.get(text.upper(), text)


def build_default_query(row: pd.Series) -> str:
    parts = [
        clean_text(row.get("ADDRESS_LINE1_INFO", "")),
        clean_text(row.get("CITY_NAME", "")),
        clean_text(row.get("STATE_NAME", "")),
        clean_text(row.get("POSTAL_CODE", "")),
        default_country_name(row.get("COUNTRY_NAME", "")),
    ]
    return ", ".join([part for part in parts if part])


def normalize_cleaned_item(address_key: str, item: dict[str, object], fallback_query: str = "") -> AsiaGeocodeQuery:
    fallback_values = item.get("fallback_geocode_queries", [])
    if not isinstance(fallback_values, list):
        fallback_values = []
    fallback_queries = tuple(dict.fromkeys(clean_text(value) for value in fallback_values if clean_text(value)))
    primary = clean_text(item.get("primary_geocode_query", "")) or clean_text(item.get("cleaned_geocode_query", "")) or clean_text(fallback_query)
    return AsiaGeocodeQuery(
        address_key=clean_text(address_key),
        primary_geocode_query=primary,
        fallback_geocode_queries=fallback_queries,
        place_name=clean_text(item.get("place_name", "")),
        street_address=clean_text(item.get("street_address", "")),
        city=clean_text(item.get("city", "")),
        state=clean_text(item.get("state", "")),
        postal_code=clean_text(item.get("postal_code", "")),
        country=clean_text(item.get("country", "")),
        removed_noise=clean_text(item.get("removed_noise", "")),
        clean_status="OK" if primary else "FAILED",
    )


def azure_clean_geocode_queries(
    records: list[dict[str, str]],
    llm_cfg: dict,
    *,
    prompt_file: Path = DEFAULT_PROMPT_FILE,
) -> dict[str, dict[str, object]]:
    prompt = load_prompt(prompt_file)
    base_url = str(llm_cfg["base_url"]).rstrip("/")
    deployment = str(llm_cfg.get("deployment") or llm_cfg.get("model"))
    api_version = str(llm_cfg["api_version"])
    url = f"{base_url}/openai/deployments/{parse.quote(deployment)}/chat/completions?api-version={parse.quote(api_version)}"
    payload = {
        "messages": [
            {"role": "system", "content": prompt},
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
    raise RuntimeError(f"Azure geocode query cleaning failed: {last_error}") from last_error


def cache_columns() -> list[str]:
    return [
        "address_key",
        "primary_geocode_query",
        "fallback_geocode_queries",
        "place_name",
        "street_address",
        "city",
        "state",
        "postal_code",
        "country",
        "removed_noise",
        "clean_status",
    ]


def queries_to_cache_frame(queries: list[AsiaGeocodeQuery]) -> pd.DataFrame:
    rows = []
    for query in queries:
        rows.append(
            {
                "address_key": query.address_key,
                "primary_geocode_query": query.primary_geocode_query,
                "fallback_geocode_queries": json.dumps(list(query.fallback_geocode_queries), ensure_ascii=False),
                "place_name": query.place_name,
                "street_address": query.street_address,
                "city": query.city,
                "state": query.state,
                "postal_code": query.postal_code,
                "country": query.country,
                "removed_noise": query.removed_noise,
                "clean_status": query.clean_status,
            }
        )
    return pd.DataFrame(rows, columns=cache_columns())


def cache_frame_to_queries(df: pd.DataFrame) -> list[AsiaGeocodeQuery]:
    rows: list[AsiaGeocodeQuery] = []
    for _, row in df.iterrows():
        fallback_raw = clean_text(row.get("fallback_geocode_queries", ""))
        fallback_queries: tuple[str, ...] = ()
        if fallback_raw:
            try:
                parsed = json.loads(fallback_raw)
                if isinstance(parsed, list):
                    fallback_queries = tuple(clean_text(value) for value in parsed if clean_text(value))
            except json.JSONDecodeError:
                fallback_queries = tuple(clean_text(value) for value in fallback_raw.split("|") if clean_text(value))
        rows.append(
            AsiaGeocodeQuery(
                address_key=clean_text(row.get("address_key", "")),
                primary_geocode_query=clean_text(row.get("primary_geocode_query", "")),
                fallback_geocode_queries=fallback_queries,
                place_name=clean_text(row.get("place_name", "")),
                street_address=clean_text(row.get("street_address", "")),
                city=clean_text(row.get("city", "")),
                state=clean_text(row.get("state", "")),
                postal_code=clean_text(row.get("postal_code", "")),
                country=clean_text(row.get("country", "")),
                removed_noise=clean_text(row.get("removed_noise", "")),
                clean_status=clean_text(row.get("clean_status", "OK")) or "OK",
            )
        )
    return rows
