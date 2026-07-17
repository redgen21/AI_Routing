from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date
from http.client import RemoteDisconnected
from pathlib import Path
from socket import timeout as SocketTimeout
from urllib import error, parse, request

import pandas as pd


DEFAULT_TIMEOUT = 30
DEFAULT_SLEEP_SEC = 0.1


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def to_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


@dataclass
class NominatimResult:
    attempted: int
    geocoded: int
    failed: int
    cache_path: Path


class NominatimGeocoder:
    def __init__(
        self,
        base_url: str,
        cache_path: Path,
        timeout: int = DEFAULT_TIMEOUT,
        sleep_sec: float = DEFAULT_SLEEP_SEC,
        user_agent: str = "ai-routing-asia-geocoder",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cache_path = cache_path
        self.timeout = int(timeout)
        self.sleep_sec = float(sleep_sec)
        self.user_agent = clean_text(user_agent) or "ai-routing-asia-geocoder"

    def geocode_missing(
        self,
        unique_df: pd.DataFrame,
        *,
        retry_failed: bool = False,
        limit: int | None = None,
    ) -> NominatimResult:
        cache = self.load_cache()
        cached_ok = cache[cache["geocode_status"].eq("OK")]
        done_keys = set(cached_ok["asia_address_key"].astype(str))
        if not retry_failed:
            done_keys.update(cache["asia_address_key"].astype(str))

        pending = unique_df[~unique_df["asia_address_key"].astype(str).isin(done_keys)].copy()
        if limit is not None and limit >= 0:
            pending = pending.head(limit).copy()

        rows: list[dict[str, object]] = []
        for idx, (_, row) in enumerate(pending.iterrows(), start=1):
            result = self.geocode_variants(
                asia_address_key=clean_text(row.get("asia_address_key", "")),
                variants=self._query_variants(row),
                country_code=clean_text(row.get("nominatim_country_code", "")),
            )
            rows.append(result)
            if self.sleep_sec > 0:
                time.sleep(self.sleep_sec)
            if idx % 200 == 0 or idx == len(pending):
                cache = self._merge_cache(cache, pd.DataFrame(rows))
                self.save_cache(cache)
                rows = []
                print(f"nominatim progress={idx}/{len(pending)}", flush=True)

        cache = self.load_cache()
        attempted_keys = set(pending["asia_address_key"].astype(str))
        attempted_cache = cache[cache["asia_address_key"].astype(str).isin(attempted_keys)]
        geocoded = int(attempted_cache["geocode_status"].eq("OK").sum())
        return NominatimResult(
            attempted=int(len(pending)),
            geocoded=geocoded,
            failed=int(len(pending) - geocoded),
            cache_path=self.cache_path,
        )

    def geocode_variants(
        self,
        *,
        asia_address_key: str,
        variants: list[tuple[str, str]],
        country_code: str = "",
    ) -> dict[str, object]:
        last_status = "NO_QUERY"
        last_error = ""
        for variant_name, query in variants:
            if not query:
                continue
            result = self.geocode_one(query, country_code=country_code)
            status = clean_text(result.get("geocode_status", ""))
            last_status = status or "UNKNOWN"
            last_error = clean_text(result.get("error_message", ""))
            if status == "OK":
                result["asia_address_key"] = asia_address_key
                result["query_variant"] = variant_name
                return result
        return {
            "asia_address_key": asia_address_key,
            "geocode_query": variants[0][1] if variants else "",
            "query_variant": variants[0][0] if variants else "",
            "geocode_status": last_status,
            "latitude": "",
            "longitude": "",
            "matched_address": "",
            "location_type": "",
            "place_id": "",
            "importance": "",
            "source": "nominatim",
            "error_message": last_error,
            "geocoded_date": date.today().isoformat(),
        }

    def geocode_one(self, query: str, *, country_code: str = "") -> dict[str, object]:
        params = {
            "format": "jsonv2",
            "q": query,
            "limit": "1",
            "addressdetails": "1",
        }
        if country_code:
            params["countrycodes"] = country_code.lower()
        url = f"{self.base_url}/search?{parse.urlencode(params)}"
        req = request.Request(url, headers={"User-Agent": self.user_agent, "Accept-Language": "en"})
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            return self._failed(query, f"HTTP_{exc.code}", str(exc))
        except error.URLError as exc:
            return self._failed(query, "URL_ERROR", str(exc))
        except (RemoteDisconnected, TimeoutError, SocketTimeout) as exc:
            return self._failed(query, "CONNECTION_CLOSED", str(exc))
        except OSError as exc:
            return self._failed(query, "NETWORK_ERROR", str(exc))
        except json.JSONDecodeError as exc:
            return self._failed(query, "BAD_JSON", str(exc))

        if not payload:
            return self._failed(query, "NO_RESULTS", "")
        top = payload[0]
        lat = to_float(top.get("lat"))
        lon = to_float(top.get("lon"))
        if lat is None or lon is None:
            return self._failed(query, "NO_COORDS", "")
        return {
            "asia_address_key": "",
            "geocode_query": query,
            "query_variant": "",
            "geocode_status": "OK",
            "latitude": lat,
            "longitude": lon,
            "matched_address": clean_text(top.get("display_name", "")),
            "location_type": "/".join(
                part for part in [clean_text(top.get("class", "")), clean_text(top.get("type", ""))] if part
            ),
            "place_id": clean_text(top.get("place_id", "")),
            "importance": top.get("importance", ""),
            "source": "nominatim",
            "error_message": "",
            "geocoded_date": date.today().isoformat(),
        }

    def load_cache(self) -> pd.DataFrame:
        if not self.cache_path.exists():
            return self.empty_cache_frame()
        df = pd.read_csv(self.cache_path, encoding="utf-8-sig", low_memory=False)
        for col in self.empty_cache_frame().columns:
            if col not in df.columns:
                df[col] = ""
        return df[self.empty_cache_frame().columns.tolist()].copy()

    def save_cache(self, df: pd.DataFrame) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.cache_path, index=False, encoding="utf-8-sig")

    @staticmethod
    def empty_cache_frame() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "asia_address_key",
                "geocode_query",
                "query_variant",
                "geocode_status",
                "latitude",
                "longitude",
                "matched_address",
                "location_type",
                "place_id",
                "importance",
                "source",
                "error_message",
                "geocoded_date",
            ]
        )

    @staticmethod
    def _query_variants(row: pd.Series) -> list[tuple[str, str]]:
        candidates = [
            ("geocode_query", row.get("geocode_query", "")),
            ("translated_query", row.get("translated_query", "")),
            (
                "address_city_state_postal_country",
                ", ".join(
                    part
                    for part in [
                        clean_text(row.get("ADDRESS_LINE1_INFO", "")),
                        clean_text(row.get("CITY_NAME", "")),
                        clean_text(row.get("STATE_NAME", "")),
                        clean_text(row.get("POSTAL_CODE", "")),
                        clean_text(row.get("translated_country", "")) or clean_text(row.get("COUNTRY_NAME", "")),
                    ]
                    if part
                ),
            ),
            (
                "city_state_postal_country",
                ", ".join(
                    part
                    for part in [
                        clean_text(row.get("CITY_NAME", "")),
                        clean_text(row.get("STATE_NAME", "")),
                        clean_text(row.get("POSTAL_CODE", "")),
                        clean_text(row.get("translated_country", "")) or clean_text(row.get("COUNTRY_NAME", "")),
                    ]
                    if part
                ),
            ),
        ]
        output: list[tuple[str, str]] = []
        seen: set[str] = set()
        for name, value in candidates:
            query = clean_text(value)
            key = query.lower()
            if query and key not in seen:
                output.append((name, query))
                seen.add(key)
        return output

    @staticmethod
    def _merge_cache(cache: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
        if new_rows.empty:
            return cache
        merged = pd.concat([cache, new_rows], ignore_index=True)
        return merged.drop_duplicates(subset=["asia_address_key"], keep="last").reset_index(drop=True)

    @staticmethod
    def _failed(query: str, status: str, message: str) -> dict[str, object]:
        return {
            "asia_address_key": "",
            "geocode_query": query,
            "query_variant": "",
            "geocode_status": status,
            "latitude": "",
            "longitude": "",
            "matched_address": "",
            "location_type": "",
            "place_id": "",
            "importance": "",
            "source": "nominatim",
            "error_message": clean_text(message),
            "geocoded_date": date.today().isoformat(),
        }
