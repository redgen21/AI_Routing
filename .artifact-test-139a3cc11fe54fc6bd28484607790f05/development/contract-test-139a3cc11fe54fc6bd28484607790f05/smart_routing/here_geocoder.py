from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib import error, parse, request

import pandas as pd

from .census_geocoder import (
    build_address_key,
    build_unique_addresses,
    clean_street_address,
    normalize_postal_code,
    normalize_text,
    read_table,
)
from .geocode_storage import GeocodeBackend, GeocodeStore, resolve_geocode_store


DEFAULT_TIMEOUT = 30
DEFAULT_SLEEP_SEC = 0.05
DEFAULT_MIN_QUERY_SCORE = 0.7
DEFAULT_MIN_FIELD_SCORE = 0.7


@dataclass
class HereFallbackResult:
    run_month: str
    monthly_limit: int
    monthly_used_before_run: int
    monthly_remaining_before_run: int
    attempted: int
    geocoded: int
    failed: int
    cache_path: Path
    attempt_log_path: Path


class HereGeocoder:
    def __init__(
        self,
        api_key: str,
        cache_path: Path,
        attempt_log_path: Path,
        monthly_limit: int = 10000,
        timeout: int = DEFAULT_TIMEOUT,
        sleep_sec: float = DEFAULT_SLEEP_SEC,
        min_query_score: float = DEFAULT_MIN_QUERY_SCORE,
        min_field_score: float = DEFAULT_MIN_FIELD_SCORE,
        store: GeocodeStore | None = None,
        cache_backend: GeocodeBackend | str | None = None,
        database_config_path: Path | str | None = None,
    ) -> None:
        self.api_key = api_key.strip()
        self.cache_path = cache_path
        self.attempt_log_path = attempt_log_path
        self.monthly_limit = int(monthly_limit)
        self.timeout = int(timeout)
        self.sleep_sec = float(sleep_sec)
        self.min_query_score = float(min_query_score)
        self.min_field_score = float(min_field_score)
        self.store = store or resolve_geocode_store(cache_backend, database_config_path)

    def run_for_unmatched(
        self,
        service_path: Path,
        census_cache_path: Path,
        run_date: str | None = None,
        ignore_attempt_log_once: bool = False,
    ) -> HereFallbackResult:
        if not self.api_key:
            raise ValueError("HERE API key is required for fallback geocoding.")

        run_dt = self._normalize_run_date(run_date)
        run_month = run_dt.strftime("%Y-%m")
        service_df = read_table(service_path)
        unique_df = build_unique_addresses(service_df)
        census_cache = self._load_cache(census_cache_path)
        here_cache = self._load_cache(self.cache_path)
        here_attempt_log = self._load_attempt_log(self.attempt_log_path)
        monthly_attempt_log = self._attempts_for_month(here_attempt_log, run_month)

        already_done = set(census_cache["address_key"]).union(set(here_cache["address_key"]))
        if not ignore_attempt_log_once:
            already_done = already_done.union(set(monthly_attempt_log["address_key"]))
        pending_df = unique_df[~unique_df["address_key"].isin(already_done)].copy()
        monthly_used = int(len(monthly_attempt_log))
        monthly_remaining = max(self.monthly_limit - monthly_used, 0)
        pending_df = pending_df.head(monthly_remaining).copy()

        rows: list[dict[str, object]] = []
        attempt_rows: list[dict[str, object]] = []
        for _, row in pending_df.iterrows():
            result, attempt_info = self._geocode_one(
                address_line1=row["address_line1"],
                city=row["city"],
                state=row["state"],
                postal_code=row["postal_code"],
                country_name=row["country_name"],
                address_key=row["address_key"],
            )
            if attempt_info is not None:
                attempt_rows.append(attempt_info)
            if result is not None:
                rows.append(result)
            if self.sleep_sec > 0:
                time.sleep(self.sleep_sec)

        new_df = pd.DataFrame(rows) if rows else self._empty_cache_frame()
        if not new_df.empty:
            merged = pd.concat([here_cache, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            self._save_cache(merged)
        elif not self.store.artifact_exists(self.cache_path):
            self._save_cache(here_cache)

        attempt_df = pd.DataFrame(attempt_rows) if attempt_rows else self._empty_attempt_log_frame()
        if not attempt_df.empty:
            merged_attempt = pd.concat([here_attempt_log, attempt_df], ignore_index=True)
            merged_attempt = merged_attempt.drop_duplicates(
                subset=["address_key", "attempted_date"], keep="last"
            ).reset_index(drop=True)
            self._save_attempt_log(merged_attempt)
        elif not self.store.artifact_exists(self.attempt_log_path):
            self._save_attempt_log(here_attempt_log)

        return HereFallbackResult(
            run_month=run_month,
            monthly_limit=self.monthly_limit,
            monthly_used_before_run=monthly_used,
            monthly_remaining_before_run=monthly_remaining,
            attempted=int(len(pending_df)),
            geocoded=int(len(new_df)),
            failed=int(len(pending_df) - len(new_df)),
            cache_path=self.cache_path,
            attempt_log_path=self.attempt_log_path,
        )

    def _geocode_one(
        self,
        address_line1: str,
        city: str,
        state: str,
        postal_code: str,
        country_name: str,
        address_key: str,
    ) -> tuple[dict[str, object] | None, dict[str, object] | None]:
        query = ", ".join([part for part in [address_line1, city, state, postal_code, country_name] if part])
        params = parse.urlencode({"q": query, "apiKey": self.api_key})
        url = f"https://geocode.search.hereapi.com/v1/geocode?{params}"
        try:
            with request.urlopen(url, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            return None, {
                "address_key": address_key,
                "attempted_date": date.today().isoformat(),
                "status": f"HTTP_{exc.code}",
                "source": "here_geocoding_api",
            }
        except error.URLError:
            return None, None

        attempt_info = {
            "address_key": address_key,
            "attempted_date": date.today().isoformat(),
            "status": "UNKNOWN",
            "source": "here_geocoding_api",
        }
        items = payload.get("items") or []
        if not items:
            attempt_info["status"] = "NO_RESULTS"
            return None, attempt_info

        top = items[0]
        if not self._passes_quality(top):
            attempt_info["status"] = "LOW_QUALITY"
            return None, attempt_info

        position = top.get("position") or {}
        lat = self._to_float(position.get("lat"))
        lon = self._to_float(position.get("lng"))
        if lat is None or lon is None:
            attempt_info["status"] = "NO_COORDS"
            return None, attempt_info

        scoring = top.get("scoring") or {}
        query_score = self._to_float(scoring.get("queryScore"))
        attempt_info["status"] = "OK"
        return (
            {
                "address_key": address_key,
                "address_line1": address_line1,
                "city": city,
                "state": state,
                "postal_code": postal_code,
                "country_name": country_name,
                "matched_address": normalize_text(top.get("title")),
                "match_indicator": "Match",
                "match_type": f"HERE_{query_score:.2f}" if query_score is not None else "HERE",
                "longitude": lon,
                "latitude": lat,
                "tiger_line_id": "",
                "tiger_line_side": "",
                "census_state_fips": "",
                "census_county_fips": "",
                "census_tract": "",
                "census_block": "",
                "geocoded_date": date.today().isoformat(),
                "source": "here_geocoding_api",
            },
            attempt_info,
        )

    def geocode_query(
        self,
        query: str,
        address_key: str,
        *,
        variant_name: str = "",
    ) -> tuple[dict[str, object] | None, dict[str, object] | None]:
        query = normalize_text(query)
        if not query:
            return None, {
                "address_key": address_key,
                "attempted_date": date.today().isoformat(),
                "status": "EMPTY_QUERY",
                "source": "here_geocoding_api",
                "variant": variant_name,
            }
        params = parse.urlencode({"q": query, "apiKey": self.api_key})
        url = f"https://geocode.search.hereapi.com/v1/geocode?{params}"
        try:
            with request.urlopen(url, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            return None, {
                "address_key": address_key,
                "attempted_date": date.today().isoformat(),
                "status": f"HTTP_{exc.code}",
                "source": "here_geocoding_api",
                "variant": variant_name,
            }
        except error.URLError:
            return None, None

        attempt_info = {
            "address_key": address_key,
            "attempted_date": date.today().isoformat(),
            "status": "UNKNOWN",
            "source": "here_geocoding_api",
            "variant": variant_name,
        }
        items = payload.get("items") or []
        if not items:
            attempt_info["status"] = "NO_RESULTS"
            return None, attempt_info

        top = items[0]
        scoring = top.get("scoring") or {}
        query_score = self._to_float(scoring.get("queryScore")) if isinstance(scoring, dict) else None
        field_score = scoring.get("fieldScore") if isinstance(scoring, dict) else {}
        attempt_info["query_score"] = query_score if query_score is not None else ""
        attempt_info["field_score"] = json.dumps(field_score or {}, ensure_ascii=False)
        attempt_info["result_type"] = normalize_text(top.get("resultType"))

        position = top.get("position") or {}
        lat = self._to_float(position.get("lat"))
        lon = self._to_float(position.get("lng"))
        if lat is None or lon is None:
            attempt_info["status"] = "NO_COORDS"
            return None, attempt_info
        if not self._passes_quality(top):
            attempt_info["status"] = "LOW_QUALITY"
            attempt_info["latitude"] = lat
            attempt_info["longitude"] = lon
            attempt_info["matched_address"] = normalize_text(top.get("title"))
            return None, attempt_info

        attempt_info["status"] = "OK"
        return (
            {
                "address_key": address_key,
                "address_line1": query,
                "city": "",
                "state": "",
                "postal_code": "",
                "country_name": "",
                "matched_address": normalize_text(top.get("title")),
                "match_indicator": "Match",
                "match_type": f"HERE_{query_score:.2f}" if query_score is not None else "HERE",
                "longitude": lon,
                "latitude": lat,
                "tiger_line_id": "",
                "tiger_line_side": "",
                "census_state_fips": "",
                "census_county_fips": "",
                "census_tract": "",
                "census_block": "",
                "geocoded_date": date.today().isoformat(),
                "source": "here_geocoding_api",
                "here_query_variant": variant_name,
            },
            attempt_info,
        )

    def geocode_query_variants(
        self,
        address_key: str,
        variants: list[tuple[str, str]],
    ) -> tuple[dict[str, object] | None, list[dict[str, object]]]:
        attempts: list[dict[str, object]] = []
        for variant_name, query in variants:
            result, attempt_info = self.geocode_query(query, address_key, variant_name=variant_name)
            if attempt_info is not None:
                attempts.append(attempt_info)
            if result is not None:
                return result, attempts
            if self.sleep_sec > 0:
                time.sleep(self.sleep_sec)
        return None, attempts

    def _passes_quality(self, item: dict[str, object]) -> bool:
        scoring = item.get("scoring") or {}
        if not isinstance(scoring, dict):
            return False
        query_score = self._to_float(scoring.get("queryScore"))
        if query_score is None or query_score < self.min_query_score:
            return False

        field_score = scoring.get("fieldScore") or {}
        if not isinstance(field_score, dict):
            return True
        for key in ["state", "city", "postalCode", "houseNumber"]:
            value = self._to_float(field_score.get(key))
            if value is not None and value < self.min_field_score:
                return False
        streets = field_score.get("streets")
        if isinstance(streets, list) and streets:
            street_score = self._to_float(streets[0])
            if street_score is not None and street_score < self.min_field_score:
                return False
        return True

    def _load_cache(self, path: Path) -> pd.DataFrame:
        df = self.store.load_cache(path)
        if df.empty:
            return self._empty_cache_frame()
        for col in self._empty_cache_frame().columns:
            if col not in df.columns:
                df[col] = ""
        if {"address_line1", "city", "state", "postal_code", "country_name"}.issubset(df.columns):
            df["address_line1"] = df.apply(
                lambda row: clean_street_address(
                    row.get("address_line1"),
                    row.get("city"),
                    row.get("state"),
                    row.get("postal_code"),
                    row.get("country_name"),
                ),
                axis=1,
            )
            df["address_key"] = df.apply(
                lambda row: build_address_key(
                    row.get("address_line1"),
                    row.get("city"),
                    row.get("state"),
                    row.get("postal_code"),
                    row.get("country_name"),
                ),
                axis=1,
            )
        return df[self._empty_cache_frame().columns.tolist()].copy()

    def _save_cache(self, df: pd.DataFrame) -> None:
        self.store.save_cache(self.cache_path, df)

    def _load_attempt_log(self, path: Path) -> pd.DataFrame:
        df = self.store.load_attempt_log(path)
        if df.empty:
            return self._empty_attempt_log_frame()
        for col in self._empty_attempt_log_frame().columns:
            if col not in df.columns:
                df[col] = ""
        return df[self._empty_attempt_log_frame().columns.tolist()].copy()

    def _save_attempt_log(self, df: pd.DataFrame) -> None:
        self.store.save_attempt_log(self.attempt_log_path, df)

    @staticmethod
    def _normalize_run_date(run_date: str | None) -> date:
        if not run_date:
            return date.today()
        return pd.to_datetime(run_date, errors="raise").date()

    @staticmethod
    def _count_monthly_attempts(df: pd.DataFrame, run_month: str) -> int:
        return int(len(HereGeocoder._attempts_for_month(df, run_month)))

    @staticmethod
    def _attempts_for_month(df: pd.DataFrame, run_month: str) -> pd.DataFrame:
        if df.empty or "attempted_date" not in df.columns:
            return df.iloc[0:0].copy()
        attempted = pd.to_datetime(df["attempted_date"], errors="coerce")
        return df.loc[attempted.dt.strftime("%Y-%m").eq(run_month)].copy()

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _empty_cache_frame() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "address_key",
                "address_line1",
                "city",
                "state",
                "postal_code",
                "country_name",
                "matched_address",
                "match_indicator",
                "match_type",
                "longitude",
                "latitude",
                "tiger_line_id",
                "tiger_line_side",
                "census_state_fips",
                "census_county_fips",
                "census_tract",
                "census_block",
                "geocoded_date",
                "source",
            ]
        )

    @staticmethod
    def _empty_attempt_log_frame() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "address_key",
                "attempted_date",
                "status",
                "source",
            ]
        )
