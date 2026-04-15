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
    normalize_text,
    read_table,
)


DEFAULT_TIMEOUT = 30
DEFAULT_SLEEP_SEC = 0.05


@dataclass
class GoogleFallbackResult:
    run_month: str
    monthly_limit: int
    monthly_used_before_run: int
    monthly_remaining_before_run: int
    attempted: int
    geocoded: int
    failed: int
    cache_path: Path
    attempt_log_path: Path


class GoogleGeocoder:
    def __init__(
        self,
        api_key: str,
        cache_path: Path,
        attempt_log_path: Path,
        monthly_limit: int = 10000,
        timeout: int = DEFAULT_TIMEOUT,
        sleep_sec: float = DEFAULT_SLEEP_SEC,
    ) -> None:
        self.api_key = api_key.strip()
        self.cache_path = cache_path
        self.attempt_log_path = attempt_log_path
        self.monthly_limit = int(monthly_limit)
        self.timeout = int(timeout)
        self.sleep_sec = float(sleep_sec)

    def run_for_unmatched(
        self,
        service_path: Path,
        census_cache_path: Path,
        run_date: str | None = None,
        ignore_attempt_log_once: bool = False,
    ) -> GoogleFallbackResult:
        if not self.api_key:
            raise ValueError("Google API key is required for fallback geocoding.")

        run_dt = self._normalize_run_date(run_date)
        run_month = run_dt.strftime("%Y-%m")
        service_df = read_table(service_path)
        unique_df = build_unique_addresses(service_df)
        census_cache = self._load_cache(census_cache_path)
        google_cache = self._load_cache(self.cache_path)
        google_attempt_log = self._load_attempt_log(self.attempt_log_path)

        already_done = set(census_cache["address_key"]).union(set(google_cache["address_key"]))
        if not ignore_attempt_log_once:
            already_done = already_done.union(set(google_attempt_log["address_key"]))
        pending_df = unique_df[~unique_df["address_key"].isin(already_done)].copy()
        monthly_used = self._count_monthly_attempts(google_attempt_log, run_month)
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
            merged = pd.concat([google_cache, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            self._save_cache(merged)
        elif not self.cache_path.exists():
            self._save_cache(google_cache)

        attempt_df = pd.DataFrame(attempt_rows) if attempt_rows else self._empty_attempt_log_frame()
        if not attempt_df.empty:
            merged_attempt = pd.concat([google_attempt_log, attempt_df], ignore_index=True)
            merged_attempt = merged_attempt.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
            self._save_attempt_log(merged_attempt)
        elif not self.attempt_log_path.exists():
            self._save_attempt_log(google_attempt_log)

        return GoogleFallbackResult(
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
        params = parse.urlencode({"address": query, "key": self.api_key})
        url = f"https://maps.googleapis.com/maps/api/geocode/json?{params}"
        try:
            with request.urlopen(url, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError:
            return None, None
        except error.URLError:
            return None, None

        status = normalize_text(payload.get("status"))
        if status in {"OVER_DAILY_LIMIT", "OVER_QUERY_LIMIT"}:
            message = normalize_text(payload.get("error_message")) or status
            raise RuntimeError(f"Google Geocoding API error: {message}")
        attempt_info = {
            "address_key": address_key,
            "attempted_date": date.today().isoformat(),
            "status": status or "UNKNOWN",
            "source": "google_geocoding_api",
        }
        if status == "REQUEST_DENIED":
            attempt_info["status"] = "REQUEST_DENIED"
            return None, attempt_info
        if status != "OK":
            return None, attempt_info
        results = payload.get("results") or []
        if not results:
            attempt_info["status"] = "NO_RESULTS"
            return None, attempt_info

        top = results[0]
        geometry = top.get("geometry") or {}
        location = geometry.get("location") or {}

        lat = self._to_float(location.get("lat"))
        lon = self._to_float(location.get("lng"))
        if lat is None or lon is None:
            attempt_info["status"] = "NO_COORDS"
            return None, attempt_info

        return (
            {
                "address_key": address_key,
                "address_line1": address_line1,
                "city": city,
                "state": state,
                "postal_code": postal_code,
                "country_name": country_name,
                "matched_address": normalize_text(top.get("formatted_address")),
                "match_indicator": "Match",
                "match_type": normalize_text(geometry.get("location_type")) or "GOOGLE",
                "longitude": lon,
                "latitude": lat,
                "tiger_line_id": "",
                "tiger_line_side": "",
                "census_state_fips": "",
                "census_county_fips": "",
                "census_tract": "",
                "census_block": "",
                "geocoded_date": date.today().isoformat(),
                "source": "google_geocoding_api",
            },
            attempt_info,
        )

    def _load_cache(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return self._empty_cache_frame()
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
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
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.cache_path, index=False, encoding="utf-8-sig")

    def _load_attempt_log(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return self._empty_attempt_log_frame()
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
        for col in self._empty_attempt_log_frame().columns:
            if col not in df.columns:
                df[col] = ""
        return df[self._empty_attempt_log_frame().columns.tolist()].copy()

    def _save_attempt_log(self, df: pd.DataFrame) -> None:
        self.attempt_log_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.attempt_log_path, index=False, encoding="utf-8-sig")

    @staticmethod
    def _normalize_run_date(run_date: str | None) -> date:
        if not run_date:
            return date.today()
        return pd.to_datetime(run_date, errors="raise").date()

    @staticmethod
    def _count_monthly_attempts(df: pd.DataFrame, run_month: str) -> int:
        if df.empty or "attempted_date" not in df.columns:
            return 0
        attempted = pd.to_datetime(df["attempted_date"], errors="coerce")
        return int((attempted.dt.strftime("%Y-%m") == run_month).sum())

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
