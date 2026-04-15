from __future__ import annotations

import csv
import json
import mimetypes
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib import error, request

import pandas as pd


DEFAULT_BENCHMARK = "Public_AR_Current"
DEFAULT_VINTAGE = "Current_Current"
DEFAULT_DAILY_LIMIT = 10_000
DEFAULT_TIMEOUT = 120
DEFAULT_BATCH_SIZE = 1_000


@dataclass
class GeocodeRunResult:
    run_date: str
    total_unique_addresses: int
    already_cached: int
    pending_before_run: int
    attempted_today: int
    geocoded_today: int
    failed_today: int
    remaining_after_run: int
    cache_path: Path
    merged_output_path: Path | None = None


def read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        encodings = ["utf-8-sig", "utf-8", "cp949", "latin1"]
        last_error: Exception | None = None
        for encoding in encodings:
            try:
                return pd.read_csv(path, encoding=encoding, low_memory=False)
            except Exception as exc:
                last_error = exc
        raise RuntimeError(f"Failed to read CSV: {path}") from last_error
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported input file type: {path.suffix}")


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def normalize_country_name(value: object) -> str:
    text = normalize_text(value).upper()
    if text in {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}:
        return "USA"
    return text


def normalize_postal_code(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = text.split("-")[0].upper()
    text = re.sub(r"\.0+$", "", text)
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        return str(int(numeric))
    return text


def clean_street_address(
    address_line1: object,
    city: object,
    state: object,
    postal_code: object,
    country_name: object,
) -> str:
    text = normalize_text(address_line1)
    if not text:
        return ""

    city_text = normalize_text(city)
    state_text = normalize_text(state)
    postal_text = normalize_postal_code(postal_code)
    country_text = normalize_text(country_name)

    cleaned = text
    suffix_patterns = []
    if city_text and state_text and postal_text and country_text:
        suffix_patterns.append(
            rf"[\s,]*{re.escape(city_text)}[\s,]+{re.escape(state_text)}[\s,]+{re.escape(postal_text)}[\s,]+{re.escape(country_text)}$"
        )
    if city_text and state_text and postal_text:
        suffix_patterns.append(
            rf"[\s,]*{re.escape(city_text)}[\s,]+{re.escape(state_text)}[\s,]+{re.escape(postal_text)}$"
        )
        suffix_patterns.append(
            rf"[\s,]*{re.escape(city_text)}[\s,]*,[\s]*{re.escape(state_text)}[\s,]*,[\s]*{re.escape(postal_text)}$"
        )
    if state_text and postal_text:
        suffix_patterns.append(
            rf"[\s,]*{re.escape(state_text)}[\s,]+{re.escape(postal_text)}$"
        )
        suffix_patterns.append(
            rf"[\s,]*{re.escape(state_text)}[\s,]*,[\s]*{re.escape(postal_text)}$"
        )
    if country_text:
        suffix_patterns.append(rf"[\s,]*{re.escape(country_text)}$")

    changed = True
    while changed:
        changed = False
        for pattern in suffix_patterns:
            updated = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip(" ,")
            if updated != cleaned:
                cleaned = updated
                changed = True

    return normalize_text(cleaned)


def build_address_key(
    address_line1: object,
    city: object,
    state: object,
    postal_code: object,
    country_name: object,
) -> str:
    parts = [
        normalize_text(address_line1).upper(),
        normalize_text(city).upper(),
        normalize_text(state).upper(),
        normalize_postal_code(postal_code),
        normalize_country_name(country_name),
    ]
    return "|".join(parts)


def build_unique_addresses(
    df: pd.DataFrame,
    address_col: str = "ADDRESS_LINE1_INFO",
    city_col: str = "CITY_NAME",
    state_col: str = "STATE_NAME",
    postal_col: str = "POSTAL_CODE",
    country_col: str = "COUNTRY_NAME",
) -> pd.DataFrame:
    required = [address_col, city_col, state_col, postal_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing address columns: {missing}")

    selected_cols = [address_col, city_col, state_col, postal_col]
    if country_col in df.columns:
        selected_cols.append(country_col)
    out = df[selected_cols].copy()
    out = out.rename(
        columns={
            address_col: "address_line1",
            city_col: "city",
            state_col: "state",
            postal_col: "postal_code",
            country_col: "country_name",
        }
    )
    if "country_name" not in out.columns:
        out["country_name"] = "USA"
    for col in ["city", "state", "postal_code", "country_name"]:
        if col == "country_name":
            out[col] = out[col].map(normalize_country_name)
        else:
            out[col] = out[col].map(normalize_text)
    out["postal_code"] = out["postal_code"].map(normalize_postal_code)
    out["address_line1_raw"] = out["address_line1"].map(normalize_text)
    out["address_line1"] = out.apply(
        lambda row: clean_street_address(
            row["address_line1_raw"],
            row["city"],
            row["state"],
            row["postal_code"],
            row["country_name"],
        ),
        axis=1,
    )
    out["address_key"] = out.apply(
        lambda row: build_address_key(
            row["address_line1"],
            row["city"],
            row["state"],
            row["postal_code"],
            row["country_name"],
        ),
        axis=1,
    )
    out = out[out["country_name"].eq("USA")].copy()
    out = out[out["address_line1"] != ""].copy()
    out = out.drop_duplicates(subset=["address_key"], keep="first").reset_index(drop=True)
    return out


def empty_geocode_cache_frame() -> pd.DataFrame:
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


def load_geocode_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return empty_geocode_cache_frame()
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    base = empty_geocode_cache_frame()
    for col in base.columns:
        if col not in df.columns:
            df[col] = ""
    df["address_key"] = df["address_key"].astype(str).replace({"nan": "", "None": "", "none": "", "NaN": ""}).str.strip()
    missing_key_mask = df["address_key"].eq("")
    if missing_key_mask.any() and {"address_line1", "city", "state", "postal_code", "country_name"}.issubset(df.columns):
        subset = df.loc[missing_key_mask, ["address_line1", "city", "state", "postal_code", "country_name"]].copy()
        subset["city"] = subset["city"].map(normalize_text)
        subset["state"] = subset["state"].map(normalize_text)
        subset["postal_code"] = subset["postal_code"].map(normalize_postal_code)
        subset["country_name"] = subset["country_name"].map(normalize_country_name)
        subset["address_line1"] = subset.apply(
            lambda row: clean_street_address(
                row.get("address_line1"),
                row.get("city"),
                row.get("state"),
                row.get("postal_code"),
                row.get("country_name"),
            ),
            axis=1,
        )
        subset["address_key"] = subset.apply(
            lambda row: build_address_key(
                row.get("address_line1"),
                row.get("city"),
                row.get("state"),
                row.get("postal_code"),
                row.get("country_name"),
            ),
            axis=1,
        )
        df.loc[missing_key_mask, "address_line1"] = subset["address_line1"].values
        df.loc[missing_key_mask, "city"] = subset["city"].values
        df.loc[missing_key_mask, "state"] = subset["state"].values
        df.loc[missing_key_mask, "postal_code"] = subset["postal_code"].values
        df.loc[missing_key_mask, "country_name"] = subset["country_name"].values
        df.loc[missing_key_mask, "address_key"] = subset["address_key"].values
    return df[base.columns.tolist()].copy()


def merge_service_with_geocodes(
    service_df: pd.DataFrame,
    geocode_cache_df: pd.DataFrame,
) -> pd.DataFrame:
    merged_df = service_df.copy()
    merged_df["city_norm"] = (
        merged_df["CITY_NAME"].map(normalize_text) if "CITY_NAME" in merged_df.columns else " "
    )
    merged_df["state_norm"] = (
        merged_df["STATE_NAME"].map(normalize_text) if "STATE_NAME" in merged_df.columns else " "
    )
    merged_df["postal_norm"] = (
        merged_df["POSTAL_CODE"].map(normalize_postal_code) if "POSTAL_CODE" in merged_df.columns else ""
    )
    merged_df["country_norm"] = (
        merged_df["COUNTRY_NAME"].map(normalize_country_name) if "COUNTRY_NAME" in merged_df.columns else "USA"
    )
    merged_df["address_line1_clean"] = merged_df.apply(
        lambda row: clean_street_address(
            row.get("ADDRESS_LINE1_INFO"),
            row.get("city_norm"),
            row.get("state_norm"),
            row.get("postal_norm"),
            row.get("country_norm"),
        ),
        axis=1,
    )
    merged_df["address_key"] = merged_df.apply(
        lambda row: build_address_key(
            row.get("address_line1_clean"),
            row.get("city_norm"),
            row.get("state_norm"),
            row.get("postal_norm"),
            row.get("country_norm"),
        ),
        axis=1,
    )
    merged_df = merged_df.merge(
        geocode_cache_df[
            [
                "address_key",
                "matched_address",
                "match_indicator",
                "match_type",
                "latitude",
                "longitude",
                "census_state_fips",
                "census_county_fips",
                "census_tract",
                "census_block",
                "geocoded_date",
                "source",
            ]
        ],
        on="address_key",
        how="left",
    )
    failure_mask = merged_df["latitude"].isna() | merged_df["longitude"].isna()
    merged_df.loc[failure_mask, "source"] = "failed"
    merged_df = merged_df.drop(
        columns=["city_norm", "state_norm", "postal_norm", "country_norm", "address_line1_clean"],
        errors="ignore",
    )
    return merged_df


class CensusBatchGeocoder:
    def __init__(
        self,
        cache_path: Path,
        log_path: Path,
        benchmark: str = DEFAULT_BENCHMARK,
        vintage: str = DEFAULT_VINTAGE,
        daily_limit: int = DEFAULT_DAILY_LIMIT,
        timeout: int = DEFAULT_TIMEOUT,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.cache_path = cache_path
        self.log_path = log_path
        self.benchmark = benchmark
        self.vintage = vintage
        self.daily_limit = int(daily_limit)
        self.timeout = int(timeout)
        self.batch_size = int(batch_size)

    def run_for_service_file(
        self,
        service_path: Path,
        merged_output_path: Path,
        report_path: Path | None = None,
        run_date: str | None = None,
        max_new_per_run: int | None = None,
    ) -> GeocodeRunResult:
        service_df = read_table(service_path)
        unique_df = build_unique_addresses(service_df)
        cache_df = self._load_cache()

        pending_df = unique_df[~unique_df["address_key"].isin(cache_df["address_key"])].copy()
        allowed = self.remaining_quota(run_date=run_date)
        if max_new_per_run is not None:
            allowed = min(allowed, int(max_new_per_run))
        batch_df = pending_df.head(max(allowed, 0)).copy()

        geocoded_df = self._geocode_batches(batch_df) if not batch_df.empty else self._empty_cache_frame()
        if not geocoded_df.empty:
            cache_df = self._upsert_cache(cache_df, geocoded_df)
            self._save_cache(cache_df)
            self._append_daily_log(run_date or date.today().isoformat(), len(geocoded_df))
        else:
            self._ensure_parent(self.cache_path)
            if not self.cache_path.exists():
                self._save_cache(cache_df)

        merged_df = merge_service_with_geocodes(service_df, cache_df)
        self._ensure_parent(merged_output_path)
        merged_df.to_csv(merged_output_path, index=False, encoding="utf-8-sig")

        result = GeocodeRunResult(
            run_date=run_date or date.today().isoformat(),
            total_unique_addresses=int(len(unique_df)),
            already_cached=int(len(unique_df) - len(pending_df)),
            pending_before_run=int(len(pending_df)),
            attempted_today=int(len(batch_df)),
            geocoded_today=int(len(geocoded_df)),
            failed_today=int(len(batch_df) - len(geocoded_df)),
            remaining_after_run=int(len(unique_df[~unique_df["address_key"].isin(cache_df["address_key"])])),
            cache_path=self.cache_path,
            merged_output_path=merged_output_path,
        )
        if report_path is not None:
            self._write_report(result, report_path)
        return result

    def remaining_quota(self, run_date: str | None = None) -> int:
        run_date = run_date or date.today().isoformat()
        daily_log = self._load_daily_log()
        used = int(daily_log.get(run_date, 0))
        return max(self.daily_limit - used, 0)

    def _geocode_batches(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return self._empty_cache_frame()

        results: list[pd.DataFrame] = []
        for start in range(0, len(df), self.batch_size):
            chunk_df = df.iloc[start : start + self.batch_size].copy()
            result_df = self._geocode_batch(chunk_df)
            if not result_df.empty:
                results.append(result_df)

        if not results:
            return self._empty_cache_frame()
        merged = pd.concat(results, ignore_index=True)
        merged = merged.drop_duplicates(subset=["address_key"], keep="first").reset_index(drop=True)
        return merged

    def _geocode_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return self._empty_cache_frame()

        self._ensure_parent(self.cache_path)
        temp_csv = self._write_batch_input(df)
        try:
            boundary = "----CodexCensusBoundary"
            content_type = mimetypes.guess_type(temp_csv.name)[0] or "text/csv"

            body = bytearray()
            for key, value in {"benchmark": self.benchmark, "vintage": self.vintage}.items():
                body.extend(f"--{boundary}\r\n".encode("utf-8"))
                body.extend(
                    f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode("utf-8")
                )
            body.extend(f"--{boundary}\r\n".encode("utf-8"))
            body.extend(
                (
                    f'Content-Disposition: form-data; name="addressFile"; filename="{temp_csv.name}"\r\n'
                    f"Content-Type: {content_type}\r\n\r\n"
                ).encode("utf-8")
            )
            body.extend(temp_csv.read_bytes())
            body.extend(f"\r\n--{boundary}--\r\n".encode("utf-8"))

            req = request.Request(
                "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch",
                data=bytes(body),
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                method="POST",
            )
            try:
                with request.urlopen(req, timeout=self.timeout) as response:
                    text = response.read().decode("utf-8")
                return self._parse_batch_response(text, df)
            except error.HTTPError as exc:
                if exc.code in {500, 502, 503, 504}:
                    return self._empty_cache_frame()
                raise
            except error.URLError:
                return self._empty_cache_frame()
        finally:
            temp_csv.unlink(missing_ok=True)

    def _parse_batch_response(self, text: str, source_df: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        reader = csv.reader(text.splitlines())
        source_index = source_df.set_index("address_key")

        for raw in reader:
            if not raw:
                continue
            cells = list(raw) + [""] * (12 - len(raw))
            record_id = cells[0].strip()
            if record_id not in source_index.index:
                continue
            src = source_index.loc[record_id]
            match_indicator = cells[2].strip()
            match_type = cells[3].strip()
            matched_address = cells[4].strip()
            lon, lat = self._parse_coords(cells[5])
            rows.append(
                {
                    "address_key": record_id,
                    "address_line1": src["address_line1"],
                    "city": src["city"],
                    "state": src["state"],
                    "postal_code": src["postal_code"],
                    "country_name": src["country_name"],
                    "matched_address": matched_address,
                    "match_indicator": match_indicator,
                    "match_type": match_type,
                    "longitude": lon,
                    "latitude": lat,
                    "tiger_line_id": cells[6].strip(),
                    "tiger_line_side": cells[7].strip(),
                    "census_state_fips": cells[8].strip(),
                    "census_county_fips": cells[9].strip(),
                    "census_tract": cells[10].strip(),
                    "census_block": cells[11].strip(),
                    "geocoded_date": date.today().isoformat(),
                    "source": "us_census_geocoder",
                }
            )

        result = pd.DataFrame(rows)
        if result.empty:
            return self._empty_cache_frame()
        result = result[result["match_indicator"] == "Match"].copy()
        result = result.drop_duplicates(subset=["address_key"], keep="first").reset_index(drop=True)
        return result

    def _write_batch_input(self, df: pd.DataFrame) -> Path:
        temp_dir = self.cache_path.parent / "_tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            prefix="census_batch_",
            delete=False,
            newline="",
            encoding="utf-8",
            dir=temp_dir,
        ) as tmp:
            writer = csv.writer(tmp)
            for _, row in df.iterrows():
                writer.writerow(
                    [
                        row["address_key"],
                        row["address_line1"],
                        row["city"],
                        row["state"],
                        row["postal_code"],
                    ]
                )
            return Path(tmp.name)

    def _load_cache(self) -> pd.DataFrame:
        return load_geocode_cache(self.cache_path)

    def _save_cache(self, df: pd.DataFrame) -> None:
        self._ensure_parent(self.cache_path)
        df.to_csv(self.cache_path, index=False, encoding="utf-8-sig")

    def _upsert_cache(self, cache_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        merged = pd.concat([cache_df, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["address_key"], keep="last").reset_index(drop=True)
        return merged

    def _load_daily_log(self) -> dict[str, int]:
        if not self.log_path.exists():
            return {}
        with self.log_path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        return {str(key): int(value) for key, value in raw.items()}

    def _append_daily_log(self, run_date: str, added_count: int) -> None:
        if added_count <= 0:
            return
        daily_log = self._load_daily_log()
        daily_log[run_date] = int(daily_log.get(run_date, 0)) + int(added_count)
        self._ensure_parent(self.log_path)
        with self.log_path.open("w", encoding="utf-8") as handle:
            json.dump(daily_log, handle, indent=2, ensure_ascii=False)

    def _write_report(self, result: GeocodeRunResult, report_path: Path) -> None:
        self._ensure_parent(report_path)
        report_df = pd.DataFrame(
            [
                {
                    "run_date": result.run_date,
                    "total_unique_addresses": result.total_unique_addresses,
                    "already_cached": result.already_cached,
                    "pending_before_run": result.pending_before_run,
                    "attempted_today": result.attempted_today,
                    "geocoded_today": result.geocoded_today,
                    "failed_today": result.failed_today,
                    "remaining_after_run": result.remaining_after_run,
                    "cache_path": str(result.cache_path),
                    "merged_output_path": str(result.merged_output_path) if result.merged_output_path else "",
                }
            ]
        )
        report_df.to_csv(report_path, index=False, encoding="utf-8-sig")

    @staticmethod
    def _to_float(value: object) -> float | None:
        text = normalize_text(value)
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    @classmethod
    def _parse_coords(cls, value: object) -> tuple[float | None, float | None]:
        text = normalize_text(value)
        if not text:
            return None, None
        parts = [part.strip() for part in text.split(",")]
        if len(parts) != 2:
            return None, None
        lon = cls._to_float(parts[0])
        lat = cls._to_float(parts[1])
        return lon, lat

    @staticmethod
    def _ensure_parent(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _empty_cache_frame() -> pd.DataFrame:
        return empty_geocode_cache_frame()
