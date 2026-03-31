from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Mapping, Any

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


DEFAULT_SQL_PATH = Path("smart_routing/select_data.sql")
PROMISE_RANGE_PATTERN = re.compile(
    r"FORMAT_DATETIME\('%Y%m%d',\s*T1\.PROMISE_TIMESTAMP\)\s+BETWEEN\s+'(?P<start>\d{8})'\s+AND\s+'(?P<end>\d{8})'",
    re.IGNORECASE,
)
MONTH_RANGE_PATTERNS = [
    re.compile(r"T12\.YYYYMM_ID\s+BETWEEN\s+'(?P<start>\d{6})'\s+AND\s+'(?P<end>\d{6})'", re.IGNORECASE),
    re.compile(r"T7\.YYYYMM_ID\s+BETWEEN\s+'(?P<start>\d{6})'\s+AND\s+'(?P<end>\d{6})'", re.IGNORECASE),
]


def _date_to_yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def _date_to_yyyymm(value: date) -> str:
    return value.strftime("%Y%m")


def render_service_query(sql_text: str, start_date: date, end_date: date) -> str:
    start_yyyymmdd = _date_to_yyyymmdd(start_date)
    end_yyyymmdd = _date_to_yyyymmdd(end_date)
    start_yyyymm = _date_to_yyyymm(start_date)
    end_yyyymm = _date_to_yyyymm(end_date)

    rendered = PROMISE_RANGE_PATTERN.sub(
        f"FORMAT_DATETIME('%Y%m%d', T1.PROMISE_TIMESTAMP) BETWEEN '{start_yyyymmdd}' AND '{end_yyyymmdd}'",
        sql_text,
    )
    for pattern in MONTH_RANGE_PATTERNS:
        rendered = pattern.sub(
            lambda _m: f"{_m.group(0).split('BETWEEN')[0].rstrip()} BETWEEN '{start_yyyymm}' AND '{end_yyyymm}'",
            rendered,
        )
    return rendered


def load_service_account_info(secrets: Mapping[str, Any]) -> dict[str, Any]:
    if "bigquery_service_account" in secrets:
        section = secrets["bigquery_service_account"]
        return {key: section[key] for key in section.keys()}
    if "gcp_service_account" in secrets:
        section = secrets["gcp_service_account"]
        return {key: section[key] for key in section.keys()}
    raise KeyError("Streamlit secrets must contain [bigquery_service_account] or [gcp_service_account].")


def query_service_data(
    start_date: date,
    end_date: date,
    secrets: Mapping[str, Any],
    sql_path: Path = DEFAULT_SQL_PATH,
) -> tuple[pd.DataFrame, str]:
    sql_text = sql_path.read_text(encoding="utf-8")
    rendered_sql = render_service_query(sql_text, start_date, end_date)
    service_account_info = load_service_account_info(secrets)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(credentials=credentials, project=service_account_info["project_id"])
    df = client.query(rendered_sql).result().to_dataframe()
    return df, rendered_sql
