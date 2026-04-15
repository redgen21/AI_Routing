from __future__ import annotations

from typing import Any


DEFAULT_TIMEZONE_OFFSET = "-04:00"
DEFAULT_ROUTING_MODE = "na_general"
DEFAULT_ROUTING_CITY = "Atlanta, GA"
SUPPORTED_ROUTING_MODES = {
    "na_general",
    "weekday_general",
    "z_weekday",
    "z_weekend",
}


def normalize_mode(mode: str | None) -> str:
    raw = str(mode or "").strip().lower()
    if not raw:
        return DEFAULT_ROUTING_MODE
    aliases = {
        "vrp": "na_general",
        "smart_routing": "na_general",
        "smart-routing": "na_general",
        "north_america_general": "na_general",
        "north-america-general": "na_general",
        "weekday": "weekday_general",
        "weekday_general": "weekday_general",
        "z_weekday": "z_weekday",
        "z-weekday": "z_weekday",
        "z_weekend": "z_weekend",
        "z-weekend": "z_weekend",
    }
    return aliases.get(raw, raw)


def normalize_city(city: str | None) -> str:
    raw = str(city or "").strip()
    return raw or DEFAULT_ROUTING_CITY


def format_planned_timestamp(service_date_key: str, time_text: str, timezone_offset: str) -> str:
    clean_time = str(time_text or "").strip() or "09:00"
    return f"{service_date_key}T{clean_time}:00{timezone_offset}"


def build_empty_result(
    request_payload: dict[str, Any],
    *,
    reason: str,
    mode: str | None = None,
) -> dict[str, Any]:
    jobs = list(request_payload.get("jobs", []))
    normalized_mode = normalize_mode(mode or request_payload.get("mode"))
    return {
        "request_id": str(request_payload.get("request_id", "")).strip(),
        "mode": normalized_mode,
        "city": normalize_city(request_payload.get("city")),
        "status": "completed",
        "summary": {
            "total_jobs": len(jobs),
            "assigned_jobs": 0,
            "unassigned_jobs": len(jobs),
        },
        "assignments": [],
        "unassigned": [
            {
                "salesforce_id": str(job.get("salesforce_id", "")).strip(),
                "receipt_no": str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip(),
                "reason": reason,
            }
            for job in jobs
        ],
        "engineer_summary": [],
    }
