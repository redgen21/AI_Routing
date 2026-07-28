"""Canonical Technician Data transform for the source four-sheet profile.

Region ownership is deliberately not inferred from ZIP coverage.  The caller
must provide the reviewed Region Data technician assignments, normally read
from ``common_region_plan_technician``.  This keeps Technician Data (address and
capability) separate from Region Data while validating their employee keys as
one bounded contract.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import pandas as pd


SOURCE_CITY = "Atlanta, GA"
TARGET_CITY = "Atlanta_6area"
SUBSIDIARY_NAME = "LGEAI"
_EMPLOYEE_RE = re.compile(r"^AI[0-9]{6}$")

PROFILE_SHEETS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "1. Zip Coverage": (
            "SVC_ENGINEER_CODE", "AREA_CODE", "AREA_NAME", "POSTAL_CODE",
            "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE",
        ),
        "2. Slot": (
            "SVC_ENGINEER_CODE", "Name", "Slot", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE",
        ),
        "3. Product": (
            "SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE",
            "REPAIR_FLAG", "AREA_PRODUCT_FLAG", "STRATEGIC_CITY_NAME", "SVC_CENTER_TYPE",
        ),
        "4. Address": (
            "SVC_ENGINEER_CODE", "Name", "Home Street Address", "City ", "State", "Zip",
        ),
    }
)
DERIVED_ADDRESS_COLUMNS = frozenset(
    {"matched_address", "match_indicator", "match_type", "geocoded_date", "source", "address_key"}
)


class TechnicianProfileDataError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class CanonicalTechnicianProfile:
    source_sha256: str
    canonical_sha256: str
    plan_id: str
    technician_rows: tuple[Mapping[str, Any], ...]
    capability_rows: tuple[Mapping[str, Any], ...]
    assignment_rows: tuple[Mapping[str, Any], ...]
    row_accounting: Mapping[str, Mapping[str, Any]]

    def safe_summary(self) -> dict[str, Any]:
        return {
            "source_sha256": self.source_sha256,
            "canonical_sha256": self.canonical_sha256,
            "plan_id": self.plan_id,
            "technician_count": len(self.technician_rows),
            "capability_count": len(self.capability_rows),
            "assigned_region_count": len(self.assignment_rows),
            "row_accounting": {
                name: dict(values) for name, values in self.row_accounting.items()
            },
        }


def _source_bytes(source: bytes | bytearray | memoryview | Path | str) -> bytes:
    if isinstance(source, bytes):
        payload = source
    elif isinstance(source, (bytearray, memoryview)):
        payload = bytes(source)
    elif isinstance(source, (Path, str)) and Path(source).is_file():
        payload = Path(source).read_bytes()
    else:
        raise TechnicianProfileDataError("SOURCE_INVALID")
    if not payload:
        raise TechnicianProfileDataError("SOURCE_EMPTY")
    return payload


def _clean(value: object) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _postal(value: object) -> str:
    text = _clean(value)
    if re.fullmatch(r"[0-9]{1,5}\.0", text):
        text = text[:-2]
    if not re.fullmatch(r"[0-9]{1,5}", text):
        raise TechnicianProfileDataError("HOME_POSTAL_INVALID")
    return text.zfill(5)


def _flag(value: object, *, code: str) -> str:
    text = _clean(value).upper()
    if text not in {"T", "F", "Y", "N"}:
        raise TechnicianProfileDataError(code)
    return text


def _read_profile(payload: bytes) -> dict[str, pd.DataFrame]:
    try:
        workbook = pd.ExcelFile(io.BytesIO(payload))
    except Exception as exc:
        raise TechnicianProfileDataError("XLSX_INVALID") from exc
    if not set(PROFILE_SHEETS).issubset(workbook.sheet_names):
        raise TechnicianProfileDataError("PROFILE_SHEETS_INVALID")
    frames: dict[str, pd.DataFrame] = {}
    for sheet, required in PROFILE_SHEETS.items():
        try:
            frame = pd.read_excel(io.BytesIO(payload), sheet_name=sheet, dtype=object)
        except Exception as exc:
            raise TechnicianProfileDataError("XLSX_INVALID") from exc
        frame.columns = [str(column).strip() for column in frame.columns]
        expected = {str(column).strip() for column in required}
        if not expected.issubset(frame.columns):
            raise TechnicianProfileDataError("PROFILE_COLUMNS_INVALID")
        if sheet == "4. Address" and DERIVED_ADDRESS_COLUMNS.intersection(frame.columns):
            raise TechnicianProfileDataError("DERIVED_PROFILE_NOT_ALLOWED")
        frames[sheet] = frame
    return frames


def _accounting(
    input_rows: int,
    accepted_rows: int,
    *,
    excluded_by_reason: Mapping[str, int] | None = None,
    rejected_by_reason: Mapping[str, int] | None = None,
) -> Mapping[str, Any]:
    excluded_reasons = dict(excluded_by_reason or {})
    rejected_reasons = dict(rejected_by_reason or {})
    excluded = sum(int(value) for value in excluded_reasons.values())
    rejected = sum(int(value) for value in rejected_reasons.values())
    if input_rows != accepted_rows + excluded + rejected:
        raise RuntimeError("TECHNICIAN_PROFILE_ROW_ACCOUNTING_INVALID")
    return MappingProxyType(
        {
            "input_rows": int(input_rows),
            "accepted_rows": int(accepted_rows),
            "excluded_rows": int(excluded),
            "excluded_by_reason": excluded_reasons,
            "rejected_rows": int(rejected),
            "rejected_by_reason": rejected_reasons,
        }
    )


def canonicalize_technician_profile(
    source: bytes | bytearray | memoryview | Path | str,
    *,
    plan_id: str,
    assignments: Mapping[str, Mapping[str, Any]],
) -> CanonicalTechnicianProfile:
    """Join source address/capability rows to reviewed Region Data assignments."""

    payload = _source_bytes(source)
    plan_id = _clean(plan_id)
    if not plan_id or not assignments:
        raise TechnicianProfileDataError("ASSIGNMENTS_REQUIRED")
    normalized_assignments: dict[str, dict[str, Any]] = {}
    for raw_code, raw in assignments.items():
        code = _clean(raw_code).upper()
        if not _EMPLOYEE_RE.fullmatch(code) or code in normalized_assignments or not isinstance(raw, Mapping):
            raise TechnicianProfileDataError("ASSIGNMENT_EMPLOYEE_INVALID")
        try:
            region_seq = int(raw.get("assigned_region_seq"))
        except (TypeError, ValueError) as exc:
            raise TechnicianProfileDataError("ASSIGNED_REGION_INVALID") from exc
        if not 1 <= region_seq <= 6:
            raise TechnicianProfileDataError("ASSIGNED_REGION_INVALID")
        name = _clean(raw.get("employee_name"))
        region_name = _clean(raw.get("assigned_region_name"))
        policy = _clean(raw.get("policy_mode"))
        if not name or not region_name or policy != "assigned_region_boundary_spillover":
            raise TechnicianProfileDataError("ASSIGNMENT_VALUE_INVALID")
        normalized_assignments[code] = {
            "employee_code": code,
            "employee_name": name,
            "assigned_region_seq": region_seq,
            "assigned_region_name": region_name,
            "policy_mode": policy,
            "active_flag": raw.get("active_flag") is not False,
        }

    frames = _read_profile(payload)
    selected_codes = set(normalized_assignments)
    zip_coverage = frames["1. Zip Coverage"]
    slot = frames["2. Slot"].copy()
    product = frames["3. Product"].copy()
    address = frames["4. Address"].copy()
    for frame in (slot, product, address):
        frame["SVC_ENGINEER_CODE"] = frame["SVC_ENGINEER_CODE"].map(_clean).str.upper()

    slot_selected = slot[slot["SVC_ENGINEER_CODE"].isin(selected_codes)].copy()
    address_selected = address[address["SVC_ENGINEER_CODE"].isin(selected_codes)].copy()
    product_selected = product[product["SVC_ENGINEER_CODE"].isin(selected_codes)].copy()
    if set(slot_selected["SVC_ENGINEER_CODE"]) != selected_codes or slot_selected["SVC_ENGINEER_CODE"].duplicated().any():
        raise TechnicianProfileDataError("SLOT_EMPLOYEE_COVERAGE_INVALID")
    if set(address_selected["SVC_ENGINEER_CODE"]) != selected_codes or address_selected["SVC_ENGINEER_CODE"].duplicated().any():
        raise TechnicianProfileDataError("ADDRESS_EMPLOYEE_COVERAGE_INVALID")
    if set(product_selected["SVC_ENGINEER_CODE"]) != selected_codes:
        raise TechnicianProfileDataError("PRODUCT_EMPLOYEE_COVERAGE_INVALID")

    slot_lookup = slot_selected.set_index("SVC_ENGINEER_CODE", drop=False)
    address_lookup = address_selected.set_index("SVC_ENGINEER_CODE", drop=False)
    technician_rows: list[Mapping[str, Any]] = []
    assignment_rows: list[Mapping[str, Any]] = []
    for code in sorted(selected_codes):
        slot_row = slot_lookup.loc[code]
        address_row = address_lookup.loc[code]
        assignment = normalized_assignments[code]
        slot_name = _clean(slot_row.get("Name"))
        address_name = _clean(address_row.get("Name"))
        if (
            slot_name.casefold() != address_name.casefold()
            or slot_name.casefold() != assignment["employee_name"].casefold()
        ):
            raise TechnicianProfileDataError("EMPLOYEE_NAME_CONFLICT")
        if _clean(slot_row.get("STRATEGIC_CITY_NAME")) != SOURCE_CITY:
            raise TechnicianProfileDataError("SOURCE_CITY_INVALID")
        center_type = _clean(slot_row.get("SVC_CENTER_TYPE")).upper()
        if center_type not in {"DMS", "DMS2"}:
            raise TechnicianProfileDataError("CENTER_TYPE_INVALID")
        home_address = _clean(address_row.get("Home Street Address"))
        home_state = _clean(address_row.get("State")).upper()
        if not home_address or not home_state:
            raise TechnicianProfileDataError("HOME_ADDRESS_REQUIRED")
        latitude_text = _clean(address_row.get("latitude"))
        longitude_text = _clean(address_row.get("longitude"))
        if bool(latitude_text) != bool(longitude_text):
            raise TechnicianProfileDataError("HOME_COORDINATE_PAIR_INVALID")
        try:
            latitude = float(latitude_text) if latitude_text else None
            longitude = float(longitude_text) if longitude_text else None
        except ValueError as exc:
            raise TechnicianProfileDataError("HOME_COORDINATE_INVALID") from exc
        if (
            latitude is not None
            and (not math.isfinite(latitude) or not -90 <= latitude <= 90)
        ) or (
            longitude is not None
            and (not math.isfinite(longitude) or not -180 <= longitude <= 180)
        ):
            raise TechnicianProfileDataError("HOME_COORDINATE_INVALID")
        technician_rows.append(
            MappingProxyType(
                {
                    "subsidiary_name": SUBSIDIARY_NAME,
                    "strategic_city_name": TARGET_CITY,
                    "employee_code": code,
                    "employee_name": slot_name,
                    "center_type": center_type,
                    "home_address": home_address,
                    "home_city": _clean(address_row.get("City")),
                    "home_state": home_state,
                    "home_country": "USA",
                    "home_postal_code": _postal(address_row.get("Zip")),
                    "home_latitude": latitude,
                    "home_longitude": longitude,
                    "active_flag": bool(assignment["active_flag"]),
                    "priority_group": "B",
                    "max_home_to_job_min": None,
                }
            )
        )
        assignment_rows.append(MappingProxyType(dict(assignment)))

    product_selected["SERVICE_PRODUCT_GROUP_CODE"] = product_selected["SERVICE_PRODUCT_GROUP_CODE"].map(_clean).str.upper()
    product_selected["SERVICE_PRODUCT_CODE"] = product_selected["SERVICE_PRODUCT_CODE"].map(_clean).str.upper()
    product_selected["REPAIR_FLAG"] = product_selected["REPAIR_FLAG"].map(lambda value: _flag(value, code="REPAIR_FLAG_INVALID"))
    product_selected["AREA_PRODUCT_FLAG"] = product_selected["AREA_PRODUCT_FLAG"].map(lambda value: _flag(value, code="AREA_PRODUCT_FLAG_INVALID"))
    if (product_selected["STRATEGIC_CITY_NAME"].map(_clean) != SOURCE_CITY).any():
        raise TechnicianProfileDataError("PRODUCT_SOURCE_CITY_INVALID")
    key = ["SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE"]
    conflict_counts = (
        product_selected.groupby(key, dropna=False)[["REPAIR_FLAG", "AREA_PRODUCT_FLAG"]]
        .nunique(dropna=False)
        .max(axis=1)
    )
    if (conflict_counts > 1).any():
        raise TechnicianProfileDataError("CAPABILITY_DUPLICATE_CONFLICT")
    duplicate_mask = product_selected.duplicated(key, keep="first")
    canonical_product = product_selected.loc[~duplicate_mask].sort_values(key)
    capability_rows: list[Mapping[str, Any]] = []
    for _, row in canonical_product.iterrows():
        group = _clean(row["SERVICE_PRODUCT_GROUP_CODE"])
        product_code = _clean(row["SERVICE_PRODUCT_CODE"])
        if not group or not product_code:
            raise TechnicianProfileDataError("CAPABILITY_KEY_REQUIRED")
        capability_rows.append(
            MappingProxyType(
                {
                    "subsidiary_name": SUBSIDIARY_NAME,
                    "strategic_city_name": TARGET_CITY,
                    "employee_code": _clean(row["SVC_ENGINEER_CODE"]),
                    "product_group_code": group,
                    "product_code": product_code,
                    "repair_allowed": _clean(row["REPAIR_FLAG"]).upper() == "T",
                    "heavy_repair_allowed": not (
                        group == "REF" and _clean(row["AREA_PRODUCT_FLAG"]).upper() == "N"
                    ),
                    "priority_score": 100,
                    "effective_start_date": None,
                    "effective_end_date": None,
                }
            )
        )

    accounting = MappingProxyType(
        {
            "zip_coverage": _accounting(
                len(zip_coverage),
                0,
                excluded_by_reason={
                    "region_assignment_from_active_region_data": len(zip_coverage)
                },
            ),
            "slot": _accounting(
                len(slot), len(slot_selected),
                excluded_by_reason={"not_assigned_to_target_plan": len(slot) - len(slot_selected)},
            ),
            "address": _accounting(
                len(address), len(address_selected),
                excluded_by_reason={"not_assigned_to_target_plan": len(address) - len(address_selected)},
            ),
            "product": _accounting(
                len(product),
                len(capability_rows),
                excluded_by_reason={
                    "not_assigned_to_target_plan": len(product) - len(product_selected),
                    "duplicate_source_scope_projection": int(duplicate_mask.sum()),
                },
            ),
            "assignment": _accounting(len(assignments), len(assignment_rows)),
        }
    )
    canonical_payload = {
        "plan_id": plan_id,
        "technicians": [dict(row) for row in technician_rows],
        "capabilities": [dict(row) for row in capability_rows],
        "assignments": [dict(row) for row in assignment_rows],
        "row_accounting": {
            name: dict(values) for name, values in accounting.items()
        },
    }
    canonical_sha = hashlib.sha256(
        json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    return CanonicalTechnicianProfile(
        source_sha256=hashlib.sha256(payload).hexdigest(),
        canonical_sha256=canonical_sha,
        plan_id=plan_id,
        technician_rows=tuple(technician_rows),
        capability_rows=tuple(capability_rows),
        assignment_rows=tuple(assignment_rows),
        row_accounting=accounting,
    )


__all__ = [
    "CanonicalTechnicianProfile",
    "PROFILE_SHEETS",
    "TechnicianProfileDataError",
    "canonicalize_technician_profile",
]
