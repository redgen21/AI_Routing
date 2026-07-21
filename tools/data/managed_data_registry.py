"""Fixed, preview-first registry for the Data Management screen.

This module deliberately does not accept filesystem paths, database table names,
or arbitrary schemas from a browser.  A caller selects one of the fixed dataset
IDs in :data:`DATASET_SPECS` and supplies bytes for preview/staging only.

Data files and code releases have separate lifecycles.  ``Common`` identifies a
single shared, versioned artifact; ``Development`` and ``Production`` identify
separate upload/staging scopes and must never be used interchangeably.
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from smart_routing.data_catalog import na_data_path


COMMON = "Common"
DEVELOPMENT = "Development"
PRODUCTION = "Production"
SCOPES = (COMMON, DEVELOPMENT, PRODUCTION)

MAX_PREVIEW_ROWS = 50
MAX_PREVIEW_COLUMNS = 80
MAX_SOURCE_COLUMNS = 512
MAX_CELL_CHARACTERS = 512
_SECRET_COLUMN_RE = re.compile(r"(?:password|passwd|secret|token|api[_-]?key|authorization)", re.IGNORECASE)
_SECRET_VALUE_RE = re.compile(r"(?:^sk-[A-Za-z0-9_-]{12,}$|^AKIA[0-9A-Z]{16}$|password\s*=)", re.IGNORECASE)
_CANONICAL_HEAVY_COLUMNS = (
    "product_group_code",
    "product_code",
    "detailed_symptom_code",
)
_HEAVY_SOURCE_COLUMNS = (
    "SERVICE_PRODUCT_GROUP_CODE",
    "SERVICE_PRODUCT_CODE",
    "SYMP_CODE_THREE",
)


class ManagedDataValidationError(ValueError):
    """A stable, non-sensitive validation failure code."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class PreviewSchema:
    """The bounded, public portion of an upload contract."""

    required_columns: tuple[str, ...] = ()
    sheet_columns: tuple[tuple[str, tuple[str, ...]], ...] = ()
    preview_columns: tuple[str, ...] = ()

    def required_for_sheet(self, sheet_name: str) -> tuple[str, ...]:
        for name, columns in self.sheet_columns:
            if name == sheet_name:
                return columns
        return self.required_columns


@dataclass(frozen=True)
class DbSyncProfile:
    """A fixed DB destination contract; it is never supplied by a caller."""

    table: str
    key_columns: tuple[str, ...]
    operation: str


@dataclass(frozen=True)
class DatasetSpec:
    id: str
    scope: str
    enabled: bool
    extensions: tuple[str, ...]
    max_bytes: int
    local_canonical_role: str
    remote_storage_class: str
    db_profile: DbSyncProfile | None
    contains_pii: bool
    pii_columns: tuple[str, ...]
    preview_schema: PreviewSchema
    description: str

    @property
    def allowed_targets(self) -> tuple[str, ...]:
        if not self.enabled:
            return ()
        if self.db_profile is None:
            return ("file_upload", "preview")
        return ("file_upload", "preview", "db_preview", "db_apply")

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "scope": self.scope,
            "enabled": self.enabled,
            "extensions": list(self.extensions),
            "max_bytes": self.max_bytes,
            "local_canonical_role": self.local_canonical_role,
            "remote_storage_class": self.remote_storage_class,
            "allowed_targets": list(self.allowed_targets),
            "db_profile": None
            if self.db_profile is None
            else {
                "table": self.db_profile.table,
                "key_columns": list(self.db_profile.key_columns),
                "operation": self.db_profile.operation,
            },
            "contains_pii": self.contains_pii,
            "preview_schema": {
                "required_columns": list(self.preview_schema.required_columns),
                "sheet_columns": {
                    name: list(columns) for name, columns in self.preview_schema.sheet_columns
                },
                "preview_columns": list(self.preview_schema.preview_columns),
            },
            "description": self.description,
        }


@dataclass(frozen=True)
class PreviewTable:
    name: str | None
    columns: tuple[str, ...]
    rows: tuple[Mapping[str, str], ...]
    sampled_row_count: int
    masked_columns: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": list(self.columns),
            "rows": [dict(row) for row in self.rows],
            "sampled_row_count": self.sampled_row_count,
            "masked_columns": list(self.masked_columns),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class UploadPreview:
    dataset_id: str
    scope: str
    filename: str
    extension: str
    size_bytes: int
    source_sha256: str
    tables: tuple[PreviewTable, ...]
    normalization: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "scope": self.scope,
            "filename": self.filename,
            "extension": self.extension,
            "size_bytes": self.size_bytes,
            "source_sha256": self.source_sha256,
            "tables": [table.as_dict() for table in self.tables],
            "normalization": dict(self.normalization) if self.normalization else None,
        }


@dataclass(frozen=True)
class HeavyRepairNormalization:
    canonical_csv: bytes
    source_sha256: str
    canonical_sha256: str
    input_rows: int
    accepted_rows: int
    rejected_rows: int
    rejected_by_reason: Mapping[str, int]

    @property
    def is_clean(self) -> bool:
        return self.rejected_rows == 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "input_rows": self.input_rows,
            "accepted_rows": self.accepted_rows,
            "rejected_rows": self.rejected_rows,
            "rejected_by_reason": dict(self.rejected_by_reason),
            "source_sha256": self.source_sha256,
            "canonical_sha256": self.canonical_sha256,
            "canonical_columns": list(_CANONICAL_HEAVY_COLUMNS),
            "clean": self.is_clean,
        }


_PROFILE_SHEETS = (
    ("1. Zip Coverage", ("SVC_ENGINEER_CODE", "POSTAL_CODE", "STRATEGIC_CITY_NAME")),
    ("2. Slot", ("SVC_ENGINEER_CODE", "STRATEGIC_CITY_NAME", "Slot")),
    ("3. Product", ("SVC_ENGINEER_CODE", "SERVICE_PRODUCT_GROUP_CODE", "SERVICE_PRODUCT_CODE")),
    ("4. Address", ("SVC_ENGINEER_CODE", "Name")),
)


DATASET_SPECS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        "symptom_mapping_source", COMMON, True, (".xlsx",), 8 * 1024 * 1024,
        "symptom_mapping", "common_reference", None, False, (),
        PreviewSchema(required_columns=_HEAVY_SOURCE_COLUMNS),
        "Common product/symptom reference source. It does not write a database table.",
    ),
    DatasetSpec(
        "heavy_repair_rules", COMMON, True, (".csv", ".xlsx"), 8 * 1024 * 1024,
        "heavy_repair_lookup", "common_db_input", DbSyncProfile(
            "common_heavy_repair_rule_master", _CANONICAL_HEAVY_COLUMNS, "preview_then_upsert"
        ), False, (),
        PreviewSchema(required_columns=_CANONICAL_HEAVY_COLUMNS),
        "Canonical heavy-repair rule input. Source symptom columns are normalized to three keys.",
    ),
    DatasetSpec(
        "client_master", COMMON, True, (".xlsx",), 32 * 1024 * 1024,
        "client_master", "common_reference", None, False, (),
        PreviewSchema(required_columns=(
            "Product Group Name", "Product Group Code", "Product Name", "Product Code",
            "Symptom Name", "Symptom Code", "Detailed Symptom Name", "Detailed Symptom Code",
        )),
        "Common client/product/symptom reference workbook. Upload and preview only.",
    ),
    DatasetSpec(
        "service_raw", DEVELOPMENT, True, (".csv",), 160 * 1024 * 1024,
        "service_raw", "development_upload_snapshot", None, True,
        ("SVC_ENGINEER_NAME", "SHIP_TO_FULL_NAME", "ADDRESS_LINE1_INFO", "BILL_TO_NAME"),
        PreviewSchema(required_columns=(
            "STRATEGIC_CITY_NAME", "GSFS_RECEIPT_NO", "POSTAL_CODE", "ADDRESS_LINE1_INFO",
        )),
        "Development raw service snapshot. Preview is intentionally tolerant of malformed trailing columns.",
    ),
    DatasetSpec(
        "service_geocoded", DEVELOPMENT, True, (".csv",), 96 * 1024 * 1024,
        "service_geocoded", "development_processed_stage", None, True,
        ("SVC_ENGINEER_NAME", "ADDRESS_LINE1_INFO", "matched_address", "address_key", "latitude", "longitude"),
        PreviewSchema(required_columns=(
            "STRATEGIC_CITY_NAME", "GSFS_RECEIPT_NO", "POSTAL_CODE", "latitude", "longitude",
        )),
        "Development processed/geocoded service artifact. Upload and preview only; job DB loads are transactional.",
    ),
    DatasetSpec(
        "profile_raw", DEVELOPMENT, True, (".xlsx",), 32 * 1024 * 1024,
        "profile_raw", "development_upload_snapshot", None, True,
        ("Name", "Home Street Address", "City ", "State", "Zip"),
        PreviewSchema(sheet_columns=_PROFILE_SHEETS),
        "Development profile source workbook. Upload and preview only.",
    ),
    DatasetSpec(
        "service_geocoded", PRODUCTION, True, (".csv",), 96 * 1024 * 1024,
        "service_geocoded", "production_staged_upload", None, True,
        ("SVC_ENGINEER_NAME", "ADDRESS_LINE1_INFO", "matched_address", "address_key", "latitude", "longitude"),
        PreviewSchema(required_columns=(
            "STRATEGIC_CITY_NAME", "GSFS_RECEIPT_NO", "POSTAL_CODE", "latitude", "longitude",
        )),
        "Production staged geocoded service artifact. It cannot directly upsert job inputs.",
    ),
    DatasetSpec(
        "profile_production", PRODUCTION, True, (".xlsx",), 32 * 1024 * 1024,
        "profile_production", "production_staged_upload", None, True,
        ("Name", "Home Street Address", "City ", "State", "Zip", "matched_address", "address_key", "latitude", "longitude"),
        PreviewSchema(sheet_columns=_PROFILE_SHEETS),
        "Production staged profile workbook. Upload and preview only; promotion is a separate approval action.",
    ),
    # Disabled specifications stay visible so the UI can describe the lifecycle
    # without exposing a write path before the related policy contracts exist.
    DatasetSpec("reviewed_regions", COMMON, False, (".csv",), 32 * 1024 * 1024,
                "reviewed_regions_dir", "common_reviewed_read_only", None, False, (),
                PreviewSchema(required_columns=("POSTAL_CODE", "region_id", "region_seq")),
                "Reviewed region plans are read-only until the promotion workflow is connected."),
    DatasetSpec("region_seeds", COMMON, False, (".csv",), 32 * 1024 * 1024,
                "region_seed_dir", "common_db_seed_read_only", None, False, (),
                PreviewSchema(required_columns=("POSTAL_CODE", "region_seq")),
                "Region DB seed files are read-only; approved plan promotion owns their lifecycle."),
    DatasetSpec("technician_list", COMMON, False, (".xlsx",), 16 * 1024 * 1024,
                "technician_list", "common_restricted_read_only", None, True,
                ("Tech Name", "Home Address", "Home Zip"), PreviewSchema(required_columns=("EMP_NUMBER", "Tech Name")),
                "Technician source data is read-only pending a reviewed technician contract."),
    DatasetSpec("technician_map", COMMON, False, (".xlsx",), 16 * 1024 * 1024,
                "technician_map", "common_read_only", None, True, ("Tech Name",),
                PreviewSchema(required_columns=("EMP_NUMBER", "Tech Name")),
                "Address-free technician map view is read-only."),
    DatasetSpec("atlanta_engineer_region", COMMON, False, (".csv",), 8 * 1024 * 1024,
                "atlanta_engineer_region", "common_restricted_read_only", None, True, ("Name", "SVC_ENGINEER_CODE"),
                PreviewSchema(required_columns=("SVC_ENGINEER_CODE", "assigned_region_seq")),
                "City-specific technician-region derivative is read-only."),
    DatasetSpec("atlanta_engineer_home", COMMON, False, (".csv",), 8 * 1024 * 1024,
                "atlanta_engineer_home", "common_restricted_read_only", None, True,
                ("Name", "Home Street Address", "City ", "State", "Zip", "latitude", "longitude"),
                PreviewSchema(required_columns=("SVC_ENGINEER_CODE", "latitude", "longitude")),
                "City-specific technician-home derivative is read-only."),
)

_SPECS_BY_KEY = {(spec.id, spec.scope): spec for spec in DATASET_SPECS}


def _require_scope(scope: str) -> str:
    if scope not in SCOPES:
        raise ManagedDataValidationError("SCOPE_NOT_ALLOWED")
    return scope


def get_dataset_spec(dataset_id: str, scope: str) -> DatasetSpec:
    """Return one fixed spec; unknown IDs/scopes cannot fall through to a path."""
    _require_scope(scope)
    spec = _SPECS_BY_KEY.get((str(dataset_id), scope))
    if spec is None:
        raise ManagedDataValidationError("DATASET_NOT_ALLOWED")
    return spec


def list_dataset_specs(
    scope: str | None = None, *, include_disabled: bool = True
) -> tuple[DatasetSpec, ...]:
    """List the fixed registry, optionally for one of the three fixed scopes."""
    if scope is not None:
        _require_scope(scope)
    return tuple(
        spec for spec in DATASET_SPECS
        if (scope is None or spec.scope == scope) and (include_disabled or spec.enabled)
    )


def list_managed_data_sets(scope: str) -> tuple[dict[str, Any], ...]:
    """Public UI/backend registry view for exactly one fixed scope."""
    return tuple(spec.as_dict() for spec in list_dataset_specs(scope))


def get_managed_data_set(dataset_id: str, scope: str | None = None) -> DatasetSpec:
    """Get a fixed data set without accepting an arbitrary path or table name.

    ``service_geocoded`` exists in both Development and Production, therefore
    callers must pass ``scope`` for it.  Single-scope Common data sets remain
    convenient to inspect by their stable ID alone.
    """
    if scope is not None:
        return get_dataset_spec(dataset_id, scope)
    candidates = tuple(spec for spec in DATASET_SPECS if spec.id == str(dataset_id))
    if not candidates:
        raise ManagedDataValidationError("DATASET_NOT_ALLOWED")
    if len(candidates) != 1:
        raise ManagedDataValidationError("SCOPE_REQUIRED")
    return candidates[0]


def resolve_fixed_local_canonical_path(
    dataset_id: str, scope: str, catalog_path: Path | str | None = None
) -> Path:
    """Resolve only the catalog role declared by a fixed registry entry."""
    spec = get_dataset_spec(dataset_id, scope)
    return na_data_path(spec.local_canonical_role, catalog_path)


def _extension(filename: str) -> str:
    name = Path(str(filename)).name
    suffix = Path(name).suffix.lower()
    if not name or name in {".", ".."} or not suffix:
        raise ManagedDataValidationError("FILE_EXTENSION_NOT_ALLOWED")
    return suffix


def _validate_upload(spec: DatasetSpec, filename: str, data: bytes) -> str:
    if not spec.enabled:
        raise ManagedDataValidationError("DATASET_DISABLED")
    if not isinstance(data, bytes) or not data:
        raise ManagedDataValidationError("FILE_EMPTY")
    if len(data) > spec.max_bytes:
        raise ManagedDataValidationError("FILE_TOO_LARGE")
    extension = _extension(filename)
    if extension not in spec.extensions:
        raise ManagedDataValidationError("FILE_EXTENSION_NOT_ALLOWED")
    if b"\x00" in data and extension == ".csv":
        raise ManagedDataValidationError("CSV_INVALID")
    return extension


def _decode_csv(data: bytes) -> str:
    # Legacy service extracts occasionally contain a few non-CP949 bytes even
    # though their headers are CP949-compatible. Latin-1 is a last-resort
    # lossless byte mapping for the *preview* parser; schema checks still gate
    # the result and no data is rewritten from this path.
    for encoding in ("utf-8-sig", "cp949", "cp1252", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ManagedDataValidationError("CSV_ENCODING_INVALID")


def _csv_preview(data: bytes, *, tolerant: bool) -> tuple[list[str], list[dict[str, Any]], tuple[str, ...]]:
    text = _decode_csv(data)
    try:
        reader = csv.reader(io.StringIO(text, newline=""), strict=not tolerant)
        headers = next(reader, None)
        if not headers:
            raise ManagedDataValidationError("CSV_EMPTY")
        raw_headers = [str(item).strip() for item in headers]
        if tolerant:
            # Match pandas' practical handling of the legacy export's blank
            # trailing headings without making that permissiveness available
            # to normal managed CSV contracts.
            seen: dict[str, int] = {}
            headers = []
            for index, value in enumerate(raw_headers):
                base = value or f"Unnamed:{index}"
                count = seen.get(base, 0)
                seen[base] = count + 1
                headers.append(base if count == 0 else f"{base}.{count}")
        else:
            headers = raw_headers
        if not all(headers) or len(set(headers)) != len(headers) or len(headers) > MAX_SOURCE_COLUMNS:
            raise ManagedDataValidationError("CSV_HEADERS_INVALID")
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        if len(headers) > MAX_PREVIEW_COLUMNS:
            warnings.append("PREVIEW_COLUMNS_TRUNCATED")
        for raw in reader:
            if len(rows) >= MAX_PREVIEW_ROWS:
                break
            if len(raw) != len(headers):
                if not tolerant:
                    raise ManagedDataValidationError("CSV_ROW_SHAPE_INVALID")
                warnings.append("TOLERANT_ROW_SHAPE")
            adjusted = (raw + [""] * len(headers))[:len(headers)]
            rows.append(dict(zip(headers, adjusted)))
        return headers, rows, tuple(dict.fromkeys(warnings))
    except csv.Error as exc:
        raise ManagedDataValidationError("CSV_INVALID") from exc


def _xlsx_preview(data: bytes, schema: PreviewSchema) -> list[tuple[str, list[str], list[dict[str, Any]]]]:
    try:
        workbook = pd.ExcelFile(io.BytesIO(data))
    except Exception as exc:
        raise ManagedDataValidationError("XLSX_INVALID") from exc
    if len(workbook.sheet_names) > MAX_PREVIEW_COLUMNS:
        raise ManagedDataValidationError("XLSX_TOO_MANY_SHEETS")
    required_sheet_names = {name for name, _columns in schema.sheet_columns}
    if required_sheet_names and not required_sheet_names.issubset(workbook.sheet_names):
        raise ManagedDataValidationError("XLSX_SHEETS_INVALID")
    selected_sheets: Iterable[str] = (
        tuple(name for name, _columns in schema.sheet_columns)
        if schema.sheet_columns else (workbook.sheet_names[0],)
    )
    result: list[tuple[str, list[str], list[dict[str, Any]]]] = []
    for sheet_name in selected_sheets:
        try:
            frame = pd.read_excel(io.BytesIO(data), sheet_name=sheet_name, nrows=MAX_PREVIEW_ROWS)
        except Exception as exc:
            raise ManagedDataValidationError("XLSX_INVALID") from exc
        headers = [str(column).strip() for column in frame.columns]
        if not headers or len(headers) > MAX_SOURCE_COLUMNS or len(set(headers)) != len(headers):
            raise ManagedDataValidationError("XLSX_HEADERS_INVALID")
        result.append((sheet_name, headers, frame.to_dict(orient="records")))
    return result


def _validate_schema(headers: Iterable[str], required: Iterable[str], *, allow_heavy_source: bool = False) -> None:
    actual = set(headers)
    expected = set(required)
    if allow_heavy_source:
        if set(_CANONICAL_HEAVY_COLUMNS).issubset(actual) or set(_HEAVY_SOURCE_COLUMNS).issubset(actual):
            return
    if not expected.issubset(actual):
        raise ManagedDataValidationError("SCHEMA_REQUIRED_COLUMNS_MISSING")


def _safe_cell(column: str, value: Any, pii_columns: set[str]) -> str:
    if column in pii_columns or _SECRET_COLUMN_RE.search(column):
        return "[REDACTED]"
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ")
    if _SECRET_VALUE_RE.search(text):
        return "[REDACTED]"
    return text[:MAX_CELL_CHARACTERS]


def _preview_table(
    name: str | None,
    headers: list[str],
    rows: Iterable[Mapping[str, Any]],
    spec: DatasetSpec,
    warnings: tuple[str, ...] = (),
) -> PreviewTable:
    selected = list(spec.preview_schema.preview_columns) or headers[:MAX_PREVIEW_COLUMNS]
    selected = [column for column in selected if column in headers][:MAX_PREVIEW_COLUMNS]
    pii_columns = set(spec.pii_columns)
    masked = tuple(column for column in selected if column in pii_columns or _SECRET_COLUMN_RE.search(column))
    safe_rows = tuple(
        {column: _safe_cell(column, row.get(column), pii_columns) for column in selected}
        for row in rows
    )
    return PreviewTable(name, tuple(selected), safe_rows, len(safe_rows), masked, warnings)


def validate_and_preview_bytes(
    dataset_id: str, scope: str, filename: str, data: bytes
) -> UploadPreview:
    """Validate a fixed upload and return a bounded, PII/secret-safe preview.

    This does not stage a file, alter a catalog pointer, or write a database.
    """
    spec = get_dataset_spec(dataset_id, scope)
    extension = _validate_upload(spec, filename, data)
    if extension == ".csv":
        headers, rows, warnings = _csv_preview(data, tolerant=spec.id == "service_raw")
        _validate_schema(headers, spec.preview_schema.required_columns, allow_heavy_source=spec.id == "heavy_repair_rules")
        tables = (_preview_table(None, headers, rows, spec, warnings),)
    else:
        frames = _xlsx_preview(data, spec.preview_schema)
        tables_list: list[PreviewTable] = []
        for sheet_name, headers, rows in frames:
            required = spec.preview_schema.required_for_sheet(sheet_name)
            _validate_schema(headers, required, allow_heavy_source=spec.id == "heavy_repair_rules")
            tables_list.append(_preview_table(sheet_name, headers, rows, spec))
        tables = tuple(tables_list)
    normalization = None
    if spec.id == "heavy_repair_rules":
        normalization = normalize_heavy_repair_rules(filename, data).as_dict()
    return UploadPreview(
        dataset_id=spec.id,
        scope=spec.scope,
        filename=Path(filename).name,
        extension=extension,
        size_bytes=len(data),
        source_sha256=hashlib.sha256(data).hexdigest(),
        tables=tables,
        normalization=normalization,
    )


def validate_managed_data_file(
    *, scope: str, dataset_id: str, file_name: str, file_bytes: bytes
) -> dict[str, Any]:
    """Backend-friendly safe preview contract.

    The response contains neither original bytes nor unmasked PII/secret-like
    values.  ``sample`` is bounded to :data:`MAX_PREVIEW_ROWS` per table and
    consists only of the same masked table payload returned in ``tables``.
    """
    preview = validate_and_preview_bytes(dataset_id, scope, file_name, file_bytes)
    payload = preview.as_dict()
    tables = payload["tables"]
    payload["file_type"] = preview.extension.lstrip(".")
    payload["summary"] = {
        "dataset_id": preview.dataset_id,
        "scope": preview.scope,
        "filename": preview.filename,
        "extension": preview.extension,
        "file_type": preview.extension.lstrip("."),
        "size_bytes": preview.size_bytes,
        "source_sha256": preview.source_sha256,
        "table_count": len(tables),
        "sampled_row_count": sum(int(table["sampled_row_count"]) for table in tables),
        "normalization": payload["normalization"],
    }
    payload["sample"] = tables
    return payload


def _heavy_frame(filename: str, data: bytes) -> pd.DataFrame:
    extension = _extension(filename)
    if extension == ".csv":
        text = _decode_csv(data)
        try:
            return pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)
        except Exception as exc:
            raise ManagedDataValidationError("CSV_INVALID") from exc
    if extension == ".xlsx":
        try:
            return pd.read_excel(io.BytesIO(data), dtype=str)
        except Exception as exc:
            raise ManagedDataValidationError("XLSX_INVALID") from exc
    raise ManagedDataValidationError("FILE_EXTENSION_NOT_ALLOWED")


def normalize_heavy_repair_rules(filename: str, data: bytes) -> HeavyRepairNormalization:
    """Create deterministic three-column UTF-8-SIG CSV with full row accounting.

    Blank keys and duplicate canonical keys are rejected from the output rather
    than silently reaching a DB upsert.  The caller can show the accounting in
    preview and require a clean upload before the DB apply action is enabled.
    """
    spec = get_dataset_spec("heavy_repair_rules", COMMON)
    _validate_upload(spec, filename, data)
    frame = _heavy_frame(filename, data)
    headers = [str(column).strip() for column in frame.columns]
    _validate_schema(headers, _CANONICAL_HEAVY_COLUMNS, allow_heavy_source=True)
    if set(_CANONICAL_HEAVY_COLUMNS).issubset(frame.columns):
        source = frame.loc[:, list(_CANONICAL_HEAVY_COLUMNS)].copy()
    else:
        source = frame.loc[:, list(_HEAVY_SOURCE_COLUMNS)].rename(columns={
            "SERVICE_PRODUCT_GROUP_CODE": "product_group_code",
            "SERVICE_PRODUCT_CODE": "product_code",
            "SYMP_CODE_THREE": "detailed_symptom_code",
        })
    for column in _CANONICAL_HEAVY_COLUMNS:
        source[column] = source[column].fillna("").astype(str).str.strip().str.upper()
    input_rows = int(len(source))
    blank_mask = source.loc[:, list(_CANONICAL_HEAVY_COLUMNS)].eq("").any(axis=1)
    non_blank = source.loc[~blank_mask].copy()
    duplicate_mask = non_blank.duplicated(subset=list(_CANONICAL_HEAVY_COLUMNS), keep="first")
    canonical = non_blank.loc[~duplicate_mask, list(_CANONICAL_HEAVY_COLUMNS)].reset_index(drop=True)
    blank_count = int(blank_mask.sum())
    duplicate_count = int(duplicate_mask.sum())
    rejected_rows = blank_count + duplicate_count
    if input_rows != len(canonical) + rejected_rows:
        raise RuntimeError("HEAVY_REPAIR_ROW_ACCOUNTING_INVALID")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(_CANONICAL_HEAVY_COLUMNS)
    writer.writerows(canonical.itertuples(index=False, name=None))
    canonical_csv = output.getvalue().encode("utf-8-sig")
    return HeavyRepairNormalization(
        canonical_csv=canonical_csv,
        source_sha256=hashlib.sha256(data).hexdigest(),
        canonical_sha256=hashlib.sha256(canonical_csv).hexdigest(),
        input_rows=input_rows,
        accepted_rows=int(len(canonical)),
        rejected_rows=rejected_rows,
        rejected_by_reason={"blank_key": blank_count, "duplicate_key": duplicate_count},
    )


# Short aliases make the intended UI integration obvious without accepting an
# arbitrary upload destination.
preview_upload = validate_and_preview_bytes
normalize_heavy_repair_upload = normalize_heavy_repair_rules
