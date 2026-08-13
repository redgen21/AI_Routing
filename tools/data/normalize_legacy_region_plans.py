"""Normalize legacy reviewed region files into the current local plan layout.

This is a copy-first migration utility.  It never deletes or changes the
legacy source files.  Values that cannot be safely inferred (notably missing
``area_type`` and technician-to-region assignments) are left blank and are
recorded as ``needs_review`` in the plan manifest.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    from smart_routing.data_catalog import PROJECT_ROOT, na_data_path
except ModuleNotFoundError:  # direct ``python tools/data/...`` invocation
    import sys

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(PROJECT_ROOT))
    from smart_routing.data_catalog import PROJECT_ROOT, na_data_path


SCHEMA = "area-map-region-plan-migration/v1"
SUBSIDIARY = "LGEAI"
AREA_COLUMNS = (
    "region_code", "region_name", "region_seq", "postal_code", "area_type",
    "required_center_type", "membership_rank", "is_primary", "overflow_allowed",
    "overflow_penalty_minutes", "overflow_reason",
)
REGION_COLUMNS = (
    "subsidiary_name", "strategic_city_name", "plan_id", "region_id",
    "region_seq", "region_name", "new_region_name", "area_type",
    "required_center_type",
)
POSTAL_COLUMNS = (
    "subsidiary_name", "strategic_city_name", "plan_id", "postal_code",
    "region_id", "region_seq", "area_type", "membership_rank", "is_primary",
    "overflow_allowed", "overflow_penalty_minutes", "overflow_reason",
)
TECH_COLUMNS = (
    "subsidiary_name", "strategic_city_name", "plan_id", "employee_code",
    "employee_name", "region_id", "region_seq", "assignment", "active",
    "policy_mode", "assignment_source",
)
TECH_CANONICAL_COLUMNS = (
    "technician_id", "region_code", "active", "policy_mode", "effective_from",
    "effective_to",
)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "nat", "<na>"} else text


def _header_key(value: Any) -> str:
    return re.sub(r"[ _-]+", " ", _clean(value)).casefold()


def _slug(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", _clean(value)).strip("_").lower() or "city"


def _safe_id(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", _clean(value)).strip("_") or "plan"


def _source_city_folder(city: str) -> str:
    """Group policy variants under one source-roster city directory."""
    base = _clean(city).split(" - ", 1)[0].strip()
    if base.startswith("Atlanta_"):
        base = "Atlanta, GA"
    return _safe_id(base)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = [str(value or "").strip() for value in (reader.fieldnames or [])]
        rows = [
            {_clean(key): _clean(value) for key, value in row.items()}
            for row in reader
        ]
    return fields, rows


def _write_csv(path: Path, fields: Iterable[str], rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(fields), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _clean(row.get(field)) for field in fields})


def _value(row: Mapping[str, Any], *names: str) -> str:
    indexed = {_header_key(key): _clean(value) for key, value in row.items()}
    for name in names:
        value = indexed.get(_header_key(name), "")
        if value:
            return value
    return ""


def _postal(value: Any) -> str:
    text = re.sub(r"\.0+$", "", _clean(value))
    return text.zfill(5) if text.isdigit() and len(text) <= 5 else text


def _sequence(value: Any) -> int | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return int(parsed) if parsed.is_integer() and parsed > 0 else None


def _city_and_target(path: Path, rows: list[dict[str, str]]) -> tuple[str, str]:
    city = next((_value(row, "STRATEGIC_CITY_NAME", "strategic_city_name") for row in rows), "")
    stem = path.stem
    if city == "Atlanta_6area":
        target = "Atlanta_6area_new" if "_new_atl_buckets_" in stem else "Atlanta_6area"
    elif city == "Los Angeles, CA - Bucket Sim Draft":
        target = "Los_Angeles_CA_Bucket_Sim_Draft"
    elif "area_type" in stem and city:
        target = f"{_slug(city)}_Area_Type_Clusters"
    elif city:
        target = city
    else:
        target = stem.removeprefix("fixed_region_postal_")
    return city, _safe_id(target)


def _plan_id(path: Path, target_city: str) -> str:
    # Keep the directory and copied source filename below Windows/UNC path
    # limits.  The short digest preserves uniqueness between candidate files.
    digest = hashlib.sha256(path.name.encode("utf-8")).hexdigest()[:12]
    return _safe_id(f"legacy_{digest}_v1")


def _find_technician_source(
    region_path: Path,
    city: str,
    target_city: str,
    *,
    assignment_path: Path | None,
) -> Path | None:
    # The only reviewed legacy technician-region source currently available is
    # Atlanta's 3-area assignment.  Do not attach it to a six-area artifact or
    # to another city merely because the employee roster exists.
    if (
        assignment_path
        and city == "Atlanta, GA"
        and target_city == "Atlanta_GA"
        and region_path.name == "fixed_region_postal_atlanta_ga_3.csv"
    ):
        return assignment_path if assignment_path.is_file() else None
    return None


def _normalize_plan(
    region_path: Path,
    *,
    subsidiary: str,
    assignment_path: Path | None,
    output_root: Path,
    destination_override: Path | None = None,
    target_city_override: str | None = None,
    plan_id_override: str | None = None,
) -> dict[str, Any]:
    source_fields, source_rows = _read_csv(region_path)
    city, target_city = _city_and_target(region_path, source_rows)
    target_city = target_city_override or target_city
    plan_id = plan_id_override or _plan_id(region_path, target_city)
    tech_path = _find_technician_source(
        region_path, city, target_city, assignment_path=assignment_path
    )
    destination = (destination_override or (output_root / subsidiary / _source_city_folder(city) / plan_id)).resolve()
    root = output_root.resolve()
    if root != destination and root not in destination.parents:
        raise ValueError(f"Migration destination escaped output root: {destination}")
    if destination.exists() and (destination / "manifest.json").exists():
        existing_manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
        if existing_manifest.get("schema") != SCHEMA:
            raise FileExistsError(f"Refusing to overwrite non-migration plan directory: {destination}")

    rejects: list[dict[str, str]] = []
    normalized_rows: list[dict[str, str]] = []
    region_defs: dict[int, dict[str, str]] = {}
    seen_postals: set[str] = set()
    for line_number, row in enumerate(source_rows, 2):
        if not any(_clean(value) for value in row.values()):
            continue
        postal = _postal(_value(row, "POSTAL_CODE", "postal_code", "ZIPCode", "zip code"))
        seq = _sequence(_value(row, "region_seq", "region seq", "assigned_region_seq"))
        region_id = _value(row, "region_id", "region code", "region_code")
        area_name = _value(row, "AREA_NAME", "area name", "Territory", "region_name")
        new_name = _value(row, "new_region_name", "region_name", "new region name") or area_name
        area_type = _value(row, "area_type", "Area Type").upper()
        if not re.fullmatch(r"\d{5}", postal) or seq is None:
            rejects.append({"source_row": str(line_number), "reason": "INVALID_POSTAL_OR_REGION_SEQ"})
            continue
        region_id = region_id or f"{_slug(target_city)}_r{seq:02d}"
        area_name = area_name or f"Region {seq}"
        region_defs.setdefault(seq, {
            "region_id": region_id,
            "region_seq": str(seq),
            "region_name": new_name or area_name,
            "new_region_name": new_name or area_name,
            "area_type": area_type,
            "required_center_type": area_type if area_type in {"DMS", "DMS2"} else "",
        })
        existing = region_defs[seq]
        if existing["region_id"] != region_id or existing["area_type"] != area_type:
            rejects.append({"source_row": str(line_number), "reason": "CONFLICTING_REGION_DEFINITION"})
            continue
        if postal in seen_postals:
            rejects.append({"source_row": str(line_number), "reason": "DUPLICATE_POSTAL_CODE"})
            continue
        seen_postals.add(postal)
        normalized_rows.append({
            "postal_code": postal,
            "region_id": region_id,
            "region_seq": str(seq),
            "area_type": area_type,
            "membership_rank": "1",
            "is_primary": "true",
            "overflow_allowed": "false",
            "overflow_penalty_minutes": "",
            "overflow_reason": "",
        })

    technician_rows: list[dict[str, str]] = []
    tech_fields: list[str] = []
    if tech_path:
        tech_fields, raw_tech_rows = _read_csv(tech_path)
        by_key: dict[str, str] = {}
        for definition in region_defs.values():
            for key in (definition["region_id"], definition["region_name"], definition["new_region_name"], definition["region_seq"]):
                if key:
                    by_key[key.casefold()] = definition["region_id"]
        seen_codes: set[str] = set()
        for line_number, row in enumerate(raw_tech_rows, 2):
            code = _value(row, "Tech ID", "technician_id", "employee_code", "SVC_ENGINEER_CODE", "EMP_NUMBER").upper()
            name = _value(row, "Tech Name", "technician_name", "employee_name", "Name", "SVC_ENGINEER_NAME")
            assignment = _value(row, "Assignment", "region_code", "region_id", "assigned_region_name", "assigned_region_seq")
            region_code = by_key.get(assignment.casefold(), "")
            if not region_code:
                match = re.search(r"(?:zone|region)\s*(\d+)", assignment, re.IGNORECASE)
                if match:
                    region_code = by_key.get(match.group(1), "")
            if not code or not region_code or code in seen_codes:
                rejects.append({"source_row": str(line_number), "reason": "INVALID_TECHNICIAN_ASSIGNMENT"})
                continue
            seen_codes.add(code)
            definition = next(item for item in region_defs.values() if item["region_id"] == region_code)
            technician_rows.append({
                "employee_code": code,
                "employee_name": name,
                "region_id": region_code,
                "region_seq": definition["region_seq"],
                "assignment": assignment,
                "active": "true",
                "policy_mode": "assigned_region_boundary_spillover",
                "assignment_source": str(tech_path),
            })

    missing_area_type = sorted({row["region_seq"] for row in region_defs.values() if row["area_type"] not in {"DMS", "DMS2"}}, key=int)
    expected_seq = list(range(1, max(region_defs, default=0) + 1))
    actual_seq = sorted(region_defs)
    reasons: list[str] = []
    if not normalized_rows:
        reasons.append("REGION_MEMBERSHIP_EMPTY")
    if missing_area_type:
        reasons.append("AREA_TYPE_MISSING_OR_NOT_CANONICAL")
    if actual_seq != expected_seq:
        reasons.append("REGION_SEQ_NOT_CONTIGUOUS")
    if not technician_rows:
        reasons.append("TECHNICIAN_REGION_ASSIGNMENT_MISSING")
    if rejects:
        reasons.append("SOURCE_ROWS_REJECTED")
    status = "ready" if not reasons else "needs_review"

    region_rows = []
    for seq in sorted(region_defs):
        item = region_defs[seq]
        region_rows.append({
            "subsidiary_name": subsidiary, "strategic_city_name": city,
            "plan_id": plan_id, **item,
        })
    postal_rows = [
        {"subsidiary_name": subsidiary, "strategic_city_name": city, "plan_id": plan_id, **row}
        for row in normalized_rows
    ]
    tech_bundle_rows = [
        {"subsidiary_name": subsidiary, "strategic_city_name": city, "plan_id": plan_id, **row}
        for row in technician_rows
    ]
    canonical_area_rows = [
        {
            "region_code": row["region_id"],
            "region_name": region_defs[int(row["region_seq"])]["region_name"],
            "region_seq": row["region_seq"], "postal_code": row["postal_code"],
            "area_type": row["area_type"],
            "required_center_type": region_defs[int(row["region_seq"])]["required_center_type"],
            "membership_rank": row["membership_rank"], "is_primary": row["is_primary"],
            "overflow_allowed": row["overflow_allowed"],
            "overflow_penalty_minutes": row["overflow_penalty_minutes"],
            "overflow_reason": row["overflow_reason"],
        }
        for row in normalized_rows
    ]
    canonical_tech_rows = [
        {"technician_id": row["employee_code"], "region_code": row["region_id"], "active": row["active"],
         "policy_mode": row["policy_mode"], "effective_from": "", "effective_to": ""}
        for row in technician_rows
    ]

    destination.mkdir(parents=True, exist_ok=True)
    (destination / "source").mkdir(exist_ok=True)
    (destination / "normalized").mkdir(exist_ok=True)
    shutil.copy2(region_path, destination / "source" / region_path.name)
    if tech_path:
        shutil.copy2(tech_path, destination / "source" / tech_path.name)
    _write_csv(destination / "normalized" / "regions.csv", REGION_COLUMNS, region_rows)
    _write_csv(destination / "normalized" / "region_postal.csv", POSTAL_COLUMNS, postal_rows)
    _write_csv(destination / "normalized" / "technician_assignments.csv", TECH_COLUMNS, tech_bundle_rows)
    _write_csv(destination / "normalized" / "area.csv", AREA_COLUMNS, canonical_area_rows)
    _write_csv(destination / "normalized" / "technician.csv", TECH_CANONICAL_COLUMNS, canonical_tech_rows)
    _write_csv(destination / "rejects.csv", ("source_row", "reason"), rejects)
    pending_canonical = {
        "schema": "region-plan-canonical-pending/v1",
        "status": "needs_review",
        "reason": "Legacy source normalization is complete, but this bundle is not importable until all review reasons are resolved.",
        "plan_id": plan_id,
        "source_strategic_city_name": city,
        "target_city_id": target_city,
        "normalized_artifacts": [
            "normalized/regions.csv", "normalized/region_postal.csv",
            "normalized/technician_assignments.csv", "normalized/area.csv",
            "normalized/technician.csv",
        ],
        "needs_review_reasons": reasons,
    }
    (destination / "canonical.json").write_text(
        json.dumps(pending_canonical, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (destination / "rejects.jsonl").open("w", encoding="utf-8", newline="") as stream:
        for reject in rejects:
            stream.write(json.dumps(reject, ensure_ascii=False, sort_keys=True) + "\n")

    manifest = {
        "schema": SCHEMA,
        "layout": "data/region_plans/<subsidiary>/<source_city>/<plan_id>/",
        "status": status,
        "lifecycle_stage": "migration_candidate",
        "legacy_source": True,
        "subsidiary_name": subsidiary,
        "source_strategic_city_name": city,
        "strategic_city_name": target_city,
        "target_city_id": target_city,
        "plan_id": plan_id,
        "source": {
            "region_file": str(region_path),
            "region_sha256": _sha256(region_path),
            "technician_file": str(tech_path) if tech_path else None,
            "technician_sha256": _sha256(tech_path) if tech_path else None,
        },
        "row_accounting": {
            "source_region_rows": len(source_rows), "accepted_region_rows": len(normalized_rows),
            "regions": len(region_rows), "postal_memberships": len(postal_rows),
            "technician_assignments": len(tech_bundle_rows), "rejects": len(rejects),
        },
        "quality": {
            "needs_review_reasons": reasons,
            "missing_or_noncanonical_area_type_region_seq": missing_area_type,
            "technician_assignment_source_present": bool(tech_path),
            "original_fields": source_fields,
            "original_area_type_column_present": any(_header_key(field) == "area type" for field in source_fields),
        },
        "artifacts": {
            "regions": "normalized/regions.csv", "region_postal": "normalized/region_postal.csv",
            "technician_assignments": "normalized/technician_assignments.csv",
            "area": "normalized/area.csv", "technician": "normalized/technician.csv",
            "canonical": "canonical.json", "rejects": "rejects.csv", "rejects_jsonl": "rejects.jsonl",
        },
    }
    manifest_bytes = (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    (destination / "manifest.json").write_bytes(manifest_bytes)
    checksums = {"manifest.json": hashlib.sha256(manifest_bytes).hexdigest()}
    for relative in (
        "normalized/regions.csv", "normalized/region_postal.csv", "normalized/technician_assignments.csv",
        "normalized/area.csv", "normalized/technician.csv", "canonical.json", "rejects.csv", "rejects.jsonl",
    ):
        checksums[relative] = _sha256(destination / relative)
    (destination / "checksums.json").write_text(json.dumps(checksums, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {**manifest, "path": str(destination)}


def normalize_all(*, output_root: Path, subsidiary: str = SUBSIDIARY) -> dict[str, Any]:
    region_dir = na_data_path("reviewed_regions_dir")
    assignment_path = na_data_path("atlanta_engineer_region")
    paths = sorted(region_dir.glob("fixed_region_postal_*.csv"))
    if not paths:
        raise FileNotFoundError(f"No reviewed region files found: {region_dir}")
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    plans: list[dict[str, Any]] = []
    existing_by_source_key: dict[tuple[str, str], dict[str, Any]] = {}
    for manifest_path in output_root.glob("*/*/*/manifest.json"):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            source_sha = _clean(((manifest.get("source") or {}).get("region_sha256")))
            target_city = _clean(manifest.get("target_city_id") or manifest.get("strategic_city_name"))
            if source_sha and target_city:
                manifest["path"] = str(manifest_path.parent)
                existing_by_source_key[(source_sha, target_city)] = manifest
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    processed_source_keys: set[tuple[str, str]] = set()
    for path in paths:
        _fields, source_rows = _read_csv(path)
        _city, target_city = _city_and_target(path, source_rows)
        source_sha = _sha256(path)
        source_key = (source_sha, target_city)
        if source_key in processed_source_keys:
            continue
        processed_source_keys.add(source_key)
        existing_manifest = existing_by_source_key.get(source_key)
        if existing_manifest:
            plans.append(_normalize_plan(
                path, subsidiary=subsidiary, assignment_path=assignment_path, output_root=output_root,
                destination_override=Path(existing_manifest["path"]),
                target_city_override=_clean(existing_manifest.get("target_city_id") or existing_manifest.get("strategic_city_name")),
                plan_id_override=_clean(existing_manifest.get("plan_id")),
            ))
        else:
            plans.append(_normalize_plan(
                path, subsidiary=subsidiary, assignment_path=assignment_path, output_root=output_root
            ))
    inventory = {
        "schema": f"{SCHEMA}/inventory",
        "output_root": str(output_root),
        "source_root": str(region_dir),
        "source_files": len(paths),
        "ready_count": sum(item["status"] == "ready" for item in plans),
        "needs_review_count": sum(item["status"] != "ready" for item in plans),
        "plans": plans,
        "copy_only": True,
        "legacy_sources_deleted": False,
    }
    (output_root / "migration_inventory.json").write_text(json.dumps(inventory, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return inventory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=PROJECT_ROOT / "data" / "region_plans")
    parser.add_argument("--subsidiary", default=SUBSIDIARY)
    args = parser.parse_args()
    result = normalize_all(output_root=args.output_root, subsidiary=args.subsidiary)
    print(json.dumps({
        "output_root": result["output_root"], "source_files": result["source_files"],
        "ready_count": result["ready_count"], "needs_review_count": result["needs_review_count"],
        "legacy_sources_deleted": result["legacy_sources_deleted"],
        "plans": [{"city": item["strategic_city_name"], "plan_id": item["plan_id"], "status": item["status"], "path": item["path"]} for item in result["plans"]],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
