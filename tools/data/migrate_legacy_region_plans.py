"""Build migration bundles for the legacy city/region data.

The legacy runtime stores postal membership and technician master data in
separate tables/files.  This tool creates a read-only, deterministic bundle
which can be reviewed before it is imported as a Region Plan candidate.

It intentionally does not write a database and it never invents a
technician-to-region assignment.  A city without a trustworthy assignment
source is emitted with ``status=needs_review`` and an empty assignment file.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    from smart_routing.data_catalog import PROJECT_ROOT
except ModuleNotFoundError:  # direct ``python tools/data/...`` invocation
    PROJECT_ROOT = Path(__file__).resolve().parents[2]


BUNDLE_SCHEMA = "region-plan-migration-bundle/v1"
INVENTORY_SCHEMA = "region-plan-migration-inventory/v1"
POLICY_MODE = "assigned_region_boundary_spillover"
REGION_FIELDS = (
    "subsidiary_name",
    "strategic_city_name",
    "plan_id",
    "region_id",
    "region_seq",
    "region_name",
    "new_region_name",
    "area_type",
    "required_center_type",
)
POSTAL_FIELDS = (
    "subsidiary_name",
    "strategic_city_name",
    "plan_id",
    "postal_code",
    "region_id",
    "region_seq",
    "area_type",
    "membership_rank",
    "is_primary",
    "overflow_allowed",
    "overflow_penalty_minutes",
    "overflow_reason",
)
TECH_FIELDS = (
    "subsidiary_name",
    "strategic_city_name",
    "plan_id",
    "employee_code",
    "employee_name",
    "region_id",
    "region_seq",
    "assignment",
    "active",
    "policy_mode",
    "assignment_source",
)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", _clean(value)).strip("_").lower()
    return value or "city"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve(path_value: str | Path, *, base: Path = PROJECT_ROOT) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    candidates = (base / path, PROJECT_ROOT / path)
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


def _resolve_catalog_path(path_value: str | Path, catalog_root: Path) -> Path:
    """Resolve both local ``data/north_america`` and server shared paths."""
    raw = Path(path_value)
    if raw.is_absolute():
        return raw
    parts = raw.parts
    if len(parts) >= 2 and parts[0].casefold() == "data" and parts[1].casefold() == "north_america":
        return (catalog_root.joinpath(*parts[2:])).resolve()
    return _resolve(raw, base=catalog_root)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return [{_clean(k): _clean(v) for k, v in row.items()} for row in csv.DictReader(stream)]


def _write_csv(path: Path, fields: Iterable[str], rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(fields), lineterminator="\n")
        writer.writeheader()
        writer.writerows({field: _clean(row.get(field)) for field in fields} for row in rows)


def _first(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = _clean(row.get(name))
        if value:
            return value
    return ""


def _postal(value: Any) -> str:
    value = _clean(value)
    value = re.sub(r"\.0+$", "", value)
    return value.zfill(5) if value.isdigit() and len(value) <= 5 else value


def _seq(value: Any) -> int | None:
    value = _clean(value)
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return int(parsed) if parsed.is_integer() and parsed > 0 else None


def _plan_id(city: str, catalog_plans: Mapping[str, Any]) -> str:
    configured = catalog_plans.get(city)
    if isinstance(configured, Mapping) and _clean(configured.get("plan_id")):
        return _clean(configured["plan_id"])
    return f"legacy_{_slug(city)}_v1"


def _region_source_rows(path: Path, city: str) -> list[dict[str, str]]:
    rows = _read_csv(path)
    if not rows:
        return []
    city_column = next((name for name in ("STRATEGIC_CITY_NAME", "strategic_city_name") if name in rows[0]), None)
    if city_column:
        matching = [row for row in rows if _clean(row.get(city_column)) == city]
        if matching:
            return matching
    return rows


def _parse_assignment_rows(
    path: Path,
    *,
    subsidiary: str,
    city: str,
    plan_id: str,
    regions_by_seq: Mapping[int, Mapping[str, str]],
    source_label: str,
    rejects: list[dict[str, str]],
) -> list[dict[str, str]]:
    rows = _read_csv(path)
    assignments: list[dict[str, str]] = []
    seen: set[str] = set()
    for line_number, row in enumerate(rows, 2):
        employee_code = _first(row, "employee_code", "SVC_ENGINEER_CODE", "Tech ID", "EMP_NUMBER")
        if not employee_code:
            rejects.append({"source": source_label, "source_row": str(line_number), "reason": "MISSING_EMPLOYEE_CODE"})
            continue
        if employee_code in seen:
            rejects.append({"source": source_label, "source_row": str(line_number), "reason": "DUPLICATE_EMPLOYEE_CODE"})
            continue
        region_seq = _seq(_first(row, "region_seq", "assigned_region_seq"))
        assignment = _first(row, "assignment", "Assignment", "assigned_region_name", "AREA_NAME")
        if region_seq is None and assignment:
            for candidate_seq, region in regions_by_seq.items():
                labels = {region["region_name"], region["new_region_name"], region["region_id"]}
                if assignment in labels or assignment.casefold() in {label.casefold() for label in labels}:
                    region_seq = candidate_seq
                    break
            if region_seq is None:
                match = re.search(r"(?:zone|region)\s*(\d+)", assignment, re.IGNORECASE)
                if match:
                    region_seq = _seq(match.group(1))
        region = regions_by_seq.get(region_seq or -1)
        if region is None:
            rejects.append({"source": source_label, "source_row": str(line_number), "reason": "UNKNOWN_REGION_ASSIGNMENT"})
            continue
        seen.add(employee_code)
        assignments.append(
            {
                "subsidiary_name": subsidiary,
                "strategic_city_name": city,
                "plan_id": plan_id,
                "employee_code": employee_code,
                "employee_name": _first(row, "employee_name", "Name", "Tech Name", "SVC_ENGINEER_NAME"),
                "region_id": region["region_id"],
                "region_seq": str(region_seq),
                "assignment": assignment or region["region_name"],
                "active": "true",
                "policy_mode": POLICY_MODE,
                "assignment_source": source_label,
            }
        )
    return assignments


def build_city_bundle(
    *,
    subsidiary: str,
    city: str,
    plan_id: str,
    region_file: Path,
    technician_file: Path | None,
    output_root: Path,
    routing_policy: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Build one legacy city bundle and return its manifest."""
    rejects: list[dict[str, str]] = []
    source_rows = _region_source_rows(region_file, city)
    regions_by_seq: dict[int, dict[str, str]] = {}
    postals: list[dict[str, str]] = []
    postal_seen: set[str] = set()
    for line_number, row in enumerate(source_rows, 2):
        postal_code = _postal(_first(row, "POSTAL_CODE", "postal_code", "ZIPCode"))
        region_seq = _seq(_first(row, "region_seq", "assigned_region_seq"))
        if not re.fullmatch(r"\d{5}", postal_code) or region_seq is None:
            rejects.append({"source": str(region_file), "source_row": str(line_number), "reason": "INVALID_REGION_POSTAL_ROW"})
            continue
        area_type = _first(row, "area_type", "Area Type").upper()
        region_id = _first(row, "region_id", "region_code") or f"{_slug(city)}_r{region_seq:02d}"
        region_name = _first(row, "region_name", "AREA_NAME", "new_region_name") or f"Region {region_seq}"
        new_region_name = _first(row, "new_region_name", "region_name", "AREA_NAME") or region_name
        candidate = {
            "subsidiary_name": subsidiary,
            "strategic_city_name": city,
            "plan_id": plan_id,
            "region_id": region_id,
            "region_seq": str(region_seq),
            "region_name": region_name,
            "new_region_name": new_region_name,
            "area_type": area_type,
            "required_center_type": area_type,
        }
        existing = regions_by_seq.get(region_seq)
        if existing and (existing["region_id"], existing["area_type"]) != (region_id, area_type):
            rejects.append({"source": str(region_file), "source_row": str(line_number), "reason": "CONFLICTING_REGION_DEFINITION"})
            continue
        regions_by_seq[region_seq] = candidate
        if postal_code in postal_seen:
            rejects.append({"source": str(region_file), "source_row": str(line_number), "reason": "DUPLICATE_POSTAL_CODE"})
            continue
        postal_seen.add(postal_code)
        postals.append(
            {
                "subsidiary_name": subsidiary,
                "strategic_city_name": city,
                "plan_id": plan_id,
                "postal_code": postal_code,
                "region_id": region_id,
                "region_seq": str(region_seq),
                "area_type": area_type,
                "membership_rank": "1",
                "is_primary": "true",
                "overflow_allowed": "false",
                "overflow_penalty_minutes": "",
                "overflow_reason": "",
            }
        )

    assignments: list[dict[str, str]] = []
    assignment_source = ""
    if technician_file and technician_file.exists():
        assignment_source = str(technician_file)
        assignments = _parse_assignment_rows(
            technician_file,
            subsidiary=subsidiary,
            city=city,
            plan_id=plan_id,
            regions_by_seq=regions_by_seq,
            source_label=assignment_source,
            rejects=rejects,
        )

    missing_area_type = sorted({row["region_seq"] for row in regions_by_seq.values() if row["area_type"] not in {"DMS", "DMS2"}})
    policy_ready = routing_policy in {"preferred_region_soft", "home_distance_only", "assigned_region_boundary_spillover", ""}
    status = "ready" if regions_by_seq and postals and assignments and not missing_area_type and policy_ready else "needs_review"
    if not technician_file:
        rejects.append({"source": "", "source_row": "", "reason": "TECHNICIAN_REGION_SOURCE_MISSING"})
    if not assignments:
        rejects.append({"source": assignment_source, "source_row": "", "reason": "TECHNICIAN_ASSIGNMENTS_MISSING"})
    if missing_area_type:
        rejects.append({"source": str(region_file), "source_row": "", "reason": "AREA_TYPE_MISSING_OR_INVALID"})

    bundle_dir = output_root / _slug(city) / plan_id
    manifest: dict[str, Any] = {
        "schema": BUNDLE_SCHEMA,
        "lifecycle_stage": "migration_candidate",
        "status": status,
        "legacy_source": True,
        "subsidiary_name": subsidiary,
        "strategic_city_name": city,
        "plan_id": plan_id,
        "routing_policy": routing_policy,
        "region_source": str(region_file),
        "technician_assignment_source": assignment_source or None,
        "assignment_status": "ready" if assignments else "needs_review",
        "row_accounting": {
            "region_source_rows": len(source_rows),
            "regions": len(regions_by_seq),
            "postal_memberships": len(postals),
            "technician_assignments": len(assignments),
            "rejects": len(rejects),
        },
        "quality": {
            "area_type_missing_region_seq": missing_area_type,
            "assignment_source_present": bool(technician_file),
            "routing_policy_requires_review": not policy_ready,
        },
        "artifacts": {
            "regions": "regions.csv",
            "region_postal": "region_postal.csv",
            "technician_assignments": "technician_assignments.csv",
            "rejects": "rejects.csv",
            "manifest": "manifest.json",
        },
    }
    manifest["bundle_path"] = str(bundle_dir)
    if not dry_run:
        bundle_dir.mkdir(parents=True, exist_ok=True)
        _write_csv(bundle_dir / "regions.csv", REGION_FIELDS, sorted(regions_by_seq.values(), key=lambda row: int(row["region_seq"])))
        _write_csv(bundle_dir / "region_postal.csv", POSTAL_FIELDS, sorted(postals, key=lambda row: row["postal_code"]))
        _write_csv(bundle_dir / "technician_assignments.csv", TECH_FIELDS, sorted(assignments, key=lambda row: row["employee_code"]))
        _write_csv(bundle_dir / "rejects.csv", ("source", "source_row", "reason"), rejects)
        for filename in ("regions.csv", "region_postal.csv", "technician_assignments.csv", "rejects.csv"):
            manifest.setdefault("checksums", {})[filename] = _sha256(bundle_dir / filename)
        (bundle_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def bundle_to_workbook_bytes(bundle_dir: Path) -> bytes:
    """Create a compatibility workbook from a standard CSV bundle.

    The CSV bundle remains the source of truth.  This in-memory adapter exists
    only so the current Region Plan v2 API can consume a reviewed bundle while
    the native bundle endpoint is being introduced.
    """
    from io import BytesIO

    import openpyxl

    bundle_dir = bundle_dir.resolve()
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "ready":
        raise ValueError("MIGRATION_BUNDLE_REQUIRES_REVIEW")
    regions = _read_csv(bundle_dir / "regions.csv")
    postals = _read_csv(bundle_dir / "region_postal.csv")
    technicians = _read_csv(bundle_dir / "technician_assignments.csv")
    region_by_id = {row["region_id"]: row for row in regions}
    workbook = openpyxl.Workbook()
    area = workbook.active
    area.title = "Area"
    area.append([
        "region_code", "region_name", "postal_code", "area_type", "required_center_type",
        "membership_rank", "is_primary", "overflow_allowed", "overflow_penalty_minutes", "overflow_reason",
    ])
    for row in postals:
        region = region_by_id.get(row["region_id"], {})
        area.append([
            row["region_id"], region.get("region_name") or row["region_id"], row["postal_code"],
            row["area_type"], region.get("required_center_type") or row["area_type"],
            row["membership_rank"], row["is_primary"], row["overflow_allowed"],
            row["overflow_penalty_minutes"], row["overflow_reason"],
        ])
    tech = workbook.create_sheet("Technician")
    tech.append(["technician_id", "region_code", "active", "policy_mode", "effective_from", "effective_to"])
    for row in technicians:
        tech.append([row["employee_code"], row["region_id"], row["active"], row["policy_mode"], "", ""])
    result = BytesIO()
    workbook.save(result)
    return result.getvalue()


def load_migration_specs(config_path: Path) -> list[dict[str, Any]]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    configured_catalog = os.environ.get("NA_DATA_CATALOG_PATH", "").strip()
    catalog_path = Path(configured_catalog).expanduser().resolve() if configured_catalog else _resolve("config/data_catalog.json")
    catalog = json.loads(catalog_path.read_text(encoding="utf-8")) if catalog_path.exists() else {}
    catalog_plans = catalog.get("region_plans") if isinstance(catalog.get("region_plans"), Mapping) else {}
    active = (catalog.get("active") or {}) if isinstance(catalog, Mapping) else {}
    catalog_root = _resolve(catalog.get("data_root", "data/north_america")) if isinstance(catalog, Mapping) else PROJECT_ROOT
    specs = config.get("region_seed_files") or []
    result: list[dict[str, Any]] = []
    for spec in specs:
        if not isinstance(spec, Mapping):
            continue
        city = _clean(spec.get("strategic_city_name"))
        region_file = _resolve_catalog_path(_clean(spec.get("file")), catalog_root)
        if not city or not _clean(spec.get("file")):
            continue
        plan_id = _plan_id(city, catalog_plans)
        technician_file_value = spec.get("technician_file")
        if not technician_file_value and "atlanta" in city.casefold():
            technician_file_value = active.get("atlanta_engineer_region")
        result.append({
            "subsidiary": _clean(spec.get("subsidiary_name")) or _clean((config.get("defaults") or {}).get("subsidiary_name")) or "LGEAI",
            "city": city,
            "plan_id": plan_id,
            "region_file": region_file,
            "technician_file": _resolve(technician_file_value, base=catalog_root) if technician_file_value else None,
            "routing_policy": ((catalog_plans.get(city) or {}).get("routing_policy", "") if isinstance(catalog_plans.get(city), Mapping) else ""),
        })
    return result


def migrate(config_path: Path, output_root: Path, *, dry_run: bool = False, city: str = "") -> dict[str, Any]:
    specs = load_migration_specs(config_path)
    manifests = []
    for spec in specs:
        if city and spec["city"] != city:
            continue
        if not spec["region_file"].exists():
            manifests.append({
                "status": "needs_review",
                "strategic_city_name": spec["city"],
                "plan_id": spec["plan_id"],
                "region_source": str(spec["region_file"]),
                "reason": "REGION_SOURCE_NOT_FOUND",
            })
            continue
        manifests.append(build_city_bundle(output_root=output_root, dry_run=dry_run, **spec))
    inventory = {
        "schema": INVENTORY_SCHEMA,
        "dry_run": dry_run,
        "city_count": len(manifests),
        "ready_count": sum(item.get("status") == "ready" for item in manifests),
        "needs_review_count": sum(item.get("status") != "ready" for item in manifests),
        "cities": manifests,
    }
    if not dry_run:
        output_root.mkdir(parents=True, exist_ok=True)
        (output_root / "inventory.json").write_text(json.dumps(inventory, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return inventory


def main() -> None:
    parser = argparse.ArgumentParser(description="Create reviewable bundles from legacy region seed files.")
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / "config/common_vrp.dev.template.json")
    parser.add_argument("--output-root", type=Path, default=PROJECT_ROOT / "data/region_plans/legacy_migration")
    parser.add_argument("--city", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(json.dumps(migrate(args.config.resolve(), args.output_root.resolve(), dry_run=args.dry_run, city=args.city), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
