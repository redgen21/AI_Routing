from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from smart_routing.data_catalog import PROJECT_ROOT, load_na_data_catalog


LEGACY_FILES = {
    "service_raw": Path(
        "260310/_SELECT_T21_CORP_STD1_NAME_AS_SUBSIDIARY_NAME_"
        "T19_STRATEGIC_CITY_202607101026.csv"
    ),
    "service_geocoded": Path("260310/input/Service_202603181109_geocoded.csv"),
    "profile_raw": Path("260310/Top 10_DMS_DMS2_Profile_20260317.xlsx"),
    "profile_production": Path(
        "260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx"
    ),
    "client_master": Path("data/All_In_One_Master.xlsx"),
    "zcta_geometry": Path("data/geo/tl_2024_us_zcta520.zip"),
    "symptom_mapping": Path("data/Notification_Symptom_mapping_20241120_3depth.xlsx"),
    "heavy_repair_lookup": Path("260310/production_input/atlanta_heavy_repair_lookup.csv"),
    "technician_list": Path("260310/Top10 City Tech List.xlsx"),
    "atlanta_engineer_region": Path("260310/production_input/atlanta_engineer_region_assignment.csv"),
    "atlanta_engineer_home": Path("260310/production_input/atlanta_engineer_home_geocoded.csv"),
}
LEGACY_REVIEWED_REGION_DIR = Path("260310/input/fixed_region_maps")
ATLANTA_REVIEWED_NAME = "fixed_region_postal_atlanta_ga_3.csv"
ATLANTA_LEGACY_SEED = Path("260310/production_input/atlanta_fixed_region_zip_3.csv")
REGION_SEED_NAMES = (
    "los_angeles_fixed_region_zip_6_area_type.csv",
    "los_angeles_area_type_clusters_region_seed.csv",
    "los_angeles_bucket_sim_draft_region_seed.csv",
    "north_jersey_nj_fixed_region_zip_5_area_type.csv",
    "philadelphia_pa_fixed_region_zip_3_area_type.csv",
    "san_diego_ca_fixed_region_zip_3_area_type.csv",
    "washington_dc_fixed_region_zip_3_area_type.csv",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_verified(source: Path, target: Path, *, dry_run: bool) -> dict[str, object]:
    if not source.is_file():
        raise FileNotFoundError(source)
    source_hash = _sha256(source)
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists() or _sha256(target) != source_hash:
            shutil.copy2(source, target)
        target_hash = _sha256(target)
        if target_hash != source_hash:
            raise RuntimeError(f"Hash mismatch after copy: {source} -> {target}")
    return {
        "source": str(source.relative_to(PROJECT_ROOT)),
        "target": str(target.relative_to(PROJECT_ROOT)),
        "size_bytes": source.stat().st_size,
        "sha256": source_hash,
    }


def _build_minimized_technician_map(source: Path, target: Path, *, dry_run: bool) -> dict[str, object]:
    """Create the server map list without home-address or ZIP columns."""
    frame = pd.read_excel(source, dtype={"EMP_NUMBER": str})
    columns = ["Tech Market", "EMP_NUMBER", "Tech Name", "ASM", "RSM"]
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Technician map source is missing columns: {missing}")
    minimized = frame[columns].copy()
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        minimized.to_excel(target, index=False)
    return {
        "source": str(source.relative_to(PROJECT_ROOT)),
        "target": str(target.relative_to(PROJECT_ROOT)),
        "transformation": "remove_home_address_and_home_zip_for_server_map",
        "columns": columns,
        "row_count": int(len(minimized)),
        "source_sha256": _sha256(source),
        "sha256": _sha256(target) if not dry_run else None,
    }


def _build_atlanta_seed_from_reviewed(source: Path, target: Path, *, dry_run: bool) -> dict[str, object]:
    frame = pd.read_csv(source, encoding="utf-8-sig", low_memory=False)
    required = {"POSTAL_CODE", "region_id", "region_seq"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Atlanta reviewed region is missing seed columns: {missing}")
    seed = frame[["POSTAL_CODE", "region_id", "region_seq"]].copy()
    seed["POSTAL_CODE"] = (
        seed["POSTAL_CODE"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(5)
    )
    seed["region_seq"] = pd.to_numeric(seed["region_seq"], errors="raise").astype(int)
    seed["new_region_name"] = seed["region_seq"].map(lambda value: f"Atlanta New Region {value}")
    seed["area_type"] = ""
    seed = seed.drop_duplicates(subset=["POSTAL_CODE"]).sort_values("POSTAL_CODE").reset_index(drop=True)
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        seed.to_csv(target, index=False, encoding="utf-8-sig")
    return {
        "source": str(source.relative_to(PROJECT_ROOT)),
        "target": str(target.relative_to(PROJECT_ROOT)),
        "transformation": "reviewed_region_to_db_seed",
        "row_count": int(len(seed)),
        "source_sha256": _sha256(source),
        "sha256": _sha256(target) if not dry_run else None,
    }


def _build_atlanta_reviewed_with_full_coverage(
    source: Path,
    service_file: Path,
    legacy_seed_file: Path,
    target: Path,
    *,
    dry_run: bool,
) -> dict[str, object]:
    reviewed = pd.read_csv(source, encoding="utf-8-sig", low_memory=False)
    service = pd.read_csv(service_file, encoding="utf-8-sig", low_memory=False)
    service = service[
        service["STRATEGIC_CITY_NAME"].astype(str).str.strip().eq("Atlanta, GA")
    ].copy()
    legacy_seed = pd.read_csv(legacy_seed_file, encoding="utf-8-sig", low_memory=False)
    for frame in (reviewed, service, legacy_seed):
        frame["POSTAL_CODE"] = (
            frame["POSTAL_CODE"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(5)
        )

    service_postals = set(service["POSTAL_CODE"].dropna())
    reviewed_postals = set(reviewed["POSTAL_CODE"].dropna())
    missing_postals = sorted(service_postals - reviewed_postals)
    seed_lookup = legacy_seed.drop_duplicates("POSTAL_CODE").set_index("POSTAL_CODE")
    missing_seed = sorted(set(missing_postals) - set(seed_lookup.index))
    if missing_seed:
        raise ValueError(f"Atlanta service ZIPs have no approved fallback assignment: {missing_seed}")

    receipt_column = "GSFS_RECEIPT_NO" if "GSFS_RECEIPT_NO" in service.columns else None
    service["latitude"] = pd.to_numeric(service.get("latitude"), errors="coerce")
    service["longitude"] = pd.to_numeric(service.get("longitude"), errors="coerce")
    grouped = service.groupby("POSTAL_CODE", dropna=False).agg(
        service_count=(receipt_column, "nunique") if receipt_column else ("POSTAL_CODE", "size"),
        latitude=("latitude", "mean"),
        longitude=("longitude", "mean"),
    )
    baseline_value = (
        str(reviewed["baseline_service_file"].dropna().iloc[0])
        if "baseline_service_file" in reviewed.columns and not reviewed["baseline_service_file"].dropna().empty
        else service_file.name
    )
    extension_rows: list[dict[str, object]] = []
    for postal_code in missing_postals:
        region_seq = int(seed_lookup.loc[postal_code, "region_seq"])
        stats = grouped.loc[postal_code]
        extension_rows.append(
            {
                "baseline_service_file": baseline_value,
                "STRATEGIC_CITY_NAME": "Atlanta, GA",
                "candidate_region_count": 3,
                "POSTAL_CODE": postal_code,
                "region_id": f"atlanta_ga_r{region_seq:02d}",
                "region_seq": region_seq,
                "AREA_NAME": f"Region {region_seq}",
                "service_count": int(stats["service_count"]),
                "latitude": float(stats["latitude"]),
                "longitude": float(stats["longitude"]),
            }
        )
    extended = pd.concat([reviewed, pd.DataFrame(extension_rows)], ignore_index=True)
    extended = extended.drop_duplicates("POSTAL_CODE").sort_values("POSTAL_CODE").reset_index(drop=True)
    uncovered = sorted(service_postals - set(extended["POSTAL_CODE"]))
    if uncovered:
        raise RuntimeError(f"Atlanta reviewed region coverage is incomplete after extension: {uncovered}")
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        extended.to_csv(target, index=False, encoding="utf-8-sig")
    return {
        "source": str(source.relative_to(PROJECT_ROOT)),
        "target": str(target.relative_to(PROJECT_ROOT)),
        "transformation": "preserve_reviewed_and_extend_missing_service_postals_from_legacy_seed",
        "row_count": int(len(extended)),
        "service_postal_count": int(len(service_postals)),
        "added_postal_count": int(len(missing_postals)),
        "added_postals": missing_postals,
        "missing_service_postal_count": 0,
        "source_sha256": _sha256(source),
        "service_sha256": _sha256(service_file),
        "fallback_seed_sha256": _sha256(legacy_seed_file),
        "sha256": _sha256(target) if not dry_run else None,
    }


def migrate(*, dry_run: bool = False) -> dict[str, object]:
    catalog = load_na_data_catalog()
    copied: list[dict[str, object]] = []
    for role, legacy_relative in LEGACY_FILES.items():
        copied.append(
            _copy_verified(
                PROJECT_ROOT / legacy_relative,
                catalog.resolve(role),
                dry_run=dry_run,
            )
        )
    copied.append(
        _build_minimized_technician_map(
            catalog.resolve("technician_list"),
            catalog.resolve("technician_map"),
            dry_run=dry_run,
        )
    )

    reviewed_dir = catalog.resolve("reviewed_regions_dir")
    for source in sorted((PROJECT_ROOT / LEGACY_REVIEWED_REGION_DIR).glob("*.csv")):
        if source.name == ATLANTA_REVIEWED_NAME:
            continue
        copied.append(_copy_verified(source, reviewed_dir / source.name, dry_run=dry_run))
    copied.append(
        _build_atlanta_reviewed_with_full_coverage(
            PROJECT_ROOT / LEGACY_REVIEWED_REGION_DIR / ATLANTA_REVIEWED_NAME,
            catalog.resolve("service_geocoded"),
            PROJECT_ROOT / ATLANTA_LEGACY_SEED,
            reviewed_dir / ATLANTA_REVIEWED_NAME,
            dry_run=dry_run,
        )
    )

    seed_dir = catalog.resolve("region_seed_dir")
    atlanta_reviewed_target = reviewed_dir / ATLANTA_REVIEWED_NAME
    if dry_run and not atlanta_reviewed_target.exists():
        copied.append(
            {
                "source": str(atlanta_reviewed_target.relative_to(PROJECT_ROOT)),
                "target": str((seed_dir / "atlanta_fixed_region_zip_3.csv").relative_to(PROJECT_ROOT)),
                "transformation": "reviewed_region_to_db_seed",
                "deferred_until_reviewed_artifact_exists": True,
            }
        )
    else:
        copied.append(
            _build_atlanta_seed_from_reviewed(
                atlanta_reviewed_target,
                seed_dir / "atlanta_fixed_region_zip_3.csv",
                dry_run=dry_run,
            )
        )
    legacy_seed_dir = PROJECT_ROOT / "260310" / "production_input"
    for name in REGION_SEED_NAMES:
        copied.append(_copy_verified(legacy_seed_dir / name, seed_dir / name, dry_run=dry_run))

    for role in (
        "region_candidates_dir",
        "development_runtime_dir",
        "production_runtime_dir",
        "reports_dir",
    ):
        if not dry_run:
            catalog.resolve(role).mkdir(parents=True, exist_ok=True)

    manifest = {
        "schema": "north-america-routing-data-migration/v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "mode": "copy-first-no-delete",
        "catalog": str(catalog.catalog_path.relative_to(PROJECT_ROOT)),
        "files": copied,
    }
    if not dry_run:
        manifest_path = catalog.resolve("migration_manifest")
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy and verify active legacy data into the canonical catalog.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = migrate(dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
