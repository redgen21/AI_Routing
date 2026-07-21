from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from smart_routing.data_catalog import load_na_data_catalog


REQUIRED_COLUMNS = {"POSTAL_CODE", "region_id", "region_seq"}
REQUIRED_EVALUATION_CHECKS = {
    "coverage_complete": True,
    "duplicate_postal_count": 0,
    "empty_region_count": 0,
    "fixed_boundaries_preserved": True,
    "routing_evaluated": True,
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_postal(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(5)


def validate_region_plan(
    candidate: Path,
    *,
    service_file: Path | None = None,
    city: str | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    frame = pd.read_csv(candidate, encoding="utf-8-sig", low_memory=False)
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"Region candidate is missing columns: {missing}")
    frame = frame.copy()
    frame["POSTAL_CODE"] = _normalize_postal(frame["POSTAL_CODE"])
    frame["region_id"] = frame["region_id"].fillna("").astype(str).str.strip()
    frame["region_seq"] = pd.to_numeric(frame["region_seq"], errors="coerce")
    if frame["POSTAL_CODE"].eq("").any() or frame["region_id"].eq("").any() or frame["region_seq"].isna().any():
        raise ValueError("Region candidate contains blank postal codes or region IDs.")
    duplicates = frame[frame["POSTAL_CODE"].duplicated(keep=False)]["POSTAL_CODE"].unique().tolist()
    if duplicates:
        raise ValueError(f"Postal codes belong to more than one region: {duplicates[:20]}")

    missing_postals: list[str] = []
    expected_postal_count = None
    if service_file is not None:
        service_df = pd.read_csv(service_file, encoding="utf-8-sig", usecols=["STRATEGIC_CITY_NAME", "POSTAL_CODE"])
        if city:
            service_df = service_df[service_df["STRATEGIC_CITY_NAME"].astype(str).str.strip().eq(city)].copy()
        service_postals = set(_normalize_postal(service_df["POSTAL_CODE"].dropna()))
        plan_postals = set(frame["POSTAL_CODE"])
        missing_postals = sorted(service_postals - plan_postals)
        expected_postal_count = len(service_postals)
        if missing_postals:
            raise ValueError(f"Region candidate does not cover service postal codes: {missing_postals[:20]}")

    metrics: dict[str, object] = {
        "row_count": int(len(frame)),
        "postal_count": int(frame["POSTAL_CODE"].nunique()),
        "region_count": int(frame["region_seq"].nunique()),
        "duplicate_postal_count": 0,
        "missing_service_postal_count": len(missing_postals),
        "expected_service_postal_count": expected_postal_count,
    }
    return frame, metrics


def _load_approved_evaluation(path: Path, candidate_sha256: str) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "north-america-region-evaluation/v1":
        raise ValueError("Unsupported or missing region evaluation schema.")
    if payload.get("status") != "passed":
        raise ValueError("Region evaluation status must be 'passed'.")
    if payload.get("candidate_sha256") != candidate_sha256:
        raise ValueError("Region evaluation does not match the candidate checksum.")
    checks = payload.get("checks") or {}
    failed = {
        key: {"expected": expected, "actual": checks.get(key)}
        for key, expected in REQUIRED_EVALUATION_CHECKS.items()
        if checks.get(key) != expected
    }
    if failed:
        raise ValueError(f"Region evaluation gates did not pass: {failed}")
    return payload


def _assert_replace_allowed(path: Path, expected_sha256: str | None) -> None:
    if not path.exists():
        return
    if not expected_sha256:
        raise FileExistsError(f"Refusing to overwrite existing region artifact without checksum: {path}")
    actual = _sha256(path)
    if actual != expected_sha256:
        raise RuntimeError(f"Region artifact changed since approval: {path} ({actual})")


def _atomic_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=f".{target.name}.", delete=False) as stream:
        temporary = Path(stream.name)
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_csv(frame: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".csv",
        delete=False,
    ) as stream:
        temporary = Path(stream.name)
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8-sig")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_json(payload: dict[str, object], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".json",
        mode="w",
        encoding="utf-8",
        delete=False,
    ) as stream:
        temporary = Path(stream.name)
        json.dump(payload, stream, ensure_ascii=False, indent=2)
    try:
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_bytes(payload: bytes, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=f".{target.name}.", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(payload)
    try:
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def promote(
    candidate: Path,
    reviewed_name: str,
    *,
    plan_id: str,
    evaluation_file: Path,
    approved_by: str,
    approval_reference: str,
    seed_name: str | None = None,
    city: str | None = None,
    service_file: Path | None = None,
    expected_reviewed_sha256: str | None = None,
    expected_seed_sha256: str | None = None,
    expected_manifest_sha256: str | None = None,
) -> dict[str, object]:
    catalog = load_na_data_catalog()
    candidate = candidate.resolve()
    candidate.relative_to(catalog.resolve("region_candidates_dir"))
    frame, metrics = validate_region_plan(candidate, service_file=service_file, city=city)
    candidate_sha256 = _sha256(candidate)
    evaluation_sha256 = _sha256(evaluation_file)
    evaluation = _load_approved_evaluation(evaluation_file, candidate_sha256)
    plan_id = str(plan_id).strip()
    approved_by = str(approved_by).strip()
    approval_reference = str(approval_reference).strip()
    if not plan_id or not approved_by or not approval_reference:
        raise ValueError("plan_id, approved_by, and approval_reference are required.")
    if plan_id not in reviewed_name or (seed_name and plan_id not in seed_name):
        raise ValueError("Reviewed and seed filenames must include the immutable plan_id.")

    reviewed_path = catalog.resolve("reviewed_regions_dir") / Path(reviewed_name).name
    _assert_replace_allowed(reviewed_path, expected_reviewed_sha256)
    seed_path = catalog.resolve("region_seed_dir") / Path(seed_name).name if seed_name else None
    if seed_path:
        _assert_replace_allowed(seed_path, expected_seed_sha256)
    manifest_path = reviewed_path.with_suffix(".review.json")
    _assert_replace_allowed(manifest_path, expected_manifest_sha256)

    seed_frame = None
    if seed_path:
        seed_frame = frame.copy()
        if "new_region_name" not in seed_frame.columns:
            prefix = (city or "Region").split(",", 1)[0]
            seed_frame["new_region_name"] = seed_frame["region_seq"].map(
                lambda value: f"{prefix} New Region {int(value)}"
            )
        if "area_type" not in seed_frame.columns:
            seed_frame["area_type"] = ""

    artifact_paths = [reviewed_path, manifest_path] + ([seed_path] if seed_path else [])
    backups = {path: path.read_bytes() if path.exists() else None for path in artifact_paths}
    try:
        if _sha256(evaluation_file) != evaluation_sha256:
            raise RuntimeError("Evaluation evidence changed during promotion.")
        _atomic_copy(candidate, reviewed_path)
        if candidate_sha256 != _sha256(reviewed_path):
            raise RuntimeError("Reviewed region copy failed checksum verification.")

        if seed_path and seed_frame is not None:
            _atomic_write_csv(seed_frame, seed_path)

        manifest = {
            "schema": "north-america-reviewed-region/v1",
            "plan_id": plan_id,
            "approved_at": datetime.now(timezone.utc).isoformat(),
            "approved_by": approved_by,
            "approval_reference": approval_reference,
            "source_candidate": str(candidate),
            "source_sha256": candidate_sha256,
            "evaluation_file": str(evaluation_file),
            "evaluation_sha256": evaluation_sha256,
            "evaluation": evaluation,
            "reviewed_path": str(reviewed_path),
            "reviewed_sha256": _sha256(reviewed_path),
            "seed_path": str(seed_path) if seed_path else None,
            "seed_sha256": _sha256(seed_path) if seed_path else None,
            "city": city,
            "metrics": metrics,
            "commit_marker": str(manifest_path),
        }
        # The manifest is written last and acts as the bundle commit marker.
        _atomic_write_json(manifest, manifest_path)
        return manifest
    except Exception:
        for path, previous in backups.items():
            if previous is None:
                path.unlink(missing_ok=True)
            else:
                _atomic_write_bytes(previous, path)
        raise


def main() -> None:
    catalog = load_na_data_catalog()
    parser = argparse.ArgumentParser(description="Validate and promote a region candidate.")
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--reviewed-name", required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--evaluation-file", type=Path, required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--approval-reference", required=True)
    parser.add_argument("--seed-name")
    parser.add_argument("--city")
    parser.add_argument("--service-file", type=Path, default=catalog.resolve("service_geocoded"))
    parser.add_argument("--replace-reviewed-sha256")
    parser.add_argument("--replace-seed-sha256")
    parser.add_argument("--replace-manifest-sha256")
    args = parser.parse_args()
    result = promote(
        args.candidate,
        args.reviewed_name,
        plan_id=args.plan_id,
        evaluation_file=args.evaluation_file,
        approved_by=args.approved_by,
        approval_reference=args.approval_reference,
        seed_name=args.seed_name,
        city=args.city,
        service_file=args.service_file,
        expected_reviewed_sha256=args.replace_reviewed_sha256,
        expected_seed_sha256=args.replace_seed_sha256,
        expected_manifest_sha256=args.replace_manifest_sha256,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
