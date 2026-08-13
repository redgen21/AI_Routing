"""Group local Region Plan candidates by source roster city.

The plan target (for example ``Atlanta_6area``) remains in each manifest and
plan ID.  Only the filesystem grouping changes to:

    data/region_plans/<subsidiary>/<source_city>/<plan_id>/

The command is dry-run by default and refuses destination collisions.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from shutil import move


def _city_folder(value: object) -> str:
    text = str(value or "").strip().split(" - ", 1)[0].strip()
    if text.startswith("Atlanta_"):
        text = "Atlanta, GA"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("_") or "city"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _rewrite_manifest(plan_dir: Path, source_city: str) -> None:
    manifest_path = plan_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["layout"] = "data/region_plans/<subsidiary>/<source_city>/<plan_id>/"
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    manifest_path.write_bytes(manifest_bytes)
    checksums_path = plan_dir / "checksums.json"
    if checksums_path.is_file():
        checksums = json.loads(checksums_path.read_text(encoding="utf-8"))
        checksums["manifest.json"] = _sha256(manifest_bytes)
        checksums_path.write_text(json.dumps(checksums, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def flatten(root: Path, *, apply: bool) -> list[tuple[Path, Path]]:
    root = root.resolve()
    moves: list[tuple[Path, Path]] = []
    for manifest_path in sorted(root.glob("*/*/*/manifest.json")):
        plan_dir = manifest_path.parent.resolve()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        metadata = manifest.get("city_metadata") if isinstance(manifest.get("city_metadata"), dict) else {}
        source_city = str(
            manifest.get("source_strategic_city_name")
            or metadata.get("source_city_id")
            or manifest.get("target_city_id")
            or plan_dir.parent.name
        ).strip()
        target_city_folder = _city_folder(source_city)
        if plan_dir.parent.name == target_city_folder:
            if apply:
                _rewrite_manifest(plan_dir, source_city)
            continue
        destination = (root / plan_dir.parent.parent.name / target_city_folder / plan_dir.name).resolve()
        if root not in destination.parents:
            raise ValueError(f"destination escaped root: {destination}")
        if destination.exists():
            raise FileExistsError(f"destination already exists: {destination}")
        moves.append((plan_dir, destination))

    if not apply:
        return moves

    for source, destination in moves:
        destination.parent.mkdir(parents=True, exist_ok=True)
        move(str(source), str(destination))
        _rewrite_manifest(destination, str(json.loads((destination / "manifest.json").read_text(encoding="utf-8")).get("source_strategic_city_name") or ""))
    for manifest_path in sorted(root.glob("*/*/*/manifest.json")):
        _rewrite_manifest(manifest_path.parent.resolve(), str(json.loads(manifest_path.read_text(encoding="utf-8")).get("source_strategic_city_name") or ""))
    for city_dir in sorted(root.glob("*/*")):
        if city_dir.is_dir() and not any(city_dir.iterdir()):
            city_dir.rmdir()
    return moves


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("data/region_plans"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    moves = flatten(args.root, apply=args.apply)
    print(f"planned_moves={len(moves)} applied={args.apply}")
    for source, destination in moves:
        print(f"{source} -> {destination}")


if __name__ == "__main__":
    main()
