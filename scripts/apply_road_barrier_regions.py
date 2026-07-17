from __future__ import annotations

import argparse
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from smart_routing.region_design import _weighted_kmeans


DEFAULT_CITY = "North Jersey, NJ"
DEFAULT_REGION_DIR = Path("260310/input/fixed_region_maps")
DEFAULT_MATRIX_FILE = Path("260310/output/north_jersey_nj_zip_road_barrier_matrix.csv")


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def slugify_city_name(city_name: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(city_name))
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    radius_km = 6371.0
    lat1_r = np.radians(lat1)
    lat2_r = np.radians(lat2)
    dlat = lat2_r - lat1_r
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2.0) ** 2
    return float(2.0 * radius_km * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0))))


def fixed_region_path(region_dir: Path, city_name: str, region_count: int) -> Path:
    return region_dir / f"fixed_region_postal_{slugify_city_name(city_name)}_{int(region_count)}.csv"


def load_base_region(region_dir: Path, city_name: str, source_region_count: int) -> pd.DataFrame:
    path = fixed_region_path(region_dir, city_name, source_region_count)
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path, encoding="utf-8-sig", dtype={"POSTAL_CODE": str}, low_memory=False)
    df["POSTAL_CODE"] = df["POSTAL_CODE"].map(clean_text).str.zfill(5)
    df["STRATEGIC_CITY_NAME"] = df["STRATEGIC_CITY_NAME"].map(clean_text)
    df = df[df["STRATEGIC_CITY_NAME"].eq(city_name)].copy()
    for col in ["service_count", "latitude", "longitude"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["service_count"] = df["service_count"].fillna(0.0)
    return df[df["latitude"].notna() & df["longitude"].notna()].drop_duplicates(subset=["POSTAL_CODE"], keep="first").reset_index(drop=True)


def load_matrix(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype={"from_zip": str, "to_zip": str}, low_memory=False)
    df["from_zip"] = df["from_zip"].map(clean_text).str.zfill(5)
    df["to_zip"] = df["to_zip"].map(clean_text).str.zfill(5)
    for col in ["from_latitude", "from_longitude", "to_latitude", "to_longitude", "haversine_km", "osrm_km", "osrm_min"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "haversine_km" not in df.columns:
        df["haversine_km"] = df.apply(
            lambda row: haversine_km(
                float(row["from_longitude"]),
                float(row["from_latitude"]),
                float(row["to_longitude"]),
                float(row["to_latitude"]),
            ),
            axis=1,
        )
    df["detour_ratio"] = df["osrm_km"] / df["haversine_km"].replace(0, pd.NA)
    df["extra_km"] = df["osrm_km"] - df["haversine_km"]
    return df


class DisjointSet:
    def __init__(self, values: list[str]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, a: str, b: str) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a

    def components(self) -> list[list[str]]:
        groups: dict[str, list[str]] = {}
        for value in self.parent:
            groups.setdefault(self.find(value), []).append(value)
        return list(groups.values())


def build_barrier_components(
    base_df: pd.DataFrame,
    matrix_df: pd.DataFrame,
    detour_ratio_threshold: float,
    extra_km_threshold: float,
) -> list[pd.DataFrame]:
    zips = sorted(base_df["POSTAL_CODE"].astype(str).unique().tolist())
    dsu = DisjointSet(zips)
    usable = matrix_df[
        matrix_df["osrm_status"].astype(str).eq("OK")
        & matrix_df["from_zip"].isin(zips)
        & matrix_df["to_zip"].isin(zips)
    ].copy()
    low_barrier = usable[
        ~(usable["detour_ratio"].ge(detour_ratio_threshold) | usable["extra_km"].ge(extra_km_threshold))
    ].copy()
    for _, row in low_barrier.iterrows():
        dsu.union(str(row["from_zip"]), str(row["to_zip"]))

    components = []
    for zips_in_component in dsu.components():
        component = base_df[base_df["POSTAL_CODE"].isin(zips_in_component)].copy()
        if not component.empty:
            components.append(component)
    return sort_region_parts(components)


def part_metrics(part: pd.DataFrame) -> dict[str, float]:
    service = float(part["service_count"].sum())
    lat_span = float(part["latitude"].max() - part["latitude"].min())
    lon_span = float(part["longitude"].max() - part["longitude"].min())
    return {
        "service": service,
        "postal_count": float(part["POSTAL_CODE"].nunique()),
        "lat_span": lat_span,
        "lon_span": lon_span,
        "span_score": max(lat_span, lon_span),
    }


def split_part(part: pd.DataFrame) -> list[pd.DataFrame]:
    if len(part) <= 1:
        return [part]
    work = part.copy().reset_index(drop=True)
    coords = work[["latitude", "longitude"]].to_numpy(dtype=float)
    weights = work["service_count"].to_numpy(dtype=float)
    weights = np.where(weights > 0, weights, 0.01)
    labels = _weighted_kmeans(coords, weights, 2)
    work["_split_label"] = labels
    pieces = [piece.drop(columns=["_split_label"]).copy() for _, piece in work.groupby("_split_label")]
    return sort_region_parts(pieces)


def sort_region_parts(parts: list[pd.DataFrame]) -> list[pd.DataFrame]:
    def key(part: pd.DataFrame) -> tuple[float, float]:
        return (float(part["longitude"].mean()), float(part["latitude"].mean()))

    return sorted(parts, key=key)


def build_target_parts(components: list[pd.DataFrame], target_count: int) -> list[pd.DataFrame]:
    parts = sort_region_parts([component.copy() for component in components])
    while len(parts) < target_count:
        candidates = []
        for idx, part in enumerate(parts):
            if len(part) <= 1:
                continue
            metrics = part_metrics(part)
            candidates.append(
                (
                    metrics["service"],
                    metrics["span_score"],
                    metrics["postal_count"],
                    idx,
                )
            )
        if not candidates:
            break
        _, _, _, split_idx = sorted(candidates, reverse=True)[0]
        split_pieces = split_part(parts.pop(split_idx))
        parts.extend(split_pieces)
        parts = sort_region_parts(parts)
    return parts[:target_count]


def detached_first(parts: list[pd.DataFrame]) -> list[pd.DataFrame]:
    # Keep the clearly detached southern component first, then order the rest west -> east.
    detached_idx = min(range(len(parts)), key=lambda idx: float(parts[idx]["latitude"].mean()))
    detached = parts[detached_idx]
    others = [part for idx, part in enumerate(parts) if idx != detached_idx]
    return [detached] + sort_region_parts(others)


def write_fixed_region(
    parts: list[pd.DataFrame],
    base_df: pd.DataFrame,
    city_name: str,
    target_count: int,
    region_dir: Path,
) -> Path:
    slug = slugify_city_name(city_name)
    baseline = clean_text(base_df.get("baseline_service_file", pd.Series([""])).dropna().astype(str).iloc[0])
    rows = []
    for seq, part in enumerate(detached_first(parts), start=1):
        piece = part.copy()
        piece["baseline_service_file"] = baseline
        piece["STRATEGIC_CITY_NAME"] = city_name
        piece["candidate_region_count"] = int(target_count)
        piece["region_seq"] = int(seq)
        piece["region_id"] = f"{slug}_r{seq:02d}"
        piece["AREA_NAME"] = f"Region {seq}"
        rows.append(piece)
    output = pd.concat(rows, ignore_index=True)
    keep_cols = [
        "baseline_service_file",
        "STRATEGIC_CITY_NAME",
        "candidate_region_count",
        "POSTAL_CODE",
        "region_id",
        "region_seq",
        "AREA_NAME",
        "service_count",
        "latitude",
        "longitude",
    ]
    output = output[keep_cols].sort_values(["region_seq", "latitude", "longitude", "POSTAL_CODE"]).reset_index(drop=True)
    path = fixed_region_path(region_dir, city_name, target_count)
    output.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def run(args: argparse.Namespace) -> None:
    city_name = str(args.city)
    region_dir = Path(args.region_dir)
    base_df = load_base_region(region_dir, city_name, int(args.source_region_count))
    matrix_df = load_matrix(Path(args.matrix_file))
    components = build_barrier_components(
        base_df,
        matrix_df,
        float(args.detour_ratio_threshold),
        float(args.extra_km_threshold),
    )
    print(f"barrier_components={len(components)}")
    for idx, component in enumerate(detached_first(components), start=1):
        metrics = part_metrics(component)
        print(
            f"component={idx} postals={int(metrics['postal_count'])} "
            f"service={metrics['service']:.2f} "
            f"lat={component['latitude'].min():.3f}..{component['latitude'].max():.3f} "
            f"lon={component['longitude'].min():.3f}..{component['longitude'].max():.3f}"
        )

    for target_count in args.region_counts:
        parts = build_target_parts(components, int(target_count))
        path = write_fixed_region(parts, base_df, city_name, int(target_count), region_dir)
        out = pd.read_csv(path, encoding="utf-8-sig", dtype={"POSTAL_CODE": str})
        summary = (
            out.groupby(["region_seq", "AREA_NAME"])
            .agg(
                postals=("POSTAL_CODE", "nunique"),
                service_count=("service_count", "sum"),
                min_lat=("latitude", "min"),
                max_lat=("latitude", "max"),
                min_lon=("longitude", "min"),
                max_lon=("longitude", "max"),
            )
            .round(2)
            .reset_index()
        )
        print(f"wrote={path}")
        print(summary.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate fixed ZIP regions from OSRM road-barrier components.")
    parser.add_argument("--city", default=DEFAULT_CITY)
    parser.add_argument("--region-counts", type=int, nargs="+", default=[4, 5, 6])
    parser.add_argument("--source-region-count", type=int, default=5)
    parser.add_argument("--region-dir", default=str(DEFAULT_REGION_DIR))
    parser.add_argument("--matrix-file", default=str(DEFAULT_MATRIX_FILE))
    parser.add_argument("--detour-ratio-threshold", type=float, default=2.5)
    parser.add_argument("--extra-km-threshold", type=float, default=12.0)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
