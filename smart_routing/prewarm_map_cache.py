from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .area_map import EXPLORER_CITIES, load_region_count_options, load_route_explorer_data
from .osrm_routing import OSRMConfig, OSRMTripClient


@dataclass
class PrewarmResult:
    city_count: int
    region_option_count: int
    route_group_count: int


def _load_config(config_file: Path) -> dict:
    if not config_file.exists():
        return {}
    return json.loads(config_file.read_text(encoding="utf-8"))


def _build_clients(config_file: Path) -> tuple[dict[str, OSRMTripClient], OSRMTripClient]:
    routing_cfg = _load_config(config_file).get("routing", {})
    distance_backend = str(routing_cfg.get("distance_backend", "osrm")).strip().lower()
    default_client = OSRMTripClient(
        OSRMConfig(
            osrm_url=str(routing_cfg.get("osrm_url", "https://router.project-osrm.org")).rstrip("/"),
            mode="haversine" if distance_backend == "city_osrm_else_haversine" else distance_backend,
            osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
            cache_file=Path(str(routing_cfg.get("osrm_cache_file", "data/cache/osrm_trip_cache.csv"))),
        )
    )
    client_map: dict[str, OSRMTripClient] = {}
    for city_name, city_url in routing_cfg.get("city_osrm_urls", {}).items():
        cache_name = str(city_name).lower().replace(",", "").replace(" ", "_")
        client_map[str(city_name)] = OSRMTripClient(
            OSRMConfig(
                osrm_url=str(city_url).rstrip("/"),
                mode="osrm" if distance_backend == "city_osrm_else_haversine" else distance_backend,
                osrm_profile=str(routing_cfg.get("osrm_profile", "driving")),
                cache_file=Path(f"data/cache/osrm_trip_cache_{cache_name}.csv"),
                fallback_osrm_url=(
                    None
                    if distance_backend == "city_osrm_else_haversine"
                    else str(routing_cfg.get("osrm_url", "https://router.project-osrm.org")).rstrip("/")
                ),
            )
        )
    return client_map, default_client


def _prewarm_route_groups(service_df, client: OSRMTripClient) -> int:
    if service_df.empty:
        return 0
    warmed = 0
    grouped = service_df.groupby(["service_date", "assigned_sm_code"], sort=True)
    for (_, _), group_df in grouped:
        coords = tuple(
            group_df[["longitude", "latitude"]]
            .dropna()
            .drop_duplicates()
            .apply(lambda r: (float(r["longitude"]), float(r["latitude"])), axis=1)
            .tolist()
        )
        if not coords:
            continue
        client.build_ordered_route(coords)
        warmed += 1
    return warmed


def prewarm_all_map_caches(config_file: Path = Path("config.json")) -> PrewarmResult:
    client_map, default_client = _build_clients(config_file)
    city_count = 0
    region_option_count = 0
    route_group_count = 0

    for city_name in EXPLORER_CITIES:
        city_count += 1
        region_counts = [None] + load_region_count_options(city_name)
        for region_count in region_counts:
            explorer_data = load_route_explorer_data(city_name=city_name, region_count=region_count, config_file=config_file)
            client = client_map.get(city_name, default_client)
            route_group_count += _prewarm_route_groups(explorer_data.current_service_df, client)
            route_group_count += _prewarm_route_groups(explorer_data.integrated_service_df, client)
            region_option_count += 1

    return PrewarmResult(
        city_count=city_count,
        region_option_count=region_option_count,
        route_group_count=route_group_count,
    )
