from __future__ import annotations

from smart_routing.prewarm_map_cache import prewarm_all_map_caches


def main() -> None:
    result = prewarm_all_map_caches()
    print(f"city_count={result.city_count}")
    print(f"region_option_count={result.region_option_count}")
    print(f"route_group_count={result.route_group_count}")


if __name__ == "__main__":
    main()
