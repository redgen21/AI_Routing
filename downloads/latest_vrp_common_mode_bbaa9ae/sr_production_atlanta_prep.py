from __future__ import annotations

from smart_routing.production_atlanta import build_atlanta_production_inputs


def main() -> None:
    result = build_atlanta_production_inputs()
    print(f"region_zip_path={result.region_zip_path}")
    print(f"engineer_region_path={result.engineer_region_path}")
    print(f"home_geocode_path={result.home_geocode_path}")
    print(f"heavy_repair_lookup_path={result.heavy_repair_lookup_path}")
    print(f"service_filtered_path={result.service_filtered_path}")
    print(f"service_enriched_path={result.service_enriched_path}")
    print(f"region_workload_summary_path={result.region_workload_summary_path}")
    print(f"profile_copy_path={result.profile_copy_path}")


if __name__ == "__main__":
    main()
