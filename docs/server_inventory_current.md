# AI Routing server inventory

- Generated (UTC): `2026-07-19T07:57:39.659844+00:00`
- Connection: read-only SFTP; host and account are recorded only in the ignored private JSON report
- Remote root: `<AI_ROUTING_ROOT>`
- Entries: 127 (102 files, 25 directories)
- Password: not recorded

## Top-level summary

| Top-level path | Listed entries |
| --- | ---: |
| `.venv` | 1 |
| `260310` | 32 |
| `__pycache__` | 1 |
| `common_vrp_api.err.log` | 1 |
| `common_vrp_api.out.log` | 1 |
| `common_vrp_client.err.log` | 1 |
| `common_vrp_client.out.log` | 1 |
| `common_vrp_client_server.err.log` | 1 |
| `common_vrp_client_server.out.log` | 1 |
| `config.json` | 1 |
| `config_common_vrp.json` | 1 |
| `data` | 25 |
| `log` | 3 |
| `nohup.out` | 1 |
| `reset_common_vrp_data.py` | 1 |
| `restart_common_vrp_api.sh` | 1 |
| `restart_common_vrp_client_server.sh` | 1 |
| `restart_smart_routing_api.sh` | 1 |
| `scripts` | 8 |
| `smart_routing` | 32 |
| `smart_routing_api.err.log` | 1 |
| `smart_routing_api.out.log` | 1 |
| `sr_common_vrp_api_server.py` | 1 |
| `sr_common_vrp_client.py` | 1 |
| `sr_common_vrp_client_asia.py` | 1 |
| `sr_common_vrp_client_server.py` | 1 |
| `sr_import_asia_technician_centroids_to_common_db.py` | 1 |
| `sr_vrp_api_client.py` | 1 |
| `sr_vrp_api_server.py` | 1 |
| `streamlit.out.log` | 1 |
| `watch_common_vrp_api.sh` | 1 |
| `watch_smart_routing_api.sh` | 1 |

Large runtime directories (`.venv`, `__pycache__`, `common_vrp_api_jobs`, `vrp_api_jobs`) are listed as directories but their children are summarized.

## Files and directories

| Type | Size | Relative path |
| --- | ---: | --- |
| directory | 4096 | `.venv` (contents summarized) |
| directory | 4096 | `260310` |
| file | 14084 | `260310/ATL Three Markets.xlsx` |
| directory | 12288 | `260310/common_vrp_api_jobs` (contents summarized) |
| directory | 4096 | `260310/input` |
| directory | 4096 | `260310/input/fixed_region_maps` |
| file | 2797323 | `260310/input/Service_202605291521_normalized.csv` |
| file | 12347469 | `260310/input/Service_202606271712_asia_here_geocoded.csv` |
| file | 12382257 | `260310/input/Service_202606271712_asia_here_geocoded2.csv` |
| file | 6255457 | `260310/input/Service_202607071543_normalized_geocoded.csv` |
| file | 19780 | `260310/LA Bucket Sim_Draft.xlsx` |
| directory | 4096 | `260310/la bucket test` |
| directory | 4096 | `260310/la bucket test/area_type_clusters` |
| directory | 4096 | `260310/la bucket test/bucket_sim_draft` |
| file | 24139 | `260310/la bucket test/la_bucket_test_file_summary.csv` |
| file | 22390 | `260310/la bucket test/la_scenario_technician_master.csv` |
| directory | 4096 | `260310/production_input` |
| file | 111677 | `260310/production_input/Asia_DMS_Profile_20260627_production.xlsx` |
| file | 3194 | `260310/production_input/atlanta_engineer_home_geocoded.csv` |
| file | 1684 | `260310/production_input/atlanta_engineer_region_assignment.csv` |
| file | 17441 | `260310/production_input/atlanta_fixed_region_zip_3.csv` |
| file | 16151 | `260310/production_input/atlanta_fixed_region_zip_3_manual320.csv` |
| file | 9149 | `260310/production_input/atlanta_heavy_repair_lookup.csv` |
| file | 946259 | `260310/production_input/atlanta_service_enriched.csv` |
| file | 922216 | `260310/production_input/atlanta_service_filtered.csv` |
| file | 130088 | `260310/production_input/los_angeles_area_type_clusters_region_seed.csv` |
| file | 40890 | `260310/production_input/los_angeles_bucket_sim_draft_region_seed.csv` |
| file | 5529 | `260310/production_input/los_angeles_engineer_home_geocoded.csv` |
| file | 21537 | `260310/production_input/los_angeles_fixed_region_zip_6.csv` |
| file | 130088 | `260310/production_input/los_angeles_fixed_region_zip_6_area_type.csv` |
| file | 1370083 | `260310/production_input/Top 10_DMS_DMS2_Profile_20260317_production.xlsx` |
| file | 1524690 | `260310/Top 10_DMS_DMS2_Profile_20260317.xlsx` |
| directory | 20480 | `260310/vrp_api_jobs` (contents summarized) |
| directory | 4096 | `__pycache__` (contents summarized) |
| file | 4642 | `common_vrp_api.err.log` |
| file | 0 | `common_vrp_api.out.log` |
| file | 165 | `common_vrp_client.err.log` |
| file | 255 | `common_vrp_client.out.log` |
| file | 180515 | `common_vrp_client_server.err.log` |
| file | 255 | `common_vrp_client_server.out.log` |
| file | 3518 | `config.json` |
| file | 4687 | `config_common_vrp.json` |
| directory | 4096 | `data` |
| directory | 4096 | `data/_tmp` |
| file | 454129 | `data/All_In_One_Master.xlsx` |
| file | 14453 | `data/atlanta_input_store.parquet` |
| file | 4441 | `data/atlanta_upload_schedule_example.csv` |
| directory | 4096 | `data/cache` |
| directory | 4096 | `data/cache/area_map` |
| directory | 4096 | `data/cache/route_explorer` |
| file | 12094 | `data/common_vrp_job_input.parquet` |
| file | 9658 | `data/common_vrp_technician_input.parquet` |
| directory | 4096 | `data/debug` |
| directory | 4096 | `data/debug/vrp_input_compare` |
| directory | 4096 | `data/exports` |
| file | 117281 | `data/exports/asia_postal_regions.csv` |
| file | 14471 | `data/exports/asia_technician_capabilities.csv` |
| file | 35514 | `data/exports/asia_technician_postal_centroids.csv` |
| directory | 4096 | `data/geo` |
| file | 528806468 | `data/geo/tl_2024_us_zcta520.zip` |
| file | 280097 | `data/geocode_attempted_google.csv` |
| file | 4719 | `data/geocode_attempted_here.csv` |
| file | 733148 | `data/geocode_cache_google.csv` |
| file | 9081 | `data/geocode_cache_here.csv` |
| file | 4525433 | `data/geocode_cache_us_census.csv` |
| file | 166 | `data/geocode_daily_log_us_census.json` |
| file | 21921 | `data/Notification_Symptom_mapping_20241120_3depth.xlsx` |
| directory | 4096 | `log` |
| file | 179 | `log/watch_common.out` |
| file | 188 | `log/watch_smart.out` |
| file | 1310 | `nohup.out` |
| file | 4284 | `reset_common_vrp_data.py` |
| file | 1420 | `restart_common_vrp_api.sh` |
| file | 1344 | `restart_common_vrp_client_server.sh` |
| file | 1454 | `restart_smart_routing_api.sh` |
| directory | 4096 | `scripts` |
| directory | 4096 | `scripts/__pycache__` (contents summarized) |
| file | 15537 | `scripts/analyze_zip_road_barriers.py` |
| file | 11328 | `scripts/apply_road_barrier_regions.py` |
| file | 6803 | `scripts/build_deploy_package.ps1` |
| file | 25037 | `scripts/build_la_bucket_vrp_inputs.py` |
| file | 43156 | `scripts/build_region_area_type_clusters.py` |
| file | 23787 | `scripts/run_la_bucket_routing_report.py` |
| directory | 4096 | `smart_routing` |
| file | 46 | `smart_routing/__init__.py` |
| directory | 4096 | `smart_routing/__pycache__` (contents summarized) |
| file | 61072 | `smart_routing/area_map.py` |
| file | 7758 | `smart_routing/asia_geocode_cleaner.py` |
| file | 3786 | `smart_routing/bigquery_runtime.py` |
| file | 24308 | `smart_routing/census_geocoder.py` |
| file | 19909 | `smart_routing/common_vrp_api_server.py` |
| file | 71399 | `smart_routing/common_vrp_db.py` |
| file | 50506 | `smart_routing/common_vrp_runtime.py` |
| file | 8931 | `smart_routing/export_daily_stats.py` |
| file | 12656 | `smart_routing/google_geocoder.py` |
| file | 18166 | `smart_routing/here_geocoder.py` |
| file | 23405 | `smart_routing/live_atlanta_runtime.py` |
| file | 10395 | `smart_routing/nominatim_geocoder.py` |
| file | 18389 | `smart_routing/osrm_routing.py` |
| file | 3725 | `smart_routing/prewarm_map_cache.py` |
| file | 82009 | `smart_routing/production_assign_atlanta.py` |
| file | 95862 | `smart_routing/production_assign_atlanta_vrp.py` |
| file | 26891 | `smart_routing/production_atlanta.py` |
| file | 7953 | `smart_routing/profile_sync.py` |
| file | 21062 | `smart_routing/region_design.py` |
| file | 13651 | `smart_routing/region_sweep.py` |
| file | 23325 | `smart_routing/routing_compare.py` |
| file | 31790 | `smart_routing/service_preprocess.py` |
| file | 4950 | `smart_routing/us_geocode_cleaner.py` |
| file | 8098 | `smart_routing/vrp_api_client.py` |
| file | 2217 | `smart_routing/vrp_api_common.py` |
| file | 3526 | `smart_routing/vrp_api_server.py` |
| file | 5051 | `smart_routing/vrp_api_service.py` |
| file | 38966 | `smart_routing/vrp_mode_na_general.py` |
| file | 18960 | `smart_routing/vrp_mode_z_weekend.py` |
| file | 929 | `smart_routing_api.err.log` |
| file | 0 | `smart_routing_api.out.log` |
| file | 458 | `sr_common_vrp_api_server.py` |
| file | 174899 | `sr_common_vrp_client.py` |
| file | 194718 | `sr_common_vrp_client_asia.py` |
| file | 206169 | `sr_common_vrp_client_server.py` |
| file | 22179 | `sr_import_asia_technician_centroids_to_common_db.py` |
| file | 75918 | `sr_vrp_api_client.py` |
| file | 454 | `sr_vrp_api_server.py` |
| file | 703 | `streamlit.out.log` |
| file | 1023 | `watch_common_vrp_api.sh` |
| file | 1020 | `watch_smart_routing_api.sh` |
