import importlib
import sys
from pathlib import Path
staging = Path(__file__).resolve().parent
sys.path.insert(0, str(staging))
for name in ('admin_tools.db.migration_runner','admin_tools.db.master_data_backend','admin_tools.db.common_vrp','admin_tools.db.data_catalog','admin_tools.db.heavy_repair','admin_tools.db.runners.reset_common_vrp_data','admin_tools.db.runners.upsert_profile_capabilities','admin_tools.db.seeds.build_la_bucket_vrp_inputs','admin_tools.db.seeds.import_asia_technician_centroids'):
 print('IMPORT', name)
 print(importlib.import_module(name))
