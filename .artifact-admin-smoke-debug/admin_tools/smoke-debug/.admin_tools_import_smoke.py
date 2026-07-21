import importlib
import sys
from pathlib import Path

staging = Path(__file__).resolve().parent
sys.path.insert(0, str(staging))
module_names = (
    "admin_tools.db.master_data_backend",
    "admin_tools.db.common_vrp",
    "admin_tools.db.data_catalog",
    "admin_tools.db.heavy_repair",
    "admin_tools.db.runners.reset_common_vrp_data",
    "admin_tools.db.seeds.build_la_bucket_vrp_inputs",
    "admin_tools.db.seeds.import_asia_technician_centroids",
)
modules = tuple(importlib.import_module(name) for name in module_names)
for module in modules:
    location = Path(module.__file__).resolve()
    if not location.is_relative_to(staging):
        raise RuntimeError(f"package import escaped staging: {module.__name__}")
backend = modules[0]
if backend.CONTRACT_VERSION != "db-admin/v1" or len(backend.TABLE_REGISTRY) != 13:
    raise RuntimeError("packaged master-data contract is invalid")
