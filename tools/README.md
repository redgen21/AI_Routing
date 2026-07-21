# Tools

`tools/preprocess/` contains source normalization, geocoding, translation, cache-merge, and retry jobs.

`tools/operations/` contains region design, production assignment, comparison,
reporting, and cache-preparation utilities.

Database reset, seed, and master import commands live under `admin_tools/db/`,
not `tools/` or the server runtime package.

Run a moved script as a module from the project root so shared imports and `config/config.json` resolve correctly:

```powershell
py -m tools.preprocess.sr_preprocess_service
py -m tools.operations.sr_region_sweep
```
