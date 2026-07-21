from __future__ import annotations

import json
import hashlib
import shutil
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path

from smart_routing.data_catalog import load_na_data_catalog


ROOT = Path(__file__).absolute().parents[2]
BUILDER = ROOT / "services" / "deploy" / "build_server_data_package.ps1"


class ServerDataPackageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.shell = shutil.which("powershell") or shutil.which("pwsh")
        if not self.shell:
            self.skipTest("PowerShell is required for the server-data package contract")

    def _fixture(self, base: Path) -> Path:
        data_root = base / "north_america"
        active = {
            "service_geocoded": "processed/service/service.csv",
            "profile_production": "processed/profile/profile.xlsx",
            "client_master": "reference/client/master.xlsx",
            "zcta_geometry": "reference/geospatial/zcta.zip",
            "symptom_mapping": "reference/lookups/symptom.xlsx",
            "heavy_repair_lookup": "db_input/lookups/heavy.csv",
            "technician_map": "processed/technicians/tech_map.xlsx",
            "atlanta_engineer_region": "db_input/technicians/region.csv",
            "atlanta_engineer_home": "db_input/technicians/home.csv",
            "reviewed_regions_dir": "reviewed/regions",
            "region_seed_dir": "db_input/regions",
            "region_candidates_dir": "planning/regions/candidates",
            "reports_dir": "reports",
        }
        for relative in active.values():
            path = data_root / relative
            if path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(f"fixture:{relative}".encode())
        (data_root / "reviewed/regions").mkdir(parents=True, exist_ok=True)
        (data_root / "db_input/regions").mkdir(parents=True, exist_ok=True)
        (data_root / "reviewed/regions/region.csv").write_text("postal,region\n1,1\n", encoding="utf-8")
        (data_root / "db_input/regions/seed.csv").write_text("postal,region\n1,1\n", encoding="utf-8")
        catalog = base / "catalog.json"
        catalog.write_text(
            json.dumps(
                {
                    "schema": "north-america-routing-data-catalog/v1",
                    "data_root": data_root.relative_to(ROOT).as_posix(),
                    "active": active,
                    "region_plans": {},
                }
            ),
            encoding="utf-8",
        )
        return catalog

    def _run(self, catalog: Path, output: Path, *, acknowledge: bool = True) -> subprocess.CompletedProcess[str]:
        command = [
            self.shell,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            "services/deploy/build_server_data_package.ps1",
            "-Version",
            "unittest",
            "-CatalogPath",
            str(catalog),
            "-OutputDir",
            output.relative_to(ROOT).as_posix(),
            "-SkipArchive",
        ]
        if acknowledge:
            command.append("-AcknowledgeSensitiveData")
        return subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def test_builds_environment_catalogs_and_checksum_manifest(self) -> None:
        data_parent = ROOT / "data"
        output = ROOT / "deployment" / "server_data" / f"_server_data_test_{uuid.uuid4().hex}"
        try:
            with tempfile.TemporaryDirectory(dir=data_parent) as directory:
                catalog = self._fixture(Path(directory))
                result = self._run(catalog, output)
                self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
                package = output / "unittest"
                manifest = json.loads((package / "manifest.json").read_text(encoding="utf-8"))
                self.assertTrue(manifest["contains_restricted_personal_data"])
                self.assertNotIn("raw/profile", json.dumps(manifest))
                self.assertNotIn("raw/technicians", json.dumps(manifest))
                for environment in ("development", "production"):
                    server_catalog = json.loads(
                        (package / f"shared/config/data_catalog.{environment}.json").read_text(encoding="utf-8")
                    )
                    self.assertEqual(server_catalog["state_root"], f"/home/csda/AI_Routing/state/{environment}")
                    self.assertEqual(
                        server_catalog["active"]["profile_runtime"],
                        server_catalog["active"]["profile_production"],
                    )
                    self.assertNotIn("profile_raw", server_catalog["active"])
                    self.assertNotIn("technician_list", server_catalog["active"])
                    loaded_catalog = load_na_data_catalog(
                        package / f"shared/config/data_catalog.{environment}.json"
                    )
                    for role in server_catalog["active"]:
                        loaded_catalog.resolve(role)
                self.assertTrue(all("sha256" in item for item in manifest["files"]))
                for item in manifest["files"]:
                    digest = hashlib.sha256((package / item["path"]).read_bytes()).hexdigest()
                    self.assertEqual(digest, item["sha256"])
        finally:
            shutil.rmtree(output, ignore_errors=True)

    def test_rejects_missing_sensitive_data_acknowledgement_and_role_escape(self) -> None:
        data_parent = ROOT / "data"
        output = ROOT / "deployment" / "server_data" / f"_server_data_test_{uuid.uuid4().hex}"
        try:
            with tempfile.TemporaryDirectory(dir=data_parent) as directory:
                catalog = self._fixture(Path(directory))
                result = self._run(catalog, output, acknowledge=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("AcknowledgeSensitiveData", result.stderr + result.stdout)

                outside_output = Path(directory) / "outside_deployment"
                result = self._run(catalog, outside_output)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must stay under", result.stderr + result.stdout)

                payload = json.loads(catalog.read_text(encoding="utf-8"))
                payload["active"]["service_geocoded"] = "../outside.csv"
                catalog.write_text(json.dumps(payload), encoding="utf-8")
                result = self._run(catalog, output)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("escapes data_root", result.stderr + result.stdout)
        finally:
            shutil.rmtree(output, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
