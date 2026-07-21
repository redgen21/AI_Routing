from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from services.api.common_vrp_config import load_and_validate_common_config
from services.api.sr_vrp_api_server import ServiceVRPRequestHandler
from smart_routing.common_vrp_api_server import CommonVRPRequestHandler


ROOT = Path(__file__).resolve().parents[2]


def _leaf_paths(value: object, prefix: str = "") -> set[str]:
    if isinstance(value, dict):
        paths: set[str] = set()
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            paths.update(_leaf_paths(item, path))
        return paths
    if isinstance(value, list):
        if not value:
            return {prefix}
        paths: set[str] = set()
        for item in value:
            paths.update(_leaf_paths(item, f"{prefix}[]"))
        return paths
    return {prefix}


def _config(environment: str = "development") -> dict:
    is_development = environment == "development"
    port = 8066 if is_development else 8065
    return {
        "environment": environment,
        "api": {"host": "0.0.0.0", "port": port},
        "routing_api_url": f"http://127.0.0.1:{port}",
        "database": {
            "host": "localhost",
            "port": 5432,
            "dbname": "vrp_db_dev" if is_development else "vrp_db",
            "user": "vrp_agent",
            "password": "test-only-password",
        },
    }


class ConfigIsolationTests(unittest.TestCase):
    def _write(self, payload: object) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "config.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_accepts_matching_development_config(self) -> None:
        loaded = load_and_validate_common_config(
            self._write(_config()),
            expected_port=8066,
            expected_environment="development",
        )
        self.assertEqual(loaded["database"]["dbname"], "vrp_db_dev")

    def test_rejects_development_config_using_production_database(self) -> None:
        payload = _config()
        payload["database"]["dbname"] = "vrp_db"
        with self.assertRaisesRegex(ValueError, "development must use database vrp_db_dev"):
            load_and_validate_common_config(self._write(payload))

    def test_rejects_port_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "Requested port 8065"):
            load_and_validate_common_config(self._write(_config()), expected_port=8065)

    def test_rejects_template_password(self) -> None:
        payload = _config()
        payload["database"]["password"] = "<REPLACE_ME>"
        with self.assertRaisesRegex(ValueError, "password placeholder"):
            load_and_validate_common_config(self._write(payload))

    def test_rejects_invalid_json(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "config.json"
        path.write_text("{", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Invalid JSON"):
            load_and_validate_common_config(path)


class StaticReleaseContractTests(unittest.TestCase):
    def test_templates_are_valid_json_and_isolated(self) -> None:
        dev = json.loads((ROOT / "config/common_vrp.dev.template.json").read_text(encoding="utf-8"))
        prod = json.loads((ROOT / "config/common_vrp.prod.template.json").read_text(encoding="utf-8"))
        json.loads((ROOT / "config/config.template.json").read_text(encoding="utf-8"))
        self.assertEqual((dev["environment"], dev["api"]["port"], dev["database"]["dbname"]),
                         ("development", 8066, "vrp_db_dev"))
        self.assertEqual((prod["environment"], prod["api"]["port"], prod["database"]["dbname"]),
                         ("production", 8065, "vrp_db"))

    def test_general_config_template_matches_runtime_leaf_contract(self) -> None:
        template_path = ROOT / "config/config.template.json"
        runtime_path = ROOT / "config/config.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))
        if runtime_path.is_file():
            runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
            self.assertSetEqual(_leaf_paths(template), _leaf_paths(runtime))

    def test_general_config_template_has_operational_sections_and_no_secrets(self) -> None:
        template = json.loads((ROOT / "config/config.template.json").read_text(encoding="utf-8"))
        required_paths = {
            "area_map.service_file",
            "area_map.profile_file",
            "area_map_usa.zcta_zip_file",
            "area_map_asia.profile_file",
            "nominatim.url",
            "nominatim.country_codes.BANGKOK",
            "routing.osrm_url",
            "routing.city_osrm_urls.Atlanta, GA",
            "routing.city_osrm_urls.Los Angeles, CA",
        }
        self.assertTrue(required_paths.issubset(_leaf_paths(template)))
        self.assertEqual(template["geocoding"]["google_api_key"], "<REPLACE_ME>")
        self.assertEqual(template["geocoding"]["here_api_key"], "<REPLACE_ME>")
        self.assertEqual(template["llm"]["api_key"], "<REPLACE_ME>")

    def test_systemd_uses_existing_service_entrypoints(self) -> None:
        common_unit = (ROOT / "systemd/common-vrp.service").read_text(encoding="utf-8")
        common_dev_unit = (ROOT / "systemd/common-vrp-dev.service").read_text(encoding="utf-8")
        client_unit = (ROOT / "systemd/common-vrp-client.service").read_text(encoding="utf-8")
        client_dev_unit = (ROOT / "systemd/common-vrp-client-dev.service").read_text(encoding="utf-8")
        smart_unit = (ROOT / "systemd/smart-routing.service").read_text(encoding="utf-8")
        smart_dev_unit = (ROOT / "systemd/smart-routing-dev.service").read_text(encoding="utf-8")
        self.assertIn("/home/csda/AI_Routing/production/sr_common_vrp_api_server.py", common_unit)
        self.assertIn("/home/csda/AI_Routing/development/sr_common_vrp_api_server.py", common_dev_unit)
        self.assertIn("/home/csda/AI_Routing/production/sr_vrp_api_server.py", smart_unit)
        self.assertIn("/home/csda/AI_Routing/development/sr_vrp_api_server.py", smart_dev_unit)
        self.assertIn("/home/csda/AI_Routing/production/sr_common_vrp_client_server.py", client_unit)
        self.assertIn("/home/csda/AI_Routing/development/sr_common_vrp_client_server.py", client_dev_unit)
        self.assertIn("--expected-environment production", common_unit)
        self.assertIn("--expected-environment development", common_dev_unit)
        self.assertIn("verify_deployment.py", common_unit)
        self.assertIn("verify_deployment.py", common_dev_unit)
        self.assertIn("verify_deployment.py", smart_unit)
        self.assertIn("verify_deployment.py", smart_dev_unit)
        all_units = common_unit + common_dev_unit + client_unit + client_dev_unit + smart_unit + smart_dev_unit
        self.assertNotIn("/home/AI_Routing", all_units)

    def test_manual_start_paths_enforce_hydration_gate(self) -> None:
        scripts = (
            "start_common_vrp_prod.sh",
            "start_common_vrp_dev.sh",
            "start_common_vrp_client_server_prod.sh",
            "start_common_vrp_client_server_dev.sh",
            "bootstrap_common_vrp_dev.sh",
            "restart_smart_routing_api.sh",
        )
        for relative_path in scripts:
            with self.subTest(script=relative_path):
                source = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn("verify_deployment.py", source)
        legacy_client_restart = (ROOT / "restart_common_vrp_client.sh").read_text(encoding="utf-8")
        self.assertIn("start_common_vrp_client_server_prod.sh", legacy_client_restart)

    def test_deploy_manifest_sources_are_checked_in_templates(self) -> None:
        build_script = (ROOT / "services/deploy/build_deploy_package.ps1").read_text(encoding="utf-8")
        self.assertIn('"config/common_vrp.dev.template.json"', build_script)
        self.assertIn('"config/common_vrp.prod.template.json"', build_script)
        self.assertIn('"services/deploy/requirements.txt"', build_script)
        self.assertIn('"services/api/common_vrp_config.py"', build_script)
        self.assertIn('$PackageName = "ai-routing-runtime-$Environment-$Version"', build_script)
        self.assertIn('[string]$OutputDir = "deployment"', build_script)
        self.assertIn('$EnvironmentOutputRoot = Join-Path $OutputRoot $Environment', build_script)
        self.assertNotIn('Copy-DirectoryFiltered -DirectoryName "services"', build_script)
        self.assertNotIn('Copy-DirectoryFiltered -DirectoryName "admin_tools"', build_script)
        self.assertNotIn('Copy-DirectoryFiltered -DirectoryName "osrm"', build_script)
        self.assertNotIn('Copy-DirectoryFiltered -DirectoryName "prompts"', build_script)
        self.assertIn("[switch]$AllowDirtySource", build_script)
        self.assertIn('[ValidateSet("development", "production")]', build_script)
        self.assertIn("if ($IsProduction -and $AllowDirtySource)", build_script)
        self.assertIn("if ($SourceDirty -and -not $AllowDirtySource)", build_script)
        self.assertIn("source_revision = $SourceRevision", build_script)
        self.assertIn("source_dirty = [bool]$SourceDirty", build_script)
        self.assertIn('source_mode = $SourceMode', build_script)
        self.assertIn('promotable = [bool]($IsProduction -and -not $SourceDirty)', build_script)
        self.assertIn('& git @GitArchiveArgs', build_script)
        self.assertIn('$SourcePath = Join-Path $SourceRoot $SourceRelativePath', build_script)
        self.assertIn('Join-Path $EnvironmentOutputRoot "_building"', build_script)
        self.assertIn('Move-PublishPathWithRetry -Kind "File"', build_script)
        self.assertIn('Move-PublishPathWithRetry -Kind "Directory"', build_script)
        self.assertIn('[int]$MaxAttempts = 8', build_script)
        self.assertIn('function Get-FileSha256WithRetry', build_script)
        self.assertIn('finally {', build_script)
        self.assertIn("target_environment = $Environment", build_script)
        self.assertIn('artifact_type = "server-runtime"', build_script)
        self.assertIn("System.Text.UTF8Encoding($false)", build_script)
        self.assertIn("[System.IO.File]::WriteAllText", build_script)
        self.assertNotIn("IncludeRuntimeData", build_script)
        self.assertNotIn("source_root =", build_script)
        self.assertNotIn("server_ftp.local.json", build_script)
        self.assertNotIn('Add-SanitizedConfigTemplate -ConfigRelativePath "config/common_vrp.prod.json"', build_script)

    def test_package_builders_normalize_provider_paths_to_native_filesystem_strings(self) -> None:
        for relative in (
            "services/deploy/build_deploy_package.ps1",
            "services/deploy/build_admin_tools_package.ps1",
            "services/deploy/build_server_data_package.ps1",
        ):
            with self.subTest(builder=relative):
                source = (ROOT / relative).read_text(encoding="utf-8")
                self.assertIn("function ConvertTo-NativeFileSystemPath", source)
                self.assertIn("GetUnresolvedProviderPathFromPSPath", source)
                self.assertIn('$Provider.Name -ne "FileSystem"', source)
                self.assertIn("[System.IO.Path]::GetFullPath", source)
                self.assertIn("Provider-qualified paths are not valid", source)
                self.assertNotIn("$Root = Resolve-Path", source)
                self.assertNotIn("$Root = (Resolve-Path", source)

    def test_server_runtime_uses_an_explicit_smart_routing_allowlist(self) -> None:
        build_script = (ROOT / "services/deploy/build_deploy_package.ps1").read_text(encoding="utf-8")
        required = {
            "__init__.py",
            "area_map.py",
            "census_geocoder.py",
            "common_vrp_api_server.py",
            "common_vrp_db.py",
            "common_vrp_runtime.py",
            "data_catalog.py",
            "geocode_storage.py",
            "google_geocoder.py",
            "here_geocoder.py",
            "live_atlanta_runtime.py",
            "nominatim_geocoder.py",
            "osrm_routing.py",
            "production_atlanta.py",
            "region_design.py",
            "region_sweep.py",
            "routing_compare.py",
            "service_preprocess.py",
            "us_geocode_cleaner.py",
            "vrp_api_common.py",
            "vrp_api_server.py",
            "vrp_api_service.py",
            "vrp_mode_na_general.py",
            "vrp_mode_z_weekend.py",
            "production_assign_atlanta.py",
            "production_assign_atlanta_vrp.py",
        }
        for name in required:
            self.assertIn(f'"smart_routing/{name}"', build_script)
        self.assertNotIn('Copy-DirectoryFiltered -DirectoryName "smart_routing"', build_script)
        for local_only in (
            "area_map_usa.py",
            "asia_geocode_cleaner.py",
            "bigquery_runtime.py",
            "export_daily_stats.py",
            "prewarm_map_cache.py",
            "production_assign_atlanta_osrm.py",
            "profile_sync.py",
            "vrp_api_client.py",
            "select_data.sql",
        ):
            self.assertNotIn(f'"smart_routing/{local_only}"', build_script)

    def test_server_data_has_a_separate_checksum_package(self) -> None:
        build_script = (ROOT / "services/deploy/build_server_data_package.ps1").read_text(encoding="utf-8")
        self.assertIn('"zcta_geometry"', build_script)
        self.assertIn('"region_seed_dir"', build_script)
        self.assertIn('Get-FileHash', build_script)
        self.assertIn('[switch]$AcknowledgeSensitiveData', build_script)
        self.assertIn('restricted-personal-data', build_script)
        self.assertIn('must stay under $AllowedOutputRoot', build_script)
        self.assertIn('Catalog role escapes data_root', build_script)
        self.assertIn('target_root = "/home/csda/AI_Routing"', build_script)
        self.assertIn('"raw", "planning", "reports", "runtime", "cache", "260310"', build_script)
        self.assertNotIn("Copy-Item -Path data", build_script)

    def test_http_bootstrap_route_is_absent_from_code_and_runbook(self) -> None:
        api_source = (ROOT / "smart_routing/common_vrp_api_server.py").read_text(encoding="utf-8")
        operations_manual = (ROOT / "docs/osrm_postgresql_docker_setup_manual.md").read_text(encoding="utf-8")
        active_docs = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "docs").rglob("*.md")
            if "history" not in path.parts
        )
        self.assertNotIn('parsed.path == "/api/v1/common/init"', api_source)
        self.assertNotIn("seed_default_masters,", api_source)
        self.assertNotIn("curl -X POST http://127.0.0.1:8065/api/v1/common/init", operations_manual)
        self.assertNotIn("`/api/v1/common/init`", active_docs)
        self.assertIn("--confirm-production-bootstrap", operations_manual)


class ServiceHealthTests(unittest.TestCase):
    def test_smart_routing_entrypoint_exposes_health(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), ServiceVRPRequestHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host, port = server.server_address
            with urlopen(f"http://{host}:{port}/api/v1/routing/health", timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(response.status, 200)
            self.assertEqual(payload, {"status": "ok"})
            with self.assertRaises(HTTPError) as context:
                urlopen(f"http://{host}:{port}/unknown", timeout=2)
            self.assertEqual(context.exception.code, 404)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_common_http_init_is_not_found_and_never_seeds(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), CommonVRPRequestHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host, port = server.server_address
            request = Request(
                f"http://{host}:{port}/api/v1/common/init",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with mock.patch("smart_routing.common_vrp_db.seed_default_masters") as seed:
                with self.assertRaises(HTTPError) as context:
                    urlopen(request, timeout=2)
                self.assertEqual(context.exception.code, 404)
                seed.assert_not_called()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
