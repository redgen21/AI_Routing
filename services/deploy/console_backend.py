"""Fail-closed backend for the local Streamlit deployment console.

Remote mutation is possible only through explicit UI actions, exact typed
confirmations, manifest allowlists, and local policy flags. Importing this module
never connects to SFTP/SSH or a database.
"""

from __future__ import annotations

import contextlib
from concurrent.futures import ThreadPoolExecutor
import base64
import errno
import hashlib
import io
import json
import locale
import os
import posixpath
import re
import shlex
import shutil
import stat
import subprocess
import threading
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Iterator, Mapping
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from admin_tools.db.release_backend import MigrationSpec


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENT_ROOT = PROJECT_ROOT / "deployment"
CONFIG_ROOT = PROJECT_ROOT / "config"
MIGRATIONS_ROOT = PROJECT_ROOT / "admin_tools" / "db" / "migrations"
MIGRATION_MANIFEST = MIGRATIONS_ROOT / "manifest.json"
HISTORY_PATH = (
    PROJECT_ROOT
    / "data"
    / "north_america"
    / "runtime"
    / "deployment_console"
    / "history.json"
)
MANAGED_DATA_ROOT = (
    PROJECT_ROOT
    / "data"
    / "north_america"
    / "runtime"
    / "deployment_console"
    / "managed_data"
)
_HISTORY_LOCK = threading.Lock()
_BUILD_LOCK = threading.Lock()
_MANAGED_DATA_LOCK = threading.Lock()
_MANAGED_DATA_JOBS_LOCK = threading.Lock()
_MANAGED_DATA_JOBS: dict[str, dict[str, Any]] = {}
_MANAGED_DATA_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="managed-data")
_MANAGED_DATA_VALIDATION_CACHE: dict[tuple[str, str, str], dict[str, Any]] = {}

_BUILD_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_BUILD_SCRIPT = PROJECT_ROOT / "services" / "deploy" / "build_deploy_package.ps1"
_ADMIN_TOOLS_BUILD_SCRIPT = (
    PROJECT_ROOT / "services" / "deploy" / "build_admin_tools_package.ps1"
)
_BUILD_TIMEOUT_SECONDS = 600
_BUILD_OUTPUT_LIMIT = 4000

ARTIFACT_TYPES = {"runtime", "server-data", "admin-tools"}
HISTORY_KINDS = ARTIFACT_TYPES | {
    "development-secure-config",
    "production-secure-config",
    "managed-data",
}
ENVIRONMENTS = {"development", "production"}
FORBIDDEN_ARTIFACT_NAMES = {
    "server_ftp.local.json",
    "server_deploy.local.json",
    "config.json",
    "common_vrp.dev.json",
    "config_common_vrp.json",
    "config_common_vrp.dev.json",
}
DEVELOPMENT_SECURE_CONFIG_TARGETS = (
    (
        "common_vrp.dev.json",
        "/home/csda/AI_Routing/development/config_common_vrp.dev.json",
    ),
    ("config.json", "/home/csda/AI_Routing/development/config/config.json"),
)
PRODUCTION_SECURE_CONFIG_TARGETS = (
    (
        "common_vrp.prod.json",
        "/home/csda/AI_Routing/production/config_common_vrp.json",
    ),
    ("config.json", "/home/csda/AI_Routing/production/config/config.json"),
)
_SECURE_CONFIG_COMMON_SOURCES = {
    "development": "common_vrp.dev.json",
    "production": "common_vrp.prod.json",
}
_SECURE_CONFIG_TARGETS = {
    "development": DEVELOPMENT_SECURE_CONFIG_TARGETS,
    "production": PRODUCTION_SECURE_CONFIG_TARGETS,
}
_SECURE_CONFIG_TARGET_DIRECTORIES = {
    "development": "/home/csda/AI_Routing/development/config",
    "production": "/home/csda/AI_Routing/production/config",
}
DEVELOPMENT_COMMON_JOB_ARCHIVE_ROOT = (
    "/home/csda/AI_Routing/state/development/common_vrp_jobs"
)
PRODUCTION_COMMON_JOB_ARCHIVE_ROOT = (
    "/home/csda/AI_Routing/state/production/common_vrp_jobs"
)
NORTH_AMERICA_SHARED_ROOT = "/home/csda/AI_Routing/shared/north_america"
PRODUCTION_REMOTE_DATA_CATALOG = (
    "/home/csda/AI_Routing/shared/config/data_catalog.production.json"
)
_REMOTE_DATA_CATALOG_MAX_BYTES = 1024 * 1024
_SECURE_CONFIG_MODE = 0o600
_SECURE_CONFIG_MODE_TEXT = "0600"
_MASTER_ADMIN_PROFILE_PATH = CONFIG_ROOT / "server_deploy.local.json"
_CONNECTION_PROFILE_NAME = "server_deploy.local.json"
_CONNECTION_CREDENTIALS_NAME = "server_ftp.local.json"
_CONNECTION_ENVIRONMENT_CONFIG_NAMES = {
    "development": "common_vrp.dev.json",
    "production": "common_vrp.prod.json",
}
_DATABASE_NAMES = {
    "development": "vrp_db_dev",
    "production": "vrp_db",
}
_CONNECTION_TRANSACTION_JOURNAL_NAME = ".connection-settings-transaction.local.json"
_CONNECTION_TRANSACTION_SCHEMA = "connection-settings-transaction/v1"
_CONNECTION_BACKUP_NAME = re.compile(
    r"^\.connection-settings-[0-9a-f]{32}-[0-9]{1,3}\.bak$"
)
_CONNECTION_SETTINGS_LOCK = threading.Lock()
_MASTER_ADMIN_CONTRACT_VERSION = "db-admin/v1"
_MASTER_ADMIN_MAX_CSV_BYTES = 16 * 1024 * 1024
_MASTER_ADMIN_MAX_JSON_BYTES = 2 * 1024 * 1024
_MASTER_ADMIN_TIMEOUT_SECONDS = 120
_MASTER_ADMIN_RELEASE_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_MASTER_ADMIN_TABLE_ID = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_MASTER_ADMIN_PREVIEW_ID = re.compile(r"^[A-Za-z0-9._:-]{1,255}$")
_MASTER_ADMIN_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_MASTER_ADMIN_CONFIG_NAMES = {
    "development": "config_common_vrp.dev.json",
    "production": "config_common_vrp.json",
}
_MASTER_ADMIN_COMMANDS = frozenset(
    {"overview", "list-specs", "preview", "apply", "receipt"}
)
_REGION_PLAN_ADMIN_MODULE = "admin_tools.db.region_plan_backend"
_REGION_PLAN_SCHEMA_ADMIN_MODULE = "admin_tools.db.region_plan_schema_backend"
_REGION_PLAN_SCHEMA_CONTRACT_VERSION = "region-plan-schema/v2"
_REGION_PLAN_SCHEMA_ID = "common_region_plan_schema_v2"
_REGION_PLAN_SCHEMA_CONFIRMATION = (
    "RECONCILE COMMON REGION PLAN SCHEMA V2 TO DEVELOPMENT vrp_db_dev"
)
_REGION_PLAN_ADMIN_COMMAND = "stage-candidate"
_FIXED_REGION_PLAN_BUNDLE_DATASET = "fixed_region_plan_bundle"
_FIXED_REGION_PLAN_BUNDLE_SCOPE = "development"
_FIXED_REGION_PLAN_BUNDLE_COMMANDS = frozenset(
    {"stage-bundle", "status-bundle", "import-bundle"}
)
_FIXED_REGION_PLAN_BUNDLE_CONTRACT_VERSION = "region-plan-bundle-import/v1"
_REGION_PLAN_CONTRACT_VERSION = "region-plan/v1"
_REGION_PLAN_MIGRATION_CONTRACT_VERSION = "region-plan-migration/v1"
_REGION_PLAN_WORKFLOW_CONTRACT_VERSION = "region-plan-workflow/v1"
_REGION_PLAN_WORKFLOW_COMMANDS = frozenset(
    {"resolve", "review", "activation-preview", "activate"}
)
_REGION_PLAN_ADMIN_COMMANDS = frozenset(
    {_REGION_PLAN_ADMIN_COMMAND, "migration-preview", "install-schema"}
    | _REGION_PLAN_WORKFLOW_COMMANDS
)
_REGION_PLAN_PLAN_ID = re.compile(r"^[a-z][a-z0-9._-]{0,159}$")
_REGION_PLAN_V2_API_ORIGIN = "http://127.0.0.1:8066"
_REGION_PLAN_V2_API_ENV = "REGION_PLAN_V2_API_ORIGIN"
_REGION_PLAN_V2_TIMEOUT_SECONDS = 30
_REGION_PLAN_V2_PRINCIPAL_ENV = "REGION_PLAN_V2_PRINCIPAL"
_REGION_PLAN_V2_MAX_WORKBOOK_BYTES = 24 * 1024 * 1024
_REGION_PLAN_FIXED_ID = "atlanta_6area_new_atl_buckets_20260721_v1"
_REGION_PLAN_VERSIONED_ID = re.compile(
    r"^atlanta_6area_v[12]_[0-9a-f]{64}$"
)
_REGION_PLAN_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_MASTER_PREVIEW_CONFIRMATION_TTL = timedelta(minutes=30)
_MASTER_PREVIEW_CONFIRMATIONS: dict[str, tuple[str, str, str, str, datetime]] = {}
_MASTER_PREVIEW_CONFIRMATION_LOCK = threading.Lock()
_MANAGED_DATA_SCOPES = frozenset({"development", "production", "common"})
_MANAGED_DATA_DATASET_ID = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_MANAGED_DATA_VERSION = re.compile(r"^[0-9a-f]{64}$")
_MANAGED_DATA_METADATA_NAME = "metadata.json"
_MANAGED_DATA_METADATA_SCHEMA = "managed-data-version/v1"
_MANAGED_DATA_DB_PREVIEW_TTL = timedelta(minutes=30)
_MANAGED_DATA_DB_PREVIEWS: dict[
    str, tuple[str, str, str, str, datetime]
] = {}
_MANAGED_DATA_DB_PREVIEW_LOCK = threading.Lock()
_MANAGED_DATA_DB_DATASET = "heavy_repair_rules"
_MANAGED_DATA_DB_TABLE = "common_heavy_repair_rule_master"
_TECHNICIAN_PROFILE_DATASET = "technician_data_workbook"
_TECHNICIAN_PROFILE_SCOPE = "development"
_TECHNICIAN_PROFILE_ADMIN_MODULE = "admin_tools.db.technician_profile_backend"
_TECHNICIAN_PROFILE_CONTRACT_VERSION = "technician-profile/v1"
_TECHNICIAN_PROFILE_ADMIN_COMMANDS = frozenset({"preview", "apply"})
_TECHNICIAN_PROFILE_PREVIEWS: dict[
    str, tuple[str, str, str, str, str, str, datetime, str | None]
] = {}
_TECHNICIAN_PROFILE_PREVIEW_LOCK = threading.Lock()
_MANAGED_DATA_CANDIDATE_DATASET = "territory_plan_workbook"
_MANAGED_DATA_CANDIDATE_SCOPE = "development"
_FIXED_REGION_PLAN_BUNDLE_REQUEST_ROOT = "region_plan_bundle_requests"
_REGION_PLAN_REQUEST_ROOT = "region_plan_requests"
_REGION_PLAN_REQUEST_MAX_BYTES = 1024 * 1024
_REGION_PLAN_SCHEMA_CONFIRMATIONS: dict[str, tuple[str, str, str, str, datetime]] = {}
_REGION_PLAN_SCHEMA_CONFIRMATION_LOCK = threading.Lock()
_REGION_PLAN_ACTIVATION_PREVIEWS: dict[
    str, tuple[str, str, int, str, str, datetime]
] = {}
_REGION_PLAN_ACTIVATION_PREVIEW_LOCK = threading.Lock()
_REGION_PLAN_RESOLUTION_DOWNLOADS: dict[
    str, tuple[str, dict[str, dict[str, Any]], str, str | None, datetime]
] = {}
_REGION_PLAN_RESOLUTION_DOWNLOAD_LOCK = threading.Lock()
_REGION_PLAN_FINAL_BINDINGS: dict[str, tuple[str, str, str, datetime]] = {}
_REGION_PLAN_FINAL_BINDING_LOCK = threading.Lock()
_REMOTE_SHA256_OUTPUT = re.compile(r"^(?P<sha256>[0-9a-fA-F]{64})[ \t]+[^\r\n]+$")
_PLACEHOLDER_VALUE = re.compile(
    r"(?i)(?:<[^>]*(?:replace|placeholder|change)[^>]*>|"
    r"(?:replace|change)[_-]?me|(?:your|example)[ _-]?(?:password|secret|token|api[_ -]?key))"
)
_RUNTIME_CREATED_AT = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,7}))?(?P<timezone>Z|[+-]\d{2}:\d{2})?$"
)
SERVICE_SPECS = {
    "development": (
        ("common-vrp-dev.service", "http://127.0.0.1:8066/api/v1/common/contexts"),
        ("smart-routing-dev.service", "http://127.0.0.1:8056/api/v1/routing/health"),
        ("common-vrp-client-dev.service", "http://127.0.0.1:8503/_stcore/health"),
    ),
    "production": (
        ("common-vrp.service", "http://127.0.0.1:8065/api/v1/common/contexts"),
        ("smart-routing.service", "http://127.0.0.1:8055/api/v1/routing/health"),
        ("common-vrp-client.service", "http://127.0.0.1:8501/_stcore/health"),
    ),
}
OSRM_MONITOR_SPECS = (
    (
        "OSRM Korea",
        "osrm-korea.service",
        "http://127.0.0.1:5000/nearest/v1/driving/126.9780,37.5665?number=1",
        5000,
    ),
    (
        "OSRM Los Angeles",
        "osrm-usa.service",
        "http://127.0.0.1:5001/nearest/v1/driving/-118.2437,34.0522?number=1",
        5001,
    ),
    (
        "OSRM Atlanta",
        "osrm-usa.service",
        "http://127.0.0.1:5002/nearest/v1/driving/-84.3880,33.7490?number=1",
        5002,
    ),
)


@dataclass(frozen=True)
class ArtifactEntry:
    id: str
    version: str
    label: str
    path: str


@dataclass(frozen=True)
class ArtifactInspection:
    path: str
    kind: str
    environment: str
    version: str
    manifest: Mapping[str, Any]
    archive_sha256: str
    target_upload_path: str
    required_confirmation: str
    restricted_data: bool


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _within(path: Path, root: Path) -> Path:
    resolved = path.expanduser().resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"Path escapes allowed root: {resolved}") from exc
    return resolved


def _safe_relative(value: str) -> str:
    path = PurePosixPath(str(value).replace("\\", "/"))
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError(f"Unsafe manifest path: {value}")
    return path.as_posix()


def _is_remote_not_found(error: OSError) -> bool:
    """Return true only for an explicit remote ENOENT condition."""

    return isinstance(error, FileNotFoundError) or error.errno == errno.ENOENT


def _artifact_root(environment: str, kind: str) -> Path:
    _require_environment(environment)
    if kind == "runtime":
        return DEPLOYMENT_ROOT / environment
    if kind == "server-data":
        return DEPLOYMENT_ROOT / "server_data"
    if kind == "admin-tools":
        return DEPLOYMENT_ROOT / "admin_tools"
    raise ValueError(f"Unknown artifact kind: {kind}")


def _manifest_path(staging: Path, kind: str) -> Path:
    return staging / ("manifest.json" if kind == "server-data" else "deploy_manifest.json")


def _require_environment(environment: str) -> str:
    normalized = str(environment).strip().lower()
    if normalized not in ENVIRONMENTS:
        raise ValueError("environment must be development or production")
    return normalized


def _require_build_version(version: str) -> str:
    normalized = str(version).strip()
    if not _BUILD_VERSION_PATTERN.fullmatch(normalized):
        raise ValueError(
            "version must start with a letter or number and contain only "
            "letters, numbers, dot, underscore, and hyphen"
        )
    return normalized


def _runtime_build_paths(environment: str, version: str) -> dict[str, Path]:
    environment = _require_environment(environment)
    version = _require_build_version(version)
    output_root = DEPLOYMENT_ROOT / environment
    return {
        "output": output_root,
        "staging": output_root / version,
        "manifest": output_root / version / "deploy_manifest.json",
        "archive": output_root / f"ai-routing-runtime-{environment}-{version}.zip",
    }


def _admin_tools_build_paths(version: str) -> dict[str, Path]:
    """Return the only local outputs permitted for an Admin Tools build."""

    version = _require_build_version(version)
    output_root = DEPLOYMENT_ROOT / "admin_tools"
    return {
        "output": output_root,
        "staging": output_root / version,
        "manifest": output_root / version / "deploy_manifest.json",
        "archive": output_root / f"ai-routing-admin-tools-{version}.zip",
    }


def _decode_process_output(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        decoded = value
    elif isinstance(value, bytes):
        if value.startswith((b"\xff\xfe", b"\xfe\xff")):
            encoding = "utf-16"
        elif value.startswith(b"\xef\xbb\xbf"):
            encoding = "utf-8-sig"
        elif value and value.count(b"\x00") * 4 > len(value):
            even_nulls = value[0::2].count(0)
            odd_nulls = value[1::2].count(0)
            encoding = "utf-16-be" if even_nulls > odd_nulls else "utf-16-le"
        else:
            encoding = "utf-8"
        try:
            decoded = value.decode(encoding, errors="strict")
        except (LookupError, UnicodeDecodeError):
            preferred = locale.getpreferredencoding(False) or "utf-8"
            try:
                decoded = value.decode(preferred, errors="strict")
            except (LookupError, UnicodeDecodeError):
                decoded = value.decode("utf-8", errors="replace")
    else:
        decoded = str(value)
    return decoded.replace("\r\n", "\n").replace("\r", "\n")


def _run_local_process(argv: list[str], *, timeout: int) -> subprocess.CompletedProcess[bytes]:
    child_env = os.environ.copy()
    child_env["PYTHONUTF8"] = "1"
    child_env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        argv,
        cwd=str(PROJECT_ROOT),
        shell=False,
        capture_output=True,
        text=False,
        env=child_env,
        timeout=timeout,
        check=False,
    )


def _safe_process_output(*values: object) -> str:
    combined = "\n".join(_decode_process_output(value) for value in values)
    cleaned = _redact(combined).strip()
    if len(cleaned) > _BUILD_OUTPUT_LIMIT:
        return cleaned[:_BUILD_OUTPUT_LIMIT] + "... [truncated]"
    return cleaned


def _git_source_state() -> tuple[str, int]:
    revision_result = _run_local_process(
        ["git", "rev-parse", "HEAD"], timeout=15
    )
    revision = _decode_process_output(revision_result.stdout).strip()
    if revision_result.returncode != 0 or not re.fullmatch(r"[0-9a-fA-F]{7,64}", revision):
        detail = _safe_process_output(revision_result.stderr)
        raise RuntimeError(f"Unable to resolve Git HEAD. {detail}".strip())

    status_result = _run_local_process(
        ["git", "status", "--porcelain", "--untracked-files=all"], timeout=30
    )
    if status_result.returncode != 0:
        detail = _safe_process_output(status_result.stderr)
        raise RuntimeError(f"Unable to inspect Git source status. {detail}".strip())
    changes = sum(
        1 for line in _decode_process_output(status_result.stdout).splitlines() if line
    )
    return revision, changes


def _ensure_runtime_build_targets_absent(paths: Mapping[str, Path]) -> None:
    collisions = [name for name in ("staging", "archive") if paths[name].exists()]
    if collisions:
        raise FileExistsError(
            "Runtime artifact output already exists; choose a new version "
            f"({', '.join(collisions)})."
        )


def _ensure_admin_tools_build_targets_absent(paths: Mapping[str, Path]) -> None:
    collisions = [name for name in ("staging", "archive") if paths[name].exists()]
    if collisions:
        raise FileExistsError(
            "Admin Tools artifact output already exists; choose a new version "
            f"({', '.join(collisions)})."
        )


def _quarantine_failed_runtime_build(
    *, environment: str, paths: Mapping[str, Path]
) -> None:
    """Hide outputs created by this build when post-build validation fails."""

    environment_root = (DEPLOYMENT_ROOT / _require_environment(environment)).resolve()
    staging = _within(paths["staging"], environment_root)
    archive = _within(paths["archive"], environment_root)
    failed_root = _within(environment_root / "_failed", environment_root)
    quarantine = _within(
        failed_root / f"runtime-{uuid.uuid4().hex}", environment_root
    )
    manifest = staging / "deploy_manifest.json"
    quarantine_ready = False
    try:
        failed_root.mkdir(parents=True, exist_ok=True)
        quarantine.mkdir(exist_ok=False)
        quarantine_ready = True
        if staging.exists():
            staging.replace(quarantine / "staging")
    except OSError:
        # If directory quarantine is unavailable, atomically remove the manifest
        # name recognized by list_artifacts and hide the archive version name.
        if manifest.is_file():
            hidden_manifest = staging / f".failed-deploy_manifest-{uuid.uuid4().hex}.json"
            manifest.replace(hidden_manifest)
        if archive.is_file():
            hidden_archive = archive.with_name(
                f".failed-{uuid.uuid4().hex}-{archive.name}"
            )
            archive.replace(hidden_archive)

    if quarantine_ready and archive.exists():
        archive.replace(quarantine / archive.name)
    if staging.exists() and manifest.is_file():
        raise RuntimeError(
            "Failed runtime artifact could not be hidden from the artifact list."
        )


def _quarantine_failed_admin_tools_build(*, paths: Mapping[str, Path]) -> None:
    """Hide failed Admin Tools outputs without touching an earlier release."""

    output_root = (DEPLOYMENT_ROOT / "admin_tools").resolve()
    staging = _within(paths["staging"], output_root)
    archive = _within(paths["archive"], output_root)
    failed_root = _within(output_root / "_failed", output_root)
    quarantine = _within(
        failed_root / f"admin-tools-{uuid.uuid4().hex}", output_root
    )
    manifest = staging / "deploy_manifest.json"
    quarantine_ready = False
    try:
        failed_root.mkdir(parents=True, exist_ok=True)
        quarantine.mkdir(exist_ok=False)
        quarantine_ready = True
        if staging.exists():
            staging.replace(quarantine / "staging")
    except OSError:
        if manifest.is_file():
            manifest.replace(
                staging / f".failed-deploy_manifest-{uuid.uuid4().hex}.json"
            )
        if archive.is_file():
            archive.replace(
                archive.with_name(f".failed-{uuid.uuid4().hex}-{archive.name}")
            )

    if quarantine_ready and archive.exists():
        archive.replace(quarantine / archive.name)
    if staging.exists() and manifest.is_file():
        raise RuntimeError(
            "Failed Admin Tools artifact could not be hidden from the artifact list."
        )


def preview_runtime_build(*, environment: str, version: str) -> dict[str, Any]:
    """Return a non-secret local-only build preview without creating files."""

    environment = _require_environment(environment)
    version = _require_build_version(version)
    paths = _runtime_build_paths(environment, version)
    revision, change_count = _git_source_state()
    collision = paths["staging"].exists() or paths["archive"].exists()
    source_dirty = change_count > 0
    return {
        "environment": environment,
        "version": version,
        "source_revision": revision,
        "source_dirty": source_dirty,
        "source_change_count": change_count,
        "requires_dirty_approval": environment == "development" and source_dirty,
        "build_allowed": not collision and not (environment == "production" and source_dirty),
        "output_path": str(paths["output"]),
        "staging_path": str(paths["staging"]),
        "manifest_path": str(paths["manifest"]),
        "archive_path": str(paths["archive"]),
        "output_exists": collision,
    }


def preview_admin_tools_build(*, version: str) -> dict[str, Any]:
    """Return a non-secret local Admin Tools build preview without writes."""

    version = _require_build_version(version)
    paths = _admin_tools_build_paths(version)
    revision, change_count = _git_source_state()
    source_dirty = change_count > 0
    collision = paths["staging"].exists() or paths["archive"].exists()
    return {
        "status": "preview",
        "kind": "admin-tools",
        "version": version,
        "source_revision": revision,
        "source_dirty": source_dirty,
        "source_change_count": change_count,
        "promotable": not source_dirty,
        "requires_dirty_approval": source_dirty,
        "build_allowed": not collision,
        "staging_path": str(paths["staging"]),
        "manifest_path": str(paths["manifest"]),
        "archive_path": str(paths["archive"]),
        "output_exists": collision,
    }


def build_runtime_artifact(
    *, environment: str, version: str, allow_dirty_source: bool = False
) -> dict[str, Any]:
    """Build one local runtime artifact through a fixed PowerShell invocation."""

    environment = _require_environment(environment)
    version = _require_build_version(version)
    if environment == "production" and allow_dirty_source:
        raise PermissionError("Production builds cannot bypass the clean-source requirement.")
    if not _BUILD_LOCK.acquire(blocking=False):
        raise RuntimeError("Another runtime artifact build is already in progress.")
    try:
        paths = _runtime_build_paths(environment, version)
        _ensure_runtime_build_targets_absent(paths)
        revision, change_count = _git_source_state()
        source_dirty = change_count > 0
        if environment == "production" and source_dirty:
            raise PermissionError("Production runtime artifacts require a clean Git checkout.")
        if source_dirty and not allow_dirty_source:
            raise PermissionError(
                "Dirty development builds require explicit Allow dirty source approval."
            )

        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if not powershell:
            raise RuntimeError("PowerShell is not available on this workstation.")
        argv = [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(_BUILD_SCRIPT),
            "-Version",
            version,
            "-OutputDir",
            "deployment",
            "-Environment",
            environment,
        ]
        if source_dirty and allow_dirty_source:
            argv.append("-AllowDirtySource")
        try:
            result = _run_local_process(argv, timeout=_BUILD_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            detail = _safe_process_output(exc.stdout, exc.stderr)
            build_error = TimeoutError(
                f"Runtime artifact build timed out after {_BUILD_TIMEOUT_SECONDS} seconds. "
                f"{detail}"
            )
            try:
                if paths["staging"].exists() or paths["archive"].exists():
                    _quarantine_failed_runtime_build(environment=environment, paths=paths)
            except Exception as quarantine_error:
                safe_quarantine_error = _safe_process_output(quarantine_error)
                raise RuntimeError(
                    f"{build_error} Failed outputs could not be quarantined: "
                    f"{safe_quarantine_error}"
                ) from build_error
            raise build_error from exc
        if result.returncode != 0:
            detail = _safe_process_output(result.stdout, result.stderr)
            build_error = RuntimeError(
                f"Runtime artifact build failed with exit code {result.returncode}. {detail}"
            )
            try:
                if paths["staging"].exists() or paths["archive"].exists():
                    _quarantine_failed_runtime_build(environment=environment, paths=paths)
            except Exception as quarantine_error:
                safe_quarantine_error = _safe_process_output(quarantine_error)
                raise RuntimeError(
                    f"{build_error} Failed outputs could not be quarantined: "
                    f"{safe_quarantine_error}"
                ) from build_error
            raise build_error

        try:
            for name in ("staging", "manifest", "archive"):
                exists = paths[name].is_dir() if name == "staging" else paths[name].is_file()
                if not exists:
                    raise RuntimeError(
                        f"Runtime artifact build did not produce the expected {name}."
                    )
            inspection = inspect_artifact(
                path=str(paths["staging"]), kind="runtime", environment=environment
            )
            if inspection.version != version:
                raise RuntimeError("Built runtime artifact version does not match the request.")
            manifest_revision = str(inspection.manifest.get("source_revision", "")).strip()
            manifest_dirty = inspection.manifest.get("source_dirty")
            expected_source_mode = (
                "immutable-git-archive" if environment == "production" else "worktree"
            )
            manifest_source_mode = inspection.manifest.get("source_mode")
            if (
                manifest_revision != revision
                or manifest_dirty is not source_dirty
                or manifest_source_mode != expected_source_mode
            ):
                raise RuntimeError(
                    "Built runtime manifest does not match the source state validated before build."
                )
        except Exception as validation_error:
            try:
                _quarantine_failed_runtime_build(environment=environment, paths=paths)
            except Exception as quarantine_error:
                raise RuntimeError(
                    "Runtime artifact validation failed and its outputs could not be quarantined."
                ) from quarantine_error
            raise validation_error
        return {
            "status": "built",
            "environment": environment,
            "version": version,
            "source_revision": revision,
            "source_dirty": source_dirty,
            "source_mode": expected_source_mode,
            "output_path": str(paths["output"]),
            "staging_path": str(paths["staging"]),
            "manifest_path": str(paths["manifest"]),
            "archive_path": str(paths["archive"]),
            "archive_sha256": inspection.archive_sha256,
        }
    finally:
        _BUILD_LOCK.release()


def build_admin_tools_artifact(
    *, version: str, allow_dirty_source: bool = False
) -> dict[str, Any]:
    """Build one local Admin Tools artifact through its fixed PowerShell script.

    Dirty source is permitted only as explicitly marked development verification;
    it is non-promotable and cannot become the common DB-admin release pin.
    """

    version = _require_build_version(version)
    if not _BUILD_LOCK.acquire(blocking=False):
        raise RuntimeError("Another artifact build is already in progress.")
    try:
        paths = _admin_tools_build_paths(version)
        _ensure_admin_tools_build_targets_absent(paths)
        revision, change_count = _git_source_state()
        source_dirty = change_count > 0
        if source_dirty and not allow_dirty_source:
            raise PermissionError(
                "Dirty Admin Tools builds require explicit Allow dirty source approval."
            )

        powershell = shutil.which("pwsh") or shutil.which("powershell")
        if not powershell:
            raise RuntimeError("PowerShell is not available on this workstation.")
        argv = [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(_ADMIN_TOOLS_BUILD_SCRIPT),
            "-Version",
            version,
            "-OutputDir",
            "deployment",
        ]
        if source_dirty:
            argv.append("-AllowDirtySource")
        try:
            result = _run_local_process(argv, timeout=_BUILD_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            detail = _safe_process_output(exc.stdout, exc.stderr)
            build_error = TimeoutError(
                f"Admin Tools artifact build timed out after {_BUILD_TIMEOUT_SECONDS} seconds. "
                f"{detail}"
            )
            try:
                if paths["staging"].exists() or paths["archive"].exists():
                    _quarantine_failed_admin_tools_build(paths=paths)
            except Exception as quarantine_error:
                raise RuntimeError(
                    "Admin Tools build timed out and failed outputs could not be quarantined."
                ) from quarantine_error
            raise build_error from exc
        if result.returncode != 0:
            detail = _safe_process_output(result.stdout, result.stderr)
            build_error = RuntimeError(
                f"Admin Tools artifact build failed with exit code {result.returncode}. {detail}"
            )
            try:
                if paths["staging"].exists() or paths["archive"].exists():
                    _quarantine_failed_admin_tools_build(paths=paths)
            except Exception as quarantine_error:
                raise RuntimeError(
                    "Admin Tools build failed and failed outputs could not be quarantined."
                ) from quarantine_error
            raise build_error

        try:
            for name in ("staging", "manifest", "archive"):
                exists = paths[name].is_dir() if name == "staging" else paths[name].is_file()
                if not exists:
                    raise RuntimeError(
                        f"Admin Tools build did not produce the expected {name}."
                    )
            inspection = inspect_artifact(
                path=str(paths["staging"]), kind="admin-tools", environment="development"
            )
            manifest_revision = str(inspection.manifest.get("source_revision", "")).strip()
            manifest_dirty = inspection.manifest.get("source_dirty")
            manifest_promotable = inspection.manifest.get("promotable")
            if (
                inspection.version != version
                or manifest_revision != revision
                or manifest_dirty is not source_dirty
                or manifest_promotable is not (not source_dirty)
            ):
                raise RuntimeError(
                    "Built Admin Tools manifest does not match the source state validated before build."
                )
            archive_sha256 = _sha256_file(paths["archive"])
        except Exception as validation_error:
            try:
                _quarantine_failed_admin_tools_build(paths=paths)
            except Exception as quarantine_error:
                raise RuntimeError(
                    "Admin Tools artifact validation failed and its outputs could not be quarantined."
                ) from quarantine_error
            raise validation_error
        return {
            "status": "built",
            "kind": "admin-tools",
            "version": version,
            "source_revision": revision,
            "source_dirty": source_dirty,
            "promotable": not source_dirty,
            "staging_path": str(paths["staging"]),
            "manifest_path": str(paths["manifest"]),
            "archive_path": str(paths["archive"]),
            "archive_sha256": archive_sha256,
        }
    finally:
        _BUILD_LOCK.release()


def _manifest_files(manifest: Mapping[str, Any]) -> dict[str, str]:
    files: dict[str, str] = {}
    for item in manifest.get("files", []):
        if not isinstance(item, Mapping):
            raise ValueError("Manifest file entries must be objects.")
        relative = _safe_relative(str(item.get("path", "")))
        checksum = str(item.get("sha256", "")).lower()
        if not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise ValueError(f"Invalid manifest checksum: {relative}")
        if relative in files:
            raise ValueError(f"Duplicate manifest path: {relative}")
        files[relative] = checksum
    if not files:
        raise ValueError("Artifact manifest contains no files.")
    return files


def _validate_runtime_manifest_policy(
    manifest: Mapping[str, Any], environment: str
) -> str:
    """Validate inexpensive runtime policy fields without hashing artifact files."""

    environment = _require_environment(environment)
    if manifest.get("artifact_type") != "server-runtime":
        raise ValueError("Runtime artifact_type mismatch.")
    if manifest.get("target_environment") != environment:
        raise ValueError("Runtime environment mismatch.")
    target = f"/home/csda/AI_Routing/{environment}"
    if manifest.get("target_root") != target:
        raise ValueError("Runtime target_root mismatch.")
    if environment == "production":
        if (
            manifest.get("source_dirty") is not False
            or manifest.get("promotable") is not True
        ):
            raise ValueError("Production requires a clean promotable runtime artifact.")
        expected_source_mode = "immutable-git-archive"
    else:
        if not isinstance(manifest.get("source_dirty"), bool):
            raise ValueError("Development runtime source_dirty must be boolean.")
        if manifest.get("promotable") is not False:
            raise ValueError("Development runtime artifacts must be non-promotable.")
        expected_source_mode = "worktree"
    if manifest.get("source_mode") != expected_source_mode:
        raise ValueError(f"Runtime source_mode must be {expected_source_mode}.")
    return target


def _validate_admin_tools_manifest_policy(files: Mapping[str, str]) -> None:
    """Reject legacy admin releases that bundle application runtime modules."""

    if any(PurePosixPath(relative).parts[:1] == ("smart_routing",) for relative in files):
        raise ValueError(
            "Admin-tools artifact must not contain smart_routing application paths."
        )


def list_artifacts(*, environment: str, kind: str) -> list[ArtifactEntry]:
    environment = _require_environment(environment)
    root = _artifact_root(environment, kind)
    if not root.is_dir():
        return []
    entries: list[ArtifactEntry] = []
    for staging in sorted(root.iterdir(), reverse=True):
        if not staging.is_dir() or staging.name.startswith("_"):
            continue
        manifest_path = _manifest_path(staging, kind)
        if not manifest_path.is_file():
            continue
        if kind in {"runtime", "admin-tools"}:
            try:
                manifest = _read_json(manifest_path)
                if kind == "runtime":
                    _validate_runtime_manifest_policy(manifest, environment)
                else:
                    _validate_admin_tools_manifest_policy(_manifest_files(manifest))
            except (OSError, ValueError, json.JSONDecodeError):
                # Legacy/malformed runtime or admin artifacts stay on disk for
                # audit/manual cleanup, but are not selectable for upload.
                continue
        entries.append(ArtifactEntry(staging.name, staging.name, staging.name, str(staging)))
    return entries


def inspect_artifact(*, path: str, kind: str, environment: str) -> ArtifactInspection:
    environment = _require_environment(environment)
    root = _artifact_root(environment, kind).resolve()
    staging = _within(Path(path), root)
    if not staging.is_dir():
        raise ValueError("Artifact staging directory is required.")
    manifest = _read_json(_manifest_path(staging, kind))
    files = _manifest_files(manifest)
    if kind == "admin-tools":
        _validate_admin_tools_manifest_policy(files)
    for relative, checksum in files.items():
        local = _within(staging / Path(relative), staging)
        if not local.is_file() or _sha256_file(local) != checksum:
            raise ValueError(f"Artifact file/checksum mismatch: {relative}")

    version = staging.name
    if kind == "runtime":
        target = _validate_runtime_manifest_policy(manifest, environment)
    elif kind == "server-data":
        if manifest.get("schema") != "ai-routing-server-data-package/v1":
            raise ValueError("Server-data manifest schema mismatch.")
        if manifest.get("sensitive_data_acknowledged") is not True:
            raise ValueError("Server-data sensitive-data acknowledgement is missing.")
        target = "/home/csda/AI_Routing"
        if manifest.get("target_root") != target:
            raise ValueError("Server-data target_root mismatch.")
    else:
        if manifest.get("artifact_type") != "db-admin-tools":
            raise ValueError("Admin-tools artifact_type mismatch.")
        target = f"/home/csda/AI_Routing/admin_tools/releases/{version}"
        if manifest.get("target_root") != target:
            raise ValueError("Admin-tools target_root mismatch.")
        if environment == "production" and (
            bool(manifest.get("source_dirty")) or manifest.get("promotable") is False
        ):
            raise ValueError("Production requires clean promotable admin tools.")

    restricted = bool(manifest.get("contains_secrets")) or any(
        PurePosixPath(name).name in FORBIDDEN_ARTIFACT_NAMES or name.endswith(".local.json")
        for name in files
    )
    archive_sha = hashlib.sha256(
        "".join(f"{name}:{files[name]}\n" for name in sorted(files)).encode()
    ).hexdigest()
    return ArtifactInspection(
        str(staging),
        kind,
        environment,
        version,
        manifest,
        archive_sha,
        target,
        f"DEPLOY {environment} {version}",
        restricted,
    )


def _manifest_created_at(manifest: Mapping[str, Any]) -> datetime:
    """Return an aware timestamp used to order fully validated artifacts."""

    raw = manifest.get("created_at")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("Runtime manifest created_at is required.")
    match = _RUNTIME_CREATED_AT.fullmatch(raw.strip())
    if not match:
        raise ValueError("Runtime manifest created_at is invalid.")
    try:
        created_at = datetime.strptime(
            f"{match.group('date')}T{match.group('time')}", "%Y-%m-%dT%H:%M:%S"
        )
    except ValueError as exc:
        raise ValueError("Runtime manifest created_at is invalid.") from exc
    fraction = match.group("fraction")
    if fraction:
        # PowerShell DateTime's round-trip format uses seven tick digits;
        # Python datetime stores microseconds.  Truncate only the final tick.
        created_at = created_at.replace(microsecond=int(fraction[:6].ljust(6, "0")))
    timezone_value = match.group("timezone")
    if timezone_value == "Z":
        created_at = created_at.replace(tzinfo=timezone.utc)
    elif timezone_value:
        sign = 1 if timezone_value[0] == "+" else -1
        hours = int(timezone_value[1:3])
        minutes = int(timezone_value[4:6])
        if hours > 23 or minutes > 59:
            raise ValueError("Runtime manifest created_at is invalid.")
        created_at = created_at.replace(
            tzinfo=timezone(sign * timedelta(hours=hours, minutes=minutes))
        )
    else:
        # Older local artifacts used PowerShell's sortable (naive) timestamp.
        # Treat it as UTC for a stable ordering; future builds emit UTC offsets.
        created_at = created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def resolve_latest_runtime_artifact(*, environment: str) -> ArtifactInspection | None:
    """Return the newest fully verified, non-secret runtime artifact.

    Listing artifacts intentionally avoids hashing files.  This resolver is used
    for the deployment default, where a manifest-only check is not sufficient.
    Invalid, incomplete, legacy, or secret-bearing candidates remain on disk for
    audit but are never selected.
    """

    environment = _require_environment(environment)
    root = _artifact_root(environment, "runtime")
    if not root.is_dir():
        return None
    candidates: list[tuple[datetime, str, str, ArtifactInspection]] = []
    for staging in root.iterdir():
        if not staging.is_dir() or staging.name.startswith("_"):
            continue
        try:
            item = inspect_artifact(
                path=str(staging), kind="runtime", environment=environment
            )
            if item.restricted_data:
                continue
            candidates.append(
                (_manifest_created_at(item.manifest), item.version, item.path, item)
            )
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    if not candidates:
        return None
    # Reverse all fields to make the fallback deterministic: newest created_at,
    # then lexical version and staging path.
    return max(candidates, key=lambda row: (row[0], row[1], row[2]))[3]


def _inspection(value: ArtifactInspection | Mapping[str, Any]) -> ArtifactInspection:
    if isinstance(value, ArtifactInspection):
        return value
    return ArtifactInspection(**dict(value))


def _load_remote_profile(config_path: str | Path) -> dict[str, Any]:
    profile_path = _within(Path(config_path), CONFIG_ROOT)
    if not profile_path.name.endswith(".local.json"):
        raise ValueError("Deployment profile must be a local ignored JSON file.")
    policy = _read_json(profile_path)
    credentials_value = str(policy.get("credentials_file", "")).strip()
    if not credentials_value:
        raise ValueError("credentials_file is required in the deployment profile.")
    credentials_path = Path(credentials_value)
    if not credentials_path.is_absolute():
        credentials_path = PROJECT_ROOT / credentials_path
    credentials_path = _within(credentials_path, CONFIG_ROOT)
    credentials = _read_json(credentials_path)
    for key in ("host", "username", "password"):
        if not str(credentials.get(key, "")).strip():
            raise ValueError(f"Missing remote credential: {key}")
    remote_root = str(policy.get("remote_root") or credentials.get("remote_root") or "").strip()
    if not remote_root.startswith("/") or ".." in PurePosixPath(remote_root).parts:
        raise ValueError("remote_root must be an absolute safe POSIX path.")
    return {
        "host": str(credentials["host"]),
        "port": int(credentials.get("sftp_port", 22)),
        "username": str(credentials["username"]),
        "password": str(credentials["password"]),
        "remote_root": remote_root,
        "allow_upload": policy.get("allow_upload") is True,
        "allow_development_secure_config_upload": (
            policy.get("allow_development_secure_config_upload") is True
        ),
        "allow_production_secure_config_upload": (
            policy.get("allow_production_secure_config_upload") is True
        ),
        "allow_service_control": policy.get("allow_service_control") is True,
        # The local profile pins the immutable Admin Tools release used by the
        # database console.  The development verification pin is intentionally
        # separate: it may name a dirty/non-promotable release and can never
        # serve a production command.
        "admin_tools_release_version": str(
            policy.get("admin_tools_release_version", "")
        ).strip(),
        "admin_tools_development_release_version": str(
            policy.get("admin_tools_development_release_version", "")
        ).strip(),
    }


def _connection_profile_path() -> Path:
    """Return the one local deployment policy file editable by the console."""

    return _within(CONFIG_ROOT / _CONNECTION_PROFILE_NAME, CONFIG_ROOT)


def _connection_credentials_path() -> Path:
    """Return the one local credential file editable by the console."""

    return _within(CONFIG_ROOT / _CONNECTION_CREDENTIALS_NAME, CONFIG_ROOT)


def _require_fixed_credentials_reference(profile: Mapping[str, Any]) -> None:
    expected = str(Path("config") / _CONNECTION_CREDENTIALS_NAME).replace("\\", "/")
    configured = str(profile.get("credentials_file", "")).replace("\\", "/")
    if configured != expected:
        raise ValueError("Deployment profile credentials_file must use the fixed local path.")


def _connection_environment_path(environment: str) -> Path:
    environment = _require_environment(environment)
    return _within(
        CONFIG_ROOT / _CONNECTION_ENVIRONMENT_CONFIG_NAMES[environment], CONFIG_ROOT
    )


def _connection_error() -> dict[str, str]:
    """Use one non-sensitive error for settings that cannot be read safely."""

    return {
        "status": "unavailable",
        "message": "Local connection settings are missing or invalid.",
    }


def _is_configured_secret(value: Any) -> bool:
    """Return a status bit without ever returning the secret itself."""

    if not isinstance(value, str):
        return False
    normalized = value.strip()
    return bool(normalized) and not _PLACEHOLDER_VALUE.fullmatch(normalized)


def _require_text(
    value: Any, *, field: str, pattern: re.Pattern[str], maximum: int
) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string.")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum or not pattern.fullmatch(normalized):
        raise ValueError(f"{field} is invalid.")
    return normalized


_HOST_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,252})$")
_USER_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,127})$")


def _require_port(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ValueError(f"{field} must be an integer from 1 through 65535.")
    return value


def _require_remote_root(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("remote_root must be a string.")
    normalized = value.strip()
    path = PurePosixPath(normalized)
    if (
        not normalized
        or len(normalized) > 1024
        or not path.is_absolute()
        or ".." in path.parts
        or any(ord(char) < 32 for char in normalized)
    ):
        raise ValueError("remote_root must be an absolute safe POSIX path.")
    return path.as_posix()


def _require_mapping_update(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object.")
    return value


def _password_update(value: Any, *, field: str) -> str | None:
    """Return a submitted password, or None when the UI says to preserve it."""

    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string when submitted.")
    normalized = value.strip()
    if (
        not normalized
        or _PLACEHOLDER_VALUE.fullmatch(normalized)
        or re.fullmatch(r"<[^>]+>", normalized)
    ):
        return None
    if len(value) > 4096 or any(ord(char) < 32 and char not in "\t" for char in value):
        raise ValueError(f"{field} is invalid.")
    return value


def _secure_local_file(path: Path) -> None:
    """Apply restrictive local permissions without making portability a failure."""

    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)
    if os.name == "nt":
        username = os.environ.get("USERNAME", "").strip()
        if username:
            with contextlib.suppress(OSError, subprocess.SubprocessError):
                subprocess.run(
                    [
                        "icacls",
                        str(path),
                        "/inheritance:r",
                        "/grant:r",
                        f"{username}:(R,W)",
                    ],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )


def _secure_local_directory(path: Path) -> None:
    """Restrict a local state directory while retaining search permission."""

    with contextlib.suppress(OSError):
        os.chmod(path, 0o700)
    if os.name == "nt":
        username = os.environ.get("USERNAME", "").strip()
        if username:
            with contextlib.suppress(OSError, subprocess.SubprocessError):
                subprocess.run(
                    [
                        "icacls",
                        str(path),
                        "/inheritance:r",
                        "/grant:r",
                        f"{username}:(OI)(CI)(F)",
                    ],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )


def _connection_allowed_paths() -> set[Path]:
    return {
        _connection_profile_path().resolve(),
        _connection_credentials_path().resolve(),
        *(_connection_environment_path(environment).resolve() for environment in ENVIRONMENTS),
    }


def _require_connection_target_path(path: Path) -> Path:
    resolved = _within(path, CONFIG_ROOT)
    if resolved not in _connection_allowed_paths():
        raise ValueError("Connection settings writes are limited to fixed local config paths.")
    return resolved


def _connection_transaction_journal_path() -> Path:
    return _within(CONFIG_ROOT / _CONNECTION_TRANSACTION_JOURNAL_NAME, CONFIG_ROOT)


def _write_private_bytes(path: Path, payload: bytes) -> None:
    """Write a private transaction artifact without printing its contents."""

    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    _secure_local_file(path)


def _json_payload_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _replace_connection_target(staged: Path, target: Path) -> None:
    """Replace a target once staging and the recovery journal are durable.

    The small wrapper is intentionally a local failpoint seam for transaction
    tests. It receives only fixed local paths and never logs their contents.
    """

    os.replace(staged, target)
    _secure_local_file(target)


def _transaction_backup_path(transaction_id: str, index: int) -> Path:
    return _within(
        CONFIG_ROOT / f".connection-settings-{transaction_id}-{index}.bak", CONFIG_ROOT
    )


def _transaction_staging_path(target: Path, transaction_id: str) -> Path:
    return _within(
        target.with_name(f".{target.name}.{transaction_id}.tmp"), CONFIG_ROOT
    )


def _remove_path(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def _write_connection_transaction_journal(entries: list[dict[str, Any]]) -> Path:
    """Persist only recovery metadata; secret bytes remain in private backups."""

    journal = _connection_transaction_journal_path()
    if journal.exists():
        raise RuntimeError("A previous connection settings transaction requires recovery.")
    payload = {
        "schema": _CONNECTION_TRANSACTION_SCHEMA,
        "entries": [
            {
                "target": entry["target"].name,
                "backup": entry["backup"].name if entry["backup"] else None,
                "existed": entry["existed"],
            }
            for entry in entries
        ],
    }
    staged = journal.with_name(f".{journal.name}.{uuid.uuid4().hex}.tmp")
    try:
        _write_private_bytes(staged, _json_payload_bytes(payload))
        os.replace(staged, journal)
        _secure_local_file(journal)
    finally:
        _remove_path(staged)
    return journal


def _load_connection_transaction_journal() -> tuple[Path, list[dict[str, Any]]] | None:
    journal = _connection_transaction_journal_path()
    if not journal.exists():
        return None
    try:
        payload = _read_json(journal)
        if payload.get("schema") != _CONNECTION_TRANSACTION_SCHEMA:
            raise ValueError("unexpected schema")
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list) or not raw_entries:
            raise ValueError("entries are required")
        by_name = {path.name: path for path in _connection_allowed_paths()}
        entries: list[dict[str, Any]] = []
        seen_targets: set[Path] = set()
        for raw in raw_entries:
            if not isinstance(raw, Mapping) or not isinstance(raw.get("target"), str):
                raise ValueError("invalid target")
            target = by_name.get(raw["target"])
            existed = raw.get("existed")
            backup_name = raw.get("backup")
            if target is None or target in seen_targets or not isinstance(existed, bool):
                raise ValueError("invalid journal entry")
            if existed:
                if (
                    not isinstance(backup_name, str)
                    or not _CONNECTION_BACKUP_NAME.fullmatch(backup_name)
                ):
                    raise ValueError("invalid backup")
                backup = _within(CONFIG_ROOT / backup_name, CONFIG_ROOT)
                if not backup.is_file():
                    raise ValueError("backup is missing")
            elif backup_name is not None:
                raise ValueError("unexpected backup")
            else:
                backup = None
            seen_targets.add(target)
            entries.append({"target": target, "backup": backup, "existed": existed})
        return journal, entries
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("Local connection settings recovery metadata is invalid.") from exc


def _restore_connection_transaction(entries: Iterable[Mapping[str, Any]]) -> None:
    """Restore every target exactly to its pre-transaction bytes or absence."""

    errors: list[BaseException] = []
    for entry in entries:
        target = _require_connection_target_path(Path(entry["target"]))
        try:
            if entry["existed"]:
                backup = entry.get("backup")
                if not isinstance(backup, Path) or not backup.is_file():
                    raise RuntimeError("Connection settings backup is unavailable.")
                restore_staging = target.with_name(
                    f".{target.name}.{uuid.uuid4().hex}.restore.tmp"
                )
                try:
                    _write_private_bytes(restore_staging, backup.read_bytes())
                    os.replace(restore_staging, target)
                    _secure_local_file(target)
                finally:
                    _remove_path(restore_staging)
            else:
                _remove_path(target)
        except (OSError, RuntimeError) as exc:
            errors.append(exc)
    if errors:
        raise RuntimeError("Connection settings rollback was incomplete.") from errors[0]


def _cleanup_connection_transaction(
    journal: Path | None, entries: Iterable[Mapping[str, Any]], staged: Iterable[Path]
) -> None:
    for path in staged:
        with contextlib.suppress(OSError):
            _remove_path(path)
    if journal is not None:
        _remove_path(journal)
    for entry in entries:
        backup = entry.get("backup")
        if isinstance(backup, Path):
            with contextlib.suppress(OSError):
                _remove_path(backup)


def _recover_connection_settings_transaction() -> None:
    """Fail closed or restore a transaction interrupted before its final cleanup."""

    recovered = _load_connection_transaction_journal()
    if recovered is None:
        return
    journal, entries = recovered
    _restore_connection_transaction(entries)
    _cleanup_connection_transaction(journal, entries, ())


def _commit_connection_settings_transaction(
    updates: Iterable[tuple[Path, Mapping[str, Any]]],
) -> None:
    """Commit fixed local settings as a compensating, journaled transaction."""

    transaction_id = uuid.uuid4().hex
    entries: list[dict[str, Any]] = []
    staged: list[Path] = []
    journal: Path | None = None
    try:
        for index, (raw_target, payload) in enumerate(updates):
            target = _require_connection_target_path(raw_target)
            stage = _transaction_staging_path(target, transaction_id)
            _write_private_bytes(stage, _json_payload_bytes(payload))
            staged.append(stage)
            existed = target.exists()
            backup = _transaction_backup_path(transaction_id, index) if existed else None
            if backup is not None:
                _write_private_bytes(backup, target.read_bytes())
            entries.append(
                {
                    "target": target,
                    "stage": stage,
                    "backup": backup,
                    "existed": existed,
                }
            )
        if not entries:
            raise ValueError("At least one connection settings update is required.")
        journal = _write_connection_transaction_journal(entries)
        for entry in entries:
            # Record intent before replacement: a filesystem error can occur
            # after the replacement has reached disk.
            _replace_connection_target(entry["stage"], entry["target"])
        _remove_path(journal)
        journal = None
        _cleanup_connection_transaction(None, entries, staged)
    except Exception as commit_error:
        if journal is not None:
            try:
                _restore_connection_transaction(entries)
            except Exception as restore_error:
                # Keep the journal and private backups for the next authorized
                # update to recover. Never attach sensitive bytes to the error.
                raise RuntimeError(
                    "Connection settings update failed and requires local recovery."
                ) from restore_error
            _cleanup_connection_transaction(journal, entries, staged)
        else:
            _cleanup_connection_transaction(None, entries, staged)
        raise RuntimeError("Connection settings update failed; no changes were kept.") from commit_error


def _connection_database_metadata(
    environment: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    database = payload.get("database")
    if not isinstance(database, Mapping):
        return {**_connection_error(), "environment": environment, "database": {}}
    try:
        dbname = database.get("dbname")
        if dbname != _DATABASE_NAMES[environment]:
            raise ValueError("database name is not allowed")
        host = _require_text(
            database.get("host"), field="database.host", pattern=_HOST_PATTERN, maximum=253
        )
        port = _require_port(database.get("port"), field="database.port")
        user = _require_text(
            database.get("user"), field="database.user", pattern=_USER_PATTERN, maximum=128
        )
    except ValueError:
        return {**_connection_error(), "environment": environment, "database": {}}
    return {
        "status": "configured",
        "environment": environment,
        "database": {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password_configured": _is_configured_secret(database.get("password")),
        },
    }


def get_connection_settings() -> dict[str, Any]:
    """Read safe connection metadata only; passwords are always write-only.

    This intentionally never calls ``_load_remote_profile`` because that helper
    creates a password-bearing mapping for remote actions.
    """

    result: dict[str, Any] = {
        "status": "unavailable",
        "connection": {},
        "environments": {},
    }
    try:
        if _connection_transaction_journal_path().exists():
            raise ValueError("Local connection settings recovery is pending.")
        profile = _read_json(_connection_profile_path())
        credentials = _read_json(_connection_credentials_path())
        _require_fixed_credentials_reference(profile)
        remote_root = _require_remote_root(
            profile.get("remote_root") or credentials.get("remote_root")
        )
        host = _require_text(
            credentials.get("host"), field="host", pattern=_HOST_PATTERN, maximum=253
        )
        port = _require_port(credentials.get("sftp_port", 22), field="sftp_port")
        username = _require_text(
            credentials.get("username"),
            field="username",
            pattern=_USER_PATTERN,
            maximum=128,
        )
        pinned_admin_tools_version = str(
            profile.get("admin_tools_release_version", "")
        ).strip()
        development_admin_tools_version = str(
            profile.get("admin_tools_development_release_version", "")
        ).strip()
        admin_tools_release_configured = bool(
            _MASTER_ADMIN_RELEASE_VERSION.fullmatch(pinned_admin_tools_version)
        )
        admin_tools_development_release_configured = bool(
            _MASTER_ADMIN_RELEASE_VERSION.fullmatch(development_admin_tools_version)
        )
        if not admin_tools_development_release_configured:
            development_release_mode = ""
        elif (
            admin_tools_release_configured
            and development_admin_tools_version == pinned_admin_tools_version
        ):
            development_release_mode = "clean"
        else:
            # The actual dirty/clean policy is proven again from the immutable
            # local artifact, receipt, and remote inventory before execution.
            development_release_mode = "development-verification"
        result["connection"] = {
            "host": host,
            "port": port,
            "username": username,
            "remote_root": remote_root,
            "password_configured": _is_configured_secret(credentials.get("password")),
            "read_only": credentials.get("read_only") is True,
            "allow_upload": profile.get("allow_upload") is True,
            "allow_development_secure_config_upload": (
                profile.get("allow_development_secure_config_upload") is True
            ),
            "allow_production_secure_config_upload": (
                profile.get("allow_production_secure_config_upload") is True
            ),
            "allow_service_control": profile.get("allow_service_control") is True,
            "admin_tools_release_version": (
                pinned_admin_tools_version if admin_tools_release_configured else ""
            ),
            "admin_tools_release_configured": admin_tools_release_configured,
            "admin_tools_development_release_version": (
                development_admin_tools_version
                if admin_tools_development_release_configured
                else ""
            ),
            "admin_tools_development_release_configured": (
                admin_tools_development_release_configured
            ),
            "admin_tools_development_release_mode": development_release_mode,
        }
        result["status"] = "configured"
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        result["connection"] = _connection_error()

    for environment in sorted(ENVIRONMENTS):
        try:
            result["environments"][environment] = _connection_database_metadata(
                environment, _read_json(_connection_environment_path(environment))
            )
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            result["environments"][environment] = {
                **_connection_error(),
                "environment": environment,
                "database": {},
            }
    if result["status"] == "configured" and all(
        item.get("status") == "configured"
        for item in result["environments"].values()
    ):
        result["status"] = "configured"
    elif result["status"] == "configured":
        result["status"] = "partial"
    return result


def _update_connection_settings(
    *,
    ssh_sftp: Mapping[str, Any] | None = None,
    databases: Mapping[str, Mapping[str, Any]] | None = None,
    confirm_production: bool = False,
) -> dict[str, Any]:
    """Safely update fixed ignored settings files, preserving unknown fields.

    ``ssh_sftp`` accepts host, port, username, remote_root, and optional
    password. ``databases`` accepts exactly one development or production entry with
    host, port, dbname, user, and optional password. Empty or placeholder
    passwords retain the current value. Any SSH/SFTP or production database
    change requires the UI's explicit production confirmation.
    """

    if ssh_sftp is None and databases is None:
        raise ValueError("At least one connection settings update is required.")
    if ssh_sftp is not None:
        ssh_sftp = _require_mapping_update(ssh_sftp, field="ssh_sftp")
    if databases is not None:
        databases = _require_mapping_update(databases, field="databases")
        unknown_environments = set(databases) - ENVIRONMENTS
        if unknown_environments:
            raise ValueError("databases contains an unsupported environment.")
        if len(databases) != 1:
            raise ValueError("databases must contain exactly one environment.")

    changes_production_target = ssh_sftp is not None or (
        databases is not None and "production" in databases
    )
    if changes_production_target and confirm_production is not True:
        raise PermissionError("Production settings changes require explicit confirmation.")

    profile_path = _connection_profile_path()
    credentials_path = _connection_credentials_path()
    profile = _read_json(profile_path)
    credentials = _read_json(credentials_path)
    if not isinstance(profile, dict) or not isinstance(credentials, dict):
        raise ValueError("Local connection settings must be JSON objects.")
    _require_fixed_credentials_reference(profile)

    updated_profile = dict(profile)
    updated_credentials = dict(credentials)
    write_profile = False
    write_credentials = False
    if ssh_sftp is not None:
        required_keys = {"host", "port", "username", "remote_root"}
        missing_keys = required_keys - set(ssh_sftp)
        if missing_keys:
            raise ValueError("ssh_sftp is missing required connection fields.")
        updated_credentials["host"] = _require_text(
            ssh_sftp["host"], field="host", pattern=_HOST_PATTERN, maximum=253
        )
        updated_credentials["sftp_port"] = _require_port(
            ssh_sftp["port"], field="port"
        )
        updated_credentials["username"] = _require_text(
            ssh_sftp["username"],
            field="username",
            pattern=_USER_PATTERN,
            maximum=128,
        )
        updated_profile["remote_root"] = _require_remote_root(ssh_sftp["remote_root"])
        password = _password_update(ssh_sftp.get("password"), field="password")
        if password is not None:
            updated_credentials["password"] = password
        write_profile = True
        write_credentials = True

    environment_updates: list[tuple[Path, dict[str, Any]]] = []
    if databases is not None:
        for environment, submitted in databases.items():
            environment = _require_environment(environment)
            submitted = _require_mapping_update(
                submitted, field=f"databases.{environment}"
            )
            required_keys = {"host", "port", "dbname", "user"}
            missing_keys = required_keys - set(submitted)
            if missing_keys:
                raise ValueError(f"databases.{environment} is missing required database fields.")
            if submitted["dbname"] != _DATABASE_NAMES[environment]:
                raise ValueError(f"databases.{environment}.dbname is not allowed.")
            config_path = _connection_environment_path(environment)
            config = _read_json(config_path)
            database = config.get("database")
            if not isinstance(database, dict):
                raise ValueError(f"{environment} database configuration must be an object.")
            updated_database = dict(database)
            updated_database["host"] = _require_text(
                submitted["host"],
                field=f"databases.{environment}.host",
                pattern=_HOST_PATTERN,
                maximum=253,
            )
            updated_database["port"] = _require_port(
                submitted["port"], field=f"databases.{environment}.port"
            )
            updated_database["dbname"] = _DATABASE_NAMES[environment]
            updated_database["user"] = _require_text(
                submitted["user"],
                field=f"databases.{environment}.user",
                pattern=_USER_PATTERN,
                maximum=128,
            )
            password = _password_update(
                submitted.get("password"), field=f"databases.{environment}.password"
            )
            if password is not None:
                updated_database["password"] = password
            updated_config = dict(config)
            updated_config["database"] = updated_database
            environment_updates.append((config_path, updated_config))

    # Validate and assemble every payload before the first replacement. The
    # transaction backs up exact source bytes and compensates every attempted
    # target if a later replacement fails.
    transaction_updates: list[tuple[Path, Mapping[str, Any]]] = []
    if write_credentials:
        transaction_updates.append((credentials_path, updated_credentials))
    if write_profile:
        transaction_updates.append((profile_path, updated_profile))
    transaction_updates.extend(environment_updates)
    _commit_connection_settings_transaction(transaction_updates)
    return get_connection_settings()


def update_connection_settings(
    *,
    ssh_sftp: Mapping[str, Any] | None = None,
    databases: Mapping[str, Mapping[str, Any]] | None = None,
    confirm_production: bool = False,
) -> dict[str, Any]:
    """Update settings through a recovered, logical local transaction.

    Passwords remain write-only. If a local process previously stopped during
    replacement, its journal is restored before this new authorized update is
    evaluated. A database request may target exactly one environment.
    """

    with _CONNECTION_SETTINGS_LOCK:
        _recover_connection_settings_transaction()
        return _update_connection_settings(
            ssh_sftp=ssh_sftp,
            databases=databases,
            confirm_production=confirm_production,
        )


def _target_id(profile: Mapping[str, Any], environment: str) -> str:
    """Bind a deployment receipt to one non-secret remote target identity."""

    identity = {
        "host": str(profile.get("host", "")).strip().lower(),
        "port": int(profile.get("port", 22)),
        "username": str(profile.get("username", "")).strip().casefold(),
        "remote_root": posixpath.normpath(str(profile.get("remote_root", "")).strip()),
        "environment": _require_environment(environment),
    }
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _redact(value: object) -> str:
    text = str(value or "")
    text = re.sub(
        r"(?i)([\"']?(?:password|passwd|pwd|secret|token|api[_-]?key)[\"']?"
        r"\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^,\s}]+)",
        r"\1\"[REDACTED]\"",
        text,
    )
    text = re.sub(
        r"(?i)([a-z][a-z0-9+.-]*://)([^\s:/@]+):([^\s@]+)@",
        r"\1[REDACTED]@",
        text,
    )
    return re.sub(r"(?i)(Bearer|Basic)\s+\S+", r"\1 [REDACTED]", text)


class ParamikoRemote:
    """Small SFTP/SSH adapter; tests replace the session factory with a fake."""

    def __init__(self, profile: Mapping[str, Any]) -> None:
        self.profile = profile
        self.client: Any = None
        self.sftp: Any = None

    def __enter__(self) -> "ParamikoRemote":
        import paramiko

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        client.connect(
            self.profile["host"],
            port=int(self.profile["port"]),
            username=self.profile["username"],
            password=self.profile["password"],
            timeout=10,
            banner_timeout=10,
            auth_timeout=10,
            allow_agent=False,
            look_for_keys=False,
        )
        self.client = client
        self.sftp = client.open_sftp()
        return self

    def __exit__(self, *_: object) -> None:
        if self.sftp is not None:
            self.sftp.close()
        if self.client is not None:
            self.client.close()

    def _allowlisted_remote_path(self, path: str) -> str:
        """Check lexical containment before using a remote path in any API."""

        raw_path = str(path)
        remote_path = PurePosixPath(raw_path)
        configured_root = str(self.profile.get("remote_root") or "").strip()
        allowed_root = PurePosixPath(configured_root)
        if (
            not raw_path
            or not configured_root
            or not remote_path.is_absolute()
            or not allowed_root.is_absolute()
            or ".." in remote_path.parts
            or ".." in allowed_root.parts
            or any(ord(char) < 32 for char in raw_path)
            or not remote_path.is_relative_to(allowed_root)
        ):
            raise ValueError("Remote path must be within the configured safe POSIX root.")
        return remote_path.as_posix()

    def _canonical_remote_path(self, path: str, *, target_must_exist: bool) -> str:
        """Resolve a target (or its parent) remotely and reject symlink escapes."""

        safe_path = self._allowlisted_remote_path(path)
        configured_root = PurePosixPath(str(self.profile["remote_root"]).strip()).as_posix()
        candidate = safe_path if target_must_exist else str(PurePosixPath(safe_path).parent)
        quoted_root = shlex.quote(configured_root)
        quoted_candidate = shlex.quote(candidate)
        command = (
            f"canonical_root=$(readlink -f -- {quoted_root}) || exit 40; "
            f"canonical_target=$(readlink -f -- {quoted_candidate}) || exit 41; "
            'case "$canonical_target" in "$canonical_root"|"$canonical_root"/*) ;; *) exit 42;; esac; '
            "printf '%s\\n%s\\n' \"$canonical_root\" \"$canonical_target\""
        )
        code, stdout, _ = self.execute(command, timeout=45)
        lines = stdout.splitlines()
        if code != 0 or len(lines) != 2:
            raise RuntimeError("Remote canonical path validation failed.")
        canonical_root = PurePosixPath(lines[0])
        canonical_target = PurePosixPath(lines[1])
        if (
            not canonical_root.is_absolute()
            or not canonical_target.is_absolute()
            or ".." in canonical_root.parts
            or ".." in canonical_target.parts
            or not canonical_target.is_relative_to(canonical_root)
        ):
            raise RuntimeError("Remote canonical path validation failed.")
        # The canonical value is a guard result, not an SFTP replacement path:
        # retain the allowlisted lexical path so a configured root that is
        # itself a symlink remains usable after its containment check.
        return safe_path

    def _canonical_write_target(self, path: str) -> str:
        """Return a safe write target and reject every existing symlink leaf."""

        safe_path = self._allowlisted_remote_path(path)
        try:
            leaf = self.sftp.lstat(safe_path)
        except OSError as exc:
            if not _is_remote_not_found(exc):
                raise
            self._canonical_remote_path(safe_path, target_must_exist=False)
            return safe_path
        if stat.S_ISLNK(int(leaf.st_mode)):
            raise ValueError("Remote write target must not be a symlink.")
        return self._canonical_remote_path(safe_path, target_must_exist=True)

    def exists(self, path: str) -> bool:
        safe_path = self._allowlisted_remote_path(path)
        try:
            self.sftp.stat(safe_path)
            return True
        except OSError as exc:
            if _is_remote_not_found(exc):
                return False
            raise

    def read_bytes(self, path: str, *, maximum_bytes: int) -> bytes:
        """Read one bounded, allowlisted remote control file safely."""

        if maximum_bytes < 1:
            raise ValueError("maximum_bytes must be positive.")
        safe_path = self._allowlisted_remote_path(path)
        try:
            leaf = self.sftp.lstat(safe_path)
        except OSError as exc:
            if _is_remote_not_found(exc):
                raise FileNotFoundError("Required remote control file is missing.") from exc
            raise
        if stat.S_ISLNK(int(leaf.st_mode)):
            raise ValueError("Remote control file must not be a symlink.")
        metadata = self.sftp.stat(safe_path)
        if int(metadata.st_size) > maximum_bytes:
            raise ValueError("Remote control file exceeds the allowed size.")
        canonical_path = self._canonical_remote_path(safe_path, target_must_exist=True)
        with self.sftp.open(canonical_path, "rb") as stream:
            payload = stream.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise ValueError("Remote control file exceeds the allowed size.")
        return bytes(payload)

    def sha256(self, path: str) -> str | None:
        """Return one remote SHA-256 without transferring the file over SFTP.

        The deployment preview may inspect large server-data packages.  Hashing
        on the remote host keeps the checksum comparison read-only while
        returning only the digest, rather than streaming a multi-hundred-MiB
        file through the operator workstation.  The path is POSIX-normalized
        and shell-quoted before the fixed command is executed.
        """

        safe_path = self._allowlisted_remote_path(path)
        try:
            leaf = self.sftp.lstat(safe_path)
        except OSError as exc:
            if _is_remote_not_found(exc):
                return None
            raise
        if stat.S_ISLNK(int(leaf.st_mode)):
            raise ValueError("Remote checksum target must not be a symlink.")
        try:
            self.sftp.stat(safe_path)
        except OSError as exc:
            if _is_remote_not_found(exc):
                raise RuntimeError("Remote checksum target disappeared after lstat.") from exc
            raise
        canonical_path = self._canonical_remote_path(safe_path, target_must_exist=True)
        quoted_path = shlex.quote(canonical_path)
        command = f"sha256sum -- {quoted_path}"
        code, stdout, _ = self.execute(command, timeout=45)
        output = stdout.strip()
        if code != 0:
            raise RuntimeError("Remote checksum command failed.")
        match = _REMOTE_SHA256_OUTPUT.fullmatch(output)
        if not match:
            raise RuntimeError("Remote checksum command returned invalid output.")
        return match.group("sha256").lower()

    def size(self, path: str) -> int | None:
        safe_path = self._allowlisted_remote_path(path)
        try:
            stat = self.sftp.stat(safe_path)
        except OSError as exc:
            if _is_remote_not_found(exc):
                return None
            raise
        self._canonical_remote_path(safe_path, target_must_exist=True)
        return int(stat.st_size)

    def mode(self, path: str) -> int | None:
        safe_path = self._allowlisted_remote_path(path)
        try:
            stat = self.sftp.stat(safe_path)
        except OSError as exc:
            if _is_remote_not_found(exc):
                return None
            raise
        self._canonical_remote_path(safe_path, target_must_exist=True)
        return int(stat.st_mode) & 0o777

    def chmod(self, path: str, mode: int) -> None:
        self.sftp.chmod(self._canonical_remote_path(path, target_must_exist=True), mode)

    def mkdirs(self, directory: str, mode: int = 0o750) -> None:
        safe_directory = self._allowlisted_remote_path(directory)
        root = PurePosixPath(str(self.profile["remote_root"]).strip())
        self._canonical_remote_path(root.as_posix(), target_must_exist=True)
        current = root
        for part in PurePosixPath(safe_directory).relative_to(root).parts:
            current /= part
            current_path = current.as_posix()
            try:
                self.sftp.stat(current_path)
            except OSError as exc:
                if not _is_remote_not_found(exc):
                    raise
                self._canonical_remote_path(current_path, target_must_exist=False)
                self.sftp.mkdir(current_path, mode=mode)
            else:
                self._canonical_remote_path(current_path, target_must_exist=True)

    def copy(self, source: str, target: str) -> None:
        safe_source = self._canonical_remote_path(source, target_must_exist=True)
        safe_target = self._allowlisted_remote_path(target)
        self.mkdirs(posixpath.dirname(safe_target))
        safe_target = self._canonical_write_target(safe_target)
        with self.sftp.open(safe_source, "rb") as reader, self.sftp.open(safe_target, "wb") as writer:
            for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                writer.write(chunk)

    def remove(self, path: str) -> None:
        """Remove only a path selected by internal upload compensation logic."""

        self.sftp.remove(self._canonical_remote_path(path, target_must_exist=True))

    def upload_atomic(self, local: Path, target: str, backup: str | None) -> None:
        target = self._allowlisted_remote_path(target)
        self.mkdirs(posixpath.dirname(target))
        target = self._canonical_write_target(target)
        if backup and self.exists(target):
            self.copy(target, backup)
        temporary = self._canonical_write_target(f"{target}.uploading.{uuid.uuid4().hex}")
        self.sftp.put(str(local), temporary)
        if self.sha256(temporary) != _sha256_file(local):
            with contextlib.suppress(OSError):
                self.sftp.remove(temporary)
            raise RuntimeError("Remote upload checksum mismatch.")
        try:
            self.sftp.posix_rename(temporary, target)
        except Exception:
            displaced = self._canonical_write_target(f"{target}.replacing.{uuid.uuid4().hex}")
            if self.exists(target):
                self.sftp.rename(target, displaced)
            try:
                self.sftp.rename(temporary, target)
            except Exception:
                if self.exists(displaced):
                    self.sftp.rename(displaced, target)
                raise
            with contextlib.suppress(OSError):
                self.sftp.remove(displaced)

    def upload_bytes_atomic(self, payload: bytes, target: str, backup: str | None) -> None:
        """Atomically upload a private in-memory configuration payload.

        Unlike runtime artifacts, secure configuration is never materialized as a
        staging file.  The temporary and final remote files are locked to 0600.
        """

        checksum = hashlib.sha256(payload).hexdigest()
        target = self._allowlisted_remote_path(target)
        self.mkdirs(posixpath.dirname(target))
        target = self._canonical_write_target(target)
        if backup and self.exists(target):
            self.copy(target, backup)
            self.chmod(backup, _SECURE_CONFIG_MODE)
        temporary = self._canonical_write_target(f"{target}.uploading.{uuid.uuid4().hex}")
        try:
            # Exclusive creation plus chmod-before-write prevents the remote
            # umask/default mode from exposing any CSV/config bytes, even
            # briefly while the transfer is still in progress.
            with self.sftp.open(temporary, "wx") as stream:
                self.chmod(temporary, _SECURE_CONFIG_MODE)
                source = io.BytesIO(payload)
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    stream.write(chunk)
                stream.flush()
            if self.sha256(temporary) != checksum:
                raise RuntimeError("Remote secure-config checksum mismatch.")
            try:
                self.sftp.posix_rename(temporary, target)
            except Exception:
                displaced = self._canonical_write_target(
                    f"{target}.replacing.{uuid.uuid4().hex}"
                )
                if self.exists(target):
                    self.sftp.rename(target, displaced)
                try:
                    self.sftp.rename(temporary, target)
                except Exception:
                    if self.exists(displaced):
                        self.sftp.rename(displaced, target)
                    raise
                with contextlib.suppress(OSError):
                    self.sftp.remove(displaced)
            self.chmod(target, _SECURE_CONFIG_MODE)
        except Exception:
            with contextlib.suppress(OSError):
                if self.exists(temporary):
                    self.sftp.remove(temporary)
            raise

    @contextlib.contextmanager
    def deployment_lock(self, base: str, deployment_id: str) -> Iterator[None]:
        self.mkdirs(base)
        lock = self._canonical_write_target(
            posixpath.join(base, ".deployment-console.lock")
        )
        stream: Any = None
        try:
            stream = self.sftp.open(lock, "wx")
        except OSError as exc:
            raise RuntimeError("Another deployment holds the remote lock.") from exc
        try:
            payload = str(deployment_id).encode("utf-8")
            if not payload:
                raise ValueError("deployment_id must not be empty")
            with stream:
                stream.write(payload)
                stream.flush()
            if int(self.sftp.stat(lock).st_size) != len(payload):
                raise RuntimeError("Deployment lock content was not persisted.")
        except Exception as exc:
            # Exclusive open succeeded, so this session owns this path. Clean up
            # only that newly-created lock; an open failure above never removes
            # another deployment's existing lock.
            with contextlib.suppress(OSError):
                self.sftp.remove(lock)
            raise RuntimeError("Unable to initialize the remote deployment lock.") from exc
        try:
            yield
        finally:
            with contextlib.suppress(OSError):
                self.sftp.remove(lock)

    def execute(self, command: str, timeout: int = 45) -> tuple[int, str, str]:
        _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        code = stdout.channel.recv_exit_status()
        return code, _redact(stdout.read().decode(errors="replace")), _redact(
            stderr.read().decode(errors="replace")
        )

    def execute_master_json(self, command: str, timeout: int = 45) -> tuple[int, str, str]:
        """Execute only the fixed master-data module and preserve JSON stdout.

        The ordinary SSH executor redacts all output immediately.  That is the
        safe default, but it destroys the one-time confirmation token before
        the JSON boundary can extract it.  This narrow path accepts only the
        exact command skeleton produced by ``_master_admin_command``; stdout is
        size-bounded and strictly UTF-8 decoded, while stderr remains redacted.
        JSON/envelope/target checks happen immediately in
        ``_parse_master_admin_json`` before any value is returned to a caller.
        """

        if not isinstance(command, str) or any(ord(char) < 32 for char in command):
            raise ValueError("Remote master-data command is invalid.")
        try:
            tokens = shlex.split(command, posix=True)
        except ValueError as exc:
            raise ValueError("Remote master-data command is invalid.") from exc
        expected_argument_flags = {
            "overview": (),
            "list-specs": (),
            "preview": ("--table", "--csv"),
            "apply": (
                "--preview-id",
                "--preview-digest",
                "--idempotency-key",
                "--confirmation",
            ),
            "receipt": ("--operation-id",),
        }
        if (
            len(tokens) < 13
            or tokens[0:2] != ["cd", "--"]
            or tokens[3:5] != ["&&", "exec"]
            # -B is mandatory: the release directory is immutable and an
            # ordinary module invocation would create __pycache__ entries,
            # intentionally tripping the exact-inventory release guard.
            or tokens[6:10]
            != ["-B", "-m", "admin_tools.db.master_data_backend", "--json"]
            or tokens[10] not in _MASTER_ADMIN_COMMANDS
            or tokens[11] != "--config"
        ):
            raise ValueError("Remote command is not an allowlisted master-data JSON command.")
        operation = tokens[10]
        trailing = tokens[13:]
        required_flags = expected_argument_flags[operation]
        if len(trailing) != len(required_flags) * 2 or tuple(trailing[0::2]) != required_flags:
            raise ValueError("Remote command arguments are not allowlisted for this operation.")
        canonical = f"cd -- {shlex.quote(tokens[2])} && exec " + " ".join(
            shlex.quote(token) for token in tokens[5:]
        )
        if command != canonical:
            raise ValueError("Remote master-data command is not canonically shell-quoted.")
        _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        stdout_bytes = stdout.read(_MASTER_ADMIN_MAX_JSON_BYTES + 1)
        if not isinstance(stdout_bytes, bytes):
            stdout_bytes = str(stdout_bytes).encode("utf-8", errors="replace")
        if len(stdout_bytes) > _MASTER_ADMIN_MAX_JSON_BYTES:
            with contextlib.suppress(Exception):
                stdout.channel.close()
            raise RuntimeError("Remote DB admin result exceeds the safe response limit.")
        stderr_bytes = stderr.read(_BUILD_OUTPUT_LIMIT + 1)
        code = stdout.channel.recv_exit_status()
        try:
            raw_stdout = stdout_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise RuntimeError("Remote DB admin result is not valid UTF-8 JSON.") from exc
        if isinstance(stderr_bytes, bytes):
            raw_stderr = stderr_bytes.decode("utf-8", errors="replace")
        else:
            raw_stderr = str(stderr_bytes)
        return code, raw_stdout, _redact(raw_stderr)

    def execute_technician_profile_json(
        self, command: str, timeout: int = 45
    ) -> tuple[int, str, str]:
        """Execute only the fixed development technician-profile workflow.

        This is intentionally separate from the generic master-table executor:
        the source is an immutable managed workbook, not a staged caller CSV,
        and the composite backend owns its fixed master/capability/region
        mappings.  No table name, source path, or environment is caller
        selectable at this transport boundary.
        """

        if not isinstance(command, str) or any(ord(char) < 32 for char in command):
            raise ValueError("Remote technician-profile command is invalid.")
        try:
            tokens = shlex.split(command, posix=True)
        except ValueError as exc:
            raise ValueError("Remote technician-profile command is invalid.") from exc
        operation = tokens[10] if len(tokens) > 10 else ""
        if (
            len(tokens) < 13
            or tokens[0:2] != ["cd", "--"]
            or tokens[3:5] != ["&&", "exec"]
            or tokens[6:10]
            != ["-B", "-m", _TECHNICIAN_PROFILE_ADMIN_MODULE, "--json"]
            or operation not in _TECHNICIAN_PROFILE_ADMIN_COMMANDS
            or tokens[11] != "--config"
        ):
            raise ValueError(
                "Remote command is not an allowlisted technician-profile JSON command."
            )
        root = PurePosixPath(str(self.profile.get("remote_root", "")).strip())
        expected_release_parent = root / "admin_tools" / "releases"
        expected_python = root / "development" / ".venv" / "bin" / "python"
        expected_config = root / "development" / "config_common_vrp.dev.json"
        if (
            not root.is_absolute()
            or ".." in root.parts
            or PurePosixPath(tokens[2]).parent != expected_release_parent
            or not _MASTER_ADMIN_RELEASE_VERSION.fullmatch(PurePosixPath(tokens[2]).name)
            or tokens[5] != expected_python.as_posix()
            or tokens[12] != expected_config.as_posix()
        ):
            raise ValueError("Remote technician-profile command arguments are invalid.")
        trailing = tokens[13:]
        if operation == "preview":
            if (
                len(trailing) != 8
                or tuple(trailing[0::2])
                != (
                    "--source",
                    "--source-sha256",
                    "--managed-version",
                    "--environment",
                )
            ):
                raise ValueError(
                    "Remote technician-profile preview arguments are not allowlisted."
                )
            version = trailing[3]
            expected_source = (
                root
                / "state"
                / _TECHNICIAN_PROFILE_SCOPE
                / "managed_data"
                / _TECHNICIAN_PROFILE_DATASET
                / version
                / "payload.xlsx"
            )
            if (
                not _MANAGED_DATA_VERSION.fullmatch(version)
                or trailing[1] != expected_source.as_posix()
                or trailing[5] != version
                or trailing[7] != _TECHNICIAN_PROFILE_SCOPE
            ):
                raise ValueError("Remote technician-profile source binding is invalid.")
        else:
            if (
                len(trailing) != 10
                or tuple(trailing[0::2])
                != (
                    "--preview-id",
                    "--preview-digest",
                    "--idempotency-key",
                    "--confirmation",
                    "--environment",
                )
            ):
                raise ValueError(
                    "Remote technician-profile apply arguments are not allowlisted."
                )
            try:
                uuid.UUID(trailing[5])
            except (ValueError, AttributeError, TypeError) as exc:
                raise ValueError("Remote technician-profile idempotency key is invalid.") from exc
            if (
                not _MASTER_ADMIN_PREVIEW_ID.fullmatch(trailing[1])
                or not _MASTER_ADMIN_DIGEST.fullmatch(trailing[3])
                or not trailing[7]
                or len(trailing[7]) > 512
                or any(ord(char) < 32 for char in trailing[7])
                or trailing[9] != _TECHNICIAN_PROFILE_SCOPE
            ):
                raise ValueError("Remote technician-profile apply binding is invalid.")
        canonical = f"cd -- {shlex.quote(tokens[2])} && exec " + " ".join(
            shlex.quote(token) for token in tokens[5:]
        )
        if command != canonical:
            raise ValueError(
                "Remote technician-profile command is not canonically shell-quoted."
            )
        _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        stdout_bytes = stdout.read(_MASTER_ADMIN_MAX_JSON_BYTES + 1)
        if not isinstance(stdout_bytes, bytes):
            stdout_bytes = str(stdout_bytes).encode("utf-8", errors="replace")
        if len(stdout_bytes) > _MASTER_ADMIN_MAX_JSON_BYTES:
            with contextlib.suppress(Exception):
                stdout.channel.close()
            raise RuntimeError("Remote technician-profile result exceeds the safe response limit.")
        stderr_bytes = stderr.read(_BUILD_OUTPUT_LIMIT + 1)
        code = stdout.channel.recv_exit_status()
        try:
            raw_stdout = stdout_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise RuntimeError(
                "Remote technician-profile result is not valid UTF-8 JSON."
            ) from exc
        if isinstance(stderr_bytes, bytes):
            raw_stderr = stderr_bytes.decode("utf-8", errors="replace")
        else:
            raw_stderr = str(stderr_bytes)
        return code, raw_stdout, _redact(raw_stderr)

    def execute_region_plan_json(self, command: str, timeout: int = 45) -> tuple[int, str, str]:
        """Execute one exact development-only region-plan Admin Tools command."""

        if not isinstance(command, str) or any(ord(char) < 32 for char in command):
            raise ValueError("Remote region-plan command is invalid.")
        try:
            tokens = shlex.split(command, posix=True)
        except ValueError as exc:
            raise ValueError("Remote region-plan command is invalid.") from exc
        operation = tokens[10] if len(tokens) > 10 else ""
        module = tokens[8] if len(tokens) > 8 else ""
        allowed_operation = (
            operation in (_REGION_PLAN_ADMIN_COMMANDS | _FIXED_REGION_PLAN_BUNDLE_COMMANDS)
            if module == _REGION_PLAN_ADMIN_MODULE
            else operation in {"preview", "reconcile"}
            if module == _REGION_PLAN_SCHEMA_ADMIN_MODULE
            else False
        )
        if (
            len(tokens) < 13
            or tokens[0:2] != ["cd", "--"]
            or tokens[3:5] != ["&&", "exec"]
            or tokens[6:8] != ["-B", "-m"]
            or tokens[9] != "--json"
            or not allowed_operation
            or tokens[11] != "--config"
        ):
            raise ValueError("Remote command is not an allowlisted region-plan JSON command.")
        root = PurePosixPath(str(self.profile.get("remote_root", "")).strip())
        expected_release_parent = root / "admin_tools" / "releases"
        expected_python = root / "development" / ".venv" / "bin" / "python"
        expected_config = root / "development" / "config_common_vrp.dev.json"
        if (
            not root.is_absolute()
            or ".." in root.parts
            or PurePosixPath(tokens[2]).parent != expected_release_parent
            or not _MASTER_ADMIN_RELEASE_VERSION.fullmatch(PurePosixPath(tokens[2]).name)
            or tokens[5] != expected_python.as_posix()
            or tokens[12] != expected_config.as_posix()
        ):
            raise ValueError("Remote region-plan command arguments are invalid.")
        trailing = tokens[13:]
        if module == _REGION_PLAN_SCHEMA_ADMIN_MODULE:
            if operation == "preview" and trailing:
                raise ValueError("Remote region-plan schema preview arguments are invalid.")
            if operation == "reconcile" and trailing != [
                "--confirmation", _REGION_PLAN_SCHEMA_CONFIRMATION
            ]:
                raise ValueError("Remote region-plan schema reconcile arguments are invalid.")
        elif operation in _FIXED_REGION_PLAN_BUNDLE_COMMANDS:
            if operation == "stage-bundle":
                if (
                    len(trailing) != 6
                    or tuple(trailing[0::2])
                    != ("--source", "--bundle-sha256", "--managed-version")
                ):
                    raise ValueError(
                        "Remote fixed Region Data bundle arguments are not allowlisted."
                    )
                bundle_sha256 = trailing[3]
                expected_bundle = (
                    root
                    / "state"
                    / _FIXED_REGION_PLAN_BUNDLE_SCOPE
                    / "managed_data"
                    / _FIXED_REGION_PLAN_BUNDLE_DATASET
                    / bundle_sha256
                    / "payload.zip"
                )
                if (
                    not _MANAGED_DATA_VERSION.fullmatch(bundle_sha256)
                    or trailing[5] != bundle_sha256
                    or trailing[1] != expected_bundle.as_posix()
                ):
                    raise ValueError(
                        "Remote fixed Region Data bundle binding is invalid."
                    )
            elif operation == "status-bundle":
                if (
                    len(trailing) != 4
                    or tuple(trailing[0::2])
                    != ("--bundle-sha256", "--managed-version")
                    or not _MANAGED_DATA_VERSION.fullmatch(trailing[1])
                    or trailing[3] != trailing[1]
                ):
                    raise ValueError(
                        "Remote fixed Region Data status arguments are not allowlisted."
                    )
            else:
                if (
                    len(trailing) != 4
                    or tuple(trailing[0::2]) != ("--request", "--request-sha256")
                ):
                    raise ValueError(
                        "Remote fixed Region Data import arguments are not allowlisted."
                    )
                request_sha256 = trailing[3]
                expected_request = (
                    root
                    / "state"
                    / _FIXED_REGION_PLAN_BUNDLE_SCOPE
                    / _FIXED_REGION_PLAN_BUNDLE_REQUEST_ROOT
                    / f"{request_sha256}.json"
                )
                if (
                    not _MANAGED_DATA_VERSION.fullmatch(request_sha256)
                    or trailing[1] != expected_request.as_posix()
                ):
                    raise ValueError(
                        "Remote fixed Region Data import request binding is invalid."
                    )
        elif operation == _REGION_PLAN_ADMIN_COMMAND:
            if (
                len(trailing) != 6
                or tuple(trailing[0::2])
                != ("--source", "--source-sha256", "--managed-version")
            ):
                raise ValueError("Remote region-plan candidate command arguments are not allowlisted.")
            source_sha256 = trailing[3]
            expected_source = (
                root
                / "state"
                / "development"
                / "managed_data"
                / _MANAGED_DATA_CANDIDATE_DATASET
                / source_sha256
                / "payload.xlsx"
            )
            if (
                not _MANAGED_DATA_VERSION.fullmatch(source_sha256)
                or trailing[5] != source_sha256
                or trailing[1] != expected_source.as_posix()
            ):
                raise ValueError("Remote region-plan candidate source binding is invalid.")
        elif operation == "migration-preview":
            if len(trailing) != 2 or trailing[0] != "--migration-id":
                raise ValueError("Remote region-plan migration preview arguments are not allowlisted.")
            _region_plan_migration_spec(trailing[1])
        elif operation == "install-schema":
            if (
                len(trailing) != 4
                or tuple(trailing[0::2]) != ("--migration-id", "--confirmation")
            ):
                raise ValueError("Remote region-plan schema install arguments are not allowlisted.")
            _region_plan_migration_spec(trailing[1])
            confirmation = trailing[3]
            if (
                not confirmation
                or len(confirmation) > 512
                or any(ord(char) < 32 for char in confirmation)
            ):
                raise ValueError("Remote region-plan schema confirmation is invalid.")
        else:
            if (
                len(trailing) != 4
                or tuple(trailing[0::2]) != ("--request", "--request-sha256")
            ):
                raise ValueError("Remote region-plan workflow arguments are not allowlisted.")
            request_sha256 = trailing[3]
            expected_request = (
                root / "state" / "development" / _REGION_PLAN_REQUEST_ROOT
                / f"{request_sha256}.json"
            )
            if (
                not _MANAGED_DATA_VERSION.fullmatch(request_sha256)
                or trailing[1] != expected_request.as_posix()
            ):
                raise ValueError("Remote region-plan workflow request binding is invalid.")
        canonical = f"cd -- {shlex.quote(tokens[2])} && exec " + " ".join(
            shlex.quote(token) for token in tokens[5:]
        )
        if command != canonical:
            raise ValueError("Remote region-plan command is not canonically shell-quoted.")
        _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        stdout_bytes = stdout.read(_MASTER_ADMIN_MAX_JSON_BYTES + 1)
        if not isinstance(stdout_bytes, bytes):
            stdout_bytes = str(stdout_bytes).encode("utf-8", errors="replace")
        if len(stdout_bytes) > _MASTER_ADMIN_MAX_JSON_BYTES:
            with contextlib.suppress(Exception):
                stdout.channel.close()
            raise RuntimeError("Remote region-plan result exceeds the safe response limit.")
        stderr_bytes = stderr.read(_BUILD_OUTPUT_LIMIT + 1)
        code = stdout.channel.recv_exit_status()
        try:
            raw_stdout = stdout_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise RuntimeError("Remote region-plan result is not valid UTF-8 JSON.") from exc
        if isinstance(stderr_bytes, bytes):
            raw_stderr = stderr_bytes.decode("utf-8", errors="replace")
        else:
            raw_stderr = str(stderr_bytes)
        return code, raw_stdout, _redact(raw_stderr)

    def inventory_files(self, directory: str) -> list[str]:
        """Return the exact non-directory inventory below one safe release root."""

        safe_directory = self._canonical_remote_path(directory, target_must_exist=True)
        command = (
            f"LC_ALL=C find {shlex.quote(safe_directory)} "
            "-xdev ! -type d -print0"
        )
        _, stdout, stderr = self.client.exec_command(command, timeout=45)
        payload = stdout.read(_MASTER_ADMIN_MAX_JSON_BYTES + 1)
        if not isinstance(payload, bytes):
            payload = str(payload).encode("utf-8", errors="replace")
        if len(payload) > _MASTER_ADMIN_MAX_JSON_BYTES:
            with contextlib.suppress(Exception):
                stdout.channel.close()
            raise RuntimeError("Remote release inventory exceeds the safe response limit.")
        error_payload = stderr.read(_BUILD_OUTPUT_LIMIT + 1)
        code = stdout.channel.recv_exit_status()
        if code != 0:
            detail = _redact(
                error_payload.decode("utf-8", errors="replace")
                if isinstance(error_payload, bytes)
                else str(error_payload)
            )
            raise RuntimeError(f"Remote release inventory failed. {detail[:240]}".strip())
        prefix = safe_directory.rstrip("/") + "/"
        result: list[str] = []
        seen: set[str] = set()
        for raw_path in payload.split(b"\0"):
            if not raw_path:
                continue
            try:
                path = raw_path.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                raise RuntimeError("Remote release inventory contains a non-UTF-8 path.") from exc
            if not path.startswith(prefix):
                raise RuntimeError("Remote release inventory escaped its release root.")
            relative = _safe_relative(path[len(prefix) :])
            if relative in seen:
                raise RuntimeError("Remote release inventory contains duplicate paths.")
            seen.add(relative)
            result.append(relative)
        return sorted(result)


_remote_session_factory: Callable[[Mapping[str, Any]], Any] = ParamikoRemote


def _remote_target(inspection: ArtifactInspection, relative: str) -> str:
    safe = _safe_relative(relative)
    base = PurePosixPath(inspection.target_upload_path)
    target = base / PurePosixPath(safe)
    if not target.is_relative_to(base):
        raise ValueError("Remote target escapes artifact root.")
    return target.as_posix()


def _selected(
    inspection: ArtifactInspection, selected_files: Iterable[str]
) -> list[tuple[str, str, Path]]:
    manifest_files = _manifest_files(inspection.manifest)
    staging = Path(inspection.path).resolve()
    result: list[tuple[str, str, Path]] = []
    seen: set[str] = set()
    for value in selected_files:
        relative = _safe_relative(value)
        if relative == "deploy_manifest.json" and inspection.kind != "admin-tools":
            raise ValueError(
                "deploy_manifest.json is selectable only for admin-tools artifacts."
            )
        if relative in seen:
            continue
        seen.add(relative)
        if relative not in manifest_files:
            raise ValueError(f"Selected file is not in the manifest: {relative}")
        local = _within(staging / Path(relative), staging)
        if _sha256_file(local) != manifest_files[relative]:
            raise ValueError(f"Local artifact changed after inspection: {relative}")
        result.append((relative, manifest_files[relative], local))
    if not result:
        raise ValueError("At least one manifest file must be selected.")
    return sorted(result)


def _release_file_map(
    inspection: ArtifactInspection,
) -> dict[str, tuple[str, Path]]:
    """Return the exact deployable files, including admin release metadata."""

    staging = Path(inspection.path).resolve()
    result = {
        relative: (checksum, _within(staging / Path(relative), staging))
        for relative, checksum in _manifest_files(inspection.manifest).items()
    }
    if inspection.kind == "admin-tools":
        manifest_path = _within(staging / "deploy_manifest.json", staging)
        if not manifest_path.is_file():
            raise ValueError("Admin-tools deploy_manifest.json is missing.")
        result["deploy_manifest.json"] = (_sha256_file(manifest_path), manifest_path)
    return result


def _selected_release_files(
    inspection: ArtifactInspection, selected_files: Iterable[str]
) -> list[tuple[str, str, Path]]:
    if inspection.kind != "admin-tools":
        return _selected(inspection, selected_files)

    # An Admin Tools manifest intentionally cannot list or checksum itself;
    # _release_file_map adds that release metadata as a separately hashed,
    # deployable entry.  A retry may therefore select only this file.
    release_files = _release_file_map(inspection)
    selected: list[tuple[str, str, Path]] = []
    seen: set[str] = set()
    for value in selected_files:
        relative = _safe_relative(value)
        if relative in seen:
            continue
        seen.add(relative)
        if relative not in release_files:
            raise ValueError(f"Selected file is not in the admin-tools release: {relative}")
        checksum, local = release_files[relative]
        if _sha256_file(local) != checksum:
            raise ValueError(f"Local artifact changed after inspection: {relative}")
        selected.append((relative, checksum, local))
    if not selected:
        raise ValueError("At least one release file must be selected.")

    # Any ordinary Admin Tools file selection also publishes the matching
    # release manifest. Do not duplicate it when a manifest-only retry was
    # selected by the diff UI.
    if "deploy_manifest.json" not in seen:
        checksum, local = release_files["deploy_manifest.json"]
        selected.append(("deploy_manifest.json", checksum, local))
    return sorted(selected)


def preview_remote_diff(
    *,
    inspection: ArtifactInspection | Mapping[str, Any],
    selected_files: Iterable[str],
    config_path: str,
) -> list[dict[str, Any]]:
    """Read the current checksum diff without taking an upload lock or writing.

    This deliberately remains available after an interrupted upload: it can
    report the per-file atomic state left on the server while another upload
    lock is absent or has become stale.  It never creates, removes, or changes
    a remote file.
    """

    item = _inspection(inspection)
    if item.restricted_data:
        raise ValueError("Artifact contains forbidden secret/config files.")
    profile = _load_remote_profile(config_path)
    rows: list[dict[str, Any]] = []
    with _remote_session_factory(profile) as remote:
        for relative, checksum, local in _selected_release_files(item, selected_files):
            remote_path = _remote_target(item, relative)
            remote_checksum = remote.sha256(remote_path)
            status = "create" if remote_checksum is None else (
                "unchanged" if remote_checksum == checksum else "update"
            )
            rows.append(
                {
                    "path": relative,
                    "local_path": str(local),
                    "remote_path": remote_path,
                    "status": status,
                    "local_sha256": checksum,
                    "remote_sha256": remote_checksum,
                    "local_size_bytes": local.stat().st_size,
                    "remote_size_bytes": remote.size(remote_path),
                }
            )
    return sorted(rows, key=lambda row: str(row["path"]))


def deployment_policy(*, environment: str, config_path: str) -> dict[str, Any]:
    """Return non-secret local policy flags for one selected remote target."""

    profile = _load_remote_profile(config_path)
    result = {
        "allow_upload": profile["allow_upload"] is True,
        "allow_service_control": profile["allow_service_control"] is True,
        "target_id": _target_id(profile, environment),
    }
    # Keep compatibility with callers/fakes from before the narrow secure
    # config feature while exposing the current local policy when available.
    if "allow_development_secure_config_upload" in profile:
        result["allow_development_secure_config_upload"] = (
            profile["allow_development_secure_config_upload"] is True
        )
    if "allow_production_secure_config_upload" in profile:
        result["allow_production_secure_config_upload"] = (
            profile["allow_production_secure_config_upload"] is True
        )
    return result


def _require_development_secure_config_environment(environment: str) -> None:
    if _require_environment(environment) != "development":
        raise PermissionError("Secure configuration upload is development-only.")


def _require_production_secure_config_environment(environment: str) -> None:
    if _require_environment(environment) != "production":
        raise PermissionError("Secure configuration upload is production-only.")


def _reject_placeholder_values(value: Any, *, source_name: str, pointer: str = "$") -> None:
    """Reject obvious template values without including their contents in errors."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_placeholder_values(
                child, source_name=source_name, pointer=f"{pointer}.{str(key)}"
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_placeholder_values(
                child, source_name=source_name, pointer=f"{pointer}[{index}]"
            )
    elif isinstance(value, str) and _PLACEHOLDER_VALUE.search(value):
        raise ValueError(f"Placeholder value is not allowed in {source_name} at {pointer}.")


def _required_mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object.")
    return value


def _catalog_relative_path(catalog: Mapping[str, Any], active_key: str) -> str:
    raw_data_root = str(catalog.get("data_root", "")).replace("\\", "/")
    data_root = raw_data_root.strip("/")
    active = catalog.get("active")
    if raw_data_root.startswith("/") or not data_root or not isinstance(active, Mapping):
        raise ValueError("data_catalog.json has no valid North America data root.")
    raw_value = str(active.get(active_key, "")).replace("\\", "/")
    value = raw_value.strip("/")
    if raw_value.startswith("/") or not value:
        raise ValueError(f"data_catalog.json active.{active_key} is required.")
    relative = PurePosixPath(data_root) / PurePosixPath(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("data_catalog.json contains an unsafe North America path.")
    return relative.as_posix()


def _production_server_catalog_relative_path(
    catalog: Mapping[str, Any], active_key: str
) -> str:
    """Resolve a production server catalog role beneath the shared data root."""

    if catalog.get("schema") != "north-america-routing-data-catalog/v1":
        raise ValueError("Production server data catalog has an unsupported schema.")
    raw_data_root = str(catalog.get("data_root", "")).replace("\\", "/").rstrip("/")
    if raw_data_root != NORTH_AMERICA_SHARED_ROOT:
        raise ValueError("Production server data catalog has an unexpected data root.")
    raw_state_root = str(catalog.get("state_root", "")).replace("\\", "/").rstrip("/")
    if raw_state_root != "/home/csda/AI_Routing/state/production":
        raise ValueError("Production server data catalog has an unexpected state root.")
    active = catalog.get("active")
    if not isinstance(active, Mapping):
        raise ValueError("Production server data catalog has no active artifacts.")
    raw_value = str(active.get(active_key, "")).replace("\\", "/")
    value = raw_value.strip("/")
    relative = PurePosixPath(value)
    if raw_value.startswith("/") or not value or ".." in relative.parts:
        raise ValueError("Production server data catalog contains an unsafe active path.")
    return (PurePosixPath("data") / "north_america" / relative).as_posix()


def _load_production_remote_data_catalog(remote: Any) -> tuple[dict[str, Any], str]:
    """Read and validate the fixed server-data catalog used by production units."""

    payload = remote.read_bytes(
        PRODUCTION_REMOTE_DATA_CATALOG, maximum_bytes=_REMOTE_DATA_CATALOG_MAX_BYTES
    )
    checksum = hashlib.sha256(payload).hexdigest()
    try:
        catalog = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Production server data catalog is not valid UTF-8 JSON.") from exc
    if not isinstance(catalog, dict):
        raise ValueError("Production server data catalog must be a JSON object.")
    # Resolve all roles used by this uploader now, before preparing any payload.
    for role in ("service_geocoded", "profile_production", "zcta_geometry"):
        _production_server_catalog_relative_path(catalog, role)
    return catalog, checksum


def _shared_north_america_path(relative: str) -> str:
    path = PurePosixPath(relative)
    expected_prefix = ("data", "north_america")
    if path.parts[:2] != expected_prefix or ".." in path.parts:
        raise ValueError("North America catalog path is outside data/north_america.")
    return (PurePosixPath(NORTH_AMERICA_SHARED_ROOT) / PurePosixPath(*path.parts[2:])).as_posix()


def _validate_common_development_config(payload: dict[str, Any]) -> dict[str, Any]:
    _reject_placeholder_values(payload, source_name="common_vrp.dev.json")
    if payload.get("environment") != "development":
        raise ValueError("common_vrp.dev.json environment must be development.")
    api = _required_mapping(payload.get("api"), name="common_vrp.dev.json api")
    if api.get("port") != 8066:
        raise ValueError("common_vrp.dev.json API port must be 8066.")
    database = _required_mapping(
        payload.get("database"), name="common_vrp.dev.json database"
    )
    if database.get("dbname") != "vrp_db_dev":
        raise ValueError("common_vrp.dev.json database must be vrp_db_dev.")
    password = database.get("password")
    if not isinstance(password, str) or not password.strip() or _PLACEHOLDER_VALUE.search(password):
        raise ValueError("common_vrp.dev.json database password is required and cannot be a placeholder.")
    storage = _required_mapping(payload.get("storage"), name="common_vrp.dev.json storage")
    storage["job_archive_root"] = DEVELOPMENT_COMMON_JOB_ARCHIVE_ROOT
    return payload


def _validate_http_origin(value: Any, *, name: str) -> None:
    """Require a credential-free HTTP(S) origin without exposing its value."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required.")
    parsed = urlparse(value.strip())
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ValueError(f"{name} must be a credential-free HTTP(S) origin.")
    try:
        parsed.port
    except ValueError as exc:
        raise ValueError(f"{name} contains an invalid port.") from exc


def _validate_common_production_config(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate the production Common VRP contract before any remote session.

    Keep this aligned with the runtime loader: production secure-config upload
    must never install a configuration the production API would reject at
    startup.  The runtime loader validates the routing API URL and its port
    relationship in addition to the explicit environment/database checks.
    """

    _reject_placeholder_values(payload, source_name="common_vrp.prod.json")
    if payload.get("environment") != "production":
        raise ValueError("common_vrp.prod.json environment must be production.")
    api = _required_mapping(payload.get("api"), name="common_vrp.prod.json api")
    if api.get("port") != 8065:
        raise ValueError("common_vrp.prod.json API port must be 8065.")
    database = _required_mapping(
        payload.get("database"), name="common_vrp.prod.json database"
    )
    missing_database_keys = [
        key for key in ("host", "port", "user", "password")
        if not str(database.get(key, "")).strip()
    ]
    if missing_database_keys:
        raise ValueError(
            "common_vrp.prod.json missing required database settings: "
            + ", ".join(missing_database_keys)
        )
    if database.get("dbname") != "vrp_db":
        raise ValueError("common_vrp.prod.json database must be vrp_db.")
    password = database.get("password")
    if not isinstance(password, str) or not password.strip() or _PLACEHOLDER_VALUE.search(password):
        raise ValueError(
            "common_vrp.prod.json database password is required and cannot be a placeholder."
        )
    try:
        database_port = int(database["port"])
    except (TypeError, ValueError) as exc:
        raise ValueError("common_vrp.prod.json database.port must be an integer.") from exc
    if not 1 <= database_port <= 65535:
        raise ValueError("common_vrp.prod.json database.port must be between 1 and 65535.")
    _validate_http_origin(
        payload.get("routing_api_url"), name="common_vrp.prod.json routing_api_url"
    )
    routing_url = urlparse(str(payload["routing_api_url"]).strip())
    if (routing_url.port or (443 if routing_url.scheme == "https" else 80)) != 8065:
        raise ValueError(
            "common_vrp.prod.json routing_api_url port must match API port 8065."
        )
    storage = _required_mapping(payload.get("storage"), name="common_vrp.prod.json storage")
    storage["job_archive_root"] = PRODUCTION_COMMON_JOB_ARCHIVE_ROOT
    return payload


def _validate_production_routing_config(payload: dict[str, Any]) -> None:
    """Verify the routing configuration shape without changing routing policy."""

    routing = _required_mapping(payload.get("routing"), name="config.json routing")
    for key in ("distance_backend", "assignment_distance_backend", "osrm_profile"):
        if not isinstance(routing.get(key), str) or not routing[key].strip():
            raise ValueError(f"config.json routing.{key} is required.")
    _validate_http_origin(routing.get("osrm_url"), name="config.json routing.osrm_url")
    city_urls = routing.get("city_osrm_urls")
    if not isinstance(city_urls, Mapping):
        raise ValueError("config.json routing.city_osrm_urls must be an object.")
    for city, city_url in city_urls.items():
        if not isinstance(city, str) or not city.strip():
            raise ValueError("config.json routing.city_osrm_urls has an invalid city key.")
        _validate_http_origin(
            city_url, name="config.json routing.city_osrm_urls entry"
        )


def _rewrite_north_america_config_paths(
    payload: dict[str, Any], *, production_server_catalog: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Translate the catalog-authoritative North America files for the server."""

    _reject_placeholder_values(payload, source_name="config.json")
    catalog = _read_json(CONFIG_ROOT / "data_catalog.json")
    service_local = _catalog_relative_path(catalog, "service_geocoded")
    profile_local = _catalog_relative_path(catalog, "profile_production")
    zcta_local = _catalog_relative_path(catalog, "zcta_geometry")
    server_paths = {
        "service_file": service_local,
        "profile_file": profile_local,
        "zcta_zip_file": zcta_local,
    }
    if production_server_catalog is not None:
        remote_paths = {
            "service_file": _production_server_catalog_relative_path(
                production_server_catalog, "service_geocoded"
            ),
            "profile_file": _production_server_catalog_relative_path(
                production_server_catalog, "profile_production"
            ),
            "zcta_zip_file": _production_server_catalog_relative_path(
                production_server_catalog, "zcta_geometry"
            ),
        }
        if remote_paths != server_paths:
            raise ValueError(
                "Local North America data catalog does not match the approved production server catalog."
            )
        server_paths = remote_paths
    replacements = {
        "service_file": (service_local, _shared_north_america_path(server_paths["service_file"])),
        "profile_file": (profile_local, _shared_north_america_path(server_paths["profile_file"])),
    }
    for section_name in ("area_map_usa", "area_map"):
        section = _required_mapping(payload.get(section_name), name=f"config.json {section_name}")
        for key, (expected_local, server_path) in replacements.items():
            if section.get(key) != expected_local:
                raise ValueError(
                    f"config.json {section_name}.{key} must match config/data_catalog.json."
                )
            section[key] = server_path
    usa = _required_mapping(payload.get("area_map_usa"), name="config.json area_map_usa")
    if usa.get("zcta_zip_file") != zcta_local:
        raise ValueError(
            "config.json area_map_usa.zcta_zip_file must match config/data_catalog.json."
        )
    usa["zcta_zip_file"] = _shared_north_america_path(server_paths["zcta_zip_file"])
    return payload


def _json_payload_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _prepare_secure_config_payloads(
    environment: str,
    *,
    production_server_catalog: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Build one environment's fixed server-ready config pair in memory only."""

    normalized_environment = _require_environment(environment)
    common_filename = _SECURE_CONFIG_COMMON_SOURCES[normalized_environment]
    common_source = CONFIG_ROOT / common_filename
    general_source = CONFIG_ROOT / "config.json"
    common_payload = _read_json(common_source)
    if normalized_environment == "development":
        common = _validate_common_development_config(common_payload)
    else:
        common = _validate_common_production_config(common_payload)
    general_payload = _read_json(general_source)
    if normalized_environment == "production":
        _validate_production_routing_config(general_payload)
    general = _rewrite_north_america_config_paths(
        general_payload, production_server_catalog=production_server_catalog
    )
    prepared = {
        common_filename: _json_payload_bytes(common),
        "config.json": _json_payload_bytes(general),
    }
    result: list[dict[str, Any]] = []
    for filename, target in _SECURE_CONFIG_TARGETS[normalized_environment]:
        payload = prepared[filename]
        result.append(
            {
                "filename": filename,
                "target": target,
                "payload": payload,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            }
        )
    return result


def _prepare_development_secure_config_payloads() -> list[dict[str, Any]]:
    """Backward-compatible development-only payload preparation helper."""

    return _prepare_secure_config_payloads("development")


def _prepare_production_secure_config_payloads() -> list[dict[str, Any]]:
    """Build the fixed production config pair without writing secret temp files."""

    return _prepare_secure_config_payloads("production")


def _secure_config_rows(prepared: Iterable[Mapping[str, Any]], remote: Any | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in prepared:
        checksum = str(item["sha256"])
        target = str(item["target"])
        remote_checksum = remote.sha256(target) if remote is not None else None
        remote_mode = remote.mode(target) if remote_checksum is not None else None
        if remote is None:
            status = "upload_disabled"
        elif remote_checksum is None:
            status = "create"
        elif remote_checksum != checksum:
            status = "update"
        elif remote_mode != _SECURE_CONFIG_MODE:
            status = "permission_update"
        else:
            status = "unchanged"
        rows.append(
            {
                "filename": str(item["filename"]),
                "target": target,
                "local_sha256": checksum,
                "remote_sha256": remote_checksum,
                "size_bytes": int(item["size_bytes"]),
                "mode": _SECURE_CONFIG_MODE_TEXT,
                "status": status,
                "changed": status in {"create", "update", "permission_update"},
                # Used only to bind the preview to the exact remote state; it
                # is intentionally omitted from public preview rows.
                "_remote_mode": remote_mode,
            }
        )
    return rows


def _secure_config_fingerprint(
    rows: Iterable[Mapping[str, Any]], *, production_catalog_sha256: str | None = None
) -> str:
    public_state: dict[str, Any] = {
        "files": [
        {
            "filename": str(row["filename"]),
            "target": str(row["target"]),
            "local_sha256": str(row["local_sha256"]),
            "remote_sha256": row.get("remote_sha256"),
            "size_bytes": int(row["size_bytes"]),
            "mode": _SECURE_CONFIG_MODE_TEXT,
            "remote_mode": row.get("_remote_mode"),
            "status": str(row["status"]),
        }
        for row in rows
        ]
    }
    if production_catalog_sha256 is not None:
        if not re.fullmatch(r"[0-9a-f]{64}", production_catalog_sha256):
            raise ValueError("Production server data catalog checksum is invalid.")
        public_state["production_catalog_sha256"] = production_catalog_sha256
    return hashlib.sha256(
        json.dumps(public_state, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _secure_config_preview_payload(
    rows: list[dict[str, Any]],
    *,
    environment: str,
    upload_allowed: bool,
    production_catalog_sha256: str | None = None,
) -> dict[str, Any]:
    normalized_environment = _require_environment(environment)
    fingerprint = _secure_config_fingerprint(
        rows, production_catalog_sha256=production_catalog_sha256
    )
    public_rows = [
        {key: value for key, value in row.items() if key != "_remote_mode"}
        for row in rows
    ]
    mutation_required = any(row["changed"] for row in rows)
    return {
        "status": (
            "ready" if upload_allowed and mutation_required else
            "unchanged" if upload_allowed else "upload_disabled"
        ),
        "upload_allowed": upload_allowed,
        "mutation_required": mutation_required,
        "target_upload_path": _SECURE_CONFIG_TARGET_DIRECTORIES[normalized_environment],
        "files": public_rows,
        "fingerprint": fingerprint,
    }


def _secure_config_upload_allowed(profile: Mapping[str, Any], *, environment: str) -> bool:
    normalized_environment = _require_environment(environment)
    return (
        profile.get("allow_upload") is True
        and profile.get(
            f"allow_{normalized_environment}_secure_config_upload"
        )
        is True
    )


def _preview_secure_config_upload(
    *, environment: str, config_path: str
) -> dict[str, Any]:
    """Preview redacted metadata for one fixed environment config pair."""

    normalized_environment = _require_environment(environment)
    profile = _load_remote_profile(config_path)
    prepared = _prepare_secure_config_payloads(normalized_environment)
    allowed = _secure_config_upload_allowed(profile, environment=normalized_environment)
    if not allowed:
        return _secure_config_preview_payload(
            _secure_config_rows(prepared, None),
            environment=normalized_environment,
            upload_allowed=False,
        )
    with _remote_session_factory(profile) as remote:
        production_catalog_sha256: str | None = None
        if normalized_environment == "production":
            production_catalog, production_catalog_sha256 = (
                _load_production_remote_data_catalog(remote)
            )
            prepared = _prepare_secure_config_payloads(
                normalized_environment,
                production_server_catalog=production_catalog,
            )
        rows = _secure_config_rows(prepared, remote)
    return _secure_config_preview_payload(
        rows,
        environment=normalized_environment,
        upload_allowed=True,
        production_catalog_sha256=production_catalog_sha256,
    )


def preview_development_secure_config_upload(
    *, environment: str, config_path: str
) -> dict[str, Any]:
    """Preview only redacted metadata for the fixed development config pair."""

    _require_development_secure_config_environment(environment)
    return _preview_secure_config_upload(environment="development", config_path=config_path)


def preview_production_secure_config_upload(
    *, environment: str, config_path: str
) -> dict[str, Any]:
    """Preview only redacted metadata for the fixed production config pair."""

    _require_production_secure_config_environment(environment)
    return _preview_secure_config_upload(environment="production", config_path=config_path)


def _secure_config_history(
    *,
    environment: str,
    deployment_id: str,
    profile: Mapping[str, Any],
    fingerprint: str,
    status: str,
    changes: Iterable[Mapping[str, Any]],
    compensated: bool | None = None,
    production_catalog_sha256: str | None = None,
) -> dict[str, Any]:
    normalized_environment = _require_environment(environment)
    entry: dict[str, Any] = {
        "id": deployment_id,
        "release_id": deployment_id,
        "version": f"secure-config-{fingerprint[:12]}",
        "environment": normalized_environment,
        "kind": f"{normalized_environment}-secure-config",
        "created_at": _now(),
        "status": status,
        "sha256": fingerprint,
        "target_id": _target_id(profile, normalized_environment),
        "service_eligible": False,
        "restart_performed": False,
        "restart_required": True,
        "operator": os.environ.get("USERNAME") or os.environ.get("USER") or "local-console",
        "changes": [dict(change) for change in changes],
    }
    if compensated is not None:
        entry["compensated"] = compensated
    if production_catalog_sha256 is not None:
        if not re.fullmatch(r"[0-9a-f]{64}", production_catalog_sha256):
            raise ValueError("Production server data catalog checksum is invalid.")
        entry["production_catalog_sha256"] = production_catalog_sha256
    return entry


def _upload_secure_config(
    *,
    environment: str,
    config_path: str,
    expected_fingerprint: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Upload one fixed config pair with checksum/mode compensation.

    No user supplied paths, files, or confirmations are accepted.  Environment
    wrappers below are the only public entrypoints and reject cross-environment
    use before this function can open a remote session.
    """

    normalized_environment = _require_environment(environment)
    profile = _load_remote_profile(config_path)
    expected_fingerprint = str(expected_fingerprint).strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_fingerprint):
        raise ValueError("A secure-config preview fingerprint is required.")
    if not _secure_config_upload_allowed(profile, environment=normalized_environment):
        raise PermissionError(
            f"{normalized_environment.capitalize()} secure-config upload is disabled "
            "in the local deployment profile."
        )
    if dry_run:
        return {
            "status": "dry_run",
            "environment": normalized_environment,
            "kind": f"{normalized_environment}-secure-config",
            "restart_performed": False,
            "restart_required": True,
        }

    deployment_id = f"{normalized_environment}-secure-config-{uuid.uuid4().hex[:12]}"
    attempted: list[dict[str, Any]] = []
    changes: list[dict[str, Any]] = []
    compensated = False
    fingerprint_for_history = expected_fingerprint
    production_catalog_sha256: str | None = None
    try:
        with _remote_session_factory(profile) as remote:
            with remote.deployment_lock(str(profile["remote_root"]), deployment_id):
                # Re-read sources inside the lock so both local edits and remote
                # drift invalidate the preview before any target is touched.
                if normalized_environment == "production":
                    production_catalog, production_catalog_sha256 = (
                        _load_production_remote_data_catalog(remote)
                    )
                    prepared = _prepare_secure_config_payloads(
                        normalized_environment,
                        production_server_catalog=production_catalog,
                    )
                else:
                    prepared = _prepare_secure_config_payloads(normalized_environment)
                rows = _secure_config_rows(prepared, remote)
                current_fingerprint = _secure_config_fingerprint(
                    rows, production_catalog_sha256=production_catalog_sha256
                )
                fingerprint_for_history = current_fingerprint
                if current_fingerprint != expected_fingerprint:
                    raise RuntimeError("Secure-config preview is stale; refresh and review it again.")
                if not any(row["changed"] for row in rows):
                    raise ValueError("Secure-config files and permissions are already unchanged.")

                backup_root = posixpath.join(
                    str(profile["remote_root"]), ".deployment_backups", deployment_id
                )
                remote.mkdirs(backup_root, mode=0o700)
                remote.chmod(backup_root, 0o700)
                if remote.mode(backup_root) != 0o700:
                    raise RuntimeError("Secure-config backup directory mode verification failed.")
                prepared_by_target = {str(item["target"]): item for item in prepared}
                try:
                    for row in rows:
                        if not row["changed"]:
                            continue
                        if (
                            normalized_environment == "production"
                            and remote.sha256(PRODUCTION_REMOTE_DATA_CATALOG)
                            != production_catalog_sha256
                        ):
                            raise RuntimeError(
                                "Production server data catalog changed during secure-config upload."
                            )
                        target = str(row["target"])
                        item = prepared_by_target[target]
                        existed = remote.exists(target)
                        previous_checksum = remote.sha256(target) if existed else None
                        previous_mode = remote.mode(target) if existed else None
                        if existed and (
                            not re.fullmatch(r"[0-9a-f]{64}", str(previous_checksum or ""))
                            or previous_mode is None
                        ):
                            raise RuntimeError("Existing secure-config target could not be verified.")
                        backup = (
                            posixpath.join(backup_root, str(item["filename"])) if existed else None
                        )
                        change = {
                            "filename": str(item["filename"]),
                            "target": target,
                            "sha256": str(item["sha256"]),
                            "previous_sha256": previous_checksum,
                            "previous_mode": previous_mode,
                            "backup": backup,
                            "created": not existed,
                            "mode": _SECURE_CONFIG_MODE_TEXT,
                        }
                        attempted.append(change)
                        remote.upload_bytes_atomic(bytes(item["payload"]), target, backup)
                        if (
                            remote.sha256(target) != item["sha256"]
                            or remote.mode(target) != _SECURE_CONFIG_MODE
                        ):
                            raise RuntimeError("Secure-config post-upload verification failed.")
                        if backup:
                            if (
                                remote.sha256(backup) != previous_checksum
                                or remote.mode(backup) != _SECURE_CONFIG_MODE
                            ):
                                raise RuntimeError("Secure-config backup verification failed.")
                            change["backup_sha256"] = previous_checksum
                        changes.append(change)
                    if (
                        normalized_environment == "production"
                        and remote.sha256(PRODUCTION_REMOTE_DATA_CATALOG)
                        != production_catalog_sha256
                    ):
                        raise RuntimeError(
                            "Production server data catalog changed during secure-config upload."
                        )
                except Exception as upload_error:
                    compensation_errors: list[str] = []
                    for change in reversed(attempted):
                        try:
                            target = str(change["target"])
                            backup = change.get("backup")
                            previous_checksum = change.get("previous_sha256")
                            previous_mode = change.get("previous_mode")
                            if backup and remote.sha256(str(backup)) == previous_checksum:
                                remote.copy(str(backup), target)
                                if previous_mode is not None:
                                    remote.chmod(target, int(previous_mode))
                                if (
                                    remote.sha256(target) != previous_checksum
                                    or remote.mode(target) != previous_mode
                                ):
                                    raise RuntimeError("Secure-config backup restoration verification failed.")
                            elif backup and (
                                remote.sha256(target) == previous_checksum
                                and remote.mode(target) == previous_mode
                            ):
                                # The backup failed before the original changed.
                                pass
                            elif backup:
                                raise RuntimeError("Secure-config backup is unavailable for compensation.")
                            elif change["created"] and remote.exists(target):
                                remote.remove(target)
                                if remote.exists(target):
                                    raise RuntimeError("Secure-config created-target compensation failed.")
                        except Exception:
                            compensation_errors.append(str(change["filename"]))
                    if compensation_errors:
                        raise RuntimeError(
                            "Secure-config upload failed and compensation was incomplete for: "
                            + ", ".join(compensation_errors)
                        ) from upload_error
                    compensated = True
                    raise
    except Exception:
        with contextlib.suppress(Exception):
            _append_history(
                _secure_config_history(
                    environment=normalized_environment,
                    deployment_id=deployment_id,
                    profile=profile,
                    fingerprint=fingerprint_for_history,
                    status="upload_failed",
                    changes=attempted,
                    compensated=compensated,
                    production_catalog_sha256=production_catalog_sha256,
                )
            )
        raise

    _append_history(
        _secure_config_history(
            environment=normalized_environment,
            deployment_id=deployment_id,
            profile=profile,
            fingerprint=fingerprint_for_history,
            status="uploaded",
            changes=changes,
            production_catalog_sha256=production_catalog_sha256,
        )
    )
    return {
        "status": "uploaded",
        "release_id": deployment_id,
        "environment": normalized_environment,
        "kind": f"{normalized_environment}-secure-config",
        "fingerprint": fingerprint_for_history,
        "restart_performed": False,
        "restart_required": True,
    }


def upload_development_secure_config(
    *,
    environment: str,
    config_path: str,
    expected_fingerprint: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Upload the fixed development config pair with compensation safeguards."""

    _require_development_secure_config_environment(environment)
    return _upload_secure_config(
        environment="development",
        config_path=config_path,
        expected_fingerprint=expected_fingerprint,
        dry_run=dry_run,
    )


def upload_production_secure_config(
    *,
    environment: str,
    config_path: str,
    expected_fingerprint: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Upload the fixed production config pair with compensation safeguards."""

    _require_production_secure_config_environment(environment)
    return _upload_secure_config(
        environment="production",
        config_path=config_path,
        expected_fingerprint=expected_fingerprint,
        dry_run=dry_run,
    )


def _load_history() -> list[dict[str, Any]]:
    if not HISTORY_PATH.is_file():
        return []
    payload = _read_json(HISTORY_PATH)
    if payload.get("schema") != "deployment-console-history/v1":
        raise ValueError("Unsupported deployment console history schema.")
    rows = payload.get("entries", [])
    if not isinstance(rows, list):
        raise ValueError("Deployment console history entries must be a list.")
    return [dict(item) for item in rows if isinstance(item, Mapping)]


def _save_history(rows: list[dict[str, Any]]) -> None:
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = HISTORY_PATH.with_suffix(".tmp")
    temporary.write_text(
        json.dumps({"schema": "deployment-console-history/v1", "entries": rows}, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, HISTORY_PATH)


def _append_history(entry: dict[str, Any]) -> None:
    with _HISTORY_LOCK:
        rows = _load_history()
        rows.append(entry)
        _save_history(rows[-500:])


def _require_managed_data_scope(scope: str) -> str:
    normalized = str(scope).strip().lower()
    if normalized not in _MANAGED_DATA_SCOPES:
        raise ValueError("scope must be development, production, or common")
    return normalized


def _managed_data_registry_module() -> Any:
    """Load the data-owned registry lazily so console import stays side-effect free."""

    from tools.data import managed_data_registry

    return managed_data_registry


def _managed_data_public_spec(raw: Any, *, scope: str) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        value = dict(raw)
    elif hasattr(raw, "to_public_dict"):
        value = dict(raw.to_public_dict())
    elif hasattr(raw, "__dict__"):
        value = dict(vars(raw))
    else:
        raise RuntimeError("Managed-data registry returned an invalid dataset entry.")
    dataset_id = str(value.get("dataset_id") or value.get("id") or "").strip()
    if not _MANAGED_DATA_DATASET_ID.fullmatch(dataset_id):
        raise RuntimeError("Managed-data registry contains an invalid dataset id.")
    raw_scopes = value.get(
        "allowed_scopes", value.get("scopes", value.get("scope", ()))
    )
    if isinstance(raw_scopes, str):
        raw_scopes = (raw_scopes,)
    if not isinstance(raw_scopes, Iterable):
        raise RuntimeError("Managed-data registry scopes are invalid.")
    scopes = sorted({_require_managed_data_scope(item) for item in raw_scopes})
    if scope not in scopes:
        raise RuntimeError("Managed-data registry returned a dataset outside its scope.")
    raw_extensions = value.get("allowed_file_types", value.get("extensions", ()))
    if isinstance(raw_extensions, str):
        raw_extensions = (raw_extensions,)
    extensions: list[str] = []
    for item in raw_extensions:
        extension = str(item).strip().lower()
        if extension and not extension.startswith("."):
            extension = "." + extension
        if not re.fullmatch(r"\.[a-z0-9]{1,10}", extension):
            raise RuntimeError("Managed-data registry file types are invalid.")
        extensions.append(extension)
    if not extensions:
        raise RuntimeError("Managed-data registry has no allowed file types.")
    maximum = value.get("max_bytes", value.get("file_size_limit", 0))
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum <= 0:
        raise RuntimeError("Managed-data registry file size limit is invalid.")
    label = str(value.get("label") or dataset_id).strip()[:160]
    description = str(value.get("description") or "").strip()[:500]
    data_domain = str(value.get("data_domain") or "").strip()[:80]
    if data_domain and any(ord(char) < 32 for char in data_domain):
        raise RuntimeError("Managed-data registry data domain is invalid.")
    enabled = value.get("enabled") is True
    raw_targets = value.get("allowed_targets", ())
    if isinstance(raw_targets, str):
        raw_targets = (raw_targets,)
    allowed_targets = sorted(
        {
            str(item)
            for item in raw_targets
            if str(item) in {"file_upload", "preview", "db_preview", "db_apply"}
        }
    )
    lifecycle_stage = str(value.get("lifecycle_stage") or "source_upload").strip()
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", lifecycle_stage):
        raise RuntimeError("Managed-data registry lifecycle stage is invalid.")
    candidate_workflow_supported = (
        dataset_id == _MANAGED_DATA_CANDIDATE_DATASET
        and scope == _MANAGED_DATA_CANDIDATE_SCOPE
        and lifecycle_stage == "candidate_plan"
        and enabled
        and value.get("promotion_required") is True
        and value.get("direct_db_upsert") is False
    )
    fixed_region_bundle_supported = (
        dataset_id == _FIXED_REGION_PLAN_BUNDLE_DATASET
        and scope == _FIXED_REGION_PLAN_BUNDLE_SCOPE
        and lifecycle_stage == "candidate_plan"
        and enabled
        and value.get("promotion_required") is True
        and value.get("direct_db_upsert") is False
        and sorted(set(extensions)) == [".zip"]
    )
    if candidate_workflow_supported or fixed_region_bundle_supported:
        # A territory workbook can only enter the reviewed-plan lifecycle.
        # Never expose a caller-selectable database target for it.
        allowed_targets = [
            item for item in allowed_targets if item in {"file_upload", "preview"}
        ]
    return {
        "dataset_id": dataset_id,
        "label": label,
        "description": description,
        "data_domain": data_domain,
        "primary_section": value.get("primary_section") is True,
        "ui_hidden": value.get("ui_hidden") is True,
        "enabled": enabled,
        "contains_pii": value.get("contains_pii") is True,
        "allowed_scopes": scopes,
        "allowed_file_types": sorted(set(extensions)),
        "max_bytes": maximum,
        "allowed_targets": allowed_targets,
        "lifecycle_stage": lifecycle_stage,
        "promotion_required": (
            candidate_workflow_supported or fixed_region_bundle_supported
        ),
        "candidate_workflow_supported": candidate_workflow_supported,
        "fixed_region_bundle_supported": fixed_region_bundle_supported,
        "db_sync_supported": (
            (
                dataset_id == _MANAGED_DATA_DB_DATASET
                and enabled
                and isinstance(value.get("db_profile"), Mapping)
                and value["db_profile"].get("table") == _MANAGED_DATA_DB_TABLE
            )
            or (
                dataset_id == _TECHNICIAN_PROFILE_DATASET
                and scope == _TECHNICIAN_PROFILE_SCOPE
                and enabled
                and lifecycle_stage == "db_candidate"
            )
        ),
    }


def _managed_data_specs(scope: str) -> list[dict[str, Any]]:
    scope = _require_managed_data_scope(scope)
    registry = _managed_data_registry_module()
    loader = getattr(registry, "list_managed_data_sets", None)
    if not callable(loader):
        raise RuntimeError("Managed-data registry API is unavailable.")
    raw = loader(scope=scope.capitalize())
    if isinstance(raw, Mapping):
        raw = raw.get("datasets", ())
    if isinstance(raw, (str, bytes)) or not isinstance(raw, Iterable):
        raise RuntimeError("Managed-data registry returned an invalid dataset list.")
    specs = [_managed_data_public_spec(item, scope=scope) for item in raw]
    identifiers = [item["dataset_id"] for item in specs]
    if len(identifiers) != len(set(identifiers)):
        raise RuntimeError("Managed-data registry contains duplicate dataset ids.")
    return sorted(specs, key=lambda item: item["dataset_id"])


def _managed_data_spec(scope: str, dataset_id: str) -> dict[str, Any]:
    scope = _require_managed_data_scope(scope)
    identifier = str(dataset_id).strip()
    if not _MANAGED_DATA_DATASET_ID.fullmatch(identifier):
        raise ValueError("dataset_id is invalid.")
    matches = [item for item in _managed_data_specs(scope) if item["dataset_id"] == identifier]
    if len(matches) != 1:
        raise ValueError("dataset_id is not registered for this scope.")
    return matches[0]


def _managed_data_payload_name(file_name: str, spec: Mapping[str, Any]) -> str:
    submitted = str(file_name).strip()
    leaf = Path(submitted).name
    if (
        not submitted
        or leaf != submitted
        or leaf in {".", ".."}
        or len(leaf) > 255
        or any(ord(char) < 32 for char in leaf)
    ):
        raise ValueError("Managed-data file name is invalid.")
    extension = Path(leaf).suffix.lower()
    if extension not in spec["allowed_file_types"]:
        raise ValueError("Managed-data file type is not allowed for this dataset.")
    return f"payload{extension}"


def _managed_data_validation(
    *, scope: str, dataset_id: str, file_name: str, file_bytes: bytes
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    scope = _require_managed_data_scope(scope)
    spec = _managed_data_spec(scope, dataset_id)
    payload_name = _managed_data_payload_name(file_name, spec)
    if not isinstance(file_bytes, bytes) or not file_bytes:
        raise ValueError("Managed-data upload requires a non-empty file.")
    if len(file_bytes) > int(spec["max_bytes"]):
        raise ValueError("Managed-data file exceeds the registered size limit.")
    digest = hashlib.sha256(file_bytes).hexdigest()
    cache_key = (scope, str(dataset_id), digest)
    with _MANAGED_DATA_JOBS_LOCK:
        cached = _MANAGED_DATA_VALIDATION_CACHE.get(cache_key)
    if cached is not None:
        return spec, payload_name, dict(cached)
    registry = _managed_data_registry_module()
    validator = getattr(registry, "validate_managed_data_file", None)
    if not callable(validator):
        raise RuntimeError("Managed-data registry validator is unavailable.")
    raw = validator(
        scope=scope.capitalize(),
        dataset_id=spec["dataset_id"],
        file_name=Path(str(file_name)).name,
        file_bytes=file_bytes,
    )
    if not isinstance(raw, Mapping):
        raise RuntimeError("Managed-data registry validator returned an invalid result.")
    summary = raw.get("summary", {})
    sample = raw.get("sample", raw.get("masked_sample", []))
    if not isinstance(summary, Mapping) or not isinstance(sample, list):
        raise RuntimeError("Managed-data registry preview metadata is invalid.")
    # The submitted filename can itself contain a person's/customer's name.
    # It is needed only for validation and never crosses the public boundary.
    forbidden_metadata_keys = {
        "filename",
        "file_name",
        "path",
        "local_path",
        "remote_path",
    }
    safe_summary = _redact_master_payload(
        {
            str(key): value
            for key, value in summary.items()
            if str(key).lower() not in forbidden_metadata_keys
        }
    )
    safe_sample = _redact_master_payload(sample[:10])
    # Ensure registry-owned safe metadata can cross the API as bounded JSON.
    rendered = json.dumps(
        {"summary": safe_summary, "sample": safe_sample},
        ensure_ascii=False,
        default=str,
    )
    if len(rendered.encode("utf-8")) > _MASTER_ADMIN_MAX_JSON_BYTES:
        raise RuntimeError("Managed-data preview metadata exceeds the safe response limit.")
    result = {"summary": safe_summary, "sample": safe_sample}
    with _MANAGED_DATA_JOBS_LOCK:
        if len(_MANAGED_DATA_VALIDATION_CACHE) >= 32:
            _MANAGED_DATA_VALIDATION_CACHE.pop(next(iter(_MANAGED_DATA_VALIDATION_CACHE)))
        _MANAGED_DATA_VALIDATION_CACHE[cache_key] = dict(result)
    return spec, payload_name, result


def _managed_data_version_root(scope: str, dataset_id: str, version: str) -> Path:
    scope = _require_managed_data_scope(scope)
    if not _MANAGED_DATA_DATASET_ID.fullmatch(str(dataset_id)):
        raise ValueError("dataset_id is invalid.")
    if not _MANAGED_DATA_VERSION.fullmatch(str(version)):
        raise ValueError("version must be a SHA-256 digest.")
    return _within(MANAGED_DATA_ROOT / scope / dataset_id / version, MANAGED_DATA_ROOT)


def _managed_data_remote_root(
    profile: Mapping[str, Any], scope: str, dataset_id: str, version: str
) -> PurePosixPath:
    scope = _require_managed_data_scope(scope)
    if not _MANAGED_DATA_DATASET_ID.fullmatch(str(dataset_id)):
        raise ValueError("dataset_id is invalid.")
    if not _MANAGED_DATA_VERSION.fullmatch(str(version)):
        raise ValueError("version must be a SHA-256 digest.")
    root = PurePosixPath(str(profile.get("remote_root", "")).strip())
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("Deployment profile remote_root is invalid.")
    if scope == "common":
        return root / "shared" / "north_america" / "managed" / dataset_id / version
    return root / "state" / scope / "managed_data" / dataset_id / version


def _managed_data_target_id(profile: Mapping[str, Any], scope: str) -> str:
    identity = {
        "host": str(profile.get("host", "")).strip().lower(),
        "port": int(profile.get("port", 22)),
        "username": str(profile.get("username", "")).strip().casefold(),
        "remote_root": posixpath.normpath(str(profile.get("remote_root", "")).strip()),
        "scope": _require_managed_data_scope(scope),
    }
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _load_managed_data_version(
    *, scope: str, dataset_id: str, version: str
) -> tuple[dict[str, Any], bytes]:
    spec = _managed_data_spec(scope, dataset_id)
    root = _managed_data_version_root(scope, spec["dataset_id"], version)
    metadata_path = root / _MANAGED_DATA_METADATA_NAME
    if not root.is_dir() or not metadata_path.is_file():
        raise ValueError("Managed-data version was not found.")
    metadata = _read_json(metadata_path)
    payload_name = str(metadata.get("payload_name", ""))
    if (
        metadata.get("schema") != _MANAGED_DATA_METADATA_SCHEMA
        or metadata.get("scope") != scope
        or metadata.get("dataset_id") != spec["dataset_id"]
        or metadata.get("version") != version
        or payload_name not in {f"payload{item}" for item in spec["allowed_file_types"]}
    ):
        raise RuntimeError("Managed-data version metadata is invalid.")
    if metadata.get("lifecycle_stage") != spec["lifecycle_stage"]:
        raise RuntimeError("Managed-data version lifecycle metadata is invalid.")
    if metadata.get("promotion_required") is not spec["promotion_required"]:
        raise RuntimeError("Managed-data version promotion metadata is invalid.")
    workflow = metadata.get("candidate_workflow")
    if spec["candidate_workflow_supported"]:
        if not isinstance(workflow, Mapping):
            raise RuntimeError("Managed-data candidate workflow metadata is missing.")
        if workflow.get("contract_version") != _REGION_PLAN_CONTRACT_VERSION:
            raise RuntimeError("Managed-data candidate workflow contract is invalid.")
        if workflow.get("source_sha256") != version:
            raise RuntimeError("Managed-data candidate workflow source binding is invalid.")
    elif workflow is not None:
        raise RuntimeError("Managed-data version has unexpected candidate workflow metadata.")
    payload_path = _within(root / payload_name, root)
    if not payload_path.is_file():
        raise RuntimeError("Managed-data version payload is missing.")
    inventory = {path.name for path in root.iterdir() if path.is_file()}
    if inventory != {_MANAGED_DATA_METADATA_NAME, payload_name}:
        raise RuntimeError("Managed-data local version inventory is invalid.")
    payload = payload_path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != version or len(payload) != metadata.get(
        "size_bytes"
    ):
        raise RuntimeError("Managed-data local version checksum is invalid.")
    return metadata, payload


def _managed_data_version_public(metadata: Mapping[str, Any], spec: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "version": str(metadata["version"]),
        "sha256": str(metadata["version"]),
        "size_bytes": int(metadata["size_bytes"]),
        "file_type": Path(str(metadata["payload_name"])).suffix.lower(),
        "created_at": str(metadata["created_at"]),
        "db_sync_supported": spec["db_sync_supported"] is True,
        "lifecycle_stage": spec["lifecycle_stage"],
        "promotion_required": spec["promotion_required"] is True,
    }
    workflow = metadata.get("candidate_workflow")
    if isinstance(workflow, Mapping):
        result["candidate_workflow"] = _safe_region_plan_candidate_result(
            workflow, version=str(metadata["version"])
        )
    return result


def list_managed_data_sets(*, scope: str) -> dict[str, Any]:
    scope = _require_managed_data_scope(scope)
    return {"status": "ok", "scope": scope, "datasets": _managed_data_specs(scope)}


def preview_managed_data_upload(
    *, scope: str, dataset_id: str, file_name: str, file_bytes: bytes
) -> dict[str, Any]:
    scope = _require_managed_data_scope(scope)
    spec, payload_name, validated = _managed_data_validation(
        scope=scope,
        dataset_id=dataset_id,
        file_name=file_name,
        file_bytes=file_bytes,
    )
    version = hashlib.sha256(file_bytes).hexdigest()
    return {
        "status": "ready",
        "scope": scope,
        "dataset_id": spec["dataset_id"],
        "file_type": Path(payload_name).suffix.lower(),
        "size_bytes": len(file_bytes),
        "sha256": version,
        "version": version,
        "summary": validated["summary"],
        "sample": validated["sample"],
        "lifecycle_stage": spec["lifecycle_stage"],
        "promotion_required": spec["promotion_required"] is True,
        "candidate_workflow_supported": spec["candidate_workflow_supported"] is True,
        "fixed_region_bundle_supported": spec.get("fixed_region_bundle_supported") is True,
        "required_confirmation": True,
    }


def _finalize_managed_data_local_version(
    *,
    scope: str,
    spec: Mapping[str, Any],
    version: str,
    payload_name: str,
    payload: bytes,
    created_at: str,
    candidate_workflow: Mapping[str, Any] | None = None,
) -> None:
    final_root = _managed_data_version_root(scope, str(spec["dataset_id"]), version)
    if final_root.exists():
        _load_managed_data_version(
            scope=scope, dataset_id=str(spec["dataset_id"]), version=version
        )
        return
    dataset_root = final_root.parent
    dataset_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    _secure_local_directory(dataset_root)
    staging = _within(
        dataset_root / f".{version}.{uuid.uuid4().hex}.tmp", MANAGED_DATA_ROOT
    )
    try:
        staging.mkdir(mode=0o700)
        _secure_local_directory(staging)
        _write_private_bytes(staging / payload_name, payload)
        metadata = {
            "schema": _MANAGED_DATA_METADATA_SCHEMA,
            "scope": scope,
            "dataset_id": spec["dataset_id"],
            "version": version,
            "payload_name": payload_name,
            "size_bytes": len(payload),
            "created_at": created_at,
            "lifecycle_stage": spec["lifecycle_stage"],
            "promotion_required": spec["promotion_required"] is True,
        }
        if spec["candidate_workflow_supported"]:
            if not isinstance(candidate_workflow, Mapping):
                raise RuntimeError("Managed-data candidate workflow did not complete.")
            metadata["candidate_workflow"] = _safe_region_plan_candidate_result(
                candidate_workflow, version=version
            )
        elif candidate_workflow is not None:
            raise RuntimeError("Unexpected candidate workflow result for managed data.")
        _write_private_bytes(
            staging / _MANAGED_DATA_METADATA_NAME, _json_payload_bytes(metadata)
        )
        os.replace(staging, final_root)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def upload_managed_data_file(
    *,
    scope: str,
    dataset_id: str,
    file_name: str,
    file_bytes: bytes,
    expected_sha256: str,
    confirm: bool,
    config_path: str,
) -> dict[str, Any]:
    scope = _require_managed_data_scope(scope)
    if confirm is not True:
        raise PermissionError("Managed-data upload requires explicit confirmation.")
    spec, payload_name, _ = _managed_data_validation(
        scope=scope,
        dataset_id=dataset_id,
        file_name=file_name,
        file_bytes=file_bytes,
    )
    version = hashlib.sha256(file_bytes).hexdigest()
    if (
        not _MANAGED_DATA_VERSION.fullmatch(str(expected_sha256).strip().lower())
        or str(expected_sha256).strip().lower() != version
    ):
        raise ValueError("Managed-data upload preview checksum is stale or invalid.")
    profile = _load_remote_profile(config_path)
    if profile.get("allow_upload") is not True:
        raise PermissionError("Managed-data upload is disabled in the local deployment profile.")
    release_id = f"managed-data-{scope}-{spec['dataset_id']}-{uuid.uuid4().hex[:12]}"
    remote_root = _managed_data_remote_root(profile, scope, spec["dataset_id"], version)
    remote_target = (remote_root / payload_name).as_posix()
    created_at = _now()
    status = "uploaded"
    candidate_workflow: dict[str, Any] | None = None
    fixed_region_bundle_capability_verified = False
    with _MANAGED_DATA_LOCK:
        with _remote_session_factory(profile) as remote:
            if spec.get("fixed_region_bundle_supported") is True:
                # Do this immutable pinned-release inspection before the first
                # remote write.  A missing/old Admin Tools release must never
                # leave an otherwise unusable bundle on the server.
                _verify_fixed_region_plan_bundle_admin_release(
                    remote,
                    _master_admin_context(profile, _FIXED_REGION_PLAN_BUNDLE_SCOPE),
                )
                fixed_region_bundle_capability_verified = True
            with remote.deployment_lock(str(profile["remote_root"]), release_id):
                if remote.exists(remote_target):
                    remote_checksum = remote.sha256(remote_target)
                    if remote_checksum != version:
                        raise RuntimeError("Managed-data immutable remote version collision.")
                    if remote.mode(remote_target) == _SECURE_CONFIG_MODE:
                        status = "already_exists"
                    else:
                        remote.upload_bytes_atomic(file_bytes, remote_target, backup=None)
                else:
                    remote.upload_bytes_atomic(file_bytes, remote_target, backup=None)
                if (
                    remote.sha256(remote_target) != version
                    or remote.mode(remote_target) != _SECURE_CONFIG_MODE
                ):
                    raise RuntimeError("Managed-data remote checksum or mode verification failed.")
                if spec["candidate_workflow_supported"]:
                    # The source remains an immutable managed-data version;
                    # candidate construction is delegated to the single
                    # packaged Admin Tools bridge.  It has no production mode,
                    # table argument, arbitrary destination, or direct DB
                    # apply path.
                    candidate_workflow = _run_region_plan_candidate_command(
                        remote,
                        profile=profile,
                        source_path=remote_target,
                        version=version,
                    )
        _finalize_managed_data_local_version(
            scope=scope,
            spec=spec,
            version=version,
            payload_name=payload_name,
            payload=file_bytes,
            created_at=created_at,
            candidate_workflow=candidate_workflow,
        )
    receipt = {
        "id": release_id,
        "release_id": release_id,
        "version": version,
        "environment": scope,
        "scope": scope,
        "kind": "managed-data",
        "dataset_id": spec["dataset_id"],
        "created_at": created_at,
        "status": status,
        "sha256": version,
        "size_bytes": len(file_bytes),
        "target_id": _managed_data_target_id(profile, scope),
        "remote_verified": True,
        "mode": _SECURE_CONFIG_MODE_TEXT,
        "lifecycle_stage": spec["lifecycle_stage"],
        "promotion_required": spec["promotion_required"] is True,
        "operator": os.environ.get("USERNAME") or os.environ.get("USER") or "local-console",
    }
    if fixed_region_bundle_capability_verified:
        receipt["admin_tools_capability_verified"] = True
    if candidate_workflow is not None:
        receipt["candidate_workflow"] = candidate_workflow
    _append_history(receipt)
    response = {
        "status": status,
        "scope": scope,
        "dataset_id": spec["dataset_id"],
        "version": version,
        "sha256": version,
        "size_bytes": len(file_bytes),
        "remote_verified": True,
        "lifecycle_stage": spec["lifecycle_stage"],
        "promotion_required": spec["promotion_required"] is True,
    }
    if fixed_region_bundle_capability_verified:
        response["admin_tools_capability_verified"] = True
    if candidate_workflow is not None:
        response["candidate_workflow"] = candidate_workflow
    return response


def start_managed_data_upload_job(*, scope: str, dataset_id: str, file_name: str,
                                  file_bytes: bytes, expected_sha256: str,
                                  config_path: str) -> dict[str, Any]:
    """Queue remote upload/candidate generation and return without blocking the UI."""
    scope = _require_managed_data_scope(scope)
    digest = hashlib.sha256(file_bytes).hexdigest()
    if str(expected_sha256).strip().lower() != digest:
        raise ValueError("Managed-data upload checksum is stale or invalid.")
    job_id = str(uuid.uuid4())
    with _MANAGED_DATA_JOBS_LOCK:
        _MANAGED_DATA_JOBS[job_id] = {"job_id": job_id, "status": "queued", "sha256": digest, "created_at": _now()}

    def run() -> None:
        with _MANAGED_DATA_JOBS_LOCK:
            _MANAGED_DATA_JOBS[job_id]["status"] = "running"
        try:
            result = upload_managed_data_file(scope=scope, dataset_id=dataset_id, file_name=file_name,
                                              file_bytes=file_bytes, expected_sha256=digest,
                                              confirm=True, config_path=config_path)
            with _MANAGED_DATA_JOBS_LOCK:
                _MANAGED_DATA_JOBS[job_id].update({"status": "completed", "result": result, "completed_at": _now()})
        except Exception as exc:
            with _MANAGED_DATA_JOBS_LOCK:
                _MANAGED_DATA_JOBS[job_id].update({"status": "failed", "error": _safe_backend_error(exc), "completed_at": _now()})
    _MANAGED_DATA_EXECUTOR.submit(run)
    return {"status": "queued", "job_id": job_id, "sha256": digest}


def get_managed_data_upload_job(*, job_id: str) -> dict[str, Any]:
    try:
        uuid.UUID(str(job_id))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("job_id is invalid.") from exc
    with _MANAGED_DATA_JOBS_LOCK:
        job = _MANAGED_DATA_JOBS.get(str(job_id))
        if job is None:
            raise KeyError("Managed-data upload job was not found.")
        return dict(job)


def list_managed_data_versions(*, scope: str, dataset_id: str) -> dict[str, Any]:
    scope = _require_managed_data_scope(scope)
    spec = _managed_data_spec(scope, dataset_id)
    dataset_root = _within(MANAGED_DATA_ROOT / scope / spec["dataset_id"], MANAGED_DATA_ROOT)
    versions: list[dict[str, Any]] = []
    if dataset_root.is_dir():
        for path in dataset_root.iterdir():
            if not path.is_dir() or path.name.startswith("."):
                continue
            if not _MANAGED_DATA_VERSION.fullmatch(path.name):
                raise RuntimeError("Managed-data local version directory is invalid.")
            metadata, _ = _load_managed_data_version(
                scope=scope, dataset_id=spec["dataset_id"], version=path.name
            )
            versions.append(_managed_data_version_public(metadata, spec))
    versions.sort(key=lambda item: (item["created_at"], item["version"]), reverse=True)
    return {
        "status": "ok",
        "scope": scope,
        "dataset_id": spec["dataset_id"],
        "versions": versions,
    }


def preview_managed_data_version(
    *, scope: str, dataset_id: str, version: str
) -> dict[str, Any]:
    scope = _require_managed_data_scope(scope)
    version = str(version).strip().lower()
    metadata, payload = _load_managed_data_version(
        scope=scope, dataset_id=dataset_id, version=version
    )
    spec, _, validated = _managed_data_validation(
        scope=scope,
        dataset_id=dataset_id,
        file_name=str(metadata["payload_name"]),
        file_bytes=payload,
    )
    return {
        "status": "ready",
        "scope": scope,
        "dataset_id": spec["dataset_id"],
        **_managed_data_version_public(metadata, spec),
        "summary": validated["summary"],
        "sample": validated["sample"],
    }


def _remember_managed_data_db_preview(
    *, result: Mapping[str, Any], dataset_id: str, version: str, target_environment: str
) -> None:
    preview_id, preview_digest = _require_preview_token(
        str(result.get("preview_id", "")), str(result.get("preview_digest", ""))
    )
    now = datetime.now(timezone.utc)
    with _MANAGED_DATA_DB_PREVIEW_LOCK:
        expired = [
            identifier
            for identifier, (_, _, _, _, issued_at) in _MANAGED_DATA_DB_PREVIEWS.items()
            if now - issued_at > _MANAGED_DATA_DB_PREVIEW_TTL
        ]
        for identifier in expired:
            _MANAGED_DATA_DB_PREVIEWS.pop(identifier, None)
        _MANAGED_DATA_DB_PREVIEWS[preview_id] = (
            preview_digest,
            dataset_id,
            version,
            target_environment,
            now,
        )


def _safe_managed_data_db_result(
    result: Mapping[str, Any], *, dataset_id: str, version: str, target_environment: str
) -> dict[str, Any]:
    allowed = {
        "status",
        "preview_id",
        "preview_digest",
        "expires_at",
        "create_count",
        "update_count",
        "unchanged_count",
        "masked_samples",
        "errors",
        "operation_id",
        "created_at",
        "completed_at",
    }
    safe = {key: result[key] for key in allowed if key in result}
    return {
        **_redact_master_payload(safe),
        "dataset_id": dataset_id,
        "version": version,
        "target_environment": target_environment,
    }


def _managed_data_heavy_db_payload(
    *, metadata: Mapping[str, Any], payload: bytes
) -> tuple[bytes, dict[str, Any]]:
    registry = _managed_data_registry_module()
    normalizer = getattr(registry, "normalize_heavy_repair_rules", None)
    if not callable(normalizer):
        raise RuntimeError("Managed-data heavy-repair normalizer is unavailable.")
    normalized = normalizer(str(metadata["payload_name"]), payload)
    canonical = getattr(normalized, "canonical_csv", None)
    if not isinstance(canonical, bytes) or not canonical:
        raise RuntimeError("Managed-data heavy-repair normalization returned no CSV.")
    accounting = (
        dict(normalized.as_dict())
        if hasattr(normalized, "as_dict")
        else dict(normalized)
        if isinstance(normalized, Mapping)
        else {}
    )
    if accounting.get("source_sha256") != metadata["version"]:
        raise RuntimeError("Managed-data heavy-repair source binding is invalid.")
    if accounting.get("rejected_rows") != 0:
        raise ValueError(
            "Managed-data DB sync requires zero rejected heavy-repair rows."
        )
    canonical_sha256 = hashlib.sha256(canonical).hexdigest()
    if accounting.get("canonical_sha256") != canonical_sha256:
        raise RuntimeError("Managed-data heavy-repair canonical checksum is invalid.")
    return canonical, _redact_master_payload(accounting)


def _technician_profile_source_path(profile: Mapping[str, Any], version: str) -> str:
    """Return the one immutable development workbook path for a version."""

    normalized_version = str(version).strip().lower()
    if not _MANAGED_DATA_VERSION.fullmatch(normalized_version):
        raise ValueError("version must be a SHA-256 digest.")
    return (
        _managed_data_remote_root(
            profile,
            _TECHNICIAN_PROFILE_SCOPE,
            _TECHNICIAN_PROFILE_DATASET,
            normalized_version,
        )
        / "payload.xlsx"
    ).as_posix()


def _technician_profile_admin_command(
    context: Mapping[str, str], command: str, arguments: Iterable[str] = ()
) -> str:
    """Construct one fixed composite technician-profile Admin Tools command."""

    if context.get("environment") != _TECHNICIAN_PROFILE_SCOPE:
        raise PermissionError("Technician-profile operations are development-only.")
    if command not in _TECHNICIAN_PROFILE_ADMIN_COMMANDS:
        raise ValueError("Technician-profile operation is not allowlisted.")
    argv = [
        context["python_path"],
        "-B",
        "-m",
        _TECHNICIAN_PROFILE_ADMIN_MODULE,
        "--json",
        command,
        "--config",
        context["config_path"],
        *[str(item) for item in arguments],
    ]
    return f"cd -- {shlex.quote(context['release_root'])} && exec " + " ".join(
        shlex.quote(item) for item in argv
    )


def _technician_profile_preview_command(
    context: Mapping[str, str], *, source_path: str, version: str
) -> str:
    normalized_version = str(version).strip().lower()
    source = PurePosixPath(str(source_path))
    if (
        not _MANAGED_DATA_VERSION.fullmatch(normalized_version)
        or not source.is_absolute()
        or ".." in source.parts
    ):
        raise ValueError("Technician-profile source binding is invalid.")
    # ``execute_technician_profile_json`` independently derives the source
    # path from its connection profile.  Retain a local path shape check here
    # so direct helper callers cannot construct an unrelated workbook command.
    expected_suffix = (
        "state",
        _TECHNICIAN_PROFILE_SCOPE,
        "managed_data",
        _TECHNICIAN_PROFILE_DATASET,
        normalized_version,
        "payload.xlsx",
    )
    if tuple(source.parts[-len(expected_suffix) :]) != expected_suffix:
        raise ValueError("Technician-profile source binding is invalid.")
    return _technician_profile_admin_command(
        context,
        "preview",
        (
            "--source",
            source.as_posix(),
            "--source-sha256",
            normalized_version,
            "--managed-version",
            normalized_version,
            "--environment",
            _TECHNICIAN_PROFILE_SCOPE,
        ),
    )


def _verify_technician_profile_admin_release(
    remote: Any, context: Mapping[str, str]
) -> None:
    """Require the composite workflow module in the exact pinned release."""

    _verify_master_admin_release(remote, context)
    inspection = inspect_artifact(
        path=str(
            _artifact_root(_TECHNICIAN_PROFILE_SCOPE, "admin-tools")
            / context["release_version"]
        ),
        kind="admin-tools",
        environment=_TECHNICIAN_PROFILE_SCOPE,
    )
    if (
        "admin_tools/db/technician_profile_backend.py"
        not in _release_file_map(inspection)
    ):
        raise PermissionError(
            "Pinned Admin Tools release does not provide technician-profile sync."
        )


def _parse_technician_profile_json(
    stdout: str,
    *,
    context: Mapping[str, str],
    command: str,
    version: str,
    preview_id: str | None = None,
    preview_digest: str | None = None,
) -> dict[str, Any]:
    """Validate the narrow PII-free composite workflow result envelope."""

    raw = str(stdout or "").strip()
    if not raw:
        raise RuntimeError("Remote technician-profile command returned no JSON result.")
    if len(raw.encode("utf-8")) > _MASTER_ADMIN_MAX_JSON_BYTES:
        raise RuntimeError("Remote technician-profile result exceeds the safe response limit.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Remote technician-profile command returned invalid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("Remote technician-profile command returned a non-object JSON result.")
    if payload.get("contract_version") != _TECHNICIAN_PROFILE_CONTRACT_VERSION:
        raise RuntimeError("Remote technician-profile contract version mismatch.")
    if (
        payload.get("environment") != context["environment"]
        or payload.get("dbname") != context["dbname"]
        or payload.get("target_id") != context["target_id"]
    ):
        raise RuntimeError(
            "Remote technician-profile result target does not match development."
        )
    normalized_version = str(version).strip().lower()
    if payload.get("managed_version") != normalized_version:
        raise RuntimeError(
            "Remote technician-profile result does not match the immutable workbook version."
        )
    status = payload.get("status")
    if command == "preview":
        if status != "ready":
            raise RuntimeError("Remote technician-profile preview was not ready.")
        if payload.get("source_sha256") != normalized_version:
            raise RuntimeError(
                "Remote technician-profile preview source does not match the immutable workbook version."
            )
        issued_preview_id, issued_preview_digest = _require_preview_token(
            str(payload.get("preview_id", "")), str(payload.get("preview_digest", ""))
        )
        confirmation = payload.get("confirmation_token")
        if (
            not isinstance(confirmation, str)
            or not confirmation
            or len(confirmation) > 512
            or any(ord(char) < 32 for char in confirmation)
        ):
            raise RuntimeError(
                "Remote technician-profile preview confirmation token is invalid."
            )
        result = dict(payload)
        result["preview_id"] = issued_preview_id
        result["preview_digest"] = issued_preview_digest
        result["_private_confirmation_token"] = confirmation
        return result
    if status not in {"applied", "already_applied"}:
        raise RuntimeError("Remote technician-profile apply was not accepted.")
    if (
        preview_id is None
        or preview_digest is None
        or payload.get("preview_id") != preview_id
        or payload.get("preview_digest") != preview_digest
    ):
        raise RuntimeError("Remote technician-profile apply result preview binding is invalid.")
    return dict(payload)


def _safe_technician_profile_result(
    result: Mapping[str, Any],
    *,
    version: str,
    target_environment: str,
) -> dict[str, Any]:
    """Expose only bounded counters and codes from a potentially sensitive workbook."""

    safe: dict[str, Any] = {}
    for key in (
        "status",
        "preview_id",
        "preview_digest",
        "expires_at",
        "operation_id",
        "created_at",
        "completed_at",
        "plan_id",
        "region_mapping_source",
    ):
        value = result.get(key)
        if isinstance(value, str) and value and len(value) <= 255:
            safe[key] = _redact(value)
    for key in (
        "technician_create_count",
        "technician_update_count",
        "technician_unchanged_count",
        "technician_applied_count",
        "capability_create_count",
        "capability_update_count",
        "capability_unchanged_count",
        "capability_delete_count",
        "capability_applied_count",
        "region_mapping_create_count",
        "region_mapping_update_count",
        "region_mapping_unchanged_count",
        "region_mapping_verified_count",
        "rejected_count",
        "error_count",
    ):
        value = result.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 10_000_000:
            safe[key] = value
    errors = result.get("errors")
    if errors is not None:
        if not isinstance(errors, list) or len(errors) > 100:
            raise RuntimeError("Remote technician-profile errors are invalid.")
        safe_errors: list[dict[str, Any]] = []
        for item in errors:
            if not isinstance(item, Mapping):
                raise RuntimeError("Remote technician-profile errors are invalid.")
            code = item.get("code")
            count = item.get("count")
            if (
                not isinstance(code, str)
                or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", code)
                or isinstance(count, bool)
                or not isinstance(count, int)
                or not 0 <= count <= 10_000_000
            ):
                raise RuntimeError("Remote technician-profile errors are invalid.")
            safe_errors.append({"code": code, "count": count})
        safe["errors"] = safe_errors
    return {
        **safe,
        "dataset_id": _TECHNICIAN_PROFILE_DATASET,
        "version": version,
        "target_environment": target_environment,
    }


def _run_technician_profile_admin_command(
    remote: Any,
    *,
    context: Mapping[str, str],
    command: str,
    arguments: Iterable[str] = (),
    version: str,
    preview_id: str | None = None,
    preview_digest: str | None = None,
) -> dict[str, Any]:
    _verify_technician_profile_admin_release(remote, context)
    code, stdout, stderr = remote.execute_technician_profile_json(
        _technician_profile_admin_command(context, command, arguments),
        timeout=_MASTER_ADMIN_TIMEOUT_SECONDS,
    )
    if code != 0:
        if str(stdout or "").strip():
            with contextlib.suppress(RuntimeError):
                return _parse_technician_profile_json(
                    stdout,
                    context=context,
                    command=command,
                    version=version,
                    preview_id=preview_id,
                    preview_digest=preview_digest,
                )
        detail = _redact(stderr).strip()
        suffix = f" ({detail[:240]})" if detail else ""
        raise RuntimeError(f"Remote technician-profile {command} command failed{suffix}")
    return _parse_technician_profile_json(
        stdout,
        context=context,
        command=command,
        version=version,
        preview_id=preview_id,
        preview_digest=preview_digest,
    )


def _verify_technician_profile_remote_source(
    remote: Any, *, profile: Mapping[str, Any], version: str
) -> str:
    source_path = _technician_profile_source_path(profile, version)
    if (
        not remote.exists(source_path)
        or remote.sha256(source_path) != version
        or remote.mode(source_path) != _SECURE_CONFIG_MODE
    ):
        raise PermissionError(
            "Technician-profile managed workbook is unavailable or no longer immutable."
        )
    return source_path


def preview_managed_technician_profile_sync(
    *, version: str, target_environment: str
) -> dict[str, Any]:
    """Preview one immutable development technician workbook composite update."""

    target_environment = _require_environment(target_environment)
    if target_environment != _TECHNICIAN_PROFILE_SCOPE:
        raise PermissionError(
            "Production technician-profile DB sync is disabled by policy."
        )
    version = str(version).strip().lower()
    metadata, payload = _load_managed_data_version(
        scope=_TECHNICIAN_PROFILE_SCOPE,
        dataset_id=_TECHNICIAN_PROFILE_DATASET,
        version=version,
    )
    spec, payload_name, _ = _managed_data_validation(
        scope=_TECHNICIAN_PROFILE_SCOPE,
        dataset_id=_TECHNICIAN_PROFILE_DATASET,
        file_name=str(metadata["payload_name"]),
        file_bytes=payload,
    )
    if (
        spec["db_sync_supported"] is not True
        or payload_name != "payload.xlsx"
        or str(metadata.get("payload_name")) != "payload.xlsx"
    ):
        raise PermissionError("This managed dataset does not support technician-profile sync.")
    profile = _master_admin_profile()
    context = _master_admin_context(profile, _TECHNICIAN_PROFILE_SCOPE)
    with _remote_session_factory(profile) as remote:
        source_path = _verify_technician_profile_remote_source(
            remote, profile=profile, version=version
        )
        result = _run_technician_profile_admin_command(
            remote,
            context=context,
            command="preview",
            arguments=(
                "--source",
                source_path,
                "--source-sha256",
                version,
                "--managed-version",
                version,
                "--environment",
                _TECHNICIAN_PROFILE_SCOPE,
            ),
            version=version,
        )
    preview_id, preview_digest = _require_preview_token(
        str(result.get("preview_id", "")), str(result.get("preview_digest", ""))
    )
    confirmation = result.get("_private_confirmation_token")
    if not isinstance(confirmation, str):
        raise RuntimeError("Technician-profile preview confirmation is unavailable.")
    now = datetime.now(timezone.utc)
    with _TECHNICIAN_PROFILE_PREVIEW_LOCK:
        expired = [
            identifier
            for identifier, (*_, issued_at, _idempotency) in _TECHNICIAN_PROFILE_PREVIEWS.items()
            if now - issued_at > _MANAGED_DATA_DB_PREVIEW_TTL
        ]
        for identifier in expired:
            _TECHNICIAN_PROFILE_PREVIEWS.pop(identifier, None)
        _TECHNICIAN_PROFILE_PREVIEWS[preview_id] = (
            preview_digest,
            version,
            target_environment,
            context["target_id"],
            context["remote_target_id"],
            confirmation,
            now,
            None,
        )
    return _safe_technician_profile_result(
        result, version=version, target_environment=target_environment
    )


def apply_managed_technician_profile_sync(
    *,
    preview_id: str,
    preview_digest: str,
    idempotency_key: str,
    target_environment: str,
    confirm: bool,
) -> dict[str, Any]:
    """Apply only the local-process-bound development workbook preview."""

    target_environment = _require_environment(target_environment)
    if target_environment != _TECHNICIAN_PROFILE_SCOPE:
        raise PermissionError(
            "Production technician-profile DB sync is disabled by policy."
        )
    if confirm is not True:
        raise PermissionError("Technician-profile DB sync requires explicit confirmation.")
    preview_id, preview_digest = _require_preview_token(preview_id, preview_digest)
    try:
        idempotency = str(uuid.UUID(str(idempotency_key)))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("idempotency_key must be a UUID.") from exc
    now = datetime.now(timezone.utc)
    with _TECHNICIAN_PROFILE_PREVIEW_LOCK:
        binding = _TECHNICIAN_PROFILE_PREVIEWS.get(preview_id)
        if binding is None:
            raise PermissionError("Technician-profile preview binding is unavailable.")
        (
            digest,
            version,
            environment,
            target_id,
            remote_target_id,
            confirmation,
            issued_at,
            bound_idempotency,
        ) = binding
        if now - issued_at > _MANAGED_DATA_DB_PREVIEW_TTL:
            _TECHNICIAN_PROFILE_PREVIEWS.pop(preview_id, None)
            raise PermissionError("Technician-profile preview binding has expired.")
        if digest != preview_digest or environment != target_environment:
            raise PermissionError(
                "Technician-profile preview binding does not match the request."
            )
        if bound_idempotency not in {None, idempotency}:
            raise PermissionError(
                "Technician-profile preview is already bound to another idempotency key."
            )
        _TECHNICIAN_PROFILE_PREVIEWS[preview_id] = (*binding[:-1], idempotency)
    # The immutable local version and remote source are checked again before a
    # remote apply.  A process restart intentionally discards the preview
    # token, while source corruption/target drift fail closed here.
    metadata, _payload = _load_managed_data_version(
        scope=_TECHNICIAN_PROFILE_SCOPE,
        dataset_id=_TECHNICIAN_PROFILE_DATASET,
        version=version,
    )
    if metadata.get("payload_name") != "payload.xlsx":
        raise RuntimeError("Technician-profile managed workbook metadata is invalid.")
    profile = _master_admin_profile()
    context = _master_admin_context(profile, _TECHNICIAN_PROFILE_SCOPE)
    if (
        context["target_id"] != target_id
        or context["remote_target_id"] != remote_target_id
    ):
        raise PermissionError("Technician-profile preview binding target has changed.")
    with _remote_session_factory(profile) as remote:
        _verify_technician_profile_remote_source(remote, profile=profile, version=version)
        result = _run_technician_profile_admin_command(
            remote,
            context=context,
            command="apply",
            arguments=(
                "--preview-id",
                preview_id,
                "--preview-digest",
                preview_digest,
                "--idempotency-key",
                idempotency,
                "--confirmation",
                confirmation,
                "--environment",
                _TECHNICIAN_PROFILE_SCOPE,
            ),
            version=version,
            preview_id=preview_id,
            preview_digest=preview_digest,
        )
    return _safe_technician_profile_result(
        result, version=version, target_environment=target_environment
    )


def preview_managed_data_db_sync(
    *, dataset_id: str, version: str, target_environment: str
) -> dict[str, Any]:
    target_environment = _require_environment(target_environment)
    if target_environment == "production":
        raise PermissionError("Production managed-data DB sync is disabled by policy.")
    if str(dataset_id).strip() == _TECHNICIAN_PROFILE_DATASET:
        return preview_managed_technician_profile_sync(
            version=version, target_environment=target_environment
        )
    if str(dataset_id).strip() != _MANAGED_DATA_DB_DATASET:
        raise ValueError("This managed dataset does not support DB sync.")
    version = str(version).strip().lower()
    metadata, payload = _load_managed_data_version(
        scope="common", dataset_id=_MANAGED_DATA_DB_DATASET, version=version
    )
    spec, _, _ = _managed_data_validation(
        scope="common",
        dataset_id=_MANAGED_DATA_DB_DATASET,
        file_name=str(metadata["payload_name"]),
        file_bytes=payload,
    )
    if spec["db_sync_supported"] is not True:
        raise PermissionError("This managed dataset does not support DB sync.")
    canonical_csv, accounting = _managed_data_heavy_db_payload(
        metadata=metadata, payload=payload
    )
    result = preview_master_csv_upsert(
        environment=target_environment,
        table_id=_MANAGED_DATA_DB_TABLE,
        file_name="heavy_repair_rules.csv",
        csv_bytes=canonical_csv,
    )
    if result.get("status") != "ready":
        raise RuntimeError("Managed-data DB sync preview was not ready.")
    _remember_managed_data_db_preview(
        result=result,
        dataset_id=_MANAGED_DATA_DB_DATASET,
        version=version,
        target_environment=target_environment,
    )
    safe = _safe_managed_data_db_result(
        result,
        dataset_id=_MANAGED_DATA_DB_DATASET,
        version=version,
        target_environment=target_environment,
    )
    safe["normalization"] = accounting
    return safe


def apply_managed_data_db_sync(
    *,
    preview_id: str,
    preview_digest: str,
    idempotency_key: str,
    target_environment: str,
    confirm: bool,
) -> dict[str, Any]:
    target_environment = _require_environment(target_environment)
    if target_environment == "production":
        raise PermissionError("Production managed-data DB sync is disabled by policy.")
    if confirm is not True:
        raise PermissionError("Managed-data DB sync requires explicit confirmation.")
    preview_id, preview_digest = _require_preview_token(preview_id, preview_digest)
    with _TECHNICIAN_PROFILE_PREVIEW_LOCK:
        technician_preview = preview_id in _TECHNICIAN_PROFILE_PREVIEWS
    if technician_preview:
        return apply_managed_technician_profile_sync(
            preview_id=preview_id,
            preview_digest=preview_digest,
            idempotency_key=idempotency_key,
            target_environment=target_environment,
            confirm=confirm,
        )
    now = datetime.now(timezone.utc)
    with _MANAGED_DATA_DB_PREVIEW_LOCK:
        binding = _MANAGED_DATA_DB_PREVIEWS.get(preview_id)
        if binding is None:
            raise PermissionError("Managed-data DB preview binding is unavailable.")
        digest, dataset_id, version, environment, issued_at = binding
        if now - issued_at > _MANAGED_DATA_DB_PREVIEW_TTL:
            _MANAGED_DATA_DB_PREVIEWS.pop(preview_id, None)
            raise PermissionError("Managed-data DB preview binding has expired.")
        if digest != preview_digest or environment != target_environment:
            raise PermissionError("Managed-data DB preview binding does not match the request.")
    # Re-read the immutable local source before delegating the bound remote
    # preview apply. This detects local removal or corruption after preview.
    _load_managed_data_version(scope="common", dataset_id=dataset_id, version=version)
    result = apply_master_csv_upsert(
        environment=target_environment,
        preview_id=preview_id,
        preview_digest=preview_digest,
        idempotency_key=idempotency_key,
        confirm=True,
    )
    return _safe_managed_data_db_result(
        result,
        dataset_id=dataset_id,
        version=version,
        target_environment=target_environment,
    )


def _admin_tools_release_mode(manifest: Mapping[str, Any]) -> str | None:
    """Return the only executable Admin Tools artifact policy modes.

    A dirty release is deliberately useful only for explicit development
    verification.  There is no mixed state: a source-clean release must be
    promotable and a dirty release must be non-promotable.
    """

    if manifest.get("source_dirty") is True and manifest.get("promotable") is False:
        return "development-verification"
    if manifest.get("source_dirty") is False and manifest.get("promotable") is True:
        return "clean"
    return None


def _pin_verified_admin_tools_release(item: ArtifactInspection) -> dict[str, str]:
    """Write the eligible local Admin Tools pin only after a trusted upload.

    Dirty artifacts can update only the development verification pin.  Clean
    artifacts atomically refresh both the common production-capable pin and
    the development pin.  A pin failure never reclassifies an already verified
    remote upload as failed.
    """

    if item.kind != "admin-tools":
        return {"status": "not_applicable"}
    mode = _admin_tools_release_mode(item.manifest)
    if mode is None:
        return {
            "status": "not_pinned_policy",
            "reason": "invalid_admin_tools_release_policy",
        }
    if mode == "development-verification" and item.environment != "development":
        return {
            "status": "not_pinned_policy",
            "reason": "development_verification_requires_development_target",
        }
    version = str(item.version).strip()
    if not _MASTER_ADMIN_RELEASE_VERSION.fullmatch(version):
        # The artifact inspection path currently constrains the version, but
        # retain a local fail-closed guard at the profile write boundary.
        return {"status": "not_pinned_invalid_version"}
    try:
        with _CONNECTION_SETTINGS_LOCK:
            _recover_connection_settings_transaction()
            profile_path = _connection_profile_path()
            profile = _read_json(profile_path)
            _require_fixed_credentials_reference(profile)
            updated_profile = dict(profile)
            if mode == "development-verification":
                if (
                    str(
                        profile.get("admin_tools_development_release_version", "")
                    ).strip()
                    == version
                ):
                    return {
                        "status": "already_pinned_development_verification",
                        "version": version,
                    }
                updated_profile["admin_tools_development_release_version"] = version
                _commit_connection_settings_transaction([(profile_path, updated_profile)])
                return {
                    "status": "pinned_development_verification",
                    "version": version,
                }
            if (
                str(profile.get("admin_tools_release_version", "")).strip() == version
                and str(
                    profile.get("admin_tools_development_release_version", "")
                ).strip()
                == version
            ):
                return {
                    "status": "already_pinned_common_and_development",
                    "version": version,
                }
            updated_profile["admin_tools_release_version"] = version
            updated_profile["admin_tools_development_release_version"] = version
            _commit_connection_settings_transaction([(profile_path, updated_profile)])
        return {"status": "pinned_common_and_development", "version": version}
    except Exception:
        # Never serialize an OS/config exception here: profile files can carry
        # operator-specific context. The history/result code is intentionally
        # stable and non-secret.
        return {"status": "pin_failed", "error_code": "local_profile_write_failed"}


def upload_artifact(
    *,
    inspection: ArtifactInspection | Mapping[str, Any],
    selected_files: Iterable[str],
    config_path: str,
    typed_confirmation: str,
    expected_remote_checksums: Mapping[str, str | None] | None = None,
    dry_run: bool = True,
    progress_callback: Callable[[int, int, str, str], None] | None = None,
) -> dict[str, Any]:
    item = _inspection(inspection)
    if typed_confirmation != item.required_confirmation:
        raise ValueError("Typed deployment confirmation mismatch.")
    if item.restricted_data:
        raise ValueError("Artifact contains forbidden secret/config files.")
    chosen = _selected_release_files(item, selected_files)
    profile = _load_remote_profile(config_path)
    manifest_files = {
        relative: checksum
        for relative, (checksum, _) in _release_file_map(item).items()
    }
    selected_names = {relative for relative, _, _ in chosen}
    selected_full_manifest = selected_names == set(manifest_files)
    remote_manifest_verified = False
    complete_manifest = False
    service_eligible = False
    target_id = _target_id(profile, item.environment)
    if dry_run:
        return {
            "status": "dry_run",
            "sha256": item.archive_sha256,
            "selected_full_manifest": selected_full_manifest,
            "remote_manifest_verified": False,
            "complete_manifest": False,
            "service_eligible": False,
        }
    if not profile["allow_upload"]:
        raise PermissionError("Upload is disabled in the local deployment profile.")

    deployment_id = f"{item.environment}-{item.kind}-{item.version}-{uuid.uuid4().hex[:12]}"
    backup_root = posixpath.join(
        str(profile["remote_root"]), ".deployment_backups", deployment_id
    )
    changes: list[dict[str, Any]] = []
    attempted: list[dict[str, Any]] = []
    verified_files: list[dict[str, str]] = []
    compensation_succeeded = False
    completed_files = 0
    total_files = len(chosen)
    expected_remote_checksums = {
        _safe_relative(path): (str(checksum).lower() if checksum else None)
        for path, checksum in (expected_remote_checksums or {}).items()
    }
    try:
        with _remote_session_factory(profile) as remote:
            with remote.deployment_lock(str(profile["remote_root"]), deployment_id):
                try:
                    for relative, checksum, local in chosen:
                        target = _remote_target(item, relative)
                        existed = remote.exists(target)
                        previous_checksum = remote.sha256(target) if existed else None
                        if relative in expected_remote_checksums and previous_checksum != expected_remote_checksums[relative]:
                            raise RuntimeError(
                                "Selected remote file changed after preview; upload aborted: "
                                + relative
                            )
                        if existed and (
                            not previous_checksum
                            or not re.fullmatch(r"[0-9a-f]{64}", previous_checksum)
                        ):
                            raise RuntimeError(
                                "Existing remote target checksum could not be verified."
                            )
                        backup = posixpath.join(backup_root, relative) if existed else None
                        change = {
                            "path": relative,
                            "target": target,
                            "sha256": checksum,
                            "backup": backup,
                            "previous_sha256": previous_checksum,
                            "created": not existed,
                        }
                        attempted.append(change)
                        remote.upload_atomic(local, target, backup)
                        if remote.sha256(target) != checksum:
                            raise RuntimeError("Remote post-upload checksum mismatch.")
                        if backup:
                            backup_checksum = remote.sha256(backup)
                            if backup_checksum != previous_checksum:
                                raise RuntimeError("Remote deployment backup checksum is invalid.")
                            change["backup_sha256"] = backup_checksum
                        changes.append(change)
                        completed_files += 1
                        if progress_callback is not None:
                            # UI telemetry must never alter the remote transaction.
                            # ``relative`` is a validated manifest-relative path.
                            with contextlib.suppress(Exception):
                                progress_callback(
                                    completed_files, total_files, relative, "verified"
                                )
                    if selected_full_manifest:
                        verified_files = []
                        for relative, checksum in sorted(manifest_files.items()):
                            target = _remote_target(item, relative)
                            if remote.sha256(target) != checksum:
                                raise RuntimeError(
                                    "Remote manifest verification failed after upload: " + relative
                                )
                            verified_files.append(
                                {"path": relative, "target": target, "sha256": checksum}
                            )
                        remote_manifest_verified = True
                        complete_manifest = True
                        service_eligible = item.kind == "runtime"
                except Exception as upload_error:
                    compensation_errors: list[str] = []
                    for change in reversed(attempted):
                        try:
                            backup = change.get("backup")
                            target = str(change["target"])
                            current_checksum = remote.sha256(target)
                            previous_checksum = change.get("previous_sha256")
                            backup_checksum = (
                                remote.sha256(str(backup)) if backup else None
                            )
                            if backup and backup_checksum == previous_checksum:
                                if current_checksum != previous_checksum:
                                    remote.copy(str(backup), target)
                                if remote.sha256(target) != previous_checksum:
                                    raise RuntimeError("Backup restoration checksum mismatch.")
                            elif backup and current_checksum == previous_checksum:
                                # Backup creation failed before the original target changed.
                                pass
                            elif backup:
                                raise RuntimeError(
                                    "Neither the original target nor a complete backup is available."
                                )
                            elif change.get("created") and remote.exists(target):
                                remote.remove(target)
                                if remote.exists(target):
                                    raise RuntimeError("Created target compensation failed.")
                        except Exception:
                            compensation_errors.append(str(change.get("path", "unknown")))
                    if compensation_errors:
                        raise RuntimeError(
                            "Upload failed and compensation was incomplete for: "
                            + ", ".join(compensation_errors)
                        ) from upload_error
                    compensation_succeeded = True
                    raise
    except Exception:
        with contextlib.suppress(Exception):
            _append_history(
                {
                    "id": deployment_id,
                    "release_id": deployment_id,
                    "version": item.version,
                    "environment": item.environment,
                    "kind": item.kind,
                    "created_at": _now(),
                    "status": "upload_failed",
                    "sha256": item.archive_sha256,
                    "target_id": target_id,
                    "selected_full_manifest": selected_full_manifest,
                    "remote_manifest_verified": False,
                    "complete_manifest": False,
                    "service_eligible": False,
                    "operator": os.environ.get("USERNAME")
                    or os.environ.get("USER")
                    or "local-console",
                    "rollback_available": False,
                    "compensated": compensation_succeeded,
                    "changes": attempted,
                }
            )
        raise
    entry = {
        "id": deployment_id,
        "release_id": deployment_id,
        "version": item.version,
        "environment": item.environment,
        "kind": item.kind,
        "created_at": _now(),
        "status": "uploaded",
        "sha256": item.archive_sha256,
        "target_id": target_id,
        "selected_full_manifest": selected_full_manifest,
        "remote_manifest_verified": remote_manifest_verified,
        "complete_manifest": complete_manifest,
        "service_eligible": service_eligible,
        "operator": os.environ.get("USERNAME") or os.environ.get("USER") or "local-console",
        "rollback_available": bool(changes)
        and all(change.get("backup") and change.get("backup_sha256") for change in changes),
        "changes": changes,
        "verified_files": verified_files,
    }
    if item.kind == "admin-tools":
        pin_result = (
            _pin_verified_admin_tools_release(item)
            if remote_manifest_verified and complete_manifest
            else {"status": "not_pinned_incomplete_remote_verification"}
        )
        entry["admin_tools_pin"] = pin_result
    _append_history(entry)
    result = {
        "status": "uploaded",
        "release_id": deployment_id,
        "environment": item.environment,
        "kind": item.kind,
        "version": item.version,
        "selected_full_manifest": selected_full_manifest,
        "remote_manifest_verified": remote_manifest_verified,
        "complete_manifest": complete_manifest,
        "service_eligible": service_eligible,
        "remote_path": item.target_upload_path,
        "sha256": item.archive_sha256,
    }
    if item.kind == "admin-tools":
        result["admin_tools_pin"] = pin_result
    return result


def _master_admin_context(profile: Mapping[str, Any], environment: str) -> dict[str, str]:
    """Resolve only the fixed remote Admin Tools and runtime paths.

    Database-console callers deliberately cannot supply a config path, Python
    executable, release path, SQL, or remote working directory.  The selected
    environment is the sole routing input; the local ignored SSH profile pins
    the immutable Admin Tools release.
    """

    environment = _require_environment(environment)
    common_version = str(profile.get("admin_tools_release_version", "")).strip()
    development_version = str(
        profile.get("admin_tools_development_release_version", "")
    ).strip()
    if environment == "development" and development_version:
        if not _MASTER_ADMIN_RELEASE_VERSION.fullmatch(development_version):
            raise ValueError(
                "The local deployment profile must pin a valid "
                "admin_tools_development_release_version."
            )
        version = development_version
        pin_scope = "development"
    else:
        if not _MASTER_ADMIN_RELEASE_VERSION.fullmatch(common_version):
            raise ValueError(
                "The local deployment profile must pin a valid admin_tools_release_version."
            )
        version = common_version
        # Production always uses this common scope.  Development reaches it
        # only as a clean/promotable fallback when no explicit dev pin exists.
        pin_scope = "common"
    root = PurePosixPath(str(profile.get("remote_root", "")).strip())
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("Deployment profile remote_root is invalid.")
    runtime_root = root / environment
    release_root = root / "admin_tools" / "releases" / version
    config_path = runtime_root / _MASTER_ADMIN_CONFIG_NAMES[environment]
    python_path = runtime_root / ".venv" / "bin" / "python"
    module_path = release_root / "admin_tools" / "db" / "master_data_backend.py"
    stage_root = root / ".deployment-console" / "master-csv" / environment
    return {
        "environment": environment,
        "dbname": "vrp_db_dev" if environment == "development" else "vrp_db",
        "target_id": f"{environment}:{'vrp_db_dev' if environment == 'development' else 'vrp_db'}",
        "remote_target_id": _target_id(profile, environment),
        "release_version": version,
        "pin_scope": pin_scope,
        "release_root": release_root.as_posix(),
        "config_path": config_path.as_posix(),
        "python_path": python_path.as_posix(),
        "module_path": module_path.as_posix(),
        "stage_root": stage_root.as_posix(),
    }


def _master_admin_confirmation(table_id: str, context: Mapping[str, str]) -> str:
    """Generate the data backend's compatibility confirmation internally.

    The console's user-facing confirmation is the preview/apply sequence.  A
    caller never types or controls this phrase, but the fixed backend still
    verifies it for compatibility with its command guard.
    """

    return f"IMPORT {table_id} TO {context['environment'].upper()} {context['dbname']}"


def _redact_master_payload(value: Any, *, field_name: str = "") -> Any:
    """Redact unexpected credential fields before a remote result reaches UI/history."""

    if re.search(r"(?i)(password|passwd|pwd|secret|token|api[_-]?key)", field_name):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {
            str(key): _redact_master_payload(item, field_name=str(key))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_master_payload(item, field_name=field_name) for item in value]
    if isinstance(value, tuple):
        return [_redact_master_payload(item, field_name=field_name) for item in value]
    if isinstance(value, str):
        return _redact(value)
    return value


def _parse_master_admin_json(
    stdout: str, *, context: Mapping[str, str], preserve_confirmation_token: bool = False
) -> dict[str, Any]:
    raw = str(stdout or "").strip()
    if not raw:
        raise RuntimeError("Remote DB admin command returned no JSON result.")
    if len(raw.encode("utf-8")) > _MASTER_ADMIN_MAX_JSON_BYTES:
        raise RuntimeError("Remote DB admin result exceeds the safe response limit.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Remote DB admin command returned invalid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("Remote DB admin command returned a non-object JSON result.")
    raw_payload = dict(payload)
    result = _redact_master_payload(raw_payload)
    if result.get("contract_version") != _MASTER_ADMIN_CONTRACT_VERSION:
        raise RuntimeError("Remote DB admin contract version mismatch.")
    for field in ("environment", "dbname", "target_id"):
        if field not in result:
            raise RuntimeError(f"Remote DB admin result is missing {field}.")
    if (
        result["environment"] != context["environment"]
        or result["dbname"] != context["dbname"]
        or result["target_id"] != context["target_id"]
    ):
        raise RuntimeError("Remote DB admin result target does not match the selected environment.")
    if preserve_confirmation_token and "confirmation_token" in raw_payload:
        token = raw_payload["confirmation_token"]
        if not isinstance(token, str):
            raise RuntimeError("Remote CSV preview confirmation token is invalid.")
        # This key is private to the preview bridge.  _remember_preview_confirmation
        # removes it before the result can reach UI/session/history output.
        result["_private_confirmation_token"] = token
    return result


def _master_admin_command(
    context: Mapping[str, str], command: str, arguments: Iterable[str] = ()
) -> str:
    """Construct one fixed remote module invocation with safely quoted values."""

    if command not in _MASTER_ADMIN_COMMANDS:
        raise ValueError("DB admin operation is not allowlisted.")
    argv = [
        context["python_path"],
        "-B",
        "-m",
        "admin_tools.db.master_data_backend",
        "--json",
        command,
        "--config",
        context["config_path"],
        *[str(item) for item in arguments],
    ]
    return f"cd -- {shlex.quote(context['release_root'])} && exec " + " ".join(
        shlex.quote(item) for item in argv
    )


def _verify_master_admin_release(remote: Any, context: Mapping[str, str]) -> None:
    """Bind a DB command to one fully uploaded, unchanged eligible release."""

    environment = _require_environment(context["environment"])
    version = str(context["release_version"])
    staging = _artifact_root(environment, "admin-tools") / version
    inspection = inspect_artifact(
        path=str(staging), kind="admin-tools", environment=environment
    )
    mode = _admin_tools_release_mode(inspection.manifest)
    if inspection.version != version or inspection.restricted_data or mode is None:
        raise PermissionError("DB admin requires a valid local Admin Tools release policy.")
    pin_scope = str(context.get("pin_scope", "common"))
    if environment == "production" or pin_scope == "common":
        if mode != "clean":
            raise PermissionError(
                "DB admin requires a clean promotable local Admin Tools release."
            )
    elif pin_scope == "development":
        # Both modes are executable in development, but only the explicit
        # development pin can authorize a dirty/non-promotable verification
        # build.  Production never reaches this branch.
        if mode not in {"development-verification", "clean"}:
            raise PermissionError("DB admin requires an eligible development Admin Tools release.")
    else:
        raise PermissionError("DB admin release pin scope is invalid.")
    expected_files = {
        relative: {
            "path": relative,
            "target": _remote_target(inspection, relative),
            "sha256": checksum,
        }
        for relative, (checksum, _) in _release_file_map(inspection).items()
    }
    candidates = [
        row
        for row in _load_history()
        if row.get("kind") == "admin-tools"
        and row.get("environment") == environment
        and row.get("version") == version
        and row.get("status") == "uploaded"
    ]
    if not candidates:
        raise PermissionError("Pinned Admin Tools release has no local upload receipt.")
    receipt = candidates[-1]
    if (
        receipt.get("target_id") != context["remote_target_id"]
        or receipt.get("complete_manifest") is not True
        or receipt.get("remote_manifest_verified") is not True
        or receipt.get("sha256") != inspection.archive_sha256
    ):
        raise PermissionError("Pinned Admin Tools upload receipt does not match this target/release.")
    raw_verified = receipt.get("verified_files")
    if not isinstance(raw_verified, list) or len(raw_verified) != len(expected_files):
        raise PermissionError("Pinned Admin Tools receipt has no exact verified-file set.")
    verified: dict[str, dict[str, str]] = {}
    for raw in raw_verified:
        if not isinstance(raw, Mapping):
            raise PermissionError("Pinned Admin Tools receipt is invalid.")
        relative = _safe_relative(str(raw.get("path", "")))
        if relative in verified:
            raise PermissionError("Pinned Admin Tools receipt contains duplicate files.")
        verified[relative] = {
            "path": relative,
            "target": str(raw.get("target", "")),
            "sha256": str(raw.get("sha256", "")).lower(),
        }
    if verified != expected_files:
        raise PermissionError("Pinned Admin Tools receipt differs from the inspected local artifact.")
    inventory = set(remote.inventory_files(context["release_root"]))
    if inventory != set(expected_files):
        raise RuntimeError("Pinned Admin Tools remote inventory differs from the exact release manifest.")
    for expected in expected_files.values():
        if remote.sha256(expected["target"]) != expected["sha256"]:
            raise RuntimeError("Pinned Admin Tools remote release hash verification failed.")


def _run_master_admin_command(
    remote: Any,
    context: Mapping[str, str],
    command: str,
    arguments: Iterable[str] = (),
) -> dict[str, Any]:
    _verify_master_admin_release(remote, context)
    code, stdout, stderr = remote.execute_master_json(
        _master_admin_command(context, command, arguments),
        timeout=_MASTER_ADMIN_TIMEOUT_SECONDS,
    )
    if code != 0:
        # The CLI may use a non-zero process status for a structured, expected
        # outcome such as PREVIEW_STALE.  Preserve that redacted contract
        # response when it is valid; only opaque failures become a generic
        # platform error.
        if str(stdout or "").strip():
            with contextlib.suppress(RuntimeError):
                return _parse_master_admin_json(
                    stdout,
                    context=context,
                    preserve_confirmation_token=command == "preview",
                )
        # The remote data boundary owns structured failure codes.  Do not relay
        # untrusted stderr (which can include connection detail) to the UI.
        detail = _redact(stderr).strip()
        suffix = f" ({detail[:240]})" if detail else ""
        raise RuntimeError(f"Remote DB admin {command} command failed{suffix}")
    return _parse_master_admin_json(
        stdout,
        context=context,
        preserve_confirmation_token=command == "preview",
    )


def _region_plan_cli_command(
    context: Mapping[str, str], command: str, arguments: Iterable[str] = ()
) -> str:
    """Construct only a fixed development region-plan Admin Tools invocation."""

    if context.get("environment") != _MANAGED_DATA_CANDIDATE_SCOPE:
        raise PermissionError("Region-plan operations are development-only.")
    if command not in _REGION_PLAN_ADMIN_COMMANDS:
        raise ValueError("Region-plan operation is not allowlisted.")
    argv = [
        context["python_path"],
        "-B",
        "-m",
        _REGION_PLAN_ADMIN_MODULE,
        "--json",
        command,
        "--config",
        context["config_path"],
        *[str(item) for item in arguments],
    ]
    return f"cd -- {shlex.quote(context['release_root'])} && exec " + " ".join(
        shlex.quote(item) for item in argv
    )


def _region_plan_schema_cli_command(
    context: Mapping[str, str], command: str, arguments: Iterable[str] = ()
) -> str:
    if context.get("environment") != _MANAGED_DATA_CANDIDATE_SCOPE:
        raise PermissionError("Region-plan schema operations are development-only.")
    if command not in {"preview", "reconcile"}:
        raise ValueError("Region-plan schema operation is not allowlisted.")
    argv = [
        context["python_path"], "-B", "-m", _REGION_PLAN_SCHEMA_ADMIN_MODULE,
        "--json", command, "--config", context["config_path"],
        *[str(item) for item in arguments],
    ]
    return f"cd -- {shlex.quote(context['release_root'])} && exec " + " ".join(
        shlex.quote(item) for item in argv
    )


def _region_plan_admin_command(
    context: Mapping[str, str], *, source_path: str, version: str
) -> str:
    """Construct the immutable workbook candidate-staging invocation."""

    normalized_version = str(version).strip().lower()
    source = PurePosixPath(str(source_path))
    if (
        not _MANAGED_DATA_VERSION.fullmatch(normalized_version)
        or not source.is_absolute()
        or ".." in source.parts
    ):
        raise ValueError("Region-plan candidate source binding is invalid.")
    return _region_plan_cli_command(
        context,
        _REGION_PLAN_ADMIN_COMMAND,
        (
        "--source",
        source.as_posix(),
        "--source-sha256",
        normalized_version,
        "--managed-version",
        normalized_version,
        ),
    )


def _safe_region_plan_candidate_result(
    result: Mapping[str, Any], *, version: str
) -> dict[str, Any]:
    """Return only bounded candidate lifecycle metadata to callers/history."""

    safe: dict[str, Any] = {
        "contract_version": _REGION_PLAN_CONTRACT_VERSION,
        "source_sha256": version,
        "managed_version": version,
    }
    for key in (
        "status",
        "error_code",
        "plan_id",
        "lifecycle_stage",
        "approval_status",
        "candidate_version",
        "artifact_sha256",
        "created_at",
    ):
        value = result.get(key)
        if isinstance(value, str) and value and len(value) <= 255:
            safe[key] = _redact(value)
    for key in (
        "promotable",
        "promotion_required",
        "direct_db_upsert",
    ):
        if isinstance(result.get(key), bool):
            safe[key] = result[key]
    for key in (
        "membership_input_rows",
        "membership_accepted_rows",
        "membership_rejected_rows",
        "unique_postal_count",
        "ambiguous_postal_count",
        "technician_input_rows",
        "technician_accepted_rows",
        "technician_rejected_rows",
        "artifact_count",
    ):
        value = result.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 10_000_000:
            safe[key] = value
    return safe


def _parse_region_plan_candidate_json(
    stdout: str, *, context: Mapping[str, str], version: str
) -> dict[str, Any]:
    raw = str(stdout or "").strip()
    if not raw:
        raise RuntimeError("Remote region-plan command returned no JSON result.")
    if len(raw.encode("utf-8")) > _MASTER_ADMIN_MAX_JSON_BYTES:
        raise RuntimeError("Remote region-plan result exceeds the safe response limit.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Remote region-plan command returned invalid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("Remote region-plan command returned a non-object JSON result.")
    if payload.get("contract_version") != _REGION_PLAN_CONTRACT_VERSION:
        raise RuntimeError("Remote region-plan contract version mismatch.")
    if payload.get("environment") != _MANAGED_DATA_CANDIDATE_SCOPE:
        raise RuntimeError("Remote region-plan result environment does not match development.")
    normalized_version = str(version).strip().lower()
    if (
        payload.get("source_sha256") != normalized_version
        or payload.get("managed_version") != normalized_version
    ):
        raise RuntimeError("Remote region-plan result source does not match the managed version.")
    status = payload.get("status")
    if status not in {"candidate_staged", "already_staged"}:
        code = payload.get("error_code")
        if isinstance(code, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", code):
            raise RuntimeError(f"Remote region-plan candidate staging was rejected ({code}).")
        raise RuntimeError("Remote region-plan candidate staging was rejected.")
    plan_id = payload.get("plan_id")
    if plan_id is not None:
        try:
            _require_region_plan_final_id(plan_id)
        except ValueError as exc:
            raise RuntimeError("Remote region-plan result plan id is invalid.") from exc
    if payload.get("lifecycle_stage") != "candidate_plan":
        raise RuntimeError("Remote region-plan result is not a candidate workflow artifact.")
    if payload.get("promotion_required") is not True or payload.get("direct_db_upsert") is not False:
        raise RuntimeError("Remote region-plan result permits an unsafe direct activation.")
    if not isinstance(payload.get("promotable"), bool):
        raise RuntimeError("Remote region-plan result promotable flag is invalid.")
    if not isinstance(payload.get("approval_status"), str):
        raise RuntimeError("Remote region-plan result approval status is invalid.")
    return _safe_region_plan_candidate_result(payload, version=normalized_version)


def _run_region_plan_candidate_command(
    remote: Any, *, profile: Mapping[str, Any], source_path: str, version: str
) -> dict[str, Any]:
    """Stage exactly one development territory-plan candidate through Admin Tools."""

    context = _master_admin_context(profile, _MANAGED_DATA_CANDIDATE_SCOPE)
    _verify_master_admin_release(remote, context)
    inspection = inspect_artifact(
        path=str(_artifact_root(_MANAGED_DATA_CANDIDATE_SCOPE, "admin-tools") / context["release_version"]),
        kind="admin-tools",
        environment=_MANAGED_DATA_CANDIDATE_SCOPE,
    )
    if "admin_tools/db/region_plan_backend.py" not in _release_file_map(inspection):
        raise PermissionError("Pinned Admin Tools release does not provide candidate-plan staging.")
    code, stdout, stderr = remote.execute_region_plan_json(
        _region_plan_admin_command(context, source_path=source_path, version=version),
        timeout=_MASTER_ADMIN_TIMEOUT_SECONDS,
    )
    if code != 0:
        if str(stdout or "").strip():
            with contextlib.suppress(RuntimeError):
                return _parse_region_plan_candidate_json(
                    stdout, context=context, version=version
                )
        detail = _redact(stderr).strip()
        suffix = f" ({detail[:240]})" if detail else ""
        raise RuntimeError(f"Remote region-plan candidate command failed{suffix}")
    return _parse_region_plan_candidate_json(stdout, context=context, version=version)


def _verify_region_plan_admin_release(remote: Any, context: Mapping[str, str]) -> None:
    """Require the fixed workflow CLI to be in the verified Admin Tools release."""

    _verify_master_admin_release(remote, context)
    inspection = inspect_artifact(
        path=str(
            _artifact_root(_MANAGED_DATA_CANDIDATE_SCOPE, "admin-tools")
            / context["release_version"]
        ),
        kind="admin-tools",
        environment=_MANAGED_DATA_CANDIDATE_SCOPE,
    )
    if "admin_tools/db/region_plan_backend.py" not in _release_file_map(inspection):
        raise PermissionError("Pinned Admin Tools release does not provide region-plan workflow commands.")


def _verify_fixed_region_plan_bundle_admin_release(
    remote: Any, context: Mapping[str, str]
) -> None:
    """Preflight the pinned capability before a bundle or request is written.

    The exact manifest/inventory/hash verification is inherited from the
    generic Admin Tools release verifier.  The additional module presence is
    deliberately the public capability boundary: callers never supply a
    Python module, working directory, command, or remote bundle path.
    """

    if context.get("environment") != _FIXED_REGION_PLAN_BUNDLE_SCOPE:
        raise PermissionError("Fixed Region Data imports are development-only.")
    _verify_master_admin_release(remote, context)
    inspection = inspect_artifact(
        path=str(
            _artifact_root(_FIXED_REGION_PLAN_BUNDLE_SCOPE, "admin-tools")
            / context["release_version"]
        ),
        kind="admin-tools",
        environment=_FIXED_REGION_PLAN_BUNDLE_SCOPE,
    )
    capability_relative = "admin_tools/db/region_plan_backend.py"
    if capability_relative not in _release_file_map(inspection):
        # Do not expose an artifact path, manifest selection, or server
        # inventory detail to the browser/history boundary.
        raise PermissionError(
            "Pinned Admin Tools release does not support fixed Region Data imports."
        )
    try:
        capability_source = (
            _within(Path(inspection.path) / capability_relative, Path(inspection.path))
            .read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError) as exc:
        raise PermissionError(
            "Pinned Admin Tools release does not support fixed Region Data imports."
        ) from exc
    if not all(
        marker in capability_source
        for marker in (
            "stage-bundle", "status-bundle", "import-bundle",
            "region-plan-bundle-import/v1",
        )
    ):
        raise PermissionError(
            "Pinned Admin Tools release does not support fixed Region Data imports."
        )


def _fixed_region_plan_bundle_cli_command(
    context: Mapping[str, str], command: str, arguments: Iterable[str] = ()
) -> str:
    """Construct one fixed bundle-import command from trusted bindings only."""

    if context.get("environment") != _FIXED_REGION_PLAN_BUNDLE_SCOPE:
        raise PermissionError("Fixed Region Data imports are development-only.")
    if command not in _FIXED_REGION_PLAN_BUNDLE_COMMANDS:
        raise ValueError("Fixed Region Data operation is not allowlisted.")
    argv = [
        context["python_path"],
        "-B",
        "-m",
        _REGION_PLAN_ADMIN_MODULE,
        "--json",
        command,
        "--config",
        context["config_path"],
        *[str(item) for item in arguments],
    ]
    return f"cd -- {shlex.quote(context['release_root'])} && exec " + " ".join(
        shlex.quote(item) for item in argv
    )


def _fixed_region_plan_bundle_remote_path(
    profile: Mapping[str, Any], version: str
) -> str:
    """Return the sole managed ZIP path accepted by the remote transport."""

    version = _require_region_plan_digest(version, field="version")
    return (
        _managed_data_remote_root(
            profile,
            _FIXED_REGION_PLAN_BUNDLE_SCOPE,
            _FIXED_REGION_PLAN_BUNDLE_DATASET,
            version,
        )
        / "payload.zip"
    ).as_posix()


def _fixed_region_plan_bundle_request_path(
    profile: Mapping[str, Any], request_sha256: str
) -> str:
    request_sha256 = _require_region_plan_digest(
        request_sha256, field="request_sha256"
    )
    root = PurePosixPath(str(profile.get("remote_root", "")).strip())
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("Deployment profile remote_root is invalid.")
    return (
        root
        / "state"
        / _FIXED_REGION_PLAN_BUNDLE_SCOPE
        / _FIXED_REGION_PLAN_BUNDLE_REQUEST_ROOT
        / f"{request_sha256}.json"
    ).as_posix()


def _fixed_region_plan_bundle_import_request(
    *, version: str, imported_by: object, idempotency_key: object
) -> dict[str, str]:
    """Create the exact, path-free request accepted by ``import-bundle``."""

    version = _require_region_plan_digest(version, field="version")
    return {
        "schema": "region-plan-bundle-import-request/v1",
        "managed_version": version,
        "bundle_sha256": version,
        "imported_by": _require_region_plan_token(imported_by, field="imported_by"),
        "idempotency_key": _require_region_plan_idempotency_key(idempotency_key),
    }


def _stage_fixed_region_plan_bundle_request(
    remote: Any,
    *,
    profile: Mapping[str, Any],
    request_bytes: bytes,
    request_sha256: str,
) -> str:
    """Stage one checksum-bound import request below its dedicated fixed root."""

    target = _fixed_region_plan_bundle_request_path(profile, request_sha256)
    if remote.exists(target):
        if remote.sha256(target) != request_sha256:
            raise RuntimeError("Fixed Region Data import request version collision.")
        if remote.mode(target) != _SECURE_CONFIG_MODE:
            raise RuntimeError("Fixed Region Data import request has an unsafe remote mode.")
        return target
    remote.upload_bytes_atomic(request_bytes, target, backup=None)
    if remote.sha256(target) != request_sha256 or remote.mode(target) != _SECURE_CONFIG_MODE:
        raise RuntimeError("Fixed Region Data import request checksum verification failed.")
    return target


def _safe_fixed_region_plan_bundle_result(
    payload: Mapping[str, Any], *, command: str, version: str
) -> dict[str, Any]:
    """Validate and redact the narrow public Admin Tools result contract."""

    if payload.get("contract_version") != _FIXED_REGION_PLAN_BUNDLE_CONTRACT_VERSION:
        raise RuntimeError("Fixed Region Data import contract version mismatch.")
    if payload.get("environment") != _FIXED_REGION_PLAN_BUNDLE_SCOPE:
        raise RuntimeError("Fixed Region Data import result environment is invalid.")
    if payload.get("managed_version") != version:
        raise RuntimeError("Fixed Region Data import result version is invalid.")
    if command == "stage-bundle":
        if (
            payload.get("status") != "ready"
            or payload.get("write_allowed") is not False
            or payload.get("target_environment") != _FIXED_REGION_PLAN_BUNDLE_SCOPE
            or payload.get("lifecycle_stage") != "resolved_candidate"
            or payload.get("verification_only") is not True
            or payload.get("promotable") is not False
            or payload.get("bundle_sha256") != version
        ):
            raise RuntimeError("Fixed Region Data import preview result is invalid.")
        result: dict[str, Any] = {
            "status": "ready",
            "environment": _FIXED_REGION_PLAN_BUNDLE_SCOPE,
            "managed_version": version,
            "bundle_sha256": version,
            "write_allowed": False,
            "lifecycle_stage": "resolved_candidate",
            "verification_only": True,
            "promotable": False,
        }
        for key in (
            "plan_id",
            "source_sha256",
            "manifest_sha256",
            "fixed_region_sha256",
            "boundary_policy_sha256",
            "technician_policy_sha256",
        ):
            value = payload.get(key)
            if isinstance(value, str) and _MANAGED_DATA_VERSION.fullmatch(value.lower()):
                result[key] = value.lower()
        for key in (
            "region_count",
            "postal_count",
            "technician_count",
            "ambiguous_postal_count",
        ):
            value = payload.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 10_000_000:
                result[key] = value
        plan_id = payload.get("plan_id")
        if plan_id is not None:
            try:
                result["plan_id"] = _require_region_plan_final_id(plan_id)
            except ValueError as exc:
                raise RuntimeError("Fixed Region Data import preview plan id is invalid.") from exc
            result["resolution_digest"] = _region_plan_versioned_digest(
                result["plan_id"]
            )
        return result
    if command == "status-bundle":
        status = str(payload.get("status", "")).strip().lower()
        lifecycle_stage = str(payload.get("lifecycle_stage", "")).strip().lower()
        expected_lifecycle = "resolved_candidate" if status == "not_imported" else status
        if (
            status not in {"not_imported", "candidate", "reviewed", "active", "superseded"}
            or lifecycle_stage != expected_lifecycle
            or payload.get("verification_only") is not True
            or payload.get("promotable") is not False
            or payload.get("bundle_sha256") != version
        ):
            raise RuntimeError("Fixed Region Data status result is invalid.")
        try:
            plan_id = _require_region_plan_final_id(payload.get("plan_id"))
        except ValueError as exc:
            raise RuntimeError("Fixed Region Data status plan id is invalid.") from exc
        result = {
            "status": status,
            "environment": _FIXED_REGION_PLAN_BUNDLE_SCOPE,
            "managed_version": version,
            "bundle_sha256": version,
            "plan_id": plan_id,
            "resolution_digest": _region_plan_versioned_digest(plan_id),
            "lifecycle_stage": lifecycle_stage,
            "verification_only": True,
            "promotable": False,
        }
        if status != "not_imported":
            try:
                result["revision"] = _require_region_plan_revision(
                    payload.get("revision"), field="revision"
                )
                result["checksum"] = _require_region_plan_digest(
                    payload.get("checksum"), field="checksum"
                )
            except ValueError as exc:
                raise RuntimeError("Fixed Region Data status binding is invalid.") from exc
            if result["checksum"] != version:
                raise RuntimeError("Fixed Region Data status checksum is invalid.")
        return result
    if command != "import-bundle":
        raise ValueError("Fixed Region Data operation is not allowlisted.")
    if (
        payload.get("status") not in {"candidate_imported", "already_imported"}
        or payload.get("lifecycle_stage") != "candidate"
        or payload.get("verification_only") is not True
        or payload.get("promotable") is not False
    ):
        raise RuntimeError("Fixed Region Data import result is invalid.")
    try:
        plan_id = _require_region_plan_final_id(payload.get("plan_id"))
        checksum = _require_region_plan_digest(payload.get("checksum"), field="checksum")
        revision = _require_region_plan_revision(payload.get("revision"), field="revision")
    except ValueError as exc:
        raise RuntimeError("Fixed Region Data import result binding is invalid.") from exc
    if checksum != version:
        raise RuntimeError("Fixed Region Data import checksum is invalid.")
    return {
        "status": str(payload["status"]),
        "environment": _FIXED_REGION_PLAN_BUNDLE_SCOPE,
        "managed_version": version,
        "bundle_sha256": version,
        "plan_id": plan_id,
        "resolution_digest": _region_plan_versioned_digest(plan_id),
        "revision": revision,
        "checksum": checksum,
        "lifecycle_stage": "candidate",
        "verification_only": True,
        "promotable": False,
    }


def _run_fixed_region_plan_bundle_command(
    remote: Any,
    *,
    context: Mapping[str, str],
    command: str,
    arguments: Iterable[str],
    version: str,
) -> dict[str, Any]:
    """Execute only a preflighted fixed bundle-import operation."""

    _verify_fixed_region_plan_bundle_admin_release(remote, context)
    code, stdout, _stderr = remote.execute_region_plan_json(
        _fixed_region_plan_bundle_cli_command(context, command, arguments),
        timeout=_MASTER_ADMIN_TIMEOUT_SECONDS,
    )
    if code != 0:
        # Structured Admin Tools failure codes are intentionally not relayed:
        # they can disclose server state while offering no UI recovery path.
        raise RuntimeError("Fixed Region Data import operation was rejected.")
    raw = str(stdout or "").strip()
    if not raw or len(raw.encode("utf-8")) > _MASTER_ADMIN_MAX_JSON_BYTES:
        raise RuntimeError("Fixed Region Data import returned an invalid result.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Fixed Region Data import returned an invalid result.") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("Fixed Region Data import returned an invalid result.")
    return _safe_fixed_region_plan_bundle_result(payload, command=command, version=version)


def _parse_region_plan_json(
    stdout: str, *, contract_version: str
) -> dict[str, Any]:
    raw = str(stdout or "").strip()
    if not raw:
        raise RuntimeError("Remote region-plan command returned no JSON result.")
    if len(raw.encode("utf-8")) > _MASTER_ADMIN_MAX_JSON_BYTES:
        raise RuntimeError("Remote region-plan result exceeds the safe response limit.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Remote region-plan command returned invalid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("Remote region-plan command returned a non-object JSON result.")
    if payload.get("contract_version") != contract_version:
        raise RuntimeError("Remote region-plan contract version mismatch.")
    if payload.get("environment") != _MANAGED_DATA_CANDIDATE_SCOPE:
        raise RuntimeError("Remote region-plan result environment does not match development.")
    if payload.get("status") == "rejected":
        code = payload.get("error_code")
        if isinstance(code, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", code):
            raise RuntimeError(f"Remote region-plan command was rejected ({code}).")
        raise RuntimeError("Remote region-plan command was rejected.")
    return dict(payload)


def _run_region_plan_admin_command(
    remote: Any,
    *,
    context: Mapping[str, str],
    command: str,
    arguments: Iterable[str] = (),
    contract_version: str,
) -> dict[str, Any]:
    """Run one already allowlisted region-plan command through the trusted release."""

    if command not in _REGION_PLAN_ADMIN_COMMANDS:
        raise ValueError("Region-plan operation is not allowlisted.")
    _verify_region_plan_admin_release(remote, context)
    code, stdout, stderr = remote.execute_region_plan_json(
        _region_plan_cli_command(context, command, arguments),
        timeout=_MASTER_ADMIN_TIMEOUT_SECONDS,
    )
    if code != 0:
        if str(stdout or "").strip():
            with contextlib.suppress(RuntimeError):
                return _parse_region_plan_json(stdout, contract_version=contract_version)
        detail = _redact(stderr).strip()
        suffix = f" ({detail[:240]})" if detail else ""
        raise RuntimeError(f"Remote region-plan {command} command failed{suffix}")
    return _parse_region_plan_json(stdout, contract_version=contract_version)


def _master_admin_profile() -> dict[str, Any]:
    """Load the one local ignored profile used by all master-data operations."""

    return _load_remote_profile(_MASTER_ADMIN_PROFILE_PATH)


def _region_plan_development_context(
    environment: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Fail closed before any profile/remote access for production requests."""

    if _require_environment(environment) != _MANAGED_DATA_CANDIDATE_SCOPE:
        raise PermissionError("Region-plan workflow is development-only.")
    profile = _master_admin_profile()
    return profile, _master_admin_context(profile, _MANAGED_DATA_CANDIDATE_SCOPE)


def _fixed_region_plan_bundle_development_context(
    environment: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Resolve the fixed bundle-import target without caller-selected paths."""

    if _require_environment(environment) != _FIXED_REGION_PLAN_BUNDLE_SCOPE:
        raise PermissionError("Fixed Region Data imports are development-only.")
    profile = _master_admin_profile()
    return profile, _master_admin_context(profile, _FIXED_REGION_PLAN_BUNDLE_SCOPE)


def _load_fixed_region_plan_bundle(version: object) -> str:
    """Prove the selected local version is the immutable managed ZIP.

    This only verifies the stored ZIP hash and metadata; it deliberately does
    not open a workbook, parse raw territory data, or rebuild a bundle.
    """

    normalized = _require_region_plan_digest(version, field="version")
    metadata, _payload = _load_managed_data_version(
        scope=_FIXED_REGION_PLAN_BUNDLE_SCOPE,
        dataset_id=_FIXED_REGION_PLAN_BUNDLE_DATASET,
        version=normalized,
    )
    if metadata.get("payload_name") != "payload.zip":
        raise RuntimeError("Fixed Region Data bundle version is invalid.")
    return normalized


def preview_fixed_region_plan_bundle_import(
    *, environment: str, version: str
) -> dict[str, Any]:
    """Ask pinned Admin Tools to inspect one immutable managed Region Data ZIP."""

    profile, context = _fixed_region_plan_bundle_development_context(environment)
    version = _load_fixed_region_plan_bundle(version)
    source = _fixed_region_plan_bundle_remote_path(profile, version)
    with _remote_session_factory(profile) as remote:
        return _run_fixed_region_plan_bundle_command(
            remote,
            context=context,
            command="stage-bundle",
            arguments=(
                "--source",
                source,
                "--bundle-sha256",
                version,
                "--managed-version",
                version,
            ),
            version=version,
        )


def get_fixed_region_plan_bundle_status(
    *, environment: str, version: str
) -> dict[str, Any]:
    """Rehydrate the DB lifecycle state for one immutable managed bundle."""

    profile, context = _fixed_region_plan_bundle_development_context(environment)
    version = _load_fixed_region_plan_bundle(version)
    with _remote_session_factory(profile) as remote:
        result = _run_fixed_region_plan_bundle_command(
            remote,
            context=context,
            command="status-bundle",
            arguments=(
                "--bundle-sha256",
                version,
                "--managed-version",
                version,
            ),
            version=version,
        )
    if result.get("status") in {"candidate", "reviewed", "active"}:
        _remember_region_plan_final_binding(
            result=result, source_version=version, context=context
        )
    return result


def apply_fixed_region_plan_bundle_import(
    *,
    environment: str,
    version: str,
    imported_by: str,
    idempotency_key: str,
    confirm: bool = False,
) -> dict[str, Any]:
    """Import a previously uploaded immutable Region Data ZIP in development.

    The server receives a canonical request containing only version/hash and
    operator/idempotency metadata.  It derives the managed bundle path itself;
    the browser never controls a source/destination/database argument.
    """

    profile, context = _fixed_region_plan_bundle_development_context(environment)
    if confirm is not True:
        raise PermissionError("Fixed Region Data import requires explicit confirmation.")
    version = _load_fixed_region_plan_bundle(version)
    request = _fixed_region_plan_bundle_import_request(
        version=version,
        imported_by=imported_by,
        idempotency_key=idempotency_key,
    )
    request_sha256, request_bytes = _region_plan_request_bytes(request)
    release_id = f"fixed-region-plan-import-{request_sha256[:12]}"
    with _remote_session_factory(profile) as remote:
        with remote.deployment_lock(str(profile["remote_root"]), release_id):
            # This verification precedes the request upload, so an unsupported
            # pin cannot produce remote state.
            _verify_fixed_region_plan_bundle_admin_release(remote, context)
            request_path = _stage_fixed_region_plan_bundle_request(
                remote,
                profile=profile,
                request_bytes=request_bytes,
                request_sha256=request_sha256,
            )
            result = _run_fixed_region_plan_bundle_command(
                remote,
                context=context,
                command="import-bundle",
                arguments=(
                    "--request",
                    request_path,
                    "--request-sha256",
                    request_sha256,
                ),
                version=version,
            )
    _remember_region_plan_final_binding(
        result=result, source_version=version, context=context
    )
    return {**result, "request_sha256": request_sha256}


def _require_region_plan_token(value: object, *, field: str) -> str:
    token = str(value).strip()
    if not _REGION_PLAN_TOKEN.fullmatch(token):
        raise ValueError(f"{field} is invalid.")
    return token


def _require_region_plan_idempotency_key(value: object) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("idempotency_key must be a UUID.") from exc


def _require_region_plan_revision(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} is invalid.")
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is invalid.") from exc
    if not 0 <= revision <= 2_147_483_647:
        raise ValueError(f"{field} is invalid.")
    return revision


def _require_region_plan_digest(value: object, *, field: str) -> str:
    digest = str(value).strip().lower()
    if not _MANAGED_DATA_VERSION.fullmatch(digest):
        raise ValueError(f"{field} must be a SHA-256 digest.")
    return digest


def _require_region_plan_final_id(value: object) -> str:
    """Accept only the legacy plan or a backend-derived decision digest ID."""

    plan_id = str(value).strip()
    if plan_id == _REGION_PLAN_FIXED_ID or _REGION_PLAN_VERSIONED_ID.fullmatch(plan_id):
        return plan_id
    raise ValueError("plan_id is not an allowlisted immutable region-plan id.")


def _region_plan_versioned_digest(value: object) -> str:
    plan_id = _require_region_plan_final_id(value)
    if plan_id == _REGION_PLAN_FIXED_ID:
        raise ValueError("Legacy fixed region-plan ids do not contain a decision digest.")
    return _require_region_plan_digest(plan_id.rsplit("_", 1)[-1], field="resolution_digest")


def _region_plan_remote_request_path(profile: Mapping[str, Any], digest: str) -> str:
    root = PurePosixPath(str(profile.get("remote_root", "")).strip())
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("Deployment profile remote_root is invalid.")
    return (
        root / "state" / "development" / _REGION_PLAN_REQUEST_ROOT / f"{digest}.json"
    ).as_posix()


def _region_plan_managed_source_path(profile: Mapping[str, Any], version: str) -> str:
    root = PurePosixPath(str(profile.get("remote_root", "")).strip())
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("Deployment profile remote_root is invalid.")
    return (
        root
        / "state"
        / "development"
        / "managed_data"
        / _MANAGED_DATA_CANDIDATE_DATASET
        / version
        / "payload.xlsx"
    ).as_posix()


def _region_plan_request_bytes(payload: Mapping[str, Any]) -> tuple[str, bytes]:
    """Canonicalize one fixed workflow request without retaining user text locally."""

    try:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("Region-plan workflow request is not JSON-serializable.") from exc
    if not raw or len(raw) > _REGION_PLAN_REQUEST_MAX_BYTES:
        raise ValueError("Region-plan workflow request exceeds the safety limit.")
    return hashlib.sha256(raw).hexdigest(), raw


def _stage_region_plan_request(
    remote: Any, *, profile: Mapping[str, Any], request_bytes: bytes, request_sha256: str
) -> str:
    """Write exactly one immutable 0600 workflow request under the fixed root."""

    target = _region_plan_remote_request_path(profile, request_sha256)
    if remote.exists(target):
        if remote.sha256(target) != request_sha256:
            raise RuntimeError("Region-plan immutable request version collision.")
        if remote.mode(target) != _SECURE_CONFIG_MODE:
            raise RuntimeError("Region-plan request has an unsafe remote mode.")
        return target
    remote.upload_bytes_atomic(request_bytes, target, backup=None)
    if remote.sha256(target) != request_sha256 or remote.mode(target) != _SECURE_CONFIG_MODE:
        raise RuntimeError("Region-plan request checksum or mode verification failed.")
    return target


def _region_plan_migration_spec(migration_id: str) -> MigrationSpec:
    """Return one manifest-registered migration, preserving manifest order as policy."""

    if not isinstance(migration_id, str):
        raise ValueError("Region-plan migration id is invalid.")
    specs = _load_migration_specs()
    matches = [spec for spec in specs if spec.migration_id == migration_id]
    if len(matches) != 1:
        raise ValueError("Region-plan migration is not registered.")
    return matches[0]


def list_region_plan_schema_migrations(*, environment: str) -> list[dict[str, Any]]:
    """Expose the ordered, local registry; no remote or database access occurs."""

    _region_plan_development_context(environment)
    return [
        {
            "migration_id": spec.migration_id,
            "description": spec.description,
            "checksum_sha256": spec.checksum_sha256,
            "rollback_instructions": spec.rollback_instructions,
            "reversible": spec.reversible,
        }
        for spec in _load_migration_specs()
    ]


def _safe_region_plan_migration_result(
    payload: Mapping[str, Any], *, preview: bool, migration: MigrationSpec
) -> tuple[dict[str, Any], str | None]:
    if payload.get("migration_id") != migration.migration_id:
        raise RuntimeError("Remote region-plan migration id is invalid.")
    checksum = _require_region_plan_digest(payload.get("checksum_sha256"), field="checksum")
    if checksum != migration.checksum_sha256:
        raise RuntimeError("Remote region-plan migration checksum differs from the registry.")
    status = payload.get("status")
    expected = {"ready"} if preview else {"applied", "already_applied"}
    if status not in expected:
        raise RuntimeError("Remote region-plan migration status is invalid.")
    count = _require_region_plan_revision(payload.get("statement_count"), field="statement_count")
    result = {
        "status": status,
        "migration_id": migration.migration_id,
        "checksum_sha256": checksum,
        "statement_count": count,
        "environment": _MANAGED_DATA_CANDIDATE_SCOPE,
    }
    confirmation: str | None = None
    if preview:
        raw_confirmation = payload.get("required_confirmation")
        if (
            not isinstance(raw_confirmation, str)
            or not raw_confirmation.strip()
            or len(raw_confirmation) > 512
            or any(ord(char) < 32 for char in raw_confirmation)
        ):
            raise RuntimeError("Remote region-plan migration preview confirmation is invalid.")
        confirmation = raw_confirmation.strip()
        types = payload.get("statement_types")
        if not isinstance(types, list) or not all(
            isinstance(item, str) and re.fullmatch(r"[A-Z ]{1,80}", item)
            for item in types
        ):
            raise RuntimeError("Remote region-plan migration statement types are invalid.")
        result["statement_types"] = list(types)
        rollback = payload.get("rollback_instructions")
        if isinstance(rollback, str) and len(rollback) <= 1000:
            result["rollback_instructions"] = _redact(rollback)
    return result, confirmation


def _safe_region_plan_workflow_result(
    payload: Mapping[str, Any], *, command: str, expected_plan_id: str | None
) -> dict[str, Any]:
    try:
        plan_id = _require_region_plan_final_id(payload.get("plan_id"))
    except ValueError as exc:
        raise RuntimeError("Remote region-plan workflow plan id is invalid.") from exc
    if expected_plan_id is not None and plan_id != expected_plan_id:
        raise RuntimeError("Remote region-plan workflow plan id is invalid.")
    status = payload.get("status")
    result: dict[str, Any] = {
        "status": status,
        "plan_id": plan_id,
        "environment": _MANAGED_DATA_CANDIDATE_SCOPE,
    }
    if command == "resolve":
        if status != "candidate_imported" or payload.get("lifecycle_stage") != "candidate_resolved":
            raise RuntimeError("Remote region-plan resolution status is invalid.")
        raw_resolution_digest = payload.get("resolution_digest")
        if raw_resolution_digest is None:
            raise RuntimeError("Remote region-plan resolution digest is missing.")
        resolution_digest = _require_region_plan_digest(
            raw_resolution_digest, field="resolution_digest"
        )
        if (
            plan_id == _REGION_PLAN_FIXED_ID
            or _region_plan_versioned_digest(plan_id) != resolution_digest
        ):
            raise RuntimeError("Remote region-plan resolution plan binding is invalid.")
        result["revision"] = _require_region_plan_revision(
            payload.get("revision"), field="revision"
        )
        result["checksum"] = _require_region_plan_digest(payload.get("checksum"), field="checksum")
        result["resolution_digest"] = resolution_digest
        result["lifecycle_stage"] = "candidate_resolved"
    elif command == "review":
        if status != "reviewed":
            raise RuntimeError("Remote region-plan review status is invalid.")
        result["revision"] = _require_region_plan_revision(
            payload.get("revision"), field="revision"
        )
        result["lifecycle_stage"] = "reviewed"
    elif command == "activation-preview":
        if status != "ready":
            raise RuntimeError("Remote region-plan activation preview status is invalid.")
        preview_id = _require_region_plan_digest(payload.get("preview_id"), field="preview_id")
        preview_digest = _require_region_plan_digest(
            payload.get("preview_digest"), field="preview_digest"
        )
        if preview_id != preview_digest:
            raise RuntimeError("Remote region-plan activation preview binding is invalid.")
        result.update(
            {
                "preview_id": preview_id,
                "preview_digest": preview_digest,
                "checksum": _require_region_plan_digest(payload.get("checksum"), field="checksum"),
                "plan_revision": _require_region_plan_revision(
                    payload.get("plan_revision"), field="plan_revision"
                ),
                "expected_activation_revision": _require_region_plan_revision(
                    payload.get("expected_activation_revision"),
                    field="expected_activation_revision",
                ),
                "region_count": _require_region_plan_revision(payload.get("region_count"), field="region_count"),
                "postal_count": _require_region_plan_revision(payload.get("postal_count"), field="postal_count"),
                "technician_count": _require_region_plan_revision(payload.get("technician_count"), field="technician_count"),
                "boundary_resolution_count": _require_region_plan_revision(
                    payload.get("boundary_resolution_count"), field="boundary_resolution_count"
                ),
            }
        )
    elif command == "activate":
        if status not in {"activated", "already_active"}:
            raise RuntimeError("Remote region-plan activation status is invalid.")
        result["activation_revision"] = _require_region_plan_revision(
            payload.get("activation_revision"), field="activation_revision"
        )
        result["preview_digest"] = _require_region_plan_digest(
            payload.get("preview_digest"), field="preview_digest"
        )
        result["lifecycle_stage"] = "active"
    else:
        raise ValueError("Region-plan workflow command is not allowlisted.")
    return result


def _region_plan_boundary_resolutions(value: object) -> dict[str, dict[str, Any]]:
    """Validate the four fixed Atlanta boundary decisions before request staging."""

    if not isinstance(value, Mapping):
        raise ValueError("boundary_resolutions must be an object.")
    expected = ("30028", "30040", "30041", "30107")
    if set(str(key) for key in value) != set(expected):
        raise ValueError("boundary_resolutions must contain the four approved boundary ZIPs.")
    result: dict[str, dict[str, Any]] = {}
    for postal in expected:
        raw = value.get(postal)
        if not isinstance(raw, Mapping):
            raise ValueError("boundary resolution is invalid.")
        primary = str(raw.get("primary_region", "")).strip()
        allow_overflow = raw.get("allow_overflow")
        rationale = raw.get("rationale", "")
        if (
            primary not in {"Zone 2", "Zone 3"}
            or not isinstance(allow_overflow, bool)
            or not isinstance(rationale, str)
            or len(rationale) > 500
            or any(ord(char) < 32 for char in rationale)
        ):
            raise ValueError("boundary resolution is invalid.")
        result[postal] = {
            "primary_region": primary,
            "allow_overflow": allow_overflow,
            "rationale": " ".join(rationale.split()),
        }
    return result


def _run_region_plan_workflow_request(
    *,
    profile: Mapping[str, Any],
    context: Mapping[str, str],
    command: str,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    plan_id = request.get("plan_id")
    if command == "resolve":
        if plan_id is not None:
            raise ValueError("Region-plan resolution request must not select a plan id.")
        expected_plan_id: str | None = None
    else:
        expected_plan_id = _require_region_plan_final_id(plan_id)
    request_sha256, request_bytes = _region_plan_request_bytes(request)
    release_id = f"region-plan-{command}-{request_sha256[:12]}"
    with _remote_session_factory(profile) as remote:
        with remote.deployment_lock(str(profile["remote_root"]), release_id):
            request_path = _stage_region_plan_request(
                remote,
                profile=profile,
                request_bytes=request_bytes,
                request_sha256=request_sha256,
            )
            raw = _run_region_plan_admin_command(
                remote,
                context=context,
                command=command,
                arguments=("--request", request_path, "--request-sha256", request_sha256),
                contract_version=_REGION_PLAN_WORKFLOW_CONTRACT_VERSION,
            )
    return _safe_region_plan_workflow_result(
        raw, command=command, expected_plan_id=expected_plan_id
    )


def _safe_region_plan_schema_v2_result(payload: Mapping[str, Any]) -> dict[str, Any]:
    status = str(payload.get("status", ""))
    if (
        payload.get("contract_version") != _REGION_PLAN_SCHEMA_CONTRACT_VERSION
        or status not in {"ready", "reconciled"}
        or payload.get("environment") != "development"
        or payload.get("dbname") != "vrp_db_dev"
        or payload.get("target_id") != "development:vrp_db_dev"
        or payload.get("schema_id") != _REGION_PLAN_SCHEMA_ID
    ):
        raise RuntimeError("Remote Region Plan schema v2 result is invalid.")
    checksum = _require_region_plan_digest(payload.get("checksum_sha256"), field="checksum")
    confirmation = str(payload.get("requires_confirmation", ""))
    if confirmation != _REGION_PLAN_SCHEMA_CONFIRMATION:
        raise RuntimeError("Remote Region Plan schema v2 confirmation is invalid.")
    return {
        "contract_version": _REGION_PLAN_SCHEMA_CONTRACT_VERSION,
        "status": status,
        "environment": "development",
        "dbname": "vrp_db_dev",
        "target_id": "development:vrp_db_dev",
        "schema_id": _REGION_PLAN_SCHEMA_ID,
        "checksum_sha256": checksum,
    }


def _run_region_plan_schema_v2(
    remote: Any, *, context: Mapping[str, str], command: str,
    arguments: Iterable[str] = (),
) -> dict[str, Any]:
    _verify_region_plan_admin_release(remote, context)
    code, stdout, stderr = remote.execute_region_plan_json(
        _region_plan_schema_cli_command(context, command, arguments),
        timeout=_MASTER_ADMIN_TIMEOUT_SECONDS,
    )
    if code != 0:
        detail = _redact(stderr).strip()
        suffix = f" ({detail[:240]})" if detail else ""
        raise RuntimeError(f"Remote Region Plan schema v2 {command} failed{suffix}")
    try:
        payload = json.loads(str(stdout or ""))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Remote Region Plan schema v2 returned invalid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("Remote Region Plan schema v2 returned an invalid result.")
    return _safe_region_plan_schema_v2_result(payload)


def preview_region_plan_schema(*, environment: str) -> dict[str, Any]:
    """Preview the single common Region Plan Schema v2 reconciler."""
    profile, context = _region_plan_development_context(environment)
    with _remote_session_factory(profile) as remote:
        result = _run_region_plan_schema_v2(
            remote, context=context, command="preview"
        )
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_SCHEMA_CONFIRMATION_LOCK:
        expired = [
            target
            for target, (_, _, _, _, issued_at) in _REGION_PLAN_SCHEMA_CONFIRMATIONS.items()
            if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL
        ]
        for target in expired:
            _REGION_PLAN_SCHEMA_CONFIRMATIONS.pop(target, None)
        _REGION_PLAN_SCHEMA_CONFIRMATIONS[context["remote_target_id"]] = (
            _REGION_PLAN_SCHEMA_CONFIRMATION,
            _REGION_PLAN_SCHEMA_ID,
            result["checksum_sha256"],
            context["release_version"],
            now,
        )
    return result


def install_region_plan_schema(*, environment: str, confirm: bool = False) -> dict[str, Any]:
    """Reconcile the single common Region Plan Schema v2 checksum."""
    profile, context = _region_plan_development_context(environment)
    if confirm is not True:
        raise PermissionError("Region-plan schema preparation requires explicit confirmation.")
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_SCHEMA_CONFIRMATION_LOCK:
        item = _REGION_PLAN_SCHEMA_CONFIRMATIONS.get(context["remote_target_id"])
        if item is None:
            raise PermissionError("Region-plan schema confirmation is unavailable; preview the migration first.")
        confirmation, preview_schema_id, preview_checksum, release_version, issued_at = item
        if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL:
            _REGION_PLAN_SCHEMA_CONFIRMATIONS.pop(context["remote_target_id"], None)
            raise PermissionError("Region-plan schema confirmation has expired; preview the migration again.")
        if release_version != context["release_version"]:
            raise PermissionError("Region-plan schema confirmation does not match the pinned release.")
        if preview_schema_id != _REGION_PLAN_SCHEMA_ID:
            raise PermissionError("Region-plan schema confirmation does not match Schema v2.")
    with _remote_session_factory(profile) as remote:
        with remote.deployment_lock(
            str(profile["remote_root"]), f"region-plan-schema-{preview_checksum[:12]}"
        ):
            result = _run_region_plan_schema_v2(
                remote, context=context, command="reconcile",
                arguments=("--confirmation", confirmation),
            )
    if result["checksum_sha256"] != preview_checksum:
        raise RuntimeError("Region-plan schema reconcile checksum differs from the preview.")
    return result


def _region_plan_resolution_request(
    *,
    profile: Mapping[str, Any],
    source_version: str,
    boundary_resolutions: Mapping[str, Any],
    imported_by: str,
    idempotency_key: str,
) -> dict[str, Any]:
    version = _require_region_plan_digest(source_version, field="source_version")
    # Revalidate the local immutable upload record before using its fixed remote
    # path. This prevents a caller from promoting an arbitrary remote workbook.
    _load_managed_data_version(
        scope=_MANAGED_DATA_CANDIDATE_SCOPE,
        dataset_id=_MANAGED_DATA_CANDIDATE_DATASET,
        version=version,
    )
    return {
        "schema": "region-plan-resolution-request/v1",
        "source": _region_plan_managed_source_path(profile, version),
        "source_sha256": version,
        "managed_version": version,
        "boundary_resolutions": _region_plan_boundary_resolutions(boundary_resolutions),
        "imported_by": _require_region_plan_token(imported_by, field="imported_by"),
        "idempotency_key": _require_region_plan_idempotency_key(idempotency_key),
    }


def _remember_region_plan_resolution_download(
    *,
    request_sha256: str,
    request: Mapping[str, Any],
    context: Mapping[str, str],
) -> None:
    resolutions = request.get("boundary_resolutions")
    if not isinstance(resolutions, Mapping):
        raise RuntimeError("Region-plan resolution request is invalid.")
    normalized = _region_plan_boundary_resolutions(resolutions)
    source_version = _require_region_plan_digest(
        request.get("source_sha256"), field="source_version"
    )
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_RESOLUTION_DOWNLOAD_LOCK:
        expired = [
            digest
            for digest, (_, _, _, _, issued_at) in _REGION_PLAN_RESOLUTION_DOWNLOADS.items()
            if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL
        ]
        for digest in expired:
            _REGION_PLAN_RESOLUTION_DOWNLOADS.pop(digest, None)
        _REGION_PLAN_RESOLUTION_DOWNLOADS[request_sha256] = (
            source_version,
            normalized,
            f"{context['remote_target_id']}:{context['release_version']}",
            None,
            now,
        )


def _remember_region_plan_final_binding(
    *, result: Mapping[str, Any], source_version: str, context: Mapping[str, str]
) -> None:
    """Retain the backend-derived dynamic plan only behind its resolution digest."""

    resolution_digest = _require_region_plan_digest(
        result.get("resolution_digest"), field="resolution_digest"
    )
    plan_id = _require_region_plan_final_id(result.get("plan_id"))
    if not plan_id.startswith("atlanta_6area_v2_"):
        raise PermissionError("Legacy region-plan versions are read-only.")
    if (
        plan_id != _REGION_PLAN_FIXED_ID
        and _region_plan_versioned_digest(plan_id) != resolution_digest
    ):
        raise RuntimeError("Region-plan final binding is invalid.")
    version = _require_region_plan_digest(source_version, field="source_version")
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_FINAL_BINDING_LOCK:
        expired = [
            digest
            for digest, (_, _, _, issued_at) in _REGION_PLAN_FINAL_BINDINGS.items()
            if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL
        ]
        for digest in expired:
            _REGION_PLAN_FINAL_BINDINGS.pop(digest, None)
        _REGION_PLAN_FINAL_BINDINGS[resolution_digest] = (
            plan_id,
            version,
            f"{context['remote_target_id']}:{context['release_version']}",
            now,
        )


def _bound_region_plan_id(
    *, context: Mapping[str, str], resolution_digest: str
) -> str:
    digest = _require_region_plan_digest(resolution_digest, field="resolution_digest")
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_FINAL_BINDING_LOCK:
        item = _REGION_PLAN_FINAL_BINDINGS.get(digest)
        if item is None:
            raise PermissionError(
                "Region-plan resolution binding is unavailable; resolve the candidate again."
            )
        plan_id, _source_version, target_release, issued_at = item
        if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL:
            _REGION_PLAN_FINAL_BINDINGS.pop(digest, None)
            raise PermissionError(
                "Region-plan resolution binding has expired; resolve the candidate again."
            )
        if target_release != f"{context['remote_target_id']}:{context['release_version']}":
            raise PermissionError("Region-plan resolution binding does not match the selected target.")
        return plan_id


def _region_plan_resolution_artifact(
    *, environment: str, resolution_digest: str, artifact_id: str
) -> dict[str, Any]:
    """Generate one explicit PII-bearing download from a memory-bound resolution."""

    _profile, context = _region_plan_development_context(environment)
    digest = _require_region_plan_digest(resolution_digest, field="resolution_digest")
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_RESOLUTION_DOWNLOAD_LOCK:
        item = _REGION_PLAN_RESOLUTION_DOWNLOADS.get(digest)
        if item is None:
            raise PermissionError("Region-plan download is unavailable; preview the resolutions again.")
        source_version, resolutions, target_release, _plan_id, issued_at = item
        if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL:
            _REGION_PLAN_RESOLUTION_DOWNLOADS.pop(digest, None)
            raise PermissionError("Region-plan download has expired; preview the resolutions again.")
        if target_release != f"{context['remote_target_id']}:{context['release_version']}":
            raise PermissionError("Region-plan download does not match the selected target.")
        # Do not keep the mutable object owned by an outside caller after the
        # lock: it contains operator-entered rationales and remains memory-only.
        bound_resolutions = {
            postal: dict(decision) for postal, decision in resolutions.items()
        }
    _metadata, source_bytes = _load_managed_data_version(
        scope=_MANAGED_DATA_CANDIDATE_SCOPE,
        dataset_id=_MANAGED_DATA_CANDIDATE_DATASET,
        version=source_version,
    )
    from tools.data.atlanta_6area_plan import (
        BOUNDARY_POLICY_FILENAME,
        FIXED_REGION_FILENAME,
        MANIFEST_FILENAME,
        TECHNICIAN_POLICY_FILENAME,
        build_atlanta_6area_bundle,
    )

    artifacts = {
        "fixed_region_csv": (FIXED_REGION_FILENAME, "text/csv"),
        "technician_policy_csv": (TECHNICIAN_POLICY_FILENAME, "text/csv"),
        "boundary_policy_csv": (BOUNDARY_POLICY_FILENAME, "text/csv"),
        "manifest": (MANIFEST_FILENAME, "application/json"),
    }
    selected = artifacts.get(str(artifact_id))
    if selected is None:
        raise ValueError("artifact_id is not allowed.")
    try:
        bundle = build_atlanta_6area_bundle(
            source_bytes, boundary_resolutions=bound_resolutions
        )
    except Exception as exc:
        # The data-owned parser supplies detailed validation internally; a
        # download endpoint should not disclose its raw inputs or paths.
        raise RuntimeError("Region-plan artifact could not be regenerated from the bound source.") from exc
    filename, content_type = selected
    payload = bundle.artifacts.get(filename)
    if not isinstance(payload, bytes):
        raise RuntimeError("Region-plan artifact is unavailable.")
    return {
        "artifact_id": str(artifact_id),
        "file_name": filename,
        "content_type": content_type,
        "sha256": hashlib.sha256(payload).hexdigest(),
        # Bytes are intentionally returned only from this explicit download
        # boundary. Do not add them to receipts, previews, or history.
        "content": payload,
    }


def download_region_plan_resolution_artifact(
    *, environment: str, resolution_digest: str, artifact_id: str
) -> dict[str, Any]:
    return _region_plan_resolution_artifact(
        environment=environment,
        resolution_digest=resolution_digest,
        artifact_id=artifact_id,
    )


def preview_region_plan_resolutions(
    *,
    environment: str,
    source_version: str,
    boundary_resolutions: Mapping[str, Any],
    imported_by: str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Validate a fixed resolution request locally without writing remote state."""

    profile, _context = _region_plan_development_context(environment)
    request = _region_plan_resolution_request(
        profile=profile,
        source_version=source_version,
        boundary_resolutions=boundary_resolutions,
        imported_by=imported_by,
        idempotency_key=idempotency_key,
    )
    digest, _payload = _region_plan_request_bytes(request)
    _remember_region_plan_resolution_download(
        request_sha256=digest, request=request, context=_context
    )
    return {
        "status": "ready",
        "environment": _MANAGED_DATA_CANDIDATE_SCOPE,
        "source_version": request["source_sha256"],
        "request_sha256": digest,
        "resolution_digest": digest,
        "lifecycle_stage": "candidate_resolution_pending",
        "artifacts": [
            {"artifact_id": artifact_id, "available": True}
            for artifact_id in (
                "fixed_region_csv",
                "technician_policy_csv",
                "boundary_policy_csv",
                "manifest",
            )
        ],
    }


def apply_region_plan_resolutions(
    *,
    environment: str,
    source_version: str,
    boundary_resolutions: Mapping[str, Any],
    imported_by: str,
    idempotency_key: str,
    expected_request_sha256: str | None = None,
    confirm: bool = False,
) -> dict[str, Any]:
    """Resolve the fixed candidate through an immutable, checksum-bound request."""

    profile, context = _region_plan_development_context(environment)
    if confirm is not True:
        raise PermissionError("Region-plan resolution requires explicit confirmation.")
    request = _region_plan_resolution_request(
        profile=profile,
        source_version=source_version,
        boundary_resolutions=boundary_resolutions,
        imported_by=imported_by,
        idempotency_key=idempotency_key,
    )
    request_sha256, _request_bytes = _region_plan_request_bytes(request)
    if expected_request_sha256 is not None and _require_region_plan_digest(
        expected_request_sha256, field="expected_request_sha256"
    ) != request_sha256:
        raise PermissionError("Region-plan resolution request does not match its preview.")
    result = _run_region_plan_workflow_request(
        profile=profile, context=context, command="resolve", request=request
    )
    _remember_region_plan_resolution_download(
        request_sha256=request_sha256, request=request, context=context
    )
    _remember_region_plan_final_binding(
        result=result,
        source_version=str(request["source_sha256"]),
        context=context,
    )
    # The browser may still download the locally regenerated bound artifacts.
    # Alias that request record by the Admin Tools decision digest returned for
    # the final plan, without replacing the preview-time request binding.
    final_digest = str(result["resolution_digest"])
    with _REGION_PLAN_RESOLUTION_DOWNLOAD_LOCK:
        item = _REGION_PLAN_RESOLUTION_DOWNLOADS.get(request_sha256)
        if item is None:
            raise RuntimeError("Region-plan resolution download binding was lost.")
        source_version, resolutions, target_release, _plan_id, issued_at = item
        _REGION_PLAN_RESOLUTION_DOWNLOADS[final_digest] = (
            source_version,
            resolutions,
            target_release,
            str(result["plan_id"]),
            issued_at,
        )
    result["request_sha256"] = request_sha256
    result["artifacts"] = [
        {"artifact_id": artifact_id, "available": True}
        for artifact_id in (
            "fixed_region_csv",
            "technician_policy_csv",
            "boundary_policy_csv",
            "manifest",
        )
    ]
    return result


def review_region_plan(
    *,
    environment: str,
    expected_revision: int,
    reviewed_by: str,
    review_reference: str,
    resolution_digest: str | None = None,
    confirm: bool = False,
) -> dict[str, Any]:
    """Review a locally bound resolved plan at an optimistic revision."""

    profile, context = _region_plan_development_context(environment)
    if confirm is not True:
        raise PermissionError("Region-plan review requires explicit confirmation.")
    plan_id = (
        _REGION_PLAN_FIXED_ID
        if resolution_digest is None
        else _bound_region_plan_id(context=context, resolution_digest=resolution_digest)
    )
    request = {
        "schema": "region-plan-review-request/v1",
        "plan_id": plan_id,
        "expected_revision": _require_region_plan_revision(
            expected_revision, field="expected_revision"
        ),
        "reviewed_by": _require_region_plan_token(reviewed_by, field="reviewed_by"),
        "review_reference": _require_region_plan_token(
            review_reference, field="review_reference"
        ),
    }
    return _run_region_plan_workflow_request(
        profile=profile, context=context, command="review", request=request
    )


def preview_region_plan_activation(
    *, environment: str, resolution_digest: str | None = None
) -> dict[str, Any]:
    """Obtain a target-bound activation digest for a locally bound reviewed plan."""

    profile, context = _region_plan_development_context(environment)
    plan_id = (
        _REGION_PLAN_FIXED_ID
        if resolution_digest is None
        else _bound_region_plan_id(context=context, resolution_digest=resolution_digest)
    )
    raw_result = _run_region_plan_workflow_request(
        profile=profile,
        context=context,
        command="activation-preview",
        request={
            "schema": "region-plan-activation-preview-request/v1",
            "plan_id": plan_id,
        },
    )
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_ACTIVATION_PREVIEW_LOCK:
        expired = [
            digest
            for digest, (_, _, _, _, _, issued_at) in _REGION_PLAN_ACTIVATION_PREVIEWS.items()
            if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL
        ]
        for digest in expired:
            _REGION_PLAN_ACTIVATION_PREVIEWS.pop(digest, None)
        _REGION_PLAN_ACTIVATION_PREVIEWS[raw_result["preview_id"]] = (
            raw_result["preview_digest"],
            raw_result["checksum"],
            raw_result["expected_activation_revision"],
            plan_id,
            f"{context['remote_target_id']}:{context['release_version']}",
            now,
        )
    return raw_result


def apply_region_plan_activation(
    *,
    environment: str,
    preview_id: str,
    preview_digest: str,
    activated_by: str,
    activation_reference: str,
    idempotency_key: str,
    confirm: bool = False,
) -> dict[str, Any]:
    """Activate only an in-memory bound preview; never accept a caller checksum/revision."""

    profile, context = _region_plan_development_context(environment)
    if confirm is not True:
        raise PermissionError("Region-plan activation requires explicit confirmation.")
    preview_id = _require_region_plan_digest(preview_id, field="preview_id")
    preview_digest = _require_region_plan_digest(preview_digest, field="preview_digest")
    now = datetime.now(timezone.utc)
    with _REGION_PLAN_ACTIVATION_PREVIEW_LOCK:
        item = _REGION_PLAN_ACTIVATION_PREVIEWS.get(preview_id)
        if item is None:
            raise PermissionError("Region-plan activation preview is unavailable; create a new preview.")
        digest, checksum, expected_revision, plan_id, target_release, issued_at = item
        if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL:
            _REGION_PLAN_ACTIVATION_PREVIEWS.pop(preview_id, None)
            raise PermissionError("Region-plan activation preview has expired; create a new preview.")
        if (
            digest != preview_digest
            or target_release != f"{context['remote_target_id']}:{context['release_version']}"
        ):
            raise PermissionError("Region-plan activation preview does not match the selected target.")
    request = {
        "schema": "region-plan-activate-request/v1",
        "plan_id": plan_id,
        "preview_id": preview_id,
        "preview_digest": preview_digest,
        "checksum": checksum,
        "expected_activation_revision": expected_revision,
        "activated_by": _require_region_plan_token(activated_by, field="activated_by"),
        "activation_reference": _require_region_plan_token(
            activation_reference, field="activation_reference"
        ),
        "idempotency_key": _require_region_plan_idempotency_key(idempotency_key),
    }
    return _run_region_plan_workflow_request(
        profile=profile, context=context, command="activate", request=request
    )


def _development_admin_tools_activation_context(
    profile: Mapping[str, Any], version: str
) -> dict[str, str]:
    """Resolve a candidate development-only pin without changing local state."""

    version = str(version).strip()
    if not _MASTER_ADMIN_RELEASE_VERSION.fullmatch(version):
        raise ValueError("version must be a valid Admin Tools release version.")
    candidate = dict(profile)
    candidate["admin_tools_development_release_version"] = version
    context = _master_admin_context(candidate, "development")
    if context["pin_scope"] != "development":  # defensive future-proofing
        raise PermissionError("Admin Tools activation requires a development-only pin.")
    return context


def _verified_development_admin_tools_activation(
    *, version: str
) -> tuple[dict[str, Any], dict[str, str], str]:
    """Collect read-only evidence that a candidate development pin is executable."""

    profile = _master_admin_profile()
    context = _development_admin_tools_activation_context(profile, version)
    with _remote_session_factory(profile) as remote:
        _verify_master_admin_release(remote, context)
    inspection = inspect_artifact(
        path=str(_artifact_root("development", "admin-tools") / context["release_version"]),
        kind="admin-tools",
        environment="development",
    )
    mode = _admin_tools_release_mode(inspection.manifest)
    if mode is None:  # _verify_master_admin_release already rejects this.
        raise PermissionError("Admin Tools activation release policy is invalid.")
    return profile, context, mode


def preview_admin_tools_development_activation(*, version: str) -> dict[str, Any]:
    """Read-only eligibility check for an already uploaded development release.

    This endpoint never writes the local profile and never mutates remote
    files.  It reuses the runtime release verifier, including the exact upload
    receipt, complete remote inventory, and every remote content hash.
    """

    _, context, mode = _verified_development_admin_tools_activation(version=version)
    return {
        "status": "ready",
        "version": context["release_version"],
        "eligible": True,
        "mode": mode,
    }


def activate_admin_tools_development_release(
    *, version: str, confirm: bool = False
) -> dict[str, Any]:
    """Activate one already verified Admin Tools release only for development.

    Activation has no remote side effect.  The immediately preceding read-only
    remote validation is repeated under the local settings lock, then the
    fixed ignored profile is updated through the existing recoverable atomic
    transaction.  The common production-capable pin is intentionally untouched.
    """

    if confirm is not True:
        raise PermissionError(
            "Development Admin Tools activation requires explicit confirmation."
        )
    with _CONNECTION_SETTINGS_LOCK:
        _recover_connection_settings_transaction()
        _, context, mode = _verified_development_admin_tools_activation(version=version)
        profile_path = _connection_profile_path()
        profile = _read_json(profile_path)
        _require_fixed_credentials_reference(profile)
        updated_profile = dict(profile)
        if (
            str(updated_profile.get("admin_tools_development_release_version", "")).strip()
            == context["release_version"]
        ):
            status = "already_activated"
        else:
            updated_profile["admin_tools_development_release_version"] = context[
                "release_version"
            ]
            _commit_connection_settings_transaction([(profile_path, updated_profile)])
            status = "activated"
    return {"status": status, "version": context["release_version"], "mode": mode}


def _require_master_table_id(table_id: str) -> str:
    normalized = str(table_id).strip()
    if not _MASTER_ADMIN_TABLE_ID.fullmatch(normalized):
        raise ValueError("Master table id is invalid.")
    return normalized


def _require_preview_token(preview_id: str, preview_digest: str) -> tuple[str, str]:
    identifier = str(preview_id).strip()
    digest = str(preview_digest).strip().lower()
    if not _MASTER_ADMIN_PREVIEW_ID.fullmatch(identifier):
        raise ValueError("Preview id is invalid.")
    if not _MASTER_ADMIN_DIGEST.fullmatch(digest):
        raise ValueError("Preview digest is invalid.")
    return identifier, digest


def _remember_preview_confirmation(
    result: dict[str, Any], context: Mapping[str, str]
) -> dict[str, Any]:
    """Retain the backend-issued apply guard only in local process memory.

    The token is not a UI field and is never written to console history.  A
    process restart deliberately forces a fresh preview; the remote data
    boundary independently validates the preview id/digest/target and expiry.
    """

    has_identifier = bool(result.get("preview_id"))
    has_digest = bool(result.get("preview_digest"))
    if not has_identifier and not has_digest:
        return result
    if not has_identifier or not has_digest:
        raise RuntimeError("Remote CSV preview returned an incomplete preview token.")
    preview_id, preview_digest = _require_preview_token(
        str(result["preview_id"]), str(result["preview_digest"])
    )
    confirmation = ""
    for key in (
        "_private_confirmation_token",
        "confirmation_token",
        "apply_confirmation",
        "required_confirmation",
    ):
        value = result.pop(key, None)
        if value is not None and not confirmation:
            confirmation = str(value).strip()
    if not confirmation or len(confirmation) > 1024 or any(ord(char) < 32 for char in confirmation):
        raise RuntimeError("Remote CSV preview did not issue a safe apply confirmation token.")
    now = datetime.now(timezone.utc)
    with _MASTER_PREVIEW_CONFIRMATION_LOCK:
        expired = [
            identifier
            for identifier, (_, _, _, _, issued_at) in _MASTER_PREVIEW_CONFIRMATIONS.items()
            if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL
        ]
        for identifier in expired:
            _MASTER_PREVIEW_CONFIRMATIONS.pop(identifier, None)
        _MASTER_PREVIEW_CONFIRMATIONS[preview_id] = (
            preview_digest,
            context["environment"],
            context["remote_target_id"],
            confirmation,
            now,
        )
    return result


def _preview_confirmation(
    preview_id: str, preview_digest: str, context: Mapping[str, str]
) -> str:
    now = datetime.now(timezone.utc)
    with _MASTER_PREVIEW_CONFIRMATION_LOCK:
        item = _MASTER_PREVIEW_CONFIRMATIONS.get(preview_id)
        if item is None:
            raise PermissionError("Preview confirmation is unavailable; create a new CSV preview.")
        digest, environment, remote_target_id, confirmation, issued_at = item
        if now - issued_at > _MASTER_PREVIEW_CONFIRMATION_TTL:
            _MASTER_PREVIEW_CONFIRMATIONS.pop(preview_id, None)
            raise PermissionError("Preview confirmation has expired; create a new CSV preview.")
        if (
            digest != preview_digest
            or environment != context["environment"]
            or remote_target_id != context["remote_target_id"]
        ):
            raise PermissionError("Preview confirmation does not match the selected target.")
        return confirmation


def get_database_overview(*, environment: str) -> dict[str, Any]:
    """Read the selected server database through its pinned Admin Tools release."""

    profile = _master_admin_profile()
    context = _master_admin_context(profile, environment)
    with _remote_session_factory(profile) as remote:
        return _run_master_admin_command(remote, context, "overview")


def list_master_table_specs(*, environment: str) -> dict[str, Any]:
    """Return only the remote data backend's fixed master-table registry."""

    profile = _master_admin_profile()
    context = _master_admin_context(profile, environment)
    with _remote_session_factory(profile) as remote:
        return _run_master_admin_command(remote, context, "list-specs")


def preview_master_csv_upsert(
    *, environment: str, table_id: str, file_name: str, csv_bytes: bytes
) -> dict[str, Any]:
    """Stage one bounded private CSV for a preview, then remove it unconditionally."""

    context_environment = _require_environment(environment)
    if context_environment == "production":
        raise PermissionError("Production master CSV preview is disabled in db-admin/v1.")
    table_id = _require_master_table_id(table_id)
    safe_name = Path(str(file_name)).name
    if not safe_name or safe_name in {".", ".."} or len(safe_name) > 255:
        raise ValueError("CSV file name is invalid.")
    if not isinstance(csv_bytes, bytes) or not csv_bytes:
        raise ValueError("A non-empty UTF-8 CSV payload is required.")
    if len(csv_bytes) > _MASTER_ADMIN_MAX_CSV_BYTES:
        raise ValueError("CSV exceeds the platform safety limit.")
    try:
        csv_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("CSV must be UTF-8 encoded.") from exc
    if b"\x00" in csv_bytes:
        raise ValueError("CSV contains an invalid NUL byte.")

    profile = _master_admin_profile()
    context = _master_admin_context(profile, context_environment)
    stage_path = posixpath.join(
        context["stage_root"], f"preview-{uuid.uuid4().hex}.csv"
    )
    cleanup_error: Exception | None = None
    result: dict[str, Any] | None = None
    with _remote_session_factory(profile) as remote:
        try:
            # This is the only DB-admin remote file write.  It occurs after an
            # explicit preview action, is checksum verified, and locks both
            # temporary and final names to 0600.
            remote.upload_bytes_atomic(csv_bytes, stage_path, backup=None)
            result = _run_master_admin_command(
                remote,
                context,
                "preview",
                ("--table", table_id, "--csv", stage_path),
            )
        finally:
            try:
                if remote.exists(stage_path):
                    remote.remove(stage_path)
            except Exception as exc:  # cleanup failure must not leave CSV retention silent
                cleanup_error = exc
    if cleanup_error is not None:
        raise RuntimeError("Remote CSV cleanup failed; preview result is withheld.") from cleanup_error
    if result is None:
        raise RuntimeError("Remote CSV preview returned no result.")
    return _remember_preview_confirmation(result, context)


def apply_master_csv_upsert(
    *,
    environment: str,
    preview_id: str,
    preview_digest: str,
    idempotency_key: str,
    confirm: bool = False,
) -> dict[str, Any]:
    """Apply an already previewed CSV without accepting CSV/table input again."""

    environment = _require_environment(environment)
    if environment == "production":
        raise PermissionError("Production master CSV apply is disabled in db-admin/v1.")
    if confirm is not True:
        raise PermissionError("Master CSV apply requires the explicit second confirmation.")
    preview_id, preview_digest = _require_preview_token(preview_id, preview_digest)
    try:
        idempotency = str(uuid.UUID(str(idempotency_key)))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("idempotency_key must be a UUID.") from exc
    profile = _master_admin_profile()
    context = _master_admin_context(profile, environment)
    confirmation = _preview_confirmation(preview_id, preview_digest, context)
    arguments = [
        "--preview-id",
        preview_id,
        "--preview-digest",
        preview_digest,
        "--idempotency-key",
        idempotency,
        "--confirmation",
        # This opaque value came from the preceding remote preview.  It never
        # originates in the UI and is bound again by the remote data backend.
        confirmation,
    ]
    with _remote_session_factory(profile) as remote:
        return _run_master_admin_command(remote, context, "apply", arguments)


def get_master_change_receipt(*, environment: str, operation_id: str) -> dict[str, Any]:
    """Retrieve one redacted, target-bound master-data operation receipt."""

    try:
        identifier = str(uuid.UUID(str(operation_id)))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("operation_id must be a UUID.") from exc
    profile = _master_admin_profile()
    context = _master_admin_context(profile, environment)
    with _remote_session_factory(profile) as remote:
        return _run_master_admin_command(
            remote, context, "receipt", ("--operation-id", identifier)
        )


def _load_migration_specs() -> tuple[MigrationSpec, ...]:
    if not MIGRATION_MANIFEST.is_file():
        return ()
    payload = _read_json(MIGRATION_MANIFEST)
    specs: list[MigrationSpec] = []
    for item in payload.get("migrations", []):
        if not isinstance(item, Mapping):
            raise ValueError("Migration manifest entries must be objects.")
        sql_path = _within(MIGRATIONS_ROOT / str(item["sql_file"]), MIGRATIONS_ROOT)
        specs.append(
            MigrationSpec(
                migration_id=str(item["migration_id"]),
                description=str(item["description"]),
                sql_path=sql_path,
                checksum_sha256=str(item["checksum_sha256"]),
                rollback_instructions=str(item["rollback_instructions"]),
                reversible=item.get("reversible") is True,
                rollback_migration_id=item.get("rollback_migration_id"),
            )
        )
    return tuple(specs)


def _legacy_db_admin_unavailable(*, operation: str, environment: str) -> dict[str, Any]:
    """Return an explicit non-mutating status for retired local DB entrypoints.

    The previous implementation accepted a workstation config path and used it
    to connect to PostgreSQL or spawn a local admin tool.  That bypasses the
    SSH target identity, pinned remote release, runtime virtual environment,
    and server-side config boundary.  Keep the public compatibility methods
    until their remote JSON CLI counterparts are released, but fail closed.
    """

    environment = _require_environment(environment)
    return {
        "status": "unavailable",
        "error_code": "REMOTE_ADMIN_CLI_REQUIRED",
        "operation": operation,
        "environment": environment,
        "message": (
            "This legacy DB operation is disabled because local database execution "
            "is not permitted. Use the remote db-admin/v1 overview and master CSV "
            "workflow, or install an allowlisted remote Admin Tools CLI."
        ),
    }


def list_migrations(*, environment: str, config_path: str) -> list[dict[str, Any]]:
    """Compatibility listing; migration counts/status live in remote overview.

    ``config_path`` remains accepted so existing callers do not break, but it
    is intentionally ignored and never read.  The remote overview displays
    registry, applied, and pending counts without exposing a local DB route.
    """

    del config_path
    _require_environment(environment)
    return []


def preview_migration(*, environment: str, migration_id: str, config_path: str) -> dict[str, Any]:
    del migration_id, config_path
    return _legacy_db_admin_unavailable(operation="preview_migration", environment=environment)


def execute_migration(
    *,
    environment: str,
    migration_id: str,
    config_path: str,
    typed_confirmation: str,
    dry_run: bool = True,
) -> Any:
    # Preserve the compatibility confirmation argument in the public shape;
    # a remote migration CLI must compare it against its server-side plan
    # before this entrypoint can ever be re-enabled.
    del migration_id, config_path, typed_confirmation, dry_run
    return _legacy_db_admin_unavailable(
        operation="execute_migration", environment=environment
    )


def list_seed_actions(*, environment: str, config_path: str) -> list[dict[str, Any]]:
    del config_path
    _require_environment(environment)
    return []


def run_seed_action(
    *,
    environment: str,
    action_id: str,
    config_path: str,
    typed_confirmation: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    del action_id, config_path, typed_confirmation, dry_run
    return _legacy_db_admin_unavailable(
        operation="run_seed_action", environment=environment
    )


def _allowed_services(environment: str) -> dict[str, str]:
    return dict(SERVICE_SPECS[_require_environment(environment)])


def _observe(
    remote: Any,
    environment: str,
    units: Iterable[str] | None = None,
    *,
    detailed: bool = True,
) -> list[dict[str, Any]]:
    allowed = _allowed_services(environment)
    selected = list(units or allowed)
    if any(unit not in allowed for unit in selected):
        raise ValueError("Service unit is not allowlisted.")
    rows: list[dict[str, Any]] = []
    for unit in selected:
        active_code, active_out, _ = remote.execute(f"systemctl is-active {unit}")
        health_code, _, _ = remote.execute(
            f"curl --silent --show-error --fail --max-time 5 {allowed[unit]} >/dev/null"
        )
        enabled_code = enabled_out = 0
        journal_code = 0
        journal_out = ""
        if detailed:
            enabled_code, enabled_out, _ = remote.execute(f"systemctl is-enabled {unit}")
            journal_code, journal_out, _ = remote.execute(
                f"journalctl -u {unit} -n 40 --no-pager --output=short-iso"
            )
        rows.append(
            {
                "unit": unit,
                "active": active_code == 0 and active_out.strip() == "active",
                "enabled": (enabled_code == 0 and "enabled" in enabled_out) if detailed else None,
                "health_ok": health_code == 0,
                "journal_tail": _redact(journal_out) if detailed and journal_code == 0 else "",
            }
        )
    return rows


def observe_services(*, environment: str, config_path: str) -> list[dict[str, Any]]:
    profile = _load_remote_profile(config_path)
    with _remote_session_factory(profile) as remote:
        return _observe(remote, environment)


def observe_platform(*, config_path: str) -> dict[str, Any]:
    """Read-only health snapshot for both app environments and shared OSRM."""

    profile = _load_remote_profile(config_path)
    rows: list[dict[str, Any]] = []
    with _remote_session_factory(profile) as remote:
        for environment in ("production", "development"):
            for item in _observe(remote, environment):
                rows.append(
                    {
                        **item,
                        "scope": environment,
                        "component": item["unit"].removesuffix(".service"),
                        "component_type": "application",
                        "health_endpoint": _allowed_services(environment)[item["unit"]],
                        "port": int(
                            re.search(
                                r":(\d+)(?:/|$)",
                                _allowed_services(environment)[item["unit"]],
                            ).group(1)
                        ),
                    }
                )

        unit_status: dict[str, tuple[bool, bool, str]] = {}
        for component, unit, health_url, port in OSRM_MONITOR_SPECS:
            if unit not in unit_status:
                active_code, active_out, _ = remote.execute(f"systemctl is-active {unit}")
                enabled_code, enabled_out, _ = remote.execute(f"systemctl is-enabled {unit}")
                journal_code, journal_out, _ = remote.execute(
                    f"journalctl -u {unit} -n 40 --no-pager --output=short-iso"
                )
                unit_status[unit] = (
                    active_code == 0 and active_out.strip() == "active",
                    enabled_code == 0 and "enabled" in enabled_out,
                    _redact(journal_out) if journal_code == 0 else "",
                )
            active, enabled, journal = unit_status[unit]
            health_code, _, _ = remote.execute(
                f"curl --silent --show-error --fail --max-time 5 {health_url} >/dev/null"
            )
            rows.append(
                {
                    "scope": "shared",
                    "component": component,
                    "component_type": "osrm",
                    "unit": unit,
                    "health_endpoint": health_url,
                    "port": port,
                    "active": active,
                    "enabled": enabled,
                    "health_ok": health_code == 0,
                    "journal_tail": journal,
                }
            )

    for row in rows:
        row["status"] = "healthy" if row["active"] and row["health_ok"] else "unhealthy"
    healthy = sum(row["status"] == "healthy" for row in rows)
    return {
        "checked_at": _now(),
        "target_id": _target_id(profile, "production"),
        "healthy": healthy,
        "total": len(rows),
        "services": rows,
    }


def _find_remote_matching_runtime_artifact(
    remote: Any,
    environment: str,
) -> tuple[ArtifactInspection, list[dict[str, str]]] | None:
    """Adopt a direct-FTP runtime only when its complete manifest matches.

    This is deliberately a fallback for service control.  It never trusts the
    FTP operation itself: every file in a fully validated local artifact must
    match the current remote runtime before the artifact can be adopted.
    """
    candidates: list[ArtifactInspection] = []
    for entry in list_artifacts(environment=environment, kind="runtime"):
        try:
            item = inspect_artifact(path=entry.path, kind="runtime", environment=environment)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if item.restricted_data or item.manifest.get("source_dirty") is True and environment == "production":
            continue
        candidates.append(item)
    candidates.sort(key=lambda item: item.version, reverse=True)
    for item in candidates:
        files = _manifest_files(item.manifest)
        verified: list[dict[str, str]] = []
        matched = True
        for relative, checksum in sorted(files.items()):
            target = _remote_target(item, relative)
            if remote.sha256(target) != checksum:
                matched = False
                break
            verified.append({"path": relative, "target": target, "sha256": checksum})
        if matched and verified:
            return item, verified
    return None


def run_service_action(
    *,
    environment: str,
    action: str,
    units: Iterable[str],
    release_id: str,
    config_path: str,
    typed_confirmation: str,
) -> dict[str, Any]:
    environment = _require_environment(environment)
    action = str(action).lower()
    if action not in {"start", "restart"}:
        raise ValueError("Only start and restart are supported.")
    allowed = _allowed_services(environment)
    if isinstance(units, (str, bytes)):
        raise ValueError("Service units must be an iterable of unit names.")
    selected_set = {str(unit) for unit in units}
    if not selected_set or not selected_set.issubset(allowed):
        raise ValueError("Service selection is empty or not allowlisted.")
    ordered = [unit for unit in allowed if unit in selected_set]
    release_id = str(release_id).strip()
    expected = f"{action.upper()} {environment} {release_id} {','.join(ordered)}"
    if typed_confirmation != expected:
        raise ValueError("Typed service confirmation mismatch.")
    profile = _load_remote_profile(config_path)
    if not profile["allow_service_control"]:
        raise PermissionError("Service control is disabled in the local deployment profile.")
    target_id = _target_id(profile, environment)
    runtime_releases = [
        row
        for row in _load_history()
        if row.get("environment") == environment
        and row.get("kind") == "runtime"
        and row.get("status") == "uploaded"
        and row.get("service_eligible") is True
        and row.get("complete_manifest") is True
        and row.get("target_id") == target_id
        and re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256", "")))
    ]
    if not release_id or not runtime_releases or runtime_releases[-1].get("id") != release_id:
        raise PermissionError(
            "The latest complete runtime upload receipt for this target is required before service action."
        )
    release = runtime_releases[-1]
    deployed_files = release.get("verified_files")
    if not isinstance(deployed_files, list) or not deployed_files:
        # Compatibility for legacy full-manifest receipts created before
        # verified_files was recorded separately from changed files.
        if "selected_full_manifest" not in release:
            deployed_files = release.get("changes")
    if not isinstance(deployed_files, list) or not deployed_files:
        raise PermissionError("Runtime deployment receipt has no verifiable remote files.")
    audit_id = f"service-{environment}-{uuid.uuid4().hex[:12]}"
    audit = {
        "id": audit_id,
        "release_id": release_id,
        "environment": environment,
        "kind": "service-action",
        "action": action,
        "units": ordered,
        "target_id": target_id,
        "created_at": _now(),
        "operator": os.environ.get("USERNAME") or os.environ.get("USER") or "local-console",
        "status": "running",
        "completed_units": [],
        "external_artifact_adopted": False,
    }
    try:
        with _remote_session_factory(profile) as remote:
            with remote.deployment_lock(str(profile["remote_root"]), audit_id):
                expected_root = PurePosixPath(f"/home/csda/AI_Routing/{environment}")
                receipt_mismatch = False
                for change in deployed_files:
                    if not isinstance(change, Mapping):
                        receipt_mismatch = True
                        break
                    relative = _safe_relative(str(change.get("path", "")))
                    expected_target = (expected_root / PurePosixPath(relative)).as_posix()
                    checksum = str(change.get("sha256", "")).lower()
                    if (
                        str(change.get("target", "")) != expected_target
                        or not re.fullmatch(r"[0-9a-f]{64}", checksum)
                        or remote.sha256(expected_target) != checksum
                    ):
                        receipt_mismatch = True
                        break
                if receipt_mismatch:
                    adopted = _find_remote_matching_runtime_artifact(remote, environment)
                    if adopted is None:
                        raise RuntimeError(
                            "Remote runtime files do not match the release receipt, "
                            "and no complete local artifact matches the server."
                        )
                    adopted_item, adopted_files = adopted
                    deployed_files = adopted_files
                    audit["external_artifact_adopted"] = True
                    audit["external_artifact_version"] = adopted_item.version
                for unit in ordered:
                    code, _, _ = remote.execute(
                        f"sudo -n systemctl {action} {unit}", timeout=90
                    )
                    if code != 0:
                        raise RuntimeError(
                            f"systemd {action} failed for allowlisted unit {unit}."
                        )
                    audit["completed_units"].append(unit)
                # systemd ExecStartPost already waits for the unit health
                # endpoint.  Use a lightweight post-action check here and
                # reserve enabled/journal collection for Monitoring.
                observations = _observe(remote, environment, ordered, detailed=False)
        audit["observations"] = [
            {
                "unit": row["unit"],
                "active": row["active"],
                "health_ok": row["health_ok"],
            }
            for row in observations
        ]
        if not all(row["active"] and row["health_ok"] for row in observations):
            raise RuntimeError("Service action completed but health verification failed.")
    except Exception as exc:
        audit["status"] = "failed"
        audit["error"] = _redact(str(exc))
        audit["completed_at"] = _now()
        _append_history(audit)
        raise
    audit["status"] = "healthy"
    audit["completed_at"] = _now()
    _append_history(audit)
    return {
        "status": "healthy",
        "action_id": audit_id,
        "release_id": release_id,
        "external_artifact_adopted": audit.get("external_artifact_adopted", False),
        "external_artifact_version": audit.get("external_artifact_version", ""),
        "environment": environment,
        "action": action,
        "units": ordered,
        "observations": observations,
    }


def list_history(*, environment: str, kind: str) -> list[dict[str, Any]]:
    environment = _require_environment(environment)
    if kind not in HISTORY_KINDS:
        raise ValueError("Unknown history artifact kind.")
    return [
        row
        for row in reversed(_load_history())
        if row.get("environment") == environment and row.get("kind") == kind
    ]


def rollback_release(
    *,
    environment: str,
    kind: str,
    release_id: str,
    config_path: str,
    typed_confirmation: str,
) -> dict[str, Any]:
    environment = _require_environment(environment)
    expected = f"ROLLBACK {environment} {release_id}"
    if typed_confirmation != expected:
        raise ValueError("Typed rollback confirmation mismatch.")
    candidates = [
        row
        for row in _load_history()
        if row.get("id") == release_id
        and row.get("environment") == environment
        and row.get("kind") == kind
    ]
    if len(candidates) != 1:
        raise ValueError("Release history entry not found or ambiguous.")
    entry = candidates[0]
    profile = _load_remote_profile(config_path)
    if not profile["allow_upload"]:
        raise PermissionError("Rollback is disabled in the local deployment profile.")
    target_id = _target_id(profile, environment)
    if entry.get("target_id") != target_id:
        raise PermissionError("Rollback release does not belong to the selected remote target.")
    if entry.get("status") != "uploaded":
        raise PermissionError("Only an uploaded release can be rolled back.")
    relevant_releases = [
        row
        for row in _load_history()
        if row.get("environment") == environment
        and row.get("kind") == kind
        and row.get("target_id") == target_id
        and row.get("status") in {"uploaded", "rolled_back"}
    ]
    if not relevant_releases or relevant_releases[-1].get("id") != release_id:
        raise PermissionError("Only the latest release for this remote target can be rolled back.")

    changes = entry.get("changes")
    if not isinstance(changes, list) or not changes:
        raise ValueError("Release has no verifiable rollback files.")
    if kind == "runtime":
        target_root = PurePosixPath(f"/home/csda/AI_Routing/{environment}")
    elif kind == "server-data":
        target_root = PurePosixPath("/home/csda/AI_Routing")
    elif kind == "admin-tools":
        target_root = PurePosixPath(
            f"/home/csda/AI_Routing/admin_tools/releases/{entry.get('version', '')}"
        )
    else:
        raise ValueError("Unknown rollback artifact kind.")
    backups: list[dict[str, Any]] = []
    for raw_change in changes:
        if not isinstance(raw_change, Mapping):
            raise ValueError("Release rollback file entry is invalid.")
        change = dict(raw_change)
        relative = _safe_relative(str(change.get("path", "")))
        expected_target = (target_root / PurePosixPath(relative)).as_posix()
        expected_backup = posixpath.join(
            str(profile["remote_root"]), ".deployment_backups", release_id, relative
        )
        deployed_checksum = str(change.get("sha256", "")).lower()
        backup_checksum = str(change.get("backup_sha256", "")).lower()
        if (
            change.get("created") is True
            or str(change.get("target", "")) != expected_target
            or str(change.get("backup", "")) != expected_backup
            or not re.fullmatch(r"[0-9a-f]{64}", deployed_checksum)
            or not re.fullmatch(r"[0-9a-f]{64}", backup_checksum)
        ):
            raise ValueError(
                "Rollback requires a complete release of checksum-verified overwritten files."
            )
        change["path"] = relative
        change["sha256"] = deployed_checksum
        change["backup_sha256"] = backup_checksum
        backups.append(change)

    with _remote_session_factory(profile) as remote:
        with remote.deployment_lock(str(profile["remote_root"]), f"rollback-{release_id}"):
            guard_root = posixpath.join(
                str(profile["remote_root"]),
                ".deployment_backups",
                f"rollback-guard-{release_id}-{uuid.uuid4().hex[:12]}",
            )
            for change in backups:
                target = str(change["target"])
                backup = str(change["backup"])
                if remote.sha256(target) != change["sha256"]:
                    raise RuntimeError("Current remote files no longer match the release receipt.")
                if remote.sha256(backup) != change["backup_sha256"]:
                    raise RuntimeError("Rollback backup checksum mismatch.")
                guard = posixpath.join(guard_root, str(change["path"]))
                remote.copy(target, guard)
                if remote.sha256(guard) != change["sha256"]:
                    raise RuntimeError("Rollback guard checksum mismatch.")
                change["guard"] = guard
            attempted_restore: list[dict[str, Any]] = []
            try:
                for change in backups:
                    attempted_restore.append(change)
                    remote.copy(str(change["backup"]), str(change["target"]))
                    if remote.sha256(str(change["target"])) != change["backup_sha256"]:
                        raise RuntimeError("Rollback post-restore checksum mismatch.")
            except Exception as rollback_error:
                compensation_failed = False
                for change in reversed(attempted_restore):
                    try:
                        remote.copy(str(change["guard"]), str(change["target"]))
                        if remote.sha256(str(change["target"])) != change["sha256"]:
                            compensation_failed = True
                    except Exception:
                        compensation_failed = True
                if compensation_failed:
                    raise RuntimeError(
                        "Rollback failed and restoration of the deployed release was incomplete."
                    ) from rollback_error
                raise
    entry["status"] = "rolled_back"
    entry["rolled_back_at"] = _now()
    with _HISTORY_LOCK:
        rows = _load_history()
        for index, row in enumerate(rows):
            if row.get("id") == release_id:
                rows[index] = entry
        _save_history(rows)
    return {"status": "rolled_back", "remote_path": entry.get("version", "")}


def _region_plan_v2_api_origin() -> str:
    """Return the single, locally configured Region Plan v2 API origin.

    The console intentionally does not accept an endpoint from the browser.  It
    prevents a UI action from becoming an SSRF primitive and keeps the API
    destination stable across upload/review/activation steps.
    """
    origin = os.environ.get(_REGION_PLAN_V2_API_ENV, _REGION_PLAN_V2_API_ORIGIN).strip()
    parsed = urlparse(origin)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise RuntimeError("Region Plan v2 API endpoint configuration is invalid.")
    return origin.rstrip("/")


def _region_plan_v2_url(path: str) -> str:
    if not path.startswith("/") or ".." in path or "?" in path:
        raise ValueError("Region Plan v2 API path is invalid.")
    return _region_plan_v2_api_origin() + "/api/region-plans/v2" + path


def _region_plan_v2_resource_id(value: object, *, field: str) -> str:
    token = str(value).strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,159}", token):
        raise ValueError(f"Region Plan v2 {field} is invalid.")
    return token


def _region_plan_v2_request(
    method: str, path: str, *, body: bytes | None = None,
    headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    request_headers = {"Accept": "application/json"}
    request_headers.update(dict(headers or {}))
    request = Request(_region_plan_v2_url(path), data=body, headers=request_headers, method=method)
    try:
        with urlopen(request, timeout=_REGION_PLAN_V2_TIMEOUT_SECONDS) as response:  # nosec B310: fixed configured origin
            raw, status = response.read(), response.status
    except HTTPError as exc:
        raw, status = exc.read(), exc.code
    except URLError as exc:
        return {"contract_version": "region-plan/v2", "status": "failed", "error": {
            "code": "REGION_PLAN_API_UNAVAILABLE", "message": "Region Plan API is unavailable.", "retryable": True,
        }}
    try:
        envelope = json.loads(raw.decode("utf-8")) if raw else {}
    except (UnicodeDecodeError, json.JSONDecodeError):
        envelope = {}
    if not isinstance(envelope, dict):
        envelope = {}
    envelope["http_status"] = int(status)
    if "status" not in envelope:
        envelope.update({"contract_version": "region-plan/v2", "status": "failed", "error": {
            "code": "REGION_PLAN_API_INVALID_RESPONSE", "message": "Region Plan API returned an invalid response.", "retryable": status >= 500,
        }})
    return envelope


def _region_plan_v2_json(method: str, path: str, payload: Mapping[str, Any] | None = None, *,
                         revision: int | None = None) -> dict[str, Any]:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if revision is not None:
        headers["If-Match"] = str(int(revision))
    body = None if payload is None else json.dumps(dict(payload), separators=(",", ":")).encode("utf-8")
    if method != "GET":
        # The same logical request gets the same key across browser/API retries.
        # Authentication is server-owned because this client is loopback-only.
        fingerprint = hashlib.sha256(method.encode() + b"\0" + path.encode() + b"\0" + (body or b"{}")).hexdigest()
        headers["Idempotency-Key"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"region-plan-v2:{fingerprint}"))
    return _region_plan_v2_request(method, path, body=body, headers=headers)


def list_region_plan_v2_cities() -> dict[str, Any]:
    return _region_plan_v2_json("GET", "/cities")


def list_region_plan_v2_candidates(*, subsidiary_id: str, target_city_id: str) -> dict[str, Any]:
    # Candidate discovery is API-owned.  The query is encoded as a JSON request
    # body only where the v2 API exposes a list endpoint; no DB fallback exists.
    city = str(target_city_id).strip()
    if not city:
        raise ValueError("Target city is required.")
    subsidiary = str(subsidiary_id).strip()
    if not subsidiary:
        raise ValueError("Subsidiary is required.")
    return _region_plan_v2_json(
        "POST", "/plans/list",
        {"subsidiary_id": subsidiary, "target_city_id": city},
    )


def import_region_plan_v2_workbook(*, workbook_name: str, workbook_bytes: bytes,
                                   metadata: Mapping[str, Any]) -> dict[str, Any]:
    if not workbook_name.lower().endswith(".xlsx") or not workbook_bytes:
        raise ValueError("Select a non-empty .xlsx workbook.")
    if len(workbook_bytes) > _REGION_PLAN_V2_MAX_WORKBOOK_BYTES:
        raise ValueError("Workbook exceeds the console upload limit.")
    city_metadata = {
        str(key): str(value) for key, value in dict(metadata).items()
        if value is not None
    }
    return _region_plan_v2_json("POST", "/imports", {
        "workbook_name": Path(workbook_name).name,
        "workbook_base64": base64.b64encode(workbook_bytes).decode("ascii"),
        "city_metadata": city_metadata,
    })


def get_region_plan_v2_plan(*, subsidiary_id: str, target_city_id: str, plan_id: str) -> dict[str, Any]:
    return adopt_region_plan_v2_candidate(
        subsidiary_id=subsidiary_id,
        target_city_id=target_city_id,
        plan_id=plan_id,
    )


def adopt_region_plan_v2_candidate(*, subsidiary_id: str, target_city_id: str,
                                   plan_id: str) -> dict[str, Any]:
    return _region_plan_v2_json("POST", "/adopt", {
        "subsidiary_id": str(subsidiary_id).strip(),
        "target_city_id": str(target_city_id).strip(),
        "plan_id": _region_plan_v2_resource_id(plan_id, field="plan id"),
    })


def review_region_plan_v2(*, subsidiary_id: str, target_city_id: str, plan_id: str,
                          plan_revision: int, activation_revision: int) -> dict[str, Any]:
    return _region_plan_v2_json("POST", f"/plans/{_region_plan_v2_resource_id(plan_id, field='plan id')}/review", {
        "subsidiary_id": str(subsidiary_id).strip(),
        "target_city_id": str(target_city_id).strip(),
        "plan_revision": int(plan_revision),
        "activation_revision": int(activation_revision),
    }, revision=plan_revision)


def preview_region_plan_v2_activation(*, subsidiary_id: str, target_city_id: str, plan_id: str,
                                      plan_revision: int,
                                      activation_revision: int) -> dict[str, Any]:
    return _region_plan_v2_json("POST", f"/plans/{_region_plan_v2_resource_id(plan_id, field='plan id')}/activation-preview", {
        "subsidiary_id": str(subsidiary_id).strip(),
        "target_city_id": str(target_city_id).strip(),
        "plan_revision": int(plan_revision),
        "activation_revision": int(activation_revision),
    }, revision=plan_revision)


def activate_region_plan_v2(*, subsidiary_id: str, target_city_id: str, plan_id: str,
                            plan_revision: int, activation_revision: int,
                            preview_token: str, activation_reference: str) -> dict[str, Any]:
    return _region_plan_v2_json("POST", f"/plans/{_region_plan_v2_resource_id(plan_id, field='plan id')}/activate", {
        "subsidiary_id": str(subsidiary_id).strip(),
        "target_city_id": str(target_city_id).strip(),
        "plan_revision": int(plan_revision),
        "activation_revision": int(activation_revision),
        "preview_token": str(preview_token).strip(),
        "activation_reference": str(activation_reference).strip(),
        "confirmation": True,
    }, revision=plan_revision)


def rollback_region_plan_v2(*, subsidiary_id: str, target_city_id: str, plan_id: str,
                            plan_revision: int, activation_revision: int,
                            preview_token: str, rollback_reason: str,
                            confirmation: str) -> dict[str, Any]:
    reason=str(rollback_reason).strip()
    if not reason or confirmation != "ROLLBACK":
        raise ValueError("Rollback reason and exact ROLLBACK confirmation are required.")
    city=_region_plan_v2_resource_id(target_city_id,field="target city id")
    return _region_plan_v2_json("POST",f"/cities/{city}/rollback",{
        "subsidiary_id":str(subsidiary_id).strip(),"target_city_id":city,
        "plan_id":_region_plan_v2_resource_id(plan_id,field="plan id"),
        "plan_revision":int(plan_revision),"activation_revision":int(activation_revision),
        "preview_token":str(preview_token).strip(),"rollback_reason":reason,
        "confirmation":"ROLLBACK",
    },revision=plan_revision)


__all__ = [
    "ArtifactEntry",
    "ArtifactInspection",
    "activate_admin_tools_development_release",
    "apply_managed_data_db_sync",
    "apply_managed_technician_profile_sync",
    "apply_fixed_region_plan_bundle_import",
    "apply_region_plan_activation",
    "apply_region_plan_resolutions",
    "build_admin_tools_artifact",
    "build_runtime_artifact",
    "deployment_policy",
    "download_region_plan_resolution_artifact",
    "execute_migration",
    "get_connection_settings",
    "get_database_overview",
    "get_master_change_receipt",
    "inspect_artifact",
    "install_region_plan_schema",
    "list_region_plan_v2_candidates",
    "list_region_plan_v2_cities",
    "list_artifacts",
    "list_history",
    "list_managed_data_sets",
    "list_managed_data_versions",
    "list_migrations",
    "list_master_table_specs",
    "list_region_plan_schema_migrations",
    "list_seed_actions",
    "observe_platform",
    "observe_services",
    "get_fixed_region_plan_bundle_status",
    "preview_migration",
    "preview_fixed_region_plan_bundle_import",
    "preview_region_plan_activation",
    "preview_region_plan_resolutions",
    "preview_region_plan_schema",
    "preview_master_csv_upsert",
    "preview_admin_tools_build",
    "preview_admin_tools_development_activation",
    "preview_managed_data_db_sync",
    "preview_managed_technician_profile_sync",
    "preview_managed_data_upload",
    "preview_managed_data_version",
    "preview_runtime_build",
    "preview_remote_diff",
    "preview_development_secure_config_upload",
    "preview_production_secure_config_upload",
    "resolve_latest_runtime_artifact",
    "review_region_plan",
    "review_region_plan_v2",
    "rollback_release",
    "run_seed_action",
    "apply_master_csv_upsert",
    "run_service_action",
    "upload_artifact",
    "upload_managed_data_file",
    "upload_development_secure_config",
    "upload_production_secure_config",
    "update_connection_settings",
    "import_region_plan_v2_workbook",
    "get_region_plan_v2_plan",
    "adopt_region_plan_v2_candidate",
    "preview_region_plan_v2_activation",
    "activate_region_plan_v2",
    "rollback_region_plan_v2",
]
