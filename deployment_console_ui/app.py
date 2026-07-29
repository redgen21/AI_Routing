"""Streamlit presentation for allowlisted code, data, and DB deployments."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping

import streamlit as st

from .backend_adapter import BackendAdapter, BackendCapabilityError
from .helpers import (
    ARTIFACT_LABELS,
    classify_sql,
    confirmation_matches,
    expected_confirmation,
    public_mapping,
    redact_text,
    safe_manifest_files,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVER_CONFIG_PATH = "config/server_deploy.local.json"
DB_CONFIG_PATHS = {
    "development": "config/common_vrp.dev.json",
    "production": "config/common_vrp.prod.json",
}
BUILD_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
UI_ASSETS = Path(__file__).with_name("assets")

NAVIGATION = (
    ("dashboard", "Dashboard", "Dashboard"),
    ("monitoring", "Monitoring", "Monitoring"),
    ("package-development", "Package Management", "Development"),
    ("package-production", "Package Management", "Production"),
    ("package-admin-tools", "Package Management", "Admin Tools"),
    ("data", "Data Management", "Managed data"),
    ("region-plans", "Data Management", "Region Plans v2"),
    ("settings", "Settings", "Connection settings"),
)


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return asdict(value)
    return getattr(value, "__dict__", {})


def _label(item: object) -> str:
    value = _mapping(item)
    return str(
        value.get("label")
        or value.get("description")
        or value.get("version")
        or value.get("migration_id")
        or value.get("command_id")
        or value.get("release_id")
        or value.get("id")
        or value
    )


def _id(item: object) -> str:
    value = _mapping(item)
    return str(
        value.get("id")
        or value.get("migration_id")
        or value.get("command_id")
        or value.get("release_id")
        or value.get("version")
        or value.get("path")
        or ""
    )


def _result_message(result: object) -> str:
    # Backend messages can accidentally contain command/config context.  The UI
    # therefore renders only structured, non-secret receipt fields.
    data = public_mapping(result, ("status", "release_id", "remote_path", "sha256"))
    return redact_text(" | ".join(str(value) for value in data.values()) or "Completed")


def _safe_backend_error(error: BaseException) -> str:
    message = " ".join(str(error or "").split())
    message = redact_text(redact_text(message))
    return message[:500] or "Backend operation failed."


def _load_console_styles() -> None:
    """Use a local stylesheet so the operational console has no remote UI dependency."""

    stylesheet = UI_ASSETS / "styles.css"
    try:
        st.markdown(f"<style>{stylesheet.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)
    except OSError:
        # Appearance must never prevent a deployment or recovery operation.
        return


def _environment_for_route(route: str) -> str:
    if route == "package-production":
        return "production"
    if route == "package-admin-tools":
        return "common"
    return "development"


def _route_label(route: str) -> str:
    return next((label for key, _, label in NAVIGATION if key == route), "Dashboard")


def _render_navigation() -> str:
    """Render keyboard-accessible menu buttons, retaining a primitive route only."""

    route_key = "console-route"
    route = str(st.session_state.get(route_key) or "dashboard")
    valid_routes = {key for key, _, _ in NAVIGATION}
    if route not in valid_routes:
        route = "dashboard"
        st.session_state[route_key] = route

    with st.sidebar:
        st.markdown("<div class='console-brand'>AI Routing<br><span>Deployment Console</span></div>", unsafe_allow_html=True)
        current_group = ""
        for key, group, label in NAVIGATION:
            if group != current_group:
                st.markdown(f"<div class='nav-group'>{group}</div>", unsafe_allow_html=True)
                current_group = group
            icon = {
                "dashboard": "▦", "monitoring": "◌", "package-development": "⌘",
                "package-production": "⌘", "package-admin-tools": "⌘",
                "data": "▤", "settings": "⚙",
            }.get(key, "•")
            if st.button(
                f"{icon}  {label}",
                key=f"nav-{key}",
                type="primary" if route == key else "secondary",
                width="stretch",
            ):
                st.session_state[route_key] = key
                st.rerun()
        st.caption("Operational actions remain backend allowlisted and confirmed.")
    return route


def _render_top_toolbar(route: str) -> None:
    environment = _environment_for_route(route)
    title = _route_label(route)
    st.markdown(
        "<div class='console-toolbar'>"
        f"<div><h1>{title}</h1><p>AI Routing Deployment Console</p></div>"
        f"<div class='environment-badge {environment}'>{environment.title()}</div>"
        "</div>",
        unsafe_allow_html=True,
    )


def _invoke(
    adapter: BackendAdapter,
    name: str,
    *,
    error_sink: Callable[[str], None] | None = None,
    **kwargs: Any,
) -> Any | None:
    try:
        return adapter.call(name, **kwargs)
    except BackendCapabilityError as exc:
        message = _safe_backend_error(exc)
        if error_sink is not None:
            error_sink(message)
        else:
            st.info(message)
    except Exception as exc:
        message = _safe_backend_error(exc)
        if error_sink is not None:
            error_sink(message)
        else:
            st.error(f"Operation failed: {message}")
    return None


def _safe_upload_failure_summary(error: str | None = None) -> str:
    """Return a useful fixed summary without preserving backend exception text."""

    text = str(error or "").lower()
    if "lock" in text or "already in progress" in text:
        return "Another deployment is in progress for this target. Wait for it to finish, then retry."
    if "checksum" in text or "manifest" in text or "integrity" in text:
        return "The server validation did not match the reviewed artifact. The server diff was refreshed."
    if "permission" in text or "disabled" in text or "denied" in text:
        return "The backend did not permit this upload. Review the approved deployment policy."
    if any(term in text for term in ("connection", "transport", "timeout", "network", "simulated")):
        return "The upload connection did not complete. The server diff was refreshed before retry."
    return "The backend did not complete the upload. The server diff was refreshed before retry."


def _safe_release_version(value: object) -> str:
    """Display only a constrained artifact version, never arbitrary backend text."""

    candidate = str(value or "").strip()
    return candidate if BUILD_VERSION_PATTERN.fullmatch(candidate) else ""


def _admin_tools_pin_message(status: object, version: object = "") -> tuple[str, str] | None:
    """Translate stable pin states without exposing receipt error/reason fields."""

    pin_status = str(status or "")
    safe_version = _safe_release_version(version)
    if pin_status in {
        "pinned", "already_pinned", "pinned_common_and_development"
    }:
        suffix = f" ({safe_version})" if safe_version else ""
        return "success", f"Clean Admin Tools execution version is set{suffix} for Production and Development."
    if pin_status == "pinned_development_verification":
        suffix = f" ({safe_version})" if safe_version else ""
        return "success", f"Development DB verification execution version is set{suffix}."
    if pin_status == "not_pinned_policy":
        return "warning", (
            "Admin Tools upload completed, but it was not selected as the DB administration execution version. "
            "A clean, promotable build is required."
        )
    if pin_status == "pin_failed":
        return "warning", (
            "Admin Tools upload completed, but saving the local execution-version pin failed. "
            "DB administration will continue using its previously configured version."
        )
    return None


def _load_remote_diff(
    cache: MutableMapping[str, Any],
    cache_key: str,
    fetch: Callable[[], Any | None],
    *,
    force: bool = False,
) -> Any | None:
    """Cache one target-bound remote diff and refresh it after a real upload."""

    if force or cache_key not in cache:
        result = fetch()
        if result is not None:
            cache[cache_key] = result
        elif force:
            cache.pop(cache_key, None)
    return cache.get(cache_key)


def _project_relative_path(value: object) -> str:
    raw = str(value or "")
    try:
        return str(Path(raw).resolve().relative_to(PROJECT_ROOT.resolve()))
    except (OSError, ValueError):
        return raw


def _short_checksum(value: object) -> str:
    checksum = str(value or "")
    return checksum[:12] if checksum else "-"


def _diff_panels(rows: object) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    # These are comparison inventories, so retain matching files as well as
    # creates and updates.  The upload work queue is derived separately by
    # _changed_upload_files(), which deliberately excludes unchanged rows.
    values = rows if isinstance(rows, list) else []
    local = [
        {
            "path": _project_relative_path(_mapping(row).get("local_path")),
            "artifact_path": _mapping(row).get("path"),
            "sha256_12": _short_checksum(_mapping(row).get("local_sha256")),
            "size_bytes": _mapping(row).get("local_size_bytes"),
            "status": _mapping(row).get("status"),
        }
        for row in values
    ]
    remote = [
        {
            "path": _mapping(row).get("remote_path"),
            "artifact_path": _mapping(row).get("path"),
            "sha256_12": _short_checksum(_mapping(row).get("remote_sha256")),
            "size_bytes": _mapping(row).get("remote_size_bytes"),
            "status": _mapping(row).get("status"),
        }
        for row in values
    ]
    return (
        sorted(local, key=lambda row: str(row.get("artifact_path", ""))),
        sorted(remote, key=lambda row: str(row.get("artifact_path", ""))),
    )


def _upload_intent(
    *,
    environment: str,
    kind: str,
    artifact_id: str,
    archive_sha256: str,
    target_id: str,
    selected_files: list[str],
) -> dict[str, Any]:
    scope_id = _upload_scope_id(
        environment=environment,
        kind=kind,
        artifact_id=artifact_id,
        archive_sha256=archive_sha256,
        target_id=target_id,
    )
    payload = {
        "environment": environment,
        "kind": kind,
        "artifact_id": artifact_id,
        "archive_sha256": archive_sha256,
        "target_id": target_id,
        "scope_id": scope_id,
        "selected_files": sorted(set(selected_files)),
    }
    payload["intent_id"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return payload


def _upload_scope_id(
    *,
    environment: str,
    kind: str,
    artifact_id: str,
    archive_sha256: str,
    target_id: str,
) -> str:
    payload = {
        "environment": environment,
        "kind": kind,
        "artifact_id": artifact_id,
        "archive_sha256": archive_sha256,
        "target_id": target_id,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _changed_upload_files(rows: object) -> list[str]:
    values = rows if isinstance(rows, list) else []
    return sorted(
        {
            str(_mapping(row).get("path"))
            for row in values
            if _mapping(row).get("status") in {"create", "update"}
            and _mapping(row).get("path")
        }
    )


def _query_value(key: str) -> str:
    """Read a non-secret UI selection from the browser URL when available."""

    try:
        value = st.query_params.get(key)
    except (AttributeError, RuntimeError):
        return ""
    if isinstance(value, list):
        value = value[-1] if value else ""
    return str(value or "")


def _remember_query_value(key: str, value: str) -> None:
    """Keep only non-sensitive deployment picker state across browser reloads."""

    try:
        if _query_value(key) != value:
            st.query_params[key] = value
    except (AttributeError, RuntimeError):
        # The console remains usable in Streamlit test/embedded contexts that
        # do not expose query parameters; session-state fallback still works.
        return


def _select_retained_artifact(
    artifacts: list[object], *, selector_key: str, query_key: str
) -> object | None:
    """Select a local artifact by its stable version instead of its DTO.

    Streamlit can retain widget state across a reconnect while the backend
    recreates ArtifactEntry instances.  Keeping the primitive version in
    session state makes a retained data package selectable after that
    reconnect, while still falling back safely if a user removed it locally.
    """

    by_version = {
        str(_mapping(candidate).get("version") or _id(candidate)): candidate
        for candidate in artifacts
        if str(_mapping(candidate).get("version") or _id(candidate))
    }
    if not by_version:
        return None
    selected_version = str(st.session_state.get(selector_key) or "")
    if not selected_version:
        selected_version = _query_value(query_key)
    if selected_version not in by_version:
        st.session_state[selector_key] = next(iter(by_version))
    elif selector_key not in st.session_state:
        st.session_state[selector_key] = selected_version
    version = st.selectbox("Version", list(by_version), key=selector_key)
    _remember_query_value(query_key, str(version))
    return by_version[str(version)]


def _secure_config_intent(preview: Mapping[str, Any]) -> str:
    """Bind the second confirmation click to the exact redacted preview."""

    return str(preview.get("fingerprint") or preview.get("preview_fingerprint") or "")


def _render_secure_config(
    adapter: BackendAdapter,
    environment: str,
    *,
    config_path: str = SERVER_CONFIG_PATH,
) -> None:
    """Render the two-click, redacted secure-config uploader for one environment."""

    environment_label = environment.capitalize()
    preview_method = f"preview_{environment}_secure_config_upload"
    upload_method = f"upload_{environment}_secure_config"
    if not (adapter.has(preview_method) and adapter.has(upload_method)):
        return

    st.markdown(f"### {environment_label} secure config")
    st.caption(
        f"Uploads the approved {environment} config only. Values, passwords, and API keys are never shown here. "
        "This action does not restart services."
    )
    # The backend owns the fixed secure-config source set.  The UI supplies only
    # the deployment profile used to resolve the approved SFTP target and policy.
    preview = _invoke(
        adapter,
        preview_method,
        environment=environment,
        config_path=config_path,
    )
    if preview is None:
        return
    data = _mapping(preview)
    fingerprint = _secure_config_intent(data)
    policy_allowed = data.get("upload_allowed") is True
    mutation_required = data.get("mutation_required") is not False
    allowed = policy_allowed and mutation_required and bool(fingerprint)
    status = redact_text(data.get("status") or "unknown")[:160]
    if not policy_allowed:
        st.error(f"{environment_label} secure config upload is unavailable: " + status)
    elif not mutation_required:
        st.success(f"{environment_label} secure config is already up to date.")

    cols = st.columns(2)
    cols[0].metric("Local config", "2 protected local files")
    cols[1].metric("Server targets", "2 fixed server files")
    rows = data.get("files") if isinstance(data.get("files"), list) else []
    safe_rows = []
    for item in rows:
        row = _mapping(item)
        safe_rows.append(
            {
                "local_file": redact_text(row.get("filename") or "-")[:300],
                "remote_path": redact_text(row.get("target") or "-")[:300],
                "sha256_12": _short_checksum(row.get("sha256") or row.get("local_sha256")),
                "size_bytes": row.get("size_bytes") or row.get("local_size_bytes"),
                "mode": str(row.get("mode") or row.get("remote_mode") or "-"),
                "status": redact_text(row.get("status") or status)[:160],
            }
        )
    if safe_rows:
        st.dataframe(safe_rows, width="stretch", hide_index=True)
    else:
        st.caption("No secret values or file contents are displayed.")

    pending_key = f"pending-{environment}-secure-config"
    notice_key = f"{environment}-secure-config-notice"
    pending = _mapping(st.session_state.get(pending_key) or {})
    # A new preview fingerprint makes an older confirmation unsafe.
    if pending and pending.get("preview_fingerprint") != fingerprint:
        st.session_state.pop(pending_key, None)
        pending = {}
        st.info("Secure config preview changed; please review and confirm again.")
    notice = _mapping(st.session_state.get(notice_key) or {})
    if notice and notice.get("preview_fingerprint") != fingerprint:
        st.session_state.pop(notice_key, None)
        notice = {}
    if notice.get("status") == "completed":
        st.success(f"{environment_label} secure config uploaded. Service restart is required and was not performed.")
    elif notice.get("status") == "failed":
        st.error(f"{environment_label} secure config upload failed. Review the preview and try again.")

    if pending:
        st.warning(f"Upload this reviewed {environment} secure config?")
        confirm_col, cancel_col = st.columns(2)
        if confirm_col.button(
            "Confirm secure config upload",
            type="primary",
            disabled=not allowed,
            key=f"secure-config-confirm-{environment}-{fingerprint}",
        ):
            with st.status("Upload phase 0/2: submitting protected configuration...", expanded=True) as progress:
                st.caption("Current/total: 0/2. The backend reports this protected pair atomically, without per-file callbacks.")
                result = _invoke(
                    adapter,
                    upload_method,
                    environment=environment,
                    config_path=config_path,
                    expected_fingerprint=fingerprint,
                    dry_run=False,
                )
                progress.update(
                    label="Upload phase 2/2: completed" if result is not None else "Upload phase 0/2: failed",
                    state="complete" if result is not None else "error",
                )
            st.session_state.pop(pending_key, None)
            if result is None:
                st.session_state[notice_key] = {
                    "status": "failed",
                    "preview_fingerprint": fingerprint,
                }
            else:
                st.session_state[notice_key] = {
                    "status": "completed",
                    "preview_fingerprint": fingerprint,
                }
            st.rerun()
        if cancel_col.button(
            "Cancel secure config upload",
            key=f"secure-config-cancel-{environment}-{fingerprint}",
        ):
            st.session_state.pop(pending_key, None)
            st.rerun()
    elif st.button(
        f"Upload {environment} secure config",
        type="primary",
        disabled=not allowed,
        key=f"secure-config-request-{environment}-{fingerprint}",
    ):
        st.session_state.pop(notice_key, None)
        st.session_state[pending_key] = {"preview_fingerprint": fingerprint}
        st.rerun()


def _render_development_secure_config(adapter: BackendAdapter) -> None:
    """Compatibility wrapper for callers of the development config panel."""

    _render_secure_config(adapter, "development")


def _render_production_secure_config(adapter: BackendAdapter) -> None:
    """Compatibility wrapper for the production config panel."""

    _render_secure_config(adapter, "production")


def _render_environment_warning(environment: str) -> None:
    if environment == "production":
        st.error(
            "PRODUCTION: uploads, DB changes, service actions, and rollbacks affect "
            "the live routing service. Verify approval, backup, health checks, and rollback."
        )


def _next_build_version(previous_version: str) -> str:
    """Return a valid timestamp default distinct from the completed build."""

    candidate = datetime.now().strftime("%Y%m%d-%H%M%S")
    if candidate == previous_version:
        return f"{candidate}-next"
    return candidate


def _render_build_artifact(adapter: BackendAdapter, environment: str) -> None:
    if not (
        adapter.has("preview_runtime_build")
        and adapter.has("build_runtime_artifact")
    ):
        return

    st.markdown("### Build artifact")
    st.caption(
        "Builds a local runtime artifact from the current project checkout. "
        "Building does not upload or deploy it."
    )
    st.info(
        "Development artifacts are never promoted by copying them to production. "
        "Build the production artifact separately from a clean checkout."
    )
    receipt = _mapping(st.session_state.get("runtime-build-receipt", {}))
    if receipt.get("environment") == environment:
        st.success(
            f"Built runtime artifact {receipt.get('version', '-')} for {environment}."
        )

    version_key = f"runtime-build-version-{environment}"
    next_version_key = f"runtime-build-next-version-{environment}"
    default_version = datetime.now().strftime("%Y%m%d-%H%M%S")
    pending_next_version = st.session_state.pop(next_version_key, None)
    if isinstance(pending_next_version, str) and BUILD_VERSION_PATTERN.fullmatch(
        pending_next_version
    ):
        # This runs before the text_input is instantiated on the post-build rerun.
        # Mutating the widget key in the button handler itself is not Streamlit-safe.
        st.session_state.pop(version_key, None)
        default_version = pending_next_version
    version = st.text_input(
        "Build version",
        value=default_version,
        key=version_key,
        help="Letters, numbers, dot, underscore, and hyphen only.",
    ).strip()
    valid_version = bool(BUILD_VERSION_PATTERN.fullmatch(version))
    if not valid_version:
        st.error(
            "Version must start with a letter or number and contain only letters, "
            "numbers, dot, underscore, and hyphen."
        )

    preview = None
    if valid_version:
        preview = _invoke(
            adapter,
            "preview_runtime_build",
            environment=environment,
            version=version,
        )
    preview_data = _mapping(preview or {})
    source_dirty = preview_data.get("source_dirty") is True
    if preview is not None:
        cols = st.columns(3)
        cols[0].metric("Git revision", str(preview_data.get("source_revision", "-"))[:12])
        cols[1].metric("Source dirty", str(source_dirty))
        cols[2].metric("Changed paths", str(preview_data.get("source_change_count", 0)))
        st.caption(
            f"Staging: {_project_relative_path(preview_data.get('staging_path', '-'))}"
        )
        st.caption(
            f"Archive: {_project_relative_path(preview_data.get('archive_path', '-'))}"
        )

    allow_dirty = False
    if source_dirty and environment == "development":
        st.warning(
            "The checkout has tracked or untracked changes. This verification artifact "
            "will be non-promotable and requires explicit approval."
        )
        allow_dirty = st.checkbox(
            "Allow dirty source for this development build",
            key=f"runtime-build-allow-dirty-{environment}",
        )
    elif source_dirty and environment == "production":
        st.error(
            "Production artifacts require a clean checkout. Dirty-source bypass is never available."
        )
    if preview_data.get("output_exists") is True:
        st.error("This version already has staging or ZIP output. Choose a new version.")

    can_build = (
        valid_version
        and preview is not None
        and preview_data.get("build_allowed") is True
        and (not source_dirty or (environment == "development" and allow_dirty))
    )
    if st.button(
        "Build runtime artifact",
        type="primary",
        disabled=not can_build,
        key=f"runtime-build-submit-{environment}",
    ):
        with st.spinner("Building and validating local runtime artifact..."):
            result = _invoke(
                adapter,
                "build_runtime_artifact",
                environment=environment,
                version=version,
                allow_dirty_source=allow_dirty,
            )
        if result is not None:
            build_receipt = dict(
                public_mapping(
                    result,
                    (
                        "status",
                        "environment",
                        "version",
                        "source_revision",
                        "source_dirty",
                        "source_mode",
                        "staging_path",
                        "manifest_path",
                        "archive_path",
                        "archive_sha256",
                    ),
                )
            )
            st.session_state["runtime-build-receipt"] = build_receipt
            completed_version = str(build_receipt.get("version") or version)
            st.session_state[next_version_key] = _next_build_version(completed_version)
            # Applied by the artifact picker on the post-build rerun, before its
            # widget is created. Keep only the primitive version, never a stale
            # artifact object/path.
            st.session_state[f"runtime-build-select-{environment}"] = completed_version
            st.rerun()


def _render_admin_tools_build(adapter: BackendAdapter, environment: str) -> None:
    """Build the immutable Admin Tools release consumed by DB administration."""

    if not (
        adapter.has("preview_admin_tools_build")
        and adapter.has("build_admin_tools_artifact")
    ):
        return

    st.markdown("### Build Admin Tools artifact")
    st.caption(
        "Builds a local Admin Tools artifact from the current checkout. "
        "Building does not upload it or change the DB administration execution version."
    )
    st.info(
        "Only a clean, promotable artifact can be pinned as the DB administration execution version after upload."
    )
    receipt = _mapping(st.session_state.get("admin-tools-build-receipt", {}))
    if receipt:
        built_version = _safe_release_version(receipt.get("version")) or "completed"
        st.success(f"Built Admin Tools artifact {built_version}.")

    version_key = "admin-tools-build-version"
    next_version_key = "admin-tools-build-next-version"
    default_version = "admin-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    pending_next_version = st.session_state.pop(next_version_key, None)
    if isinstance(pending_next_version, str) and BUILD_VERSION_PATTERN.fullmatch(
        pending_next_version
    ):
        st.session_state.pop(version_key, None)
        default_version = pending_next_version
    version = st.text_input(
        "Admin Tools build version",
        value=default_version,
        key=version_key,
        help="Letters, numbers, dot, underscore, and hyphen only.",
    ).strip()
    valid_version = bool(BUILD_VERSION_PATTERN.fullmatch(version))
    if not valid_version:
        st.error(
            "Version must start with a letter or number and contain only letters, "
            "numbers, dot, underscore, and hyphen."
        )

    preview = (
        _invoke(adapter, "preview_admin_tools_build", version=version)
        if valid_version
        else None
    )
    preview_data = _mapping(preview or {})
    source_dirty = preview_data.get("source_dirty") is True
    if preview is not None:
        cols = st.columns(3)
        cols[0].metric("Git revision", str(preview_data.get("source_revision") or "-")[:12])
        cols[1].metric("Source dirty", str(source_dirty))
        cols[2].metric("Changed paths", str(preview_data.get("source_change_count", 0)))
        st.caption(f"Staging: {_project_relative_path(preview_data.get('staging_path', '-'))}")
        st.caption(f"Archive: {_project_relative_path(preview_data.get('archive_path', '-'))}")

    allow_dirty = False
    if source_dirty:
        st.warning(
            "Dirty source can produce a verification artifact only. It is non-promotable "
            "and cannot be pinned for DB administration execution."
        )
        allow_dirty = st.checkbox(
            "Allow dirty source for this Admin Tools verification build",
            key="admin-tools-build-allow-dirty",
        )
    if preview_data.get("output_exists") is True:
        st.error("This version already has staging or ZIP output. Choose a new version.")

    can_build = (
        valid_version
        and preview is not None
        and preview_data.get("build_allowed") is True
        and (not source_dirty or allow_dirty)
    )
    if st.button(
        "Build Admin Tools artifact",
        type="primary",
        disabled=not can_build,
        key="admin-tools-build-submit",
    ):
        with st.spinner("Building and validating local Admin Tools artifact..."):
            result = _invoke(
                adapter,
                "build_admin_tools_artifact",
                version=version,
                allow_dirty_source=allow_dirty,
            )
        if result is not None:
            build_receipt = dict(
                public_mapping(
                    result,
                    (
                        "status", "version", "source_revision", "source_dirty",
                        "source_mode", "promotable", "staging_path", "manifest_path",
                        "archive_path", "archive_sha256",
                    ),
                )
            )
            st.session_state["admin-tools-build-receipt"] = build_receipt
            completed_version = _safe_release_version(build_receipt.get("version")) or version
            st.session_state[next_version_key] = "admin-" + _next_build_version(completed_version)
            st.session_state[f"artifact-version-{environment}-admin-tools"] = completed_version
            st.rerun()


def _render_admin_tools_development_activation(
    adapter: BackendAdapter, artifacts: list[object]
) -> None:
    """Offer the newest remotely verified non-promotable release for Development only."""

    if not (
        adapter.has("preview_admin_tools_development_activation")
        and adapter.has("activate_admin_tools_development_release")
    ):
        return

    candidate: Mapping[str, Any] = {}
    for artifact in artifacts:
        version = _safe_release_version(_mapping(artifact).get("version") or _id(artifact))
        if not version:
            continue
        preview_errors: list[str] = []
        preview = _mapping(
            _invoke(
                adapter,
                "preview_admin_tools_development_activation",
                version=version,
                error_sink=preview_errors.append,
            )
            or {}
        )
        if (
            preview.get("status") == "ready"
            and preview.get("eligible") is True
            and preview.get("mode") == "development-verification"
            and _safe_release_version(preview.get("version")) == version
        ):
            candidate = {"version": version, "mode": "development-verification"}
            break

    pending_key = "admin-tools-development-activation-pending"
    notice_key = "admin-tools-development-activation-notice"
    notice = _mapping(st.session_state.get(notice_key) or {})
    if notice.get("status") == "activated":
        version = _safe_release_version(notice.get("version"))
        st.success(
            "Development DB verification execution version activated"
            + (f" ({version})." if version else ".")
        )
        st.warning(
            "Development verification only: this dirty/non-promotable Admin Tools release cannot be used for Production."
        )
    elif notice.get("status") == "failed":
        st.error(
            "Development DB activation did not complete. The backend did not change the configured execution version."
        )

    if not candidate:
        return
    version = str(candidate["version"])
    st.markdown("### Development verification execution")
    st.caption(f"Latest eligible verified non-promotable version: {version}")
    st.warning(
        "Development verification only. This release may execute Development DB tests, but Production requires a clean, promotable release."
    )
    pending = _mapping(st.session_state.get(pending_key) or {})
    if pending and pending.get("version") != version:
        st.session_state.pop(pending_key, None)
        pending = {}
    if pending:
        confirm, cancel = st.columns(2)
        if confirm.button(
            "Confirm Development DB activation",
            type="primary",
            key=f"admin-tools-development-activate-confirm-{version}",
        ):
            activation_errors: list[str] = []
            result = _mapping(
                _invoke(
                    adapter,
                    "activate_admin_tools_development_release",
                    version=version,
                    confirm=True,
                    error_sink=activation_errors.append,
                )
                or {}
            )
            st.session_state.pop(pending_key, None)
            if (
                result.get("status") == "activated"
                and result.get("mode") == "development-verification"
                and _safe_release_version(result.get("version")) == version
            ):
                st.session_state[notice_key] = {
                    "status": "activated",
                    "version": version,
                    "mode": "development-verification",
                }
            else:
                st.session_state[notice_key] = {"status": "failed"}
            st.rerun()
        if cancel.button(
            "Cancel Development DB activation",
            key=f"admin-tools-development-activate-cancel-{version}",
        ):
            st.session_state.pop(pending_key, None)
            st.rerun()
    elif st.button(
        "Use for Development DB",
        type="primary",
        key=f"admin-tools-development-activate-request-{version}",
    ):
        st.session_state.pop(notice_key, None)
        st.session_state[pending_key] = {
            "version": version,
            "mode": "development-verification",
        }
        st.rerun()


def _render_artifact_tab(
    adapter: BackendAdapter,
    environment: str,
    config_path: str,
    *,
    allowed_kinds: tuple[str, ...] | None = None,
) -> None:
    """Render a backend-validated artifact flow, optionally scoped to one area."""

    kinds = tuple(allowed_kinds or tuple(ARTIFACT_LABELS))
    st.subheader("Artifact deployment")
    if "runtime" in kinds:
        _render_build_artifact(adapter, environment)
    if "admin-tools" in kinds:
        _render_admin_tools_build(adapter, environment)
    if environment in {"development", "production"} and "runtime" in kinds:
        _render_secure_config(adapter, environment, config_path=config_path)
    st.markdown("### Select and upload artifact")
    artifact_kind_key = f"artifact-kind-{environment}"
    artifact_kind_query_key = f"deploy-kind-{environment}"
    if artifact_kind_key not in st.session_state:
        query_kind = _query_value(artifact_kind_query_key)
        if query_kind in kinds:
            st.session_state[artifact_kind_key] = query_kind
    if st.session_state.get(artifact_kind_key) not in kinds:
        st.session_state[artifact_kind_key] = kinds[0]
    kind = (
        kinds[0]
        if len(kinds) == 1
        else st.selectbox(
            "Artifact type",
            list(kinds),
            format_func=lambda item: ARTIFACT_LABELS[item],
            key=artifact_kind_key,
        )
    )
    _remember_query_value(artifact_kind_query_key, str(kind))
    if kind in {"server-data", "admin-tools"}:
        st.warning(
            "This artifact type is uploaded under the shared server root. "
            "It is not isolated by the development/production runtime directory."
        )
    if kind == "runtime":
        latest = _invoke(
            adapter, "resolve_latest_runtime_artifact", environment=environment
        )
        if latest is None:
            st.warning("No backend-validated runtime artifact is available.")
            return
        latest_data = _mapping(latest)
        latest_version = str(latest_data.get("version") or _id(latest))
        artifacts = _invoke(
            adapter, "list_artifacts", environment=environment, kind="runtime"
        ) or []
        by_version = {
            str(_mapping(candidate).get("version") or _id(candidate)): candidate
            for candidate in artifacts
            if str(_mapping(candidate).get("version") or _id(candidate))
        }
        # The resolver has fully validated the default. Preserve it even if a
        # lightweight list is briefly stale after a new build.
        by_version.setdefault(latest_version, latest)
        versions = list(by_version)
        handoff_key = f"runtime-build-select-{environment}"
        pending_version = str(st.session_state.pop(handoff_key, "") or "")
        selector_key = f"artifact-version-{environment}-runtime"
        selected_value = st.session_state.get(selector_key)
        if pending_version in by_version:
            st.session_state[selector_key] = pending_version
        elif selected_value not in by_version:
            st.session_state[selector_key] = latest_version
        selected_version = st.selectbox(
            "Artifact version",
            versions,
            key=selector_key,
        )
        artifact = by_version[str(selected_version)]
        st.caption(f"Newest runtime artifact: {latest_version}")
    else:
        artifacts = _invoke(adapter, "list_artifacts", environment=environment, kind=kind) or []
        if not artifacts:
            st.warning("No backend-validated artifacts are available for this selection.")
            return
        if kind == "admin-tools" and environment == "development":
            _render_admin_tools_development_activation(adapter, list(artifacts))
        artifact = _select_retained_artifact(
            list(artifacts),
            selector_key=f"artifact-version-{environment}-{kind}",
            query_key=f"deploy-version-{environment}-{kind}",
        )
        if artifact is None:
            st.warning("No backend-validated artifacts are available for this selection.")
            return
    artifact_data = _mapping(artifact)
    artifact_path = artifact_data.get("path") or artifact_data.get("archive_path")
    if not artifact_path:
        st.error("Artifact entry has no backend-provided path.")
        return
    inspection = _invoke(
        adapter,
        "inspect_artifact",
        path=str(artifact_path),
        kind=kind,
        environment=environment,
    )
    if inspection is None:
        return
    info = public_mapping(
        inspection,
        ("archive_sha256", "target_upload_path", "restricted_data"),
    )
    manifest = _mapping(inspection).get("manifest") or {}
    cols = st.columns(3)
    cols[0].metric("Target", str(info.get("target_upload_path", "-")))
    cols[1].metric("Source dirty", str(manifest.get("source_dirty", "unknown")))
    cols[2].metric("Promotable", str(manifest.get("promotable", "unknown")))
    with st.expander("Validated manifest", expanded=False):
        st.json(
            {
                key: manifest.get(key)
                for key in (
                    "package_name",
                    "artifact_type",
                    "created_at",
                    "source_revision",
                    "source_dirty",
                    "source_mode",
                    "target_environment",
                    "target_root",
                    "promotable",
                )
                if key in manifest
            }
        )
        st.caption(f"Archive SHA-256: {info.get('archive_sha256', '-')}")

    identifier = _id(artifact)
    files = safe_manifest_files(manifest)
    if info.get("restricted_data"):
        st.error(
            "Backend inspection found restricted content. Remote comparison and upload are blocked."
        )
        return
    policy = _invoke(
        adapter,
        "deployment_policy",
        environment=environment,
        config_path=config_path,
    )
    policy_data = _mapping(policy or {})
    allow_upload = policy_data.get("allow_upload") is True
    target_id = str(policy_data.get("target_id", "unavailable"))

    cache_key = (
        f"remote-diff:{environment}:{kind}:{identifier}:"
        f"{info.get('archive_sha256', '')}:{target_id}"
    )

    def fetch_diff() -> Any | None:
        return _invoke(
            adapter,
            "preview_remote_diff",
            inspection=inspection,
            selected_files=files,
            config_path=config_path,
        )

    refresh_diff_key = "refresh-remote-diff-" + hashlib.sha256(
        cache_key.encode("utf-8")
    ).hexdigest()[:16]
    preview = _load_remote_diff(
        st.session_state,
        cache_key,
        fetch_diff,
        force=st.session_state.pop(refresh_diff_key, False) is True,
    )
    local_rows, remote_rows = _diff_panels(preview)
    left, right = st.columns(2)
    with left:
        st.markdown("#### Local artifact files")
        local_panel = st.empty()
        local_panel.dataframe(local_rows, width="stretch", hide_index=True)
    with right:
        st.markdown("#### Server files")
        remote_panel = st.empty()
        remote_panel.dataframe(remote_rows, width="stretch", hide_index=True)
    if preview is None:
        st.warning("Remote diff is unavailable; upload remains blocked until validation succeeds.")

    st.markdown("### Actual upload")
    if allow_upload:
        st.success("Deployment policy: allow_upload is enabled for this local profile.")
    else:
        st.warning(
            "Deployment policy: allow_upload is disabled. Validation remains read-only. "
            f"To authorize an upload, review `{config_path}` and explicitly set "
            '`"allow_upload": true`.'
        )
    changed_files = _changed_upload_files(preview)
    upload_widget_key = "upload-files-" + hashlib.sha256(
        cache_key.encode("utf-8")
    ).hexdigest()[:16]
    widget_selection = st.session_state.get(upload_widget_key)
    if isinstance(widget_selection, list) and not set(widget_selection).issubset(
        changed_files
    ):
        st.session_state.pop(upload_widget_key, None)
    if changed_files:
        selected_files = st.multiselect(
            "Files to upload",
            changed_files,
            default=changed_files,
            key=upload_widget_key,
        )
    else:
        st.session_state.pop(upload_widget_key, None)
        selected_files = []
        if preview is not None:
            st.info("No changed files to upload")
    phrase = str(
        _mapping(inspection).get("required_confirmation")
        or expected_confirmation("DEPLOY", environment, identifier)
    )
    allowed = (
        allow_upload
        and preview is not None
        and bool(selected_files)
    )
    if info.get("restricted_data"):
        st.error("Backend inspection found restricted content; upload is blocked.")
        allowed = False
    if environment == "production" and (
        bool(manifest.get("source_dirty")) or manifest.get("promotable") is False
    ):
        st.error("A dirty or non-promotable artifact cannot be uploaded to production.")
        allowed = False
    intent = _upload_intent(
        environment=environment,
        kind=kind,
        artifact_id=identifier,
        archive_sha256=str(info.get("archive_sha256", "")),
        target_id=target_id,
        selected_files=selected_files,
    )
    scope_id = intent["scope_id"]
    pending_key = "pending-artifact-upload"
    inflight_key = "inflight-artifact-upload"
    notice_key = "artifact-upload-notice"
    # An active upload never reruns this script.  Therefore a marker found at
    # the start of a new render was left by an interrupted browser/session,
    # not by a live upload; it must not disable resuming the remaining files.
    st.session_state.pop(inflight_key, None)
    pending = _mapping(st.session_state.get(pending_key) or {})
    # Upload uses the reviewed artifact preview as its confirmation boundary;
    # do not require a second browser rerun/click for the same immutable intent.
    if pending:
        st.session_state.pop(pending_key, None)
        pending = {}
    notice = _mapping(st.session_state.get(notice_key) or {})
    if notice and notice.get("scope_id") != scope_id:
        st.session_state.pop(notice_key, None)
        notice = {}
    if notice.get("status") == "completed":
        release_text = (
            f" | release_id: {notice.get('release_id')}"
            if notice.get("release_id")
            else ""
        )
        st.success(
            f"Upload completed{release_text} | uploaded files: {notice.get('file_count', 0)}"
        )
        pin_message = _admin_tools_pin_message(
            notice.get("admin_tools_pin_status"),
            notice.get("admin_tools_pin_version"),
        )
        if pin_message is not None:
            level, message = pin_message
            getattr(st, level)(message)
            if notice.get("admin_tools_pin_status") == "pinned_development_verification":
                st.warning(
                    "Development verification only: this dirty/non-promotable Admin Tools release cannot be used for Production."
                )
    elif notice.get("status") == "failed":
        st.error(
            "Upload failed. "
            + str(notice.get("error_summary") or "Review the server diff and retry with Upload selected files.")
        )

    inflight = st.session_state.get(inflight_key) == intent["intent_id"]
    allowed = allowed and not inflight
    confirmation_area = st.empty()
    confirm = False
    cancel = False
    with confirmation_area.container():
        if changed_files:
            confirm = st.button(
                "Upload selected files",
                disabled=not allowed,
                type="primary",
                key=f"upload-request-{environment}-{kind}-{identifier}",
            )
            if confirm:
                st.session_state.pop(notice_key, None)

    if confirm and not inflight:
        confirmation_area.empty()
        st.session_state.pop(pending_key, None)
        st.session_state[inflight_key] = intent["intent_id"]
        file_count = len(selected_files)
        result: Any | None = None
        receipt: dict[str, Any] = {}
        upload_succeeded = False
        upload_errors: list[str] = []
        try:
            with st.status(
                f"Upload phase 0/{file_count}: submitting reviewed files to {environment}...",
                expanded=True,
            ) as progress:
                progress_bar = st.progress(0, text=f"Current/total: 0/{file_count}")

                def report_progress(completed: int, total: int, path: str, status: str) -> None:
                    safe_total = max(int(total or file_count), 1)
                    safe_completed = min(max(int(completed or 0), 0), safe_total)
                    progress_bar.progress(
                        safe_completed / safe_total,
                        text=f"Current/total: {safe_completed}/{safe_total} — {redact_text(path)[:160]} ({redact_text(status)[:40]})",
                    )

                result = _invoke(
                    adapter,
                    "upload_artifact",
                    inspection=inspection,
                    selected_files=selected_files,
                    config_path=config_path,
                    typed_confirmation=phrase,
                    dry_run=False,
                    progress_callback=report_progress,
                    error_sink=upload_errors.append,
                )
                receipt = _mapping(result or {})
                upload_succeeded = (
                    receipt.get("status") == "uploaded"
                    and bool(str(receipt.get("release_id") or "").strip())
                )
                if not upload_succeeded:
                    progress.update(label=f"Upload phase 0/{file_count}: failed", state="error")
                else:
                    progress.update(label=f"Upload phase {file_count}/{file_count}: completed", state="complete")
        finally:
            st.session_state.pop(inflight_key, None)

        if not upload_succeeded:
            st.session_state[notice_key] = {
                "scope_id": scope_id,
                "status": "failed",
                "file_count": file_count,
                "error_summary": _safe_upload_failure_summary(
                    upload_errors[-1] if upload_errors else None
                ),
            }
            # A backend may return a non-raising failure receipt after a partial
            # transport attempt. Always discard the cached comparison so the
            # next render shows the actual remaining server differences. If
            # this read also fails, the cache is removed and the rerun retries.
            _load_remote_diff(st.session_state, cache_key, fetch_diff, force=True)
        else:
            release_id = redact_text(str(receipt.get("release_id", "")))[:160]
            pin_data = _mapping(receipt.get("admin_tools_pin") or {})
            st.session_state[notice_key] = {
                "scope_id": scope_id,
                "status": "completed",
                "release_id": release_id,
                "file_count": file_count,
                # Only stable status and a constrained release version survive
                # the rerun; backend pin error/reason fields are never retained.
                "admin_tools_pin_status": (
                    str(pin_data.get("status") or "") if kind == "admin-tools" else ""
                ),
                "admin_tools_pin_version": (
                    _safe_release_version(pin_data.get("version"))
                    if kind == "admin-tools"
                    else ""
                ),
            }
            if (
                kind == "runtime"
                and receipt.get("status") == "uploaded"
                and receipt.get("release_id")
            ):
                st.session_state[f"deployed-{environment}"] = str(
                    receipt["release_id"]
                )
            _load_remote_diff(st.session_state, cache_key, fetch_diff, force=True)
            st.session_state.pop(upload_widget_key, None)
        st.rerun()


def _render_db_tab(adapter: BackendAdapter, environment: str, db_config_path: str) -> None:
    """Render the deliberately narrow db-admin/v1 console.

    The browser never chooses a SQL statement or table name.  Mutations are
    bound to a backend preview and require a second, separate click.
    """
    st.subheader("Database administration")
    st.warning("Writes use the selected target only, are delegated to the backend, and are transactional where supported.")

    if adapter.has("get_connection_settings"):
        settings_errors: list[str] = []
        settings = _mapping(
            _invoke(adapter, "get_connection_settings", error_sink=settings_errors.append)
            or {}
        )
        connection = _mapping(settings.get("connection") or {})
        common_pinned_version = _safe_release_version(
            connection.get("admin_tools_release_version")
        )
        common_ready = (
            connection.get("admin_tools_release_configured") is True
            and bool(common_pinned_version)
        )
        development_pinned_version = _safe_release_version(
            connection.get("admin_tools_development_release_version")
        )
        development_mode = str(
            connection.get("admin_tools_development_release_mode") or ""
        )
        development_ready = (
            environment == "development"
            and connection.get("admin_tools_development_release_configured") is True
            and bool(development_pinned_version)
            and development_mode in {"clean", "development-verification"}
        )
        if not (common_ready or development_ready):
            st.warning(
                "Database administration is locked. Build and upload an Admin Tools artifact, "
                "then select it as the execution version. Development may use an explicitly "
                "activated verification release; Production requires a clean, promotable release."
            )
            return
        if development_ready and development_mode == "development-verification":
            st.caption(
                f"Development verification execution version: {development_pinned_version}"
            )
            st.warning(
                "Development verification only: this dirty/non-promotable Admin Tools release cannot be used for Production."
            )
        else:
            st.caption(f"Clean Admin Tools execution version: {common_pinned_version}")

    overview_errors: list[str] = []
    overview = _invoke(
        adapter,
        "get_database_overview",
        environment=environment,
        error_sink=overview_errors.append,
    )
    if overview is None:
        error_text = " ".join(overview_errors).lower()
        if any(term in error_text for term in ("admin tools", "admin_tools", "pinned", "promotable")):
            st.warning(
                "Database administration is locked. Activate a verified Admin Tools release for "
                "Development, or build and upload a clean, promotable release for Production."
            )
        else:
            st.error("Database overview is unavailable. DB write controls are disabled.")
        return
    overview_data = _mapping(overview or {})
    overview_error_code = str(
        overview_data.get("error_code")
        or overview_data.get("admin_tools_release_status")
        or ""
    ).lower()
    if any(term in overview_error_code for term in ("admin_tools", "pin", "promotable")):
        st.warning(
            "Database administration is locked. Activate a verified Admin Tools release for "
            "Development, or build and upload a clean, promotable release for Production."
        )
        return
    target = _mapping(overview_data.get("target") or {})
    target_db = target.get("dbname") or overview_data.get("dbname") or overview_data.get("database") or "unavailable"
    target_env = target.get("environment") or overview_data.get("environment") or environment
    st.caption(f"Target database: {target_db} | environment: {target_env}")
    database_available = overview is not None and str(overview_data.get("status", "")).lower() not in {"unavailable", "error"}
    if not database_available:
        st.error("Database overview is unavailable. No DB action can be confirmed from this screen.")
    else:
        table_rows = overview_data.get("tables") or overview_data.get("table_overview") or []
        if table_rows:
            st.markdown("#### Database overview")
            st.dataframe(
                [
                    public_mapping(
                        row,
                        ("table", "table_name", "row_count", "exists", "schema_status", "primary_key", "write_capability", "write_allowed"),
                    )
                    for row in table_rows
                ],
                width="stretch",
            )
            st.caption(f"{len(table_rows)} tables returned by db-admin/v1 (expected operational inventory: 13).")
        else:
            st.info("No table overview was returned by the database adapter.")

    st.divider()
    st.markdown("#### Migrations")
    migration_state = _mapping(overview_data.get("migration") or {})
    if migration_state:
        st.json(
            public_mapping(
                migration_state,
                ("registry_status", "registered_count", "applied_count", "pending_count", "history_status", "legacy_unversioned_schema"),
            )
        )
        if migration_state.get("legacy_unversioned_schema") is True:
            st.warning("Legacy unversioned schema detected. Its migration state is not silently treated as current.")
    migrations = _invoke(
        adapter,
        "list_migrations",
        environment=environment,
        config_path=db_config_path,
    ) or []
    if migrations:
        st.dataframe(
            [
                public_mapping(
                    item,
                    ("migration_id", "id", "description", "checksum_sha256", "checksum", "status", "statement_count", "rollback_instructions", "reversible"),
                )
                for item in migrations
            ],
            width="stretch",
        )
        selected = st.selectbox("Migration", migrations, format_func=_label)
        migration_id = _id(selected)
        preview = _invoke(
            adapter,
            "preview_migration",
            environment=environment,
            migration_id=migration_id,
            config_path=db_config_path,
        )
        if preview is not None:
            preview_data = _mapping(preview)
            plan_data = _mapping(preview_data.get("plan") or {})
            sql = str(preview_data.get("sql") or preview_data.get("sql_preview") or "")
            st.code(sql, language="sql")
            statement_types = list(plan_data.get("statement_types") or ())
            statements = list(preview_data.get("statements") or ())
            classifications = (
                [
                    {
                        "statement": str(index + 1),
                        "category": statement_type,
                        "preview": " ".join(str(statements[index]).split())[:160]
                        if index < len(statements)
                        else "",
                    }
                    for index, statement_type in enumerate(statement_types)
                ]
                or classify_sql(sql)
            )
            st.dataframe(classifications, width="stretch")
            phrase = str(
                preview_data.get("required_confirmation")
                or plan_data.get("required_confirmation")
                or _mapping(selected).get("required_confirmation")
                or expected_confirmation("MIGRATE", environment, migration_id)
            )
            dry_run = st.checkbox(
                "Migration dry-run", value=True, key=f"migration-dry-{environment}-{migration_id}"
            )
            intent = {
                "environment": environment,
                "migration_id": migration_id,
                "checksum": str(plan_data.get("checksum_sha256") or preview_data.get("checksum_sha256") or ""),
                "dry_run": dry_run,
                "phrase": phrase,
            }
            intent_key = f"migration-intent-{environment}"
            if st.button("Review execution", key=f"migration-review-{environment}-{migration_id}"):
                st.session_state[intent_key] = intent
                st.rerun()
            pending = _mapping(st.session_state.get(intent_key) or {})
            if pending == intent:
                st.warning(
                    f"Review complete: {migration_id} targets {target_db}; "
                    f"{len(classifications)} statement(s), dry-run={dry_run}."
                )
                left, right = st.columns(2)
                if left.button("Confirm migration", type="primary", key=f"migration-apply-{environment}-{migration_id}"):
                    result = _invoke(
                        adapter,
                        "execute_migration",
                        environment=environment,
                        migration_id=migration_id,
                        config_path=db_config_path,
                        typed_confirmation=phrase,
                        dry_run=dry_run,
                    )
                    st.session_state.pop(intent_key, None)
                    if result is not None:
                        st.success(_result_message(result))
                if right.button("Cancel migration", key=f"migration-cancel-{environment}-{migration_id}"):
                    st.session_state.pop(intent_key, None)
                    st.rerun()
    else:
        registry_status = str(migration_state.get("registry_status") or "").lower()
        registry_exists = overview_data.get("migration_registry_exists")
        if registry_exists is False or registry_status in {"missing", "absent", "unavailable"}:
            st.warning("Migration registry is not present for this target; migration status cannot be established.")
        elif migration_state.get("legacy_unversioned_schema") is True:
            st.warning("No pending migrations were returned, but the legacy unversioned schema still requires review.")
        else:
            st.success("Migration registry is up to date; no pending migrations were returned.")

def _render_master_csv_admin(
    adapter: BackendAdapter, environment: str, db_config_path: str, database_available: bool
) -> None:
    """Render only backend-allowlisted master CSV upserts; never retain CSV text."""
    st.markdown("#### Master CSV upsert")
    specs_result = _invoke(adapter, "list_master_table_specs", environment=environment)
    specs_payload = _mapping(specs_result or {})
    specs = (
        specs_payload.get("specs")
        or specs_payload.get("tables")
        or specs_result
        or []
    )
    if not isinstance(specs, list):
        specs = []
    if not specs:
        st.info("No master-table write specifications are available for this target.")
        return
    def is_write_allowed(item: object) -> bool:
        data = _mapping(item)
        capability = _mapping(data.get("write_capability") or {})
        return data.get("write_allowed") is True or capability.get("allowed") is True

    allowed = [item for item in specs if is_write_allowed(item)]
    denied = [item for item in specs if not is_write_allowed(item)]
    if denied:
        st.caption("Read-only transactional or non-allowlisted tables: " + ", ".join(_label(item) for item in denied))
    if not allowed:
        st.warning("All returned tables are read-only; CSV writes are disabled.")
        return
    selected = st.selectbox("Allowlisted master table", allowed, format_func=_label, key=f"master-table-{environment}")
    spec = _mapping(selected)
    table_id = _id(selected) or str(spec.get("table_name") or "")
    st.json(public_mapping(spec, ("table_name", "description", "required_columns", "primary_key", "row_limit", "file_size_limit", "write_allowed")))
    uploaded = st.file_uploader("Master CSV", type=["csv"], key=f"master-csv-{environment}-{table_id}")
    if uploaded is None:
        return
    st.caption(f"Current/total: 1/1 selected — {redact_text(uploaded.name)[:160]}. Validation occurs before any database write.")
    csv_bytes = uploaded.getvalue()
    csv_digest = hashlib.sha256(csv_bytes).hexdigest()
    preview_key = f"master-preview-{environment}"
    if st.button("Validate CSV", disabled=not database_available, key=f"master-validate-{environment}-{table_id}"):
        preview = _invoke(
            adapter,
            "preview_master_csv_upsert",
            environment=environment,
            table_id=table_id,
            file_name=uploaded.name,
            csv_bytes=csv_bytes,
        )
        if preview is not None:
            data = _mapping(preview)
            st.session_state[preview_key] = {
                "table_id": table_id,
                "csv_digest": csv_digest,
                "preview_id": data.get("preview_id"),
                "preview_digest": data.get("preview_digest") or data.get("digest"),
                # Stable across reruns/timeouts; the backend uses this UUID as
                # both idempotency key and operation receipt identifier.
                "idempotency_key": str(uuid.uuid4()),
                "data": public_mapping(data, ("status", "errors", "create_count", "update_count", "unchanged_count", "masked_samples", "preview_id", "preview_digest", "digest", "expires_at")),
            }
            st.rerun()
    preview = _mapping(st.session_state.get(preview_key) or {})
    if preview.get("table_id") != table_id or preview.get("csv_digest") != csv_digest:
        return
    preview_data = _mapping(preview.get("data") or {})
    st.info("Validated preview; CSV contents are not persisted in the URL or UI log.")
    st.json(preview_data)
    valid = not bool(preview_data.get("errors")) and bool(preview.get("preview_id")) and bool(preview.get("preview_digest"))
    operation_id = str(preview.get("idempotency_key") or "")
    if operation_id and st.button(
        "Refresh operation receipt",
        disabled=not database_available,
        key=f"master-receipt-{environment}-{table_id}",
    ):
        receipt = _invoke(
            adapter,
            "get_master_change_receipt",
            environment=environment,
            operation_id=operation_id,
        )
        if receipt is not None:
            receipt_status = str(_mapping(receipt).get("status") or "").lower()
            if receipt_status in {"applied", "already_applied"}:
                st.session_state.pop(preview_key, None)
                st.success(_result_message(receipt))
            elif receipt_status == "pending":
                st.warning("DB operation is still pending.")
            else:
                st.error("DB operation failed or no longer matches the preview.")
            st.json(public_mapping(receipt, ("operation_id", "status", "error_code", "table_name", "create_count", "update_count", "unchanged_count", "applied_at")))
    if st.button("Confirm apply CSV", type="primary", disabled=not valid or not database_available, key=f"master-apply-{environment}-{table_id}"):
        result = _invoke(
            adapter,
            "apply_master_csv_upsert",
            environment=environment,
            preview_id=str(preview.get("preview_id")),
            preview_digest=str(preview.get("preview_digest")),
            idempotency_key=operation_id,
            confirm=True,
        )
        if result is not None:
            receipt_data = _mapping(result)
            apply_status = str(receipt_data.get("status") or "").lower()
            if apply_status not in {"applied", "already_applied"}:
                if apply_status == "pending":
                    st.warning("DB operation is still pending. Refresh the receipt before retrying.")
                else:
                    st.error(
                        "DB operation was not applied. The validated preview is retained so it can be reviewed or retried."
                    )
                st.json(public_mapping(receipt_data, ("status", "error_code", "operation_id", "message")))
                return
            receipt_id = receipt_data.get("operation_id") or receipt_data.get("receipt_id") or receipt_data.get("id")
            receipt = _invoke(
                adapter,
                "get_master_change_receipt",
                environment=environment,
                operation_id=receipt_id,
            ) if receipt_id else result
            final_data = _mapping(receipt or result)
            final_status = str(final_data.get("status") or "").lower()
            if final_status in {"applied", "already_applied"}:
                st.session_state.pop(preview_key, None)
                st.success(_result_message(receipt or result))
            elif final_status == "pending":
                st.warning("DB operation is still pending. The preview has not been cleared.")
            else:
                st.error("DB operation failed or became stale. The preview has not been cleared.")
            st.json(public_mapping(receipt or result, ("operation_id", "receipt_id", "status", "error_code", "table_name", "create_count", "update_count", "unchanged_count", "digest", "applied_at")))


def _render_service_controls(
    adapter: BackendAdapter,
    environment: str,
    config_path: str,
    allowed_services: list[str],
) -> None:
    st.markdown(f"#### {environment.title()}")
    selected_input = st.multiselect(
        "Allowlisted services",
        allowed_services,
        default=allowed_services,
        key=f"service-units-{environment}",
    )
    selected_set = set(selected_input)
    selected = [unit for unit in allowed_services if unit in selected_set]
    action = st.radio(
        "Action",
        ("start", "restart"),
        horizontal=True,
        key=f"service-action-{environment}",
    )
    history = _invoke(
        adapter, "list_history", environment=environment, kind="runtime"
    ) or []
    uploaded_releases = [
        item
        for item in history
        if _mapping(item).get("status") == "uploaded"
        and _mapping(item).get("service_eligible") is True
        and _mapping(item).get("complete_manifest") is True
    ]
    latest_release = uploaded_releases[0] if uploaded_releases else None
    release_id = _id(latest_release) if latest_release is not None else ""
    st.caption(
        "Start/restart requires the latest successful non-dry-run runtime deployment."
    )
    if release_id:
        st.caption(f"Runtime release: {release_id}")
    else:
        st.warning("No eligible runtime deployment history was found for this environment.")
    identifier = f"{action}:{release_id}:{','.join(selected)}"
    phrase = expected_confirmation(
        action, environment, f"{release_id} {','.join(selected)}"
    )
    action_id = hashlib.sha256(identifier.encode("utf-8")).hexdigest()
    inflight_key = f"inflight-service-action-{environment}"
    notice_key = f"service-action-notice-{environment}"
    inflight = st.session_state.get(inflight_key) == action_id
    notice = _mapping(st.session_state.get(notice_key) or {})
    if notice and notice.get("action_id") != action_id:
        st.session_state.pop(notice_key, None)
        notice = {}
    if notice.get("status") == "completed":
        st.success(
            f"{str(notice.get('action', action)).title()} completed | "
            f"services: {notice.get('service_count', 0)}"
        )
    elif notice.get("status") == "failed":
        st.error(f"{action.title()} failed. Review service status and try again.")

    action_area = st.empty()
    with action_area.container():
        clicked = st.button(
            action.title(),
            disabled=not (release_id and selected) or inflight,
            key=f"service-{environment}-{identifier}",
            type="primary",
        )
    if clicked and not inflight:
        action_area.empty()
        st.session_state.pop(notice_key, None)
        st.session_state[inflight_key] = action_id
        report: Any | None = None
        progress_label = "Starting" if action == "start" else "Restarting"
        try:
            with st.status(
                f"{progress_label} {len(selected)} services in {environment}...",
                expanded=True,
            ) as progress:
                report = _invoke(
                    adapter,
                    "run_service_action",
                    environment=environment,
                    action=action,
                    units=selected,
                    release_id=release_id,
                    config_path=config_path,
                    typed_confirmation=phrase,
                )
                if report is None:
                    progress.update(label=f"{action.title()} failed", state="error")
                else:
                    progress.update(label=f"{action.title()} completed", state="complete")
        finally:
            st.session_state.pop(inflight_key, None)
        st.session_state[notice_key] = {
            "action_id": action_id,
            "status": "completed" if report is not None else "failed",
            "action": action,
            "service_count": len(selected),
        }
        st.rerun()


def _render_monitor_tab(adapter: BackendAdapter, config_path: str) -> None:
    st.subheader("Server monitoring")
    st.caption(
        "Production, development, and shared OSRM are checked through read-only "
        "systemd status, health endpoints, and journal queries."
    )
    auto_refresh = st.toggle("Auto refresh every 10 seconds", value=False)

    @st.fragment(run_every=10 if auto_refresh else None)
    def _snapshot() -> None:
        st.button("Refresh now", key="monitor-refresh")
        report = _invoke(adapter, "observe_platform", config_path=config_path)
        if report is None:
            return
        payload = _mapping(report)
        services = payload.get("services", [])
        rows = [
            public_mapping(
                item,
                (
                    "scope",
                    "component",
                    "unit",
                    "health_endpoint",
                    "port",
                    "active",
                    "enabled",
                    "health_ok",
                    "status",
                ),
            )
            for item in services
        ]
        total = int(payload.get("total", len(rows)) or 0)
        healthy = int(payload.get("healthy", 0) or 0)
        cols = st.columns(3)
        cols[0].metric("Healthy", f"{healthy}/{total}")
        cols[1].metric("Unhealthy", max(total - healthy, 0))
        cols[2].metric("Checked (UTC)", str(payload.get("checked_at", "-")))
        if rows:
            st.dataframe(rows, width="stretch", hide_index=True)
        unhealthy = [item for item in services if _mapping(item).get("status") != "healthy"]
        if unhealthy:
            st.error("One or more processes or health endpoints are unavailable.")
            for item in unhealthy:
                data = _mapping(item)
                with st.expander(
                    f"{data.get('scope', '-')} / {data.get('component', data.get('unit', '-'))}"
                ):
                    journal = redact_text(data.get("journal_tail", ""))
                    if journal:
                        st.code(journal, language="text")
                    else:
                        st.caption("No readable journal output was returned.")
        elif rows:
            st.success("All monitored processes and health endpoints are available.")

        st.divider()
        st.subheader("Service controls")
        production_column, development_column = st.columns(2)
        for environment, column in (
            ("production", production_column),
            ("development", development_column),
        ):
            units = [
                str(_mapping(item).get("unit"))
                for item in services
                if _mapping(item).get("scope") == environment
                and _mapping(item).get("unit")
            ]
            with column:
                _render_service_controls(
                    adapter,
                    environment,
                    config_path,
                    list(dict.fromkeys(units)),
                )

    _snapshot()


def _render_history_tab(adapter: BackendAdapter, environment: str, config_path: str) -> None:
    st.subheader("Deployment history and rollback")
    kind = st.selectbox(
        "History artifact type",
        list(ARTIFACT_LABELS),
        format_func=lambda item: ARTIFACT_LABELS[item],
        key="history-kind",
    )
    history = _invoke(adapter, "list_history", environment=environment, kind=kind) or []
    if not history:
        st.caption("No history returned by the backend.")
        return
    st.dataframe(
        [
            public_mapping(
                item,
                ("id", "version", "created_at", "status", "sha256", "operator", "rollback_available"),
            )
            for item in history
        ],
        width="stretch",
    )
    release = st.selectbox("Rollback target", history, format_func=_label)
    release_id = _id(release)
    phrase = str(
        _mapping(release).get("required_confirmation")
        or expected_confirmation("ROLLBACK", environment, release_id)
    )
    st.code(phrase)
    typed = st.text_input(
        "Type the rollback phrase exactly",
        type="password",
        key=f"rollback-confirm-{environment}-{kind}-{release_id}",
    )
    if st.button(
        "Rollback",
        disabled=not confirmation_matches(typed, phrase),
        key=f"rollback-{environment}-{kind}-{release_id}",
    ):
        result = _invoke(
            adapter,
            "rollback_release",
            environment=environment,
            kind=kind,
            release_id=release_id,
            config_path=config_path,
            typed_confirmation=typed,
        )
        if result is not None:
            st.success(_result_message(result))


def _render_dashboard(adapter: BackendAdapter, config_path: str) -> None:
    """Read-only operational summary; detailed controls remain under Monitoring."""

    st.caption("Read-only service health summary. Use Monitoring for journal details and service controls.")
    report = _invoke(adapter, "observe_platform", config_path=config_path)
    if report is None:
        st.error("Monitoring data is unavailable. No health state is inferred.")
        return
    payload = _mapping(report)
    services = payload.get("services") if isinstance(payload.get("services"), list) else []
    total = int(payload.get("total", len(services)) or 0)
    healthy = int(payload.get("healthy", 0) or 0)
    unhealthy = max(total - healthy, 0)
    production = sum(1 for item in services if _mapping(item).get("scope") == "production")
    cards = st.columns(4)
    cards[0].metric("Healthy services", f"{healthy}/{total}")
    cards[1].metric("Needs attention", unhealthy)
    cards[2].metric("Production units", production)
    cards[3].metric("Checked (UTC)", str(payload.get("checked_at") or "-"))
    if unhealthy:
        st.error("One or more services need attention. Open Monitoring for the affected units and safe controls.")
    elif total:
        st.success("All returned health checks are healthy.")
    else:
        st.info("No monitored services were returned by the backend.")


def _managed_data_preview_payload(value: object) -> dict[str, Any]:
    """Retain only the versioned, backend-safe managed-data preview contract."""

    data = _mapping(value)
    return dict(
        public_mapping(
            data,
            (
                "status", "scope", "dataset_id", "file_name", "size_bytes",
                "sha256", "version", "summary", "sample", "row_count",
                "create_count", "update_count", "unchanged_count", "warnings",
                "db_sync_supported", "target_environment", "production_allowed",
                "preview_id", "preview_digest", "type", "tables", "pii_redacted",
                "file_type", "secret_redacted", "canonical_sha256",
                "canonical_row_count", "masked_samples", "normalization", "errors",
                "expires_at", "operation_id", "created_at", "completed_at",
                "plan_id", "region_mapping_source",
                "technician_create_count", "technician_update_count",
                "technician_unchanged_count", "capability_create_count",
                "capability_update_count", "capability_unchanged_count",
                "capability_delete_count",
                "region_mapping_create_count", "region_mapping_update_count",
                "region_mapping_unchanged_count", "rejected_count", "error_count",
            ),
        )
    )


def _managed_dataset_id(dataset: Mapping[str, Any]) -> str:
    return str(dataset.get("dataset_id") or dataset.get("id") or "")


def _managed_dataset_db_supported(dataset: Mapping[str, Any]) -> bool:
    if dataset.get("db_sync_supported") is True:
        return True
    targets = dataset.get("allowed_targets")
    return bool(dataset.get("db_profile")) and isinstance(targets, list) and bool(targets)


def _managed_dataset_ui_metadata(dataset: Mapping[str, Any]) -> dict[str, Any]:
    """Apply presentation-only grouping without changing registry source meaning."""

    data = dict(dataset)
    dataset_id = _managed_dataset_id(data)
    hidden_inputs = {"service_raw", "service_geocoded", "map_debug", "map_debug_input"}
    if dataset_id in hidden_inputs:
        data["ui_hidden"] = True
        return data
    # Raw workbooks are legacy inputs.  The normal Region Data lane consumes
    # an immutable, already-resolved candidate ZIP bundle instead.
    if dataset_id == "territory_plan_workbook" and data.get("ui_legacy_visible") is not True:
        data["ui_hidden"] = True
        return data
    if dataset_id == "technician_data_workbook":
        data["label"] = "Technician workbook (Address / Product / Region)"
        data["ui_role"] = "source_workbook"
    elif dataset_id == "technician_profile_workbook":
        data["label"] = "Legacy technician profile (upload only)"
        data["ui_role"] = "source_workbook"
    elif dataset_id == "profile_raw":
        data["label"] = "Technician profile workbook (source)"
        data["ui_role"] = "source_workbook"
    elif dataset_id in {"atlanta_engineer_home", "atlanta_engineer_region"}:
        data["label"] = (
            "Engineer home projection" if dataset_id.endswith("_home")
            else "Engineer region projection"
        )
        data["ui_role"] = "derived_projection"
        data["ui_upload_allowed"] = False
    elif dataset_id == "territory_plan_workbook":
        data["label"] = "Legacy region workbook"
        data["ui_role"] = "legacy_region_workbook"
    elif dataset_id == "fixed_region_plan_bundle":
        data["label"] = "Canonical fixed region DB-input bundle (ZIP)"
        data["ui_role"] = "fixed_region_bundle"
    elif "territory" in dataset_id or "region_plan" in dataset_id:
        data["ui_role"] = "candidate_lifecycle"
    elif str(data.get("scope") or "").lower() == "common":
        data["ui_role"] = "reference_common"
    return data


def _managed_dataset_section(dataset: Mapping[str, Any]) -> str:
    """Return the primary operational section for one backend dataset."""

    dataset_id = _managed_dataset_id(dataset).lower()
    role = str(dataset.get("ui_role") or "").lower()
    if "technician" in dataset_id or dataset_id in {
        "profile_raw", "atlanta_engineer_home", "atlanta_engineer_region",
    } or role in {"source_workbook", "derived_projection"}:
        return "technician"
    if (
        "territory" in dataset_id or "region_plan" in dataset_id
        or role in {"candidate_lifecycle", "fixed_region_bundle", "legacy_region_workbook"}
    ):
        return "region"
    return "other"


def _render_active_region_binding(dataset: Mapping[str, Any]) -> None:
    """Show only non-personal active-region binding metadata when supplied by API."""

    binding = _mapping(
        dataset.get("active_region_binding")
        or dataset.get("region_binding")
        or {}
    )
    if not binding:
        binding = {
            key: dataset[key]
            for key in (
                "active_region_plan_id", "active_region_plan_version",
                "active_region_version", "region_binding_status",
            )
            if dataset.get(key) not in (None, "")
        }
    safe_binding = public_mapping(
        binding,
        (
            "status", "binding_status", "plan_id", "plan_version", "version",
            "region_plan_id", "region_plan_version", "active_region_plan_id",
            "active_region_plan_version", "active_region_version", "source_version",
        ),
    )
    if safe_binding:
        st.markdown("#### Active region binding")
        st.caption(
            "Read-only binding returned by the backend contract. This screen cannot assign technicians directly."
        )
        st.json(safe_binding)
    else:
        st.info(
            "No active-region binding is exposed for this technician dataset by the current backend contract."
        )


def _render_managed_data_preview(data: Mapping[str, Any]) -> None:
    summary = public_mapping(
        data,
        (
            "status", "scope", "dataset_id", "file_name", "size_bytes", "version",
            "row_count", "create_count", "update_count", "unchanged_count",
            "target_environment", "warnings", "plan_id", "region_mapping_source",
            "technician_create_count", "technician_update_count",
            "technician_unchanged_count", "capability_create_count",
            "capability_update_count", "capability_unchanged_count",
            "capability_delete_count",
            "region_mapping_create_count", "region_mapping_update_count",
            "region_mapping_unchanged_count", "rejected_count", "error_count",
        ),
    )
    if summary:
        st.json(summary)
    checksum = str(data.get("sha256") or "")
    if checksum:
        st.caption(f"SHA-256: {_short_checksum(checksum)}…")
    sample = data.get("sample")
    if isinstance(sample, list) and sample:
        st.dataframe(sample, width="stretch", hide_index=True)
    tables = data.get("tables")
    if isinstance(tables, list) and tables:
        st.dataframe(tables, width="stretch", hide_index=True)
    masked_samples = data.get("masked_samples")
    if isinstance(masked_samples, list) and masked_samples:
        st.dataframe(masked_samples, width="stretch", hide_index=True)
    normalization = data.get("normalization")
    if isinstance(normalization, Mapping) and normalization:
        st.json(dict(normalization))
    errors = data.get("errors")
    if isinstance(errors, list) and errors:
        st.markdown("#### Validation summary")
        st.dataframe(errors, width="stretch", hide_index=True)


def _render_managed_data_file_section(
    adapter: BackendAdapter,
    *,
    scope: str,
    dataset: Mapping[str, Any],
    config_path: str,
) -> str:
    dataset_id = _managed_dataset_id(dataset)
    st.markdown("### File")
    st.caption("Preview and upload only the selected backend-allowlisted dataset.")

    versions: list[object] = []
    if adapter.has("list_managed_data_versions"):
        version_errors: list[str] = []
        result = _mapping(
            _invoke(
                adapter,
                "list_managed_data_versions",
                scope=scope,
                dataset_id=dataset_id,
                error_sink=version_errors.append,
            )
            or {}
        )
        raw_versions = result.get("versions")
        versions = raw_versions if isinstance(raw_versions, list) else []
    if versions:
        rows = []
        for item in versions:
            data = _mapping(item)
            rows.append(
                {
                    "version": _safe_release_version(data.get("version")) or "-",
                    "checksum": _short_checksum(data.get("sha256")),
                    "rows": data.get("row_count"),
                    "updated": data.get("updated_at") or data.get("created_at"),
                    "status": data.get("status"),
                }
            )
        st.dataframe(rows, width="stretch", hide_index=True)
        version_values = [
            _safe_release_version(_mapping(item).get("version"))
            for item in versions
        ]
        version_values = [value for value in version_values if value]
    else:
        version_values = []
        st.caption("No managed file versions are available yet.")

    selected_version = ""
    if version_values:
        selected_version = str(
            st.selectbox(
                "Managed file version",
                version_values,
                key=f"managed-data-version-{scope}-{dataset_id}",
            )
        )
        if adapter.has("preview_managed_data_version") and st.button(
            "View version preview",
            key=f"managed-data-view-{scope}-{dataset_id}-{selected_version}",
        ):
            preview_errors: list[str] = []
            preview = _invoke(
                adapter,
                "preview_managed_data_version",
                scope=scope,
                dataset_id=dataset_id,
                version=selected_version,
                error_sink=preview_errors.append,
            )
            if preview is not None:
                st.session_state[f"managed-data-view-result-{scope}-{dataset_id}"] = (
                    _managed_data_preview_payload(preview)
                )
        viewed = _mapping(
            st.session_state.get(f"managed-data-view-result-{scope}-{dataset_id}")
            or {}
        )
        if viewed and viewed.get("version") == selected_version:
            _render_managed_data_preview(viewed)

    if dataset.get("ui_upload_allowed") is False:
        st.info(
            "Derived projection: this dataset is displayed from an approved source workflow and is not a separate source upload."
        )
        return selected_version

    allowed_types = dataset.get("allowed_file_types") or dataset.get("extensions")
    uploader_types = (
        [str(item).lower().lstrip(".") for item in allowed_types if str(item).strip()]
        if isinstance(allowed_types, list)
        else None
    )
    if not adapter.has("preview_managed_data_upload"):
        st.info("Managed file upload is unavailable from this backend version.")
        return selected_version
    uploaded = st.file_uploader(
        "Managed data file",
        type=uploader_types,
        key=f"managed-data-upload-{scope}-{dataset_id}",
    )
    preview_key = f"managed-data-upload-preview-{scope}-{dataset_id}"
    pending_key = f"managed-data-upload-pending-{scope}-{dataset_id}"
    notice_key = f"managed-data-upload-notice-{scope}-{dataset_id}"
    if uploaded is None:
        return selected_version
    file_bytes = uploaded.getvalue()
    digest = hashlib.sha256(file_bytes).hexdigest()
    st.caption(f"Current/total: 1/1 selected — {redact_text(uploaded.name)[:160]}")
    retained = _mapping(st.session_state.get(preview_key) or {})
    if retained and retained.get("bound_sha256") != digest:
        st.session_state.pop(preview_key, None)
        st.session_state.pop(pending_key, None)
        retained = {}
    if st.button(
        "Validate and upload managed data",
        type="primary",
        disabled=not adapter.has("start_managed_data_upload_job"),
        key=f"managed-data-apply-{scope}-{dataset_id}-{digest}",
    ):
        preview_errors: list[str] = []
        preview = _invoke(
            adapter,
            "preview_managed_data_upload",
            scope=scope,
            dataset_id=dataset_id,
            file_name=uploaded.name,
            file_bytes=file_bytes,
            error_sink=preview_errors.append,
        )
        if preview is None:
            st.session_state[notice_key] = {
                "status": "failed",
                "message": "Managed file validation did not complete.",
            }
        else:
            safe_preview = _managed_data_preview_payload(preview)
            safe_preview["bound_sha256"] = digest
            st.session_state[preview_key] = safe_preview
            if safe_preview.get("status") != "ready" or safe_preview.get("sha256") != digest:
                st.session_state[notice_key] = {"status": "failed", "message": "Managed file validation did not complete."}
            else:
                upload_errors: list[str] = []
                result = _invoke(
                    adapter,
                    "start_managed_data_upload_job",
                    scope=scope,
                    dataset_id=dataset_id,
                    file_name=uploaded.name,
                    file_bytes=file_bytes,
                    expected_sha256=digest,
                    confirm=True,
                    config_path=config_path,
                    error_sink=upload_errors.append,
                )
                result_data = _mapping(result or {})
                succeeded = result_data.get("status") in {"uploaded", "already_exists"}
                queued = result_data.get("status") == "queued" and result_data.get("job_id")
                if queued:
                    st.session_state[f"managed-data-upload-job-{scope}-{dataset_id}"] = str(result_data["job_id"])
                    st.session_state[notice_key] = {"status": "queued", "job_id": str(result_data["job_id"])}
                    succeeded = False
                if succeeded:
                    st.session_state[notice_key] = {"status": str(result_data.get("status")), "version": _safe_release_version(result_data.get("version"))}
                    candidate_workflow = _mapping(result_data.get("candidate_workflow") or {})
                    if dataset_id in {"territory_plan_workbook", "fixed_region_plan_bundle"} and candidate_workflow:
                        st.session_state[f"region-plan-candidate-summary-{scope}-{dataset_id}"] = dict(public_mapping(candidate_workflow, ("status", "plan_id", "lifecycle_stage", "approval_status", "candidate_version", "artifact_sha256", "created_at", "promotable", "promotion_required", "direct_db_upsert", "membership_input_rows", "membership_accepted_rows", "membership_rejected_rows", "unique_postal_count", "ambiguous_postal_count", "technician_input_rows", "technician_accepted_rows", "technician_rejected_rows")))
                    st.session_state.pop(preview_key, None)
                elif not queued:
                    st.session_state[notice_key] = {"status": "failed", "message": "Managed data upload did not complete. Review the validated file and retry."}
        st.rerun()
    retained = _mapping(st.session_state.get(preview_key) or {})
    if retained and retained.get("bound_sha256") == digest:
        _render_managed_data_preview(retained)
    notice = _mapping(st.session_state.get(notice_key) or {})
    job_key = f"managed-data-upload-job-{scope}-{dataset_id}"
    job_id = st.session_state.get(job_key)
    if job_id and adapter.has("get_managed_data_upload_job"):
        job = _mapping(_invoke(adapter, "get_managed_data_upload_job", job_id=str(job_id)) or {})
        if job.get("status") == "completed":
            notice = _mapping(job.get("result") or {})
            st.session_state[notice_key] = {"status": notice.get("status", "uploaded"), "version": notice.get("version")}
            st.session_state.pop(job_key, None)
        elif job.get("status") == "failed":
            st.session_state[notice_key] = {"status": "failed", "message": str(job.get("error") or "Managed data upload failed.")}
            st.session_state.pop(job_key, None)
        else:
            st.info(f"Upload is running in the background (job {str(job_id)[:8]}…).")
    if notice.get("status") in {"uploaded", "already_exists"}:
        version = _safe_release_version(notice.get("version"))
        st.success("Managed data upload completed" + (f" ({version})." if version else "."))
    elif notice.get("status") == "failed":
        st.error(str(notice.get("message") or "Managed data upload did not complete."))

    preview_ready = (
        retained.get("status") == "ready"
        and retained.get("bound_sha256") == digest
        and str(retained.get("sha256") or "") == digest
    )
    # The single action above performs validation and upload atomically from the
    # operator's perspective.  The backend still enforces checksum and policy.
    return selected_version


def _render_managed_data_db_section(
    adapter: BackendAdapter,
    *,
    dataset: Mapping[str, Any],
    selected_version: str,
) -> None:
    if not _managed_dataset_db_supported(dataset):
        return
    dataset_id = _managed_dataset_id(dataset)
    st.markdown("### Database")
    st.caption("Database updates use only the selected managed dataset version and backend-owned target mapping.")
    if not selected_version:
        st.info("Upload or select a managed file version before previewing a database update.")
        return
    if not (
        adapter.has("preview_managed_data_db_sync")
        and adapter.has("apply_managed_data_db_sync")
    ):
        st.info("Managed dataset database sync is unavailable from this backend version.")
        return
    allowed_targets = dataset.get("allowed_targets")
    environment_targets = (
        [item for item in allowed_targets if item in {"development", "production"}]
        if isinstance(allowed_targets, list)
        else []
    )
    target_options = environment_targets or ["development", "production"]
    if not target_options:
        st.info("This dataset has no enabled database targets.")
        return
    target = st.segmented_control(
        "Database target",
        target_options,
        default=target_options[0],
        key=f"managed-data-db-target-{dataset_id}",
    )
    target = str(target or "development")
    preview_key = f"managed-data-db-preview-{dataset_id}-{target}"
    if st.button(
        "Validate and apply DB update",
        type="primary",
        key=f"managed-data-db-apply-once-{dataset_id}-{selected_version}-{target}",
    ):
        preview_errors: list[str] = []
        preview = _invoke(
            adapter,
            "preview_managed_data_db_sync",
            dataset_id=dataset_id,
            version=selected_version,
            target_environment=target,
            error_sink=preview_errors.append,
        )
        preview_data = _managed_data_preview_payload(preview or {})
        if preview is None or preview_data.get("status") != "ready":
            st.session_state[preview_key] = {
                "status": "disabled",
                "version": selected_version,
                "target_environment": target,
            }
        else:
            preview_data["idempotency_key"] = str(uuid.uuid4())
            st.session_state[preview_key] = preview_data
            valid = bool(preview_data.get("preview_id") and preview_data.get("preview_digest"))
            if valid:
                result = _invoke(
                    adapter,
                    "apply_managed_data_db_sync",
                    preview_id=str(preview_data.get("preview_id")),
                    preview_digest=str(preview_data.get("preview_digest")),
                    idempotency_key=str(preview_data.get("idempotency_key")),
                    target_environment=target,
                    confirm=True,
                )
                result_data = _mapping(result or {})
                if result_data.get("status") in {"applied", "already_applied"}:
                    st.session_state.pop(preview_key, None)
                    st.session_state[f"managed-data-db-notice-{dataset_id}-{target}"] = "Managed dataset database update applied."
                else:
                    st.session_state[f"managed-data-db-notice-{dataset_id}-{target}"] = "Managed dataset database update did not complete."
        st.rerun()
    preview = _mapping(st.session_state.get(preview_key) or {})
    if preview and preview.get("version") == selected_version:
        if preview.get("status") != "ready":
            if target == "production":
                st.warning("Production DB update is disabled by the backend policy for this dataset or version.")
            else:
                st.warning("Development DB update preview is unavailable for this dataset or version.")
            return
        _render_managed_data_preview(preview)
    notice = st.session_state.pop(f"managed-data-db-notice-{dataset_id}-{target}", None)
    if notice:
        st.success(notice) if notice.endswith("applied.") else st.error(notice)


REGION_PLAN_BOUNDARY_ZIPS = ("30028", "30040", "30041", "30107")


def _first_backend_capability(adapter: BackendAdapter, *names: str) -> str:
    """Select a versioned bundle capability without assuming one backend release."""
    return next((name for name in names if adapter.has(name)), "")


def _render_fixed_region_plan_bundle_workflow(
    adapter: BackendAdapter,
    *,
    scope: str,
    dataset: Mapping[str, Any],
    source_version: str,
) -> None:
    """Import an already-resolved ZIP bundle; ambiguity resolution is intentionally absent."""
    if _managed_dataset_id(dataset) != "fixed_region_plan_bundle":
        return
    st.markdown("### Fixed region plan bundle lifecycle")
    st.caption(
        "Upload one immutable canonical DB-input bundle ZIP. The backend validates its fixed schema; "
        "arbitrary historical fixed-region CSV/XLSX files are rejected. This console never reparses a workbook "
        "or asks for ZIP owner/overflow decisions."
    )
    if scope != "development":
        st.error("Fixed region plan bundle import, review, and activation are Development-only.")
        return
    if not source_version:
        st.info("Upload or select a fixed region plan bundle version first.")
        return

    preview_capability = _first_backend_capability(
        adapter, "preview_fixed_region_plan_bundle_import", "preview_region_plan_bundle_import",
        "preview_region_plan_bundle",
    )
    import_capability = _first_backend_capability(
        adapter, "apply_fixed_region_plan_bundle_import",
        "import_fixed_region_plan_bundle", "import_region_plan_bundle",
        "apply_region_plan_bundle_import",
    )
    status_capability = _first_backend_capability(
        adapter, "get_fixed_region_plan_bundle_status"
    )
    review_capability = _first_backend_capability(adapter, "review_region_plan")
    activation_preview_capability = _first_backend_capability(adapter, "preview_region_plan_activation")
    activation_capability = _first_backend_capability(adapter, "apply_region_plan_activation")
    missing = [label for label, capability in (
        ("bundle preview", preview_capability), ("bundle status", status_capability),
        ("bundle import", import_capability),
        ("review", review_capability), ("activation preview", activation_preview_capability),
        ("activation", activation_capability),
    ) if not capability]
    if missing:
        st.info("Fixed bundle workflow capability is unavailable from this backend version: " + ", ".join(missing))
        return

    state_key = f"fixed-region-plan-bundle-workflow-{source_version}"
    state = dict(_mapping(st.session_state.get(state_key) or {}))
    status_errors: list[str] = []
    persisted_status = _safe_region_plan_result(
        _invoke(
            adapter,
            status_capability,
            environment="development",
            version=source_version,
            error_sink=status_errors.append,
        )
        or {}
    )
    if status_errors:
        st.error(
            "The checksum-bound Region Data lifecycle status is unavailable; "
            "review and activation remain disabled."
        )
    candidate = _mapping(
        persisted_status
        if persisted_status.get("status") in {"candidate", "reviewed", "active", "superseded"}
        else state.get("import_result")
        or st.session_state.get(
            f"region-plan-candidate-summary-{scope}-{_managed_dataset_id(dataset)}"
        )
        or {}
    )

    st.markdown("#### 1. Backend bundle preview and import")
    if st.button("Preview fixed region plan bundle import", key=f"fixed-region-bundle-preview-{source_version}"):
        errors: list[str] = []
        result = _invoke(
            adapter, preview_capability, environment="development",
            version=source_version, error_sink=errors.append,
        )
        state["preview"] = _safe_region_plan_result(result or {})
        st.session_state[state_key] = state
        st.rerun()
    preview = _mapping(state.get("preview") or {})
    if preview:
        st.json(preview)
    preview_ready = preview.get("status") == "ready"
    imported_by = st.text_input("Bundle import actor", value="console-ui", key=f"fixed-region-bundle-imported-by-{source_version}").strip()
    if preview_ready and st.button(
        "Prepare fixed region plan bundle import", disabled=not imported_by,
        key=f"fixed-region-bundle-import-prepare-{source_version}",
    ):
        state["import_pending"] = {"imported_by": imported_by, "idempotency_key": str(uuid.uuid4())}
        st.session_state[state_key] = state
        st.rerun()
    pending_import = _mapping(state.get("import_pending") or {})
    if pending_import and st.button(
        "Confirm Import Fixed Region Plan Bundle", type="primary",
        key=f"fixed-region-bundle-import-confirm-{source_version}",
    ):
        errors: list[str] = []
        result = _invoke(
            adapter, import_capability, environment="development", version=source_version,
            imported_by=str(pending_import.get("imported_by") or ""),
            idempotency_key=str(pending_import.get("idempotency_key") or ""), confirm=True,
            error_sink=errors.append,
        )
        state.pop("import_pending", None)
        state["import_result"] = _safe_region_plan_result(result or {})
        st.session_state[state_key] = state
        st.rerun()
    imported = not status_errors and candidate.get("status") in {
        "candidate", "reviewed", "active", "candidate_imported",
        "candidate_imported_for_development_verification", "already_imported"
    }
    if candidate:
        st.json(_safe_region_plan_result(candidate))
    if "import_result" in state and not imported:
        st.error("Fixed region plan bundle import did not complete; review and activation remain disabled.")

    st.markdown("#### 2. Review")
    reviewed_by = st.text_input("Bundle review actor", value="console-ui", key=f"fixed-region-bundle-reviewed-by-{source_version}").strip()
    review_reference = st.text_input("Bundle review reference", key=f"fixed-region-bundle-review-reference-{source_version}").strip()
    if st.button("Prepare Fixed Region Plan Review", disabled=not imported or not reviewed_by or not review_reference, key=f"fixed-region-bundle-review-prepare-{source_version}"):
        state["review_pending"] = {"reviewed_by": reviewed_by, "review_reference": review_reference, "expected_revision": int(candidate.get("revision", 0))}
        st.session_state[state_key] = state
        st.rerun()
    pending_review = _mapping(state.get("review_pending") or {})
    if pending_review and st.button("Confirm Fixed Region Plan Review", type="primary", key=f"fixed-region-bundle-review-confirm-{source_version}"):
        errors: list[str] = []
        result = _invoke(adapter, review_capability, environment="development", confirm=True,
                         resolution_digest=str(candidate.get("resolution_digest") or ""),
                         expected_revision=int(pending_review.get("expected_revision", 0)),
                         reviewed_by=str(pending_review.get("reviewed_by") or ""),
                         review_reference=str(pending_review.get("review_reference") or ""),
                         error_sink=errors.append)
        state.pop("review_pending", None)
        state["review_result"] = _safe_region_plan_result(result or {})
        st.session_state[state_key] = state
        st.rerun()
    reviewed = not status_errors and (
        candidate.get("status") in {"reviewed", "active"}
        or _mapping(state.get("review_result") or {}).get("status") == "reviewed"
    )

    st.markdown("#### 3. Activation preview and confirm")
    if st.button("Preview Fixed Region Plan Activation", disabled=not reviewed, key=f"fixed-region-bundle-activation-preview-{source_version}"):
        errors: list[str] = []
        result = _invoke(adapter, activation_preview_capability, environment="development",
                         resolution_digest=str(candidate.get("resolution_digest") or ""),
                         error_sink=errors.append)
        state["activation_preview"] = _safe_region_plan_result(result or {})
        st.session_state[state_key] = state
        st.rerun()
    activation_preview = _mapping(state.get("activation_preview") or {})
    if activation_preview:
        st.json(activation_preview)
    activated_by = st.text_input("Bundle activation actor", value="console-ui", key=f"fixed-region-bundle-activated-by-{source_version}").strip()
    activation_reference = st.text_input("Bundle activation reference", key=f"fixed-region-bundle-activation-reference-{source_version}").strip()
    if activation_preview.get("status") == "ready" and st.button("Confirm Activate Fixed Region Plan", type="primary", disabled=not activated_by or not activation_reference, key=f"fixed-region-bundle-activation-confirm-{source_version}"):
        errors: list[str] = []
        result = _invoke(adapter, activation_capability, environment="development",
                         preview_id=str(activation_preview.get("preview_id") or ""),
                         preview_digest=str(activation_preview.get("preview_digest") or ""),
                         activated_by=activated_by, activation_reference=activation_reference,
                         idempotency_key=str(uuid.uuid4()), confirm=True,
                         error_sink=errors.append)
        state["activation_result"] = _safe_region_plan_result(result or {})
        st.session_state[state_key] = state
        st.rerun()
    if candidate.get("status") == "active" or _mapping(
        state.get("activation_result") or {}
    ).get("status") in {"activated", "already_active"}:
        st.success("Fixed region plan bundle is active in Development only.")


def _safe_region_plan_result(value: object) -> dict[str, Any]:
    return dict(
        public_mapping(
            value,
            (
                "status", "migration_id", "checksum_sha256", "statement_count",
                "statement_types", "rollback_instructions", "environment", "plan_id",
                "source_version", "request_sha256", "lifecycle_stage", "revision",
                "checksum", "preview_id", "preview_digest", "plan_revision",
                "expected_activation_revision", "region_count", "postal_count",
                "technician_count", "boundary_resolution_count", "activation_revision",
                "resolution_digest", "artifacts", "managed_version", "bundle_sha256",
                "verification_only", "promotable", "write_allowed",
            ),
        )
    )


def _render_region_plan_artifacts(
    adapter: BackendAdapter, resolution_result: Mapping[str, Any]
) -> None:
    if not adapter.has("download_region_plan_resolution_artifact"):
        st.info("Reviewed region-plan artifact downloads are unavailable from this backend version.")
        return
    resolution_digest = str(resolution_result.get("resolution_digest") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", resolution_digest):
        st.info("Resolution-bound artifact downloads are not available for this candidate result.")
        return
    st.markdown("#### Resolution artifacts")
    specifications = (
        ("fixed_region_csv", "reviewed fixed-region CSV", "text/csv"),
        ("technician_policy_csv", "technician policy CSV", "text/csv"),
        ("boundary_policy_csv", "boundary policy CSV", "text/csv"),
        ("manifest", "reviewed plan manifest", "application/json"),
    )
    columns = st.columns(4)
    for column, (key, label, mime) in zip(columns, specifications):
        if column.button(
            f"Prepare {label}",
            key=f"region-plan-artifact-prepare-{resolution_digest}-{key}",
        ):
            errors: list[str] = []
            result = _invoke(
                adapter,
                "download_region_plan_resolution_artifact",
                environment="development",
                resolution_digest=resolution_digest,
                artifact_id=key,
                error_sink=errors.append,
            )
            item = _mapping(result or {})
            content = item.get("content")
            if isinstance(content, str):
                content = content.encode("utf-8")
            if isinstance(content, bytes):
                # PII-bearing bytes live only for this render and are never
                # copied into session_state, logs, JSON, or captions.
                column.download_button(
                    f"Download {label}",
                    data=content,
                    file_name=Path(str(item.get("file_name") or f"{key}.csv")).name,
                    mime=mime,
                    key=f"region-plan-artifact-download-{resolution_digest}-{key}",
                )
            else:
                column.error("Artifact download did not complete.")


def _render_region_plan_workflow(
    adapter: BackendAdapter,
    *,
    scope: str,
    dataset: Mapping[str, Any],
    source_version: str,
) -> None:
    if _managed_dataset_id(dataset) != "territory_plan_workbook":
        return
    st.markdown("### Territory plan candidate lifecycle")
    st.caption(
        "Candidate, resolved, reviewed, and active plans are immutable separate stages. "
        "This workflow never uses the generic master-table uploader."
    )
    if scope != "development":
        st.error(
            "Production region-plan schema, resolution, review, and activation writes are disabled."
        )
        for label in (
            "Preview region-plan schema", "Apply ambiguity resolutions",
            "Review region plan", "Preview activation",
        ):
            st.button(label, disabled=True, key=f"region-plan-production-disabled-{label}")
        return
    required = (
        "preview_region_plan_schema", "install_region_plan_schema",
        "apply_region_plan_resolutions", "review_region_plan",
        "preview_region_plan_activation", "apply_region_plan_activation",
    )
    missing = [name for name in required if not adapter.has(name)]
    if missing:
        st.info(
            "Region-plan workflow capability is unavailable from this backend version: "
            + ", ".join(missing)
        )
        return
    if not source_version:
        st.info("Select or upload an immutable territory plan workbook version first.")
        return

    state_key = f"region-plan-workflow-{source_version}"
    state = dict(_mapping(st.session_state.get(state_key) or {}))
    candidate = _mapping(
        st.session_state.get(
            f"region-plan-candidate-summary-{scope}-{_managed_dataset_id(dataset)}"
        )
        or {}
    )
    st.markdown("#### Candidate summary")
    summary = {
        "source_version": source_version,
        **public_mapping(
            candidate,
            (
                "status", "plan_id", "lifecycle_stage", "approval_status",
                "membership_input_rows", "membership_accepted_rows",
                "membership_rejected_rows", "unique_postal_count",
                "ambiguous_postal_count", "technician_input_rows",
                "technician_accepted_rows", "technician_rejected_rows",
            ),
        ),
    }
    st.json(summary)
    st.warning(
        "Development verification only - not production-approved or promotable. Four boundary "
        "decisions, 57 ZIPs without ZCTA polygons, jobs outside the plan, missing bound demand/solver "
        "evidence, and thin Zone 3 / ATL Outer Area technician coverage remain explicit limitations."
    )

    st.markdown("#### 1. Development schema")
    migrations = _invoke(
        adapter,
        "list_region_plan_schema_migrations",
        environment="development",
        error_sink=lambda _message: None,
    ) if adapter.has("list_region_plan_schema_migrations") else []
    migration_rows = [item for item in migrations if isinstance(item, Mapping)] if isinstance(migrations, list) else []
    migration_ids = [str(item.get("migration_id")) for item in migration_rows if item.get("migration_id")]
    if not migration_ids:
        # Compatibility with an older console backend. Current deployments
        # expose the registry above; this fallback remains V001-only.
        migration_ids = ["V001__atlanta_6area_region_plan"]
    selected_migration = st.selectbox(
        "Schema migration",
        migration_ids,
        key=f"region-plan-schema-migration-{source_version}",
    )
    if st.button("Preview region-plan schema", key=f"region-plan-schema-preview-{source_version}"):
        errors: list[str] = []
        result = _invoke(
            adapter,
            "preview_region_plan_schema",
            environment="development",
            migration_id=selected_migration,
            error_sink=errors.append,
        )
        state["schema_preview"] = _safe_region_plan_result(result or {})
        state["schema_error"] = state["schema_preview"].get("status") != "ready"
        state.pop("schema_install", None)
        st.session_state[state_key] = state
        st.rerun()
    schema_preview = _mapping(state.get("schema_preview") or {})
    if state.get("schema_error") is True:
        st.error("Region-plan schema preview did not complete; schema installation remains disabled.")
    if schema_preview.get("status") == "ready":
        st.json(schema_preview)
        if st.button(
            "Confirm Install Region Plan Schema",
            type="primary",
            key=f"region-plan-schema-install-{source_version}",
        ):
            errors: list[str] = []
            result = _invoke(
                adapter,
                "install_region_plan_schema",
                environment="development",
                migration_id=str(schema_preview.get("migration_id") or selected_migration),
                confirm=True,
                error_sink=errors.append,
            )
            state["schema_install"] = _safe_region_plan_result(result or {})
            state["schema_error"] = state["schema_install"].get("status") not in {
                "applied", "already_applied"
            }
            st.session_state[state_key] = state
            st.rerun()
    schema_install = _mapping(state.get("schema_install") or {})
    schema_ready = schema_install.get("status") in {"applied", "already_applied"}
    if schema_ready:
        st.success("Development region-plan schema is installed for the previewed checksum.")

    st.markdown("#### 2. Resolve four ambiguous ZIPs")
    boundary_resolutions: dict[str, dict[str, Any]] = {}
    resolutions_valid = schema_ready
    for postal_code in REGION_PLAN_BOUNDARY_ZIPS:
        cols = st.columns([1, 1.4, 1, 2.6])
        cols[0].markdown(f"**{postal_code}**")
        owner = cols[1].selectbox(
            "Owner",
            ("Select owner", "Zone 2", "Zone 3"),
            key=f"region-plan-owner-{source_version}-{postal_code}",
            label_visibility="collapsed",
        )
        allow_overflow = cols[2].checkbox(
            "Allow overflow",
            key=f"region-plan-overflow-{source_version}-{postal_code}",
        )
        rationale = cols[3].text_input(
            "Rationale",
            key=f"region-plan-rationale-{source_version}-{postal_code}",
            placeholder="Required decision rationale",
            label_visibility="collapsed",
        ).strip()
        valid = owner in {"Zone 2", "Zone 3"} and bool(rationale) and len(rationale) <= 500
        resolutions_valid = resolutions_valid and valid
        boundary_resolutions[postal_code] = {
            "primary_region": owner if owner in {"Zone 2", "Zone 3"} else "",
            "allow_overflow": bool(allow_overflow),
            "rationale": rationale,
        }
    imported_by = st.text_input(
        "Resolution actor",
        value="console-ui",
        key=f"region-plan-imported-by-{source_version}",
    ).strip()
    preview_resolutions_available = adapter.has("preview_region_plan_resolutions")
    if st.button(
        "Prepare ambiguity resolutions",
        disabled=not resolutions_valid or not bool(imported_by),
        key=f"region-plan-resolution-prepare-{source_version}",
    ):
        idempotency_key = str(uuid.uuid4())
        expected_request_sha256 = ""
        if preview_resolutions_available:
            errors: list[str] = []
            preview = _invoke(
                adapter,
                "preview_region_plan_resolutions",
                environment="development",
                source_version=source_version,
                boundary_resolutions=boundary_resolutions,
                imported_by=imported_by,
                idempotency_key=idempotency_key,
                error_sink=errors.append,
            )
            preview_data = _safe_region_plan_result(preview or {})
            if preview_data.get("status") != "ready":
                state["resolution_error"] = True
                st.session_state[state_key] = state
                st.rerun()
            expected_request_sha256 = str(preview_data.get("request_sha256") or "")
        state["resolution_pending"] = {
            "source_version": source_version,
            "boundary_resolutions": boundary_resolutions,
            "imported_by": imported_by,
            "idempotency_key": idempotency_key,
            "expected_request_sha256": expected_request_sha256,
        }
        state.pop("resolution_error", None)
        st.session_state[state_key] = state
        st.rerun()
    pending_resolution = _mapping(state.get("resolution_pending") or {})
    if state.get("resolution_error") is True:
        st.error("Ambiguity resolution preview did not complete; no candidate state was changed.")
    if pending_resolution:
        st.warning("Apply these four reviewed owner/overflow decisions to the Development candidate?")
        if st.button(
            "Confirm Apply Ambiguity Resolutions",
            type="primary",
            key=f"region-plan-resolution-confirm-{source_version}",
        ):
            errors: list[str] = []
            kwargs: dict[str, Any] = {
                "environment": "development",
                "source_version": str(pending_resolution.get("source_version")),
                "boundary_resolutions": dict(_mapping(pending_resolution.get("boundary_resolutions") or {})),
                "imported_by": str(pending_resolution.get("imported_by")),
                "idempotency_key": str(pending_resolution.get("idempotency_key")),
                "confirm": True,
            }
            expected = str(pending_resolution.get("expected_request_sha256") or "")
            if expected:
                kwargs["expected_request_sha256"] = expected
            result = _invoke(
                adapter,
                "apply_region_plan_resolutions",
                error_sink=errors.append,
                **kwargs,
            )
            state.pop("resolution_pending", None)
            state["resolution_result"] = _safe_region_plan_result(result or {})
            st.session_state[state_key] = state
            st.rerun()
    resolution_result = _mapping(state.get("resolution_result") or {})
    resolution_ready = (
        resolution_result.get("status") == "candidate_imported"
        and resolution_result.get("lifecycle_stage") == "candidate_resolved"
    )
    if resolution_ready:
        st.success("All four ambiguity decisions are applied to the immutable candidate revision.")
        st.json(resolution_result)
        _render_region_plan_artifacts(adapter, resolution_result)
    elif "resolution_result" in state:
        st.error("Ambiguity resolution did not complete; review and activation remain disabled.")

    st.markdown("#### 3. Review")
    verification_acknowledged = st.checkbox(
        "I acknowledge this review is for Development verification only and does not approve Production promotion.",
        key=f"region-plan-verification-ack-{source_version}",
    )
    default_revision = int(resolution_result.get("revision") or 1)
    expected_revision = int(
        st.number_input(
            "Expected candidate revision",
            min_value=1,
            value=max(default_revision, 1),
            step=1,
            key=f"region-plan-review-revision-{source_version}",
        )
    )
    reviewed_by = st.text_input(
        "Review actor", value="console-ui", key=f"region-plan-reviewed-by-{source_version}"
    ).strip()
    review_reference = st.text_input(
        "Review reference", key=f"region-plan-review-reference-{source_version}"
    ).strip()
    if st.button(
        "Prepare Region Plan Review",
        disabled=(
            not resolution_ready
            or not reviewed_by
            or not review_reference
            or not verification_acknowledged
        ),
        key=f"region-plan-review-prepare-{source_version}",
    ):
        state["review_pending"] = {
            "expected_revision": expected_revision,
            "reviewed_by": reviewed_by,
            "review_reference": review_reference,
            "resolution_digest": str(resolution_result.get("resolution_digest") or ""),
        }
        st.session_state[state_key] = state
        st.rerun()
    pending_review = _mapping(state.get("review_pending") or {})
    if pending_review and st.button(
        "Confirm Review Region Plan",
        type="primary",
        key=f"region-plan-review-confirm-{source_version}",
    ):
        errors: list[str] = []
        review_kwargs: dict[str, Any] = {
            "environment": "development",
            "expected_revision": int(pending_review.get("expected_revision") or 0),
            "reviewed_by": str(pending_review.get("reviewed_by")),
            "review_reference": str(pending_review.get("review_reference")),
            "confirm": True,
        }
        review_resolution_digest = str(pending_review.get("resolution_digest") or "")
        if review_resolution_digest:
            review_kwargs["resolution_digest"] = review_resolution_digest
        result = _invoke(
            adapter,
            "review_region_plan",
            error_sink=errors.append,
            **review_kwargs,
        )
        state.pop("review_pending", None)
        state["review_result"] = _safe_region_plan_result(result or {})
        st.session_state[state_key] = state
        st.rerun()
    review_result = _mapping(state.get("review_result") or {})
    reviewed = review_result.get("status") == "reviewed"
    if reviewed:
        st.success("Development verification review completed; Production promotion remains prohibited.")
        st.json(review_result)
    elif "review_result" in state:
        st.error("Region plan review did not complete; activation remains disabled.")

    st.markdown("#### 4. Activation preview and confirm")
    activated_by = st.text_input(
        "Activation actor", value="console-ui", key=f"region-plan-activated-by-{source_version}"
    ).strip()
    activation_reference = st.text_input(
        "Activation reference", key=f"region-plan-activation-reference-{source_version}"
    ).strip()
    if st.button(
        "Preview Region Plan Activation",
        disabled=not reviewed,
        key=f"region-plan-activation-preview-{source_version}",
    ):
        errors: list[str] = []
        activation_kwargs: dict[str, Any] = {"environment": "development"}
        resolution_digest = str(resolution_result.get("resolution_digest") or "")
        if resolution_digest:
            activation_kwargs["resolution_digest"] = resolution_digest
        result = _invoke(
            adapter,
            "preview_region_plan_activation",
            error_sink=errors.append,
            **activation_kwargs,
        )
        state["activation_preview"] = _safe_region_plan_result(result or {})
        st.session_state[state_key] = state
        st.rerun()
    activation_preview = _mapping(state.get("activation_preview") or {})
    if "activation_preview" in state and activation_preview.get("status") != "ready":
        st.error("Activation preview did not complete; activation remains disabled.")
    if activation_preview.get("status") == "ready":
        st.json(
            public_mapping(
                activation_preview,
                (
                    "region_count", "postal_count", "technician_count",
                    "boundary_resolution_count", "checksum", "plan_revision",
                    "expected_activation_revision",
                ),
            )
        )
        if st.button(
            "Confirm Activate Region Plan",
            type="primary",
            disabled=not activated_by or not activation_reference,
            key=f"region-plan-activation-confirm-{source_version}",
        ):
            errors: list[str] = []
            result = _invoke(
                adapter,
                "apply_region_plan_activation",
                environment="development",
                preview_id=str(activation_preview.get("preview_id")),
                preview_digest=str(activation_preview.get("preview_digest")),
                activated_by=activated_by,
                activation_reference=activation_reference,
                idempotency_key=str(uuid.uuid4()),
                confirm=True,
                error_sink=errors.append,
            )
            state["activation_result"] = _safe_region_plan_result(result or {})
            state.pop("activation_preview", None)
            st.session_state[state_key] = state
            st.rerun()
    activation_result = _mapping(state.get("activation_result") or {})
    if activation_result.get("status") in {"activated", "already_active"}:
        st.success("Atlanta_6area verification plan is active in Development only.")
        st.json(activation_result)
    elif "activation_result" in state:
        st.error("Region plan activation did not complete; the active plan was not changed.")


def _region_plan_v2_data(value: object) -> Mapping[str, Any]:
    envelope = _mapping(value)
    data = envelope.get("data")
    return _mapping(data) if isinstance(data, Mapping) else {}


def _render_region_plan_v2_receipt(value: object) -> None:
    envelope = _mapping(value)
    data = _region_plan_v2_data(envelope)
    nested_plan = _mapping(data.get("plan"))
    visible = dict(data)
    if nested_plan:
        visible.update(nested_plan)
    error = _mapping(envelope.get("error"))
    safe = {
        key: visible[key] for key in (
            "job_id", "plan_id", "lifecycle", "state", "workbook_sha256",
            "canonical_sha256", "plan_revision", "activation_revision", "preview_token",
            "row_accounting", "counts", "reject_summary", "reject_count", "checksum",
            "accepted", "rejected", "region_count", "postal_count",
            "technician_count", "boundary_resolution_count", "source_sha256",
            "manifest_sha256", "bundle_sha256", "plan_status",
        ) if key in visible
    }
    if safe:
        st.json(safe)
    if str(envelope.get("status")) in {"rejected", "failed"}:
        st.error(f"{error.get('code', 'REGION_PLAN_REQUEST_FAILED')}: {error.get('message', 'Request did not complete.')}")


_fragment = getattr(st, "fragment", lambda fn: fn)


@_fragment
def _render_region_plan_v2(adapter: BackendAdapter) -> None:
    """The only visible Region Plan workflow: browser -> versioned HTTP API."""
    st.caption("Upload one Area + Technician Excel workbook, then explicitly review, preview, and activate through the Region Plan v2 API.")
    required = ("preview_region_plan_schema", "install_region_plan_schema", "list_region_plan_v2_cities", "import_region_plan_v2_workbook", "review_region_plan_v2", "preview_region_plan_v2_activation", "activate_region_plan_v2")
    if not all(adapter.has(name) for name in required):
        st.info("Region Plan v2 is unavailable from this console backend version.")
        return
    state = st.session_state.setdefault("region-plan-v2", {})
    st.subheader("Common Region Plan Schema v2")
    if "schema_preview" not in state:
        state["schema_preview"] = _mapping(
            _invoke(adapter, "preview_region_plan_schema", environment="development") or {}
        )
    schema_preview = _mapping(state.get("schema_preview"))
    if schema_preview.get("status") == "ready":
        st.json(public_mapping(schema_preview, (
            "status", "schema_id", "target_id", "checksum_sha256",
        )))
    else:
        st.error("Common Region Plan Schema v2 readiness check failed.")
        return
    schema_result = _mapping(state.get("schema_result"))
    if st.button(
        "Prepare common Region Plan schema", type="primary",
        disabled=schema_result.get("status") == "reconciled", key="rp2-schema-prepare",
    ):
        state["schema_result"] = _mapping(
            _invoke(
                adapter, "install_region_plan_schema",
                environment="development", confirm=True,
            ) or {}
        )
        schema_result = _mapping(state["schema_result"])
    if schema_result.get("status") != "reconciled":
        st.info("Prepare the common schema once before importing or adopting a plan.")
        return
    st.success("Common Region Plan Schema v2 is prepared for development.")
    st.divider()
    cities_response = _invoke(adapter, "list_region_plan_v2_cities")
    cities_data = _region_plan_v2_data(cities_response or {})
    cities = cities_data.get("cities", [])
    cities = [item for item in cities if isinstance(item, Mapping)] if isinstance(cities, list) else []
    if not cities:
        _render_region_plan_v2_receipt(cities_response or {})
        st.warning("No permitted city registry entries were returned by the Region Plan API.")
        return
    subsidiaries = sorted({str(item.get("subsidiary_id") or "") for item in cities if str(item.get("subsidiary_id") or "")})
    subsidiary_id = st.selectbox("Subsidiary", subsidiaries, key="rp2-subsidiary")
    registries = [item for item in cities if str(item.get("subsidiary_id") or "") == subsidiary_id]
    registry_by_source = {str(item.get("source_city_id") or ""): item for item in registries if str(item.get("source_city_id") or "")}
    source_city_id = st.selectbox("Source city", list(registry_by_source), key="rp2-source-city")
    city = registry_by_source[source_city_id]
    target_city_id = st.text_input(
        "Target city ID", placeholder="LA_6area", key="rp2-target-city",
        help="New runtime scenario ID. Letters, numbers, dot, underscore, and hyphen only.",
    ).strip()
    target_valid = bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{1,127}", target_city_id))
    if target_city_id and not target_valid:
        st.error("Target city ID format is invalid.")
    policies = [item for item in city.get("policies", []) if isinstance(item, Mapping)]
    policy_by_version = {
        str(item.get("policy_version")): item for item in policies
        if str(item.get("policy_version") or "") and str(item.get("technician_policy_mode") or "")
    }
    policy_version = st.selectbox("Policy", list(policy_by_version), key="rp2-policy")
    selected_policy = policy_by_version[policy_version]
    technician_policy_mode = str(selected_policy["technician_policy_mode"])
    st.caption(f"Technician policy mode: {technician_policy_mode} (set by registry policy)")
    overlap_options = selected_policy.get("allowed_overlap_policies") or city.get("allowed_overlap_policies") or ("registry_default", "explicit_workbook")
    overlap_policy = st.selectbox("Overlap policy", [str(value) for value in overlap_options], key="rp2-overlap")
    # Upload is deliberately candidate-only. Review and activation always need
    # separate visible operator actions below.
    intent = "review_only"
    selection_key = f"{subsidiary_id}:{source_city_id}:{target_city_id}"
    if state.get("selection_key") != selection_key:
        for key in ("import", "adopt", "plan", "review", "preview", "activation", "candidates"):
            state.pop(key, None)
        state["selection_key"] = selection_key
    workbook = st.file_uploader("Area + Technician workbook (.xlsx)", type=["xlsx"], key="rp2-workbook")
    if st.button("Upload and validate", type="primary", disabled=workbook is None or not target_valid, key="rp2-upload"):
        result = _invoke(adapter, "import_region_plan_v2_workbook", workbook_name=workbook.name, workbook_bytes=workbook.getvalue(), metadata={
            "subsidiary_id": subsidiary_id, "target_city_id": target_city_id, "source_city_id": source_city_id,
            "policy_version": policy_version, "technician_policy_mode": technician_policy_mode,
            "overlap_policy": overlap_policy, "activation_intent": intent,
        })
        state["import"] = _mapping(result or {})
        imported = _region_plan_v2_data(result or {})
        imported_plan_id = str(imported.get("plan_id") or "")
        if imported_plan_id and str(_mapping(result).get("status")) == "accepted" and adapter.has("adopt_region_plan_v2_candidate"):
            adopted = _invoke(adapter, "adopt_region_plan_v2_candidate", subsidiary_id=subsidiary_id, target_city_id=target_city_id, plan_id=imported_plan_id)
            state["adopt"] = _mapping(adopted or {})
            adopted_plan = _region_plan_v2_data(adopted or {}).get("plan", {})
            if isinstance(adopted_plan, Mapping):
                state["plan"] = dict(adopted_plan)
        _render_region_plan_v2_receipt(result or {})
    st.divider()
    st.subheader("Existing candidate")
    if adapter.has("list_region_plan_v2_candidates") and st.button("List candidates for selected city", disabled=not target_valid, key="rp2-list"):
        state["candidates"] = _mapping(_invoke(adapter, "list_region_plan_v2_candidates", subsidiary_id=subsidiary_id, target_city_id=target_city_id) or {})
    candidates = _region_plan_v2_data(state.get("candidates", {})).get("plans", [])
    candidates = [item for item in candidates if isinstance(item, Mapping)] if isinstance(candidates, list) else []
    candidate_ids = [str(item.get("plan_id")) for item in candidates if str(item.get("plan_id") or "")]
    selected_plan = st.selectbox("Candidate plan", candidate_ids or ["No candidate loaded"], disabled=not candidate_ids, key="rp2-candidate")
    if adapter.has("adopt_region_plan_v2_candidate") and st.button("Adopt selected candidate", disabled=not candidate_ids, key="rp2-adopt"):
        result = _invoke(adapter, "adopt_region_plan_v2_candidate", subsidiary_id=subsidiary_id, target_city_id=target_city_id, plan_id=selected_plan)
        state["adopt"] = _mapping(result or {})
        plan = _region_plan_v2_data(result or {}).get("plan", {})
        if isinstance(plan, Mapping):
            state["plan"] = dict(plan)
    plan = _region_plan_v2_data(state.get("import", {}))
    plan = dict(state.get("plan", {})) if state.get("plan") else dict(plan)
    if state.get("adopt"):
        _render_region_plan_v2_receipt(state["adopt"])
    plan_id = str(plan.get("plan_id") or "")
    revision = plan.get("plan_revision", plan.get("revision"))
    activation_revision = plan.get("activation_revision")
    if not plan_id or revision is None or activation_revision is None:
        st.info("Upload completion or candidate adoption is required before review.")
        return
    st.divider()
    st.subheader(f"Lifecycle: {plan_id}")
    if st.button("Review", key="rp2-review"):
        result = _invoke(adapter, "review_region_plan_v2", subsidiary_id=subsidiary_id, target_city_id=target_city_id, plan_id=plan_id, plan_revision=int(revision), activation_revision=int(activation_revision))
        state["review"] = _mapping(result or {}); plan.update(_region_plan_v2_data(result or {})); state["plan"] = plan
        _render_region_plan_v2_receipt(result or {})
    reviewed = _region_plan_v2_data(state.get("review", {}))
    if st.button("Preview activation", disabled=reviewed.get("lifecycle") != "reviewed", key="rp2-preview"):
        result = _invoke(adapter, "preview_region_plan_v2_activation", subsidiary_id=subsidiary_id, target_city_id=target_city_id, plan_id=plan_id, plan_revision=int(plan.get("plan_revision", revision)), activation_revision=int(plan.get("activation_revision", activation_revision)))
        state["preview"] = _mapping(result or {})
        plan.update(_region_plan_v2_data(result or {})); state["plan"] = plan
        _render_region_plan_v2_receipt(result or {})
    preview = _region_plan_v2_data(state.get("preview", {}))
    reference = st.text_input("Activation reference", key="rp2-activation-reference")
    if st.button("Activate", type="primary", disabled=not preview.get("preview_token") or not reference.strip(), key="rp2-activate"):
        result = _invoke(adapter, "activate_region_plan_v2", subsidiary_id=subsidiary_id, target_city_id=target_city_id, plan_id=plan_id, plan_revision=int(preview.get("plan_revision", plan.get("plan_revision", revision))), activation_revision=int(preview.get("activation_revision", plan.get("activation_revision", activation_revision))), preview_token=str(preview.get("preview_token")), activation_reference=reference)
        state["activation"] = _mapping(result or {})
        plan.update(_region_plan_v2_data(result or {})); state["plan"] = plan
        _render_region_plan_v2_receipt(result or {})
    superseded = [item for item in candidates if str(item.get("lifecycle")) == "superseded"]
    if adapter.has("rollback_region_plan_v2") and superseded:
        st.divider(); st.subheader("Rollback (roll-forward activation)")
        rollback_ids=[str(item["plan_id"]) for item in superseded]
        rollback_id=st.selectbox("Previous plan",rollback_ids,key="rp2-rollback-plan")
        if st.button("Load rollback target",key="rp2-rollback-load"):
            loaded=_invoke(adapter,"adopt_region_plan_v2_candidate",subsidiary_id=subsidiary_id,target_city_id=target_city_id,plan_id=rollback_id)
            state["rollback_plan"]=_mapping(_region_plan_v2_data(loaded or {}).get("plan",{}))
        rollback_plan=_mapping(state.get("rollback_plan"))
        if rollback_plan:
            if st.button("Preview rollback",key="rp2-rollback-preview"):
                result=_invoke(adapter,"preview_region_plan_v2_activation",subsidiary_id=subsidiary_id,target_city_id=target_city_id,plan_id=str(rollback_plan["plan_id"]),plan_revision=int(rollback_plan["plan_revision"]),activation_revision=int(rollback_plan["activation_revision"]))
                state["rollback_preview"]=_mapping(result or {})
            rollback_preview=_region_plan_v2_data(state.get("rollback_preview",{}))
            reason=st.text_input("Rollback reason",key="rp2-rollback-reason")
            confirmation=st.text_input("Type ROLLBACK",key="rp2-rollback-confirm")
            if st.button("Activate previous plan",disabled=not rollback_preview.get("preview_token") or not reason.strip() or confirmation!="ROLLBACK",key="rp2-rollback-apply"):
                result=_invoke(adapter,"rollback_region_plan_v2",subsidiary_id=subsidiary_id,target_city_id=target_city_id,plan_id=str(rollback_plan["plan_id"]),plan_revision=int(rollback_preview["plan_revision"]),activation_revision=int(rollback_preview["activation_revision"]),preview_token=str(rollback_preview["preview_token"]),rollback_reason=reason,confirmation=confirmation)
                state["rollback_result"]=_mapping(result or {}); _render_region_plan_v2_receipt(result or {})


@_fragment
def _render_data_management(adapter: BackendAdapter, config_path: str) -> None:
    st.caption(
        "Manage versioned allowlisted operational datasets. Source-code packages are managed separately under Package Management."
    )
    scope = st.segmented_control(
        "Data scope",
        ("development", "production", "common"),
        default="development",
        key="managed-data-scope",
    )
    scope = str(scope or "development")
    if not adapter.has("list_managed_data_sets"):
        st.info("Managed data is unavailable from this backend version.")
        return
    list_errors: list[str] = []
    response = _mapping(
        _invoke(
            adapter,
            "list_managed_data_sets",
            scope=scope,
            error_sink=list_errors.append,
        )
        or {}
    )
    datasets = response.get("datasets")
    datasets = (
        [
            presented
            for item in datasets
            if isinstance(item, Mapping)
            for presented in [_managed_dataset_ui_metadata(_mapping(item))]
            if presented.get("ui_hidden") is not True
        ]
        if isinstance(datasets, list)
        else []
    )
    if not datasets:
        st.info(f"No managed datasets are available for the {scope} scope.")
        return
    rows = []
    for item in datasets:
        data = _mapping(item)
        rows.append(
            {
                "dataset": data.get("label") or data.get("dataset_id"),
                "scope": scope,
                "mode": "DB-sync" if _managed_dataset_db_supported(data) else "Upload-only",
                "version": data.get("current_version") or "-",
                "checksum": _short_checksum(data.get("sha256")),
                "rows": data.get("row_count"),
                "updated": data.get("updated_at"),
            }
        )
    st.dataframe(rows, width="stretch", hide_index=True)

    grouped: dict[str, list[Mapping[str, Any]]] = {
        "technician": [], "region": [], "other": [],
    }
    for item in datasets:
        data = _mapping(item)
        section = _managed_dataset_section(data)
        # Region plans have one visible API-only lane under Region Plans v2.
        # Legacy workbook, fixed bundle, SQL, and migration controls remain
        # backend-compatible but are never presented as alternate workflows.
        if section == "region":
            continue
        grouped[section].append(data)

    def render_dataset_control(section: str, label: str) -> None:
        options = grouped[section]
        if not options:
            return
        by_id = {
            _managed_dataset_id(item): item
            for item in options
            if _managed_dataset_id(item)
        }
        if not by_id:
            return
        selected_id = st.selectbox(
            label,
            list(by_id),
            format_func=lambda value: str(by_id[value].get("label") or value),
            key=f"managed-data-{section}-dataset-{scope}",
        )
        dataset = by_id[str(selected_id)]
        st.caption(str(dataset.get("description") or "Backend-allowlisted managed dataset."))
        badge = "DB-sync" if _managed_dataset_db_supported(dataset) else "Upload-only"
        privacy_value = dataset.get("contains_pii")
        if privacy_value is None:
            privacy_value = dataset.get("PII", dataset.get("pii", "not specified"))
        st.info(f"Scope: {scope} | Capability: {badge} | PII: {privacy_value}")
        if section == "technician":
            _render_active_region_binding(dataset)
        selected_version = _render_managed_data_file_section(
            adapter,
            scope=scope,
            dataset=dataset,
            config_path=config_path,
        )
        _render_managed_data_db_section(
            adapter,
            dataset=dataset,
            selected_version=selected_version,
        )
        if section == "region":
            if _managed_dataset_id(dataset) == "fixed_region_plan_bundle":
                _render_fixed_region_plan_bundle_workflow(
                    adapter,
                    scope=scope,
                    dataset=dataset,
                    source_version=selected_version,
                )
            elif dataset.get("ui_legacy_visible") is True:
                _render_region_plan_workflow(
                    adapter,
                    scope=scope,
                    dataset=dataset,
                    source_version=selected_version,
                )

    st.markdown("## Technician Data")
    st.caption(
        "Upload and validate versioned technician workbooks. Previews, counts, and validation errors are redacted; database changes require a backend preview and confirmation."
    )
    if grouped["technician"]:
        render_dataset_control("technician", "Technician dataset")
    else:
        st.info("No technician dataset is allowlisted for this scope.")

    st.markdown("## Region Data")
    st.caption(
        "Region plan upload, review, and activation are available only in Region Plans v2."
    )
    st.info("Open Region Plans v2 to manage Area + Technician workbooks through the versioned API.")

    if grouped["other"]:
        with st.expander("Other managed datasets", expanded=False):
            render_dataset_control("other", "Managed dataset")

    with st.expander("Advanced Database Operations", expanded=False):
        load_advanced = st.toggle(
            "Load migration and schema operations",
            value=False,
            key=f"managed-data-advanced-{scope}",
        )
        if load_advanced:
            default_target = scope if scope in {"development", "production"} else "development"
            target = st.segmented_control(
                "Advanced database target",
                ("development", "production"),
                default=default_target,
                key=f"managed-data-advanced-target-{scope}",
            )
            target = str(target or default_target)
            _render_db_tab(adapter, target, DB_CONFIG_PATHS[target])


def _render_settings(adapter: BackendAdapter) -> None:
    """Use only the settings contract and never render returned secret values."""

    st.caption("Connection values are managed through the backend settings contract. Existing passwords and tokens are never displayed.")
    if not adapter.has("get_connection_settings"):
        st.info("Connection settings are not available from this backend version.")
        return
    settings = _mapping(_invoke(adapter, "get_connection_settings") or {})
    st.caption(f"Backend status: {redact_text(settings.get('status') or 'unknown')[:120]}")
    connection = _mapping(settings.get("connection") or {})
    environments = _mapping(settings.get("environments") or {})
    admin_tools_version = _safe_release_version(connection.get("admin_tools_release_version"))
    if connection.get("admin_tools_release_configured") is True and admin_tools_version:
        st.info(f"Production Admin Tools execution version (clean/promotable): {admin_tools_version}")
    else:
        st.warning("No clean/promotable Production Admin Tools execution version is configured.")
    development_version = _safe_release_version(
        connection.get("admin_tools_development_release_version")
    )
    development_mode = str(
        connection.get("admin_tools_development_release_mode") or ""
    )
    if (
        connection.get("admin_tools_development_release_configured") is True
        and development_version
        and development_mode in {"clean", "development-verification"}
    ):
        st.info(
            f"Development Admin Tools execution version: {development_version} ({development_mode})"
        )
        if development_mode == "development-verification":
            st.warning(
                "Development verification only: this dirty/non-promotable execution version cannot be used for Production."
            )
    pending_key = "connection-settings-review"
    ssh_tab, development_tab, production_tab = st.tabs(("SSH / SFTP", "Development database", "Production database"))
    with ssh_tab:
        st.warning("SSH/SFTP settings affect deployment targets and require a separate confirmation.")
        with st.form("connection-settings-ssh"):
            host = st.text_input("SSH host", value=str(connection.get("host") or ""))
            port = st.number_input(
                "SSH port", min_value=1, max_value=65535,
                value=int(connection.get("port") or 22), step=1,
            )
            username = st.text_input("SSH username", value=str(connection.get("username") or ""))
            remote_root = st.text_input("Remote root", value=str(connection.get("remote_root") or ""))
            password = st.text_input("New SSH password (leave blank to keep unchanged)", type="password")
            if st.form_submit_button("Review SSH/SFTP settings", type="primary", disabled=not adapter.has("update_connection_settings")):
                value = {"host": host.strip(), "port": int(port), "username": username.strip(), "remote_root": remote_root.strip()}
                if password:
                    value["password"] = password
                st.session_state[pending_key] = {"kind": "ssh_sftp", "payload": value, "requires_production_confirmation": True}
                st.rerun()
    for environment, tab in (("development", development_tab), ("production", production_tab)):
        with tab:
            database = _mapping(_mapping(environments.get(environment) or {}).get("database") or {})
            if environment == "production":
                st.warning("Production database changes require a separate confirmation.")
            with st.form(f"connection-settings-db-{environment}"):
                host = st.text_input("Database host", value=str(database.get("host") or ""), key=f"db-host-{environment}")
                port = st.number_input(
                    "Database port", min_value=1, max_value=65535,
                    value=int(database.get("port") or 5432), step=1,
                    key=f"db-port-{environment}",
                )
                dbname = st.text_input("Database name", value=str(database.get("dbname") or ""), key=f"db-name-{environment}")
                user = st.text_input("Database username", value=str(database.get("user") or ""), key=f"db-user-{environment}")
                password = st.text_input("New database password (leave blank to keep unchanged)", type="password", key=f"db-password-{environment}")
                if st.form_submit_button("Review database settings", type="primary", disabled=not adapter.has("update_connection_settings")):
                    value = {"host": host.strip(), "port": int(port), "dbname": dbname.strip(), "user": user.strip()}
                    if password:
                        value["password"] = password
                    st.session_state[pending_key] = {"kind": "database", "environment": environment, "payload": value, "requires_production_confirmation": environment == "production"}
                    st.rerun()
    pending = _mapping(st.session_state.get(pending_key) or {})
    if pending:
        st.warning("Review complete. Apply these connection settings through the backend?")
        reviewed = _mapping(pending.get("payload") or {})
        st.json({key: "configured" if key == "password" else value for key, value in reviewed.items()})
        confirm, cancel = st.columns(2)
        if confirm.button("Confirm connection settings", type="primary", disabled=not adapter.has("update_connection_settings")):
            kwargs: dict[str, Any] = {"confirm_production": pending.get("requires_production_confirmation") is True}
            if pending.get("kind") == "ssh_sftp":
                kwargs["ssh_sftp"] = dict(reviewed)
            else:
                kwargs["databases"] = {str(pending.get("environment")): dict(reviewed)}
            result = _invoke(adapter, "update_connection_settings", **kwargs)
            st.session_state.pop(pending_key, None)
            if result is not None:
                st.success(_result_message(result))
            st.rerun()
        if cancel.button("Cancel connection settings"):
            st.session_state.pop(pending_key, None)
            st.rerun()


def render_app(backend: object | None = None) -> None:
    st.set_page_config(page_title="AI Routing Deployment Console", page_icon="🚚", layout="wide")
    _load_console_styles()
    try:
        adapter = BackendAdapter(backend)
    except Exception:
        st.error("Platform backend unavailable. Review the local application log.")
        return

    server_config_path = SERVER_CONFIG_PATH
    route = _render_navigation()
    _render_top_toolbar(route)

    if route == "dashboard":
        _render_dashboard(adapter, server_config_path)
        return
    if route == "monitoring":
        _render_monitor_tab(adapter, server_config_path)
        return
    if route in {"package-development", "package-production"}:
        environment = _environment_for_route(route)
        _render_environment_warning(environment)
        _render_artifact_tab(adapter, environment, server_config_path, allowed_kinds=("runtime",))
        st.divider()
        _render_history_tab(adapter, environment, server_config_path)
        return
    if route == "data":
        _render_data_management(adapter, server_config_path)
        return
    if route == "region-plans":
        _render_region_plan_v2(adapter)
        return
    if route == "package-admin-tools":
        st.caption(
            "Admin Tools source-code artifacts only. Managed operational data belongs under Data Management."
        )
        _render_artifact_tab(
            adapter,
            "development",
            server_config_path,
            allowed_kinds=("admin-tools",),
        )
        return
    if route == "settings":
        _render_settings(adapter)
