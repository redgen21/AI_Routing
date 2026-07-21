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
            }[key]
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


def _render_development_secure_config(adapter: BackendAdapter) -> None:
    """Render the deliberately narrow, secret-free development config uploader."""

    if not (
        adapter.has("preview_development_secure_config_upload")
        and adapter.has("upload_development_secure_config")
    ):
        return

    st.markdown("### Development secure config")
    st.caption(
        "Uploads the approved development config only. Values, passwords, and API keys are never shown here. "
        "This action does not restart services."
    )
    # The backend owns the fixed secure-config source set.  The UI supplies only
    # the deployment profile used to resolve the approved SFTP target and policy.
    config_path = SERVER_CONFIG_PATH
    preview = _invoke(
        adapter,
        "preview_development_secure_config_upload",
        environment="development",
        config_path=config_path,
    )
    if preview is None:
        return
    data = _mapping(preview)
    fingerprint = _secure_config_intent(data)
    allowed = data.get("upload_allowed") is True and bool(fingerprint)
    status = redact_text(data.get("status") or "unknown")[:160]
    if not allowed:
        st.error("Development secure config upload is unavailable: " + status)

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

    pending_key = "pending-development-secure-config"
    notice_key = "development-secure-config-notice"
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
        st.success("Development secure config uploaded. Service restart is required and was not performed.")
    elif notice.get("status") == "failed":
        st.error("Development secure config upload failed. Review the preview and try again.")

    if pending:
        st.warning("Upload this reviewed development secure config?")
        confirm_col, cancel_col = st.columns(2)
        if confirm_col.button(
            "Confirm secure config upload",
            type="primary",
            disabled=not allowed,
            key=f"secure-config-confirm-{fingerprint}",
        ):
            with st.status("Upload phase 0/2: submitting protected configuration...", expanded=True) as progress:
                st.caption("Current/total: 0/2. The backend reports this protected pair atomically, without per-file callbacks.")
                result = _invoke(
                    adapter,
                    "upload_development_secure_config",
                    environment="development",
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
        if cancel_col.button("Cancel secure config upload", key=f"secure-config-cancel-{fingerprint}"):
            st.session_state.pop(pending_key, None)
            st.rerun()
    elif st.button(
        "Upload development secure config",
        type="primary",
        disabled=not allowed,
        key=f"secure-config-request-{fingerprint}",
    ):
        st.session_state.pop(notice_key, None)
        st.session_state[pending_key] = {"preview_fingerprint": fingerprint}
        st.rerun()


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
    if environment == "development" and "runtime" in kinds:
        _render_development_secure_config(adapter)
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
    if pending and pending.get("intent_id") != intent["intent_id"]:
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
        if pending:
            st.warning(
                f"{pending.get('environment')} server: Upload "
                f"{len(pending.get('selected_files') or [])} selected files?"
            )
            confirm_column, cancel_column = st.columns(2)
            confirm = confirm_column.button(
                "Confirm upload",
                type="primary",
                key=f"upload-confirm-{intent['intent_id']}",
                disabled=not allowed,
            )
            cancel = cancel_column.button(
                "Cancel",
                key=f"upload-cancel-{intent['intent_id']}",
            )
        elif changed_files:
            if st.button(
                "Upload selected files",
                disabled=not allowed,
                type="primary",
                key=f"upload-request-{environment}-{kind}-{identifier}",
            ):
                st.session_state.pop(notice_key, None)
                st.session_state[pending_key] = intent
                st.rerun()
    if cancel:
        confirmation_area.empty()
        st.session_state.pop(pending_key, None)
        st.rerun()

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


def _render_managed_data_preview(data: Mapping[str, Any]) -> None:
    summary = public_mapping(
        data,
        (
            "status", "scope", "dataset_id", "file_name", "size_bytes", "version",
            "row_count", "create_count", "update_count", "unchanged_count",
            "target_environment", "warnings",
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
        "Validate managed data file",
        key=f"managed-data-validate-{scope}-{dataset_id}",
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
            st.session_state.pop(notice_key, None)
        st.rerun()
    retained = _mapping(st.session_state.get(preview_key) or {})
    if retained and retained.get("bound_sha256") == digest:
        _render_managed_data_preview(retained)
    notice = _mapping(st.session_state.get(notice_key) or {})
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
    pending = _mapping(st.session_state.get(pending_key) or {})
    if pending and pending.get("sha256") != digest:
        st.session_state.pop(pending_key, None)
        pending = {}
    if pending:
        st.warning("Upload this reviewed managed data file?")
        confirm, cancel = st.columns(2)
        if confirm.button(
            "Confirm managed data upload",
            type="primary",
            disabled=not adapter.has("upload_managed_data_file"),
            key=f"managed-data-upload-confirm-{scope}-{dataset_id}-{digest}",
        ):
            upload_errors: list[str] = []
            with st.status("Upload phase 0/1: submitting reviewed managed data...", expanded=True) as progress:
                result = _invoke(
                    adapter,
                    "upload_managed_data_file",
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
                progress.update(
                    label="Upload phase 1/1: completed" if succeeded else "Upload phase 0/1: failed",
                    state="complete" if succeeded else "error",
                )
            st.session_state.pop(pending_key, None)
            if succeeded:
                st.session_state[notice_key] = {
                    "status": str(result_data.get("status")),
                    "version": _safe_release_version(result_data.get("version")),
                }
                st.session_state.pop(preview_key, None)
            else:
                st.session_state[notice_key] = {
                    "status": "failed",
                    "message": "Managed data upload did not complete. Review the validated file and retry.",
                }
            st.rerun()
        if cancel.button(
            "Cancel managed data upload",
            key=f"managed-data-upload-cancel-{scope}-{dataset_id}-{digest}",
        ):
            st.session_state.pop(pending_key, None)
            st.rerun()
    elif preview_ready and st.button(
        "Upload reviewed file",
        type="primary",
        disabled=not adapter.has("upload_managed_data_file"),
        key=f"managed-data-upload-request-{scope}-{dataset_id}-{digest}",
    ):
        st.session_state.pop(notice_key, None)
        st.session_state[pending_key] = {"sha256": digest}
        st.rerun()
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
        "Preview DB update",
        key=f"managed-data-db-preview-button-{dataset_id}-{selected_version}-{target}",
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
        valid = bool(preview.get("preview_id") and preview.get("preview_digest"))
        if st.button(
            "Confirm Apply",
            type="primary",
            disabled=not valid,
            key=f"managed-data-db-apply-{dataset_id}-{selected_version}-{target}",
        ):
            apply_errors: list[str] = []
            result = _invoke(
                adapter,
                "apply_managed_data_db_sync",
                preview_id=str(preview.get("preview_id")),
                preview_digest=str(preview.get("preview_digest")),
                idempotency_key=str(preview.get("idempotency_key")),
                target_environment=target,
                confirm=True,
                error_sink=apply_errors.append,
            )
            result_data = _mapping(result or {})
            if result_data.get("status") in {"applied", "already_applied"}:
                st.session_state.pop(preview_key, None)
                st.success("Managed dataset database update applied.")
            else:
                st.error("Managed dataset database update did not complete. No raw backend error is displayed.")


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
    datasets = datasets if isinstance(datasets, list) else []
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
    by_id = {
        _managed_dataset_id(_mapping(item)): _mapping(item)
        for item in datasets
        if _managed_dataset_id(_mapping(item))
    }
    selected_id = st.selectbox(
        "Managed dataset",
        list(by_id),
        format_func=lambda value: str(by_id[value].get("label") or value),
        key=f"managed-data-dataset-{scope}",
    )
    dataset = by_id[str(selected_id)]
    st.caption(str(dataset.get("description") or "Backend-allowlisted managed dataset."))
    badge = "DB-sync" if _managed_dataset_db_supported(dataset) else "Upload-only"
    privacy_value = dataset.get("contains_pii")
    if privacy_value is None:
        privacy_value = dataset.get("PII", dataset.get("pii", "not specified"))
    privacy = str(privacy_value)
    st.info(f"Scope: {scope} | Capability: {badge} | PII: {privacy}")
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
