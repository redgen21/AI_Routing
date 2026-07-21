from __future__ import annotations

import argparse
import json
import posixpath
import stat
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "server_ftp.local.json"
DEFAULT_JSON = PROJECT_ROOT / "data" / "north_america" / "reports" / "server_inventory_current.json"
DEFAULT_MARKDOWN = PROJECT_ROOT / "docs" / "server_inventory_current.md"
DEFAULT_SUMMARY_DIRS = {".venv", "__pycache__", "common_vrp_api_jobs", "vrp_api_jobs"}


def _load_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("protocol") != "sftp":
        raise ValueError("Only SFTP is supported by the read-only inventory tool.")
    if payload.get("read_only") is not True:
        raise ValueError("The connection config must explicitly set read_only=true.")
    for key in ("host", "username", "password", "remote_root"):
        if not str(payload.get(key, "")).strip():
            raise ValueError(f"Missing SFTP config value: {key}")
    return payload


def _entry_type(mode: int) -> str:
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISLNK(mode):
        return "symlink"
    return "other"


def collect_inventory(config: dict[str, Any], *, max_depth: int) -> list[dict[str, Any]]:
    try:
        import paramiko
    except ImportError as exc:  # pragma: no cover - environment-specific guidance
        raise RuntimeError("paramiko is required: python -m pip install paramiko") from exc

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(
        hostname=str(config["host"]),
        port=int(config.get("sftp_port", 22)),
        username=str(config["username"]),
        password=str(config["password"]),
        timeout=10,
        banner_timeout=10,
        auth_timeout=10,
        allow_agent=False,
        look_for_keys=False,
    )
    sftp = client.open_sftp()
    root = posixpath.normpath(str(config["remote_root"]))
    entries: list[dict[str, Any]] = []

    def visit(directory: str, depth: int) -> None:
        for attr in sorted(sftp.listdir_attr(directory), key=lambda item: item.filename.lower()):
            remote_path = posixpath.join(directory, attr.filename)
            relative = posixpath.relpath(remote_path, root)
            kind = _entry_type(attr.st_mode)
            entry: dict[str, Any] = {
                "path": relative,
                "type": kind,
                "size_bytes": int(attr.st_size),
                "modified_at_utc": datetime.fromtimestamp(attr.st_mtime, timezone.utc).isoformat(),
                "mode": oct(stat.S_IMODE(attr.st_mode)),
            }
            entries.append(entry)
            if kind != "directory":
                continue
            if attr.filename in DEFAULT_SUMMARY_DIRS:
                entry["contents_summarized"] = True
                try:
                    entry["immediate_child_count"] = len(sftp.listdir(remote_path))
                except OSError:
                    entry["immediate_child_count"] = None
                continue
            if depth < max_depth:
                visit(remote_path, depth + 1)

    try:
        visit(root, 1)
    finally:
        sftp.close()
        client.close()
    return entries


def _write_reports(
    entries: list[dict[str, Any]],
    config: dict[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    report = {
        "schema": "ai-routing-server-inventory/v1",
        "generated_at_utc": generated_at,
        "connection": {
            "protocol": "sftp",
            "host": config["host"],
            "port": int(config.get("sftp_port", 22)),
            "username": config["username"],
            "remote_root": config["remote_root"],
            "read_only": True,
        },
        "summary_only_directories": sorted(DEFAULT_SUMMARY_DIRS),
        "entries": entries,
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    top_level = Counter(entry["path"].split("/", 1)[0] for entry in entries)
    type_counts = Counter(entry["type"] for entry in entries)
    lines = [
        "# AI Routing server inventory",
        "",
        f"- Generated (UTC): `{generated_at}`",
        "- Connection: read-only SFTP; host and account are recorded only in the ignored private JSON report",
        "- Remote root: `<AI_ROUTING_ROOT>`",
        f"- Entries: {len(entries)} ({type_counts['file']} files, {type_counts['directory']} directories)",
        "- Password: not recorded",
        "",
        "## Top-level summary",
        "",
        "| Top-level path | Listed entries |",
        "| --- | ---: |",
    ]
    lines.extend(f"| `{name}` | {count} |" for name, count in sorted(top_level.items()))
    lines.extend(
        [
            "",
            "Large runtime directories (`.venv`, `__pycache__`, `common_vrp_api_jobs`, "
            "`vrp_api_jobs`) are listed as directories but their children are summarized.",
            "",
            "## Files and directories",
            "",
            "| Type | Size | Relative path |",
            "| --- | ---: | --- |",
        ]
    )
    for entry in entries:
        note = " (contents summarized)" if entry.get("contents_summarized") else ""
        safe_path = str(entry["path"]).replace("|", "\\|")
        lines.append(f"| {entry['type']} | {entry['size_bytes']} | `{safe_path}`{note} |")
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only SFTP inventory for the AI Routing server.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--max-depth", type=int, default=3)
    args = parser.parse_args()
    if args.max_depth < 1:
        raise ValueError("--max-depth must be at least 1")
    config = _load_config(args.config)
    entries = collect_inventory(config, max_depth=args.max_depth)
    _write_reports(entries, config, json_path=args.output_json, markdown_path=args.output_markdown)
    print(f"Inventory complete: {len(entries)} entries")
    print(f"Markdown: {args.output_markdown}")
    print(f"JSON: {args.output_json}")


if __name__ == "__main__":
    main()
