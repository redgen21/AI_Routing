from __future__ import annotations

import argparse
import subprocess
import time
from datetime import datetime
from pathlib import Path


WATCH_ROOTS = [
    Path("."),
    Path("docs"),
    Path(".claude"),
]
AUTO_GENERATED_FILES = {
    Path("docs/vrp_feedback_watch_status.md"),
    Path("docs/vrp_level_benchmark_20260112.md"),
}


def _iter_watch_files() -> list[Path]:
    found: set[Path] = set()
    for root in WATCH_ROOTS:
        if not root.exists():
            continue
        if root.is_file() and root.suffix.lower() == ".md":
            if root not in AUTO_GENERATED_FILES:
                found.add(root)
            continue
        for path in root.rglob("*.md"):
            relative_path = path
            if relative_path not in AUTO_GENERATED_FILES:
                found.add(relative_path)
    return sorted(found)


def _snapshot() -> dict[Path, float]:
    result: dict[Path, float] = {}
    for path in _iter_watch_files():
        try:
            result[path] = path.stat().st_mtime
        except FileNotFoundError:
            continue
    return result


def _run(command: list[str]) -> tuple[int, str]:
    completed = subprocess.run(command, capture_output=True, text=True)
    output = (completed.stdout or "") + (completed.stderr or "")
    return completed.returncode, output.strip()


def _trigger_type(changed_files: list[str]) -> str:
    normalized = [str(path).lower() for path in changed_files]
    if any(path.startswith(".claude") or "claude" in path for path in normalized):
        return "claude_feedback"
    if any("feedback" in path or "review" in path for path in normalized):
        return "review_update"
    if changed_files:
        return "markdown_update"
    return "initial_validation"


def _write_status(changed_files: list[str], diff_output: str, unit_output: str, benchmark_output: str) -> None:
    status_path = Path("docs/vrp_feedback_watch_status.md")
    status_path.parent.mkdir(parents=True, exist_ok=True)
    trigger = _trigger_type(changed_files)
    lines = [
        "# VRP Feedback Watch Status",
        "",
        f"- Updated: {datetime.now().isoformat(timespec='seconds')}",
        f"- Trigger: {trigger}",
        f"- Changed markdown files: {', '.join(changed_files) if changed_files else '(none)'}",
        "- Benchmark target: 2026-01-12 only",
        "",
        "## Git Diff",
        "",
        "```text",
        diff_output,
        "```",
        "",
        "## Unit Test",
        "",
        "```text",
        unit_output,
        "```",
        "",
        "## Benchmark",
        "",
        "```text",
        benchmark_output,
        "```",
    ]
    status_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _validate(changed_files: list[str]) -> None:
    _, diff_output = _run(["git", "diff", "--stat"])
    _, unit_output = _run(["python", "sr_test_vrp_level_unit.py"])
    _, benchmark_output = _run(
        [
            "python",
            "sr_benchmark_vrp_level_vs_vrp.py",
            "--date",
            "2026-01-12",
            "--write",
            "docs/vrp_level_benchmark_20260112.md",
        ]
    )
    _write_status(changed_files, diff_output, unit_output, benchmark_output)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    previous = _snapshot()
    _validate([])
    previous = _snapshot()
    if args.once:
        return 0

    while True:
        time.sleep(max(args.interval, 5))
        current = _snapshot()
        changed = sorted(
            str(path)
            for path, mtime in current.items()
            if previous.get(path) != mtime
        )
        deleted = sorted(str(path) for path in previous.keys() - current.keys())
        changed.extend(deleted)
        if changed:
            _validate(changed)
            previous = _snapshot()


if __name__ == "__main__":
    raise SystemExit(main())
