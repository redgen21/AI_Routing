# VRP Feedback Loop Protocol

Date: 2026-04-01

## Goal

Use a document-based feedback loop between Codex and Claude while keeping validation limited to `2026-01-01` through `2026-01-12`.

## Shared Files

- Codex work log: [UPDATED_BY_CODEX.md](c:/Python/북미 라우팅/docs/UPDATED_BY_CODEX.md)
- Claude feedback: [UPDATED_BY_CLAUDE.md](c:/Python/북미 라우팅/docs/UPDATED_BY_CLAUDE.md)
- Current benchmark artifact: [vrp_level_benchmark_20260112.md](c:/Python/북미 라우팅/docs/vrp_level_benchmark_20260112.md)
- Watch status: [vrp_feedback_watch_status.md](c:/Python/북미 라우팅/docs/vrp_feedback_watch_status.md)

## Operating Loop

1. Codex makes code changes.
2. Codex updates the work log.
3. Claude reads the work log and writes feedback into the Claude feedback document.
4. The watcher detects markdown updates.
5. The watcher reruns:
   - `git diff --stat`
   - `python sr_test_vrp_level_unit.py`
   - `python sr_benchmark_vrp_level_vs_vrp.py --date 2026-01-12`
6. Validation output is written back to the benchmark and watch-status documents.
7. Codex reads the updated Claude feedback and applies the next round of fixes.

## Scope Rule

- Performance comparison is currently pinned to `2026-01-12` only.
- Other dates are out of scope for the active Codex/Claude loop until the user changes the benchmark target.

## Practical Limitation

- The watcher can detect feedback updates and rerun validation automatically.
- Actual code modification by Codex still requires an active Codex session.
