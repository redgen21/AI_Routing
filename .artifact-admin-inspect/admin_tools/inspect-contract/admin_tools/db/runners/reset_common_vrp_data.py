from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from admin_tools.db.common_vrp import get_db_connection, load_common_config
from admin_tools.db.guard import require_db_write_allowed


DEFAULT_COMMON_CONFIG_PATH = Path("config/common_vrp.dev.json")


RESET_TABLES = [
    "common_routing_result",
    "common_routing_request",
    "common_request_technician_input",
    "common_job_input",
]


def _default_context(config_path: Path) -> tuple[str, str]:
    cfg = load_common_config(config_path)
    defaults = cfg.get("defaults", {})
    return (
        str(defaults.get("subsidiary_name", "LGEAI")).strip() or "LGEAI",
        str(defaults.get("strategic_city_name", "Atlanta, GA")).strip() or "Atlanta, GA",
    )


def _where_clause(promise_date: str | None = None) -> tuple[str, list[Any]]:
    clause = "subsidiary_name = %s and strategic_city_name = %s"
    params: list[Any] = []
    if promise_date:
        clause += " and promise_date = %s"
        params.append(str(promise_date).strip())
    return clause, params


def reset_common_vrp_data(
    subsidiary_name: str,
    strategic_city_name: str,
    promise_date: str | None = None,
    *,
    config_path: Path,
    dry_run: bool = True,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    base_params = [subsidiary_name, strategic_city_name]
    where_sql, extra_params = _where_clause(promise_date)
    params = base_params + extra_params

    with get_db_connection(config_path) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select count(*)
                from common_routing_result r
                where exists (
                    select 1
                    from common_routing_request q
                    where q.request_id = r.request_id
                      and {where_sql}
                )
                """,
                params,
            )
            counts["common_routing_result"] = int(cur.fetchone()[0] or 0)

            for table in ["common_routing_request", "common_request_technician_input", "common_job_input"]:
                cur.execute(f"select count(*) from {table} where {where_sql}", params)
                counts[table] = int(cur.fetchone()[0] or 0)

            if dry_run:
                conn.rollback()
                return counts

            cur.execute(
                f"""
                delete from common_routing_result
                where request_id in (
                    select request_id
                    from common_routing_request
                    where {where_sql}
                )
                """,
                params,
            )
            cur.execute(f"delete from common_routing_request where {where_sql}", params)
            cur.execute(f"delete from common_request_technician_input where {where_sql}", params)
            cur.execute(f"delete from common_job_input where {where_sql}", params)
        conn.commit()

    return counts


def main() -> None:
    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument(
        "--runtime-root",
        default="",
        help="Application environment root used for relative config paths.",
    )
    config_parser.add_argument("--config", default=str(DEFAULT_COMMON_CONFIG_PATH))
    config_parser.add_argument("--confirm-production", action="store_true")
    config_args, _ = config_parser.parse_known_args()
    if config_args.runtime_root:
        os.chdir(Path(config_args.runtime_root).expanduser().resolve())
    config_path = Path(config_args.config)
    default_subsidiary, default_city = _default_context(config_path)
    parser = argparse.ArgumentParser(
        description=(
            "Reset Common VRP transactional data: job input, request technician input, "
            "routing requests, and routing results. Master tables are not deleted."
        ),
        parents=[config_parser],
    )
    parser.add_argument("--subsidiary-name", default=default_subsidiary)
    parser.add_argument("--strategic-city-name", default=default_city)
    parser.add_argument("--promise-date", default="", help="Optional YYYYMMDD date. Omit to reset all dates in the context.")
    parser.add_argument("--yes", action="store_true", help="Actually delete rows. Without this flag, only prints counts.")
    args = parser.parse_args()
    config_path = Path(args.config)

    promise_date = str(args.promise_date).strip() or None
    if args.yes:
        require_db_write_allowed(config_path, confirm_production=args.confirm_production)
    counts = reset_common_vrp_data(
        str(args.subsidiary_name).strip(),
        str(args.strategic_city_name).strip(),
        promise_date,
        config_path=config_path,
        dry_run=not args.yes,
    )

    mode = "DELETE" if args.yes else "DRY RUN"
    scope = f"{args.subsidiary_name} / {args.strategic_city_name}"
    if promise_date:
        scope += f" / {promise_date}"
    print(f"[{mode}] {scope}")
    for table in RESET_TABLES:
        print(f"{table}: {counts.get(table, 0)}")
    if not args.yes:
        print("No rows were deleted. Re-run with --yes to reset this data.")


if __name__ == "__main__":
    main()
