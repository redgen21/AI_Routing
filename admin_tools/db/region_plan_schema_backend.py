"""One idempotent, development-only reconciler for common Region Plan Schema v2."""
from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path
from typing import Any

from admin_tools.db.region_plan_backend import RegionPlanContractError, _config_target, _connect_config, _require_development

ROOT = Path(__file__).resolve().parent
BASE_SQL = ROOT / "migrations" / "V001__atlanta_6area_region_plan.sql"
V2_SQL = ROOT / "region_plan_schema_v2.sql"
AREA_PLAN_CATALOG_SQL = ROOT / "migrations" / "V005__area_plan_catalog.sql"
LOCK_KEY = 772240602
READINESS_SQL = """select to_regclass('public.common_region_plan') is not null,
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_region_plan_region' and column_name='required_center_type'),
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_region_plan' and column_name='verified_content_sha256'),
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_region_plan' and column_name='verified_at'),
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_region_plan' and column_name='verified_by'),
  exists(select 1 from pg_constraint where conname='common_region_plan_verified_content_sha256_v2_check'),
  case when to_regclass('public.common_region_plan') is null then false
       else has_table_privilege('vrp_agent','public.common_region_plan','INSERT') end,
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_routing_config_master' and column_name='region_plan_id'),
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_routing_config_master' and column_name='region_plan_revision'),
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_routing_config_master' and column_name='region_plan_checksum'),
  exists(select 1 from pg_constraint where conname='common_routing_config_master_region_plan_checksum_v2_check'),
  to_regclass('public.common_region_set') is not null,
  to_regclass('public.common_region_set_region') is not null,
  to_regclass('public.common_region_set_postal') is not null,
  to_regclass('public.common_routing_plan') is not null,
  to_regclass('public.common_routing_plan_technician') is not null,
  to_regclass('public.common_routing_plan_activation') is not null,
  to_regclass('public.common_area_plan') is not null,
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_area_plan' and column_name='city_name'),
  exists(select 1 from information_schema.columns where table_schema='public' and table_name='common_area_plan' and column_name='legacy_storage_city_name')"""
READINESS_RESULT = (True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True)

def _sql() -> str:
    return (
        BASE_SQL.read_text(encoding="utf-8")
        + "\n" + V2_SQL.read_text(encoding="utf-8")
        + "\n" + AREA_PLAN_CATALOG_SQL.read_text(encoding="utf-8")
    )

def _check(config: Path | str) -> tuple[str, str]:
    _, environment, dbname = _config_target(config)
    _require_development(environment, dbname)
    return environment, dbname

def preview(config: Path | str) -> dict[str, Any]:
    environment, dbname = _check(config)
    sql = _sql()
    return {"contract_version":"region-plan-schema/v2", "status":"ready", "environment":environment,
            "dbname":dbname, "target_id":"development:vrp_db_dev", "schema_id":"common_region_plan_schema_v2",
            "checksum_sha256":hashlib.sha256(sql.encode()).hexdigest(), "requires_confirmation":"RECONCILE COMMON REGION PLAN SCHEMA V2 TO DEVELOPMENT vrp_db_dev"}

def reconcile(config: Path | str, *, confirmation: str) -> dict[str, Any]:
    expected = "RECONCILE COMMON REGION PLAN SCHEMA V2 TO DEVELOPMENT vrp_db_dev"
    if confirmation != expected: raise RegionPlanContractError("CONFIRMATION_REQUIRED")
    _check(config)
    connection, environment, dbname = _connect_config(config)
    changed = False
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("select pg_advisory_xact_lock(%s)", (LOCK_KEY,))
                cursor.execute(READINESS_SQL)
                if cursor.fetchone() != READINESS_RESULT:
                    cursor.execute(_sql())
                    changed = True
                    cursor.execute(READINESS_SQL)
                    if cursor.fetchone() != READINESS_RESULT: raise RegionPlanContractError("SCHEMA_DRIFT_DETECTED")
    finally: connection.close()
    return {**preview(config), "status":"reconciled", "changed":changed}

def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(prog="python -m admin_tools.db.region_plan_schema_backend")
    parser.add_argument("--json", action="store_true"); subs=parser.add_subparsers(dest="cmd",required=True)
    for name in ("preview","reconcile"):
        p=subs.add_parser(name); p.add_argument("--config",type=Path,required=True)
        if name=="reconcile": p.add_argument("--confirmation",required=True)
    a=parser.parse_args(argv)
    try: result=preview(a.config) if a.cmd=="preview" else reconcile(a.config, confirmation=a.confirmation)
    except RegionPlanContractError as exc: result={"status":"rejected","error_code":str(exc)}
    print(json.dumps(result, sort_keys=True)); return 0 if result["status"] in {"ready","reconciled"} else 2
if __name__ == "__main__": raise SystemExit(main())
