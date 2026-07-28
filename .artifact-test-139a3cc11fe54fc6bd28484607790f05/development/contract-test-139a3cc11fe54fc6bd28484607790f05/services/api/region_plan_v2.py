"""Public, city-neutral Region Plan v2 lifecycle facade.

The HTTP layer never accepts SQL, filesystem paths, or table names.  Existing
candidate rows can be adopted into the v2 contract; the legacy repository is
used only as the transactional implementation while its request shape remains
an internal detail.
"""
from __future__ import annotations

import uuid
import base64
from http import HTTPStatus
from typing import Any
import hashlib, json
import os, re, shutil, tempfile
from pathlib import Path

from smart_routing.common_vrp_db import COMMON_CONFIG_PATH, get_db_connection, load_common_config

CONTRACT_VERSION = "region-plan/v2"
POLICY_MODES = {
    "assigned_region_boundary_spillover/v1": "assigned_region_boundary_spillover",
    "active_roster_type_hard_region_soft/v1": "active_roster_type_hard_region_soft",
    "active_roster_area_type_fallback_region_soft/v1": "active_roster_area_type_fallback_region_soft",
}
_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
_MUTATING_OPERATIONS = frozenset({"imports", "review", "activate", "rollback"})


class RegionPlanV2Error(ValueError):
    def __init__(self, code: str, status: int = HTTPStatus.UNPROCESSABLE_ENTITY):
        self.code, self.status = code, int(status)
        super().__init__(code)


def _validate_scope_identifiers(metadata: dict[str, Any]) -> None:
    if not _SAFE_ID.fullmatch(str(metadata.get("subsidiary_id", ""))):
        raise RegionPlanV2Error("SUBSIDIARY_ID_INVALID", HTTPStatus.BAD_REQUEST)
    if not _SAFE_ID.fullmatch(str(metadata.get("target_city_id", ""))):
        raise RegionPlanV2Error("TARGET_CITY_ID_INVALID", HTTPStatus.BAD_REQUEST)


def _development_config(config_path: str) -> dict[str, Any]:
    config = load_common_config(config_path)
    environment = str(config.get("environment", "")).strip().lower()
    dbname = str((config.get("database") or {}).get("dbname", "")).strip()
    if environment not in {"development", "dev"} or dbname != "vrp_db_dev":
        raise RegionPlanV2Error("REGION_PLAN_V2_MUTATION_FORBIDDEN", HTTPStatus.FORBIDDEN)
    return config


def _state_root(config: dict[str, Any]) -> Path:
    environment = str(config.get("environment", "development")).strip().lower()
    configured = str(config.get("REGION_PLAN_STATE_ROOT", config.get("region_plan_state_root", ""))).strip()
    root = Path(configured or f"/home/csda/AI_Routing/state/{environment}/region-plan").resolve()
    server_root = Path("/home/csda/AI_Routing/state").resolve()
    if root == server_root or server_root in root.parents:
        expected = server_root / environment
        if root != expected and expected not in root.parents:
            raise RegionPlanV2Error("STATE_ROOT_ENVIRONMENT_MISMATCH", HTTPStatus.SERVICE_UNAVAILABLE)
    return root

class CandidateRepository:
    """Candidate-only writer; activation remains in the lifecycle repository."""
    def __init__(self, connection_factory): self.connection_factory = connection_factory
    def import_candidate(self, candidate, workbook, *, config_path, principal, idempotency_key):
        m=candidate["manifest"]; meta=m["city_metadata"]; areas=m["areas"]; techs=m["technicians"]
        if not idempotency_key: raise RegionPlanV2Error("IDEMPOTENCY_KEY_REQUIRED", 400)
        _validate_scope_identifiers(meta)
        registry_mode=str(meta.get("technician_policy_mode", "")).strip()
        if not registry_mode or any(t["policy_mode"] != registry_mode for t in techs):
            raise RegionPlanV2Error("CITY_POLICY_MODE_INVALID")
        regions=sorted({a["region_code"]:(a["region_name"],a["area_type"]) for a in areas}.items())
        seq={code:i+1 for i,(code,_) in enumerate(regions)}
        active_technician_count=sum(bool(t["active"]) for t in techs)
        cfg=_development_config(config_path)
        root=_state_root(cfg)
        environment=str(cfg.get("environment","development")).strip().lower(); workbook_digest=m["source_workbook_sha256"]
        target=(root / "v2" / environment / meta["subsidiary_id"] / meta["target_city_id"] / workbook_digest).resolve()
        if root != target and root not in target.parents:
            raise RegionPlanV2Error("STATE_PATH_INVALID", HTTPStatus.BAD_REQUEST)
        target.parent.mkdir(parents=True, exist_ok=True); os.chmod(target.parent,0o700)
        staging=Path(tempfile.mkdtemp(prefix=".staging-",dir=target.parent)); os.chmod(staging,0o700)
        def write(name,data):
            p=staging/name; p.write_bytes(data); os.chmod(p,0o600)
        write("source.xlsx",workbook); write("source.sha256",(workbook_digest+"\n").encode()); write("canonical.json",candidate["artifacts"]["canonical.json"]); write("rejects.jsonl",candidate["artifacts"]["rejects.jsonl"])
        write("validation.json",json.dumps({"status":"candidate","plan_id":m["plan_id"]},sort_keys=True).encode())
        conn=self.connection_factory(config_path)
        try:
            with conn.cursor() as c:
                c.execute("set transaction isolation level serializable")
                c.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", ("region-plan:"+meta["subsidiary_id"]+":"+meta["target_city_id"],))
                c.execute("select plan_id,source_sha256 from common_region_plan where subsidiary_name=%s and strategic_city_name=%s and import_idempotency_key=%s for update", (meta["subsidiary_id"],meta["target_city_id"],idempotency_key))
                replay=c.fetchone()
                if replay:
                    if tuple(map(str,replay)) != (m["plan_id"],workbook_digest):
                        raise RegionPlanV2Error("IDEMPOTENCY_CONFLICT", HTTPStatus.CONFLICT)
                    conn.rollback()
                    shutil.rmtree(staging, ignore_errors=True)
                    return {"plan_id":m["plan_id"],"workbook_sha256":workbook_digest,"lifecycle":"candidate","idempotent_replay":True}
                c.execute("select 1 from common_technician_master where subsidiary_name=%s and strategic_city_name=%s and active_flag limit 1", (meta["subsidiary_id"],meta["source_city_id"]))
                if c.fetchone() is None: raise RegionPlanV2Error("SOURCE_CITY_NOT_REGISTERED")
                c.execute("select source_strategic_city_name from common_city_context where subsidiary_name=%s and strategic_city_name=%s for update", (meta["subsidiary_id"],meta["target_city_id"]))
                context=c.fetchone()
                if context and str(context[0]) != str(meta["source_city_id"]):
                    raise RegionPlanV2Error("SOURCE_CITY_CONTEXT_CONFLICT", HTTPStatus.CONFLICT)
                # current source roster prevents stale/PII workbook technicians from becoming candidates.
                codes=[t["technician_id"] for t in techs if t["active"]]
                c.execute("select employee_code,center_type from common_technician_master where subsidiary_name=%s and strategic_city_name=%s and active_flag and employee_code=any(%s)", (meta["subsidiary_id"],meta["source_city_id"],codes))
                roster={r[0]:r[1] for r in c.fetchall()}
                if set(codes)!=set(roster): raise RegionPlanV2Error("SOURCE_ROSTER_INVALID")
                required={t["technician_id"]: dict(regions)[t["region_code"]][1] for t in techs if t["active"]}
                if any(roster[code] != center for code,center in required.items()): raise RegionPlanV2Error("CENTER_TYPE_MISMATCH")
                c.execute("select distinct employee_code from common_technician_capability_master where subsidiary_name=%s and strategic_city_name=%s and employee_code=any(%s)", (meta["subsidiary_id"],meta["source_city_id"],codes))
                if {r[0] for r in c.fetchall()} != set(codes): raise RegionPlanV2Error("SOURCE_CAPABILITY_INVALID")
                c.execute("insert into common_city_context(subsidiary_name,strategic_city_name,source_strategic_city_name,context_version,policy_version,context_status) values(%s,%s,%s,%s,%s,'candidate') on conflict(subsidiary_name,strategic_city_name) do nothing", (meta["subsidiary_id"],meta["target_city_id"],meta["source_city_id"],"region-plan-workflow/v2",meta["policy_version"]))
                digest=m["canonical_sha256"]; source=workbook_digest
                c.execute("""insert into common_region_plan(subsidiary_name,strategic_city_name,plan_id,schema_version,policy_version,source_file_name,source_sha256,manifest_sha256,bundle_sha256,fixed_region_sha256,boundary_policy_sha256,technician_policy_sha256,membership_input_rows,membership_accepted_rows,membership_rejected_rows,unique_postal_count,technician_count,ambiguous_postal_count,import_idempotency_key,imported_by) values(%s,%s,%s,%s,%s,'upload.xlsx',%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s,%s) on conflict(subsidiary_name,strategic_city_name,plan_id) do update set updated_at=common_region_plan.updated_at where common_region_plan.manifest_sha256=excluded.manifest_sha256 returning revision""", (meta["subsidiary_id"],meta["target_city_id"],m["plan_id"],"region-plan-workflow/v2",meta["policy_version"],source,digest,digest,digest,digest,digest,len(areas),len(areas),len({a["postal_code"] for a in areas}),active_technician_count,sum(1 for a in areas if a["membership_rank"]>1),idempotency_key,principal))
                if c.fetchone() is None: raise RegionPlanV2Error("PLAN_IDENTITY_CONFLICT",409)
                c.executemany("insert into common_region_plan_region(subsidiary_name,strategic_city_name,plan_id,region_seq,region_id,region_name,source_territory,required_center_type) values(%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing", [(meta["subsidiary_id"],meta["target_city_id"],m["plan_id"],seq[k],k,v[0],k,v[1]) for k,v in regions])
                by_postal={}
                for a in areas: by_postal.setdefault(a["postal_code"],[]).append(a)
                primary=[]; overflow=[]
                for postal,members in by_postal.items():
                    main=next(a for a in members if a["is_primary"])
                    primary.append((meta["subsidiary_id"],meta["target_city_id"],m["plan_id"],postal,seq[main["region_code"]],main["area_type"],len(members),"not_required" if len(members)==1 else "resolved",json.dumps([seq[a["region_code"]] for a in members]),json.dumps(None)))
                    for alt in members:
                        if not alt["is_primary"]:
                            overflow.append((meta["subsidiary_id"],meta["target_city_id"],m["plan_id"],postal,seq[main["region_code"]],seq[alt["region_code"]],bool(alt["overflow_allowed"]),alt["overflow_penalty_minutes"] if alt["overflow_allowed"] else None,alt["overflow_reason"] or None,meta["policy_version"]))
                c.executemany("insert into common_region_plan_postal(subsidiary_name,strategic_city_name,plan_id,postal_code,region_seq,area_type,source_membership_count,resolution_status,source_region_seqs,resolution_metadata) values(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb) on conflict do nothing", primary)
                if overflow: c.executemany("insert into common_region_plan_boundary_overflow(subsidiary_name,strategic_city_name,plan_id,postal_code,primary_region_seq,alternate_region_seq,allow_overflow,penalty_cost,rationale,policy_version) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing", overflow)
                c.executemany("insert into common_region_plan_technician(subsidiary_name,strategic_city_name,plan_id,employee_code,assigned_region_seq,policy_mode,active_flag) values(%s,%s,%s,%s,%s,%s,%s) on conflict do nothing", [(meta["subsidiary_id"],meta["target_city_id"],m["plan_id"],t["technician_id"],seq[t["region_code"]],t["policy_mode"],t["active"]) for t in techs])
                c.execute("select (select count(*) from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),(select count(*) from common_region_plan_postal where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),(select count(*) from common_region_plan_technician where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),(select count(*) from common_region_plan_boundary_overflow where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s)", (meta["subsidiary_id"],meta["target_city_id"],m["plan_id"])*4)
                if tuple(c.fetchone()) != (len(regions),len(by_postal),len(techs),len(overflow)): raise RegionPlanV2Error("PLAN_ROW_COUNTS_INVALID")
            receipt={"plan_id":m["plan_id"],"workbook_sha256":source,"lifecycle":"candidate"}
            conn.commit()
            try:
                if not target.exists(): os.replace(staging,target)
                else: shutil.rmtree(staging, ignore_errors=True)
                (target/"receipt.json").write_text(json.dumps(receipt,sort_keys=True),encoding="utf-8"); os.chmod(target/"receipt.json",0o600)
                receipt["artifact_status"]="stored"
            except OSError:
                # The committed candidate remains authoritative and retryable;
                # an artifact write incident must not be misreported as DB loss.
                shutil.rmtree(staging, ignore_errors=True)
                receipt["artifact_status"]="storage_retry_required"
            return receipt
        except Exception:
            conn.rollback(); shutil.rmtree(staging, ignore_errors=True); raise
        finally: conn.close()


def _envelope(status: str, data: dict | None = None, error: RegionPlanV2Error | None = None) -> dict:
    result = {"contract_version": CONTRACT_VERSION, "request_id": str(uuid.uuid4()), "status": status}
    if data is not None: result["data"] = data
    if error: result["error"] = {"code": error.code, "message": "Region plan request was rejected.", "retryable": False}
    return result


def _identity(payload: dict) -> dict:
    required = ("subsidiary_name", "strategic_city_name", "source_strategic_city_name", "plan_id", "policy_version", "source_sha256", "manifest_sha256", "bundle_sha256")
    missing = [name for name in required if not str(payload.get(name, "")).strip()]
    if missing: raise RegionPlanV2Error("PLAN_IDENTITY_REQUIRED", HTTPStatus.BAD_REQUEST)
    return {"contract_version": "region-plan-lifecycle-request/v1", **{name: str(payload[name]).strip() for name in required},
            "region_count": int(payload["region_count"]), "postal_count": int(payload["postal_count"]),
            "technician_count": int(payload["technician_count"]), "boundary_resolution_count": int(payload.get("boundary_resolution_count", 0))}


def _lifecycle_identity(payload: dict, *, config_path=COMMON_CONFIG_PATH) -> dict:
    """Load signed lifecycle identity from DB; callers provide no hashes/counts."""
    authoritative = _adopt(payload, config_path=config_path)
    supplied_revision = int(payload.get("plan_revision", -1))
    supplied_activation = int(payload.get("activation_revision", -1))
    if supplied_revision != int(authoritative["plan_revision"]):
        raise RegionPlanV2Error("PLAN_REVISION_CONFLICT", HTTPStatus.CONFLICT)
    if supplied_activation != int(authoritative["activation_revision"]):
        raise RegionPlanV2Error("ACTIVATION_PREVIEW_STALE", HTTPStatus.CONFLICT)
    return {
        "contract_version": "region-plan-lifecycle-request/v1",
        "subsidiary_name": authoritative["subsidiary_name"],
        "strategic_city_name": authoritative["strategic_city_name"],
        "source_strategic_city_name": authoritative["source_strategic_city_name"],
        "plan_id": authoritative["plan_id"],
        "policy_version": authoritative["policy_version"],
        "source_sha256": authoritative["source_sha256"],
        "manifest_sha256": authoritative["manifest_sha256"],
        "bundle_sha256": authoritative["bundle_sha256"],
        "region_count": int(authoritative["region_count"]),
        "postal_count": int(authoritative["postal_count"]),
        "technician_count": int(authoritative["technician_count"]),
        "boundary_resolution_count": int(authoritative["boundary_resolution_count"]),
        "expected_plan_revision": supplied_revision,
        "expected_activation_revision": supplied_activation,
    }


def _adopt(payload: dict, *, config_path=COMMON_CONFIG_PATH) -> dict:
    """Read the immutable DB candidate and return the v2 linkage identity."""
    city, plan = str(payload.get("target_city_id", payload.get("strategic_city_name", ""))).strip(), str(payload.get("plan_id", "")).strip()
    subsidiary = str(payload.get("subsidiary_id", payload.get("subsidiary_name", ""))).strip()
    if not subsidiary or not city or not plan: raise RegionPlanV2Error("PLAN_IDENTITY_REQUIRED", HTTPStatus.BAD_REQUEST)
    _validate_scope_identifiers({"subsidiary_id": subsidiary, "target_city_id": city})
    with get_db_connection(config_path) as connection, connection.cursor() as cur:
        cur.execute("""select p.subsidiary_name,p.strategic_city_name,c.source_strategic_city_name,p.plan_id,p.policy_version,
                             p.source_sha256,p.manifest_sha256,p.bundle_sha256,p.revision,c.activation_revision,
                             p.membership_accepted_rows,p.membership_rejected_rows,p.unique_postal_count,p.technician_count,p.plan_status,
                             (select count(*) from common_region_plan_region r where r.subsidiary_name=p.subsidiary_name and r.strategic_city_name=p.strategic_city_name and r.plan_id=p.plan_id),
                             (select count(*) from common_region_plan_boundary_overflow b where b.subsidiary_name=p.subsidiary_name and b.strategic_city_name=p.strategic_city_name and b.plan_id=p.plan_id),
                             (select count(*) from common_region_plan_technician t where t.subsidiary_name=p.subsidiary_name and t.strategic_city_name=p.strategic_city_name and t.plan_id=p.plan_id and t.active_flag)
                      from common_region_plan p join common_city_context c using(subsidiary_name,strategic_city_name)
                      where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s""", (subsidiary, city, plan))
        row = cur.fetchone()
    if not row: raise RegionPlanV2Error("PLAN_NOT_FOUND", HTTPStatus.NOT_FOUND)
    keys = ("subsidiary_name","strategic_city_name","source_strategic_city_name","plan_id","policy_version","source_sha256","manifest_sha256","bundle_sha256","plan_revision","activation_revision","accepted","rejected","postal_count","technician_count","plan_status","region_count","boundary_resolution_count","actual_technician_count")
    found = dict(zip(keys, row))
    if (found["policy_version"] not in POLICY_MODES or
            int(found["accepted"]) != int(found["postal_count"]) + int(found["boundary_resolution_count"]) or
            int(found["technician_count"]) != int(found["actual_technician_count"]) or
            int(found["rejected"]) or not all(int(found[k]) > 0 for k in ("region_count", "postal_count", "technician_count"))):
        raise RegionPlanV2Error("PLAN_IDENTITY_MISMATCH")
    found.pop("actual_technician_count", None)
    return found


def _repo(config_path):
    # Runtime owns this dependency: deployment/admin tooling is never imported.
    from services.api.region_plan_repository_v2 import GenericRegionPlanLifecycleRepository
    return GenericRegionPlanLifecycleRepository(lambda: get_db_connection(config_path))


def handle(operation: str, payload: dict, *, config_path=COMMON_CONFIG_PATH, repository=None) -> tuple[int, dict]:
    try:
        if operation in _MUTATING_OPERATIONS:
            _development_config(config_path)
        if operation == "imports":
            if not str(payload.get("principal", "")).strip():
                raise RegionPlanV2Error("AUTHENTICATION_REQUIRED", HTTPStatus.UNAUTHORIZED)
            if not str(payload.get("idempotency_key", "")).strip():
                raise RegionPlanV2Error("IDEMPOTENCY_KEY_REQUIRED", HTTPStatus.BAD_REQUEST)
            # JSON/base64 is deliberately bounded; the handler never receives a path.
            encoded = str(payload.get("workbook_base64", ""))
            if not encoded or len(encoded) > 32 * 1024 * 1024:
                raise RegionPlanV2Error("WORKBOOK_SIZE_LIMIT", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            from tools.data.region_plan_workflow_v2 import canonicalize_workbook, RegionPlanV2ValidationError
            try:
                workbook = base64.b64decode(encoded, validate=True)
                metadata = dict(payload.get("city_metadata") or {})
                _validate_scope_identifiers(metadata)
                if str(metadata.get("activation_intent", "review_only")) != "review_only":
                    raise RegionPlanV2Error("ACTIVATION_INTENT_INVALID", HTTPStatus.BAD_REQUEST)
                if POLICY_MODES.get(str(metadata.get("policy_version", ""))) != str(metadata.get("technician_policy_mode", "")):
                    raise RegionPlanV2Error("CITY_POLICY_MODE_INVALID", HTTPStatus.FORBIDDEN)
                candidate = canonicalize_workbook(workbook, metadata)
            except (ValueError, RegionPlanV2ValidationError) as exc:
                raise RegionPlanV2Error(getattr(exc, "code", "WORKBOOK_FORMAT_INVALID"), HTTPStatus.BAD_REQUEST) from exc
            # Persistence is intentionally delegated to the repository; callers never supply SQL.
            if candidate["manifest"]["status"] == "rejected":
                return HTTPStatus.UNPROCESSABLE_ENTITY, _envelope("rejected", {"plan_id": candidate["manifest"]["plan_id"], "reject_count": len(candidate["rejects"])}, RegionPlanV2Error("REVIEW_GATE_FAILED"))
            repository = repository or CandidateRepository(get_db_connection)
            if not hasattr(repository, "import_candidate"):
                raise RegionPlanV2Error("IMPORT_REPOSITORY_UNAVAILABLE", HTTPStatus.SERVICE_UNAVAILABLE)
            result = repository.import_candidate(candidate, workbook, config_path=config_path, principal=str(payload["principal"]), idempotency_key=str(payload["idempotency_key"]))
            return HTTPStatus.ACCEPTED, _envelope("accepted", dict(result))
        if operation == "cities":
            with get_db_connection(config_path) as conn, conn.cursor() as cur:
                cur.execute("select distinct subsidiary_name, strategic_city_name from common_technician_master where active_flag order by 1,2")
                sources = cur.fetchall()
            cities = [
                {"subsidiary_id": sub, "source_city_id": city,
                 "policies": [{"policy_version": policy, "technician_policy_mode": mode} for policy, mode in POLICY_MODES.items()]}
                for sub, city in sources
            ]
            return HTTPStatus.OK, _envelope("completed", {"cities": cities})
        if operation == "list":
            city = str(payload.get("target_city_id", payload.get("strategic_city_name", ""))).strip()
            subsidiary = str(payload.get("subsidiary_id", payload.get("subsidiary_name", ""))).strip()
            if not city or not subsidiary: raise RegionPlanV2Error("CITY_METADATA_MISSING", HTTPStatus.BAD_REQUEST)
            with get_db_connection(config_path) as conn, conn.cursor() as cur:
                cur.execute("select plan_id,revision,policy_version,bundle_sha256,plan_status from common_region_plan where subsidiary_name=%s and strategic_city_name=%s and plan_status in ('candidate','reviewed','active') order by updated_at desc", (subsidiary, city))
                rows = cur.fetchall()
            return HTTPStatus.OK, _envelope("completed", {"plans": [dict(zip(("plan_id","plan_revision","policy_version","checksum","lifecycle"), row)) for row in rows]})
        if operation in {"adopt", "get"}:
            item = _adopt(payload, config_path=config_path)
            return HTTPStatus.OK, _envelope("completed", {"plan": item, "verified": True})
        if operation == "active":
            city = str(payload.get("target_city_id", payload.get("strategic_city_name", ""))).strip()
            subsidiary = str(payload.get("subsidiary_id", payload.get("subsidiary_name", ""))).strip()
            if not city or not subsidiary: raise RegionPlanV2Error("CITY_METADATA_MISSING", HTTPStatus.BAD_REQUEST)
            with get_db_connection(config_path) as conn, conn.cursor() as cur:
                cur.execute("""select a.plan_id,a.activation_revision,p.revision,p.policy_version,p.bundle_sha256
                               from common_region_plan_activation a join common_region_plan p using(subsidiary_name,strategic_city_name,plan_id)
                               where a.subsidiary_name=%s and a.strategic_city_name=%s and a.active_flag""", (subsidiary,city))
                row = cur.fetchone()
            return HTTPStatus.OK, _envelope("completed", {"active": None if not row else dict(zip(("plan_id","activation_revision","plan_revision","policy_version","checksum"), row))})
        if not str(payload.get("principal", "")).strip():
            raise RegionPlanV2Error("AUTHENTICATION_REQUIRED", HTTPStatus.UNAUTHORIZED)
        identity = _lifecycle_identity(payload, config_path=config_path)
        config = load_common_config(config_path)
        environment = str(config.get("environment", "development"))
        dbname = str((config.get("database") or {}).get("dbname", "vrp_db_dev"))
        repository = repository or _repo(config_path)
        if operation == "review":
            if not str(payload.get("idempotency_key", "")).strip(): raise RegionPlanV2Error("IDEMPOTENCY_KEY_REQUIRED", HTTPStatus.BAD_REQUEST)
            identity.update(reviewed_by=str(payload["principal"]), review_reference=str(payload.get("review_reference", "v2-review")))
            identity["idempotency_key"] = str(payload["idempotency_key"])
            result = repository.review(identity, environment=environment, dbname=dbname)
            return HTTPStatus.OK, _envelope("completed", {"plan_id": result.plan_id, "plan_revision": result.revision, "lifecycle": result.status})
        if operation == "activation-preview":
            preview = repository.preview(identity, environment=environment, dbname=dbname)
            return HTTPStatus.OK, _envelope("completed", {"plan_id": preview.identity.plan_id, "plan_revision": preview.plan_revision, "activation_revision": preview.expected_activation_revision, "preview_token": preview.preview_digest})
        if operation in {"activate", "rollback"}:
            if not str(payload.get("idempotency_key", "")).strip(): raise RegionPlanV2Error("IDEMPOTENCY_KEY_REQUIRED", HTTPStatus.BAD_REQUEST)
            if operation == "rollback" and (not str(payload.get("rollback_reason", "")).strip() or str(payload.get("confirmation", "")) != "ROLLBACK"):
                raise RegionPlanV2Error("ROLLBACK_CONFIRMATION_REQUIRED", HTTPStatus.BAD_REQUEST)
            identity.update(preview_digest=str(payload.get("preview_token", "")), activated_by=str(payload["principal"]), activation_reference=str(payload.get("activation_reference", "v2-activation")), idempotency_key=str(payload["idempotency_key"]))
            result = repository.activate(identity, environment=environment, dbname=dbname)
            lifecycle = "rolled_back" if operation == "rollback" and result.status == "activated" else result.status
            return HTTPStatus.OK, _envelope("completed", {"plan_id": result.plan_id, "activation_revision": result.activation_revision, "lifecycle": lifecycle})
        raise RegionPlanV2Error("NOT_FOUND", HTTPStatus.NOT_FOUND)
    except RegionPlanV2Error as exc: return exc.status, _envelope("rejected", error=exc)
    except ValueError as exc: return HTTPStatus.CONFLICT, _envelope("rejected", error=RegionPlanV2Error(str(exc), HTTPStatus.CONFLICT))
    except Exception: return HTTPStatus.SERVICE_UNAVAILABLE, _envelope("failed", error=RegionPlanV2Error("DB_RETRYABLE", HTTPStatus.SERVICE_UNAVAILABLE))
