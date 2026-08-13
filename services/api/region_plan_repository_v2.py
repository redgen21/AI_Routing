"""Runtime-owned repository facade for Region Plan v2.

The API deliberately has no dependency on deployment/admin packages.  The
repository keeps the transaction boundary and city advisory lock here; SQL is
never supplied by a caller.
"""
from __future__ import annotations

import hashlib, json, re
from dataclasses import dataclass
from typing import Any, Mapping


class RegionPlanRepositoryError(ValueError):
    def __init__(self, code: str): self.code = code; super().__init__(code)


@dataclass(frozen=True)
class ReviewResult: status: str; plan_id: str; revision: int
@dataclass(frozen=True)
class ActivationPreview:
    identity: Any; plan_revision: int; expected_activation_revision: int; preview_digest: str
@dataclass(frozen=True)
class ActivationResult: status: str; plan_id: str; activation_revision: int; preview_digest: str

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
POLICY_ALLOWLIST = frozenset({
    "home_distance_only", "preferred_region_soft",
    "explicit_workbook_membership/v1",
    "own_region_with_approved_boundary_overflow/v2",
    "active_roster_type_hard_region_soft/v1",
    "active_roster_area_type_fallback_region_soft/v1",
})
@dataclass(frozen=True)
class GenericPlanIdentity:
    subsidiary_name: str; strategic_city_name: str; source_strategic_city_name: str; plan_id: str; policy_version: str; source_sha256: str; manifest_sha256: str; bundle_sha256: str; region_count: int; postal_count: int; technician_count: int; boundary_resolution_count: int
def _generic_identity(r: Mapping[str,Any]) -> GenericPlanIdentity:
    if r.get('contract_version') != 'region-plan-lifecycle-request/v1': raise RegionPlanRepositoryError('LIFECYCLE_CONTRACT_INVALID')
    values=[str(r.get(k,'')).strip() for k in ('subsidiary_name','strategic_city_name','source_strategic_city_name','plan_id','policy_version')]
    if not all(values) or values[4] not in POLICY_ALLOWLIST: raise RegionPlanRepositoryError('GENERIC_POLICY_NOT_ALLOWED')
    hashes=[str(r.get(k,'')).lower() for k in ('source_sha256','manifest_sha256','bundle_sha256')]
    if not all(_SHA256.fullmatch(x) for x in hashes): raise RegionPlanRepositoryError('PLAN_CHECKSUM_INVALID')
    try: counts=[int(r[k]) for k in ('region_count','postal_count','technician_count','boundary_resolution_count')]
    except (KeyError,TypeError,ValueError) as exc: raise RegionPlanRepositoryError('PLAN_ROW_COUNTS_INVALID') from exc
    if any(x<0 for x in counts) or not all(counts[:3]): raise RegionPlanRepositoryError('PLAN_ROW_COUNTS_INVALID')
    return GenericPlanIdentity(*values,*hashes,*counts)
def _generic_roster_digest(m,c): return hashlib.sha256(json.dumps({'master':m,'capabilities':c},sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
def _generic_preview_digest(i,pr,ar,active,roster,content): return hashlib.sha256(json.dumps({**i.__dict__,'plan_revision':pr,'activation_revision':ar,'current_active_plan_id':active,'source_roster_digest':roster,'plan_content_digest':content},sort_keys=True,separators=(',',':')).encode()).hexdigest()


def _plan_content_digest(cursor, identity: GenericPlanIdentity) -> str:
    """Bind preview tokens to every immutable child row, not just row counts."""
    scope=(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)
    queries=(
        "select region_seq,region_id,region_name,source_territory,required_center_type from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by region_seq",
        "select postal_code,region_seq,area_type,source_membership_count,resolution_status,source_region_seqs,resolution_metadata from common_region_plan_postal where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by postal_code,region_seq",
        "select postal_code,primary_region_seq,alternate_region_seq,allow_overflow,penalty_cost,rationale,policy_version from common_region_plan_boundary_overflow where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by postal_code,primary_region_seq,alternate_region_seq",
        "select employee_code,assigned_region_seq,policy_mode,active_flag from common_region_plan_technician where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s order by employee_code",
    )
    content=[]
    for sql in queries:
        cursor.execute(sql,scope)
        content.append(tuple(map(tuple,cursor.fetchall())))
    return hashlib.sha256(json.dumps(content,separators=(',',':'),default=str).encode()).hexdigest()


class GenericRegionPlanLifecycleRepository:
    """Generic lifecycle implementation; all changes use one serializable city lock."""
    def __init__(self, connection_factory): self._connection_factory = connection_factory
    def _connection(self): return self._connection_factory()
    @staticmethod
    def _begin(c, r):
        c.execute("set transaction isolation level serializable")
        c.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", (f"region-plan:{r['subsidiary_name']}:{r['strategic_city_name']}",))
    @staticmethod
    def _digest(r, revision, activation, active, roster):
        raw = "|".join(map(str,(r['plan_id'],r['manifest_sha256'],revision,activation,active or '',roster)))
        return hashlib.sha256(raw.encode()).hexdigest()
    def review(self, r: Mapping[str, Any], **_):
        identity = _generic_identity(r)
        conn=self._connection()
        try:
            with conn.cursor() as c:
                self._begin(c,r)
                c.execute("select revision,activation_revision,plan_status,policy_version,source_sha256,manifest_sha256,bundle_sha256,membership_accepted_rows,membership_rejected_rows,unique_postal_count,technician_count,source_strategic_city_name,verified_content_sha256 from common_region_plan p join common_city_context cc using(subsidiary_name,strategic_city_name) where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s for update",(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)); current=c.fetchone()
                if not current or current[2] != 'candidate' or tuple(map(str,current[3:7])) != (identity.policy_version,identity.source_sha256,identity.manifest_sha256,identity.bundle_sha256) or tuple(map(int,current[7:11])) != (identity.postal_count + identity.boundary_resolution_count,0,identity.postal_count,identity.technician_count) or current[11] != identity.source_strategic_city_name: raise RegionPlanRepositoryError('PLAN_IDENTITY_MISMATCH')
                if (int(current[0]),int(current[1])) != (int(r['expected_plan_revision']),int(r['expected_activation_revision'])): raise RegionPlanRepositoryError('PLAN_REVIEW_REVISION_CONFLICT')
                c.execute("select (select count(*) from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),(select count(*) from common_region_plan_postal where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),(select count(*) from common_region_plan_technician where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and active_flag),(select count(*) from common_region_plan_boundary_overflow where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s)",(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)*4)
                if tuple(map(int,c.fetchone())) != (identity.region_count,identity.postal_count,identity.technician_count,identity.boundary_resolution_count): raise RegionPlanRepositoryError('PLAN_ROW_COUNTS_INVALID')
                if not current[12] or _plan_content_digest(c,identity) != current[12]: raise RegionPlanRepositoryError('PLAN_CONTENT_CHECKSUM_MISMATCH')
                c.execute("update common_region_plan set plan_status='reviewed',revision=revision+1,reviewed_by=%s,review_reference=%s,reviewed_at=now() where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and plan_status='candidate' and revision=%s returning revision",(r['reviewed_by'],r['review_reference'],r['subsidiary_name'],r['strategic_city_name'],r['plan_id'],r['expected_plan_revision']))
                row=c.fetchone()
                if not row: raise RegionPlanRepositoryError('PLAN_REVIEW_REVISION_CONFLICT')
            conn.commit(); return ReviewResult('reviewed',r['plan_id'],int(row[0]))
        except Exception: conn.rollback(); raise
        finally: conn.close()
    def preview(self,r: Mapping[str,Any],**_):
        identity=_generic_identity(r)
        conn=self._connection()
        try:
            with conn.cursor() as c:
                self._begin(c,r); c.execute("select p.revision,cc.activation_revision,(select plan_id from common_region_plan_activation a where a.subsidiary_name=p.subsidiary_name and a.strategic_city_name=p.strategic_city_name and a.active_flag) from common_region_plan p join common_city_context cc using(subsidiary_name,strategic_city_name) where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s and p.plan_status in ('reviewed','superseded') for update",(r['subsidiary_name'],r['strategic_city_name'],r['plan_id'])); row=c.fetchone()
                if not row or (int(row[0]),int(row[1])) != (r['expected_plan_revision'],r['expected_activation_revision']): raise RegionPlanRepositoryError('ACTIVATION_PREVIEW_STALE')
                # Preview is a signed read of the stored candidate, not merely a
                # status/revision check: reject swapped artifacts and row drift.
                c.execute("select plan_status,policy_version,source_sha256,manifest_sha256,bundle_sha256,membership_accepted_rows,membership_rejected_rows,unique_postal_count,technician_count,source_strategic_city_name,verified_content_sha256 from common_region_plan where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s for share",(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)); stored=c.fetchone()
                if not stored or stored[0] not in ('reviewed','superseded') or tuple(map(str,stored[1:5])) != (identity.policy_version,identity.source_sha256,identity.manifest_sha256,identity.bundle_sha256) or tuple(map(int,stored[5:9])) != (identity.postal_count + identity.boundary_resolution_count,0,identity.postal_count,identity.technician_count) or stored[9] != identity.source_strategic_city_name: raise RegionPlanRepositoryError('PLAN_IDENTITY_MISMATCH')
                c.execute("select (select count(*) from common_region_plan_region where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),(select count(*) from common_region_plan_postal where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s),(select count(*) from common_region_plan_technician where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and active_flag),(select count(*) from common_region_plan_boundary_overflow where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s)",(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)*4)
                counts=c.fetchone()
                if tuple(map(int,counts)) != (identity.region_count,identity.postal_count,identity.technician_count,identity.boundary_resolution_count): raise RegionPlanRepositoryError('PLAN_ROW_COUNTS_INVALID')
                c.execute("select m.employee_code,m.employee_name,m.center_type,m.home_address,m.home_city,m.home_state,m.home_country,m.home_postal_code,m.home_latitude,m.home_longitude,m.active_flag,m.priority_group,m.max_home_to_job_min from common_region_plan_technician t join common_technician_master m on m.subsidiary_name=t.subsidiary_name and m.strategic_city_name=%s and m.employee_code=t.employee_code join common_region_plan_region pr on pr.subsidiary_name=t.subsidiary_name and pr.strategic_city_name=t.strategic_city_name and pr.plan_id=t.plan_id and pr.region_seq=t.assigned_region_seq where t.subsidiary_name=%s and t.strategic_city_name=%s and t.plan_id=%s and t.active_flag and m.active_flag and m.center_type=pr.required_center_type order by m.employee_code for share",(identity.source_strategic_city_name,identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)); masters=tuple(map(tuple,c.fetchall()))
                if len(masters)!=identity.technician_count: raise RegionPlanRepositoryError('SOURCE_ROSTER_INVALID')
                codes=[x[0] for x in masters]; c.execute("select employee_code,product_group_code,product_code,repair_allowed,heavy_repair_allowed,priority_score,effective_start_date,effective_end_date from common_technician_capability_master where subsidiary_name=%s and strategic_city_name=%s and employee_code=any(%s) order by employee_code,product_group_code,product_code for share",(identity.subsidiary_name,identity.source_strategic_city_name,codes)); caps=tuple(map(tuple,c.fetchall()))
                if not caps or {x[0] for x in caps} != set(codes): raise RegionPlanRepositoryError('SOURCE_CAPABILITY_INVALID')
                content_digest=_plan_content_digest(c,identity)
                if not stored[10] or content_digest != stored[10]: raise RegionPlanRepositoryError('PLAN_CONTENT_CHECKSUM_MISMATCH')
                digest=_generic_preview_digest(identity,int(row[0]),int(row[1]),row[2],_generic_roster_digest(masters,caps),content_digest)
            conn.rollback(); return ActivationPreview(type('Identity',(),{'plan_id':r['plan_id']})(),int(row[0]),int(row[1]),digest)
        except Exception: conn.rollback(); raise
        finally: conn.close()
    def activate(self,r: Mapping[str,Any],**_):
        identity = _generic_identity(r)
        preview_digest = str(r.get("preview_digest", ""))
        if not _SHA256.fullmatch(preview_digest): raise RegionPlanRepositoryError("ACTIVATION_PREVIEW_INVALID")
        try:
            expected_plan, expected_activation = int(r["expected_plan_revision"]), int(r["expected_activation_revision"])
        except (KeyError, TypeError, ValueError) as exc: raise RegionPlanRepositoryError("ACTIVATION_PREVIEW_STALE") from exc
        for key, code in (("idempotency_key", "IDEMPOTENCY_KEY_INVALID"), ("activated_by", "ACTIVATED_BY_INVALID"), ("activation_reference", "ACTIVATION_REFERENCE_INVALID")):
            if not str(r.get(key, "")).strip(): raise RegionPlanRepositoryError(code)
        conn=self._connection()
        try:
            with conn.cursor() as c:
                self._begin(c,r)
                # An idempotency key can replay only the still-current activation.
                c.execute("select plan_id,plan_revision,preview_digest,activation_revision,activated_by,activation_reference from common_region_plan_activation where subsidiary_name=%s and strategic_city_name=%s and idempotency_key=%s for update",(identity.subsidiary_name,identity.strategic_city_name,r['idempotency_key']))
                existing=c.fetchone()
                if existing:
                    if tuple(map(str,(existing[0],existing[2],existing[4],existing[5]))) != (identity.plan_id,preview_digest,str(r['activated_by']),str(r['activation_reference'])) or int(existing[1]) != expected_plan or int(existing[3]) != expected_activation+1: raise RegionPlanRepositoryError('ACTIVATION_IDEMPOTENCY_CONFLICT')
                    c.execute("select activation_revision from common_city_context where subsidiary_name=%s and strategic_city_name=%s for update",(identity.subsidiary_name,identity.strategic_city_name)); context=c.fetchone()
                    c.execute("select plan_id,activation_revision from common_region_plan_activation where subsidiary_name=%s and strategic_city_name=%s and active_flag for update",(identity.subsidiary_name,identity.strategic_city_name)); active=c.fetchone()
                    if not context or not active or int(context[0]) != int(existing[3]) or str(active[0]) != identity.plan_id or int(active[1]) != int(existing[3]): raise RegionPlanRepositoryError('ACTIVATION_IDEMPOTENCY_STALE')
                    conn.rollback(); return ActivationResult('already_active',identity.plan_id,int(existing[3]),preview_digest)
                c.execute("select revision,activation_revision,plan_status,policy_version,source_sha256,manifest_sha256,bundle_sha256,membership_accepted_rows,membership_rejected_rows,unique_postal_count,technician_count,source_strategic_city_name,verified_content_sha256 from common_region_plan p join common_city_context cc using(subsidiary_name,strategic_city_name) where p.subsidiary_name=%s and p.strategic_city_name=%s and p.plan_id=%s for update",(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)); plan=c.fetchone()
                if not plan or plan[2] not in ('reviewed','superseded') or tuple(map(str,plan[3:7])) != (identity.policy_version,identity.source_sha256,identity.manifest_sha256,identity.bundle_sha256) or tuple(map(int,plan[7:11])) != (identity.postal_count + identity.boundary_resolution_count,0,identity.postal_count,identity.technician_count) or plan[11] != identity.source_strategic_city_name: raise RegionPlanRepositoryError('PLAN_IDENTITY_MISMATCH')
                revision, activation = int(plan[0]), int(plan[1])
                c.execute("select (select plan_id from common_region_plan_activation where subsidiary_name=%s and strategic_city_name=%s and active_flag)",(identity.subsidiary_name,identity.strategic_city_name)); active_id=c.fetchone()[0]
                c.execute("select m.employee_code,m.employee_name,m.center_type,m.home_address,m.home_city,m.home_state,m.home_country,m.home_postal_code,m.home_latitude,m.home_longitude,m.active_flag,m.priority_group,m.max_home_to_job_min from common_region_plan_technician t join common_technician_master m on m.subsidiary_name=t.subsidiary_name and m.strategic_city_name=%s and m.employee_code=t.employee_code join common_region_plan_region pr on pr.subsidiary_name=t.subsidiary_name and pr.strategic_city_name=t.strategic_city_name and pr.plan_id=t.plan_id and pr.region_seq=t.assigned_region_seq where t.subsidiary_name=%s and t.strategic_city_name=%s and t.plan_id=%s and t.active_flag and m.active_flag and m.center_type=pr.required_center_type order by m.employee_code for share",(identity.source_strategic_city_name,identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)); masters=tuple(map(tuple,c.fetchall()))
                if len(masters)!=identity.technician_count or len({x[0] for x in masters}) != len(masters): raise RegionPlanRepositoryError('SOURCE_ROSTER_INVALID')
                codes=[x[0] for x in masters]; c.execute("select employee_code,product_group_code,product_code,repair_allowed,heavy_repair_allowed,priority_score,effective_start_date,effective_end_date from common_technician_capability_master where subsidiary_name=%s and strategic_city_name=%s and employee_code=any(%s) order by employee_code,product_group_code,product_code for share",(identity.subsidiary_name,identity.source_strategic_city_name,codes)); caps=tuple(map(tuple,c.fetchall()))
                if not caps or {x[0] for x in caps} != set(codes): raise RegionPlanRepositoryError('SOURCE_CAPABILITY_INVALID')
                content_digest=_plan_content_digest(c,identity)
                if not plan[12] or content_digest != plan[12]: raise RegionPlanRepositoryError('PLAN_CONTENT_CHECKSUM_MISMATCH')
                actual=_generic_preview_digest(identity,revision,activation,active_id,_generic_roster_digest(masters,caps),content_digest)
                if revision != expected_plan or activation != expected_activation or actual != preview_digest: raise RegionPlanRepositoryError('ACTIVATION_PREVIEW_STALE')
                # Region membership is read from the immutable Area Plan v2
                # tables.  Do not project it into legacy common_region_master;
                # that table previously made deleted/hardcoded regions appear
                # active again.
                c.execute("delete from common_technician_capability_master where subsidiary_name=%s and strategic_city_name=%s",(identity.subsidiary_name,identity.strategic_city_name)); c.execute("delete from common_technician_master where subsidiary_name=%s and strategic_city_name=%s",(identity.subsidiary_name,identity.strategic_city_name)); c.executemany("insert into common_technician_master(subsidiary_name,strategic_city_name,employee_code,employee_name,center_type,home_address,home_city,home_state,home_country,home_postal_code,home_latitude,home_longitude,active_flag,priority_group,max_home_to_job_min) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",[(identity.subsidiary_name,identity.strategic_city_name,*x) for x in masters])
                if c.rowcount != identity.technician_count: raise RegionPlanRepositoryError('ACTIVATION_TECHNICIAN_PROJECTION_INVALID')
                c.executemany("insert into common_technician_capability_master(subsidiary_name,strategic_city_name,employee_code,product_group_code,product_code,repair_allowed,heavy_repair_allowed,priority_score,effective_start_date,effective_end_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",[(identity.subsidiary_name,identity.strategic_city_name,*x) for x in caps])
                if c.rowcount != len(caps): raise RegionPlanRepositoryError('ACTIVATION_CAPABILITY_PROJECTION_INVALID')
                next_rev=activation+1; c.execute("update common_region_plan_activation set active_flag=false,superseded_at=now() where subsidiary_name=%s and strategic_city_name=%s and active_flag",(identity.subsidiary_name,identity.strategic_city_name)); c.execute("update common_region_plan set plan_status='superseded',revision=revision+1 where subsidiary_name=%s and strategic_city_name=%s and plan_status='active' and plan_id<>%s",(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id)); c.execute("update common_region_plan set plan_status='active',revision=revision+1 where subsidiary_name=%s and strategic_city_name=%s and plan_id=%s and plan_status in ('reviewed','superseded') and revision=%s",(identity.subsidiary_name,identity.strategic_city_name,identity.plan_id,revision))
                if c.rowcount != 1: raise RegionPlanRepositoryError('ACTIVATION_PLAN_REVISION_CONFLICT')
                c.execute("update common_city_context set activation_revision=%s,policy_version=%s,context_status='active' where subsidiary_name=%s and strategic_city_name=%s and activation_revision=%s",(next_rev,identity.policy_version,identity.subsidiary_name,identity.strategic_city_name,activation))
                if c.rowcount != 1: raise RegionPlanRepositoryError('ACTIVATION_REVISION_CONFLICT')
                c.execute("insert into common_region_plan_activation(subsidiary_name,strategic_city_name,activation_revision,plan_id,plan_revision,preview_digest,idempotency_key,activated_by,activation_reference) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)",(identity.subsidiary_name,identity.strategic_city_name,next_rev,identity.plan_id,revision,preview_digest,r['idempotency_key'],r['activated_by'],r['activation_reference']))
                c.execute("select to_regclass('public.common_routing_plan')")
                if c.fetchone()[0] is not None:
                    c.execute("select region_set_id from common_routing_plan where subsidiary_name=%s and strategic_city_name=%s and routing_plan_id=%s", (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id))
                    normalized_plan = c.fetchone()
                    if normalized_plan:
                        c.execute("update common_routing_plan_activation set active_flag=false where subsidiary_name=%s and strategic_city_name=%s and active_flag", (identity.subsidiary_name, identity.strategic_city_name))
                        c.execute("update common_routing_plan set plan_status='superseded',revision=revision+1,updated_at=now() where subsidiary_name=%s and strategic_city_name=%s and plan_status='active' and routing_plan_id<>%s", (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id))
                        c.execute("update common_routing_plan set plan_status='active',revision=revision+1,updated_at=now() where subsidiary_name=%s and strategic_city_name=%s and routing_plan_id=%s", (identity.subsidiary_name, identity.strategic_city_name, identity.plan_id))
                        c.execute("insert into common_routing_plan_activation(subsidiary_name,strategic_city_name,activation_revision,routing_plan_id,activated_by,activation_reference,active_flag) values(%s,%s,%s,%s,%s,%s,true)", (identity.subsidiary_name, identity.strategic_city_name, next_rev, identity.plan_id, r['activated_by'], r['activation_reference']))
            conn.commit(); return ActivationResult('activated',identity.plan_id,next_rev,preview_digest)
        except Exception: conn.rollback(); raise
        finally: conn.close()

