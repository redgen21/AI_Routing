import unittest
import io
from types import SimpleNamespace
from unittest.mock import patch

from services.api import region_plan_v2 as api
from services.api.region_plan_repository_v2 import GenericRegionPlanLifecycleRepository, RegionPlanRepositoryError
from smart_routing.common_vrp_api_server import _region_plan_v2_headers, _region_plan_v2_mutation_allowed, _region_plan_v2_route
from smart_routing.common_vrp_api_server import CommonVRPRequestHandler


def payload(**extra):
    value = dict(subsidiary_name="LGEAI", strategic_city_name="Phoenix_4area", source_strategic_city_name="Phoenix, AZ",
                 plan_id="rp2_phx", policy_version="explicit_workbook_membership/v1", source_sha256="a"*64,
                 manifest_sha256="b"*64, bundle_sha256="c"*64, region_count=4, postal_count=99,
                 technician_count=12, boundary_resolution_count=0, plan_revision=0, activation_revision=1)
    value.update(extra); return value


class Repo:
    def review(self, request, **_):
        self.request = request; return SimpleNamespace(plan_id=request["plan_id"], revision=1, status="reviewed")
    def preview(self, request, **_):
        return SimpleNamespace(identity=SimpleNamespace(plan_id=request["plan_id"]), plan_revision=1, expected_activation_revision=1, preview_digest="d"*64)
    def activate(self, request, **_):
        return SimpleNamespace(plan_id=request["plan_id"], activation_revision=2, status="activated")


class RegionPlanV2ApiTests(unittest.TestCase):
    def test_legacy_city_names_are_valid_scope_keys(self):
        api._validate_scope_identifiers({"subsidiary_id": "LGEAI", "target_city_id": "Atlanta, GA"})

    def test_city_registry_includes_legacy_region_only_city(self):
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def execute(self, *_): pass
            def fetchall(self): return [("LGEAI", "Atlanta, GA", 137, 0, ["region_master"])]

        class Connection:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def cursor(self): return Cursor()

        with patch.object(api, "get_db_connection", return_value=Connection()):
            status, result = api.handle("cities", {}, config_path="injected.json")
        self.assertEqual(200, status)
        city = result["data"]["cities"][0]
        self.assertEqual("Atlanta, GA", city["source_city_id"])
        self.assertEqual("needs_review", city["migration_status"])
        self.assertEqual(["region_master"], city["registry_sources"])

    def test_console_routes_and_headers(self):
        self.assertEqual("imports", _region_plan_v2_route("/api/region-plans/v2/imports", {})[0])
        self.assertEqual("list", _region_plan_v2_route("/api/region-plans/v2/plans/list", {})[0])
        handler = SimpleNamespace(headers={"X-Authenticated-Principal":"console-user", "Idempotency-Key":"idem-1", "If-Match":"\"7\""})
        bound = _region_plan_v2_headers(handler, {"principal":"spoofed", "plan_revision":1})
        self.assertEqual(("deployment-console","idem-1",7), (bound["principal"],bound["idempotency_key"],bound["plan_revision"]))

    @patch("smart_routing.common_vrp_api_server.load_common_config", return_value={"environment":"development", "database":{"dbname":"vrp_db_dev"}})
    def test_mutations_are_loopback_development_only(self, _config):
        self.assertTrue(_region_plan_v2_mutation_allowed(SimpleNamespace(client_address=("127.0.0.1", 1)), "imports"))
        self.assertFalse(_region_plan_v2_mutation_allowed(SimpleNamespace(client_address=("20.51.244.68", 1)), "imports"))

    @patch("smart_routing.common_vrp_api_server.load_common_config", return_value={"environment":"production", "database":{"dbname":"vrp_db"}})
    def test_production_mutation_is_rejected(self, _config):
        self.assertFalse(_region_plan_v2_mutation_allowed(SimpleNamespace(client_address=("127.0.0.1", 1)), "activate"))

    @patch("smart_routing.common_vrp_api_server.load_common_config", return_value={"environment":"development", "database":{"dbname":"vrp_db_dev"}})
    @patch("smart_routing.common_vrp_api_server.region_plan_v2_handle")
    @patch("smart_routing.common_vrp_api_server._json_response")
    def test_external_post_is_rejected_before_repository_dispatch(self, response, dispatch, _config):
        handler = object.__new__(CommonVRPRequestHandler)
        handler.path = "/api/region-plans/v2/imports"
        handler.client_address = ("20.51.244.68", 1234)
        handler.headers = {"Content-Length": "2", "X-Authenticated-Principal": "spoofed"}
        handler.rfile = io.BytesIO(b"{}")
        handler.do_POST()
        dispatch.assert_not_called()
        self.assertEqual(403, int(response.call_args.args[1]))

    @patch("services.api.region_plan_v2.load_common_config", return_value={"environment":"development", "database":{"dbname":"vrp_db_dev"}})
    @patch("services.api.region_plan_v2._adopt")
    def test_dynamic_non_la_review_preview_activate(self, adopt, _config):
        def authoritative(revision):
            return dict(subsidiary_name="LGEAI", strategic_city_name="Phoenix_4area", source_strategic_city_name="Phoenix, AZ",
                        plan_id="rp2_phx", policy_version="explicit_workbook_membership/v1", source_sha256="a"*64,
                        manifest_sha256="b"*64, bundle_sha256="c"*64, region_count=4, postal_count=99,
                        technician_count=12, boundary_resolution_count=0, plan_revision=revision, activation_revision=1,
                        plan_status="candidate", reviewed_by=None, review_reference=None)
        adopt.side_effect = [authoritative(0), authoritative(1), authoritative(1)]
        repo = Repo()
        status, result = api.handle("review", payload(principal="operator", idempotency_key="review-key"), repository=repo, config_path="injected.json")
        self.assertEqual((200, "reviewed", 4, 99), (status, result["data"]["lifecycle"], repo.request["region_count"], repo.request["postal_count"]))
        status, result = api.handle("activation-preview", payload(plan_revision=1, principal="operator"), repository=repo, config_path="injected.json")
        self.assertEqual((200, "d"*64), (status, result["data"]["preview_token"]))
        status, result = api.handle("activate", payload(plan_revision=1, preview_token="d"*64, idempotency_key="key", principal="operator"), repository=repo, config_path="injected.json")
        self.assertEqual((200, 2), (status, result["data"]["activation_revision"]))

    def test_identity_has_no_la_defaults_or_caller_sql(self):
        with self.assertRaises(KeyError): api._identity({key: value for key, value in payload().items() if key != "postal_count"})
        self.assertNotIn("sql", api._identity(payload()))
        self.assertNotIn("path", api._identity(payload()))

    def test_adopt_rejects_identity_mismatch(self):
        row = ("LGEAI", "LA_6area", "Los Angeles, CA", "la", "active_roster_area_type_fallback_region_soft/v1", "a"*64, "b"*64, "c"*64, 0, 1, 413, 0, 412, 54, "candidate", None, None, None, None, None, 6, 0, 54)
        cursor = SimpleNamespace(execute=lambda *args: None, fetchone=lambda: row, fetchall=lambda: [])
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def cursor(self):
                class C:
                    def __enter__(self): return cursor
                    def __exit__(self, *_): pass
                return C()
        with patch("services.api.region_plan_v2.get_db_connection", return_value=Conn()):
            with self.assertRaisesRegex(api.RegionPlanV2Error, "PLAN_IDENTITY_MISMATCH"):
                api._adopt({"subsidiary_id":"LGEAI", "target_city_id":"LA_6area", "plan_id":"la"}, config_path="injected.json")

    def test_adopt_rejects_bound_child_content_drift(self):
        row = ("LGEAI", "LA_6area", "Los Angeles, CA", "la", "active_roster_area_type_fallback_region_soft/v1", "a"*64, "b"*64, "c"*64, 0, 1, 413, 0, 413, 54, "candidate", None, None, "f"*64, "operator", None, 6, 0, 54)
        cursor = SimpleNamespace(execute=lambda *args: None, fetchone=lambda: row, fetchall=lambda: [])
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def cursor(self):
                class C:
                    def __enter__(self): return cursor
                    def __exit__(self, *_): pass
                return C()
        with patch("services.api.region_plan_v2.get_db_connection", return_value=Conn()):
            with self.assertRaisesRegex(api.RegionPlanV2Error, "PLAN_CONTENT_CHECKSUM_MISMATCH"):
                api._adopt({"subsidiary_id":"LGEAI", "target_city_id":"LA_6area", "plan_id":"la"}, config_path="injected.json")



class _Cursor:
    def __init__(self, rows, rowcount=1): self.rows=list(rows); self.rowcount=rowcount; self.sql=[]
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def execute(self, sql, *_): self.sql.append(sql)
    def executemany(self, sql, *_): self.sql.append(sql)
    def fetchone(self): return self.rows.pop(0) if self.rows else None
    def fetchall(self): return self.rows.pop(0) if self.rows else []
class _Conn:
    def __init__(self, cursor): self.cursor_value=cursor; self.commits=0; self.rollbacks=0; self.closed=False
    def cursor(self): return self.cursor_value
    def commit(self): self.commits+=1
    def rollback(self): self.rollbacks+=1
    def close(self): self.closed=True

class RuntimeActivationReplayTests(unittest.TestCase):
    def _request(self, **extra):
        r=payload(contract_version='region-plan-lifecycle-request/v1', expected_plan_revision=1, expected_activation_revision=4, preview_digest='d'*64, idempotency_key='idem', activated_by='tester', activation_reference='ref')
        r.update(extra); return r
    def test_exact_current_replay_returns_already_active_without_projection(self):
        c=_Cursor([('rp2_phx',1,'d'*64,5,'tester','ref'), (5,), ('rp2_phx',5)])
        conn=_Conn(c); got=GenericRegionPlanLifecycleRepository(lambda: conn).activate(self._request())
        self.assertEqual(('already_active',5),(got.status,got.activation_revision)); self.assertEqual(0,conn.commits); self.assertGreaterEqual(conn.rollbacks,1)
        self.assertFalse(any('common_region_master' in sql for sql in c.sql))
    def test_historical_replay_is_stale_and_rolls_back(self):
        c=_Cursor([('rp2_phx',1,'d'*64,5,'tester','ref'), (6,), ('different',6)])
        conn=_Conn(c)
        with self.assertRaisesRegex(RegionPlanRepositoryError,'ACTIVATION_IDEMPOTENCY_STALE'):
            GenericRegionPlanLifecycleRepository(lambda: conn).activate(self._request())
        self.assertEqual(0,conn.commits); self.assertGreaterEqual(conn.rollbacks,1)
    def test_invalid_preview_rejected_before_any_pointer_change(self):
        c=_Cursor([]); conn=_Conn(c)
        with self.assertRaisesRegex(RegionPlanRepositoryError,'ACTIVATION_PREVIEW_INVALID'):
            GenericRegionPlanLifecycleRepository(lambda: conn).activate(self._request(preview_digest='bad'))
        self.assertEqual([],c.sql); self.assertEqual(0,conn.commits)

