from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .common_vrp_db import (
    delete_avoid_area,
    delete_job,
    delete_technician_master,
    get_routing_config,
    init_schema,
    list_routing_request_dates,
    list_capabilities,
    list_avoid_areas,
    list_contexts,
    list_engineers,
    list_jobs,
    list_request_technicians,
    list_regions,
    replace_request_technicians,
    upsert_jobs,
    upsert_avoid_area,
    upsert_routing_config,
    upsert_technician_master,
)
from .common_vrp_runtime import (
    build_payload_from_inputs,
    get_latest_routing_snapshot,
    refresh_routing_result,
    submit_routing_from_payload,
    submit_routing_from_inputs,
)


def _build_payload_debug(payload: dict) -> dict:
    jobs = list(payload.get("jobs", []))
    technicians = list(payload.get("technicians", []))
    capabilities = list(payload.get("capabilities", []))
    heavy_jobs = [job for job in jobs if bool(job.get("is_heavy_repair", False))]
    technician_codes = {
        str(tech.get("employee_code", "")).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
    }
    technician_names = {
        str(tech.get("employee_code", "")).strip(): str(tech.get("employee_name", tech.get("employee_code", ""))).strip()
        for tech in technicians
        if str(tech.get("employee_code", "")).strip()
    }
    technician_slots: dict[str, int] = {}
    for tech in technicians:
        code = str(tech.get("employee_code", "")).strip()
        if not code:
            continue
        try:
            slot_count = int(float(tech.get("slot_count", tech.get("max_slots", 8)) or 8))
        except Exception:
            slot_count = 8
        technician_slots[code] = max(0, slot_count)
    capability_products = {
        (
            str(row.get("product_group_code", "")).strip().upper(),
            str(row.get("product_code", "")).strip().upper(),
        )
        for row in capabilities
        if str(row.get("product_group_code", "")).strip() and str(row.get("product_code", "")).strip()
    }
    capability_groups = {
        str(row.get("product_group_code", "")).strip().upper()
        for row in capabilities
        if str(row.get("product_group_code", "")).strip() and not str(row.get("product_code", "")).strip()
    }
    capability_lookup: dict[tuple[str, str], set[str]] = {}
    group_capability_lookup: dict[str, set[str]] = {}
    for row in capabilities:
        product_key = (
            str(row.get("product_group_code", "")).strip().upper(),
            str(row.get("product_code", "")).strip().upper(),
        )
        employee_code = str(row.get("employee_code", "")).strip()
        if product_key[0] and employee_code:
            if product_key[1]:
                capability_lookup.setdefault(product_key, set()).add(employee_code)
            else:
                group_capability_lookup.setdefault(product_key[0], set()).add(employee_code)
    job_product_counts: dict[str, int] = {}
    unmatched_product_counts: dict[str, int] = {}
    no_candidate_receipts: list[str] = []
    fixed_unavailable_receipts: list[str] = []
    fixed_slots_by_employee: dict[str, int] = {}
    fixed_jobs_by_employee: dict[str, int] = {}
    total_job_slots = 0
    total_reschedule_slots = 0
    for job in jobs:
        product_key = (
            str(job.get("product_group", "")).strip().upper(),
            str(job.get("product", "")).strip().upper(),
        )
        product_label = f"{product_key[0]}/{product_key[1]}"
        job_product_counts[product_label] = job_product_counts.get(product_label, 0) + 1
        if product_key not in capability_products and product_key[0] not in capability_groups:
            unmatched_product_counts[product_label] = unmatched_product_counts.get(product_label, 0) + 1
        explicit_eligible = job.get("eligible_employee_codes")
        has_explicit_eligible = isinstance(explicit_eligible, list) and len(explicit_eligible) > 0
        if (
            not has_explicit_eligible
            and not capability_lookup.get(product_key)
            and not group_capability_lookup.get(product_key[0])
        ):
            no_candidate_receipts.append(str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip())
        try:
            job_slots = max(1, int(float(job.get("job_slot_count", 1) or 1)))
        except Exception:
            job_slots = 1
        total_job_slots += job_slots
        fixed = bool(job.get("fixed", False))
        reschedule = bool(job.get("reschedule", False)) and not fixed
        if reschedule:
            total_reschedule_slots += job_slots
        if fixed:
            employee_code = str(job.get("current_employee_code", "")).strip()
            if employee_code and employee_code not in technician_codes:
                no = str(job.get("receipt_no", "") or job.get("salesforce_id", "")).strip()
                fixed_unavailable_receipts.append(no)
            fixed_slots_by_employee[employee_code] = fixed_slots_by_employee.get(employee_code, 0) + job_slots
            fixed_jobs_by_employee[employee_code] = fixed_jobs_by_employee.get(employee_code, 0) + 1
    total_technician_slots = sum(technician_slots.values())
    fixed_capacity_violations = {
        employee_code: {
            "employee_name": technician_names.get(employee_code, employee_code),
            "fixed_jobs": fixed_jobs_by_employee.get(employee_code, 0),
            "fixed_slots": fixed_slots,
            "slot_capacity": technician_slots.get(employee_code, 8),
        }
        for employee_code, fixed_slots in sorted(fixed_slots_by_employee.items())
        if employee_code and fixed_slots > technician_slots.get(employee_code, 8)
    }
    precheck_messages: list[str] = []
    if total_job_slots > total_technician_slots:
        precheck_messages.append(f"Total slot capacity warning: job slots {total_job_slots} > technician slots {total_technician_slots}.")
    if fixed_capacity_violations:
        labels = [
            f"{info['employee_name']} ({employee_code}): fixed slots {info['fixed_slots']} > slot_count {info['slot_capacity']}"
            for employee_code, info in fixed_capacity_violations.items()
        ]
        precheck_messages.append("Fixed capacity override will be applied. " + "; ".join(labels[:5]))
    if fixed_unavailable_receipts:
        precheck_messages.append(f"Fixed technician unavailable: {len(fixed_unavailable_receipts)} fixed job(s) reference technicians not in the selected list.")
    if no_candidate_receipts:
        precheck_messages.append(f"Capability warning: {len(no_candidate_receipts)} job(s) have no eligible technician candidate.")
    if total_reschedule_slots > total_technician_slots:
        precheck_messages.append(f"Reschedule volume warning: reschedule slots {total_reschedule_slots} > technician slots {total_technician_slots}.")
    return {
        "job_count": len(jobs),
        "technician_count": len(technicians),
        "capability_count": len(capabilities),
        "total_job_slots": total_job_slots,
        "total_technician_slots": total_technician_slots,
        "total_reschedule_slots": total_reschedule_slots,
        "fixed_capacity_violations": fixed_capacity_violations,
        "fixed_unavailable_receipts": fixed_unavailable_receipts[:20],
        "jobs_without_candidate_receipts": no_candidate_receipts[:20],
        "precheck_messages": precheck_messages,
        "job_product_counts": dict(sorted(job_product_counts.items())),
        "unmatched_job_product_counts": dict(sorted(unmatched_product_counts.items())),
        "unmatched_job_product_total": sum(unmatched_product_counts.values()),
        "heavy_repair_job_count": len(heavy_jobs),
        "heavy_repair_receipts": [str(job.get("receipt_no", "")).strip() for job in heavy_jobs],
        "service_minutes_distribution": {
            str(minutes): sum(1 for job in jobs if int(job.get("service_minutes", 0) or 0) == minutes)
            for minutes in sorted({int(job.get("service_minutes", 0) or 0) for job in jobs})
        },
    }


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_json_request(handler: BaseHTTPRequestHandler) -> dict:
    content_length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(content_length) if content_length > 0 else b"{}"
    return json.loads(raw.decode("utf-8"))


def _query_value(parsed, key: str, default: str = "") -> str:
    values = parse_qs(parsed.query).get(key, [])
    return str(values[0]).strip() if values else default


class CommonVRPRequestHandler(BaseHTTPRequestHandler):
    server_version = "CommonVRPServer/1.0"

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/v1/common/jobs/bulk_upsert":
                payload = _read_json_request(self)
                saved = upsert_jobs(list(payload.get("rows", [])))
                _json_response(self, HTTPStatus.OK, {"saved_rows": saved})
                return
            if parsed.path == "/api/v1/common/jobs/delete":
                payload = _read_json_request(self)
                deleted = delete_job(
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("record_id", "")).strip(),
                )
                _json_response(self, HTTPStatus.OK, {"deleted_rows": deleted})
                return
            if parsed.path == "/api/v1/common/technicians/replace":
                payload = _read_json_request(self)
                saved = replace_request_technicians(
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("promise_date", "")).strip(),
                    list(payload.get("rows", [])),
                )
                _json_response(self, HTTPStatus.OK, {"saved_rows": saved})
                return
            if parsed.path == "/api/v1/common/engineers/upsert":
                payload = _read_json_request(self)
                saved = upsert_technician_master(payload)
                _json_response(self, HTTPStatus.OK, {"saved_rows": saved})
                return
            if parsed.path == "/api/v1/common/engineers/delete":
                payload = _read_json_request(self)
                deleted = delete_technician_master(
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("employee_code", "")).strip(),
                )
                _json_response(self, HTTPStatus.OK, {"deleted_rows": deleted})
                return
            if parsed.path == "/api/v1/common/routing-config/upsert":
                payload = _read_json_request(self)
                saved = upsert_routing_config(payload)
                _json_response(self, HTTPStatus.OK, {"saved_rows": saved})
                return
            if parsed.path == "/api/v1/common/avoid-areas/upsert":
                payload = _read_json_request(self)
                saved = upsert_avoid_area(payload)
                _json_response(self, HTTPStatus.OK, {"saved_rows": saved})
                return
            if parsed.path == "/api/v1/common/avoid-areas/delete":
                payload = _read_json_request(self)
                deleted = delete_avoid_area(
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("avoid_area_id", "")).strip(),
                )
                _json_response(self, HTTPStatus.OK, {"deleted_rows": deleted})
                return
            if parsed.path == "/api/v1/common/routing/build-payload":
                payload = _read_json_request(self)
                built = build_payload_from_inputs(
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("promise_date", "")).strip(),
                    list(payload.get("jobs", [])),
                    list(payload.get("technicians", [])),
                    list(payload.get("capabilities", [])),
                )
                _json_response(self, HTTPStatus.OK, {"payload": built, "debug": _build_payload_debug(built)})
                return
            if parsed.path == "/api/v1/common/routing/submit":
                payload = _read_json_request(self)
                result = submit_routing_from_payload(
                    dict(payload.get("payload") or {}),
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("promise_date", "")).strip(),
                )
                _json_response(self, HTTPStatus.OK, result)
                return
            if parsed.path == "/api/v1/common/routing/run":
                payload = _read_json_request(self)
                result = submit_routing_from_inputs(
                    str(payload.get("subsidiary_name", "")).strip(),
                    str(payload.get("strategic_city_name", "")).strip(),
                    str(payload.get("promise_date", "")).strip(),
                    list(payload.get("jobs", [])),
                    list(payload.get("technicians", [])),
                    list(payload.get("capabilities", [])),
                )
                response = dict(result)
                if response.get("payload"):
                    response["debug"] = _build_payload_debug(response["payload"])
                _json_response(self, HTTPStatus.OK, response)
                return
            if parsed.path == "/api/v1/common/routing/check":
                payload = _read_json_request(self)
                result = refresh_routing_result(str(payload.get("request_id", "")).strip())
                _json_response(self, HTTPStatus.OK, result)
                return
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
        except Exception as exc:
            _json_response(self, HTTPStatus.BAD_REQUEST, {"error": "INVALID_REQUEST", "message": str(exc)})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/v1/common/contexts":
                _json_response(self, HTTPStatus.OK, list_contexts())
                return
            if parsed.path == "/api/v1/common/engineers":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                df = list_engineers(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/capabilities":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                df = list_capabilities(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/jobs":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                df = list_jobs(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/technicians":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                promise_date = _query_value(parsed, "promise_date")
                df = list_request_technicians(subsidiary_name, strategic_city_name, promise_date)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/regions":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                df = list_regions(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/routing-config":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                row = get_routing_config(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"row": row})
                return
            if parsed.path == "/api/v1/common/avoid-areas":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                active_only = _query_value(parsed, "active_only").lower() in {"1", "true", "y", "yes"}
                df = list_avoid_areas(subsidiary_name, strategic_city_name, active_only=active_only)
                _json_response(self, HTTPStatus.OK, {"rows": df.to_dict("records")})
                return
            if parsed.path == "/api/v1/common/routing/history-dates":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                rows = list_routing_request_dates(subsidiary_name, strategic_city_name)
                _json_response(self, HTTPStatus.OK, {"rows": rows})
                return
            if parsed.path == "/api/v1/common/routing/latest":
                subsidiary_name = _query_value(parsed, "subsidiary_name")
                strategic_city_name = _query_value(parsed, "strategic_city_name")
                promise_date = _query_value(parsed, "promise_date")
                snapshot = get_latest_routing_snapshot(subsidiary_name, strategic_city_name, promise_date)
                _json_response(self, HTTPStatus.OK, {"snapshot": snapshot})
                return
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
        except Exception as exc:
            _json_response(self, HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "SERVER_ERROR", "message": str(exc)})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def run_server(host: str = "0.0.0.0", port: int = 8065) -> None:
    init_schema()
    server = ThreadingHTTPServer((host, int(port)), CommonVRPRequestHandler)
    print(f"Common VRP API server listening on http://{host}:{port}")
    server.serve_forever()
