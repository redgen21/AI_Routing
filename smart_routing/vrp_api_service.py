from __future__ import annotations

import importlib
import json
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .vrp_api_common import SUPPORTED_ROUTING_MODES, normalize_city, normalize_mode


JOB_ROOT = Path("260310/vrp_api_jobs")
_JOB_LOCK = threading.Lock()
MODE_HANDLER_MODULES = {
    "na_general": "smart_routing.vrp_mode_na_general",
    "z_weekend": "smart_routing.vrp_mode_z_weekend",
}


@dataclass
class JobPaths:
    job_dir: Path
    request_path: Path
    status_path: Path
    result_path: Path
    error_path: Path


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _ensure_job_root() -> Path:
    JOB_ROOT.mkdir(parents=True, exist_ok=True)
    return JOB_ROOT


def build_job_paths(job_id: str) -> JobPaths:
    job_dir = _ensure_job_root() / str(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    return JobPaths(
        job_dir=job_dir,
        request_path=job_dir / "request.json",
        status_path=job_dir / "status.json",
        result_path=job_dir / "result.json",
        error_path=job_dir / "error.txt",
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def create_job_id(request_id: str | None = None) -> str:
    base_id = str(request_id).strip() if request_id else ""
    if base_id:
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in base_id)
        return f"{safe}_{uuid.uuid4().hex[:8]}"
    return f"vrp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"


def save_new_job(job_id: str, request_payload: dict[str, Any]) -> None:
    paths = build_job_paths(job_id)
    _write_json(paths.request_path, request_payload)
    _write_json(
        paths.status_path,
        {
            "job_id": job_id,
            "status": "queued",
            "mode": normalize_mode(request_payload.get("mode")),
            "city": normalize_city(request_payload.get("city")),
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
        },
    )


def load_status(job_id: str) -> dict[str, Any]:
    paths = build_job_paths(job_id)
    if not paths.status_path.exists():
        raise FileNotFoundError(job_id)
    status = _read_json(paths.status_path)
    if paths.error_path.exists():
        status["error_message"] = paths.error_path.read_text(encoding="utf-8").strip()
    return status


def load_result(job_id: str) -> dict[str, Any]:
    paths = build_job_paths(job_id)
    if not paths.result_path.exists():
        raise FileNotFoundError(job_id)
    return _read_json(paths.result_path)


def _update_status(job_id: str, **fields: Any) -> None:
    with _JOB_LOCK:
        paths = build_job_paths(job_id)
        status = _read_json(paths.status_path) if paths.status_path.exists() else {"job_id": job_id}
        status.update(fields)
        status["updated_at"] = _utc_now_iso()
        _write_json(paths.status_path, status)


def _load_mode_handler(mode: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    module_name = MODE_HANDLER_MODULES.get(mode)
    if not module_name:
        raise NotImplementedError(f"Routing mode not implemented yet: {mode}")
    module = importlib.import_module(module_name)
    handler = getattr(module, "run_mode", None)
    if not callable(handler):
        raise RuntimeError(f"Invalid routing handler for mode: {mode}")
    return handler


def run_routing_request(request_payload: dict[str, Any]) -> dict[str, Any]:
    mode = normalize_mode(request_payload.get("mode"))
    if mode not in SUPPORTED_ROUTING_MODES:
        raise ValueError(f"Unsupported routing mode: {mode}")
    return _load_mode_handler(mode)(request_payload)


def run_vrp_request(request_payload: dict[str, Any]) -> dict[str, Any]:
    return run_routing_request(request_payload)


def process_job(job_id: str) -> None:
    paths = build_job_paths(job_id)
    request_payload = _read_json(paths.request_path)
    mode = normalize_mode(request_payload.get("mode"))
    city = normalize_city(request_payload.get("city"))
    _update_status(job_id, status="running", started_at=_utc_now_iso())
    try:
        result_payload = run_routing_request(request_payload)
        _write_json(paths.result_path, result_payload)
        _update_status(
            job_id,
            status="completed",
            completed_at=_utc_now_iso(),
            request_id=str(request_payload.get("request_id", "")).strip(),
            mode=mode,
            city=city,
            summary=result_payload.get("summary", {}),
        )
    except Exception as exc:
        paths.error_path.write_text(str(exc), encoding="utf-8")
        _update_status(job_id, status="failed", completed_at=_utc_now_iso(), mode=mode, city=city)
        raise
