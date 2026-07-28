#!/usr/bin/env bash

# Shared process and readiness helpers. The calling script must define ROOT.

select_python() {
  if [ -n "${PYTHON_BIN:-}" ]; then
    command -v "${PYTHON_BIN}" >/dev/null 2>&1 || [ -x "${PYTHON_BIN}" ] || {
      echo "Configured PYTHON_BIN is not executable: ${PYTHON_BIN}" >&2
      return 1
    }
  elif [ -x "${ROOT}/.venv/bin/python" ]; then
    PYTHON_BIN="${ROOT}/.venv/bin/python"
  else
    PYTHON_BIN="$(command -v python3)" || {
      echo "python3 is required." >&2
      return 1
    }
  fi
  export PYTHON_BIN
}

select_streamlit() {
  if [ -n "${STREAMLIT_BIN:-}" ]; then
    command -v "${STREAMLIT_BIN}" >/dev/null 2>&1 || [ -x "${STREAMLIT_BIN}" ] || {
      echo "Configured STREAMLIT_BIN is not executable: ${STREAMLIT_BIN}" >&2
      return 1
    }
  elif [ -x "${ROOT}/.venv/bin/streamlit" ]; then
    STREAMLIT_BIN="${ROOT}/.venv/bin/streamlit"
  else
    STREAMLIT_BIN="$(command -v streamlit)" || {
      echo "streamlit is required. Create .venv or set STREAMLIT_BIN." >&2
      return 1
    }
  fi
  export STREAMLIT_BIN
}

stop_matching_processes() {
  local pattern
  for pattern in "$@"; do
    if pgrep -f -- "${pattern}" >/dev/null 2>&1; then
      pkill -f -- "${pattern}" || true
    fi
  done
  for _ in 1 2 3 4 5; do
    local found=0
    for pattern in "$@"; do
      pgrep -f -- "${pattern}" >/dev/null 2>&1 && found=1
    done
    [ "${found}" -eq 0 ] && return 0
    sleep 1
  done
  for pattern in "$@"; do
    pkill -9 -f -- "${pattern}" >/dev/null 2>&1 || true
  done
}

assert_tcp_port_free() {
  local port="$1"
  "${PYTHON_BIN}" - "${port}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
with socket.socket() as sock:
    sock.settimeout(0.5)
    if sock.connect_ex(("127.0.0.1", port)) == 0:
        raise SystemExit(f"TCP port {port} is still occupied; refusing to start a conflicting service.")
PY
}

wait_for_http_status() {
  local url="$1"
  local expected_status="${2:-200}"
  local attempts="${3:-30}"
  local process_id="${4:-}"
  local attempt
  for ((attempt=1; attempt<=attempts; attempt++)); do
    if [ -n "${process_id}" ] && ! kill -0 "${process_id}" >/dev/null 2>&1; then
      return 1
    fi
    if "${PYTHON_BIN}" - "${url}" "${expected_status}" <<'PY' >/dev/null 2>&1
import sys
from urllib.error import HTTPError
from urllib.request import urlopen

url, expected = sys.argv[1], int(sys.argv[2])
try:
    with urlopen(url, timeout=2) as response:
        status = response.status
        response.read(1)
except HTTPError as exc:
    status = exc.code
raise SystemExit(0 if status == expected else 1)
PY
    then
      return 0
    fi
    sleep 1
  done
  return 1
}
