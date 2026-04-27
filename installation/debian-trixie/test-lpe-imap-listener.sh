#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"
HOST="${HOST:-}"
PORT="${PORT:-}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

warn() {
  echo "[WARN] $*" >&2
}

pass() {
  echo "[OK] $*"
}

[[ -f "${ENV_FILE}" ]] || fail "Environment file not found: ${ENV_FILE}"

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

IMAP_BIND_ADDRESS="${LPE_IMAP_BIND_ADDRESS:-127.0.0.1:1143}"
IMAP_HOST="${HOST:-${LPE_IMAP_BIND_HOST:-${IMAP_BIND_ADDRESS%:*}}}"
IMAP_PORT="${PORT:-${LPE_IMAP_BIND_PORT:-${IMAP_BIND_ADDRESS##*:}}}"
CONNECT_HOST="${IMAP_HOST}"

if [[ -z "${IMAP_HOST}" || -z "${IMAP_PORT}" || "${IMAP_HOST}" == "${IMAP_BIND_ADDRESS}" ]]; then
  fail "LPE_IMAP_BIND_ADDRESS must be a host:port address, got: ${IMAP_BIND_ADDRESS}"
fi

if [[ "${CONNECT_HOST}" == "0.0.0.0" ]]; then
  CONNECT_HOST="127.0.0.1"
fi

listener_snapshot() {
  if command -v ss >/dev/null 2>&1; then
    echo "[DIAG] Listening sockets matching :${IMAP_PORT}:"
    ss -ltnp 2>/dev/null | grep -E "[:.]${IMAP_PORT}[[:space:]]" || true
  else
    echo "[DIAG] ss is not available; cannot list listening sockets."
  fi
}

service_snapshot() {
  if command -v systemctl >/dev/null 2>&1; then
    echo "[DIAG] ${SERVICE_NAME} active state:"
    systemctl is-active "${SERVICE_NAME}" 2>/dev/null || true
    echo "[DIAG] ${SERVICE_NAME} status summary:"
    systemctl --no-pager --lines=8 status "${SERVICE_NAME}" 2>/dev/null || true
  fi
}

recent_logs() {
  if command -v journalctl >/dev/null 2>&1; then
    echo "[DIAG] Recent ${SERVICE_NAME} logs:"
    journalctl -u "${SERVICE_NAME}" --no-pager -n 30 2>/dev/null || true
  fi
}

if ! timeout 5 bash -c ":</dev/tcp/${CONNECT_HOST}/${IMAP_PORT}" >/dev/null 2>&1; then
  echo "[DIAG] ENV_FILE=${ENV_FILE}"
  echo "[DIAG] LPE_IMAP_BIND_ADDRESS=${LPE_IMAP_BIND_ADDRESS:-unset}"
  echo "[DIAG] Tested IMAP endpoint=${CONNECT_HOST}:${IMAP_PORT}"
  listener_snapshot
  service_snapshot
  recent_logs
  fail "Core LPE IMAP listener is not reachable on ${CONNECT_HOST}:${IMAP_PORT}"
fi

pass "Core LPE IMAP listener is reachable on ${CONNECT_HOST}:${IMAP_PORT}"

case "${IMAP_HOST}" in
  127.*|localhost)
    warn "This listener is loopback-only. A separate LPE-CT host cannot reach it; set LPE_IMAP_BIND_ADDRESS to the core LPE private LAN address, for example 192.168.1.25:1143."
    ;;
  0.0.0.0|::)
    warn "This listener accepts connections on all interfaces. Restrict firewall access to LPE-CT only."
    ;;
esac

echo "LPE core IMAP listener test completed successfully."
