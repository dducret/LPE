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

IMAP_RESPONSE=""

imap_read_until() {
  local tag="$1"
  local line
  IMAP_RESPONSE=""
  while IFS= read -r -t 5 line <&3; do
    line="${line%$'\r'}"
    IMAP_RESPONSE+="${line}"$'\n'
    if [[ "${line}" == "${tag} "* ]]; then
      return 0
    fi
  done
  return 1
}

imap_read_greeting() {
  local line
  IMAP_RESPONSE=""
  if ! IFS= read -r -t 5 line <&3; then
    return 1
  fi
  line="${line%$'\r'}"
  IMAP_RESPONSE="${line}"$'\n'
  [[ "${line}" == "* OK "* ]]
}

imap_send() {
  printf '%s\r\n' "$1" >&3
}

imap_deep_probe() {
  local test_email="${LPE_IMAP_TEST_EMAIL:-${IMAP_TEST_EMAIL:-}}"
  local test_password="${LPE_IMAP_TEST_PASSWORD:-${IMAP_TEST_PASSWORD:-}}"
  local test_email_size
  local test_password_size
  if [[ -z "${test_email}" || -z "${test_password}" ]]; then
    warn "Skipping authenticated IMAP probe. Set LPE_IMAP_TEST_EMAIL and LPE_IMAP_TEST_PASSWORD to test CAPABILITY, literal LOGIN, and SELECT INBOX."
    return 0
  fi
  test_email_size="$(printf '%s' "${test_email}" | wc -c | tr -d ' ')"
  test_password_size="$(printf '%s' "${test_password}" | wc -c | tr -d ' ')"

  if ! exec 3<>"/dev/tcp/${CONNECT_HOST}/${IMAP_PORT}"; then
    fail "Unable to open IMAP probe connection to ${CONNECT_HOST}:${IMAP_PORT}"
  fi

  if ! imap_read_greeting; then
    echo "[DIAG] IMAP greeting response:"
    printf '%s' "${IMAP_RESPONSE}"
    fail "IMAP listener did not return a valid greeting"
  fi

  imap_send "A1 CAPABILITY"
  if ! imap_read_until "A1" || [[ "${IMAP_RESPONSE}" != *"AUTH=PLAIN"* ]]; then
    echo "[DIAG] CAPABILITY response:"
    printf '%s' "${IMAP_RESPONSE}"
    fail "IMAP CAPABILITY did not complete with expected authentication support"
  fi
  pass "IMAP CAPABILITY completed"

  imap_send "A2 LOGIN {${test_email_size}}"
  if ! imap_read_until "+" || [[ "${IMAP_RESPONSE}" != *"+ Ready for literal data"* ]]; then
    echo "[DIAG] Literal username prompt response:"
    printf '%s' "${IMAP_RESPONSE}"
    fail "IMAP LOGIN did not accept a username literal"
  fi

  imap_send "${test_email} {${test_password_size}}"
  if ! imap_read_until "+" || [[ "${IMAP_RESPONSE}" != *"+ Ready for literal data"* ]]; then
    echo "[DIAG] Literal password prompt response:"
    printf '%s' "${IMAP_RESPONSE}"
    fail "IMAP LOGIN did not accept a password literal"
  fi

  imap_send "${test_password}"
  if ! imap_read_until "A2" || [[ "${IMAP_RESPONSE}" != *"A2 OK LOGIN completed"* ]]; then
    echo "[DIAG] LOGIN response:"
    printf '%s' "${IMAP_RESPONSE}"
    fail "IMAP literal LOGIN failed for ${test_email}"
  fi
  pass "IMAP literal LOGIN completed for ${test_email}"

  imap_send "A3 SELECT INBOX"
  if ! imap_read_until "A3" || [[ "${IMAP_RESPONSE}" != *"A3 OK [READ-WRITE] SELECT completed"* ]]; then
    echo "[DIAG] SELECT INBOX response:"
    printf '%s' "${IMAP_RESPONSE}"
    fail "IMAP SELECT INBOX failed for ${test_email}"
  fi
  pass "IMAP SELECT INBOX completed"

  imap_send "A4 LOGOUT"
  imap_read_until "A4" >/dev/null 2>&1 || true
  exec 3>&-
  exec 3<&-
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
imap_deep_probe

case "${IMAP_HOST}" in
  127.*|localhost)
    warn "This listener is loopback-only. A separate LPE-CT host cannot reach it; set LPE_IMAP_BIND_ADDRESS to the core LPE private LAN address, for example 192.168.1.25:1143."
    ;;
  0.0.0.0|::)
    warn "This listener accepts connections on all interfaces. Restrict firewall access to LPE-CT only."
    ;;
esac

echo "LPE core IMAP listener test completed successfully."
