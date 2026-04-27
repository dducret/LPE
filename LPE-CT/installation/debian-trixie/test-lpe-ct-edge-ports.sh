#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
HOST="${HOST:-127.0.0.1}"
SERVICE_NAME="${SERVICE_NAME:-lpe-ct.service}"
SERVICE_USER="${SERVICE_USER:-lpe-ct}"

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

[[ -f "$ENV_FILE" ]] || fail "Environment file not found: $ENV_FILE"
set -a
source "$ENV_FILE"
set +a

listener_snapshot() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    echo "[DIAG] Listening sockets matching :${port}:"
    ss -ltnp 2>/dev/null | grep -E "[:.]${port}[[:space:]]" || true
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

service_user_can_read() {
  local path="$1"
  if [[ -z "${path}" ]]; then
    return 1
  fi
  if command -v sudo >/dev/null 2>&1; then
    sudo -u "${SERVICE_USER}" test -r "${path}" >/dev/null 2>&1
  elif command -v runuser >/dev/null 2>&1; then
    runuser -u "${SERVICE_USER}" -- test -r "${path}" >/dev/null 2>&1
  else
    test -r "${path}"
  fi
}

check_tls_file_readable() {
  local label="$1"
  local path="$2"
  [[ -n "${path}" ]] || return 0
  if service_user_can_read "${path}"; then
    pass "${label} is readable by ${SERVICE_USER}: ${path}"
    return 0
  fi
  echo "[DIAG] ${label} is not readable by ${SERVICE_USER}: ${path}"
  if command -v namei >/dev/null 2>&1; then
    echo "[DIAG] Path permissions for ${path}:"
    namei -l "${path}" || true
  fi
  fail "${label} must be readable by ${SERVICE_USER}; fix owner/group/mode under /etc/lpe-ct/tls before testing edge ports."
}

diagnose_tcp_failure() {
  local name="$1"
  local host="$2"
  local port="$3"
  echo "[DIAG] ${name} TCP probe failed on ${host}:${port}."
  echo "[DIAG] ENV_FILE=${ENV_FILE}"
  echo "[DIAG] LPE_CT_SMTP_BIND_ADDRESS=${LPE_CT_SMTP_BIND_ADDRESS:-unset}"
  echo "[DIAG] LPE_CT_SMTP_HOST=${LPE_CT_SMTP_HOST:-unset}"
  echo "[DIAG] LPE_CT_SMTP_PORT=${LPE_CT_SMTP_PORT:-unset}"
  listener_snapshot "${port}"
  service_snapshot
  recent_logs
  if [[ "${port}" =~ ^[0-9]+$ && "${port}" -lt 1024 ]]; then
    echo "[DIAG] Port ${port} is privileged. The systemd unit must grant CAP_NET_BIND_SERVICE to ${SERVICE_NAME}."
  fi
  warn "If LPE_CT_SMTP_BIND_ADDRESS is a specific LAN IP, rerun this test with HOST=<that-ip>."
}

tcp_probe() {
  local name="$1"
  local host="$2"
  local port="$3"
  timeout 5 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1 \
    || {
      diagnose_tcp_failure "${name}" "${host}" "${port}"
      fail "${name} port ${port} is not reachable on ${host}"
    }
  pass "${name} port ${port} is reachable on ${host}"
}

tls_probe_if_possible() {
  local host="$1"
  local port="$2"
  local name="$3"
  if ! command -v openssl >/dev/null 2>&1; then
    echo "[SKIP] openssl is not available; TCP reachability for ${name} was checked, TLS handshake was not."
    return
  fi
  timeout 10 openssl s_client -connect "${host}:${port}" -servername "${LPE_CT_PUBLIC_HOSTNAME:-localhost}" </dev/null 2>/dev/null \
    | grep -q "BEGIN CERTIFICATE" \
    || fail "${name} on ${host}:${port} did not present a TLS certificate"
  pass "${name} on ${host}:${port} presented a TLS certificate"
}

SMTP_BIND="${LPE_CT_SMTP_BIND_ADDRESS:-}"
if [[ -n "${LPE_CT_SMTP_PORT:-}" ]]; then
  SMTP_PORT="${LPE_CT_SMTP_PORT}"
elif [[ -n "${SMTP_BIND}" && "${SMTP_BIND}" == *:* ]]; then
  SMTP_PORT="${SMTP_BIND##*:}"
else
  SMTP_PORT="25"
fi
HTTPS_PORT="${LPE_CT_NGINX_LISTEN_PORT:-443}"
SUBMISSION_BIND="${LPE_CT_SUBMISSION_BIND_ADDRESS:-}"
IMAPS_BIND="${LPE_CT_IMAPS_BIND_ADDRESS:-}"
SUBMISSION_PORT="${SUBMISSION_BIND##*:}"
IMAPS_PORT="${IMAPS_BIND##*:}"

check_tls_file_readable "public TLS certificate" "${LPE_CT_PUBLIC_TLS_CERT_PATH:-}"
check_tls_file_readable "public TLS private key" "${LPE_CT_PUBLIC_TLS_KEY_PATH:-}"
if [[ -n "${LPE_CT_SUBMISSION_BIND_ADDRESS:-}" ]]; then
  check_tls_file_readable "submission TLS certificate" "${LPE_CT_SUBMISSION_TLS_CERT_PATH:-${LPE_CT_PUBLIC_TLS_CERT_PATH:-}}"
  check_tls_file_readable "submission TLS private key" "${LPE_CT_SUBMISSION_TLS_KEY_PATH:-${LPE_CT_PUBLIC_TLS_KEY_PATH:-}}"
fi
if [[ -n "${LPE_CT_IMAPS_BIND_ADDRESS:-}" ]]; then
  check_tls_file_readable "IMAPS TLS certificate" "${LPE_CT_IMAPS_TLS_CERT_PATH:-${LPE_CT_PUBLIC_TLS_CERT_PATH:-}}"
  check_tls_file_readable "IMAPS TLS private key" "${LPE_CT_IMAPS_TLS_KEY_PATH:-${LPE_CT_PUBLIC_TLS_KEY_PATH:-}}"
fi

tcp_probe "SMTP ingress" "$HOST" "${SMTP_PORT:-25}"

curl --silent --show-error --fail --insecure "https://${HOST}:${HTTPS_PORT}/" >/dev/null \
  || fail "HTTPS management edge is not reachable on ${HOST}:${HTTPS_PORT}"
pass "HTTPS management edge is reachable on ${HOST}:${HTTPS_PORT}"
tls_probe_if_possible "$HOST" "$HTTPS_PORT" "HTTPS"

if [[ -n "${LPE_CT_SUBMISSION_BIND_ADDRESS:-}" ]]; then
  tcp_probe "SMTPS submission" "$HOST" "$SUBMISSION_PORT"
  tls_probe_if_possible "$HOST" "$SUBMISSION_PORT" "SMTPS submission"
else
  echo "[SKIP] LPE_CT_SUBMISSION_BIND_ADDRESS is not configured; port 465 is intentionally not enabled."
fi

if [[ -n "${LPE_CT_IMAPS_BIND_ADDRESS:-}" ]]; then
  tcp_probe "IMAPS" "$HOST" "$IMAPS_PORT"
  tls_probe_if_possible "$HOST" "$IMAPS_PORT" "IMAPS"
else
  echo "[SKIP] LPE_CT_IMAPS_BIND_ADDRESS is not configured; port 993 is not enabled."
fi

echo "LPE-CT edge port test completed successfully."
