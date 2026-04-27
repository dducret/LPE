#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
HOST="${HOST:-127.0.0.1}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
}

[[ -f "$ENV_FILE" ]] || fail "Environment file not found: $ENV_FILE"
set -a
source "$ENV_FILE"
set +a

tcp_probe() {
  local host="$1"
  local port="$2"
  timeout 5 bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1 \
    || fail "Port ${port} is not reachable on ${host}"
  pass "Port ${port} is reachable on ${host}"
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

SMTP_PORT="${LPE_CT_SMTP_PORT:-${LPE_CT_SMTP_BIND_ADDRESS##*:}}"
HTTPS_PORT="${LPE_CT_NGINX_LISTEN_PORT:-443}"
SUBMISSION_BIND="${LPE_CT_SUBMISSION_BIND_ADDRESS:-}"
IMAPS_BIND="${LPE_CT_IMAPS_BIND_ADDRESS:-}"
SUBMISSION_PORT="${SUBMISSION_BIND##*:}"
IMAPS_PORT="${IMAPS_BIND##*:}"

tcp_probe "$HOST" "${SMTP_PORT:-25}"

curl --silent --show-error --fail --insecure "https://${HOST}:${HTTPS_PORT}/" >/dev/null \
  || fail "HTTPS management edge is not reachable on ${HOST}:${HTTPS_PORT}"
pass "HTTPS management edge is reachable on ${HOST}:${HTTPS_PORT}"
tls_probe_if_possible "$HOST" "$HTTPS_PORT" "HTTPS"

if [[ -n "${LPE_CT_SUBMISSION_BIND_ADDRESS:-}" ]]; then
  tcp_probe "$HOST" "$SUBMISSION_PORT"
  tls_probe_if_possible "$HOST" "$SUBMISSION_PORT" "SMTPS submission"
else
  echo "[SKIP] LPE_CT_SUBMISSION_BIND_ADDRESS is not configured; port 465 is intentionally not enabled."
fi

if [[ -n "${LPE_CT_IMAPS_BIND_ADDRESS:-}" ]]; then
  tcp_probe "$HOST" "$IMAPS_PORT"
  tls_probe_if_possible "$HOST" "$IMAPS_PORT" "IMAPS"
else
  echo "[SKIP] LPE_CT_IMAPS_BIND_ADDRESS is not configured; port 993 is not enabled."
fi

echo "LPE-CT edge port test completed successfully."
