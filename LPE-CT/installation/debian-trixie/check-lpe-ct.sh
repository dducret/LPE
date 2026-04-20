#!/usr/bin/env bash
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe-ct}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_PATH="${BIN_PATH:-$INSTALL_ROOT/bin/lpe-ct}"
WEB_ROOT="${WEB_ROOT:-$INSTALL_ROOT/www/management}"
ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
STATE_FILE="${STATE_FILE:-/var/lib/lpe-ct/state.json}"
SPOOL_DIR="${SPOOL_DIR:-/var/spool/lpe-ct}"
SERVICE_NAME="${SERVICE_NAME:-lpe-ct.service}"
NGINX_SITE_PATH="${NGINX_SITE_PATH:-/etc/nginx/sites-available/lpe-ct.conf}"
TAKERI_BIN_PATH="${TAKERI_BIN_PATH:-$INSTALL_ROOT/bin/Shuhari-CyberForge-CLI}"

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

pass() {
  echo "[OK] $*"
}

check_file() {
  local path="$1"
  [[ -e "$path" ]] || fail "Missing: $path"
  pass "Found: $path"
}

check_command() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || fail "Command not available: $cmd"
  pass "Command available: $cmd"
}

check_command curl
check_command nginx
check_command systemctl
check_file "$SRC_DIR"
check_file "$BIN_PATH"
check_file "$WEB_ROOT/index.html"
check_file "$ENV_FILE"
check_file "$STATE_FILE"
check_file "$SPOOL_DIR"
check_file "/etc/systemd/system/$SERVICE_NAME"
check_file "$NGINX_SITE_PATH"

set -a
source "$ENV_FILE"
set +a

[[ -n "${LPE_CT_BOOTSTRAP_ADMIN_EMAIL:-}" ]] || fail "LPE_CT_BOOTSTRAP_ADMIN_EMAIL is not set in $ENV_FILE"
pass "Bootstrap management email is configured"

if [[ "${LPE_CT_ANTIVIRUS_ENABLED:-false}" == "true" ]]; then
  check_file "${LPE_CT_ANTIVIRUS_TAKERI_BIN:-$TAKERI_BIN_PATH}"
  pass "Antivirus provider chain is configured"
fi

API_HEALTH_URL="http://${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}/health"
API_DASHBOARD_URL="http://${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}/api/v1/dashboard"
SMTP_HOST="${LPE_CT_SMTP_TEST_HOST:-127.0.0.1}"
SMTP_PORT="${LPE_CT_SMTP_TEST_PORT:-${LPE_CT_SMTP_BIND_ADDRESS##*:}}"
SMTP_TEST_SENDER="${LPE_CT_SMTP_TEST_SENDER:?Set LPE_CT_SMTP_TEST_SENDER in $ENV_FILE or the shell environment}"
SMTP_TEST_RECIPIENT="${LPE_CT_SMTP_TEST_RECIPIENT:?Set LPE_CT_SMTP_TEST_RECIPIENT in $ENV_FILE or the shell environment}"

systemctl is-enabled "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not enabled: $SERVICE_NAME"
pass "Service enabled: $SERVICE_NAME"
systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not active: $SERVICE_NAME"
pass "Service active: $SERVICE_NAME"
systemctl is-enabled nginx >/dev/null 2>&1 || fail "Service is not enabled: nginx"
pass "Service enabled: nginx"
systemctl is-active nginx >/dev/null 2>&1 || fail "Service is not active: nginx"
pass "Service active: nginx"

health_body="$(curl --silent --show-error --fail "$API_HEALTH_URL")" || fail "Health request failed: $API_HEALTH_URL"
[[ "$health_body" == *"\"status\":\"ok\""* ]] || fail "Unexpected health response: $health_body"
pass "Management API health endpoint responded correctly"

health_live_body="$(curl --silent --show-error --fail "http://${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}/health/live")" || fail "Health live request failed"
[[ "$health_live_body" == *"\"status\":\"ok\""* ]] || fail "Unexpected live health response: $health_live_body"
pass "Management API live health endpoint responded correctly"

health_ready_body="$(curl --silent --show-error --fail "http://${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}/health/ready")" || fail "Health ready request failed"
[[ "$health_ready_body" == *"\"status\":\"ready\""* ]] || fail "Unexpected readiness response: $health_ready_body"
pass "Management API readiness endpoint responded correctly"

dashboard_body="$(curl --silent --show-error --fail "$API_DASHBOARD_URL")" || fail "Dashboard request failed: $API_DASHBOARD_URL"
[[ "$dashboard_body" == *"dmz-sorting-center"* ]] || fail "Unexpected dashboard response: $dashboard_body"
pass "Dashboard endpoint responded correctly"

smtp_response="$({
  printf 'EHLO check-lpe-ct.local\r\n'
  printf 'MAIL FROM:<%s>\r\n' "$SMTP_TEST_SENDER"
  printf 'RCPT TO:<%s>\r\n' "$SMTP_TEST_RECIPIENT"
  printf 'DATA\r\n'
  printf 'Subject: LPE-CT local installation check\r\n'
  printf '\r\n'
  printf 'check-lpe-ct generated message\r\n'
  printf '.\r\n'
  printf 'QUIT\r\n'
} | timeout 10 bash -c "cat < /dev/stdin > /dev/tcp/${SMTP_HOST}/${SMTP_PORT}" 2>&1 || true)"
sleep 1
dashboard_after_smtp="$(curl --silent --show-error --fail "$API_DASHBOARD_URL")" || fail "Dashboard request failed after SMTP test"
[[ "$dashboard_after_smtp" == *"deferred_messages"* ]] || fail "Dashboard missing queue metrics after SMTP test"
pass "SMTP listener accepted an installation-check message"

console_body="$(curl --silent --show-error --fail "http://127.0.0.1/")" || fail "Console request failed"
[[ "$console_body" == *"Centre de Tri"* ]] || fail "Unexpected management index content"
pass "Management console is served by nginx"

echo
echo "LPE-CT installation check completed successfully."
