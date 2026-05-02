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

if [[ "${LPE_CT_LOCAL_DB_ENABLED:-false}" == "true" ]]; then
  check_command psql
  [[ -n "${LPE_CT_LOCAL_DB_URL:-}" ]] || fail "LPE_CT_LOCAL_DB_URL is not set in $ENV_FILE"
  db_probe="$(psql "${LPE_CT_LOCAL_DB_URL}" -tAc "SELECT 1" 2>/dev/null || true)"
  [[ "${db_probe}" == "1" ]] || fail "Dedicated LPE-CT PostgreSQL probe failed"
  pass "Dedicated LPE-CT PostgreSQL responded correctly"
fi

API_HEALTH_URL="http://${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}/health"
API_DASHBOARD_URL="http://${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}/api/v1/dashboard"
CONSOLE_URL="${LPE_CT_CONSOLE_TEST_URL:-https://127.0.0.1:${LPE_CT_NGINX_LISTEN_PORT:-443}/}"
PUBLIC_HTTPS_BASE="${LPE_CT_PUBLICATION_TEST_URL:-https://127.0.0.1:${LPE_CT_NGINX_LISTEN_PORT:-443}}"
PUBLIC_HOST_HEADER="${LPE_CT_PUBLICATION_TEST_HOST:-${LPE_CT_PUBLIC_HOSTNAME:-${LPE_CT_SERVER_NAME:-localhost}}}"
AUTODISCOVER_TEST_EMAIL="${LPE_CT_AUTODISCOVER_TEST_EMAIL:-${LPE_CT_BOOTSTRAP_ADMIN_EMAIL:-admin@example.test}}"
SMTP_HOST="${LPE_CT_SMTP_TEST_HOST:-127.0.0.1}"
SMTP_PORT="${LPE_CT_SMTP_TEST_PORT:-${LPE_CT_SMTP_BIND_ADDRESS##*:}}"
SMTP_TEST_SENDER="${LPE_CT_SMTP_TEST_SENDER:-}"
SMTP_TEST_RECIPIENT="${LPE_CT_SMTP_TEST_RECIPIENT:-}"

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

autoconfig_body="$(curl --silent --show-error --fail --insecure \
  --header "Host: ${PUBLIC_HOST_HEADER}" \
  "${PUBLIC_HTTPS_BASE}/autoconfig/mail/config-v1.1.xml")" \
  || fail "Thunderbird autoconfig request failed through LPE-CT public HTTPS edge"
[[ "$autoconfig_body" == *"<incomingServer type=\"imap\">"* ]] \
  || fail "Thunderbird autoconfig endpoint does not publish IMAP through LPE-CT"
pass "Thunderbird autoconfig endpoint is published by LPE-CT"

well_known_autoconfig_body="$(curl --silent --show-error --fail --insecure \
  --header "Host: ${PUBLIC_HOST_HEADER}" \
  "${PUBLIC_HTTPS_BASE}/.well-known/autoconfig/mail/config-v1.1.xml")" \
  || fail "Thunderbird well-known autoconfig request failed through LPE-CT public HTTPS edge"
[[ "$well_known_autoconfig_body" == *"<clientConfig version=\"1.1\">"* ]] \
  || fail "Unexpected Thunderbird well-known autoconfig content through LPE-CT"
pass "Thunderbird well-known autoconfig endpoint is published by LPE-CT"

autodiscover_body="$(curl --silent --show-error --fail --insecure \
  --header "Host: ${PUBLIC_HOST_HEADER}" \
  --header 'Content-Type: application/xml' \
  --data "<?xml version=\"1.0\" encoding=\"utf-8\"?><Autodiscover><Request><EMailAddress>${AUTODISCOVER_TEST_EMAIL}</EMailAddress></Request></Autodiscover>" \
  "${PUBLIC_HTTPS_BASE}/autodiscover/autodiscover.xml")" \
  || fail "Outlook autodiscover POST failed through LPE-CT public HTTPS edge"
[[ "$autodiscover_body" == *"<Type>MobileSync</Type>"* ]] \
  || fail "Autodiscover endpoint does not publish MobileSync through LPE-CT"
pass "Outlook autodiscover POST publishes MobileSync through LPE-CT"

autodiscover_upper_body="$(curl --silent --show-error --fail --insecure \
  --header "Host: ${PUBLIC_HOST_HEADER}" \
  --header 'Content-Type: application/xml' \
  --data "<?xml version=\"1.0\" encoding=\"utf-8\"?><Autodiscover><Request><EMailAddress>${AUTODISCOVER_TEST_EMAIL}</EMailAddress></Request></Autodiscover>" \
  "${PUBLIC_HTTPS_BASE}/Autodiscover/Autodiscover.xml")" \
  || fail "Outlook uppercase Autodiscover POST failed through LPE-CT public HTTPS edge"
[[ "$autodiscover_upper_body" == *"<Type>MobileSync</Type>"* ]] \
  || fail "Uppercase Autodiscover endpoint does not publish MobileSync through LPE-CT"
pass "Uppercase Outlook Autodiscover POST publishes MobileSync through LPE-CT"

activesync_headers="$(mktemp)"
curl --silent --show-error --fail --insecure --http1.1 \
  --request OPTIONS \
  --header "Host: ${PUBLIC_HOST_HEADER}" \
  --dump-header "$activesync_headers" \
  --output /dev/null \
  "${PUBLIC_HTTPS_BASE}/Microsoft-Server-ActiveSync" \
  || {
    rm -f "$activesync_headers"
    fail "ActiveSync OPTIONS failed through LPE-CT public HTTPS edge"
  }
grep -qi '^ms-asprotocolversions:' "$activesync_headers" \
  || {
    sed -n '1,40p' "$activesync_headers" >&2 || true
    rm -f "$activesync_headers"
    fail "ActiveSync OPTIONS response is missing ms-asprotocolversions"
  }
grep -qi '^ms-asprotocolcommands:' "$activesync_headers" \
  || {
    sed -n '1,40p' "$activesync_headers" >&2 || true
    rm -f "$activesync_headers"
    fail "ActiveSync OPTIONS response is missing ms-asprotocolcommands"
  }
rm -f "$activesync_headers"
pass "ActiveSync OPTIONS exposes ms-asprotocolversions and ms-asprotocolcommands through LPE-CT"

[[ -n "$SMTP_TEST_SENDER" ]] || fail "Set LPE_CT_SMTP_TEST_SENDER in $ENV_FILE or the shell environment"
[[ -n "$SMTP_TEST_RECIPIENT" ]] || fail "Set LPE_CT_SMTP_TEST_RECIPIENT in $ENV_FILE or the shell environment"

smtp_response="$({
  printf 'EHLO check-lpe-ct.local\r\n'
  printf 'MAIL FROM:<%s>\r\n' "$SMTP_TEST_SENDER"
  printf 'RCPT TO:<%s>\r\n' "$SMTP_TEST_RECIPIENT"
  printf 'DATA\r\n'
  printf 'From: LPE-CT Check <%s>\r\n' "$SMTP_TEST_SENDER"
  printf 'To: <%s>\r\n' "$SMTP_TEST_RECIPIENT"
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

console_body="$(curl --silent --show-error --fail --insecure "$CONSOLE_URL")" || fail "Console request failed: $CONSOLE_URL"
[[ "$console_body" == *"Centre de Tri"* ]] || fail "Unexpected management index content"
pass "Management console is served by nginx"

echo
echo "LPE-CT installation check completed successfully."
