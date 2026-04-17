#!/usr/bin/env bash
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_PATH="${BIN_PATH:-$INSTALL_ROOT/bin/lpe-cli}"
ADMIN_WEB_ROOT="${ADMIN_WEB_ROOT:-$INSTALL_ROOT/www/admin}"
CLIENT_WEB_ROOT="${CLIENT_WEB_ROOT:-$INSTALL_ROOT/www/client}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"
NGINX_SERVICE_NAME="${NGINX_SERVICE_NAME:-nginx}"
NGINX_SITE_PATH="${NGINX_SITE_PATH:-/etc/nginx/sites-available/lpe.conf}"
EXPECTED_FORMATS="${EXPECTED_FORMATS:-pdf docx odt}"

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

check_http_json_field() {
  local url="$1"
  local expected="$2"
  local body
  body="$(curl --silent --show-error --fail "$url")" || fail "HTTP request failed: $url"
  [[ "$body" == *"$expected"* ]] || fail "Unexpected response from $url: $body"
  pass "Endpoint responded as expected: $url"
}

check_command curl
check_command nginx
check_command psql
check_command systemctl
check_file "/etc/systemd/system/${SERVICE_NAME}"
check_file "${NGINX_SITE_PATH}"
check_file "${ADMIN_WEB_ROOT}/index.html"
check_file "${CLIENT_WEB_ROOT}/index.html"

check_file "$SRC_DIR"
check_file "$BIN_PATH"
check_file "$ENV_FILE"

set -a
source "$ENV_FILE"
set +a

[[ -n "${DATABASE_URL:-}" ]] || fail "DATABASE_URL is not set in $ENV_FILE"
pass "DATABASE_URL is configured"

BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
HTTP_BASE="http://${BIND_ADDRESS}"

systemctl is-enabled "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not enabled: $SERVICE_NAME"
pass "Service enabled: $SERVICE_NAME"

systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not active: $SERVICE_NAME"
pass "Service active: $SERVICE_NAME"

systemctl is-enabled "$NGINX_SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not enabled: $NGINX_SERVICE_NAME"
pass "Service enabled: $NGINX_SERVICE_NAME"

systemctl is-active "$NGINX_SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not active: $NGINX_SERVICE_NAME"
pass "Service active: $NGINX_SERVICE_NAME"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.accounts');" | grep -qx 'accounts' \
  || fail "Table public.accounts is missing. Run /opt/lpe/src/installation/debian-trixie/run-migrations.sh"
pass "Found table public.accounts"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.searchable_mail_documents');" | grep -qx 'searchable_mail_documents' \
  || fail "View public.searchable_mail_documents is missing. Run /opt/lpe/src/installation/debian-trixie/run-migrations.sh"
pass "Found view public.searchable_mail_documents"

check_http_json_field "$HTTP_BASE/health" '"status":"ok"'
check_http_json_field "$HTTP_BASE/bootstrap/admin" '"email":"admin@example.test"'
check_http_json_field "$HTTP_BASE/health/local-ai" '"provider":"stub-local"'
check_http_json_field "http://127.0.0.1/api/health" '"status":"ok"'
check_http_json_field "http://127.0.0.1/api/bootstrap/admin" '"email":"admin@example.test"'

admin_index="$(curl --silent --show-error --fail "http://127.0.0.1/")" \
  || fail "HTTP request failed: http://127.0.0.1/"
[[ "$admin_index" == *"LPE Administration Console"* ]] || fail "Unexpected admin index content from nginx"
pass "Admin console is served by nginx"

mail_redirect="$(curl --silent --show-error --head --location-trusted --write-out '%{url_effective}' --output /dev/null "http://127.0.0.1/mail")" \
  || fail "HTTP request failed: http://127.0.0.1/mail"
[[ "$mail_redirect" == "http://127.0.0.1/mail/" ]] || fail "Unexpected /mail redirect target: $mail_redirect"
pass "Web client shortcut redirects from /mail to /mail/"

client_index="$(curl --silent --show-error --fail "http://127.0.0.1/mail/")" \
  || fail "HTTP request failed: http://127.0.0.1/mail/"
[[ "$client_index" == *"/mail/assets/"* ]] || fail "Unexpected web client index content from nginx"
pass "Web client is served by nginx on /mail/"

attachment_body="$(curl --silent --show-error --fail "$HTTP_BASE/capabilities/attachments")" \
  || fail "HTTP request failed: $HTTP_BASE/capabilities/attachments"
for format in $EXPECTED_FORMATS; do
  [[ "$attachment_body" == *"\"$format\""* ]] || fail "Attachment format missing from API response: $format"
done
pass "Attachment capability endpoint includes expected formats: $EXPECTED_FORMATS"

echo
echo "LPE installation check completed successfully."
