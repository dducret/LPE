#!/usr/bin/env bash
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_PATH="${BIN_PATH:-$INSTALL_ROOT/bin/lpe-cli}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"
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
  [[ -e "$path" ]] || fail "Introuvable / Missing: $path"
  pass "Trouve / Found: $path"
}

check_command() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || fail "Commande indisponible / Command not available: $cmd"
  pass "Commande disponible / Command available: $cmd"
}

check_http_json_field() {
  local url="$1"
  local expected="$2"
  local body
  body="$(curl --silent --show-error --fail "$url")" || fail "Requete HTTP en echec / HTTP request failed: $url"
  [[ "$body" == *"$expected"* ]] || fail "Reponse inattendue depuis / Unexpected response from $url: $body"
  pass "Endpoint conforme / Endpoint responded as expected: $url"
}

check_command curl
check_command psql
check_command systemctl
check_file "/etc/systemd/system/${SERVICE_NAME}"

check_file "$SRC_DIR"
check_file "$BIN_PATH"
check_file "$ENV_FILE"

set -a
source "$ENV_FILE"
set +a

[[ -n "${DATABASE_URL:-}" ]] || fail "DATABASE_URL n'est pas defini dans / is not set in $ENV_FILE"
pass "DATABASE_URL configure / DATABASE_URL is configured"

BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
HTTP_BASE="http://${BIND_ADDRESS}"

systemctl is-enabled "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service non active au demarrage / Service is not enabled: $SERVICE_NAME"
pass "Service active au demarrage / Service enabled: $SERVICE_NAME"

systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service non demarre / Service is not active: $SERVICE_NAME"
pass "Service demarre / Service active: $SERVICE_NAME"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.accounts');" | grep -qx 'accounts' \
  || fail "Table public.accounts introuvable / Table public.accounts is missing"
pass "Table public.accounts trouvee / Found table public.accounts"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.searchable_mail_documents');" | grep -qx 'searchable_mail_documents' \
  || fail "Vue public.searchable_mail_documents introuvable / View public.searchable_mail_documents is missing"
pass "Vue public.searchable_mail_documents trouvee / Found view public.searchable_mail_documents"

check_http_json_field "$HTTP_BASE/health" '"status":"ok"'
check_http_json_field "$HTTP_BASE/bootstrap/admin" '"email":"admin@example.test"'
check_http_json_field "$HTTP_BASE/health/local-ai" '"provider":"stub-local"'

attachment_body="$(curl --silent --show-error --fail "$HTTP_BASE/capabilities/attachments")" \
  || fail "Requete HTTP en echec / HTTP request failed: $HTTP_BASE/capabilities/attachments"
for format in $EXPECTED_FORMATS; do
  [[ "$attachment_body" == *"\"$format\""* ]] || fail "Format de piece jointe manquant dans l'API / Attachment format missing from API response: $format"
done
pass "Endpoint pieces jointes conforme / Attachment capability endpoint includes expected formats: $EXPECTED_FORMATS"

echo
echo "Verification de l'installation LPE terminee avec succes. / LPE installation check completed successfully."
