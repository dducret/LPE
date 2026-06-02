#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/install-common.sh"

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_PATH="${BIN_PATH:-$INSTALL_ROOT/bin/lpe-cli}"
ADMIN_WEB_ROOT="${ADMIN_WEB_ROOT:-$INSTALL_ROOT/www/admin}"
CLIENT_WEB_ROOT="${CLIENT_WEB_ROOT:-$INSTALL_ROOT/www/client}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"
NGINX_SERVICE_NAME="${NGINX_SERVICE_NAME:-nginx}"
NGINX_SITE_PATH="${NGINX_SITE_PATH:-/etc/nginx/sites-available/lpe.conf}"
EXPECTED_FORMATS="${EXPECTED_FORMATS:-PDF DOCX ODT}"
SCHEMA_FILE="${SCHEMA_FILE:-$SRC_DIR/crates/lpe-storage/sql/schema.sql}"

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

ensure_database_url || fail "DATABASE_URL is not set in $ENV_FILE and could not be derived from LPE_DB_HOST/LPE_DB_PORT/LPE_DB_NAME/LPE_DB_USER/LPE_DB_PASSWORD"
pass "DATABASE_URL is configured"
[[ -n "${LPE_BOOTSTRAP_ADMIN_EMAIL:-}" ]] || fail "LPE_BOOTSTRAP_ADMIN_EMAIL is not set in $ENV_FILE"
pass "Bootstrap administrator email is configured"

BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
HTTP_BASE="http://${BIND_ADDRESS}"
BOOTSTRAP_EMAIL="${LPE_BOOTSTRAP_ADMIN_EMAIL}"
AUTODISCOVER_TEST_EMAIL="${LPE_AUTODISCOVER_TEST_EMAIL:-$BOOTSTRAP_EMAIL}"

systemctl is-enabled "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not enabled: $SERVICE_NAME"
pass "Service enabled: $SERVICE_NAME"

systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not active: $SERVICE_NAME"
pass "Service active: $SERVICE_NAME"

systemctl is-enabled "$NGINX_SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not enabled: $NGINX_SERVICE_NAME"
pass "Service enabled: $NGINX_SERVICE_NAME"

systemctl is-active "$NGINX_SERVICE_NAME" >/dev/null 2>&1 || fail "Service is not active: $NGINX_SERVICE_NAME"
pass "Service active: $NGINX_SERVICE_NAME"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.accounts');" | grep -qx 'accounts' \
  || fail "Table public.accounts is missing. Run /opt/lpe/src/installation/debian-trixie/init-schema.sh"
pass "Found table public.accounts"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.searchable_mail_documents');" | grep -qx 'searchable_mail_documents' \
  || fail "View public.searchable_mail_documents is missing. Run /opt/lpe/src/installation/debian-trixie/init-schema.sh"
pass "Found view public.searchable_mail_documents"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_named_properties');" | grep -qx 'mapi_named_properties' \
  || fail "Table public.mapi_named_properties is missing. LPE 0.4 requires an empty database initialized with /opt/lpe/src/installation/debian-trixie/init-schema.sh."
pass "Found table public.mapi_named_properties"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_custom_property_values');" | grep -qx 'mapi_custom_property_values' \
  || fail "Table public.mapi_custom_property_values is missing. LPE 0.4 requires an empty database initialized with /opt/lpe/src/installation/debian-trixie/init-schema.sh."
pass "Found table public.mapi_custom_property_values"

mapi_custom_public_folder_item_constraint_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.mapi_custom_property_values'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%public_folder_item%';")" \
  || fail "Unable to inspect MAPI custom property object-kind constraint"
[[ "$mapi_custom_public_folder_item_constraint_count" -ge "1" ]] \
  || fail "MAPI custom property object-kind constraint does not allow public_folder_item. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "MAPI custom property object-kind constraint includes public_folder_item"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_profile_settings');" | grep -qx 'mapi_profile_settings' \
  || fail "Table public.mapi_profile_settings is missing. LPE 0.4 requires an empty database initialized with /opt/lpe/src/installation/debian-trixie/init-schema.sh."
pass "Found table public.mapi_profile_settings"

mapi_shortcut_group_column_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mapi_navigation_shortcuts' AND column_name IN ('group_header_id', 'group_name');")" \
  || fail "Unable to inspect MAPI navigation shortcut columns"
[[ "$mapi_shortcut_group_column_count" == "2" ]] \
  || fail "MAPI navigation shortcut group/header columns are missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "MAPI navigation shortcut group/header columns are present"

mapi_shortcut_target_nullable="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mapi_navigation_shortcuts' AND column_name = 'target_folder_id';")" \
  || fail "Unable to inspect MAPI navigation shortcut target column"
[[ "$mapi_shortcut_target_nullable" == "YES" ]] \
  || fail "MAPI navigation shortcut target_folder_id is still NOT NULL. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "MAPI navigation shortcut target folder column supports group headers"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.recoverable_items');" | grep -qx 'recoverable_items' \
  || fail "Table public.recoverable_items is missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Found table public.recoverable_items"

recoverable_account_column_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'accounts' AND column_name IN ('recoverable_items_retention_days', 'litigation_hold_enabled', 'litigation_hold_started_at');")" \
  || fail "Unable to inspect recoverable-item account columns"
[[ "$recoverable_account_column_count" == "3" ]] \
  || fail "Recoverable-item account retention/hold columns are missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Recoverable-item account retention/hold columns are present"

recoverable_mailbox_column_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mailboxes' AND column_name = 'recoverable_items_retention_days';")" \
  || fail "Unable to inspect recoverable-item mailbox columns"
[[ "$recoverable_mailbox_column_count" == "1" ]] \
  || fail "Recoverable-item mailbox retention override column is missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Recoverable-item mailbox retention override column is present"

recoverable_change_constraint_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid IN ('public.mail_change_log'::regclass, 'public.tombstones'::regclass) AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%recoverable_item%';")" \
  || fail "Unable to inspect recoverable-item change-log constraints"
[[ "$recoverable_change_constraint_count" -ge "4" ]] \
  || fail "Recoverable-item change-log constraints are missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Recoverable-item change-log constraints are present"

recoverable_shape_constraint_ok="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.mail_change_log'::regclass AND conname = 'mail_change_log_object_shape_check' AND pg_get_constraintdef(oid) LIKE '%sourceMailboxMessageId%' AND pg_get_constraintdef(oid) LIKE '%[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}%' AND pg_get_constraintdef(oid) NOT LIKE '%[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}%';")" \
  || fail "Unable to inspect recoverable-item replay shape constraint"
[[ "$recoverable_shape_constraint_ok" -ge "1" ]] \
  || fail "Recoverable-item replay shape constraint is stale. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Recoverable-item replay shape constraint is current"

public_folder_table_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('public_folder_trees', 'public_folders', 'public_folder_items', 'public_folder_permissions', 'public_folder_replicas', 'public_folder_per_user_state');")" \
  || fail "Unable to inspect public-folder tables"
[[ "$public_folder_table_count" == "6" ]] \
  || fail "Public-folder canonical tables are missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Public-folder canonical tables are present"

public_folder_change_constraint_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid IN ('public.mail_change_log'::regclass, 'public.tombstones'::regclass) AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%public_folder_replica%';")" \
  || fail "Unable to inspect public-folder change-log constraints"
[[ "$public_folder_change_constraint_count" -ge "4" ]] \
  || fail "Public-folder change-log constraints are missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Public-folder change-log constraints are present"

public_folder_sync_constraint_count="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid IN ('public.account_sync_state'::regclass, 'public.canonical_change_journal'::regclass) AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%public_folders%';")" \
  || fail "Unable to inspect public-folder sync constraints"
[[ "$public_folder_sync_constraint_count" == "2" ]] \
  || fail "Public-folder sync constraints are missing. Run /opt/lpe/src/installation/debian-trixie/update-lpe.sh."
pass "Public-folder sync constraints are present"

schema_version="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "SELECT schema_version FROM public.schema_metadata WHERE singleton = TRUE;")" \
  || fail "Schema metadata is missing. Run /opt/lpe/src/installation/debian-trixie/init-schema.sh"
expected_schema_version="$(
  awk -F"'" '/schema_version TEXT NOT NULL CHECK/ { print $2; exit }' "$SCHEMA_FILE"
)"
[[ -n "$expected_schema_version" ]] || fail "Unable to read expected schema version from $SCHEMA_FILE"
[[ "$schema_version" == "$expected_schema_version" ]] || fail "Unexpected schema version: $schema_version; expected $expected_schema_version"
pass "Schema version is $expected_schema_version"

mapi_identity_constraint_count="$(mapi_identity_key_constraint_count "$DATABASE_URL")" \
  || fail "Unable to inspect MAPI identity key constraints"
[[ "$mapi_identity_constraint_count" == "3" ]] \
  || fail "MAPI identity key constraints do not match the current 22-byte schema. LPE 0.4 requires an empty database initialized with /opt/lpe/src/installation/debian-trixie/init-schema.sh."
pass "MAPI identity key constraints match the current 22-byte schema"

check_http_json_field "$HTTP_BASE/health" '"status":"ok"'
check_http_json_field "$HTTP_BASE/health/live" '"status":"ok"'
check_http_json_field "$HTTP_BASE/health/ready" '"status":"ready"'
check_http_json_field "$HTTP_BASE/health/local-ai" '"provider":"stub-local"'
check_http_json_field "http://127.0.0.1/api/health" '"status":"ok"'
check_http_json_field "http://127.0.0.1/api/health/live" '"status":"ok"'
check_http_json_field "http://127.0.0.1/api/health/ready" '"status":"ready"'

bootstrap_admin_exists="$(
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -v email="$BOOTSTRAP_EMAIL" -At <<'SQL'
SELECT 1
FROM public.admin_credentials
WHERE lower(email) = lower(:'email')
LIMIT 1;
SQL
)" || fail "Unable to query bootstrap administrator from public.admin_credentials"
[[ "$bootstrap_admin_exists" == "1" ]] \
  || fail "Bootstrap administrator ${BOOTSTRAP_EMAIL} is missing from public.admin_credentials"
pass "Bootstrap administrator exists: ${BOOTSTRAP_EMAIL}"

autoconfig_body="$(curl --silent --show-error --fail "http://127.0.0.1/autoconfig/mail/config-v1.1.xml")" \
  || fail "HTTP request failed: http://127.0.0.1/autoconfig/mail/config-v1.1.xml"
[[ "$autoconfig_body" == *"<incomingServer type=\"imap\">"* ]] \
  || fail "Thunderbird autoconfig endpoint does not publish IMAP"
pass "Thunderbird autoconfig endpoint is published by nginx"

well_known_autoconfig_body="$(curl --silent --show-error --fail "http://127.0.0.1/.well-known/autoconfig/mail/config-v1.1.xml")" \
  || fail "HTTP request failed: http://127.0.0.1/.well-known/autoconfig/mail/config-v1.1.xml"
[[ "$well_known_autoconfig_body" == *"<clientConfig version=\"1.1\">"* ]] \
  || fail "Unexpected Thunderbird well-known autoconfig content"
pass "Thunderbird well-known autoconfig endpoint is published by nginx"

autodiscover_body="$(curl --silent --show-error --fail \
  --header 'Content-Type: application/xml' \
  --data "<?xml version=\"1.0\" encoding=\"utf-8\"?><Autodiscover><Request><EMailAddress>${AUTODISCOVER_TEST_EMAIL}</EMailAddress></Request></Autodiscover>" \
  "http://127.0.0.1/autodiscover/autodiscover.xml")" \
  || fail "HTTP request failed: http://127.0.0.1/autodiscover/autodiscover.xml"
[[ "$autodiscover_body" == *"<Type>IMAP</Type>"* ]] \
  || fail "Autodiscover endpoint does not publish IMAP"
pass "Outlook autodiscover endpoint is published by nginx"

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
