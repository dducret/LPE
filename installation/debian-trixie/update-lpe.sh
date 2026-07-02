#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=installation/debian-trixie/lib/install-common.sh
source "${SCRIPT_DIR}/lib/install-common.sh"

REPO_URL="${REPO_URL:-https://github.com/dducret/LPE}"
BRANCH="${BRANCH:-main}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_DIR="${BIN_DIR:-$INSTALL_ROOT/bin}"
WEB_ROOT="${WEB_ROOT:-$INSTALL_ROOT/www}"
ADMIN_WEB_ROOT="${ADMIN_WEB_ROOT:-$WEB_ROOT/admin}"
CLIENT_WEB_ROOT="${CLIENT_WEB_ROOT:-$WEB_ROOT/client}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
INSTALL_ENV_FILE="${INSTALL_ENV_FILE:-/etc/lpe/install.env}"
DATA_DIR="${DATA_DIR:-/var/lib/lpe}"
SERVICE_USER="${SERVICE_USER:-lpe}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe}"
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-lpe.conf}"
MAGIKA_VERSION="${MAGIKA_VERSION:-1.0.2}"
MAGIKA_LINUX_X86_64_SHA256="${MAGIKA_LINUX_X86_64_SHA256:-4ce475c965cd20e724b5fc53e8a303a479b9d8649beef8721d05e9b3988fbab4}"

load_env_file_if_present "${INSTALL_ENV_FILE}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

if [[ ! -d "${SRC_DIR}/.git" ]]; then
  echo "Source repository not found in ${SRC_DIR}. Run install-lpe.sh first." >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found in ${ENV_FILE}. Run install-lpe.sh first." >&2
  exit 1
fi

install_magika() {
  local version="$1"
  local expected_sha="$2"
  local archive="magika-x86_64-unknown-linux-gnu.tar.xz"
  local url="https://github.com/google/magika/releases/download/cli/v${version}/${archive}"
  local temp_dir="/tmp/magika"
  local extracted_bin

  rm -rf "${temp_dir}"
  mkdir -p "${temp_dir}"
  trap "rm -rf '${temp_dir}'" RETURN

  curl --proto '=https' --tlsv1.2 -LsSf "${url}" -o "${temp_dir}/${archive}"
  echo "${expected_sha}  ${temp_dir}/${archive}" | sha256sum -c -
  tar -xJf "${temp_dir}/${archive}" -C "${temp_dir}"
  extracted_bin="$(find "${temp_dir}" -type f -name magika | head -n 1)"
  [[ -n "${extracted_bin}" ]] || { echo "magika binary not found after archive extraction." >&2; exit 1; }
  install -m 0755 "${extracted_bin}" "${BIN_DIR}/magika"
  trap - RETURN
  rm -rf "${temp_dir}"
}

run_npm() {
  local ignored_warning='npm warn Unknown builtin config "globalignorefile". This will stop working in the next major version of npm. See `npm help npmrc` for supported config options.'
  npm "$@" 2> >(
    grep -v -F "${ignored_warning}" >&2
  )
}

path_changed_since_previous_head() {
  if [[ -z "${PREVIOUS_HEAD:-}" || -z "${CURRENT_HEAD:-}" ]]; then
    return 0
  fi

  if [[ "${PREVIOUS_HEAD}" != "${CURRENT_HEAD}" ]] \
    && ! git -C "${SRC_DIR}" diff --quiet "${PREVIOUS_HEAD}" "${CURRENT_HEAD}" -- "$@"; then
    return 0
  fi

  [[ -n "$(git -C "${SRC_DIR}" status --porcelain -- "$@")" ]]
}

tracked_path_newer_than_file() {
  local reference_file="$1"
  shift

  [[ -e "${reference_file}" ]] || return 0

  local tracked_file
  while IFS= read -r -d '' tracked_file; do
    if [[ "${SRC_DIR}/${tracked_file}" -nt "${reference_file}" ]]; then
      return 0
    fi
  done < <(git -C "${SRC_DIR}" ls-files -z -- "$@")

  return 1
}

directory_has_files() {
  local directory="$1"
  [[ -d "${directory}" ]] && find "${directory}" -mindepth 1 -maxdepth 1 -print -quit | grep -q .
}

rust_build_needed() {
  [[ "${LPE_FORCE_RUST_BUILD:-false}" == "true" ]] && return 0
  [[ -x "${BIN_DIR}/lpe-cli" ]] || return 0
  path_changed_since_previous_head Cargo.toml Cargo.lock rust-toolchain.toml crates \
    || tracked_path_newer_than_file "${BIN_DIR}/lpe-cli" Cargo.toml Cargo.lock rust-toolchain.toml crates
}

magika_install_needed() {
  [[ "${LPE_FORCE_MAGIKA_INSTALL:-false}" == "true" ]] && return 0
  [[ -x "${BIN_DIR}/magika" ]] || return 0
  if "${BIN_DIR}/magika" --version 2>/dev/null | grep -q "${MAGIKA_VERSION}"; then
    return 1
  fi
  return 0
}

web_dependencies_needed() {
  local app_path="$1"
  [[ "${LPE_FORCE_WEB_DEPS:-false}" == "true" ]] && return 0
  [[ -d "${SRC_DIR}/${app_path}/node_modules" ]] || return 0
  path_changed_since_previous_head "${app_path}/package.json" "${app_path}/package-lock.json" \
    || tracked_path_newer_than_file "${SRC_DIR}/${app_path}/node_modules" "${app_path}/package.json" "${app_path}/package-lock.json"
}

web_build_needed() {
  local app_path="$1"
  local web_root="$2"
  local build_stamp="${SRC_DIR}/${app_path}/dist/.lpe-build-stamp"

  [[ "${LPE_FORCE_WEB_BUILD:-false}" == "true" ]] && return 0
  [[ -d "${SRC_DIR}/${app_path}/dist" ]] || return 0
  directory_has_files "${web_root}" || return 0
  path_changed_since_previous_head "${app_path}" \
    || tracked_path_newer_than_file "${build_stamp}" \
      "${app_path}/src" \
      "${app_path}/index.html" \
      "${app_path}/package.json" \
      "${app_path}/package-lock.json" \
      "${app_path}/postcss.config.cjs" \
      "${app_path}/tailwind.config.ts" \
      "${app_path}/tsconfig.json" \
      "${app_path}/vite.config.ts"
}

build_web_app() {
  local app_path="$1"
  local web_root="$2"
  local label="$3"

  cd "${SRC_DIR}/${app_path}"
  if web_dependencies_needed "${app_path}"; then
    run_npm ci
  else
    echo "Skipping ${label} npm ci; package files and node_modules are unchanged."
  fi

  if web_build_needed "${app_path}" "${web_root}"; then
    run_npm run build
    touch "${SRC_DIR}/${app_path}/dist/.lpe-build-stamp"
    install -d -o root -g root "${web_root}"
    cp -a "${SRC_DIR}/${app_path}/dist/." "${web_root}/"
  else
    echo "Skipping ${label} web build; sources and installed assets are unchanged."
  fi
}

git config --global --add safe.directory "${SRC_DIR}" || true

RUSTUP_BIN="$(command -v rustup || true)"
if [[ -z "${RUSTUP_BIN}" ]]; then
  echo "rustup executable not found. Install prerequisites first." >&2
  exit 1
fi

PREVIOUS_HEAD="$(git -C "${SRC_DIR}" rev-parse HEAD 2>/dev/null || true)"
git -C "${SRC_DIR}" remote set-url origin "${REPO_URL}" || true
git -C "${SRC_DIR}" fetch --all --tags
git -C "${SRC_DIR}" checkout "${BRANCH}"
git -C "${SRC_DIR}" pull --ff-only origin "${BRANCH}"
CURRENT_HEAD="$(git -C "${SRC_DIR}" rev-parse HEAD 2>/dev/null || true)"

ENV_CHECK_SCRIPT="${SRC_DIR}/installation/debian-trixie/check-lpe-env.sh"
ENV_EXAMPLE_FILE="${SRC_DIR}/installation/debian-trixie/lpe.env.example"
if [[ -x "${ENV_CHECK_SCRIPT}" || -f "${ENV_CHECK_SCRIPT}" ]]; then
  if [[ "${LPE_ENV_CHECK_STRICT:-false}" == "true" ]]; then
    bash "${ENV_CHECK_SCRIPT}" --env-file "${ENV_FILE}" --example-file "${ENV_EXAMPLE_FILE}" --strict
  else
    bash "${ENV_CHECK_SCRIPT}" --env-file "${ENV_FILE}" --example-file "${ENV_EXAMPLE_FILE}" || true
  fi
fi

"${RUSTUP_BIN}" default stable
export PATH="/root/.cargo/bin:${PATH}"

CARGO_BIN="$(command -v cargo || true)"
if [[ -z "${CARGO_BIN}" ]]; then
  echo "cargo executable not found after rustup toolchain initialization." >&2
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

if ! ensure_database_url; then
  echo "DATABASE_URL is not set in ${ENV_FILE} and could not be derived from LPE_DB_HOST/LPE_DB_PORT/LPE_DB_NAME/LPE_DB_USER/LPE_DB_PASSWORD" >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql executable not found. Install PostgreSQL client tools before updating LPE." >&2
  exit 1
fi

SCHEMA_FILE="${SRC_DIR}/crates/lpe-storage/sql/schema.sql"
EXPECTED_SCHEMA_VERSION="$(
  awk -F"'" '/schema_version TEXT NOT NULL CHECK/ { print $2; exit }' "${SCHEMA_FILE}"
)"
if [[ -z "${EXPECTED_SCHEMA_VERSION}" ]]; then
  echo "Unable to read expected schema version from ${SCHEMA_FILE}" >&2
  exit 1
fi

INSTALLED_SCHEMA_VERSION="$(
  psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -Atc "SELECT schema_version FROM public.schema_metadata WHERE singleton = TRUE"
)" || {
  echo "Unable to read installed schema metadata. LPE 0.4 requires an empty database initialized with init-schema.sh." >&2
  exit 1
}

if [[ "${INSTALLED_SCHEMA_VERSION}" != "${EXPECTED_SCHEMA_VERSION}" ]]; then
  echo "Installed schema version ${INSTALLED_SCHEMA_VERSION} does not match required ${EXPECTED_SCHEMA_VERSION}." >&2
  echo "LPE 0.4 requires an empty database initialized with init-schema.sh." >&2
  exit 1
fi

if ! psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_profile_settings');" | grep -qx 'mapi_profile_settings'; then
  echo "Table public.mapi_profile_settings is missing. LPE 0.4 requires an empty database initialized with init-schema.sh." >&2
  exit 1
fi

if ! psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_navigation_shortcuts');" | grep -qx 'mapi_navigation_shortcuts'; then
  echo "Table public.mapi_navigation_shortcuts is missing. LPE 0.4 requires an initialized database before compatibility updates can run." >&2
  exit 1
fi

if ! psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_custom_property_values');" | grep -qx 'mapi_custom_property_values'; then
  echo "Table public.mapi_custom_property_values is missing. LPE 0.4 requires an initialized database before compatibility updates can run." >&2
  exit 1
fi

echo "Applying LPE 0.4 schema compatibility updates..."
psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -f "${SRC_DIR}/installation/debian-trixie/update-lpe-0.4-compat.sql"

mapi_shortcut_group_column_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mapi_navigation_shortcuts' AND column_name IN ('group_header_id', 'group_name');")"
mapi_shortcut_target_nullable="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mapi_navigation_shortcuts' AND column_name = 'target_folder_id';")"
mapi_shortcut_save_stamp_column_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mapi_navigation_shortcuts' AND column_name = 'save_stamp' AND is_nullable = 'NO' AND column_default = '0';")"
mapi_shortcut_save_stamp_constraint_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass AND conname = 'mapi_navigation_shortcuts_save_stamp_check' AND pg_get_constraintdef(oid) LIKE '%save_stamp >= 0%' AND pg_get_constraintdef(oid) LIKE '%4294967295%';")"
if [[ "${mapi_shortcut_group_column_count}" != "2" || "${mapi_shortcut_target_nullable}" != "YES" || "${mapi_shortcut_save_stamp_column_count}" != "1" || "${mapi_shortcut_save_stamp_constraint_count}" != "1" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce the expected mapi_navigation_shortcuts shape." >&2
  exit 1
fi
mapi_profile_ost_constraint_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.mapi_profile_settings'::regclass AND conname = 'mapi_profile_settings_ipm_subtree_ost_id_check' AND pg_get_constraintdef(oid) LIKE '%<= 2048%';")"
if [[ "${mapi_profile_ost_constraint_count}" != "1" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce the expected mapi_profile_settings OST identity shape." >&2
  exit 1
fi
mapi_folder_profile_property_table="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_folder_profile_property_values');")"
mapi_folder_profile_property_idx="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_folder_profile_property_values_account_idx');")"
if [[ "${mapi_folder_profile_property_table}" != "mapi_folder_profile_property_values" || "${mapi_folder_profile_property_idx}" != "mapi_folder_profile_property_values_account_idx" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce the expected mapi_folder_profile_property_values shape." >&2
  exit 1
fi
mapi_associated_config_table="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_associated_config_messages');")"
mapi_associated_config_logical_idx="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.mapi_associated_config_messages_logical_idx');")"
if [[ "${mapi_associated_config_table}" != "mapi_associated_config_messages" || "${mapi_associated_config_logical_idx}" != "mapi_associated_config_messages_logical_idx" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce the expected mapi_associated_config_messages shape." >&2
  exit 1
fi
search_folder_user_saved_name_idx="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.search_folders_user_saved_name_idx');")"
if [[ "${search_folder_user_saved_name_idx}" != "search_folders_user_saved_name_idx" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce search_folders_user_saved_name_idx." >&2
  exit 1
fi
recipient_suggestions_table="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.recipient_suggestions');")"
recipient_suggestions_active_idx="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.recipient_suggestions_active_email_idx');")"
recipient_suggestions_rank_idx="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.recipient_suggestions_rank_idx');")"
if [[ "${recipient_suggestions_table}" != "recipient_suggestions" || "${recipient_suggestions_active_idx}" != "recipient_suggestions_active_email_idx" || "${recipient_suggestions_rank_idx}" != "recipient_suggestions_rank_idx" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce the expected recipient_suggestions shape." >&2
  exit 1
fi
recoverable_table="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT to_regclass('public.recoverable_items');")"
recoverable_account_column_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'accounts' AND column_name IN ('recoverable_items_retention_days', 'litigation_hold_enabled', 'litigation_hold_started_at');")"
recoverable_mailbox_column_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mailboxes' AND column_name = 'recoverable_items_retention_days';")"
managed_retention_mailbox_column_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mailboxes' AND column_name = 'retention_policy_tag_id';")"
managed_retention_mailbox_fk_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.mailboxes'::regclass AND conname = 'mailboxes_retention_policy_tag_fk';")"
recoverable_change_constraint_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid IN ('public.mail_change_log'::regclass, 'public.tombstones'::regclass) AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%recoverable_item%';")"
recoverable_shape_constraint_ok="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.mail_change_log'::regclass AND conname = 'mail_change_log_object_shape_check' AND pg_get_constraintdef(oid) LIKE '%sourceMailboxMessageId%' AND pg_get_constraintdef(oid) LIKE '%[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}%' AND pg_get_constraintdef(oid) NOT LIKE '%[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}%';")"
if [[ "${recoverable_table}" != "recoverable_items" || "${recoverable_account_column_count}" != "3" || "${recoverable_mailbox_column_count}" != "1" || "${managed_retention_mailbox_column_count}" != "1" || "${managed_retention_mailbox_fk_count}" != "1" || "${recoverable_change_constraint_count}" -lt "4" || "${recoverable_shape_constraint_ok}" -lt "1" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce the expected recoverable-items shape." >&2
  exit 1
fi
public_folder_table_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('public_folder_trees', 'public_folders', 'public_folder_items', 'public_folder_permissions', 'public_folder_replicas', 'public_folder_per_user_state');")"
public_folder_change_constraint_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid IN ('public.mail_change_log'::regclass, 'public.tombstones'::regclass) AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%public_folder_replica%';")"
public_folder_sync_constraint_count="$(psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -tAc "SELECT COUNT(*) FROM pg_constraint WHERE conrelid IN ('public.account_sync_state'::regclass, 'public.canonical_change_journal'::regclass) AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%public_folders%';")"
if [[ "${public_folder_table_count}" != "6" || "${public_folder_change_constraint_count}" -lt "4" || "${public_folder_sync_constraint_count}" != "2" ]]; then
  echo "LPE 0.4 schema compatibility update did not produce the expected public-folder shape." >&2
  exit 1
fi
echo "Applied idempotent LPE 0.4 schema compatibility updates."
LPE_BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
LPE_IMAP_BIND_ADDRESS="${LPE_IMAP_BIND_ADDRESS:-127.0.0.1:1143}"
validate_host_port "${LPE_IMAP_BIND_ADDRESS}" \
  || { echo "LPE_IMAP_BIND_ADDRESS must be a host:port address in ${ENV_FILE}; got '${LPE_IMAP_BIND_ADDRESS}'" >&2; exit 1; }
LPE_SERVER_NAME="${LPE_SERVER_NAME:-_}"
LPE_NGINX_LISTEN_PORT="${LPE_NGINX_LISTEN_PORT:-80}"
LPE_NGINX_CLIENT_MAX_BODY_SIZE="${LPE_NGINX_CLIENT_MAX_BODY_SIZE:-20g}"
LPE_PST_IMPORT_DIR="${LPE_PST_IMPORT_DIR:-${DATA_DIR}/imports}"
LPE_OUTBOUND_WORKER_INTERVAL_MS="${LPE_OUTBOUND_WORKER_INTERVAL_MS:-1000}"
LPE_OUTBOUND_WORKER_BATCH_SIZE="${LPE_OUTBOUND_WORKER_BATCH_SIZE:-50}"
write_env_value "${ENV_FILE}" "LPE_OUTBOUND_WORKER_INTERVAL_MS" "${LPE_OUTBOUND_WORKER_INTERVAL_MS}"
write_env_value "${ENV_FILE}" "LPE_OUTBOUND_WORKER_BATCH_SIZE" "${LPE_OUTBOUND_WORKER_BATCH_SIZE}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${LPE_PST_IMPORT_DIR}"

cd "${SRC_DIR}"
systemctl stop "${SERVICE_NAME}" || true
if rust_build_needed; then
  "${CARGO_BIN}" build --release -p lpe-cli
  install -m 0755 "${SRC_DIR}/target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
else
  echo "Skipping Rust build; Rust sources and installed lpe-cli are unchanged."
fi
if magika_install_needed; then
  install_magika "${MAGIKA_VERSION}" "${MAGIKA_LINUX_X86_64_SHA256}"
else
  echo "Skipping Magika install; installed binary matches requested version."
fi
install -d -o root -g root "${ADMIN_WEB_ROOT}" "${CLIENT_WEB_ROOT}"
build_web_app "web/admin" "${ADMIN_WEB_ROOT}" "admin"
build_web_app "web/client" "${CLIENT_WEB_ROOT}" "client"
render_template \
  "${SRC_DIR}/installation/debian-trixie/lpe.service" \
  "/etc/systemd/system/lpe.service" \
  "LPE_SERVICE_USER=${SERVICE_USER}" \
  "LPE_SERVICE_GROUP=${SERVICE_GROUP}" \
  "LPE_SRC_DIR=${SRC_DIR}" \
  "LPE_ENV_FILE=${ENV_FILE}" \
  "LPE_BIN_DIR=${BIN_DIR}" \
  "LPE_INSTALL_ROOT=${INSTALL_ROOT}" \
  "LPE_DATA_DIR=${DATA_DIR}"
render_template \
  "${SRC_DIR}/installation/debian-trixie/lpe.nginx.conf" \
  "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" \
  "LPE_NGINX_LISTEN_PORT=${LPE_NGINX_LISTEN_PORT}" \
  "LPE_BIND_ADDRESS=${LPE_BIND_ADDRESS}" \
  "LPE_SERVER_NAME=${LPE_SERVER_NAME}" \
  "LPE_NGINX_CLIENT_MAX_BODY_SIZE=${LPE_NGINX_CLIENT_MAX_BODY_SIZE}" \
  "LPE_ADMIN_WEB_ROOT=${ADMIN_WEB_ROOT}"

ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
rm -f "${NGINX_ENABLED_DIR}/default"
nginx -t

systemctl daemon-reload
systemctl restart "${SERVICE_NAME}"
systemctl restart nginx

echo "LPE updated from ${REPO_URL} (${BRANCH})."
