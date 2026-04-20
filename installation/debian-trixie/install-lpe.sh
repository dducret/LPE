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
ENV_DIR="${ENV_DIR:-/etc/lpe}"
ENV_FILE="${ENV_FILE:-$ENV_DIR/lpe.env}"
INSTALL_ENV_FILE="${INSTALL_ENV_FILE:-$ENV_DIR/install.env}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
DATA_DIR="${DATA_DIR:-/var/lib/lpe}"
SERVICE_USER="${SERVICE_USER:-lpe}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe}"
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-lpe.conf}"
MAGIKA_VERSION="${MAGIKA_VERSION:-1.0.2}"
MAGIKA_LINUX_X86_64_SHA256="${MAGIKA_LINUX_X86_64_SHA256:-4ce475c965cd20e724b5fc53e8a303a479b9d8649beef8721d05e9b3988fbab4}"
FIRST_INSTALL=0
if [[ ! -f "${INSTALL_ENV_FILE}" && ! -d "${SRC_DIR}/.git" ]]; then
  FIRST_INSTALL=1
fi
load_env_file_if_present "${INSTALL_ENV_FILE}"
load_env_file_if_present "${ENV_FILE}"

LPE_BIND_ADDRESS_CURRENT="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
LPE_LOCAL_BIND_HOST_DEFAULT="${LPE_LOCAL_BIND_HOST:-${LPE_BIND_ADDRESS_CURRENT%:*}}"
LPE_LOCAL_BIND_PORT_DEFAULT="${LPE_LOCAL_BIND_PORT:-${LPE_BIND_ADDRESS_CURRENT##*:}}"
LPE_NGINX_LISTEN_PORT_DEFAULT="${LPE_NGINX_LISTEN_PORT:-80}"
LPE_DB_HOST_DEFAULT="${LPE_DB_HOST:-localhost}"
LPE_DB_PORT_DEFAULT="${LPE_DB_PORT:-5432}"
LPE_DB_NAME_DEFAULT="${LPE_DB_NAME:-lpe}"
LPE_DB_USER_DEFAULT="${LPE_DB_USER:-lpe}"
LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME_DEFAULT="${LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME:-Bootstrap Administrator}"
LPE_PST_IMPORT_DIR_DEFAULT="${LPE_PST_IMPORT_DIR:-${DATA_DIR}/imports}"
LPE_PUBLIC_SCHEME_DEFAULT="${LPE_PUBLIC_SCHEME:-https}"
LPE_ENABLE_SERVICES_DEFAULT="${LPE_ENABLE_SERVICES:-yes}"
if [[ -n "${LPE_RUN_MIGRATIONS:-}" ]]; then
  LPE_RUN_MIGRATIONS_DEFAULT="${LPE_RUN_MIGRATIONS}"
elif [[ "${FIRST_INSTALL}" == "1" ]]; then
  LPE_RUN_MIGRATIONS_DEFAULT="yes"
else
  LPE_RUN_MIGRATIONS_DEFAULT="no"
fi

usage() {
  cat <<'EOF'
Usage: install-lpe.sh [--non-interactive] [--interactive]
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --non-interactive)
        INSTALL_NONINTERACTIVE=1
        shift
        ;;
      --interactive)
        INSTALL_FORCE_INTERACTIVE=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        fail_install "Unknown argument: $1"
        ;;
    esac
  done
}

install_magika() {
  local version="$1"
  local expected_sha="$2"
  local archive="magika-x86_64-unknown-linux-gnu.tar.xz"
  local url="https://github.com/google/magika/releases/download/cli/v${version}/${archive}"
  local temp_dir="/tmp/magika"
  local extracted_bin

  rm -rf "${temp_dir}"
  mkdir -p "${temp_dir}"
  trap 'rm -rf "${temp_dir}"' RETURN

  curl --proto '=https' --tlsv1.2 -LsSf "${url}" -o "${temp_dir}/${archive}"
  echo "${expected_sha}  ${temp_dir}/${archive}" | sha256sum -c -
  tar -xJf "${temp_dir}/${archive}" -C "${temp_dir}"
  extracted_bin="$(find "${temp_dir}" -type f -name magika | head -n 1)"
  [[ -n "${extracted_bin}" ]] || fail_install "magika binary not found after archive extraction."
  install -m 0755 "${extracted_bin}" "${BIN_DIR}/magika"
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    fail_install "This script must be run as root."
  fi
}

recompute_layout() {
  SRC_DIR="${INSTALL_ROOT}/src"
  BIN_DIR="${INSTALL_ROOT}/bin"
  WEB_ROOT="${INSTALL_ROOT}/www"
  ADMIN_WEB_ROOT="${WEB_ROOT}/admin"
  CLIENT_WEB_ROOT="${WEB_ROOT}/client"
}

collect_runtime_values() {
  local public_hostname_default="${LPE_PUBLIC_HOSTNAME:-}"
  local lpe_ct_api_base_url_default="${LPE_CT_API_BASE_URL:-}"
  local bootstrap_admin_email_default="${LPE_BOOTSTRAP_ADMIN_EMAIL:-}"
  local bootstrap_admin_password_default="${LPE_BOOTSTRAP_ADMIN_PASSWORD:-}"
  local db_password_default="${LPE_DB_PASSWORD:-}"
  local shared_secret_default="${LPE_INTEGRATION_SHARED_SECRET:-}"
  local service_choice_default="${LPE_ENABLE_SERVICES_DEFAULT}"
  local migrations_choice_default="${LPE_RUN_MIGRATIONS_DEFAULT}"

  print_section "Installation"
  INSTALL_ROOT="$(ask_with_default "Installation directory" "/opt/lpe" "validate_exact_path /opt/lpe" "Use /opt/lpe.")"
  recompute_layout

  print_section "Network"
  LPE_PUBLIC_HOSTNAME="$(ask_required "Public hostname" "${public_hostname_default}" validate_hostname "Enter a valid hostname.")"
  LPE_SERVER_NAME="$(ask_with_default "Server name" "${LPE_SERVER_NAME:-$LPE_PUBLIC_HOSTNAME}" validate_hostname "Enter a valid hostname.")"
  LPE_LOCAL_BIND_HOST="$(ask_with_default "Local service host" "${LPE_LOCAL_BIND_HOST_DEFAULT}" validate_host_token "Enter a valid host token.")"
  LPE_LOCAL_BIND_PORT="$(ask_with_default "Local service port" "${LPE_LOCAL_BIND_PORT_DEFAULT}" validate_port "Enter a valid TCP port.")"
  LPE_BIND_ADDRESS="${LPE_LOCAL_BIND_HOST}:${LPE_LOCAL_BIND_PORT}"
  LPE_NGINX_LISTEN_PORT="$(ask_with_default "HTTPS port" "${LPE_NGINX_LISTEN_PORT_DEFAULT}" validate_port "Enter a valid TCP port.")"

  print_section "Database"
  LPE_DB_HOST="$(ask_with_default "PostgreSQL host" "${LPE_DB_HOST_DEFAULT}" validate_host_token "Enter a valid PostgreSQL host.")"
  LPE_DB_PORT="$(ask_with_default "PostgreSQL port" "${LPE_DB_PORT_DEFAULT}" validate_port "Enter a valid PostgreSQL port.")"
  LPE_DB_NAME="$(ask_with_default "PostgreSQL database name" "${LPE_DB_NAME_DEFAULT}" validate_nonempty "Enter a PostgreSQL database name.")"
  LPE_DB_USER="$(ask_with_default "PostgreSQL username" "${LPE_DB_USER_DEFAULT}" validate_nonempty "Enter a PostgreSQL username.")"
  LPE_DB_PASSWORD="$(ask_secret_with_default_behavior_when_possible "PostgreSQL password" "${db_password_default}" validate_password_nonempty "Enter a PostgreSQL password.")"
  DATABASE_URL="$(build_postgres_url "${LPE_DB_HOST}" "${LPE_DB_PORT}" "${LPE_DB_NAME}" "${LPE_DB_USER}" "${LPE_DB_PASSWORD}")"

  print_section "Integration"
  if [[ -z "${lpe_ct_api_base_url_default}" ]]; then
    lpe_ct_api_base_url_default="http://${LPE_PUBLIC_HOSTNAME}:8380"
  fi
  LPE_CT_API_BASE_URL="$(ask_required "LPE-CT API base URL" "${lpe_ct_api_base_url_default}" validate_http_url "Enter a valid http:// or https:// URL.")"
  LPE_INTEGRATION_SHARED_SECRET="$(ask_secret_with_default_behavior_when_possible "Integration shared secret" "${shared_secret_default}" validate_shared_secret "Enter a strong secret with at least 32 characters.")"

  print_section "Administrator"
  LPE_BOOTSTRAP_ADMIN_EMAIL="$(ask_required "Admin email" "${bootstrap_admin_email_default}" validate_email "Enter a valid email address.")"
  LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME="$(ask_with_default "Admin display name" "${LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME_DEFAULT}" validate_nonempty "Enter an administrator display name.")"
  LPE_BOOTSTRAP_ADMIN_PASSWORD="$(ask_secret_with_default_behavior_when_possible "Admin password" "${bootstrap_admin_password_default}" validate_password_nonempty "Enter an administrator password.")"

  print_section "Services"
  LPE_ENABLE_SERVICES="$(ask_yes_no "Enable and start systemd services now" "${service_choice_default}")"
  LPE_RUN_MIGRATIONS="$(ask_yes_no "Run migrations now" "${migrations_choice_default}")"

  LPE_PUBLIC_SCHEME="${LPE_PUBLIC_SCHEME_DEFAULT}"
  LPE_PST_IMPORT_DIR="${LPE_PST_IMPORT_DIR_DEFAULT}"
  LPE_NGINX_CLIENT_MAX_BODY_SIZE="${LPE_NGINX_CLIENT_MAX_BODY_SIZE:-20g}"
  LPE_AUTODISCOVER_ACTIVESYNC_URL="$(format_public_url "${LPE_PUBLIC_SCHEME}" "${LPE_PUBLIC_HOSTNAME}" "${LPE_NGINX_LISTEN_PORT}" "/Microsoft-Server-ActiveSync")"
}

validate_runtime_values() {
  validate_nonempty "${LPE_DB_PASSWORD:-}" || fail_install "PostgreSQL password ended up empty during installation."
  validate_nonempty "${LPE_BOOTSTRAP_ADMIN_EMAIL:-}" || fail_install "Bootstrap administrator email ended up empty during installation."
  validate_nonempty "${LPE_BOOTSTRAP_ADMIN_PASSWORD:-}" || fail_install "Bootstrap administrator password ended up empty during installation."
  validate_shared_secret "${LPE_INTEGRATION_SHARED_SECRET:-}" || fail_install "Integration shared secret is missing or too weak."
  ensure_database_url || fail_install "DATABASE_URL could not be built from the PostgreSQL settings."
}

write_install_layout_file() {
  install -d -o root -g root "${ENV_DIR}"
  : > "${INSTALL_ENV_FILE}"
  write_env_value "${INSTALL_ENV_FILE}" "INSTALL_ROOT" "${INSTALL_ROOT}"
  write_env_value "${INSTALL_ENV_FILE}" "SRC_DIR" "${SRC_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "BIN_DIR" "${BIN_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "WEB_ROOT" "${WEB_ROOT}"
  write_env_value "${INSTALL_ENV_FILE}" "ADMIN_WEB_ROOT" "${ADMIN_WEB_ROOT}"
  write_env_value "${INSTALL_ENV_FILE}" "CLIENT_WEB_ROOT" "${CLIENT_WEB_ROOT}"
  write_env_value "${INSTALL_ENV_FILE}" "ENV_DIR" "${ENV_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "ENV_FILE" "${ENV_FILE}"
  write_env_value "${INSTALL_ENV_FILE}" "SYSTEMD_DIR" "${SYSTEMD_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "DATA_DIR" "${DATA_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "SERVICE_USER" "${SERVICE_USER}"
  write_env_value "${INSTALL_ENV_FILE}" "SERVICE_GROUP" "${SERVICE_GROUP}"
  write_env_value "${INSTALL_ENV_FILE}" "NGINX_AVAILABLE_DIR" "${NGINX_AVAILABLE_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "NGINX_ENABLED_DIR" "${NGINX_ENABLED_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "NGINX_SITE_NAME" "${NGINX_SITE_NAME}"
}

write_runtime_env_file() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    install -m 0640 "${SCRIPT_DIR}/lpe.env.example" "${ENV_FILE}"
  fi

  write_env_value "${ENV_FILE}" "RUST_LOG" "${RUST_LOG:-info}"
  write_env_value "${ENV_FILE}" "LPE_BIND_ADDRESS" "${LPE_BIND_ADDRESS}"
  write_env_value "${ENV_FILE}" "LPE_LOCAL_BIND_HOST" "${LPE_LOCAL_BIND_HOST}"
  write_env_value "${ENV_FILE}" "LPE_LOCAL_BIND_PORT" "${LPE_LOCAL_BIND_PORT}"
  write_env_value "${ENV_FILE}" "LPE_SERVER_NAME" "${LPE_SERVER_NAME}"
  write_env_value "${ENV_FILE}" "LPE_NGINX_LISTEN_PORT" "${LPE_NGINX_LISTEN_PORT}"
  write_env_value "${ENV_FILE}" "LPE_DB_HOST" "${LPE_DB_HOST}"
  write_env_value "${ENV_FILE}" "LPE_DB_PORT" "${LPE_DB_PORT}"
  write_env_value "${ENV_FILE}" "LPE_DB_NAME" "${LPE_DB_NAME}"
  write_env_value "${ENV_FILE}" "LPE_DB_USER" "${LPE_DB_USER}"
  write_env_value "${ENV_FILE}" "LPE_DB_PASSWORD" "${LPE_DB_PASSWORD}"
  write_env_value "${ENV_FILE}" "DATABASE_URL" "${DATABASE_URL}"
  write_env_value "${ENV_FILE}" "LPE_CT_API_BASE_URL" "${LPE_CT_API_BASE_URL}"
  write_env_value "${ENV_FILE}" "LPE_INTEGRATION_SHARED_SECRET" "${LPE_INTEGRATION_SHARED_SECRET}"
  write_env_value "${ENV_FILE}" "LPE_BOOTSTRAP_ADMIN_EMAIL" "${LPE_BOOTSTRAP_ADMIN_EMAIL}"
  write_env_value "${ENV_FILE}" "LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME" "${LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME}"
  write_env_value "${ENV_FILE}" "LPE_BOOTSTRAP_ADMIN_PASSWORD" "${LPE_BOOTSTRAP_ADMIN_PASSWORD}"
  write_env_value "${ENV_FILE}" "LPE_PST_IMPORT_DIR" "${LPE_PST_IMPORT_DIR}"
  write_env_value "${ENV_FILE}" "LPE_NGINX_CLIENT_MAX_BODY_SIZE" "${LPE_NGINX_CLIENT_MAX_BODY_SIZE}"
  write_env_value "${ENV_FILE}" "LPE_MAGIKA_BIN" "${BIN_DIR}/magika"
  write_env_value "${ENV_FILE}" "LPE_MAGIKA_MIN_SCORE" "${LPE_MAGIKA_MIN_SCORE:-0.80}"
  write_env_value "${ENV_FILE}" "LPE_PUBLIC_SCHEME" "${LPE_PUBLIC_SCHEME}"
  write_env_value "${ENV_FILE}" "LPE_PUBLIC_HOSTNAME" "${LPE_PUBLIC_HOSTNAME}"
  write_env_value "${ENV_FILE}" "LPE_AUTODISCOVER_ACTIVESYNC_URL" "${LPE_AUTODISCOVER_ACTIVESYNC_URL}"
}

verify_runtime_env_file() {
  local env_db_password=""
  local env_database_url=""
  local env_admin_email=""
  local env_admin_password=""
  local env_shared_secret=""

  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a

  env_db_password="${LPE_DB_PASSWORD:-}"
  env_database_url="${DATABASE_URL:-}"
  env_admin_email="${LPE_BOOTSTRAP_ADMIN_EMAIL:-}"
  env_admin_password="${LPE_BOOTSTRAP_ADMIN_PASSWORD:-}"
  env_shared_secret="${LPE_INTEGRATION_SHARED_SECRET:-}"

  validate_nonempty "${env_db_password}" || fail_install "LPE_DB_PASSWORD was written empty to ${ENV_FILE}."
  validate_nonempty "${env_database_url}" || fail_install "DATABASE_URL was written empty to ${ENV_FILE}."
  validate_nonempty "${env_admin_email}" || fail_install "LPE_BOOTSTRAP_ADMIN_EMAIL was written empty to ${ENV_FILE}."
  validate_nonempty "${env_admin_password}" || fail_install "LPE_BOOTSTRAP_ADMIN_PASSWORD was written empty to ${ENV_FILE}."
  validate_shared_secret "${env_shared_secret}" || fail_install "LPE_INTEGRATION_SHARED_SECRET was written missing or too weak to ${ENV_FILE}."
}

install_prerequisites() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    curl \
    git \
    libpq-dev \
    nginx \
    nodejs \
    npm \
    pkg-config \
    postgresql-client \
    rustup \
    xz-utils
}

ensure_service_user() {
  if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    useradd --system --home-dir "${INSTALL_ROOT}" --create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
  fi
}

prepare_directories() {
  install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${INSTALL_ROOT}" "${SRC_DIR}" "${BIN_DIR}"
  install -d -o root -g root "${ADMIN_WEB_ROOT}" "${CLIENT_WEB_ROOT}" "${ENV_DIR}"
  install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${DATA_DIR}" "${LPE_PST_IMPORT_DIR}"
}

checkout_source() {
  git config --global --add safe.directory "${SRC_DIR}" || true

  if [[ ! -d "${SRC_DIR}/.git" ]]; then
    git clone --branch "${BRANCH}" "${REPO_URL}" "${SRC_DIR}"
    git -C "${SRC_DIR}" config core.fileMode false
    return 0
  fi

  # Ignore local chmod noise in the installed checkout so maintenance pulls do
  # not fail on mode-only changes to helper scripts.
  git -C "${SRC_DIR}" config core.fileMode false
  git -C "${SRC_DIR}" fetch --all --tags
  git -C "${SRC_DIR}" checkout "${BRANCH}"
  git -C "${SRC_DIR}" pull --ff-only origin "${BRANCH}"
}

prepare_rust() {
  local rustup_bin
  rustup_bin="$(command -v rustup || true)"
  [[ -n "${rustup_bin}" ]] || fail_install "rustup executable not found after package installation."
  "${rustup_bin}" default stable
  export PATH="/root/.cargo/bin:${PATH}"
}

build_lpe() {
  local cargo_bin
  cargo_bin="$(command -v cargo || true)"
  [[ -n "${cargo_bin}" ]] || fail_install "cargo executable not found after rustup toolchain initialization."

  cd "${SRC_DIR}"
  "${cargo_bin}" build --release -p lpe-cli
  [[ -x "target/release/lpe-cli" ]] || fail_install "lpe-cli binary not found after build."

  install -m 0755 "target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
  install_magika "${MAGIKA_VERSION}" "${MAGIKA_LINUX_X86_64_SHA256}"
}

build_web() {
  cd "${SRC_DIR}/web/admin"
  npm ci
  npm run build

  cd "${SRC_DIR}/web/client"
  npm ci
  npm run build

  cp -a "${SRC_DIR}/web/admin/dist/." "${ADMIN_WEB_ROOT}/"
  cp -a "${SRC_DIR}/web/client/dist/." "${CLIENT_WEB_ROOT}/"
}

render_service_files() {
  render_template \
    "${SCRIPT_DIR}/lpe.service" \
    "${SYSTEMD_DIR}/lpe.service" \
    "LPE_SERVICE_USER=${SERVICE_USER}" \
    "LPE_SERVICE_GROUP=${SERVICE_GROUP}" \
    "LPE_SRC_DIR=${SRC_DIR}" \
    "LPE_ENV_FILE=${ENV_FILE}" \
    "LPE_BIN_DIR=${BIN_DIR}" \
    "LPE_INSTALL_ROOT=${INSTALL_ROOT}" \
    "LPE_DATA_DIR=${DATA_DIR}"

  render_template \
    "${SCRIPT_DIR}/lpe.nginx.conf" \
    "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" \
    "LPE_NGINX_LISTEN_PORT=${LPE_NGINX_LISTEN_PORT}" \
    "LPE_SERVER_NAME=${LPE_SERVER_NAME}" \
    "LPE_BIND_ADDRESS=${LPE_BIND_ADDRESS}" \
    "LPE_NGINX_CLIENT_MAX_BODY_SIZE=${LPE_NGINX_CLIENT_MAX_BODY_SIZE}" \
    "LPE_ADMIN_WEB_ROOT=${ADMIN_WEB_ROOT}"
}

activate_services() {
  ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
  rm -f "${NGINX_ENABLED_DIR}/default"
  nginx -t

  systemctl daemon-reload

  if [[ "${LPE_ENABLE_SERVICES}" == "yes" ]]; then
    systemctl enable lpe.service
    systemctl enable nginx
    systemctl restart lpe.service
    systemctl restart nginx
    return 0
  fi

  echo "Services were installed but not started."
}

run_schema_init_if_requested() {
  if [[ "${LPE_RUN_MIGRATIONS}" == "yes" ]]; then
    "${SCRIPT_DIR}/init-schema.sh"
  fi
}

main() {
  parse_args "$@"
  require_root
  collect_runtime_values
  validate_runtime_values
  install_prerequisites
  ensure_service_user
  prepare_directories
  write_install_layout_file
  checkout_source
  prepare_rust
  build_lpe
  write_runtime_env_file
  verify_runtime_env_file
  build_web
  render_service_files
  run_schema_init_if_requested
  activate_services

  echo "LPE installed in ${INSTALL_ROOT}."
  echo "Runtime configuration written to ${ENV_FILE}."
  echo "Install layout written to ${INSTALL_ENV_FILE}."
}

main "$@"
