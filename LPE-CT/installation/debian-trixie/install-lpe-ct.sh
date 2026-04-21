#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=installation/debian-trixie/lib/install-common.sh
source "${SCRIPT_DIR}/../../../installation/debian-trixie/lib/install-common.sh"

REPO_URL="${REPO_URL:-https://github.com/dducret/LPE}"
BRANCH="${BRANCH:-main}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe-ct}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_DIR="${BIN_DIR:-$INSTALL_ROOT/bin}"
WEB_ROOT="${WEB_ROOT:-$INSTALL_ROOT/www/management}"
VENDOR_DIR="${VENDOR_DIR:-$INSTALL_ROOT/vendor}"
ENV_DIR="${ENV_DIR:-/etc/lpe-ct}"
ENV_FILE="${ENV_FILE:-$ENV_DIR/lpe-ct.env}"
INSTALL_ENV_FILE="${INSTALL_ENV_FILE:-$ENV_DIR/install.env}"
STATE_DIR="${STATE_DIR:-/var/lib/lpe-ct}"
SPOOL_DIR="${SPOOL_DIR:-/var/spool/lpe-ct}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
SERVICE_USER="${SERVICE_USER:-lpe-ct}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe-ct}"
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-lpe-ct.conf}"
MAGIKA_VERSION="${MAGIKA_VERSION:-1.0.2}"
MAGIKA_LINUX_X86_64_SHA256="${MAGIKA_LINUX_X86_64_SHA256:-4ce475c965cd20e724b5fc53e8a303a479b9d8649beef8721d05e9b3988fbab4}"
TAKERI_REPO_URL="${TAKERI_REPO_URL:-https://github.com/AnimeForLife191/Shuhari-CyberForge.git}"
TAKERI_BRANCH="${TAKERI_BRANCH:-main}"
TAKERI_SYNC_DIR="${TAKERI_SYNC_DIR:-$VENDOR_DIR/takeri-src}"
TAKERI_BIN_PATH="${TAKERI_BIN_PATH:-$BIN_DIR/Shuhari-CyberForge-CLI}"
load_env_file_if_present "${INSTALL_ENV_FILE}"
load_env_file_if_present "${ENV_FILE}"

LPE_CT_BIND_ADDRESS_CURRENT="${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}"
LPE_CT_SMTP_BIND_ADDRESS_CURRENT="${LPE_CT_SMTP_BIND_ADDRESS:-0.0.0.0:25}"
LPE_CT_BIND_HOST_DEFAULT="${LPE_CT_BIND_HOST:-${LPE_CT_BIND_ADDRESS_CURRENT%:*}}"
LPE_CT_BIND_PORT_DEFAULT="${LPE_CT_BIND_PORT:-${LPE_CT_BIND_ADDRESS_CURRENT##*:}}"
LPE_CT_SMTP_HOST_DEFAULT="${LPE_CT_SMTP_HOST:-${LPE_CT_SMTP_BIND_ADDRESS_CURRENT%:*}}"
LPE_CT_SMTP_PORT_DEFAULT="${LPE_CT_SMTP_PORT:-${LPE_CT_SMTP_BIND_ADDRESS_CURRENT##*:}}"
LPE_CT_NGINX_LISTEN_PORT_DEFAULT="${LPE_CT_NGINX_LISTEN_PORT:-80}"
LPE_CT_BOOTSTRAP_ADMIN_EMAIL_DEFAULT="${LPE_CT_BOOTSTRAP_ADMIN_EMAIL:-}"
LPE_CT_BOOTSTRAP_ADMIN_PASSWORD_DEFAULT="${LPE_CT_BOOTSTRAP_ADMIN_PASSWORD:-}"
LPE_CT_CORE_DELIVERY_BASE_URL_DEFAULT="${LPE_CT_CORE_DELIVERY_BASE_URL:-}"
LPE_CT_RELAY_PRIMARY_DEFAULT="${LPE_CT_RELAY_PRIMARY:-smtp://10.20.0.12:2525}"
LPE_CT_RELAY_SECONDARY_DEFAULT="${LPE_CT_RELAY_SECONDARY:-smtp://10.20.0.13:2525}"
LPE_CT_ENABLE_SERVICES_DEFAULT="${LPE_CT_ENABLE_SERVICES:-yes}"

usage() {
  cat <<'EOF'
Usage: install-lpe-ct.sh [--non-interactive] [--interactive]
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
  trap "rm -rf '${temp_dir}'" RETURN

  curl --proto '=https' --tlsv1.2 -LsSf "${url}" -o "${temp_dir}/${archive}"
  echo "${expected_sha}  ${temp_dir}/${archive}" | sha256sum -c -
  tar -xJf "${temp_dir}/${archive}" -C "${temp_dir}"
  extracted_bin="$(find "${temp_dir}" -type f -name magika | head -n 1)"
  [[ -n "${extracted_bin}" ]] || fail_install "magika binary not found after archive extraction."
  install -m 0755 "${extracted_bin}" "${BIN_DIR}/magika"
  trap - RETURN
  rm -rf "${temp_dir}"
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    fail_install "This script must be run as root."
  fi
}

recompute_layout() {
  SRC_DIR="${INSTALL_ROOT}/src"
  BIN_DIR="${INSTALL_ROOT}/bin"
  WEB_ROOT="${INSTALL_ROOT}/www/management"
  VENDOR_DIR="${INSTALL_ROOT}/vendor"
  TAKERI_SYNC_DIR="${VENDOR_DIR}/takeri-src"
  TAKERI_BIN_PATH="${BIN_DIR}/Shuhari-CyberForge-CLI"
}

collect_runtime_values() {
  local shared_secret_default="${LPE_INTEGRATION_SHARED_SECRET:-}"

  print_section "Installation"
  INSTALL_ROOT="$(ask_with_default "Installation directory" "/opt/lpe-ct" "validate_exact_path /opt/lpe-ct" "Use /opt/lpe-ct.")"
  recompute_layout

  print_section "Network"
  LPE_CT_PUBLIC_HOSTNAME="$(ask_required "Public hostname" "${LPE_CT_PUBLIC_HOSTNAME:-${LPE_CT_SERVER_NAME:-}}" validate_hostname "Enter a valid hostname.")"
  LPE_CT_SERVER_NAME="$(ask_with_default "Server name" "${LPE_CT_SERVER_NAME:-$LPE_CT_PUBLIC_HOSTNAME}" validate_hostname "Enter a valid hostname.")"
  LPE_CT_BIND_HOST="$(ask_with_default "Local management host" "${LPE_CT_BIND_HOST_DEFAULT}" validate_host_token "Enter a valid host token.")"
  LPE_CT_BIND_PORT="$(ask_with_default "Local management port" "${LPE_CT_BIND_PORT_DEFAULT}" validate_port "Enter a valid TCP port.")"
  LPE_CT_BIND_ADDRESS="${LPE_CT_BIND_HOST}:${LPE_CT_BIND_PORT}"
  LPE_CT_SMTP_HOST="$(ask_with_default "SMTP ingress host" "${LPE_CT_SMTP_HOST_DEFAULT}" validate_host_token "Enter a valid host token.")"
  LPE_CT_SMTP_PORT="$(ask_with_default "SMTP ingress port" "${LPE_CT_SMTP_PORT_DEFAULT}" validate_port "Enter a valid TCP port.")"
  LPE_CT_SMTP_BIND_ADDRESS="${LPE_CT_SMTP_HOST}:${LPE_CT_SMTP_PORT}"
  LPE_CT_NGINX_LISTEN_PORT="$(ask_with_default "HTTPS port" "${LPE_CT_NGINX_LISTEN_PORT_DEFAULT}" validate_port "Enter a valid TCP port.")"

  print_section "Integration"
  LPE_CT_CORE_DELIVERY_BASE_URL="$(ask_required "Internal LPE delivery URL" "${LPE_CT_CORE_DELIVERY_BASE_URL_DEFAULT}" validate_http_url "Enter a valid http:// or https:// URL.")"
  LPE_INTEGRATION_SHARED_SECRET="$(ask_secret_with_default_behavior_when_possible "Integration shared secret" "${shared_secret_default}" validate_shared_secret "Enter a strong secret with at least 32 characters.")"
  LPE_CT_RELAY_PRIMARY="$(ask_with_default "Primary relay endpoint" "${LPE_CT_RELAY_PRIMARY_DEFAULT}" validate_smtp_url "Enter a valid smtp:// relay endpoint.")"
  LPE_CT_RELAY_SECONDARY="$(ask_with_default "Secondary relay endpoint" "${LPE_CT_RELAY_SECONDARY_DEFAULT}" validate_smtp_url "Enter a valid smtp:// relay endpoint.")"

  print_section "Storage"
  SPOOL_DIR="$(ask_with_default "Quarantine root path" "${SPOOL_DIR}" validate_directory_path "Enter an absolute directory path.")"

  print_section "Administrator"
  LPE_CT_BOOTSTRAP_ADMIN_EMAIL="$(ask_required "Admin email" "${LPE_CT_BOOTSTRAP_ADMIN_EMAIL_DEFAULT}" validate_email "Enter a valid email address.")"
  LPE_CT_BOOTSTRAP_ADMIN_PASSWORD="$(ask_secret_with_default_behavior_when_possible "Admin password" "${LPE_CT_BOOTSTRAP_ADMIN_PASSWORD_DEFAULT}" validate_password_nonempty "Enter an administrator password.")"

  print_section "Services"
  LPE_CT_ENABLE_SERVICES="$(ask_yes_no "Enable and start systemd services now" "${LPE_CT_ENABLE_SERVICES_DEFAULT}")"
}

write_install_layout_file() {
  install -d -o root -g root "${ENV_DIR}"
  : > "${INSTALL_ENV_FILE}"
  write_env_value "${INSTALL_ENV_FILE}" "INSTALL_ROOT" "${INSTALL_ROOT}"
  write_env_value "${INSTALL_ENV_FILE}" "SRC_DIR" "${SRC_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "BIN_DIR" "${BIN_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "WEB_ROOT" "${WEB_ROOT}"
  write_env_value "${INSTALL_ENV_FILE}" "VENDOR_DIR" "${VENDOR_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "ENV_DIR" "${ENV_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "ENV_FILE" "${ENV_FILE}"
  write_env_value "${INSTALL_ENV_FILE}" "INSTALL_ENV_FILE" "${INSTALL_ENV_FILE}"
  write_env_value "${INSTALL_ENV_FILE}" "STATE_DIR" "${STATE_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "SPOOL_DIR" "${SPOOL_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "SYSTEMD_DIR" "${SYSTEMD_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "SERVICE_USER" "${SERVICE_USER}"
  write_env_value "${INSTALL_ENV_FILE}" "SERVICE_GROUP" "${SERVICE_GROUP}"
  write_env_value "${INSTALL_ENV_FILE}" "NGINX_AVAILABLE_DIR" "${NGINX_AVAILABLE_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "NGINX_ENABLED_DIR" "${NGINX_ENABLED_DIR}"
  write_env_value "${INSTALL_ENV_FILE}" "NGINX_SITE_NAME" "${NGINX_SITE_NAME}"
}

write_runtime_env_file() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    install -m 0640 "${SCRIPT_DIR}/lpe-ct.env.example" "${ENV_FILE}"
  fi

  write_env_value "${ENV_FILE}" "LPE_CT_BIND_ADDRESS" "${LPE_CT_BIND_ADDRESS}"
  write_env_value "${ENV_FILE}" "LPE_CT_BIND_HOST" "${LPE_CT_BIND_HOST}"
  write_env_value "${ENV_FILE}" "LPE_CT_BIND_PORT" "${LPE_CT_BIND_PORT}"
  write_env_value "${ENV_FILE}" "LPE_CT_SMTP_BIND_ADDRESS" "${LPE_CT_SMTP_BIND_ADDRESS}"
  write_env_value "${ENV_FILE}" "LPE_CT_SMTP_HOST" "${LPE_CT_SMTP_HOST}"
  write_env_value "${ENV_FILE}" "LPE_CT_SMTP_PORT" "${LPE_CT_SMTP_PORT}"
  write_env_value "${ENV_FILE}" "LPE_CT_SERVER_NAME" "${LPE_CT_SERVER_NAME}"
  write_env_value "${ENV_FILE}" "LPE_CT_PUBLIC_HOSTNAME" "${LPE_CT_PUBLIC_HOSTNAME}"
  write_env_value "${ENV_FILE}" "LPE_CT_NGINX_LISTEN_PORT" "${LPE_CT_NGINX_LISTEN_PORT}"
  write_env_value "${ENV_FILE}" "LPE_CT_STATE_FILE" "${STATE_DIR}/state.json"
  write_env_value "${ENV_FILE}" "LPE_CT_SPOOL_DIR" "${SPOOL_DIR}"
  write_env_value "${ENV_FILE}" "LPE_CT_BOOTSTRAP_ADMIN_EMAIL" "${LPE_CT_BOOTSTRAP_ADMIN_EMAIL}"
  write_env_value "${ENV_FILE}" "LPE_CT_BOOTSTRAP_ADMIN_PASSWORD" "${LPE_CT_BOOTSTRAP_ADMIN_PASSWORD}"
  write_env_value "${ENV_FILE}" "LPE_CT_CORE_DELIVERY_BASE_URL" "${LPE_CT_CORE_DELIVERY_BASE_URL}"
  write_env_value "${ENV_FILE}" "LPE_CT_RELAY_PRIMARY" "${LPE_CT_RELAY_PRIMARY}"
  write_env_value "${ENV_FILE}" "LPE_CT_RELAY_SECONDARY" "${LPE_CT_RELAY_SECONDARY}"
  write_env_value "${ENV_FILE}" "LPE_INTEGRATION_SHARED_SECRET" "${LPE_INTEGRATION_SHARED_SECRET}"
  write_env_value "${ENV_FILE}" "RUST_LOG" "${RUST_LOG:-info}"
  write_env_value "${ENV_FILE}" "LPE_MAGIKA_BIN" "${BIN_DIR}/magika"
  write_env_value "${ENV_FILE}" "LPE_MAGIKA_MIN_SCORE" "${LPE_MAGIKA_MIN_SCORE:-0.80}"
  write_env_value "${ENV_FILE}" "LPE_CT_ANTIVIRUS_TAKERI_BIN" "${TAKERI_BIN_PATH}"
  write_env_value "${ENV_FILE}" "LPE_CT_ANTIVIRUS_TAKERI_REPO_URL" "${TAKERI_REPO_URL}"
  write_env_value "${ENV_FILE}" "LPE_CT_ANTIVIRUS_TAKERI_BRANCH" "${TAKERI_BRANCH}"
  write_env_value "${ENV_FILE}" "LPE_CT_ANTIVIRUS_TAKERI_SYNC_DIR" "${TAKERI_SYNC_DIR}"
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
    nginx \
    pkg-config \
    rustup \
    xz-utils
}

ensure_service_user() {
  if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    useradd --system --home-dir "${INSTALL_ROOT}" --create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
  fi
}

prepare_directories() {
  install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${INSTALL_ROOT}" "${SRC_DIR}" "${BIN_DIR}" "${VENDOR_DIR}" "${STATE_DIR}" "${SPOOL_DIR}"
  install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${SPOOL_DIR}/incoming" "${SPOOL_DIR}/deferred" "${SPOOL_DIR}/quarantine" "${SPOOL_DIR}/held" "${SPOOL_DIR}/sent"
  install -d -o root -g root "${WEB_ROOT}" "${ENV_DIR}"
}

checkout_source() {
  git config --global --add safe.directory "${SRC_DIR}" || true

  if [[ ! -d "${SRC_DIR}/.git" ]]; then
    git clone --branch "${BRANCH}" "${REPO_URL}" "${SRC_DIR}"
    return 0
  fi

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

build_lpe_ct() {
  local cargo_bin
  cargo_bin="$(command -v cargo || true)"
  [[ -n "${cargo_bin}" ]] || fail_install "cargo executable not found after rustup toolchain initialization."

  cd "${SRC_DIR}"
  "${cargo_bin}" build --release --manifest-path "${SRC_DIR}/LPE-CT/Cargo.toml"
  install -m 0755 "${SRC_DIR}/LPE-CT/target/release/lpe-ct" "${BIN_DIR}/lpe-ct"

  TAKERI_REPO_URL="${TAKERI_REPO_URL}" \
  TAKERI_BRANCH="${TAKERI_BRANCH}" \
  TAKERI_SYNC_DIR="${TAKERI_SYNC_DIR}" \
  TAKERI_BIN_PATH="${TAKERI_BIN_PATH}" \
  CARGO_BIN="${cargo_bin}" \
  bash "${SRC_DIR}/LPE-CT/installation/debian-trixie/sync-takeri.sh"

  install_magika "${MAGIKA_VERSION}" "${MAGIKA_LINUX_X86_64_SHA256}"
}

deploy_web() {
  cp -a "${SRC_DIR}/LPE-CT/web/." "${WEB_ROOT}/"
}

render_service_files() {
  render_template \
    "${SCRIPT_DIR}/lpe-ct.service" \
    "${SYSTEMD_DIR}/lpe-ct.service" \
    "LPE_CT_SERVICE_USER=${SERVICE_USER}" \
    "LPE_CT_SERVICE_GROUP=${SERVICE_GROUP}" \
    "LPE_CT_SRC_DIR=${SRC_DIR}" \
    "LPE_CT_ENV_FILE=${ENV_FILE}" \
    "LPE_CT_BIN_DIR=${BIN_DIR}" \
    "LPE_CT_INSTALL_ROOT=${INSTALL_ROOT}" \
    "LPE_CT_STATE_DIR=${STATE_DIR}" \
    "LPE_CT_SPOOL_DIR=${SPOOL_DIR}"

  render_template \
    "${SCRIPT_DIR}/lpe-ct.nginx.conf" \
    "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" \
    "LPE_CT_NGINX_LISTEN_PORT=${LPE_CT_NGINX_LISTEN_PORT}" \
    "LPE_CT_SERVER_NAME=${LPE_CT_SERVER_NAME}" \
    "LPE_CT_BIND_ADDRESS=${LPE_CT_BIND_ADDRESS}" \
    "LPE_CT_WEB_ROOT=${WEB_ROOT}"
}

activate_services() {
  ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
  rm -f "${NGINX_ENABLED_DIR}/default"
  nginx -t

  chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${STATE_DIR}" "${SPOOL_DIR}" "${VENDOR_DIR}"
  systemctl daemon-reload

  if [[ "${LPE_CT_ENABLE_SERVICES}" == "yes" ]]; then
    systemctl enable lpe-ct.service
    systemctl enable nginx
    systemctl restart lpe-ct.service
    systemctl restart nginx
    return 0
  fi

  echo "Services were installed but not started."
}

main() {
  parse_args "$@"
  require_root
  collect_runtime_values
  install_prerequisites
  ensure_service_user
  prepare_directories
  write_install_layout_file
  checkout_source
  prepare_rust
  build_lpe_ct
  write_runtime_env_file
  deploy_web
  render_service_files
  activate_services

  echo "LPE-CT installed in ${INSTALL_ROOT}."
  echo "Runtime configuration written to ${ENV_FILE}."
  echo "Install layout written to ${INSTALL_ENV_FILE}."
}

main "$@"
