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
ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
INSTALL_ENV_FILE="${INSTALL_ENV_FILE:-/etc/lpe-ct/install.env}"
SPOOL_DIR="${SPOOL_DIR:-/var/spool/lpe-ct}"
SERVICE_NAME="${SERVICE_NAME:-lpe-ct.service}"
SERVICE_USER="${SERVICE_USER:-lpe-ct}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe-ct}"
STATE_DIR="${STATE_DIR:-/var/lib/lpe-ct}"
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

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

if [[ ! -d "${SRC_DIR}/.git" ]]; then
  echo "Source repository not found in ${SRC_DIR}. Run install-lpe-ct.sh first." >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found in ${ENV_FILE}. Run install-lpe-ct.sh first." >&2
  exit 1
fi
load_env_file_if_present "${ENV_FILE}"
LPE_CT_CORE_DELIVERY_BASE_URL="${LPE_CT_CORE_DELIVERY_BASE_URL:-http://127.0.0.1:8080}"
LPE_CT_CORE_DELIVERY_BASE_URL="${LPE_CT_CORE_DELIVERY_BASE_URL%/}"
LPE_CT_PUBLIC_TLS_CERT_PATH="${LPE_CT_PUBLIC_TLS_CERT_PATH:-/etc/lpe-ct/tls/fullchain.pem}"
LPE_CT_PUBLIC_TLS_KEY_PATH="${LPE_CT_PUBLIC_TLS_KEY_PATH:-/etc/lpe-ct/tls/privkey.pem}"

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

git config --global --add safe.directory "${SRC_DIR}" || true

RUSTUP_BIN="$(command -v rustup || true)"
if [[ -z "${RUSTUP_BIN}" ]]; then
  echo "rustup executable not found. Install prerequisites first." >&2
  exit 1
fi

git -C "${SRC_DIR}" remote set-url origin "${REPO_URL}" || true
git -C "${SRC_DIR}" fetch --all --tags
git -C "${SRC_DIR}" checkout "${BRANCH}"
git -C "${SRC_DIR}" pull --ff-only origin "${BRANCH}"

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

TAKERI_REPO_URL="${LPE_CT_ANTIVIRUS_TAKERI_REPO_URL:-$TAKERI_REPO_URL}"
TAKERI_BRANCH="${LPE_CT_ANTIVIRUS_TAKERI_BRANCH:-$TAKERI_BRANCH}"
TAKERI_SYNC_DIR="${LPE_CT_ANTIVIRUS_TAKERI_SYNC_DIR:-$TAKERI_SYNC_DIR}"
TAKERI_BIN_PATH="${LPE_CT_ANTIVIRUS_TAKERI_BIN:-$TAKERI_BIN_PATH}"

LPE_CT_RESET_STATE_ON_UPDATE="${LPE_CT_RESET_STATE_ON_UPDATE:-false}"
LPE_CT_BIND_ADDRESS="${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}"
LPE_CT_SERVER_NAME="${LPE_CT_SERVER_NAME:-_}"
LPE_CT_NGINX_LISTEN_PORT="${LPE_CT_NGINX_LISTEN_PORT:-443}"
LPE_CT_PUBLIC_TLS_CERT_PATH="${LPE_CT_PUBLIC_TLS_CERT_PATH:-/etc/lpe-ct/tls/fullchain.pem}"
LPE_CT_PUBLIC_TLS_KEY_PATH="${LPE_CT_PUBLIC_TLS_KEY_PATH:-/etc/lpe-ct/tls/privkey.pem}"

if [[ "${LPE_CT_RESET_STATE_ON_UPDATE}" == "true" ]]; then
  systemctl stop "${SERVICE_NAME}" || true
  rm -f "${LPE_CT_STATE_FILE:-/var/lib/lpe-ct/state.json}"
  rm -rf \
    "${SPOOL_DIR}/incoming" \
    "${SPOOL_DIR}/outbound" \
    "${SPOOL_DIR}/deferred" \
    "${SPOOL_DIR}/quarantine" \
    "${SPOOL_DIR}/held" \
    "${SPOOL_DIR}/bounces" \
    "${SPOOL_DIR}/policy" \
    "${SPOOL_DIR}/greylist" \
    "${SPOOL_DIR}/sent"
fi

cd "${SRC_DIR}"
"${CARGO_BIN}" build --release --manifest-path "${SRC_DIR}/LPE-CT/Cargo.toml"

install -m 0755 "${SRC_DIR}/LPE-CT/target/release/lpe-ct" "${BIN_DIR}/lpe-ct"
TAKERI_REPO_URL="${TAKERI_REPO_URL}" \
TAKERI_BRANCH="${TAKERI_BRANCH}" \
TAKERI_SYNC_DIR="${TAKERI_SYNC_DIR}" \
TAKERI_BIN_PATH="${TAKERI_BIN_PATH}" \
CARGO_BIN="${CARGO_BIN}" \
bash "${SRC_DIR}/LPE-CT/installation/debian-trixie/sync-takeri.sh"
install_magika "${MAGIKA_VERSION}" "${MAGIKA_LINUX_X86_64_SHA256}"
install -d -o root -g root "${WEB_ROOT}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" \
  "${VENDOR_DIR}" \
  "${SPOOL_DIR}" \
  "${SPOOL_DIR}/incoming" \
  "${SPOOL_DIR}/outbound" \
  "${SPOOL_DIR}/deferred" \
  "${SPOOL_DIR}/quarantine" \
  "${SPOOL_DIR}/held" \
  "${SPOOL_DIR}/bounces" \
  "${SPOOL_DIR}/sent" \
  "${SPOOL_DIR}/policy" \
  "${SPOOL_DIR}/greylist"
cp -a "${SRC_DIR}/LPE-CT/web/." "${WEB_ROOT}/"
render_template \
  "${SRC_DIR}/LPE-CT/installation/debian-trixie/lpe-ct.service" \
  "/etc/systemd/system/lpe-ct.service" \
  "LPE_CT_SERVICE_USER=${SERVICE_USER}" \
  "LPE_CT_SERVICE_GROUP=${SERVICE_GROUP}" \
  "LPE_CT_SRC_DIR=${SRC_DIR}" \
  "LPE_CT_ENV_FILE=${ENV_FILE}" \
  "LPE_CT_BIN_DIR=${BIN_DIR}" \
  "LPE_CT_INSTALL_ROOT=${INSTALL_ROOT}" \
  "LPE_CT_STATE_DIR=${STATE_DIR}" \
  "LPE_CT_SPOOL_DIR=${SPOOL_DIR}"
render_template \
  "${SRC_DIR}/LPE-CT/installation/debian-trixie/lpe-ct.nginx.conf" \
  "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" \
  "LPE_CT_NGINX_LISTEN_PORT=${LPE_CT_NGINX_LISTEN_PORT}" \
  "LPE_CT_BIND_ADDRESS=${LPE_CT_BIND_ADDRESS}" \
  "LPE_CT_SERVER_NAME=${LPE_CT_SERVER_NAME}" \
  "LPE_CT_CORE_DELIVERY_BASE_URL=${LPE_CT_CORE_DELIVERY_BASE_URL}" \
  "LPE_CT_PUBLIC_TLS_CERT_PATH=${LPE_CT_PUBLIC_TLS_CERT_PATH}" \
  "LPE_CT_PUBLIC_TLS_KEY_PATH=${LPE_CT_PUBLIC_TLS_KEY_PATH}" \
  "LPE_CT_WEB_ROOT=${WEB_ROOT}"

ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
rm -f "${NGINX_ENABLED_DIR}/default"
nginx -t

systemctl daemon-reload
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${VENDOR_DIR}"
systemctl restart "${SERVICE_NAME}"
systemctl restart nginx

echo "LPE-CT updated from ${REPO_URL} (${BRANCH})."
