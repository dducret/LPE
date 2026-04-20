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
  trap 'rm -rf "${temp_dir}"' RETURN

  curl --proto '=https' --tlsv1.2 -LsSf "${url}" -o "${temp_dir}/${archive}"
  echo "${expected_sha}  ${temp_dir}/${archive}" | sha256sum -c -
  tar -xJf "${temp_dir}/${archive}" -C "${temp_dir}"
  extracted_bin="$(find "${temp_dir}" -type f -name magika | head -n 1)"
  [[ -n "${extracted_bin}" ]] || { echo "magika binary not found after archive extraction." >&2; exit 1; }
  install -m 0755 "${extracted_bin}" "${BIN_DIR}/magika"
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

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is not set in ${ENV_FILE}" >&2
  exit 1
fi

LPE_BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
LPE_SERVER_NAME="${LPE_SERVER_NAME:-_}"
LPE_NGINX_LISTEN_PORT="${LPE_NGINX_LISTEN_PORT:-80}"
LPE_NGINX_CLIENT_MAX_BODY_SIZE="${LPE_NGINX_CLIENT_MAX_BODY_SIZE:-20g}"
LPE_PST_IMPORT_DIR="${LPE_PST_IMPORT_DIR:-${DATA_DIR}/imports}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${LPE_PST_IMPORT_DIR}"

cd "${SRC_DIR}"
systemctl stop "${SERVICE_NAME}" || true
"${SRC_DIR}/installation/debian-trixie/init-schema.sh"
"${CARGO_BIN}" build --release -p lpe-cli
cd "${SRC_DIR}/web/admin"
npm ci
npm run build
cd "${SRC_DIR}/web/client"
npm ci
npm run build

install -m 0755 "${SRC_DIR}/target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
install_magika "${MAGIKA_VERSION}" "${MAGIKA_LINUX_X86_64_SHA256}"
install -d -o root -g root "${ADMIN_WEB_ROOT}" "${CLIENT_WEB_ROOT}"
cp -a "${SRC_DIR}/web/admin/dist/." "${ADMIN_WEB_ROOT}/"
cp -a "${SRC_DIR}/web/client/dist/." "${CLIENT_WEB_ROOT}/"
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
