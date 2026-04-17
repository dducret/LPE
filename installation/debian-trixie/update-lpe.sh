#!/usr/bin/env bash
set -euo pipefail

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
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-lpe.conf}"

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

LPE_BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
LPE_SERVER_NAME="${LPE_SERVER_NAME:-_}"
LPE_NGINX_CLIENT_MAX_BODY_SIZE="${LPE_NGINX_CLIENT_MAX_BODY_SIZE:-20g}"
LPE_PST_IMPORT_DIR="${LPE_PST_IMPORT_DIR:-/var/lib/lpe/imports}"
install -d -o lpe -g lpe "${LPE_PST_IMPORT_DIR}"

cd "${SRC_DIR}"
"${CARGO_BIN}" build --release -p lpe-cli
cd "${SRC_DIR}/web/admin"
npm ci
npm run build
cd "${SRC_DIR}/web/client"
npm ci
npm run build

install -m 0755 "${SRC_DIR}/target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
install -d -o root -g root "${ADMIN_WEB_ROOT}" "${CLIENT_WEB_ROOT}"
cp -a "${SRC_DIR}/web/admin/dist/." "${ADMIN_WEB_ROOT}/"
cp -a "${SRC_DIR}/web/client/dist/." "${CLIENT_WEB_ROOT}/"
install -m 0644 "${SRC_DIR}/installation/debian-trixie/lpe.service" "/etc/systemd/system/lpe.service"
sed \
  -e "s/__LPE_BIND_ADDRESS__/${LPE_BIND_ADDRESS//\//\\/}/g" \
  -e "s/__LPE_SERVER_NAME__/${LPE_SERVER_NAME//\//\\/}/g" \
  -e "s/__LPE_NGINX_CLIENT_MAX_BODY_SIZE__/${LPE_NGINX_CLIENT_MAX_BODY_SIZE//\//\\/}/g" \
  "${SRC_DIR}/installation/debian-trixie/lpe.nginx.conf" \
  > "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}"

ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
rm -f "${NGINX_ENABLED_DIR}/default"
nginx -t

systemctl daemon-reload
systemctl restart "${SERVICE_NAME}"
systemctl restart nginx

echo "LPE updated from ${REPO_URL} (${BRANCH})."
