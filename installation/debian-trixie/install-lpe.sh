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
ENV_DIR="${ENV_DIR:-/etc/lpe}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
DATA_DIR="${DATA_DIR:-/var/lib/lpe}"
SERVICE_USER="${SERVICE_USER:-lpe}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe}"
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-lpe.conf}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

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
  rustup

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${INSTALL_ROOT}" --create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${INSTALL_ROOT}" "${SRC_DIR}" "${BIN_DIR}"
install -d -o root -g root "${ADMIN_WEB_ROOT}" "${CLIENT_WEB_ROOT}"
install -d -o root -g root "${ENV_DIR}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${DATA_DIR}"

git config --global --add safe.directory "${SRC_DIR}" || true

RUSTUP_BIN="$(command -v rustup || true)"
if [[ -z "${RUSTUP_BIN}" ]]; then
  echo "rustup executable not found after package installation." >&2
  exit 1
fi

if [[ ! -d "${SRC_DIR}/.git" ]]; then
  git clone --branch "${BRANCH}" "${REPO_URL}" "${SRC_DIR}"
else
  git -C "${SRC_DIR}" fetch --all --tags
  git -C "${SRC_DIR}" checkout "${BRANCH}"
  git -C "${SRC_DIR}" pull --ff-only
fi

"${RUSTUP_BIN}" default stable
export PATH="/root/.cargo/bin:${PATH}"

CARGO_BIN="$(command -v cargo || true)"
if [[ -z "${CARGO_BIN}" ]]; then
  echo "cargo executable not found after rustup toolchain initialization." >&2
  exit 1
fi

cd "${SRC_DIR}"
"${CARGO_BIN}" build --release -p lpe-cli

if [[ ! -x "target/release/lpe-cli" ]]; then
  echo "lpe-cli binary not found after build." >&2
  exit 1
fi

install -m 0755 "target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
install -m 0644 "${SRC_DIR}/installation/debian-trixie/lpe.service" "${SYSTEMD_DIR}/lpe.service"

if [[ ! -f "${ENV_DIR}/lpe.env" ]]; then
  install -m 0640 "${SRC_DIR}/installation/debian-trixie/lpe.env.example" "${ENV_DIR}/lpe.env"
fi

set -a
source "${ENV_DIR}/lpe.env"
set +a

LPE_BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
LPE_SERVER_NAME="${LPE_SERVER_NAME:-_}"

cd "${SRC_DIR}/web/admin"
npm ci
npm run build

cd "${SRC_DIR}/web/client"
npm ci
npm run build

cp -a "${SRC_DIR}/web/admin/dist/." "${ADMIN_WEB_ROOT}/"
cp -a "${SRC_DIR}/web/client/dist/." "${CLIENT_WEB_ROOT}/"

sed \
  -e "s/__LPE_BIND_ADDRESS__/${LPE_BIND_ADDRESS//\//\\/}/g" \
  -e "s/__LPE_SERVER_NAME__/${LPE_SERVER_NAME//\//\\/}/g" \
  "${SRC_DIR}/installation/debian-trixie/lpe.nginx.conf" \
  > "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}"

ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
rm -f "${NGINX_ENABLED_DIR}/default"
nginx -t

systemctl daemon-reload
systemctl enable lpe.service
systemctl enable nginx
systemctl restart lpe.service
systemctl restart nginx

echo "LPE installed in ${INSTALL_ROOT}."
echo "Service lpe.service has been started."
echo "nginx now serves the admin console on / and the web client on /mail/."
echo "Review ${ENV_DIR}/lpe.env and run the migrations script if the database schema is not initialized yet."
