#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/dducret/LPE}"
BRANCH="${BRANCH:-main}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe-ct}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_DIR="${BIN_DIR:-$INSTALL_ROOT/bin}"
WEB_ROOT="${WEB_ROOT:-$INSTALL_ROOT/www/management}"
ENV_DIR="${ENV_DIR:-/etc/lpe-ct}"
STATE_DIR="${STATE_DIR:-/var/lib/lpe-ct}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
SERVICE_USER="${SERVICE_USER:-lpe-ct}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe-ct}"
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-lpe-ct.conf}"

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
  nginx \
  pkg-config \
  rustup

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${INSTALL_ROOT}" --create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${INSTALL_ROOT}" "${SRC_DIR}" "${BIN_DIR}" "${STATE_DIR}"
install -d -o root -g root "${WEB_ROOT}" "${ENV_DIR}"

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
  git -C "${SRC_DIR}" pull --ff-only origin "${BRANCH}"
fi

"${RUSTUP_BIN}" default stable
export PATH="/root/.cargo/bin:${PATH}"

CARGO_BIN="$(command -v cargo || true)"
if [[ -z "${CARGO_BIN}" ]]; then
  echo "cargo executable not found after rustup toolchain initialization." >&2
  exit 1
fi

cd "${SRC_DIR}"
"${CARGO_BIN}" build --release --manifest-path "${SRC_DIR}/LPE-CT/Cargo.toml"

install -m 0755 "${SRC_DIR}/LPE-CT/target/release/lpe-ct" "${BIN_DIR}/lpe-ct"
install -m 0644 "${SRC_DIR}/LPE-CT/installation/debian-trixie/lpe-ct.service" "${SYSTEMD_DIR}/lpe-ct.service"

if [[ ! -f "${ENV_DIR}/lpe-ct.env" ]]; then
  install -m 0640 "${SRC_DIR}/LPE-CT/installation/debian-trixie/lpe-ct.env.example" "${ENV_DIR}/lpe-ct.env"
fi

set -a
source "${ENV_DIR}/lpe-ct.env"
set +a

LPE_CT_BIND_ADDRESS="${LPE_CT_BIND_ADDRESS:-127.0.0.1:8380}"
LPE_CT_SERVER_NAME="${LPE_CT_SERVER_NAME:-_}"

cp -a "${SRC_DIR}/LPE-CT/web/." "${WEB_ROOT}/"

sed \
  -e "s/__LPE_CT_BIND_ADDRESS__/${LPE_CT_BIND_ADDRESS//\//\\/}/g" \
  -e "s/__LPE_CT_SERVER_NAME__/${LPE_CT_SERVER_NAME//\//\\/}/g" \
  "${SRC_DIR}/LPE-CT/installation/debian-trixie/lpe-ct.nginx.conf" \
  > "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}"

ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
rm -f "${NGINX_ENABLED_DIR}/default"
nginx -t

chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${STATE_DIR}"
systemctl daemon-reload
systemctl enable lpe-ct.service
systemctl enable nginx
systemctl restart lpe-ct.service
systemctl restart nginx

echo "LPE-CT installed in ${INSTALL_ROOT}."
echo "Management console served by nginx on port 80."
