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
MAGIKA_VERSION="${MAGIKA_VERSION:-1.0.2}"
MAGIKA_LINUX_X86_64_SHA256="${MAGIKA_LINUX_X86_64_SHA256:-4ce475c965cd20e724b5fc53e8a303a479b9d8649beef8721d05e9b3988fbab4}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

install_magika() {
  local version="$1"
  local expected_sha="$2"
  local archive="magika-x86_64-unknown-linux-gnu.tar.xz"
  local url="https://github.com/google/magika/releases/download/cli/v${version}/${archive}"
  local temp_dir
  temp_dir="$(mktemp -d)"
  trap 'rm -rf "${temp_dir}"' RETURN

  curl --proto '=https' --tlsv1.2 -LsSf "${url}" -o "${temp_dir}/${archive}"
  echo "${expected_sha}  ${temp_dir}/${archive}" | sha256sum -c -
  tar -xJf "${temp_dir}/${archive}" -C "${temp_dir}"
  install -m 0755 "${temp_dir}/magika" "${BIN_DIR}/magika"
}

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
install_magika "${MAGIKA_VERSION}" "${MAGIKA_LINUX_X86_64_SHA256}"
install -m 0644 "${SRC_DIR}/installation/debian-trixie/lpe.service" "${SYSTEMD_DIR}/lpe.service"

if [[ ! -f "${ENV_DIR}/lpe.env" ]]; then
  install -m 0640 "${SRC_DIR}/installation/debian-trixie/lpe.env.example" "${ENV_DIR}/lpe.env"
fi

set -a
source "${ENV_DIR}/lpe.env"
set +a

LPE_BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
LPE_SERVER_NAME="${LPE_SERVER_NAME:-_}"
LPE_NGINX_CLIENT_MAX_BODY_SIZE="${LPE_NGINX_CLIENT_MAX_BODY_SIZE:-20g}"
LPE_PST_IMPORT_DIR="${LPE_PST_IMPORT_DIR:-${DATA_DIR}/imports}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${LPE_PST_IMPORT_DIR}"

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
  -e "s/__LPE_NGINX_CLIENT_MAX_BODY_SIZE__/${LPE_NGINX_CLIENT_MAX_BODY_SIZE//\//\\/}/g" \
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
echo "Review ${ENV_DIR}/lpe.env and run installation/debian-trixie/init-schema.sh to create a fresh 0.1.3 database."
