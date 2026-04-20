#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/dducret/LPE}"
BRANCH="${BRANCH:-main}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe-ct}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_DIR="${BIN_DIR:-$INSTALL_ROOT/bin}"
WEB_ROOT="${WEB_ROOT:-$INSTALL_ROOT/www/management}"
VENDOR_DIR="${VENDOR_DIR:-$INSTALL_ROOT/vendor}"
ENV_FILE="${ENV_FILE:-/etc/lpe-ct/lpe-ct.env}"
SPOOL_DIR="${SPOOL_DIR:-/var/spool/lpe-ct}"
SERVICE_NAME="${SERVICE_NAME:-lpe-ct.service}"
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

if [[ "${LPE_CT_RESET_STATE_ON_UPDATE}" == "true" ]]; then
  systemctl stop "${SERVICE_NAME}" || true
  rm -f "${LPE_CT_STATE_FILE:-/var/lib/lpe-ct/state.json}"
  rm -rf \
    "${SPOOL_DIR}/incoming" \
    "${SPOOL_DIR}/deferred" \
    "${SPOOL_DIR}/quarantine" \
    "${SPOOL_DIR}/held" \
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
install -m 0644 "${SRC_DIR}/LPE-CT/installation/debian-trixie/lpe-ct.service" "/etc/systemd/system/lpe-ct.service"
install -d -o root -g root "${WEB_ROOT}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${VENDOR_DIR}" "${SPOOL_DIR}" "${SPOOL_DIR}/incoming" "${SPOOL_DIR}/deferred" "${SPOOL_DIR}/quarantine" "${SPOOL_DIR}/held" "${SPOOL_DIR}/sent"
cp -a "${SRC_DIR}/LPE-CT/web/." "${WEB_ROOT}/"

sed \
  -e "s/__LPE_CT_BIND_ADDRESS__/${LPE_CT_BIND_ADDRESS//\//\\/}/g" \
  -e "s/__LPE_CT_SERVER_NAME__/${LPE_CT_SERVER_NAME//\//\\/}/g" \
  "${SRC_DIR}/LPE-CT/installation/debian-trixie/lpe-ct.nginx.conf" \
  > "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}"

ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
rm -f "${NGINX_ENABLED_DIR}/default"
nginx -t

systemctl daemon-reload
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${VENDOR_DIR}"
systemctl restart "${SERVICE_NAME}"
systemctl restart nginx

echo "LPE-CT updated from ${REPO_URL} (${BRANCH})."
