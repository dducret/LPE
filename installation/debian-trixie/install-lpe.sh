#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/dducret/LPE}"
BRANCH="${BRANCH:-main}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_DIR="${BIN_DIR:-$INSTALL_ROOT/bin}"
ENV_DIR="${ENV_DIR:-/etc/lpe}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
DATA_DIR="${DATA_DIR:-/var/lib/lpe}"
SERVICE_USER="${SERVICE_USER:-lpe}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe}"

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
  pkg-config \
  postgresql-client \
  rustup

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${INSTALL_ROOT}" --create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${INSTALL_ROOT}" "${SRC_DIR}" "${BIN_DIR}"
install -d -o root -g root "${ENV_DIR}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${DATA_DIR}"

git config --global --add safe.directory "${SRC_DIR}" || true

if [[ ! -d "${SRC_DIR}/.git" ]]; then
  git clone --branch "${BRANCH}" "${REPO_URL}" "${SRC_DIR}"
else
  git -C "${SRC_DIR}" fetch --all --tags
  git -C "${SRC_DIR}" checkout "${BRANCH}"
  git -C "${SRC_DIR}" pull --ff-only
fi

if [[ ! -x /root/.cargo/bin/rustup ]]; then
  echo "rustup executable not found after package installation." >&2
  exit 1
fi

/root/.cargo/bin/rustup default stable

cd "${SRC_DIR}"
/root/.cargo/bin/cargo build --release -p lpe-cli

if [[ ! -x "target/release/lpe-cli" ]]; then
  echo "lpe-cli binary not found after build." >&2
  exit 1
fi

install -m 0755 "target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
install -m 0644 "${SRC_DIR}/installation/debian-trixie/lpe.service" "${SYSTEMD_DIR}/lpe.service"

if [[ ! -f "${ENV_DIR}/lpe.env" ]]; then
  install -m 0640 "${SRC_DIR}/installation/debian-trixie/lpe.env.example" "${ENV_DIR}/lpe.env"
fi

systemctl daemon-reload
systemctl enable lpe.service

echo "LPE installed in ${INSTALL_ROOT}."
echo "Review ${ENV_DIR}/lpe.env, then run 'systemctl start lpe.service'."
