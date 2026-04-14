#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/dducret/LPE}"
BRANCH="${BRANCH:-main}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_DIR="${BIN_DIR:-$INSTALL_ROOT/bin}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

if [[ ! -d "${SRC_DIR}/.git" ]]; then
  echo "Source repository not found in ${SRC_DIR}. Run install-lpe.sh first." >&2
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

cd "${SRC_DIR}"
"${CARGO_BIN}" build --release -p lpe-cli

install -m 0755 "target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
install -m 0644 "${SRC_DIR}/installation/debian-trixie/lpe.service" "/etc/systemd/system/lpe.service"

systemctl daemon-reload
systemctl restart "${SERVICE_NAME}"

echo "LPE updated from ${REPO_URL} (${BRANCH})."
