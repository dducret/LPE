#!/usr/bin/env bash
set -euo pipefail

TAKERI_REPO_URL="${TAKERI_REPO_URL:-https://github.com/AnimeForLife191/Shuhari-CyberForge.git}"
TAKERI_BRANCH="${TAKERI_BRANCH:-main}"
TAKERI_SYNC_DIR="${TAKERI_SYNC_DIR:?Set TAKERI_SYNC_DIR to the local takeri checkout path}"
TAKERI_BIN_PATH="${TAKERI_BIN_PATH:?Set TAKERI_BIN_PATH to the destination binary path}"
CARGO_BIN="${CARGO_BIN:-cargo}"

if [[ -d "${TAKERI_SYNC_DIR}/.git" ]]; then
  git -C "${TAKERI_SYNC_DIR}" remote set-url origin "${TAKERI_REPO_URL}" || true
  git -C "${TAKERI_SYNC_DIR}" sparse-checkout init --cone
else
  mkdir -p "$(dirname "${TAKERI_SYNC_DIR}")"
  git clone --filter=blob:none --no-checkout --branch "${TAKERI_BRANCH}" "${TAKERI_REPO_URL}" "${TAKERI_SYNC_DIR}"
  git -C "${TAKERI_SYNC_DIR}" sparse-checkout init --cone
fi

git -C "${TAKERI_SYNC_DIR}" sparse-checkout set Cargo.toml cli tools/takeri tools/shugo LICENSE README.md
git -C "${TAKERI_SYNC_DIR}" fetch --depth 1 origin "${TAKERI_BRANCH}"
git -C "${TAKERI_SYNC_DIR}" checkout -B "${TAKERI_BRANCH}" "origin/${TAKERI_BRANCH}"

"${CARGO_BIN}" build --release --manifest-path "${TAKERI_SYNC_DIR}/cli/Cargo.toml"
install -m 0755 "${TAKERI_SYNC_DIR}/target/release/Shuhari-CyberForge-CLI" "${TAKERI_BIN_PATH}"

echo "takeri synchronized from ${TAKERI_REPO_URL} (${TAKERI_BRANCH})."
