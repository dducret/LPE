#!/usr/bin/env bash
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
SCHEMA_FILE="${SCHEMA_FILE:-$SRC_DIR/crates/lpe-storage/sql/schema.sql}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found: ${ENV_FILE}" >&2
  exit 1
fi

if [[ ! -f "${SCHEMA_FILE}" ]]; then
  echo "Schema file not found: ${SCHEMA_FILE}" >&2
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=installation/debian-trixie/lib/install-common.sh
source "${SCRIPT_DIR}/lib/install-common.sh"

if ! ensure_database_url; then
  echo "DATABASE_URL is not set in ${ENV_FILE} and could not be derived from LPE_DB_HOST/LPE_DB_PORT/LPE_DB_NAME/LPE_DB_USER/LPE_DB_PASSWORD" >&2
  exit 1
fi

psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS public CASCADE;"
psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "CREATE SCHEMA public;"
psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -f "${SCHEMA_FILE}"

echo "LPE 0.1.3 schema initialized successfully."
