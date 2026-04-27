#!/usr/bin/env bash
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
SCHEMA_FILE="${SCHEMA_FILE:-$SRC_DIR/crates/lpe-storage/sql/schema.sql}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$SRC_DIR/crates/lpe-storage/sql/migrations}"

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

expected_schema_version="$(
  awk -F"'" '/schema_version TEXT NOT NULL CHECK/ { print $2; exit }' "${SCHEMA_FILE}"
)"

if [[ -z "${expected_schema_version}" ]]; then
  echo "Unable to read expected schema version from ${SCHEMA_FILE}" >&2
  exit 1
fi

metadata_table="$(
  psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -Atc "SELECT to_regclass('public.schema_metadata')" 2>/dev/null || true
)"

if [[ -z "${metadata_table}" ]]; then
  echo "Database schema is not initialized. For a fresh install run init-schema.sh; update-lpe.sh will not create a destructive schema reset." >&2
  exit 1
fi

current_schema_version="$(
  psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -Atc "SELECT schema_version FROM schema_metadata WHERE singleton = TRUE"
)"

if [[ -z "${current_schema_version}" ]]; then
  echo "schema_metadata exists but does not contain a schema version." >&2
  exit 1
fi

psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 <<SQL
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO schema_migrations (version)
VALUES ('${current_schema_version}')
ON CONFLICT (version) DO NOTHING;
SQL

if [[ -d "${MIGRATIONS_DIR}" ]]; then
  while IFS= read -r migration_file; do
    migration_name="$(basename "${migration_file}" .sql)"
    already_applied="$(
      psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -Atc "SELECT 1 FROM schema_migrations WHERE version = '${migration_name}'"
    )"
    if [[ "${already_applied}" == "1" ]]; then
      continue
    fi

    echo "Applying schema migration ${migration_name}."
    psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -f "${migration_file}"
    psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "INSERT INTO schema_migrations (version) VALUES ('${migration_name}') ON CONFLICT (version) DO NOTHING;"
  done < <(find "${MIGRATIONS_DIR}" -maxdepth 1 -type f -name '*.sql' | sort)
fi

current_schema_version="$(
  psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -Atc "SELECT schema_version FROM schema_metadata WHERE singleton = TRUE"
)"

if [[ "${current_schema_version}" != "${expected_schema_version}" ]]; then
  echo "Database schema version is ${current_schema_version}, but this checkout expects ${expected_schema_version}. Add and apply a non-destructive migration before updating this instance." >&2
  exit 1
fi

psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "INSERT INTO schema_migrations (version) VALUES ('${current_schema_version}') ON CONFLICT (version) DO NOTHING;"

echo "LPE schema ${current_schema_version} is up to date."
