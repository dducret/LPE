#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME:-lpe}"
DB_USER="${DB_USER:-lpe}"
DB_PASSWORD="${DB_PASSWORD:-change-me}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Ce script doit etre execute en root. / This script must be run as root." >&2
  exit 1
fi

sudo -u postgres psql <<SQL
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${DB_USER}') THEN
      CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}';
   END IF;
END
\$\$;
SQL

sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" | grep -q 1 || \
  sudo -u postgres createdb --owner="${DB_USER}" "${DB_NAME}"

echo "Initialisation PostgreSQL terminee. / PostgreSQL bootstrap complete."
echo "Chaine de connexion / Connection string: postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

