#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME:-lpe}"
DB_USER="${DB_USER:-lpe}"
DB_PASSWORD="${DB_PASSWORD:-change-me}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgresql}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Ce script doit etre execute en root. / This script must be run as root." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

if ! command -v psql >/dev/null 2>&1; then
  apt-get update
  apt-get install -y --no-install-recommends postgresql postgresql-client
fi

systemctl enable "${POSTGRES_SERVICE}" >/dev/null 2>&1 || true
systemctl start "${POSTGRES_SERVICE}"

if ! id -u postgres >/dev/null 2>&1; then
  echo "Utilisateur postgres introuvable. / postgres user not found." >&2
  exit 1
fi

run_as_postgres() {
  if command -v runuser >/dev/null 2>&1; then
    runuser -u postgres -- "$@"
    return
  fi

  su -s /bin/sh postgres -c "$(printf '%q ' "$@")"
}

run_as_postgres psql <<SQL
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${DB_USER}') THEN
      CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}';
   END IF;
END
\$\$;
SQL

run_as_postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" | grep -q 1 || \
  run_as_postgres createdb --owner="${DB_USER}" "${DB_NAME}"

echo "Initialisation PostgreSQL terminee. / PostgreSQL bootstrap complete."
echo "Chaine de connexion / Connection string: postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
