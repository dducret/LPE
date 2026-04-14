#!/usr/bin/env bash
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$SRC_DIR/crates/lpe-storage/migrations}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Fichier d'environnement introuvable : ${ENV_FILE}. / Environment file not found: ${ENV_FILE}" >&2
  exit 1
fi

if [[ ! -d "${MIGRATIONS_DIR}" ]]; then
  echo "Repertoire des migrations introuvable : ${MIGRATIONS_DIR}. / Migrations directory not found: ${MIGRATIONS_DIR}" >&2
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL n'est pas defini dans ${ENV_FILE}. / DATABASE_URL is not set in ${ENV_FILE}" >&2
  exit 1
fi

shopt -s nullglob
migration_files=("${MIGRATIONS_DIR}"/*.sql)
shopt -u nullglob

if [[ ${#migration_files[@]} -eq 0 ]]; then
  echo "Aucun fichier de migration trouve dans ${MIGRATIONS_DIR}. / No migration files found in ${MIGRATIONS_DIR}" >&2
  exit 1
fi

for migration in "${migration_files[@]}"; do
  echo "Application de la migration / Applying migration: ${migration}"
  psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 -f "${migration}"
done

echo "Migrations appliquees avec succes. / Migrations applied successfully."
