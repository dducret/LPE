#!/usr/bin/env bash
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
SCHEMA_FILE="${SCHEMA_FILE:-$SRC_DIR/crates/lpe-storage/sql/schema.sql}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"

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

if systemctl is-active --quiet "${SERVICE_NAME}"; then
  echo "Service ${SERVICE_NAME} is active; init-schema.sh will not reset its database." >&2
  echo "Stop ${SERVICE_NAME} before running init-schema.sh." >&2
  exit 1
fi

expected_schema_version="$(
  awk -F"'" '/schema_version TEXT NOT NULL CHECK/ { print $2; exit }' "${SCHEMA_FILE}"
)"
if [[ -z "${expected_schema_version}" ]]; then
  echo "Unable to read expected schema version from ${SCHEMA_FILE}" >&2
  exit 1
fi

existing_public_objects="$(
  psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 -Atc "
    SELECT COUNT(*)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f');
  "
)"

existing_non_public_objects="$(
  psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 -Atc "
    SELECT COUNT(*)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname <> 'public'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_'
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f');
  "
)"

if [[ "${existing_non_public_objects}" != "0" ]]; then
  echo "The target database contains objects outside the public schema; init-schema.sh will not delete them." >&2
  echo "Use a new empty database so non-public objects cannot become parallel LPE state." >&2
  exit 1
fi

if [[ "${existing_public_objects}" != "0" && "${LPE_RESET_SCHEMA:-false}" != "true" ]]; then
  echo "The target database is not empty. LPE 0.5.0 requires an empty SQL database." >&2
  echo "Create a new empty database, or set LPE_RESET_SCHEMA=true only for an intentional destructive reset." >&2
  exit 1
fi

psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 --single-transaction \
  -c "DROP SCHEMA IF EXISTS public CASCADE;" \
  -c "CREATE SCHEMA public;" \
  -c "SET search_path TO public;" \
  -f "${SCHEMA_FILE}"

schema_version="$(
  psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 -Atc "SELECT schema_version FROM public.schema_metadata WHERE singleton = TRUE"
)"
mapi_identity_version_column_count="$(
  psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 -Atc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'mapi_object_identities' AND column_name IN ('mapi_change_number', 'predecessor_change_list') AND is_nullable = 'NO' AND data_type = CASE column_name WHEN 'mapi_change_number' THEN 'bigint' WHEN 'predecessor_change_list' THEN 'bytea' END"
)"
calendar_event_lifecycle_column_count="$(
  psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 -Atc "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'calendar_events' AND column_name IN ('lifecycle_state', 'deleted_at') AND is_nullable = CASE column_name WHEN 'lifecycle_state' THEN 'NO' WHEN 'deleted_at' THEN 'YES' END AND data_type = CASE column_name WHEN 'lifecycle_state' THEN 'text' WHEN 'deleted_at' THEN 'timestamp with time zone' END"
)"
mapi_calendar_event_identity_moves_table="$(
  psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 -Atc "SELECT to_regclass('public.mapi_calendar_event_identity_moves')"
)"
mapi_local_replica_range_shape_ok="$(
  mapi_local_replica_range_shape_ok "${DATABASE_URL}"
)"
mapi_outlook_cache_fidelity_shape_ok="$(
  mapi_outlook_cache_fidelity_shape_ok "${DATABASE_URL}"
)"
deleted_calendar_event_constraint_count="$(
  psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 -Atc "SELECT COUNT(DISTINCT table_row.relname) FROM pg_constraint constraint_row JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace WHERE namespace_row.nspname = 'public' AND table_row.relname IN ('mail_change_log', 'mapi_object_identities') AND constraint_row.contype = 'c' AND pg_get_constraintdef(constraint_row.oid) LIKE '%deleted_calendar_event%'"
)"
mapi_identity_constraint_count="$(
  mapi_identity_key_constraint_count "${DATABASE_URL}"
)"
mapi_calendar_event_move_change_key_constraint_count="$(
  mapi_calendar_event_move_change_key_constraint_count "${DATABASE_URL}"
)"
mapi_special_folder_alias_shape_ok="$(
  mapi_special_folder_alias_shape_ok "${DATABASE_URL}"
)"

if [[ "${schema_version}" != "${expected_schema_version}" \
  || "${mapi_identity_version_column_count}" != "2" \
  || "${calendar_event_lifecycle_column_count}" != "2" \
  || "${mapi_calendar_event_identity_moves_table}" != "mapi_calendar_event_identity_moves" \
  || "${mapi_local_replica_range_shape_ok}" != "1" \
  || "${mapi_outlook_cache_fidelity_shape_ok}" != "1" \
  || "${deleted_calendar_event_constraint_count}" != "2" \
  || "${mapi_identity_constraint_count}" != "3" \
  || "${mapi_calendar_event_move_change_key_constraint_count}" != "2" \
  || "${mapi_special_folder_alias_shape_ok}" != "1" ]]; then
  echo "Schema initialization validation failed: version=${schema_version}, MAPI identity version shape count=${mapi_identity_version_column_count}, Calendar lifecycle shape count=${calendar_event_lifecycle_column_count}, Calendar identity-move table=${mapi_calendar_event_identity_moves_table:-missing}, MAPI local replica range table shape=${mapi_local_replica_range_shape_ok}, MAPI WLink/configuration FAI fidelity shape=${mapi_outlook_cache_fidelity_shape_ok}, deleted Calendar object-kind constraint count=${deleted_calendar_event_constraint_count}, MAPI identity key constraint count=${mapi_identity_constraint_count}, Calendar move ChangeKey constraint count=${mapi_calendar_event_move_change_key_constraint_count}, MAPI special-folder alias shape=${mapi_special_folder_alias_shape_ok}." >&2
  echo "Initialize a fresh LPE 0.5.0 database after correcting the canonical schema source." >&2
  exit 1
fi

echo "LPE schema ${schema_version}, including MAPI local replica ranges and WLink/configuration FAI fidelity, initialized successfully."
