#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found: ${ENV_FILE}" >&2
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

psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;

ALTER TABLE public.mapi_object_identities
  DROP CONSTRAINT IF EXISTS mapi_object_identities_source_key_check,
  DROP CONSTRAINT IF EXISTS mapi_object_identities_change_key_check,
  DROP CONSTRAINT IF EXISTS mapi_object_identities_instance_key_check;

UPDATE public.mapi_object_identities
SET source_key = substring(source_key FROM 1 FOR 22)
WHERE octet_length(source_key) = 24
  AND substring(source_key FROM 23 FOR 2) = decode('0000', 'hex');

UPDATE public.mapi_object_identities
SET change_key = substring(change_key FROM 1 FOR 22)
WHERE octet_length(change_key) = 24
  AND substring(change_key FROM 23 FOR 2) = decode('0000', 'hex');

UPDATE public.mapi_object_identities
SET instance_key = substring(instance_key FROM 1 FOR 22)
WHERE octet_length(instance_key) = 24
  AND substring(instance_key FROM 23 FOR 2) = decode('0000', 'hex');

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.mapi_object_identities
    WHERE octet_length(source_key) <> 22
       OR octet_length(change_key) <> 22
       OR octet_length(instance_key) <> 22
  ) THEN
    RAISE EXCEPTION 'mapi_object_identities contains keys that cannot be converted to 22-byte XIDs automatically';
  END IF;
END $$;

ALTER TABLE public.mapi_object_identities
  ADD CONSTRAINT mapi_object_identities_source_key_check CHECK (octet_length(source_key) = 22),
  ADD CONSTRAINT mapi_object_identities_change_key_check CHECK (octet_length(change_key) = 22),
  ADD CONSTRAINT mapi_object_identities_instance_key_check CHECK (octet_length(instance_key) = 22);

COMMIT;
SQL

constraint_count="$(mapi_identity_key_constraint_count "${DATABASE_URL}")"
if [[ "${constraint_count}" != "3" ]]; then
  echo "MAPI identity key repair did not leave the expected 22-byte constraints in place." >&2
  exit 1
fi

echo "MAPI identity key constraints repaired for the current 22-byte schema."
