#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=installation/debian-trixie/lib/install-common.sh
source "${SCRIPT_DIR}/lib/install-common.sh"

REPO_URL="${REPO_URL:-https://github.com/dducret/LPE}"
BRANCH="${BRANCH:-main}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/lpe}"
SRC_DIR="${SRC_DIR:-$INSTALL_ROOT/src}"
BIN_DIR="${BIN_DIR:-$INSTALL_ROOT/bin}"
WEB_ROOT="${WEB_ROOT:-$INSTALL_ROOT/www}"
ADMIN_WEB_ROOT="${ADMIN_WEB_ROOT:-$WEB_ROOT/admin}"
CLIENT_WEB_ROOT="${CLIENT_WEB_ROOT:-$WEB_ROOT/client}"
SERVICE_NAME="${SERVICE_NAME:-lpe.service}"
ENV_FILE="${ENV_FILE:-/etc/lpe/lpe.env}"
INSTALL_ENV_FILE="${INSTALL_ENV_FILE:-/etc/lpe/install.env}"
DATA_DIR="${DATA_DIR:-/var/lib/lpe}"
SERVICE_USER="${SERVICE_USER:-lpe}"
SERVICE_GROUP="${SERVICE_GROUP:-lpe}"
NGINX_AVAILABLE_DIR="${NGINX_AVAILABLE_DIR:-/etc/nginx/sites-available}"
NGINX_ENABLED_DIR="${NGINX_ENABLED_DIR:-/etc/nginx/sites-enabled}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-lpe.conf}"
MAGIKA_VERSION="${MAGIKA_VERSION:-1.0.2}"
MAGIKA_LINUX_X86_64_SHA256="${MAGIKA_LINUX_X86_64_SHA256:-4ce475c965cd20e724b5fc53e8a303a479b9d8649beef8721d05e9b3988fbab4}"

load_env_file_if_present "${INSTALL_ENV_FILE}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

if [[ ! -d "${SRC_DIR}/.git" ]]; then
  echo "Source repository not found in ${SRC_DIR}. Run install-lpe.sh first." >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Environment file not found in ${ENV_FILE}. Run install-lpe.sh first." >&2
  exit 1
fi

install_magika() {
  local version="$1"
  local expected_sha="$2"
  local archive="magika-x86_64-unknown-linux-gnu.tar.xz"
  local url="https://github.com/google/magika/releases/download/cli/v${version}/${archive}"
  local temp_dir="/tmp/magika"
  local extracted_bin

  rm -rf "${temp_dir}"
  mkdir -p "${temp_dir}"
  trap "rm -rf '${temp_dir}'" RETURN

  curl --proto '=https' --tlsv1.2 -LsSf "${url}" -o "${temp_dir}/${archive}"
  echo "${expected_sha}  ${temp_dir}/${archive}" | sha256sum -c -
  tar -xJf "${temp_dir}/${archive}" -C "${temp_dir}"
  extracted_bin="$(find "${temp_dir}" -type f -name magika | head -n 1)"
  [[ -n "${extracted_bin}" ]] || { echo "magika binary not found after archive extraction." >&2; exit 1; }
  install -m 0755 "${extracted_bin}" "${BIN_DIR}/magika"
  trap - RETURN
  rm -rf "${temp_dir}"
}

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

ENV_CHECK_SCRIPT="${SRC_DIR}/installation/debian-trixie/check-lpe-env.sh"
ENV_EXAMPLE_FILE="${SRC_DIR}/installation/debian-trixie/lpe.env.example"
if [[ -x "${ENV_CHECK_SCRIPT}" || -f "${ENV_CHECK_SCRIPT}" ]]; then
  if [[ "${LPE_ENV_CHECK_STRICT:-false}" == "true" ]]; then
    bash "${ENV_CHECK_SCRIPT}" --env-file "${ENV_FILE}" --example-file "${ENV_EXAMPLE_FILE}" --strict
  else
    bash "${ENV_CHECK_SCRIPT}" --env-file "${ENV_FILE}" --example-file "${ENV_EXAMPLE_FILE}" || true
  fi
fi

"${RUSTUP_BIN}" default stable
export PATH="/root/.cargo/bin:${PATH}"

CARGO_BIN="$(command -v cargo || true)"
if [[ -z "${CARGO_BIN}" ]]; then
  echo "cargo executable not found after rustup toolchain initialization." >&2
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

if ! ensure_database_url; then
  echo "DATABASE_URL is not set in ${ENV_FILE} and could not be derived from LPE_DB_HOST/LPE_DB_PORT/LPE_DB_NAME/LPE_DB_USER/LPE_DB_PASSWORD" >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql executable not found. Install PostgreSQL client tools before updating LPE." >&2
  exit 1
fi

MAPI_IDENTITY_CONSTRAINT_COUNT="$(mapi_identity_key_constraint_count "${DATABASE_URL}")" || {
  echo "Unable to inspect MAPI identity key constraints. Run init-schema.sh for a fresh deployment or repair the database before updating." >&2
  exit 1
}
if [[ "${MAPI_IDENTITY_CONSTRAINT_COUNT}" != "3" ]]; then
  echo "MAPI identity key constraints do not match the current 22-byte schema." >&2
  echo "Run ${SRC_DIR}/installation/debian-trixie/repair-mapi-identity-keys.sh before update-lpe.sh, or intentionally reset the schema with init-schema.sh." >&2
  exit 1
fi

echo "Applying bounded LPE schema compatibility patches..."
psql "${DATABASE_URL}" -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;

DROP TABLE IF EXISTS public.mapi_folder_properties;

CREATE TABLE IF NOT EXISTS public.search_folders (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  account_id UUID NOT NULL,
  role TEXT NOT NULL DEFAULT 'custom'
    CHECK (role IN (
      'reminders', 'todo_search', 'contacts_search',
      'tracked_mail_processing', 'custom'
    )),
  display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
  definition_kind TEXT NOT NULL DEFAULT 'exchange_builtin'
    CHECK (definition_kind IN ('exchange_builtin', 'user_saved')),
  result_object_kind TEXT NOT NULL
    CHECK (result_object_kind IN ('message', 'contact', 'task', 'mixed')),
  scope_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  restriction_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  excluded_folder_roles TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  is_builtin BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS search_folders_builtin_role_idx
  ON public.search_folders (tenant_id, account_id, role)
  WHERE is_builtin;

CREATE INDEX IF NOT EXISTS search_folders_account_idx
  ON public.search_folders (tenant_id, account_id, display_name);

CREATE TABLE IF NOT EXISTS public.conversation_actions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  account_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  subject TEXT NOT NULL DEFAULT '',
  categories_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(categories_json) = 'array'),
  move_folder_entry_id BYTEA,
  move_store_entry_id BYTEA,
  move_target_mailbox_id UUID,
  max_delivery_time TIMESTAMPTZ,
  last_applied_time TIMESTAMPTZ,
  version INTEGER NOT NULL DEFAULT 3984588,
  processed INTEGER NOT NULL DEFAULT 0 CHECK (processed >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, account_id, conversation_id),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT conversation_actions_move_target_mailbox_id_fkey
  FOREIGN KEY (tenant_id, account_id, move_target_mailbox_id)
    REFERENCES public.mailboxes (tenant_id, account_id, id)
    ON DELETE SET NULL (move_target_mailbox_id)
);

ALTER TABLE public.conversation_actions
  ADD COLUMN IF NOT EXISTS move_target_mailbox_id UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public'
      AND r.relname = 'conversation_actions'
      AND c.contype = 'f'
      AND pg_get_constraintdef(c.oid) LIKE '%move_target_mailbox_id%'
      AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES mailboxes%'
  ) THEN
    ALTER TABLE public.conversation_actions
      ADD CONSTRAINT conversation_actions_move_target_mailbox_id_fkey
      FOREIGN KEY (tenant_id, account_id, move_target_mailbox_id)
      REFERENCES public.mailboxes (tenant_id, account_id, id)
      ON DELETE SET NULL (move_target_mailbox_id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS conversation_actions_account_idx
  ON public.conversation_actions (tenant_id, account_id, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS public.mapi_named_properties (
  tenant_id UUID NOT NULL,
  account_id UUID NOT NULL,
  property_id INTEGER NOT NULL CHECK (property_id BETWEEN 32769 AND 65534),
  property_guid BYTEA NOT NULL CHECK (octet_length(property_guid) = 16),
  property_kind TEXT NOT NULL CHECK (property_kind IN ('lid', 'name')),
  property_lid INTEGER CHECK (property_lid IS NULL OR property_lid >= 0),
  property_name TEXT CHECK (property_name IS NULL OR btrim(property_name) <> ''),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, account_id, property_id),
  CHECK (
    (property_kind = 'lid' AND property_lid IS NOT NULL AND property_name IS NULL)
    OR (property_kind = 'name' AND property_lid IS NULL AND property_name IS NOT NULL)
  ),
  FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS mapi_named_properties_lid_idx
  ON public.mapi_named_properties (tenant_id, account_id, property_guid, property_lid)
  WHERE property_kind = 'lid';

CREATE UNIQUE INDEX IF NOT EXISTS mapi_named_properties_name_idx
  ON public.mapi_named_properties (tenant_id, account_id, property_guid, property_name)
  WHERE property_kind = 'name';

CREATE TABLE IF NOT EXISTS public.mapi_custom_property_values (
  tenant_id UUID NOT NULL,
  account_id UUID NOT NULL,
  object_kind TEXT NOT NULL CHECK (object_kind IN ('message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'attachment')),
  canonical_id UUID NOT NULL,
  property_tag BIGINT NOT NULL CHECK (property_tag >= 0 AND property_tag <= 4294967295),
  property_type INTEGER NOT NULL CHECK (property_type >= 0 AND property_type <= 65535),
  property_value BYTEA NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, account_id, object_kind, canonical_id, property_tag, property_type),
  FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mapi_custom_property_values_object_idx
  ON public.mapi_custom_property_values (tenant_id, account_id, object_kind, canonical_id);

ALTER TABLE public.mailbox_messages
  ADD COLUMN IF NOT EXISTS followup_flag_status TEXT NOT NULL DEFAULT 'none'
    CHECK (followup_flag_status IN ('none', 'flagged', 'complete')),
  ADD COLUMN IF NOT EXISTS followup_icon INTEGER NOT NULL DEFAULT 0 CHECK (followup_icon >= 0),
  ADD COLUMN IF NOT EXISTS todo_item_flags INTEGER NOT NULL DEFAULT 0 CHECK (todo_item_flags >= 0),
  ADD COLUMN IF NOT EXISTS followup_request TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS followup_start_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS followup_due_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS followup_completed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reminder_set BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS reminder_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reminder_dismissed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS swapped_todo_store_id UUID,
  ADD COLUMN IF NOT EXISTS swapped_todo_data BYTEA;

ALTER TABLE public.mailboxes
  DROP CONSTRAINT IF EXISTS mailboxes_role_check,
  ADD CONSTRAINT mailboxes_role_check CHECK (role IN (
    'inbox', 'sent', 'drafts', 'trash', 'archive', 'junk',
    'outbox', 'conversation_history', 'rss_feeds',
    'sync_issues', 'conflicts', 'local_failures', 'server_failures',
    'custom'
  ));

ALTER TABLE public.contact_books
  DROP CONSTRAINT IF EXISTS contact_books_role_check,
  ADD CONSTRAINT contact_books_role_check CHECK (role IN (
    'contacts', 'suggested_contacts', 'quick_contacts', 'im_contact_list',
    'directory', 'custom'
  ));

DO $$
DECLARE
  constraint_name TEXT;
  constraint_def TEXT;
BEGIN
  SELECT c.conname, pg_get_constraintdef(c.oid)
  INTO constraint_name, constraint_def
  FROM pg_constraint c
  JOIN pg_class r ON r.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = r.relnamespace
  WHERE n.nspname = 'public'
    AND r.relname = 'mapi_object_identities'
    AND c.contype = 'c'
    AND pg_get_constraintdef(c.oid) LIKE '%object_kind%'
  ORDER BY c.conname
  LIMIT 1;

  IF constraint_name IS NOT NULL
     AND (
       constraint_def NOT LIKE '%search_folder_definition%'
       OR constraint_def NOT LIKE '%conversation_action%'
     ) THEN
    EXECUTE format('ALTER TABLE public.mapi_object_identities DROP CONSTRAINT %I', constraint_name);
    ALTER TABLE public.mapi_object_identities
      ADD CONSTRAINT mapi_object_identities_object_kind_check CHECK (object_kind IN (
        'account', 'mailbox', 'message', 'contact', 'calendar_event', 'task',
        'note', 'journal_entry', 'search_folder_definition', 'conversation_action'
      ));
  ELSIF constraint_name IS NULL THEN
    ALTER TABLE public.mapi_object_identities
      ADD CONSTRAINT mapi_object_identities_object_kind_check CHECK (object_kind IN (
        'account', 'mailbox', 'message', 'contact', 'calendar_event', 'task',
        'note', 'journal_entry', 'search_folder_definition', 'conversation_action'
      ));
  END IF;
END $$;

COMMIT;
SQL

NOTES_JOURNAL_SCHEMA_REPAIR_SCRIPT="${SRC_DIR}/installation/debian-trixie/repair-notes-journal-reminders-schema.sh"
if [[ -f "${NOTES_JOURNAL_SCHEMA_REPAIR_SCRIPT}" ]]; then
  ENV_FILE="${ENV_FILE}" bash "${NOTES_JOURNAL_SCHEMA_REPAIR_SCRIPT}"
else
  echo "Notes/Journal/Reminder schema repair script is missing: ${NOTES_JOURNAL_SCHEMA_REPAIR_SCRIPT}" >&2
  exit 1
fi

LPE_BIND_ADDRESS="${LPE_BIND_ADDRESS:-127.0.0.1:8080}"
LPE_IMAP_BIND_ADDRESS="${LPE_IMAP_BIND_ADDRESS:-127.0.0.1:1143}"
validate_host_port "${LPE_IMAP_BIND_ADDRESS}" \
  || { echo "LPE_IMAP_BIND_ADDRESS must be a host:port address in ${ENV_FILE}; got '${LPE_IMAP_BIND_ADDRESS}'" >&2; exit 1; }
LPE_SERVER_NAME="${LPE_SERVER_NAME:-_}"
LPE_NGINX_LISTEN_PORT="${LPE_NGINX_LISTEN_PORT:-80}"
LPE_NGINX_CLIENT_MAX_BODY_SIZE="${LPE_NGINX_CLIENT_MAX_BODY_SIZE:-20g}"
LPE_PST_IMPORT_DIR="${LPE_PST_IMPORT_DIR:-${DATA_DIR}/imports}"
LPE_OUTBOUND_WORKER_INTERVAL_MS="${LPE_OUTBOUND_WORKER_INTERVAL_MS:-1000}"
LPE_OUTBOUND_WORKER_BATCH_SIZE="${LPE_OUTBOUND_WORKER_BATCH_SIZE:-50}"
write_env_value "${ENV_FILE}" "LPE_OUTBOUND_WORKER_INTERVAL_MS" "${LPE_OUTBOUND_WORKER_INTERVAL_MS}"
write_env_value "${ENV_FILE}" "LPE_OUTBOUND_WORKER_BATCH_SIZE" "${LPE_OUTBOUND_WORKER_BATCH_SIZE}"
install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${LPE_PST_IMPORT_DIR}"

cd "${SRC_DIR}"
systemctl stop "${SERVICE_NAME}" || true
"${CARGO_BIN}" build --release -p lpe-cli
cd "${SRC_DIR}/web/admin"
npm ci
npm run build
cd "${SRC_DIR}/web/client"
npm ci
npm run build

install -m 0755 "${SRC_DIR}/target/release/lpe-cli" "${BIN_DIR}/lpe-cli"
install_magika "${MAGIKA_VERSION}" "${MAGIKA_LINUX_X86_64_SHA256}"
install -d -o root -g root "${ADMIN_WEB_ROOT}" "${CLIENT_WEB_ROOT}"
cp -a "${SRC_DIR}/web/admin/dist/." "${ADMIN_WEB_ROOT}/"
cp -a "${SRC_DIR}/web/client/dist/." "${CLIENT_WEB_ROOT}/"
render_template \
  "${SRC_DIR}/installation/debian-trixie/lpe.service" \
  "/etc/systemd/system/lpe.service" \
  "LPE_SERVICE_USER=${SERVICE_USER}" \
  "LPE_SERVICE_GROUP=${SERVICE_GROUP}" \
  "LPE_SRC_DIR=${SRC_DIR}" \
  "LPE_ENV_FILE=${ENV_FILE}" \
  "LPE_BIN_DIR=${BIN_DIR}" \
  "LPE_INSTALL_ROOT=${INSTALL_ROOT}" \
  "LPE_DATA_DIR=${DATA_DIR}"
render_template \
  "${SRC_DIR}/installation/debian-trixie/lpe.nginx.conf" \
  "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" \
  "LPE_NGINX_LISTEN_PORT=${LPE_NGINX_LISTEN_PORT}" \
  "LPE_BIND_ADDRESS=${LPE_BIND_ADDRESS}" \
  "LPE_SERVER_NAME=${LPE_SERVER_NAME}" \
  "LPE_NGINX_CLIENT_MAX_BODY_SIZE=${LPE_NGINX_CLIENT_MAX_BODY_SIZE}" \
  "LPE_ADMIN_WEB_ROOT=${ADMIN_WEB_ROOT}"

ln -sfn "${NGINX_AVAILABLE_DIR}/${NGINX_SITE_NAME}" "${NGINX_ENABLED_DIR}/${NGINX_SITE_NAME}"
rm -f "${NGINX_ENABLED_DIR}/default"
nginx -t

systemctl daemon-reload
systemctl restart "${SERVICE_NAME}"
systemctl restart nginx

echo "LPE updated from ${REPO_URL} (${BRANCH})."
