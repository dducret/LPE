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

ALTER TABLE public.calendar_events
  ADD COLUMN IF NOT EXISTS reminder_set BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS reminder_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reminder_dismissed_at TIMESTAMPTZ;

ALTER TABLE public.tasks
  ADD COLUMN IF NOT EXISTS reminder_set BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS reminder_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reminder_dismissed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reminder_reset BOOLEAN NOT NULL DEFAULT FALSE;

DO $$
DECLARE
  row RECORD;
BEGIN
  FOR row IN
    SELECT r.relname, c.conname
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public'
      AND r.relname IN ('account_sync_state', 'canonical_change_journal')
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) LIKE '%category%'
  LOOP
    EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I', row.relname, row.conname);
  END LOOP;
END $$;

ALTER TABLE public.account_sync_state
  ADD CONSTRAINT account_sync_state_category_check
  CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'notes', 'journal', 'rights'));

ALTER TABLE public.canonical_change_journal
  ADD CONSTRAINT canonical_change_journal_category_check
  CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'notes', 'journal', 'rights'));

DO $$
DECLARE
  row RECORD;
BEGIN
  FOR row IN
    SELECT r.relname, c.conname
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'public'
      AND r.relname IN ('mail_change_log', 'tombstones')
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) LIKE '%object_kind%'
  LOOP
    EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I', row.relname, row.conname);
  END LOOP;
END $$;

ALTER TABLE public.mail_change_log
  ADD CONSTRAINT mail_change_log_object_kind_check CHECK (object_kind IN (
    'message', 'mailbox', 'mailbox_message', 'attachment', 'submission',
    'contact_book', 'contact', 'calendar', 'calendar_event', 'task_list', 'task',
    'note', 'journal_entry',
    'contact_book_grant', 'calendar_grant', 'task_list_grant',
    'mailbox_delegation_grant', 'sender_right'
  )),
  ADD CONSTRAINT mail_change_log_object_shape_check CHECK (
    (
      object_kind = 'message'
      AND account_id IS NOT NULL
      AND mailbox_id IS NULL
      AND collection_id IS NULL
    )
    OR (
      object_kind = 'mailbox'
      AND account_id IS NOT NULL
      AND mailbox_id = object_id
      AND collection_id IS NULL
    )
    OR (
      object_kind = 'mailbox_message'
      AND account_id IS NOT NULL
      AND mailbox_id IS NOT NULL
      AND collection_id IS NULL
      AND summary_json ? 'messageId'
      AND summary_json ? 'threadId'
      AND summary_json ? 'imapUid'
      AND (summary_json ->> 'messageId') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND (summary_json ->> 'threadId') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND (summary_json ->> 'imapUid') ~ '^[0-9]+$'
    )
    OR (
      object_kind = 'attachment'
      AND account_id IS NOT NULL
      AND mailbox_id IS NULL
      AND collection_id IS NULL
      AND summary_json ? 'messageId'
      AND summary_json ? 'attachmentId'
    )
    OR (
      object_kind = 'submission'
      AND account_id IS NOT NULL
      AND mailbox_id IS NULL
      AND collection_id IS NULL
      AND summary_json ? 'messageId'
      AND summary_json ? 'status'
    )
    OR (
      object_kind IN (
        'contact_book', 'contact', 'calendar', 'calendar_event', 'task_list', 'task',
        'note', 'journal_entry',
        'contact_book_grant', 'calendar_grant', 'task_list_grant',
        'mailbox_delegation_grant', 'sender_right'
      )
      AND account_id IS NOT NULL
      AND mailbox_id IS NULL
    )
  );

ALTER TABLE public.tombstones
  ADD CONSTRAINT tombstones_object_kind_check CHECK (object_kind IN (
    'message', 'mailbox', 'mailbox_message',
    'contact_book', 'contact', 'calendar', 'calendar_event', 'task_list', 'task',
    'note', 'journal_entry',
    'contact_book_grant', 'calendar_grant', 'task_list_grant',
    'mailbox_delegation_grant', 'sender_right'
  )),
  ADD CONSTRAINT tombstones_object_shape_check CHECK (
    (
      object_kind = 'message'
      AND object_id = message_id
      AND message_id IS NOT NULL
      AND mailbox_message_id IS NULL
      AND mailbox_id IS NULL
      AND imap_uid IS NULL
    )
    OR (
      object_kind = 'mailbox'
      AND object_id = mailbox_id
      AND account_id IS NOT NULL
      AND mailbox_id IS NOT NULL
      AND message_id IS NULL
      AND mailbox_message_id IS NULL
      AND imap_uid IS NULL
    )
    OR (
      object_kind = 'mailbox_message'
      AND object_id = mailbox_message_id
      AND account_id IS NOT NULL
      AND mailbox_id IS NOT NULL
      AND mailbox_message_id IS NOT NULL
      AND message_id IS NOT NULL
      AND imap_uid IS NOT NULL
    )
    OR (
      object_kind IN (
        'contact_book', 'contact', 'calendar', 'calendar_event', 'task_list', 'task',
        'note', 'journal_entry',
        'contact_book_grant', 'calendar_grant', 'task_list_grant',
        'mailbox_delegation_grant', 'sender_right'
      )
      AND account_id IS NOT NULL
      AND mailbox_id IS NULL
      AND message_id IS NULL
      AND mailbox_message_id IS NULL
      AND imap_uid IS NULL
    )
  );

CREATE TABLE IF NOT EXISTS public.notes (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  owner_account_id UUID NOT NULL,
  title TEXT NOT NULL DEFAULT '',
  body_text TEXT NOT NULL DEFAULT '',
  color TEXT NOT NULL DEFAULT '' CHECK (color IN ('', 'blue', 'green', 'pink', 'white', 'yellow')),
  categories_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  import_source TEXT NOT NULL DEFAULT 'local' CHECK (import_source IN ('local', 'jmap', 'ews', 'mapi', 'activesync', 'import')),
  source_uid TEXT,
  source_etag TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  CHECK (jsonb_typeof(categories_json) = 'array'),
  CHECK (jsonb_typeof(source_payload_json) = 'object'),
  FOREIGN KEY (tenant_id, owner_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS notes_owner_updated_idx
  ON public.notes (tenant_id, owner_account_id, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS public.journal_entries (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  owner_account_id UUID NOT NULL,
  subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
  body_text TEXT NOT NULL DEFAULT '',
  entry_type TEXT NOT NULL DEFAULT '' CHECK (entry_type IN ('', 'document', 'email', 'fax', 'letter', 'meeting', 'note', 'phone-call', 'task')),
  message_class TEXT NOT NULL DEFAULT 'IPM.Activity' CHECK (btrim(message_class) <> ''),
  starts_at TIMESTAMPTZ,
  ends_at TIMESTAMPTZ,
  occurred_at TIMESTAMPTZ,
  companies_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  contacts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  import_source TEXT NOT NULL DEFAULT 'local' CHECK (import_source IN ('local', 'jmap', 'ews', 'mapi', 'activesync', 'import')),
  source_uid TEXT,
  source_etag TEXT,
  source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  CHECK (ends_at IS NULL OR starts_at IS NULL OR ends_at >= starts_at),
  CHECK (jsonb_typeof(companies_json) = 'array'),
  CHECK (jsonb_typeof(contacts_json) = 'array'),
  CHECK (jsonb_typeof(source_payload_json) = 'object'),
  FOREIGN KEY (tenant_id, owner_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS journal_entries_owner_time_idx
  ON public.journal_entries (tenant_id, owner_account_id, COALESCE(starts_at, occurred_at, updated_at) DESC, id);

CREATE INDEX IF NOT EXISTS calendar_events_owner_reminder_idx
  ON public.calendar_events (tenant_id, owner_account_id, reminder_set, reminder_at)
  WHERE reminder_set;

CREATE INDEX IF NOT EXISTS tasks_owner_reminder_idx
  ON public.tasks (tenant_id, owner_account_id, reminder_set, reminder_at)
  WHERE reminder_set;

COMMIT;
SQL

echo "Notes, Journal, and Reminder schema repaired for the current LPE release."
