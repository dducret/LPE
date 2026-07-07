SET client_min_messages = warning;

CREATE OR REPLACE FUNCTION pg_temp.lpe_try_parse_message_date_header(value TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN value::TIMESTAMPTZ;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END $$;

WITH first_date_header AS (
    SELECT DISTINCT ON (tenant_id, message_id)
           tenant_id,
           message_id,
           pg_temp.lpe_try_parse_message_date_header(header_value) AS sent_at
    FROM public.message_headers
    WHERE lower(header_name) = 'date'
    ORDER BY tenant_id, message_id, ordinal
),
backfillable_messages AS (
    SELECT messages.tenant_id,
           messages.id,
           first_date_header.sent_at
    FROM public.messages messages
    JOIN first_date_header
      ON first_date_header.tenant_id = messages.tenant_id
     AND first_date_header.message_id = messages.id
    WHERE messages.sent_at IS NULL
      AND first_date_header.sent_at IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM public.mailbox_messages mailbox_messages
          JOIN public.mailboxes mailboxes
            ON mailboxes.tenant_id = mailbox_messages.tenant_id
           AND mailboxes.account_id = mailbox_messages.account_id
           AND mailboxes.id = mailbox_messages.mailbox_id
          WHERE mailbox_messages.tenant_id = messages.tenant_id
            AND mailbox_messages.message_id = messages.id
            AND (mailbox_messages.is_draft OR mailboxes.role = 'drafts')
      )
)
UPDATE public.messages messages
SET sent_at = backfillable_messages.sent_at
FROM backfillable_messages
WHERE messages.tenant_id = backfillable_messages.tenant_id
  AND messages.id = backfillable_messages.id;

WITH backfillable_messages AS (
    SELECT messages.tenant_id,
           messages.id,
           messages.received_at
    FROM public.messages messages
    WHERE messages.sent_at IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM public.mailbox_messages mailbox_messages
          JOIN public.mailboxes mailboxes
            ON mailboxes.tenant_id = mailbox_messages.tenant_id
           AND mailboxes.account_id = mailbox_messages.account_id
           AND mailboxes.id = mailbox_messages.mailbox_id
          WHERE mailbox_messages.tenant_id = messages.tenant_id
            AND mailbox_messages.message_id = messages.id
            AND (mailbox_messages.is_draft OR mailboxes.role = 'drafts')
      )
)
UPDATE public.messages messages
SET sent_at = backfillable_messages.received_at
FROM backfillable_messages
WHERE messages.tenant_id = backfillable_messages.tenant_id
  AND messages.id = backfillable_messages.id;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.mapi_custom_property_values'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%object_kind%'
    LOOP
        EXECUTE format('ALTER TABLE public.mapi_custom_property_values DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END $$;

ALTER TABLE public.mapi_custom_property_values
    ADD CONSTRAINT mapi_custom_property_values_object_kind_check
    CHECK (object_kind IN ('message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'attachment', 'public_folder_item'));

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.mapi_object_identities'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%object_kind%'
    LOOP
        EXECUTE format('ALTER TABLE public.mapi_object_identities DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END $$;

ALTER TABLE public.mapi_object_identities
    ADD CONSTRAINT mapi_object_identities_object_kind_check
    CHECK (object_kind IN ('account', 'mailbox', 'message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'search_folder_definition', 'conversation_action', 'navigation_shortcut', 'associated_config', 'delegate_freebusy_message'));

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.mapi_profile_settings'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%ipm_subtree_ost_id%'
    LOOP
        EXECUTE format('ALTER TABLE public.mapi_profile_settings DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END $$;

ALTER TABLE public.mapi_profile_settings
    ADD CONSTRAINT mapi_profile_settings_ipm_subtree_ost_id_check
    CHECK (ipm_subtree_ost_id IS NULL OR (octet_length(ipm_subtree_ost_id) > 0 AND octet_length(ipm_subtree_ost_id) <= 2048));

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.search_folders
        WHERE NOT is_builtin
          AND definition_kind = 'user_saved'
        GROUP BY tenant_id, account_id, lower(btrim(display_name)), result_object_kind
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'Duplicate user-saved Search Folder names exist; remove duplicates before applying search_folders_user_saved_name_idx';
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS search_folders_user_saved_name_idx
    ON public.search_folders (tenant_id, account_id, lower(btrim(display_name)), result_object_kind)
    WHERE NOT is_builtin AND definition_kind = 'user_saved';

ALTER TABLE public.mapi_navigation_shortcuts
  ALTER COLUMN target_folder_id DROP NOT NULL,
  ADD COLUMN IF NOT EXISTS save_stamp BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS group_header_id UUID,
  ADD COLUMN IF NOT EXISTS group_name TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.mapi_navigation_shortcuts'::regclass
          AND conname = 'mapi_navigation_shortcuts_save_stamp_check'
    ) THEN
        ALTER TABLE public.mapi_navigation_shortcuts
            ADD CONSTRAINT mapi_navigation_shortcuts_save_stamp_check
            CHECK (save_stamp >= 0 AND save_stamp <= 4294967295) NOT VALID;
    END IF;
END $$;

ALTER TABLE public.mapi_navigation_shortcuts
  VALIDATE CONSTRAINT mapi_navigation_shortcuts_save_stamp_check;

UPDATE public.mapi_navigation_shortcuts
SET group_name = ''
WHERE group_name IS NULL;

ALTER TABLE public.mapi_navigation_shortcuts
  ALTER COLUMN group_name SET DEFAULT '',
  ALTER COLUMN group_name SET NOT NULL;

CREATE TABLE IF NOT EXISTS public.mapi_folder_profile_property_values (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    folder_id BIGINT NOT NULL CHECK (folder_id > 0),
    property_tag BIGINT NOT NULL CHECK (property_tag >= 0 AND property_tag <= 4294967295),
    property_type INTEGER NOT NULL CHECK (property_type >= 0 AND property_type <= 65535),
    property_value BYTEA NOT NULL CHECK (octet_length(property_value) > 0 AND octet_length(property_value) <= 4096),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, folder_id, property_tag, property_type),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mapi_folder_profile_property_values_account_idx
    ON public.mapi_folder_profile_property_values (tenant_id, account_id, folder_id);

CREATE TABLE IF NOT EXISTS public.mapi_associated_config_messages (
    tenant_id UUID NOT NULL,
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    folder_id BIGINT NOT NULL CHECK (folder_id > 0),
    message_class TEXT NOT NULL CHECK (btrim(message_class) <> ''),
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    properties_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mapi_associated_config_messages_account_folder_idx
    ON public.mapi_associated_config_messages (tenant_id, account_id, folder_id, subject, id);

WITH ranked_associated_configs AS (
    SELECT tenant_id,
           account_id,
           id,
           folder_id,
           message_class,
           subject,
           row_number() OVER (
               PARTITION BY tenant_id, account_id, folder_id, message_class, subject
               ORDER BY updated_at DESC, id
           ) AS row_number
    FROM public.mapi_associated_config_messages
)
DELETE FROM public.mapi_associated_config_messages config
USING ranked_associated_configs ranked
WHERE config.tenant_id = ranked.tenant_id
  AND config.id = ranked.id
  AND ranked.row_number > 1;

DROP INDEX IF EXISTS public.mapi_associated_config_messages_logical_idx;

CREATE UNIQUE INDEX IF NOT EXISTS mapi_associated_config_messages_logical_idx
    ON public.mapi_associated_config_messages (tenant_id, account_id, folder_id, message_class, subject);

DELETE FROM public.mapi_associated_config_messages
WHERE message_class IN (
    'IPM.Microsoft.MigrationStatus',
    'IPM.Microsoft.PendingChange.MigrateCategoriesList',
    'IPM.Microsoft.PendingChange.MigrateFlags',
    'IPM.Microsoft.PendingChange.MigrateLabels'
);

DELETE FROM public.mapi_associated_config_messages
WHERE folder_id = 327681
  AND message_class = 'IPM.RuleOrganizer'
  AND COALESCE(properties_json #>> '{0x68020102,value}', '') = '';

DELETE FROM public.mapi_associated_config_messages
WHERE folder_id = 327681
  AND message_class LIKE 'IPM.Configuration.%'
  AND message_class <> 'IPM.Configuration.UMOLK.UserOptions'
  AND NOT (
      properties_json ? '0x7c070102'
      OR properties_json ? '0x7c080102'
      OR properties_json ? '0x0e0b0102'
      OR properties_json ? '0x7c090102'
  );

UPDATE public.mapi_object_identities
SET source_key = decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
        || decode(lpad(to_hex(mapi_global_counter), 12, '0'), 'hex'),
    change_key = decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
        || decode(lpad(to_hex(mapi_global_counter), 12, '0'), 'hex'),
    instance_key = decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
        || decode(lpad(to_hex(mapi_global_counter), 12, '0'), 'hex'),
    updated_at = NOW()
WHERE deleted_at IS NULL
  AND (
      source_key IS DISTINCT FROM decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
          || decode(lpad(to_hex(mapi_global_counter), 12, '0'), 'hex')
      OR change_key IS DISTINCT FROM decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
          || decode(lpad(to_hex(mapi_global_counter), 12, '0'), 'hex')
      OR instance_key IS DISTINCT FROM decode('741f6fd38e1a654f9d422dfb451c8f10', 'hex')
          || decode(lpad(to_hex(mapi_global_counter), 12, '0'), 'hex')
  );

CREATE UNIQUE INDEX IF NOT EXISTS mapi_object_identities_active_source_key_uidx
    ON public.mapi_object_identities (tenant_id, account_id, source_key)
    WHERE deleted_at IS NULL;

DO $$
BEGIN
    IF to_regclass('public.mapi_named_properties') IS NOT NULL THEN
        WITH duplicate_rows AS (
            SELECT ctid
            FROM (
                SELECT ctid,
                       row_number() OVER (
                           PARTITION BY tenant_id, account_id, property_id
                           ORDER BY created_at, property_guid, property_kind, property_lid NULLS LAST, property_name NULLS LAST
                       ) AS row_number
                FROM public.mapi_named_properties
            ) ranked
            WHERE row_number > 1
        )
        DELETE FROM public.mapi_named_properties properties
        USING duplicate_rows
        WHERE properties.ctid = duplicate_rows.ctid;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = 'public.mapi_named_properties'::regclass
              AND contype = 'p'
              AND conname = 'mapi_named_properties_pkey'
        ) THEN
            ALTER TABLE public.mapi_named_properties
                ADD CONSTRAINT mapi_named_properties_pkey
                PRIMARY KEY (tenant_id, account_id, property_id);
        END IF;
    END IF;
END $$;

ALTER TABLE public.accounts
  ADD COLUMN IF NOT EXISTS recoverable_items_retention_days INTEGER NOT NULL DEFAULT 14,
  ADD COLUMN IF NOT EXISTS litigation_hold_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS litigation_hold_started_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.accounts'::regclass
          AND conname = 'accounts_recoverable_items_retention_days_check'
    ) THEN
        ALTER TABLE public.accounts
            ADD CONSTRAINT accounts_recoverable_items_retention_days_check CHECK (recoverable_items_retention_days >= 0) NOT VALID;
    END IF;
END $$;

ALTER TABLE public.accounts
  VALIDATE CONSTRAINT accounts_recoverable_items_retention_days_check;

ALTER TABLE public.mailboxes
  ADD COLUMN IF NOT EXISTS recoverable_items_retention_days INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.mailboxes'::regclass
          AND conname = 'mailboxes_recoverable_items_retention_days_check'
    ) THEN
        ALTER TABLE public.mailboxes
            ADD CONSTRAINT mailboxes_recoverable_items_retention_days_check CHECK (recoverable_items_retention_days IS NULL OR recoverable_items_retention_days >= 0) NOT VALID;
    END IF;
END $$;

ALTER TABLE public.mailboxes
  VALIDATE CONSTRAINT mailboxes_recoverable_items_retention_days_check;

CREATE TABLE IF NOT EXISTS public.recoverable_items (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    message_id UUID NOT NULL,
    source_mailbox_message_id UUID NOT NULL,
    source_mailbox_id UUID NOT NULL,
    source_imap_uid BIGINT NOT NULL CHECK (source_imap_uid > 0),
    source_thread_id UUID,
    recoverable_folder TEXT NOT NULL CHECK (recoverable_folder IN ('deletions', 'versions', 'purges')),
    delete_kind TEXT NOT NULL CHECK (delete_kind IN (
        'hard_delete',
        'expunge',
        'retention_expire',
        'copy_on_write_version',
        'admin_purge'
    )),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'restored', 'purged')),
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retained_until TIMESTAMPTZ,
    legal_hold BOOLEAN NOT NULL DEFAULT FALSE,
    restored_at TIMESTAMPTZ,
    restored_mailbox_message_id UUID,
    purged_at TIMESTAMPTZ,
    created_by_protocol TEXT NOT NULL CHECK (created_by_protocol IN (
        'jmap',
        'imap',
        'ews',
        'mapi',
        'api',
        'retention_worker'
    )),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, source_mailbox_message_id),
    CHECK (retained_until IS NULL OR retained_until >= deleted_at),
    CHECK ((status = 'restored' AND restored_at IS NOT NULL) OR status <> 'restored'),
    CHECK ((status = 'purged' AND purged_at IS NOT NULL) OR status <> 'purged'),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES public.messages (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, source_mailbox_id)
        REFERENCES public.mailboxes (tenant_id, account_id, id)
        ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, source_mailbox_message_id, message_id)
        REFERENCES public.mailbox_messages (tenant_id, account_id, id, message_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, restored_mailbox_message_id)
        REFERENCES public.mailbox_messages (tenant_id, account_id, id)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS recoverable_items_active_folder_idx
    ON public.recoverable_items (tenant_id, account_id, recoverable_folder, deleted_at DESC)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS recoverable_items_cleanup_idx
    ON public.recoverable_items (tenant_id, retained_until, deleted_at)
    WHERE status = 'active' AND legal_hold = FALSE;

CREATE INDEX IF NOT EXISTS recoverable_items_message_idx
    ON public.recoverable_items (tenant_id, message_id);

CREATE TABLE IF NOT EXISTS public.public_folder_trees (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    canonical_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active', 'disabled', 'deleted')),
    admin_owner_account_id UUID NOT NULL,
    root_folder_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, canonical_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, admin_owner_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS public_folder_trees_tenant_state_idx
    ON public.public_folder_trees (tenant_id, lifecycle_state, display_name, id);

CREATE TABLE IF NOT EXISTS public.public_folders (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    tree_id UUID NOT NULL,
    parent_folder_id UUID,
    canonical_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    folder_class TEXT NOT NULL DEFAULT 'IPF.Note' CHECK (btrim(folder_class) <> ''),
    path TEXT NOT NULL CHECK (btrim(path) <> ''),
    sort_order INTEGER NOT NULL DEFAULT 0,
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active', 'hidden', 'deleted')),
    change_counter BIGINT NOT NULL DEFAULT 1 CHECK (change_counter > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, canonical_id),
    UNIQUE (tenant_id, tree_id, parent_folder_id, display_name),
    FOREIGN KEY (tenant_id, tree_id) REFERENCES public.public_folder_trees (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, parent_folder_id) REFERENCES public.public_folders (tenant_id, id) ON DELETE CASCADE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.public_folder_trees'::regclass
          AND conname = 'public_folder_trees_root_folder_fk'
    ) THEN
        ALTER TABLE public.public_folder_trees
            ADD CONSTRAINT public_folder_trees_root_folder_fk
            FOREIGN KEY (tenant_id, root_folder_id) REFERENCES public.public_folders (tenant_id, id) ON DELETE RESTRICT;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS public_folders_tree_parent_idx
    ON public.public_folders (tenant_id, tree_id, parent_folder_id, lifecycle_state, sort_order, display_name, id);

CREATE TABLE IF NOT EXISTS public.public_folder_items (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    public_folder_id UUID NOT NULL,
    message_id UUID,
    item_kind TEXT NOT NULL DEFAULT 'post' CHECK (item_kind IN ('post', 'message', 'contact', 'calendar', 'task', 'note', 'journal')),
    message_class TEXT NOT NULL DEFAULT 'IPM.Post' CHECK (btrim(message_class) <> ''),
    subject TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    body_html_sanitized TEXT,
    source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active', 'deleted')),
    change_counter BIGINT NOT NULL DEFAULT 1 CHECK (change_counter > 0),
    created_by_account_id UUID NOT NULL,
    updated_by_account_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (jsonb_typeof(source_payload_json) = 'object'),
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public.public_folders (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES public.messages (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, updated_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS public_folder_items_folder_idx
    ON public.public_folder_items (tenant_id, public_folder_id, lifecycle_state, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS public.public_folder_permissions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    public_folder_id UUID NOT NULL,
    principal_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, public_folder_id, principal_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write),
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public.public_folders (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, principal_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS public_folder_permissions_principal_idx
    ON public.public_folder_permissions (tenant_id, principal_account_id, public_folder_id);

CREATE TABLE IF NOT EXISTS public.public_folder_replicas (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    public_folder_id UUID NOT NULL,
    server_name TEXT NOT NULL CHECK (btrim(server_name) <> ''),
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active', 'inactive', 'deleted')),
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, public_folder_id, server_name),
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public.public_folders (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS public_folder_replicas_folder_idx
    ON public.public_folder_replicas (tenant_id, public_folder_id, lifecycle_state, sort_order, server_name, id);

CREATE TABLE IF NOT EXISTS public.public_folder_per_user_state (
    tenant_id UUID NOT NULL,
    public_folder_id UUID NOT NULL,
    item_id UUID NOT NULL,
    account_id UUID NOT NULL,
    is_read BOOLEAN NOT NULL DEFAULT FALSE,
    last_seen_change BIGINT NOT NULL DEFAULT 0 CHECK (last_seen_change >= 0),
    private_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, public_folder_id, item_id, account_id),
    CHECK (jsonb_typeof(private_json) = 'object'),
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public.public_folders (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, item_id) REFERENCES public.public_folder_items (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS public_folder_per_user_state_account_idx
    ON public.public_folder_per_user_state (tenant_id, account_id, public_folder_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS public.retention_policy_tags (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    tag_type TEXT NOT NULL CHECK (tag_type IN ('all', 'inbox', 'sent', 'deleted_items', 'junk_email', 'custom_folder', 'personal')),
    action TEXT NOT NULL CHECK (action IN ('delete_and_allow_recovery', 'permanently_delete', 'move_to_archive', 'none')),
    retention_days INTEGER CHECK (retention_days IS NULL OR retention_days >= 0),
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    is_visible BOOLEAN NOT NULL DEFAULT TRUE,
    description TEXT NOT NULL DEFAULT '',
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active', 'disabled', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK ((action = 'none' AND retention_days IS NULL) OR (action <> 'none' AND retention_days IS NOT NULL)),
    FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS retention_policy_tags_tenant_idx
    ON public.retention_policy_tags (tenant_id, lifecycle_state, is_visible, display_name, id);

CREATE UNIQUE INDEX IF NOT EXISTS retention_policy_tags_default_type_idx
    ON public.retention_policy_tags (tenant_id, tag_type)
    WHERE is_default = TRUE AND lifecycle_state = 'active';

CREATE TABLE IF NOT EXISTS public.account_retention_policy_assignments (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    default_tag_id UUID,
    policy_name TEXT NOT NULL DEFAULT '',
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by_account_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, default_tag_id) REFERENCES public.retention_policy_tags (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, assigned_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

ALTER TABLE public.mailboxes
  ADD COLUMN IF NOT EXISTS retention_policy_tag_id UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.mailboxes'::regclass
      AND conname = 'mailboxes_retention_policy_tag_fk'
  ) THEN
    ALTER TABLE public.mailboxes
      ADD CONSTRAINT mailboxes_retention_policy_tag_fk
      FOREIGN KEY (tenant_id, retention_policy_tag_id)
      REFERENCES public.retention_policy_tags (tenant_id, id)
      ON DELETE RESTRICT
      NOT VALID;
  END IF;
END $$;

ALTER TABLE public.mailboxes
  VALIDATE CONSTRAINT mailboxes_retention_policy_tag_fk;

CREATE INDEX IF NOT EXISTS mailboxes_retention_policy_tag_idx
    ON public.mailboxes (tenant_id, account_id, retention_policy_tag_id)
    WHERE retention_policy_tag_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.mailbox_item_transfer_jobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('import', 'export')),
    source_protocol TEXT NOT NULL DEFAULT 'ews' CHECK (source_protocol IN ('ews', 'mapi', 'api')),
    status TEXT NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested', 'running', 'completed', 'failed', 'cancelled')),
    requested_by_account_id UUID NOT NULL,
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    total_items INTEGER NOT NULL DEFAULT 0 CHECK (total_items >= 0),
    processed_items INTEGER NOT NULL DEFAULT 0 CHECK (processed_items >= 0),
    failed_items INTEGER NOT NULL DEFAULT 0 CHECK (failed_items >= 0),
    error_message TEXT,
    idempotency_key TEXT CHECK (idempotency_key IS NULL OR btrim(idempotency_key) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (jsonb_typeof(request_json) = 'object'),
    CHECK (processed_items <= total_items),
    CHECK (failed_items <= total_items),
    CHECK (
        (status IN ('requested', 'running') AND completed_at IS NULL)
        OR (status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, requested_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS mailbox_item_transfer_jobs_account_idx
    ON public.mailbox_item_transfer_jobs (tenant_id, account_id, direction, created_at DESC, id);

CREATE INDEX IF NOT EXISTS mailbox_item_transfer_jobs_status_idx
    ON public.mailbox_item_transfer_jobs (tenant_id, status, updated_at, id);

CREATE UNIQUE INDEX IF NOT EXISTS mailbox_item_transfer_jobs_idempotency_idx
    ON public.mailbox_item_transfer_jobs (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.mailbox_item_transfer_entries (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    job_id UUID NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    item_kind TEXT NOT NULL CHECK (item_kind IN ('message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'public_folder_item')),
    canonical_id UUID,
    mailbox_message_id UUID,
    source_item_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processed', 'failed', 'skipped')),
    error_message TEXT,
    source_payload_sha256 TEXT CHECK (source_payload_sha256 IS NULL OR source_payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, job_id, ordinal),
    CHECK ((status IN ('pending', 'skipped') AND processed_at IS NULL) OR (status IN ('processed', 'failed') AND processed_at IS NOT NULL)),
    FOREIGN KEY (tenant_id, job_id) REFERENCES public.mailbox_item_transfer_jobs (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mailbox_item_transfer_entries_job_idx
    ON public.mailbox_item_transfer_entries (tenant_id, job_id, status, ordinal);

CREATE TABLE IF NOT EXISTS public.compliance_cases (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
    created_by_account_id UUID NOT NULL,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK ((status = 'closed' AND closed_at IS NOT NULL) OR (status = 'open' AND closed_at IS NULL)),
    FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS compliance_cases_tenant_status_idx
    ON public.compliance_cases (tenant_id, status, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS public.compliance_holds (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    case_id UUID,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    query_text TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'released')),
    created_by_account_id UUID NOT NULL,
    released_by_account_id UUID,
    released_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK ((status = 'released' AND released_at IS NOT NULL) OR (status = 'active' AND released_at IS NULL)),
    FOREIGN KEY (tenant_id, case_id) REFERENCES public.compliance_cases (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, released_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS compliance_holds_tenant_status_idx
    ON public.compliance_holds (tenant_id, status, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS public.compliance_hold_mailboxes (
    tenant_id UUID NOT NULL,
    hold_id UUID NOT NULL,
    account_id UUID NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by_account_id UUID NOT NULL,
    released_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, hold_id, account_id),
    CHECK (released_at IS NULL OR released_at >= applied_at),
    FOREIGN KEY (tenant_id, hold_id) REFERENCES public.compliance_holds (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, applied_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS compliance_hold_mailboxes_account_idx
    ON public.compliance_hold_mailboxes (tenant_id, account_id, released_at, hold_id);

CREATE TABLE IF NOT EXISTS public.discovery_searches (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    case_id UUID,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    query_text TEXT NOT NULL CHECK (btrim(query_text) <> ''),
    scope_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by_account_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (jsonb_typeof(scope_json) = 'object'),
    FOREIGN KEY (tenant_id, case_id) REFERENCES public.compliance_cases (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS discovery_searches_case_idx
    ON public.discovery_searches (tenant_id, case_id, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS public.discovery_search_jobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    search_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    result_count INTEGER NOT NULL DEFAULT 0 CHECK (result_count >= 0),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (
        (status IN ('queued', 'running') AND completed_at IS NULL)
        OR (status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, search_id) REFERENCES public.discovery_searches (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS discovery_search_jobs_status_idx
    ON public.discovery_search_jobs (tenant_id, status, updated_at, id);

CREATE TABLE IF NOT EXISTS public.discovery_result_items (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    job_id UUID NOT NULL,
    account_id UUID NOT NULL,
    mailbox_message_id UUID NOT NULL,
    message_id UUID NOT NULL,
    rank INTEGER NOT NULL DEFAULT 0 CHECK (rank >= 0),
    preview TEXT NOT NULL DEFAULT '',
    matched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, job_id, account_id, mailbox_message_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES public.discovery_search_jobs (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, mailbox_message_id, message_id)
        REFERENCES public.mailbox_messages (tenant_id, account_id, id, message_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS discovery_result_items_job_idx
    ON public.discovery_result_items (tenant_id, job_id, rank, id);

CREATE INDEX IF NOT EXISTS discovery_result_items_message_idx
    ON public.discovery_result_items (tenant_id, message_id);

CREATE TABLE IF NOT EXISTS public.non_indexable_item_reports (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    message_id UUID,
    attachment_id UUID,
    report_kind TEXT NOT NULL CHECK (report_kind IN ('message', 'attachment')),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    CHECK (
        (report_kind = 'message' AND message_id IS NOT NULL AND attachment_id IS NULL)
        OR (report_kind = 'attachment' AND attachment_id IS NOT NULL)
    ),
    CHECK (jsonb_typeof(detail_json) = 'object'),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES public.messages (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, attachment_id) REFERENCES public.attachments (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS non_indexable_item_reports_account_idx
    ON public.non_indexable_item_reports (tenant_id, account_id, detected_at DESC, id);

CREATE INDEX IF NOT EXISTS non_indexable_item_reports_open_idx
    ON public.non_indexable_item_reports (tenant_id, report_kind, detected_at DESC, id)
    WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS public.lpe_ct_transport_trace_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    trace_id TEXT NOT NULL CHECK (btrim(trace_id) <> ''),
    submission_queue_id UUID,
    recipient_address TEXT CHECK (recipient_address IS NULL OR btrim(recipient_address) <> ''),
    event_kind TEXT NOT NULL CHECK (event_kind IN ('accepted', 'queued', 'deferred', 'relayed', 'bounced', 'failed', 'quarantined', 'released', 'delivered', 'duplicate', 'rejected')),
    event_source TEXT NOT NULL DEFAULT 'lpe-ct' CHECK (event_source = 'lpe-ct'),
    dsn_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    route_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    technical_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, trace_id, event_kind, recipient_address, occurred_at),
    CHECK (jsonb_typeof(dsn_json) = 'object'),
    CHECK (jsonb_typeof(route_json) = 'object'),
    CHECK (jsonb_typeof(technical_json) = 'object'),
    FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, submission_queue_id) REFERENCES public.submission_queue (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS lpe_ct_transport_trace_events_trace_idx
    ON public.lpe_ct_transport_trace_events (tenant_id, trace_id, occurred_at DESC, id);

CREATE INDEX IF NOT EXISTS lpe_ct_transport_trace_events_submission_idx
    ON public.lpe_ct_transport_trace_events (tenant_id, submission_queue_id, occurred_at DESC, id)
    WHERE submission_queue_id IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgrelid = 'public.lpe_ct_transport_trace_events'::regclass
          AND tgname = 'lpe_ct_transport_trace_events_append_only_update_guard'
    ) THEN
        CREATE TRIGGER lpe_ct_transport_trace_events_append_only_update_guard
            BEFORE UPDATE ON public.lpe_ct_transport_trace_events
            FOR EACH ROW
            EXECUTE FUNCTION prevent_append_only_update();
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.recipient_suggestions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    normalized_email TEXT NOT NULL CHECK (btrim(normalized_email) <> ''),
    display_name TEXT NOT NULL DEFAULT '',
    source_kind TEXT NOT NULL CHECK (source_kind IN ('sent_to', 'sent_cc', 'manual', 'contact')),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    use_count INTEGER NOT NULL DEFAULT 1 CHECK (use_count > 0),
    dismissed_at TIMESTAMPTZ,
    contact_id UUID,
    source_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (jsonb_typeof(source_metadata_json) = 'object'),
    CHECK (last_used_at >= first_seen_at),
    CHECK (dismissed_at IS NULL OR dismissed_at >= first_seen_at),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, contact_id) REFERENCES public.contacts (tenant_id, id) ON DELETE SET NULL (contact_id)
);

DO $$
BEGIN
    IF to_regclass('public.recipient_suggestions') IS NOT NULL THEN
        ALTER TABLE public.recipient_suggestions
            DROP CONSTRAINT IF EXISTS recipient_suggestions_tenant_id_contact_id_fkey;
        ALTER TABLE public.recipient_suggestions
            DROP CONSTRAINT IF EXISTS recipient_suggestions_contact_fk;
        ALTER TABLE public.recipient_suggestions
            ADD CONSTRAINT recipient_suggestions_contact_fk
            FOREIGN KEY (tenant_id, contact_id)
            REFERENCES public.contacts (tenant_id, id)
            ON DELETE SET NULL (contact_id);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS recipient_suggestions_active_email_idx
    ON public.recipient_suggestions (tenant_id, account_id, normalized_email)
    WHERE dismissed_at IS NULL;

CREATE INDEX IF NOT EXISTS recipient_suggestions_rank_idx
    ON public.recipient_suggestions (tenant_id, account_id, dismissed_at, use_count DESC, last_used_at DESC);

CREATE TABLE IF NOT EXISTS public.contact_groups (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    contact_book_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    normalized_display_name TEXT GENERATED ALWAYS AS (lower(display_name)) STORED,
    group_kind TEXT NOT NULL DEFAULT 'contact_group'
        CHECK (group_kind IN ('contact_group', 'im_group')),
    notes TEXT NOT NULL DEFAULT '',
    source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, id),
    UNIQUE (tenant_id, owner_account_id, contact_book_id, normalized_display_name),
    CHECK (jsonb_typeof(source_payload_json) = 'object'),
    FOREIGN KEY (tenant_id, owner_account_id, contact_book_id)
        REFERENCES public.contact_books (tenant_id, owner_account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS contact_groups_owner_idx
    ON public.contact_groups (tenant_id, owner_account_id, contact_book_id, group_kind, display_name);

CREATE TABLE IF NOT EXISTS public.contact_group_members (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    contact_group_id UUID NOT NULL,
    member_kind TEXT NOT NULL CHECK (member_kind IN ('contact', 'account', 'distribution_group', 'tel_uri')),
    contact_id UUID,
    account_id UUID,
    external_address TEXT CHECK (external_address IS NULL OR btrim(external_address) <> ''),
    display_name TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (
        (member_kind = 'contact' AND contact_id IS NOT NULL AND account_id IS NULL AND external_address IS NULL)
        OR (member_kind = 'account' AND contact_id IS NULL AND account_id IS NOT NULL AND external_address IS NULL)
        OR (member_kind IN ('distribution_group', 'tel_uri') AND contact_id IS NULL AND account_id IS NULL AND external_address IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, owner_account_id, contact_group_id)
        REFERENCES public.contact_groups (tenant_id, owner_account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, contact_id)
        REFERENCES public.contacts (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS contact_group_members_contact_idx
    ON public.contact_group_members (tenant_id, owner_account_id, contact_group_id, contact_id)
    WHERE member_kind = 'contact';

CREATE UNIQUE INDEX IF NOT EXISTS contact_group_members_account_idx
    ON public.contact_group_members (tenant_id, owner_account_id, contact_group_id, account_id)
    WHERE member_kind = 'account';

CREATE UNIQUE INDEX IF NOT EXISTS contact_group_members_external_idx
    ON public.contact_group_members (tenant_id, owner_account_id, contact_group_id, member_kind, lower(external_address))
    WHERE member_kind IN ('distribution_group', 'tel_uri');

CREATE TABLE IF NOT EXISTS public.account_client_configurations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    scope_kind TEXT NOT NULL DEFAULT 'account' CHECK (scope_kind IN ('account', 'mailbox', 'public_folder')),
    mailbox_id UUID,
    public_folder_id UUID,
    config_name TEXT NOT NULL CHECK (btrim(config_name) <> ''),
    config_class TEXT NOT NULL DEFAULT 'ews_user_configuration' CHECK (btrim(config_class) <> ''),
    dictionary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    xml_payload TEXT,
    binary_payload BYTEA,
    payload_size_octets INTEGER NOT NULL DEFAULT 0 CHECK (payload_size_octets >= 0),
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (
        (scope_kind = 'account' AND mailbox_id IS NULL AND public_folder_id IS NULL)
        OR (scope_kind = 'mailbox' AND mailbox_id IS NOT NULL AND public_folder_id IS NULL)
        OR (scope_kind = 'public_folder' AND mailbox_id IS NULL AND public_folder_id IS NOT NULL)
    ),
    CHECK (jsonb_typeof(dictionary_json) = 'object'),
    CHECK (payload_size_octets = COALESCE(length(xml_payload), 0) + COALESCE(octet_length(binary_payload), 0)),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, mailbox_id)
        REFERENCES public.mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, public_folder_id)
        REFERENCES public.public_folders (tenant_id, id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS account_client_configurations_account_idx
    ON public.account_client_configurations (tenant_id, account_id, config_class, config_name)
    WHERE scope_kind = 'account';

CREATE UNIQUE INDEX IF NOT EXISTS account_client_configurations_mailbox_idx
    ON public.account_client_configurations (tenant_id, account_id, mailbox_id, config_class, config_name)
    WHERE scope_kind = 'mailbox';

CREATE UNIQUE INDEX IF NOT EXISTS account_client_configurations_public_folder_idx
    ON public.account_client_configurations (tenant_id, account_id, public_folder_id, config_class, config_name)
    WHERE scope_kind = 'public_folder';

CREATE TABLE IF NOT EXISTS public.delegate_preferences (
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    meeting_request_delivery TEXT NOT NULL DEFAULT 'delegate_and_owner'
        CHECK (meeting_request_delivery IN ('delegate_only', 'delegate_and_owner', 'owner_only')),
    receives_meeting_request_copy BOOLEAN NOT NULL DEFAULT TRUE,
    may_view_private_items BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, owner_account_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS delegate_preferences_grantee_idx
    ON public.delegate_preferences (tenant_id, grantee_account_id, owner_account_id);

CREATE TABLE IF NOT EXISTS public.mail_app_catalog (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    app_id TEXT NOT NULL CHECK (btrim(app_id) <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    manifest_xml TEXT NOT NULL CHECK (btrim(manifest_xml) <> ''),
    provider_name TEXT NOT NULL DEFAULT '',
    version TEXT NOT NULL DEFAULT '',
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active', 'disabled', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, app_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mail_app_catalog_tenant_state_idx
    ON public.mail_app_catalog (tenant_id, lifecycle_state, display_name, id);

CREATE TABLE IF NOT EXISTS public.mail_app_tenant_policies (
    tenant_id UUID PRIMARY KEY,
    marketplace_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    marketplace_url TEXT CHECK (marketplace_url IS NULL OR btrim(marketplace_url) <> ''),
    default_install_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.mail_app_installations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    app_catalog_id UUID NOT NULL,
    account_id UUID,
    install_scope TEXT NOT NULL CHECK (install_scope IN ('tenant', 'account')),
    status TEXT NOT NULL DEFAULT 'installed' CHECK (status IN ('installed', 'disabled', 'uninstalled')),
    installed_by_account_id UUID,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (
        (install_scope = 'tenant' AND account_id IS NULL)
        OR (install_scope = 'account' AND account_id IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, app_catalog_id) REFERENCES public.mail_app_catalog (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, installed_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS mail_app_installations_tenant_idx
    ON public.mail_app_installations (tenant_id, app_catalog_id)
    WHERE install_scope = 'tenant' AND status <> 'uninstalled';

CREATE UNIQUE INDEX IF NOT EXISTS mail_app_installations_account_idx
    ON public.mail_app_installations (tenant_id, account_id, app_catalog_id)
    WHERE install_scope = 'account' AND status <> 'uninstalled';

CREATE TABLE IF NOT EXISTS public.mail_app_consents (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    app_catalog_id UUID NOT NULL,
    account_id UUID NOT NULL,
    consent_scope TEXT NOT NULL CHECK (btrim(consent_scope) <> ''),
    granted_by_account_id UUID NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, app_catalog_id, account_id, consent_scope),
    CHECK (revoked_at IS NULL OR revoked_at >= granted_at),
    FOREIGN KEY (tenant_id, app_catalog_id) REFERENCES public.mail_app_catalog (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, granted_by_account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS mail_app_consents_account_idx
    ON public.mail_app_consents (tenant_id, account_id, app_catalog_id, revoked_at);

CREATE TABLE IF NOT EXISTS public.mail_app_token_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    app_catalog_id UUID NOT NULL,
    account_id UUID NOT NULL,
    token_hash TEXT NOT NULL CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    scopes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, token_hash),
    CHECK (jsonb_typeof(scopes_json) = 'array'),
    CHECK (expires_at > issued_at),
    CHECK (revoked_at IS NULL OR revoked_at >= issued_at),
    FOREIGN KEY (tenant_id, app_catalog_id) REFERENCES public.mail_app_catalog (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS mail_app_token_events_account_idx
    ON public.mail_app_token_events (tenant_id, account_id, app_catalog_id, issued_at DESC);

CREATE INDEX IF NOT EXISTS mail_app_token_events_expiry_idx
    ON public.mail_app_token_events (tenant_id, expires_at)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS public.unified_messaging_calls (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    call_id TEXT NOT NULL CHECK (btrim(call_id) <> ''),
    call_kind TEXT NOT NULL CHECK (call_kind IN ('play_on_phone', 'voicemail', 'missed_call')),
    status TEXT NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested', 'ringing', 'connected', 'completed', 'failed', 'cancelled')),
    phone_number TEXT CHECK (phone_number IS NULL OR btrim(phone_number) <> ''),
    message_id UUID,
    technical_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    connected_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, call_id),
    CHECK (jsonb_typeof(technical_json) = 'object'),
    CHECK (
        (status IN ('requested', 'ringing', 'connected') AND completed_at IS NULL)
        OR (status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, account_id) REFERENCES public.accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES public.messages (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS unified_messaging_calls_account_idx
    ON public.unified_messaging_calls (tenant_id, account_id, requested_at DESC, id);

CREATE INDEX IF NOT EXISTS unified_messaging_calls_status_idx
    ON public.unified_messaging_calls (tenant_id, status, updated_at, id);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.account_sync_state'::regclass
          AND conname = 'account_sync_state_category_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%public_folders%'
    ) THEN
        ALTER TABLE public.account_sync_state
            DROP CONSTRAINT account_sync_state_category_check;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.account_sync_state'::regclass
          AND conname = 'account_sync_state_category_check'
    ) THEN
        ALTER TABLE public.account_sync_state
            ADD CONSTRAINT account_sync_state_category_check CHECK (category IN (
                'mail',
                'contacts',
                'calendar',
                'tasks',
                'notes',
                'journal',
                'rights',
                'search',
                'rules',
                'conversation_actions',
                'public_folders'
            ));
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.canonical_change_journal'::regclass
          AND conname = 'canonical_change_journal_category_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%public_folders%'
    ) THEN
        ALTER TABLE public.canonical_change_journal
            DROP CONSTRAINT canonical_change_journal_category_check;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.canonical_change_journal'::regclass
          AND conname = 'canonical_change_journal_category_check'
    ) THEN
        ALTER TABLE public.canonical_change_journal
            ADD CONSTRAINT canonical_change_journal_category_check CHECK (category IN (
                'mail',
                'contacts',
                'calendar',
                'tasks',
                'notes',
                'journal',
                'rights',
                'search',
                'rules',
                'conversation_actions',
                'public_folders'
            ));
    END IF;
END $$;

DO $$
DECLARE
    existing_constraint TEXT;
BEGIN
    FOR existing_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.mail_change_log'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%object_kind%'
          AND pg_get_constraintdef(oid) LIKE '%conversation_action%'
          AND (
              pg_get_constraintdef(oid) NOT LIKE '%public_folder_replica%'
              OR pg_get_constraintdef(oid) NOT LIKE '%associated_config%'
              OR pg_get_constraintdef(oid) NOT LIKE '%navigation_shortcut%'
          )
    LOOP
        EXECUTE format('ALTER TABLE public.mail_change_log DROP CONSTRAINT %I', existing_constraint);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.mail_change_log'::regclass
          AND conname = 'mail_change_log_object_kind_check'
    ) THEN
        ALTER TABLE public.mail_change_log
            ADD CONSTRAINT mail_change_log_object_kind_check CHECK (object_kind IN (
                'message',
                'mailbox',
                'mailbox_message',
                'attachment',
                'submission',
                'contact_book',
                'contact',
                'calendar',
                'calendar_event',
                'task_list',
                'task',
                'note',
                'journal_entry',
                'contact_book_grant',
                'calendar_grant',
                'task_list_grant',
                'mailbox_delegation_grant',
                'sender_right',
                'search_folder_definition',
                'sieve_script',
                'conversation_action',
                'navigation_shortcut',
                'associated_config',
                'recoverable_item',
                'public_folder_tree',
                'public_folder',
                'public_folder_item',
                'public_folder_permission',
                'public_folder_replica',
                'public_folder_per_user_state'
            ));
    END IF;

    FOR existing_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.mail_change_log'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%summary_json%'
          AND pg_get_constraintdef(oid) LIKE '%mailbox_message%'
          AND (
              pg_get_constraintdef(oid) NOT LIKE '%public_folder_replica%'
              OR pg_get_constraintdef(oid) NOT LIKE '%associated_config%'
              OR pg_get_constraintdef(oid) NOT LIKE '%navigation_shortcut%'
              OR (
                  pg_get_constraintdef(oid) LIKE '%sourceMailboxMessageId%'
                  AND pg_get_constraintdef(oid) LIKE '%[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}%'
              )
          )
    LOOP
        EXECUTE format('ALTER TABLE public.mail_change_log DROP CONSTRAINT %I', existing_constraint);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.mail_change_log'::regclass
          AND conname = 'mail_change_log_object_shape_check'
    ) THEN
        ALTER TABLE public.mail_change_log
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
                    AND mailbox_id IS NOT NULL
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
                    object_kind = 'recoverable_item'
                    AND account_id IS NOT NULL
                    AND mailbox_id IS NULL
                    AND collection_id IS NULL
                    AND summary_json ? 'messageId'
                    AND summary_json ? 'sourceMailboxMessageId'
                    AND summary_json ? 'recoverableFolder'
                    AND (summary_json ->> 'messageId') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    AND (summary_json ->> 'sourceMailboxMessageId') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
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
                        'contact_book',
                        'contact',
                        'calendar',
                        'calendar_event',
                        'task_list',
                        'task',
                        'note',
                        'journal_entry',
                        'contact_book_grant',
                        'calendar_grant',
                        'task_list_grant',
                        'mailbox_delegation_grant',
                        'sender_right',
                        'search_folder_definition',
                        'sieve_script',
                        'conversation_action',
                        'navigation_shortcut',
                        'associated_config',
                        'public_folder_tree',
                        'public_folder',
                        'public_folder_item',
                        'public_folder_permission',
                        'public_folder_replica',
                        'public_folder_per_user_state'
                    )
                    AND account_id IS NOT NULL
                    AND mailbox_id IS NULL
                )
            );
    END IF;

    FOR existing_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.tombstones'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%object_kind%'
          AND pg_get_constraintdef(oid) LIKE '%sieve_script%'
          AND pg_get_constraintdef(oid) NOT LIKE '%public_folder_replica%'
    LOOP
        EXECUTE format('ALTER TABLE public.tombstones DROP CONSTRAINT %I', existing_constraint);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.tombstones'::regclass
          AND conname = 'tombstones_object_kind_check'
    ) THEN
        ALTER TABLE public.tombstones
            ADD CONSTRAINT tombstones_object_kind_check CHECK (object_kind IN (
                'message',
                'mailbox',
                'mailbox_message',
                'contact_book',
                'contact',
                'calendar',
                'calendar_event',
                'task_list',
                'task',
                'note',
                'journal_entry',
                'contact_book_grant',
                'calendar_grant',
                'task_list_grant',
                'mailbox_delegation_grant',
                'sender_right',
                'search_folder_definition',
                'sieve_script',
                'recoverable_item',
                'public_folder_tree',
                'public_folder',
                'public_folder_item',
                'public_folder_permission',
                'public_folder_replica',
                'public_folder_per_user_state'
            ));
    END IF;

    FOR existing_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'public.tombstones'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%mailbox_message_id%'
          AND pg_get_constraintdef(oid) LIKE '%mailbox_message%'
          AND pg_get_constraintdef(oid) NOT LIKE '%public_folder_replica%'
    LOOP
        EXECUTE format('ALTER TABLE public.tombstones DROP CONSTRAINT %I', existing_constraint);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.tombstones'::regclass
          AND conname = 'tombstones_object_shape_check'
    ) THEN
        ALTER TABLE public.tombstones
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
                    object_kind = 'recoverable_item'
                    AND account_id IS NOT NULL
                    AND mailbox_id IS NULL
                    AND message_id IS NOT NULL
                    AND mailbox_message_id IS NULL
                    AND imap_uid IS NULL
                )
                OR (
                    object_kind IN (
                        'contact_book',
                        'contact',
                        'calendar',
                        'calendar_event',
                        'task_list',
                        'task',
                        'note',
                        'journal_entry',
                        'contact_book_grant',
                        'calendar_grant',
                        'task_list_grant',
                        'mailbox_delegation_grant',
                        'sender_right',
                        'search_folder_definition',
                        'sieve_script',
                        'public_folder_tree',
                        'public_folder',
                        'public_folder_item',
                        'public_folder_permission',
                        'public_folder_replica',
                        'public_folder_per_user_state'
                    )
                    AND account_id IS NOT NULL
                    AND mailbox_id IS NULL
                    AND message_id IS NULL
                    AND mailbox_message_id IS NULL
                    AND imap_uid IS NULL
                )
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS mail_change_log_recoverable_item_idx
    ON public.mail_change_log (tenant_id, account_id, object_kind, cursor)
    WHERE object_kind = 'recoverable_item';

DO $$
BEGIN
    IF to_regclass('public.mapi_named_properties') IS NOT NULL THEN
        CREATE TEMP TABLE IF NOT EXISTS mapi_named_property_low_dynamic_renumber (
            tenant_id UUID NOT NULL,
            account_id UUID NOT NULL,
            old_property_id INTEGER NOT NULL, new_property_id INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, account_id, old_property_id)
        ) ON COMMIT DROP;
        TRUNCATE mapi_named_property_low_dynamic_renumber;
        WITH low_dynamic AS (
            SELECT tenant_id, account_id, property_id,
                   row_number() OVER (
                       PARTITION BY tenant_id, account_id ORDER BY property_id, property_guid, property_kind, property_lid NULLS LAST, property_name NULLS LAST
                   ) AS row_number
            FROM public.mapi_named_properties
            WHERE property_id >= 32769
              AND property_id < 36864
              AND NOT (
                  property_id BETWEEN 32768 AND 33023
                  OR property_id BETWEEN 33280 AND 33535
                  OR property_id BETWEEN 34048 AND 34303
                  OR property_id BETWEEN 34560 AND 34815
                  OR property_id BETWEEN 35328 AND 35839
                  OR property_id BETWEEN 35072 AND 35078
                  OR property_id IN (33005, 33261, 33643, 33872, 36615)
              )
        ),
        account_bases AS (
            SELECT low_dynamic.tenant_id, low_dynamic.account_id,
                   GREATEST(36864, COALESCE(MAX(existing.property_id), 0)) AS base_property_id
            FROM low_dynamic
            LEFT JOIN public.mapi_named_properties existing
              ON existing.tenant_id = low_dynamic.tenant_id
             AND existing.account_id = low_dynamic.account_id
             AND existing.property_id >= 36864
            GROUP BY low_dynamic.tenant_id, low_dynamic.account_id
        )
        INSERT INTO mapi_named_property_low_dynamic_renumber (
            tenant_id, account_id, old_property_id, new_property_id
        )
        SELECT low_dynamic.tenant_id, low_dynamic.account_id, low_dynamic.property_id,
               account_bases.base_property_id + low_dynamic.row_number
        FROM low_dynamic
        JOIN account_bases
          ON account_bases.tenant_id = low_dynamic.tenant_id
         AND account_bases.account_id = low_dynamic.account_id;
        IF EXISTS (
            SELECT 1 FROM mapi_named_property_low_dynamic_renumber
            WHERE new_property_id > 65534
        ) THEN
            RAISE EXCEPTION 'MAPI named property id space exhausted while renumbering low dynamic ids';
        END IF;
        IF to_regclass('public.mapi_custom_property_values') IS NOT NULL THEN
            UPDATE public.mapi_custom_property_values values
            SET property_tag = (renumber.new_property_id::BIGINT << 16) | (values.property_tag::BIGINT & 65535),
                updated_at = NOW()
            FROM mapi_named_property_low_dynamic_renumber renumber
            WHERE values.tenant_id = renumber.tenant_id
              AND values.account_id = renumber.account_id
              AND ((values.property_tag::BIGINT >> 16)::INTEGER) = renumber.old_property_id;
        END IF;
        IF to_regclass('public.mapi_folder_profile_property_values') IS NOT NULL THEN
            UPDATE public.mapi_folder_profile_property_values values
            SET property_tag = (renumber.new_property_id::BIGINT << 16) | (values.property_tag::BIGINT & 65535),
                updated_at = NOW()
            FROM mapi_named_property_low_dynamic_renumber renumber
            WHERE values.tenant_id = renumber.tenant_id
              AND values.account_id = renumber.account_id
              AND ((values.property_tag::BIGINT >> 16)::INTEGER) = renumber.old_property_id;
        END IF;
        IF to_regclass('public.mapi_associated_config_messages') IS NOT NULL THEN
            IF EXISTS (
                SELECT 1
                FROM public.mapi_associated_config_messages config
                CROSS JOIN LATERAL jsonb_object_keys(config.properties_json) key
                JOIN mapi_named_property_low_dynamic_renumber renumber
                  ON renumber.tenant_id = config.tenant_id AND renumber.account_id = config.account_id
                 AND key ~ '^0x[0-9a-fA-F]{8}$' AND (('x' || substring(key FROM 3 FOR 4))::BIT(16)::INTEGER) = renumber.old_property_id
                WHERE config.properties_json ? ('0x' || lpad(to_hex(renumber.new_property_id), 4, '0') || substring(key FROM 7 FOR 4))
            ) THEN
                RAISE EXCEPTION 'MAPI associated config property key collision while renumbering low dynamic ids';
            END IF;
            WITH expanded AS (
                SELECT config.tenant_id, config.id,
                       jsonb_object_agg(
                           CASE
                               WHEN renumber.new_property_id IS NULL THEN entries.key
                               ELSE '0x' || lpad(to_hex(renumber.new_property_id), 4, '0') || substring(entries.key FROM 7 FOR 4)
                           END, entries.value
                       ) AS properties_json
                FROM public.mapi_associated_config_messages config
                CROSS JOIN LATERAL jsonb_each(config.properties_json) entries(key, value)
                LEFT JOIN mapi_named_property_low_dynamic_renumber renumber
                  ON renumber.tenant_id = config.tenant_id
                 AND renumber.account_id = config.account_id
                 AND entries.key ~ '^0x[0-9a-fA-F]{8}$'
                 AND (('x' || substring(entries.key FROM 3 FOR 4))::BIT(16)::INTEGER) = renumber.old_property_id
                WHERE EXISTS (
                    SELECT 1 FROM jsonb_object_keys(config.properties_json) key
                    JOIN mapi_named_property_low_dynamic_renumber renumber_exists
                     ON renumber_exists.tenant_id = config.tenant_id AND renumber_exists.account_id = config.account_id
                     AND key ~ '^0x[0-9a-fA-F]{8}$'
                     AND (('x' || substring(key FROM 3 FOR 4))::BIT(16)::INTEGER) = renumber_exists.old_property_id
                )
                GROUP BY config.tenant_id, config.id
            )
            UPDATE public.mapi_associated_config_messages config
            SET properties_json = expanded.properties_json, updated_at = NOW()
            FROM expanded
            WHERE config.tenant_id = expanded.tenant_id
              AND config.id = expanded.id;
        END IF;
        UPDATE public.mapi_named_properties properties
        SET property_id = renumber.new_property_id
        FROM mapi_named_property_low_dynamic_renumber renumber
        WHERE properties.tenant_id = renumber.tenant_id
          AND properties.account_id = renumber.account_id
          AND properties.property_id = renumber.old_property_id;
    END IF;
END $$;
