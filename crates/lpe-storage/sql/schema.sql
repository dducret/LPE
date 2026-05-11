-- Copyright 2026 LPE Contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE schema_metadata (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton = TRUE),
    schema_version TEXT NOT NULL CHECK (schema_version = '0.3.0-sql-v2'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE tenants (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL CHECK (slug = lower(btrim(slug)) AND slug <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'disabled')),
    default_locale TEXT NOT NULL DEFAULT 'en' CHECK (default_locale IN ('en', 'fr', 'de', 'it', 'es')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (slug)
);

CREATE TABLE domains (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (name = lower(btrim(name)) AND name <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    inbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    outbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    default_quota_mb INTEGER NOT NULL DEFAULT 4096 CHECK (default_quota_mb >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, name),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    primary_domain_id UUID NOT NULL,
    primary_email TEXT NOT NULL CHECK (primary_email = lower(btrim(primary_email)) AND primary_email <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    account_kind TEXT NOT NULL DEFAULT 'person'
        CHECK (account_kind IN ('person', 'shared_mailbox', 'room', 'equipment', 'service')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'disabled')),
    quota_mb INTEGER NOT NULL DEFAULT 4096 CHECK (quota_mb >= 0),
    quota_used_octets BIGINT NOT NULL DEFAULT 0 CHECK (quota_used_octets >= 0),
    gal_visibility TEXT NOT NULL DEFAULT 'tenant' CHECK (gal_visibility IN ('tenant', 'hidden')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, primary_email),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, primary_domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE account_email_addresses (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    email TEXT NOT NULL CHECK (email = lower(btrim(email)) AND email <> ''),
    address_kind TEXT NOT NULL DEFAULT 'primary' CHECK (address_kind IN ('primary', 'alias', 'reply_to')),
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, email),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX account_email_addresses_primary_idx
    ON account_email_addresses (tenant_id, account_id)
    WHERE is_primary = TRUE;

CREATE TABLE aliases (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    source_email TEXT NOT NULL CHECK (source_email = lower(btrim(source_email)) AND source_email <> ''),
    target_account_id UUID,
    target_email TEXT CHECK (target_email IS NULL OR (target_email = lower(btrim(target_email)) AND target_email <> '')),
    alias_kind TEXT NOT NULL CHECK (alias_kind IN ('account', 'external', 'group')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, source_email),
    CHECK (
        (alias_kind = 'account' AND target_account_id IS NOT NULL AND target_email IS NULL)
        OR (alias_kind IN ('external', 'group') AND target_account_id IS NULL AND target_email IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, target_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE account_identities (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    email_address_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    reply_to_email TEXT CHECK (reply_to_email IS NULL OR reply_to_email = lower(btrim(reply_to_email))),
    signature_text TEXT NOT NULL DEFAULT '',
    may_send BOOLEAN NOT NULL DEFAULT TRUE,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, email_address_id) REFERENCES account_email_addresses (tenant_id, id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX account_identities_default_idx
    ON account_identities (tenant_id, account_id)
    WHERE is_default = TRUE;

CREATE TABLE account_credentials (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE account_sessions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    token_hash TEXT NOT NULL CHECK (btrim(token_hash) <> ''),
    auth_method TEXT NOT NULL DEFAULT 'password' CHECK (auth_method IN ('password', 'oidc', 'app_password')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (token_hash),
    CHECK (expires_at > created_at),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX account_sessions_account_idx
    ON account_sessions (tenant_id, account_id, expires_at DESC);

CREATE TABLE admin_credentials (
    tenant_id UUID NOT NULL,
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email)) AND admin_email <> ''),
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, admin_email),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE admin_sessions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email)) AND admin_email <> ''),
    token_hash TEXT NOT NULL CHECK (btrim(token_hash) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (token_hash),
    CHECK (expires_at > created_at),
    FOREIGN KEY (tenant_id, admin_email) REFERENCES admin_credentials (tenant_id, admin_email) ON DELETE CASCADE
);

CREATE TABLE server_administrators (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email)) AND admin_email <> ''),
    role TEXT NOT NULL CHECK (role IN ('global_admin', 'tenant_admin', 'domain_admin')),
    domain_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, admin_email) REFERENCES admin_credentials (tenant_id, admin_email) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE account_sync_state (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'rights')),
    current_modseq BIGINT NOT NULL DEFAULT 1 CHECK (current_modseq > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, category),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE mailboxes (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    parent_mailbox_id UUID,
    role TEXT NOT NULL DEFAULT 'custom'
        CHECK (role IN ('inbox', 'sent', 'drafts', 'trash', 'archive', 'junk', 'custom')),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    normalized_display_name TEXT GENERATED ALWAYS AS (lower(display_name)) STORED,
    sort_order INTEGER NOT NULL DEFAULT 0,
    uidvalidity BIGINT NOT NULL CHECK (uidvalidity > 0),
    uidnext BIGINT NOT NULL DEFAULT 1 CHECK (uidnext > 0),
    highest_modseq BIGINT NOT NULL DEFAULT 1 CHECK (highest_modseq > 0),
    total_messages INTEGER NOT NULL DEFAULT 0 CHECK (total_messages >= 0),
    unread_messages INTEGER NOT NULL DEFAULT 0 CHECK (unread_messages >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, id),
    UNIQUE (tenant_id, account_id, normalized_display_name),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, parent_mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX mailboxes_account_role_idx
    ON mailboxes (tenant_id, account_id, role)
    WHERE role <> 'custom';

CREATE TABLE message_raw_blobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    blob_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, domain_id, content_sha256),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    raw_blob_id UUID NOT NULL,
    internet_message_id TEXT,
    message_hash TEXT NOT NULL CHECK (message_hash ~ '^[0-9a-f]{64}$'),
    normalized_subject TEXT NOT NULL DEFAULT '',
    sent_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    has_attachments BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, raw_blob_id) REFERENCES message_raw_blobs (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX messages_tenant_received_idx
    ON messages (tenant_id, received_at DESC);

CREATE INDEX messages_internet_message_idx
    ON messages (tenant_id, internet_message_id)
    WHERE internet_message_id IS NOT NULL;

CREATE TABLE message_headers (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    header_name TEXT NOT NULL CHECK (btrim(header_name) <> ''),
    header_value TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    UNIQUE (tenant_id, message_id, ordinal),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX message_headers_lookup_idx
    ON message_headers (tenant_id, message_id, lower(header_name));

CREATE TABLE message_visible_recipients (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('from', 'sender', 'reply_to', 'to', 'cc')),
    address TEXT NOT NULL CHECK (address = lower(btrim(address)) AND address <> ''),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX message_visible_recipients_message_idx
    ON message_visible_recipients (tenant_id, message_id, role, ordinal);

CREATE INDEX message_visible_recipients_address_idx
    ON message_visible_recipients (tenant_id, address);

CREATE TABLE message_protected_recipients (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role = 'bcc'),
    address TEXT NOT NULL CHECK (address = lower(btrim(address)) AND address <> ''),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    metadata_scope TEXT NOT NULL DEFAULT 'audit-compliance' CHECK (metadata_scope = 'audit-compliance'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX message_protected_recipients_message_idx
    ON message_protected_recipients (tenant_id, message_id, ordinal);

CREATE TABLE attachment_blobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    blob_bytes BYTEA NOT NULL,
    magika_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (magika_status IN ('pending', 'valid', 'rejected', 'failed')),
    magika_media_type TEXT,
    magika_confidence NUMERIC(5,4) CHECK (magika_confidence IS NULL OR (magika_confidence >= 0 AND magika_confidence <= 1)),
    extraction_status TEXT NOT NULL DEFAULT 'not_requested'
        CHECK (extraction_status IN ('not_requested', 'queued', 'running', 'succeeded', 'failed', 'unsupported')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    validated_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, domain_id, content_sha256),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX attachment_blobs_validation_idx
    ON attachment_blobs (tenant_id, magika_status, created_at);

CREATE INDEX attachment_blobs_extraction_idx
    ON attachment_blobs (tenant_id, extraction_status, created_at);

CREATE TABLE message_mime_parts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    parent_part_id UUID,
    part_path TEXT NOT NULL CHECK (btrim(part_path) <> ''),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    content_type TEXT NOT NULL CHECK (btrim(content_type) <> ''),
    content_disposition TEXT CHECK (content_disposition IS NULL OR content_disposition IN ('inline', 'attachment')),
    content_id TEXT,
    file_name TEXT,
    transfer_encoding TEXT,
    charset_name TEXT,
    size_octets BIGINT NOT NULL DEFAULT 0 CHECK (size_octets >= 0),
    body_blob_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, message_id, id),
    UNIQUE (tenant_id, message_id, part_path),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, parent_part_id) REFERENCES message_mime_parts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, body_blob_id) REFERENCES attachment_blobs (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX message_mime_parts_message_idx
    ON message_mime_parts (tenant_id, message_id, ordinal);

CREATE TABLE message_body_parts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    mime_part_id UUID NOT NULL,
    body_kind TEXT NOT NULL CHECK (body_kind IN ('text', 'html')),
    body_text TEXT NOT NULL,
    sanitized_html TEXT,
    language_code TEXT,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    search_vector TSVECTOR NOT NULL,
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, message_id, mime_part_id, body_kind),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id, mime_part_id)
        REFERENCES message_mime_parts (tenant_id, message_id, id)
        ON DELETE CASCADE
);

CREATE INDEX message_body_parts_search_idx
    ON message_body_parts USING GIN (search_vector);

CREATE TABLE account_messages (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    message_id UUID NOT NULL,
    thread_id UUID NOT NULL,
    lifecycle_status TEXT NOT NULL DEFAULT 'stored'
        CHECK (lifecycle_status IN ('stored', 'draft', 'submitted')),
    preview_text TEXT NOT NULL DEFAULT '',
    received_at TIMESTAMPTZ NOT NULL,
    snoozed_until TIMESTAMPTZ,
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, id),
    UNIQUE (tenant_id, account_id, message_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX account_messages_account_received_idx
    ON account_messages (tenant_id, account_id, received_at DESC);

CREATE INDEX account_messages_thread_idx
    ON account_messages (tenant_id, account_id, thread_id);

CREATE TABLE account_message_keywords (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    account_message_id UUID NOT NULL,
    keyword TEXT NOT NULL CHECK (btrim(keyword) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, account_message_id, keyword),
    FOREIGN KEY (tenant_id, account_id, account_message_id)
        REFERENCES account_messages (tenant_id, account_id, id)
        ON DELETE CASCADE
);

CREATE TABLE mailbox_message_memberships (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    mailbox_id UUID NOT NULL,
    account_message_id UUID NOT NULL,
    imap_uid BIGINT NOT NULL CHECK (imap_uid > 0),
    membership_modseq BIGINT NOT NULL DEFAULT 1 CHECK (membership_modseq > 0),
    is_seen BOOLEAN NOT NULL DEFAULT FALSE,
    is_flagged BOOLEAN NOT NULL DEFAULT FALSE,
    is_answered BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    is_draft BOOLEAN NOT NULL DEFAULT FALSE,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, mailbox_id, imap_uid),
    UNIQUE (tenant_id, account_id, mailbox_id, account_message_id),
    FOREIGN KEY (tenant_id, account_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, account_message_id)
        REFERENCES account_messages (tenant_id, account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX mailbox_memberships_uid_idx
    ON mailbox_message_memberships (tenant_id, account_id, mailbox_id, imap_uid);

CREATE INDEX mailbox_memberships_modseq_idx
    ON mailbox_message_memberships (tenant_id, account_id, mailbox_id, membership_modseq);

CREATE INDEX mailbox_memberships_deleted_idx
    ON mailbox_message_memberships (tenant_id, account_id, mailbox_id, imap_uid)
    WHERE is_deleted = TRUE;

CREATE TABLE attachments (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    account_message_id UUID NOT NULL,
    message_id UUID NOT NULL,
    mime_part_id UUID,
    attachment_blob_id UUID NOT NULL,
    file_name TEXT NOT NULL CHECK (btrim(file_name) <> ''),
    disposition TEXT NOT NULL DEFAULT 'attachment' CHECK (disposition IN ('attachment', 'inline')),
    content_id TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id, account_message_id)
        REFERENCES account_messages (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id, mime_part_id)
        REFERENCES message_mime_parts (tenant_id, message_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, attachment_blob_id) REFERENCES attachment_blobs (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX attachments_account_message_idx
    ON attachments (tenant_id, account_id, account_message_id, ordinal);

CREATE INDEX attachments_blob_idx
    ON attachments (tenant_id, attachment_blob_id);

CREATE TABLE attachment_extraction_jobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    attachment_blob_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'unsupported')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, attachment_blob_id) REFERENCES attachment_blobs (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX attachment_extraction_jobs_pending_idx
    ON attachment_extraction_jobs (tenant_id, status, next_attempt_at);

CREATE TABLE attachment_texts (
    tenant_id UUID NOT NULL,
    attachment_blob_id UUID NOT NULL,
    extracted_text TEXT NOT NULL,
    language_code TEXT,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    search_vector TSVECTOR NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, attachment_blob_id),
    FOREIGN KEY (tenant_id, attachment_blob_id) REFERENCES attachment_blobs (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX attachment_texts_search_idx
    ON attachment_texts USING GIN (search_vector);

CREATE TABLE mail_search_documents (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    account_message_id UUID NOT NULL,
    message_id UUID NOT NULL,
    subject_text TEXT NOT NULL DEFAULT '',
    participants_visible TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    attachment_text TEXT NOT NULL DEFAULT '',
    search_vector TSVECTOR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, account_message_id),
    FOREIGN KEY (tenant_id, account_id, account_message_id)
        REFERENCES account_messages (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mail_search_documents_search_idx
    ON mail_search_documents USING GIN (search_vector);

CREATE TABLE object_change_log (
    sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID,
    mailbox_id UUID,
    category TEXT NOT NULL CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'rights')),
    object_kind TEXT NOT NULL CHECK (btrim(object_kind) <> ''),
    object_id UUID NOT NULL,
    change_kind TEXT NOT NULL CHECK (change_kind IN ('created', 'updated', 'destroyed', 'moved', 'expunged')),
    account_modseq BIGINT CHECK (account_modseq IS NULL OR account_modseq > 0),
    mailbox_modseq BIGINT CHECK (mailbox_modseq IS NULL OR mailbox_modseq > 0),
    visible_to_account_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, mailbox_id) REFERENCES mailboxes (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX object_change_log_account_idx
    ON object_change_log (tenant_id, account_id, category, sequence);

CREATE INDEX object_change_log_mailbox_idx
    ON object_change_log (tenant_id, mailbox_id, sequence)
    WHERE mailbox_id IS NOT NULL;

CREATE INDEX object_change_log_visible_gin_idx
    ON object_change_log USING GIN (visible_to_account_ids);

CREATE TABLE object_tombstones (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID,
    mailbox_id UUID,
    category TEXT NOT NULL CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'rights')),
    object_kind TEXT NOT NULL CHECK (btrim(object_kind) <> ''),
    object_id UUID NOT NULL,
    account_message_id UUID,
    imap_uid BIGINT CHECK (imap_uid IS NULL OR imap_uid > 0),
    deleted_modseq BIGINT NOT NULL CHECK (deleted_modseq > 0),
    change_sequence BIGINT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('delete', 'expunge', 'destroyed', 'move', 'purge')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, mailbox_id) REFERENCES mailboxes (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (change_sequence) REFERENCES object_change_log (sequence) ON DELETE RESTRICT
);

CREATE INDEX object_tombstones_account_idx
    ON object_tombstones (tenant_id, account_id, category, change_sequence);

CREATE INDEX object_tombstones_mailbox_uid_idx
    ON object_tombstones (tenant_id, mailbox_id, imap_uid)
    WHERE mailbox_id IS NOT NULL AND imap_uid IS NOT NULL;

CREATE TABLE jmap_upload_blobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    blob_bytes BYTEA NOT NULL,
    magika_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (magika_status IN ('pending', 'valid', 'rejected', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    CHECK (expires_at > created_at),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX jmap_upload_blobs_expiry_idx
    ON jmap_upload_blobs (tenant_id, expires_at);

CREATE TABLE jmap_query_states (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    method_name TEXT NOT NULL CHECK (btrim(method_name) <> ''),
    filter_hash TEXT NOT NULL CHECK (btrim(filter_hash) <> ''),
    sort_hash TEXT NOT NULL CHECK (btrim(sort_hash) <> ''),
    last_change_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_change_sequence >= 0),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX jmap_query_states_account_idx
    ON jmap_query_states (tenant_id, account_id, method_name, expires_at);

CREATE TABLE activesync_sync_cursors (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    device_id TEXT NOT NULL CHECK (btrim(device_id) <> ''),
    collection_kind TEXT NOT NULL CHECK (collection_kind IN ('mail', 'contacts', 'calendar', 'tasks')),
    collection_id UUID NOT NULL,
    sync_key TEXT NOT NULL CHECK (btrim(sync_key) <> ''),
    last_change_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_change_sequence >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, account_id, device_id, collection_kind, collection_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX activesync_sync_cursors_account_idx
    ON activesync_sync_cursors (tenant_id, account_id, device_id, updated_at DESC);

CREATE TABLE mapi_sync_checkpoints (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    mailbox_id UUID,
    checkpoint_kind TEXT NOT NULL CHECK (checkpoint_kind IN ('hierarchy', 'content', 'read_state')),
    mapi_replica_guid UUID NOT NULL,
    last_change_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_change_sequence >= 0),
    last_modseq BIGINT NOT NULL DEFAULT 1 CHECK (last_modseq > 0),
    cursor_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, account_id, mailbox_id, checkpoint_kind, mapi_replica_guid),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, mailbox_id) REFERENCES mailboxes (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mapi_sync_checkpoints_account_idx
    ON mapi_sync_checkpoints (tenant_id, account_id, updated_at DESC);

CREATE TABLE submission_requests (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    identity_id UUID,
    sent_account_message_id UUID NOT NULL,
    from_address TEXT NOT NULL CHECK (from_address = lower(btrim(from_address)) AND from_address <> ''),
    sender_address TEXT CHECK (sender_address IS NULL OR sender_address = lower(btrim(sender_address))),
    authorization_kind TEXT NOT NULL DEFAULT 'self'
        CHECK (authorization_kind IN ('self', 'send_as', 'send_on_behalf')),
    source_protocol TEXT NOT NULL CHECK (source_protocol IN ('web', 'jmap', 'ews', 'mapi', 'activesync', 'lpe_ct_submission')),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'handed_off', 'relayed', 'deferred', 'quarantined', 'bounced', 'failed')),
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, identity_id) REFERENCES account_identities (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, sent_account_message_id)
        REFERENCES account_messages (tenant_id, account_id, id)
        ON DELETE RESTRICT
);

CREATE INDEX submission_requests_status_idx
    ON submission_requests (tenant_id, status, created_at);

CREATE TABLE submission_recipients (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    submission_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('to', 'cc', 'bcc')),
    address TEXT NOT NULL CHECK (address = lower(btrim(address)) AND address <> ''),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    protected_metadata BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK ((role = 'bcc' AND protected_metadata = TRUE) OR (role <> 'bcc' AND protected_metadata = FALSE)),
    FOREIGN KEY (tenant_id, submission_id) REFERENCES submission_requests (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX submission_recipients_submission_idx
    ON submission_recipients (tenant_id, submission_id, ordinal);

CREATE TABLE outbound_message_queue (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    submission_id UUID NOT NULL,
    account_id UUID NOT NULL,
    account_message_id UUID NOT NULL,
    transport TEXT NOT NULL DEFAULT 'lpe-ct-smtp' CHECK (transport = 'lpe-ct-smtp'),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'handed_off', 'relayed', 'deferred', 'quarantined', 'bounced', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    last_trace_id TEXT,
    remote_message_ref TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, submission_id),
    FOREIGN KEY (tenant_id, submission_id) REFERENCES submission_requests (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, account_message_id)
        REFERENCES account_messages (tenant_id, account_id, id)
        ON DELETE RESTRICT
);

CREATE INDEX outbound_message_queue_pending_idx
    ON outbound_message_queue (tenant_id, status, next_attempt_at);

CREATE TABLE submission_result_history (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    queue_id UUID NOT NULL,
    submission_id UUID NOT NULL,
    trace_id TEXT NOT NULL CHECK (btrim(trace_id) <> ''),
    result_status TEXT NOT NULL
        CHECK (result_status IN ('accepted', 'duplicate', 'relayed', 'deferred', 'quarantined', 'bounced', 'failed')),
    remote_message_ref TEXT,
    dsn_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    technical_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    route_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    throttle_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, queue_id, trace_id),
    FOREIGN KEY (tenant_id, queue_id) REFERENCES outbound_message_queue (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, submission_id) REFERENCES submission_requests (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX submission_result_history_submission_idx
    ON submission_result_history (tenant_id, submission_id, received_at DESC);

CREATE TABLE lpe_ct_inbound_delivery_receipts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    trace_id TEXT NOT NULL CHECK (btrim(trace_id) <> ''),
    recipient_account_id UUID NOT NULL,
    account_message_id UUID,
    status TEXT NOT NULL CHECK (status IN ('delivered', 'duplicate', 'rejected')),
    response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, trace_id, recipient_account_id),
    CHECK ((status IN ('delivered', 'duplicate') AND account_message_id IS NOT NULL) OR status = 'rejected'),
    FOREIGN KEY (tenant_id, recipient_account_id, account_message_id)
        REFERENCES account_messages (tenant_id, account_id, id)
        ON DELETE RESTRICT
);

CREATE TABLE contact_books (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    role TEXT NOT NULL DEFAULT 'contacts' CHECK (role IN ('contacts', 'directory', 'custom')),
    sync_modseq BIGINT NOT NULL DEFAULT 1 CHECK (sync_modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE contacts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    contact_book_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    given_name TEXT NOT NULL DEFAULT '',
    family_name TEXT NOT NULL DEFAULT '',
    emails_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    phones_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    notes TEXT NOT NULL DEFAULT '',
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, owner_account_id, contact_book_id)
        REFERENCES contact_books (tenant_id, owner_account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX contacts_owner_name_idx
    ON contacts (tenant_id, owner_account_id, display_name);

CREATE TABLE calendars (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    color TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'calendar' CHECK (role IN ('calendar', 'birthdays', 'custom')),
    sync_modseq BIGINT NOT NULL DEFAULT 1 CHECK (sync_modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE calendar_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    calendar_id UUID NOT NULL,
    uid TEXT NOT NULL CHECK (btrim(uid) <> ''),
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    description TEXT NOT NULL DEFAULT '',
    location TEXT NOT NULL DEFAULT '',
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    time_zone TEXT NOT NULL DEFAULT '',
    recurrence_rule TEXT,
    attendees_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (ends_at >= starts_at),
    FOREIGN KEY (tenant_id, owner_account_id, calendar_id)
        REFERENCES calendars (tenant_id, owner_account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX calendar_events_owner_time_idx
    ON calendar_events (tenant_id, owner_account_id, starts_at, ends_at);

CREATE TABLE task_lists (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    role TEXT NOT NULL DEFAULT 'custom' CHECK (role IN ('inbox', 'custom')),
    sync_modseq BIGINT NOT NULL DEFAULT 1 CHECK (sync_modseq > 0),
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX task_lists_owner_role_idx
    ON task_lists (tenant_id, owner_account_id, role)
    WHERE role <> 'custom';

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    task_list_id UUID NOT NULL,
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'needs_action'
        CHECK (status IN ('needs_action', 'in_progress', 'completed', 'cancelled')),
    due_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, owner_account_id, task_list_id)
        REFERENCES task_lists (tenant_id, owner_account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX tasks_owner_status_idx
    ON tasks (tenant_id, owner_account_id, task_list_id, status, sort_order);

CREATE TABLE collection_grants (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    collection_kind TEXT NOT NULL CHECK (collection_kind IN ('mailbox', 'contacts', 'calendar', 'tasks')),
    collection_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, collection_kind, collection_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX collection_grants_grantee_idx
    ON collection_grants (tenant_id, collection_kind, grantee_account_id, owner_account_id);

CREATE TABLE mailbox_delegation_grants (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    mailbox_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, mailbox_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete)),
    CHECK ((NOT may_delete) OR may_write),
    FOREIGN KEY (tenant_id, owner_account_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mailbox_delegation_grants_grantee_idx
    ON mailbox_delegation_grants (tenant_id, grantee_account_id, owner_account_id);

CREATE TABLE sender_rights (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    identity_id UUID,
    sender_right TEXT NOT NULL CHECK (sender_right IN ('send_as', 'send_on_behalf')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, owner_account_id, grantee_account_id, identity_id, sender_right),
    CHECK (owner_account_id <> grantee_account_id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, identity_id) REFERENCES account_identities (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX sender_rights_grantee_idx
    ON sender_rights (tenant_id, grantee_account_id, owner_account_id);

CREATE UNIQUE INDEX sender_rights_account_wide_idx
    ON sender_rights (tenant_id, owner_account_id, grantee_account_id, sender_right)
    WHERE identity_id IS NULL;

CREATE UNIQUE INDEX sender_rights_identity_idx
    ON sender_rights (tenant_id, owner_account_id, grantee_account_id, identity_id, sender_right)
    WHERE identity_id IS NOT NULL;

CREATE TABLE document_projections (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('mail', 'contact', 'calendar', 'task', 'attachment')),
    source_object_id UUID NOT NULL,
    acl_fingerprint TEXT NOT NULL CHECK (btrim(acl_fingerprint) <> ''),
    title TEXT NOT NULL DEFAULT '',
    preview TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL,
    participants_visible TEXT NOT NULL DEFAULT '',
    language_code TEXT,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    search_vector TSVECTOR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, source_kind, source_object_id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX document_projections_owner_idx
    ON document_projections (tenant_id, owner_account_id, source_kind, updated_at DESC);

CREATE INDEX document_projections_search_idx
    ON document_projections USING GIN (search_vector);

CREATE TABLE document_chunks (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    document_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    chunk_text TEXT NOT NULL,
    token_estimate INTEGER NOT NULL DEFAULT 0 CHECK (token_estimate >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, document_id, ordinal),
    FOREIGN KEY (tenant_id, document_id) REFERENCES document_projections (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE inference_runs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    principal_account_id UUID NOT NULL,
    model_name TEXT NOT NULL CHECK (btrim(model_name) <> ''),
    operation TEXT NOT NULL CHECK (btrim(operation) <> ''),
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_payload JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, principal_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX inference_runs_principal_idx
    ON inference_runs (tenant_id, principal_account_id, started_at DESC);

CREATE TABLE inference_run_chunks (
    tenant_id UUID NOT NULL,
    inference_run_id UUID NOT NULL,
    chunk_id UUID NOT NULL,
    PRIMARY KEY (tenant_id, inference_run_id, chunk_id),
    FOREIGN KEY (tenant_id, inference_run_id) REFERENCES inference_runs (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, chunk_id) REFERENCES document_chunks (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE audit_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    actor_account_id UUID,
    action TEXT NOT NULL CHECK (btrim(action) <> ''),
    subject_kind TEXT NOT NULL CHECK (btrim(subject_kind) <> ''),
    subject_id UUID,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, actor_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX audit_events_tenant_created_idx
    ON audit_events (tenant_id, created_at DESC);

CREATE OR REPLACE VIEW searchable_mail_documents AS
SELECT
    msd.tenant_id,
    msd.account_id,
    msd.account_message_id,
    msd.message_id,
    msd.search_vector
FROM mail_search_documents msd;

INSERT INTO schema_metadata (singleton, schema_version)
VALUES (TRUE, '0.3.0-sql-v2');

COMMIT;
