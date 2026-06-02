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
    schema_version TEXT NOT NULL CHECK (schema_version = '0.4.0-sql-v2'),
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

INSERT INTO tenants (id, slug, display_name)
VALUES ('00000000-0000-0000-0000-000000000001', 'platform', 'LPE Platform');

CREATE TABLE domains (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (name = btrim(name) AND name <> ''),
    normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    inbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    outbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    default_quota_mb INTEGER NOT NULL DEFAULT 4096 CHECK (default_quota_mb >= 0),
    default_sieve_script TEXT NOT NULL DEFAULT '',
    jmap_push_journal_retention_days INTEGER NOT NULL DEFAULT 30
        CHECK (jmap_push_journal_retention_days >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, name),
    UNIQUE (tenant_id, normalized_name),
    UNIQUE (name),
    UNIQUE (normalized_name),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE INDEX domains_tenant_status_idx
    ON domains (tenant_id, status, name);

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    primary_domain_id UUID NOT NULL,
    primary_email TEXT NOT NULL CHECK (
        primary_email = btrim(primary_email)
        AND split_part(primary_email, '@', 1) <> ''
        AND split_part(primary_email, '@', 2) <> ''
        AND split_part(primary_email, '@', 3) = ''
    ),
    normalized_primary_email TEXT GENERATED ALWAYS AS (lower(primary_email)) STORED,
    normalized_primary_email_local_part TEXT GENERATED ALWAYS AS (lower(split_part(primary_email, '@', 1))) STORED,
    normalized_primary_email_domain TEXT GENERATED ALWAYS AS (lower(split_part(primary_email, '@', 2))) STORED,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    account_kind TEXT NOT NULL DEFAULT 'person'
        CHECK (account_kind IN ('person', 'shared_mailbox', 'room', 'equipment', 'service')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'disabled')),
    quota_mb INTEGER NOT NULL DEFAULT 4096 CHECK (quota_mb >= 0),
    quota_used_octets BIGINT NOT NULL DEFAULT 0 CHECK (quota_used_octets >= 0),
    gal_visibility TEXT NOT NULL DEFAULT 'tenant' CHECK (gal_visibility IN ('tenant', 'hidden')),
    directory_kind TEXT NOT NULL DEFAULT 'person' CHECK (directory_kind IN ('person', 'room', 'equipment')),
    recoverable_items_retention_days INTEGER NOT NULL DEFAULT 14 CHECK (recoverable_items_retention_days >= 0),
    litigation_hold_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    litigation_hold_started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, primary_email),
    UNIQUE (tenant_id, normalized_primary_email),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, primary_domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX accounts_tenant_status_idx
    ON accounts (tenant_id, status, account_kind);

CREATE INDEX accounts_primary_domain_idx
    ON accounts (tenant_id, primary_domain_id, status);

CREATE TABLE account_email_addresses (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    email TEXT NOT NULL CHECK (
        email = btrim(email)
        AND split_part(email, '@', 1) <> ''
        AND split_part(email, '@', 2) <> ''
        AND split_part(email, '@', 3) = ''
    ),
    normalized_email TEXT GENERATED ALWAYS AS (lower(email)) STORED,
    normalized_email_local_part TEXT GENERATED ALWAYS AS (lower(split_part(email, '@', 1))) STORED,
    normalized_email_domain TEXT GENERATED ALWAYS AS (lower(split_part(email, '@', 2))) STORED,
    address_kind TEXT NOT NULL DEFAULT 'primary' CHECK (address_kind IN ('primary', 'alias', 'reply_to')),
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, id),
    UNIQUE (tenant_id, email),
    UNIQUE (tenant_id, normalized_email),
    CHECK ((NOT is_primary) OR address_kind = 'primary'),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX account_email_addresses_primary_idx
    ON account_email_addresses (tenant_id, account_id)
    WHERE is_primary = TRUE;

CREATE INDEX account_email_addresses_account_idx
    ON account_email_addresses (tenant_id, account_id, status);

CREATE TABLE aliases (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    source TEXT NOT NULL CHECK (source = btrim(source) AND source <> ''),
    normalized_source TEXT GENERATED ALWAYS AS (lower(source)) STORED,
    target TEXT NOT NULL CHECK (target = btrim(target) AND target <> ''),
    normalized_target TEXT GENERATED ALWAYS AS (lower(target)) STORED,
    kind TEXT NOT NULL CHECK (kind IN ('account', 'external', 'group')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, source),
    UNIQUE (tenant_id, normalized_source),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE account_identities (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    email_address_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    reply_to_email TEXT CHECK (reply_to_email IS NULL OR (reply_to_email = lower(btrim(reply_to_email)) AND reply_to_email <> '')),
    signature_text TEXT NOT NULL DEFAULT '',
    may_send BOOLEAN NOT NULL DEFAULT TRUE,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, id),
    CHECK ((NOT is_default) OR may_send),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, email_address_id)
        REFERENCES account_email_addresses (tenant_id, account_id, id)
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX account_identities_default_idx
    ON account_identities (tenant_id, account_id)
    WHERE is_default = TRUE;

CREATE TABLE account_credentials (
    tenant_id UUID NOT NULL,
    account_email TEXT NOT NULL CHECK (
        account_email = btrim(account_email)
        AND split_part(account_email, '@', 1) <> ''
        AND split_part(account_email, '@', 2) <> ''
        AND split_part(account_email, '@', 3) = ''
    ),
    normalized_account_email TEXT GENERATED ALWAYS AS (lower(account_email)) STORED,
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_email),
    UNIQUE (tenant_id, normalized_account_email),
    FOREIGN KEY (tenant_id, account_email) REFERENCES accounts (tenant_id, primary_email) ON DELETE CASCADE
);

CREATE TABLE account_sessions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    token TEXT NOT NULL CHECK (btrim(token) <> ''),
    account_email TEXT NOT NULL CHECK (
        account_email = btrim(account_email)
        AND split_part(account_email, '@', 1) <> ''
        AND split_part(account_email, '@', 2) <> ''
        AND split_part(account_email, '@', 3) = ''
    ),
    normalized_account_email TEXT GENERATED ALWAYS AS (lower(account_email)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (token),
    CHECK (expires_at > created_at),
    FOREIGN KEY (tenant_id, account_email) REFERENCES account_credentials (tenant_id, account_email) ON DELETE CASCADE
);

CREATE INDEX account_sessions_account_idx
    ON account_sessions (tenant_id, normalized_account_email, expires_at DESC);

CREATE INDEX account_sessions_expiry_idx
    ON account_sessions (expires_at);

CREATE TABLE admin_credentials (
    tenant_id UUID NOT NULL,
    email TEXT NOT NULL CHECK (email = lower(btrim(email)) AND email <> ''),
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, email),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE admin_sessions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    token TEXT NOT NULL CHECK (btrim(token) <> ''),
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email)) AND admin_email <> ''),
    auth_method TEXT NOT NULL DEFAULT 'password' CHECK (auth_method IN ('password', 'oidc')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (token),
    CHECK (expires_at > created_at),
    FOREIGN KEY (tenant_id, admin_email) REFERENCES admin_credentials (tenant_id, email) ON DELETE CASCADE
);

CREATE INDEX admin_sessions_expiry_idx
    ON admin_sessions (expires_at);

CREATE TABLE server_administrators (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID,
    email TEXT NOT NULL CHECK (email = lower(btrim(email)) AND email <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    role TEXT NOT NULL CHECK (role IN ('server-admin', 'tenant-admin', 'domain-admin', 'security-admin', 'auditor', 'custom')),
    rights_summary TEXT NOT NULL DEFAULT '',
    permissions_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, email),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE server_settings (
    tenant_id UUID PRIMARY KEY,
    primary_hostname TEXT NOT NULL DEFAULT 'localhost',
    admin_bind_address TEXT NOT NULL DEFAULT '127.0.0.1:8080',
    smtp_bind_address TEXT NOT NULL DEFAULT '0.0.0.0:25',
    imap_bind_address TEXT NOT NULL DEFAULT '0.0.0.0:143',
    jmap_bind_address TEXT NOT NULL DEFAULT '0.0.0.0:8081',
    default_locale TEXT NOT NULL DEFAULT 'en' CHECK (default_locale IN ('en', 'fr', 'de', 'it', 'es')),
    max_message_size_mb INTEGER NOT NULL DEFAULT 64 CHECK (max_message_size_mb >= 8),
    tls_mode TEXT NOT NULL DEFAULT 'required',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE security_settings (
    tenant_id UUID PRIMARY KEY,
    password_login_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mfa_required_for_admins BOOLEAN NOT NULL DEFAULT TRUE,
    session_timeout_minutes INTEGER NOT NULL DEFAULT 45 CHECK (session_timeout_minutes >= 5),
    audit_retention_days INTEGER NOT NULL DEFAULT 365 CHECK (audit_retention_days >= 30),
    oidc_login_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    oidc_provider_label TEXT NOT NULL DEFAULT 'Corporate SSO',
    oidc_auto_link_by_email BOOLEAN NOT NULL DEFAULT TRUE,
    mailbox_password_login_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mailbox_oidc_login_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    mailbox_oidc_provider_label TEXT NOT NULL DEFAULT 'Mailbox SSO',
    mailbox_oidc_auto_link_by_email BOOLEAN NOT NULL DEFAULT TRUE,
    mailbox_app_passwords_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE admin_oidc_config (
    tenant_id UUID PRIMARY KEY,
    issuer_url TEXT NOT NULL DEFAULT '',
    authorization_endpoint TEXT NOT NULL DEFAULT '',
    token_endpoint TEXT NOT NULL DEFAULT '',
    userinfo_endpoint TEXT NOT NULL DEFAULT '',
    client_id TEXT NOT NULL DEFAULT '',
    client_secret TEXT NOT NULL DEFAULT '',
    scopes TEXT NOT NULL DEFAULT 'openid profile email',
    claim_email TEXT NOT NULL DEFAULT 'email',
    claim_display_name TEXT NOT NULL DEFAULT 'name',
    claim_subject TEXT NOT NULL DEFAULT 'sub',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE account_oidc_config (
    tenant_id UUID PRIMARY KEY,
    issuer_url TEXT NOT NULL DEFAULT '',
    authorization_endpoint TEXT NOT NULL DEFAULT '',
    token_endpoint TEXT NOT NULL DEFAULT '',
    userinfo_endpoint TEXT NOT NULL DEFAULT '',
    client_id TEXT NOT NULL DEFAULT '',
    client_secret TEXT NOT NULL DEFAULT '',
    scopes TEXT NOT NULL DEFAULT 'openid profile email',
    claim_email TEXT NOT NULL DEFAULT 'email',
    claim_display_name TEXT NOT NULL DEFAULT 'name',
    claim_subject TEXT NOT NULL DEFAULT 'sub',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE admin_oidc_identities (
    tenant_id UUID NOT NULL,
    issuer_url TEXT NOT NULL CHECK (btrim(issuer_url) <> ''),
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email)) AND admin_email <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, issuer_url, subject),
    FOREIGN KEY (tenant_id, admin_email) REFERENCES server_administrators (tenant_id, email) ON DELETE CASCADE
);

CREATE INDEX admin_oidc_identities_admin_idx
    ON admin_oidc_identities (tenant_id, admin_email);

CREATE INDEX admin_oidc_identities_provider_subject_idx
    ON admin_oidc_identities (issuer_url, subject);

CREATE TABLE account_oidc_identities (
    tenant_id UUID NOT NULL,
    issuer_url TEXT NOT NULL CHECK (btrim(issuer_url) <> ''),
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    account_email TEXT NOT NULL CHECK (
        account_email = btrim(account_email)
        AND split_part(account_email, '@', 1) <> ''
        AND split_part(account_email, '@', 2) <> ''
        AND split_part(account_email, '@', 3) = ''
    ),
    normalized_account_email TEXT GENERATED ALWAYS AS (lower(account_email)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, issuer_url, subject),
    FOREIGN KEY (tenant_id, account_email) REFERENCES accounts (tenant_id, primary_email) ON DELETE CASCADE
);

CREATE INDEX account_oidc_identities_account_idx
    ON account_oidc_identities (tenant_id, normalized_account_email);

CREATE INDEX account_oidc_identities_provider_subject_idx
    ON account_oidc_identities (issuer_url, subject);

CREATE TABLE admin_auth_factors (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email)) AND admin_email <> ''),
    factor_type TEXT NOT NULL CHECK (factor_type IN ('totp')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'active', 'revoked')),
    secret_ciphertext TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verified_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    CHECK ((status = 'active' AND verified_at IS NOT NULL) OR status <> 'active'),
    FOREIGN KEY (tenant_id, admin_email) REFERENCES admin_credentials (tenant_id, email) ON DELETE CASCADE
);

CREATE INDEX admin_auth_factors_admin_idx
    ON admin_auth_factors (tenant_id, admin_email, factor_type, status);

CREATE TABLE account_auth_factors (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_email TEXT NOT NULL CHECK (
        account_email = btrim(account_email)
        AND split_part(account_email, '@', 1) <> ''
        AND split_part(account_email, '@', 2) <> ''
        AND split_part(account_email, '@', 3) = ''
    ),
    normalized_account_email TEXT GENERATED ALWAYS AS (lower(account_email)) STORED,
    factor_type TEXT NOT NULL CHECK (factor_type IN ('totp')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'active', 'revoked')),
    secret_ciphertext TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verified_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    CHECK ((status = 'active' AND verified_at IS NOT NULL) OR status <> 'active'),
    FOREIGN KEY (tenant_id, account_email) REFERENCES account_credentials (tenant_id, account_email) ON DELETE CASCADE
);

CREATE INDEX account_auth_factors_account_idx
    ON account_auth_factors (tenant_id, normalized_account_email, factor_type, status);

CREATE TABLE account_app_passwords (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_email TEXT NOT NULL CHECK (
        account_email = btrim(account_email)
        AND split_part(account_email, '@', 1) <> ''
        AND split_part(account_email, '@', 2) <> ''
        AND split_part(account_email, '@', 3) = ''
    ),
    normalized_account_email TEXT GENERATED ALWAYS AS (lower(account_email)) STORED,
    label TEXT NOT NULL CHECK (btrim(label) <> ''),
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_email) REFERENCES account_credentials (tenant_id, account_email) ON DELETE CASCADE
);

CREATE INDEX account_app_passwords_account_idx
    ON account_app_passwords (tenant_id, normalized_account_email, status, created_at DESC);

CREATE TABLE sieve_scripts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED,
    content TEXT NOT NULL CHECK (btrim(content) <> ''),
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, normalized_name),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX sieve_scripts_active_account_idx
    ON sieve_scripts (tenant_id, account_id)
    WHERE is_active = TRUE;

CREATE INDEX sieve_scripts_account_idx
    ON sieve_scripts (tenant_id, account_id, normalized_name);

CREATE TABLE sieve_vacation_responses (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    sender_address TEXT NOT NULL CHECK (sender_address = lower(btrim(sender_address)) AND sender_address <> ''),
    response_key TEXT NOT NULL CHECK (response_key ~ '^[0-9a-f]{64}$'),
    last_sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, sender_address, response_key),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX sieve_vacation_responses_last_sent_idx
    ON sieve_vacation_responses (tenant_id, account_id, last_sent_at);

CREATE TABLE local_ai_settings (
    tenant_id UUID PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    provider TEXT NOT NULL DEFAULT 'stub-local',
    model TEXT NOT NULL DEFAULT 'gemma3-local',
    offline_only BOOLEAN NOT NULL DEFAULT TRUE,
    indexing_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE account_sync_state (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'notes', 'journal', 'rights', 'search', 'rules', 'conversation_actions', 'public_folders')),
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
        CHECK (role IN (
            'inbox', 'sent', 'drafts', 'trash', 'archive', 'junk',
            'outbox', 'conversation_history', 'rss_feeds',
            'sync_issues', 'conflicts', 'local_failures', 'server_failures',
            'custom'
        )),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    -- Runtime storage APIs enforce NFC, canonical-key, and confusable mailbox-name policy.
    -- This lower(...) column is only the remaining SQL compatibility guard, not Unicode identity.
    normalized_display_name TEXT GENERATED ALWAYS AS (lower(display_name)) STORED,
    sort_order INTEGER NOT NULL DEFAULT 0,
    retention_days INTEGER NOT NULL DEFAULT 365 CHECK (retention_days >= 0),
    recoverable_items_retention_days INTEGER CHECK (recoverable_items_retention_days IS NULL OR recoverable_items_retention_days >= 0),
    hierarchy_path TEXT NOT NULL DEFAULT '/' CHECK (left(hierarchy_path, 1) = '/'),
    hierarchy_depth INTEGER NOT NULL DEFAULT 0 CHECK (hierarchy_depth >= 0),
    uid_validity BIGINT NOT NULL CHECK (uid_validity > 0),
    uid_next BIGINT NOT NULL DEFAULT 1 CHECK (uid_next > 0),
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    total_messages INTEGER NOT NULL DEFAULT 0 CHECK (total_messages >= 0),
    unread_messages INTEGER NOT NULL DEFAULT 0 CHECK (unread_messages >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, id),
    UNIQUE (tenant_id, account_id, parent_mailbox_id, normalized_display_name),
    CHECK (parent_mailbox_id IS NULL OR parent_mailbox_id <> id),
    CHECK (unread_messages <= total_messages),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, parent_mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX mailboxes_account_role_idx
    ON mailboxes (tenant_id, account_id, role)
    WHERE role <> 'custom';

CREATE UNIQUE INDEX mailboxes_account_root_name_idx
    ON mailboxes (tenant_id, account_id, normalized_display_name)
    WHERE parent_mailbox_id IS NULL;

CREATE INDEX mailboxes_parent_idx
    ON mailboxes (tenant_id, account_id, parent_mailbox_id, sort_order);

CREATE INDEX mailboxes_hierarchy_idx
    ON mailboxes (tenant_id, account_id, hierarchy_path);

CREATE TABLE search_folders (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX search_folders_builtin_role_idx
    ON search_folders (tenant_id, account_id, role)
    WHERE is_builtin;

CREATE INDEX search_folders_account_idx
    ON search_folders (tenant_id, account_id, display_name);

CREATE TABLE conversation_actions (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, move_target_mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE SET NULL (move_target_mailbox_id)
);

CREATE INDEX conversation_actions_account_idx
    ON conversation_actions (tenant_id, account_id, updated_at DESC, id);

CREATE TABLE mailbox_subscriptions (
    tenant_id UUID NOT NULL,
    mailbox_account_id UUID NOT NULL,
    mailbox_id UUID NOT NULL,
    subscriber_account_id UUID NOT NULL,
    is_subscribed BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, mailbox_account_id, mailbox_id, subscriber_account_id),
    FOREIGN KEY (tenant_id, mailbox_account_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, subscriber_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX mailbox_subscriptions_subscriber_idx
    ON mailbox_subscriptions (tenant_id, subscriber_account_id, is_subscribed);

CREATE TABLE storage_pools (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL CHECK (name = lower(btrim(name)) AND name <> ''),
    pool_kind TEXT NOT NULL CHECK (pool_kind IN ('postgres', 's3_compatible')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (pool_kind = 'postgres' AND config_json = '{}'::jsonb)
        OR (pool_kind = 's3_compatible' AND jsonb_typeof(config_json) = 'object')
    ),
    UNIQUE (name)
);

INSERT INTO storage_pools (id, name, pool_kind)
VALUES ('00000000-0000-0000-0000-000000000001', 'postgres-primary', 'postgres');

CREATE TABLE storage_policy_assignments (
    id UUID PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('platform', 'tenant', 'domain', 'account')),
    tenant_id UUID,
    domain_id UUID,
    account_id UUID,
    storage_pool_id UUID NOT NULL,
    updated_by TEXT NOT NULL CHECK (btrim(updated_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (scope_kind = 'platform' AND tenant_id IS NULL AND domain_id IS NULL AND account_id IS NULL)
        OR (scope_kind = 'tenant' AND tenant_id IS NOT NULL AND domain_id IS NULL AND account_id IS NULL)
        OR (scope_kind = 'domain' AND tenant_id IS NOT NULL AND domain_id IS NOT NULL AND account_id IS NULL)
        OR (scope_kind = 'account' AND tenant_id IS NOT NULL AND domain_id IS NULL AND account_id IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX storage_policy_platform_idx
    ON storage_policy_assignments (scope_kind)
    WHERE scope_kind = 'platform';

CREATE UNIQUE INDEX storage_policy_tenant_idx
    ON storage_policy_assignments (tenant_id)
    WHERE scope_kind = 'tenant';

CREATE UNIQUE INDEX storage_policy_domain_idx
    ON storage_policy_assignments (tenant_id, domain_id)
    WHERE scope_kind = 'domain';

CREATE UNIQUE INDEX storage_policy_account_idx
    ON storage_policy_assignments (tenant_id, account_id)
    WHERE scope_kind = 'account';

CREATE INDEX storage_policy_pool_idx
    ON storage_policy_assignments (storage_pool_id, scope_kind);

INSERT INTO storage_policy_assignments (id, scope_kind, storage_pool_id, updated_by)
VALUES (
    '00000000-0000-0000-0000-000000000002',
    'platform',
    '00000000-0000-0000-0000-000000000001',
    'schema'
);

CREATE TABLE blobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    blob_kind TEXT NOT NULL CHECK (blob_kind IN ('raw_message', 'mime_part', 'attachment')),
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    blob_bytes BYTEA,
    magika_status TEXT NOT NULL DEFAULT 'not_required'
        CHECK (magika_status IN ('not_required', 'pending', 'valid', 'rejected', 'failed')),
    magika_media_type TEXT,
    magika_confidence NUMERIC(5,4) CHECK (magika_confidence IS NULL OR (magika_confidence >= 0 AND magika_confidence <= 1)),
    extraction_status TEXT NOT NULL DEFAULT 'not_requested'
        CHECK (extraction_status IN ('not_requested', 'queued', 'running', 'succeeded', 'failed', 'unsupported')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    validated_at TIMESTAMPTZ,
    retained_until TIMESTAMPTZ,
    legal_hold BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, id, blob_kind),
    UNIQUE (tenant_id, domain_id, id),
    UNIQUE (tenant_id, domain_id, id, blob_kind),
    UNIQUE (tenant_id, domain_id, blob_kind, content_sha256),
    UNIQUE (tenant_id, domain_id, id, blob_kind, content_sha256, size_octets),
    CHECK (
        (magika_status IN ('not_required', 'pending') AND validated_at IS NULL)
        OR (magika_status IN ('valid', 'rejected', 'failed') AND validated_at IS NOT NULL)
    ),
    CHECK (blob_kind <> 'raw_message' OR blob_bytes IS NOT NULL),
    CHECK (validated_at IS NULL OR validated_at >= created_at),
    CHECK (retained_until IS NULL OR retained_until >= created_at),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX blobs_validation_idx
    ON blobs (tenant_id, magika_status, created_at)
    WHERE magika_status = 'pending';

CREATE INDEX blobs_extraction_idx
    ON blobs (tenant_id, extraction_status, created_at)
    WHERE extraction_status IN ('queued', 'running');

CREATE UNIQUE INDEX blobs_attachment_dedupe_idx
    ON blobs (tenant_id, domain_id, content_sha256)
    WHERE blob_kind = 'attachment';

CREATE INDEX blobs_lifecycle_protection_idx
    ON blobs (tenant_id, retained_until)
    WHERE retained_until IS NOT NULL OR legal_hold = TRUE;

CREATE TABLE blob_placements (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    blob_id UUID NOT NULL,
    blob_kind TEXT NOT NULL CHECK (blob_kind IN ('attachment', 'mime_part')),
    storage_pool_id UUID NOT NULL,
    placement_status TEXT NOT NULL DEFAULT 'active'
        CHECK (placement_status IN (
            'active',
            'copying',
            'verified',
            'retiring',
            'failed',
            'cleaning',
            'cleanup_failed',
            'deleted'
        )),
    verified_content_sha256 TEXT NOT NULL CHECK (verified_content_sha256 ~ '^[0-9a-f]{64}$'),
    verified_size_octets BIGINT NOT NULL CHECK (verified_size_octets >= 0),
    verified_at TIMESTAMPTZ,
    rollback_until TIMESTAMPTZ,
    cleanup_attempts INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_attempts >= 0),
    cleanup_claimed_at TIMESTAMPTZ,
    cleaned_at TIMESTAMPTZ,
    cleanup_error TEXT,
    next_cleanup_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, domain_id, id, blob_id, blob_kind, storage_pool_id),
    CHECK (placement_status IN ('copying', 'failed') OR verified_at IS NOT NULL),
    CHECK (verified_at IS NULL OR verified_at >= created_at),
    CHECK (rollback_until IS NULL OR placement_status IN ('retiring', 'cleaning', 'cleanup_failed', 'deleted')),
    CHECK (cleaned_at IS NULL OR placement_status = 'deleted'),
    CHECK (cleaned_at IS NULL OR cleaned_at >= created_at),
    CHECK (next_cleanup_attempt_at IS NULL OR placement_status = 'cleanup_failed'),
    FOREIGN KEY (
        tenant_id,
        domain_id,
        blob_id,
        blob_kind,
        verified_content_sha256,
        verified_size_octets
    )
        REFERENCES blobs (
            tenant_id,
            domain_id,
            id,
            blob_kind,
            content_sha256,
            size_octets
        )
        ON DELETE CASCADE,
    FOREIGN KEY (storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX blob_placements_active_idx
    ON blob_placements (tenant_id, domain_id, blob_id)
    WHERE placement_status = 'active';

CREATE UNIQUE INDEX blob_placements_live_pool_idx
    ON blob_placements (tenant_id, domain_id, blob_id, storage_pool_id)
    WHERE placement_status IN ('active', 'copying', 'verified', 'retiring');

CREATE INDEX blob_placements_fetch_idx
    ON blob_placements (tenant_id, domain_id, blob_id, blob_kind)
    WHERE placement_status = 'active';

CREATE INDEX blob_placements_status_idx
    ON blob_placements (tenant_id, placement_status, updated_at);

CREATE INDEX blob_placements_pool_status_idx
    ON blob_placements (storage_pool_id, placement_status, updated_at);

CREATE INDEX blob_placements_cleanup_due_idx
    ON blob_placements (rollback_until, updated_at, id)
    WHERE placement_status IN ('retiring', 'cleanup_failed');

CREATE TABLE blob_migration_jobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    blob_id UUID NOT NULL,
    blob_kind TEXT NOT NULL CHECK (blob_kind IN ('attachment', 'mime_part')),
    job_kind TEXT NOT NULL DEFAULT 'placement_migration' CHECK (job_kind = 'placement_migration'),
    source_placement_id UUID NOT NULL,
    source_storage_pool_id UUID NOT NULL,
    target_storage_pool_id UUID NOT NULL,
    target_placement_id UUID,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'verified', 'switched', 'failed', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    started_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    switched_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    rollback_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (source_storage_pool_id <> target_storage_pool_id),
    CHECK (target_placement_id IS NULL OR target_placement_id <> source_placement_id),
    CHECK (started_at IS NULL OR started_at >= created_at),
    CHECK (verified_at IS NULL OR verified_at >= created_at),
    CHECK (switched_at IS NULL OR verified_at IS NOT NULL),
    CHECK (rollback_until IS NULL OR switched_at IS NOT NULL),
    CHECK (status <> 'running' OR (started_at IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK (status NOT IN ('verified', 'switched') OR target_placement_id IS NOT NULL),
    CHECK (status <> 'switched' OR (switched_at IS NOT NULL AND rollback_until IS NOT NULL)),
    CHECK (status <> 'cancelled' OR cancelled_at IS NOT NULL),
    FOREIGN KEY (
        tenant_id,
        domain_id,
        source_placement_id,
        blob_id,
        blob_kind,
        source_storage_pool_id
    )
        REFERENCES blob_placements (
            tenant_id,
            domain_id,
            id,
            blob_id,
            blob_kind,
            storage_pool_id
        )
        ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id,
        domain_id,
        target_placement_id,
        blob_id,
        blob_kind,
        target_storage_pool_id
    )
        REFERENCES blob_placements (
            tenant_id,
            domain_id,
            id,
            blob_id,
            blob_kind,
            storage_pool_id
        )
        ON DELETE RESTRICT,
    FOREIGN KEY (source_storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT,
    FOREIGN KEY (target_storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX blob_migration_jobs_open_target_idx
    ON blob_migration_jobs (tenant_id, domain_id, blob_id, target_storage_pool_id)
    WHERE status IN ('pending', 'running', 'verified');

CREATE INDEX blob_migration_jobs_pending_idx
    ON blob_migration_jobs (next_attempt_at, created_at, id)
    WHERE status = 'pending';

CREATE INDEX blob_migration_jobs_running_lease_idx
    ON blob_migration_jobs (lease_expires_at, started_at)
    WHERE status = 'running';

CREATE INDEX blob_migration_jobs_blob_idx
    ON blob_migration_jobs (tenant_id, domain_id, blob_id, created_at DESC);

CREATE INDEX blob_migration_jobs_source_placement_idx
    ON blob_migration_jobs (tenant_id, source_placement_id);

CREATE INDEX blob_migration_jobs_target_placement_idx
    ON blob_migration_jobs (tenant_id, target_placement_id)
    WHERE target_placement_id IS NOT NULL;

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    blob_id UUID NOT NULL,
    blob_kind TEXT NOT NULL DEFAULT 'raw_message' CHECK (blob_kind = 'raw_message'),
    internet_message_id TEXT,
    message_hash TEXT NOT NULL CHECK (message_hash ~ '^[0-9a-f]{64}$'),
    normalized_subject TEXT NOT NULL DEFAULT '',
    sent_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    has_attachments BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retained_until TIMESTAMPTZ,
    legal_hold BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, id, domain_id),
    CHECK (retained_until IS NULL OR retained_until >= created_at),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)
        REFERENCES blobs (tenant_id, domain_id, id, blob_kind)
        ON DELETE RESTRICT
);

CREATE INDEX messages_tenant_received_idx
    ON messages (tenant_id, received_at DESC);

CREATE INDEX messages_internet_message_idx
    ON messages (tenant_id, internet_message_id)
    WHERE internet_message_id IS NOT NULL;

CREATE INDEX messages_lifecycle_protection_idx
    ON messages (tenant_id, retained_until)
    WHERE retained_until IS NOT NULL OR legal_hold = TRUE;

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

CREATE TABLE message_recipients (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('from', 'sender', 'reply_to', 'to', 'cc')),
    address TEXT NOT NULL CHECK (address = lower(btrim(address)) AND address <> ''),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    UNIQUE (tenant_id, message_id, role, ordinal),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX message_recipients_message_idx
    ON message_recipients (tenant_id, message_id, role, ordinal);

CREATE INDEX message_recipients_address_idx
    ON message_recipients (tenant_id, address);

CREATE TABLE protected_bcc_recipients (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    address TEXT NOT NULL CHECK (address = lower(btrim(address)) AND address <> ''),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    metadata_scope TEXT NOT NULL DEFAULT 'audit-compliance' CHECK (metadata_scope = 'audit-compliance'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, message_id, ordinal),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX protected_bcc_recipients_message_idx
    ON protected_bcc_recipients (tenant_id, message_id, ordinal);

CREATE TABLE mime_parts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    message_id UUID NOT NULL,
    domain_id UUID NOT NULL,
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
    blob_id UUID,
    blob_kind TEXT CHECK (blob_kind IS NULL OR blob_kind IN ('mime_part', 'attachment')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, message_id, id),
    UNIQUE (tenant_id, message_id, domain_id, id, blob_id, blob_kind),
    UNIQUE (tenant_id, message_id, part_path),
    CHECK ((blob_id IS NULL AND blob_kind IS NULL) OR (blob_id IS NOT NULL AND blob_kind IS NOT NULL)),
    FOREIGN KEY (tenant_id, message_id, domain_id) REFERENCES messages (tenant_id, id, domain_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id, parent_part_id)
        REFERENCES mime_parts (tenant_id, message_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)
        REFERENCES blobs (tenant_id, domain_id, id, blob_kind)
        ON DELETE RESTRICT
);

CREATE INDEX mime_parts_message_idx
    ON mime_parts (tenant_id, message_id, ordinal);

CREATE TABLE message_bodies (
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
        REFERENCES mime_parts (tenant_id, message_id, id)
        ON DELETE CASCADE
);

CREATE INDEX message_bodies_search_idx
    ON message_bodies USING GIN (search_vector);

CREATE TABLE mailbox_messages (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    mailbox_id UUID NOT NULL,
    message_id UUID NOT NULL,
    thread_id UUID,
    imap_uid BIGINT NOT NULL CHECK (imap_uid > 0),
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    is_seen BOOLEAN NOT NULL DEFAULT FALSE,
    is_flagged BOOLEAN NOT NULL DEFAULT FALSE,
    is_answered BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    is_draft BOOLEAN NOT NULL DEFAULT FALSE,
    followup_flag_status TEXT NOT NULL DEFAULT 'none'
        CHECK (followup_flag_status IN ('none', 'flagged', 'complete')),
    followup_icon INTEGER NOT NULL DEFAULT 0 CHECK (followup_icon >= 0),
    todo_item_flags INTEGER NOT NULL DEFAULT 0 CHECK (todo_item_flags >= 0),
    followup_request TEXT NOT NULL DEFAULT '',
    followup_start_at TIMESTAMPTZ,
    followup_due_at TIMESTAMPTZ,
    followup_completed_at TIMESTAMPTZ,
    reminder_set BOOLEAN NOT NULL DEFAULT FALSE,
    reminder_at TIMESTAMPTZ,
    reminder_dismissed_at TIMESTAMPTZ,
    swapped_todo_store_id UUID,
    swapped_todo_data BYTEA,
    keywords TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    visibility TEXT NOT NULL DEFAULT 'visible' CHECK (visibility IN ('visible', 'hidden', 'expunged')),
    received_at TIMESTAMPTZ NOT NULL,
    snoozed_until TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    expunged_at TIMESTAMPTZ,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, id),
    UNIQUE (tenant_id, account_id, id, message_id),
    UNIQUE (tenant_id, mailbox_id, imap_uid),
    CHECK ((NOT is_deleted AND deleted_at IS NULL) OR (is_deleted AND deleted_at IS NOT NULL)),
    CHECK ((visibility <> 'expunged' AND expunged_at IS NULL) OR (visibility = 'expunged' AND expunged_at IS NOT NULL)),
    CHECK (deleted_at IS NULL OR deleted_at >= added_at),
    CHECK (expunged_at IS NULL OR expunged_at >= added_at),
    FOREIGN KEY (tenant_id, account_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX mailbox_messages_live_message_idx
    ON mailbox_messages (tenant_id, account_id, mailbox_id, message_id)
    WHERE visibility <> 'expunged';

CREATE INDEX mailbox_messages_uid_idx
    ON mailbox_messages (tenant_id, account_id, mailbox_id, imap_uid);

CREATE INDEX mailbox_messages_visible_uid_idx
    ON mailbox_messages (tenant_id, account_id, mailbox_id, imap_uid)
    WHERE visibility = 'visible';

CREATE INDEX mailbox_messages_visible_account_message_idx
    ON mailbox_messages (tenant_id, account_id, message_id, mailbox_id)
    WHERE visibility = 'visible';

CREATE INDEX mailbox_messages_modseq_idx
    ON mailbox_messages (tenant_id, account_id, mailbox_id, modseq);

CREATE INDEX mailbox_messages_deleted_idx
    ON mailbox_messages (tenant_id, account_id, mailbox_id, imap_uid)
    WHERE is_deleted = TRUE;

CREATE TABLE recoverable_items (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, source_mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, source_mailbox_message_id, message_id)
        REFERENCES mailbox_messages (tenant_id, account_id, id, message_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, restored_mailbox_message_id)
        REFERENCES mailbox_messages (tenant_id, account_id, id)
        ON DELETE SET NULL
);

CREATE INDEX recoverable_items_active_folder_idx
    ON recoverable_items (tenant_id, account_id, recoverable_folder, deleted_at DESC)
    WHERE status = 'active';

CREATE INDEX recoverable_items_cleanup_idx
    ON recoverable_items (tenant_id, retained_until, deleted_at)
    WHERE status = 'active' AND legal_hold = FALSE;

CREATE INDEX recoverable_items_message_idx
    ON recoverable_items (tenant_id, message_id);

CREATE TABLE retention_policy_tags (
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
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE INDEX retention_policy_tags_tenant_idx
    ON retention_policy_tags (tenant_id, lifecycle_state, is_visible, display_name, id);

CREATE UNIQUE INDEX retention_policy_tags_default_type_idx
    ON retention_policy_tags (tenant_id, tag_type)
    WHERE is_default = TRUE AND lifecycle_state = 'active';

CREATE TABLE account_retention_policy_assignments (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    default_tag_id UUID,
    policy_name TEXT NOT NULL DEFAULT '',
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by_account_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, default_tag_id) REFERENCES retention_policy_tags (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, assigned_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE mailbox_pst_jobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    mailbox_id UUID NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('import', 'export')),
    server_path TEXT NOT NULL CHECK (btrim(server_path) <> ''),
    status TEXT NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested', 'running', 'completed', 'failed')),
    requested_by TEXT NOT NULL CHECK (btrim(requested_by) <> ''),
    processed_messages INTEGER NOT NULL DEFAULT 0 CHECK (processed_messages >= 0),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    CHECK (
        (status IN ('requested', 'running') AND completed_at IS NULL)
        OR (status IN ('completed', 'failed') AND completed_at IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, account_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX mailbox_pst_jobs_pending_idx
    ON mailbox_pst_jobs (tenant_id, created_at, id)
    WHERE status IN ('requested', 'failed');

CREATE INDEX mailbox_pst_jobs_mailbox_idx
    ON mailbox_pst_jobs (tenant_id, account_id, mailbox_id, created_at DESC);

CREATE TABLE mailbox_item_transfer_jobs (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, requested_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX mailbox_item_transfer_jobs_account_idx
    ON mailbox_item_transfer_jobs (tenant_id, account_id, direction, created_at DESC, id);

CREATE INDEX mailbox_item_transfer_jobs_status_idx
    ON mailbox_item_transfer_jobs (tenant_id, status, updated_at, id);

CREATE UNIQUE INDEX mailbox_item_transfer_jobs_idempotency_idx
    ON mailbox_item_transfer_jobs (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE mailbox_item_transfer_entries (
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
    FOREIGN KEY (tenant_id, job_id) REFERENCES mailbox_item_transfer_jobs (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mailbox_item_transfer_entries_job_idx
    ON mailbox_item_transfer_entries (tenant_id, job_id, status, ordinal);

CREATE TABLE attachments (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    mailbox_message_id UUID,
    message_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    mime_part_id UUID,
    blob_id UUID NOT NULL,
    blob_kind TEXT NOT NULL DEFAULT 'attachment' CHECK (blob_kind = 'attachment'),
    file_name TEXT NOT NULL CHECK (btrim(file_name) <> ''),
    disposition TEXT NOT NULL DEFAULT 'attachment' CHECK (disposition IN ('attachment', 'inline')),
    content_id TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, message_id, mime_part_id),
    CHECK (mailbox_message_id IS NOT NULL OR mime_part_id IS NOT NULL),
    FOREIGN KEY (tenant_id, account_id, mailbox_message_id, message_id)
        REFERENCES mailbox_messages (tenant_id, account_id, id, message_id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id, domain_id) REFERENCES messages (tenant_id, id, domain_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id, domain_id, mime_part_id, blob_id, blob_kind)
        REFERENCES mime_parts (tenant_id, message_id, domain_id, id, blob_id, blob_kind)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)
        REFERENCES blobs (tenant_id, domain_id, id, blob_kind)
        ON DELETE RESTRICT
);

CREATE INDEX attachments_mailbox_message_idx
    ON attachments (tenant_id, account_id, mailbox_message_id, ordinal);

CREATE INDEX attachments_message_idx
    ON attachments (tenant_id, message_id, ordinal);

CREATE INDEX attachments_blob_idx
    ON attachments (tenant_id, blob_id);

CREATE TABLE attachment_extraction_jobs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    blob_id UUID NOT NULL,
    blob_kind TEXT NOT NULL DEFAULT 'attachment' CHECK (blob_kind = 'attachment'),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'unsupported')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    CHECK (started_at IS NULL OR started_at >= created_at),
    CHECK (completed_at IS NULL OR started_at IS NOT NULL),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK (
        (status = 'queued' AND started_at IS NULL AND completed_at IS NULL)
        OR (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (status IN ('succeeded', 'failed', 'unsupported') AND completed_at IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, blob_id, blob_kind)
        REFERENCES blobs (tenant_id, id, blob_kind)
        ON DELETE CASCADE
);

CREATE INDEX attachment_extraction_jobs_pending_idx
    ON attachment_extraction_jobs (tenant_id, status, next_attempt_at);

CREATE INDEX attachment_extraction_jobs_blob_idx
    ON attachment_extraction_jobs (tenant_id, blob_id);

CREATE TABLE attachment_texts (
    tenant_id UUID NOT NULL,
    blob_id UUID NOT NULL,
    blob_kind TEXT NOT NULL DEFAULT 'attachment' CHECK (blob_kind = 'attachment'),
    extracted_text TEXT NOT NULL,
    language_code TEXT,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    search_vector TSVECTOR NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, blob_id),
    FOREIGN KEY (tenant_id, blob_id, blob_kind)
        REFERENCES blobs (tenant_id, id, blob_kind)
        ON DELETE CASCADE
);

CREATE INDEX attachment_texts_search_idx
    ON attachment_texts USING GIN (search_vector);

CREATE TABLE mail_search_documents (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    mailbox_message_id UUID NOT NULL,
    message_id UUID NOT NULL,
    subject_text TEXT NOT NULL DEFAULT '',
    participants_visible TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    attachment_text TEXT NOT NULL DEFAULT '',
    search_vector TSVECTOR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, mailbox_message_id),
    FOREIGN KEY (tenant_id, account_id, mailbox_message_id, message_id)
        REFERENCES mailbox_messages (tenant_id, account_id, id, message_id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mail_search_documents_search_idx
    ON mail_search_documents USING GIN (search_vector);

CREATE INDEX mail_search_documents_updated_idx
    ON mail_search_documents (tenant_id, updated_at DESC);

CREATE INDEX mail_search_documents_account_message_idx
    ON mail_search_documents (account_id, message_id, mailbox_message_id);

CREATE TABLE compliance_cases (
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
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX compliance_cases_tenant_status_idx
    ON compliance_cases (tenant_id, status, updated_at DESC, id);

CREATE TABLE compliance_holds (
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
    FOREIGN KEY (tenant_id, case_id) REFERENCES compliance_cases (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, released_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX compliance_holds_tenant_status_idx
    ON compliance_holds (tenant_id, status, updated_at DESC, id);

CREATE TABLE compliance_hold_mailboxes (
    tenant_id UUID NOT NULL,
    hold_id UUID NOT NULL,
    account_id UUID NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by_account_id UUID NOT NULL,
    released_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, hold_id, account_id),
    CHECK (released_at IS NULL OR released_at >= applied_at),
    FOREIGN KEY (tenant_id, hold_id) REFERENCES compliance_holds (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, applied_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX compliance_hold_mailboxes_account_idx
    ON compliance_hold_mailboxes (tenant_id, account_id, released_at, hold_id);

CREATE TABLE discovery_searches (
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
    FOREIGN KEY (tenant_id, case_id) REFERENCES compliance_cases (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX discovery_searches_case_idx
    ON discovery_searches (tenant_id, case_id, updated_at DESC, id);

CREATE TABLE discovery_search_jobs (
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
    FOREIGN KEY (tenant_id, search_id) REFERENCES discovery_searches (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX discovery_search_jobs_status_idx
    ON discovery_search_jobs (tenant_id, status, updated_at, id);

CREATE TABLE discovery_result_items (
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
    FOREIGN KEY (tenant_id, job_id) REFERENCES discovery_search_jobs (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, mailbox_message_id, message_id)
        REFERENCES mailbox_messages (tenant_id, account_id, id, message_id)
        ON DELETE RESTRICT
);

CREATE INDEX discovery_result_items_job_idx
    ON discovery_result_items (tenant_id, job_id, rank, id);

CREATE INDEX discovery_result_items_message_idx
    ON discovery_result_items (tenant_id, message_id);

CREATE TABLE non_indexable_item_reports (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, attachment_id) REFERENCES attachments (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX non_indexable_item_reports_account_idx
    ON non_indexable_item_reports (tenant_id, account_id, detected_at DESC, id);

CREATE INDEX non_indexable_item_reports_open_idx
    ON non_indexable_item_reports (tenant_id, report_kind, detected_at DESC, id)
    WHERE resolved_at IS NULL;

CREATE TABLE mail_change_log (
    cursor BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID,
    mailbox_id UUID,
    collection_id UUID,
    object_kind TEXT NOT NULL CHECK (object_kind IN (
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
        'recoverable_item',
        'public_folder_tree',
        'public_folder',
        'public_folder_item',
        'public_folder_permission',
        'public_folder_replica',
        'public_folder_per_user_state'
    )),
    object_id UUID NOT NULL,
    object_uid TEXT,
    change_kind TEXT NOT NULL CHECK (change_kind IN ('created', 'updated', 'destroyed', 'moved', 'expunged')),
    modseq BIGINT NOT NULL CHECK (modseq > 0),
    affected_principal_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retained_until TIMESTAMPTZ,
    UNIQUE (tenant_id, cursor),
    UNIQUE (tenant_id, cursor, object_kind, object_id),
    CHECK (mailbox_id IS NULL OR account_id IS NOT NULL),
    CHECK (jsonb_typeof(summary_json) = 'object'),
    CHECK (array_position(affected_principal_ids, NULL) IS NULL),
    CHECK (
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
    ),
    CHECK (retained_until IS NULL OR retained_until >= created_at),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mail_change_log_account_idx
    ON mail_change_log (tenant_id, account_id, object_kind, cursor);

CREATE INDEX mail_change_log_account_cursor_idx
    ON mail_change_log (tenant_id, account_id, cursor)
    WHERE account_id IS NOT NULL;

CREATE INDEX mail_change_log_mailbox_idx
    ON mail_change_log (tenant_id, mailbox_id, cursor)
    WHERE mailbox_id IS NOT NULL;

CREATE INDEX mail_change_log_collaboration_idx
    ON mail_change_log (tenant_id, account_id, collection_id, object_kind, cursor)
    WHERE object_kind IN (
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
    );

CREATE INDEX mail_change_log_recoverable_item_idx
    ON mail_change_log (tenant_id, account_id, object_kind, cursor)
    WHERE object_kind = 'recoverable_item';

CREATE INDEX mail_change_log_principals_gin_idx
    ON mail_change_log USING GIN (affected_principal_ids);

CREATE INDEX mail_change_log_retention_idx
    ON mail_change_log (tenant_id, retained_until)
    WHERE retained_until IS NOT NULL;

CREATE OR REPLACE FUNCTION prevent_append_only_update()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only and cannot be updated', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER mail_change_log_append_only_update_guard
    BEFORE UPDATE ON mail_change_log
    FOR EACH ROW
    EXECUTE FUNCTION prevent_append_only_update();

CREATE TABLE tombstones (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID,
    mailbox_id UUID,
    collection_id UUID,
    object_kind TEXT NOT NULL CHECK (object_kind IN (
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
    )),
    object_id UUID NOT NULL,
    object_uid TEXT,
    message_id UUID,
    mailbox_message_id UUID,
    imap_uid BIGINT CHECK (imap_uid IS NULL OR imap_uid > 0),
    deleted_modseq BIGINT NOT NULL CHECK (deleted_modseq > 0),
    change_cursor BIGINT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('delete', 'expunge', 'destroyed', 'move', 'purge')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retained_until TIMESTAMPTZ,
    UNIQUE (tenant_id, id),
    CHECK (mailbox_id IS NULL OR account_id IS NOT NULL),
    CHECK (mailbox_message_id IS NULL OR account_id IS NOT NULL),
    CHECK (
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
                'sieve_script'
            )
            AND account_id IS NOT NULL
            AND mailbox_id IS NULL
            AND message_id IS NULL
            AND mailbox_message_id IS NULL
            AND imap_uid IS NULL
        )
        OR (
            object_kind IN (
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
    ),
    CHECK (retained_until IS NULL OR retained_until >= created_at),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, change_cursor) REFERENCES mail_change_log (tenant_id, cursor) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, change_cursor, object_kind, object_id)
        REFERENCES mail_change_log (tenant_id, cursor, object_kind, object_id)
        ON DELETE RESTRICT
);

CREATE INDEX tombstones_account_idx
    ON tombstones (tenant_id, account_id, object_kind, change_cursor);

CREATE INDEX tombstones_mailbox_idx
    ON tombstones (tenant_id, account_id, mailbox_id, change_cursor)
    WHERE object_kind = 'mailbox';

CREATE INDEX tombstones_mailbox_uid_idx
    ON tombstones (tenant_id, mailbox_id, imap_uid)
    WHERE mailbox_id IS NOT NULL AND imap_uid IS NOT NULL;

CREATE INDEX tombstones_collaboration_idx
    ON tombstones (tenant_id, account_id, collection_id, object_kind, change_cursor)
    WHERE object_kind IN (
        'contact_book',
        'contact',
        'calendar',
        'calendar_event',
        'task_list',
        'task',
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
    );

CREATE INDEX tombstones_retention_idx
    ON tombstones (tenant_id, retained_until)
    WHERE retained_until IS NOT NULL;

CREATE TRIGGER tombstones_append_only_update_guard
    BEFORE UPDATE ON tombstones
    FOR EACH ROW
    EXECUTE FUNCTION prevent_append_only_update();

CREATE TABLE canonical_change_journal (
    sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id UUID NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'notes', 'journal', 'rights', 'search', 'rules', 'conversation_actions', 'public_folders')),
    principal_account_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
    account_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, sequence),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE INDEX canonical_change_journal_principals_gin_idx
    ON canonical_change_journal USING GIN (principal_account_ids);

CREATE INDEX canonical_change_journal_replay_idx
    ON canonical_change_journal (tenant_id, category, sequence);

CREATE INDEX canonical_change_journal_retention_idx
    ON canonical_change_journal (tenant_id, created_at);

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
    CHECK (consumed_at IS NULL OR consumed_at >= created_at),
    CHECK (consumed_at IS NULL OR consumed_at <= expires_at),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX jmap_upload_blobs_expiry_idx
    ON jmap_upload_blobs (tenant_id, expires_at);

CREATE INDEX jmap_upload_blobs_unconsumed_expiry_idx
    ON jmap_upload_blobs (tenant_id, expires_at)
    WHERE consumed_at IS NULL;

CREATE TABLE jmap_query_states (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    method_name TEXT NOT NULL CHECK (btrim(method_name) <> ''),
    filter_hash TEXT NOT NULL CHECK (btrim(filter_hash) <> ''),
    sort_hash TEXT NOT NULL CHECK (btrim(sort_hash) <> ''),
    last_change_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_change_sequence >= 0),
    snapshot_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(snapshot_ids) = 'array'),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (expires_at > created_at),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX jmap_query_states_account_idx
    ON jmap_query_states (tenant_id, account_id, method_name, expires_at);

CREATE INDEX jmap_query_states_expiry_idx
    ON jmap_query_states (tenant_id, expires_at);

CREATE TABLE activesync_devices (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    device_id TEXT NOT NULL CHECK (btrim(device_id) <> ''),
    device_type TEXT NOT NULL DEFAULT 'unknown' CHECK (btrim(device_type) <> ''),
    policy_key TEXT CHECK (policy_key IS NULL OR (btrim(policy_key) <> '' AND length(policy_key) <= 64)),
    pending_policy_key TEXT CHECK (pending_policy_key IS NULL OR (btrim(pending_policy_key) <> '' AND length(pending_policy_key) <= 64)),
    provision_status TEXT NOT NULL DEFAULT 'pending' CHECK (provision_status IN ('pending', 'active', 'blocked')),
    wipe_status TEXT NOT NULL DEFAULT 'none' CHECK (wipe_status IN ('none', 'pending', 'acknowledged')),
    account_wipe_status TEXT NOT NULL DEFAULT 'none' CHECK (account_wipe_status IN ('none', 'pending', 'acknowledged')),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, account_id, device_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX activesync_devices_account_idx
    ON activesync_devices (tenant_id, account_id, last_seen_at DESC);

CREATE TABLE activesync_sync_cursors (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    device_id TEXT NOT NULL CHECK (btrim(device_id) <> ''),
    collection_kind TEXT NOT NULL CHECK (collection_kind IN ('folders', 'mail', 'contacts', 'calendar', 'tasks')),
    collection_key TEXT NOT NULL CHECK (btrim(collection_key) <> ''),
    sync_key TEXT NOT NULL CHECK (btrim(sync_key) <> ''),
    last_change_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_change_sequence >= 0),
    state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > created_at),
    CHECK (jsonb_typeof(state_json) IN ('object', 'array')),
    UNIQUE (tenant_id, account_id, device_id, collection_kind, collection_key),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX activesync_sync_cursors_account_idx
    ON activesync_sync_cursors (tenant_id, account_id, device_id, updated_at DESC);

CREATE INDEX activesync_sync_cursors_expiry_idx
    ON activesync_sync_cursors (tenant_id, expires_at);

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
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > created_at),
    CHECK (jsonb_typeof(cursor_json) = 'object'),
    CHECK (
        (checkpoint_kind = 'hierarchy' AND mailbox_id IS NULL)
        OR (checkpoint_kind IN ('content', 'read_state') AND mailbox_id IS NOT NULL)
    ),
    UNIQUE (tenant_id, account_id, mailbox_id, checkpoint_kind, mapi_replica_guid),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mapi_sync_checkpoints_account_idx
    ON mapi_sync_checkpoints (tenant_id, account_id, updated_at DESC);

CREATE INDEX mapi_sync_checkpoints_expiry_idx
    ON mapi_sync_checkpoints (tenant_id, expires_at);

CREATE UNIQUE INDEX mapi_sync_checkpoints_hierarchy_idx
    ON mapi_sync_checkpoints (tenant_id, account_id, checkpoint_kind, mapi_replica_guid)
    WHERE mailbox_id IS NULL;

CREATE TABLE mapi_profile_settings (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    ipm_subtree_ost_id BYTEA CHECK (ipm_subtree_ost_id IS NULL OR (octet_length(ipm_subtree_ost_id) > 0 AND octet_length(ipm_subtree_ost_id) <= 1024)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE mapi_mailbox_replicas (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    replica_guid UUID NOT NULL,
    next_global_counter BIGINT NOT NULL DEFAULT 21 CHECK (next_global_counter > 20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id),
    UNIQUE (tenant_id, account_id, replica_guid),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE mapi_object_identities (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('account', 'mailbox', 'message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'search_folder_definition', 'conversation_action', 'navigation_shortcut', 'delegate_freebusy_message')),
    canonical_id UUID NOT NULL,
    mapi_global_counter BIGINT NOT NULL CHECK (mapi_global_counter > 0 AND mapi_global_counter <= 140737488355327),
    mapi_object_id BIGINT NOT NULL CHECK ((mapi_object_id & 65535) = 1),
    source_key BYTEA NOT NULL CHECK (octet_length(source_key) = 22),
    change_key BYTEA NOT NULL CHECK (octet_length(change_key) = 22),
    instance_key BYTEA NOT NULL CHECK (octet_length(instance_key) = 22),
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, object_kind, canonical_id),
    UNIQUE (tenant_id, account_id, mapi_global_counter),
    UNIQUE (tenant_id, account_id, mapi_object_id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mapi_object_identities_lookup_idx
    ON mapi_object_identities (tenant_id, account_id, mapi_object_id);

CREATE INDEX mapi_object_identities_source_key_idx
    ON mapi_object_identities (tenant_id, account_id, source_key);

CREATE TABLE mapi_named_properties (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX mapi_named_properties_lid_idx
    ON mapi_named_properties (tenant_id, account_id, property_guid, property_lid)
    WHERE property_kind = 'lid';

CREATE UNIQUE INDEX mapi_named_properties_name_idx
    ON mapi_named_properties (tenant_id, account_id, property_guid, property_name)
    WHERE property_kind = 'name';

CREATE TABLE mapi_custom_property_values (
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'attachment')),
    canonical_id UUID NOT NULL,
    property_tag BIGINT NOT NULL CHECK (property_tag >= 0 AND property_tag <= 4294967295),
    property_type INTEGER NOT NULL CHECK (property_type >= 0 AND property_type <= 65535),
    property_value BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, object_kind, canonical_id, property_tag, property_type),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mapi_custom_property_values_object_idx
    ON mapi_custom_property_values (tenant_id, account_id, object_kind, canonical_id);

CREATE TABLE mapi_navigation_shortcuts (
    tenant_id UUID NOT NULL,
    id UUID NOT NULL,
    account_id UUID NOT NULL,
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    target_folder_id BIGINT CHECK (target_folder_id IS NULL OR target_folder_id > 0),
    shortcut_type BIGINT NOT NULL CHECK (shortcut_type >= 0 AND shortcut_type <= 4294967295),
    flags BIGINT NOT NULL DEFAULT 0 CHECK (flags >= 0 AND flags <= 4294967295),
    section BIGINT NOT NULL DEFAULT 0 CHECK (section >= 0 AND section <= 4294967295),
    ordinal BIGINT NOT NULL DEFAULT 0 CHECK (ordinal >= 0 AND ordinal <= 4294967295),
    group_header_id UUID,
    group_name TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mapi_navigation_shortcuts_account_idx
    ON mapi_navigation_shortcuts (tenant_id, account_id, section, ordinal, subject, id);

CREATE TABLE submission_queue (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    account_id UUID NOT NULL,
    identity_id UUID,
    sent_mailbox_message_id UUID NOT NULL,
    from_address TEXT NOT NULL CHECK (from_address = lower(btrim(from_address)) AND from_address <> ''),
    sender_address TEXT CHECK (sender_address IS NULL OR (sender_address = lower(btrim(sender_address)) AND sender_address <> '')),
    authorization_kind TEXT NOT NULL DEFAULT 'self'
        CHECK (authorization_kind IN ('self', 'send_as', 'send_on_behalf')),
    source_protocol TEXT NOT NULL CHECK (source_protocol IN ('web', 'jmap', 'ews', 'mapi', 'activesync', 'lpe_ct_submission')),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'ready', 'handed_off', 'deferred', 'relayed', 'bounced', 'failed', 'cancelled')),
    transport TEXT NOT NULL DEFAULT 'lpe-ct-smtp' CHECK (transport = 'lpe-ct-smtp'),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    last_trace_id TEXT,
    remote_message_ref TEXT,
    last_error TEXT,
    terminal_at TIMESTAMPTZ,
    idempotency_key TEXT CHECK (idempotency_key IS NULL OR btrim(idempotency_key) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, id, account_id, sent_mailbox_message_id),
    CHECK ((attempts = 0 AND last_attempt_at IS NULL) OR attempts > 0),
    CHECK (last_attempt_at IS NULL OR last_attempt_at >= created_at),
    CHECK (
        (status IN ('queued', 'ready', 'handed_off', 'deferred') AND terminal_at IS NULL)
        OR (status IN ('relayed', 'bounced', 'failed', 'cancelled') AND terminal_at IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, identity_id)
        REFERENCES account_identities (tenant_id, account_id, id)
        ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, account_id, sent_mailbox_message_id)
        REFERENCES mailbox_messages (tenant_id, account_id, id)
        ON DELETE RESTRICT
);

CREATE INDEX submission_queue_status_idx
    ON submission_queue (tenant_id, status, next_attempt_at);

CREATE INDEX submission_queue_worker_due_idx
    ON submission_queue (next_attempt_at, created_at, id)
    WHERE status IN ('queued', 'ready', 'deferred');

CREATE INDEX submission_queue_trace_idx
    ON submission_queue (tenant_id, last_trace_id)
    WHERE last_trace_id IS NOT NULL;

CREATE UNIQUE INDEX submission_queue_idempotency_idx
    ON submission_queue (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE OR REPLACE FUNCTION prevent_submission_queue_terminal_regression()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status IN ('relayed', 'bounced', 'failed', 'cancelled')
       AND NEW.status <> OLD.status THEN
        RAISE EXCEPTION 'submission queue terminal status cannot change from % to %', OLD.status, NEW.status;
    END IF;

    IF OLD.terminal_at IS NOT NULL AND NEW.terminal_at IS NULL THEN
        RAISE EXCEPTION 'submission queue terminal timestamp cannot be cleared';
    END IF;

    IF OLD.terminal_at IS NOT NULL AND NEW.terminal_at <> OLD.terminal_at THEN
        RAISE EXCEPTION 'submission queue terminal timestamp cannot be changed';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER submission_queue_terminal_regression_guard
    BEFORE UPDATE ON submission_queue
    FOR EACH ROW
    EXECUTE FUNCTION prevent_submission_queue_terminal_regression();

CREATE TABLE submission_recipients (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    submission_queue_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('to', 'cc', 'bcc')),
    address TEXT NOT NULL CHECK (address = lower(btrim(address)) AND address <> ''),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    protected_metadata BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (tenant_id, submission_queue_id, role, ordinal),
    CHECK ((role = 'bcc' AND protected_metadata = TRUE) OR (role <> 'bcc' AND protected_metadata = FALSE)),
    FOREIGN KEY (tenant_id, submission_queue_id) REFERENCES submission_queue (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX submission_recipients_submission_idx
    ON submission_recipients (tenant_id, submission_queue_id, ordinal);

CREATE TABLE submission_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    submission_queue_id UUID NOT NULL,
    trace_id TEXT NOT NULL CHECK (btrim(trace_id) <> ''),
    event_kind TEXT NOT NULL
        CHECK (event_kind IN ('created', 'accepted', 'duplicate', 'handed_off', 'deferred', 'relayed', 'bounced', 'failed', 'cancelled')),
    remote_message_ref TEXT,
    dsn_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    technical_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    route_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    throttle_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, submission_queue_id, trace_id, event_kind),
    FOREIGN KEY (tenant_id, submission_queue_id) REFERENCES submission_queue (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX submission_events_queue_idx
    ON submission_events (tenant_id, submission_queue_id, received_at DESC);

CREATE INDEX submission_events_trace_idx
    ON submission_events (tenant_id, trace_id, received_at DESC);

CREATE TRIGGER submission_events_append_only_update_guard
    BEFORE UPDATE ON submission_events
    FOR EACH ROW
    EXECUTE FUNCTION prevent_append_only_update();

CREATE TABLE lpe_ct_inbound_delivery_receipts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    trace_id TEXT NOT NULL CHECK (btrim(trace_id) <> ''),
    recipient_account_id UUID NOT NULL,
    mailbox_message_id UUID,
    status TEXT NOT NULL CHECK (status IN ('delivered', 'duplicate', 'rejected')),
    response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, trace_id, recipient_account_id),
    CHECK (
        (status IN ('delivered', 'duplicate') AND mailbox_message_id IS NOT NULL)
        OR (status = 'rejected' AND mailbox_message_id IS NULL)
    ),
    FOREIGN KEY (tenant_id, recipient_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, recipient_account_id, mailbox_message_id)
        REFERENCES mailbox_messages (tenant_id, account_id, id)
        ON DELETE RESTRICT
);

CREATE INDEX lpe_ct_inbound_delivery_receipts_created_idx
    ON lpe_ct_inbound_delivery_receipts (tenant_id, created_at DESC);

CREATE TABLE lpe_ct_transport_trace_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    trace_id TEXT NOT NULL CHECK (btrim(trace_id) <> ''),
    submission_queue_id UUID,
    recipient_address TEXT CHECK (recipient_address IS NULL OR btrim(recipient_address) <> ''),
    event_kind TEXT NOT NULL CHECK (event_kind IN (
        'accepted',
        'queued',
        'deferred',
        'relayed',
        'bounced',
        'failed',
        'quarantined',
        'released',
        'delivered',
        'duplicate',
        'rejected'
    )),
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
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, submission_queue_id) REFERENCES submission_queue (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX lpe_ct_transport_trace_events_trace_idx
    ON lpe_ct_transport_trace_events (tenant_id, trace_id, occurred_at DESC, id);

CREATE INDEX lpe_ct_transport_trace_events_submission_idx
    ON lpe_ct_transport_trace_events (tenant_id, submission_queue_id, occurred_at DESC, id)
    WHERE submission_queue_id IS NOT NULL;

CREATE TRIGGER lpe_ct_transport_trace_events_append_only_update_guard
    BEFORE UPDATE ON lpe_ct_transport_trace_events
    FOR EACH ROW
    EXECUTE FUNCTION prevent_append_only_update();

CREATE TABLE contact_books (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    normalized_display_name TEXT GENERATED ALWAYS AS (lower(display_name)) STORED,
    role TEXT NOT NULL DEFAULT 'contacts' CHECK (role IN (
        'contacts', 'suggested_contacts', 'quick_contacts', 'im_contact_list',
        'directory', 'custom'
    )),
    sync_modseq BIGINT NOT NULL DEFAULT 1 CHECK (sync_modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, id),
    UNIQUE (tenant_id, owner_account_id, normalized_display_name),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX contact_books_owner_role_idx
    ON contact_books (tenant_id, owner_account_id, role)
    WHERE role <> 'custom';

CREATE TABLE contacts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    contact_book_id UUID NOT NULL,
    uid TEXT NOT NULL CHECK (btrim(uid) <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    name_prefix TEXT NOT NULL DEFAULT '',
    given_name TEXT NOT NULL DEFAULT '',
    middle_name TEXT NOT NULL DEFAULT '',
    family_name TEXT NOT NULL DEFAULT '',
    name_suffix TEXT NOT NULL DEFAULT '',
    nickname TEXT NOT NULL DEFAULT '',
    phonetic_given_name TEXT NOT NULL DEFAULT '',
    phonetic_family_name TEXT NOT NULL DEFAULT '',
    job_title TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT '',
    organization_name TEXT NOT NULL DEFAULT '',
    organization_unit TEXT NOT NULL DEFAULT '',
    emails_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    phones_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    addresses_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    urls_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    notes TEXT NOT NULL DEFAULT '',
    raw_vcard TEXT,
    import_source TEXT NOT NULL DEFAULT 'local' CHECK (import_source IN ('local', 'jmap', 'dav', 'ews', 'mapi', 'activesync', 'import')),
    source_uid TEXT,
    source_etag TEXT,
    source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, contact_book_id, uid),
    CHECK (jsonb_typeof(emails_json) = 'array'),
    CHECK (jsonb_typeof(phones_json) = 'array'),
    CHECK (jsonb_typeof(addresses_json) = 'array'),
    CHECK (jsonb_typeof(urls_json) = 'array'),
    CHECK (jsonb_typeof(source_payload_json) = 'object'),
    FOREIGN KEY (tenant_id, owner_account_id, contact_book_id)
        REFERENCES contact_books (tenant_id, owner_account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX contacts_owner_name_idx
    ON contacts (tenant_id, owner_account_id, display_name);

CREATE TABLE contact_groups (
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
        REFERENCES contact_books (tenant_id, owner_account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX contact_groups_owner_idx
    ON contact_groups (tenant_id, owner_account_id, contact_book_id, group_kind, display_name);

CREATE TABLE contact_group_members (
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
        REFERENCES contact_groups (tenant_id, owner_account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, contact_id)
        REFERENCES contacts (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX contact_group_members_contact_idx
    ON contact_group_members (tenant_id, owner_account_id, contact_group_id, contact_id)
    WHERE member_kind = 'contact';

CREATE UNIQUE INDEX contact_group_members_account_idx
    ON contact_group_members (tenant_id, owner_account_id, contact_group_id, account_id)
    WHERE member_kind = 'account';

CREATE UNIQUE INDEX contact_group_members_external_idx
    ON contact_group_members (tenant_id, owner_account_id, contact_group_id, member_kind, lower(external_address))
    WHERE member_kind IN ('distribution_group', 'tel_uri');

CREATE TABLE calendars (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    normalized_display_name TEXT GENERATED ALWAYS AS (lower(display_name)) STORED,
    color TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'calendar' CHECK (role IN ('calendar', 'birthdays', 'custom')),
    sync_modseq BIGINT NOT NULL DEFAULT 1 CHECK (sync_modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, id),
    UNIQUE (tenant_id, owner_account_id, normalized_display_name),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX calendars_owner_role_idx
    ON calendars (tenant_id, owner_account_id, role)
    WHERE role <> 'custom';

CREATE TABLE calendar_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    calendar_id UUID NOT NULL,
    uid TEXT NOT NULL CHECK (btrim(uid) <> ''),
    sequence INTEGER NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    body_text TEXT NOT NULL DEFAULT '',
    body_html TEXT,
    location TEXT NOT NULL DEFAULT '',
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    time_zone TEXT NOT NULL DEFAULT '',
    all_day BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed', 'tentative', 'cancelled')),
    organizer_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    attendees_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    recurrence_rule TEXT,
    recurrence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    recurrence_exceptions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    reminder_set BOOLEAN NOT NULL DEFAULT FALSE,
    reminder_at TIMESTAMPTZ,
    reminder_dismissed_at TIMESTAMPTZ,
    exception_for_event_id UUID,
    exception_recurrence_id TEXT,
    raw_icalendar TEXT,
    import_source TEXT NOT NULL DEFAULT 'local' CHECK (import_source IN ('local', 'jmap', 'dav', 'ews', 'mapi', 'activesync', 'import')),
    source_uid TEXT,
    source_etag TEXT,
    source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, calendar_id, id),
    UNIQUE (tenant_id, owner_account_id, calendar_id, uid),
    CHECK (ends_at >= starts_at),
    CHECK ((NOT reminder_set) OR reminder_at IS NOT NULL),
    CHECK (jsonb_typeof(organizer_json) = 'object'),
    CHECK (jsonb_typeof(attendees_json) = 'object'),
    CHECK (jsonb_typeof(recurrence_json) = 'object'),
    CHECK (jsonb_typeof(recurrence_exceptions_json) = 'array'),
    CHECK (jsonb_typeof(source_payload_json) = 'object'),
    FOREIGN KEY (tenant_id, owner_account_id, calendar_id)
        REFERENCES calendars (tenant_id, owner_account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, owner_account_id, calendar_id, exception_for_event_id)
        REFERENCES calendar_events (tenant_id, owner_account_id, calendar_id, id)
        ON DELETE CASCADE
);

CREATE INDEX calendar_events_owner_time_idx
    ON calendar_events (tenant_id, owner_account_id, starts_at, ends_at);

CREATE INDEX calendar_events_owner_reminder_idx
    ON calendar_events (tenant_id, owner_account_id, reminder_set, reminder_at)
    WHERE reminder_set;

CREATE TABLE calendar_event_attachments (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    calendar_id UUID NOT NULL,
    event_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    blob_id UUID NOT NULL,
    blob_kind TEXT NOT NULL DEFAULT 'attachment' CHECK (blob_kind = 'attachment'),
    file_name TEXT NOT NULL CHECK (btrim(file_name) <> ''),
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    disposition TEXT NOT NULL DEFAULT 'attachment' CHECK (disposition IN ('attachment', 'inline')),
    content_id TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, event_id, ordinal),
    FOREIGN KEY (tenant_id, owner_account_id, calendar_id, event_id)
        REFERENCES calendar_events (tenant_id, owner_account_id, calendar_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)
        REFERENCES blobs (tenant_id, domain_id, id, blob_kind)
        ON DELETE RESTRICT
);

CREATE INDEX calendar_event_attachments_event_idx
    ON calendar_event_attachments (tenant_id, owner_account_id, event_id, ordinal);

CREATE INDEX calendar_event_attachments_blob_idx
    ON calendar_event_attachments (tenant_id, blob_id);

CREATE TABLE task_lists (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    normalized_display_name TEXT GENERATED ALWAYS AS (lower(display_name)) STORED,
    role TEXT NOT NULL DEFAULT 'custom' CHECK (role IN ('inbox', 'custom')),
    sync_modseq BIGINT NOT NULL DEFAULT 1 CHECK (sync_modseq > 0),
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, id),
    UNIQUE (tenant_id, owner_account_id, normalized_display_name),
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
    uid TEXT NOT NULL CHECK (btrim(uid) <> ''),
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'needs-action'
        CHECK (status IN ('needs-action', 'in-progress', 'completed', 'cancelled')),
    starts_at TIMESTAMPTZ,
    due_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 9),
    recurrence_rule TEXT,
    recurrence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    reminder_set BOOLEAN NOT NULL DEFAULT FALSE,
    reminder_at TIMESTAMPTZ,
    reminder_dismissed_at TIMESTAMPTZ,
    reminder_reset BOOLEAN NOT NULL DEFAULT FALSE,
    raw_vtodo TEXT,
    import_source TEXT NOT NULL DEFAULT 'local' CHECK (import_source IN ('local', 'jmap', 'dav', 'ews', 'mapi', 'activesync', 'import')),
    source_uid TEXT,
    source_etag TEXT,
    source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    modseq BIGINT NOT NULL DEFAULT 1 CHECK (modseq > 0),
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, owner_account_id, task_list_id, uid),
    CHECK ((status = 'completed' AND completed_at IS NOT NULL) OR (status <> 'completed' AND completed_at IS NULL)),
    CHECK ((NOT reminder_set) OR reminder_at IS NOT NULL),
    CHECK (jsonb_typeof(recurrence_json) = 'object'),
    CHECK (jsonb_typeof(source_payload_json) = 'object'),
    FOREIGN KEY (tenant_id, owner_account_id, task_list_id)
        REFERENCES task_lists (tenant_id, owner_account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX tasks_owner_status_idx
    ON tasks (tenant_id, owner_account_id, task_list_id, status, sort_order);

CREATE INDEX tasks_owner_reminder_idx
    ON tasks (tenant_id, owner_account_id, reminder_set, reminder_at)
    WHERE reminder_set;

CREATE TABLE reminder_occurrence_dismissals (
    tenant_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    source_type TEXT NOT NULL CHECK (source_type IN ('calendar', 'task')),
    source_id UUID NOT NULL,
    occurrence_start_at TIMESTAMPTZ NOT NULL,
    dismissed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, owner_account_id, source_type, source_id, occurrence_start_at),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX reminder_occurrence_dismissals_owner_idx
    ON reminder_occurrence_dismissals (tenant_id, owner_account_id, source_type, source_id, occurrence_start_at);

CREATE TABLE notes (
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
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX notes_owner_updated_idx
    ON notes (tenant_id, owner_account_id, updated_at DESC, id);

CREATE TABLE journal_entries (
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
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX journal_entries_owner_time_idx
    ON journal_entries (tenant_id, owner_account_id, COALESCE(starts_at, occurred_at, updated_at) DESC, id);

CREATE TABLE public_folder_trees (
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
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, admin_owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX public_folder_trees_tenant_state_idx
    ON public_folder_trees (tenant_id, lifecycle_state, display_name, id);

CREATE TABLE public_folders (
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
    FOREIGN KEY (tenant_id, tree_id) REFERENCES public_folder_trees (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, parent_folder_id) REFERENCES public_folders (tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE public_folder_trees
    ADD CONSTRAINT public_folder_trees_root_folder_fk
    FOREIGN KEY (tenant_id, root_folder_id) REFERENCES public_folders (tenant_id, id) ON DELETE RESTRICT;

CREATE INDEX public_folders_tree_parent_idx
    ON public_folders (tenant_id, tree_id, parent_folder_id, lifecycle_state, sort_order, display_name, id);

CREATE TABLE public_folder_items (
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
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public_folders (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, created_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, updated_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX public_folder_items_folder_idx
    ON public_folder_items (tenant_id, public_folder_id, lifecycle_state, updated_at DESC, id);

CREATE TABLE public_folder_permissions (
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
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public_folders (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, principal_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX public_folder_permissions_principal_idx
    ON public_folder_permissions (tenant_id, principal_account_id, public_folder_id);

CREATE TABLE public_folder_replicas (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    public_folder_id UUID NOT NULL,
    server_name TEXT NOT NULL CHECK (btrim(server_name) <> ''),
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active', 'inactive', 'deleted')),
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, public_folder_id, server_name),
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public_folders (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX public_folder_replicas_folder_idx
    ON public_folder_replicas (tenant_id, public_folder_id, lifecycle_state, sort_order, server_name, id);

CREATE TABLE public_folder_per_user_state (
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
    FOREIGN KEY (tenant_id, public_folder_id) REFERENCES public_folders (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, item_id) REFERENCES public_folder_items (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX public_folder_per_user_state_account_idx
    ON public_folder_per_user_state (tenant_id, account_id, public_folder_id, updated_at DESC);

CREATE TABLE account_client_configurations (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, public_folder_id)
        REFERENCES public_folders (tenant_id, id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX account_client_configurations_account_idx
    ON account_client_configurations (tenant_id, account_id, config_class, config_name)
    WHERE scope_kind = 'account';

CREATE UNIQUE INDEX account_client_configurations_mailbox_idx
    ON account_client_configurations (tenant_id, account_id, mailbox_id, config_class, config_name)
    WHERE scope_kind = 'mailbox';

CREATE UNIQUE INDEX account_client_configurations_public_folder_idx
    ON account_client_configurations (tenant_id, account_id, public_folder_id, config_class, config_name)
    WHERE scope_kind = 'public_folder';

CREATE TABLE contact_book_grants (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    contact_book_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, contact_book_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write),
    FOREIGN KEY (tenant_id, owner_account_id, contact_book_id)
        REFERENCES contact_books (tenant_id, owner_account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX contact_book_grants_grantee_idx
    ON contact_book_grants (tenant_id, grantee_account_id, owner_account_id, contact_book_id);

CREATE TABLE calendar_grants (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    calendar_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, calendar_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write),
    FOREIGN KEY (tenant_id, owner_account_id, calendar_id)
        REFERENCES calendars (tenant_id, owner_account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX calendar_grants_grantee_idx
    ON calendar_grants (tenant_id, grantee_account_id, owner_account_id, calendar_id);

CREATE TABLE task_list_grants (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    task_list_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, task_list_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write),
    FOREIGN KEY (tenant_id, owner_account_id, task_list_id)
        REFERENCES task_lists (tenant_id, owner_account_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX task_list_grants_grantee_idx
    ON task_list_grants (tenant_id, grantee_account_id, owner_account_id, task_list_id);

CREATE TABLE mailbox_delegation_grants (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    mailbox_id UUID NOT NULL,
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, mailbox_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write),
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
    CHECK (owner_account_id <> grantee_account_id),
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, owner_account_id, identity_id)
        REFERENCES account_identities (tenant_id, account_id, id)
        ON DELETE CASCADE
);

CREATE INDEX sender_rights_grantee_idx
    ON sender_rights (tenant_id, grantee_account_id, owner_account_id);

CREATE UNIQUE INDEX sender_rights_account_wide_idx
    ON sender_rights (tenant_id, owner_account_id, grantee_account_id, sender_right)
    WHERE identity_id IS NULL;

CREATE UNIQUE INDEX sender_rights_identity_idx
    ON sender_rights (tenant_id, owner_account_id, grantee_account_id, identity_id, sender_right)
    WHERE identity_id IS NOT NULL;

CREATE TABLE delegate_preferences (
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
    FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX delegate_preferences_grantee_idx
    ON delegate_preferences (tenant_id, grantee_account_id, owner_account_id);

CREATE TABLE mail_app_catalog (
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
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE INDEX mail_app_catalog_tenant_state_idx
    ON mail_app_catalog (tenant_id, lifecycle_state, display_name, id);

CREATE TABLE mail_app_tenant_policies (
    tenant_id UUID PRIMARY KEY,
    marketplace_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    marketplace_url TEXT CHECK (marketplace_url IS NULL OR btrim(marketplace_url) <> ''),
    default_install_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE TABLE mail_app_installations (
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
    FOREIGN KEY (tenant_id, app_catalog_id) REFERENCES mail_app_catalog (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, installed_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX mail_app_installations_tenant_idx
    ON mail_app_installations (tenant_id, app_catalog_id)
    WHERE install_scope = 'tenant' AND status <> 'uninstalled';

CREATE UNIQUE INDEX mail_app_installations_account_idx
    ON mail_app_installations (tenant_id, account_id, app_catalog_id)
    WHERE install_scope = 'account' AND status <> 'uninstalled';

CREATE TABLE mail_app_consents (
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
    FOREIGN KEY (tenant_id, app_catalog_id) REFERENCES mail_app_catalog (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, granted_by_account_id) REFERENCES accounts (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX mail_app_consents_account_idx
    ON mail_app_consents (tenant_id, account_id, app_catalog_id, revoked_at);

CREATE TABLE mail_app_token_events (
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
    FOREIGN KEY (tenant_id, app_catalog_id) REFERENCES mail_app_catalog (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX mail_app_token_events_account_idx
    ON mail_app_token_events (tenant_id, account_id, app_catalog_id, issued_at DESC);

CREATE INDEX mail_app_token_events_expiry_idx
    ON mail_app_token_events (tenant_id, expires_at)
    WHERE revoked_at IS NULL;

CREATE TABLE unified_messaging_calls (
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
    FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, id) ON DELETE RESTRICT
);

CREATE INDEX unified_messaging_calls_account_idx
    ON unified_messaging_calls (tenant_id, account_id, requested_at DESC, id);

CREATE INDEX unified_messaging_calls_status_idx
    ON unified_messaging_calls (tenant_id, status, updated_at, id);

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
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    action TEXT NOT NULL CHECK (btrim(action) <> ''),
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
);

CREATE INDEX audit_events_tenant_created_idx
    ON audit_events (tenant_id, created_at DESC);

CREATE OR REPLACE VIEW searchable_mail_documents AS
SELECT
    msd.tenant_id,
    msd.account_id,
    msd.mailbox_message_id,
    msd.message_id,
    msd.search_vector
FROM mail_search_documents msd;

INSERT INTO schema_metadata (singleton, schema_version)
VALUES (TRUE, '0.4.0-sql-v2');

COMMIT;
