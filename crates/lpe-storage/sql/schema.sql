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

CREATE EXTENSION pg_trgm;

CREATE SEQUENCE message_imap_uid_seq;

CREATE TABLE schema_metadata (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton = TRUE),
    schema_version TEXT NOT NULL CHECK (schema_version = '0.1.3'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    primary_email TEXT NOT NULL CHECK (primary_email = lower(btrim(primary_email))),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    quota_mb INTEGER NOT NULL DEFAULT 4096 CHECK (quota_mb >= 0),
    used_mb INTEGER NOT NULL DEFAULT 0 CHECK (used_mb >= 0),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'disabled')),
    gal_visibility TEXT NOT NULL DEFAULT 'tenant' CHECK (gal_visibility IN ('tenant', 'hidden')),
    directory_kind TEXT NOT NULL DEFAULT 'person'
        CHECK (directory_kind IN ('person', 'room', 'equipment')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, primary_email)
);

CREATE TABLE domains (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    name TEXT NOT NULL CHECK (name = lower(btrim(name))),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    inbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    outbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    default_quota_mb INTEGER NOT NULL DEFAULT 4096 CHECK (default_quota_mb >= 0),
    default_sieve_script TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, name)
);

CREATE TABLE mailboxes (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (btrim(role) <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    normalized_display_name TEXT GENERATED ALWAYS AS (lower(display_name)) STORED,
    sort_order INTEGER NOT NULL DEFAULT 0,
    retention_days INTEGER NOT NULL DEFAULT 365 CHECK (retention_days > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, account_id, normalized_display_name),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX mailboxes_account_role_idx
    ON mailboxes (tenant_id, account_id, role);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    mailbox_id UUID NOT NULL,
    thread_id UUID NOT NULL,
    internet_message_id TEXT,
    imap_uid BIGINT NOT NULL DEFAULT nextval('message_imap_uid_seq'),
    received_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    from_display TEXT,
    from_address TEXT NOT NULL CHECK (from_address = lower(btrim(from_address))),
    sender_display TEXT,
    sender_address TEXT,
    sender_authorization_kind TEXT NOT NULL DEFAULT 'self'
        CHECK (sender_authorization_kind IN ('self', 'send-as', 'send-on-behalf')),
    submitted_by_account_id UUID NOT NULL,
    subject_normalized TEXT NOT NULL,
    preview_text TEXT NOT NULL,
    submission_source TEXT NOT NULL DEFAULT 'unknown' CHECK (btrim(submission_source) <> ''),
    delivery_status TEXT NOT NULL DEFAULT 'stored'
        CHECK (delivery_status IN ('stored', 'draft', 'queued', 'relayed', 'deferred', 'quarantined', 'bounced', 'failed')),
    unread BOOLEAN NOT NULL DEFAULT TRUE,
    flagged BOOLEAN NOT NULL DEFAULT FALSE,
    has_attachments BOOLEAN NOT NULL DEFAULT FALSE,
    size_octets BIGINT NOT NULL DEFAULT 0 CHECK (size_octets >= 0),
    mime_blob_ref TEXT NOT NULL CHECK (btrim(mime_blob_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, submitted_by_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX messages_mailbox_received_idx
    ON messages (tenant_id, account_id, mailbox_id, received_at DESC);

CREATE INDEX messages_account_thread_idx
    ON messages (tenant_id, account_id, thread_id);

CREATE INDEX messages_submitted_by_idx
    ON messages (tenant_id, submitted_by_account_id, sent_at DESC);

CREATE INDEX messages_unread_partial_idx
    ON messages (tenant_id, account_id, mailbox_id, received_at DESC)
    WHERE unread = TRUE;

CREATE INDEX messages_flagged_partial_idx
    ON messages (tenant_id, account_id, mailbox_id, received_at DESC)
    WHERE flagged = TRUE;

CREATE UNIQUE INDEX messages_mailbox_imap_uid_idx
    ON messages (mailbox_id, imap_uid);

CREATE INDEX messages_account_mailbox_imap_uid_idx
    ON messages (tenant_id, account_id, mailbox_id, imap_uid ASC);

CREATE TABLE message_bodies (
    message_id UUID PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    body_text TEXT NOT NULL,
    body_html_sanitized TEXT,
    participants_normalized TEXT NOT NULL,
    language_code TEXT,
    content_hash TEXT NOT NULL CHECK (btrim(content_hash) <> ''),
    search_vector TSVECTOR NOT NULL
);

CREATE INDEX message_bodies_search_vector_idx
    ON message_bodies USING GIN (search_vector);

CREATE INDEX message_bodies_body_text_trgm_idx
    ON message_bodies USING GIN (body_text gin_trgm_ops);

CREATE TABLE attachment_blobs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    domain_name TEXT NOT NULL CHECK (domain_name = lower(btrim(domain_name))),
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    blob_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, id),
    UNIQUE (tenant_id, domain_name, content_sha256)
);

CREATE TABLE attachments (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    message_id UUID NOT NULL,
    file_name TEXT NOT NULL CHECK (btrim(file_name) <> ''),
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    size_octets BIGINT NOT NULL CHECK (size_octets >= 0),
    blob_ref TEXT NOT NULL CHECK (btrim(blob_ref) <> ''),
    attachment_blob_id UUID,
    extracted_text TEXT,
    extracted_text_tsv TSVECTOR,
    FOREIGN KEY (tenant_id, message_id)
        REFERENCES messages (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, attachment_blob_id)
        REFERENCES attachment_blobs (tenant_id, id)
        ON DELETE RESTRICT
);

CREATE INDEX attachments_message_idx
    ON attachments (tenant_id, message_id);

CREATE INDEX attachments_blob_id_idx
    ON attachments (tenant_id, attachment_blob_id);

CREATE INDEX attachments_extracted_text_tsv_idx
    ON attachments USING GIN (extracted_text_tsv);

CREATE TABLE document_projections (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    source_object_id UUID NOT NULL,
    source_kind TEXT NOT NULL CHECK (btrim(source_kind) <> ''),
    owner_account_id UUID NOT NULL,
    acl_fingerprint TEXT NOT NULL CHECK (btrim(acl_fingerprint) <> ''),
    title TEXT NOT NULL,
    preview TEXT NOT NULL,
    body_text TEXT NOT NULL,
    language_code TEXT,
    participants_normalized TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (btrim(content_hash) <> ''),
    search_vector TSVECTOR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, owner_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX document_projections_owner_kind_idx
    ON document_projections (tenant_id, owner_account_id, source_kind, updated_at DESC);

CREATE INDEX document_projections_search_vector_idx
    ON document_projections USING GIN (search_vector);

CREATE TABLE document_chunks (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES document_projections(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    chunk_text TEXT NOT NULL,
    token_estimate INTEGER NOT NULL CHECK (token_estimate >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (document_id, ordinal)
);

CREATE TABLE document_annotations (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES document_projections(id) ON DELETE CASCADE,
    annotation_type TEXT NOT NULL CHECK (btrim(annotation_type) <> ''),
    payload_json JSONB NOT NULL,
    model_name TEXT,
    created_by_account_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (created_by_account_id) REFERENCES accounts(id) ON DELETE SET NULL
);

CREATE INDEX document_annotations_document_type_idx
    ON document_annotations (document_id, annotation_type, created_at DESC);

CREATE TABLE inference_runs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    principal_account_id UUID,
    model_name TEXT NOT NULL CHECK (btrim(model_name) <> ''),
    operation TEXT NOT NULL CHECK (btrim(operation) <> ''),
    request_payload JSONB NOT NULL,
    response_payload JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, principal_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE SET NULL
);

CREATE INDEX inference_runs_tenant_started_idx
    ON inference_runs (tenant_id, started_at DESC);

CREATE TABLE inference_run_chunks (
    inference_run_id UUID NOT NULL REFERENCES inference_runs(id) ON DELETE CASCADE,
    chunk_id UUID NOT NULL REFERENCES document_chunks(id) ON DELETE CASCADE,
    PRIMARY KEY (inference_run_id, chunk_id)
);

CREATE TABLE aliases (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    source TEXT NOT NULL CHECK (source = lower(btrim(source))),
    target TEXT NOT NULL CHECK (target = lower(btrim(target))),
    kind TEXT NOT NULL CHECK (btrim(kind) <> ''),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, source)
);

CREATE TABLE server_settings (
    tenant_id TEXT PRIMARY KEY,
    primary_hostname TEXT NOT NULL CHECK (btrim(primary_hostname) <> ''),
    admin_bind_address TEXT NOT NULL CHECK (btrim(admin_bind_address) <> ''),
    smtp_bind_address TEXT NOT NULL CHECK (btrim(smtp_bind_address) <> ''),
    imap_bind_address TEXT NOT NULL CHECK (btrim(imap_bind_address) <> ''),
    jmap_bind_address TEXT NOT NULL CHECK (btrim(jmap_bind_address) <> ''),
    default_locale TEXT NOT NULL DEFAULT 'en' CHECK (default_locale IN ('en', 'fr', 'de', 'it', 'es')),
    max_message_size_mb INTEGER NOT NULL DEFAULT 64 CHECK (max_message_size_mb > 0),
    tls_mode TEXT NOT NULL DEFAULT 'required' CHECK (tls_mode IN ('required', 'optional', 'disabled')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE security_settings (
    tenant_id TEXT PRIMARY KEY,
    password_login_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mfa_required_for_admins BOOLEAN NOT NULL DEFAULT TRUE,
    session_timeout_minutes INTEGER NOT NULL DEFAULT 45 CHECK (session_timeout_minutes > 0),
    audit_retention_days INTEGER NOT NULL DEFAULT 365 CHECK (audit_retention_days > 0),
    oidc_login_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    oidc_provider_label TEXT NOT NULL DEFAULT 'Corporate SSO',
    oidc_auto_link_by_email BOOLEAN NOT NULL DEFAULT TRUE,
    mailbox_password_login_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mailbox_oidc_login_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    mailbox_oidc_provider_label TEXT NOT NULL DEFAULT 'Mailbox SSO',
    mailbox_oidc_auto_link_by_email BOOLEAN NOT NULL DEFAULT TRUE,
    mailbox_app_passwords_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE local_ai_settings (
    tenant_id TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    provider TEXT NOT NULL DEFAULT 'stub-local',
    model TEXT NOT NULL DEFAULT 'gemma3-local',
    offline_only BOOLEAN NOT NULL DEFAULT TRUE,
    indexing_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE audit_events (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    action TEXT NOT NULL CHECK (btrim(action) <> ''),
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX audit_events_tenant_created_idx
    ON audit_events (tenant_id, created_at DESC);

CREATE TABLE server_administrators (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    domain_id UUID,
    email TEXT NOT NULL CHECK (email = lower(btrim(email))),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    role TEXT NOT NULL CHECK (btrim(role) <> ''),
    rights_summary TEXT NOT NULL,
    permissions_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, domain_id)
        REFERENCES domains (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX server_administrators_tenant_domain_idx
    ON server_administrators (tenant_id, domain_id, created_at DESC);

CREATE TABLE admin_credentials (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    email TEXT NOT NULL CHECK (email = lower(btrim(email))),
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, email)
);

CREATE INDEX admin_credentials_tenant_status_idx
    ON admin_credentials (tenant_id, status);

CREATE TABLE admin_sessions (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    token TEXT NOT NULL UNIQUE CHECK (btrim(token) <> ''),
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email))),
    auth_method TEXT NOT NULL DEFAULT 'password' CHECK (auth_method IN ('password', 'oidc')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > created_at),
    FOREIGN KEY (tenant_id, admin_email)
        REFERENCES admin_credentials (tenant_id, email)
        ON DELETE CASCADE
);

CREATE INDEX admin_sessions_token_expires_idx
    ON admin_sessions (token, expires_at);

CREATE INDEX admin_sessions_admin_idx
    ON admin_sessions (tenant_id, admin_email, expires_at DESC);

CREATE TABLE admin_oidc_config (
    tenant_id TEXT PRIMARY KEY,
    issuer_url TEXT NOT NULL CHECK (btrim(issuer_url) <> ''),
    authorization_endpoint TEXT NOT NULL CHECK (btrim(authorization_endpoint) <> ''),
    token_endpoint TEXT NOT NULL CHECK (btrim(token_endpoint) <> ''),
    userinfo_endpoint TEXT NOT NULL CHECK (btrim(userinfo_endpoint) <> ''),
    client_id TEXT NOT NULL CHECK (btrim(client_id) <> ''),
    client_secret TEXT NOT NULL CHECK (btrim(client_secret) <> ''),
    scopes TEXT NOT NULL DEFAULT 'openid profile email',
    claim_email TEXT NOT NULL DEFAULT 'email',
    claim_display_name TEXT NOT NULL DEFAULT 'name',
    claim_subject TEXT NOT NULL DEFAULT 'sub',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE admin_oidc_identities (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    issuer_url TEXT NOT NULL CHECK (btrim(issuer_url) <> ''),
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email))),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, issuer_url, subject),
    FOREIGN KEY (tenant_id, admin_email)
        REFERENCES admin_credentials (tenant_id, email)
        ON DELETE CASCADE
);

CREATE INDEX admin_oidc_identities_admin_idx
    ON admin_oidc_identities (tenant_id, admin_email);

CREATE TABLE admin_auth_factors (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    admin_email TEXT NOT NULL CHECK (admin_email = lower(btrim(admin_email))),
    factor_type TEXT NOT NULL CHECK (factor_type IN ('totp')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'verified', 'disabled')),
    secret_ciphertext TEXT,
    recovery_codes_hashes_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verified_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, admin_email)
        REFERENCES admin_credentials (tenant_id, email)
        ON DELETE CASCADE
);

CREATE INDEX admin_auth_factors_admin_idx
    ON admin_auth_factors (tenant_id, admin_email, factor_type);

CREATE TABLE account_credentials (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_email TEXT NOT NULL CHECK (account_email = lower(btrim(account_email))),
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_email),
    FOREIGN KEY (tenant_id, account_email)
        REFERENCES accounts (tenant_id, primary_email)
        ON DELETE CASCADE
);

CREATE INDEX account_credentials_tenant_status_idx
    ON account_credentials (tenant_id, status);

CREATE TABLE account_oidc_config (
    tenant_id TEXT PRIMARY KEY,
    issuer_url TEXT NOT NULL CHECK (btrim(issuer_url) <> ''),
    authorization_endpoint TEXT NOT NULL CHECK (btrim(authorization_endpoint) <> ''),
    token_endpoint TEXT NOT NULL CHECK (btrim(token_endpoint) <> ''),
    userinfo_endpoint TEXT NOT NULL CHECK (btrim(userinfo_endpoint) <> ''),
    client_id TEXT NOT NULL CHECK (btrim(client_id) <> ''),
    client_secret TEXT NOT NULL CHECK (btrim(client_secret) <> ''),
    scopes TEXT NOT NULL DEFAULT 'openid profile email',
    claim_email TEXT NOT NULL DEFAULT 'email',
    claim_display_name TEXT NOT NULL DEFAULT 'name',
    claim_subject TEXT NOT NULL DEFAULT 'sub',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE account_oidc_identities (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    issuer_url TEXT NOT NULL CHECK (btrim(issuer_url) <> ''),
    subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
    account_email TEXT NOT NULL CHECK (account_email = lower(btrim(account_email))),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, issuer_url, subject),
    FOREIGN KEY (tenant_id, account_email)
        REFERENCES account_credentials (tenant_id, account_email)
        ON DELETE CASCADE
);

CREATE INDEX account_oidc_identities_account_idx
    ON account_oidc_identities (tenant_id, account_email);

CREATE TABLE account_auth_factors (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_email TEXT NOT NULL CHECK (account_email = lower(btrim(account_email))),
    factor_type TEXT NOT NULL CHECK (factor_type IN ('totp')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'active', 'revoked')),
    secret_ciphertext TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verified_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, account_email)
        REFERENCES account_credentials (tenant_id, account_email)
        ON DELETE CASCADE
);

CREATE INDEX account_auth_factors_account_idx
    ON account_auth_factors (tenant_id, account_email, factor_type);

CREATE TABLE account_app_passwords (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_email TEXT NOT NULL CHECK (account_email = lower(btrim(account_email))),
    label TEXT NOT NULL CHECK (btrim(label) <> ''),
    password_hash TEXT NOT NULL CHECK (btrim(password_hash) <> ''),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, account_email)
        REFERENCES account_credentials (tenant_id, account_email)
        ON DELETE CASCADE
);

CREATE INDEX account_app_passwords_account_idx
    ON account_app_passwords (tenant_id, account_email, status);

CREATE TABLE account_sessions (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    token TEXT NOT NULL UNIQUE CHECK (btrim(token) <> ''),
    account_email TEXT NOT NULL CHECK (account_email = lower(btrim(account_email))),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > created_at),
    FOREIGN KEY (tenant_id, account_email)
        REFERENCES account_credentials (tenant_id, account_email)
        ON DELETE CASCADE
);

CREATE INDEX account_sessions_token_expires_idx
    ON account_sessions (token, expires_at);

CREATE INDEX account_sessions_account_idx
    ON account_sessions (tenant_id, account_email, expires_at DESC);

CREATE TABLE antispam_settings (
    tenant_id TEXT PRIMARY KEY,
    content_filtering_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    spam_engine TEXT NOT NULL DEFAULT 'rspamd-ready',
    quarantine_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    quarantine_retention_days INTEGER NOT NULL DEFAULT 30 CHECK (quarantine_retention_days > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE antispam_filter_rules (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    scope TEXT NOT NULL CHECK (btrim(scope) <> ''),
    action TEXT NOT NULL CHECK (btrim(action) <> ''),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX antispam_filter_rules_tenant_created_idx
    ON antispam_filter_rules (tenant_id, created_at DESC);

CREATE TABLE antispam_quarantine (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    message_ref TEXT NOT NULL CHECK (btrim(message_ref) <> ''),
    sender TEXT NOT NULL CHECK (sender = lower(btrim(sender))),
    recipient TEXT NOT NULL CHECK (recipient = lower(btrim(recipient))),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    status TEXT NOT NULL CHECK (status IN ('held', 'released', 'deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX antispam_quarantine_tenant_created_idx
    ON antispam_quarantine (tenant_id, created_at DESC);

CREATE TABLE mailbox_pst_jobs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    mailbox_id UUID NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('import', 'export')),
    server_path TEXT NOT NULL CHECK (btrim(server_path) <> ''),
    status TEXT NOT NULL DEFAULT 'requested' CHECK (status IN ('requested', 'running', 'completed', 'failed')),
    requested_by TEXT NOT NULL CHECK (btrim(requested_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    processed_messages INTEGER NOT NULL DEFAULT 0 CHECK (processed_messages >= 0),
    FOREIGN KEY (tenant_id, mailbox_id)
        REFERENCES mailboxes (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX mailbox_pst_jobs_tenant_mailbox_created_idx
    ON mailbox_pst_jobs (tenant_id, mailbox_id, created_at DESC);

CREATE INDEX mailbox_pst_jobs_pending_idx
    ON mailbox_pst_jobs (tenant_id, status, created_at)
    WHERE status IN ('requested', 'failed');

CREATE TABLE message_recipients (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    message_id UUID NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('to', 'cc')),
    address TEXT NOT NULL CHECK (address = lower(btrim(address))),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    FOREIGN KEY (tenant_id, message_id)
        REFERENCES messages (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX message_recipients_message_idx
    ON message_recipients (tenant_id, message_id, ordinal);

CREATE INDEX message_recipients_address_idx
    ON message_recipients (tenant_id, address);

CREATE TABLE message_bcc_recipients (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    message_id UUID NOT NULL,
    address TEXT NOT NULL CHECK (address = lower(btrim(address))),
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    metadata_scope TEXT NOT NULL DEFAULT 'audit-compliance' CHECK (metadata_scope = 'audit-compliance'),
    FOREIGN KEY (tenant_id, message_id)
        REFERENCES messages (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX message_bcc_recipients_message_idx
    ON message_bcc_recipients (tenant_id, message_id, ordinal);

CREATE INDEX message_bcc_recipients_address_idx
    ON message_bcc_recipients (tenant_id, address);

CREATE TABLE outbound_message_queue (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    message_id UUID NOT NULL,
    account_id UUID NOT NULL,
    transport TEXT NOT NULL DEFAULT 'lpe-ct-smtp' CHECK (btrim(transport) <> ''),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'relayed', 'deferred', 'quarantined', 'bounced', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    last_error TEXT,
    remote_message_ref TEXT,
    retry_after_seconds INTEGER CHECK (retry_after_seconds IS NULL OR retry_after_seconds >= 0),
    retry_policy TEXT,
    last_dsn_action TEXT,
    last_dsn_status TEXT,
    last_smtp_code INTEGER CHECK (last_smtp_code IS NULL OR last_smtp_code >= 100),
    last_enhanced_status TEXT,
    last_routing_rule TEXT,
    last_throttle_scope TEXT,
    last_throttle_delay_seconds INTEGER CHECK (last_throttle_delay_seconds IS NULL OR last_throttle_delay_seconds >= 0),
    last_result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, message_id)
        REFERENCES messages (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX outbound_message_queue_status_idx
    ON outbound_message_queue (tenant_id, status, next_attempt_at);

CREATE INDEX outbound_message_queue_message_idx
    ON outbound_message_queue (tenant_id, message_id);

CREATE TABLE contacts (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    role TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL CHECK (email = lower(btrim(email))),
    phone TEXT NOT NULL DEFAULT '',
    team TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX contacts_account_name_idx
    ON contacts (tenant_id, account_id, name);

CREATE TABLE calendar_events (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    event_date DATE NOT NULL,
    event_time TIME NOT NULL,
    time_zone TEXT NOT NULL DEFAULT '',
    duration_minutes INTEGER NOT NULL DEFAULT 0 CHECK (duration_minutes >= 0),
    recurrence_rule TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    location TEXT NOT NULL DEFAULT '',
    attendees TEXT NOT NULL DEFAULT '',
    attendees_json TEXT NOT NULL DEFAULT '[]',
    notes TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX calendar_events_account_datetime_idx
    ON calendar_events (tenant_id, account_id, event_date, event_time);

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'needs-action'
        CHECK (status IN ('needs-action', 'in-progress', 'completed', 'cancelled')),
    due_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX tasks_account_status_due_idx
    ON tasks (tenant_id, account_id, status, sort_order, due_at);

CREATE INDEX tasks_account_updated_idx
    ON tasks (tenant_id, account_id, updated_at DESC);

CREATE TABLE jmap_upload_blobs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    octet_size BIGINT NOT NULL CHECK (octet_size >= 0),
    blob_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX jmap_upload_blobs_account_created_idx
    ON jmap_upload_blobs (tenant_id, account_id, created_at DESC);

CREATE TABLE activesync_sync_states (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    device_id TEXT NOT NULL CHECK (btrim(device_id) <> ''),
    collection_id TEXT NOT NULL CHECK (btrim(collection_id) <> ''),
    sync_key TEXT NOT NULL CHECK (btrim(sync_key) <> ''),
    snapshot_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE,
    UNIQUE (tenant_id, account_id, device_id, collection_id, sync_key)
);

CREATE INDEX activesync_sync_states_collection_created_idx
    ON activesync_sync_states (tenant_id, account_id, device_id, collection_id, created_at DESC);

CREATE TABLE sieve_scripts (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED,
    content TEXT NOT NULL CHECK (btrim(content) <> ''),
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE,
    UNIQUE (tenant_id, account_id, normalized_name)
);

CREATE UNIQUE INDEX sieve_scripts_account_active_idx
    ON sieve_scripts (tenant_id, account_id)
    WHERE is_active = TRUE;

CREATE INDEX sieve_scripts_account_updated_idx
    ON sieve_scripts (tenant_id, account_id, updated_at DESC);

CREATE TABLE sieve_vacation_responses (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    account_id UUID NOT NULL,
    sender_address TEXT NOT NULL CHECK (sender_address = lower(btrim(sender_address))),
    response_key TEXT NOT NULL CHECK (btrim(response_key) <> ''),
    last_sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, sender_address, response_key),
    FOREIGN KEY (tenant_id, account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX sieve_vacation_responses_account_sent_idx
    ON sieve_vacation_responses (tenant_id, account_id, last_sent_at DESC);

CREATE TABLE collaboration_collection_grants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    collection_kind TEXT NOT NULL CHECK (collection_kind IN ('contacts', 'calendar')),
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, collection_kind, owner_account_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write),
    FOREIGN KEY (tenant_id, owner_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX collaboration_collection_grants_grantee_idx
    ON collaboration_collection_grants (tenant_id, collection_kind, grantee_account_id, owner_account_id);

CREATE INDEX collaboration_collection_grants_owner_idx
    ON collaboration_collection_grants (tenant_id, collection_kind, owner_account_id, grantee_account_id);

CREATE TABLE mailbox_delegation_grants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, owner_account_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    FOREIGN KEY (tenant_id, owner_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX mailbox_delegation_grants_grantee_idx
    ON mailbox_delegation_grants (tenant_id, grantee_account_id, owner_account_id);

CREATE INDEX mailbox_delegation_grants_owner_idx
    ON mailbox_delegation_grants (tenant_id, owner_account_id, grantee_account_id);

CREATE TABLE sender_delegation_grants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    owner_account_id UUID NOT NULL,
    grantee_account_id UUID NOT NULL,
    sender_right TEXT NOT NULL CHECK (sender_right IN ('send_as', 'send_on_behalf')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, owner_account_id, grantee_account_id, sender_right),
    CHECK (owner_account_id <> grantee_account_id),
    FOREIGN KEY (tenant_id, owner_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, grantee_account_id)
        REFERENCES accounts (tenant_id, id)
        ON DELETE CASCADE
);

CREATE INDEX sender_delegation_grants_grantee_idx
    ON sender_delegation_grants (tenant_id, grantee_account_id, owner_account_id);

CREATE INDEX sender_delegation_grants_owner_idx
    ON sender_delegation_grants (tenant_id, owner_account_id, grantee_account_id, sender_right);

CREATE OR REPLACE VIEW searchable_mail_documents AS
SELECT
    m.id AS message_id,
    m.account_id,
    m.mailbox_id,
    m.received_at,
    m.subject_normalized,
    mb.search_vector AS message_search_vector,
    COALESCE(
        to_tsvector('simple', string_agg(a.extracted_text, ' ' ORDER BY a.file_name)),
        ''::tsvector
    ) AS attachment_search_vector
FROM messages m
JOIN message_bodies mb ON mb.message_id = m.id
LEFT JOIN attachments a
    ON a.tenant_id = m.tenant_id
   AND a.message_id = m.id
GROUP BY m.id, m.account_id, m.mailbox_id, m.received_at, m.subject_normalized, mb.search_vector;

INSERT INTO schema_metadata (singleton, schema_version)
VALUES (TRUE, '0.1.3');

INSERT INTO security_settings (
    tenant_id,
    password_login_enabled,
    mfa_required_for_admins,
    session_timeout_minutes,
    audit_retention_days,
    oidc_login_enabled,
    oidc_provider_label,
    oidc_auto_link_by_email
)
VALUES ('__platform__', TRUE, TRUE, 45, 365, FALSE, 'Corporate SSO', TRUE);

INSERT INTO local_ai_settings (
    tenant_id,
    enabled,
    provider,
    model,
    offline_only,
    indexing_enabled
)
VALUES ('__platform__', TRUE, 'stub-local', 'gemma3-local', TRUE, TRUE);

INSERT INTO antispam_settings (
    tenant_id,
    content_filtering_enabled,
    spam_engine,
    quarantine_enabled,
    quarantine_retention_days
)
VALUES ('__platform__', TRUE, 'rspamd-ready', TRUE, 30);

COMMIT;
