ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS quota_mb INTEGER NOT NULL DEFAULT 4096,
    ADD COLUMN IF NOT EXISTS used_mb INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';

ALTER TABLE mailboxes
    ADD COLUMN IF NOT EXISTS retention_days INTEGER NOT NULL DEFAULT 365;

CREATE TABLE IF NOT EXISTS domains (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    inbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    outbound_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    default_quota_mb INTEGER NOT NULL DEFAULT 4096,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS domains_tenant_name_idx
    ON domains (tenant_id, name);

CREATE TABLE IF NOT EXISTS aliases (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS aliases_tenant_source_idx
    ON aliases (tenant_id, source);

CREATE TABLE IF NOT EXISTS server_settings (
    tenant_id TEXT PRIMARY KEY,
    primary_hostname TEXT NOT NULL,
    admin_bind_address TEXT NOT NULL,
    smtp_bind_address TEXT NOT NULL,
    imap_bind_address TEXT NOT NULL,
    jmap_bind_address TEXT NOT NULL,
    default_locale TEXT NOT NULL DEFAULT 'en',
    max_message_size_mb INTEGER NOT NULL DEFAULT 64,
    tls_mode TEXT NOT NULL DEFAULT 'required',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS security_settings (
    tenant_id TEXT PRIMARY KEY,
    password_login_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mfa_required_for_admins BOOLEAN NOT NULL DEFAULT TRUE,
    session_timeout_minutes INTEGER NOT NULL DEFAULT 45,
    audit_retention_days INTEGER NOT NULL DEFAULT 365,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS local_ai_settings (
    tenant_id TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    provider TEXT NOT NULL DEFAULT 'stub-local',
    model TEXT NOT NULL DEFAULT 'gemma3-local',
    offline_only BOOLEAN NOT NULL DEFAULT TRUE,
    indexing_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    subject TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS audit_events_tenant_created_idx
    ON audit_events (tenant_id, created_at DESC);
