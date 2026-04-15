CREATE TABLE IF NOT EXISTS server_administrators (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain_id UUID REFERENCES domains(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    rights_summary TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS server_administrators_tenant_domain_idx
    ON server_administrators (tenant_id, domain_id, created_at DESC);

CREATE TABLE IF NOT EXISTS antispam_settings (
    tenant_id TEXT PRIMARY KEY,
    content_filtering_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    spam_engine TEXT NOT NULL DEFAULT 'rspamd-ready',
    quarantine_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    quarantine_retention_days INTEGER NOT NULL DEFAULT 30,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS antispam_filter_rules (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    scope TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS antispam_filter_rules_tenant_created_idx
    ON antispam_filter_rules (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS antispam_quarantine (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_ref TEXT NOT NULL,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS antispam_quarantine_tenant_created_idx
    ON antispam_quarantine (tenant_id, created_at DESC);

INSERT INTO antispam_settings (
    tenant_id, content_filtering_enabled, spam_engine, quarantine_enabled, quarantine_retention_days
)
VALUES ('default', TRUE, 'rspamd-ready', TRUE, 30)
ON CONFLICT (tenant_id) DO NOTHING;
