CREATE TABLE IF NOT EXISTS account_credentials (
    account_email TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS account_credentials_tenant_status_idx
    ON account_credentials (tenant_id, status);
