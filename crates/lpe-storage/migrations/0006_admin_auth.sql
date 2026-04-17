CREATE TABLE IF NOT EXISTS admin_credentials (
    email TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS admin_credentials_tenant_status_idx
    ON admin_credentials (tenant_id, status);

CREATE TABLE IF NOT EXISTS admin_sessions (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    admin_email TEXT NOT NULL REFERENCES admin_credentials(email) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS admin_sessions_token_expires_idx
    ON admin_sessions (token, expires_at);

CREATE INDEX IF NOT EXISTS admin_sessions_admin_idx
    ON admin_sessions (tenant_id, admin_email, expires_at DESC);
