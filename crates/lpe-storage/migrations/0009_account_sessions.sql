CREATE TABLE IF NOT EXISTS account_sessions (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    account_email TEXT NOT NULL REFERENCES account_credentials(account_email) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS account_sessions_token_expires_idx
    ON account_sessions (token, expires_at);

CREATE INDEX IF NOT EXISTS account_sessions_account_idx
    ON account_sessions (tenant_id, account_email, expires_at DESC);
