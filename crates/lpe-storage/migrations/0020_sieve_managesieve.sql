CREATE TABLE IF NOT EXISTS sieve_scripts (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED,
    content TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS sieve_scripts_account_name_idx
    ON sieve_scripts (tenant_id, account_id, normalized_name);

CREATE UNIQUE INDEX IF NOT EXISTS sieve_scripts_account_active_idx
    ON sieve_scripts (tenant_id, account_id)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS sieve_scripts_account_updated_idx
    ON sieve_scripts (tenant_id, account_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS sieve_vacation_responses (
    tenant_id TEXT NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    sender_address TEXT NOT NULL,
    response_key TEXT NOT NULL,
    last_sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, sender_address, response_key)
);

CREATE INDEX IF NOT EXISTS sieve_vacation_responses_account_sent_idx
    ON sieve_vacation_responses (tenant_id, account_id, last_sent_at DESC);
