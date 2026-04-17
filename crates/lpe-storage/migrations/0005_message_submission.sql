ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS submission_source TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS delivery_status TEXT NOT NULL DEFAULT 'stored';

CREATE TABLE IF NOT EXISTS message_recipients (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    address TEXT NOT NULL,
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS message_recipients_message_idx
    ON message_recipients (message_id, ordinal);

CREATE INDEX IF NOT EXISTS message_recipients_address_idx
    ON message_recipients (tenant_id, address);

CREATE TABLE IF NOT EXISTS outbound_message_queue (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    transport TEXT NOT NULL DEFAULT 'lpe-ct-smtp',
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS outbound_message_queue_status_idx
    ON outbound_message_queue (tenant_id, status, next_attempt_at);

CREATE INDEX IF NOT EXISTS outbound_message_queue_message_idx
    ON outbound_message_queue (message_id);
