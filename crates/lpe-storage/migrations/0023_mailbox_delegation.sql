CREATE TABLE IF NOT EXISTS mailbox_delegation_grants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    owner_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    grantee_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, owner_account_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id)
);

CREATE INDEX IF NOT EXISTS mailbox_delegation_grants_grantee_idx
    ON mailbox_delegation_grants (tenant_id, grantee_account_id, owner_account_id);

CREATE INDEX IF NOT EXISTS mailbox_delegation_grants_owner_idx
    ON mailbox_delegation_grants (tenant_id, owner_account_id, grantee_account_id);

CREATE TABLE IF NOT EXISTS sender_delegation_grants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    owner_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    grantee_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    sender_right TEXT NOT NULL CHECK (sender_right IN ('send_as', 'send_on_behalf')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, owner_account_id, grantee_account_id, sender_right),
    CHECK (owner_account_id <> grantee_account_id)
);

CREATE INDEX IF NOT EXISTS sender_delegation_grants_grantee_idx
    ON sender_delegation_grants (tenant_id, grantee_account_id, owner_account_id, sender_right);

CREATE INDEX IF NOT EXISTS sender_delegation_grants_owner_idx
    ON sender_delegation_grants (tenant_id, owner_account_id, grantee_account_id, sender_right);

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS submitted_by_account_id UUID,
    ADD COLUMN IF NOT EXISTS sender_address TEXT,
    ADD COLUMN IF NOT EXISTS sender_display TEXT,
    ADD COLUMN IF NOT EXISTS sender_authorization_kind TEXT NOT NULL DEFAULT 'self';

UPDATE messages
SET submitted_by_account_id = account_id
WHERE submitted_by_account_id IS NULL;

ALTER TABLE messages
    ALTER COLUMN submitted_by_account_id SET NOT NULL;

ALTER TABLE messages
    DROP CONSTRAINT IF EXISTS messages_sender_authorization_kind_chk;

ALTER TABLE messages
    ADD CONSTRAINT messages_sender_authorization_kind_chk
    CHECK (sender_authorization_kind IN ('self', 'send-as', 'send-on-behalf'));

ALTER TABLE messages
    DROP CONSTRAINT IF EXISTS messages_submitted_by_account_fk;

ALTER TABLE messages
    ADD CONSTRAINT messages_submitted_by_account_fk
    FOREIGN KEY (submitted_by_account_id) REFERENCES accounts(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS messages_submitted_by_account_idx
    ON messages (tenant_id, submitted_by_account_id, received_at DESC);
