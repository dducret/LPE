CREATE TABLE IF NOT EXISTS message_bcc_recipients (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    address TEXT NOT NULL,
    display_name TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0,
    metadata_scope TEXT NOT NULL DEFAULT 'audit-compliance'
);

CREATE INDEX IF NOT EXISTS message_bcc_recipients_message_idx
    ON message_bcc_recipients (message_id, ordinal);

CREATE INDEX IF NOT EXISTS message_bcc_recipients_address_idx
    ON message_bcc_recipients (tenant_id, address);

INSERT INTO message_bcc_recipients (
    id, tenant_id, message_id, address, display_name, ordinal, metadata_scope
)
SELECT
    id,
    tenant_id,
    message_id,
    address,
    display_name,
    ordinal,
    'audit-compliance'
FROM message_recipients
WHERE kind = 'bcc'
ON CONFLICT (id) DO NOTHING;

DELETE FROM message_recipients
WHERE kind = 'bcc';

ALTER TABLE message_recipients
    DROP CONSTRAINT IF EXISTS message_recipients_visible_kind_chk;

ALTER TABLE message_recipients
    ADD CONSTRAINT message_recipients_visible_kind_chk
    CHECK (kind IN ('to', 'cc'));

ALTER TABLE message_bcc_recipients
    DROP CONSTRAINT IF EXISTS message_bcc_recipients_scope_chk;

ALTER TABLE message_bcc_recipients
    ADD CONSTRAINT message_bcc_recipients_scope_chk
    CHECK (metadata_scope = 'audit-compliance');
