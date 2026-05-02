BEGIN;

CREATE SEQUENCE IF NOT EXISTS message_modseq_seq START WITH 2;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS imap_modseq BIGINT;

UPDATE messages
SET imap_modseq = nextval('message_modseq_seq')
WHERE imap_modseq IS NULL;

SELECT setval(
    'message_modseq_seq',
    GREATEST((SELECT COALESCE(MAX(imap_modseq), 1) FROM messages), 1),
    TRUE
);

ALTER TABLE messages
    ALTER COLUMN imap_modseq SET DEFAULT nextval('message_modseq_seq'),
    ALTER COLUMN imap_modseq SET NOT NULL;

CREATE INDEX IF NOT EXISTS messages_account_mailbox_imap_modseq_idx
    ON messages (tenant_id, account_id, mailbox_id, imap_modseq ASC);

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    SELECT conname
    INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'schema_metadata'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%schema_version%'
    LIMIT 1;

    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE schema_metadata DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;

UPDATE schema_metadata
SET schema_version = '0.1.8'
WHERE singleton = TRUE;

ALTER TABLE schema_metadata
    ADD CONSTRAINT schema_metadata_schema_version_check
    CHECK (schema_version = '0.1.8');

COMMIT;
