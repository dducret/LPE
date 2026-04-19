CREATE SEQUENCE IF NOT EXISTS message_imap_uid_seq;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS imap_uid BIGINT;

ALTER TABLE messages
    ALTER COLUMN imap_uid SET DEFAULT nextval('message_imap_uid_seq');

UPDATE messages
SET imap_uid = nextval('message_imap_uid_seq')
WHERE imap_uid IS NULL;

ALTER TABLE messages
    ALTER COLUMN imap_uid SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS messages_imap_uid_idx
    ON messages (imap_uid);

CREATE INDEX IF NOT EXISTS messages_account_mailbox_imap_uid_idx
    ON messages (account_id, mailbox_id, imap_uid ASC);
