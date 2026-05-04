ALTER TABLE mailbox_delegation_grants
    ADD COLUMN IF NOT EXISTS may_write BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE schema_metadata
SET schema_version = '0.1.11'
WHERE singleton = TRUE;
