BEGIN;

ALTER TABLE mailbox_delegation_grants
    ADD COLUMN IF NOT EXISTS may_write BOOLEAN NOT NULL DEFAULT TRUE;

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
SET schema_version = '0.1.11'
WHERE singleton = TRUE;

ALTER TABLE schema_metadata
    ADD CONSTRAINT schema_metadata_schema_version_check
    CHECK (schema_version = '0.1.11');

COMMIT;
