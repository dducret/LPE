BEGIN;

ALTER TABLE domains
    ADD COLUMN IF NOT EXISTS jmap_push_journal_retention_days INTEGER NOT NULL DEFAULT 30
    CHECK (jmap_push_journal_retention_days > 0);

CREATE INDEX IF NOT EXISTS canonical_change_journal_tenant_created_idx
    ON canonical_change_journal (tenant_id, created_at ASC);

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
SET schema_version = '0.1.12'
WHERE singleton = TRUE;

ALTER TABLE schema_metadata
    ADD CONSTRAINT schema_metadata_schema_version_check
    CHECK (schema_version = '0.1.12');

COMMIT;
