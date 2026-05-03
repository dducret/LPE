BEGIN;

WITH trash_candidates AS (
    SELECT
        id,
        tenant_id,
        account_id,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_id, account_id
            ORDER BY
                CASE
                    WHEN lower(btrim(display_name)) = 'deleted' THEN 0
                    WHEN lower(btrim(role)) = 'trash' THEN 1
                    WHEN lower(btrim(display_name)) = 'trash' THEN 2
                    WHEN lower(btrim(display_name)) = 'deleted items' THEN 3
                    ELSE 4
                END,
                created_at ASC
        ) AS rank
    FROM mailboxes
    WHERE lower(btrim(role)) = 'trash'
       OR lower(btrim(display_name)) IN ('deleted', 'deleted items', 'trash')
),
canonical_trash AS (
    UPDATE mailboxes mb
    SET role = 'trash',
        display_name = 'Deleted',
        sort_order = 30,
        retention_days = 365
    FROM trash_candidates c
    WHERE mb.id = c.id
      AND c.rank = 1
    RETURNING mb.tenant_id, mb.account_id, mb.id
),
alias_trash AS (
    SELECT
        c.tenant_id,
        c.account_id,
        c.id AS alias_id,
        ct.id AS canonical_id
    FROM trash_candidates c
    JOIN canonical_trash ct
      ON ct.tenant_id = c.tenant_id
     AND ct.account_id = c.account_id
    WHERE c.rank > 1
),
moved_messages AS (
    UPDATE messages m
    SET mailbox_id = a.canonical_id,
        imap_uid = nextval('message_imap_uid_seq'),
        imap_modseq = nextval('message_modseq_seq')
    FROM alias_trash a
    WHERE m.tenant_id = a.tenant_id
      AND m.account_id = a.account_id
      AND m.mailbox_id = a.alias_id
    RETURNING m.tenant_id, m.account_id, m.imap_modseq
),
moved_jobs AS (
    UPDATE mailbox_pst_jobs job
    SET mailbox_id = a.canonical_id
    FROM alias_trash a
    WHERE job.tenant_id = a.tenant_id
      AND job.mailbox_id = a.alias_id
    RETURNING job.tenant_id
),
advanced_accounts AS (
    UPDATE accounts acc
    SET mail_sync_modseq = GREATEST(
        acc.mail_sync_modseq,
        COALESCE((
            SELECT MAX(m.imap_modseq)
            FROM moved_messages m
            WHERE m.tenant_id = acc.tenant_id
              AND m.account_id = acc.id
        ), acc.mail_sync_modseq)
    )
    WHERE EXISTS (
        SELECT 1
        FROM moved_messages m
        WHERE m.tenant_id = acc.tenant_id
          AND m.account_id = acc.id
    )
    RETURNING acc.id
)
DELETE FROM mailboxes mb
USING alias_trash a
WHERE mb.tenant_id = a.tenant_id
  AND mb.account_id = a.account_id
  AND mb.id = a.alias_id
  AND NOT EXISTS (
      SELECT 1
      FROM messages m
      WHERE m.tenant_id = mb.tenant_id
        AND m.mailbox_id = mb.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM mailbox_pst_jobs job
      WHERE job.tenant_id = mb.tenant_id
        AND job.mailbox_id = mb.id
  );

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
SET schema_version = '0.1.10'
WHERE singleton = TRUE;

ALTER TABLE schema_metadata
    ADD CONSTRAINT schema_metadata_schema_version_check
    CHECK (schema_version = '0.1.10');

COMMIT;
