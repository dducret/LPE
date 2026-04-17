ALTER TABLE mailbox_pst_jobs
    ADD COLUMN IF NOT EXISTS error_message TEXT,
    ADD COLUMN IF NOT EXISTS processed_messages INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS mailbox_pst_jobs_pending_idx
    ON mailbox_pst_jobs (tenant_id, status, created_at)
    WHERE status IN ('requested', 'failed');
