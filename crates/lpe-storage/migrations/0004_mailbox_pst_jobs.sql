CREATE TABLE IF NOT EXISTS mailbox_pst_jobs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    mailbox_id UUID NOT NULL REFERENCES mailboxes(id) ON DELETE CASCADE,
    direction TEXT NOT NULL,
    server_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'requested',
    requested_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS mailbox_pst_jobs_tenant_mailbox_created_idx
    ON mailbox_pst_jobs (tenant_id, mailbox_id, created_at DESC);
