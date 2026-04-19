CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'needs-action',
    due_at TIMESTAMPTZ NULL,
    completed_at TIMESTAMPTZ NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tasks_status_check CHECK (
        status IN ('needs-action', 'in-progress', 'completed', 'cancelled')
    )
);

CREATE INDEX IF NOT EXISTS tasks_account_status_due_idx
    ON tasks (tenant_id, account_id, status, sort_order, due_at);

CREATE INDEX IF NOT EXISTS tasks_account_updated_idx
    ON tasks (tenant_id, account_id, updated_at DESC);
