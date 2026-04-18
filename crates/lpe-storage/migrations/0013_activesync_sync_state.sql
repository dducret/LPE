CREATE TABLE IF NOT EXISTS activesync_sync_states (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    collection_id TEXT NOT NULL,
    sync_key TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS activesync_sync_states_sync_key_idx
    ON activesync_sync_states (tenant_id, account_id, device_id, collection_id, sync_key);

CREATE INDEX IF NOT EXISTS activesync_sync_states_collection_created_idx
    ON activesync_sync_states (tenant_id, account_id, device_id, collection_id, created_at DESC);
