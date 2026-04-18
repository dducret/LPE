CREATE TABLE IF NOT EXISTS jmap_upload_blobs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    media_type TEXT NOT NULL,
    octet_size BIGINT NOT NULL,
    blob_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS jmap_upload_blobs_account_created_idx
    ON jmap_upload_blobs (tenant_id, account_id, created_at DESC);
