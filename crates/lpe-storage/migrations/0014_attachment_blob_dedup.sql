CREATE TABLE IF NOT EXISTS attachment_blobs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    domain_name TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size_octets BIGINT NOT NULL,
    blob_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS attachment_blobs_domain_hash_idx
    ON attachment_blobs (tenant_id, domain_name, content_sha256);

ALTER TABLE attachments
    ADD COLUMN IF NOT EXISTS attachment_blob_id UUID REFERENCES attachment_blobs(id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS attachments_blob_id_idx
    ON attachments (attachment_blob_id);
