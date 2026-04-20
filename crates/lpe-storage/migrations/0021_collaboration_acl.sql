CREATE TABLE IF NOT EXISTS collaboration_collection_grants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    collection_kind TEXT NOT NULL CHECK (collection_kind IN ('contacts', 'calendar')),
    owner_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    grantee_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    may_read BOOLEAN NOT NULL DEFAULT TRUE,
    may_write BOOLEAN NOT NULL DEFAULT FALSE,
    may_delete BOOLEAN NOT NULL DEFAULT FALSE,
    may_share BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, collection_kind, owner_account_id, grantee_account_id),
    CHECK (owner_account_id <> grantee_account_id),
    CHECK (may_read OR (NOT may_write AND NOT may_delete AND NOT may_share)),
    CHECK ((NOT may_delete) OR may_write),
    CHECK ((NOT may_share) OR may_write)
);

CREATE INDEX IF NOT EXISTS collaboration_collection_grants_grantee_idx
    ON collaboration_collection_grants (tenant_id, collection_kind, grantee_account_id, owner_account_id);

CREATE INDEX IF NOT EXISTS collaboration_collection_grants_owner_idx
    ON collaboration_collection_grants (tenant_id, collection_kind, owner_account_id, grantee_account_id);
