CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    primary_email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX accounts_tenant_primary_email_idx
    ON accounts (tenant_id, primary_email);

CREATE TABLE mailboxes (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    display_name TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX mailboxes_account_role_idx
    ON mailboxes (account_id, role);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    mailbox_id UUID NOT NULL REFERENCES mailboxes(id) ON DELETE CASCADE,
    thread_id UUID NOT NULL,
    internet_message_id TEXT,
    received_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    from_display TEXT,
    from_address TEXT NOT NULL,
    subject_normalized TEXT NOT NULL,
    preview_text TEXT NOT NULL,
    unread BOOLEAN NOT NULL DEFAULT TRUE,
    flagged BOOLEAN NOT NULL DEFAULT FALSE,
    has_attachments BOOLEAN NOT NULL DEFAULT FALSE,
    size_octets BIGINT NOT NULL DEFAULT 0,
    mime_blob_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX messages_mailbox_received_idx
    ON messages (account_id, mailbox_id, received_at DESC);

CREATE INDEX messages_account_thread_idx
    ON messages (account_id, thread_id);

CREATE INDEX messages_unread_partial_idx
    ON messages (account_id, mailbox_id, received_at DESC)
    WHERE unread = TRUE;

CREATE INDEX messages_flagged_partial_idx
    ON messages (account_id, mailbox_id, received_at DESC)
    WHERE flagged = TRUE;

CREATE TABLE message_bodies (
    message_id UUID PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    body_text TEXT NOT NULL,
    body_html_sanitized TEXT,
    participants_normalized TEXT NOT NULL,
    language_code TEXT,
    content_hash TEXT NOT NULL,
    search_vector TSVECTOR NOT NULL
);

CREATE INDEX message_bodies_search_vector_idx
    ON message_bodies USING GIN (search_vector);

CREATE INDEX message_bodies_body_text_trgm_idx
    ON message_bodies USING GIN (body_text gin_trgm_ops);

CREATE TABLE attachments (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size_octets BIGINT NOT NULL,
    blob_ref TEXT NOT NULL,
    extracted_text TEXT,
    extracted_text_tsv TSVECTOR
);

CREATE INDEX attachments_message_idx
    ON attachments (message_id);

CREATE INDEX attachments_extracted_text_tsv_idx
    ON attachments USING GIN (extracted_text_tsv);

CREATE TABLE document_projections (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source_object_id UUID NOT NULL,
    source_kind TEXT NOT NULL,
    owner_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    acl_fingerprint TEXT NOT NULL,
    title TEXT NOT NULL,
    preview TEXT NOT NULL,
    body_text TEXT NOT NULL,
    language_code TEXT,
    participants_normalized TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    search_vector TSVECTOR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX document_projections_owner_kind_idx
    ON document_projections (owner_account_id, source_kind, updated_at DESC);

CREATE INDEX document_projections_search_vector_idx
    ON document_projections USING GIN (search_vector);

CREATE TABLE document_chunks (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES document_projections(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    token_estimate INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX document_chunks_document_ordinal_idx
    ON document_chunks (document_id, ordinal);

CREATE TABLE document_annotations (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES document_projections(id) ON DELETE CASCADE,
    annotation_type TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    model_name TEXT,
    created_by_account_id UUID REFERENCES accounts(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX document_annotations_document_type_idx
    ON document_annotations (document_id, annotation_type, created_at DESC);

CREATE TABLE inference_runs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    principal_account_id UUID REFERENCES accounts(id) ON DELETE SET NULL,
    model_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_payload JSONB NOT NULL,
    response_payload JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX inference_runs_tenant_started_idx
    ON inference_runs (tenant_id, started_at DESC);

CREATE TABLE inference_run_chunks (
    inference_run_id UUID NOT NULL REFERENCES inference_runs(id) ON DELETE CASCADE,
    chunk_id UUID NOT NULL REFERENCES document_chunks(id) ON DELETE CASCADE,
    PRIMARY KEY (inference_run_id, chunk_id)
);

CREATE VIEW searchable_mail_documents AS
SELECT
    m.id AS message_id,
    m.account_id,
    m.mailbox_id,
    m.received_at,
    m.subject_normalized,
    mb.search_vector AS message_search_vector,
    COALESCE(
        to_tsvector('simple', string_agg(a.extracted_text, ' ' ORDER BY a.file_name)),
        ''::tsvector
    ) AS attachment_search_vector
FROM messages m
JOIN message_bodies mb ON mb.message_id = m.id
LEFT JOIN attachments a ON a.message_id = m.id
GROUP BY m.id, m.account_id, m.mailbox_id, m.received_at, m.subject_normalized, mb.search_vector;
