const SCHEMA: &str = include_str!("../sql/schema.sql");
const CHANGE_STORAGE: &str = include_str!("change.rs");
const COLLABORATION_STORAGE: &str = include_str!("collaboration.rs");
const INBOUND_STORAGE: &str = include_str!("inbound.rs");
const MESSAGE_OPS_STORAGE: &str = include_str!("message_ops.rs");
const PROTOCOLS_STORAGE: &str = include_str!("protocols.rs");
const SHARED_STORAGE: &str = include_str!("shared.rs");
const SUBMISSION_STORAGE: &str = include_str!("submission.rs");
const TASKS_STORAGE: &str = include_str!("tasks.rs");
const WORKSPACE_STORAGE: &str = include_str!("workspace.rs");
const ADMIN_STORAGE: &str = include_str!("admin.rs");

fn assert_schema_contains_all(needles: &[&str]) {
    for needle in needles {
        assert!(
            SCHEMA.contains(needle),
            "schema.sql is missing expected collaboration contract fragment: {needle}"
        );
    }
}

fn table_definition(table_name: &str) -> &str {
    let start = SCHEMA
        .find(&format!("CREATE TABLE {table_name}"))
        .unwrap_or_else(|| panic!("schema.sql is missing CREATE TABLE {table_name}"));
    let rest = &SCHEMA[start..];
    let end = rest.find("\n\nCREATE ").unwrap_or(rest.len());
    &rest[..end]
}

fn assert_contains_before(haystack: &str, first: &str, second: &str, message: &str) {
    let first_index = haystack
        .find(first)
        .unwrap_or_else(|| panic!("{message}: missing {first}"));
    let second_index = haystack
        .find(second)
        .unwrap_or_else(|| panic!("{message}: missing {second}"));
    assert!(first_index < second_index, "{message}");
}

fn function_body<'a>(source: &'a str, signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("missing function signature: {signature}"));
    let rest = &source[start..];
    let next = rest.find("\n    pub ").unwrap_or(rest.len());
    &rest[..next]
}

#[test]
fn collaboration_objects_have_canonical_projection_fields() {
    assert_schema_contains_all(&[
        "CREATE TABLE contact_books",
        "CREATE TABLE contacts",
        "name_prefix TEXT NOT NULL DEFAULT ''",
        "given_name TEXT NOT NULL DEFAULT ''",
        "middle_name TEXT NOT NULL DEFAULT ''",
        "family_name TEXT NOT NULL DEFAULT ''",
        "name_suffix TEXT NOT NULL DEFAULT ''",
        "emails_json JSONB NOT NULL DEFAULT '[]'::jsonb",
        "phones_json JSONB NOT NULL DEFAULT '[]'::jsonb",
        "organization_name TEXT NOT NULL DEFAULT ''",
        "organization_unit TEXT NOT NULL DEFAULT ''",
        "raw_vcard TEXT",
        "import_source TEXT NOT NULL DEFAULT 'local'",
        "source_uid TEXT",
        "source_etag TEXT",
        "CREATE TABLE calendars",
        "CREATE TABLE calendar_events",
        "uid TEXT NOT NULL CHECK (btrim(uid) <> '')",
        "sequence INTEGER NOT NULL DEFAULT 0 CHECK (sequence >= 0)",
        "organizer_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "attendees_json JSONB NOT NULL DEFAULT '[]'::jsonb",
        "recurrence_rule TEXT",
        "recurrence_exceptions_json JSONB NOT NULL DEFAULT '[]'::jsonb",
        "time_zone TEXT NOT NULL DEFAULT ''",
        "location TEXT NOT NULL DEFAULT ''",
        "body_text TEXT NOT NULL DEFAULT ''",
        "CREATE TABLE task_lists",
        "CREATE TABLE tasks",
        "starts_at TIMESTAMPTZ",
        "due_at TIMESTAMPTZ",
        "completed_at TIMESTAMPTZ",
        "priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 9)",
        "recurrence_json JSONB NOT NULL DEFAULT '{}'::jsonb",
    ]);
}

#[test]
fn collaboration_rights_are_canonical_and_same_tenant() {
    assert_schema_contains_all(&[
        "CREATE TABLE contact_book_grants",
        "CREATE TABLE calendar_grants",
        "CREATE TABLE task_list_grants",
        "CREATE TABLE mailbox_delegation_grants",
        "CREATE TABLE sender_rights",
        "FOREIGN KEY (tenant_id, grantee_account_id) REFERENCES accounts (tenant_id, id)",
        "CHECK (owner_account_id <> grantee_account_id)",
        "sender_right TEXT NOT NULL CHECK (sender_right IN ('send_as', 'send_on_behalf'))",
    ]);

    for forbidden in [
        "dav_grants",
        "dav_acl",
        "ews_grants",
        "ews_acl",
        "mapi_grants",
        "mapi_acl",
    ] {
        assert!(
            !SCHEMA.contains(forbidden),
            "schema.sql must not introduce protocol-local rights table: {forbidden}"
        );
    }
}

#[test]
fn sender_delegation_storage_uses_canonical_sender_rights_table() {
    assert!(
        SUBMISSION_STORAGE.contains("FROM sender_rights")
            && SUBMISSION_STORAGE.contains("INSERT INTO sender_rights")
            && SUBMISSION_STORAGE.contains("DELETE FROM sender_rights"),
        "sender delegation storage must use the canonical sender_rights table"
    );
    for retired_table_reference in [
        "FROM sender_delegation_grants",
        "INSERT INTO sender_delegation_grants",
        "DELETE FROM sender_delegation_grants",
    ] {
        assert!(
            !SUBMISSION_STORAGE.contains(retired_table_reference),
            "sender delegation storage must not query the retired sender_delegation_grants table"
        );
    }
}

#[test]
fn collaboration_grant_storage_uses_concrete_grant_tables() {
    for source in [COLLABORATION_STORAGE, CHANGE_STORAGE] {
        assert!(
            source.contains("contact_book_grants")
                && source.contains("calendar_grants")
                && source.contains("task_list_grants"),
            "collaboration storage must use canonical contact, calendar, and task grant tables"
        );
        assert!(
            !source.contains("collaboration_collection_grants"),
            "collaboration storage must not query the retired generic collaboration_collection_grants table"
        );
    }
    assert!(
        COLLABORATION_STORAGE.contains("Self::ensure_default_task_list")
            && !COLLABORATION_STORAGE.contains("task-list grants require a task list id")
            && !COLLABORATION_STORAGE.contains("task collections use task-list grants"),
        "generic task collaboration grants must project to the canonical default task list"
    );
}

#[test]
fn grant_changes_emit_canonical_rights_journal_entries() {
    assert!(
        CHANGE_STORAGE.contains("pub(crate) async fn emit_mail_delegation_change")
            && CHANGE_STORAGE.contains("pub(crate) async fn emit_collaboration_grant_change")
            && CHANGE_STORAGE
                .matches("CanonicalChangeCategory::Rights")
                .count()
                >= 3,
        "grant changes must emit canonical rights journal entries as well as object changes"
    );
    let collaboration_grant_change = CHANGE_STORAGE
        .split("pub(crate) async fn emit_collaboration_grant_change")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub(crate) async fn emit_task_access_change")
                .next()
        })
        .unwrap_or_default();
    assert!(
        collaboration_grant_change.contains("CanonicalChangeCategory::Rights"),
        "collaboration grant changes must emit rights journal entries"
    );
}

#[test]
fn grant_changes_emit_object_level_mail_change_log_entries() {
    assert!(
        COLLABORATION_STORAGE.contains("insert_mail_change_log_in_tx")
            && COLLABORATION_STORAGE.contains("\"contact_book_grant\"")
            && COLLABORATION_STORAGE.contains("\"calendar_grant\"")
            && COLLABORATION_STORAGE.contains("\"task_list_grant\"")
            && TASKS_STORAGE.contains("insert_mail_change_log_in_tx")
            && TASKS_STORAGE.contains("\"task_list_grant\"")
            && SUBMISSION_STORAGE.contains("insert_mail_change_log_in_tx")
            && SUBMISSION_STORAGE.contains("\"mailbox_delegation_grant\"")
            && SUBMISSION_STORAGE.contains("\"sender_right\""),
        "grant upsert/delete paths must write object-level mail_change_log entries"
    );
}

#[test]
fn collaboration_changes_and_tombstones_are_object_level() {
    assert_schema_contains_all(&[
        "CREATE TABLE mail_change_log",
        "CREATE TABLE tombstones",
        "CREATE TABLE canonical_change_journal",
        "'mailbox'",
        "'contact_book'",
        "'contact'",
        "'calendar'",
        "'calendar_event'",
        "'task_list'",
        "'task'",
        "'contact_book_grant'",
        "'calendar_grant'",
        "'task_list_grant'",
        "'mailbox_delegation_grant'",
        "'sender_right'",
        "category TEXT NOT NULL CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'rights'))",
        "affected_principal_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[]",
        "principal_account_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[]",
    ]);
}

#[test]
fn collaboration_deletes_write_tombstones() {
    assert!(
        CHANGE_STORAGE.contains("pub(crate) async fn insert_collaboration_tombstone_in_tx")
            && CHANGE_STORAGE.contains("INSERT INTO tombstones")
            && SHARED_STORAGE.contains("pub(crate) async fn allocate_account_modseq_in_tx"),
        "storage must provide category-aware collaboration tombstone writes"
    );
    assert!(
        MESSAGE_OPS_STORAGE
            .matches("insert_collaboration_tombstone_in_tx")
            .count()
            >= 2
            && TASKS_STORAGE.contains("insert_collaboration_tombstone_in_tx"),
        "contact, event, and task deletes must write collaboration tombstones"
    );
    assert!(
        PROTOCOLS_STORAGE.contains("INSERT INTO tombstones")
            && PROTOCOLS_STORAGE.contains("'mailbox'")
            && TASKS_STORAGE
                .matches("insert_collaboration_tombstone_in_tx")
                .count()
                >= 2,
        "mailbox and task-list physical deletes must write tombstones before removing rows"
    );
    assert_contains_before(
        function_body(MESSAGE_OPS_STORAGE, "pub async fn delete_client_contact"),
        "insert_collaboration_tombstone_in_tx",
        "DELETE FROM contacts",
        "contact delete must write a tombstone before physical deletion",
    );
    assert_contains_before(
        function_body(MESSAGE_OPS_STORAGE, "pub async fn delete_client_event"),
        "insert_collaboration_tombstone_in_tx",
        "DELETE FROM calendar_events",
        "calendar event delete must write a tombstone before physical deletion",
    );
    assert_contains_before(
        function_body(TASKS_STORAGE, "pub async fn delete_client_task"),
        "insert_collaboration_tombstone_in_tx",
        "DELETE FROM tasks",
        "task delete must write a tombstone before physical deletion",
    );
    assert!(
        TASKS_STORAGE.contains("let mut affected_account_ids = grantee_account_ids.clone();")
            && TASKS_STORAGE.contains("&affected_account_ids")
            && TASKS_STORAGE.contains("&grantee_account_ids"),
        "task deletes must include shared task-list grantees in tombstones and access changes"
    );
}

#[test]
fn attachment_metadata_changes_write_mail_change_log_entries() {
    assert!(
        PROTOCOLS_STORAGE.contains("pub async fn delete_message_attachment")
            && PROTOCOLS_STORAGE.contains("pub async fn add_message_attachment")
            && PROTOCOLS_STORAGE.contains("\"attachment\"")
            && PROTOCOLS_STORAGE.contains("\"attachmentId\"")
            && PROTOCOLS_STORAGE.contains("\"created\"")
            && PROTOCOLS_STORAGE.contains("\"destroyed\""),
        "attachment metadata changes must write durable mail_change_log entries"
    );
}

#[test]
fn blob_placement_metadata_is_tenant_domain_and_blob_safe() {
    let storage_pools = table_definition("storage_pools");
    assert!(
        storage_pools.contains("pool_kind TEXT NOT NULL CHECK (pool_kind IN ('postgres'))")
            && storage_pools.contains(
                "status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled'))"
            )
            && storage_pools.contains("UNIQUE (name)"),
        "storage_pools must represent the current database-backed storage pool only"
    );

    let blob_placements = table_definition("blob_placements");
    for required in [
        "blob_kind TEXT NOT NULL CHECK (blob_kind IN ('attachment', 'mime_part'))",
        "placement_status TEXT NOT NULL DEFAULT 'active'",
        "'cleanup_failed'",
        "'deleted'",
        "verified_content_sha256 TEXT NOT NULL CHECK (verified_content_sha256 ~ '^[0-9a-f]{64}$')",
        "verified_size_octets BIGINT NOT NULL CHECK (verified_size_octets >= 0)",
        "rollback_until TIMESTAMPTZ",
        "cleanup_attempts INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_attempts >= 0)",
        "cleanup_claimed_at TIMESTAMPTZ",
        "cleaned_at TIMESTAMPTZ",
        "cleanup_error TEXT",
        "next_cleanup_attempt_at TIMESTAMPTZ",
        "UNIQUE (tenant_id, id)",
        "UNIQUE (tenant_id, domain_id, id, blob_id, blob_kind, storage_pool_id)",
        "CHECK (placement_status IN ('copying', 'failed') OR verified_at IS NOT NULL)",
        "CHECK (rollback_until IS NULL OR placement_status IN ('retiring', 'cleaning', 'cleanup_failed', 'deleted'))",
        "CHECK (cleaned_at IS NULL OR placement_status = 'deleted')",
        "CHECK (next_cleanup_attempt_at IS NULL OR placement_status = 'cleanup_failed')",
        "FOREIGN KEY (",
        "tenant_id,",
        "domain_id,",
        "blob_id,",
        "blob_kind,",
        "verified_content_sha256,",
        "verified_size_octets",
        "REFERENCES blobs (",
        "content_sha256,",
        "size_octets",
        "FOREIGN KEY (storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT",
    ] {
        assert!(
            blob_placements.contains(required),
            "blob_placements is missing required placement contract fragment: {required}"
        );
    }
    assert!(
        !blob_placements.contains("'raw_message'"),
        "raw RFC 5322 blobs must not require placement metadata in Milestone 2"
    );

    assert_schema_contains_all(&[
        "UNIQUE (tenant_id, domain_id, id, blob_kind, content_sha256, size_octets)",
        "INSERT INTO storage_pools (id, name, pool_kind)",
        "'postgres-primary', 'postgres'",
        "CREATE UNIQUE INDEX blob_placements_active_idx",
        "ON blob_placements (tenant_id, domain_id, blob_id)",
        "WHERE placement_status = 'active'",
        "CREATE UNIQUE INDEX blob_placements_live_pool_idx",
        "ON blob_placements (tenant_id, domain_id, blob_id, storage_pool_id)",
        "WHERE placement_status IN ('active', 'copying', 'verified', 'retiring')",
        "CREATE INDEX blob_placements_fetch_idx",
        "ON blob_placements (tenant_id, domain_id, blob_id, blob_kind)",
        "CREATE INDEX blob_placements_status_idx",
        "CREATE INDEX blob_placements_pool_status_idx",
        "CREATE INDEX blob_placements_cleanup_due_idx",
        "WHERE placement_status IN ('retiring', 'cleanup_failed')",
    ]);

    for unsupported_backend in ["s3", "aws", "azure", "cloud", "bucket"] {
        assert!(
            !storage_pools
                .to_ascii_lowercase()
                .contains(unsupported_backend)
                && !blob_placements
                    .to_ascii_lowercase()
                    .contains(unsupported_backend),
            "Milestone 2 schema must not introduce backend config for {unsupported_backend}"
        );
    }
}

#[test]
fn storage_policy_assignments_capture_milestone_five_scope_contract() {
    let policies = table_definition("storage_policy_assignments");
    for required in [
        "scope_kind TEXT NOT NULL CHECK (scope_kind IN ('platform', 'tenant', 'domain', 'account'))",
        "tenant_id UUID",
        "domain_id UUID",
        "account_id UUID",
        "storage_pool_id UUID NOT NULL",
        "updated_by TEXT NOT NULL CHECK (btrim(updated_by) <> '')",
        "FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE",
        "FOREIGN KEY (tenant_id, domain_id) REFERENCES domains (tenant_id, id) ON DELETE CASCADE",
        "FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
        "FOREIGN KEY (storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT",
    ] {
        assert!(
            policies.contains(required),
            "storage_policy_assignments is missing required Milestone 5 policy fragment: {required}"
        );
    }

    assert_schema_contains_all(&[
        "CREATE UNIQUE INDEX storage_policy_platform_idx",
        "WHERE scope_kind = 'platform'",
        "CREATE UNIQUE INDEX storage_policy_tenant_idx",
        "WHERE scope_kind = 'tenant'",
        "CREATE UNIQUE INDEX storage_policy_domain_idx",
        "WHERE scope_kind = 'domain'",
        "CREATE UNIQUE INDEX storage_policy_account_idx",
        "WHERE scope_kind = 'account'",
        "CREATE INDEX storage_policy_pool_idx",
        "INSERT INTO storage_policy_assignments (id, scope_kind, storage_pool_id, updated_by)",
    ]);

    for forbidden in ["s3", "aws", "azure", "cloud", "bucket", "mailbox_id"] {
        assert!(
            !policies.to_ascii_lowercase().contains(forbidden),
            "Milestone 5 storage policy must not introduce forbidden scope/backend fragment: {forbidden}"
        );
    }
}

#[test]
fn blob_migration_jobs_capture_milestone_three_worker_contract() {
    let jobs = table_definition("blob_migration_jobs");
    for required in [
        "blob_kind TEXT NOT NULL CHECK (blob_kind IN ('attachment', 'mime_part'))",
        "job_kind TEXT NOT NULL DEFAULT 'placement_migration' CHECK (job_kind = 'placement_migration')",
        "source_placement_id UUID NOT NULL",
        "source_storage_pool_id UUID NOT NULL",
        "target_storage_pool_id UUID NOT NULL",
        "target_placement_id UUID",
        "status TEXT NOT NULL DEFAULT 'pending'",
        "CHECK (status IN ('pending', 'running', 'verified', 'switched', 'failed', 'cancelled'))",
        "attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)",
        "next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "lease_expires_at TIMESTAMPTZ",
        "rollback_until TIMESTAMPTZ",
        "UNIQUE (tenant_id, id)",
        "CHECK (source_storage_pool_id <> target_storage_pool_id)",
        "CHECK (target_placement_id IS NULL OR target_placement_id <> source_placement_id)",
        "CHECK (status <> 'running' OR (started_at IS NOT NULL AND lease_expires_at IS NOT NULL))",
        "CHECK (status NOT IN ('verified', 'switched') OR target_placement_id IS NOT NULL)",
        "CHECK (status <> 'switched' OR (switched_at IS NOT NULL AND rollback_until IS NOT NULL))",
        "CHECK (status <> 'cancelled' OR cancelled_at IS NOT NULL)",
        "REFERENCES blob_placements (",
        "storage_pool_id",
        "ON DELETE RESTRICT",
        "FOREIGN KEY (source_storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT",
        "FOREIGN KEY (target_storage_pool_id) REFERENCES storage_pools (id) ON DELETE RESTRICT",
    ] {
        assert!(
            jobs.contains(required),
            "blob_migration_jobs is missing required Milestone 3 contract fragment: {required}"
        );
    }
    assert!(
        !jobs.contains("'raw_message'"),
        "raw RFC 5322 blobs must stay out of migration jobs"
    );

    for required in [
        "tenant_id,\n        domain_id,\n        source_placement_id,\n        blob_id,\n        blob_kind,\n        source_storage_pool_id",
        "tenant_id,\n        domain_id,\n        target_placement_id,\n        blob_id,\n        blob_kind,\n        target_storage_pool_id",
        "CREATE UNIQUE INDEX blob_migration_jobs_open_target_idx",
        "ON blob_migration_jobs (tenant_id, domain_id, blob_id, target_storage_pool_id)",
        "WHERE status IN ('pending', 'running', 'verified')",
        "CREATE INDEX blob_migration_jobs_pending_idx",
        "ON blob_migration_jobs (next_attempt_at, created_at, id)",
        "WHERE status = 'pending'",
        "CREATE INDEX blob_migration_jobs_running_lease_idx",
        "ON blob_migration_jobs (lease_expires_at, started_at)",
        "WHERE status = 'running'",
        "CREATE INDEX blob_migration_jobs_blob_idx",
        "ON blob_migration_jobs (tenant_id, domain_id, blob_id, created_at DESC)",
        "CREATE INDEX blob_migration_jobs_source_placement_idx",
        "ON blob_migration_jobs (tenant_id, source_placement_id)",
        "CREATE INDEX blob_migration_jobs_target_placement_idx",
        "ON blob_migration_jobs (tenant_id, target_placement_id)",
        "WHERE target_placement_id IS NOT NULL",
    ] {
        assert!(
            SCHEMA.contains(required),
            "schema.sql is missing required migration job index or ownership fragment: {required}"
        );
    }

    for unsupported_backend in ["s3", "aws", "azure", "cloud", "bucket"] {
        assert!(
            !jobs.to_ascii_lowercase().contains(unsupported_backend),
            "Milestone 3 migration jobs must not introduce backend config for {unsupported_backend}"
        );
    }
}

#[test]
fn blob_and_message_lifecycle_metadata_support_cleanup_guards() {
    let blobs = table_definition("blobs");
    for required in [
        "retained_until TIMESTAMPTZ",
        "legal_hold BOOLEAN NOT NULL DEFAULT FALSE",
        "CHECK (retained_until IS NULL OR retained_until >= created_at)",
    ] {
        assert!(
            blobs.contains(required),
            "blobs is missing required lifecycle guard fragment: {required}"
        );
    }
    let messages = table_definition("messages");
    for required in [
        "retained_until TIMESTAMPTZ",
        "legal_hold BOOLEAN NOT NULL DEFAULT FALSE",
        "CHECK (retained_until IS NULL OR retained_until >= created_at)",
    ] {
        assert!(
            messages.contains(required),
            "messages is missing required lifecycle guard fragment: {required}"
        );
    }
    assert_schema_contains_all(&[
        "CREATE INDEX blobs_lifecycle_protection_idx",
        "WHERE retained_until IS NOT NULL OR legal_hold = TRUE",
        "CREATE INDEX messages_lifecycle_protection_idx",
    ]);
}

#[test]
fn existing_draft_updates_write_mailbox_message_change_log_entries() {
    assert!(
        SUBMISSION_STORAGE.contains("existing_draft_update")
            && SUBMISSION_STORAGE.contains("insert_mail_change_log_in_tx")
            && SUBMISSION_STORAGE.contains("\"mailbox_message\"")
            && SUBMISSION_STORAGE.contains("\"updated\"")
            && SUBMISSION_STORAGE.contains("\"threadId\""),
        "existing draft updates must write durable mailbox_message change rows"
    );
}

#[test]
fn imap_uid_state_is_mailbox_scoped_without_global_sequence() {
    assert_schema_contains_all(&[
        "uid_validity BIGINT NOT NULL CHECK (uid_validity > 0)",
        "uid_next BIGINT NOT NULL DEFAULT 1 CHECK (uid_next > 0)",
    ]);
    for required in [
        "SET uid_next = uid_next + 1",
        "RETURNING uid_next - 1 AS imap_uid",
    ] {
        assert!(
            SHARED_STORAGE.contains(required) || MESSAGE_OPS_STORAGE.contains(required),
            "storage must contain mailbox-scoped UID allocation fragment: {required}"
        );
    }
    assert!(
        !SCHEMA.contains("message_imap_uid_seq"),
        "schema.sql must not use the retired global message_imap_uid_seq"
    );
    assert!(
        !SHARED_STORAGE.contains("MAX(imap_uid)")
            && !PROTOCOLS_STORAGE.contains("MAX(imap_uid)")
            && !MESSAGE_OPS_STORAGE.contains("MAX(imap_uid)"),
        "storage must allocate UIDNEXT from mailbox uid_next, not visible max UID"
    );
    assert!(
        SHARED_STORAGE.contains("pub(crate) fn allocate_uid_validity() -> i64")
            && ADMIN_STORAGE.contains("allocate_uid_validity()")
            && INBOUND_STORAGE.contains("allocate_uid_validity()")
            && PROTOCOLS_STORAGE.contains("allocate_uid_validity()"),
        "new mailbox paths must share the mailbox UIDVALIDITY allocator"
    );
    assert!(
        !ADMIN_STORAGE.contains("fn mailbox_uid_validity")
            && !INBOUND_STORAGE.contains("fn mailbox_uid_validity")
            && !PROTOCOLS_STORAGE.contains("fn mailbox_uid_validity"),
        "new mailbox paths must not carry protocol-local UIDVALIDITY helpers"
    );
}

#[test]
fn jmap_mail_changes_have_durable_replay_path() {
    assert!(
        PROTOCOLS_STORAGE.contains("pub async fn replay_jmap_mail_object_changes")
            && PROTOCOLS_STORAGE.contains("pub async fn replay_jmap_object_changes")
            && PROTOCOLS_STORAGE.contains("FROM mail_change_log")
            && PROTOCOLS_STORAGE.contains("sourceMailboxId")
            && PROTOCOLS_STORAGE.contains("messageId")
            && PROTOCOLS_STORAGE.contains("threadId")
            && PROTOCOLS_STORAGE.contains("\"Thread\"")
            && PROTOCOLS_STORAGE.contains("\"EmailSubmission\"")
            && PROTOCOLS_STORAGE.contains("jmap_object_replay_kinds"),
        "JMAP Mailbox/changes, Email/changes, Thread/changes, EmailSubmission/changes, and collaboration changes need durable mail_change_log replay paths"
    );
    assert!(
        PROTOCOLS_STORAGE.contains("fn jmap_replay_object_id")
            && PROTOCOLS_STORAGE.contains("contact_book_grant")
            && PROTOCOLS_STORAGE.contains("calendar_grant")
            && PROTOCOLS_STORAGE.contains("task_list_grant")
            && PROTOCOLS_STORAGE.contains("collectionId"),
        "collection-level JMAP changes must replay grant rows through durable collection ids"
    );
}

#[test]
fn bcc_is_absent_from_search_log_cursor_and_ai_projection_tables() {
    assert!(
        SCHEMA.contains("CREATE TABLE protected_bcc_recipients"),
        "Bcc must remain in the explicit protected metadata table"
    );
    for table_name in [
        "mail_search_documents",
        "document_projections",
        "document_chunks",
        "mail_change_log",
        "jmap_query_states",
        "activesync_sync_cursors",
        "mapi_sync_checkpoints",
    ] {
        let definition = table_definition(table_name).to_ascii_lowercase();
        assert!(
            !definition.contains("bcc"),
            "{table_name} must not carry Bcc columns or Bcc-named payloads"
        );
    }
}

#[test]
fn protocol_cursor_tables_do_not_store_canonical_content() {
    for table_name in [
        "jmap_query_states",
        "activesync_sync_cursors",
        "mapi_sync_checkpoints",
    ] {
        let definition = table_definition(table_name).to_ascii_lowercase();
        for forbidden in [
            "subject_text",
            "body_text",
            "attachment_text",
            "search_vector",
            "raw_mime",
            "message_rfc822",
            "participants_visible",
            "protected_bcc",
        ] {
            assert!(
                !definition.contains(forbidden),
                "{table_name} must stay a protocol cursor/checkpoint table, not a canonical content store"
            );
        }
    }
}

#[test]
fn core_schema_excludes_lpe_ct_quarantine_and_perimeter_tables() {
    for retired in [
        "antispam_quarantine",
        "antispam_filter_rules",
        "CREATE TABLE antispam_settings",
        "CREATE TABLE greylisting",
        "CREATE TABLE reputation",
        "CREATE TABLE bayesian",
    ] {
        assert!(
            !SCHEMA.contains(retired),
            "schema.sql must not contain LPE-CT perimeter state: {retired}"
        );
    }
    for retired_query in [
        "FROM antispam_quarantine",
        "FROM antispam_filter_rules",
        "FROM antispam_settings",
        "INSERT INTO antispam_filter_rules",
        "INSERT INTO antispam_settings",
    ] {
        assert!(
            !ADMIN_STORAGE.contains(retired_query),
            "admin storage must not query LPE-CT perimeter state: {retired_query}"
        );
    }
}

#[test]
fn activesync_sync_state_uses_v2_cursor_table() {
    assert_schema_contains_all(&[
        "CREATE TABLE activesync_sync_cursors",
        "collection_kind TEXT NOT NULL CHECK (collection_kind IN ('folders', 'mail', 'contacts', 'calendar', 'tasks'))",
        "collection_key TEXT NOT NULL CHECK (btrim(collection_key) <> '')",
        "last_change_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_change_sequence >= 0)",
        "state_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "UNIQUE (tenant_id, account_id, device_id, collection_kind, collection_key)",
    ]);
    assert!(
        !SCHEMA.contains("activesync_sync_states"),
        "schema.sql must not define the retired ActiveSync snapshot table"
    );
    assert!(
        PROTOCOLS_STORAGE.contains("INSERT INTO activesync_sync_cursors")
            && PROTOCOLS_STORAGE.contains("state_json")
            && !PROTOCOLS_STORAGE.contains("activesync_sync_states")
            && !MESSAGE_OPS_STORAGE.contains("activesync_sync_states"),
        "ActiveSync storage must use v2 cursor rows, not the retired snapshot table"
    );
}

#[test]
fn runtime_collaboration_sql_uses_canonical_v2_columns() {
    for (name, source) in [
        ("workspace.rs", WORKSPACE_STORAGE),
        ("collaboration.rs", COLLABORATION_STORAGE),
        ("message_ops.rs", MESSAGE_OPS_STORAGE),
        ("protocols.rs", PROTOCOLS_STORAGE),
    ] {
        assert!(
            source.contains("owner_account_id"),
            "{name} must use owner_account_id for collaboration ownership"
        );
    }

    for (name, source, retired) in [
        ("workspace.rs", WORKSPACE_STORAGE, "contacts.account_id"),
        (
            "workspace.rs",
            WORKSPACE_STORAGE,
            "calendar_events.account_id",
        ),
        ("workspace.rs", WORKSPACE_STORAGE, "event_date"),
        ("workspace.rs", WORKSPACE_STORAGE, "event_time"),
        ("collaboration.rs", COLLABORATION_STORAGE, "l.account_id"),
        ("tasks.rs", TASKS_STORAGE, "task_lists.account_id"),
        ("tasks.rs", TASKS_STORAGE, "tasks.account_id"),
        (
            "tasks.rs",
            TASKS_STORAGE,
            "ON CONFLICT (tenant_id, account_id, role)",
        ),
    ] {
        assert!(
            !source.contains(retired),
            "{name} must not query retired collaboration column fragment: {retired}"
        );
    }

    for (name, source, canonical) in [
        ("workspace.rs", WORKSPACE_STORAGE, "emails_json"),
        ("workspace.rs", WORKSPACE_STORAGE, "phones_json"),
        ("workspace.rs", WORKSPACE_STORAGE, "starts_at"),
        ("workspace.rs", WORKSPACE_STORAGE, "ends_at"),
        ("tasks.rs", TASKS_STORAGE, "task_lists.owner_account_id"),
        ("tasks.rs", TASKS_STORAGE, "tasks.owner_account_id"),
        ("tasks.rs", TASKS_STORAGE, "uid"),
    ] {
        assert!(
            source.contains(canonical),
            "{name} must query canonical collaboration column fragment: {canonical}"
        );
    }
}
