const SCHEMA: &str = include_str!("../sql/schema.sql");
const CHANGE_STORAGE: &str = include_str!("change.rs");
const COLLABORATION_STORAGE: &str = include_str!("collaboration.rs");
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
            && TASKS_STORAGE.matches("insert_collaboration_tombstone_in_tx").count() >= 2,
        "mailbox and task-list physical deletes must write tombstones before removing rows"
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
