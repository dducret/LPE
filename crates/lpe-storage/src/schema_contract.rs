const SCHEMA: &str = include_str!("../sql/schema.sql");
const ACTIVESYNC_STORAGE: &str = include_str!("activesync.rs");
const ATTACHMENTS_STORAGE: &str = include_str!("attachments.rs");
const BLOB_STORE_STORAGE: &str = include_str!("blob_store.rs");
const CHANGE_STORAGE: &str = include_str!("change.rs");
const COLLABORATION_STORAGE: &str = include_str!("collaboration.rs");
const COLLABORATION_GRANTS_STORAGE: &str = include_str!("collaboration/grants.rs");
const COLLABORATION_TYPES_STORAGE: &str = include_str!("collaboration/types.rs");
const CONVERSATION_ACTIONS_STORAGE: &str = include_str!("conversation_actions.rs");
const CORE_STORAGE: &str = include_str!("core.rs");
const INBOUND_STORAGE: &str = include_str!("inbound.rs");
const IMAP_STORAGE: &str = include_str!("imap.rs");
const JMAP_BLOBS_STORAGE: &str = include_str!("jmap_blobs.rs");
const JMAP_QUERIES_STORAGE: &str = include_str!("jmap_queries.rs");
const MAIL_ITEMS_STORAGE: &str = include_str!("mail_items.rs");
const MAILBOXES_STORAGE: &str = include_str!("mailboxes.rs");
const MAPI_EVENTS_STORAGE: &str = include_str!("mapi_events.rs");
const MESSAGE_OPS_STORAGE: &str = include_str!("message_ops.rs");
const NOTES_JOURNAL_STORAGE: &str = include_str!("notes_journal.rs");
const OUTBOUND_STORAGE: &str = include_str!("outbound.rs");
const PROTOCOLS_STORAGE: &str = include_str!("protocols.rs");
const PST_STORAGE: &str = include_str!("pst.rs");
const PUBLIC_FOLDERS_STORAGE: &str = include_str!("public_folders.rs");
const PUBLIC_FOLDERS_CHANGES_STORAGE: &str = include_str!("public_folders/changes.rs");
const RECOVERABLE_ITEMS_STORAGE: &str = include_str!("recoverable_items.rs");
const SEARCH_FOLDERS_STORAGE: &str = include_str!("search_folders.rs");
const SHARED_STORAGE: &str = include_str!("shared.rs");
const SUBMISSION_STORAGE: &str = include_str!("submission.rs");
const SUBMISSION_DELEGATION_STORAGE: &str = include_str!("submission/delegation.rs");
const SUBMISSION_TYPES_STORAGE: &str = include_str!("submission/types.rs");
const TASKS_STORAGE: &str = include_str!("tasks.rs");
const WORKSPACE_STORAGE: &str = include_str!("workspace.rs");
const ADMIN_STORAGE: &str = include_str!("admin.rs");
const ADMIN_PROVISIONING_STORAGE: &str = include_str!("admin/provisioning.rs");
const AUTH_STORAGE: &str = include_str!("auth.rs");
const EXCHANGE_STORE: &str = include_str!("../../lpe-exchange/src/store.rs");
const EXCHANGE_STORE_MAPI_METADATA: &str =
    include_str!("../../lpe-exchange/src/store/storage_impl/mapi_metadata.rs");
const EXCHANGE_STORE_HELPERS_MAPI: &str =
    include_str!("../../lpe-exchange/src/store/storage_impl/helpers_mapi.rs");
const EXCHANGE_TESTS: &str = include_str!("../../lpe-exchange/src/tests/mapi_over_http.rs");
const EXCHANGE_MAPI_CALENDAR_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/calendar.rs");
const EXCHANGE_MAPI_CONNECT_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/connect.rs");
const EXCHANGE_MAPI_CONTACTS_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/contacts.rs");
const EXCHANGE_MAPI_HIERARCHY_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/hierarchy.rs");
const EXCHANGE_MAPI_PERMISSIONS_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/permissions.rs");
const EXCHANGE_MAPI_PUBLIC_FOLDERS_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/public_folders.rs");
const EXCHANGE_MAPI_SYNC_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/sync.rs");
const EXCHANGE_MAPI_TABLES_TESTS: &str =
    include_str!("../../lpe-exchange/src/tests/mapi_over_http/tables.rs");
const JMAP_TESTS: &str = include_str!("../../lpe-jmap/src/tests.rs");
const IMAP_TESTS: &str = include_str!("../../lpe-imap/src/tests.rs");
const ACTIVESYNC_TESTS: &str = include_str!("../../lpe-activesync/src/tests.rs");
const UPDATE_LPE_SCRIPT: &str = include_str!("../../../installation/debian-trixie/update-lpe.sh");
const CHECK_LPE_SCRIPT: &str = include_str!("../../../installation/debian-trixie/check-lpe.sh");

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

fn submission_storage_contains(needle: &str) -> bool {
    [
        SUBMISSION_STORAGE,
        SUBMISSION_DELEGATION_STORAGE,
        SUBMISSION_TYPES_STORAGE,
    ]
    .iter()
    .any(|source| source.contains(needle))
}

fn collaboration_storage_contains(needle: &str) -> bool {
    [
        COLLABORATION_STORAGE,
        COLLABORATION_GRANTS_STORAGE,
        COLLABORATION_TYPES_STORAGE,
    ]
    .iter()
    .any(|source| source.contains(needle))
}

fn assert_source_contains_all(name: &str, source: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            source.contains(needle),
            "{name} is missing expected canonical adapter coverage: {needle}"
        );
    }
}

fn assert_sources_contain_all(name: &str, sources: &[&str], needles: &[&str]) {
    for needle in needles {
        assert!(
            sources.iter().any(|source| source.contains(needle)),
            "{name} is missing expected canonical adapter coverage: {needle}"
        );
    }
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
        "CREATE TABLE recipient_suggestions",
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
        "CREATE TABLE calendar_event_attachments",
        "FOREIGN KEY (tenant_id, owner_account_id, calendar_id, event_id)",
        "REFERENCES calendar_events (tenant_id, owner_account_id, calendar_id, id)",
        "FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)",
        "REFERENCES blobs (tenant_id, domain_id, id, blob_kind)",
        "CREATE INDEX calendar_event_attachments_event_idx",
        "CREATE INDEX calendar_event_attachments_blob_idx",
        "uid TEXT NOT NULL CHECK (btrim(uid) <> '')",
        "sequence INTEGER NOT NULL DEFAULT 0 CHECK (sequence >= 0)",
        "organizer_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "attendees_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "recurrence_rule TEXT",
        "recurrence_exceptions_json JSONB NOT NULL DEFAULT '[]'::jsonb",
        "time_zone TEXT NOT NULL DEFAULT ''",
        "location TEXT NOT NULL DEFAULT ''",
        "body_text TEXT NOT NULL DEFAULT ''",
        "CREATE TABLE task_lists",
        "CREATE TABLE tasks",
        "CREATE TABLE notes",
        "CREATE TABLE journal_entries",
        "starts_at TIMESTAMPTZ",
        "due_at TIMESTAMPTZ",
        "completed_at TIMESTAMPTZ",
        "reminder_set BOOLEAN NOT NULL DEFAULT FALSE",
        "reminder_at TIMESTAMPTZ",
        "reminder_dismissed_at TIMESTAMPTZ",
        "priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 9)",
        "recurrence_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "color TEXT NOT NULL DEFAULT '' CHECK (color IN ('', 'blue', 'green', 'pink', 'white', 'yellow'))",
        "message_class TEXT NOT NULL DEFAULT 'IPM.Activity'",
        "entry_type TEXT NOT NULL DEFAULT ''",
    ]);
}

#[test]
fn public_folder_schema_uses_canonical_tables_permissions_and_replay() {
    assert_schema_contains_all(&[
        "CREATE TABLE public_folder_trees",
        "CREATE TABLE public_folders",
        "CREATE TABLE public_folder_items",
        "CREATE TABLE public_folder_permissions",
        "CREATE TABLE public_folder_replicas",
        "CREATE TABLE public_folder_per_user_state",
        "public_folder_trees_root_folder_fk",
        "public_folder_items_folder_idx",
        "public_folder_permissions_principal_idx",
        "public_folder_replicas_folder_idx",
        "public_folder_per_user_state_account_idx",
        "'public_folder_tree'",
        "'public_folder'",
        "'public_folder_item'",
        "'public_folder_permission'",
        "'public_folder_replica'",
        "'public_folder_per_user_state'",
        "object_kind IN (\n                'public_folder_tree'",
    ]);
    let public_folder_storage = [PUBLIC_FOLDERS_STORAGE, PUBLIC_FOLDERS_CHANGES_STORAGE].concat();
    assert_source_contains_all(
        "public_folders module",
        &public_folder_storage,
        &[
            "public_folder_access",
            "fetch_public_folder_trees",
            "update_public_folder",
            "delete_public_folder",
            "upsert_public_folder_item",
            "fetch_public_folder_items_by_ids",
            "fetch_public_folder_replicas",
            "upsert_public_folder_replica",
            "fetch_public_folder_per_user_state",
            "patch_public_folder_per_user_state",
            "insert_mail_change_log_in_tx",
            "CanonicalChangeCategory::PublicFolders",
        ],
    );
}

#[test]
fn ews_compatibility_gap_models_are_canonical_sql_state() {
    assert_schema_contains_all(&[
        "CREATE TABLE account_client_configurations",
        "scope_kind TEXT NOT NULL DEFAULT 'account' CHECK (scope_kind IN ('account', 'mailbox', 'public_folder'))",
        "CREATE UNIQUE INDEX account_client_configurations_mailbox_idx",
        "CREATE TABLE delegate_preferences",
        "meeting_request_delivery TEXT NOT NULL DEFAULT 'delegate_and_owner'",
        "CREATE TABLE retention_policy_tags",
        "CREATE TABLE account_retention_policy_assignments",
        "retention_policy_tag_id UUID",
        "mailboxes_retention_policy_tag_fk",
        "CREATE TABLE compliance_cases",
        "CREATE TABLE compliance_holds",
        "CREATE TABLE compliance_hold_mailboxes",
        "CREATE TABLE discovery_searches",
        "CREATE TABLE discovery_search_jobs",
        "CREATE TABLE discovery_result_items",
        "CREATE TABLE non_indexable_item_reports",
        "CREATE TABLE mailbox_item_transfer_jobs",
        "CREATE TABLE mailbox_item_transfer_entries",
        "CREATE TABLE lpe_ct_transport_trace_events",
        "CREATE TRIGGER lpe_ct_transport_trace_events_append_only_update_guard",
        "CREATE TABLE mail_app_catalog",
        "CREATE TABLE mail_app_tenant_policies",
        "CREATE TABLE mail_app_installations",
        "CREATE TABLE mail_app_consents",
        "CREATE TABLE mail_app_token_events",
        "CREATE TABLE unified_messaging_calls",
        "CREATE TABLE contact_groups",
        "CREATE TABLE contact_group_members",
    ]);

    assert!(
        SCHEMA.contains("FOREIGN KEY (tenant_id, submission_queue_id) REFERENCES submission_queue (tenant_id, id)")
            && SCHEMA.contains("event_source TEXT NOT NULL DEFAULT 'lpe-ct' CHECK (event_source = 'lpe-ct')")
            && !SCHEMA.contains("CREATE TABLE ews_user_configurations")
            && !SCHEMA.contains("CREATE TABLE ews_delegate")
            && !SCHEMA.contains("CREATE TABLE ews_message_tracking"),
        "EWS compatibility gaps must use canonical LPE/LPE-CT state, not protocol-local Exchange tables"
    );
}

#[test]
fn calendar_event_attachments_use_canonical_event_and_blob_tables() {
    assert_schema_contains_all(&[
        "CREATE TABLE calendar_event_attachments",
        "blob_kind TEXT NOT NULL DEFAULT 'attachment' CHECK (blob_kind = 'attachment')",
        "UNIQUE (tenant_id, owner_account_id, event_id, ordinal)",
        "FOREIGN KEY (tenant_id, owner_account_id, calendar_id, event_id)",
        "REFERENCES calendar_events (tenant_id, owner_account_id, calendar_id, id)",
        "FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)",
        "REFERENCES blobs (tenant_id, domain_id, id, blob_kind)",
        "CREATE INDEX calendar_event_attachments_event_idx",
        "CREATE INDEX calendar_event_attachments_blob_idx",
    ]);
    assert!(
        ATTACHMENTS_STORAGE.contains("pub async fn delete_calendar_event_attachment")
            && ATTACHMENTS_STORAGE.contains("DELETE FROM calendar_event_attachments")
            && ATTACHMENTS_STORAGE.contains("\"attachmentChanged\": true")
            && ATTACHMENTS_STORAGE.contains("CanonicalChangeCategory::Calendar"),
        "calendar attachment delete must mutate canonical event attachment rows and emit calendar change state"
    );
}

#[test]
fn notes_journal_and_reminders_stay_canonical() {
    assert_schema_contains_all(&[
        "CREATE TABLE notes",
        "CREATE TABLE journal_entries",
        "FOREIGN KEY (tenant_id, owner_account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
        "CREATE INDEX notes_owner_updated_idx",
        "CREATE INDEX journal_entries_owner_time_idx",
        "CREATE INDEX calendar_events_owner_reminder_idx",
        "CREATE INDEX tasks_owner_reminder_idx",
    ]);
    assert!(
        NOTES_JOURNAL_STORAGE.contains("FROM calendar_events")
            && NOTES_JOURNAL_STORAGE.contains("FROM tasks")
            && NOTES_JOURNAL_STORAGE.contains(") mail_reminders")
            && NOTES_JOURNAL_STORAGE.contains("UNION ALL")
            && !SCHEMA.contains("CREATE TABLE reminders"),
        "reminders must be a computed query over canonical reminder-bearing objects, not a table"
    );
    assert!(
        NOTES_JOURNAL_STORAGE.contains("insert_mail_change_log_in_tx")
            && NOTES_JOURNAL_STORAGE.contains("\"note\"")
            && NOTES_JOURNAL_STORAGE.contains("\"journal_entry\"")
            && NOTES_JOURNAL_STORAGE.contains("insert_collaboration_tombstone_in_tx"),
        "notes and journal entries must participate in canonical object replay and tombstones"
    );
    assert!(
        NOTES_JOURNAL_STORAGE.contains("SELECT owner_account_id")
            && NOTES_JOURNAL_STORAGE.contains("FROM notes")
            && NOTES_JOURNAL_STORAGE.contains("FROM journal_entries"),
        "notes and journal upserts must reject ids owned by another account before writing"
    );
}

#[test]
fn mailbox_messages_persist_outlook_followup_state() {
    let mailbox_messages = table_definition("mailbox_messages");
    for needle in [
        "followup_flag_status TEXT NOT NULL DEFAULT 'none'",
        "CHECK (followup_flag_status IN ('none', 'flagged', 'complete'))",
        "followup_icon INTEGER NOT NULL DEFAULT 0 CHECK (followup_icon >= 0)",
        "todo_item_flags INTEGER NOT NULL DEFAULT 0 CHECK (todo_item_flags >= 0)",
        "followup_request TEXT NOT NULL DEFAULT ''",
        "followup_start_at TIMESTAMPTZ",
        "followup_due_at TIMESTAMPTZ",
        "followup_completed_at TIMESTAMPTZ",
        "reminder_set BOOLEAN NOT NULL DEFAULT FALSE",
        "reminder_at TIMESTAMPTZ",
        "reminder_dismissed_at TIMESTAMPTZ",
        "swapped_todo_store_id UUID",
        "swapped_todo_data BYTEA",
        "keywords TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]",
    ] {
        assert!(
            mailbox_messages.contains(needle),
            "mailbox_messages must persist Outlook follow-up flag state: {needle}"
        );
    }
    assert!(
        MESSAGE_OPS_STORAGE.contains("followup_flag_status = COALESCE")
            && MESSAGE_OPS_STORAGE.contains("WHEN $6 IN ('none', 'flagged') THEN NULL")
            && MESSAGE_OPS_STORAGE.contains("WHEN $10::text = '' THEN NULL")
            && MESSAGE_OPS_STORAGE.contains("WHEN $11::text = '' THEN NULL")
            && MESSAGE_OPS_STORAGE.contains("reminder_set = CASE")
            && MESSAGE_OPS_STORAGE.contains("WHEN $14::text = '' THEN NULL")
            && MESSAGE_OPS_STORAGE.contains("keywords = COALESCE")
            && NOTES_JOURNAL_STORAGE.contains("'mail'::text AS source_type")
            && MAIL_ITEMS_STORAGE.contains("followup_flag_status = CASE")
            && PROTOCOLS_STORAGE.contains("categories: Vec<String>")
            && PROTOCOLS_STORAGE.contains("array_agg(to_jsonb(keywords)")
            && MAIL_ITEMS_STORAGE.contains("WHEN $5 THEN 'flagged'")
            && MAIL_ITEMS_STORAGE.contains("JmapEmailFollowupUpdate"),
        "canonical message writes must expose a protocol-neutral follow-up update path"
    );
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
        submission_storage_contains("FROM sender_rights")
            && submission_storage_contains("INSERT INTO sender_rights")
            && submission_storage_contains("DELETE FROM sender_rights"),
        "sender delegation storage must use the canonical sender_rights table"
    );
    for retired_table_reference in [
        "FROM sender_delegation_grants",
        "INSERT INTO sender_delegation_grants",
        "DELETE FROM sender_delegation_grants",
    ] {
        assert!(
            !submission_storage_contains(retired_table_reference),
            "sender delegation storage must not query the retired sender_delegation_grants table"
        );
    }
}

#[test]
fn mapi_permission_mutations_use_canonical_mailbox_delegation_grants() {
    assert!(
        submission_storage_contains("pub async fn set_mailbox_folder_delegation_grant")
            && submission_storage_contains("INSERT INTO mailbox_delegation_grants")
            && submission_storage_contains("DELETE FROM mailbox_delegation_grants")
            && submission_storage_contains("\"mailbox_delegation_grant\"")
            && submission_storage_contains("insert_mail_change_log_in_tx")
            && submission_storage_contains("insert_audit"),
        "MAPI folder permission writes must use canonical mailbox_delegation_grants with audit and change-log rows"
    );
    assert!(
        [EXCHANGE_STORE, EXCHANGE_STORE_MAPI_METADATA]
            .iter()
            .any(|source| source.contains("set_mailbox_folder_delegation_grant"))
            && [EXCHANGE_STORE, EXCHANGE_STORE_MAPI_METADATA]
                .iter()
                .any(|source| source.contains("fetch_mapi_folder_permissions"))
            && EXCHANGE_MAPI_PERMISSIONS_TESTS
                .contains("mapi_over_http_modify_permissions_maps_acl_rows_to_canonical_grants"),
        "MAPI permission ROPs must call the canonical mailbox delegation store path"
    );
    for forbidden in [
        "CREATE TABLE mapi_acl",
        "CREATE TABLE mapi_acls",
        "CREATE TABLE mapi_folder_acl",
        "CREATE TABLE mapi_folder_acls",
    ] {
        assert!(
            !SCHEMA.contains(forbidden),
            "schema.sql must not introduce MAPI-local ACL storage: {forbidden}"
        );
    }
}

#[test]
fn collaboration_grant_storage_uses_concrete_grant_tables() {
    for source in [
        COLLABORATION_STORAGE,
        COLLABORATION_GRANTS_STORAGE,
        CHANGE_STORAGE,
    ] {
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
        collaboration_storage_contains("Self::ensure_default_task_list")
            && !collaboration_storage_contains("task-list grants require a task list id")
            && !collaboration_storage_contains("task collections use task-list grants"),
        "generic task collaboration grants must project to the canonical default task list"
    );
}

#[test]
fn default_task_list_upsert_targets_partial_role_index() {
    assert!(
        SCHEMA.contains("CREATE UNIQUE INDEX task_lists_owner_role_idx")
            && SCHEMA.contains(
                "ON task_lists (tenant_id, owner_account_id, role)\n    WHERE role <> 'custom'"
            ),
        "task list default-role uniqueness must remain a partial index"
    );
    assert!(
        TASKS_STORAGE.contains(
            "ON CONFLICT (tenant_id, owner_account_id, role)\n            WHERE role <> 'custom'"
        ),
        "default task-list bootstrap must target the same partial unique index predicate"
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
        collaboration_storage_contains("insert_mail_change_log_in_tx")
            && collaboration_storage_contains("\"contact_book_grant\"")
            && collaboration_storage_contains("\"calendar_grant\"")
            && collaboration_storage_contains("\"task_list_grant\"")
            && TASKS_STORAGE.contains("insert_mail_change_log_in_tx")
            && TASKS_STORAGE.contains("\"task_list_grant\"")
            && submission_storage_contains("insert_mail_change_log_in_tx")
            && submission_storage_contains("\"mailbox_delegation_grant\"")
            && submission_storage_contains("\"sender_right\""),
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
        "'note'",
        "'journal_entry'",
        "'contact_book_grant'",
        "'calendar_grant'",
        "'task_list_grant'",
        "'mailbox_delegation_grant'",
        "'sender_right'",
        "'search_folder_definition'",
        "'sieve_script'",
        "'conversation_action'",
        "category TEXT NOT NULL CHECK (category IN ('mail', 'contacts', 'calendar', 'tasks', 'notes', 'journal', 'rights', 'search', 'rules', 'conversation_actions', 'public_folders'))",
        "affected_principal_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[]",
        "principal_account_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[]",
    ]);
}

#[test]
fn collaboration_mutations_write_object_level_change_rows() {
    for (label, source, signature, object_kind) in [
        (
            "contact upsert",
            WORKSPACE_STORAGE,
            "pub(crate) async fn upsert_client_contact_in_book_role",
            "\"contact\"",
        ),
        (
            "calendar event upsert",
            WORKSPACE_STORAGE,
            "pub async fn upsert_client_event",
            "\"calendar_event\"",
        ),
        (
            "task upsert",
            TASKS_STORAGE,
            "pub async fn upsert_client_task",
            "\"task\"",
        ),
        (
            "task reminder update",
            TASKS_STORAGE,
            "pub async fn update_accessible_task_reminder",
            "\"task\"",
        ),
        (
            "calendar reminder update",
            COLLABORATION_STORAGE,
            "pub async fn update_accessible_event_reminder",
            "\"calendar_event\"",
        ),
        (
            "task list create",
            TASKS_STORAGE,
            "pub async fn create_task_list",
            "\"task_list\"",
        ),
        (
            "task list update",
            TASKS_STORAGE,
            "pub async fn update_task_list",
            "\"task_list\"",
        ),
    ] {
        let body = function_body(source, signature);
        assert!(
            body.contains("allocate_account_modseq_in_tx")
                && body.contains("insert_mail_change_log_in_tx")
                && body.contains(object_kind)
                && body.contains("emit_"),
            "{label} must append an object-level change row before notifying clients"
        );
    }
}

#[test]
fn replay_logs_tombstones_and_cursors_have_structural_constraints() {
    let change_log = table_definition("mail_change_log");
    for required in [
        "UNIQUE (tenant_id, cursor, object_kind, object_id)",
        "CHECK (jsonb_typeof(summary_json) = 'object')",
        "CHECK (array_position(affected_principal_ids, NULL) IS NULL)",
        "object_kind = 'mailbox'\n            AND account_id IS NOT NULL\n            AND mailbox_id IS NOT NULL",
        "object_kind = 'mailbox_message'",
        "summary_json ? 'messageId'",
        "summary_json ? 'threadId'",
        "summary_json ? 'imapUid'",
        "object_kind = 'submission'",
        "summary_json ? 'status'",
        "'navigation_shortcut'",
    ] {
        assert!(
            change_log.contains(required),
            "mail_change_log must enforce replay row shape: {required}"
        );
    }

    let tombstones = table_definition("tombstones");
    assert!(
        tombstones.contains("FOREIGN KEY (tenant_id, change_cursor, object_kind, object_id)")
            && tombstones
                .contains("REFERENCES mail_change_log (tenant_id, cursor, object_kind, object_id)"),
        "tombstones must point at a matching change-log object row"
    );

    let mapi = table_definition("mapi_sync_checkpoints");
    assert!(
        mapi.contains("CHECK (jsonb_typeof(cursor_json) = 'object')")
            && mapi.contains("(checkpoint_kind = 'hierarchy' AND mailbox_id IS NULL)")
            && mapi
                .contains("(checkpoint_kind IN ('content', 'read_state') AND mailbox_id IS NOT NULL)")
            && !mapi.contains("FOREIGN KEY (tenant_id, account_id, mailbox_id)"),
        "MAPI checkpoints must encode hierarchy as account-wide and content/read-state as folder/scope-scoped"
    );

    assert!(
        CHANGE_STORAGE.contains("pub async fn purge_expired_replay_rows")
            && CHANGE_STORAGE.contains("DELETE FROM tombstones")
            && CHANGE_STORAGE.contains("DELETE FROM mail_change_log")
            && PROTOCOLS_STORAGE.contains("retained_until IS NULL OR retained_until > NOW()"),
        "retained replay cleanup must remove expired tombstone/log rows and replay must ignore unretained rows"
    );
    assert!(
        PROTOCOLS_STORAGE.contains("FROM mail_change_log")
            && ACTIVESYNC_STORAGE.contains("fetch_canonical_change_cursor(account_id)")
            && SCHEMA.contains("CREATE TABLE mapi_sync_checkpoints")
            && SCHEMA.contains("CREATE TABLE mapi_mailbox_replicas")
            && SCHEMA.contains("CREATE TABLE mapi_object_identities")
            && table_definition("mapi_sync_checkpoints").contains("last_change_sequence"),
        "JMAP, ActiveSync, and MAPI cursors must store positions and replay from canonical logs; MAPI identity rows must store protocol IDs without protocol-owned content snapshots"
    );
}

#[test]
fn mapi_identity_mapping_is_store_backed() {
    let replicas = table_definition("mapi_mailbox_replicas");
    for required in [
        "replica_guid UUID NOT NULL",
        "next_global_counter BIGINT NOT NULL DEFAULT 21",
        "PRIMARY KEY (tenant_id, account_id)",
        "UNIQUE (tenant_id, account_id, replica_guid)",
    ] {
        assert!(
            replicas.contains(required),
            "mapi_mailbox_replicas must persist replica allocation state: {required}"
        );
    }

    let identities = table_definition("mapi_object_identities");
    for required in [
        "object_kind TEXT NOT NULL CHECK (object_kind IN ('account', 'mailbox', 'message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'search_folder_definition', 'conversation_action', 'navigation_shortcut', 'associated_config', 'delegate_freebusy_message'))",
        "canonical_id UUID NOT NULL",
        "mapi_global_counter BIGINT NOT NULL",
        "mapi_object_id BIGINT NOT NULL",
        "source_key BYTEA NOT NULL CHECK (octet_length(source_key) = 22)",
        "change_key BYTEA NOT NULL CHECK (octet_length(change_key) = 22)",
        "mapi_change_number BIGINT NOT NULL CHECK (mapi_change_number > 0 AND mapi_change_number <= 140737488355327)",
        "predecessor_change_list BYTEA NOT NULL CHECK (octet_length(predecessor_change_list) > 0)",
        "instance_key BYTEA NOT NULL CHECK (octet_length(instance_key) = 22)",
        "PRIMARY KEY (tenant_id, account_id, object_kind, canonical_id)",
        "UNIQUE (tenant_id, account_id, mapi_global_counter)",
        "UNIQUE (tenant_id, account_id, mapi_object_id)",
    ] {
        assert!(
            identities.contains(required),
            "mapi_object_identities must persist durable MAPI object identity: {required}"
        );
    }

    assert_schema_contains_all(&[
        "CREATE INDEX mapi_object_identities_lookup_idx",
        "CREATE INDEX mapi_object_identities_source_key_idx",
        "CREATE UNIQUE INDEX mapi_object_identities_active_source_key_uidx",
        "WHERE deleted_at IS NULL",
    ]);
}

#[test]
fn calendar_event_mutations_advance_canonical_and_mapi_versions() {
    assert_sources_contain_all(
        "atomic MAPI Event commit helper",
        &[MAPI_EVENTS_STORAGE],
        &[
            "pub async fn fetch_mapi_event_versions",
            "pub async fn commit_mapi_event_update",
            "FOR UPDATE OF event",
            "ObjectModified",
            "advance_calendar_event_version_in_tx",
            "rotate_active_mapi_event_identities_in_tx",
            "mapi_change_number",
            "predecessor_change_list",
            "insert_mail_change_log_in_tx",
            "emit_collaboration_change",
        ],
    );
    for (source, function, behavior) in [
        (
            WORKSPACE_STORAGE,
            "pub(crate) async fn upsert_client_event_in_calendar",
            "canonical Event core writes",
        ),
        (
            COLLABORATION_STORAGE,
            "pub async fn update_accessible_event_reminder",
            "calendar reminder writes",
        ),
        (
            ATTACHMENTS_STORAGE,
            "pub async fn add_calendar_event_attachment",
            "calendar attachment creation",
        ),
        (
            ATTACHMENTS_STORAGE,
            "pub async fn delete_calendar_event_attachment",
            "calendar attachment deletion",
        ),
        (
            COLLABORATION_STORAGE,
            "pub async fn delete_accessible_calendar_collection",
            "calendar deletion Event moves",
        ),
    ] {
        assert!(
            function_body(source, function).contains("advance_calendar_event_version_in_tx"),
            "{behavior} must advance calendar_events.modseq and active MAPI Event identities"
        );
    }
    assert!(
        function_body(MESSAGE_OPS_STORAGE, "pub async fn delete_client_event")
            .contains("retire_mapi_event_identities_in_tx"),
        "calendar Event deletion must retire durable MAPI identities in the deleting transaction"
    );
}

#[test]
fn mapi_navigation_shortcuts_persist_group_header_links() {
    let shortcuts = table_definition("mapi_navigation_shortcuts");
    for required in [
        "target_folder_id BIGINT CHECK (target_folder_id IS NULL OR target_folder_id > 0)",
        "shortcut_type BIGINT NOT NULL CHECK (shortcut_type >= 0 AND shortcut_type <= 4294967295)",
        "save_stamp BIGINT NOT NULL DEFAULT 0 CHECK (save_stamp >= 0 AND save_stamp <= 4294967295)",
        "group_header_id UUID",
        "group_name TEXT NOT NULL DEFAULT ''",
    ] {
        assert!(
            shortcuts.contains(required),
            "mapi_navigation_shortcuts must persist Common Views shortcut group/header state: {required}"
        );
    }
}

#[test]
fn mapi_associated_config_messages_are_bounded_mapi_only_state() {
    let configs = table_definition("mapi_associated_config_messages");
    for required in [
        "folder_id BIGINT NOT NULL CHECK (folder_id > 0)",
        "message_class TEXT NOT NULL CHECK (btrim(message_class) <> '')",
        "subject TEXT NOT NULL CHECK (btrim(subject) <> '')",
        "properties_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "PRIMARY KEY (tenant_id, id)",
    ] {
        assert!(
            configs.contains(required),
            "mapi_associated_config_messages must persist bounded Outlook FAI config state: {required}"
        );
    }
    assert_schema_contains_all(&[
        "CREATE INDEX mapi_associated_config_messages_account_folder_idx",
        "CREATE UNIQUE INDEX mapi_associated_config_messages_logical_idx",
        "ON mapi_associated_config_messages (tenant_id, account_id, folder_id, message_class, subject)",
    ]);
}

#[test]
fn mapi_delegate_freebusy_messages_are_computed_from_canonical_state() {
    assert!(
        !SCHEMA.contains("CREATE TABLE mapi_delegate_freebusy_messages"),
        "delegate/free-busy projections must not introduce MAPI-local storage"
    );
    assert_source_contains_all(
        "collaboration storage",
        COLLABORATION_STORAGE,
        &[
            "project_delegate_freebusy_messages",
            "fetch_delegate_freebusy_messages",
            "compute_delegate_freebusy_messages",
            "fetch_delegate_access_objects",
            "fetch_free_busy_blocks",
            "delegate_freebusy_message_objects",
        ],
    );
    assert!(
        !COLLABORATION_STORAGE.contains("mapi_delegate_freebusy_messages"),
        "delegate/free-busy storage must stay computed from calendar grants, sender rights, accounts, and calendar events"
    );
}

#[test]
fn mapi_named_properties_and_custom_values_are_durable() {
    let named = table_definition("mapi_named_properties");
    for required in [
        "property_id INTEGER NOT NULL CHECK (property_id BETWEEN 32769 AND 65534)",
        "property_guid BYTEA NOT NULL CHECK (octet_length(property_guid) = 16)",
        "property_kind TEXT NOT NULL CHECK (property_kind IN ('lid', 'name'))",
        "PRIMARY KEY (tenant_id, account_id, property_id)",
        "REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
    ] {
        assert!(
            named.contains(required),
            "mapi_named_properties must persist stable Outlook named-property mappings: {required}"
        );
    }

    let values = table_definition("mapi_custom_property_values");
    for required in [
        "object_kind TEXT NOT NULL CHECK (object_kind IN ('message', 'contact', 'calendar_event', 'task', 'note', 'journal_entry', 'attachment', 'public_folder_item'))",
        "canonical_id UUID NOT NULL",
        "property_tag BIGINT NOT NULL CHECK (property_tag >= 0 AND property_tag <= 4294967295)",
        "property_type INTEGER NOT NULL CHECK (property_type >= 0 AND property_type <= 65535)",
        "property_value BYTEA NOT NULL",
        "PRIMARY KEY (tenant_id, account_id, object_kind, canonical_id, property_tag, property_type)",
    ] {
        assert!(
            values.contains(required),
            "mapi_custom_property_values must persist custom MAPI property values by canonical object: {required}"
        );
    }

    assert_schema_contains_all(&[
        "CREATE UNIQUE INDEX mapi_named_properties_lid_idx",
        "CREATE UNIQUE INDEX mapi_named_properties_name_idx",
        "CREATE INDEX mapi_custom_property_values_object_idx",
    ]);
}

#[test]
fn mapi_property_store_runtime_sql_matches_durable_schema() {
    assert_sources_contain_all(
        "lpe-exchange store",
        &[
            EXCHANGE_STORE,
            EXCHANGE_STORE_MAPI_METADATA,
            EXCHANGE_STORE_HELPERS_MAPI,
        ],
        &[
            "fn fetch_or_allocate_mapi_named_property_ids",
            "fn fetch_mapi_named_properties_by_ids",
            "fn fetch_mapi_named_properties",
            "FROM mapi_named_properties",
            "INSERT INTO mapi_named_properties",
            "is_unique_violation",
            "fn upsert_mapi_custom_property_values",
            "fn fetch_mapi_custom_property_values",
            "fn delete_mapi_custom_property_values",
            "INSERT INTO mapi_custom_property_values",
            "ON CONFLICT (",
            "canonical_id,",
            "property_tag,",
            "property_type",
            "SELECT property_tag, property_type, property_value",
            "DELETE FROM mapi_custom_property_values",
        ],
    );
}

#[test]
fn mapi_profile_settings_are_canonical_account_settings() {
    let profile = table_definition("mapi_profile_settings");
    for required in [
        "account_id UUID NOT NULL",
        "ipm_subtree_ost_id BYTEA CHECK (ipm_subtree_ost_id IS NULL OR (octet_length(ipm_subtree_ost_id) > 0 AND octet_length(ipm_subtree_ost_id) <= 2048))",
        "PRIMARY KEY (tenant_id, account_id)",
        "REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
    ] {
        assert!(
            profile.contains(required),
            "mapi_profile_settings must persist bounded Outlook profile settings by account: {required}"
        );
    }

    assert_source_contains_all(
        "storage admin profile settings",
        ADMIN_STORAGE,
        &[
            "pub async fn fetch_outlook_profile_state",
            "pub async fn fetch_mapi_ipm_subtree_ost_id",
            "pub async fn store_mapi_ipm_subtree_ost_id",
            "FROM mapi_profile_settings",
            "INSERT INTO mapi_profile_settings",
            "ipm_subtree_ost_id = EXCLUDED.ipm_subtree_ost_id",
        ],
    );

    assert_sources_contain_all(
        "lpe-exchange store",
        &[EXCHANGE_STORE, EXCHANGE_STORE_MAPI_METADATA],
        &[
            "fn fetch_mapi_ipm_subtree_ost_id",
            "Storage::fetch_mapi_ipm_subtree_ost_id",
            "fn store_mapi_ipm_subtree_ost_id",
            "Storage::store_mapi_ipm_subtree_ost_id",
        ],
    );
}

#[test]
fn mapi_folder_profile_properties_are_bounded_profile_state() {
    let profile = table_definition("mapi_folder_profile_property_values");
    for required in [
        "account_id UUID NOT NULL",
        "folder_id BIGINT NOT NULL CHECK (folder_id > 0)",
        "property_tag BIGINT NOT NULL CHECK (property_tag >= 0 AND property_tag <= 4294967295)",
        "property_value BYTEA NOT NULL CHECK (octet_length(property_value) > 0 AND octet_length(property_value) <= 4096)",
        "PRIMARY KEY (tenant_id, account_id, folder_id, property_tag, property_type)",
        "REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
    ] {
        assert!(
            profile.contains(required),
            "mapi_folder_profile_property_values must persist bounded Outlook folder profile state: {required}"
        );
    }

    assert_sources_contain_all(
        "lpe-exchange folder profile property store",
        &[EXCHANGE_STORE, EXCHANGE_STORE_MAPI_METADATA],
        &[
            "fn fetch_mapi_folder_profile_property_values",
            "fn upsert_mapi_folder_profile_property_values",
            "FROM mapi_folder_profile_property_values",
            "INSERT INTO mapi_folder_profile_property_values",
        ],
    );
}

#[test]
fn update_script_requires_the_current_schema_without_mutating_it() {
    assert_source_contains_all(
        "update-lpe.sh",
        UPDATE_LPE_SCRIPT,
        &[
            "SELECT schema_version FROM public.schema_metadata WHERE singleton = TRUE",
            "INSTALLED_SCHEMA_VERSION",
            "EXPECTED_SCHEMA_VERSION",
            "Upgrades from releases before LPE 0.5.0 are unsupported",
            "no compatibility SQL is required",
        ],
    );

    for forbidden in [
        "psql \"${DATABASE_URL}\" -v ON_ERROR_STOP=1 -f",
        "CREATE TABLE",
        "ALTER TABLE",
        "UPDATE public.",
        "DELETE FROM public.",
    ] {
        assert!(
            !UPDATE_LPE_SCRIPT.contains(forbidden),
            "update-lpe.sh must not mutate the LPE 0.5.0 schema: {forbidden}"
        );
    }
}

#[test]
fn fresh_schema_checks_validate_constraint_shape_without_migration_names() {
    assert!(
        !CHECK_LPE_SCRIPT.contains("conname = 'mail_change_log_object_shape_check'"),
        "check-lpe.sh must validate the canonical CHECK definition without requiring a migration-assigned constraint name"
    );
    assert_source_contains_all(
        "check-lpe.sh canonical mail change constraint checks",
        CHECK_LPE_SCRIPT,
        &[
            "conrelid = 'public.mail_change_log'::regclass AND contype = 'c'",
            "pg_get_constraintdef(oid) LIKE '%associated_config%'",
            "pg_get_constraintdef(oid) LIKE '%sourceMailboxMessageId%'",
        ],
    );
}

#[test]
fn runtime_schema_check_rejects_missing_required_mapi_tables() {
    assert_source_contains_all(
        "storage core schema assertion",
        CORE_STORAGE,
        &[
            "assert_required_schema_objects",
            "\"mapi_named_properties\"",
            "\"mapi_custom_property_values\"",
            "\"mapi_associated_config_messages\"",
            "\"mapi_profile_settings\"",
            "SELECT to_regclass($1) IS NOT NULL",
            "required table public.{table} is missing",
            "LPE 0.5.0 requires an empty database initialized from crates/lpe-storage/sql/schema.sql",
        ],
    );
}

#[test]
fn mapi_folder_properties_are_not_protocol_local_state() {
    assert!(
        !SCHEMA.contains("CREATE TABLE mapi_folder_properties")
            && !SCHEMA.contains("mapi_folder_properties_folder_idx"),
        "folder MAPI properties must be canonical/computed projections, not protocol-local storage"
    );
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
        MAILBOXES_STORAGE.contains("INSERT INTO tombstones")
            && MAILBOXES_STORAGE.contains("'mailbox'")
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
        ATTACHMENTS_STORAGE.contains("pub async fn delete_message_attachment")
            && ATTACHMENTS_STORAGE.contains("pub async fn add_message_attachment")
            && ATTACHMENTS_STORAGE.contains("\"attachment\"")
            && ATTACHMENTS_STORAGE.contains("\"attachmentId\"")
            && ATTACHMENTS_STORAGE.contains("\"created\"")
            && ATTACHMENTS_STORAGE.contains("\"destroyed\""),
        "attachment metadata changes must write durable mail_change_log entries"
    );
}

#[test]
fn blob_placement_metadata_is_tenant_domain_and_blob_safe() {
    let storage_pools = table_definition("storage_pools");
    assert!(
        storage_pools
            .contains("pool_kind TEXT NOT NULL CHECK (pool_kind IN ('postgres', 's3_compatible'))")
            && storage_pools.contains(
                "status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled'))"
            )
            && storage_pools.contains("config_json JSONB NOT NULL DEFAULT '{}'::jsonb")
            && storage_pools.contains("(pool_kind = 'postgres' AND config_json = '{}'::jsonb)")
            && storage_pools
                .contains("(pool_kind = 's3_compatible' AND jsonb_typeof(config_json) = 'object')")
            && storage_pools.contains("UNIQUE (name)"),
        "storage_pools must represent database-backed and provider-neutral S3-compatible pools"
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
    let blobs = table_definition("blobs");
    for required in [
        "blob_bytes BYTEA",
        "CHECK (blob_kind <> 'raw_message' OR blob_bytes IS NOT NULL)",
    ] {
        assert!(
            blobs.contains(required),
            "blobs must keep raw messages DB-backed while allowing externally placed durable bytes: {required}"
        );
    }
    assert!(
        !blobs.contains("blob_bytes BYTEA NOT NULL"),
        "externally placed durable attachment and MIME blobs must not require database bytes"
    );

    assert_schema_contains_all(&[
        "UNIQUE (tenant_id, id, blob_kind)",
        "UNIQUE (tenant_id, domain_id, id, blob_kind)",
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

    for unsupported_backend in ["aws", "azure", "cloud"] {
        assert!(
            !storage_pools
                .to_ascii_lowercase()
                .contains(unsupported_backend)
                && !blob_placements
                    .to_ascii_lowercase()
                    .contains(unsupported_backend),
            "Milestone 6 schema must not introduce provider-specific backend config for {unsupported_backend}"
        );
    }
}

#[test]
fn blob_references_enforce_kind_and_attachment_ownership() {
    let messages = table_definition("messages");
    for required in [
        "blob_kind TEXT NOT NULL DEFAULT 'raw_message' CHECK (blob_kind = 'raw_message')",
        "FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)",
        "REFERENCES blobs (tenant_id, domain_id, id, blob_kind)",
    ] {
        assert!(
            messages.contains(required),
            "messages must constrain raw RFC 5322 blob references by kind and domain: {required}"
        );
    }

    let mime_parts = table_definition("mime_parts");
    for required in [
        "blob_kind TEXT CHECK (blob_kind IS NULL OR blob_kind IN ('mime_part', 'attachment'))",
        "UNIQUE (tenant_id, message_id, domain_id, id, blob_id, blob_kind)",
        "CHECK ((blob_id IS NULL AND blob_kind IS NULL) OR (blob_id IS NOT NULL AND blob_kind IS NOT NULL))",
        "FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)",
        "REFERENCES blobs (tenant_id, domain_id, id, blob_kind)",
    ] {
        assert!(
            mime_parts.contains(required),
            "mime_parts must constrain durable MIME/attachment blob references: {required}"
        );
    }

    let attachments = table_definition("attachments");
    for required in [
        "blob_kind TEXT NOT NULL DEFAULT 'attachment' CHECK (blob_kind = 'attachment')",
        "FOREIGN KEY (tenant_id, account_id, mailbox_message_id, message_id)",
        "REFERENCES mailbox_messages (tenant_id, account_id, id, message_id)",
        "FOREIGN KEY (tenant_id, message_id, domain_id) REFERENCES messages (tenant_id, id, domain_id)",
        "FOREIGN KEY (tenant_id, message_id, domain_id, mime_part_id, blob_id, blob_kind)",
        "REFERENCES mime_parts (tenant_id, message_id, domain_id, id, blob_id, blob_kind)",
        "FOREIGN KEY (tenant_id, domain_id, blob_id, blob_kind)",
        "REFERENCES blobs (tenant_id, domain_id, id, blob_kind)",
    ] {
        assert!(
            attachments.contains(required),
            "attachments must prove tenant/domain/account/message/membership/blob consistency: {required}"
        );
    }

    let extraction_jobs = table_definition("attachment_extraction_jobs");
    let attachment_texts = table_definition("attachment_texts");
    for (name, source) in [
        ("attachment_extraction_jobs", extraction_jobs),
        ("attachment_texts", attachment_texts),
    ] {
        assert!(
            source.contains(
                "blob_kind TEXT NOT NULL DEFAULT 'attachment' CHECK (blob_kind = 'attachment')"
            ) && source.contains("FOREIGN KEY (tenant_id, blob_id, blob_kind)")
                && source.contains("REFERENCES blobs (tenant_id, id, blob_kind)"),
            "{name} must only reference attachment blobs"
        );
    }

    assert!(
        ATTACHMENTS_STORAGE.contains("blob_kind, status")
            && ATTACHMENTS_STORAGE.contains("'attachment', 'queued'")
            && ATTACHMENTS_STORAGE.contains("blob_id, blob_kind")
            && ATTACHMENTS_STORAGE.contains("'attachment', $8"),
        "attachment ingestion must bind attachment blob kind explicitly"
    );
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
fn audit_events_support_platform_and_tenant_admin_policy_events() {
    let audit = table_definition("audit_events");
    for required in [
        "tenant_id UUID NOT NULL",
        "actor TEXT NOT NULL CHECK (btrim(actor) <> '')",
        "action TEXT NOT NULL CHECK (btrim(action) <> '')",
        "subject TEXT NOT NULL CHECK (btrim(subject) <> '')",
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "UNIQUE (tenant_id, id)",
        "FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE",
    ] {
        assert!(
            audit.contains(required),
            "audit_events is missing required admin policy audit fragment: {required}"
        );
    }
}

#[test]
fn platform_administration_uses_real_tenant_uuid_not_string_pseudo_tenant() {
    assert!(
        SCHEMA.contains(
            "INSERT INTO tenants (id, slug, display_name)\nVALUES ('00000000-0000-0000-0000-000000000001', 'platform', 'LPE Platform')"
        ),
        "schema.sql must seed the real platform tenant row"
    );
    assert!(
        !SCHEMA.contains("__platform__")
            && !SHARED_STORAGE.contains("__platform__")
            && !ADMIN_STORAGE.contains("__platform__"),
        "platform state must not use the retired __platform__ pseudo-tenant string"
    );
    assert!(
        SHARED_STORAGE.contains("pub(crate) const PLATFORM_TENANT_ID: Uuid = Uuid::from_u128(1)")
            && SHARED_STORAGE
                .contains("tenant_id_for_domain_name(&self, domain_name: &str) -> Result<Uuid>")
            && SHARED_STORAGE
                .contains("tenant_id_for_account_email(&self, email: &str) -> Result<Uuid>")
            && SHARED_STORAGE
                .contains("tenant_id_for_admin_email(&self, email: &str) -> Result<Uuid>"),
        "tenant lookup helpers must return real UUID tenant ids"
    );
}

#[test]
fn admin_settings_and_auth_runtime_tables_exist_in_core_schema() {
    for required_table in [
        "server_settings",
        "security_settings",
        "admin_oidc_config",
        "account_oidc_config",
        "admin_oidc_identities",
        "account_oidc_identities",
        "admin_auth_factors",
        "account_auth_factors",
        "account_app_passwords",
        "sieve_scripts",
        "sieve_vacation_responses",
        "local_ai_settings",
    ] {
        let definition = table_definition(required_table);
        assert!(
            definition.contains("tenant_id"),
            "{required_table} must be tenant-scoped core administration state"
        );
    }

    assert_schema_contains_all(&[
        "CREATE TABLE server_settings",
        "primary_hostname TEXT NOT NULL DEFAULT 'localhost'",
        "default_locale TEXT NOT NULL DEFAULT 'en' CHECK (default_locale IN ('en', 'fr', 'de', 'it', 'es'))",
        "CREATE TABLE security_settings",
        "password_login_enabled BOOLEAN NOT NULL DEFAULT TRUE",
        "mailbox_app_passwords_enabled BOOLEAN NOT NULL DEFAULT TRUE",
        "CREATE TABLE admin_auth_factors",
        "CREATE TABLE account_app_passwords",
        "CREATE TABLE sieve_scripts",
        "normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED",
        "CREATE UNIQUE INDEX sieve_scripts_active_account_idx",
        "CREATE TABLE sieve_vacation_responses",
        "CREATE TABLE local_ai_settings",
    ]);
}

#[test]
fn admin_domain_account_and_audit_paths_bind_uuid_tenant_ids() {
    let admin_storage = [ADMIN_STORAGE, ADMIN_PROVISIONING_STORAGE].concat();
    assert!(
        admin_storage.contains("let tenant_id = PLATFORM_TENANT_ID;")
            && admin_storage.contains("INSERT INTO domains (\n                id, tenant_id, name")
            && admin_storage.contains(
                "INSERT INTO accounts (\n                id, tenant_id, primary_domain_id"
            )
            && SHARED_STORAGE
                .contains("INSERT INTO audit_events (id, tenant_id, actor, action, subject)")
            && SHARED_STORAGE.contains("tenant_id: &Uuid"),
        "admin domain creation, account creation, and audit writes must bind UUID tenant ids"
    );
}

#[test]
fn mailbox_identity_schema_has_generated_normalized_address_helpers() {
    let domains = table_definition("domains");
    assert!(
        domains.contains("normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED")
            && domains.contains("UNIQUE (tenant_id, normalized_name)")
            && domains.contains("UNIQUE (normalized_name)"),
        "domains must keep generated IDNA-ready normalized lookup keys"
    );

    let accounts = table_definition("accounts");
    for required in [
        "normalized_primary_email TEXT GENERATED ALWAYS AS (lower(primary_email)) STORED",
        "normalized_primary_email_local_part TEXT GENERATED ALWAYS AS (lower(split_part(primary_email, '@', 1))) STORED",
        "normalized_primary_email_domain TEXT GENERATED ALWAYS AS (lower(split_part(primary_email, '@', 2))) STORED",
        "directory_kind TEXT NOT NULL DEFAULT 'person' CHECK (directory_kind IN ('person', 'room', 'equipment'))",
        "UNIQUE (tenant_id, normalized_primary_email)",
    ] {
        assert!(
            accounts.contains(required),
            "accounts must expose generated mailbox normalization helper: {required}"
        );
    }

    let addresses = table_definition("account_email_addresses");
    for required in [
        "normalized_email TEXT GENERATED ALWAYS AS (lower(email)) STORED",
        "normalized_email_local_part TEXT GENERATED ALWAYS AS (lower(split_part(email, '@', 1))) STORED",
        "normalized_email_domain TEXT GENERATED ALWAYS AS (lower(split_part(email, '@', 2))) STORED",
        "UNIQUE (tenant_id, normalized_email)",
    ] {
        assert!(
            addresses.contains(required),
            "account_email_addresses must expose generated mailbox normalization helper: {required}"
        );
    }

    let aliases = table_definition("aliases");
    assert!(
        aliases.contains("normalized_source TEXT GENERATED ALWAYS AS (lower(source)) STORED")
            && aliases
                .contains("normalized_target TEXT GENERATED ALWAYS AS (lower(target)) STORED")
            && aliases.contains("UNIQUE (tenant_id, normalized_source)"),
        "aliases must use generated normalized source and target helpers"
    );
}

#[test]
fn mailbox_identity_runtime_uses_generated_normalized_lookup_keys() {
    let admin_storage = [ADMIN_STORAGE, ADMIN_PROVISIONING_STORAGE].concat();
    assert!(
        admin_storage.contains("normalize_domain_name(&input.name)")
            && admin_storage.contains("normalize_email(&input.email)")
            && admin_storage.contains("ON CONFLICT (tenant_id, normalized_primary_email)")
            && admin_storage.contains("ON CONFLICT (tenant_id, normalized_name)")
            && admin_storage.contains("ON CONFLICT (tenant_id, normalized_source)"),
        "admin identity writes must normalize once and target generated normalized keys"
    );
    for (name, source) in [
        ("shared.rs", SHARED_STORAGE),
        ("auth.rs", AUTH_STORAGE),
        ("submission.rs", SUBMISSION_STORAGE),
        ("inbound.rs", INBOUND_STORAGE),
    ] {
        assert!(
            source.contains("normalized_primary_email")
                || source.contains("normalized_account_email"),
            "{name} must use generated normalized mailbox lookup keys"
        );
    }
}

#[test]
fn account_creation_allocates_canonical_send_identity_rows() {
    let admin_storage = [ADMIN_STORAGE, ADMIN_PROVISIONING_STORAGE].concat();
    let identities = table_definition("account_identities");
    for required in [
        "email_address_id UUID NOT NULL",
        "may_send BOOLEAN NOT NULL DEFAULT TRUE",
        "is_default BOOLEAN NOT NULL DEFAULT FALSE",
        "CHECK ((NOT is_default) OR may_send)",
        "FOREIGN KEY (tenant_id, account_id, email_address_id)",
        "REFERENCES account_email_addresses (tenant_id, account_id, id)",
        "CREATE UNIQUE INDEX account_identities_default_idx",
    ] {
        assert!(
            identities.contains(required) || SCHEMA.contains(required),
            "account_identities must enforce canonical send identity allocation: {required}"
        );
    }
    assert!(
        admin_storage.contains("INSERT INTO account_email_addresses")
            && admin_storage.contains("address_kind, is_primary")
            && admin_storage.contains("INSERT INTO account_identities")
            && admin_storage.contains("may_send, is_default")
            && submission_storage_contains("FROM sender_rights")
            && submission_storage_contains("sender_identity_id("),
        "account creation must allocate canonical primary address/default identity rows while sender projection remains derived from canonical rights"
    );
    for forbidden in [
        "CREATE TABLE ews_identities",
        "CREATE TABLE mapi_send_identities",
        "CREATE TABLE activesync_identities",
        "CREATE TABLE protocol_identities",
    ] {
        assert!(
            !SCHEMA.contains(forbidden),
            "send identities must not be allocated in protocol-local tables: {forbidden}"
        );
    }
}

#[test]
fn admin_workspace_and_pst_use_v2_mailbox_membership_schema() {
    assert_schema_contains_all(&[
        "retention_days INTEGER NOT NULL DEFAULT 365 CHECK (retention_days >= 0)",
        "jmap_push_journal_retention_days INTEGER NOT NULL DEFAULT 30",
        "CREATE TABLE mailbox_pst_jobs",
        "FOREIGN KEY (tenant_id, account_id, mailbox_id)\n        REFERENCES mailboxes (tenant_id, account_id, id)",
    ]);

    for (name, source) in [
        ("admin.rs", ADMIN_STORAGE),
        ("protocols.rs", PROTOCOLS_STORAGE),
        ("workspace.rs", WORKSPACE_STORAGE),
        ("pst.rs", PST_STORAGE),
    ] {
        assert!(
            !source.contains("messages m ON m.mailbox_id")
                && !source.contains("JOIN mailboxes mb ON mb.id = m.mailbox_id")
                && !source.contains("m.delivery_status")
                && !source.contains("m.from_address")
                && !source.contains("m.subject_normalized"),
            "{name} must not query retired v1 message projection columns"
        );
        assert!(
            source.contains("mailbox_messages"),
            "{name} must use mailbox_messages for mailbox membership"
        );
    }

    assert!(
        !ADMIN_STORAGE.contains("outbound_message_queue")
            && !ADMIN_STORAGE.contains("q.message_id")
            && ADMIN_STORAGE.contains("submission_queue")
            && ADMIN_STORAGE.contains("q.sent_mailbox_message_id"),
        "admin mail flow views must use the v2 submission queue and resolve messages through sent mailbox membership"
    );
}

#[test]
fn mailbox_schema_allows_canonical_outlook_compatibility_mail_roles() {
    let mailboxes = table_definition("mailboxes");
    for role in [
        "outbox",
        "conversation_history",
        "rss_feeds",
        "sync_issues",
        "conflicts",
        "local_failures",
        "server_failures",
    ] {
        assert!(
            mailboxes.contains(&format!("'{role}'")),
            "mailboxes.role CHECK must allow {role}"
        );
        assert!(
            MAILBOXES_STORAGE.contains(&format!("\"{role}\"")),
            "mailbox bootstrap must create {role}"
        );
    }
}

#[test]
fn search_folder_schema_persists_exchange_builtin_definitions() {
    let search_folders = table_definition("search_folders");
    for fragment in [
        "role TEXT NOT NULL DEFAULT 'custom'",
        "'reminders'",
        "'todo_search'",
        "'contacts_search'",
        "'tracked_mail_processing'",
        "definition_kind TEXT NOT NULL DEFAULT 'exchange_builtin'",
        "result_object_kind TEXT NOT NULL",
        "scope_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "restriction_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "excluded_folder_roles TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]",
        "FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
    ] {
        assert!(
            search_folders.contains(fragment),
            "search_folders table must persist {fragment}"
        );
    }

    assert!(
        SCHEMA.contains("CREATE UNIQUE INDEX search_folders_builtin_role_idx")
            && SCHEMA.contains("WHERE is_builtin"),
        "built-in search folder definitions must be unique per account and role"
    );
    assert!(
        SCHEMA.contains("CREATE UNIQUE INDEX search_folders_user_saved_name_idx")
            && SCHEMA.contains("lower(btrim(display_name))")
            && SCHEMA.contains("WHERE NOT is_builtin AND definition_kind = 'user_saved'"),
        "user-saved search folder definitions must not duplicate Outlook-created names per account and result kind"
    );
    for role in [
        "reminders",
        "todo_search",
        "contacts_search",
        "tracked_mail_processing",
    ] {
        assert!(
            SEARCH_FOLDERS_STORAGE.contains(&format!("role: \"{role}\"")),
            "canonical search-folder storage must persist the built-in {role} definition"
        );
    }
    assert!(
        SEARCH_FOLDERS_STORAGE.contains("\"search_folder_definition\"")
            && SEARCH_FOLDERS_STORAGE.contains("CanonicalChangeCategory::Search")
            && SEARCH_FOLDERS_STORAGE.contains("insert_mail_change_log_in_tx")
            && SEARCH_FOLDERS_STORAGE.contains("emit_account_scoped_change"),
        "search-folder definition bootstrap must write canonical object changes instead of MAPI-local FAI state"
    );
}

#[test]
fn conversation_actions_are_canonical_fai_state() {
    let actions = table_definition("conversation_actions");
    for fragment in [
        "conversation_id UUID NOT NULL",
        "categories_json JSONB NOT NULL DEFAULT '[]'::jsonb",
        "move_folder_entry_id BYTEA",
        "move_store_entry_id BYTEA",
        "move_target_mailbox_id UUID",
        "max_delivery_time TIMESTAMPTZ",
        "last_applied_time TIMESTAMPTZ",
        "version INTEGER NOT NULL DEFAULT 3984588",
        "processed INTEGER NOT NULL DEFAULT 0",
        "UNIQUE (tenant_id, account_id, conversation_id)",
        "FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
        "FOREIGN KEY (tenant_id, account_id, move_target_mailbox_id)",
    ] {
        assert!(
            actions.contains(fragment),
            "conversation_actions table must persist {fragment}"
        );
    }
    assert_schema_contains_all(&[
        "CREATE INDEX conversation_actions_account_idx",
        "'conversation_actions'",
        "'conversation_action'",
    ]);
    assert!(
        CONVERSATION_ACTIONS_STORAGE.contains("CanonicalChangeCategory::ConversationActions")
            && CONVERSATION_ACTIONS_STORAGE.contains("\"conversation_action\"")
            && CONVERSATION_ACTIONS_STORAGE.contains("insert_mail_change_log_in_tx")
            && CONVERSATION_ACTIONS_STORAGE.contains("emit_canonical_change"),
        "conversation actions must write canonical change rows instead of MAPI-local FAI state"
    );
}

#[test]
fn mailbox_rules_are_canonical_sieve_scripts_with_replay() {
    assert_schema_contains_all(&[
        "CREATE TABLE sieve_scripts",
        "CREATE UNIQUE INDEX sieve_scripts_active_account_idx",
        "'sieve_script'",
        "'rules'",
    ]);
    assert!(
        ADMIN_STORAGE.contains("\"sieve_script\"")
            && ADMIN_STORAGE.contains("CanonicalChangeCategory::Rules")
            && ADMIN_STORAGE.contains("insert_mail_change_log_in_tx")
            && ADMIN_STORAGE.contains("insert_collaboration_tombstone_in_tx")
            && ADMIN_STORAGE.contains("emit_account_scoped_change"),
        "Sieve script mutations must be canonical rule changes with replay tombstones"
    );
    for forbidden in [
        "CREATE TABLE ews_rules",
        "CREATE TABLE mapi_rules",
        "CREATE TABLE exchange_rules",
        "CREATE TABLE deferred_action_messages",
    ] {
        assert!(
            !SCHEMA.contains(forbidden),
            "rules must not be stored in an Exchange-only table: {forbidden}"
        );
    }
}

#[test]
fn cross_protocol_adapter_tests_cover_canonical_model_first_paths() {
    let exchange_mapi_tests = format!(
        "{EXCHANGE_TESTS}{EXCHANGE_MAPI_CALENDAR_TESTS}{EXCHANGE_MAPI_CONNECT_TESTS}{EXCHANGE_MAPI_CONTACTS_TESTS}{EXCHANGE_MAPI_HIERARCHY_TESTS}{EXCHANGE_MAPI_PUBLIC_FOLDERS_TESTS}{EXCHANGE_MAPI_SYNC_TESTS}{EXCHANGE_MAPI_TABLES_TESTS}"
    );
    assert_source_contains_all(
        "Exchange/MAPI tests",
        &exchange_mapi_tests,
        &[
            "mapi_over_http_contact_crud_uses_canonical_contacts",
            "mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection",
            "fetch_accessible_events_in_collection",
            "mapi_over_http_task_crud_uses_canonical_tasks",
            "mapi_over_http_common_views_sync_suppresses_lpe_search_definition_fai",
            "mapi_over_http_set_get_search_criteria_updates_canonical_search_folder",
            "mapi_over_http_set_get_search_criteria_round_trips_attachment_exists",
            "mapi_over_http_set_search_criteria_rejects_unsupported_restriction",
            "mapi_over_http_common_views_create_associated_navigation_shortcut_persists",
            "mapi_over_http_sync_import_associated_message_persists_and_replays_fai",
            "mapi_over_http_associated_config_content_sync_exports_deletes",
            "SearchFolderDefinition",
            "mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc",
            "mapi_over_http_modify_rules_writes_bounded_canonical_sieve_rule",
            "mapi_over_http_modify_rules_accepts_bounded_sieve_actions",
            "mapi_over_http_modify_rules_rejects_exchange_rule_blobs",
            "mapi_over_http_update_deferred_action_messages_rejects_without_sieve_side_effect",
            "active_sieve.lock().unwrap().is_none()",
            "mapi_over_http_public_folder_replica_rops_validate_canonical_folder_ids",
            "mapi_over_http_public_folder_get_owning_servers_uses_ordered_canonical_replicas",
            "mapi_over_http_public_folder_is_ghosted_validates_canonical_folder",
            "mapi_over_http_public_folder_per_user_information_round_trips_canonical_read_state",
            "mapi_over_http_public_folder_per_user_information_rejects_exchange_blob_without_state_change",
            "mapi_over_http_public_folder_permissions_table_projects_canonical_grants",
            "mapi_over_http_public_folder_modify_permissions_writes_canonical_grants",
            "mapi_over_http_public_folder_modify_permissions_rejects_unknown_member_without_grant",
            "mapi_over_http_public_folder_create_message_rejects_recipients",
        ],
    );
    assert_source_contains_all(
        "JMAP tests",
        JMAP_TESTS,
        &[
            "mailbox_and_email_changes_return_existing_ids_from_initial_state",
            "email_changes_use_durable_log_ids_when_state_has_cursor",
            "collaboration_changes_use_durable_log_ids_when_state_has_cursor",
            "task_query_changes_tracks_sort_order_and_updates",
            "identity_changes_tracks_sender_identity_projection",
            "email_submission_changes_use_durable_log_ids_when_state_has_cursor",
            "vacation_response_set_writes_canonical_active_sieve_script",
        ],
    );
    assert_source_contains_all(
        "IMAP tests",
        IMAP_TESTS,
        &[
            "store_and_uid_store_update_only_canonical_supported_flags",
            "append_copy_move_and_expunge_preserve_canonical_uid_state",
            "search_and_uid_search_use_canonical_visible_fields_without_bcc",
            "acl_commands_project_canonical_mailbox_and_sender_delegation",
        ],
    );
    assert_source_contains_all(
        "ActiveSync tests",
        ACTIVESYNC_TESTS,
        &[
            "move_items_moves_message_between_canonical_mail_folders",
            "sync_add_command_saves_draft_through_canonical_storage",
            "send_mail_uses_canonical_submission_model",
            "search_queries_canonical_mail_projection",
            "sync_contact_and_calendar_mutations_update_canonical_models",
        ],
    );
}

#[test]
fn recipient_suggestions_are_owner_scoped_private_ranked_signals() {
    let suggestions = table_definition("recipient_suggestions");
    for required in [
        "id UUID PRIMARY KEY",
        "tenant_id UUID NOT NULL",
        "account_id UUID NOT NULL",
        "normalized_email TEXT NOT NULL CHECK (btrim(normalized_email) <> '')",
        "display_name TEXT NOT NULL DEFAULT ''",
        "source_kind TEXT NOT NULL CHECK (source_kind IN ('sent_to', 'sent_cc', 'manual', 'contact'))",
        "first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "use_count INTEGER NOT NULL DEFAULT 1 CHECK (use_count > 0)",
        "dismissed_at TIMESTAMPTZ",
        "contact_id UUID",
        "source_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb",
        "CHECK (jsonb_typeof(source_metadata_json) = 'object')",
        "CHECK (last_used_at >= first_seen_at)",
        "FOREIGN KEY (tenant_id, account_id) REFERENCES accounts (tenant_id, id) ON DELETE CASCADE",
        "FOREIGN KEY (tenant_id, contact_id) REFERENCES contacts (tenant_id, id) ON DELETE SET NULL (contact_id)",
    ] {
        assert!(
            suggestions.contains(required),
            "recipient_suggestions is missing expected contract fragment: {required}"
        );
    }
    assert!(
        SCHEMA.contains(
            "CREATE UNIQUE INDEX recipient_suggestions_active_email_idx\n    ON recipient_suggestions (tenant_id, account_id, normalized_email)\n    WHERE dismissed_at IS NULL"
        ),
        "recipient suggestions must prevent duplicate active suggestions per account/email"
    );
    assert!(
        SCHEMA.contains(
            "CREATE INDEX recipient_suggestions_rank_idx\n    ON recipient_suggestions (tenant_id, account_id, dismissed_at, use_count DESC, last_used_at DESC)"
        ),
        "recipient suggestions need an owner-scoped ranking index"
    );
}

#[test]
fn recipient_suggestions_contact_delete_clears_only_contact_id() {
    let suggestions = table_definition("recipient_suggestions");
    assert!(
        suggestions.contains(
            "FOREIGN KEY (tenant_id, contact_id) REFERENCES contacts (tenant_id, id) ON DELETE SET NULL (contact_id)"
        ),
        "recipient_suggestions contact FK must clear contact_id only so tenant/account scope survives contact deletion"
    );
    assert!(
        !suggestions.contains(
            "FOREIGN KEY (tenant_id, contact_id) REFERENCES contacts (tenant_id, id) ON DELETE SET NULL\n"
        ),
        "recipient_suggestions contact FK must not use composite-column SET NULL"
    );
}

#[test]
fn contact_book_schema_allows_outlook_compatibility_roles() {
    let contact_books = table_definition("contact_books");
    for role in ["suggested_contacts", "quick_contacts", "im_contact_list"] {
        assert!(
            contact_books.contains(&format!("'{role}'")),
            "contact_books.role CHECK must allow {role}"
        );
    }
    assert!(
        SCHEMA.contains(
            "ON contact_books (tenant_id, owner_account_id, role)\n    WHERE role <> 'custom'"
        ),
        "contact book default-role uniqueness must remain a partial index"
    );
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
fn recoverable_items_are_canonical_lifecycle_state() {
    let _recoverable_items = table_definition("recoverable_items");
    for required in [
        "source_mailbox_message_id UUID NOT NULL",
        "source_mailbox_id UUID NOT NULL",
        "source_imap_uid BIGINT NOT NULL CHECK (source_imap_uid > 0)",
        "recoverable_folder TEXT NOT NULL CHECK (recoverable_folder IN ('deletions', 'versions', 'purges'))",
        "delete_kind TEXT NOT NULL CHECK (delete_kind IN (",
        "status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'restored', 'purged'))",
        "retained_until TIMESTAMPTZ",
        "legal_hold BOOLEAN NOT NULL DEFAULT FALSE",
        "created_by_protocol TEXT NOT NULL CHECK (created_by_protocol IN (",
        "UNIQUE (tenant_id, account_id, source_mailbox_message_id)",
        "CREATE INDEX recoverable_items_active_folder_idx",
        "CREATE INDEX recoverable_items_cleanup_idx",
        "CREATE INDEX recoverable_items_message_idx",
    ] {
        assert!(
            SCHEMA.contains(required),
            "recoverable item lifecycle schema is missing required fragment: {required}"
        );
    }
    assert!(
        SCHEMA.contains("'recoverable_item'")
            && SCHEMA.contains("summary_json ? 'sourceMailboxMessageId'")
            && MAIL_ITEMS_STORAGE.contains("INSERT INTO recoverable_items")
            && RECOVERABLE_ITEMS_STORAGE.contains("pub async fn list_recoverable_items")
            && RECOVERABLE_ITEMS_STORAGE.contains("pub async fn restore_recoverable_item")
            && RECOVERABLE_ITEMS_STORAGE.contains("pub async fn purge_recoverable_item")
            && RECOVERABLE_ITEMS_STORAGE.contains("sourceMailboxMessageId")
            && RECOVERABLE_ITEMS_STORAGE.contains("restoredMailboxMessageId")
            && RECOVERABLE_ITEMS_STORAGE.contains("sourceImapUid")
            && RECOVERABLE_ITEMS_STORAGE.contains("let recoverable_folder")
            && MAIL_ITEMS_STORAGE.contains("\"recoverable_item\"")
            && MAIL_ITEMS_STORAGE.contains("\"recoverableFolder\": \"deletions\""),
        "hard delete must write canonical recoverable item state and replay rows"
    );
    for forbidden in [
        "CREATE TABLE mapi_recoverable",
        "CREATE TABLE mapi_dumpster",
        "CREATE TABLE exchange_dumpster",
    ] {
        assert!(
            !SCHEMA.contains(forbidden),
            "recoverable items must not use protocol-local dumpster storage: {forbidden}"
        );
    }
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
fn mailbox_count_mutations_recalculate_from_visible_memberships() {
    let draft_destroy_body =
        function_body(SUBMISSION_STORAGE, "async fn delete_draft_message_in_tx");
    let imap_expunge_body = function_body(MAIL_ITEMS_STORAGE, "pub async fn expunge_imap_deleted");
    assert!(
        SUBMISSION_STORAGE
            .matches("recalculate_mailbox_counts_in_tx")
            .count()
            >= 2,
        "draft creation/update paths must recalculate mailbox counters from visible memberships"
    );
    assert!(
        draft_destroy_body.contains("visibility = 'expunged'")
            && draft_destroy_body.contains("recalculate_mailbox_counts_in_tx"),
        "draft membership destruction must recalculate counters after expunging the visible row"
    );
    assert!(
        imap_expunge_body.contains("visibility = 'expunged'")
            && imap_expunge_body.contains("recalculate_mailbox_counts_in_tx"),
        "IMAP expunge must recalculate counters after expunging visible rows"
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
            && !IMAP_STORAGE.contains("MAX(imap_uid)")
            && !PROTOCOLS_STORAGE.contains("MAX(imap_uid)")
            && !MESSAGE_OPS_STORAGE.contains("MAX(imap_uid)"),
        "storage must allocate UIDNEXT from mailbox uid_next, not visible max UID"
    );
    assert!(
        SHARED_STORAGE.contains("pub(crate) fn allocate_uid_validity() -> i64")
            && [ADMIN_STORAGE, ADMIN_PROVISIONING_STORAGE]
                .concat()
                .contains("allocate_uid_validity()")
            && INBOUND_STORAGE.contains("allocate_uid_validity()")
            && MAILBOXES_STORAGE.contains("allocate_uid_validity()"),
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
fn mailbox_moves_create_target_membership_and_tombstone_source_uid() {
    let move_body = function_body(MESSAGE_OPS_STORAGE, "async fn move_jmap_email_membership");
    let shared_membership_body = function_body(
        SHARED_STORAGE,
        "pub(crate) async fn allocate_mailbox_membership_in_tx",
    );
    assert!(
        move_body.contains("INSERT INTO mailbox_messages")
            && move_body.contains("UPDATE mailboxes\n            SET uid_next = uid_next + 1")
            && move_body.contains("visibility = 'expunged'")
            && move_body.contains("'move'")
            && move_body.contains("sourceImapUid")
            && move_body.contains("targetImapUid")
            && move_body.matches("recalculate_mailbox_counts_in_tx").count() >= 2,
        "mailbox moves must create a target membership from target UIDNEXT, tombstone the source membership with the original IMAP UID, and recalculate both mailbox counters"
    );
    assert!(
        SHARED_STORAGE.contains("pub(crate) async fn recalculate_mailbox_counts_in_tx")
            && SHARED_STORAGE.contains("COUNT(*) FILTER (WHERE NOT is_seen)::integer")
            && shared_membership_body.contains("recalculate_mailbox_counts_in_tx"),
        "mailbox membership inserts must keep stored counts exact from visible mailbox_messages rows"
    );
    assert!(
        !move_body.contains("SET mailbox_id = $4"),
        "mailbox moves must not rewrite the source membership mailbox_id in place"
    );
}

#[test]
fn jmap_email_projection_preserves_multi_mailbox_memberships() {
    let fetch_body = function_body(PROTOCOLS_STORAGE, "pub async fn fetch_jmap_emails");
    let query_body = function_body(JMAP_QUERIES_STORAGE, "pub async fn query_jmap_email_ids");
    assert!(
        fetch_body.contains("array_agg(mailbox_id")
            && fetch_body.contains("mailbox_ids: row.mailbox_ids.clone()")
            && fetch_body.contains("mailbox_states")
            && !fetch_body.contains("DISTINCT ON (m.id)"),
        "JMAP Email/get must aggregate all visible mailbox memberships instead of choosing one"
    );
    assert!(
        query_body.contains("GROUP BY s.message_id")
            && query_body.contains("COUNT(*)\n            FROM (\n                SELECT s.message_id"),
        "JMAP Email/query must deduplicate unscoped results by canonical message id while preserving mailbox-scoped membership filters"
    );
}

#[test]
fn imported_and_inbound_mail_persist_message_date_metadata() {
    let inbound_body = function_body(INBOUND_STORAGE, "async fn store_inbound_message_in_tx");
    let import_body = function_body(MESSAGE_OPS_STORAGE, "pub async fn import_jmap_email");
    assert!(
        inbound_body.contains("parse_message_date_header(&request.raw_message)")
            && inbound_body.contains("COALESCE($8::timestamptz, NOW()), NOW()"),
        "inbound storage must persist RFC Date as messages.sent_at and fall back to receive time"
    );
    assert!(
        import_body.contains("parse_message_date_header(&raw_message)")
            && import_body.contains("COALESCE($9::timestamptz, NOW())")
            && import_body.contains("CASE WHEN $11 THEN NULL ELSE COALESCE($8::timestamptz, $9::timestamptz, NOW()) END")
            && import_body.contains("input.received_at.as_deref()"),
        "JMAP/MAPI imports must persist non-draft sent_at from RFC Date or received_at while preserving draft null sent_at"
    );
    assert!(
        !inbound_body.contains("$7, NULL, NOW()") && !import_body.contains("$7, NULL, NOW()"),
        "message insertion paths must not discard available message date metadata"
    );
}

#[test]
fn jmap_mailbox_storage_uses_shared_name_policy() {
    for (name, body) in [
        (
            "create_jmap_mailbox",
            function_body(MAILBOXES_STORAGE, "pub async fn create_jmap_mailbox"),
        ),
        (
            "update_jmap_mailbox",
            function_body(MAILBOXES_STORAGE, "pub async fn update_jmap_mailbox"),
        ),
    ] {
        assert!(
            body.contains("MailboxDisplayName::new")
                && body.contains("ensure_mailbox_name_available_in_tx"),
            "{name} must validate JMAP mailbox names and sibling collisions through the shared storage policy"
        );
    }
    assert!(
        MAILBOXES_STORAGE.contains("MailboxNamePolicy::canonical_key"),
        "storage duplicate checks must use shared mailbox canonical keys"
    );
}

#[test]
fn mailbox_hierarchy_and_subscriptions_are_canonical_storage() {
    assert_schema_contains_all(&[
        "parent_mailbox_id UUID",
        "CREATE TABLE mailbox_subscriptions",
        "PRIMARY KEY (tenant_id, mailbox_account_id, mailbox_id, subscriber_account_id)",
        "CREATE INDEX mailbox_subscriptions_subscriber_idx",
    ]);
    assert!(
        MAILBOXES_STORAGE.contains("mb.parent_mailbox_id")
            && MAILBOXES_STORAGE.contains("COALESCE(ms.is_subscribed, TRUE)")
            && MAILBOXES_STORAGE.contains("ensure_mailbox_parent_valid_in_tx")
            && MAILBOXES_STORAGE.contains("set_mailbox_subscription"),
        "mailbox storage must expose mailbox hierarchy and persisted subscription state"
    );
}

#[test]
fn system_mailbox_creation_uses_canonical_backend_names() {
    let admin_storage = [ADMIN_STORAGE, ADMIN_PROVISIONING_STORAGE].concat();
    assert!(
        MAILBOXES_STORAGE.contains("\"inbox\", \"INBOX\", 0, 365")
            && MAILBOXES_STORAGE.contains("\"trash\", \"Trash\", 30, 365"),
        "IMAP mailbox bootstrap must store canonical system display names"
    );
    assert!(
        admin_storage.contains("'inbox', 'INBOX', 0, 365"),
        "new account creation must store canonical INBOX display name"
    );
    assert!(
        INBOUND_STORAGE.contains("\"inbox\",\n                        \"INBOX\""),
        "inbound final delivery must create the canonical INBOX display name"
    );
}

#[test]
fn runtime_access_paths_have_scaling_indexes() {
    assert_schema_contains_all(&[
        "CREATE INDEX mailbox_messages_visible_uid_idx",
        "ON mailbox_messages (tenant_id, account_id, mailbox_id, imap_uid)",
        "WHERE visibility = 'visible'",
        "CREATE INDEX mailbox_messages_visible_account_message_idx",
        "ON mailbox_messages (tenant_id, account_id, message_id, mailbox_id)",
        "CREATE INDEX mail_search_documents_account_message_idx",
        "ON mail_search_documents (account_id, message_id, mailbox_message_id)",
        "CREATE INDEX mail_change_log_account_cursor_idx",
        "ON mail_change_log (tenant_id, account_id, cursor)",
        "CREATE INDEX submission_queue_worker_due_idx",
        "ON submission_queue (next_attempt_at, created_at, id)",
        "WHERE status IN ('queued', 'ready', 'deferred')",
        "CREATE INDEX attachment_extraction_jobs_blob_idx",
        "ON attachment_extraction_jobs (tenant_id, blob_id)",
        "CREATE INDEX blob_placements_fetch_idx",
        "ON blob_placements (tenant_id, domain_id, blob_id, blob_kind)",
    ]);
    assert!(
        IMAP_STORAGE.contains("ORDER BY mm.imap_uid ASC")
            && JMAP_QUERIES_STORAGE.contains("GROUP BY s.message_id")
            && PROTOCOLS_STORAGE.contains("ORDER BY cursor ASC"),
        "protocol runtime SQL must retain the mailbox UID, JMAP query, and change replay paths covered by scaling indexes"
    );
    assert!(
        OUTBOUND_STORAGE.contains("q.status IN ('queued', 'ready', 'deferred')")
            && OUTBOUND_STORAGE.contains("ORDER BY q.created_at ASC, q.id ASC"),
        "submission worker SQL must match the due-queue access path covered by submission_queue_worker_due_idx"
    );
    assert!(
        ATTACHMENTS_STORAGE.contains("INSERT INTO attachment_extraction_jobs")
            && BLOB_STORE_STORAGE.contains("FROM attachment_extraction_jobs"),
        "attachment extraction and blob cleanup blocker SQL must keep the access paths covered by extraction indexes"
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
    assert!(
        JMAP_BLOBS_STORAGE.contains("strip_protected_bcc_headers"),
        "JMAP raw-message blob projection must keep protected Bcc stripping outside canonical message storage"
    );
    for table_name in [
        "mail_search_documents",
        "document_projections",
        "document_chunks",
        "mail_change_log",
        "jmap_query_states",
        "activesync_devices",
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
        "activesync_devices",
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
        "CREATE TABLE activesync_devices",
        "device_type TEXT NOT NULL DEFAULT 'unknown' CHECK (btrim(device_type) <> '')",
        "policy_key TEXT CHECK (policy_key IS NULL OR (btrim(policy_key) <> '' AND length(policy_key) <= 64))",
        "pending_policy_key TEXT CHECK (pending_policy_key IS NULL OR (btrim(pending_policy_key) <> '' AND length(pending_policy_key) <= 64))",
        "provision_status TEXT NOT NULL DEFAULT 'pending' CHECK (provision_status IN ('pending', 'active', 'blocked'))",
        "wipe_status TEXT NOT NULL DEFAULT 'none' CHECK (wipe_status IN ('none', 'pending', 'acknowledged'))",
        "account_wipe_status TEXT NOT NULL DEFAULT 'none' CHECK (account_wipe_status IN ('none', 'pending', 'acknowledged'))",
        "UNIQUE (tenant_id, account_id, device_id)",
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
        ACTIVESYNC_STORAGE.contains("INSERT INTO activesync_sync_cursors")
            && ACTIVESYNC_STORAGE.contains("INSERT INTO activesync_devices")
            && ACTIVESYNC_STORAGE.contains("UPDATE activesync_devices")
            && ACTIVESYNC_STORAGE.contains("state_json")
            && ACTIVESYNC_STORAGE.contains("DELETE FROM activesync_sync_cursors")
            && ACTIVESYNC_STORAGE.contains("expires_at <= NOW()")
            && !ACTIVESYNC_STORAGE.contains("activesync_sync_states")
            && !MESSAGE_OPS_STORAGE.contains("activesync_sync_states"),
        "ActiveSync storage must use v2 cursor rows, not the retired snapshot table"
    );
    let cleanup_start = ACTIVESYNC_STORAGE
        .find("DELETE FROM activesync_sync_cursors")
        .expect("ActiveSync cursor cleanup SQL is required");
    let cleanup_end = (cleanup_start + 260).min(ACTIVESYNC_STORAGE.len());
    let cleanup_sql = &ACTIVESYNC_STORAGE[cleanup_start..cleanup_end];
    for canonical_table in ["messages", "mailboxes", "mailbox_messages"] {
        assert!(
            !cleanup_sql.contains(canonical_table),
            "ActiveSync cursor cleanup must not delete canonical mailbox data"
        );
    }
}

#[test]
fn runtime_collaboration_sql_uses_canonical_v2_columns() {
    assert!(
        !WORKSPACE_STORAGE.contains("FROM calendar_events\n            WHERE contacts."),
        "workspace calendar queries must filter calendar_events, not contacts"
    );
    assert!(
        !WORKSPACE_STORAGE.contains("a.media_type"),
        "workspace attachment queries must not read retired attachments.media_type"
    );
    assert!(
        WORKSPACE_STORAGE.contains("mp.content_type"),
        "workspace attachment queries must read attachment MIME type from mime_parts"
    );
    assert!(
        WORKSPACE_STORAGE
            .contains("WHERE contacts.tenant_id = $1 AND contacts.owner_account_id = $2"),
        "workspace contact queries must qualify contact ownership columns"
    );

    for (name, source) in [
        ("workspace.rs", WORKSPACE_STORAGE),
        ("collaboration.rs", COLLABORATION_STORAGE),
        ("message_ops.rs", MESSAGE_OPS_STORAGE),
        ("activesync.rs", ACTIVESYNC_STORAGE),
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
