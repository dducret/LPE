use super::super::sync::DRAFTS_FOLDER_ID;
use super::*;
use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

fn empty_session() -> MapiSession {
    MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        created_at: SystemTime::UNIX_EPOCH,
        last_seen_at: SystemTime::UNIX_EPOCH,
        first_request_type: "Connect".to_string(),
        first_request_id: "test:1".to_string(),
        last_request_type: "Connect".to_string(),
        last_request_id: "test:1".to_string(),
        request_count: 1,
        execute_request_count: 0,
        next_handle: 1,
        handles: HashMap::new(),
        message_statuses: HashMap::new(),
        message_save_generations: HashMap::new(),
        message_handle_generations: HashMap::new(),
        pending_message_recipient_replacements: HashMap::new(),
        pending_message_attachments: HashMap::new(),
        pending_attachment_parent_messages: HashMap::new(),
        pending_attachment_deletions: HashSet::new(),
        pending_embedded_message_ids: HashMap::new(),
        pending_embedded_message_attachments: HashMap::new(),
        saved_embedded_messages: HashMap::new(),
        saved_search_folder_definitions: HashMap::new(),
        special_folder_aliases: HashMap::new(),
        deleted_advertised_special_folders: HashSet::new(),
        deleted_search_folder_definitions: HashSet::new(),
        named_properties: HashMap::new(),
        named_property_ids: HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: VecDeque::new(),
        completed_execute_requests: HashMap::new(),
        completed_execute_request_order: VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        default_view_advertisements: HashMap::new(),
        inbox_associated_config_stream_handles: HashSet::new(),
        inbox_rule_organizer_stream_handles: HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    }
}

fn single_rop_buffer(rop: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(rop.len() as u16).to_le_bytes());
    buffer.extend_from_slice(rop);
    buffer.extend_from_slice(&1u32.to_le_bytes());
    buffer
}

fn rop_buffer(rops: &[u8], handles: &[u32]) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(rops.len() as u16).to_le_bytes());
    buffer.extend_from_slice(rops);
    for handle in handles {
        buffer.extend_from_slice(&handle.to_le_bytes());
    }
    buffer
}

#[test]
fn deduplicate_mapi_identity_requests_keeps_distinct_kinds() {
    let canonical_id = Uuid::from_u128(0x6d617069_6964_5265_7100_000000000001);
    let other_id = Uuid::from_u128(0x6d617069_6964_5265_7100_000000000002);
    let requests = vec![
        MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
            canonical_id,
            reserved_global_counter: None,
            source_key: None,
        },
        MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
            canonical_id,
            reserved_global_counter: None,
            source_key: None,
        },
        MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::ConversationAction,
            canonical_id,
            reserved_global_counter: None,
            source_key: None,
        },
        MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
            canonical_id: other_id,
            reserved_global_counter: None,
            source_key: None,
        },
    ];

    let deduplicated = deduplicate_mapi_identity_requests(requests);

    assert_eq!(deduplicated.len(), 3);
    assert_eq!(
        deduplicated[0].object_kind,
        MapiIdentityObjectKind::SearchFolderDefinition
    );
    assert_eq!(
        deduplicated[1].object_kind,
        MapiIdentityObjectKind::ConversationAction
    );
    assert_eq!(deduplicated[2].canonical_id, other_id);
}

fn release_handle_zero_rop_buffer() -> Vec<u8> {
    single_rop_buffer(&[0x01, 0x00, 0x00])
}

fn mailbox(id: &str, role: &str, name: &str) -> JmapMailbox {
    JmapMailbox {
        id: Uuid::parse_str(id).unwrap(),
        parent_id: None,
        role: role.to_string(),
        name: name.to_string(),
        sort_order: 40,
        modseq: 40,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }
}

#[test]
fn merge_requested_mailboxes_adds_custom_identity_rows() {
    let inbox = mailbox("11111111-1111-1111-1111-111111111111", "inbox", "Inbox");
    let custom = mailbox("22222222-2222-2222-2222-222222222222", "custom", "RCA Sync");
    let missing = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let mut loaded = vec![inbox.clone()];
    let all_mailboxes = vec![inbox, custom.clone()];

    merge_requested_mailboxes(
        &mut loaded,
        &all_mailboxes,
        &[custom.id, custom.id, missing],
    );

    assert_eq!(loaded.len(), 2);
    assert!(loaded.iter().any(|mailbox| mailbox.id == custom.id));
}

#[test]
fn search_folder_role_summary_includes_builtin_flags() {
    let roles = format_search_folder_roles(&[lpe_storage::SearchFolderDefinition {
        id: Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        account_id: Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
        role: "reminders".to_string(),
        display_name: "Reminders".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    }]);

    assert_eq!(roles, "reminders:exchange_builtin:mixed:builtin");
}

#[test]
fn access_plan_includes_long_term_id_source_in_trailing_replid_form() {
    let object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 9,
    );
    let global_counter = crate::mapi::identity::global_counter_from_store_id(object_id)
        .expect("dynamic object id has a global counter");
    let mut rop = vec![0x43, 0x00, 0x00];
    rop.extend_from_slice(&crate::mapi::identity::globcnt_bytes(global_counter));
    rop.extend_from_slice(&1u16.to_le_bytes());

    let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

    assert!(
        plan.object_ids.contains(&object_id),
        "object_id={object_id:#018x} plan={:?}",
        plan.object_ids
    );
}

#[test]
fn access_plan_resolves_learned_special_folder_aliases() {
    let alias_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 9,
    );
    let mut session = empty_session();
    session.record_special_folder_alias(alias_id, crate::mapi::identity::JUNK_FOLDER_ID);
    let mut rop = vec![0x02, 0x00, 0x00, 0x01];
    rop.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(alias_id)
            .expect("alias id is encodable"),
    );
    rop.push(0);

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&rop));

    assert_eq!(
        plan.object_ids,
        vec![crate::mapi::identity::JUNK_FOLDER_ID],
        "plan={:?}",
        plan.object_ids
    );
}

#[test]
fn access_plan_does_not_decode_get_properties_payload_as_object_id() {
    let mut rop = vec![0x07, 0x00, 0x00];
    rop.extend_from_slice(&[0x01, 0x00]);
    rop.extend_from_slice(&1u16.to_le_bytes());
    rop.extend_from_slice(&[0x00, 0x00, 0x2f, 0x00]);

    let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

    assert!(plan.object_ids.is_empty(), "plan={:?}", plan.object_ids);
}

#[test]
fn access_plan_loads_common_views_associated_contents_on_table_open() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::Folder {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    session.next_handle = 2;

    let associated_get_contents_table = [0x05, 0x00, 0x00, 0x01, 0x02];
    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&associated_get_contents_table));

    assert!(
        plan.requires_associated_contents,
        "plan should preload Common Views associated rows: {plan:?}"
    );

    let normal_get_contents_table = [0x05, 0x00, 0x00, 0x01, 0x00];
    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&normal_get_contents_table));

    assert!(
        !plan.requires_associated_contents,
        "normal Common Views contents should not preload associated rows: {plan:?}"
    );
}

#[test]
fn access_plan_cached_mode_transfer_state_get_buffer_uses_session_state() {
    let mut session = empty_session();
    session.handles.insert(
        0x38,
        MapiObject::SynchronizationSource {
            folder_id: INBOX_FOLDER_ID,
            mailbox_id: None,
            checkpoint_kind: crate::store::MapiCheckpointKind::Content,
            checkpoint_change_sequence: 88,
            checkpoint_modseq: 44,
            checkpoint_store_allowed: true,
            checkpoint_skip_reason: "",
            checkpoint_zero_delta: false,
            sync_type: 0x01,
            initial_state: vec![0x01, 0x02],
            state: vec![0x03, 0x04],
            state_upload_property_tag: None,
            state_upload_buffer: Vec::new(),
            client_state_uploaded_bytes: 0,
            client_state_uploaded_marker_mask: 0,
            incremental_transfer_buffer: None,
            transfer_buffer: vec![0xaa],
            transfer_position: 1,
        },
    );
    session.handles.insert(
        0x39,
        MapiObject::Folder {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    session.next_handle = 0x3a;
    let mut rops = vec![
        0x01, 0x00, 0x00, // Release handle slot 0.
        0x82, 0x00, 0x01, 0x02, // SynchronizationGetTransferState slot 1 -> slot 2.
        0x4e, 0x00, 0x02, // FastTransferSourceGetBuffer from slot 2.
    ];
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let plan = plan_mapi_store_access(&session, &rop_buffer(&rops, &[0x39, 0x38, u32::MAX]));

    assert!(
        !plan.requires_full_snapshot,
        "cached-mode transfer state should not force a full snapshot: {plan:?}"
    );
    assert_eq!(
        plan.object_ids,
        vec![COMMON_VIEWS_FOLDER_ID, INBOX_FOLDER_ID]
    );
}

#[test]
fn access_plan_hierarchy_query_ignores_unrelated_live_calendar_handle() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::HierarchyTable {
            folder_id: crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            columns: vec![
                PID_TAG_MID,
                PID_TAG_CONTAINER_CLASS_W,
                PID_TAG_DISPLAY_NAME_W,
                PID_TAG_CONTENT_UNREAD_COUNT,
            ],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 22,
        },
    );
    session.handles.insert(
        2,
        MapiObject::Folder {
            folder_id: CALENDAR_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    session.next_handle = 3;
    let query_rows = [0x15, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00];

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&query_rows));

    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
    assert!(!plan.requires_associated_contents, "plan={plan:?}");
    assert_eq!(
        plan.object_ids,
        vec![crate::mapi::identity::IPM_SUBTREE_FOLDER_ID],
        "plan={plan:?}"
    );
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
    assert!(
        requires_snapshot_backed_contents(&plan, &[]),
        "IPM subtree hierarchy rows need collaboration item counts: {plan:?}"
    );
}

#[test]
fn access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::HierarchyTable {
            folder_id: crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            columns: default_hierarchy_columns(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.handles.insert(
        2,
        MapiObject::Folder {
            folder_id: CALENDAR_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    session.next_handle = 3;
    let mut rops = vec![0x12, 0x00, 0x00, 0x00, 0x00, 0x00];
    rops.extend_from_slice(&[0x18, 0x00, 0x00, 0x00]);
    rops.extend_from_slice(&0i32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&[0x15, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00]);

    let plan = plan_mapi_store_access(&session, &rop_buffer(&rops, &[1]));

    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
    assert!(!plan.requires_associated_contents, "plan={plan:?}");
    assert_eq!(
        plan.object_ids,
        vec![crate::mapi::identity::IPM_SUBTREE_FOLDER_ID],
        "plan={plan:?}"
    );
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
    assert!(
        requires_snapshot_backed_contents(&plan, &[]),
        "IPM subtree hierarchy rows need collaboration item counts: {plan:?}"
    );
}

#[test]
fn access_plan_contents_seek_from_end_still_requires_full_snapshot() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let mut seek_row = vec![0x18, 0x00, 0x02];
    seek_row.extend_from_slice(&0i32.to_le_bytes());
    seek_row.push(0x01);
    seek_row.extend_from_slice(&0u16.to_le_bytes());

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&seek_row));

    assert!(plan.requires_full_snapshot, "plan={plan:?}");
}

#[test]
fn access_plan_normal_mail_contents_seek_uses_content_window_total() {
    let session = empty_session();
    let mut handles = HashMap::new();
    handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: DRAFTS_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };
    let mut next_handle = 2;
    let mut handle_slots = vec![1];
    let mut payload = vec![0x00];
    payload.extend_from_slice(&1i32.to_le_bytes());
    payload.push(0x01);
    let request = RopRequest {
        rop_id: 0x18,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    simulate_table_access(
        &mut plan,
        &session,
        &mut handles,
        &mut next_handle,
        &mut handle_slots,
        &request,
    );
    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].folder_id, DRAFTS_FOLDER_ID);
    assert_eq!(plan.content_queries[0].offset, 1);
    assert_eq!(plan.content_queries[0].limit, 0);
}

#[test]
fn access_plan_normal_mail_contents_setcolumns_prefetches_first_row() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_DELIVERY_TIME,
                order: 0x01,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let mut set_columns = vec![0x12, 0x00, 0x00, 0x00];
    set_columns.extend_from_slice(&1u16.to_le_bytes());
    set_columns.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&set_columns));

    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].folder_id, INBOX_FOLDER_ID);
    assert_eq!(plan.content_queries[0].offset, 0);
    assert_eq!(plan.content_queries[0].limit, 1);
}

#[test]
fn access_plan_non_mail_contents_query_rows_requires_full_snapshot() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: CALENDAR_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let query_rows = [0x15, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00];

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&query_rows));

    assert!(plan.requires_full_snapshot, "plan={plan:?}");
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
}

#[test]
fn access_plan_associated_contents_query_rows_stays_store_selective() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let query_rows = [0x15, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00];

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&query_rows));

    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
}

#[test]
fn access_plan_common_views_query_rows_requests_common_views_backing_data() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 7,
        },
    );
    session.next_handle = 2;
    let query_rows = [0x15, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00];

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&query_rows));

    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
    assert_eq!(
        plan.object_ids,
        vec![COMMON_VIEWS_FOLDER_ID],
        "plan={plan:?}"
    );
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
}

#[test]
fn common_views_object_id_requires_snapshot_backed_contents() {
    let plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: vec![COMMON_VIEWS_FOLDER_ID],
        content_queries: Vec::new(),
    };

    assert!(requires_snapshot_backed_contents(&plan, &[]));
}

#[test]
fn default_contacts_object_id_requires_snapshot_backed_contents() {
    let plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: vec![CONTACTS_FOLDER_ID],
        content_queries: Vec::new(),
    };

    assert!(requires_snapshot_backed_contents(&plan, &[]));
}

#[test]
fn special_content_folder_ids_require_snapshot_backed_contents() {
    for folder_id in [
        TODO_SEARCH_FOLDER_ID,
        REMINDERS_FOLDER_ID,
        NOTES_FOLDER_ID,
        JOURNAL_FOLDER_ID,
        TRACKED_MAIL_PROCESSING_FOLDER_ID,
        crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
        crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID,
        crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
        crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    ] {
        let plan = MapiAccessPlan {
            requires_full_snapshot: false,
            requires_associated_contents: false,
            object_ids: vec![folder_id],
            content_queries: Vec::new(),
        };

        assert!(
            requires_snapshot_backed_contents(&plan, &[]),
            "folder_id=0x{folder_id:016x}"
        );
    }
}

#[test]
fn access_plan_does_not_apply_mail_default_sort_to_contacts_contents_table() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: CONTACTS_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_DELIVERY_TIME,
                order: 0x01,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let mut rops = vec![
        0x15, 0x00, 0x00, 0x00, 0x01, // RopQueryRows
    ];
    rops.extend_from_slice(&1u16.to_le_bytes());

    let plan = plan_mapi_store_access(&session, &rop_buffer(&rops, &[1]));

    assert!(plan.requires_full_snapshot, "plan={plan:?}");
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
}

#[test]
fn access_plan_query_position_does_not_window_contacts_contents_table() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: CONTACTS_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: vec![MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_DELIVERY_TIME,
                order: 0x01,
            }],
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let rops = [0x17, 0x00, 0x00];

    let plan = plan_mapi_store_access(&session, &rop_buffer(&rops, &[1]));

    assert!(plan.requires_full_snapshot, "plan={plan:?}");
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
}

#[test]
fn access_plan_merges_seek_total_query_with_following_query_rows_window() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 1, 0, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 1, 40, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].offset, 1);
    assert_eq!(plan.content_queries[0].limit, 40);
}

#[test]
fn access_plan_merges_overlapping_content_windows() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 0, 16, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 1, 16, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].offset, 0);
    assert_eq!(plan.content_queries[0].limit, 17);
}

#[test]
fn access_plan_merges_content_window_that_bridges_existing_ranges() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 0, 10, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 20, 10, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 10, 10, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].offset, 0);
    assert_eq!(plan.content_queries[0].limit, 30);
}

#[test]
fn access_plan_merges_total_probe_inside_existing_content_window() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 0, 20, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 5, 0, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].offset, 0);
    assert_eq!(plan.content_queries[0].limit, 20);
}

#[test]
fn access_plan_merges_existing_total_probe_inside_later_content_window() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 5, 0, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 0, 20, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].offset, 0);
    assert_eq!(plan.content_queries[0].limit, 20);
}

#[test]
fn access_plan_merges_total_probe_before_existing_content_window_without_widening() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 5, 20, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 0, 0, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].offset, 5);
    assert_eq!(plan.content_queries[0].limit, 20);
}

#[test]
fn access_plan_merges_existing_total_probe_before_later_content_window_without_widening() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 0, 0, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 5, 20, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].offset, 5);
    assert_eq!(plan.content_queries[0].limit, 20);
}

#[test]
fn access_plan_merges_total_probes_at_different_offsets() {
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };

    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 1, 0, Vec::new());
    add_content_query(&mut plan, DRAFTS_FOLDER_ID, 42, 40, 0, Vec::new());

    assert_eq!(plan.content_queries.len(), 1, "plan={plan:?}");
    assert_eq!(plan.content_queries[0].limit, 0);
}

#[test]
fn access_plan_non_mail_contents_seek_still_requires_full_snapshot() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: CALENDAR_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let mut seek_row = vec![0x18, 0x00, 0x00];
    seek_row.extend_from_slice(&1i32.to_le_bytes());
    seek_row.push(0x01);

    let plan = plan_mapi_store_access(&session, &single_rop_buffer(&seek_row));

    assert!(plan.requires_full_snapshot, "plan={plan:?}");
}

#[test]
fn access_plan_associated_contents_seek_stays_selective() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    session.next_handle = 2;
    let mut rops = vec![0x18, 0x00, 0x01];
    rops.extend_from_slice(&1i32.to_le_bytes());
    rops.push(0x00);
    rops.extend_from_slice(&[0x15, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00]);
    rops.extend_from_slice(&0u16.to_le_bytes());

    let plan = plan_mapi_store_access(&session, &rop_buffer(&rops, &[1]));

    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
    assert_eq!(plan.object_ids, vec![INBOX_FOLDER_ID], "plan={plan:?}");
    assert!(plan.content_queries.is_empty(), "plan={plan:?}");
}

#[test]
fn access_plan_associated_contents_find_row_stays_selective() {
    let session = empty_session();
    let mut handles = HashMap::new();
    handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            associated: true,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };
    let mut next_handle = 2;
    let mut handle_slots = vec![1];
    let request = RopRequest {
        rop_id: 0x4F,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };

    assert!(!rop_requires_full_snapshot(0x4F));
    simulate_table_access(
        &mut plan,
        &session,
        &mut handles,
        &mut next_handle,
        &mut handle_slots,
        &request,
    );
    assert!(!plan.requires_full_snapshot, "plan={plan:?}");
}

#[test]
fn access_plan_normal_contents_find_row_still_requires_full_snapshot() {
    let session = empty_session();
    let mut handles = HashMap::new();
    handles.insert(
        1,
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        },
    );
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };
    let mut next_handle = 2;
    let mut handle_slots = vec![1];
    let request = RopRequest {
        rop_id: 0x4F,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };

    simulate_table_access(
        &mut plan,
        &session,
        &mut handles,
        &mut next_handle,
        &mut handle_slots,
        &request,
    );
    assert!(plan.requires_full_snapshot, "plan={plan:?}");
}

#[test]
fn access_plan_does_not_fetch_virtual_default_conversation_action_identity() {
    let default_action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
    let folder_id = crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID;
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::ConversationAction {
            folder_id,
            conversation_action_id: default_action_id,
        },
    );

    let plan = plan_mapi_store_access(&session, &release_handle_zero_rop_buffer());

    assert_eq!(plan.object_ids, vec![folder_id], "plan={plan:?}");
}

#[test]
fn access_plan_does_not_fetch_default_common_views_shortcut_identity() {
    let shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::NavigationShortcut {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            shortcut_id,
        },
    );

    let plan = plan_mapi_store_access(&session, &release_handle_zero_rop_buffer());

    assert_eq!(
        plan.object_ids,
        vec![COMMON_VIEWS_FOLDER_ID],
        "plan={plan:?}"
    );
}

#[test]
fn access_plan_does_not_fetch_default_common_views_named_view_identity() {
    let view_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::CommonViewNamedView {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            view_id,
        },
    );

    let plan = plan_mapi_store_access(&session, &release_handle_zero_rop_buffer());

    assert_eq!(
        plan.object_ids,
        vec![COMMON_VIEWS_FOLDER_ID],
        "plan={plan:?}"
    );
}

#[test]
fn access_plan_does_not_fetch_default_folder_named_view_identity() {
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::CommonViewNamedView {
            folder_id: CONTACTS_FOLDER_ID,
            view_id: mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        },
    );

    let plan = plan_mapi_store_access(&session, &release_handle_zero_rop_buffer());

    assert_eq!(plan.object_ids, vec![CONTACTS_FOLDER_ID], "plan={plan:?}");
}

#[test]
fn access_plan_does_not_fetch_virtual_inbox_associated_config_identity() {
    let config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC);
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::AssociatedConfig {
            folder_id: INBOX_FOLDER_ID,
            config_id,
            saved_message: None,
        },
    );

    let plan = plan_mapi_store_access(&session, &release_handle_zero_rop_buffer());

    assert_eq!(plan.object_ids, vec![INBOX_FOLDER_ID], "plan={plan:?}");
}

#[test]
fn access_plan_fetches_non_virtual_quick_step_associated_config_identity() {
    let config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4);
    let folder_id = crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID;
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message: None,
        },
    );

    let plan = plan_mapi_store_access(&session, &release_handle_zero_rop_buffer());

    assert_eq!(plan.object_ids, vec![folder_id, config_id], "plan={plan:?}");
}

#[test]
fn access_plan_does_not_fetch_virtual_contact_associated_config_identity() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x54);
    let config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FF00_0054);
    let mut session = empty_session();
    session.handles.insert(
        1,
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message: None,
        },
    );

    let plan = plan_mapi_store_access(&session, &release_handle_zero_rop_buffer());

    assert_eq!(plan.object_ids, vec![folder_id], "plan={plan:?}");
}

#[test]
fn access_plan_does_not_decode_set_properties_payload_as_import_source_key() {
    let mut rop = vec![0x0A, 0x00, 0x00];
    rop.extend_from_slice(&[0x01, 0x00]);
    rop.extend_from_slice(&PID_TAG_SOURCE_KEY.to_le_bytes());
    rop.extend_from_slice(&22u16.to_le_bytes());
    rop.extend_from_slice(&crate::mapi::identity::source_key_for_object_id(
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 12,
        ),
    ));

    let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

    assert!(plan.object_ids.is_empty(), "plan={:?}", plan.object_ids);
}

#[test]
fn access_plan_does_not_decode_set_properties_payload_as_read_state_change() {
    let mut rop = vec![0x0A, 0x00, 0x00];
    rop.extend_from_slice(&[0x01, 0x00]);
    rop.extend_from_slice(&PID_TAG_OST_OSTID.to_le_bytes());
    rop.extend_from_slice(&20u16.to_le_bytes());
    rop.extend_from_slice(&[
        0xea, 0x33, 0x94, 0x46, 0x27, 0xb9, 0x4a, 0x9c, 0xb0, 0xde, 0x87, 0x3f, 0x03, 0xa3, 0x53,
        0x76, 0x00, 0x00, 0x00, 0x00,
    ]);

    let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

    assert!(plan.object_ids.is_empty(), "plan={:?}", plan.object_ids);
}

#[test]
fn access_plan_decodes_synchronization_import_read_state_changes() {
    let message_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 42,
    );
    let message_id_bytes = crate::mapi::identity::wire_id_bytes_from_object_id(message_id)
        .expect("MAPI store id is encodable");
    let mut rop = vec![0x80, 0x00, 0x00];
    rop.extend_from_slice(&11u16.to_le_bytes());
    rop.extend_from_slice(&8u16.to_le_bytes());
    rop.extend_from_slice(&message_id_bytes);
    rop.push(1);

    let buffer = single_rop_buffer(&rop);
    let (requests, _) = split_rop_buffer(&buffer).expect("ROP buffer should split");
    let mut cursor = Cursor::new(requests);
    let request = read_rop_request(&mut cursor).expect("ROP request should parse");
    assert_eq!(
        request.import_read_state_changes(),
        vec![(message_id, false)]
    );
    assert_eq!(cursor.remaining(), 0);
    assert!(!rop_requires_full_snapshot(0x80));

    let plan = plan_mapi_store_access(&empty_session(), &buffer);

    assert_eq!(
        plan.object_ids,
        vec![message_id],
        "requires_full_snapshot={}",
        plan.requires_full_snapshot
    );
}

#[test]
fn access_plan_preloads_long_term_id_from_id_source() {
    let object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 59,
    );
    let mut rop = vec![0x43, 0x00, 0x00];
    rop.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(object_id)
            .expect("MAPI store id is encodable"),
    );

    let plan = plan_mapi_store_access(&empty_session(), &single_rop_buffer(&rop));

    assert_eq!(plan.object_ids, vec![object_id]);
}

#[test]
fn missing_mapi_identity_summary_names_object_and_canonical_ids() {
    let missing_id = Uuid::parse_str("fb129372-d6b6-4d69-99f7-977ab2a8093f").unwrap();
    let loaded_id = Uuid::parse_str("17b18079-e962-4d53-9d2f-d68cfb37dcad").unwrap();
    let identities = vec![
        MapiIdentityLookupRecord {
            object_kind: MapiIdentityObjectKind::Contact,
            canonical_id: missing_id,
            object_id: 0x0000_0000_003b_0001,
            source_key: Vec::new(),
        },
        MapiIdentityLookupRecord {
            object_kind: MapiIdentityObjectKind::Contact,
            canonical_id: loaded_id,
            object_id: 0x0000_0000_0037_0001,
            source_key: Vec::new(),
        },
    ];

    assert_eq!(
            format_missing_mapi_identities(
                &identities,
                MapiIdentityObjectKind::Contact,
                &[loaded_id],
            ),
            "object_id=0x00000000003b0001;canonical_id=fb129372-d6b6-4d69-99f7-977ab2a8093f;kind=contact"
        );
}

#[test]
fn requested_store_identity_requires_backing_row_for_optional_mapi_state() {
    let orphan_id = Uuid::parse_str("dcf3fa88-eefa-4231-a932-7747c6f38fb5").unwrap();
    let live_id = Uuid::parse_str("28848baa-5f82-44cf-8ac6-26e1d6ffcc96").unwrap();
    let live_mailbox_id = Uuid::parse_str("87b34a59-29dd-4638-a2e8-91e8f7616f36").unwrap();
    let live_mailbox_ids = HashSet::from([live_mailbox_id]);
    let live_search_ids = HashSet::from([live_id]);
    let live_config_ids = HashSet::from([live_id]);
    let orphan_identity = MapiIdentityLookupRecord {
        object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
        canonical_id: orphan_id,
        object_id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 91,
        ),
        source_key: Vec::new(),
    };
    let live_identity = MapiIdentityLookupRecord {
        object_kind: MapiIdentityObjectKind::SearchFolderDefinition,
        canonical_id: live_id,
        object_id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 92,
        ),
        source_key: Vec::new(),
    };
    let mailbox_identity = MapiIdentityLookupRecord {
        object_kind: MapiIdentityObjectKind::Mailbox,
        canonical_id: orphan_id,
        object_id: INBOX_FOLDER_ID,
        source_key: Vec::new(),
    };
    let live_mailbox_identity = MapiIdentityLookupRecord {
        object_kind: MapiIdentityObjectKind::Mailbox,
        canonical_id: live_mailbox_id,
        object_id: INBOX_FOLDER_ID,
        source_key: Vec::new(),
    };
    let orphan_config_identity = MapiIdentityLookupRecord {
        object_kind: MapiIdentityObjectKind::AssociatedConfig,
        canonical_id: orphan_id,
        object_id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 93,
        ),
        source_key: Vec::new(),
    };
    let live_config_identity = MapiIdentityLookupRecord {
        object_kind: MapiIdentityObjectKind::AssociatedConfig,
        canonical_id: live_id,
        object_id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 94,
        ),
        source_key: Vec::new(),
    };

    assert!(!requested_identity_has_backing_row(
        &orphan_identity,
        &live_mailbox_ids,
        &live_search_ids,
        &live_config_ids
    ));
    assert!(requested_identity_has_backing_row(
        &live_identity,
        &live_mailbox_ids,
        &live_search_ids,
        &live_config_ids
    ));
    assert!(!requested_identity_has_backing_row(
        &mailbox_identity,
        &live_mailbox_ids,
        &live_search_ids,
        &live_config_ids
    ));
    assert!(requested_identity_has_backing_row(
        &live_mailbox_identity,
        &live_mailbox_ids,
        &live_search_ids,
        &live_config_ids
    ));
    assert!(!requested_identity_has_backing_row(
        &orphan_config_identity,
        &live_mailbox_ids,
        &live_search_ids,
        &live_config_ids
    ));
    assert!(requested_identity_has_backing_row(
        &live_config_identity,
        &live_mailbox_ids,
        &live_search_ids,
        &live_config_ids
    ));
}

#[test]
fn unresolved_mapi_identity_summary_classifies_expected_special_and_invalid_ids() {
    let invalid_replid_id = 0x0201_047c_2800_0002;
    let dynamic_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 10,
    );
    let common_view_named_view_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
    let common_view_shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
    let quick_step_config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4);
    let contact_sync_config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FF00_0054);
    let conversation_action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);

    assert_eq!(
            format_unresolved_mapi_object_scopes(&[
                ROOT_FOLDER_ID,
                common_view_named_view_id,
                common_view_shortcut_id,
                quick_step_config_id,
                contact_sync_config_id,
                conversation_action_id,
                dynamic_id,
                invalid_replid_id
            ]),
            format!(
                "{ROOT_FOLDER_ID:#018x}:advertised_special_folder,{common_view_named_view_id:#018x}:virtual_common_view_named_view,{common_view_shortcut_id:#018x}:virtual_common_view_navigation_shortcut,{quick_step_config_id:#018x}:virtual_quick_step_associated_config,{contact_sync_config_id:#018x}:virtual_contact_associated_config,{conversation_action_id:#018x}:virtual_conversation_action,{dynamic_id:#018x}:unallocated_store_object,{invalid_replid_id:#018x}:foreign_or_invalid_replid"
            )
        );
}

#[test]
fn expected_unbacked_mapi_objects_include_virtual_outlook_config_messages() {
    let dynamic_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 10,
    );
    let inbox_default_config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC);
    let quick_step_config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4);
    let contact_sync_config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FF00_0054);
    let common_view_named_view_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
    let common_view_shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
    let conversation_action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);

    assert!(is_expected_unbacked_mapi_object(ROOT_FOLDER_ID));
    assert!(is_expected_unbacked_mapi_object(inbox_default_config_id));
    assert!(is_expected_unbacked_mapi_object(quick_step_config_id));
    assert!(is_expected_unbacked_mapi_object(contact_sync_config_id));
    assert!(is_expected_unbacked_mapi_object(common_view_named_view_id));
    assert!(is_expected_unbacked_mapi_object(common_view_shortcut_id));
    assert!(is_expected_unbacked_mapi_object(conversation_action_id));
    assert!(!is_expected_unbacked_mapi_object(dynamic_id));
}
