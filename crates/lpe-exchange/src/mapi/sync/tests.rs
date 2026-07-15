use super::*;
use crate::mapi::rop::RopRequest;

fn sync_principal(account_id: Uuid) -> AccountPrincipal {
    AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "test@l-p-e.ch".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    }
}

fn mailbox(id: u128, role: &str, name: &str) -> JmapMailbox {
    JmapMailbox {
        id: Uuid::from_u128(id),
        parent_id: None,
        role: role.to_string(),
        name: name.to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }
}

fn assert_associated_fai_core_payload(item: &mapi_mailstore::ContentTransferFaiItemDebug) {
    assert_eq!(item.associated, Some(true));
    assert_has_tags(
        item,
        &[
            PID_TAG_SOURCE_KEY,
            PID_TAG_PARENT_SOURCE_KEY,
            PID_TAG_ENTRY_ID,
            PID_TAG_RECORD_KEY,
            PID_TAG_SEARCH_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_ASSOCIATED,
            PID_TAG_MESSAGE_FLAGS,
            PID_TAG_MESSAGE_SIZE,
            PID_TAG_LAST_MODIFICATION_TIME,
        ],
    );
    assert!(item.source_key_len > 0);
    assert!(item.parent_source_key_len > 0);
    assert!(item.entry_id_len > 0);
    assert!(item.change_number_in_final_cnset_fai);
}

fn assert_has_tags(item: &mapi_mailstore::ContentTransferFaiItemDebug, tags: &[u32]) {
    for tag in tags {
        assert!(
            item.property_tags.contains(tag),
            "missing 0x{tag:08x} on {} / {}",
            item.message_class,
            item.subject
        );
    }
}

fn associated_content_sync_buffer(
    account_id: Uuid,
    folder_id: u64,
    objects: &[mapi_mailstore::SpecialMessageSyncFact],
) -> Vec<u8> {
    mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
        account_id,
        0x01,
        0x0010,
        0x0000_0001 | 0x0000_0004 | 0x0000_0008,
        &[],
        folder_id,
        &[],
        &[],
        &[],
        objects,
        &[],
        &[],
        &[],
        &[],
        &[],
        objects,
        &[],
        &[],
        1,
    )
}

fn assert_fai_boundary_summary(
    buffer: &[u8],
    summary: &mapi_mailstore::ContentTransferFaiDebugSummary,
    expected_count: usize,
) {
    assert_eq!(summary.fai_items.len(), expected_count);
    let mut previous_end = 0usize;
    let mut total_item_bytes = 0usize;
    for item in &summary.fai_items {
        assert!(item.item_start_offset >= previous_end);
        assert!(item.item_end_offset > item.item_start_offset);
        assert_eq!(
            item.item_byte_len,
            item.item_end_offset - item.item_start_offset
        );
        assert!(item.item_end_offset <= buffer.len());
        assert!(!item.payload_preview_hex.is_empty());
        assert!(!item.payload_tail_hex.is_empty());
        assert!(item.source_key_len > 0);
        assert!(item.parent_source_key_len > 0);
        assert!(item.entry_id_len > 0);
        assert!(item.record_key_len > 0);
        assert!(item.change_key_len > 0);
        assert!(item.predecessor_change_list_len > 0);
        assert!(item.property_tags.contains(&PID_TAG_SOURCE_KEY));
        assert!(item.property_tags.contains(&PID_TAG_PARENT_SOURCE_KEY));
        assert!(item.property_tags.contains(&PID_TAG_MESSAGE_CLASS_W));
        assert!(item.property_tags.contains(&PID_TAG_SUBJECT_W));
        total_item_bytes += item.item_byte_len;
        previous_end = item.item_end_offset;
    }
    assert!(total_item_bytes > 0);
    assert!(buffer.len() >= total_item_bytes);
}

fn persisted_inbox_associated_configs(
    account_id: Uuid,
) -> Vec<crate::store::MapiAssociatedConfigRecord> {
    [
        (
            0x6d617069_6163_6350_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7900),
            "IPM.Configuration.AccountPrefs",
        ),
        (
            0x6d617069_636f_6e76_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7901),
            "IPM.Configuration.ConversationPrefs",
        ),
        (
            0x6d617069_7273_7352_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7904),
            "IPM.Configuration.RssRule",
        ),
        (
            0x6d617069_7476_5072_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7903),
            "IPM.Configuration.TableViewPreviewPrefs",
        ),
        (
            0x6d617069_7463_5072_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7902),
            "IPM.Configuration.TCPrefs",
        ),
        (
            0x6d617069_6578_5275_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7905),
            "IPM.ExtendedRule.Message",
        ),
    ]
    .into_iter()
    .map(|(id, item_id, class)| {
        let id = Uuid::from_u128(id);
        crate::mapi::identity::remember_mapi_identity(id, item_id);
        crate::store::MapiAssociatedConfigRecord {
            id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: class.to_string(),
            subject: class.to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "392d30"}
            }),
        }
    })
    .collect()
}

fn persisted_common_views_shortcuts(
    account_id: Uuid,
) -> Vec<crate::store::MapiNavigationShortcutRecord> {
    [
        (
            0x6d617069_776c_496e_8000_000000000120,
            crate::mapi::identity::mapi_store_id(0x7800),
            "Inbox",
            INBOX_FOLDER_ID,
            127,
        ),
        (
            0x6d617069_776c_5365_8000_000000000120,
            crate::mapi::identity::mapi_store_id(0x7801),
            "Sent",
            SENT_FOLDER_ID,
            128,
        ),
        (
            0x6d617069_776c_5472_8000_000000000120,
            crate::mapi::identity::mapi_store_id(0x7802),
            "Trash",
            TRASH_FOLDER_ID,
            129,
        ),
        (
            0x6d617069_776c_4361_8000_000000000120,
            crate::mapi::identity::mapi_store_id(0x7803),
            "Calendar",
            CALENDAR_FOLDER_ID,
            130,
        ),
    ]
    .into_iter()
    .map(|(id, item_id, subject, target_folder_id, ordinal)| {
        let id = Uuid::from_u128(id);
        crate::mapi::identity::remember_mapi_identity(id, item_id);
        crate::store::MapiNavigationShortcutRecord {
            id,
            account_id,
            subject: subject.to_string(),
            target_folder_id: Some(target_folder_id),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        }
    })
    .collect()
}

#[test]
fn common_views_fai_fasttransfer_boundaries_cover_shortcuts_and_named_views() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = MapiMailStoreSnapshot::empty()
        .with_navigation_shortcuts(persisted_common_views_shortcuts(account_id));
    let objects = special_sync_objects_for(
        COMMON_VIEWS_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );
    let buffer = associated_content_sync_buffer(account_id, COMMON_VIEWS_FOLDER_ID, &objects);

    let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

    assert_fai_boundary_summary(&buffer, &summary, 6);
    assert_eq!(
        summary
            .fai_items
            .iter()
            .filter(|item| item.message_class == "IPM.Microsoft.WunderBar.Link")
            .count(),
        4
    );
    assert_eq!(
        summary
            .fai_items
            .iter()
            .filter(|item| item.message_class == "IPM.Microsoft.FolderDesign.NamedView")
            .count(),
        2
    );
    assert!(summary.fai_items.iter().any(|item| {
        item.item_id == Some(crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID)
            && item.subject == "Compact"
    }));
    let summary_property_count = summary
        .fai_items
        .iter()
        .map(|item| item.property_tags.len())
        .sum::<usize>();
    assert!(summary_property_count >= summary.fai_items.len());
    for item in &summary.fai_items {
        let item_id = item.item_id.unwrap();
        let special_object = objects.iter().find(|object| object.item_id == item_id);
        let origin =
            mapi_mailstore::fai_debug_state_origin(COMMON_VIEWS_FOLDER_ID, special_object, item_id);
        if item.message_class == "IPM.Microsoft.FolderDesign.NamedView" {
            assert_eq!(origin, "mapi_synthetic_default");
        } else {
            assert_eq!(origin, "sql_associated");
        }
    }
}

#[test]
fn inbox_fai_fasttransfer_boundaries_export_folder_local_default_view() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = MapiMailStoreSnapshot::empty()
        .with_associated_configs(persisted_inbox_associated_configs(account_id));
    let objects = special_sync_objects_for(
        INBOX_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );
    let buffer = associated_content_sync_buffer(account_id, INBOX_FOLDER_ID, &objects);

    let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

    assert_fai_boundary_summary(&buffer, &summary, 7);
    let summary_property_count = summary
        .fai_items
        .iter()
        .map(|item| item.property_tags.len())
        .sum::<usize>();
    assert!(summary_property_count >= summary.fai_items.len());
    for item in &summary.fai_items {
        let item_id = item.item_id.unwrap();
        let special_object = objects.iter().find(|object| object.item_id == item_id);
        let origin =
            mapi_mailstore::fai_debug_state_origin(INBOX_FOLDER_ID, special_object, item_id);
        if item.message_class == "IPM.Microsoft.FolderDesign.NamedView" {
            assert_eq!(origin, "mapi_synthetic_default");
        } else {
            assert_eq!(origin, "sql_associated");
        }
    }
    assert!(summary.fai_items.iter().any(|item| {
        item.item_id
            == Some(crate::mapi_store::outlook_default_folder_named_view_id(
                INBOX_FOLDER_ID,
            ))
            && item.message_class == "IPM.Microsoft.FolderDesign.NamedView"
            && item.subject == "Compact"
    }));
    assert!(!summary
        .fai_items
        .iter()
        .any(|item| item.item_id
            == Some(crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID)));
}

#[test]
fn calendar_fai_content_sync_preserves_imported_ics_identity_properties() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let canonical_id = Uuid::parse_str("4ff7d398-6c12-4a91-aa0f-6efb4fdba738").unwrap();
    let item_id = crate::mapi::identity::mapi_store_id(0x35);
    crate::mapi::identity::remember_mapi_identity(canonical_id, item_id);
    let source_key = [
        crate::mapi::identity::STORE_REPLICA_GUID.as_slice(),
        &[0xa0, 0x0a, 0x52, 0x07, 0x36, 0x10],
    ]
    .concat();
    let change_key = [
        0x8e, 0x42, 0x51, 0x20, 0xb7, 0xa4, 0x26, 0x4a, 0x87, 0x53, 0x6c, 0x05, 0x1f, 0x77, 0xbc,
        0x30, 0x00, 0x00, 0x04, 0x1e,
    ];
    let predecessor_change_list =
        [std::slice::from_ref(&(change_key.len() as u8)), &change_key].concat();
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: canonical_id,
            account_id,
            folder_id: CALENDAR_FOLDER_ID,
            message_class: "IPM.Configuration.Calendar".to_string(),
            subject: "IPM.Configuration.Calendar".to_string(),
            properties_json: serde_json::json!({
                "0x65e00102": {
                    "type": "binary",
                    "value": "741f6fd38e1a654f9d422dfb451c8f10a00a52073610"
                },
                "0x65e20102": {
                    "type": "binary",
                    "value": "8e425120b7a4264a87536c051f77bc300000041e"
                },
                "0x65e30102": {
                    "type": "binary",
                    "value": "148e425120b7a4264a87536c051f77bc300000041e"
                },
                "0x7c060003": {"type": "i32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "3c2f3e"}
            }),
        },
    ]);
    let objects = special_sync_objects_for(
        CALENDAR_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );
    let buffer = associated_content_sync_buffer(account_id, CALENDAR_FOLDER_ID, &objects);
    let copy_buffer = mapi_mailstore::fast_transfer_manifest_buffer_with_special_objects(
        CALENDAR_FOLDER_ID,
        &objects,
    );

    for transfer in [&buffer, &copy_buffer] {
        for (tag, value) in [
            (PID_TAG_SOURCE_KEY, source_key.as_slice()),
            (PID_TAG_CHANGE_KEY, change_key.as_slice()),
            (
                PID_TAG_PREDECESSOR_CHANGE_LIST,
                predecessor_change_list.as_slice(),
            ),
        ] {
            let mut encoded = tag.to_le_bytes().to_vec();
            encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
            encoded.extend_from_slice(value);
            assert!(
                transfer
                    .windows(encoded.len())
                    .any(|window| window == encoded),
                "missing imported property 0x{tag:08x}"
            );
        }
    }
}

#[test]
fn import_rop_success_responses_return_zero_object_ids() {
    let import_change = RopRequest {
        rop_id: 0x72,
        input_handle_index: Some(1),
        output_handle_index: Some(3),
        payload: Vec::new(),
    };
    let import_hierarchy = RopRequest {
        rop_id: 0x73,
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let import_move = RopRequest {
        rop_id: 0x78,
        input_handle_index: Some(4),
        output_handle_index: None,
        payload: Vec::new(),
    };

    assert_eq!(
        rop_synchronization_import_message_change_response(&import_change),
        vec![0x72, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        rop_synchronization_import_hierarchy_change_response(&import_hierarchy),
        vec![0x73, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        rop_synchronization_import_message_move_response(&import_move),
        vec![0x78, 0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
}

#[test]
fn hierarchy_sync_mailboxes_deduplicate_fixed_special_folder_ids() {
    let duplicate_folder_id = crate::mapi::identity::mapi_store_id(100);
    let first_id = Uuid::from_u128(0x11111111111111111111111111111111);
    let second_id = Uuid::from_u128(0x22222222222222222222222222222222);
    crate::mapi::identity::remember_mapi_identity(first_id, duplicate_folder_id);
    crate::mapi::identity::remember_mapi_identity(second_id, duplicate_folder_id);
    let mailboxes = vec![
        mailbox(first_id.as_u128(), "custom", "Duplicate"),
        mailbox(second_id.as_u128(), "custom", "Duplicate"),
    ];

    let rows = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes);
    let duplicate_rows = rows
        .iter()
        .filter(|mailbox| mailbox.id == first_id || mailbox.id == second_id)
        .count();

    assert_eq!(duplicate_rows, 1);
}

#[test]
fn hierarchy_sync_mailboxes_include_custom_sync_root() {
    let root_id = Uuid::from_u128(0x11111111111111111111111111111112);
    let child_id = Uuid::from_u128(0x22222222222222222222222222222223);
    let root_folder_id = crate::mapi::identity::mapi_store_id(101);
    let child_folder_id = crate::mapi::identity::mapi_store_id(102);
    crate::mapi::identity::remember_mapi_identity(root_id, root_folder_id);
    crate::mapi::identity::remember_mapi_identity(child_id, child_folder_id);
    let root = mailbox(root_id.as_u128(), "custom", "Project");
    let mut child = mailbox(child_id.as_u128(), "custom", "Archive");
    child.parent_id = Some(root_id);
    let rows = sync_mailboxes_for(root_folder_id, 0x02, &[child, root]);
    let row_ids = rows.iter().map(mapi_folder_id).collect::<Vec<_>>();

    assert!(row_ids.contains(&root_folder_id));
    assert!(row_ids.contains(&child_folder_id));
}

#[test]
fn calendar_sync_object_projects_canonical_attachment_presence() {
    let event_id = Uuid::from_u128(0x71717171717141719171717171717171);
    let event = crate::mapi_store::MapiEvent {
        id: crate::mapi::identity::mapi_store_id(123),
        source_key: vec![0x53, 0x43, 0x4f, 0x50, 0x45, 0x44],
        folder_id: CALENDAR_FOLDER_ID,
        canonical_id: event_id,
        event: lpe_storage::AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: Uuid::nil(),
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            rights: lpe_storage::CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            date: "2026-05-25".to_string(),
            time: "14:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Attachment sync".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        },
        version: lpe_storage::MapiEventVersion {
            event_id,
            canonical_modseq: 7,
            change_number: 124,
            change_key: mapi_mailstore::change_key_for_change_number(124),
            predecessor_change_list: mapi_mailstore::predecessor_change_list(124),
            updated_at: "2026-05-25T14:00:00Z".to_string(),
        },
        attachments: vec![crate::mapi_store::MapiAttachment {
            canonical_id: Uuid::from_u128(0x81818181818141819181818181818181),
            attach_num: 0,
            file_reference: "calendar-attachment:ref".to_string(),
            file_name: "agenda.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            disposition: None,
            content_id: None,
            size_octets: 12,
        }],
    };

    let sync = calendar_sync_object(&event, None);

    assert!(sync.named_properties.iter().any(|(tag, value)| {
        *tag == PID_TAG_HAS_ATTACHMENTS
            && matches!(
                value,
                mapi_mailstore::SpecialMessagePropertyValue::Bool(true)
            )
    }));
    assert!(sync.named_properties.iter().any(|(tag, value)| {
        *tag == PID_TAG_SOURCE_KEY
            && matches!(
                value,
                mapi_mailstore::SpecialMessagePropertyValue::Binary(bytes)
                    if bytes == &event.source_key
            )
    }));
    assert!(sync.named_properties.iter().any(|(tag, value)| {
        *tag == PID_TAG_CHANGE_NUMBER
            && matches!(value, mapi_mailstore::SpecialMessagePropertyValue::U64(124))
    }));
    assert!(sync.named_properties.iter().any(|(tag, value)| {
        *tag == PID_TAG_CHANGE_KEY
            && matches!(
                value,
                mapi_mailstore::SpecialMessagePropertyValue::Binary(bytes)
                    if bytes == &mapi_mailstore::change_key_for_change_number(124)
            )
    }));
    assert!(sync.named_properties.iter().any(|(tag, value)| {
        *tag == PID_TAG_PREDECESSOR_CHANGE_LIST
            && matches!(
                value,
                mapi_mailstore::SpecialMessagePropertyValue::Binary(bytes)
                    if bytes == &mapi_mailstore::predecessor_change_list(124)
            )
    }));
    assert!(sync.named_properties.iter().any(|(tag, value)| {
        *tag == PID_TAG_LOCAL_COMMIT_TIME
            && matches!(
                value,
                mapi_mailstore::SpecialMessagePropertyValue::I64(filetime)
                    if *filetime
                        == mapi_mailstore::filetime_from_rfc3339_utc(
                            "2026-05-25T14:00:00Z"
                        ) as i64
            )
    }));
    assert!(!sync
        .named_properties
        .iter()
        .any(|(tag, _)| *tag == 0x3A0D_001F));
    assert_eq!(
        sync.last_modified_filetime,
        mapi_mailstore::filetime_from_rfc3339_utc("2026-05-25T14:00:00Z")
    );
}

#[test]
fn calendar_special_content_sync_advertises_appointment_objects() {
    let account_id = Uuid::from_u128(0xbc737006441349b9aefc3cb6e0088492);
    let event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
    crate::mapi::identity::remember_mapi_identity(
        event_id,
        crate::mapi::identity::mapi_store_id(0x43),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![lpe_storage::AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account_id,
            owner_email: "test@l-p-e.ch".to_string(),
            owner_display_name: "test".to_string(),
            rights: lpe_storage::CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: false,
            },
            date: "2026-06-01".to_string(),
            time: "10:00".to_string(),
            time_zone: String::new(),
            duration_minutes: 0,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Test".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "[]".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }],
        Vec::new(),
        Vec::new(),
    );

    let objects = special_sync_objects_for(
        CALENDAR_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].message_class, "IPM.Appointment");
    assert_eq!(objects[0].subject, "Test");
}

#[test]
fn collaboration_default_views_are_not_synthetic_fai_sync_objects() {
    let account_id = Uuid::from_u128(0xbc737006441349b9aefc3cb6e0088492);
    let snapshot = MapiMailStoreSnapshot::empty();

    for folder_id in [
        CALENDAR_FOLDER_ID,
        TASKS_FOLDER_ID,
        NOTES_FOLDER_ID,
        JOURNAL_FOLDER_ID,
    ] {
        let objects =
            special_sync_objects_for(folder_id, 0x01, &snapshot, &sync_principal(account_id));

        assert!(
            objects
                .iter()
                .all(|object| object.message_class != "IPM.Microsoft.FolderDesign.NamedView"),
            "folder 0x{folder_id:016x} should not synthesize default view FAI sync objects"
        );
    }
}

#[test]
fn hierarchy_sync_mailboxes_deduplicate_outlook_special_roles() {
    let roles = [
        ("suggested_contacts", SUGGESTED_CONTACTS_FOLDER_ID),
        ("sync_issues", SYNC_ISSUES_FOLDER_ID),
        ("conflicts", CONFLICTS_FOLDER_ID),
        ("local_failures", LOCAL_FAILURES_FOLDER_ID),
        ("server_failures", SERVER_FAILURES_FOLDER_ID),
        ("junk", JUNK_FOLDER_ID),
        ("rss_feeds", RSS_FEEDS_FOLDER_ID),
        ("archive", ARCHIVE_FOLDER_ID),
    ];
    let mailboxes = roles
        .iter()
        .enumerate()
        .map(|(index, (role, _))| {
            mailbox(
                0x33333333333333333333333333333330 + index as u128,
                role,
                role,
            )
        })
        .collect::<Vec<_>>();

    let rows = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes);

    for (role, folder_id) in roles {
        assert_eq!(
            rows.iter()
                .filter(|mailbox| mailbox.role == role && mapi_folder_id(mailbox) == folder_id)
                .count(),
            1,
            "{role} should appear once"
        );
    }
    assert_eq!(
            rows.iter()
                .filter(|mailbox| mailbox.role == "conversation_history")
                .count(),
            0,
            "persisted Conversation History is Outlook-internal and must stay out of startup hierarchy sync"
        );
}

#[test]
fn hierarchy_scope_places_reminders_under_root_not_ipm_subtree() {
    let mailboxes = vec![mailbox(
        0x44444444444444444444444444444444,
        "reminders",
        "Reminders",
    )];

    assert!(hierarchy_virtual_folder_ids(ROOT_FOLDER_ID).contains(&REMINDERS_FOLDER_ID));
    assert!(!hierarchy_virtual_folder_ids(IPM_SUBTREE_FOLDER_ID).contains(&REMINDERS_FOLDER_ID));
    assert_eq!(
        sync_mailboxes_for(ROOT_FOLDER_ID, 0x02, &mailboxes)
            .iter()
            .filter(|mailbox| mapi_folder_id(mailbox) == REMINDERS_FOLDER_ID)
            .count(),
        1
    );
}

#[test]
fn hierarchy_scope_places_contacts_search_under_search_not_ipm_subtree() {
    let mailboxes = vec![mailbox(
        0x45454545454545454545454545454545,
        "contacts_search",
        "Contacts Search",
    )];

    assert!(hierarchy_virtual_folder_ids(ROOT_FOLDER_ID).contains(&CONTACTS_SEARCH_FOLDER_ID));
    assert!(!hierarchy_virtual_folder_ids(SEARCH_FOLDER_ID).contains(&SEARCH_FOLDER_ID));
    assert!(
        !hierarchy_virtual_folder_ids(IPM_SUBTREE_FOLDER_ID).contains(&CONTACTS_SEARCH_FOLDER_ID)
    );
    assert_eq!(
        sync_mailboxes_for(SEARCH_FOLDER_ID, 0x02, &[])
            .iter()
            .filter(|mailbox| mapi_folder_id(mailbox) == SEARCH_FOLDER_ID)
            .count(),
        0
    );
    assert_eq!(
        sync_mailboxes_for(SEARCH_FOLDER_ID, 0x02, &mailboxes)
            .iter()
            .filter(|mailbox| mapi_folder_id(mailbox) == CONTACTS_SEARCH_FOLDER_ID)
            .count(),
        1
    );
}

#[test]
fn ipm_hierarchy_runtime_uses_outlook_safe_folder_projection() {
    std::env::set_var("LPE_MAPI_EXPERIMENT_MINIMAL_IPM_HIERARCHY", "1");
    std::env::set_var(
        "LPE_MAPI_EXPERIMENT_IPM_HIERARCHY_GROUPS",
        "minimal sync-issues",
    );
    let folder_ids = hierarchy_virtual_folder_ids(IPM_SUBTREE_FOLDER_ID);
    std::env::remove_var("LPE_MAPI_EXPERIMENT_MINIMAL_IPM_HIERARCHY");
    std::env::remove_var("LPE_MAPI_EXPERIMENT_IPM_HIERARCHY_GROUPS");

    assert_eq!(folder_ids.as_slice(), IPM_SUBTREE_VIRTUAL_FOLDER_IDS);
}

#[test]
fn ipm_hierarchy_state_matches_emitted_folder_projection() {
    let rows = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &[]);
    let state_rows = sync_state_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &[]);
    let row_ids = rows.iter().map(mapi_folder_id).collect::<Vec<_>>();
    let state_ids = state_rows.iter().map(mapi_folder_id).collect::<Vec<_>>();

    assert_eq!(row_ids.as_slice(), IPM_SUBTREE_VIRTUAL_FOLDER_IDS);
    assert_eq!(state_ids, row_ids);
}

#[test]
fn common_views_shortcut_sync_uses_account_bound_entry_ids() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000002);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 101,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id,
            subject: "Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 0,
            ordinal: 0x81,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
    ]);

    let objects = special_sync_objects_for(
        COMMON_VIEWS_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );
    let inbox_shortcut = objects
        .iter()
        .find(|object| object.subject == "Inbox")
        .expect("persisted Inbox navigation shortcut");

    let property = |tag| {
        inbox_shortcut
            .named_properties
            .iter()
            .find_map(|(property_tag, value)| (*property_tag == tag).then_some(value))
            .expect("shortcut property")
    };
    assert_eq!(
        property(PID_TAG_WLINK_ENTRY_ID),
        &crate::mapi_mailstore::SpecialMessagePropertyValue::Binary(
            crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID,)
                .unwrap()
        )
    );
    assert_eq!(
        property(PID_TAG_WLINK_STORE_ENTRY_ID),
        &crate::mapi_mailstore::SpecialMessagePropertyValue::Binary(
            crate::mapi::identity::principal_mailbox_store_entry_id(&sync_principal(account_id))
        )
    );
    assert_eq!(
        property(PID_TAG_WLINK_FOLDER_TYPE),
        &crate::mapi_mailstore::SpecialMessagePropertyValue::Guid([
            0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ])
    );
}

#[test]
fn common_views_shortcut_sync_does_not_emit_materialized_mail_header() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000012);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 112,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id,
            subject: "Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0x81,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
    ]);

    let objects = special_sync_objects_for(
        COMMON_VIEWS_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );

    assert_eq!(
        objects
            .iter()
            .filter(|object| object.message_class == "IPM.Microsoft.WunderBar.Link")
            .count(),
        1
    );
    let default_mail_header_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE7);
    assert!(objects
        .iter()
        .all(|object| object.item_id != default_mail_header_id));
}

#[test]
fn common_views_group_header_sync_includes_group_identity_without_target() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_4361_8000_000000000101);
    let group_id = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 113,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id,
            subject: "My Calendars".to_string(),
            target_folder_id: None,
            shortcut_type: 4,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0x80,
            group_header_id: Some(group_id),
            group_name: "My Calendars".to_string(),
        },
    ]);

    let objects = special_sync_objects_for(
        COMMON_VIEWS_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );
    let group_header = objects
        .iter()
        .find(|object| object.subject == "My Calendars")
        .expect("persisted My Calendars group header");

    let property = |tag| {
        group_header
            .named_properties
            .iter()
            .find_map(|(property_tag, value)| (*property_tag == tag).then_some(value))
    };
    assert_eq!(
        property(PID_TAG_WLINK_TYPE),
        Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::U32(4))
    );
    assert_eq!(
        property(PID_TAG_WLINK_GROUP_HEADER_ID),
        Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::Guid(
            *group_id.as_bytes()
        ))
    );
    assert_eq!(
        property(PID_TAG_WLINK_GROUP_CLSID),
        Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::Guid(
            *group_id.as_bytes()
        ))
    );
    assert_eq!(
        property(PID_TAG_WLINK_GROUP_NAME_W),
        Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::String(
            "My Calendars".to_string()
        ))
    );
    assert_eq!(property(PID_TAG_WLINK_ENTRY_ID), None);
    assert_eq!(property(PID_TAG_WLINK_RECORD_KEY), None);
    assert_eq!(property(PID_TAG_WLINK_STORE_ENTRY_ID), None);
}

#[test]
fn inbox_associated_content_sync_payload_emits_required_fai_properties() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let umolk_id = Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000201);
    let named_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000201);
    crate::mapi::identity::remember_mapi_identity(
        umolk_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 201,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        named_view_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 202,
        ),
    );
    let persisted = [
        (
            umolk_id.as_u128(),
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 201,
            ),
            "IPM.Configuration.UMOLK.UserOptions",
            "IPM.Configuration.UMOLK.UserOptions",
        ),
        (
            named_view_id.as_u128(),
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 202,
            ),
            "IPM.Microsoft.FolderDesign.NamedView",
            "Compact",
        ),
        (
            0x6d617069_6163_6350_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7900),
            "IPM.Configuration.AccountPrefs",
            "IPM.Configuration.AccountPrefs",
        ),
        (
            0x6d617069_636f_6e76_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7901),
            "IPM.Configuration.ConversationPrefs",
            "IPM.Configuration.ConversationPrefs",
        ),
        (
            0x6d617069_7463_5072_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7902),
            "IPM.Configuration.TCPrefs",
            "IPM.Configuration.TCPrefs",
        ),
        (
            0x6d617069_7476_5072_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7903),
            "IPM.Configuration.TableViewPreviewPrefs",
            "IPM.Configuration.TableViewPreviewPrefs",
        ),
        (
            0x6d617069_7273_7352_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7904),
            "IPM.Configuration.RssRule",
            "IPM.Configuration.RssRule",
        ),
        (
            0x6d617069_6578_5275_8000_000000000101,
            crate::mapi::identity::mapi_store_id(0x7905),
            "IPM.ExtendedRule.Message",
            "IPM.ExtendedRule.Message",
        ),
    ]
    .into_iter()
    .map(|(id, item_id, class, subject)| {
        let id = Uuid::from_u128(id);
        crate::mapi::identity::remember_mapi_identity(id, item_id);
        crate::store::MapiAssociatedConfigRecord {
            id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: class.to_string(),
            subject: subject.to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "392d30"}
            }),
        }
    })
    .collect::<Vec<_>>();
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(persisted);
    let objects = special_sync_objects_for(
        INBOX_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );
    let buffer = mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
        account_id,
        0x01,
        0x0010,
        0x0000_0001 | 0x0000_0004 | 0x0000_0008,
        &[],
        INBOX_FOLDER_ID,
        &[],
        &[],
        &[],
        &objects,
        &[],
        &[],
        &[],
        &[],
        &[],
        &objects,
        &[],
        &[],
        1,
    );
    let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

    assert!(summary.fai_items.len() >= 8);
    for item in &summary.fai_items {
        assert_associated_fai_core_payload(item);
    }
    let expected = [
        (
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 202,
            ),
            "IPM.Microsoft.FolderDesign.NamedView",
            "Compact",
        ),
        (
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 201,
            ),
            "IPM.Configuration.UMOLK.UserOptions",
            "IPM.Configuration.UMOLK.UserOptions",
        ),
    ];
    for (item_id, message_class, subject) in expected {
        let item = summary
            .fai_items
            .iter()
            .find(|item| item.item_id == Some(item_id))
            .expect("expected Inbox FAI item");
        assert_eq!(item.message_class, message_class);
        assert_eq!(item.subject, subject);
    }
    assert!(!summary.fai_items.iter().any(|item| {
        item.item_id == Some(crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF6))
            || item.item_id == Some(crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFA))
    }));
    let named_view = summary
        .fai_items
        .iter()
        .find(|item| item.message_class == "IPM.Microsoft.FolderDesign.NamedView")
        .expect("persisted Inbox named view");
    assert_has_tags(
        named_view,
        &[
            PID_TAG_VIEW_DESCRIPTOR_NAME_W,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
            PID_TAG_VIEW_DESCRIPTOR_BINARY,
            OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
            OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
        ],
    );
    let umolk = summary
        .fai_items
        .iter()
        .find(|item| item.message_class == "IPM.Configuration.UMOLK.UserOptions")
        .expect("persisted UMOLK user options");
    assert_has_tags(
        umolk,
        &[
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_ROAMING_DICTIONARY,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
        ],
    );
    for class in [
        "IPM.Configuration.AccountPrefs",
        "IPM.Configuration.ConversationPrefs",
        "IPM.Configuration.TCPrefs",
        "IPM.Configuration.TableViewPreviewPrefs",
        "IPM.Configuration.RssRule",
        "IPM.ExtendedRule.Message",
    ] {
        assert!(
            summary
                .fai_items
                .iter()
                .any(|item| item.message_class == class),
            "missing persisted class {class}"
        );
    }
}

#[test]
fn common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000120);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 120,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id,
            subject: "Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
    ]);
    let mut objects = special_sync_objects_for(
        COMMON_VIEWS_FOLDER_ID,
        0x01,
        &snapshot,
        &sync_principal(account_id),
    );
    objects.push(common_view_named_view_sync_object(
        &crate::mapi_store::MapiCommonViewNamedViewMessage {
            id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_6376_4e76_8000_000000000001),
            name: "Compact".to_string(),
            view_flags: 14_745_605,
            view_type: 8,
        },
        account_id,
    ));
    let buffer = mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
        account_id,
        0x01,
        0x0010,
        0x0000_0001 | 0x0000_0004 | 0x0000_0008,
        &[],
        COMMON_VIEWS_FOLDER_ID,
        &[],
        &[],
        &[],
        &objects,
        &[],
        &[],
        &[],
        &[],
        &[],
        &objects,
        &[],
        &[],
        1,
    );
    let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

    assert!(!summary.fai_items.is_empty());
    for item in &summary.fai_items {
        assert_associated_fai_core_payload(item);
    }
    let named_view = summary
        .fai_items
        .iter()
        .find(|item| {
            item.message_class == "IPM.Microsoft.FolderDesign.NamedView"
                && item.subject == "Compact"
        })
        .expect("Compact named view");
    assert_has_tags(
        named_view,
        &[
            PID_TAG_VIEW_DESCRIPTOR_NAME_W,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
            PID_TAG_VIEW_DESCRIPTOR_BINARY,
            PID_TAG_WLINK_GROUP_HEADER_ID,
        ],
    );
    let shortcut = summary
        .fai_items
        .iter()
        .find(|item| {
            item.message_class == "IPM.Microsoft.WunderBar.Link" && item.subject == "Inbox"
        })
        .expect("Inbox WunderBar shortcut");
    assert_has_tags(
        shortcut,
        &[
            PID_TAG_WLINK_TYPE,
            PID_TAG_WLINK_FLAGS,
            PID_TAG_WLINK_SAVE_STAMP,
            PID_TAG_WLINK_ENTRY_ID,
            PID_TAG_WLINK_RECORD_KEY,
            PID_TAG_WLINK_STORE_ENTRY_ID,
            PID_TAG_WLINK_GROUP_CLSID,
            PID_TAG_WLINK_SECTION,
            PID_TAG_WLINK_ORDINAL,
        ],
    );
}

#[test]
fn fast_transfer_manifest_rejects_unbacked_common_views_shortcut() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
    let object = MapiObject::NavigationShortcut {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        shortcut_id,
    };

    assert!(fast_transfer_manifest_for_object(
        RopId::FastTransferSourceCopyTo.as_u8(),
        &object,
        &sync_principal(account_id),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    )
    .is_none());
}

#[test]
fn fast_transfer_manifest_exports_default_common_views_named_view() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let view_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
    let object = MapiObject::CommonViewNamedView {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        view_id,
    };

    let manifest = fast_transfer_manifest_for_object(
        RopId::FastTransferSourceCopyTo.as_u8(),
        &object,
        &sync_principal(account_id),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    )
    .expect("default common views named view manifest");

    assert_eq!(manifest.0, COMMON_VIEWS_FOLDER_ID);
    let utf16z = |value: &str| {
        let mut bytes = value
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        bytes.extend_from_slice(&[0, 0]);
        bytes
    };
    let named_view_class = utf16z("IPM.Microsoft.FolderDesign.NamedView");
    let compact_name = utf16z("Compact");
    assert!(manifest
        .1
        .windows(named_view_class.len())
        .any(|window| window == named_view_class.as_slice()));
    assert!(manifest
        .1
        .windows(compact_name.len())
        .any(|window| window == compact_name.as_slice()));
}

#[test]
fn common_view_named_view_sync_projects_canonical_descriptor_properties() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let message = crate::mapi_store::MapiCommonViewNamedViewMessage {
        id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x6d617069_6376_4e76_8000_000000000001),
        name: "Compact".to_string(),
        view_flags: 14_745_605,
        view_type: 8,
    };

    let sync = common_view_named_view_sync_object(&message, account_id);
    let property = |tag| {
        sync.named_properties
            .iter()
            .find_map(|(property_tag, value)| (*property_tag == tag).then_some(value))
            .expect("sync property")
    };

    assert_eq!(
        property(PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL),
        &mapi_mailstore::SpecialMessagePropertyValue::U32(8)
    );
    assert_eq!(
        property(PID_TAG_VIEW_DESCRIPTOR_CLSID),
        &mapi_mailstore::SpecialMessagePropertyValue::Guid([
            0x00, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ])
    );
    let expected_descriptor = view_descriptor_binary(&outlook_mail_view_definition("Compact"));
    assert_eq!(
        property(PID_TAG_VIEW_DESCRIPTOR_BINARY),
        &mapi_mailstore::SpecialMessagePropertyValue::Binary(expected_descriptor.clone())
    );
    assert_eq!(
        property(OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835),
        &mapi_mailstore::SpecialMessagePropertyValue::Binary(expected_descriptor)
    );
    assert_eq!(
        property(PID_TAG_VIEW_DESCRIPTOR_STRINGS_W),
        &mapi_mailstore::SpecialMessagePropertyValue::String(
            "\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nFrom\nSubject\nReceived\nSize\nCategories\n"
                .to_string()
        )
    );
    assert!(matches!(
        property(OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C),
        mapi_mailstore::SpecialMessagePropertyValue::Binary(value)
            if value.starts_with(&[0x0a, 0x00])
                && value.ends_with(&[0x0a, 0x00])
                && value.chunks_exact(2).filter(|unit| *unit == [0x0a, 0x00]).count() == 11
    ));
}

#[test]
fn fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let object = MapiObject::AssociatedConfig {
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        config_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC),
        saved_message: None,
    };

    let manifest = fast_transfer_manifest_for_object(
        RopId::FastTransferSourceCopyTo.as_u8(),
        &object,
        &sync_principal(account_id),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(manifest.is_none());
}

#[test]
fn fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let object = MapiObject::NavigationShortcut {
        folder_id: INBOX_FOLDER_ID,
        shortcut_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9),
    };

    let manifest = fast_transfer_manifest_for_object(
        RopId::FastTransferSourceCopyTo.as_u8(),
        &object,
        &sync_principal(account_id),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(manifest.is_none());
}

#[test]
fn fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let object = MapiObject::CommonViewNamedView {
        folder_id: INBOX_FOLDER_ID,
        view_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7),
    };

    let manifest = fast_transfer_manifest_for_object(
        RopId::FastTransferSourceCopyTo.as_u8(),
        &object,
        &sync_principal(account_id),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(manifest.is_none());
}

#[test]
fn fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let object = MapiObject::ConversationAction {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        conversation_action_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2),
    };

    let manifest = fast_transfer_manifest_for_object(
        RopId::FastTransferSourceCopyTo.as_u8(),
        &object,
        &sync_principal(account_id),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(manifest.is_none());
}

#[test]
fn fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let message_id = Uuid::parse_str("56565656-5656-4656-8656-565656565656").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        message_id,
        crate::mapi::identity::mapi_store_id(610),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_delegate_freebusy_messages(vec![
        lpe_storage::DelegateFreeBusyMessageObject {
            id: message_id,
            account_id,
            owner_account_id: Uuid::nil(),
            owner_email: "owner@example.test".to_string(),
            message_kind: "freebusy".to_string(),
            subject: "owner@example.test: busy".to_string(),
            body_text: "busy".to_string(),
            starts_at: None,
            ends_at: None,
            busy_status: None,
            payload_json: "{}".to_string(),
            updated_at: "2026-05-26T08:00:00Z".to_string(),
        },
    ]);
    let object = MapiObject::DelegateFreeBusyMessage {
        folder_id: INBOX_FOLDER_ID,
        message_id: snapshot.delegate_freebusy_messages()[0].id,
    };

    let manifest = fast_transfer_manifest_for_object(
        RopId::FastTransferSourceCopyTo.as_u8(),
        &object,
        &sync_principal(account_id),
        &[],
        &[],
        &snapshot,
    );

    assert!(manifest.is_none());
}
