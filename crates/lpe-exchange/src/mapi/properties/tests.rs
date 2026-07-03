use super::*;
use crate::mapi::wire::MapiRestrictionType;
use crate::mapi_store::{MapiCollaborationFolder, MapiCollaborationFolderKind, MapiPublicFolder};
use lpe_storage::{
    CollaborationCollection, CollaborationRights, ContactSourceFields, PublicFolder,
    PublicFolderRights,
};

fn mailbox(id: &str, parent_id: Option<Uuid>, role: &str, name: &str) -> JmapMailbox {
    JmapMailbox {
        id: Uuid::parse_str(id).unwrap(),
        parent_id,
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

fn utf16z(value: &str) -> Vec<u8> {
    value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .chain([0, 0])
        .collect()
}

fn valid_swapped_todo_data() -> Vec<u8> {
    let mut value = vec![0; SWAPPED_TODO_DATA_LEN];
    value[0..4].copy_from_slice(&SWAPPED_TODO_DATA_VERSION.to_le_bytes());
    let flags = SWAPPED_TODO_FLAG_TODO_ITEM
        | SWAPPED_TODO_FLAG_FLAG_TO
        | SWAPPED_TODO_FLAG_START_DATE
        | SWAPPED_TODO_FLAG_DUE_DATE
        | SWAPPED_TODO_FLAG_REMINDER
        | SWAPPED_TODO_FLAG_REMINDER_SET;
    value[4..8].copy_from_slice(&flags.to_le_bytes());
    value[8..12].copy_from_slice(&8u32.to_le_bytes());
    for (index, unit) in "Follow up".encode_utf16().enumerate() {
        let offset = 12 + index * 2;
        value[offset..offset + 2].copy_from_slice(&unit.to_le_bytes());
    }
    value[524..528].copy_from_slice(&1_000_000u32.to_le_bytes());
    value[528..532].copy_from_slice(&1_001_440u32.to_le_bytes());
    value[532..536].copy_from_slice(&1_000_030u32.to_le_bytes());
    value[536..540].copy_from_slice(&1u32.to_le_bytes());
    value
}

#[test]
fn pending_html_only_message_derives_plain_body_for_save_and_submit() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "sender@example.test".to_string(),
        display_name: "Sender".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mailbox = mailbox(
        "11111111-1111-4111-8111-111111111111",
        None,
        "drafts",
        "Drafts",
    );
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("HTML draft".to_string()),
    );
    properties.insert(
        PID_TAG_HTML_BINARY,
        MapiValue::Binary(b"<html><body>Hello<br>World &amp; team</body></html>".to_vec()),
    );
    let recipients = vec![PendingRecipient {
        row_id: 1,
        address: "to@example.test".to_string(),
        display_name: Some("To".to_string()),
        recipient_type: 0x01,
    }];

    let imported = jmap_import_from_pending_message(
        &principal,
        &mailbox,
        &properties,
        &recipients,
        Vec::new(),
    );
    assert_eq!(imported.body_text, "Hello\nWorld & team");
    assert_eq!(
        imported.body_html_sanitized.as_deref(),
        Some("<html><body>Hello<br>World &amp; team</body></html>")
    );
    assert_eq!(imported.size_octets, "HTML draft".len() as i64 + 18);

    let submitted = mapi_submit_from_pending_message(&principal, &properties, &recipients);
    assert_eq!(submitted.body_text, "Hello\nWorld & team");
    assert_eq!(
        submitted.body_html_sanitized.as_deref(),
        Some("<html><body>Hello<br>World &amp; team</body></html>")
    );
    assert_eq!(submitted.size_octets, "HTML draft".len() as i64 + 18);
}

#[test]
fn microsoft_inline_image_html_body_preserves_cid_for_save_and_submit() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "sender@example.test".to_string(),
        display_name: "Sender".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mailbox = mailbox(
        "11111111-1111-4111-8111-111111111111",
        None,
        "drafts",
        "Drafts",
    );
    let html = r#"<html><body><p>This is a sample body text</p><p><img width=174 height=152 id="Picture_x0020_2" src="cid:image001.png@01C86E1C.F1954390" alt="cid:image001.png@01C86E1C.F1954390" /></p></body></html>"#;
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("HTML inline image draft".to_string()),
    );
    properties.insert(PID_TAG_BODY_HTML_W, MapiValue::String(html.to_string()));
    let recipients = vec![PendingRecipient {
        row_id: 1,
        address: "to@example.test".to_string(),
        display_name: Some("To".to_string()),
        recipient_type: 0x01,
    }];

    let imported = jmap_import_from_pending_message(
        &principal,
        &mailbox,
        &properties,
        &recipients,
        Vec::new(),
    );
    assert_eq!(imported.body_html_sanitized.as_deref(), Some(html));
    assert!(imported.body_text.contains("This is a sample body text"));

    let submitted = mapi_submit_from_pending_message(&principal, &properties, &recipients);
    assert_eq!(submitted.body_html_sanitized.as_deref(), Some(html));
    assert!(submitted.body_text.contains("This is a sample body text"));
}

#[test]
fn read_recipients_success_response_includes_row_count() {
    let request = RopRequest {
        rop_id: 0x0F,
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: 0u32.to_le_bytes().to_vec(),
    };
    let object = MapiObject::PendingMessage {
        folder_id: DRAFTS_FOLDER_ID,
        properties: HashMap::new(),
        recipients: vec![
            PendingRecipient {
                row_id: 0,
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
                recipient_type: 0x01,
            },
            PendingRecipient {
                row_id: 1,
                address: "carol@example.test".to_string(),
                display_name: Some("Carol".to_string()),
                recipient_type: 0x02,
            },
        ],
    };

    let response = rop_read_recipients_response(
        &request,
        Some(&object),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x0F, 0x02, 0, 0, 0, 0, 2]);
    assert_eq!(u32::from_le_bytes(response[7..11].try_into().unwrap()), 0);
    assert_eq!(response[11], 0x01);
    assert!(response
        .windows(utf16z("Bob").len())
        .any(|window| window == utf16z("Bob").as_slice()));
    assert!(response
        .windows(utf16z("Carol").len())
        .any(|window| window == utf16z("Carol").as_slice()));
}

#[test]
fn read_recipients_uses_row_id_value_not_vector_index() {
    let request = RopRequest {
        rop_id: 0x0F,
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: 20u32.to_le_bytes().to_vec(),
    };
    let object = MapiObject::PendingMessage {
        folder_id: DRAFTS_FOLDER_ID,
        properties: HashMap::new(),
        recipients: vec![
            PendingRecipient {
                row_id: 10,
                address: "alice@example.test".to_string(),
                display_name: Some("Alice".to_string()),
                recipient_type: 0x01,
            },
            PendingRecipient {
                row_id: 20,
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
                recipient_type: 0x01,
            },
            PendingRecipient {
                row_id: 30,
                address: "carol@example.test".to_string(),
                display_name: Some("Carol".to_string()),
                recipient_type: 0x02,
            },
        ],
    };

    let response = rop_read_recipients_response(
        &request,
        Some(&object),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x0F, 0x02, 0, 0, 0, 0, 2]);
    assert_eq!(u32::from_le_bytes(response[7..11].try_into().unwrap()), 20);
    assert_eq!(response[11], 0x01);
    assert!(response
        .windows(utf16z("Bob").len())
        .any(|window| window == utf16z("Bob").as_slice()));
    assert!(response
        .windows(utf16z("Carol").len())
        .any(|window| window == utf16z("Carol").as_slice()));
    assert!(!response
        .windows(utf16z("Alice").len())
        .any(|window| window == utf16z("Alice").as_slice()));
}

#[test]
fn read_recipients_row_zero_on_empty_message_returns_not_found() {
    let request = RopRequest {
        rop_id: 0x0F,
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: 0u32.to_le_bytes().to_vec(),
    };
    let object = MapiObject::PendingMessage {
        folder_id: DRAFTS_FOLDER_ID,
        properties: HashMap::new(),
        recipients: Vec::new(),
    };

    let response = rop_read_recipients_response(
        &request,
        Some(&object),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(response, vec![0x0F, 0x02, 0x0F, 0x01, 0x04, 0x80]);
}

#[test]
fn associated_fai_identity_properties_do_not_reuse_source_key_for_change_keys() {
    let shortcut_id = crate::mapi::identity::mapi_store_id(91);
    let shortcut = MapiNavigationShortcutMessage {
        id: shortcut_id,
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x9191),
        subject: "Inbox".to_string(),
        target_folder_id: Some(INBOX_FOLDER_ID),
        shortcut_type: 1,
        flags: 0,
        save_stamp: 0,
        section: 0,
        ordinal: 0,
        group_header_id: None,
        group_name: String::new(),
    };
    let source_key = navigation_shortcut_property_value(&shortcut, Uuid::nil(), PID_TAG_SOURCE_KEY);
    let change_key = navigation_shortcut_property_value(&shortcut, Uuid::nil(), PID_TAG_CHANGE_KEY);
    let predecessor =
        navigation_shortcut_property_value(&shortcut, Uuid::nil(), PID_TAG_PREDECESSOR_CHANGE_LIST);

    assert_eq!(
        source_key,
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            shortcut_id
        )))
    );
    assert_eq!(
        change_key,
        Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(shortcut_id)
            )
        ))
    );
    assert_eq!(
        predecessor,
        Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
            mapi_mailstore::change_number_for_store_id(shortcut_id)
        )))
    );
    assert_ne!(source_key, predecessor);

    let action_id = crate::mapi::identity::mapi_store_id(92);
    let action = MapiConversationActionMessage {
        id: action_id,
        folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x9292),
        action: lpe_storage::ConversationAction {
            id: Uuid::from_u128(0x9292),
            conversation_id: Uuid::from_u128(0xabab),
            subject: "Conv.Action".to_string(),
            move_folder_entry_id: None,
            move_store_entry_id: None,
            move_target_mailbox_id: None,
            categories_json: "[]".to_string(),
            max_delivery_time: None,
            last_applied_time: None,
            version: 1,
            processed: 0,
            created_at: "2026-05-30T00:00:00Z".to_string(),
            updated_at: "2026-05-30T00:00:00Z".to_string(),
        },
    };
    let source_key = conversation_action_property_value(&action, PID_TAG_SOURCE_KEY);
    let change_key = conversation_action_property_value(&action, PID_TAG_CHANGE_KEY);
    let predecessor = conversation_action_property_value(&action, PID_TAG_PREDECESSOR_CHANGE_LIST);
    let entry_id = conversation_action_property_value(&action, PID_TAG_ENTRY_ID);
    let instance_key = conversation_action_property_value(&action, PID_TAG_INSTANCE_KEY);
    assert_eq!(
        entry_id,
        crate::mapi::identity::message_entry_id_from_object_ids(
            Uuid::nil(),
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            action_id,
        )
        .map(MapiValue::Binary)
    );
    assert_eq!(
        instance_key,
        Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(action_id)
        ))
    );
    assert_ne!(entry_id, instance_key);
    assert_eq!(
        change_key,
        Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::change_number_for_store_id(action_id)
            )
        ))
    );
    assert_eq!(
        predecessor,
        Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
            mapi_mailstore::change_number_for_store_id(action_id)
        )))
    );
    assert_ne!(source_key, predecessor);
}

#[test]
fn microsoft_oxocfg_conversation_action_example_projects_fai_properties() {
    let conversation_id = Uuid::from_bytes([
        0xB7, 0xA2, 0xB5, 0xC4, 0xAA, 0x65, 0x1C, 0xF2, 0xD3, 0x8C, 0x62, 0x8C, 0x0E, 0xAF, 0x56,
        0xC4,
    ]);
    let move_folder_entry_id =
        hex_to_bytes("000000000C99F4EDA2F1E441B15B9B2510913E9D02810000").unwrap();
    let move_store_entry_id = hex_to_bytes(
        "0000000038A1BB1005E5101AA1BB08002B2A56C200006D737073742E646C6C00\
             000000004E495441F9BFB80100AA0037D96E0000000043003A005C0044006F00\
             630075006D0065006E0074007300200061006E00640020005300650074007400\
             69006E00670073005C0061006A0061006D00650073005C004C006F0063006100\
             6C002000530065007400740069006E00670073005C004100700070006C006900\
             63006100740069006F006E00200044006100740061005C004D00690063007200\
             6F0073006F00660074005C004F00750074006C006F006F006B005C0041007200\
             63006800690076006500640020004D00610069006C002E007000730074000000",
    )
    .unwrap();
    let action = lpe_storage::ConversationAction {
        id: Uuid::from_u128(0x4301),
        conversation_id,
        subject: "Solidifying our proposal to Fabrikam, Inc.".to_string(),
        categories_json: "[\"Fabrikam\",\"Business Proposals\"]".to_string(),
        move_folder_entry_id: Some(move_folder_entry_id.clone()),
        move_store_entry_id: Some(move_store_entry_id.clone()),
        move_target_mailbox_id: None,
        max_delivery_time: Some("2009-02-17T23:31:42Z".to_string()),
        last_applied_time: Some("2009-02-17T23:51:11Z".to_string()),
        version: lpe_storage::CONVERSATION_ACTION_VERSION,
        processed: 1,
        created_at: "2009-02-17T23:51:11Z".to_string(),
        updated_at: "2009-02-17T23:51:11Z".to_string(),
    };
    let message = MapiConversationActionMessage {
        id: crate::mapi::identity::mapi_store_id(0x4301),
        folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        canonical_id: action.id,
        action,
    };

    assert_eq!(
        conversation_action_property_value(&message, PID_TAG_SUBJECT_W),
        Some(MapiValue::String(
            "Conv.Action: Solidifying our proposal to Fabrikam, Inc.".to_string()
        ))
    );
    assert_eq!(
        conversation_action_property_value(&message, PID_TAG_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.ConversationAction".to_string()))
    );
    assert_eq!(
        conversation_action_property_value(&message, PID_TAG_CONVERSATION_INDEX),
        Some(MapiValue::Binary(
            hex_to_bytes("010000000000B7A2B5C4AA651CF2D38C628C0EAF56C4").unwrap()
        ))
    );
    assert_eq!(
        conversation_action_property_value(
            &message,
            PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG
        ),
        Some(MapiValue::Binary(move_folder_entry_id))
    );
    assert_eq!(
        conversation_action_property_value(
            &message,
            PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG
        ),
        Some(MapiValue::Binary(move_store_entry_id))
    );
    assert_eq!(
        conversation_action_property_value(&message, PID_LID_CONVERSATION_ACTION_VERSION_TAG),
        Some(MapiValue::I32(0x003C_CCCC))
    );
    assert_eq!(
        conversation_action_property_value(&message, PID_NAME_KEYWORDS_TAG),
        Some(MapiValue::MultiString(vec![
            "Fabrikam".to_string(),
            "Business Proposals".to_string()
        ]))
    );
}

fn round_trip(property_tag: u32, value: &MapiValue) -> MapiValue {
    let mut encoded = Vec::new();
    write_mapi_value(&mut encoded, property_tag, value);
    parse_mapi_property_value(&mut Cursor::new(&encoded), property_tag).unwrap()
}

#[test]
fn property_tag_splits_id_type_and_named_range() {
    let tag = MapiPropertyTag::new(PID_TAG_SUBJECT_W);

    assert_eq!(tag.property_id(), 0x0037);
    assert_eq!(tag.property_type_code(), 0x001F);
    assert_eq!(tag.property_type(), Some(MapiPropertyType::String));
    assert!(MapiPropertyTag::new(0x8001_001F).property_id() >= FIRST_NAMED_PROPERTY_ID);
    assert_eq!(
        MapiPropertyTag::new(0x8031_3003).property_type(),
        Some(MapiPropertyType::MultipleInteger32)
    );
}

#[test]
fn mailbox_properties_report_real_subfolder_state() {
    let parent = mailbox("11111111-1111-1111-1111-111111111111", None, "", "Parent");
    let child = mailbox(
        "22222222-2222-2222-2222-222222222222",
        Some(parent.id),
        "",
        "Child",
    );
    crate::mapi::identity::remember_mapi_identity(
        parent.id,
        crate::mapi::identity::mapi_store_id(0x1001),
    );
    crate::mapi::identity::remember_mapi_identity(
        child.id,
        crate::mapi::identity::mapi_store_id(0x1002),
    );
    let mailboxes = vec![parent.clone(), child.clone()];

    assert_eq!(
        mailbox_property_value_with_context(&parent, &mailboxes, PID_TAG_SUBFOLDERS),
        Some(MapiValue::Bool(true))
    );
    assert_eq!(
        mailbox_property_value_with_context(&child, &mailboxes, PID_TAG_SUBFOLDERS),
        Some(MapiValue::Bool(false))
    );
}

#[test]
fn sync_issues_folder_properties_stay_leaf_with_persisted_children() {
    let parent = mailbox(
        "11111111-1111-1111-1111-11111111111a",
        None,
        "sync_issues",
        "Sync Issues",
    );
    let child = mailbox(
        "11111111-1111-1111-1111-11111111111b",
        Some(parent.id),
        "conflicts",
        "Conflicts",
    );
    let mailboxes = vec![parent.clone(), child];

    assert_eq!(
        mailbox_property_value_with_context(&parent, &mailboxes, PID_TAG_SUBFOLDERS),
        Some(MapiValue::Bool(false))
    );
}

#[test]
fn microsoft_oxcfold_folder_message_size_projects_32_and_64_bit_values() {
    let mut inbox = mailbox(
        "11111111-1111-1111-1111-111111111111",
        None,
        "inbox",
        "Inbox",
    );
    inbox.size_octets = u64::from(u32::MAX) + 10;

    assert_eq!(
        mailbox_property_value_with_context(&inbox, &[], PID_TAG_MESSAGE_SIZE),
        Some(MapiValue::U32(u32::MAX))
    );
    assert_eq!(
        mailbox_property_value_with_context(&inbox, &[], PID_TAG_MESSAGE_SIZE_EXTENDED),
        Some(MapiValue::I64(i64::from(u32::MAX) + 10))
    );
}

#[test]
fn folder_properties_report_deleted_count_total() {
    let mailbox = mailbox(
        "55555555-5555-5555-5555-555555555555",
        None,
        "inbox",
        "Inbox",
    );
    let collection = MapiCollaborationFolder {
        id: CALENDAR_FOLDER_ID,
        kind: MapiCollaborationFolderKind::Calendar,
        collection: CollaborationCollection {
            id: "calendar-default".to_string(),
            kind: "calendar".to_string(),
            owner_account_id: Uuid::nil(),
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            display_name: "Calendar".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        },
        item_count: 0,
    };

    assert_eq!(
        mailbox_property_value_with_context(
            &mailbox,
            std::slice::from_ref(&mailbox),
            PID_TAG_DELETED_COUNT_TOTAL
        ),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_DELETED_COUNT_TOTAL),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_PARENT_FOLDER_ID),
        Some(MapiValue::U64(IPM_SUBTREE_FOLDER_ID))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_RIGHTS),
        Some(MapiValue::U32(MAPI_FOLDER_ACCESS))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_EXTENDED_FOLDER_FLAGS),
        Some(MapiValue::Binary(extended_folder_flags()))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_FOLDER_WEBVIEWINFO),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_DEFAULT_FORM_NAME_W),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_ARCHIVE_TAG),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_RETENTION_PERIOD),
        Some(MapiValue::U32(0))
    );
}

#[test]
fn mailbox_properties_report_persisted_search_folder_type() {
    let mailbox = mailbox(
        "57575757-5757-4757-9757-575757575757",
        None,
        "__mapi_search_folder_message",
        "Categories Rename Search Folder",
    );
    crate::mapi::identity::remember_mapi_identity(
        mailbox.id,
        crate::mapi::identity::mapi_store_id(0x195),
    );

    assert_eq!(
        mailbox_property_value_with_context(
            &mailbox,
            std::slice::from_ref(&mailbox),
            PID_TAG_FOLDER_TYPE
        ),
        Some(MapiValue::U32(FOLDER_SEARCH))
    );
}

#[test]
fn inbox_mailbox_properties_advertise_common_views_default_view() {
    let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_4bbb_8bbb_bbbbbbbbbbbb);
    let mailbox = mailbox(
        "56565656-5656-4656-9656-565656565656",
        None,
        "inbox",
        "Inbox",
    );
    crate::mapi::identity::remember_mapi_identity(mailbox.id, INBOX_FOLDER_ID);

    let expected_entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        account_id,
        COMMON_VIEWS_FOLDER_ID,
        crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID,
    )
    .unwrap();

    assert_eq!(
        mailbox_property_value_with_context_for_account(
            &mailbox,
            &[],
            PID_TAG_DEFAULT_VIEW_ENTRY_ID,
            account_id,
        ),
        Some(MapiValue::Binary(expected_entry_id))
    );
}

#[test]
fn mailbox_backed_internal_note_folders_do_not_advertise_mail_default_view() {
    let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_4bbb_8bbb_bbbbbbbbbbbb);
    let mailbox = mailbox(
        "57575757-5757-4757-9757-575757575757",
        None,
        "sync_issues",
        "Sync Issues",
    );

    assert_eq!(
        mailbox_property_value_with_context_for_account(
            &mailbox,
            &[],
            PID_TAG_DEFAULT_VIEW_ENTRY_ID,
            account_id,
        ),
        None
    );
}

#[test]
fn collaboration_classes_advertise_type_specific_default_views() {
    for (folder_id, container_class) in [
        (TASKS_FOLDER_ID, "IPF.Task"),
        (TODO_SEARCH_FOLDER_ID, "IPF.Task"),
        (NOTES_FOLDER_ID, "IPF.StickyNote"),
        (JOURNAL_FOLDER_ID, "IPF.Journal"),
    ] {
        assert!(default_view_supported_folder(folder_id, container_class));
    }
}

#[test]
fn auxiliary_contact_folders_do_not_advertise_folder_default_views() {
    for folder_id in [
        SUGGESTED_CONTACTS_FOLDER_ID,
        QUICK_CONTACTS_FOLDER_ID,
        IM_CONTACT_LIST_FOLDER_ID,
    ] {
        assert!(!default_view_supported_folder(folder_id, "IPF.Contact"));
    }
}

#[test]
fn collaboration_folder_projects_default_post_message_class_for_contacts() {
    let account_id = Uuid::from_u128(0xcccccccc_cccc_4ccc_8ccc_cccccccccccc);
    let collection = MapiCollaborationFolder {
        id: CONTACTS_FOLDER_ID,
        kind: MapiCollaborationFolderKind::Contacts,
        collection: CollaborationCollection {
            id: "contacts-default".to_string(),
            kind: "contacts".to_string(),
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            display_name: "Contacts".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        },
        item_count: 0,
    };

    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.Contact".to_string()))
    );
    assert_eq!(
        collaboration_folder_property_value(
            &collection,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8
        ),
        Some(MapiValue::String("IPM.Contact".to_string()))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_DEFAULT_VIEW_ENTRY_ID),
        crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            CONTACTS_FOLDER_ID,
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        )
        .map(MapiValue::Binary)
    );
}

#[test]
fn collaboration_calendar_advertises_calendar_default_view() {
    let account_id = Uuid::from_u128(0xdddddddd_dddd_4ddd_8ddd_dddddddddddd);
    let collection = MapiCollaborationFolder {
        id: CALENDAR_FOLDER_ID,
        kind: MapiCollaborationFolderKind::Calendar,
        collection: CollaborationCollection {
            id: "calendar-default".to_string(),
            kind: "calendar".to_string(),
            owner_account_id: account_id,
            owner_email: "alice@example.test".to_string(),
            owner_display_name: "Alice".to_string(),
            display_name: "Calendar".to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        },
        item_count: 1,
    };

    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.Appointment".to_string()))
    );
    assert_eq!(
        collaboration_folder_property_value(&collection, PID_TAG_DEFAULT_VIEW_ENTRY_ID),
        crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            CALENDAR_FOLDER_ID,
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        )
        .map(MapiValue::Binary)
    );
}

#[test]
fn contact_restriction_uses_projected_folder_context() {
    let account_id = Uuid::from_u128(0x11111111_1111_4111_8111_111111111111);
    let mut contact = default_contact_for_mapping(account_id, "default");
    contact.id = Uuid::from_u128(0x22222222_2222_4222_8222_222222222222);
    crate::mapi::identity::remember_mapi_identity(
        contact.id,
        crate::mapi::identity::mapi_store_id(88),
    );
    let expected_parent_source_key = contact_property_value(
        &contact,
        mapi_item_id(&contact.id),
        CONTACTS_SEARCH_FOLDER_ID,
        PID_TAG_PARENT_SOURCE_KEY,
    )
    .expect("projected contact parent source key");
    let restriction = MapiRestriction::Property {
        relop: 0x04,
        property_tag: PID_TAG_PARENT_SOURCE_KEY,
        value: expected_parent_source_key,
    };

    assert!(restriction_matches_contact_in_folder(
        Some(&restriction),
        &contact,
        CONTACTS_SEARCH_FOLDER_ID
    ));
    assert!(!restriction_matches_contact_in_folder(
        Some(&restriction),
        &contact,
        CONTACTS_FOLDER_ID
    ));
}

#[test]
fn content_restriction_uses_microsoft_fuzzy_prefix_semantics() {
    let restriction = MapiRestriction::Content {
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: "IPM.Schedule".to_string(),
        fuzzy_level_low: 0x0002,
        fuzzy_level_high: 0x0001,
    };
    let value_for = |tag| {
        (tag == PID_TAG_MESSAGE_CLASS_W)
            .then(|| MapiValue::String("IPM.Schedule.Meeting.Request".to_string()))
    };
    let non_prefix_value_for = |tag| {
        (tag == PID_TAG_MESSAGE_CLASS_W)
            .then(|| MapiValue::String("IPM.Note.IPM.Schedule".to_string()))
    };

    assert!(restriction_matches(Some(&restriction), value_for));
    assert!(!restriction_matches(
        Some(&restriction),
        non_prefix_value_for
    ));
}

#[test]
fn property_restriction_compares_folder_entry_ids_by_decoded_object_id() {
    let account_id = Uuid::from_u128(0x11111111_2222_4333_8444_555555555555);
    let actual_deleted =
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, TRASH_FOLDER_ID)
            .expect("deleted items entry id");
    let mut microsoft_example_deleted =
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, TRASH_FOLDER_ID)
            .expect("deleted items entry id");
    microsoft_example_deleted[22..38].copy_from_slice(&[0xA5; 16]);
    let microsoft_example_drafts =
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, DRAFTS_FOLDER_ID)
            .expect("drafts entry id");
    let deleted_items_exclusion = MapiRestriction::Property {
        relop: 0x05,
        property_tag: PID_TAG_PARENT_ENTRY_ID,
        value: MapiValue::Binary(microsoft_example_deleted),
    };
    let drafts_exclusion = MapiRestriction::Property {
        relop: 0x05,
        property_tag: PID_TAG_PARENT_ENTRY_ID,
        value: MapiValue::Binary(microsoft_example_drafts),
    };
    let value_for =
        |tag| (tag == PID_TAG_PARENT_ENTRY_ID).then(|| MapiValue::Binary(actual_deleted.clone()));

    assert!(!restriction_matches(
        Some(&deleted_items_exclusion),
        value_for
    ));
    assert!(restriction_matches(Some(&drafts_exclusion), value_for));
}

#[test]
fn microsoft_oxocfg_todo_search_folder_flags_include_required_version() {
    assert_eq!(
        extended_folder_flags_for_folder(INBOX_FOLDER_ID),
        extended_folder_flags()
    );

    let flags = extended_folder_flags_for_folder(TODO_SEARCH_FOLDER_ID);

    assert!(flags
        .windows(6)
        .any(|window| window == [0x01, 0x04, 0x00, 0x00, 0x10, 0x00]));
    assert!(flags
        .windows(6)
        .any(|window| window == [0x05, 0x04, 0x00, 0x00, 0x0C, 0x00]));
}

#[test]
fn microsoft_oxocfg_search_folder_flags_match_search_folder_id_property() {
    let definition = SearchFolderDefinition {
        id: Uuid::from_u128(0x12345678_9abc_4def_8123_456789abcdef),
        account_id: Uuid::from_u128(0xea339446_27b9_4a9c_b0de_873f03a35376),
        role: "contacts_search".to_string(),
        display_name: "Contacts Search".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "contact".to_string(),
        scope_json: serde_json::json!({"scope": "contacts_folders"}),
        restriction_json: serde_json::json!({"kind": "exchange_contacts_search"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    };
    let search_folder_id = search_folder_definition_property_value(
        &definition,
        CONTACTS_SEARCH_FOLDER_ID,
        PID_TAG_SEARCH_FOLDER_ID,
        Uuid::nil(),
    );
    let extended_flags = search_folder_definition_property_value(
        &definition,
        CONTACTS_SEARCH_FOLDER_ID,
        PID_TAG_EXTENDED_FOLDER_FLAGS,
        Uuid::nil(),
    );
    let Some(MapiValue::Binary(search_folder_id)) = search_folder_id else {
        panic!("expected PidTagSearchFolderId");
    };
    let Some(MapiValue::Binary(extended_flags)) = extended_flags else {
        panic!("expected PidTagExtendedFolderFlags");
    };
    let mut expected_subproperty = vec![0x02, 0x10];
    expected_subproperty.extend_from_slice(&search_folder_id);

    assert_eq!(search_folder_id, definition.id.as_bytes());
    assert!(extended_flags
        .windows(expected_subproperty.len())
        .any(|window| window == expected_subproperty.as_slice()));
}

#[test]
fn microsoft_oxosrch_search_folder_definition_blob_header_is_little_endian() {
    let definition_id = Uuid::from_u128(0x12345678_9abc_4def_8123_456789abcdef);
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(123),
    );
    let definition = SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::from_u128(0xea339446_27b9_4a9c_b0de_873f03a35376),
        role: "reminders".to_string(),
        display_name: "Reminders".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({
            "scope": "top_of_personal_folders",
            "recursive": true
        }),
        restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    };

    let Some(MapiValue::Binary(blob)) = search_folder_definition_message_property_value(
        &definition,
        Uuid::nil(),
        PID_TAG_SEARCH_FOLDER_DEFINITION,
    ) else {
        panic!("expected PidTagSearchFolderDefinition");
    };

    assert_eq!(
        &blob[0..8],
        &[0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00]
    );
    assert_eq!(&blob[17..21], &[0x01, 0x00, 0x00, 0x00]);
}

#[test]
fn microsoft_oxosrch_stored_definition_blob_projects_storage_type() {
    let definition_id = Uuid::parse_str("757154c8-c1df-c14c-91de-09c2044d2d1c").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(124),
    );
    let stored_blob = vec![
        0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let definition = SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::from_u128(0xea339446_27b9_4a9c_b0de_873f03a35376),
        role: "unread_mail".to_string(),
        display_name: "Unread Mail".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({
            "kind": "exchange_unread_mail",
            "pidTagSearchFolderTag": 1045439171u32,
            "pidTagSearchFolderDefinition": BASE64_STANDARD.encode(&stored_blob)
        }),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    };

    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_STORAGE_TYPE
        ),
        Some(MapiValue::U32(0x48))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_DEFINITION
        ),
        Some(MapiValue::Binary(stored_blob))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_ID
        ),
        Some(MapiValue::Binary(definition_id.as_bytes().to_vec()))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_MESSAGE_CLASS_W
        ),
        Some(MapiValue::String(
            "IPM.Microsoft.WunderBar.SFInfo".to_string()
        ))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_DISPLAY_NAME_W
        ),
        Some(MapiValue::String("Unread Mail".to_string()))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_LAST_USED
        ),
        Some(MapiValue::U32(214_089_600))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_EXPIRATION
        ),
        Some(MapiValue::U32(214_089_641))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_TEMPLATE_ID
        ),
        Some(MapiValue::U32(2))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_TAG
        ),
        Some(MapiValue::U32(1_045_439_171))
    );
    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_EFP_FLAGS
        ),
        Some(MapiValue::U32(0))
    );
}

#[test]
fn microsoft_oxosrch_large_messages_template_projects_text_and_numerical_search() {
    let definition_id = Uuid::from_u128(0x757154c8_c1df_c14c_91de_09c2044d2d1c);
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(125),
    );
    let definition = SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::from_u128(0xea339446_27b9_4a9c_b0de_873f03a35376),
        role: "large_messages".to_string(),
        display_name: "Large Messages".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({
            "scope": "top_of_personal_folders",
            "recursive": true
        }),
        restriction_json: serde_json::json!({
            "kind": "exchange_large_messages",
            "pidTagSearchFolderTemplateId": 10,
            "pidTagSearchFolderTextSearch": "larger than 1024 KB",
            "pidTagSearchFolderNumericalSearch": 1024
        }),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    };

    let Some(MapiValue::Binary(blob)) = search_folder_definition_message_property_value(
        &definition,
        Uuid::nil(),
        PID_TAG_SEARCH_FOLDER_DEFINITION,
    ) else {
        panic!("expected PidTagSearchFolderDefinition");
    };

    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_TEMPLATE_ID
        ),
        Some(MapiValue::U32(10))
    );
    assert_eq!(&blob[4..8], &0x4Bu32.to_le_bytes());
    assert_eq!(&blob[8..12], &1024u32.to_le_bytes());
    assert_eq!(blob[12], "larger than 1024 KB".len() as u8);
    assert_eq!(&blob[13..32], b"larger than 1024 KB");
    assert_eq!(&blob[32..36], &0u32.to_le_bytes());
    assert_eq!(&blob[36..40], &1u32.to_le_bytes());
}

#[test]
fn microsoft_oxosrch_old_mail_template_projects_big_endian_age_numerical_search() {
    let definition_id = Uuid::from_u128(0x857154c8_c1df_c14c_91de_09c2044d2d1c);
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(126),
    );
    let definition = SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::from_u128(0xea339446_27b9_4a9c_b0de_873f03a35376),
        role: "old_mail".to_string(),
        display_name: "Old Mail".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({
            "scope": "top_of_personal_folders",
            "recursive": true
        }),
        restriction_json: serde_json::json!({
            "kind": "exchange_old_mail",
            "pidTagSearchFolderTemplateId": 11,
            "pidTagSearchFolderStorageType": 0x00002049u32,
            "pidTagSearchFolderNumericalSearchAge": {
                "unit": "weeks",
                "amount": 42
            }
        }),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    };

    let Some(MapiValue::Binary(blob)) = search_folder_definition_message_property_value(
        &definition,
        Uuid::nil(),
        PID_TAG_SEARCH_FOLDER_DEFINITION,
    ) else {
        panic!("expected PidTagSearchFolderDefinition");
    };

    assert_eq!(
        search_folder_definition_message_property_value(
            &definition,
            Uuid::nil(),
            PID_TAG_SEARCH_FOLDER_TEMPLATE_ID
        ),
        Some(MapiValue::U32(11))
    );
    assert_eq!(&blob[4..8], &0x0000_2049u32.to_le_bytes());
    assert_eq!(&blob[8..12], &[0x00, 0x01, 0x00, 0x2A]);
    assert_eq!(blob[12], 0);
    assert_eq!(&blob[13..17], &0u32.to_le_bytes());
    assert_eq!(&blob[17..21], &1u32.to_le_bytes());
}

#[test]
fn contact_email_named_property_restriction_matches_primary_email() {
    let account_id = Uuid::from_u128(0x33333333_3333_4333_8333_333333333333);
    let mut contact = default_contact_for_mapping(account_id, "default");
    contact.id = Uuid::from_u128(0x55555555_5555_4555_8555_555555555555);
    contact.email = "denis@example.test".to_string();
    crate::mapi::identity::remember_mapi_identity(
        contact.id,
        crate::mapi::identity::mapi_store_id(89),
    );
    let restriction = MapiRestriction::Or(vec![
        MapiRestriction::Exist {
            property_tag: PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG,
        },
        MapiRestriction::Exist {
            property_tag: PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG,
        },
        MapiRestriction::Exist {
            property_tag: PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG,
        },
    ]);

    assert!(restriction_matches_contact_in_folder(
        Some(&restriction),
        &contact,
        CONTACTS_FOLDER_ID
    ));
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG
        ),
        Some(MapiValue::String("denis@example.test".to_string()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG
        ),
        Some(MapiValue::String("SMTP".to_string()))
    );
}

#[test]
fn contact_secondary_email_named_property_uses_emails_json() {
    let account_id = Uuid::from_u128(0x44444444_4444_4444_8444_444444444444);
    let mut contact = default_contact_for_mapping(account_id, "default");
    contact.email = "primary@example.test".to_string();
    contact.emails_json = serde_json::json!([
        {"label": "work", "email": "primary@example.test"},
        {"label": "home", "email": "secondary@example.test"}
    ]);

    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
        ),
        Some(MapiValue::String("secondary@example.test".to_string()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG
        ),
        Some(MapiValue::String("SMTP".to_string()))
    );
}

#[test]
fn outlook_contact_view_email_alias_restriction_matches_primary_email() {
    let account_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777777);
    let mut contact = default_contact_for_mapping(account_id, "default");
    contact.id = Uuid::from_u128(0x88888888_8888_4888_8888_888888888888);
    contact.email = "denis@example.test".to_string();
    crate::mapi::identity::remember_mapi_identity(
        contact.id,
        crate::mapi::identity::mapi_store_id(91),
    );
    let restriction = MapiRestriction::Or(vec![
        MapiRestriction::Exist {
            property_tag: PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS_W_TAG,
        },
        MapiRestriction::Exist {
            property_tag: PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS_W_TAG,
        },
        MapiRestriction::Exist {
            property_tag: PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS_W_TAG,
        },
    ]);

    assert!(restriction_matches_contact_in_folder(
        Some(&restriction),
        &contact,
        CONTACTS_FOLDER_ID
    ));
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS_W_TAG
        ),
        Some(MapiValue::String("denis@example.test".to_string()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME_W_TAG
        ),
        Some(MapiValue::String("denis@example.test".to_string()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_ADDRESS_TYPE_W_TAG
        ),
        Some(MapiValue::String("SMTP".to_string()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS_W_TAG
        ),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG
        ),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
        ),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG
        ),
        Some(MapiValue::String(String::new()))
    );
}

#[test]
fn contact_property_projects_outlook_table_identity_columns() {
    let account_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777779);
    let mut contact = default_contact_for_mapping(account_id, "default");
    contact.id = Uuid::from_u128(0x99999999_9999_4999_8999_999999999999);
    let item_id = crate::mapi::identity::mapi_store_id(0x043);
    crate::mapi::identity::remember_mapi_identity(contact.id, item_id);

    assert_eq!(
        contact_property_value(&contact, item_id, CONTACTS_FOLDER_ID, PID_TAG_FOLDER_ID),
        Some(MapiValue::U64(CONTACTS_FOLDER_ID))
    );
    assert_eq!(
        contact_property_value(&contact, item_id, CONTACTS_FOLDER_ID, PID_TAG_MID),
        Some(MapiValue::U64(item_id))
    );
    assert_eq!(
        contact_property_value(&contact, item_id, CONTACTS_FOLDER_ID, PID_TAG_INST_ID),
        Some(MapiValue::U64(item_id))
    );
    assert_eq!(
        contact_property_value(&contact, item_id, CONTACTS_FOLDER_ID, PID_TAG_INSTANCE_NUM),
        Some(MapiValue::U32(0))
    );
}

#[test]
fn outlook_contact_search_source_columns_project_empty_values() {
    let account_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777778);
    let contact = default_contact_for_mapping(account_id, "default");

    assert_eq!(
        contact_property_value(&contact, 1, CONTACTS_SEARCH_FOLDER_ID, 0x8450_0102),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_SEARCH_FOLDER_ID,
            PID_NAME_OSC_CONTACT_SOURCES_TAG
        ),
        Some(MapiValue::MultiString(Vec::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_SEARCH_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_SOURCE_80E0_TAG
        ),
        Some(MapiValue::Bool(false))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_SEARCH_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_SOURCE_80E2_TAG
        ),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_SEARCH_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_SOURCE_80E3_TAG
        ),
        Some(MapiValue::MultiString(Vec::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_SEARCH_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_SOURCE_80E5_TAG
        ),
        Some(MapiValue::MultiBinary(Vec::new()))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_SEARCH_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_SOURCE_80E6_TAG
        ),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_SEARCH_FOLDER_ID,
            PID_LID_OUTLOOK_CONTACT_SOURCE_80E8_TAG
        ),
        Some(MapiValue::Guid(Uuid::nil().into_bytes()))
    );
}

#[test]
fn public_folder_projects_default_post_message_class_from_folder_class() {
    let folder = MapiPublicFolder {
        id: PUBLIC_FOLDERS_ROOT_FOLDER_ID + 0x10000,
        folder: PublicFolder {
            id: Uuid::from_u128(1),
            tree_id: Uuid::from_u128(2),
            parent_folder_id: None,
            canonical_id: Uuid::from_u128(3),
            display_name: "Public Contacts".to_string(),
            folder_class: "IPF.Contact".to_string(),
            path: "/Public Contacts".to_string(),
            sort_order: 0,
            lifecycle_state: "active".to_string(),
            change_counter: 1,
            rights: PublicFolderRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        },
        item_count: 0,
        child_count: 0,
    };

    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.Contact".to_string()))
    );
    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8),
        Some(MapiValue::String("IPM.Contact".to_string()))
    );
    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_RIGHTS),
        Some(MapiValue::U32(MAPI_FOLDER_ACCESS))
    );
    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_EXTENDED_FOLDER_FLAGS),
        Some(MapiValue::Binary(extended_folder_flags()))
    );
    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_FOLDER_WEBVIEWINFO),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_DEFAULT_FORM_NAME_W),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_ARCHIVE_TAG),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        public_folder_property_value(&folder, PID_TAG_RETENTION_PERIOD),
        Some(MapiValue::U32(0))
    );
}

#[test]
fn note_derived_folder_classes_project_default_post_message_class() {
    assert_eq!(
        default_post_message_class_for_container_class("IPF.Note.OutlookHomepage"),
        Some("IPM.Note")
    );
}

#[test]
fn moc_contact_folder_classes_project_contact_default_post_message_class() {
    assert_eq!(
        default_post_message_class_for_container_class("IPF.Contact.MOC.QuickContacts"),
        Some("IPM.Contact")
    );
    assert_eq!(
        default_post_message_class_for_container_class("IPF.Contact.MOC.ImContactList"),
        Some("IPM.Contact")
    );
}

#[test]
fn mailbox_parent_source_key_uses_real_parent_when_context_is_available() {
    let parent = mailbox("33333333-3333-3333-3333-333333333333", None, "", "Parent");
    let child = mailbox(
        "44444444-4444-4444-4444-444444444444",
        Some(parent.id),
        "",
        "Child",
    );
    crate::mapi::identity::remember_mapi_identity(
        parent.id,
        crate::mapi::identity::mapi_store_id(0x1003),
    );
    crate::mapi::identity::remember_mapi_identity(
        child.id,
        crate::mapi::identity::mapi_store_id(0x1004),
    );
    let mailboxes = vec![parent.clone(), child.clone()];

    assert_eq!(
        mailbox_property_value_with_context(&child, &mailboxes, PID_TAG_PARENT_SOURCE_KEY),
        Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_folder(&parent)
        ))
    );
    assert_eq!(
        mailbox_property_value_with_context(&parent, &mailboxes, PID_TAG_PARENT_SOURCE_KEY),
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            IPM_SUBTREE_FOLDER_ID
        )))
    );
}

#[test]
fn mailbox_parent_source_key_keeps_root_level_search_specials_under_root() {
    for (role, expected_folder_id) in [
        ("reminders", REMINDERS_FOLDER_ID),
        ("todo_search", TODO_SEARCH_FOLDER_ID),
        ("tracked_mail_processing", TRACKED_MAIL_PROCESSING_FOLDER_ID),
    ] {
        let mailbox = mailbox("55555555-5555-4555-9555-555555555555", None, role, role);

        assert_eq!(mapi_folder_id(&mailbox), expected_folder_id);
        assert_eq!(
            mailbox_property_value_with_context(
                &mailbox,
                std::slice::from_ref(&mailbox),
                PID_TAG_PARENT_SOURCE_KEY
            ),
            Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
                ROOT_FOLDER_ID
            )))
        );
    }
}

#[test]
fn swapped_todo_data_parser_accepts_documented_layout() {
    let parsed = parse_swapped_todo_data(&valid_swapped_todo_data()).unwrap();
    assert_eq!(parsed.todo_item_flags, Some(8));
    assert_eq!(parsed.flag_request.as_deref(), Some("Follow up"));
    assert_eq!(parsed.start_minutes, Some(1_000_000));
    assert_eq!(parsed.due_minutes, Some(1_001_440));
    assert_eq!(parsed.reminder_minutes, Some(1_000_030));
    assert_eq!(parsed.reminder_set, Some(true));
}

#[test]
fn swapped_todo_data_parser_rejects_placeholder_bytes() {
    assert!(parse_swapped_todo_data(&[1, 2, 3, 4]).is_err());
    let mut unsupported = valid_swapped_todo_data();
    unsupported[0..4].copy_from_slice(&2u32.to_le_bytes());
    assert!(parse_swapped_todo_data(&unsupported).is_err());
    let mut unknown_flags = valid_swapped_todo_data();
    unknown_flags[4..8].copy_from_slice(&0x8000_0000u32.to_le_bytes());
    assert!(parse_swapped_todo_data(&unknown_flags).is_err());
}

#[test]
fn special_folder_identification_properties_project_store_folder_ids() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    assert_eq!(PID_TAG_VALID_FOLDER_MASK, 0x35DF_0003);
    assert_eq!(PID_TAG_IPM_SUBTREE_ENTRY_ID, 0x35E0_0102);
    assert_eq!(PID_TAG_IPM_OUTBOX_ENTRY_ID, 0x35E2_0102);
    assert_eq!(PID_TAG_IPM_WASTEBASKET_ENTRY_ID, 0x35E3_0102);
    assert_eq!(PID_TAG_IPM_SENTMAIL_ENTRY_ID, 0x35E4_0102);
    assert_eq!(PID_TAG_VIEWS_ENTRY_ID, 0x35E5_0102);
    assert_eq!(PID_TAG_COMMON_VIEWS_ENTRY_ID, 0x35E6_0102);
    assert_eq!(PID_TAG_FINDER_ENTRY_ID, 0x35E7_0102);
    assert_eq!(PID_TAG_IPM_ARCHIVE_ENTRY_ID, 0x35FF_0102);
    assert_eq!(PID_TAG_IPM_APPOINTMENT_ENTRY_ID, 0x36D0_0102);
    assert_eq!(PID_TAG_IPM_CONTACT_ENTRY_ID, 0x36D1_0102);
    assert_eq!(PID_TAG_IPM_JOURNAL_ENTRY_ID, 0x36D2_0102);
    assert_eq!(PID_TAG_IPM_NOTE_ENTRY_ID, 0x36D3_0102);
    assert_eq!(PID_TAG_IPM_TASK_ENTRY_ID, 0x36D4_0102);
    assert_eq!(PID_TAG_REM_ONLINE_ENTRY_ID, 0x36D5_0102);
    assert_eq!(PID_TAG_REM_OFFLINE_ENTRY_ID, 0x36D6_0102);
    assert_eq!(PID_TAG_IPM_DRAFTS_ENTRY_ID, 0x36D7_0102);
    assert_eq!(PID_TAG_FREE_BUSY_ENTRY_IDS, 0x36E4_1102);

    assert_eq!(
        special_folder_identification_property_value(Uuid::nil(), PID_TAG_VALID_FOLDER_MASK),
        Some(MapiValue::U32(0xFF))
    );

    for (property_tag, folder_id) in [
        (PID_TAG_IPM_SUBTREE_ENTRY_ID, IPM_SUBTREE_FOLDER_ID),
        (PID_TAG_IPM_OUTBOX_ENTRY_ID, OUTBOX_FOLDER_ID),
        (PID_TAG_IPM_WASTEBASKET_ENTRY_ID, TRASH_FOLDER_ID),
        (PID_TAG_IPM_SENTMAIL_ENTRY_ID, SENT_FOLDER_ID),
        (PID_TAG_VIEWS_ENTRY_ID, VIEWS_FOLDER_ID),
        (PID_TAG_COMMON_VIEWS_ENTRY_ID, COMMON_VIEWS_FOLDER_ID),
        (PID_TAG_FINDER_ENTRY_ID, SEARCH_FOLDER_ID),
        (PID_TAG_IPM_ARCHIVE_ENTRY_ID, ARCHIVE_FOLDER_ID),
        (PID_TAG_IPM_APPOINTMENT_ENTRY_ID, CALENDAR_FOLDER_ID),
        (PID_TAG_IPM_CONTACT_ENTRY_ID, CONTACTS_FOLDER_ID),
        (PID_TAG_IPM_JOURNAL_ENTRY_ID, JOURNAL_FOLDER_ID),
        (PID_TAG_IPM_NOTE_ENTRY_ID, NOTES_FOLDER_ID),
        (PID_TAG_IPM_TASK_ENTRY_ID, TASKS_FOLDER_ID),
        (PID_TAG_IPM_DRAFTS_ENTRY_ID, DRAFTS_FOLDER_ID),
    ] {
        let entry_id =
            crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id).unwrap();
        assert_eq!(
            special_folder_identification_property_value(mailbox_guid, property_tag),
            Some(MapiValue::Binary(entry_id.clone()))
        );
        assert_eq!(entry_id.len(), 46);
        assert_eq!(
            crate::mapi::identity::object_id_from_folder_identifier_bytes(&entry_id),
            Some(folder_id)
        );
    }
    let reminders_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, REMINDERS_FOLDER_ID)
            .unwrap();
    assert_eq!(
        special_folder_identification_property_value(mailbox_guid, PID_TAG_REM_ONLINE_ENTRY_ID),
        Some(MapiValue::Binary(reminders_entry_id.clone()))
    );
    assert_eq!(
        special_folder_identification_property_value(mailbox_guid, PID_TAG_REM_OFFLINE_ENTRY_ID),
        Some(MapiValue::Binary(reminders_entry_id.clone()))
    );
    assert_eq!(
        crate::mapi::identity::object_id_from_folder_identifier_bytes(&reminders_entry_id),
        Some(REMINDERS_FOLDER_ID)
    );
}

#[test]
fn additional_ren_entry_ids_ex_advertises_outlook_store_special_folders() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let Some(MapiValue::Binary(value)) = special_folder_identification_property_value(
        mailbox_guid,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
    ) else {
        panic!("expected AdditionalRenEntryIdsEx binary value");
    };

    let mut offset = 0;
    let mut entries = Vec::new();
    loop {
        let persist_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
        let data_size = u16::from_le_bytes(value[offset + 2..offset + 4].try_into().unwrap());
        offset += 4;
        if persist_id == 0 {
            break;
        }
        let block_end = offset + data_size as usize;
        let mut folder_id = None;
        while offset < block_end {
            let element_id = u16::from_le_bytes(value[offset..offset + 2].try_into().unwrap());
            let element_size =
                u16::from_le_bytes(value[offset + 2..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if element_id == 0 {
                break;
            }
            let element = &value[offset..offset + element_size];
            if element_id == 0x0001 {
                folder_id = crate::mapi::identity::object_id_from_folder_identifier_bytes(element);
            }
            offset += element_size;
        }
        offset = block_end;
        entries.push((persist_id, folder_id));
    }

    assert_eq!(
        entries,
        vec![
            (0x8001, Some(RSS_FEEDS_FOLDER_ID)),
            (0x8002, Some(TRACKED_MAIL_PROCESSING_FOLDER_ID)),
            (0x8004, Some(TODO_SEARCH_FOLDER_ID)),
            (0x8006, Some(CONVERSATION_ACTION_SETTINGS_FOLDER_ID)),
            (0x8007, Some(QUICK_STEP_SETTINGS_FOLDER_ID)),
            (0x8008, Some(SUGGESTED_CONTACTS_FOLDER_ID)),
            (0x8009, Some(CONTACTS_SEARCH_FOLDER_ID)),
            (0x800A, Some(IM_CONTACT_LIST_FOLDER_ID)),
            (0x800B, Some(QUICK_CONTACTS_FOLDER_ID)),
            (0x800F, Some(ARCHIVE_FOLDER_ID)),
        ]
    );
    assert_eq!(value.len(), 544);
}

#[test]
fn additional_ren_entry_ids_advertises_documented_indexed_special_folders() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let Some(MapiValue::MultiBinary(values)) = special_folder_identification_property_value(
        mailbox_guid,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
    ) else {
        panic!("expected AdditionalRenEntryIds multi-binary value");
    };

    assert_eq!(
        values
            .iter()
            .map(|entry_id| crate::mapi::identity::object_id_from_folder_identifier_bytes(entry_id))
            .collect::<Vec<_>>(),
        vec![
            Some(CONFLICTS_FOLDER_ID),
            Some(SYNC_ISSUES_FOLDER_ID),
            Some(LOCAL_FAILURES_FOLDER_ID),
            Some(SERVER_FAILURES_FOLDER_ID),
            Some(JUNK_FOLDER_ID),
        ]
    );
    assert!(values.iter().all(|entry_id| entry_id.len() == 46));
}

#[test]
fn free_busy_entry_ids_advertises_freebusy_data_at_documented_index() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let Some(MapiValue::MultiBinary(values)) =
        special_folder_identification_property_value(mailbox_guid, PID_TAG_FREE_BUSY_ENTRY_IDS)
    else {
        panic!("expected FreeBusyEntryIds multi-binary value");
    };

    assert_eq!(values.len(), 4);
    assert!(values[..3].iter().all(Vec::is_empty));
    assert_eq!(
        crate::mapi::identity::object_id_from_folder_identifier_bytes(&values[3]),
        Some(FREEBUSY_DATA_FOLDER_ID)
    );
    assert_eq!(values[3].len(), 46);
}

#[test]
fn typed_scalar_property_values_round_trip() {
    assert_eq!(
        round_trip(0x3001_001F, &MapiValue::String("Inbox".to_string())),
        MapiValue::String("Inbox".to_string())
    );
    assert_eq!(
        round_trip(0x3001_001E, &MapiValue::String("Inbox".to_string())),
        MapiValue::String("Inbox".to_string())
    );
    assert_eq!(
        round_trip(0x3602_0003, &MapiValue::I32(42)),
        MapiValue::I32(42)
    );
    assert_eq!(
        round_trip(0x360A_000B, &MapiValue::Bool(true)),
        MapiValue::Bool(true)
    );
    assert_eq!(
        round_trip(0x6748_0014, &MapiValue::I64(99)),
        MapiValue::I64(99)
    );
    assert_eq!(
        round_trip(
            PID_LID_PERCENT_COMPLETE_TAG,
            &MapiValue::F64(1.0f64.to_bits())
        ),
        MapiValue::F64(1.0f64.to_bits())
    );
}

#[test]
fn object_id_properties_use_mapi_wire_ids() {
    let mut encoded = Vec::new();
    write_mapi_value(
        &mut encoded,
        PID_TAG_FOLDER_ID,
        &MapiValue::U64(crate::mapi::identity::CALENDAR_FOLDER_ID),
    );

    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&encoded),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
}

#[test]
fn microsoft_oxprops_message_size_projects_integer32_property() {
    assert_eq!(mapi_message_size_value(512), MapiValue::U32(512));
    assert_eq!(
        mapi_message_size_value(i64::from(u32::MAX) + 10),
        MapiValue::U32(u32::MAX)
    );
    assert_eq!(mapi_message_size_value(-1), MapiValue::U32(0));
    assert_eq!(
        round_trip(PID_TAG_MESSAGE_SIZE, &mapi_message_size_value(512)),
        MapiValue::I32(512)
    );
    assert_eq!(
        mapi_message_size_extended_value(i64::from(u32::MAX) + 10),
        MapiValue::I64(i64::from(u32::MAX) + 10)
    );
    assert_eq!(mapi_message_size_extended_value(-1), MapiValue::I64(0));
    assert_eq!(
        round_trip(
            PID_TAG_MESSAGE_SIZE_EXTENDED,
            &mapi_message_size_extended_value(i64::from(u32::MAX) + 10),
        ),
        MapiValue::I64(i64::from(u32::MAX) + 10)
    );

    let account_id = Uuid::from_u128(0x33333333_3333_4333_8333_333333333333);
    let mut contact = default_contact_for_mapping(account_id, "default");
    contact.name = "Ada".to_string();
    assert!(matches!(
        contact_property_value(&contact, 1, CONTACTS_FOLDER_ID, PID_TAG_MESSAGE_SIZE),
        Some(MapiValue::U32(_))
    ));
    assert!(matches!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_TAG_MESSAGE_SIZE_EXTENDED
        ),
        Some(MapiValue::I64(_))
    ));

    let mut event = default_event_for_mapping(account_id, "default");
    event.title = "Standup".to_string();
    assert!(matches!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_MESSAGE_SIZE),
        Some(MapiValue::U32(_))
    ));
    assert!(matches!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_MESSAGE_SIZE_EXTENDED),
        Some(MapiValue::I64(_))
    ));

    let mut task = default_task_for_mapping(account_id, "default");
    task.title = "Follow up".to_string();
    assert!(matches!(
        task_property_value(&task, 1, TASKS_FOLDER_ID, PID_TAG_MESSAGE_SIZE),
        Some(MapiValue::U32(_))
    ));
    assert!(matches!(
        task_property_value(&task, 1, TASKS_FOLDER_ID, PID_TAG_MESSAGE_SIZE_EXTENDED),
        Some(MapiValue::I64(_))
    ));

    let note = ClientNote {
        title: "Note".to_string(),
        ..default_note_for_mapping()
    };
    assert!(matches!(
        note_property_value(&note, 1, NOTES_FOLDER_ID, PID_TAG_MESSAGE_SIZE),
        Some(MapiValue::U32(_))
    ));
    assert!(matches!(
        note_property_value(&note, 1, NOTES_FOLDER_ID, PID_TAG_MESSAGE_SIZE_EXTENDED),
        Some(MapiValue::I64(_))
    ));

    let journal = JournalEntry {
        subject: "Call".to_string(),
        ..default_journal_entry_for_mapping()
    };
    assert!(matches!(
        journal_entry_property_value(&journal, 1, JOURNAL_FOLDER_ID, PID_TAG_MESSAGE_SIZE),
        Some(MapiValue::U32(_))
    ));
    assert!(matches!(
        journal_entry_property_value(
            &journal,
            1,
            JOURNAL_FOLDER_ID,
            PID_TAG_MESSAGE_SIZE_EXTENDED,
        ),
        Some(MapiValue::I64(_))
    ));
}

#[test]
fn binary_property_uses_rop_u16_length_prefix() {
    let mut encoded = Vec::new();
    write_mapi_value(
        &mut encoded,
        PID_TAG_ATTACH_DATA_BINARY,
        &MapiValue::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    );

    assert_eq!(encoded, vec![0x04, 0x00, 0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(
        parse_mapi_property_value(&mut Cursor::new(&encoded), PID_TAG_ATTACH_DATA_BINARY).unwrap(),
        MapiValue::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF])
    );
}

#[test]
fn microsoft_oxcmsg_attachment_size_projects_integer32_property() {
    let attachment = MapiAttachment {
        attach_num: 1,
        canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
        file_reference: "attachment:message:one".to_string(),
        file_name: "test.txt".to_string(),
        media_type: "text/plain".to_string(),
        disposition: None,
        content_id: None,
        size_octets: 512,
    };

    assert_eq!(
        attachment_property_value(&attachment, PID_TAG_ATTACH_SIZE),
        Some(MapiValue::U32(512))
    );
    assert_eq!(
        round_trip(PID_TAG_ATTACH_SIZE, &MapiValue::U32(512)),
        MapiValue::I32(512)
    );
}

#[test]
fn multivalue_strings_and_binaries_round_trip() {
    let strings = MapiValue::MultiString(vec!["alpha".to_string(), "beta".to_string()]);
    let binaries = MapiValue::MultiBinary(vec![vec![0x01, 0x02], vec![0xAA, 0xBB, 0xCC]]);

    assert_eq!(round_trip(0x8001_101F, &strings), strings);
    assert_eq!(round_trip(0x8002_1102, &binaries), binaries);
}

#[test]
fn large_inline_string_round_trips_through_common_codec() {
    let large = MapiValue::String("A".repeat(4096));

    assert_eq!(round_trip(PID_TAG_BODY_W, &large), large);
}

#[test]
fn mapi_note_and_journal_inputs_preserve_canonical_fields() {
    let mut note_properties = HashMap::new();
    note_properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Mapped note".into()));
    note_properties.insert(PID_TAG_BODY_W, MapiValue::String("Note body".into()));
    note_properties.insert(PID_LID_NOTE_COLOR_TAG, MapiValue::I32(1));
    let note = note_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(1)),
        &default_note_for_mapping(),
        &note_properties,
    );
    assert_eq!(note.id, Some(Uuid::from_u128(1)));
    assert_eq!(note.title, "Mapped note");
    assert_eq!(note.body_text, "Note body");
    assert_eq!(note.color, "green");

    let mut journal_properties = HashMap::new();
    journal_properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Mapped call".into()));
    journal_properties.insert(PID_TAG_BODY_W, MapiValue::String("Call body".into()));
    journal_properties.insert(
        PID_TAG_MESSAGE_CLASS_W,
        MapiValue::String("IPM.Activity".into()),
    );
    journal_properties.insert(
        PID_LID_LOG_TYPE_W_TAG,
        MapiValue::String("Phone call".into()),
    );
    journal_properties.insert(
        PID_LID_COMPANIES_TAG,
        MapiValue::MultiString(vec!["Contoso".into()]),
    );
    journal_properties.insert(
        PID_LID_CONTACTS_TAG,
        MapiValue::MultiString(vec!["Adam Barr".into(), "Ryan Gregg".into()]),
    );
    journal_properties.insert(
        PID_LID_CONTACT_LINK_ENTRY_TAG,
        MapiValue::Binary(empty_contact_link_entry_blob()),
    );
    journal_properties.insert(
        PID_LID_CONTACT_LINK_SEARCH_KEY_TAG,
        MapiValue::Binary(empty_contact_link_search_key_blob()),
    );
    let journal = journal_entry_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(2)),
        &default_journal_entry_for_mapping(),
        &journal_properties,
    );
    assert_eq!(journal.id, Some(Uuid::from_u128(2)));
    assert_eq!(journal.subject, "Mapped call");
    assert_eq!(journal.body_text, "Call body");
    assert_eq!(journal.entry_type, "Phone call");
    assert_eq!(journal.message_class, "IPM.Activity");
    assert_eq!(journal.companies_json, "[\"Contoso\"]");
    assert_eq!(journal.contacts_json, "[\"Adam Barr\",\"Ryan Gregg\"]");

    let mut link_name_properties = HashMap::new();
    link_name_properties.insert(
        PID_LID_CONTACT_LINK_NAME_W_TAG,
        MapiValue::String("Adam Barr; Ryan Gregg".into()),
    );
    let journal = journal_entry_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(3)),
        &default_journal_entry_for_mapping(),
        &link_name_properties,
    );
    assert_eq!(journal.contacts_json, "[\"Adam Barr\",\"Ryan Gregg\"]");
}

#[test]
fn mapi_contact_narrow_update_omits_unowned_rich_fields() {
    let existing = AccessibleContact {
        id: Uuid::from_u128(0xabc1),
        name: "Ada Example".to_string(),
        email: "ada@example.test".to_string(),
        phone: "+1 555 0100".to_string(),
        addresses_json: serde_json::json!([{"full": "1 Example Way"}]),
        urls_json: serde_json::json!([{"url": "https://example.test"}]),
        raw_vcard: Some("BEGIN:VCARD\nEND:VCARD".to_string()),
        source: ContactSourceFields {
            import_source: "carddav".to_string(),
            source_uid: Some("uid-1".to_string()),
            source_etag: Some("etag-1".to_string()),
            source_payload_json: serde_json::json!({"href": "/contacts/1.vcf"}),
        },
        ..AccessibleContact::default()
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_DISPLAY_NAME_W,
        MapiValue::String("Ada Updated".to_string()),
    );

    let input = contact_input_from_mapi(
        Uuid::from_u128(0xabc2),
        Some(existing.id),
        &existing,
        &properties,
    );

    assert_eq!(input.name, "Ada Updated");
    assert_eq!(input.addresses_json, None);
    assert_eq!(input.raw_vcard, None);
    assert!(!input.raw_vcard_is_explicit);
    assert!(!input.source_is_explicit);
}

#[test]
fn mapi_note_and_journal_named_properties_project_canonical_values() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_NOTE_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_NOTE_COLOR),
        }),
        Some(PID_LID_NOTE_COLOR as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_LOG_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_LOG_TYPE),
        }),
        Some(PID_LID_LOG_TYPE as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_CONTACT_LINK_ENTRY),
        }),
        Some(PID_LID_CONTACT_LINK_ENTRY as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_CONTACT_LINK_SEARCH_KEY),
        }),
        Some(PID_LID_CONTACT_LINK_SEARCH_KEY as u16)
    );

    let note = ClientNote {
        color: "pink".to_string(),
        ..default_note_for_mapping()
    };
    assert_eq!(
        note_property_value(&note, 1, NOTES_FOLDER_ID, PID_LID_NOTE_COLOR_TAG),
        Some(MapiValue::I32(2))
    );

    let journal = JournalEntry {
        entry_type: "Phone call".to_string(),
        starts_at: Some("2026-05-19T10:00:00Z".to_string()),
        companies_json: "[\"Contoso\"]".to_string(),
        contacts_json: "[\"Adam Barr\",\"Ryan Gregg\"]".to_string(),
        ..default_journal_entry_for_mapping()
    };
    assert_eq!(
        journal_entry_property_value(&journal, 1, JOURNAL_FOLDER_ID, PID_LID_LOG_TYPE_W_TAG),
        Some(MapiValue::String("Phone call".to_string()))
    );
    assert_eq!(
        journal_entry_property_value(&journal, 1, JOURNAL_FOLDER_ID, PID_LID_COMPANIES_TAG),
        Some(MapiValue::MultiString(vec!["Contoso".to_string()]))
    );
    assert_eq!(
        journal_entry_property_value(
            &journal,
            1,
            JOURNAL_FOLDER_ID,
            PID_LID_CONTACT_LINK_NAME_W_TAG
        ),
        Some(MapiValue::String("Adam Barr; Ryan Gregg".to_string()))
    );
    assert_eq!(
        journal_entry_property_value(&journal, 1, JOURNAL_FOLDER_ID, PID_LID_CONTACTS_TAG),
        Some(MapiValue::MultiString(vec![
            "Adam Barr".to_string(),
            "Ryan Gregg".to_string()
        ]))
    );
    assert_eq!(
        journal_entry_property_value(
            &journal,
            1,
            JOURNAL_FOLDER_ID,
            PID_LID_CONTACT_LINK_ENTRY_TAG
        ),
        Some(MapiValue::Binary(empty_contact_link_entry_blob()))
    );
    assert_eq!(
        journal_entry_property_value(
            &journal,
            1,
            JOURNAL_FOLDER_ID,
            PID_LID_CONTACT_LINK_SEARCH_KEY_TAG
        ),
        Some(MapiValue::Binary(empty_contact_link_search_key_blob()))
    );
}

#[test]
fn internet_header_content_named_properties_have_stable_ids() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PS_INTERNET_HEADERS_GUID,
            kind: MapiNamedPropertyKind::Name("content-class".to_string()),
        }),
        Some(MapiPropertyTag::new(PID_NAME_CONTENT_CLASS_W_TAG).property_id())
    );
    assert_eq!(
        well_known_named_property_id(&normalize_named_property(MapiNamedProperty {
            guid: PS_INTERNET_HEADERS_GUID,
            kind: MapiNamedPropertyKind::Name("Content-Class".to_string()),
        })),
        Some(MapiPropertyTag::new(PID_NAME_CONTENT_CLASS_W_TAG).property_id())
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PS_INTERNET_HEADERS_GUID,
            kind: MapiNamedPropertyKind::Name("content-type".to_string()),
        }),
        Some(MapiPropertyTag::new(PID_NAME_CONTENT_TYPE_W_TAG).property_id())
    );
}

#[test]
fn outlook_common_probe_named_properties_have_stable_ids() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_SIDE_EFFECTS),
        }),
        Some(PID_LID_SIDE_EFFECTS as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_COMMON_8514),
        }),
        Some(PID_LID_OUTLOOK_COMMON_8514 as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_COMMON_8578),
        }),
        Some(PID_LID_OUTLOOK_COMMON_8578 as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_COMMON_85B1),
        }),
        Some(PID_LID_OUTLOOK_COMMON_85B1 as u16)
    );
}

#[test]
fn microsoft_oxcdata_reminder_restriction_matches_recurring_calendar_items() {
    const MSGFLAG_SUBMIT: u32 = 0x0000_0004;

    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_APPOINTMENT_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_RECURRING),
        }),
        Some(PID_LID_RECURRING as u16)
    );

    let restriction = MapiRestriction::And(vec![
        MapiRestriction::Not(Box::new(MapiRestriction::And(vec![
            MapiRestriction::Exist {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
            },
            MapiRestriction::Content {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                value: "IPM.Schedule".to_string(),
                fuzzy_level_low: 0x0002,
                fuzzy_level_high: 0,
            },
        ]))),
        MapiRestriction::Bitmask {
            property_tag: PID_TAG_MESSAGE_FLAGS,
            mask: MSGFLAG_SUBMIT,
            must_be_nonzero: false,
        },
        MapiRestriction::Or(vec![
            MapiRestriction::Property {
                relop: 0x04,
                property_tag: PID_LID_REMINDER_SET_TAG,
                value: MapiValue::Bool(true),
            },
            MapiRestriction::And(vec![
                MapiRestriction::Exist {
                    property_tag: PID_LID_RECURRING_TAG,
                },
                MapiRestriction::Property {
                    relop: 0x04,
                    property_tag: PID_LID_RECURRING_TAG,
                    value: MapiValue::Bool(true),
                },
            ]),
        ]),
    ]);

    let mut recurring = default_event_for_mapping(Uuid::nil(), "default");
    recurring.recurrence_rule = "FREQ=DAILY;COUNT=2".to_string();
    assert_eq!(
        event_property_value(&recurring, 1, CALENDAR_FOLDER_ID, PID_LID_RECURRING_TAG),
        Some(MapiValue::Bool(true))
    );
    assert!(restriction_matches(Some(&restriction), |property_tag| {
        event_property_value(&recurring, 1, CALENDAR_FOLDER_ID, property_tag)
    }));

    let one_off = default_event_for_mapping(Uuid::nil(), "default");
    assert_eq!(
        event_property_value(&one_off, 1, CALENDAR_FOLDER_ID, PID_LID_RECURRING_TAG),
        Some(MapiValue::Bool(false))
    );
    assert!(!restriction_matches(Some(&restriction), |property_tag| {
        event_property_value(&one_off, 1, CALENDAR_FOLDER_ID, property_tag)
    }));
}

#[test]
fn microsoft_oxcdata_reminder_restriction_example_parses_and_matches() {
    const MSGFLAG_SUBMIT: u32 = 0x0000_0004;

    let account_id = Uuid::from_u128(0xea339446_27b9_4a9c_b0de_873f03a35376);
    let calendar_parent_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, CALENDAR_FOLDER_ID)
            .expect("calendar entry id");

    fn push_property_restriction(
        restriction: &mut Vec<u8>,
        relop: u8,
        property_tag: u32,
        value: &MapiValue,
    ) {
        restriction.push(MapiRestrictionType::Property as u8);
        restriction.push(relop);
        restriction.extend_from_slice(&property_tag.to_le_bytes());
        restriction.extend_from_slice(&property_tag.to_le_bytes());
        write_mapi_value(restriction, property_tag, value);
    }

    fn push_content_restriction(
        restriction: &mut Vec<u8>,
        property_tag: u32,
        fuzzy_level_low: u16,
        value: &str,
    ) {
        restriction.push(MapiRestrictionType::Content as u8);
        restriction.extend_from_slice(&fuzzy_level_low.to_le_bytes());
        restriction.extend_from_slice(&0u16.to_le_bytes());
        restriction.extend_from_slice(&property_tag.to_le_bytes());
        restriction.extend_from_slice(&property_tag.to_le_bytes());
        write_utf16z(restriction, value);
    }

    fn push_exist_restriction(restriction: &mut Vec<u8>, property_tag: u32) {
        restriction.push(MapiRestrictionType::Exist as u8);
        restriction.extend_from_slice(&property_tag.to_le_bytes());
    }

    let mut restriction = vec![MapiRestrictionType::And as u8];
    restriction.extend_from_slice(&2u16.to_le_bytes());

    restriction.push(MapiRestrictionType::And as u8);
    restriction.extend_from_slice(&8u16.to_le_bytes());
    for folder_id in [
        TRASH_FOLDER_ID,
        JUNK_FOLDER_ID,
        DRAFTS_FOLDER_ID,
        OUTBOX_FOLDER_ID,
        CONFLICTS_FOLDER_ID,
        LOCAL_FAILURES_FOLDER_ID,
        SERVER_FAILURES_FOLDER_ID,
        SYNC_ISSUES_FOLDER_ID,
    ] {
        let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(account_id, folder_id)
            .expect("excluded folder entry id");
        push_property_restriction(
            &mut restriction,
            0x05,
            PID_TAG_PARENT_ENTRY_ID,
            &MapiValue::Binary(entry_id),
        );
    }

    restriction.push(MapiRestrictionType::And as u8);
    restriction.extend_from_slice(&3u16.to_le_bytes());
    restriction.push(MapiRestrictionType::Not as u8);
    restriction.push(MapiRestrictionType::And as u8);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    push_exist_restriction(&mut restriction, PID_TAG_MESSAGE_CLASS_W);
    push_content_restriction(
        &mut restriction,
        PID_TAG_MESSAGE_CLASS_W,
        0x0002,
        "IPM.Schedule",
    );
    restriction.push(MapiRestrictionType::Bitmask as u8);
    restriction.push(0);
    restriction.extend_from_slice(&PID_TAG_MESSAGE_FLAGS.to_le_bytes());
    restriction.extend_from_slice(&MSGFLAG_SUBMIT.to_le_bytes());
    restriction.push(MapiRestrictionType::Or as u8);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    push_property_restriction(
        &mut restriction,
        0x04,
        PID_LID_REMINDER_SET_TAG,
        &MapiValue::Bool(true),
    );
    restriction.push(MapiRestrictionType::And as u8);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    push_exist_restriction(&mut restriction, PID_LID_RECURRING_TAG);
    push_property_restriction(
        &mut restriction,
        0x04,
        PID_LID_RECURRING_TAG,
        &MapiValue::Bool(true),
    );

    let parsed = parse_mapi_restriction(&restriction).unwrap();
    let MapiRestriction::And(top_level) = &parsed else {
        panic!("expected top-level AndRestriction");
    };
    assert_eq!(top_level.len(), 2);
    assert!(matches!(
        &top_level[0],
        MapiRestriction::And(children) if children.len() == 8
    ));
    assert!(matches!(
        &top_level[1],
        MapiRestriction::And(children) if children.len() == 3
    ));

    let mut recurring = default_event_for_mapping(Uuid::nil(), "default");
    recurring.recurrence_rule = "FREQ=DAILY;COUNT=2".to_string();
    assert!(restriction_matches(Some(&parsed), |property_tag| {
        if property_tag == PID_TAG_PARENT_ENTRY_ID {
            Some(MapiValue::Binary(calendar_parent_entry_id.clone()))
        } else {
            event_property_value(&recurring, 1, CALENDAR_FOLDER_ID, property_tag)
        }
    }));

    let one_off = default_event_for_mapping(Uuid::nil(), "default");
    assert!(!restriction_matches(Some(&parsed), |property_tag| {
        if property_tag == PID_TAG_PARENT_ENTRY_ID {
            Some(MapiValue::Binary(calendar_parent_entry_id.clone()))
        } else {
            event_property_value(&one_off, 1, CALENDAR_FOLDER_ID, property_tag)
        }
    }));
}

#[test]
fn rss_feed_messages_project_rss_message_class_and_named_properties() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_POST_RSS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_POST_RSS_ITEM_GUID),
        }),
        Some(PID_LID_POST_RSS_ITEM_GUID as u16)
    );

    let mailbox_id = Uuid::from_u128(0x3333);
    crate::mapi::identity::remember_mapi_identity(
        Uuid::from_u128(0x1111),
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 111,
        ),
    );
    let email = JmapEmail {
        id: Uuid::from_u128(0x1111),
        thread_id: Uuid::from_u128(0x2222),
        mailbox_id,
        mailbox_role: "rss_feeds".to_string(),
        mailbox_name: "RSS Feeds".to_string(),
        modseq: 7,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
            mailbox_id,
            role: "rss_feeds".to_string(),
            name: "RSS Feeds".to_string(),
            modseq: 7,
            unread: false,
            flagged: false,
            followup_flag_status: "none".to_string(),
            followup_icon: 0,
            todo_item_flags: 0,
            followup_request: String::new(),
            followup_start_at: None,
            followup_due_at: None,
            followup_completed_at: None,
            reminder_set: false,
            reminder_at: None,
            reminder_dismissed_at: None,
            swapped_todo_store_id: None,
            swapped_todo_data: None,
            categories: Vec::new(),
            draft: false,
        }],
        received_at: "2026-05-20T10:00:00Z".to_string(),
        sent_at: None,
        from_address: "feed@example.test".to_string(),
        from_display: Some("Feed".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: vec![lpe_storage::JmapEmailAddress {
            address: "hidden@example.test".to_string(),
            display_name: Some("Hidden".to_string()),
        }],
        subject: "RSS item".to_string(),
        preview: "Preview".to_string(),
        body_text: "<item>RSS item</item>".to_string(),
        body_html_sanitized: Some("<p>RSS item</p>".to_string()),
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 128,
        internet_message_id: Some("rss-guid".to_string()),
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };

    assert_eq!(
        email_property_value(&email, PID_TAG_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.Post.RSS".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_ORIGINAL_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.Post.RSS".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_ACCESS_LEVEL),
        Some(MapiValue::U32(1))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_SENDER_ADDRESS_TYPE_W),
        Some(MapiValue::String("SMTP".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_SENDER_SMTP_ADDRESS_W),
        Some(MapiValue::String("feed@example.test".to_string()))
    );
    let mut delegated = email.clone();
    delegated.from_display = Some("Represented User".to_string());
    delegated.from_address = "represented@example.test".to_string();
    delegated.sender_display = Some("Delegate Sender".to_string());
    delegated.sender_address = Some("delegate@example.test".to_string());
    assert_eq!(
        email_property_value(&delegated, PID_TAG_SENDER_NAME_W),
        Some(MapiValue::String("Delegate Sender".to_string()))
    );
    assert_eq!(
        email_property_value(&delegated, PID_TAG_SENDER_EMAIL_ADDRESS_W),
        Some(MapiValue::String("delegate@example.test".to_string()))
    );
    assert_eq!(
        email_property_value(&delegated, PID_TAG_SENT_REPRESENTING_NAME_W),
        Some(MapiValue::String("Represented User".to_string()))
    );
    assert_eq!(
        email_property_value(&delegated, PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W),
        Some(MapiValue::String("represented@example.test".to_string()))
    );
    assert_eq!(
        email_property_value(&delegated, PID_TAG_MESSAGE_SIZE),
        Some(MapiValue::U32(128))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_PRIORITY),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_SENSITIVITY),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_SUBJECT_PREFIX_W),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_POST_RSS_ITEM_GUID_W_TAG),
        Some(MapiValue::String("rss-guid".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_POST_RSS_CHANNEL_W_TAG),
        Some(MapiValue::String("RSS Feeds".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_POST_RSS_ITEM_XML_W_TAG),
        Some(MapiValue::String("<item>RSS item</item>".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_CONVERSATION_TOPIC_W),
        Some(MapiValue::String("RSS item".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_CONVERSATION_INDEX),
        Some(MapiValue::Binary(conversation_index_for_uuid(
            email.thread_id
        )))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_MESSAGE_STATUS),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_SEARCH_KEY),
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &email.id
        )))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_DISPLAY_BCC_W),
        Some(MapiValue::String("Hidden".to_string()))
    );

    let headers = match email_property_value(&email, PID_TAG_TRANSPORT_MESSAGE_HEADERS_W) {
        Some(MapiValue::String(headers)) => headers,
        other => panic!("unexpected transport headers value: {other:?}"),
    };
    assert!(headers.contains("Message-ID: rss-guid"));
    assert!(headers.contains("From: Feed"));
    assert!(headers.contains("Subject: RSS item"));
    assert!(!headers.contains("Bcc:"));
    assert_eq!(
        email_property_value(&email, PID_TAG_BODY_HTML_W),
        Some(MapiValue::String("<p>RSS item</p>".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_HTML_BINARY),
        Some(MapiValue::Binary(b"<p>RSS item</p>".to_vec()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_RTF_IN_SYNC),
        Some(MapiValue::Bool(false))
    );
    let rtf = match email_property_value(&email, PID_TAG_RTF_COMPRESSED) {
        Some(MapiValue::Binary(value)) => value,
        other => panic!("unexpected RTF body value: {other:?}"),
    };
    assert!(rtf.len() > 16);
    assert_eq!(
        u32::from_le_bytes([rtf[0], rtf[1], rtf[2], rtf[3]]) as usize,
        rtf.len() - 4
    );
    assert_eq!(
        u32::from_le_bytes([rtf[4], rtf[5], rtf[6], rtf[7]]) as usize,
        rtf.len() - 16
    );
    assert_eq!(
        u32::from_le_bytes([rtf[8], rtf[9], rtf[10], rtf[11]]),
        0x414C_454D
    );
    assert_eq!(u32::from_le_bytes([rtf[12], rtf[13], rtf[14], rtf[15]]), 0);
    assert!(String::from_utf8_lossy(&rtf[16..]).contains("RSS item"));
    assert_eq!(
        email_property_value(&email, PID_TAG_NATIVE_BODY),
        Some(MapiValue::U32(3))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_INTERNET_CODEPAGE),
        Some(MapiValue::U32(65001))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_MESSAGE_LOCALE_ID),
        Some(MapiValue::U32(0x0409))
    );
}

#[test]
fn followup_mail_projects_outlook_flag_properties() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_FLAG_REQUEST),
        }),
        Some(PID_LID_FLAG_REQUEST as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_TASK_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_PERCENT_COMPLETE),
        }),
        Some(PID_LID_PERCENT_COMPLETE as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_TASK_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_TASK_START_DATE),
        }),
        Some(PID_LID_TASK_START_DATE as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_TASK_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_TASK_DUE_DATE),
        }),
        Some(PID_LID_TASK_DUE_DATE as u16)
    );

    let mailbox_id = Uuid::from_u128(0x4444);
    let store_id = Uuid::from_u128(0x5555);
    let email = JmapEmail {
        id: Uuid::from_u128(0x1111),
        thread_id: Uuid::from_u128(0x2222),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 7,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
            mailbox_id,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            modseq: 7,
            unread: false,
            flagged: true,
            followup_flag_status: "complete".to_string(),
            followup_icon: 6,
            todo_item_flags: 8,
            followup_request: "Follow up".to_string(),
            followup_start_at: Some("2026-05-20T09:00:00Z".to_string()),
            followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
            followup_completed_at: Some("2026-05-20T10:30:00Z".to_string()),
            reminder_set: true,
            reminder_at: Some("2026-05-20T09:30:00Z".to_string()),
            reminder_dismissed_at: None,
            swapped_todo_store_id: Some(store_id),
            swapped_todo_data: Some(vec![1, 2, 3, 4]),
            categories: Vec::new(),
            draft: false,
        }],
        received_at: "2026-05-20T10:00:00Z".to_string(),
        sent_at: None,
        from_address: "alice@example.test".to_string(),
        from_display: Some("Alice".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Flagged item".to_string(),
        preview: "Flagged item".to_string(),
        body_text: "Flagged item".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: true,
        followup_flag_status: "complete".to_string(),
        followup_icon: 6,
        todo_item_flags: 8,
        followup_request: "Follow up".to_string(),
        followup_start_at: Some("2026-05-20T09:00:00Z".to_string()),
        followup_due_at: Some("2026-05-21T17:00:00Z".to_string()),
        followup_completed_at: Some("2026-05-20T10:30:00Z".to_string()),
        reminder_set: true,
        reminder_at: Some("2026-05-20T09:30:00Z".to_string()),
        reminder_dismissed_at: None,
        swapped_todo_store_id: Some(store_id),
        swapped_todo_data: Some(valid_swapped_todo_data()),
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 128,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };

    assert_eq!(
        email_property_value(&email, PID_TAG_FOLDER_ID),
        Some(MapiValue::U64(INBOX_FOLDER_ID))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_PARENT_FOLDER_ID),
        Some(MapiValue::U64(INBOX_FOLDER_ID))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_FLAG_STATUS),
        Some(MapiValue::U32(1))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_FOLLOWUP_ICON),
        Some(MapiValue::I32(6))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_TODO_ITEM_FLAGS),
        Some(MapiValue::I32(8))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_FLAG_REQUEST_W_TAG),
        Some(MapiValue::String("Follow up".to_string()))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_PERCENT_COMPLETE_TAG),
        Some(MapiValue::F64(1.0f64.to_bits()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_FLAG_COMPLETE_TIME),
        Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-05-20T10:30:00Z"
        )))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_TASK_START_DATE_TAG),
        Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-05-20T09:00:00Z"
        )))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_TASK_DUE_DATE_TAG),
        Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-05-21T17:00:00Z"
        )))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_REMINDER_SET_TAG),
        Some(MapiValue::Bool(true))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_REMINDER_TIME_TAG),
        Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-05-20T09:30:00Z"
        )))
    );
    assert_eq!(
        email_property_value(&email, PID_LID_REMINDER_SIGNAL_TIME_TAG),
        Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-05-20T09:30:00Z"
        )))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_SWAPPED_TODO_STORE),
        Some(MapiValue::Binary(store_id.as_bytes().to_vec()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_SWAPPED_TODO_DATA),
        Some(MapiValue::Binary(valid_swapped_todo_data()))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_BODY_HTML_W),
        Some(MapiValue::String(
            "<html><body>Flagged item</body></html>".to_string()
        ))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_HTML_BINARY),
        Some(MapiValue::Binary(
            b"<html><body>Flagged item</body></html>".to_vec()
        ))
    );
    assert_eq!(
        email_property_value(&email, PID_TAG_NATIVE_BODY),
        Some(MapiValue::U32(1))
    );
}

#[test]
fn reminder_named_properties_project_from_canonical_reminder_links() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_REMINDER_SET),
        }),
        Some(PID_LID_REMINDER_SET as u16)
    );
    let rights = lpe_storage::CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: false,
    };
    let event_id = Uuid::from_u128(0x3333);
    let event = lpe_storage::AccessibleEvent {
        id: event_id,
        uid: "event-uid".to_string(),
        collection_id: "default".to_string(),
        owner_account_id: Uuid::nil(),
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights: rights.clone(),
        date: "2026-05-21".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Standup".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let reminder = lpe_storage::ClientReminder {
        source_type: "calendar".to_string(),
        source_id: event_id,
        occurrence_start_at: None,
        title: "Standup".to_string(),
        due_at: Some("2026-05-21T09:30:00Z".to_string()),
        reminder_at: "2026-05-21T08:45:00Z".to_string(),
        dismissed_at: None,
        completed_at: None,
        status: "pending".to_string(),
    };
    assert_eq!(
        event_property_value_with_reminder(
            &event,
            1,
            REMINDERS_FOLDER_ID,
            PID_LID_REMINDER_SET_TAG,
            Some(&reminder)
        ),
        Some(MapiValue::Bool(true))
    );
    assert_eq!(
        event_property_value_with_reminder(
            &event,
            1,
            REMINDERS_FOLDER_ID,
            PID_LID_REMINDER_SIGNAL_TIME_TAG,
            Some(&reminder)
        ),
        Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-05-21T08:45:00Z"
        )))
    );
    assert_eq!(
        event_property_value_with_reminder(
            &event,
            1,
            REMINDERS_FOLDER_ID,
            PID_LID_REMINDER_DELTA_TAG,
            Some(&reminder)
        ),
        Some(MapiValue::I32(15))
    );
    assert_eq!(
        event_property_value_with_reminder(
            &event,
            1,
            REMINDERS_FOLDER_ID,
            PID_LID_REMINDER_OVERRIDE_TAG,
            Some(&reminder)
        ),
        Some(MapiValue::Bool(false))
    );

    let task = lpe_storage::ClientTask {
        id: Uuid::from_u128(0x4444),
        owner_account_id: Uuid::nil(),
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        is_owned: true,
        rights,
        task_list_id: Uuid::nil(),
        task_list_sort_order: 0,
        title: "Follow up".to_string(),
        description: String::new(),
        status: "needs-action".to_string(),
        due_at: Some("2026-05-21T12:00:00Z".to_string()),
        completed_at: None,
        recurrence_rule: String::new(),
        sort_order: 0,
        updated_at: "2026-05-20T09:00:00Z".to_string(),
    };
    let task_reminder = lpe_storage::ClientReminder {
        source_type: "task".to_string(),
        source_id: task.id,
        occurrence_start_at: None,
        title: "Follow up".to_string(),
        due_at: task.due_at.clone(),
        reminder_at: "2026-05-21T11:45:00Z".to_string(),
        dismissed_at: None,
        completed_at: None,
        status: "pending".to_string(),
    };
    assert_eq!(
        task_property_value_with_reminder(
            &task,
            2,
            REMINDERS_FOLDER_ID,
            PID_LID_PERCENT_COMPLETE_TAG,
            Some(&task_reminder)
        ),
        Some(MapiValue::F64(0.0f64.to_bits()))
    );
    assert_eq!(
        task_property_value_with_reminder(
            &task,
            2,
            REMINDERS_FOLDER_ID,
            PID_LID_REMINDER_TIME_TAG,
            Some(&task_reminder)
        ),
        Some(MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-05-21T11:45:00Z"
        )))
    );
    assert_eq!(
        task_property_value_with_reminder(
            &task,
            2,
            REMINDERS_FOLDER_ID,
            PID_LID_REMINDER_DELTA_TAG,
            Some(&task_reminder)
        ),
        Some(MapiValue::I32(15))
    );
    assert_eq!(
        task_property_value_with_reminder(
            &task,
            2,
            REMINDERS_FOLDER_ID,
            PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
            Some(&task_reminder)
        ),
        Some(MapiValue::String(String::new()))
    );
}

#[test]
fn zero_duration_events_project_non_zero_mapi_appointment_window() {
    let event = lpe_storage::AccessibleEvent {
        id: Uuid::from_u128(0x5555),
        uid: "zero-duration".to_string(),
        collection_id: "default".to_string(),
        owner_account_id: Uuid::nil(),
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights: lpe_storage::CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        },
        date: "2026-05-21".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 0,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Zero duration".to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };

    assert!(event_end_filetime(&event) > event_start_filetime(&event));
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_END_DATE),
        Some(MapiValue::I64(event_end_filetime(&event) as i64))
    );
}

#[test]
fn calendar_projection_uses_canonical_all_day_status_and_participants() {
    let event = lpe_storage::AccessibleEvent {
        id: Uuid::from_u128(0x7777),
        uid: "canonical-calendar".to_string(),
        collection_id: "default".to_string(),
        owner_account_id: Uuid::nil(),
        owner_email: "alice@example.test".to_string(),
        owner_display_name: "Alice".to_string(),
        rights: lpe_storage::CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        },
        date: "2026-05-21".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 60,
        all_day: true,
        status: "cancelled".to_string(),
        sequence: 3,
        recurrence_rule: "FREQ=WEEKLY;COUNT=2".to_string(),
        recurrence_json: "{\"frequency\":\"weekly\"}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Canonical appointment".to_string(),
        location: "Room A".to_string(),
        organizer_json: "{\"email\":\"alice@example.test\"}".to_string(),
        attendees: "Bob".to_string(),
        attendees_json: serialize_calendar_participants_metadata(&CalendarParticipantsMetadata {
            organizer: Some(lpe_storage::CalendarOrganizerMetadata {
                email: "alice@example.test".to_string(),
                common_name: "Alice".to_string(),
            }),
            attendees: vec![
                lpe_storage::CalendarParticipantMetadata {
                    email: "bob@example.test".to_string(),
                    common_name: "Bob".to_string(),
                    role: "REQ-PARTICIPANT".to_string(),
                    partstat: "accepted".to_string(),
                    rsvp: false,
                },
                lpe_storage::CalendarParticipantMetadata {
                    email: "cara@example.test".to_string(),
                    common_name: "Cara".to_string(),
                    role: "OPT-PARTICIPANT".to_string(),
                    partstat: "needs-action".to_string(),
                    rsvp: false,
                },
            ],
        }),
        notes: "Body".to_string(),
        body_html: "<p>Body</p>".to_string(),
    };

    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_APPOINTMENT_SUB_TYPE_TAG
        ),
        Some(MapiValue::Bool(true))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_BUSY_STATUS_TAG),
        Some(MapiValue::I32(0))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_COLOR_TAG),
        Some(MapiValue::I32(0))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_SIDE_EFFECTS_TAG),
        Some(MapiValue::I32(CALENDAR_EVENT_SIDE_EFFECTS))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_OUTLOOK_COMMON_8578_TAG
        ),
        Some(MapiValue::I32(0))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_APPOINTMENT_STATE_FLAGS_TAG
        ),
        Some(MapiValue::I32(0x0000_0005))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_COMMON_START_TAG),
        Some(MapiValue::I64(event_start_filetime(&event) as i64))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_COMMON_END_TAG),
        Some(MapiValue::I64(event_end_filetime(&event) as i64))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_LOCATION_W),
        Some(MapiValue::String("Room A".to_string()))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_SENDER_NAME_W),
        Some(MapiValue::String("Alice".to_string()))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_TAG_SENDER_EMAIL_ADDRESS_W
        ),
        Some(MapiValue::String("alice@example.test".to_string()))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_DISPLAY_TO_W),
        Some(MapiValue::String("Bob, Cara".to_string()))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_DISPLAY_CC_W),
        Some(MapiValue::String("Cara".to_string()))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_BODY_HTML_W),
        Some(MapiValue::String("<p>Body</p>".to_string()))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_HTML_BINARY),
        Some(MapiValue::Binary(b"<p>Body</p>".to_vec()))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_ALL_ATTENDEES_STRING_W_TAG
        ),
        Some(MapiValue::String("Bob; Cara".to_string()))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_TO_ATTENDEES_STRING_W_TAG
        ),
        Some(MapiValue::String("Bob".to_string()))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_CC_ATTENDEES_STRING_W_TAG
        ),
        Some(MapiValue::String("Cara".to_string()))
    );
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_LOCATION_W_TAG),
        Some(MapiValue::String("Room A".to_string()))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_APPOINTMENT_DURATION_TAG
        ),
        Some(MapiValue::I32(60))
    );
    assert_eq!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_TIME_ZONE_DESCRIPTION_W_TAG
        ),
        Some(MapiValue::String("UTC".to_string()))
    );
    assert!(matches!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_TIME_ZONE_STRUCT_TAG),
        Some(MapiValue::Binary(value)) if value.len() == 48
    ));
    assert!(matches!(
        event_property_value(
            &event,
            1,
            CALENDAR_FOLDER_ID,
            PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG
        ),
        Some(MapiValue::Binary(value)) if value.starts_with(&[0x02, 0x01]) && value.ends_with(&[0; 16])
    ));
    assert_eq!(
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_TAG_HAS_ATTACHMENTS),
        Some(MapiValue::Bool(false))
    );
}

#[test]
fn calendar_projection_backs_outlook_table_identity_and_status_columns() {
    let event = default_event_for_mapping(Uuid::nil(), "default");
    let item_id = 0x0000_0000_0044_0001;

    assert_eq!(
        event_property_value(&event, item_id, CALENDAR_FOLDER_ID, PID_TAG_FOLDER_ID),
        Some(MapiValue::U64(CALENDAR_FOLDER_ID))
    );
    assert_eq!(
        event_property_value(
            &event,
            item_id,
            CALENDAR_FOLDER_ID,
            PID_TAG_PARENT_FOLDER_ID
        ),
        Some(MapiValue::U64(CALENDAR_FOLDER_ID))
    );
    assert_eq!(
        event_property_value(&event, item_id, CALENDAR_FOLDER_ID, PID_TAG_MID),
        Some(MapiValue::U64(item_id))
    );
    assert_eq!(
        event_property_value(&event, item_id, CALENDAR_FOLDER_ID, PID_TAG_INST_ID),
        Some(MapiValue::U64(item_id))
    );
    assert_eq!(
        event_property_value(&event, item_id, CALENDAR_FOLDER_ID, PID_TAG_INSTANCE_NUM),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        event_property_value(&event, item_id, CALENDAR_FOLDER_ID, PID_TAG_MESSAGE_STATUS),
        Some(MapiValue::U32(0))
    );
}

#[test]
fn mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Updated".to_string()));
    properties.insert(PID_LID_APPOINTMENT_SUB_TYPE_TAG, MapiValue::Bool(true));
    properties.insert(PID_LID_BUSY_STATUS_TAG, MapiValue::I32(1));
    properties.insert(
        PID_TAG_SENDER_NAME_W,
        MapiValue::String("Alice Owner".to_string()),
    );
    properties.insert(
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        MapiValue::String("Alice@Example.Test".to_string()),
    );
    properties.insert(
        PID_TAG_DISPLAY_TO_W,
        MapiValue::String("Bob One".to_string()),
    );
    properties.insert(
        PID_TAG_DISPLAY_CC_W,
        MapiValue::String("Cara Two".to_string()),
    );
    properties.insert(
        PID_TAG_BODY_HTML_W,
        MapiValue::String("<p>Updated</p>".to_string()),
    );
    properties.insert(
        PID_LID_LOCATION_W_TAG,
        MapiValue::String("Room B".to_string()),
    );
    properties.insert(
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
        MapiValue::String("W. Europe Standard Time".to_string()),
    );
    properties.insert(
        PID_TAG_START_DATE,
        MapiValue::I64(date_time_to_filetime("2026-05-22", "10:00") as i64),
    );
    properties.insert(PID_LID_APPOINTMENT_DURATION_TAG, MapiValue::I32(45));

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x8888)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.title, "Updated");
    assert!(input.all_day);
    assert_eq!(input.body_html, "<p>Updated</p>");
    assert_eq!(input.date, "2026-05-22");
    assert_eq!(input.time, "10:00");
    assert_eq!(input.duration_minutes, 45);
    assert_eq!(input.location, "Room B");
    assert_eq!(input.time_zone, "W. Europe Standard Time");
    assert_eq!(input.status, "tentative");
    assert_eq!(input.recurrence_rule, existing.recurrence_rule);
    assert_eq!(input.attendees, "Bob One, Cara Two");
    assert!(input.organizer_json.contains("alice@example.test"));
    assert!(input.attendees_json.contains("Bob One"));
    assert!(input.attendees_json.contains("OPT-PARTICIPANT"));
}

#[test]
fn mapi_over_http_calendar_binary_payloads_fail_explicitly() {
    let mut properties = HashMap::new();
    properties.insert(0x8200_0102, MapiValue::Binary(vec![1, 2, 3]));

    let error = reject_unsupported_mapi_event_properties(&properties).unwrap_err();

    assert!(error
        .to_string()
        .contains("MAPI binary calendar recurrence or meeting payloads are not supported"));
}

#[test]
fn mapi_over_http_calendar_state_flags_map_bounded_cancel_state() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(PID_LID_APPOINTMENT_STATE_FLAGS_TAG, MapiValue::I32(0x5));

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x8888)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.status, "cancelled");

    properties.insert(PID_LID_APPOINTMENT_STATE_FLAGS_TAG, MapiValue::I32(0x8));
    let error = reject_unsupported_mapi_event_properties(&properties).unwrap_err();
    assert!(error
        .to_string()
        .contains("unsupported MAPI appointment state flags"));
}

#[test]
fn mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(
        PID_LID_APPOINTMENT_START_WHOLE_TAG,
        MapiValue::I64(date_time_to_filetime("2026-06-01", "13:15") as i64),
    );
    properties.insert(
        PID_LID_APPOINTMENT_END_WHOLE_TAG,
        MapiValue::I64(date_time_to_filetime("2026-06-01", "14:45") as i64),
    );

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x8888)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.date, "2026-06-01");
    assert_eq!(input.time, "13:15");
    assert_eq!(input.duration_minutes, 90);
}

#[test]
fn mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(
        PID_LID_COMMON_START_TAG,
        MapiValue::I64(date_time_to_filetime("2026-06-02", "08:00") as i64),
    );
    properties.insert(
        PID_LID_COMMON_END_TAG,
        MapiValue::I64(date_time_to_filetime("2026-06-02", "09:30") as i64),
    );

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x8888)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.date, "2026-06-02");
    assert_eq!(input.time, "08:00");
    assert_eq!(input.duration_minutes, 90);
}

#[test]
fn mapi_over_http_calendar_meeting_classes_fail_explicitly() {
    for message_class in [
        "IPM.Schedule.Meeting.Resp.Pos",
        "IPM.Schedule.Meeting.Canceled",
        "IPM.Note",
    ] {
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String(message_class.to_string()),
        );

        let error = reject_unsupported_mapi_event_properties(&properties).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("is not mapped to canonical calendar state"),
            "{message_class}"
        );
    }

    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_MESSAGE_CLASS_W,
        MapiValue::String("IPM.Appointment".to_string()),
    );

    reject_unsupported_mapi_event_properties(&properties).unwrap();

    properties.insert(
        PID_TAG_MESSAGE_CLASS_W,
        MapiValue::String("IPM.Schedule.Meeting.Request".to_string()),
    );

    reject_unsupported_mapi_event_properties(&properties).unwrap();
}

#[test]
fn mapi_over_http_calendar_meeting_response_classes_map_to_partstat() {
    let mut existing = default_event_for_mapping(Uuid::nil(), "default");
    existing.attendees = "Bob".to_string();
    existing.attendees_json = r#"{"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}]}"#.to_string();

    for (message_class, expected_partstat) in [
        ("IPM.Schedule.Meeting.Resp.Pos", "accepted"),
        ("IPM.Schedule.Meeting.Resp.Tent", "tentative"),
        ("IPM.Schedule.Meeting.Resp.Neg", "declined"),
    ] {
        let mut properties = HashMap::new();
        properties.insert(
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String(message_class.to_string()),
        );
        properties.insert(
            PID_TAG_SENDER_EMAIL_ADDRESS_W,
            MapiValue::String("bob@example.test".to_string()),
        );
        properties.insert(PID_TAG_SENDER_NAME_W, MapiValue::String("Bob".to_string()));

        let input = meeting_response_event_input_from_mapi(
            Uuid::nil(),
            Some(existing.id),
            &existing,
            &properties,
        )
        .unwrap()
        .expect("meeting response should map");

        assert!(input
            .attendees_json
            .contains(&format!(r#""partstat":"{expected_partstat}""#)));
    }
}

#[test]
fn mapi_over_http_calendar_recurrence_maps_month_end_rule() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut month_end = Vec::new();
    append_recur_header(&mut month_end, 0x200C, 0x0004, 1);
    month_end.extend_from_slice(&31u32.to_le_bytes());
    append_recur_tail(
        &mut month_end,
        0x0000_2022,
        3,
        &[],
        &[],
        "2026-05-31",
        "2026-07-31",
    );
    append_appointment_recur_suffix(&mut month_end, 9 * 60, 10 * 60, 0);
    let mut properties = HashMap::new();
    properties.insert(PID_LID_APPOINTMENT_RECUR_TAG, MapiValue::Binary(month_end));

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x999E)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.recurrence_rule, "FREQ=MONTHLY;COUNT=3;BYMONTHDAY=31");
    assert_eq!(
        input.recurrence_json,
        r#"{"frequency":"monthly","count":3,"byMonthDay":31}"#
    );
}

#[test]
fn mapi_over_http_calendar_recurrence_rejects_unsupported_shapes() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let modified_exception = test_weekly_recur_blob_with_modified_instance(0x0004, "", "");
    let mut properties = HashMap::new();
    properties.insert(
        PID_LID_APPOINTMENT_RECUR_TAG,
        MapiValue::Binary(modified_exception),
    );

    let error = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x999F)),
        &existing,
        &properties,
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("unsupported MAPI calendar recurrence exception override"));
}

#[test]
fn mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(0x8216_0102, MapiValue::Binary(test_daily_recur_blob(2, 3)));

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x9999)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.recurrence_rule, "FREQ=DAILY;INTERVAL=2;COUNT=3");
    assert_eq!(
        input.recurrence_json,
        r#"{"frequency":"daily","interval":2,"count":3}"#
    );
    assert_eq!(input.recurrence_exceptions_json, "[]");
}

#[test]
fn mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut monthly_properties = HashMap::new();
    monthly_properties.insert(
        0x8216_0102,
        MapiValue::Binary(test_monthly_recur_blob(2, 12)),
    );

    let monthly = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x999B)),
        &existing,
        &monthly_properties,
    )
    .unwrap();

    assert_eq!(
        monthly.recurrence_rule,
        "FREQ=MONTHLY;INTERVAL=2;COUNT=5;BYMONTHDAY=12"
    );
    assert_eq!(
        monthly.recurrence_json,
        r#"{"frequency":"monthly","interval":2,"count":5,"byMonthDay":12}"#
    );

    let mut monthly_nth_properties = HashMap::new();
    monthly_nth_properties.insert(
        0x8216_0102,
        MapiValue::Binary(test_monthly_nth_recur_blob()),
    );

    let monthly_nth = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x999D)),
        &existing,
        &monthly_nth_properties,
    )
    .unwrap();

    assert_eq!(
        monthly_nth.recurrence_rule,
        "FREQ=MONTHLY;COUNT=3;BYDAY=TU,TH;BYSETPOS=2"
    );
    assert_eq!(
        monthly_nth.recurrence_json,
        r#"{"frequency":"monthly","count":3,"byDay":["TU","TH"],"bySetPosition":2}"#
    );

    let mut yearly_properties = HashMap::new();
    yearly_properties.insert(0x8216_0102, MapiValue::Binary(test_yearly_recur_blob()));

    let yearly = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x999C)),
        &existing,
        &yearly_properties,
    )
    .unwrap();

    assert_eq!(
        yearly.recurrence_rule,
        "FREQ=YEARLY;COUNT=2;BYDAY=FR;BYMONTH=5;BYSETPOS=-1"
    );
    assert_eq!(
        yearly.recurrence_json,
        r#"{"frequency":"yearly","count":2,"byDay":["FR"],"byMonth":5,"bySetPosition":-1}"#
    );
}

#[test]
fn mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(
        0x8216_0102,
        MapiValue::Binary(test_weekly_recur_blob_with_deleted_instance()),
    );

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x999A)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.recurrence_rule, "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE");
    assert_eq!(
        input.recurrence_json,
        r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#
    );
    assert_eq!(
        input.recurrence_exceptions_json,
        r#"[{"recurrenceId":"2026-05-25","excluded":true}]"#
    );
}

#[test]
fn mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(
        0x8216_0102,
        MapiValue::Binary(test_weekly_recur_blob_with_modified_instance(0, "", "")),
    );

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x9990)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(input.recurrence_rule, "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE");
    assert_eq!(
        input.recurrence_exceptions_json,
        r#"[{"end":"2026-05-25T11:30:00","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00"}]"#
    );
}

#[test]
fn mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(
        0x8216_0102,
        MapiValue::Binary(test_weekly_recur_blob_with_modified_instance(
            0x0011,
            "Changed subject",
            "Room B",
        )),
    );

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x9991)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(
        input.recurrence_exceptions_json,
        r#"[{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
    );
}

#[test]
fn mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances() {
    let existing = default_event_for_mapping(Uuid::nil(), "default");
    let mut properties = HashMap::new();
    properties.insert(
        0x8216_0102,
        MapiValue::Binary(test_weekly_recur_blob_with_deleted_and_modified_instances()),
    );

    let input = event_input_from_mapi(
        Uuid::nil(),
        Some(Uuid::from_u128(0x9992)),
        &existing,
        &properties,
    )
    .unwrap();

    assert_eq!(
        input.recurrence_exceptions_json,
        r#"[{"recurrenceId":"2026-05-27","excluded":true},{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
    );
}

#[test]
fn mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary() {
    let mut event = default_event_for_mapping(Uuid::nil(), "default");
    event.date = "2026-05-18".to_string();
    event.time = "09:00".to_string();
    event.duration_minutes = 60;
    event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
    event.recurrence_json = r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
    event.recurrence_exceptions_json =
        r#"[{"recurrenceId":"2026-05-25","excluded":true}]"#.to_string();

    let Some(MapiValue::Binary(value)) =
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
    else {
        panic!("expected recurrence binary projection");
    };
    let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

    assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
    assert_eq!(recurrence.recurrence_json, event.recurrence_json);
    assert_eq!(
        recurrence.recurrence_exceptions_json,
        event.recurrence_exceptions_json
    );
}

#[test]
fn mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary() {
    let mut event = default_event_for_mapping(Uuid::nil(), "default");
    event.date = "2026-05-18".to_string();
    event.time = "09:00".to_string();
    event.duration_minutes = 60;
    event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
    event.recurrence_json = r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
    event.recurrence_exceptions_json =
            r#"[{"recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","end":"2026-05-25T11:30:00"}]"#.to_string();

    let Some(MapiValue::Binary(value)) =
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
    else {
        panic!("expected recurrence binary projection");
    };
    let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

    assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&recurrence.recurrence_exceptions_json).unwrap(),
        serde_json::from_str::<serde_json::Value>(&event.recurrence_exceptions_json).unwrap()
    );
}

#[test]
fn mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary() {
    let mut event = default_event_for_mapping(Uuid::nil(), "default");
    event.date = "2026-05-18".to_string();
    event.time = "09:00".to_string();
    event.duration_minutes = 60;
    event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
    event.recurrence_json = r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
    event.recurrence_exceptions_json =
            r#"[{"recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","end":"2026-05-25T11:30:00","title":"Changed subject","location":"Room B"}]"#.to_string();

    let Some(MapiValue::Binary(value)) =
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
    else {
        panic!("expected recurrence binary projection");
    };
    let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

    assert_eq!(
        recurrence.recurrence_exceptions_json,
        r#"[{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
    );
}

#[test]
fn mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary() {
    let mut event = default_event_for_mapping(Uuid::nil(), "default");
    event.date = "2026-05-18".to_string();
    event.time = "09:00".to_string();
    event.duration_minutes = 60;
    event.recurrence_rule = "FREQ=WEEKLY;COUNT=4;BYDAY=MO,WE".to_string();
    event.recurrence_json = r#"{"frequency":"weekly","count":4,"byDay":["MO","WE"]}"#.to_string();
    event.recurrence_exceptions_json =
            r#"[{"recurrenceId":"2026-05-27","excluded":true},{"recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","end":"2026-05-25T11:30:00","title":"Changed subject","location":"Room B"}]"#.to_string();

    let Some(MapiValue::Binary(value)) =
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
    else {
        panic!("expected recurrence binary projection");
    };
    let recurrence = appointment_recurrence_from_mapi(&value).unwrap();

    assert_eq!(
        recurrence.recurrence_exceptions_json,
        r#"[{"recurrenceId":"2026-05-27","excluded":true},{"end":"2026-05-25T11:30:00","location":"Room B","recurrenceId":"2026-05-25","start":"2026-05-25T11:00:00","title":"Changed subject"}]"#
    );
}

#[test]
fn mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary() {
    let mut event = default_event_for_mapping(Uuid::nil(), "default");
    event.date = "2026-05-31".to_string();
    event.time = "09:00".to_string();
    event.duration_minutes = 60;
    event.recurrence_rule = "FREQ=MONTHLY;COUNT=3;BYMONTHDAY=31".to_string();
    event.recurrence_json = r#"{"frequency":"monthly","count":3,"byMonthDay":31}"#.to_string();
    event.recurrence_exceptions_json = "[]".to_string();

    let Some(MapiValue::Binary(value)) =
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
    else {
        panic!("expected recurrence binary projection");
    };

    assert_eq!(u16::from_le_bytes([value[4], value[5]]), 0x200C);
    assert_eq!(u16::from_le_bytes([value[6], value[7]]), 0x0004);
    let recurrence = appointment_recurrence_from_mapi(&value).unwrap();
    assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
    assert_eq!(recurrence.recurrence_json, event.recurrence_json);
}

#[test]
fn mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month() {
    let mut event = default_event_for_mapping(Uuid::nil(), "default");
    event.date = "2026-01-14".to_string();
    event.time = "09:00".to_string();
    event.duration_minutes = 60;
    event.recurrence_rule = "FREQ=YEARLY;COUNT=2;BYMONTHDAY=14;BYMONTH=7".to_string();
    event.recurrence_json =
        r#"{"frequency":"yearly","count":2,"byMonthDay":14,"byMonth":7}"#.to_string();
    event.recurrence_exceptions_json = "[]".to_string();

    let Some(MapiValue::Binary(value)) =
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
    else {
        panic!("expected recurrence binary projection");
    };

    let first_date_time = u32::from_le_bytes(value[10..14].try_into().unwrap());
    assert_eq!(
        recurrence_date_string(first_date_time).unwrap(),
        "2026-07-14"
    );
    let recurrence = appointment_recurrence_from_mapi(&value).unwrap();
    assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
    assert_eq!(recurrence.recurrence_json, event.recurrence_json);
}

#[test]
fn mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month() {
    let mut event = default_event_for_mapping(Uuid::nil(), "default");
    event.date = "2026-01-09".to_string();
    event.time = "09:00".to_string();
    event.duration_minutes = 60;
    event.recurrence_rule = "FREQ=YEARLY;COUNT=3;BYDAY=FR;BYMONTH=10;BYSETPOS=2".to_string();
    event.recurrence_json =
        r#"{"frequency":"yearly","count":3,"byDay":["FR"],"byMonth":10,"bySetPosition":2}"#
            .to_string();
    event.recurrence_exceptions_json = "[]".to_string();

    let Some(MapiValue::Binary(value)) =
        event_property_value(&event, 1, CALENDAR_FOLDER_ID, PID_LID_APPOINTMENT_RECUR_TAG)
    else {
        panic!("expected recurrence binary projection");
    };

    assert_eq!(u16::from_le_bytes([value[4], value[5]]), 0x200D);
    assert_eq!(u16::from_le_bytes([value[6], value[7]]), 0x0003);
    let first_date_time = u32::from_le_bytes(value[10..14].try_into().unwrap());
    assert_eq!(
        recurrence_date_string(first_date_time).unwrap(),
        "2026-10-09"
    );
    let recurrence = appointment_recurrence_from_mapi(&value).unwrap();
    assert_eq!(recurrence.recurrence_rule, event.recurrence_rule);
    assert_eq!(recurrence.recurrence_json, event.recurrence_json);
}

fn test_daily_recur_blob(interval_days: u32, count: u32) -> Vec<u8> {
    let mut value = Vec::new();
    append_recur_header(&mut value, 0x200A, 0x0000, interval_days * 1440);
    append_recur_tail(
        &mut value,
        0x0000_2022,
        count,
        &[],
        &[],
        "2026-05-21",
        "2026-05-25",
    );
    append_appointment_recur_suffix(&mut value, 9 * 60, 10 * 60, 0);
    value
}

fn test_weekly_recur_blob_with_deleted_instance() -> Vec<u8> {
    let mut value = Vec::new();
    append_recur_header(&mut value, 0x200B, 0x0001, 1);
    value.extend_from_slice(&0x0000_000Au32.to_le_bytes());
    append_recur_tail(
        &mut value,
        0x0000_2022,
        4,
        &[recurrence_minutes_since_1601("2026-05-25")],
        &[],
        "2026-05-18",
        "2026-06-08",
    );
    append_appointment_recur_suffix(&mut value, 9 * 60, 10 * 60, 0);
    value
}

fn test_weekly_recur_blob_with_modified_instance(
    override_flags: u16,
    subject: &str,
    location: &str,
) -> Vec<u8> {
    let original = recurrence_minutes_since_1601("2026-05-25");
    let start = original + 11 * 60;
    let end = original + 11 * 60 + 30;
    let mut value = Vec::new();
    append_recur_header(&mut value, 0x200B, 0x0001, 1);
    value.extend_from_slice(&0x0000_000Au32.to_le_bytes());
    append_recur_tail(
        &mut value,
        0x0000_2022,
        4,
        &[original],
        &[original],
        "2026-05-18",
        "2026-06-08",
    );
    value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
    value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
    value.extend_from_slice(&(9u32 * 60).to_le_bytes());
    value.extend_from_slice(&(10u32 * 60).to_le_bytes());
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&start.to_le_bytes());
    value.extend_from_slice(&end.to_le_bytes());
    value.extend_from_slice(&original.to_le_bytes());
    value.extend_from_slice(&override_flags.to_le_bytes());
    if override_flags & 0x0001 != 0 {
        value.extend_from_slice(&((subject.len() + 1) as u16).to_le_bytes());
        value.extend_from_slice(&(subject.len() as u16).to_le_bytes());
        value.extend_from_slice(subject.as_bytes());
    }
    if override_flags & 0x0010 != 0 {
        value.extend_from_slice(&((location.len() + 1) as u16).to_le_bytes());
        value.extend_from_slice(&(location.len() as u16).to_le_bytes());
        value.extend_from_slice(location.as_bytes());
    }
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&4u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    if override_flags & 0x0011 != 0 {
        value.extend_from_slice(&start.to_le_bytes());
        value.extend_from_slice(&end.to_le_bytes());
        value.extend_from_slice(&original.to_le_bytes());
        if override_flags & 0x0001 != 0 {
            append_recur_wide_string(&mut value, subject);
        }
        if override_flags & 0x0010 != 0 {
            append_recur_wide_string(&mut value, location);
        }
        value.extend_from_slice(&0u32.to_le_bytes());
    }
    value.extend_from_slice(&0u32.to_le_bytes());
    value
}

fn test_weekly_recur_blob_with_deleted_and_modified_instances() -> Vec<u8> {
    let deleted_only = recurrence_minutes_since_1601("2026-05-27");
    let modified = recurrence_minutes_since_1601("2026-05-25");
    let start = modified + 11 * 60;
    let end = modified + 11 * 60 + 30;
    let subject = "Changed subject";
    let location = "Room B";
    let override_flags = 0x0011u16;
    let mut value = Vec::new();
    append_recur_header(&mut value, 0x200B, 0x0001, 1);
    value.extend_from_slice(&0x0000_000Au32.to_le_bytes());
    append_recur_tail(
        &mut value,
        0x0000_2022,
        4,
        &[deleted_only, modified],
        &[modified],
        "2026-05-18",
        "2026-06-08",
    );
    value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
    value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
    value.extend_from_slice(&(9u32 * 60).to_le_bytes());
    value.extend_from_slice(&(10u32 * 60).to_le_bytes());
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&start.to_le_bytes());
    value.extend_from_slice(&end.to_le_bytes());
    value.extend_from_slice(&modified.to_le_bytes());
    value.extend_from_slice(&override_flags.to_le_bytes());
    append_recur_ansi_string(&mut value, subject);
    append_recur_ansi_string(&mut value, location);
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&4u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&start.to_le_bytes());
    value.extend_from_slice(&end.to_le_bytes());
    value.extend_from_slice(&modified.to_le_bytes());
    append_recur_wide_string(&mut value, subject);
    append_recur_wide_string(&mut value, location);
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value
}

fn test_monthly_recur_blob(interval_months: u32, day: u32) -> Vec<u8> {
    let mut value = Vec::new();
    append_recur_header(&mut value, 0x200C, 0x0002, interval_months);
    value.extend_from_slice(&day.to_le_bytes());
    append_recur_tail(
        &mut value,
        0x0000_2022,
        5,
        &[],
        &[],
        "2026-05-12",
        "2027-01-12",
    );
    append_appointment_recur_suffix(&mut value, 8 * 60, 9 * 60, 0);
    value
}

fn test_yearly_recur_blob() -> Vec<u8> {
    let mut value = Vec::new();
    append_recur_header(&mut value, 0x200D, 0x0003, 12);
    value.extend_from_slice(&0x0000_0020u32.to_le_bytes());
    value.extend_from_slice(&5u32.to_le_bytes());
    append_recur_tail(
        &mut value,
        0x0000_2022,
        2,
        &[],
        &[],
        "2026-05-29",
        "2027-05-28",
    );
    append_appointment_recur_suffix(&mut value, 13 * 60, 14 * 60, 0);
    value
}

fn test_monthly_nth_recur_blob() -> Vec<u8> {
    let mut value = Vec::new();
    append_recur_header(&mut value, 0x200C, 0x0003, 1);
    value.extend_from_slice(&0x0000_0014u32.to_le_bytes());
    value.extend_from_slice(&2u32.to_le_bytes());
    append_recur_tail(
        &mut value,
        0x0000_2022,
        3,
        &[],
        &[],
        "2026-05-12",
        "2026-07-14",
    );
    append_appointment_recur_suffix(&mut value, 10 * 60, 11 * 60, 0);
    value
}

fn append_recur_header(value: &mut Vec<u8>, frequency: u16, pattern_type: u16, period: u32) {
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&frequency.to_le_bytes());
    value.extend_from_slice(&pattern_type.to_le_bytes());
    value.extend_from_slice(&0x0000u16.to_le_bytes());
    value.extend_from_slice(&recurrence_minutes_since_1601("2026-05-01").to_le_bytes());
    value.extend_from_slice(&period.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
}

fn append_recur_tail(
    value: &mut Vec<u8>,
    end_type: u32,
    count: u32,
    deleted: &[u32],
    modified: &[u32],
    start_date: &str,
    end_date: &str,
) {
    value.extend_from_slice(&end_type.to_le_bytes());
    value.extend_from_slice(&count.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&(deleted.len() as u32).to_le_bytes());
    for date in deleted {
        value.extend_from_slice(&date.to_le_bytes());
    }
    value.extend_from_slice(&(modified.len() as u32).to_le_bytes());
    for date in modified {
        value.extend_from_slice(&date.to_le_bytes());
    }
    value.extend_from_slice(&recurrence_minutes_since_1601(start_date).to_le_bytes());
    value.extend_from_slice(&recurrence_minutes_since_1601(end_date).to_le_bytes());
}

fn append_appointment_recur_suffix(value: &mut Vec<u8>, start: u32, end: u32, exceptions: u16) {
    value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
    value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
    value.extend_from_slice(&start.to_le_bytes());
    value.extend_from_slice(&end.to_le_bytes());
    value.extend_from_slice(&exceptions.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
}

#[test]
fn unsupported_property_types_fail_explicitly() {
    let result = parse_mapi_property_value(&mut Cursor::new(&[]), 0x0037_000D);

    assert!(result.is_err());
}

#[test]
fn logon_projects_valid_server_icon_payloads() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };

    for tag in [PID_TAG_SERVER_CONNECTED_ICON, PID_TAG_SERVER_ACCOUNT_ICON] {
        assert_eq!(
            logon_property_value(&principal, tag),
            Some(MapiValue::Binary(OUTLOOK_STORE_ICON_ICO.to_vec()))
        );
    }
}

#[test]
fn logon_projects_private_mailbox_store_flag() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };

    assert_eq!(
        logon_property_value(&principal, PID_TAG_PRIVATE),
        Some(MapiValue::Bool(true))
    );
}

#[test]
fn navigation_shortcut_group_header_and_link_properties_round_trip_group_identity() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let group_id = Uuid::from_bytes([0x33; 16]);
    let header = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(900),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x1111),
        subject: "Projects".to_string(),
        target_folder_id: None,
        shortcut_type: 4,
        flags: 0,
        save_stamp: 0,
        section: 3,
        ordinal: 0x80,
        group_header_id: Some(group_id),
        group_name: "Projects".to_string(),
    };
    let link = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(901),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x2222),
        subject: "Project Inbox".to_string(),
        target_folder_id: Some(INBOX_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0,
        section: 3,
        ordinal: 0x81,
        group_header_id: Some(group_id),
        group_name: "Projects".to_string(),
    };

    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_GROUP_HEADER_ID),
        Some(MapiValue::Guid([0x33; 16]))
    );
    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_GROUP_CLSID),
        Some(MapiValue::Guid([0x33; 16]))
    );
    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_GROUP_NAME_W),
        Some(MapiValue::String("Projects".to_string()))
    );
    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_ENTRY_ID),
        None
    );
    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_RECORD_KEY),
        None
    );
    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_STORE_ENTRY_ID),
        None
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_GROUP_CLSID),
        Some(MapiValue::Guid([0x33; 16]))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_GROUP_HEADER_ID),
        Some(MapiValue::Guid([0x33; 16]))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_GROUP_NAME_W),
        Some(MapiValue::String("Projects".to_string()))
    );
    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_SAVE_STAMP),
        Some(MapiValue::U32(0x3333_3333))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_SAVE_STAMP),
        Some(MapiValue::U32(0x3333_3333))
    );
}

#[test]
fn microsoft_navigation_shortcut_example_preserves_wlink_properties() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let group_id = Uuid::parse_str("5ba943d8-daaa-462c-a63e-9136f65c8681").unwrap();
    let header = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(920),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x9200),
        subject: "My Work Calendars".to_string(),
        target_folder_id: None,
        shortcut_type: 4,
        flags: 0,
        save_stamp: 0x1234_5678,
        section: 3,
        ordinal: 0x80,
        group_header_id: Some(group_id),
        group_name: "My Work Calendars".to_string(),
    };
    let link = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(921),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x9210),
        subject: "Meetings".to_string(),
        target_folder_id: Some(CALENDAR_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0x1234_5678,
        section: 3,
        ordinal: 0x80,
        group_header_id: Some(group_id),
        group_name: "My Work Calendars".to_string(),
    };
    let calendar_folder_type = [
        0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];

    for shortcut in [&header, &link] {
        assert_eq!(
            navigation_shortcut_property_value(shortcut, account_id, PID_TAG_WLINK_SAVE_STAMP),
            Some(MapiValue::U32(0x1234_5678))
        );
        assert_eq!(
            navigation_shortcut_property_value(shortcut, account_id, PID_TAG_WLINK_SECTION),
            Some(MapiValue::U32(3))
        );
        assert_eq!(
            navigation_shortcut_property_value(shortcut, account_id, PID_TAG_WLINK_ORDINAL),
            Some(MapiValue::Binary(vec![0x80]))
        );
        assert_eq!(
            navigation_shortcut_property_value(shortcut, account_id, PID_TAG_WLINK_GROUP_NAME_W),
            Some(MapiValue::String("My Work Calendars".to_string()))
        );
        assert_eq!(
            navigation_shortcut_property_value(shortcut, account_id, PID_TAG_WLINK_GROUP_CLSID),
            Some(MapiValue::Guid(*group_id.as_bytes()))
        );
    }
    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, PID_TAG_WLINK_ENTRY_ID),
        None
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_FOLDER_TYPE),
        Some(MapiValue::Guid(calendar_folder_type))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_CALENDAR_COLOR),
        Some(MapiValue::I32(-1))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_ADDRESS_BOOK_EID),
        Some(MapiValue::Binary(navigation_shortcut_owner_entry_id(
            account_id
        )))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,),
        Some(MapiValue::Binary(mapi_mailstore::private_store_entry_id(
            account_id
        )))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_CLIENT_ID),
        Some(MapiValue::Binary(0x1234_5678u32.to_le_bytes().to_vec()))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_RO_GROUP_TYPE),
        Some(MapiValue::I32(-1))
    );

    let parsed_link = navigation_shortcut_from_mapi_properties(
        account_id,
        None,
        &HashMap::from([
            (
                PID_TAG_WLINK_ENTRY_ID,
                MapiValue::Binary(
                    crate::mapi::identity::folder_entry_id_from_object_id(
                        account_id,
                        CALENDAR_FOLDER_ID,
                    )
                    .unwrap(),
                ),
            ),
            (PID_TAG_SUBJECT_W, MapiValue::String("Meetings".to_string())),
            (PID_TAG_WLINK_TYPE, MapiValue::U32(0)),
            (PID_TAG_WLINK_FLAGS, MapiValue::U32(0)),
            (PID_TAG_WLINK_SAVE_STAMP, MapiValue::U32(0x1234_5678)),
            (PID_TAG_WLINK_SECTION, MapiValue::U32(3)),
            (PID_TAG_WLINK_ORDINAL, MapiValue::Binary(vec![0x80])),
            (
                PID_TAG_WLINK_GROUP_CLSID,
                MapiValue::Guid(*group_id.as_bytes()),
            ),
            (
                PID_TAG_WLINK_GROUP_NAME_W,
                MapiValue::String("My Work Calendars".to_string()),
            ),
        ]),
    );

    assert_eq!(parsed_link.subject, "Meetings");
    assert_eq!(parsed_link.target_folder_id, Some(CALENDAR_FOLDER_ID));
    assert_eq!(parsed_link.shortcut_type, 0);
    assert_eq!(parsed_link.flags, 0);
    assert_eq!(parsed_link.save_stamp, 0x1234_5678);
    assert_eq!(parsed_link.section, 3);
    assert_eq!(parsed_link.ordinal, 0x80);
    assert_eq!(parsed_link.group_header_id, Some(group_id));
    assert_eq!(parsed_link.group_name, "My Work Calendars");
}

#[test]
fn navigation_shortcut_projects_associated_table_identity_columns() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(901),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x2222),
        subject: "Project Inbox".to_string(),
        target_folder_id: Some(INBOX_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0,
        section: 3,
        ordinal: 0x81,
        group_header_id: None,
        group_name: "Projects".to_string(),
    };

    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_FOLDER_ID),
        Some(MapiValue::U64(COMMON_VIEWS_FOLDER_ID))
    );
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_INST_ID),
        Some(MapiValue::U64(shortcut.id))
    );
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_INSTANCE_NUM),
        Some(MapiValue::U32(0))
    );
    let expected_entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        account_id,
        COMMON_VIEWS_FOLDER_ID,
        shortcut.id,
    )
    .unwrap();
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_ENTRY_ID),
        Some(MapiValue::Binary(expected_entry_id))
    );
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_INSTANCE_KEY),
        Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(shortcut.id)
        ))
    );
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_RECORD_KEY),
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            shortcut.id
        )))
    );
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_PARENT_SOURCE_KEY),
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            COMMON_VIEWS_FOLDER_ID
        )))
    );
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_PARENT_ENTRY_ID),
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, COMMON_VIEWS_FOLDER_ID)
            .map(MapiValue::Binary)
    );
    assert_eq!(
        navigation_shortcut_property_value(&shortcut, account_id, PID_TAG_MESSAGE_FLAGS),
        Some(MapiValue::U32(MSGFLAG_FAI))
    );
}

fn descriptor_column_property_tags(descriptor: &[u8]) -> Vec<u32> {
    view_descriptor_property_tags(descriptor)
}

#[derive(Debug, PartialEq, Eq)]
struct TestViewColumnPacket {
    property_type: u16,
    property_id: u16,
    width: u32,
    flags: u32,
    kind: u32,
    id: u32,
    guid: Option<[u8; 16]>,
    name: Option<String>,
}

fn descriptor_column_packets(descriptor: &[u8]) -> Vec<TestViewColumnPacket> {
    let Some(column_count) = descriptor
        .get(20..24)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .and_then(|count| usize::try_from(count).ok())
    else {
        return Vec::new();
    };

    let mut offset = 60usize;
    let mut packets = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let Some(packet) = descriptor.get(offset..offset + 36) else {
            break;
        };
        let property_type = u16::from_le_bytes([packet[0], packet[1]]);
        let property_id = u16::from_le_bytes([packet[2], packet[3]]);
        let width = u32::from_le_bytes([packet[4], packet[5], packet[6], packet[7]]);
        let flags = u32::from_le_bytes([packet[12], packet[13], packet[14], packet[15]]);
        let kind = u32::from_le_bytes([packet[28], packet[29], packet[30], packet[31]]);
        let id = u32::from_le_bytes([packet[32], packet[33], packet[34], packet[35]]);
        offset += 36;

        let mut guid = None;
        let mut name = None;
        if flags & 0x0000_1000 != 0 {
            let Some(guid_bytes) = descriptor.get(offset..offset + 16) else {
                break;
            };
            guid = Some(
                guid_bytes
                    .try_into()
                    .expect("slice length checked for view descriptor guid"),
            );
            offset += 16;

            if kind == 1 {
                let Some(length_bytes) = descriptor.get(offset..offset + 4) else {
                    break;
                };
                let buffer_length = u32::from_le_bytes(
                    length_bytes
                        .try_into()
                        .expect("slice length checked for view descriptor name length"),
                ) as usize;
                offset += 4;
                let Some(buffer) = descriptor.get(offset..offset + buffer_length) else {
                    break;
                };
                let units = buffer
                    .chunks_exact(2)
                    .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
                    .take_while(|unit| *unit != 0)
                    .collect::<Vec<_>>();
                name = Some(String::from_utf16(&units).expect("valid UTF-16 view name"));
                offset += buffer_length;
            }
        }

        packets.push(TestViewColumnPacket {
            property_type,
            property_id,
            width,
            flags,
            kind,
            id,
            guid,
            name,
        });
    }

    packets
}

#[test]
fn common_view_named_view_projects_descriptor_properties_for_outlook() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let view = MapiCommonViewNamedViewMessage {
        id: crate::mapi::identity::mapi_store_id(0x7fff_ffff_fff7),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x11111111111111111111111111111111),
        name: "Messages".to_string(),
        view_flags: 0,
        view_type: 8,
    };

    let Some(MapiValue::Binary(descriptor)) =
        common_view_named_view_property_value(&view, account_id, PID_TAG_VIEW_DESCRIPTOR_BINARY)
    else {
        panic!("expected PidTagViewDescriptorBinary");
    };
    assert_eq!(
        common_view_named_view_property_value(
            &view,
            account_id,
            OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
        ),
        Some(MapiValue::Binary(descriptor.clone()))
    );
    assert_eq!(
        common_view_named_view_property_value(
            &view,
            account_id,
            OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
        ),
        Some(MapiValue::Binary(view_descriptor_strings_binary(
            &outlook_mail_view_definition("Messages")
        )))
    );
    assert_eq!(descriptor.len(), 436);
    assert_eq!(&descriptor[8..12], &8u32.to_le_bytes());
    assert_eq!(&descriptor[12..16], &2u32.to_le_bytes());
    assert_eq!(&descriptor[20..24], &10u32.to_le_bytes());
    assert_eq!(&descriptor[24..28], &8u32.to_le_bytes());
    assert_eq!(&descriptor[60..62], &1u16.to_le_bytes());
    assert_eq!(&descriptor[62..64], &4u16.to_le_bytes());
    assert_eq!(&descriptor[64..68], &7u32.to_le_bytes());
    assert_eq!(&descriptor[72..76], &0x28u32.to_le_bytes());
    assert_eq!(&descriptor[92..96], &4u32.to_le_bytes());
    assert_eq!(
        descriptor_column_property_tags(&descriptor),
        vec![
            PID_TAG_IMPORTANCE,
            PID_LID_OUTLOOK_COMMON_8514_TAG,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_MESSAGE_STATUS,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_SENT_REPRESENTING_NAME_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_MESSAGE_DELIVERY_TIME,
            OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG,
        ]
    );
    assert_eq!(&descriptor[96..98], &0x0003u16.to_le_bytes());
    assert_eq!(&descriptor[98..100], &0x0017u16.to_le_bytes());
    assert_eq!(&descriptor[100..104], &18u32.to_le_bytes());
    assert_eq!(&descriptor[108..112], &0x0000_2F4Au32.to_le_bytes());
    assert_eq!(&descriptor[124..128], &0u32.to_le_bytes());
    assert_eq!(&descriptor[128..132], &0x0017u32.to_le_bytes());
    assert_eq!(
        common_view_named_view_property_value(
            &view,
            account_id,
            PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
        ),
        Some(MapiValue::U32(8))
    );
    assert_eq!(
        common_view_named_view_property_value(&view, account_id, PID_TAG_MESSAGE_FLAGS),
        Some(MapiValue::U32(MSGFLAG_FAI))
    );
    assert_eq!(
        common_view_named_view_property_value(&view, account_id, PID_TAG_VIEW_DESCRIPTOR_NAME_W),
        Some(MapiValue::String("Messages".to_string()))
    );
    assert_eq!(
            common_view_named_view_property_value(
                &view,
                account_id,
                PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
            ),
            Some(MapiValue::String(
                "\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nFrom\nSubject\nReceived\nSize\n".to_string()
            ))
        );
    assert_eq!(
        common_view_named_view_property_value(
            &view,
            account_id,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
        ),
        Some(MapiValue::Binary(descriptor.clone()))
    );
    assert_eq!(
        common_view_named_view_property_value(&view, account_id, PID_TAG_PARENT_ENTRY_ID),
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, COMMON_VIEWS_FOLDER_ID)
            .map(MapiValue::Binary)
    );
}

#[test]
fn messages_view_definition_matches_outlook_visible_inbox_projection() {
    let definition = outlook_mail_view_definition("Messages");
    let descriptor = view_descriptor_binary(&definition);

    assert_eq!(descriptor.len(), 436);
    assert_eq!(&descriptor[8..12], &8u32.to_le_bytes());
    assert_eq!(&descriptor[12..16], &2u32.to_le_bytes());
    assert_eq!(&descriptor[20..24], &10u32.to_le_bytes());
    assert_eq!(&descriptor[24..28], &8u32.to_le_bytes());
    assert_eq!(
        view_descriptor_strings(&definition),
        "\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nFrom\nSubject\nReceived\nSize\n"
    );
    assert_eq!(
        descriptor_column_property_tags(&descriptor),
        vec![
            PID_TAG_IMPORTANCE,
            PID_LID_OUTLOOK_COMMON_8514_TAG,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_MESSAGE_STATUS,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_SENT_REPRESENTING_NAME_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_MESSAGE_DELIVERY_TIME,
            OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG,
        ]
    );
    assert_eq!(
        descriptor_column_packets(&descriptor),
        vec![
            TestViewColumnPacket {
                property_type: 0x0001,
                property_id: 0x0004,
                width: 0x07,
                flags: 0x0000_0028,
                kind: 0,
                id: 0x0004,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x0003,
                property_id: 0x0017,
                width: 0x12,
                flags: 0x0000_2F4A,
                kind: 0,
                id: 0x0017,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x000B,
                property_id: 0x8514,
                width: 0x12,
                flags: 0x0000_3F40,
                kind: 0,
                id: 0x8514,
                guid: Some(PSETID_COMMON_GUID),
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x001F,
                property_id: 0x001A,
                width: 0x12,
                flags: 0x0000_270A,
                kind: 0,
                id: 0x001A,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x0003,
                property_id: 0x0E17,
                width: 0x12,
                flags: 0x0000_2F4A,
                kind: 0,
                id: 0x0E17,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x000B,
                property_id: 0x0E1B,
                width: 0x12,
                flags: 0x0000_2F4A,
                kind: 0,
                id: 0x0E1B,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x001F,
                property_id: 0x0042,
                width: 0x0C,
                flags: 0x0000_2F00,
                kind: 0,
                id: 0x0042,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x001F,
                property_id: 0x0037,
                width: 0x11,
                flags: 0x0000_2F00,
                kind: 0,
                id: 0x0037,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x0040,
                property_id: 0x0E06,
                width: 0x10,
                flags: 0x0000_2F40,
                kind: 0,
                id: 0x0E06,
                guid: None,
                name: None,
            },
            TestViewColumnPacket {
                property_type: 0x0003,
                property_id: 0x1213,
                width: 0x0C,
                flags: 0x0000_2740,
                kind: 0,
                id: 0x1213,
                guid: None,
                name: None,
            },
        ]
    );
}

#[test]
fn outlook_compact_view_definition_binary_matches_visible_trace_contract() {
    let descriptor = view_descriptor_binary(&outlook_mail_view_definition("Messages"));

    assert_eq!(descriptor.len(), 436);
    assert_eq!(&descriptor[20..24], &10u32.to_le_bytes());
    assert!(!descriptor_column_property_tags(&descriptor).contains(&PID_NAME_KEYWORDS_TAG));
}

#[test]
fn common_view_sent_to_descriptor_uses_recipient_columns() {
    let compact = view_descriptor_binary(&outlook_mail_view_definition("Compact"));
    let sent_to_definition = outlook_mail_view_definition("Sent To");
    let sent_to = view_descriptor_binary(&sent_to_definition);

    assert_ne!(sent_to, compact);
    assert_eq!(sent_to_definition.kind, ViewDefinitionKind::MailSentTo);
    assert_eq!(
        descriptor_column_property_tags(&sent_to),
        vec![
            PID_TAG_IMPORTANCE,
            PID_LID_REMINDER_SET_TAG,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_FLAG_STATUS,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_DISPLAY_TO_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_CLIENT_SUBMIT_TIME,
            PID_TAG_MESSAGE_SIZE,
            PID_NAME_KEYWORDS_TAG,
        ]
    );
    assert_eq!(
            view_descriptor_strings(&sent_to_definition),
            "\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nTo\nSubject\nSent\nSize\nCategories\n"
        );
}

#[test]
fn folder_default_view_definitions_use_type_specific_columns() {
    let contact = outlook_folder_view_definition(CONTACTS_FOLDER_ID, "Compact");
    let calendar = outlook_folder_view_definition(CALENDAR_FOLDER_ID, "Compact");
    let task = outlook_folder_view_definition(TASKS_FOLDER_ID, "Compact");
    let note = outlook_folder_view_definition(NOTES_FOLDER_ID, "Compact");
    let journal = outlook_folder_view_definition(JOURNAL_FOLDER_ID, "Compact");
    let mail = outlook_folder_view_definition(INBOX_FOLDER_ID, "Compact");

    assert_eq!(contact.kind, ViewDefinitionKind::ContactList);
    assert_eq!(
        descriptor_column_property_tags(&view_descriptor_binary(&contact)),
        vec![
            string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
            string8_property_tag(PID_TAG_DISPLAY_NAME_W),
            string8_property_tag(PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG),
            string8_property_tag(PID_TAG_MOBILE_TELEPHONE_NUMBER_W),
            string8_property_tag(PID_TAG_COMPANY_NAME_W),
            string8_property_tag(PID_TAG_TITLE_W),
        ]
    );
    assert_eq!(
        view_descriptor_strings(&contact),
        "\nIcon\nFull Name\nEmail\nMobile\nCompany\nJob Title\n"
    );

    assert_eq!(calendar.kind, ViewDefinitionKind::CalendarCompact);
    assert_eq!(
        descriptor_column_property_tags(&view_descriptor_binary(&calendar)),
        vec![
            string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
            string8_property_tag(PID_TAG_SUBJECT_W),
            PID_LID_COMMON_START_TAG,
            PID_LID_COMMON_END_TAG,
            string8_property_tag(PID_LID_LOCATION_W_TAG),
            PID_LID_BUSY_STATUS_TAG,
        ]
    );
    assert_eq!(
        view_descriptor_strings(&calendar),
        "\nIcon\nSubject\nStart\nEnd\nLocation\nBusy\n"
    );

    assert_eq!(task.kind, ViewDefinitionKind::TaskList);
    assert_eq!(
        descriptor_column_property_tags(&view_descriptor_binary(&task)),
        vec![
            string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
            string8_property_tag(PID_TAG_SUBJECT_W),
            PID_TAG_FLAG_STATUS,
            PID_LID_TASK_DUE_DATE_TAG,
            PID_LID_TASK_START_DATE_TAG,
            PID_LID_PERCENT_COMPLETE_TAG,
        ]
    );
    assert_eq!(
        view_descriptor_strings(&task),
        "\nIcon\nSubject\nStatus\nDue Date\nStart Date\n% Complete\n"
    );

    assert_eq!(note.kind, ViewDefinitionKind::NoteList);
    assert_eq!(
        descriptor_column_property_tags(&view_descriptor_binary(&note)),
        vec![
            string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
            string8_property_tag(PID_TAG_SUBJECT_W),
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_LID_NOTE_COLOR_TAG,
        ]
    );
    assert_eq!(
        view_descriptor_strings(&note),
        "\nIcon\nSubject\nModified\nColor\n"
    );

    assert_eq!(journal.kind, ViewDefinitionKind::JournalList);
    assert_eq!(
        descriptor_column_property_tags(&view_descriptor_binary(&journal)),
        vec![
            string8_property_tag(PID_TAG_MESSAGE_CLASS_W),
            string8_property_tag(PID_TAG_SUBJECT_W),
            PID_LID_LOG_START_TAG,
            PID_LID_LOG_DURATION_TAG,
            string8_property_tag(PID_LID_LOG_TYPE_W_TAG),
        ]
    );
    assert_eq!(
        view_descriptor_strings(&journal),
        "\nIcon\nSubject\nStart\nDuration\nType\n"
    );

    assert_eq!(mail.kind, ViewDefinitionKind::MailCompact);
}

#[test]
fn common_view_named_view_descriptor_opens_as_stream() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let view_id = crate::mapi::identity::mapi_store_id(0x7fff_ffff_fff7);
    let mut handles = std::collections::HashMap::new();
    handles.insert(
        1,
        MapiObject::CommonViewNamedView {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            view_id,
        },
    );
    let mut session = MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: Uuid::nil(),
        account_id,
        email: "test@example.com".to_string(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        last_seen_at: std::time::SystemTime::UNIX_EPOCH,
        first_request_type: String::new(),
        first_request_id: String::new(),
        last_request_type: String::new(),
        last_request_id: String::new(),
        request_count: 0,
        execute_request_count: 0,
        next_handle: 2,
        handles,
        message_statuses: std::collections::HashMap::new(),
        message_save_generations: std::collections::HashMap::new(),
        message_handle_generations: std::collections::HashMap::new(),
        pending_message_recipient_replacements: std::collections::HashMap::new(),
        pending_message_attachments: std::collections::HashMap::new(),
        pending_attachment_parent_messages: std::collections::HashMap::new(),
        pending_attachment_deletions: std::collections::HashSet::new(),
        pending_embedded_message_ids: std::collections::HashMap::new(),
        pending_embedded_message_attachments: std::collections::HashMap::new(),
        saved_embedded_messages: std::collections::HashMap::new(),
        saved_search_folder_definitions: std::collections::HashMap::new(),
        special_folder_aliases: std::collections::HashMap::new(),
        deleted_advertised_special_folders: std::collections::HashSet::new(),
        deleted_search_folder_definitions: std::collections::HashSet::new(),
        named_properties: std::collections::HashMap::new(),
        named_property_ids: std::collections::HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: std::collections::VecDeque::new(),
        completed_execute_requests: std::collections::HashMap::new(),
        completed_execute_request_order: std::collections::VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        inbox_associated_config_stream_handles: std::collections::HashSet::new(),
        inbox_rule_organizer_stream_handles: std::collections::HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    };
    let snapshot = MapiMailStoreSnapshot::empty();

    let (stream, writable_target) = property_stream_data(
        &mut session,
        1,
        PID_TAG_VIEW_DESCRIPTOR_BINARY,
        0,
        &[],
        account_id,
        &snapshot,
    )
    .expect("common view descriptor stream");

    assert_eq!(
        stream,
        view_descriptor_binary(&outlook_mail_view_definition("Compact"))
    );
    assert_eq!(stream.len(), 510);
    assert!(writable_target.is_none());

    let (strings_stream, writable_target) = property_stream_data(
        &mut session,
        1,
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
        0,
        &[],
        account_id,
        &snapshot,
    )
    .expect("common view descriptor strings stream");
    assert_eq!(
        strings_stream,
        view_descriptor_strings_binary(&outlook_mail_view_definition("Compact"))
    );
    assert!(strings_stream.ends_with(&[0x0A, 0x00]));
    assert!(writable_target.is_none());
}

#[test]
fn common_view_named_view_descriptor_accepts_microsoft_write_stream_sequence() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let view_id = crate::mapi::identity::mapi_store_id(0x7fff_ffff_fff7);
    let mut handles = std::collections::HashMap::new();
    handles.insert(
        1,
        MapiObject::CommonViewNamedView {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            view_id,
        },
    );
    let mut session = MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: Uuid::nil(),
        account_id,
        email: "test@example.com".to_string(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        last_seen_at: std::time::SystemTime::UNIX_EPOCH,
        first_request_type: String::new(),
        first_request_id: String::new(),
        last_request_type: String::new(),
        last_request_id: String::new(),
        request_count: 0,
        execute_request_count: 0,
        next_handle: 2,
        handles,
        message_statuses: std::collections::HashMap::new(),
        message_save_generations: std::collections::HashMap::new(),
        message_handle_generations: std::collections::HashMap::new(),
        pending_message_recipient_replacements: std::collections::HashMap::new(),
        pending_message_attachments: std::collections::HashMap::new(),
        pending_attachment_parent_messages: std::collections::HashMap::new(),
        pending_attachment_deletions: std::collections::HashSet::new(),
        pending_embedded_message_ids: std::collections::HashMap::new(),
        pending_embedded_message_attachments: std::collections::HashMap::new(),
        saved_embedded_messages: std::collections::HashMap::new(),
        saved_search_folder_definitions: std::collections::HashMap::new(),
        special_folder_aliases: std::collections::HashMap::new(),
        deleted_advertised_special_folders: std::collections::HashSet::new(),
        deleted_search_folder_definitions: std::collections::HashSet::new(),
        named_properties: std::collections::HashMap::new(),
        named_property_ids: std::collections::HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: std::collections::VecDeque::new(),
        completed_execute_requests: std::collections::HashMap::new(),
        completed_execute_request_order: std::collections::VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        inbox_associated_config_stream_handles: std::collections::HashSet::new(),
        inbox_rule_organizer_stream_handles: std::collections::HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    };
    let snapshot = MapiMailStoreSnapshot::empty();

    let (stream, writable_target) = property_stream_data(
        &mut session,
        1,
        PID_TAG_VIEW_DESCRIPTOR_BINARY,
        1,
        &[],
        account_id,
        &snapshot,
    )
    .expect("writable common view descriptor stream");

    assert_eq!(writable_target, Some(StreamWriteTarget::VolatileProperty));
    session.handles.insert(
        2,
        MapiObject::AttachmentStream {
            data: stream,
            position: 0,
            writable_target,
        },
    );
    assert_eq!(set_attachment_stream_size(&mut session, 2, 4), Some(()));
    assert_eq!(write_stream(&mut session, 2, b"view"), Some(4));
    let Some(MapiObject::AttachmentStream { data, .. }) = session.handles.get(&2) else {
        panic!("expected descriptor stream");
    };
    assert_eq!(data, b"view");
}

#[test]
fn associated_config_missing_binary_property_opens_writable_stream() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let config_id = crate::mapi::identity::mapi_store_id(0x15c);
    let mut handles = std::collections::HashMap::new();
    handles.insert(
        1,
        MapiObject::AssociatedConfig {
            folder_id: INBOX_FOLDER_ID,
            config_id,
            saved_message: Some(MapiAssociatedConfigMessage {
                id: config_id,
                folder_id: INBOX_FOLDER_ID,
                canonical_id: Uuid::from_u128(0x11111111222243338444555555555556),
                message_class: "IPM.ExtendedRule.Message".to_string(),
                subject: "Junk E-mail Rule".to_string(),
                properties_json: serde_json::json!({
                    "0x001a001f": {
                        "type": "string",
                        "value": "IPM.ExtendedRule.Message"
                    },
                    "0x0037001f": {
                        "type": "string",
                        "value": "Junk E-mail Rule"
                    }
                }),
            }),
        },
    );
    let mut session = MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: Uuid::nil(),
        account_id,
        email: "test@example.com".to_string(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        last_seen_at: std::time::SystemTime::UNIX_EPOCH,
        first_request_type: String::new(),
        first_request_id: String::new(),
        last_request_type: String::new(),
        last_request_id: String::new(),
        request_count: 0,
        execute_request_count: 0,
        next_handle: 2,
        handles,
        message_statuses: std::collections::HashMap::new(),
        message_save_generations: std::collections::HashMap::new(),
        message_handle_generations: std::collections::HashMap::new(),
        pending_message_recipient_replacements: std::collections::HashMap::new(),
        pending_message_attachments: std::collections::HashMap::new(),
        pending_attachment_parent_messages: std::collections::HashMap::new(),
        pending_attachment_deletions: std::collections::HashSet::new(),
        pending_embedded_message_ids: std::collections::HashMap::new(),
        pending_embedded_message_attachments: std::collections::HashMap::new(),
        saved_embedded_messages: std::collections::HashMap::new(),
        saved_search_folder_definitions: std::collections::HashMap::new(),
        special_folder_aliases: std::collections::HashMap::new(),
        deleted_advertised_special_folders: std::collections::HashSet::new(),
        deleted_search_folder_definitions: std::collections::HashSet::new(),
        named_properties: std::collections::HashMap::new(),
        named_property_ids: std::collections::HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: std::collections::VecDeque::new(),
        completed_execute_requests: std::collections::HashMap::new(),
        completed_execute_request_order: std::collections::VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        inbox_associated_config_stream_handles: std::collections::HashSet::new(),
        inbox_rule_organizer_stream_handles: std::collections::HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    };
    let snapshot = MapiMailStoreSnapshot::empty();

    let (stream, writable_target) =
        property_stream_data(&mut session, 1, 0x0e9a_0102, 1, &[], account_id, &snapshot)
            .expect("writable extended-rule action stream");

    assert!(stream.is_empty());
    assert_eq!(
        writable_target,
        Some(StreamWriteTarget::AssociatedConfigProperty {
            handle: 1,
            property_tag: 0x0e9a_0102
        })
    );
    assert_eq!(write_stream(&mut session, 2, b"ignored"), None);
    let handle = 2;
    session.handles.insert(
        handle,
        MapiObject::AttachmentStream {
            data: stream,
            position: 0,
            writable_target,
        },
    );
    assert_eq!(resolve_writable_stream_handle(&session, 1), Some(handle));
    assert_eq!(set_attachment_stream_size(&mut session, 1, 4), None);
    let resolved_handle = resolve_writable_stream_handle(&session, 1).unwrap();
    assert_eq!(
        set_attachment_stream_size(&mut session, resolved_handle, 4),
        Some(())
    );
    assert_eq!(
        write_stream(&mut session, handle, b"rule-actions"),
        Some(12)
    );
    let Some(MapiObject::AssociatedConfig {
        saved_message: Some(message),
        ..
    }) = session.handles.get(&1)
    else {
        panic!("expected associated config handle");
    };
    assert_eq!(
        mapi_properties_from_json(&message.properties_json).get(&0x0e9a_0102),
        Some(&MapiValue::Binary(b"rule-actions".to_vec()))
    );
}

#[test]
fn associated_config_unknown_binary_property_does_not_open_as_empty_stream() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let config_id = crate::mapi::identity::mapi_store_id(0x13f);
    let rule_organizer_config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFED);
    let message = MapiAssociatedConfigMessage {
        id: config_id,
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x11111111222243338444555555555555),
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        subject: "IPM.Configuration.MessageListSettings".to_string(),
        properties_json: serde_json::json!({}),
    };
    let mut handles = std::collections::HashMap::new();
    handles.insert(
        1,
        MapiObject::AssociatedConfig {
            folder_id: INBOX_FOLDER_ID,
            config_id,
            saved_message: Some(message),
        },
    );
    handles.insert(
        2,
        MapiObject::AssociatedConfig {
            folder_id: INBOX_FOLDER_ID,
            config_id: rule_organizer_config_id,
            saved_message: Some(MapiAssociatedConfigMessage {
                id: rule_organizer_config_id,
                folder_id: INBOX_FOLDER_ID,
                canonical_id: Uuid::from_u128(0x6d617069_7275_6c65_8000_000000000001),
                message_class: crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS
                    .to_string(),
                subject: crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
                properties_json: serde_json::json!({}),
            }),
        },
    );
    let mut session = MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: Uuid::nil(),
        account_id,
        email: "test@example.com".to_string(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        last_seen_at: std::time::SystemTime::UNIX_EPOCH,
        first_request_type: String::new(),
        first_request_id: String::new(),
        last_request_type: String::new(),
        last_request_id: String::new(),
        request_count: 0,
        execute_request_count: 0,
        next_handle: 2,
        handles,
        message_statuses: std::collections::HashMap::new(),
        message_save_generations: std::collections::HashMap::new(),
        message_handle_generations: std::collections::HashMap::new(),
        pending_message_recipient_replacements: std::collections::HashMap::new(),
        pending_message_attachments: std::collections::HashMap::new(),
        pending_attachment_parent_messages: std::collections::HashMap::new(),
        pending_attachment_deletions: std::collections::HashSet::new(),
        pending_embedded_message_ids: std::collections::HashMap::new(),
        pending_embedded_message_attachments: std::collections::HashMap::new(),
        saved_embedded_messages: std::collections::HashMap::new(),
        saved_search_folder_definitions: std::collections::HashMap::new(),
        special_folder_aliases: std::collections::HashMap::new(),
        deleted_advertised_special_folders: std::collections::HashSet::new(),
        deleted_search_folder_definitions: std::collections::HashSet::new(),
        named_properties: std::collections::HashMap::new(),
        named_property_ids: std::collections::HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: std::collections::VecDeque::new(),
        completed_execute_requests: std::collections::HashMap::new(),
        completed_execute_request_order: std::collections::VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        inbox_associated_config_stream_handles: std::collections::HashSet::new(),
        inbox_rule_organizer_stream_handles: std::collections::HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    };
    let snapshot = MapiMailStoreSnapshot::empty();

    assert!(property_stream_data(
        &mut session,
        1,
        OUTLOOK_RULE_ORGANIZER_BINARY_6802,
        0,
        &[],
        account_id,
        &snapshot
    )
    .is_none());
    assert!(property_stream_data(
        &mut session,
        2,
        OUTLOOK_RULE_ORGANIZER_BINARY_6802,
        0,
        &[],
        account_id,
        &snapshot,
    )
    .is_none());
    let (modeled_stream, writable_target) = property_stream_data(
        &mut session,
        1,
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
        0,
        &[],
        account_id,
        &snapshot,
    )
    .expect("modeled associated config stream");
    assert!(modeled_stream.is_empty());
    assert!(writable_target.is_none());
}

#[test]
fn mapi_mailbox_display_name_normalizes_canonical_inbox() {
    let inbox = mailbox(
        "11111111-1111-1111-1111-111111111111",
        None,
        "inbox",
        "INBOX",
    );
    let custom = mailbox(
        "22222222-2222-2222-2222-222222222222",
        None,
        "",
        "INBOX Reports",
    );

    assert_eq!(mapi_mailbox_display_name(&inbox), "Inbox");
    assert_eq!(
        mailbox_property_value_with_context(&inbox, &[], PID_TAG_DISPLAY_NAME_W),
        Some(MapiValue::String("Inbox".to_string()))
    );
    assert_eq!(mapi_mailbox_display_name(&custom), "INBOX Reports");
}

#[test]
fn sharing_local_folder_id_named_property_maps_to_outlook_id() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name(
                "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
            ),
        }),
        Some(0x8010)
    );
}

#[test]
fn outlook_sharing_probe_named_properties_map_to_stable_ids() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_SHARING_REMOTE_NAME),
        }),
        Some(PID_LID_OUTLOOK_SHARING_REMOTE_NAME as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_SHARING_REMOTE_UID),
        }),
        Some(PID_LID_OUTLOOK_SHARING_REMOTE_UID as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_SHARING_LOCAL_TYPE),
        }),
        Some(PID_LID_OUTLOOK_SHARING_LOCAL_TYPE as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_SHARING_PROVIDER_GUID),
        }),
        Some(PID_LID_OUTLOOK_SHARING_PROVIDER_GUID as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_SHARING_CAPABILITIES),
        }),
        Some(PID_LID_OUTLOOK_SHARING_CAPABILITIES as u16)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name("SharingSendAsState".to_string()),
        }),
        Some(0x81ED)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_SHARING_8AA6),
        }),
        Some(PID_LID_OUTLOOK_SHARING_8AA6 as u16)
    );
}

#[test]
fn outlook_contact_link_probe_named_properties_map_to_stable_ids() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("OscContactSources".to_string()),
        }),
        Some(MapiPropertyTag::new(PID_NAME_OSC_CONTACT_SOURCES_TAG).property_id())
    );

    for lid in [
        PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1,
        PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC,
        PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA,
        PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED,
    ] {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PS_PUBLIC_STRINGS_GUID,
                kind: MapiNamedPropertyKind::Lid(lid),
            }),
            Some(lid as u16),
            "PS_PUBLIC_STRINGS lid 0x{lid:04x} should not allocate a transient id"
        );
    }
}

#[test]
fn outlook_contact_source_probe_named_properties_map_to_stable_ids() {
    for lid in [
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E0,
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E2,
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E3,
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E5,
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E6,
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E8,
    ] {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_ADDRESS_GUID,
                kind: MapiNamedPropertyKind::Lid(lid),
            }),
            Some(lid as u16),
            "PSETID_Address lid 0x{lid:04x} should not allocate a transient id"
        );
    }
}

#[test]
fn outlook_calendar_sharing_probe_named_properties_map_to_stable_ids() {
    for lid in [
        PID_LID_OUTLOOK_SHARING_8A70,
        PID_LID_OUTLOOK_SHARING_8A71,
        PID_LID_OUTLOOK_SHARING_8A72,
        PID_LID_OUTLOOK_SHARING_8A74,
        PID_LID_OUTLOOK_SHARING_8A75,
        PID_LID_OUTLOOK_SHARING_8A73,
        PID_LID_OUTLOOK_SHARING_8A76,
        PID_LID_OUTLOOK_SHARING_8A77,
        PID_LID_OUTLOOK_SHARING_8A78,
        PID_LID_OUTLOOK_SHARING_8A7E,
        PID_LID_OUTLOOK_SHARING_8A80,
        PID_LID_OUTLOOK_SHARING_8A8B,
        PID_LID_OUTLOOK_SHARING_8A88,
        PID_LID_OUTLOOK_SHARING_8A8E,
        PID_LID_OUTLOOK_SHARING_8A8D,
    ] {
        assert_eq!(
            well_known_named_property_id(&MapiNamedProperty {
                guid: PSETID_SHARING_GUID,
                kind: MapiNamedPropertyKind::Lid(lid),
            }),
            Some(lid as u16),
            "PSETID_Sharing lid 0x{lid:04x} should not allocate a transient id"
        );
    }
}

#[test]
fn appointment_color_named_property_maps_to_stable_id() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_APPOINTMENT_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_APPOINTMENT_COLOR),
        }),
        Some(PID_LID_APPOINTMENT_COLOR as u16)
    );
}

#[test]
fn outlook_visible_inbox_probe_named_property_maps_to_stable_id() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_APPOINTMENT_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_APPOINTMENT_8F07),
        }),
        Some(PID_LID_OUTLOOK_APPOINTMENT_8F07 as u16)
    );
}

#[test]
fn contact_email_named_properties_map_to_outlook_address_ids() {
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL1_EMAIL_ADDRESS),
        }),
        Some(0x8083)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL2_EMAIL_ADDRESS),
        }),
        Some(0x8093)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL3_EMAIL_ADDRESS),
        }),
        Some(0x80A3)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL1_DISPLAY_NAME),
        }),
        Some(0x8080)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME),
        }),
        Some(0x8084)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME),
        }),
        Some(0x80B6)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS),
        }),
        Some(0x80B7)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_DISPLAY_NAME),
        }),
        Some(0x80D6)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS),
        }),
        Some(0x80D7)
    );
    assert_eq!(
        well_known_named_property_id(&MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS),
        }),
        Some(0x8060)
    );
}

#[test]
fn navigation_shortcut_projects_sharing_local_folder_id() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(901),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x2222),
        subject: "Project Inbox".to_string(),
        target_folder_id: Some(INBOX_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0,
        section: 3,
        ordinal: 0x81,
        group_header_id: None,
        group_name: "Projects".to_string(),
    };
    let expected =
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID).unwrap();

    assert_eq!(
        navigation_shortcut_property_value(
            &shortcut,
            account_id,
            PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
        ),
        Some(MapiValue::Binary(expected.clone()))
    );
    assert_eq!(
        navigation_shortcut_property_value(
            &shortcut,
            account_id,
            OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG,
        ),
        Some(MapiValue::Binary(expected))
    );
}

#[test]
fn navigation_shortcut_projects_address_book_store_entry_id() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(901),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x2222),
        subject: "Inbox".to_string(),
        target_folder_id: Some(INBOX_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0,
        section: 1,
        ordinal: 0x10,
        group_header_id: None,
        group_name: "Mail".to_string(),
    };

    assert_eq!(
        navigation_shortcut_property_value(
            &shortcut,
            account_id,
            PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        ),
        Some(MapiValue::Binary(mapi_mailstore::private_store_entry_id(
            account_id
        )))
    );
}

#[test]
fn microsoft_oxocfg_wlink_ordinal_never_ends_with_reserved_insert_markers() {
    for ordinal in [0, 0x0100, 0x01ff] {
        let bytes = wlink_ordinal_bytes(ordinal);
        assert!(!bytes.is_empty());
        assert!(!matches!(bytes.last(), Some(0 | 0xff)));
    }
}

#[test]
fn navigation_shortcut_wlink_guid_fields_follow_requested_property_type() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let group_id = Uuid::from_bytes([0x33; 16]);
    let header = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(900),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x1111),
        subject: "Projects".to_string(),
        target_folder_id: None,
        shortcut_type: 4,
        flags: 0,
        save_stamp: 0,
        section: 3,
        ordinal: 0x80,
        group_header_id: Some(group_id),
        group_name: "Projects".to_string(),
    };
    let link = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(901),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x2222),
        subject: "Project Inbox".to_string(),
        target_folder_id: Some(INBOX_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0,
        section: 3,
        ordinal: 0x81,
        group_header_id: Some(group_id),
        group_name: "Projects".to_string(),
    };
    let calendar_link = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(902),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x3333),
        subject: "Project Calendar".to_string(),
        target_folder_id: Some(CALENDAR_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0,
        section: 3,
        ordinal: 0x82,
        group_header_id: Some(group_id),
        group_name: "Projects".to_string(),
    };

    assert_eq!(
        navigation_shortcut_property_value(&header, account_id, 0x6842_0102),
        Some(MapiValue::Binary([0x33; 16].to_vec()))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, 0x6850_0102),
        Some(MapiValue::Binary([0x33; 16].to_vec()))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, 0x684F_0102),
        Some(MapiValue::Binary(wlink_folder_type_guid(&link).to_vec()))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, PID_TAG_WLINK_FOLDER_TYPE),
        Some(MapiValue::Guid(wlink_folder_type_guid(&link)))
    );
    assert_eq!(
        navigation_shortcut_property_value(&link, account_id, 0x684F_0102),
        Some(MapiValue::Binary(
            [
                0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x46,
            ]
            .to_vec()
        ))
    );
    assert_eq!(
        navigation_shortcut_property_value(&calendar_link, account_id, 0x684F_0102),
        Some(MapiValue::Binary(
            [
                0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x46,
            ]
            .to_vec()
        ))
    );
}

#[test]
fn navigation_shortcut_section_one_projects_favorites_group_name() {
    let shortcut = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(903),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x4444),
        subject: "Inbox".to_string(),
        target_folder_id: Some(INBOX_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 0,
        section: 1,
        ordinal: 0x7f,
        group_header_id: Some(default_wlink_group_uuid()),
        group_name: "Mail".to_string(),
    };

    assert_eq!(
        navigation_shortcut_property_value(&shortcut, Uuid::nil(), PID_TAG_WLINK_GROUP_NAME_W),
        Some(MapiValue::String("Favorites".to_string()))
    );
}

#[test]
fn navigation_shortcut_section_one_preserves_non_mail_group_name() {
    let group_id = Uuid::parse_str("b7f00600-0000-0000-c000-000000000046").unwrap();
    let shortcut = MapiNavigationShortcutMessage {
        id: crate::mapi::identity::mapi_store_id(904),
        folder_id: COMMON_VIEWS_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x4445),
        subject: "My Calendars".to_string(),
        target_folder_id: None,
        shortcut_type: 4,
        flags: 0,
        save_stamp: 0,
        section: 1,
        ordinal: 0x80,
        group_header_id: Some(group_id),
        group_name: "My Calendars".to_string(),
    };

    assert_eq!(
        navigation_shortcut_property_value(&shortcut, Uuid::nil(), PID_TAG_WLINK_GROUP_NAME_W),
        Some(MapiValue::String("My Calendars".to_string()))
    );
}

#[test]
fn logon_projects_outlook_bootstrap_identity_metadata() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "Test User".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };

    assert_eq!(
        logon_property_value(&principal, PID_TAG_OUTLOOK_STORE_STATE),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_RESOURCE_FLAGS),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_MAILBOX_OWNER_NAME_W),
        Some(MapiValue::String("Test User".to_string()))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_ASSOCIATED_SHARING_PROVIDER),
        Some(MapiValue::Guid(OUTLOOK_SHARING_PROVIDER_GUID))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_USER_GUID),
        Some(MapiValue::Binary(principal.account_id.as_bytes().to_vec()))
    );
    let Some(MapiValue::Binary(owner_entry_id)) =
        logon_property_value(&principal, PID_TAG_MAILBOX_OWNER_ENTRY_ID)
    else {
        panic!("expected mailbox owner EntryID");
    };
    assert_eq!(&owner_entry_id[..4], &[0, 0, 0, 0]);
    assert_eq!(
        &owner_entry_id[4..20],
        &NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID
    );
    assert!(owner_entry_id.ends_with(&[0]));
    assert_eq!(
        logon_property_value(&principal, PID_TAG_USER_ENTRY_ID),
        Some(MapiValue::Binary(owner_entry_id))
    );
    let Some(MapiValue::Binary(public_folder_entry_id)) =
        logon_property_value(&principal, PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID)
    else {
        panic!("expected public folders EntryID");
    };
    assert_eq!(
        crate::mapi::identity::object_id_from_folder_entry_id(&public_folder_entry_id),
        Some(PUBLIC_FOLDERS_ROOT_FOLDER_ID)
    );
}

#[test]
fn logon_projects_max_submit_message_size() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: Some(4096),
        quota_used_octets: Some(12_345),
    };

    assert_eq!(
        logon_property_value(&principal, PID_TAG_MAX_SUBMIT_MESSAGE_SIZE),
        Some(MapiValue::U32(35 * 1024))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_EXTENDED_RULE_SIZE_LIMIT),
        Some(MapiValue::U32(35 * 1024))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_MESSAGE_SIZE_EXTENDED),
        Some(MapiValue::I64(12_345))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_PROHIBIT_RECEIVE_QUOTA),
        Some(MapiValue::U32(4096 * 1024))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_PROHIBIT_SEND_QUOTA),
        Some(MapiValue::U32(4096 * 1024))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_STORAGE_QUOTA_LIMIT),
        Some(MapiValue::U32(4096 * 1024))
    );
    assert_eq!(
        logon_property_value(&principal, PID_TAG_PST_PATH_W),
        Some(MapiValue::String(String::new()))
    );
}
