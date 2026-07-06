use super::*;
use crate::mapi::wire::MapiRestrictionType;
use crate::mapi::wire::RopId;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationRights, MailboxRule,
    SearchFolderDefinition,
};

fn exchange_builtin_excluded_folder_roles() -> Vec<String> {
    [
        "trash",
        "junk",
        "drafts",
        "outbox",
        "conflicts",
        "local_failures",
        "server_failures",
        "sync_issues",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

#[test]
fn default_hierarchy_columns_cover_table_projection_contract() {
    let columns = default_hierarchy_columns();
    for property_tag in [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_ACCESS,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_SERIALIZED_REPLID_GUID_MAP,
        PID_TAG_SUBFOLDERS,
    ] {
        assert!(columns.contains(&property_tag));
    }
}

#[test]
fn default_store_identity_columns_include_offline_reminders_entry_id() {
    assert!(default_store_property_tags().contains(&PID_TAG_REM_OFFLINE_ENTRY_ID));
    assert!(default_folder_identity_property_tags().contains(&PID_TAG_REM_OFFLINE_ENTRY_ID));
}

#[test]
fn property_defaults_serialize_floating_types_with_wire_widths() {
    let mut single = Vec::new();
    write_property_default(&mut single, 0x80BF_0004);
    assert_eq!(single, 0.0f32.to_le_bytes());

    let mut double = Vec::new();
    write_property_default(&mut double, 0x80BF_0005);
    assert_eq!(double, 0.0f64.to_le_bytes());
}

#[test]
fn property_defaults_serialize_server_ids_as_empty_counted_binary() {
    let mut row = Vec::new();
    write_property_default(&mut row, PID_TAG_SENT_MAIL_SVR_EID);
    assert_eq!(row, 0u16.to_le_bytes());
}

#[test]
fn property_defaults_serialize_multi_value_instance_columns() {
    let mut row = Vec::new();
    write_property_default(&mut row, 0x8031_3003);
    assert_eq!(row, 0u32.to_le_bytes());
}

#[test]
fn microsoft_read_flags_validation_matches_message_protocol_rules() {
    for flags in [0x00, 0x01, 0x05, 0x10, 0x20, 0x40, 0x0A] {
        assert!(read_flags_are_valid(Some(flags), true));
    }
    for flags in [0x01, 0x05, 0x10, 0x20, 0x40] {
        assert!(read_flags_are_valid(Some(flags), false));
    }

    assert!(!read_flags_are_valid(Some(0x00), false));
    assert!(!read_flags_are_valid(Some(0x0A), false));
    assert!(!read_flags_are_valid(Some(0x04), true));
    assert!(!read_flags_are_valid(Some(0x11), true));
    assert!(!read_flags_are_valid(Some(0x60), true));
    assert!(!read_flags_are_valid(Some(0x80), true));
    assert!(!read_flags_are_valid(None, true));
}

#[test]
fn outlook_bootstrap_row_invariant_classifier_reports_consistency() {
    let folder_id = INBOX_FOLDER_ID;
    let parent_id = IPM_SUBTREE_FOLDER_ID;
    let mailbox_guid = Uuid::new_v4();
    let entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id).unwrap();
    let parent_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, parent_id).unwrap();
    let source_key = mapi_mailstore::source_key_for_store_id(folder_id);
    let parent_source_key = mapi_mailstore::source_key_for_store_id(parent_id);
    let instance_key = crate::mapi::identity::instance_key_for_object_id(folder_id);

    let summary = classify_outlook_bootstrap_row_invariants(
        0,
        "hierarchy_folder",
        folder_id,
        Some(folder_id),
        Some(parent_id),
        Some("IPF.Note"),
        |tag| match canonical_property_storage_tag(tag) {
            PID_TAG_ENTRY_ID => Some(MapiValue::Binary(entry_id.clone())),
            PID_TAG_RECORD_KEY => Some(MapiValue::Binary(source_key.clone())),
            PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(source_key.clone())),
            PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(parent_source_key.clone())),
            PID_TAG_PARENT_ENTRY_ID => Some(MapiValue::Binary(parent_entry_id.clone())),
            PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
            PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(instance_key.clone())),
            PID_TAG_DISPLAY_NAME_W => Some(MapiValue::String("Inbox".to_string())),
            PID_TAG_CONTAINER_CLASS_W => Some(MapiValue::String("IPF.Note".to_string())),
            PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
            PID_TAG_CONTENT_COUNT | PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(0)),
            _ => None,
        },
    );

    assert!(summary.contains("folder_id_consistent=true"));
    assert!(summary.contains("parent_id_consistent=true"));
    assert!(summary.contains("source_key_stable_non_empty=true"));
    assert!(summary.contains("record_key_stable_non_empty=true"));
    assert!(summary.contains("issues=none"));
}

#[test]
fn inbox_associated_invariant_uses_mailbox_guid_entry_id() {
    let mailbox_guid = Uuid::parse_str("bc737006-4413-49b9-aefc-3cb6e0088492").unwrap();
    let object = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: Vec::new(),
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: std::collections::HashSet::new(),
        restriction: None,
        bookmarks: std::collections::HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let summaries = outlook_bootstrap_row_invariant_summaries(
        Some(&object),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        mailbox_guid,
        true,
        1,
    );

    assert_eq!(summaries.len(), 1, "{summaries:?}");
    assert!(
        summaries[0].contains("row_kind=inbox_associated")
            || summaries[0].contains("kind=inbox_associated"),
        "{summaries:?}"
    );
    assert!(summaries[0].contains("issues=none"), "{summaries:?}");
}

#[test]
fn common_views_invariant_reports_decoded_row_identity() {
    let mailbox_guid = Uuid::parse_str("bc737006-4413-49b9-aefc-3cb6e0088492").unwrap();
    let object = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: Vec::new(),
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: std::collections::HashSet::new(),
        restriction: None,
        bookmarks: std::collections::HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let summaries = outlook_bootstrap_row_invariant_summaries(
        Some(&object),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        mailbox_guid,
        true,
        1,
    );

    assert_eq!(summaries.len(), 1);
    assert!(summaries[0].contains("kind=common_views_associated"));
    assert!(summaries[0].contains("source_key_decoded=0x"));
    assert!(summaries[0].contains("parent_source_key_decoded=0x0000000000090001"));
    assert!(summaries[0].contains("issues=none"));
}

#[test]
fn outlook_bootstrap_expected_container_class_matches_special_rows() {
    for (folder_id, expected) in [
        (TASKS_FOLDER_ID, "IPF.Task"),
        (RSS_FEEDS_FOLDER_ID, "IPF.Note.OutlookHomepage"),
        (CONVERSATION_ACTION_SETTINGS_FOLDER_ID, "IPF.Configuration"),
        (QUICK_STEP_SETTINGS_FOLDER_ID, "IPF.Configuration"),
        (QUICK_CONTACTS_FOLDER_ID, "IPF.Contact.MOC.QuickContacts"),
        (IM_CONTACT_LIST_FOLDER_ID, "IPF.Contact.MOC.ImContactList"),
        (FREEBUSY_DATA_FOLDER_ID, "IPF.Note"),
    ] {
        assert_eq!(debug_expected_container_class(folder_id), Some(expected));
    }
}

#[test]
fn outlook_bootstrap_row_invariant_classifier_flags_missing_record_key() {
    let folder_id = INBOX_FOLDER_ID;
    let source_key = mapi_mailstore::source_key_for_store_id(folder_id);
    let summary = classify_outlook_bootstrap_row_invariants(
        0,
        "hierarchy_folder",
        folder_id,
        Some(folder_id),
        None,
        None,
        |tag| match canonical_property_storage_tag(tag) {
            PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(source_key.clone())),
            PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
            PID_TAG_CONTENT_COUNT | PID_TAG_ASSOCIATED_CONTENT_COUNT => Some(MapiValue::U32(0)),
            PID_TAG_FOLDER_TYPE => Some(MapiValue::U32(FOLDER_GENERIC)),
            _ => None,
        },
    );

    assert!(summary.contains("record_key_stable_non_empty=false"));
    assert!(summary.contains("issues="));
    assert!(summary.contains("record_key"));
}

#[test]
fn default_contents_columns_cover_table_projection_contract() {
    let columns = default_contents_columns();
    for property_tag in [
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_CLIENT_SUBMIT_TIME,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_DISPLAY_CC_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_READ,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_HAS_ATTACHMENTS,
    ] {
        assert!(columns.contains(&property_tag));
    }
}

#[test]
fn pending_message_projects_non_empty_change_identity() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_MID,
        MapiValue::U64(crate::mapi::identity::mapi_store_id(42)),
    );

    assert_eq!(
        pending_message_property_value(&principal, &properties, PID_TAG_CHANGE_NUMBER),
        Some(MapiValue::U64(42))
    );
    assert_eq!(
        pending_message_property_value(&principal, &properties, PID_TAG_CHANGE_KEY),
        Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(42)
        ))
    );
    assert_eq!(
        pending_message_property_value(&principal, &properties, PID_TAG_PREDECESSOR_CHANGE_LIST),
        Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
            42
        )))
    );
}

#[test]
fn pending_associated_message_projects_configuration_defaults() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_CHANGE_KEY,
        MapiValue::Binary(vec![0x11, 0x22, 0x33]),
    );

    assert_eq!(
        pending_associated_message_property_value(&principal, &properties, PID_TAG_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.Configuration".to_string()))
    );
    assert!(matches!(
        pending_associated_message_property_value(
            &principal,
            &properties,
            PID_TAG_ROAMING_DICTIONARY
        ),
        Some(MapiValue::Binary(value))
            if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
                && value.windows(b"18-OLPrefsVersion".len()).any(|window| window == b"18-OLPrefsVersion")
    ));
    assert_eq!(
        pending_associated_message_property_value(&principal, &properties, PID_TAG_CHANGE_KEY),
        Some(MapiValue::Binary(vec![0x11, 0x22, 0x33]))
    );
}

#[test]
fn default_associated_config_columns_cover_required_configuration_contract() {
    let columns = default_associated_config_columns();
    for property_tag in [
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ROAMING_DATATYPES,
    ] {
        assert!(columns.contains(&property_tag));
    }
}

#[test]
fn contacts_search_folder_message_count_matches_projected_results() {
    let account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let collection = CollaborationCollection {
        id: "default".to_string(),
        kind: "contacts".to_string(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        display_name: "Contacts".to_string(),
        is_owned: true,
        rights: CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        },
    };
    let contact_id = Uuid::parse_str("71717171-7171-7171-7171-717171717171").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        contact_id,
        crate::mapi::identity::mapi_store_id(67),
    );
    let contact = AccessibleContact {
        id: contact_id,
        collection_id: collection.id.clone(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        rights: collection.rights.clone(),
        name: "Denis Ducret".to_string(),
        role: String::new(),
        email: "denis@example.test".to_string(),
        phone: String::new(),
        team: String::new(),
        notes: String::new(),
        ..Default::default()
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![collection],
        Vec::new(),
        Vec::new(),
        vec![contact],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: Uuid::parse_str("34343434-3434-4434-8434-343434343402").unwrap(),
        account_id,
        role: "contacts_search".to_string(),
        display_name: "Contacts Search".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "contact".to_string(),
        scope_json: serde_json::json!({"scope": "contacts"}),
        restriction_json: serde_json::json!({"kind": "contacts_search"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    }]);

    assert_eq!(
        folder_message_count(CONTACTS_SEARCH_FOLDER_ID, &[], &[], &snapshot),
        1
    );
}

#[test]
fn default_contacts_contents_table_uses_contact_rows_and_columns() {
    let account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let collection = CollaborationCollection {
        id: "default".to_string(),
        kind: "contacts".to_string(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        display_name: "Contacts".to_string(),
        is_owned: true,
        rights: CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        },
    };
    let contact_id = Uuid::parse_str("81818181-8181-4181-8181-818181818181").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        contact_id,
        crate::mapi::identity::mapi_store_id(681),
    );
    let contact = AccessibleContact {
        id: contact_id,
        collection_id: collection.id.clone(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        rights: collection.rights.clone(),
        name: "Denis Ducret".to_string(),
        role: String::new(),
        email: "denis@example.test".to_string(),
        phone: String::new(),
        team: String::new(),
        notes: String::new(),
        ..Default::default()
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![collection],
        Vec::new(),
        Vec::new(),
        vec![contact],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_collaboration_folder_item_count(CONTACTS_FOLDER_ID, 0);
    let mut table = MapiObject::ContentsTable {
        folder_id: CONTACTS_FOLDER_ID,
        associated: false,
        columns: Vec::new(),
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };

    assert_eq!(
        folder_message_count(CONTACTS_FOLDER_ID, &[], &[], &snapshot),
        1
    );

    let position_response = rop_query_position_response(
        &RopRequest {
            rop_id: RopId::QueryPosition.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        },
        Some(&table),
        &[],
        &[],
        &snapshot,
        account_id,
    );
    assert_eq!(
        u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
        1
    );

    let rows_response = rop_query_rows_response(
        &RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 1, 0],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        account_id,
    );

    assert_eq!(
        u16::from_le_bytes(rows_response[7..9].try_into().unwrap()),
        1
    );
    assert_response_contains_utf16(&rows_response, "Denis Ducret");
    assert_response_contains_utf16(&rows_response, "denis@example.test");
}

#[test]
fn contact_table_projects_missing_secondary_email_slots_as_empty_strings() {
    let account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111112").unwrap();
    let contact = AccessibleContact {
        id: Uuid::parse_str("81818181-8181-4181-8181-818181818182").unwrap(),
        collection_id: "default".to_string(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        name: "Denis Ducret".to_string(),
        email: "denis@example.test".to_string(),
        ..Default::default()
    };
    let columns = [
        PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG,
        PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG,
        PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG,
        PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG,
        PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG,
        PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG,
    ];

    assert_eq!(
        contact_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
        ),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        contact_table_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
        ),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        contact_table_property_value(
            &contact,
            1,
            CONTACTS_FOLDER_ID,
            PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG
        ),
        Some(MapiValue::String(String::new()))
    );

    let row = serialize_contact_row(&contact, 1, CONTACTS_FOLDER_ID, &columns);
    assert_response_contains_utf16(&row, "denis@example.test");
    assert_response_contains_utf16(&row, "SMTP");
}

#[test]
fn get_status_rejects_folder_handles_matching_microsoft_table_scope() {
    let request = RopRequest {
        rop_id: RopId::GetStatus.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let folder = MapiObject::Folder {
        folder_id: CONTACTS_SEARCH_FOLDER_ID,
        properties: HashMap::new(),
    };

    assert_eq!(
        rop_get_status_response(&request, Some(&folder)),
        vec![RopId::GetStatus.as_u8(), 1, 0x02, 0x01, 0x04, 0x80]
    );
}

#[test]
fn special_folder_rows_use_global_counters_for_change_xids() {
    let row = serialize_special_folder_row(
        INBOX_FOLDER_ID,
        &[],
        &[PID_TAG_CHANGE_NUMBER, PID_TAG_CHANGE_KEY],
        None,
    );
    let change_number = u64::from_le_bytes(row[0..8].try_into().unwrap());
    let change_key_len = u16::from_le_bytes(row[8..10].try_into().unwrap()) as usize;
    let change_key = &row[10..10 + change_key_len];

    assert_eq!(change_number, crate::mapi::identity::INBOX_FOLDER_COUNTER);
    assert_eq!(change_key_len, 22);
    assert_eq!(
        &change_key[16..22],
        &crate::mapi::identity::globcnt_bytes(change_number)
    );
}

#[test]
fn special_folder_rows_project_deleted_count_total() {
    let row = serialize_special_folder_row(
        COMMON_VIEWS_FOLDER_ID,
        &[],
        &[
            PID_TAG_LOCAL_COMMIT_TIME_MAX,
            PID_TAG_DELETED_COUNT_TOTAL,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_TAG_CONTENT_COUNT,
        ],
        None,
    );

    assert_eq!(row.len(), 20);
    assert_eq!(u32::from_le_bytes(row[8..12].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(row[12..16].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(row[16..20].try_into().unwrap()), 0);
}

#[test]
fn quick_step_settings_is_projected_as_leaf_configuration_folder() {
    assert_eq!(
        special_folder_property_value(
            QUICK_STEP_SETTINGS_FOLDER_ID,
            PID_TAG_SUBFOLDERS,
            Uuid::nil()
        ),
        Some(MapiValue::Bool(false))
    );

    let row = serialize_special_folder_row(
        QUICK_STEP_SETTINGS_FOLDER_ID,
        &[],
        &[PID_TAG_SUBFOLDERS],
        None,
    );
    assert_eq!(row, vec![0]);
}

#[test]
fn quick_step_settings_normal_contents_stays_empty_when_folder_row_has_count() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let quick_step = JmapMailbox {
        id: Uuid::from_u128(0x71756963_6b73_7465_8000_000000000001),
        parent_id: None,
        role: "quick_step_settings".to_string(),
        name: "Quick Step Settings".to_string(),
        sort_order: 175,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [quick_step];

    assert_eq!(
        folder_message_count(QUICK_STEP_SETTINGS_FOLDER_ID, &mailboxes, &[], &snapshot),
        0
    );
    assert!(associated_folder_message_count(QUICK_STEP_SETTINGS_FOLDER_ID, &snapshot) > 0);
}

#[test]
fn conversation_action_settings_normal_contents_stays_empty_when_folder_row_has_count() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let conversation_actions = JmapMailbox {
        id: Uuid::from_u128(0x636f6e76_6163_746e_8000_000000000001),
        parent_id: None,
        role: "conversation_action_settings".to_string(),
        name: "Conversation Action Settings".to_string(),
        sort_order: 170,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [conversation_actions];

    assert_eq!(
        folder_message_count(
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            &mailboxes,
            &[],
            &snapshot
        ),
        0
    );
}

#[test]
fn quick_step_settings_normal_query_rows_returns_end_without_rows() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let quick_step = JmapMailbox {
        id: Uuid::from_u128(0x71756963_6b73_7465_8000_000000000002),
        parent_id: None,
        role: "quick_step_settings".to_string(),
        name: "Quick Step Settings".to_string(),
        sort_order: 175,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [quick_step];
    let mut table = MapiObject::ContentsTable {
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 40, 0],
    };

    let response = rop_query_rows_response(
        &request,
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(response[6], 0x02);
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
    assert_eq!(table_position(&table), Some(0));
}

#[test]
fn configuration_folders_project_hidden_attribute() {
    assert_eq!(
        special_folder_property_value(
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            PID_TAG_ATTRIBUTE_HIDDEN,
            Uuid::nil()
        ),
        Some(MapiValue::Bool(true))
    );
    assert_eq!(
        special_folder_property_value(
            QUICK_STEP_SETTINGS_FOLDER_ID,
            PID_TAG_ATTRIBUTE_HIDDEN,
            Uuid::nil()
        ),
        Some(MapiValue::Bool(true))
    );

    let row = serialize_special_folder_row(
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        &[],
        &[PID_TAG_ATTRIBUTE_HIDDEN],
        None,
    );
    assert_eq!(row, vec![1]);

    let row = serialize_special_folder_row(
        QUICK_STEP_SETTINGS_FOLDER_ID,
        &[],
        &[PID_TAG_ATTRIBUTE_HIDDEN],
        None,
    );
    assert_eq!(row, vec![1]);
}

#[test]
fn sync_issues_hierarchy_table_is_leaf_until_backed() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let inbox = JmapMailbox {
        id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "INBOX".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 18,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [inbox];
    let rows = hierarchy_rows(
        SYNC_ISSUES_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    let row_ids = rows.iter().map(hierarchy_row_id).collect::<Vec<_>>();

    assert!(row_ids.is_empty());
}

#[test]
fn ipm_subtree_hierarchy_does_not_duplicate_sync_issues_children() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let rows = hierarchy_rows(
        IPM_SUBTREE_FOLDER_ID,
        &[],
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    let row_ids = rows.iter().map(hierarchy_row_id).collect::<HashSet<_>>();

    assert!(row_ids.contains(&SYNC_ISSUES_FOLDER_ID));
    assert!(!row_ids.contains(&CONFLICTS_FOLDER_ID));
    assert!(!row_ids.contains(&LOCAL_FAILURES_FOLDER_ID));
    assert!(!row_ids.contains(&SERVER_FAILURES_FOLDER_ID));
}

#[test]
fn contacts_search_hierarchy_row_belongs_to_search_folder() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let ipm_rows = hierarchy_rows(
        IPM_SUBTREE_FOLDER_ID,
        &[],
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    let search_rows = hierarchy_rows(SEARCH_FOLDER_ID, &[], &snapshot, None, &[], Uuid::nil());

    assert!(!ipm_rows
        .iter()
        .any(|row| hierarchy_row_id(row) == CONTACTS_SEARCH_FOLDER_ID));
    let row = search_rows
        .iter()
        .find(|row| hierarchy_row_id(row) == CONTACTS_SEARCH_FOLDER_ID)
        .expect("contacts search row under Search");
    assert_eq!(hierarchy_row_parent_id(row, &[]), SEARCH_FOLDER_ID);
}

#[test]
fn sync_issues_query_rows_returns_no_children_until_backed() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let inbox = JmapMailbox {
        id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "INBOX".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 18,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [inbox];
    let mut table = MapiObject::HierarchyTable {
        folder_id: SYNC_ISSUES_FOLDER_ID,
        columns: vec![PID_TAG_DISPLAY_NAME_W, PID_TAG_FOLDER_ID],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        deleted_advertised_special_folders: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 10, 0],
    };

    let response = rop_query_rows_response(
        &request,
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
    assert!(utf16_position(&response, "INBOX").is_none());
    assert!(utf16_position(&response, "Conflicts").is_none());
    assert!(utf16_position(&response, "Local Failures").is_none());
    assert!(utf16_position(&response, "Server Failures").is_none());
    assert_eq!(table_position(&table), Some(0));
}

#[test]
fn persisted_sync_issues_roles_stay_leaf_in_startup_hierarchy() {
    let sync_id = Uuid::parse_str("11111111-1111-1111-1111-11111111111a").unwrap();
    let mailboxes = vec![
        JmapMailbox {
            id: sync_id,
            parent_id: None,
            role: "sync_issues".to_string(),
            name: "Sync Issues".to_string(),
            sort_order: 90,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-11111111111b").unwrap(),
            parent_id: Some(sync_id),
            role: "conflicts".to_string(),
            name: "Conflicts".to_string(),
            sort_order: 91,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-11111111111c").unwrap(),
            parent_id: Some(sync_id),
            role: "local_failures".to_string(),
            name: "Local Failures".to_string(),
            sort_order: 92,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: Uuid::parse_str("11111111-1111-1111-1111-11111111111d").unwrap(),
            parent_id: Some(sync_id),
            role: "server_failures".to_string(),
            name: "Server Failures".to_string(),
            sort_order: 93,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
    ];
    let snapshot = MapiMailStoreSnapshot::empty();
    let rows = hierarchy_rows(
        IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    let row_ids = rows.iter().map(hierarchy_row_id).collect::<HashSet<_>>();

    assert!(row_ids.contains(&SYNC_ISSUES_FOLDER_ID));
    assert!(!row_ids.contains(&CONFLICTS_FOLDER_ID));
    assert!(!row_ids.contains(&LOCAL_FAILURES_FOLDER_ID));
    assert!(!row_ids.contains(&SERVER_FAILURES_FOLDER_ID));
    let sync_row = rows
        .iter()
        .find(|row| hierarchy_row_id(row) == SYNC_ISSUES_FOLDER_ID)
        .expect("sync issues startup row");
    assert_eq!(
        serialize_hierarchy_row(
            *sync_row,
            &mailboxes,
            &snapshot,
            &[PID_TAG_SUBFOLDERS],
            Uuid::nil(),
        ),
        vec![0]
    );
    assert!(!mailbox_has_subfolders(&mailboxes[0], &mailboxes));
    assert!(hierarchy_rows(
        SYNC_ISSUES_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    )
    .is_empty());
}

#[test]
fn query_rows_request_validation_matches_microsoft_flags() {
    fn request(flags: u8, forward_read: u8) -> RopRequest {
        RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![flags, forward_read, 1, 0],
        }
    }
    fn table() -> MapiObject {
        MapiObject::HierarchyTable {
            folder_id: SYNC_ISSUES_FOLDER_ID,
            columns: vec![PID_TAG_DISPLAY_NAME_W],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        }
    }

    for valid in [
        request(0x00, 0x00),
        request(0x00, 0x01),
        request(0x01, 0x01),
        request(0x02, 0x01),
        request(0x03, 0x01),
    ] {
        let mut table = table();
        let response = rop_query_rows_response(
            &valid,
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(&response[..6], &[0x15, 0x00, 0, 0, 0, 0]);
    }

    for invalid in [request(0x04, 0x01), request(0x00, 0x02)] {
        let mut table = table();
        let response = rop_query_rows_response(
            &invalid,
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(&response[..2], &[0x15, 0x00]);
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8007_0057
        );
    }
}

#[test]
fn query_rows_truncates_variable_property_values_to_microsoft_limit() {
    let mut row = Vec::new();
    write_utf16z(&mut row, &"A".repeat(400));
    write_u16_prefixed_bytes(&mut row, &vec![0x42; 700]);

    let mut response = Vec::new();
    write_query_rows_property_row(&mut response, &[PID_TAG_SUBJECT_W, PID_TAG_ENTRY_ID], &row);

    assert_eq!(response[0], 0);
    let mut cursor = Cursor::new(&response[1..]);
    let subject = parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_W).unwrap();
    assert_eq!(subject, MapiValue::String("A".repeat(254)));
    assert_eq!(cursor.position(), QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES);

    let entry_id = parse_mapi_property_value(&mut cursor, PID_TAG_ENTRY_ID).unwrap();
    let MapiValue::Binary(entry_id) = entry_id else {
        panic!("entry id should be binary");
    };
    assert_eq!(entry_id.len(), QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES);
    assert!(entry_id.iter().all(|byte| *byte == 0x42));
    assert_eq!(
        cursor.position(),
        QUERY_ROWS_MAX_PROPERTY_VALUE_BYTES * 2 + 2
    );
}

#[test]
fn query_rows_origin_tracks_cursor_boundary() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let inbox = JmapMailbox {
        id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "INBOX".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 18,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [inbox];
    let mut table = MapiObject::HierarchyTable {
        folder_id: IPM_SUBTREE_FOLDER_ID,
        columns: vec![PID_TAG_DISPLAY_NAME_W],
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
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 1, 0],
    };
    let total_rows =
        table_position_and_count(Some(&table), &mailboxes, &[], &snapshot, Uuid::nil()).1;
    assert!(
        total_rows > 11,
        "fixture should have enough rows to exercise non-terminal and terminal origins"
    );

    let response = rop_query_rows_response(
        &request,
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(response[6], 0x01);
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 1);
    assert_eq!(table_position(&table), Some(1));

    let response = rop_query_rows_response(
        &RopRequest {
            payload: vec![0, 1, 10, 0],
            ..request
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[6], 0x01);
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 10);
    assert_eq!(table_position(&table), Some(11));

    let response = rop_query_rows_response(
        &RopRequest {
            payload: vec![0, 1, 10, 0],
            ..request
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[6], 0x01);
    assert_eq!(
        u16::from_le_bytes(response[7..9].try_into().unwrap()),
        (total_rows - 11) as u16
    );
    assert_eq!(table_position(&table), Some(total_rows));

    let response = rop_query_rows_response(
        &RopRequest {
            payload: vec![0, 1, 10, 0],
            ..request
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[6], 0x02);
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
    assert_eq!(table_position(&table), Some(total_rows));
}

#[test]
fn query_rows_origin_uses_global_position_for_windowed_content_tables() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 4,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 501,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 502,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        mailboxes.clone(),
        vec![
            test_table_email(first_id, mailbox_id, "Window A"),
            test_table_email(second_id, mailbox_id, "Window B"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
        folder_id: INBOX_FOLDER_ID,
        view_signature: table_view_signature(&[], None),
        offset: 2,
        total: 4,
        message_ids: vec![first_id, second_id],
    }]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 2,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 2, 0],
    };

    let response = rop_query_rows_response(
        &request,
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(response[6], 0x01);
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 2);
    assert_eq!(table_position(&table), Some(4));
}

#[test]
fn query_rows_ignores_incomplete_windowed_content_table_rows() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 2,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let missing_id = Uuid::parse_str("55555555-5555-4555-8555-555555555555").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 801,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        mailboxes.clone(),
        vec![test_table_email(first_id, mailbox_id, "Only stored row")],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
        folder_id: INBOX_FOLDER_ID,
        view_signature: table_view_signature(&[], None),
        offset: 0,
        total: 2,
        message_ids: vec![first_id, missing_id],
    }]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let response = rop_query_rows_response(
        &RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 2, 0],
        },
        Some(&mut table),
        &mailboxes,
        &snapshot.emails(),
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 1);
    assert_eq!(table_position(&table), Some(1));
    assert_response_contains_utf16(&response, "Only stored row");

    let position_response = rop_query_position_response(
        &RopRequest {
            rop_id: RopId::QueryPosition.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        },
        Some(&table),
        &mailboxes,
        &snapshot.emails(),
        &snapshot,
        Uuid::nil(),
    );
    assert_eq!(position_response[0], RopId::QueryPosition.as_u8());
    assert_eq!(
        u32::from_le_bytes(position_response[6..10].try_into().unwrap()),
        1
    );
    assert_eq!(
        u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
        1
    );
}

#[test]
fn bookmark_seek_preserves_global_position_for_windowed_content_tables() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 4,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 601,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 602,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        mailboxes.clone(),
        vec![
            test_table_email(first_id, mailbox_id, "Window A"),
            test_table_email(second_id, mailbox_id, "Window B"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
        folder_id: INBOX_FOLDER_ID,
        view_signature: table_view_signature(&[], None),
        offset: 2,
        total: 4,
        message_ids: vec![first_id, second_id],
    }]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 2,
    };
    let create_response = rop_create_bookmark_response(
        &RopRequest {
            rop_id: RopId::CreateBookmark.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );
    let bookmark_size = u16::from_le_bytes(create_response[6..8].try_into().unwrap()) as usize;
    let bookmark = create_response[8..8 + bookmark_size].to_vec();
    let mut seek_payload = Vec::new();
    seek_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    seek_payload.extend_from_slice(&bookmark);
    seek_payload.extend_from_slice(&1i32.to_le_bytes());
    seek_payload.push(1);

    let seek_response = rop_seek_row_bookmark_response(
        &RopRequest {
            rop_id: RopId::SeekRowBookmark.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: seek_payload,
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(seek_response[0], RopId::SeekRowBookmark.as_u8());
    assert_eq!(seek_response[6], 0);
    assert_eq!(
        i32::from_le_bytes(seek_response[8..12].try_into().unwrap()),
        1
    );
    assert_eq!(table_position(&table), Some(3));
}

#[test]
fn bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222223").unwrap();
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 4,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333334").unwrap();
    let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444445").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 603,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 604,
        ),
    );
    let first_email = test_table_email(first_id, mailbox_id, "Window A");
    let second_email = test_table_email(second_id, mailbox_id, "Window B");
    let snapshot = MapiMailStoreSnapshot::new(
        mailboxes.clone(),
        vec![second_email],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
        folder_id: INBOX_FOLDER_ID,
        view_signature: table_view_signature(&[], None),
        offset: 3,
        total: 4,
        message_ids: vec![second_id],
    }]);
    let bookmark = 7u32.to_le_bytes().to_vec();
    let mut bookmarks = HashMap::new();
    bookmarks.insert(
        bookmark.clone(),
        TableBookmark {
            position: 2,
            row_key: Some(mapi_message_id(&first_email)),
        },
    );
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks,
        next_bookmark: 8,
        position: 0,
    };
    let mut seek_payload = Vec::new();
    seek_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    seek_payload.extend_from_slice(&bookmark);
    seek_payload.extend_from_slice(&1i32.to_le_bytes());
    seek_payload.push(1);

    let seek_response = rop_seek_row_bookmark_response(
        &RopRequest {
            rop_id: RopId::SeekRowBookmark.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: seek_payload,
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(seek_response[0], RopId::SeekRowBookmark.as_u8());
    assert_eq!(seek_response[6], 0);
    assert_eq!(
        i32::from_le_bytes(seek_response[8..12].try_into().unwrap()),
        1
    );
    assert_eq!(table_position(&table), Some(3));
}

#[test]
fn find_row_uses_windowed_content_table_rows_with_global_position() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 4,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 701,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 702,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        mailboxes.clone(),
        vec![
            test_table_email(first_id, mailbox_id, "Window A"),
            test_table_email(second_id, mailbox_id, "Window B"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
        folder_id: INBOX_FOLDER_ID,
        view_signature: table_view_signature(&[], None),
        offset: 2,
        total: 4,
        message_ids: vec![first_id, second_id],
    }]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 2,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    write_utf16z(&mut restriction, "Window B");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());

    let response = rop_find_row_response(
        &RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    assert_eq!(table_position(&table), Some(3));
    assert_response_contains_utf16(&response, "Window B");
}

#[test]
fn find_row_beginning_origin_keeps_windowed_global_position() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 4,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let first_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let second_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 711,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 712,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        mailboxes.clone(),
        vec![
            test_table_email(first_id, mailbox_id, "Window A"),
            test_table_email(second_id, mailbox_id, "Window B"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
        folder_id: INBOX_FOLDER_ID,
        view_signature: table_view_signature(&[], None),
        offset: 2,
        total: 4,
        message_ids: vec![first_id, second_id],
    }]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 2,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    write_utf16z(&mut restriction, "Window A");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(0);
    payload.extend_from_slice(&0u16.to_le_bytes());

    let response = rop_find_row_response(
        &RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        },
        Some(&mut table),
        &mailboxes,
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    assert_eq!(table_position(&table), Some(2));
    assert_response_contains_utf16(&response, "Window A");
}

#[test]
fn find_row_beginning_origin_falls_back_when_complete_rows_are_loaded() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 4,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let first_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let second_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let third_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let fourth_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    for (index, id) in [first_id, second_id, third_id, fourth_id]
        .into_iter()
        .enumerate()
    {
        crate::mapi::identity::remember_mapi_identity(
            id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 820 + index as u64,
            ),
        );
    }
    let snapshot = MapiMailStoreSnapshot::new(
        mailboxes.clone(),
        vec![
            test_table_email(first_id, mailbox_id, "Earlier A"),
            test_table_email(second_id, mailbox_id, "Earlier B"),
            test_table_email(third_id, mailbox_id, "Window A"),
            test_table_email(fourth_id, mailbox_id, "Window B"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_content_windows(vec![crate::mapi_store::MapiContentTableWindow {
        folder_id: INBOX_FOLDER_ID,
        view_signature: table_view_signature(&[], None),
        offset: 2,
        total: 4,
        message_ids: vec![third_id, fourth_id],
    }]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 2,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    write_utf16z(&mut restriction, "Earlier B");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(0);
    payload.extend_from_slice(&0u16.to_le_bytes());

    let response = rop_find_row_response(
        &RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        },
        Some(&mut table),
        &mailboxes,
        &snapshot.emails(),
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    assert_eq!(table_position(&table), Some(1));
    assert_response_contains_utf16(&response, "Earlier B");
}

#[test]
fn query_position_clamps_stale_cursor_to_current_row_count() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let expected_count =
        restricted_associated_folder_message_count(INBOX_FOLDER_ID, &snapshot, None, Uuid::nil())
            as u32;
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 50,
    };
    let request = RopRequest {
        rop_id: RopId::QueryPosition.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };

    let response =
        rop_query_position_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryPosition.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[6..10].try_into().unwrap()),
        expected_count
    );
    assert_eq!(
        u32::from_le_bytes(response[10..14].try_into().unwrap()),
        expected_count
    );
}

#[test]
fn restricted_associated_query_position_reports_filtered_row_count() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let restriction = MapiRestriction::Property {
        relop: 0x04,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String("IPM.Configuration.ExtensionMasterTable".to_string()),
    };
    let expected_count = restricted_associated_folder_message_count(
        INBOX_FOLDER_ID,
        &snapshot,
        Some(&restriction),
        Uuid::nil(),
    ) as u32;
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(restriction),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let query_position = RopRequest {
        rop_id: RopId::QueryPosition.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };

    let response = rop_query_position_response(
        &query_position,
        Some(&table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryPosition.as_u8());
    assert_eq!(u32::from_le_bytes(response[6..10].try_into().unwrap()), 0);
    assert_eq!(
        u32::from_le_bytes(response[10..14].try_into().unwrap()),
        expected_count
    );

    let query_rows = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 36, 0],
    };
    let response = rop_query_rows_response(
        &query_rows,
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(response[6], 0x02);
    assert_eq!(
        u16::from_le_bytes(response[7..9].try_into().unwrap()),
        expected_count as u16
    );
}

#[test]
fn calendar_contents_table_projects_canonical_events() {
    let account_id = Uuid::from_u128(0xbc737006441349b9aefc3cb6e0088492);
    let event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
    crate::mapi::identity::remember_mapi_identity(
        event_id,
        crate::mapi::identity::mapi_store_id(0x42),
    );
    let event = AccessibleEvent {
        id: event_id,
        uid: "zero-duration".to_string(),
        collection_id: "default".to_string(),
        owner_account_id: account_id,
        owner_email: "test@l-p-e.ch".to_string(),
        owner_display_name: "test".to_string(),
        rights: CollaborationRights {
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
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![event],
        Vec::new(),
        Vec::new(),
    );
    let mut table = MapiObject::ContentsTable {
        folder_id: CALENDAR_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };

    assert_eq!(
        folder_message_count(CALENDAR_FOLDER_ID, &[], &[], &snapshot),
        1
    );

    let position_response = rop_query_position_response(
        &RopRequest {
            rop_id: RopId::QueryPosition.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        },
        Some(&table),
        &[],
        &[],
        &snapshot,
        account_id,
    );
    assert_eq!(position_response[0], RopId::QueryPosition.as_u8());
    assert_eq!(
        u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
        1
    );

    let rows_response = rop_query_rows_response(
        &RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 10, 0],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        account_id,
    );
    assert_eq!(rows_response[0], RopId::QueryRows.as_u8());
    assert_eq!(
        u16::from_le_bytes(rows_response[7..9].try_into().unwrap()),
        1
    );
    assert_response_contains_utf16(&rows_response, "Test");
}

#[test]
fn query_rows_clamps_stale_cursor_to_current_row_count() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 50,
    };
    let expected_count = table_position_and_count(Some(&table), &[], &[], &snapshot, Uuid::nil()).1;
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 10, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(response[6], 0x02);
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 0);
    assert_eq!(table_position(&table), Some(expected_count));
}

#[test]
fn seek_row_clamps_stale_current_position_to_row_count() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 50,
    };
    let expected_count = table_position_and_count(Some(&table), &[], &[], &snapshot, Uuid::nil()).1;
    let response = rop_seek_row_response(
        &RopRequest {
            rop_id: RopId::SeekRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![1, 0, 0, 0, 0, 1],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::SeekRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[6], 0);
    assert_eq!(i32::from_le_bytes(response[7..11].try_into().unwrap()), 0);
    assert_eq!(table_position(&table), Some(expected_count));
}

#[test]
fn seek_row_request_validation_matches_microsoft_bookmark_and_boolean_values() {
    fn table() -> MapiObject {
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: true,
            columns: vec![PID_TAG_SUBJECT_W],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 1,
        }
    }
    fn request(origin: u8, want_row_moved_count: u8) -> RopRequest {
        let mut payload = vec![origin];
        payload.extend_from_slice(&0i32.to_le_bytes());
        payload.push(want_row_moved_count);
        RopRequest {
            rop_id: RopId::SeekRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        }
    }

    for valid in [
        request(0x00, 0x00),
        request(0x01, 0x01),
        request(0x02, 0x01),
    ] {
        let mut table = table();
        let response = rop_seek_row_response(
            &valid,
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(&response[..6], &[0x18, 0x00, 0, 0, 0, 0]);
    }

    for invalid in [request(0x03, 0x01), request(0x01, 0x02)] {
        let mut table = table();
        let response = rop_seek_row_response(
            &invalid,
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(&response[..2], &[0x18, 0x00]);
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8007_0057
        );
        assert_eq!(table_position(&table), Some(1));
    }
}

#[test]
fn seek_row_bookmark_request_validation_matches_microsoft_boolean_values() {
    let bookmark = 1u32.to_le_bytes().to_vec();
    let mut bookmarks = HashMap::new();
    bookmarks.insert(
        bookmark.clone(),
        TableBookmark {
            position: 1,
            row_key: None,
        },
    );
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks,
        next_bookmark: 2,
        position: 1,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    payload.extend_from_slice(&bookmark);
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0x02);
    let request = RopRequest {
        rop_id: RopId::SeekRowBookmark.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_seek_row_bookmark_response(
        &request,
        Some(&mut table),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        Uuid::nil(),
    );

    assert_eq!(&response[..2], &[0x19, 0x00]);
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8007_0057
    );
    assert_eq!(table_position(&table), Some(1));
}

#[test]
fn query_position_counts_categorized_content_rows() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let first_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let second_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 901,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 902,
        ),
    );
    let mut first = test_table_email(first_id, mailbox_id, "Alpha");
    first.categories = vec!["Blue".to_string()];
    let mut second = test_table_email(second_id, mailbox_id, "Beta");
    second.categories = vec!["Green".to_string()];
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 2,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let emails = vec![first, second];
    let snapshot = MapiMailStoreSnapshot::empty();
    let table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_NAME_KEYWORDS_TAG,
            order: 0,
        }],
        category_count: 1,
        expanded_count: 1,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };

    let response = rop_query_position_response(
        &RopRequest {
            rop_id: RopId::QueryPosition.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        },
        Some(&table),
        &mailboxes,
        &emails,
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::QueryPosition.as_u8());
    assert_eq!(u32::from_le_bytes(response[6..10].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(response[10..14].try_into().unwrap()), 4);
}

#[test]
fn categorized_keywords_project_multivalue_instances_and_table_row_metadata() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let email_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 903,
        ),
    );
    let mut email = test_table_email(email_id, mailbox_id, "Categorized");
    email.categories = vec!["Blue".to_string(), "Customer".to_string()];
    email.unread = true;
    let columns = [
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_ROW_TYPE,
        PID_TAG_DEPTH,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_NAME_KEYWORDS_TAG,
        PID_TAG_SUBJECT_W,
    ];

    let rows = categorized_email_rows(
        INBOX_FOLDER_ID,
        vec![&email],
        &columns,
        &[MapiSortOrder {
            property_tag: PID_NAME_KEYWORDS_TAG,
            order: 0,
        }],
        1,
        &HashSet::new(),
    );

    assert_eq!(rows.len(), 4);
    assert_category_header_row(&rows[0].row, "Blue", 1, 1, TABLE_EXPANDED_CATEGORY);
    assert_category_leaf_row(&rows[1].row, &email, 1, "Blue");
    assert_category_header_row(&rows[2].row, "Customer", 1, 1, TABLE_EXPANDED_CATEGORY);
    assert_category_leaf_row(&rows[3].row, &email, 2, "Customer");
}

#[test]
fn microsoft_oxctabl_category_values_preserve_all_multistring_instances() {
    assert_eq!(
        category_values_from_mapi_value(MapiValue::MultiString(vec![
            " Category1 ".to_string(),
            String::new(),
            "Category2".to_string(),
        ])),
        vec!["Category1".to_string(), "Category2".to_string()]
    );
    assert_eq!(
        category_values_from_mapi_value(MapiValue::MultiString(vec![
            String::new(),
            " ".to_string(),
        ])),
        vec![String::new()]
    );
}

fn assert_category_header_row(
    row: &[u8],
    category: &str,
    content_count: u32,
    unread_count: u32,
    row_type: u32,
) {
    let mut cursor = Cursor::new(row);
    parse_mapi_property_value(&mut cursor, PID_TAG_INST_ID).unwrap();
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_INSTANCE_NUM).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ROW_TYPE).unwrap(),
        MapiValue::I32(row_type as i32)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_DEPTH).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_COUNT).unwrap(),
        MapiValue::I32(content_count as i32)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_UNREAD_COUNT).unwrap(),
        MapiValue::I32(unread_count as i32)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_NAME_KEYWORDS_TAG).unwrap(),
        MapiValue::MultiString(vec![category.to_string()])
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_W).unwrap(),
        MapiValue::String(String::new())
    );
}

fn assert_category_leaf_row(row: &[u8], email: &JmapEmail, instance_num: u32, category: &str) {
    let mut cursor = Cursor::new(row);
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_INST_ID).unwrap(),
        MapiValue::I64(mapi_message_id(email) as i64)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_INSTANCE_NUM).unwrap(),
        MapiValue::I32(instance_num as i32)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ROW_TYPE).unwrap(),
        MapiValue::I32(TABLE_LEAF_ROW as i32)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_DEPTH).unwrap(),
        MapiValue::I32(1)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_COUNT).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_CONTENT_UNREAD_COUNT).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_NAME_KEYWORDS_TAG).unwrap(),
        MapiValue::MultiString(vec![category.to_string()])
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_W).unwrap(),
        MapiValue::String("Categorized".to_string())
    );
}

#[test]
fn mapi_hierarchy_row_projects_inbox_display_name() {
    let inbox = JmapMailbox {
        id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "INBOX".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 18,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    let row =
        serialize_folder_row_with_context(&inbox, &[], &[PID_TAG_DISPLAY_NAME_W], Uuid::nil());

    assert!(utf16_position(&row, "INBOX").is_none());
    assert_response_contains_utf16(&row, "Inbox");
}

#[test]
fn microsoft_oxcfold_hierarchy_row_projects_folder_message_size_columns() {
    let inbox = JmapMailbox {
        id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 18,
        unread_emails: 0,
        size_octets: u64::from(u32::MAX) + 10,
        is_subscribed: true,
    };

    let row = serialize_folder_row_with_context(
        &inbox,
        &[],
        &[PID_TAG_MESSAGE_SIZE, PID_TAG_MESSAGE_SIZE_EXTENDED],
        Uuid::nil(),
    );
    let mut cursor = Cursor::new(&row);

    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE).unwrap(),
        MapiValue::I32(-1)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE_EXTENDED).unwrap(),
        MapiValue::I64(i64::from(u32::MAX) + 10)
    );
}

#[test]
fn ipm_subtree_row_projects_principal_ost_identity_when_available() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let row = serialize_special_folder_row(IPM_SUBTREE_FOLDER_ID, &[], &[PID_TAG_OST_OSTID], None);
    assert_eq!(u16::from_le_bytes(row[0..2].try_into().unwrap()), 0);

    let row = serialize_special_folder_row(
        IPM_SUBTREE_FOLDER_ID,
        &[],
        &[PID_TAG_OST_OSTID],
        Some(&principal),
    );
    assert_eq!(u16::from_le_bytes(row[0..2].try_into().unwrap()), 20);
    assert_eq!(&row[2..18], principal.account_id.as_bytes());
    assert_eq!(u32::from_le_bytes(row[18..22].try_into().unwrap()), 1);
}

#[test]
fn root_and_ipm_subtree_rows_project_entry_id_identity() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };

    for folder_id in [ROOT_FOLDER_ID, IPM_SUBTREE_FOLDER_ID] {
        let row = serialize_special_folder_row(
            folder_id,
            &[],
            &[PID_TAG_ENTRY_ID, PID_TAG_INSTANCE_KEY],
            Some(&principal),
        );
        let entry_id_len = u16::from_le_bytes(row[0..2].try_into().unwrap()) as usize;
        let entry_id = &row[2..2 + entry_id_len];
        let instance_key_offset = 2 + entry_id_len;
        let instance_key_len = u16::from_le_bytes(
            row[instance_key_offset..instance_key_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let instance_key =
            &row[instance_key_offset + 2..instance_key_offset + 2 + instance_key_len];

        assert_eq!(entry_id_len, 46);
        assert_eq!(
            crate::mapi::identity::object_id_from_folder_entry_id(entry_id),
            Some(folder_id)
        );
        assert_eq!(
            instance_key,
            crate::mapi::identity::instance_key_for_object_id(folder_id)
        );
    }
}

#[test]
fn ipm_subtree_hierarchy_restrictions_match_serialized_display_name() {
    let restriction = MapiRestriction::Content {
        property_tag: PID_TAG_DISPLAY_NAME_W,
        value: "Top of Information Store".to_string(),
        fuzzy_level_low: 0x0001,
        fuzzy_level_high: 0x0001,
    };

    assert!(special_hierarchy_row_matches(
        IPM_SUBTREE_FOLDER_ID,
        Some(&restriction),
        Uuid::nil()
    ));
    assert_eq!(
        special_folder_property_value(IPM_SUBTREE_FOLDER_ID, PID_TAG_DISPLAY_NAME_W, Uuid::nil()),
        Some(MapiValue::String("Top of Information Store".to_string()))
    );
}

#[test]
fn folder_type_rows_follow_microsoft_values() {
    let mailbox = JmapMailbox {
        id: Uuid::nil(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    let mailbox_row =
        serialize_folder_row_with_context(&mailbox, &[], &[PID_TAG_FOLDER_TYPE], Uuid::nil());
    assert_eq!(
        u32::from_le_bytes(mailbox_row.try_into().unwrap()),
        FOLDER_GENERIC
    );

    let root_row = serialize_special_folder_row(ROOT_FOLDER_ID, &[], &[PID_TAG_FOLDER_TYPE], None);
    assert_eq!(
        u32::from_le_bytes(root_row.try_into().unwrap()),
        FOLDER_ROOT
    );

    let ipm_row =
        serialize_special_folder_row(IPM_SUBTREE_FOLDER_ID, &[], &[PID_TAG_FOLDER_TYPE], None);
    assert_eq!(
        u32::from_le_bytes(ipm_row.try_into().unwrap()),
        FOLDER_GENERIC
    );

    let finder_root_row =
        serialize_special_folder_row(SEARCH_FOLDER_ID, &[], &[PID_TAG_FOLDER_TYPE], None);
    assert_eq!(
        u32::from_le_bytes(finder_root_row.try_into().unwrap()),
        FOLDER_SEARCH
    );

    for folder_id in [
        CONTACTS_SEARCH_FOLDER_ID,
        REMINDERS_FOLDER_ID,
        TRACKED_MAIL_PROCESSING_FOLDER_ID,
        TODO_SEARCH_FOLDER_ID,
    ] {
        let search_row = serialize_special_folder_row(folder_id, &[], &[PID_TAG_FOLDER_TYPE], None);
        assert_eq!(
            u32::from_le_bytes(search_row.try_into().unwrap()),
            FOLDER_SEARCH
        );
    }
}

#[test]
fn hierarchy_table_projects_user_saved_search_folder() {
    let definition_id = Uuid::parse_str("aaaaaaaa-5556-4111-8111-aaaaaaaaaaaa").unwrap();
    let folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_1124);
    crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::nil(),
        role: "custom".to_string(),
        display_name: "Unread from Alice".to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({"kind": "text", "query": "alice"}),
        excluded_folder_roles: vec!["trash".to_string()],
        is_builtin: false,
    }]);
    let mailboxes = snapshot.mailboxes();
    let rows = hierarchy_rows(
        IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    let row = rows
        .iter()
        .find(|row| hierarchy_row_id(row) == folder_id)
        .expect("search folder hierarchy row");

    assert_eq!(hierarchy_row_display_name(row), "Unread from Alice");
    let serialized = serialize_hierarchy_row(
        *row,
        &mailboxes,
        &snapshot,
        &[
            PID_TAG_FOLDER_TYPE,
            PID_TAG_PARENT_FOLDER_ID,
            PID_TAG_CONTAINER_CLASS_W,
        ],
        Uuid::nil(),
    );
    assert_eq!(
        u32::from_le_bytes(serialized[0..4].try_into().unwrap()),
        FOLDER_SEARCH
    );
    let mailbox = match row {
        HierarchyRow::Mailbox(mailbox) => mailbox,
        _ => panic!("expected mailbox-backed search folder row"),
    };
    assert_eq!(mapi_parent_folder_id(mailbox), IPM_SUBTREE_FOLDER_ID);
    let class = "IPF.Note"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    assert!(serialized
        .windows(class.len())
        .any(|window| window == class));
}

#[test]
fn custom_collaboration_folders_are_only_ipm_subtree_children() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x7FFF_1000_1130);
    let collection = CollaborationCollection {
        id: "project-calendar".to_string(),
        kind: "calendar".to_string(),
        owner_account_id: Uuid::nil(),
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        display_name: "Project Calendar".to_string(),
        is_owned: true,
        rights: CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        },
    };
    crate::mapi::identity::remember_mapi_identity(
        crate::mapi_store::collaboration_folder_identity_canonical_id(
            crate::mapi_store::MapiCollaborationFolderKind::Calendar,
            &collection,
        )
        .unwrap(),
        folder_id,
    );
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![collection],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let mailboxes = snapshot.mailboxes();

    let ipm_rows = hierarchy_rows(
        IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    assert!(ipm_rows
        .iter()
        .any(|row| hierarchy_row_id(row) == folder_id));

    let root_rows = hierarchy_rows(
        ROOT_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    assert!(!root_rows
        .iter()
        .any(|row| hierarchy_row_id(row) == folder_id));
}

#[test]
fn ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders() {
    let shadow_id = Uuid::parse_str("aaaaaaaa-5555-4111-8111-aaaaaaaaaaaa").unwrap();
    let suggested_shadow_id = Uuid::parse_str("aaaaaaaa-6666-4111-8111-aaaaaaaaaaaa").unwrap();
    let quick_contacts_shadow_id = Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaaa").unwrap();
    let im_contacts_shadow_id = Uuid::parse_str("aaaaaaaa-8888-4111-8111-aaaaaaaaaaaa").unwrap();
    let tasks_shadow_id = Uuid::parse_str("aaaaaaaa-9999-4111-8111-aaaaaaaaaaaa").unwrap();
    let quick_step_shadow_id = Uuid::parse_str("aaaaaaaa-aaaa-4111-8111-aaaaaaaaaaaa").unwrap();
    let conversation_history_shadow_id =
        Uuid::parse_str("aaaaaaaa-bbbb-4111-8111-aaaaaaaaaaaa").unwrap();
    let shadow_folder_id = crate::mapi::identity::mapi_store_id(0x4f);
    let suggested_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x54);
    let quick_contacts_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x55);
    let im_contacts_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x56);
    let tasks_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x57);
    let quick_step_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x58);
    let conversation_history_shadow_folder_id = crate::mapi::identity::mapi_store_id(0x59);
    crate::mapi::identity::remember_mapi_identity(shadow_id, shadow_folder_id);
    crate::mapi::identity::remember_mapi_identity(suggested_shadow_id, suggested_shadow_folder_id);
    crate::mapi::identity::remember_mapi_identity(
        quick_contacts_shadow_id,
        quick_contacts_shadow_folder_id,
    );
    crate::mapi::identity::remember_mapi_identity(
        im_contacts_shadow_id,
        im_contacts_shadow_folder_id,
    );
    crate::mapi::identity::remember_mapi_identity(tasks_shadow_id, tasks_shadow_folder_id);
    crate::mapi::identity::remember_mapi_identity(
        quick_step_shadow_id,
        quick_step_shadow_folder_id,
    );
    crate::mapi::identity::remember_mapi_identity(
        conversation_history_shadow_id,
        conversation_history_shadow_folder_id,
    );
    let mailboxes = vec![
        JmapMailbox {
            id: shadow_id,
            parent_id: None,
            role: String::new(),
            name: "Calendar".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: suggested_shadow_id,
            parent_id: None,
            role: String::new(),
            name: "Suggested Contacts".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: quick_contacts_shadow_id,
            parent_id: None,
            role: "contacts".to_string(),
            name: "Quick Contacts".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: im_contacts_shadow_id,
            parent_id: None,
            role: "contacts".to_string(),
            name: "IM Contact List".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: tasks_shadow_id,
            parent_id: None,
            role: "tasks".to_string(),
            name: "Tasks".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: quick_step_shadow_id,
            parent_id: None,
            role: String::new(),
            name: "Quick Step Settings".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: conversation_history_shadow_id,
            parent_id: None,
            role: "conversation_history".to_string(),
            name: "Conversation History".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
    ];
    let task_collection = CollaborationCollection {
        id: "default".to_string(),
        kind: "tasks".to_string(),
        owner_account_id: Uuid::nil(),
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        display_name: "Tasks".to_string(),
        is_owned: true,
        rights: CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        },
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![task_collection],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let rows = hierarchy_rows(
        IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
    );
    let row_ids = rows.iter().map(hierarchy_row_id).collect::<Vec<_>>();
    assert!(row_ids.contains(&CALENDAR_FOLDER_ID));
    assert!(row_ids.contains(&SUGGESTED_CONTACTS_FOLDER_ID));
    assert!(row_ids.contains(&TASKS_FOLDER_ID));
    assert!(!row_ids.contains(&QUICK_CONTACTS_FOLDER_ID));
    assert!(!row_ids.contains(&IM_CONTACT_LIST_FOLDER_ID));
    assert!(!row_ids.contains(&CONVERSATION_ACTION_SETTINGS_FOLDER_ID));
    assert!(!row_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
    assert!(!row_ids.contains(&shadow_folder_id));
    assert!(!row_ids.contains(&suggested_shadow_folder_id));
    assert!(!row_ids.contains(&quick_contacts_shadow_folder_id));
    assert!(!row_ids.contains(&im_contacts_shadow_folder_id));
    assert!(!row_ids.contains(&tasks_shadow_folder_id));
    assert!(!row_ids.contains(&quick_step_shadow_folder_id));
    assert!(!row_ids.contains(&conversation_history_shadow_folder_id));
    assert_eq!(
        rows.iter()
            .filter(|row| hierarchy_row_display_name(row) == "Tasks")
            .count(),
        1
    );

    let sync_ids = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes)
        .iter()
        .map(mapi_folder_id)
        .collect::<Vec<_>>();
    assert!(sync_ids.contains(&CALENDAR_FOLDER_ID));
    assert!(sync_ids.contains(&SUGGESTED_CONTACTS_FOLDER_ID));
    assert!(sync_ids.contains(&TASKS_FOLDER_ID));
    assert!(!sync_ids.contains(&QUICK_CONTACTS_FOLDER_ID));
    assert!(!sync_ids.contains(&IM_CONTACT_LIST_FOLDER_ID));
    assert!(!sync_ids.contains(&CONVERSATION_ACTION_SETTINGS_FOLDER_ID));
    assert!(!sync_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
    assert!(!sync_ids.contains(&shadow_folder_id));
    assert!(!sync_ids.contains(&suggested_shadow_folder_id));
    assert!(!sync_ids.contains(&quick_contacts_shadow_folder_id));
    assert!(!sync_ids.contains(&im_contacts_shadow_folder_id));
    assert!(!sync_ids.contains(&tasks_shadow_folder_id));
    assert!(!sync_ids.contains(&quick_step_shadow_folder_id));
    assert!(!sync_ids.contains(&conversation_history_shadow_folder_id));

    let calendar_row = rows
        .iter()
        .find(|row| hierarchy_row_id(row) == CALENDAR_FOLDER_ID)
        .expect("calendar special folder row");
    let serialized = serialize_hierarchy_row(
        *calendar_row,
        &mailboxes,
        &snapshot,
        &[PID_TAG_CONTAINER_CLASS_W],
        Uuid::nil(),
    );
    let class = "IPF.Appointment"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    assert!(serialized
        .windows(class.len())
        .any(|window| window == class));

    for (folder_id, expected) in [(TASKS_FOLDER_ID, "IPF.Task")] {
        let row = rows
            .iter()
            .find(|row| hierarchy_row_id(row) == folder_id)
            .expect("special folder row");
        let serialized = serialize_hierarchy_row(
            *row,
            &mailboxes,
            &snapshot,
            &[PID_TAG_CONTAINER_CLASS_W],
            Uuid::nil(),
        );
        let class = expected
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert!(serialized
            .windows(class.len())
            .any(|window| window == class));
    }
    for (folder_id, expected) in [
        (QUICK_CONTACTS_FOLDER_ID, "IPF.Contact.MOC.QuickContacts"),
        (IM_CONTACT_LIST_FOLDER_ID, "IPF.Contact.MOC.ImContactList"),
    ] {
        let serialized = serialize_advertised_special_folder_row_with_mailbox_guid(
            folder_id,
            &[PID_TAG_CONTAINER_CLASS_W],
            Uuid::nil(),
        );
        let class = expected
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        assert!(serialized
            .windows(class.len())
            .any(|window| window == class));
    }

    assert_eq!(
        folder_message_class(
            &mapi_mailstore::virtual_special_mailbox(QUICK_CONTACTS_FOLDER_ID)
                .expect("quick contacts virtual mailbox")
        ),
        "IPF.Contact.MOC.QuickContacts"
    );
}

#[test]
fn deleted_advertised_quick_step_folder_unshadows_real_folder_in_hierarchy() {
    let quick_step_id = Uuid::parse_str("99999999-9999-4999-9999-999999999999").unwrap();
    let quick_step_folder_id = crate::mapi::identity::mapi_store_id(0x99);
    crate::mapi::identity::remember_mapi_identity(quick_step_id, quick_step_folder_id);
    let quick_step = JmapMailbox {
        id: quick_step_id,
        parent_id: None,
        role: "custom".to_string(),
        name: "Quick Step Settings".to_string(),
        sort_order: 40,
        modseq: 40,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [quick_step];
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut deleted = HashSet::new();
    deleted.insert(QUICK_STEP_SETTINGS_FOLDER_ID);

    let rows = hierarchy_rows_excluding_deleted(
        IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &snapshot,
        None,
        &[],
        Uuid::nil(),
        &deleted,
    );
    let row_ids = rows.iter().map(hierarchy_row_id).collect::<Vec<_>>();

    assert!(!row_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
    assert!(row_ids.contains(&quick_step_folder_id));
}

#[test]
fn real_quick_step_folder_projects_configuration_class() {
    let quick_step_id = Uuid::parse_str("99999999-9999-4999-9999-999999999998").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        quick_step_id,
        crate::mapi::identity::mapi_store_id(0x97),
    );
    let quick_step = JmapMailbox {
        id: quick_step_id,
        parent_id: None,
        role: "custom".to_string(),
        name: "Quick Step Settings".to_string(),
        sort_order: 40,
        modseq: 40,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    assert_eq!(folder_message_class(&quick_step), "IPF.Configuration");
    assert_eq!(
        mailbox_property_value_with_context(
            &quick_step,
            std::slice::from_ref(&quick_step),
            PID_TAG_ATTRIBUTE_HIDDEN,
        ),
        Some(MapiValue::Bool(true))
    );
    assert_eq!(
        mailbox_property_value_with_context(
            &quick_step,
            std::slice::from_ref(&quick_step),
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ),
        Some(MapiValue::String("IPM.Configuration".to_string()))
    );

    let row = serialize_folder_row_with_context(
        &quick_step,
        std::slice::from_ref(&quick_step),
        &[
            PID_TAG_ATTRIBUTE_HIDDEN,
            PID_TAG_CONTAINER_CLASS_W,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ],
        Uuid::nil(),
    );
    let container_class = "IPF.Configuration"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    let default_post_class = "IPM.Configuration"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();

    assert_eq!(row.first(), Some(&1));
    assert!(row
        .windows(container_class.len())
        .any(|window| window == container_class));
    assert!(row
        .windows(default_post_class.len())
        .any(|window| window == default_post_class));
}

#[test]
fn deleted_advertised_quick_step_folder_is_excluded_from_hierarchy_sync() {
    let quick_step_id = Uuid::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
    let quick_step_folder_id = crate::mapi::identity::mapi_store_id(0x98);
    crate::mapi::identity::remember_mapi_identity(quick_step_id, quick_step_folder_id);
    let quick_step = JmapMailbox {
        id: quick_step_id,
        parent_id: None,
        role: "custom".to_string(),
        name: "Quick Step Settings".to_string(),
        sort_order: 40,
        modseq: 40,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let mailboxes = [quick_step];
    let mut deleted = HashSet::new();
    deleted.insert(QUICK_STEP_SETTINGS_FOLDER_ID);

    let sync_ids =
        sync_mailboxes_for_excluding_deleted(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes, &deleted)
            .iter()
            .map(mapi_folder_id)
            .collect::<Vec<_>>();

    assert!(!sync_ids.contains(&QUICK_STEP_SETTINGS_FOLDER_ID));
    assert!(sync_ids.contains(&quick_step_folder_id));
}

#[test]
fn rule_table_projects_canonical_sieve_rule() {
    let rule_id = Uuid::parse_str("aaaaaaaa-4444-4111-8111-aaaaaaaaaaaa").unwrap();
    let object_id = crate::mapi::identity::mapi_store_id(125);
    crate::mapi::identity::remember_mapi_identity(rule_id, object_id);
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_rules(vec![MailboxRule {
        id: rule_id,
        name: "Reports".to_string(),
        is_active: true,
        source_kind: "sieve_script".to_string(),
        condition_summary: "header Subject contains report".to_string(),
        action_summary: "fileinto Reports".to_string(),
        supported_outlook_projection: true,
        unsupported_exchange_features: vec!["deferred_action_messages".to_string()],
        size_octets: 128,
        updated_at: "2026-05-28T08:00:00Z".to_string(),
    }]);

    let row = serialize_rule_row(
        &snapshot.rules()[0],
        &[
            PID_TAG_RULE_ID,
            PID_TAG_RULE_STATE,
            PID_TAG_RULE_PROVIDER,
            PID_TAG_RULE_NAME,
            PID_TAG_RULE_PROVIDER_DATA,
        ],
    );
    assert_eq!(u64::from_le_bytes(row[0..8].try_into().unwrap()), object_id);
    assert_eq!(
        u32::from_le_bytes(row[8..12].try_into().unwrap()),
        ST_ENABLED
    );
    let provider = "LPE Sieve"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    let name = "Reports"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    assert!(row.windows(provider.len()).any(|window| window == provider));
    assert!(row.windows(name.len()).any(|window| window == name));
    assert!(String::from_utf8_lossy(&row).contains("fileinto Reports"));
}

#[test]
fn microsoft_oxosrch_common_views_projects_search_folder_definition_messages() {
    let definition_id = Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        definition_id,
        crate::mapi::identity::mapi_store_id(123),
    );
    let mut definition_blob = vec![
        0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    ];
    definition_blob.extend_from_slice(&1u32.to_le_bytes());
    definition_blob.push(0xAA);
    definition_blob.extend_from_slice(&0u32.to_le_bytes());
    definition_blob.push(0xBB);
    definition_blob.extend_from_slice(&0u32.to_le_bytes());
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![SearchFolderDefinition {
        id: definition_id,
        account_id: Uuid::nil(),
        role: "reminders".to_string(),
        display_name: "Reminders".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
        restriction_json: serde_json::json!({
            "kind": "exchange_reminders",
            "pidTagSearchFolderDefinition": BASE64_STANDARD.encode(&definition_blob)
        }),
        excluded_folder_roles: exchange_builtin_excluded_folder_roles(),
        is_builtin: true,
    }]);
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: vec![
            PID_TAG_MID,
            PID_TAG_ASSOCIATED,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_SEARCH_FOLDER_ID,
            PID_TAG_SEARCH_FOLDER_TEMPLATE_ID,
            PID_TAG_SEARCH_FOLDER_STORAGE_TYPE,
            PID_TAG_SEARCH_FOLDER_TAG,
            PID_TAG_SEARCH_FOLDER_EFP_FLAGS,
            PID_TAG_SEARCH_FOLDER_DEFINITION,
        ],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: 0x15,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 20, 0],
    };

    assert_eq!(
        associated_folder_message_count(COMMON_VIEWS_FOLDER_ID, &snapshot),
        7
    );
    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], 0x15);
    assert_eq!(u16::from_le_bytes(response[7..9].try_into().unwrap()), 7);
    let mut shortcut_class = Vec::new();
    for code_unit in "IPM.Microsoft.WunderBar.Link".encode_utf16() {
        shortcut_class.extend_from_slice(&code_unit.to_le_bytes());
    }
    let mut search_class = Vec::new();
    for code_unit in "IPM.Microsoft.WunderBar.SFInfo".encode_utf16() {
        search_class.extend_from_slice(&code_unit.to_le_bytes());
    }
    let mut named_view_class = Vec::new();
    for code_unit in "IPM.Microsoft.FolderDesign.NamedView".encode_utf16() {
        named_view_class.extend_from_slice(&code_unit.to_le_bytes());
    }
    assert!(response
        .windows(shortcut_class.len())
        .any(|window| window == shortcut_class.as_slice()));
    assert!(response
        .windows(search_class.len())
        .any(|window| window == search_class.as_slice()));
    assert!(response
        .windows(16)
        .any(|window| window == definition_id.as_bytes()));
    assert!(response
        .windows(4)
        .any(|window| window == 0x48u32.to_le_bytes()));
}

#[test]
fn common_views_default_columns_are_navigation_shortcut_columns() {
    let columns = default_navigation_shortcut_property_tags();

    assert!(columns.contains(&PID_TAG_WLINK_ENTRY_ID));
    assert!(columns.contains(&PID_TAG_WLINK_FOLDER_TYPE));
    assert!(columns.contains(&PID_TAG_WLINK_CALENDAR_COLOR));
    assert!(columns.contains(&PID_TAG_WLINK_ADDRESS_BOOK_EID));
    assert!(columns.contains(&PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID));
    assert!(columns.contains(&PID_TAG_WLINK_CLIENT_ID));
    assert!(columns.contains(&PID_TAG_WLINK_RO_GROUP_TYPE));
    assert!(columns.contains(&PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG));
    assert!(!columns.contains(&0x6842_0003));
    assert!(!columns.contains(&0x6845_0102));
}

#[test]
fn navigation_shortcut_parser_accepts_binary_wlink_group_ids() {
    let group_id = Uuid::from_bytes([0x33; 16]);
    let mut header_properties = HashMap::new();
    header_properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("My Calendars".to_string()),
    );
    header_properties.insert(PID_TAG_WLINK_TYPE, MapiValue::U32(4));
    header_properties.insert(0x6842_0102, MapiValue::Binary(group_id.as_bytes().to_vec()));

    let header = navigation_shortcut_from_mapi_properties(Uuid::nil(), None, &header_properties);

    assert_eq!(header.shortcut_type, 4);
    assert_eq!(header.group_header_id, Some(group_id));

    let mut link_properties = HashMap::new();
    link_properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Calendar".to_string()));
    link_properties.insert(PID_TAG_WLINK_TYPE, MapiValue::U32(0));
    link_properties.insert(0x6850_0102, MapiValue::Binary(group_id.as_bytes().to_vec()));

    let link = navigation_shortcut_from_mapi_properties(Uuid::nil(), None, &link_properties);

    assert_eq!(link.shortcut_type, 0);
    assert_eq!(link.group_header_id, Some(group_id));
}

#[test]
fn navigation_shortcut_parser_decodes_typed_and_wrapped_entry_id() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let inbox_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID).unwrap();
    let mut wrapped_entry_id = vec![0xaa; 17];
    wrapped_entry_id.extend_from_slice(&inbox_entry_id);
    wrapped_entry_id.extend_from_slice(&[0xbb; 13]);
    let mut properties = HashMap::new();
    properties.insert(PID_TAG_SUBJECT_W, MapiValue::String("Inbox".to_string()));
    properties.insert(PID_TAG_WLINK_TYPE, MapiValue::U32(0));
    properties.insert(0x684C_0102, MapiValue::Binary(wrapped_entry_id));
    properties.insert(0x6850_0102, MapiValue::Binary([0x44; 16].to_vec()));

    let shortcut = navigation_shortcut_from_mapi_properties(account_id, None, &properties);

    assert_eq!(shortcut.target_folder_id, Some(INBOX_FOLDER_ID));
    assert_eq!(shortcut.group_header_id, Some(Uuid::from_bytes([0x44; 16])));
}

#[test]
fn find_row_request_validation_matches_microsoft_flags() {
    fn request(flags: u8) -> RopRequest {
        let mut restriction = vec![MapiRestrictionType::Exist as u8];
        restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
        let mut payload = vec![flags];
        payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
        payload.extend_from_slice(&restriction);
        payload.push(1);
        payload.extend_from_slice(&0u16.to_le_bytes());
        RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        }
    }
    fn table() -> MapiObject {
        MapiObject::HierarchyTable {
            folder_id: SYNC_ISSUES_FOLDER_ID,
            columns: vec![PID_TAG_DISPLAY_NAME_W],
            columns_set: true,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            deleted_advertised_special_folders: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        }
    }

    for valid in [request(0x00), request(0x01)] {
        let mut table = table();
        let response = rop_find_row_response(
            &valid,
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_ne!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8007_0057
        );
    }

    for invalid in [request(0x02), request(0x80)] {
        let mut table = table();
        let response = rop_find_row_response(
            &invalid,
            Some(&mut table),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        );
        assert_eq!(&response[..2], &[0x4F, 0x00]);
        assert_eq!(
            u32::from_le_bytes(response[2..6].try_into().unwrap()),
            0x8007_0057
        );
    }
}

#[test]
fn common_views_find_row_honors_restriction() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000002);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 102,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id,
            subject: "Pinned Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0x81,
            group_header_id: Some(default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    write_utf16z(&mut restriction, "Archive");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
    assert_eq!(response.len(), 6);
    assert_eq!(table_position(&table), Some(0));
}

#[test]
fn contents_find_row_matches_message_search_key() {
    let mailbox_id = Uuid::from_u128(0x3333);
    let email_id = Uuid::from_u128(0x4444);
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 444,
        ),
    );
    let search_key = crate::mapi_mailstore::source_key_for_uuid(&email_id);
    let email = JmapEmail {
        id: email_id,
        thread_id: Uuid::from_u128(0x5555),
        mailbox_id,
        mailbox_role: "sent".to_string(),
        mailbox_name: "Sent".to_string(),
        modseq: 7,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
            mailbox_id,
            role: "sent".to_string(),
            name: "Sent".to_string(),
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
        sent_at: Some("2026-05-20T10:00:00Z".to_string()),
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Search key probe".to_string(),
        preview: "Preview".to_string(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
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
        internet_message_id: Some("<search-key-probe@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };
    let mut table = MapiObject::ContentsTable {
        folder_id: SENT_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SEARCH_KEY, PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_SEARCH_KEY.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SEARCH_KEY.to_le_bytes());
    write_rop_binary(&mut restriction, &search_key);
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_find_row_response(
        &request,
        Some(&mut table),
        &[],
        &[email],
        &MapiMailStoreSnapshot::empty(),
        Uuid::nil(),
    );

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    assert!(response
        .windows(search_key.len())
        .any(|window| window == search_key.as_slice()));
}

#[test]
fn contacts_contents_find_row_matches_display_name() {
    let account_id = Uuid::from_u128(0x7171);
    let contact_id = Uuid::from_u128(0x7172);
    crate::mapi::identity::remember_mapi_identity(
        contact_id,
        crate::mapi::identity::mapi_store_id(0x7172),
    );
    let rights = CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: true,
    };
    let collection = CollaborationCollection {
        id: "default".to_string(),
        kind: "contacts".to_string(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        display_name: "Contacts".to_string(),
        is_owned: true,
        rights: rights.clone(),
    };
    let contact = AccessibleContact {
        id: contact_id,
        collection_id: collection.id.clone(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        rights,
        name: "Denis Ducret".to_string(),
        role: String::new(),
        email: "denis@example.test".to_string(),
        phone: String::new(),
        team: String::new(),
        notes: String::new(),
        ..Default::default()
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![collection],
        Vec::new(),
        Vec::new(),
        vec![contact],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let mut table = MapiObject::ContentsTable {
        folder_id: CONTACTS_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_MID, PID_TAG_DISPLAY_NAME_W, PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
    write_utf16z(&mut restriction, "Denis Ducret");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    let name = "Denis Ducret"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    assert!(response
        .windows(name.len())
        .any(|window| window == name.as_slice()));
}

#[test]
fn calendar_contents_find_row_matches_outlook_date_window() {
    let account_id = Uuid::from_u128(0x8181);
    let event_id = Uuid::from_u128(0x8182);
    crate::mapi::identity::remember_mapi_identity(
        event_id,
        crate::mapi::identity::mapi_store_id(0x8182),
    );
    let mut event = default_event_for_mapping(account_id, "default");
    event.id = event_id;
    event.title = "Project review".to_string();
    event.date = "2026-06-01".to_string();
    event.time = "10:00".to_string();
    event.duration_minutes = 60;
    let start = match event_property_value(
        &event,
        mapi_item_id(&event.id),
        CALENDAR_FOLDER_ID,
        PID_LID_APPOINTMENT_START_WHOLE_TAG,
    ) {
        Some(MapiValue::I64(value)) => value,
        _ => panic!("event start filetime missing"),
    };
    let end = match event_property_value(
        &event,
        mapi_item_id(&event.id),
        CALENDAR_FOLDER_ID,
        PID_LID_APPOINTMENT_END_WHOLE_TAG,
    ) {
        Some(MapiValue::I64(value)) => value,
        _ => panic!("event end filetime missing"),
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![event],
        Vec::new(),
        Vec::new(),
    );
    let mut table = MapiObject::ContentsTable {
        folder_id: CALENDAR_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Or as u8];
    restriction.extend_from_slice(&2u16.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&0x8021_000Bu32.to_le_bytes());
    restriction.extend_from_slice(&0x8021_000Bu32.to_le_bytes());
    restriction.push(1);
    restriction.push(MapiRestrictionType::And as u8);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x03);
    restriction.extend_from_slice(&PID_LID_APPOINTMENT_START_WHOLE_TAG.to_le_bytes());
    restriction.extend_from_slice(&PID_LID_APPOINTMENT_START_WHOLE_TAG.to_le_bytes());
    restriction.extend_from_slice(&(start - 1).to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x01);
    restriction.extend_from_slice(&PID_LID_APPOINTMENT_END_WHOLE_TAG.to_le_bytes());
    restriction.extend_from_slice(&PID_LID_APPOINTMENT_END_WHOLE_TAG.to_le_bytes());
    restriction.extend_from_slice(&(end + 1).to_le_bytes());
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    assert_response_contains_utf16(&response, "Project review");
    assert_response_contains_utf16(&response, "IPM.Appointment");
}

#[test]
fn common_views_find_row_returns_default_compact_named_view() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: vec![
            PID_TAG_SUBJECT_W,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_VIEW_DESCRIPTOR_FLAGS,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
            PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
        ],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::And as u8];
    restriction.extend_from_slice(&5u16.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Microsoft.FolderDesign.NamedView");
    restriction.push(MapiRestrictionType::Bitmask as u8);
    restriction.push(1);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
    restriction.extend_from_slice(&1u32.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
    restriction.extend_from_slice(&8u32.to_le_bytes());
    restriction.push(MapiRestrictionType::Or as u8);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    restriction.push(MapiRestrictionType::Content as u8);
    restriction.extend_from_slice(&0u32.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    write_utf16z(&mut restriction, "Compact");
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
    restriction.extend_from_slice(&14_745_605u32.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
    restriction.extend_from_slice(&16u16.to_le_bytes());
    restriction.extend_from_slice(&[
        0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]);
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(0);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_response_contains_utf16(&response, "Compact");
    assert!(response
        .windows(4)
        .any(|window| window == 14_745_605u32.to_le_bytes()));
}

#[test]
fn common_views_find_row_returns_default_sent_to_named_view() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: vec![
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_SUBJECT_W,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_VIEW_DESCRIPTOR_FLAGS,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
            PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
        ],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::And as u8];
    restriction.extend_from_slice(&5u16.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Microsoft.FolderDesign.NamedView");
    restriction.push(MapiRestrictionType::Bitmask as u8);
    restriction.push(1);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
    restriction.extend_from_slice(&1u32.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
    restriction.extend_from_slice(&8u32.to_le_bytes());
    restriction.push(MapiRestrictionType::Or as u8);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    restriction.push(MapiRestrictionType::Content as u8);
    restriction.extend_from_slice(&0u32.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    write_utf16z(&mut restriction, "Sent To");
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FLAGS.to_le_bytes());
    restriction.extend_from_slice(&15_269_893u32.to_le_bytes());
    restriction.push(MapiRestrictionType::Property as u8);
    restriction.push(0x04);
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE.to_le_bytes());
    restriction.extend_from_slice(&16u16.to_le_bytes());
    restriction.extend_from_slice(&[
        0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]);
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(0);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_response_contains_utf16(&response, "Sent To");
    assert!(response
        .windows(4)
        .any(|window| window == 15_269_893u32.to_le_bytes()));
}

#[test]
fn common_views_find_row_matches_mail_wlink_folder_type() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000003);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 103,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id,
            subject: "Pinned Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0x81,
            group_header_id: Some(default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_SUBJECT_W, 0x684F_0102],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&0x684F_0102u32.to_le_bytes());
    restriction.extend_from_slice(&0x684F_0102u32.to_le_bytes());
    restriction.extend_from_slice(&16u16.to_le_bytes());
    restriction.extend_from_slice(&[
        0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ]);
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    assert_response_contains_utf16(&response, "Pinned Inbox");
}

#[test]
fn common_views_query_rows_uses_account_bound_wlink_entry_ids() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = common_views_sort_snapshot(account_id);
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_WLINK_ENTRY_ID],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 10, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    let expected =
        crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID).unwrap();
    let zero_guid_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(Uuid::nil(), INBOX_FOLDER_ID)
            .unwrap();
    assert!(response
        .windows(expected.len())
        .any(|window| window == expected.as_slice()));
    assert!(!response
        .windows(zero_guid_entry_id.len())
        .any(|window| window == zero_guid_entry_id.as_slice()));
}

#[test]
fn common_views_wlink_query_rows_keep_named_views_without_restriction() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = common_views_sort_snapshot(account_id);
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: default_navigation_shortcut_property_tags(),
        columns_set: true,
        sort_orders: vec![
            MapiSortOrder {
                property_tag: 0x684F_0102,
                order: 0,
            },
            MapiSortOrder {
                property_tag: 0x6850_0102,
                order: 0,
            },
            MapiSortOrder {
                property_tag: 0x684B_0102,
                order: 0,
            },
        ],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 10, 0],
    };

    let (_, projected_total) =
        table_position_and_count(Some(&table), &[], &[], &snapshot, account_id);
    let full_common_views_count = snapshot.common_views_table_messages().count();
    assert_eq!(projected_total, full_common_views_count);

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    assert_response_contains_utf16(&response, "Alpha");
    assert_eq!(
        u16::from_le_bytes(response[7..9].try_into().unwrap()) as usize,
        full_common_views_count
    );
    assert_eq!(response[6], 0x01);
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
    assert!(utf16_position(&response, "Compact").is_some());

    let end_response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    assert_eq!(end_response[6], 0x02);
    assert_eq!(
        u16::from_le_bytes(end_response[7..9].try_into().unwrap()),
        0
    );
}

#[test]
fn common_views_restricted_named_view_query_rows_remain_available() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = common_views_sort_snapshot(account_id);
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: default_navigation_shortcut_property_tags(),
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Microsoft.FolderDesign.NamedView".to_string()),
        }),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 10, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    assert_response_contains_utf16(&response, "IPM.Microsoft.FolderDesign.NamedView");
    assert_response_contains_utf16(&response, "Compact");
}

#[test]
fn common_views_query_rows_uses_wlink_sort_order() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = common_views_sort_snapshot(account_id);
    let mut table = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: vec![
            MapiSortOrder {
                property_tag: 0x684F_0102,
                order: 0,
            },
            MapiSortOrder {
                property_tag: 0x6850_0102,
                order: 0,
            },
            MapiSortOrder {
                property_tag: 0x684B_0102,
                order: 0,
            },
        ],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 10, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, account_id);

    let alpha = utf16_position(&response, "Alpha").unwrap();
    let zulu = utf16_position(&response, "Zulu").unwrap();
    assert!(alpha < zulu);
}

#[test]
fn inbox_associated_find_row_suppresses_outlook_eas_config() {
    assert_inbox_associated_find_row_no_match_for_message_class("IPM.Configuration.EAS");
}

#[test]
fn inbox_associated_find_row_returns_outlook_elc_config() {
    let _guard = outlook_smart_input_variant_test_lock();
    let previous = std::env::var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT").ok();
    std::env::remove_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT");

    assert_inbox_associated_find_row_returns_message_class("IPM.Configuration.ELC");

    if let Some(value) = previous {
        std::env::set_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT", value);
    }
}

#[test]
fn inbox_associated_find_row_variant_returns_not_found_for_synthetic_elc_config() {
    let _guard = outlook_smart_input_variant_test_lock();
    let previous = std::env::var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT").ok();
    std::env::set_var(
        "LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT",
        "synthetic_elc_findrow_not_found",
    );

    let response = inbox_associated_find_row_response_for_message_class("IPM.Configuration.ELC");

    match previous {
        Some(value) => std::env::set_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT", value),
        None => std::env::remove_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT"),
    }
    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
    assert_eq!(response.len(), 6);
}

fn outlook_smart_input_variant_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("Outlook smart input variant test lock poisoned")
}

#[test]
fn inbox_associated_find_row_returns_common_views_default_named_view() {
    assert_inbox_associated_find_row_returns_message_class("IPM.Microsoft.FolderDesign.NamedView");
}

#[test]
fn inbox_associated_find_row_returns_outlook_sharing_configuration() {
    assert_inbox_associated_find_row_returns_message_class("IPM.Sharing.Configuration");
}

#[test]
fn inbox_associated_exact_virtual_find_row_filters_followup_query_rows() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Content as u8];
    restriction.extend_from_slice(&0u32.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Sharing.Configuration");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let find_request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let find_response = rop_find_row_response(
        &find_request,
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(find_response[0], RopId::FindRow.as_u8());
    assert_eq!(find_response[7], 1);
    assert_response_contains_utf16(&find_response, "IPM.Sharing.Configuration");

    let query_request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 10, 0],
    };
    let query_response = rop_query_rows_response(
        &query_request,
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(query_response[0], RopId::QueryRows.as_u8());
    assert_eq!(
        u16::from_le_bytes([query_response[7], query_response[8]]),
        1
    );
    assert_response_contains_utf16(&query_response, "IPM.Sharing.Configuration");
}

#[test]
fn inbox_associated_find_row_does_not_return_empty_virtual_rule_organizer() {
    assert_inbox_associated_find_row_no_match_for_message_class("IPM.RuleOrganizer");
}

#[test]
fn inbox_associated_find_row_returns_outlook_sharing_index() {
    assert_inbox_associated_find_row_returns_message_class("IPM.Sharing.Index");
}

#[test]
fn inbox_associated_find_row_returns_outlook_aggregation_config() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_MESSAGE_CLASS_W,
            0x81AB_001F,
            0x81AC_001F,
            0x81A1_0048,
            0x81ED_0003,
            0x8AA6_0003,
        ],
        columns_set: true,
        sort_orders: vec![
            MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            },
            MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 1,
            },
        ],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Content as u8];
    restriction.extend_from_slice(&0u32.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Aggregation");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[6], 0);
    assert_eq!(response[7], 1);
    assert_response_contains_utf16(&response, "IPM.Aggregation");
}

#[test]
fn inbox_associated_find_row_returns_sharing_index_private_defaults() {
    let snapshot = MapiMailStoreSnapshot::empty();
    assert!(snapshot
        .associated_config_messages_for_folder(INBOX_FOLDER_ID)
        .into_iter()
        .all(|message| message.message_class != "IPM.Sharing.Index"));
}

#[test]
fn inbox_associated_broad_configuration_find_row_ignores_virtual_defaults() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_MESSAGE_CLASS_W,
            0x685D_0003,
            PID_TAG_LAST_MODIFICATION_TIME,
        ],
        columns_set: true,
        sort_orders: vec![
            MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            },
            MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 0,
            },
        ],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x02];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Configuration.");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_response_contains_utf16(&response, "IPM.Configuration.AccountPrefs");
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.EAS").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
    assert_eq!(table_position(&table), Some(0));
}

#[test]
fn quick_step_associated_find_row_returns_custom_action_config() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W, 0x7C08_0102],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Microsoft.CustomAction");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(response[7], 1);
    let mut encoded_message_class = Vec::new();
    write_utf16z(&mut encoded_message_class, "IPM.Microsoft.CustomAction");
    assert!(response
        .windows(encoded_message_class.len())
        .any(|window| window == encoded_message_class.as_slice()));
    assert!(response
        .windows(b"<?xml version=\"1.0\" encoding=\"utf-8\"?>".len())
        .any(|window| window == b"<?xml version=\"1.0\" encoding=\"utf-8\"?>"));
}

#[test]
fn contacts_associated_find_row_returns_osc_contact_sync_config() {
    assert_contact_folder_associated_find_row_returns_osc_contact_sync(CONTACTS_FOLDER_ID);
}

#[test]
fn contacts_associated_find_row_returns_contact_link_timestamp_config() {
    assert_contact_folder_associated_find_row_returns_config(
        CONTACTS_FOLDER_ID,
        "IPM.Microsoft.ContactLink.TimeStamp",
        &MapiMailStoreSnapshot::empty(),
    );
}

#[test]
fn contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let contact_prefs_id = Uuid::from_u128(0x6d617069_6370_7266_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        contact_prefs_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 83,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: contact_prefs_id,
            account_id,
            folder_id: CONTACTS_FOLDER_ID,
            message_class: "IPM.Configuration.ContactPrefs".to_string(),
            subject: "IPM.Configuration.ContactPrefs".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: CONTACTS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Microsoft.ContactLink.TimeStamp");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(response[7], 1);
    assert_response_contains_utf16(&response, "IPM.Microsoft.ContactLink.TimeStamp");
    assert_eq!(table_position(&table), Some(1));

    let position_response = rop_query_position_response(
        &RopRequest {
            rop_id: RopId::QueryPosition.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        },
        Some(&table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );
    assert_eq!(position_response[0], RopId::QueryPosition.as_u8());
    assert_eq!(
        u32::from_le_bytes(position_response[6..10].try_into().unwrap()),
        1
    );
    assert_eq!(
        u32::from_le_bytes(position_response[10..14].try_into().unwrap()),
        4
    );
}

#[test]
fn suggested_contacts_associated_find_row_does_not_return_empty_osc_contact_sync_config() {
    assert_contact_folder_associated_find_row_does_not_return_config(
        SUGGESTED_CONTACTS_FOLDER_ID,
        "IPM.Microsoft.OSC.ContactSync",
        &MapiMailStoreSnapshot::empty(),
    );
}

#[test]
fn suggested_contacts_associated_table_does_not_expose_folder_default_named_view() {
    let rows = associated_table_rows(
        SUGGESTED_CONTACTS_FOLDER_ID,
        &MapiMailStoreSnapshot::empty(),
        None,
        Uuid::nil(),
    );

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0], AssociatedTableRow::Config(_)));
    assert_eq!(
        associated_folder_message_count(
            SUGGESTED_CONTACTS_FOLDER_ID,
            &MapiMailStoreSnapshot::empty()
        ),
        1
    );
}

#[test]
fn inbox_associated_table_exposes_common_views_default_named_view_for_exact_lookup() {
    let restriction = MapiRestriction::Property {
        relop: 0x04,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String(
            crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
        ),
    };
    let rows = associated_table_rows(
        INBOX_FOLDER_ID,
        &MapiMailStoreSnapshot::empty(),
        Some(&restriction),
        Uuid::nil(),
    );

    assert_eq!(rows.len(), 1);
    let AssociatedTableRow::NamedView(view) = &rows[0] else {
        panic!("expected Common Views named view row");
    };
    assert_eq!(view.folder_id, COMMON_VIEWS_FOLDER_ID);
    assert_eq!(
        view.id,
        crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID
    );
    assert_eq!(
        restricted_associated_folder_message_count(
            INBOX_FOLDER_ID,
            &MapiMailStoreSnapshot::empty(),
            Some(&restriction),
            Uuid::nil()
        ),
        1
    );
}

#[test]
fn quick_contacts_associated_find_row_returns_osc_contact_sync_config() {
    assert_contact_folder_associated_find_row_returns_osc_contact_sync(QUICK_CONTACTS_FOLDER_ID);
}

#[test]
fn im_contact_list_associated_find_row_returns_osc_contact_sync_config() {
    assert_contact_folder_associated_find_row_returns_osc_contact_sync(IM_CONTACT_LIST_FOLDER_ID);
}

#[test]
fn dynamic_contacts_associated_find_row_returns_osc_contact_sync_config() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x4e);
    let collection = CollaborationCollection {
        id: "outlook-log-dynamic-contacts-table".to_string(),
        kind: "contacts".to_string(),
        display_name: "Contacts".to_string(),
        owner_account_id: Uuid::from_u128(0x4e),
        owner_email: "owner@example.test".to_string(),
        owner_display_name: "Owner".to_string(),
        is_owned: true,
        rights: CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        },
    };
    crate::mapi::identity::remember_mapi_identity(
        crate::mapi_store::collaboration_folder_identity_canonical_id(
            crate::mapi_store::MapiCollaborationFolderKind::Contacts,
            &collection,
        )
        .unwrap(),
        folder_id,
    );
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![collection],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
        folder_id, &snapshot,
    );

    let folder = snapshot
        .collaboration_folder_for_id(folder_id)
        .expect("dynamic contacts folder");
    assert_eq!(
        hierarchy_row_expected_container_class(&HierarchyRow::Collaboration(folder)),
        Some("IPF.Contact")
    );
    let row = serialize_hierarchy_row(
        HierarchyRow::Collaboration(folder),
        &[],
        &snapshot,
        &[PID_TAG_ASSOCIATED_CONTENT_COUNT],
        Uuid::nil(),
    );

    assert_eq!(u32::from_le_bytes(row.try_into().unwrap()), 2);
}

#[test]
fn mailbox_backed_quick_contacts_associated_find_row_returns_osc_contact_sync_config() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x55);
    let mailbox_id = Uuid::parse_str("aaaaaaaa-7777-4111-8111-aaaaaaaaaaaa").unwrap();
    crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::new(
        vec![JmapMailbox {
            id: mailbox_id,
            parent_id: None,
            role: String::new(),
            name: "Quick Contacts".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
        folder_id, &snapshot,
    );
}

#[test]
fn empty_conversation_action_settings_find_row_returns_default_action() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.ConversationAction");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(response[7], 1);
    let mut encoded_message_class = Vec::new();
    write_utf16z(&mut encoded_message_class, "IPM.ConversationAction");
    assert!(response
        .windows(encoded_message_class.len())
        .any(|window| window == encoded_message_class.as_slice()));
}

#[test]
fn conversation_action_settings_find_row_honors_restriction() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.NotConversationAction");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
    assert_eq!(response.len(), 6);
    assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_none());
    assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
}

#[test]
fn microsoft_conversation_action_example_round_trips_fai_properties() {
    let conversation_id = Uuid::from_bytes([
        0xb7, 0xa2, 0xb5, 0xc4, 0xaa, 0x65, 0x1c, 0xf2, 0xd3, 0x8c, 0x62, 0x8c, 0x0e, 0xaf, 0x56,
        0xc4,
    ]);
    let move_folder_entry_id = vec![
        0x00, 0x00, 0x00, 0x00, 0x0c, 0x99, 0xf4, 0xed, 0xa2, 0xf1, 0xe4, 0x41, 0xb1, 0x5b, 0x9b,
        0x25, 0x10, 0x91, 0x3e, 0x9d, 0x02, 0x81, 0x00, 0x00,
    ];
    let move_store_entry_id = vec![
        0x00, 0x00, 0x00, 0x00, 0x38, 0xa1, 0xbb, 0x10, 0x05, 0xe5, 0x10, 0x1a, 0xa1, 0xbb, 0x08,
        0x00, 0x2b, 0x2a, 0x56, 0xc2, 0x00, 0x00, 0x6d, 0x73, 0x70, 0x73, 0x74, 0x2e, 0x64, 0x6c,
        0x6c, 0x00,
    ];
    let max_delivery_time = mapi_mailstore::filetime_from_rfc3339_utc("2009-02-17T23:31:42Z");
    let last_applied_time = mapi_mailstore::filetime_from_rfc3339_utc("2009-02-17T23:51:11Z");
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_CONVERSATION_INDEX,
        MapiValue::Binary(conversation_index_for_uuid(conversation_id)),
    );
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("Conv.Action: Solidifying our proposal to Fabrikam, Inc.".to_string()),
    );
    properties.insert(
        PID_NAME_KEYWORDS_TAG,
        MapiValue::MultiString(vec![
            "Fabrikam".to_string(),
            "Business Proposals".to_string(),
        ]),
    );
    properties.insert(
        PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
        MapiValue::Binary(move_folder_entry_id.clone()),
    );
    properties.insert(
        PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
        MapiValue::Binary(move_store_entry_id.clone()),
    );
    properties.insert(
        PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
        MapiValue::U64(max_delivery_time),
    );
    properties.insert(
        PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
        MapiValue::U64(last_applied_time),
    );
    properties.insert(
        PID_LID_CONVERSATION_ACTION_VERSION_TAG,
        MapiValue::I32(lpe_storage::CONVERSATION_ACTION_VERSION),
    );
    properties.insert(PID_LID_CONVERSATION_PROCESSED_TAG, MapiValue::I32(7));

    let action = conversation_action_from_mapi_properties(&properties);
    assert_eq!(action.id, conversation_id);
    assert_eq!(action.conversation_id, conversation_id);
    assert_eq!(
        action.subject,
        "Conv.Action: Solidifying our proposal to Fabrikam, Inc."
    );
    assert_eq!(
        action.move_folder_entry_id,
        Some(move_folder_entry_id.clone())
    );
    assert_eq!(
        action.move_store_entry_id,
        Some(move_store_entry_id.clone())
    );
    assert_eq!(
        action.max_delivery_time.as_deref(),
        Some("2009-02-17T23:31:00Z")
    );
    assert_eq!(
        action.last_applied_time.as_deref(),
        Some("2009-02-17T23:51:00Z")
    );
    assert_eq!(action.version, lpe_storage::CONVERSATION_ACTION_VERSION);
    assert_eq!(action.processed, 7);
    let categories: Vec<String> = serde_json::from_str(&action.categories_json).unwrap();
    assert_eq!(
        categories,
        vec!["Fabrikam".to_string(), "Business Proposals".to_string()]
    );

    let message = MapiConversationActionMessage {
        id: crate::mapi::identity::mapi_store_id(0x7fff_ffff_ffe8),
        folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        canonical_id: action.id,
        action,
    };

    assert_eq!(
        conversation_action_property_value(&message, PID_TAG_MESSAGE_CLASS_W),
        Some(MapiValue::String("IPM.ConversationAction".to_string()))
    );
    assert_eq!(
        conversation_action_property_value(&message, PID_TAG_CONVERSATION_INDEX),
        Some(MapiValue::Binary(conversation_index_for_uuid(
            conversation_id
        )))
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
        conversation_action_property_value(&message, PID_LID_CONVERSATION_PROCESSED_TAG),
        Some(MapiValue::I32(7))
    );
}

#[test]
fn inbox_associated_exact_configuration_find_row_uses_sort_order() {
    let snapshot = inbox_associated_sort_snapshot();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Configuration.AccountPrefs");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_response_contains_utf16(&response, "IPM.Configuration.AccountPrefs");
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
    assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
}

#[test]
fn inbox_associated_broad_configuration_find_row_projects_single_followup_row() {
    let _guard = outlook_smart_input_variant_test_lock();
    let previous = std::env::var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT").ok();
    std::env::remove_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT");

    let snapshot = inbox_associated_sort_snapshot();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x02];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Configuration.");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let find_request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let find_response = rop_find_row_response(
        &find_request,
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    if let Some(value) = previous {
        std::env::set_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT", value);
    }

    assert_eq!(find_response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(find_response[2..6].try_into().unwrap()),
        0
    );
    assert_response_contains_utf16(&find_response, "IPM.Configuration.AccountPrefs");
    assert!(utf16_position(&find_response, "IPM.Configuration.MessageListSettings").is_none());

    let query_request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };
    let query_response = rop_query_rows_response(
        &query_request,
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(query_response[0], RopId::QueryRows.as_u8());
    assert_eq!(
        u16::from_le_bytes([query_response[7], query_response[8]]),
        1
    );
    assert_response_contains_utf16(&query_response, "IPM.Configuration.AccountPrefs");
    assert!(utf16_position(&query_response, "IPM.Configuration.EAS").is_none());
    assert!(utf16_position(&query_response, "IPM.Configuration.ELC").is_none());
    assert!(utf16_position(&query_response, "IPM.Configuration.MessageListSettings").is_none());
    assert!(utf16_position(&query_response, "IPM.RuleOrganizer").is_none());
    assert!(utf16_position(&query_response, "IPM.Sharing.Configuration").is_none());
    assert!(utf16_position(&query_response, "IPM.Microsoft.FolderDesign.NamedView").is_none());
}

#[test]
fn inbox_associated_broad_configuration_find_row_variant_skips_followup_handoff() {
    let _guard = outlook_smart_input_variant_test_lock();
    let previous = std::env::var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT").ok();
    std::env::set_var(
        "LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT",
        "broad_findrow_no_handoff",
    );

    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let account_prefs_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000001);
    let message_list_settings_id = Uuid::from_u128(0x6d617069_6d6c_5374_8000_000000000101);
    crate::mapi::identity::remember_mapi_identity(
        account_prefs_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 81,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        message_list_settings_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 801,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: account_prefs_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "Account prefs".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: message_list_settings_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "IPM.Configuration.MessageListSettings".to_string(),
            properties_json: serde_json::json!({
                "0x001a001f": {
                    "type": "string",
                    "value": "IPM.Configuration.MessageListSettings"
                },
                "0x7c070102": {
                    "type": "binary",
                    "value": "3c786d6c2f3e"
                }
            }),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x02];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Configuration.");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());

    let find_response = rop_find_row_response(
        &RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    match previous {
        Some(value) => std::env::set_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT", value),
        None => std::env::remove_var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT"),
    }

    assert_eq!(find_response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(find_response[2..6].try_into().unwrap()),
        0
    );
    assert_response_contains_utf16(&find_response, "IPM.Configuration.AccountPrefs");
    assert_eq!(table_position(&table), Some(0));
    assert!(matches!(
        table,
        MapiObject::ContentsTable {
            restriction: None,
            ..
        }
    ));

    let seek_response = rop_seek_row_response(
        &RopRequest {
            rop_id: RopId::SeekRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![1, 1, 0, 0, 0, 1],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(seek_response[0], RopId::SeekRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(seek_response[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(table_position(&table), Some(1));

    let query_response = rop_query_rows_response(
        &RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 50, 0],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(query_response[0], RopId::QueryRows.as_u8());
    assert!(
        u16::from_le_bytes([query_response[7], query_response[8]]) > 0,
        "broad no-handoff follow-up QueryRows should continue over the full FAI table"
    );
    assert!(utf16_position(&query_response, "IPM.Configuration.MessageListSettings").is_some());
}

#[test]
fn inbox_associated_exact_named_view_find_row_restricts_followup_handoff() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let broad_restriction = outlook_configuration_prefix_restriction();
    let broad_row_count = associated_table_rows(
        INBOX_FOLDER_ID,
        &snapshot,
        Some(&broad_restriction),
        Uuid::nil(),
    )
    .len();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(broad_restriction),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: broad_row_count,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Microsoft.FolderDesign.NamedView");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());

    let find_response = rop_find_row_response(
        &RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(find_response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(find_response[2..6].try_into().unwrap()),
        0
    );
    assert_response_contains_utf16(&find_response, "IPM.Microsoft.FolderDesign.NamedView");
    assert_eq!(table_position(&table), Some(0));

    let query_response = rop_query_rows_response(
        &RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 50, 0],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(query_response[0], RopId::QueryRows.as_u8());
    assert_eq!(query_response[6], 0x02);
    assert_eq!(
        u16::from_le_bytes([query_response[7], query_response[8]]),
        1
    );
    assert_response_contains_utf16(&query_response, "IPM.Microsoft.FolderDesign.NamedView");
}

#[test]
fn inbox_associated_broad_find_row_suppresses_persisted_followup_rows() {
    let account_id = Uuid::from_u128(0x73a6_121f_9c0d_423b_8fcb_7174f28e1608);
    let earlier_id = Uuid::from_u128(0x73a6_121f_9c0d_423b_8fcb_7174f28e1609);
    let persisted_id = Uuid::from_u128(0x73a6_121f_9c0d_423b_8fcb_7174f28e1610);
    crate::mapi::identity::remember_mapi_identity(
        earlier_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 800,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        persisted_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 801,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: earlier_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.ClientOptions".to_string(),
            subject: "ClientOptions".to_string(),
            properties_json: serde_json::json!({}),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "IPM.Configuration.MessageListSettings".to_string(),
            properties_json: serde_json::json!({
                "0x001a001f": {
                    "type": "string",
                    "value": "IPM.Configuration.MessageListSettings"
                },
                "0x7c070102": {
                    "type": "binary",
                    "value": "3c786d6c2f3e"
                }
            }),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x02];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Configuration.");
    let mut find_payload = vec![0];
    find_payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    find_payload.extend_from_slice(&restriction);
    find_payload.push(1);
    find_payload.extend_from_slice(&0u16.to_le_bytes());
    let find_response = rop_find_row_response(
        &RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: find_payload,
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(find_response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(find_response[2..6].try_into().unwrap()),
        0
    );
    assert_response_contains_utf16(&find_response, "IPM.Configuration.AccountPrefs");
    assert!(utf16_position(&find_response, "IPM.Configuration.MessageListSettings").is_none());
    assert_eq!(table_position(&table), Some(0));

    let seek_response = rop_seek_row_response(
        &RopRequest {
            rop_id: RopId::SeekRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![1, 1, 0, 0, 0, 1],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(seek_response[0], RopId::SeekRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(seek_response[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(table_position(&table), Some(1));

    let query_response = rop_query_rows_response(
        &RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 50, 0],
        },
        Some(&mut table),
        &[],
        &[],
        &snapshot,
        Uuid::nil(),
    );

    assert_eq!(query_response[0], RopId::QueryRows.as_u8());
    assert_eq!(query_response[6], 0x02);
    assert_eq!(
        u16::from_le_bytes([query_response[7], query_response[8]]),
        0
    );
    assert!(utf16_position(&query_response, "IPM.Configuration.ClientOptions").is_none());
    assert!(utf16_position(&query_response, "IPM.Configuration.AccountPrefs").is_none());
    assert!(utf16_position(&query_response, "IPM.Configuration.MessageListSettings").is_none());
}

#[test]
fn inbox_associated_broad_configuration_restriction_projects_startup_configs() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let restriction = MapiRestriction::Property {
        relop: 0x02,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String("IPM.Configuration.".to_string()),
    };

    let rows = associated_table_rows(INBOX_FOLDER_ID, &snapshot, Some(&restriction), Uuid::nil());
    assert!(
        rows.iter()
            .all(|row| matches!(row, AssociatedTableRow::Config(_))),
        "broad IPM.Configuration startup scans must not return FolderDesign views"
    );
    let classes = rows
        .iter()
        .filter_map(associated_table_row_config)
        .map(|message| message.message_class.as_str())
        .collect::<Vec<_>>();

    assert_eq!(classes, vec!["IPM.Configuration.AccountPrefs"]);
}

#[test]
fn inbox_associated_broad_configuration_restriction_suppresses_persisted_followup_configs() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let autocomplete_id = Uuid::from_u128(0x6d617069_6175_746f_8000_000000000101);
    crate::mapi::identity::remember_mapi_identity(
        autocomplete_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 185,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: autocomplete_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.Autocomplete".to_string(),
            subject: "Autocomplete".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
    ]);
    let restriction = MapiRestriction::Property {
        relop: 0x02,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String("IPM.Configuration.".to_string()),
    };

    let rows = associated_table_rows(INBOX_FOLDER_ID, &snapshot, Some(&restriction), Uuid::nil());
    let classes = rows
        .iter()
        .filter_map(associated_table_row_config)
        .map(|message| message.message_class.as_str())
        .collect::<Vec<_>>();

    assert_eq!(classes, vec!["IPM.Configuration.AccountPrefs"]);
}

#[test]
fn inbox_associated_broad_configuration_restriction_dedupes_modeled_startup_class() {
    let snapshot = inbox_associated_sort_snapshot();
    let restriction = MapiRestriction::Property {
        relop: 0x02,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String("IPM.Configuration.".to_string()),
    };

    let rows = associated_table_rows(INBOX_FOLDER_ID, &snapshot, Some(&restriction), Uuid::nil());
    let account_prefs_count = rows
        .iter()
        .filter_map(associated_table_row_config)
        .filter(|message| message.message_class == "IPM.Configuration.AccountPrefs")
        .count();

    assert_eq!(account_prefs_count, 1);
}

#[test]
fn inbox_associated_broad_configuration_find_row_ignores_extended_rule_message() {
    let snapshot = inbox_associated_extended_rule_snapshot();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x02];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Configuration.");
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response =
        rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_response_contains_utf16(&response, "IPM.Configuration.AccountPrefs");
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
    assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
}

#[test]
fn inbox_associated_query_rows_uses_sort_order() {
    let snapshot = inbox_associated_sort_snapshot();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
    assert!(utf16_position(&response, "IPM.Configuration.UMOLK.UserOptions").is_none());
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.EAS").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
    assert!(utf16_position(&response, "IPM.Sharing.Configuration").is_none());
}

#[test]
fn inbox_associated_query_rows_suppresses_extended_rule_message() {
    let snapshot = inbox_associated_extended_rule_snapshot();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.ExtendedRule.Message").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
    assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
}

#[test]
fn inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let account_prefs_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000101);
    let persisted_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000101);
    let account_prefs_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 181,
    );
    let persisted_view_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 182,
    );
    crate::mapi::identity::remember_mapi_identity(account_prefs_id, account_prefs_object_id);
    crate::mapi::identity::remember_mapi_identity(persisted_view_id, persisted_view_object_id);
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: account_prefs_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "Account prefs".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_view_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
            subject: "Compact".to_string(),
            properties_json: serde_json::json!({
                "0x0e0b0102": {"type": "binary", "value": "010203"}
            }),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 2);
    assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
    assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
}

#[test]
fn inbox_associated_query_rows_replaces_empty_persisted_compact_named_view() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let account_prefs_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000111);
    let persisted_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000111);
    crate::mapi::identity::remember_mapi_identity(
        account_prefs_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 183,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        persisted_view_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 184,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: account_prefs_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "Account prefs".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_view_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
            subject: "Compact".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MID, PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
    assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_none());
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
}

#[test]
fn junk_associated_query_rows_exposes_default_named_view() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: JUNK_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
}

#[test]
fn contacts_associated_query_rows_expose_contact_default_named_view() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: CONTACTS_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 3);
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
}

#[test]
fn calendar_associated_query_rows_expose_calendar_default_named_view() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: CALENDAR_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W, PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Microsoft.FolderDesign.NamedView").is_some());
    assert_response_contains_utf16(&response, "Calendar");
    assert!(utf16_position(&response, "Compact").is_none());
}

#[test]
fn calendar_associated_query_rows_prefix_configuration_returns_calendar_config() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let availability_id = Uuid::from_u128(0x6d617069_6361_6c43_8000_000000000001);
    let calendar_id = Uuid::from_u128(0x6d617069_6361_6c43_8000_000000000002);
    crate::mapi::identity::remember_mapi_identity(
        availability_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 84,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        calendar_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 85,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: availability_id,
            account_id,
            folder_id: CALENDAR_FOLDER_ID,
            message_class: "IPM.Configuration.AvailabilityOptions".to_string(),
            subject: "Availability options".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "0102"}
            }),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: calendar_id,
            account_id,
            folder_id: CALENDAR_FOLDER_ID,
            message_class: "IPM.Configuration.Calendar".to_string(),
            subject: "Calendar".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "0304"}
            }),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: CALENDAR_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(outlook_configuration_prefix_restriction()),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Configuration.Calendar").is_some());
    assert!(utf16_position(&response, "IPM.Configuration.AvailabilityOptions").is_none());
}

#[test]
fn inbox_associated_query_rows_prefix_configuration_suppresses_stored_stream() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let config_id = Uuid::from_u128(0x6d617069_6d6c_7343_8000_000000000099);
    crate::mapi::identity::remember_mapi_identity(
        config_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 82,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: config_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "Message list settings".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
    ]);
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(outlook_configuration_prefix_restriction()),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Configuration.AccountPrefs").is_some());
    assert!(utf16_position(&response, "IPM.Configuration.MessageListSettings").is_none());
}

#[test]
fn inbox_associated_query_rows_prefix_configuration_suppresses_virtual_elc() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(outlook_configuration_prefix_restriction()),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 50, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert!(utf16_position(&response, "IPM.Configuration.ELC").is_none());
}

#[test]
fn inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows() {
    let snapshot = inbox_associated_sort_snapshot();
    let columns = vec![
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_ROAMING_DATATYPES,
        PID_TAG_MESSAGE_CLASS_W,
        0x685D_0003,
        PID_TAG_LAST_MODIFICATION_TIME,
    ];
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: columns.clone(),
        columns_set: true,
        sort_orders: vec![
            MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            },
            MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 0,
            },
        ],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.AccountPrefs".to_string()),
        }),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 1, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    let mut cursor = Cursor::new(&response[9..]);
    assert_eq!(cursor.read_u8().unwrap(), 0);
    for column in columns {
        parse_mapi_property_value(&mut cursor, column).unwrap();
    }
    assert!(cursor.remaining_is_zero_padding());
}

#[test]
fn inbox_associated_query_rows_returns_umolk_user_options_default() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MID, PID_TAG_MESSAGE_CLASS_W, PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_LAST_MODIFICATION_TIME,
            order: 1,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.UMOLK.UserOptions".to_string()),
        }),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 2, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Configuration.UMOLK.UserOptions").is_some());
}

#[test]
fn microsoft_oxocfg_inbox_mrm_configuration_uses_xml_stream() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_ROAMING_XML_STREAM,
        ],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_LAST_MODIFICATION_TIME,
            order: 1,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.MRM".to_string()),
        }),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 2, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    assert!(utf16_position(&response, "IPM.Configuration.MRM").is_some());
    assert!(response
        .windows(4)
        .any(|window| window == 2u32.to_le_bytes()));
    assert!(response
        .windows(b"RetentionHold".len())
        .any(|window| window == b"RetentionHold"));
}

#[test]
fn inbox_associated_query_rows_does_not_return_empty_virtual_rule_organizer() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_LAST_MODIFICATION_TIME,
        ],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_LAST_MODIFICATION_TIME,
            order: 1,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.RuleOrganizer".to_string()),
        }),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 35, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 0);
}

#[test]
fn rule_organizer_without_client_payload_has_no_synthetic_stream_property() {
    let message = MapiAssociatedConfigMessage {
        id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFED),
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x6d617069_7275_6c65_8000_000000000001),
        message_class: crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
        subject: crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS.to_string(),
        properties_json: serde_json::json!({}),
    };

    assert_eq!(
        associated_config_property_value(&message, OUTLOOK_RULE_ORGANIZER_BINARY_6802),
        None
    );
}

#[test]
fn delegate_freebusy_projects_outlook_view_probe_properties() {
    let message = MapiDelegateFreeBusyMessage {
        id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE4),
        folder_id: FREEBUSY_DATA_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x6d617069_6672_4266_8000_000000000001),
        message: lpe_storage::DelegateFreeBusyMessageObject {
            id: Uuid::from_u128(0x6d617069_6672_4266_8000_000000000001),
            account_id: Uuid::nil(),
            owner_account_id: Uuid::nil(),
            owner_email: String::new(),
            message_kind: "freebusy".to_string(),
            subject: "LocalFreebusy".to_string(),
            body_text: String::new(),
            starts_at: None,
            ends_at: None,
            busy_status: None,
            payload_json: "{}".to_string(),
            updated_at: "1970-01-01T00:00:00Z".to_string(),
        },
    };

    for tag in [
        0x6841_0003,
        0x6842_000B,
        0x6843_000B,
        0x684A_101F,
        0x6845_1102,
        0x686B_1003,
        0x6870_1102,
        0x6871_1003,
        0x6872_001F,
        0x686D_000B,
        0x686E_000B,
        0x686F_000B,
        0x684B_000B,
        0x6844_101F,
        0x3008_0040,
        0x0E0B_0102,
    ] {
        assert!(
            delegate_freebusy_property_value(&message, tag).is_some(),
            "missing modeled freebusy property 0x{tag:08x}"
        );
    }
    assert_ne!(
        delegate_freebusy_property_value(&message, PID_TAG_LAST_MODIFICATION_TIME),
        Some(MapiValue::I64(0))
    );
}

#[test]
fn inbox_associated_query_rows_default_columns_cover_required_configuration_contract() {
    let snapshot = inbox_associated_sort_snapshot();
    let columns = default_associated_config_columns();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: Vec::new(),
        columns_set: true,
        sort_orders: vec![
            MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            },
            MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 0,
            },
        ],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: Some(MapiRestriction::Property {
            relop: 0x04,
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: MapiValue::String("IPM.Configuration.AccountPrefs".to_string()),
        }),
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: vec![0, 1, 1, 0],
    };

    let response =
        rop_query_rows_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil());

    assert_eq!(response[0], RopId::QueryRows.as_u8());
    assert_eq!(u16::from_le_bytes([response[7], response[8]]), 1);
    let mut cursor = Cursor::new(&response[9..]);
    assert_eq!(cursor.read_u8().unwrap(), 0);
    for column in columns {
        parse_mapi_property_value(&mut cursor, column).unwrap();
    }
    assert!(cursor.remaining_is_zero_padding());
}

#[test]
fn inbox_associated_rows_project_folder_id_and_last_modification_time() {
    let message = MapiAssociatedConfigMessage {
        id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 91,
        ),
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::nil(),
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        subject: "Message list settings".to_string(),
        properties_json: serde_json::json!({}),
    };
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);

    assert_eq!(
        associated_config_property_value(&message, PID_TAG_FOLDER_ID),
        Some(MapiValue::U64(INBOX_FOLDER_ID))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_INST_ID),
        Some(MapiValue::U64(message.id))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_INSTANCE_NUM),
        Some(MapiValue::U32(0))
    );
    let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        Uuid::nil(),
        INBOX_FOLDER_ID,
        message.id,
    )
    .unwrap();
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_ENTRY_ID),
        Some(MapiValue::Binary(entry_id))
    );
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let mailbox_entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        mailbox_guid,
        INBOX_FOLDER_ID,
        message.id,
    )
    .unwrap();
    assert_eq!(
        associated_config_property_value_with_mailbox_guid(
            &message,
            mailbox_guid,
            PID_TAG_ENTRY_ID
        ),
        Some(MapiValue::Binary(mailbox_entry_id.clone()))
    );
    let source_key = mapi_mailstore::source_key_for_store_id(message.id);
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_SOURCE_KEY),
        Some(MapiValue::Binary(source_key.clone()))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_RECORD_KEY),
        Some(MapiValue::Binary(source_key))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_CONVERSATION_TOPIC_W),
        Some(MapiValue::String("Message list settings".to_string()))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_MESSAGE_CLASS_W),
        Some(MapiValue::String(
            "IPM.Configuration.MessageListSettings".to_string()
        ))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_ORIGINAL_MESSAGE_CLASS_W),
        Some(MapiValue::String(
            "IPM.Configuration.MessageListSettings".to_string()
        ))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_MESSAGE_STATUS),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_ACCESS_LEVEL),
        Some(MapiValue::U32(1))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_SENT_MAIL_SVR_EID),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_SEARCH_KEY),
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            message.id
        )))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_PARENT_SOURCE_KEY),
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            INBOX_FOLDER_ID
        )))
    );
    assert_eq!(
        associated_config_property_value_with_mailbox_guid(
            &message,
            mailbox_guid,
            PID_TAG_PARENT_ENTRY_ID
        ),
        crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, INBOX_FOLDER_ID)
            .map(MapiValue::Binary)
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_LAST_MODIFICATION_TIME),
        Some(MapiValue::I64(
            mapi_mailstore::filetime_from_change_number(change_number) as i64
        ))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_ROAMING_DATATYPES),
        Some(MapiValue::U32(4))
    );
    assert!(matches!(
        associated_config_property_value(&message, PID_TAG_ROAMING_DICTIONARY),
        Some(MapiValue::Binary(value))
            if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
                && value.windows(b"18-OLPrefsVersion".len()).any(|window| window == b"18-OLPrefsVersion")
                && value.windows(b"9-1".len()).any(|window| window == b"9-1")
    ));
    assert!(matches!(
        associated_config_property_value(&message, 0x685D_0003),
        Some(MapiValue::U32(value)) if value != 0
    ));
    assert_eq!(
        associated_config_property_value(&message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_NAME_CONTENT_CLASS_W_TAG),
        Some(MapiValue::String("urn:content-classes:message".to_string()))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_NAME_CONTENT_TYPE_W_TAG),
        Some(MapiValue::String("text/xml".to_string()))
    );
    let explicit_marker = MapiAssociatedConfigMessage {
        properties_json: serde_json::json!({
            "0x685d0003": {"type": "u32", "value": 42}
        }),
        ..message.clone()
    };
    assert_eq!(
        associated_config_property_value(&explicit_marker, 0x685D_0003),
        Some(MapiValue::U32(42))
    );
    let xml_only = MapiAssociatedConfigMessage {
        properties_json: serde_json::json!({
            "0x7c080102": {"type": "binary", "value": "3c786d6c2f3e"}
        }),
        ..message.clone()
    };
    assert_eq!(
        associated_config_property_value(&xml_only, PID_TAG_ROAMING_DATATYPES),
        Some(MapiValue::U32(2))
    );
    assert_eq!(
        associated_config_property_value(&xml_only, PID_TAG_ROAMING_XML_STREAM),
        Some(MapiValue::Binary(b"<xml/>".to_vec()))
    );
    let binary_only = MapiAssociatedConfigMessage {
        properties_json: serde_json::json!({
            "0x7c090102": {"type": "binary", "value": "010203"}
        }),
        ..message.clone()
    };
    assert_eq!(
        associated_config_property_value(&binary_only, PID_TAG_ROAMING_DATATYPES),
        Some(MapiValue::U32(1))
    );
    assert_eq!(
        associated_config_property_value(&binary_only, 0x7C09_0102),
        Some(MapiValue::Binary(vec![1, 2, 3]))
    );
    let explicit_no_streams = MapiAssociatedConfigMessage {
        properties_json: serde_json::json!({
            "0x7c060003": {"type": "i32", "value": 0}
        }),
        ..message.clone()
    };
    assert_eq!(
        associated_config_property_value(&explicit_no_streams, PID_TAG_ROAMING_DATATYPES),
        Some(MapiValue::I32(0))
    );
    assert_eq!(
        associated_config_property_value(&explicit_no_streams, PID_TAG_ROAMING_DICTIONARY),
        None
    );
    let work_hours = MapiAssociatedConfigMessage {
        id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 93,
        ),
        folder_id: CALENDAR_FOLDER_ID,
        canonical_id: Uuid::nil(),
        message_class: "IPM.Configuration.WorkHours".to_string(),
        subject: "WorkHours".to_string(),
        properties_json: serde_json::json!({}),
    };
    assert_eq!(
        associated_config_property_value(&work_hours, PID_TAG_ROAMING_DATATYPES),
        Some(MapiValue::U32(2))
    );
    assert!(matches!(
        associated_config_property_value(&work_hours, PID_TAG_ROAMING_XML_STREAM),
        Some(MapiValue::Binary(value))
            if value.windows(b"WorkingHours.xsd".len()).any(|window| window == b"WorkingHours.xsd")
                && value.windows(b"WorkHoursVersion1".len()).any(|window| window == b"WorkHoursVersion1")
    ));
    assert_eq!(
        associated_config_property_value(&work_hours, PID_TAG_ROAMING_DICTIONARY),
        None
    );
    let category_list = MapiAssociatedConfigMessage {
        id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 94,
        ),
        folder_id: CALENDAR_FOLDER_ID,
        canonical_id: Uuid::nil(),
        message_class: "IPM.Configuration.CategoryList".to_string(),
        subject: "CategoryList".to_string(),
        properties_json: serde_json::json!({}),
    };
    assert_eq!(
        associated_config_property_value(&category_list, PID_TAG_ROAMING_DATATYPES),
        Some(MapiValue::U32(2))
    );
    assert!(matches!(
        associated_config_property_value(&category_list, PID_TAG_ROAMING_XML_STREAM),
        Some(MapiValue::Binary(value))
            if value.windows(b"CategoryList.xsd".len()).any(|window| window == b"CategoryList.xsd")
                && value.windows(b"Red Category".len()).any(|window| window == b"Red Category")
    ));
    assert_eq!(
        associated_config_property_value(&category_list, PID_TAG_ROAMING_DICTIONARY),
        None
    );
    let quick_step = MapiAssociatedConfigMessage {
        id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 92,
        ),
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        canonical_id: Uuid::nil(),
        message_class: crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
        subject: crate::mapi_store::OUTLOOK_QUICK_STEP_CUSTOM_ACTION_CLASS.to_string(),
        properties_json: serde_json::json!({}),
    };
    assert_eq!(
        associated_config_property_value(&quick_step, PID_TAG_ROAMING_DATATYPES),
        Some(MapiValue::U32(2))
    );
    assert!(matches!(
        associated_config_property_value(&quick_step, PID_TAG_ROAMING_XML_STREAM),
        Some(MapiValue::Binary(value))
            if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
                && value.windows(b"customActions".len()).any(|window| window == b"customActions")
    ));
    assert_eq!(
        associated_config_property_value(&quick_step, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        Some(MapiValue::Binary(Vec::new()))
    );

    let row = serialize_associated_config_row_with_mailbox_guid(
        &message,
        mailbox_guid,
        &[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_ROAMING_DATATYPES,
            0x685D_0003,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
            PID_TAG_LAST_MODIFICATION_TIME,
        ],
    );

    assert_eq!(row.len(), 46);
    let mut row_cursor = Cursor::new(&row);
    for column in [
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_ROAMING_DATATYPES,
        0x685D_0003,
    ] {
        parse_mapi_property_value(&mut row_cursor, column).unwrap();
    }
    assert_eq!(
        parse_mapi_property_value(&mut row_cursor, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B).unwrap(),
        MapiValue::Binary(Vec::new())
    );

    let entry_id_row = serialize_associated_config_row_with_mailbox_guid(
        &message,
        mailbox_guid,
        &[PID_TAG_ENTRY_ID],
    );
    assert!(entry_id_row
        .windows(mailbox_entry_id.len())
        .any(|window| window == mailbox_entry_id));
}

#[test]
fn contact_link_timestamp_config_projects_outlook_osc_defaults() {
    let message = MapiAssociatedConfigMessage {
        id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC),
        folder_id: CONTACTS_FOLDER_ID,
        canonical_id: Uuid::nil(),
        message_class: "IPM.Microsoft.ContactLink.TimeStamp".to_string(),
        subject: "IPM.Microsoft.ContactLink.TimeStamp".to_string(),
        properties_json: serde_json::json!({}),
    };

    assert_eq!(
        associated_config_property_value(&message, PID_NAME_OSC_CONTACT_SOURCES_TAG),
        Some(MapiValue::MultiString(Vec::new()))
    );
    assert_eq!(
        associated_config_property_value(
            &message,
            (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1 << 16) | 0x0102
        ),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        associated_config_property_value(
            &message,
            (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1 << 16) | 0x0040
        ),
        Some(MapiValue::I64(0))
    );
    assert_eq!(
        associated_config_property_value(
            &message,
            (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC << 16) | 0x001F
        ),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        associated_config_property_value(
            &message,
            (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA << 16) | 0x0003
        ),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        associated_config_property_value(
            &message,
            (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED << 16) | 0x000B
        ),
        Some(MapiValue::Bool(false))
    );
    assert_eq!(
        associated_config_property_value(&message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        Some(MapiValue::Binary(Vec::new()))
    );
}

#[test]
fn contacts_helper_associated_configs_project_table_config_columns() {
    for message_class in [
        "IPM.Microsoft.ContactLink.TimeStamp",
        "IPM.Microsoft.OSC.ContactSync",
    ] {
        let message = MapiAssociatedConfigMessage {
            id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC),
            folder_id: CONTACTS_FOLDER_ID,
            canonical_id: Uuid::nil(),
            message_class: message_class.to_string(),
            subject: message_class.to_string(),
            properties_json: serde_json::json!({}),
        };

        assert_eq!(
            associated_config_property_value(&message, PID_TAG_ROAMING_DATATYPES),
            Some(MapiValue::U32(0))
        );
        assert!(matches!(
            associated_config_property_value(&message, 0x685D_0003),
            Some(MapiValue::U32(value)) if value != 0
        ));
        assert_eq!(
            associated_config_property_value(&message, PID_NAME_OSC_CONTACT_SOURCES_TAG),
            Some(MapiValue::MultiString(Vec::new()))
        );
        assert_eq!(
            associated_config_property_value(
                &message,
                (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC << 16) | 0x0003
            ),
            Some(MapiValue::U32(0))
        );

        let row = serialize_associated_config_row_with_mailbox_guid(
            &message,
            Uuid::nil(),
            &[
                PID_TAG_FOLDER_ID,
                PID_TAG_MID,
                PID_TAG_INST_ID,
                PID_TAG_INSTANCE_NUM,
                PID_TAG_ROAMING_DATATYPES,
                PID_TAG_MESSAGE_CLASS_W,
                0x685D_0003,
                PID_TAG_LAST_MODIFICATION_TIME,
            ],
        );
        let mut row_cursor = Cursor::new(&row);
        for column in [
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
        ] {
            parse_mapi_property_value(&mut row_cursor, column).unwrap();
        }
        assert_eq!(
            parse_mapi_property_value(&mut row_cursor, PID_TAG_ROAMING_DATATYPES).unwrap(),
            MapiValue::I32(0)
        );
        parse_mapi_property_value(&mut row_cursor, PID_TAG_MESSAGE_CLASS_W).unwrap();
        assert!(matches!(
            parse_mapi_property_value(&mut row_cursor, 0x685D_0003).unwrap(),
            MapiValue::I32(value) if value != 0
        ));
        parse_mapi_property_value(&mut row_cursor, PID_TAG_LAST_MODIFICATION_TIME).unwrap();
        assert_eq!(row_cursor.position() as usize, row.len());
    }
}

#[test]
fn inbox_named_view_associated_row_projects_view_descriptor_properties() {
    let message = MapiAssociatedConfigMessage {
        id: crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 91,
        ),
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x6d617069_696e_5669_8000_000000000001),
        message_class: crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS.to_string(),
        subject: "Compact".to_string(),
        properties_json: serde_json::json!({}),
    };

    assert_eq!(
        associated_config_property_value(&message, PID_TAG_MESSAGE_CLASS_W),
        Some(MapiValue::String(
            "IPM.Microsoft.FolderDesign.NamedView".to_string()
        ))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_SUBJECT_W),
        Some(MapiValue::String("Compact".to_string()))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_VERSION),
        Some(MapiValue::U32(8))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL),
        Some(MapiValue::U32(8))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_NAME_W),
        Some(MapiValue::String("Compact".to_string()))
    );
    assert_eq!(
            associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_STRINGS_W),
            Some(MapiValue::String(
                "\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nFrom\nSubject\nReceived\nSize\nCategories\n".to_string()
            ))
        );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE),
        Some(MapiValue::U32(0))
    );
    assert!(matches!(
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_FLAGS),
        Some(MapiValue::U32(value)) if value != 0
    ));
    assert!(matches!(
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_BINARY),
        Some(MapiValue::Binary(value)) if !value.is_empty()
    ));
    assert_eq!(
        associated_config_property_value(&message, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_BINARY)
    );
    assert_eq!(
        associated_config_property_value(&message, OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C),
        Some(MapiValue::Binary(view_descriptor_strings_binary(
            &outlook_mail_view_definition("Compact")
        )))
    );
    assert_eq!(
        associated_config_property_value(&message, PID_TAG_VIEW_DESCRIPTOR_CLSID),
        Some(MapiValue::Guid(*message.canonical_id.as_bytes()))
    );
    assert_eq!(
        associated_config_property_value(&message, 0x6833_0102),
        Some(MapiValue::Binary(message.canonical_id.as_bytes().to_vec()))
    );
    assert_eq!(
        associated_config_property_value(&message, 0x6842_0102),
        Some(MapiValue::Binary(default_wlink_group_guid().to_vec()))
    );

    let row = serialize_associated_config_row_with_mailbox_guid(
        &message,
        Uuid::nil(),
        &[
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_SUBJECT_W,
            PID_TAG_VIEW_DESCRIPTOR_CLSID,
            PID_TAG_VIEW_DESCRIPTOR_FLAGS,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
            0x6842_0102,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_MESSAGE_CLASS_W,
        ],
    );
    let mut row_cursor = Cursor::new(&row);
    for column in [
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_SUBJECT_W,
        PID_TAG_VIEW_DESCRIPTOR_CLSID,
        PID_TAG_VIEW_DESCRIPTOR_FLAGS,
        PID_TAG_VIEW_DESCRIPTOR_VERSION,
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
        0x6842_0102,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_MESSAGE_CLASS_W,
    ] {
        parse_mapi_property_value(&mut row_cursor, column).unwrap();
    }
    assert!(row_cursor.remaining_is_zero_padding());
}

#[test]
fn microsoft_oxocfg_associated_config_sort_uses_persisted_last_modification_time() {
    let older_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 111,
    );
    let newer_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 110,
    );
    let older = MapiAssociatedConfigMessage {
        id: older_id,
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::nil(),
        message_class: "IPM.Configuration.ClientOptions".to_string(),
        subject: "Older client options".to_string(),
        properties_json: serde_json::json!({
            "__lpe_updated_at": "2026-01-01T00:00:00Z"
        }),
    };
    let newer = MapiAssociatedConfigMessage {
        id: newer_id,
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::nil(),
        message_class: "IPM.Configuration.ClientOptions".to_string(),
        subject: "Newer client options".to_string(),
        properties_json: serde_json::json!({
            "__lpe_updated_at": "2026-06-01T00:00:00Z"
        }),
    };

    assert_eq!(
        associated_config_property_value(&newer, PID_TAG_LAST_MODIFICATION_TIME),
        Some(MapiValue::I64(
            mapi_mailstore::filetime_from_rfc3339_utc("2026-06-01T00:00:00Z") as i64
        ))
    );

    let mut rows = vec![
        AssociatedTableRow::Config(older),
        AssociatedTableRow::Config(newer),
    ];
    sort_associated_table_rows(
        &mut rows,
        &[
            MapiSortOrder {
                property_tag: PID_TAG_MESSAGE_CLASS_W,
                order: 0,
            },
            MapiSortOrder {
                property_tag: PID_TAG_LAST_MODIFICATION_TIME,
                order: 1,
            },
        ],
        Uuid::nil(),
    );

    assert_eq!(associated_table_row_id(&rows[0]), newer_id);
    assert_eq!(associated_table_row_id(&rows[1]), older_id);
}

fn assert_inbox_associated_find_row_no_match_for_message_class(message_class: &str) {
    let response = inbox_associated_find_row_response_for_message_class(message_class);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
    assert_eq!(response.len(), 6);
}

fn assert_inbox_associated_find_row_returns_message_class(message_class: &str) {
    let response = inbox_associated_find_row_response_for_message_class(message_class);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[6], 0);
    assert_eq!(response[7], 1);
    assert_response_contains_utf16(&response, message_class);
}

fn inbox_associated_find_row_response_for_message_class(message_class: &str) -> Vec<u8> {
    let snapshot = MapiMailStoreSnapshot::empty();
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: true,
        columns: vec![PID_TAG_MESSAGE_CLASS_W],
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, message_class);
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil())
}

fn assert_contact_folder_associated_find_row_returns_osc_contact_sync(folder_id: u64) {
    let snapshot = MapiMailStoreSnapshot::empty();
    assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
        folder_id, &snapshot,
    );
}

fn assert_contact_folder_associated_find_row_returns_osc_contact_sync_for_snapshot(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) {
    assert_contact_folder_associated_find_row_returns_config(
        folder_id,
        "IPM.Microsoft.OSC.ContactSync",
        snapshot,
    );
}

fn assert_contact_folder_associated_find_row_returns_config(
    folder_id: u64,
    message_class: &str,
    snapshot: &MapiMailStoreSnapshot,
) {
    let response = contact_folder_associated_find_row_response(folder_id, message_class, snapshot);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(u32::from_le_bytes(response[3..7].try_into().unwrap()), 0);
    assert_eq!(response[7], 1);
    let mut encoded_message_class = Vec::new();
    write_utf16z(&mut encoded_message_class, message_class);
    assert!(response
        .windows(encoded_message_class.len())
        .any(|window| window == encoded_message_class.as_slice()));
}

fn assert_contact_folder_associated_find_row_does_not_return_config(
    folder_id: u64,
    message_class: &str,
    snapshot: &MapiMailStoreSnapshot,
) {
    let response = contact_folder_associated_find_row_response(folder_id, message_class, snapshot);

    assert_eq!(response[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
    assert_eq!(response.len(), 6);
}

fn contact_folder_associated_find_row_response(
    folder_id: u64,
    message_class: &str,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let mut table = MapiObject::ContentsTable {
        folder_id,
        associated: true,
        columns: vec![
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_MESSAGE_CLASS_W,
        ],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        }],
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, message_class);
    let mut payload = vec![0];
    payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    payload.extend_from_slice(&restriction);
    payload.push(1);
    payload.extend_from_slice(&0u16.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::FindRow.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    rop_find_row_response(&request, Some(&mut table), &[], &[], &snapshot, Uuid::nil())
}

fn inbox_associated_sort_snapshot() -> MapiMailStoreSnapshot {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let persisted_id = Uuid::from_u128(0x6d617069_6163_6350_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        persisted_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 81,
        ),
    );
    MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "Account prefs".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
    ])
}

fn inbox_associated_extended_rule_snapshot() -> MapiMailStoreSnapshot {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let persisted_id = Uuid::from_u128(0x6d617069_6578_5275_8000_000000000101);
    crate::mapi::identity::remember_mapi_identity(
        persisted_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 83,
        ),
    );
    MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: persisted_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.ExtendedRule.Message".to_string(),
            subject: "Junk E-mail Rule".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "392d30"}
            }),
        },
    ])
}

fn common_views_sort_snapshot(account_id: Uuid) -> MapiMailStoreSnapshot {
    let zulu_id = Uuid::from_u128(0x6d617069_776c_5a75_8000_000000000001);
    let alpha_id = Uuid::from_u128(0x6d617069_776c_416c_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        zulu_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 111,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        alpha_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 112,
        ),
    );
    let group_header_id = Some(default_wlink_group_uuid());
    MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: zulu_id,
            account_id,
            subject: "Zulu".to_string(),
            target_folder_id: Some(SENT_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0x20,
            group_header_id,
            group_name: "Mail".to_string(),
        },
        crate::store::MapiNavigationShortcutRecord {
            id: alpha_id,
            account_id,
            subject: "Alpha".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0x10,
            group_header_id,
            group_name: "Mail".to_string(),
        },
    ])
}

fn test_table_email(id: Uuid, mailbox_id: Uuid, subject: &str) -> JmapEmail {
    JmapEmail {
        id,
        thread_id: Uuid::from_u128(0x5555),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 1,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: vec![lpe_storage::JmapEmailMailboxState {
            mailbox_id,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            modseq: 1,
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
        received_at: "2026-06-09T20:00:00Z".to_string(),
        sent_at: Some("2026-06-09T20:00:00Z".to_string()),
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: subject.to_string(),
        preview: String::new(),
        body_text: String::new(),
        body_html_sanitized: None,
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
        internet_message_id: Some(format!("<{}@example.test>", id)),
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    }
}

fn assert_response_contains_utf16(response: &[u8], value: &str) {
    assert!(
        utf16_position(response, value).is_some(),
        "response did not contain {value}"
    );
}

fn utf16_position(response: &[u8], value: &str) -> Option<usize> {
    let mut encoded = Vec::new();
    write_utf16z(&mut encoded, value);
    response
        .windows(encoded.len())
        .position(|window| window == encoded.as_slice())
}

#[test]
fn message_row_projects_containing_folder_ids() {
    let email_id = Uuid::from_u128(0x7171);
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(0x81),
    );
    let mut email = test_table_email(email_id, Uuid::from_u128(0x8181), "Test Draft");
    email.mailbox_role = "drafts".to_string();

    let row = serialize_message_row(
        &email,
        &[PID_TAG_FOLDER_ID, PID_TAG_PARENT_FOLDER_ID, PID_TAG_MID],
    );

    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&row[0..8]),
        Some(DRAFTS_FOLDER_ID)
    );
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&row[8..16]),
        Some(DRAFTS_FOLDER_ID)
    );
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&row[16..24]),
        Some(mapi_message_id(&email))
    );
}

#[test]
fn normal_message_row_projects_outlook_inbox_view_columns() {
    let email_id = Uuid::from_u128(0x7172);
    let mut email = test_table_email(email_id, Uuid::from_u128(0x8182), "Inbox row");
    email.received_at = "2026-06-20T16:28:38Z".to_string();
    email.from_display = Some("Denis Ducret".to_string());
    email.from_address = "denis.ducret@sdic.ch".to_string();
    email.sender_display = Some("Delegate Sender".to_string());
    email.sender_address = Some("delegate@example.test".to_string());
    email.size_octets = 2048;
    email.has_attachments = true;
    email.followup_flag_status = "flagged".to_string();
    email.reminder_set = true;
    email.categories = vec!["Blue".to_string(), "Customer".to_string()];
    let expected_time = mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at);
    let columns = [
        PID_TAG_CREATION_TIME,
        PID_TAG_IMPORTANCE,
        PID_TAG_PRIORITY,
        PID_TAG_SENSITIVITY,
        PID_LID_REMINDER_SET_TAG,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_SUBJECT_PREFIX_W,
        PID_TAG_FLAG_STATUS,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_NAME_W,
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_MESSAGE_SIZE_EXTENDED,
        PID_NAME_KEYWORDS_TAG,
        PID_LID_OUTLOOK_COMMON_8514_TAG,
        0x8017_000B,
        PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG,
        PID_NAME_CONTENT_CLASS_W_TAG,
    ];

    let row = serialize_message_row(&email, &columns);
    let mut cursor = Cursor::new(&row);

    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_CREATION_TIME).unwrap(),
        MapiValue::I64(expected_time as i64)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_IMPORTANCE).unwrap(),
        MapiValue::I32(1)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_PRIORITY).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SENSITIVITY).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_LID_REMINDER_SET_TAG).unwrap(),
        MapiValue::Bool(true)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_CLASS_W).unwrap(),
        MapiValue::String("IPM.Note".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SUBJECT_PREFIX_W).unwrap(),
        MapiValue::String(String::new())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_FLAG_STATUS).unwrap(),
        MapiValue::I32(2)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_HAS_ATTACHMENTS).unwrap(),
        MapiValue::Bool(true)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SENDER_NAME_W).unwrap(),
        MapiValue::String("Delegate Sender".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SENDER_EMAIL_ADDRESS_W).unwrap(),
        MapiValue::String("delegate@example.test".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SENT_REPRESENTING_NAME_W).unwrap(),
        MapiValue::String("Denis Ducret".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W).unwrap(),
        MapiValue::String("SMTP".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W).unwrap(),
        MapiValue::String("denis.ducret@sdic.ch".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_DELIVERY_TIME).unwrap(),
        MapiValue::I64(expected_time as i64)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE).unwrap(),
        MapiValue::I32(2048)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_MESSAGE_SIZE_EXTENDED).unwrap(),
        MapiValue::I64(2048)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_NAME_KEYWORDS_TAG).unwrap(),
        MapiValue::MultiString(vec!["Blue".to_string(), "Customer".to_string()])
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_LID_OUTLOOK_COMMON_8514_TAG).unwrap(),
        MapiValue::Bool(false)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, 0x8017_000B).unwrap(),
        MapiValue::Bool(false)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG).unwrap(),
        MapiValue::Bool(false)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_NAME_CONTENT_CLASS_W_TAG).unwrap(),
        MapiValue::String("urn:content-classes:message".to_string())
    );
}

#[test]
fn normal_message_row_projects_microsoft_view_descriptor_string8_columns() {
    let email_id = Uuid::from_u128(0x7173);
    let mut email = test_table_email(email_id, Uuid::from_u128(0x8183), "ANSI subject");
    email.from_display = Some("Denis Ducret".to_string());
    email.categories = vec!["Blue".to_string(), "Customer".to_string()];
    let message_class_a = (PID_TAG_MESSAGE_CLASS_W & 0xFFFF_0000) | 0x001E;
    let sent_representing_name_a = (PID_TAG_SENT_REPRESENTING_NAME_W & 0xFFFF_0000) | 0x001E;
    let subject_a = (PID_TAG_SUBJECT_W & 0xFFFF_0000) | 0x001E;
    let keywords_a = (PID_NAME_KEYWORDS_TAG & 0xFFFF_0000) | 0x101E;

    let row = serialize_message_row(
        &email,
        &[
            message_class_a,
            sent_representing_name_a,
            subject_a,
            keywords_a,
        ],
    );
    let mut cursor = Cursor::new(&row);

    assert_eq!(
        parse_mapi_property_value(&mut cursor, message_class_a).unwrap(),
        MapiValue::String("IPM.Note".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, sent_representing_name_a).unwrap(),
        MapiValue::String("Denis Ducret".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, subject_a).unwrap(),
        MapiValue::String("ANSI subject".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, keywords_a).unwrap(),
        MapiValue::MultiString(vec!["Blue".to_string(), "Customer".to_string()])
    );
}

#[test]
fn access_rows_follow_microsoft_flags() {
    let mailbox = JmapMailbox {
        id: Uuid::nil(),
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    let mailbox_row =
        serialize_folder_row_with_context(&mailbox, &[], &[PID_TAG_ACCESS], Uuid::nil());
    assert_eq!(
        u32::from_le_bytes(mailbox_row.try_into().unwrap()),
        MAPI_FOLDER_ACCESS
    );

    let root_row = serialize_special_folder_row(ROOT_FOLDER_ID, &[], &[PID_TAG_ACCESS], None);
    assert_eq!(
        u32::from_le_bytes(root_row.try_into().unwrap()),
        MAPI_FOLDER_ACCESS
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ACCESS, Uuid::nil()),
        Some(MapiValue::U32(MAPI_FOLDER_ACCESS))
    );
}

#[test]
fn reminders_folder_projects_reminder_container_class() {
    let row = serialize_special_folder_row(
        REMINDERS_FOLDER_ID,
        &[],
        &[PID_TAG_CONTAINER_CLASS_W, PID_TAG_MESSAGE_CLASS_W],
        None,
    );
    let expected = utf16z_test_bytes("Outlook.Reminder");

    assert_eq!(&row[..expected.len()], expected.as_slice());
    assert_eq!(&row[expected.len()..], expected.as_slice());
}

#[test]
fn reminders_folder_projects_default_post_message_class() {
    assert_eq!(
        special_folder_property_value(
            REMINDERS_FOLDER_ID,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            Uuid::nil()
        ),
        Some(MapiValue::String("IPM.Note".to_string()))
    );

    let row = serialize_special_folder_row(
        REMINDERS_FOLDER_ID,
        &[],
        &[PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W],
        None,
    );

    assert_eq!(row, utf16z_test_bytes("IPM.Note"));
}

#[test]
fn special_folder_property_projects_record_key() {
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_RECORD_KEY, Uuid::nil()),
        Some(MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            INBOX_FOLDER_ID
        )))
    );
}

#[test]
fn special_folder_property_projects_empty_archive_policy_defaults() {
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ARCHIVE_TAG, Uuid::nil()),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_POLICY_TAG, Uuid::nil()),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_RETENTION_PERIOD, Uuid::nil()),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_RETENTION_FLAGS, Uuid::nil()),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ARCHIVE_PERIOD, Uuid::nil()),
        Some(MapiValue::U32(0))
    );
}

#[test]
fn special_folder_property_projects_view_defaults_for_outlook_folders() {
    let account_id = Uuid::from_u128(0xaaaaaaaa_aaaa_4aaa_8aaa_aaaaaaaaaaaa);
    for folder_id in [
        INBOX_FOLDER_ID,
        OUTBOX_FOLDER_ID,
        SENT_FOLDER_ID,
        TRASH_FOLDER_ID,
        DRAFTS_FOLDER_ID,
        JUNK_FOLDER_ID,
        ARCHIVE_FOLDER_ID,
        CONVERSATION_HISTORY_FOLDER_ID,
        CONTACTS_SEARCH_FOLDER_ID,
        CONTACTS_FOLDER_ID,
        CALENDAR_FOLDER_ID,
    ] {
        assert!(
            matches!(
                special_folder_property_value(folder_id, PID_TAG_DEFAULT_VIEW_ENTRY_ID, account_id),
                Some(MapiValue::Binary(value)) if !value.is_empty()
            ),
            "folder 0x{folder_id:016x} should project a default view entry id"
        );
    }
    for folder_id in [
        DEFERRED_ACTION_FOLDER_ID,
        FREEBUSY_DATA_FOLDER_ID,
        TRACKED_MAIL_PROCESSING_FOLDER_ID,
        SUGGESTED_CONTACTS_FOLDER_ID,
        QUICK_CONTACTS_FOLDER_ID,
        IM_CONTACT_LIST_FOLDER_ID,
        JOURNAL_FOLDER_ID,
        NOTES_FOLDER_ID,
        TASKS_FOLDER_ID,
        TODO_SEARCH_FOLDER_ID,
        IPM_SUBTREE_FOLDER_ID,
        SYNC_ISSUES_FOLDER_ID,
        CONFLICTS_FOLDER_ID,
        LOCAL_FAILURES_FOLDER_ID,
        SERVER_FAILURES_FOLDER_ID,
        RSS_FEEDS_FOLDER_ID,
        QUICK_STEP_SETTINGS_FOLDER_ID,
    ] {
        assert_eq!(
            special_folder_property_value(folder_id, PID_TAG_DEFAULT_VIEW_ENTRY_ID, account_id),
            None
        );
    }
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_FORM_FLAGS, Uuid::nil()),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_WEBVIEWINFO, Uuid::nil()),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_XVIEWINFO_E, Uuid::nil()),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_VIEWS_ONLY, Uuid::nil()),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_DEFAULT_FORM_NAME_W, Uuid::nil()),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_FORM_STORAGE, Uuid::nil()),
        Some(MapiValue::Binary(Vec::new()))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_ACL_MEMBER_NAME_W, Uuid::nil()),
        Some(MapiValue::String(String::new()))
    );
    assert_eq!(
        special_folder_property_value(INBOX_FOLDER_ID, PID_TAG_FOLDER_VIEWLIST_FLAGS, Uuid::nil()),
        Some(MapiValue::U32(0))
    );
    assert_eq!(
        special_folder_property_value(
            FREEBUSY_DATA_FOLDER_ID,
            PID_TAG_CONTAINER_CLASS_W,
            account_id
        ),
        Some(MapiValue::String("IPF.Note".to_string()))
    );
    assert_eq!(
        special_folder_property_value(
            FREEBUSY_DATA_FOLDER_ID,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            account_id
        ),
        Some(MapiValue::String("IPM.Note".to_string()))
    );
}

#[test]
fn configuration_special_folder_projects_default_post_message_class() {
    assert_eq!(
        special_folder_property_value(
            QUICK_STEP_SETTINGS_FOLDER_ID,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
            Uuid::nil()
        ),
        Some(MapiValue::String("IPM.Configuration".to_string()))
    );
    assert_eq!(
        special_folder_property_value(
            QUICK_STEP_SETTINGS_FOLDER_ID,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
            Uuid::nil()
        ),
        Some(MapiValue::String("IPM.Configuration".to_string()))
    );

    let row = serialize_special_folder_row(
        QUICK_STEP_SETTINGS_FOLDER_ID,
        &[],
        &[
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ],
        None,
    );
    let ascii = b"IPM.Configuration\0";
    assert!(row.windows(ascii.len()).any(|window| window == ascii));
    assert!(row
        .windows(utf16z_test_bytes("IPM.Configuration").len())
        .any(|window| window == utf16z_test_bytes("IPM.Configuration")));
}

#[test]
fn ipm_subtree_row_projects_default_post_message_class() {
    let row = serialize_special_folder_row(
        IPM_SUBTREE_FOLDER_ID,
        &[],
        &[
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ],
        None,
    );
    let ascii = b"IPM.Note\0";
    assert!(row.windows(ascii.len()).any(|window| window == ascii));
    assert!(row
        .windows(utf16z_test_bytes("IPM.Note").len())
        .any(|window| window == utf16z_test_bytes("IPM.Note")));
}

#[test]
fn ms_oxosfld_none_container_classes_serialize_as_empty_strings() {
    for folder_id in [
        ROOT_FOLDER_ID,
        DEFERRED_ACTION_FOLDER_ID,
        SPOOLER_QUEUE_FOLDER_ID,
        COMMON_VIEWS_FOLDER_ID,
        VIEWS_FOLDER_ID,
    ] {
        let row = serialize_special_folder_row(folder_id, &[], &[PID_TAG_CONTAINER_CLASS_W], None);
        assert_eq!(row, utf16z_test_bytes(""));
    }

    let row = serialize_special_folder_row(
        FREEBUSY_DATA_FOLDER_ID,
        &[],
        &[PID_TAG_CONTAINER_CLASS_W],
        None,
    );
    assert_eq!(row, utf16z_test_bytes("IPF.Note"));
}

#[test]
fn attachment_rows_use_by_value_method() {
    let attachment = MapiAttachment {
        attach_num: 0,
        canonical_id: Uuid::nil(),
        file_reference: "file-ref".to_string(),
        file_name: "report.pdf".to_string(),
        media_type: "application/pdf".to_string(),
        disposition: None,
        content_id: None,
        size_octets: 16,
    };

    let row = serialize_attachment_row(&attachment, &[PID_TAG_ATTACH_METHOD]);
    assert_eq!(u32::from_le_bytes(row.try_into().unwrap()), ATTACH_BY_VALUE);
}

#[test]
fn attachment_row_projects_microsoft_message_attachment_example_columns() {
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
    let columns = [
        PID_TAG_ATTACH_METHOD,
        PID_TAG_RENDERING_POSITION,
        PID_TAG_ATTACHMENT_FLAGS,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ATTACHMENT_LINK_ID,
        PID_TAG_ATTACH_FLAGS,
        PID_TAG_ATTACHMENT_HIDDEN,
        PID_TAG_ATTACH_LONG_FILENAME_W,
        PID_TAG_ATTACH_FILENAME_W,
        PID_TAG_ATTACH_EXTENSION_W,
        PID_TAG_ATTACH_MIME_TAG_W,
        PID_TAG_ATTACH_CONTENT_ID_W,
        PID_TAG_ATTACH_RENDERING,
        PID_TAG_CREATION_TIME,
        PID_TAG_LAST_MODIFICATION_TIME,
    ];

    let row = serialize_attachment_row(&attachment, &columns);
    let mut cursor = Cursor::new(&row);

    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_METHOD).unwrap(),
        MapiValue::I32(ATTACH_BY_VALUE as i32)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_RENDERING_POSITION).unwrap(),
        MapiValue::I32(-1)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_FLAGS).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_DISPLAY_NAME_W).unwrap(),
        MapiValue::String("test.txt".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_LINK_ID).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FLAGS).unwrap(),
        MapiValue::I32(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_HIDDEN).unwrap(),
        MapiValue::Bool(false)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_LONG_FILENAME_W).unwrap(),
        MapiValue::String("test.txt".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FILENAME_W).unwrap(),
        MapiValue::String("test.txt".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_EXTENSION_W).unwrap(),
        MapiValue::String(".txt".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_MIME_TAG_W).unwrap(),
        MapiValue::String("text/plain".to_string())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_CONTENT_ID_W).unwrap(),
        MapiValue::String(String::new())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_RENDERING).unwrap(),
        MapiValue::Binary(Vec::new())
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_CREATION_TIME).unwrap(),
        MapiValue::I64(0)
    );
    assert_eq!(
        parse_mapi_property_value(&mut cursor, PID_TAG_LAST_MODIFICATION_TIME).unwrap(),
        MapiValue::I64(0)
    );
}

#[test]
fn attachment_row_projects_microsoft_inline_image_example_columns() {
    let attachment = MapiAttachment {
        attach_num: 1,
        canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
        file_reference: "attachment:message:inline-image".to_string(),
        file_name: "image001.PNG".to_string(),
        media_type: "image/png".to_string(),
        disposition: Some("inline".to_string()),
        content_id: Some("image001.PNG@01C86E1C.F1954390".to_string()),
        size_octets: 1024,
    };
    let columns = [
        PID_TAG_ATTACH_METHOD,
        PID_TAG_RENDERING_POSITION,
        PID_TAG_ATTACHMENT_FLAGS,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ATTACHMENT_LINK_ID,
        PID_TAG_ATTACH_FLAGS,
        PID_TAG_ATTACHMENT_HIDDEN,
        PID_TAG_ATTACH_LONG_FILENAME_W,
        PID_TAG_ATTACH_FILENAME_W,
        PID_TAG_ATTACH_EXTENSION_W,
        PID_TAG_ATTACH_MIME_TAG_W,
        PID_TAG_ATTACH_CONTENT_ID_W,
        PID_TAG_ATTACH_RENDERING,
    ];

    for row in [
        serialize_attachment_row(&attachment, &columns),
        serialize_saved_attachment_row(
            attachment.attach_num,
            &attachment.file_reference,
            &attachment.file_name,
            &attachment.media_type,
            attachment.disposition.as_deref(),
            attachment.content_id.as_deref(),
            attachment.size_octets,
            &columns,
        ),
        serialize_pending_attachment_row(
            0,
            &HashMap::from([
                (PID_TAG_ATTACH_METHOD, MapiValue::U32(ATTACH_BY_VALUE)),
                (PID_TAG_RENDERING_POSITION, MapiValue::U32(u32::MAX)),
                (PID_TAG_ATTACHMENT_FLAGS, MapiValue::U32(0)),
                (
                    PID_TAG_DISPLAY_NAME_W,
                    MapiValue::String("image001.PNG".to_string()),
                ),
                (PID_TAG_ATTACHMENT_LINK_ID, MapiValue::U32(0)),
                (PID_TAG_ATTACH_FLAGS, MapiValue::U32(4)),
                (PID_TAG_ATTACHMENT_HIDDEN, MapiValue::Bool(true)),
                (
                    PID_TAG_ATTACH_LONG_FILENAME_W,
                    MapiValue::String("image001.PNG".to_string()),
                ),
                (
                    PID_TAG_ATTACH_FILENAME_W,
                    MapiValue::String("image001.PNG".to_string()),
                ),
                (
                    PID_TAG_ATTACH_EXTENSION_W,
                    MapiValue::String(".PNG".to_string()),
                ),
                (
                    PID_TAG_ATTACH_MIME_TAG_W,
                    MapiValue::String("image/png".to_string()),
                ),
                (
                    PID_TAG_ATTACH_CONTENT_ID_W,
                    MapiValue::String("image001.PNG@01C86E1C.F1954390".to_string()),
                ),
                (PID_TAG_ATTACH_RENDERING, MapiValue::Binary(Vec::new())),
            ]),
            &[],
            &columns,
        ),
    ] {
        let mut cursor = Cursor::new(&row);
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_METHOD).unwrap(),
            MapiValue::I32(ATTACH_BY_VALUE as i32)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_RENDERING_POSITION).unwrap(),
            MapiValue::I32(-1)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_FLAGS).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_DISPLAY_NAME_W).unwrap(),
            MapiValue::String("image001.PNG".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_LINK_ID).unwrap(),
            MapiValue::I32(0)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FLAGS).unwrap(),
            MapiValue::I32(4)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACHMENT_HIDDEN).unwrap(),
            MapiValue::Bool(true)
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_LONG_FILENAME_W).unwrap(),
            MapiValue::String("image001.PNG".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_FILENAME_W).unwrap(),
            MapiValue::String("image001.PNG".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_EXTENSION_W).unwrap(),
            MapiValue::String(".PNG".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_MIME_TAG_W).unwrap(),
            MapiValue::String("image/png".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_CONTENT_ID_W).unwrap(),
            MapiValue::String("image001.PNG@01C86E1C.F1954390".to_string())
        );
        assert_eq!(
            parse_mapi_property_value(&mut cursor, PID_TAG_ATTACH_RENDERING).unwrap(),
            MapiValue::Binary(Vec::new())
        );
    }
}

#[test]
fn categorized_table_expand_collapse_require_set_columns() {
    let category_id = category_id_for_value(INBOX_FOLDER_ID, PID_TAG_SUBJECT_W, "Alpha");
    let mut table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: Vec::new(),
        columns_set: false,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_SUBJECT_W,
            order: 0,
        }],
        category_count: 1,
        expanded_count: 1,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut expand_payload = 1u16.to_le_bytes().to_vec();
    expand_payload.extend_from_slice(&category_id.to_le_bytes());
    let expand = rop_expand_row_response(
        &RopRequest {
            rop_id: RopId::ExpandRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: expand_payload,
        },
        Some(&mut table),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );
    assert_eq!(expand[0], RopId::ExpandRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(expand[2..6].try_into().unwrap()),
        0x0000_04B9
    );

    let collapse = rop_collapse_row_response(
        &RopRequest {
            rop_id: RopId::CollapseRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: category_id.to_le_bytes().to_vec(),
        },
        Some(&mut table),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );
    assert_eq!(collapse[0], RopId::CollapseRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(collapse[2..6].try_into().unwrap()),
        0x0000_04B9
    );
}

#[test]
fn microsoft_categorized_expand_collapse_report_current_state_errors() {
    let mailbox_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let email_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 904,
        ),
    );
    let email = test_table_email(email_id, mailbox_id, "Alpha");
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let emails = vec![email];
    let category_id = category_id_for_value(INBOX_FOLDER_ID, PID_TAG_SUBJECT_W, "Alpha");
    let mut expanded_table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_SUBJECT_W,
            order: 0,
        }],
        category_count: 1,
        expanded_count: 1,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let mut expand_payload = 1u16.to_le_bytes().to_vec();
    expand_payload.extend_from_slice(&category_id.to_le_bytes());
    let expand = rop_expand_row_response(
        &RopRequest {
            rop_id: RopId::ExpandRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: expand_payload,
        },
        Some(&mut expanded_table),
        &mailboxes,
        &emails,
        &MapiMailStoreSnapshot::empty(),
    );
    assert_eq!(expand[0], RopId::ExpandRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(expand[2..6].try_into().unwrap()),
        0x0000_04F8
    );

    let mut collapsed_categories = HashSet::new();
    collapsed_categories.insert(category_id);
    let mut collapsed_table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W],
        columns_set: true,
        sort_orders: vec![MapiSortOrder {
            property_tag: PID_TAG_SUBJECT_W,
            order: 0,
        }],
        category_count: 1,
        expanded_count: 1,
        collapsed_categories,
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    };
    let collapse = rop_collapse_row_response(
        &RopRequest {
            rop_id: RopId::CollapseRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: category_id.to_le_bytes().to_vec(),
        },
        Some(&mut collapsed_table),
        &mailboxes,
        &emails,
        &MapiMailStoreSnapshot::empty(),
    );
    assert_eq!(collapse[0], RopId::CollapseRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(collapse[2..6].try_into().unwrap()),
        0x0000_04F7
    );
}

#[test]
fn microsoft_table_bookmark_and_collapse_rops_require_set_columns() {
    fn table() -> MapiObject {
        let bookmark = 1u32.to_le_bytes().to_vec();
        let mut bookmarks = HashMap::new();
        bookmarks.insert(
            bookmark,
            TableBookmark {
                position: 0,
                row_key: None,
            },
        );
        MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 1,
            expanded_count: 1,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks,
            next_bookmark: 2,
            position: 0,
        }
    }

    let bookmark = 1u32.to_le_bytes().to_vec();
    let mut seek_payload = Vec::new();
    seek_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    seek_payload.extend_from_slice(&bookmark);
    seek_payload.extend_from_slice(&0i32.to_le_bytes());
    seek_payload.push(0);
    let mut seek_table = table();
    let seek = rop_seek_row_bookmark_response(
        &RopRequest {
            rop_id: RopId::SeekRowBookmark.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: seek_payload,
        },
        Some(&mut seek_table),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        Uuid::nil(),
    );
    assert_eq!(seek[0], RopId::SeekRowBookmark.as_u8());
    assert_eq!(
        u32::from_le_bytes(seek[2..6].try_into().unwrap()),
        0x0000_04B9
    );

    let mut free_payload = Vec::new();
    free_payload.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    free_payload.extend_from_slice(&bookmark);
    let mut free_table = table();
    let free = rop_free_bookmark_response(
        &RopRequest {
            rop_id: RopId::FreeBookmark.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: free_payload,
        },
        Some(&mut free_table),
    );
    assert_eq!(free[0], RopId::FreeBookmark.as_u8());
    assert_eq!(
        u32::from_le_bytes(free[2..6].try_into().unwrap()),
        0x0000_04B9
    );

    let get = rop_get_collapse_state_response(
        &RopRequest {
            rop_id: RopId::GetCollapseState.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: Vec::new(),
        },
        Some(&table()),
    );
    assert_eq!(get[0], RopId::GetCollapseState.as_u8());
    assert_eq!(
        u32::from_le_bytes(get[2..6].try_into().unwrap()),
        0x0000_04B9
    );

    let mut collapse_state = Vec::new();
    collapse_state.extend_from_slice(COLLAPSE_STATE_MAGIC);
    write_u64(&mut collapse_state, INBOX_FOLDER_ID);
    write_u64(&mut collapse_state, 0);
    write_u32(&mut collapse_state, 0);
    write_u32(&mut collapse_state, 0);
    write_u16(&mut collapse_state, 1);
    write_u16(&mut collapse_state, 1);
    write_u16(&mut collapse_state, 0);
    let mut set_payload = Vec::new();
    set_payload.extend_from_slice(&(collapse_state.len() as u16).to_le_bytes());
    set_payload.extend_from_slice(&collapse_state);
    let mut set_table = table();
    let set = rop_set_collapse_state_response(
        &RopRequest {
            rop_id: RopId::SetCollapseState.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: set_payload,
        },
        Some(&mut set_table),
    );
    assert_eq!(set[0], RopId::SetCollapseState.as_u8());
    assert_eq!(
        u32::from_le_bytes(set[2..6].try_into().unwrap()),
        0x0000_04B9
    );
}

#[test]
fn microsoft_contents_table_query_find_and_expand_require_set_columns() {
    let mut table = MapiObject::ContentsTable {
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
    };

    let query = rop_query_rows_response(
        &RopRequest {
            rop_id: RopId::QueryRows.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: vec![0, 1, 1, 0],
        },
        Some(&mut table),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        Uuid::nil(),
    );
    assert_eq!(query[0], RopId::QueryRows.as_u8());
    assert_eq!(
        u32::from_le_bytes(query[2..6].try_into().unwrap()),
        0x0000_04B9
    );

    let mut restriction = vec![MapiRestrictionType::Property as u8, 0x04];
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    write_utf16z(&mut restriction, "Alpha");
    let mut find_payload = vec![0];
    find_payload.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    find_payload.extend_from_slice(&restriction);
    find_payload.push(1);
    find_payload.extend_from_slice(&0u16.to_le_bytes());
    let find = rop_find_row_response(
        &RopRequest {
            rop_id: RopId::FindRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: find_payload,
        },
        Some(&mut table),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        Uuid::nil(),
    );
    assert_eq!(find[0], RopId::FindRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(find[2..6].try_into().unwrap()),
        0x0000_04B9
    );

    let mut expand_payload = 1u16.to_le_bytes().to_vec();
    expand_payload.extend_from_slice(&0u64.to_le_bytes());
    let expand = rop_expand_row_response(
        &RopRequest {
            rop_id: RopId::ExpandRow.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: expand_payload,
        },
        Some(&mut table),
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );
    assert_eq!(expand[0], RopId::ExpandRow.as_u8());
    assert_eq!(
        u32::from_le_bytes(expand[2..6].try_into().unwrap()),
        0x0000_04B9
    );
}

fn utf16z_test_bytes(value: &str) -> Vec<u8> {
    value
        .encode_utf16()
        .chain(std::iter::once(0))
        .flat_map(u16::to_le_bytes)
        .collect()
}
