use super::super::*;
use super::*;

#[test]
fn quick_step_synthetic_folder_allows_associated_message_creation() {
    assert!(synthetic_folder_allows_create_message(
        QUICK_STEP_SETTINGS_FOLDER_ID
    ));
    assert!(synthetic_folder_allows_create_message(
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID
    ));
    assert!(!synthetic_folder_allows_create_message(0x7777_0001));
}

#[test]
fn freebusy_open_prefers_delegate_message_over_stale_associated_config_identity() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let delegate_id = Uuid::from_u128(0x64656c65_6761_7465_8000_000000000001);
    let stale_config_id = Uuid::from_u128(0x636f6e66_6967_6672_8000_000000000001);
    let object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 311,
    );
    crate::mapi::identity::remember_mapi_identity(delegate_id, object_id);
    crate::mapi::identity::remember_mapi_identity(stale_config_id, object_id);

    let snapshot = MapiMailStoreSnapshot::empty()
        .with_delegate_freebusy_messages(vec![lpe_storage::DelegateFreeBusyMessageObject {
            id: delegate_id,
            account_id,
            owner_account_id: account_id,
            owner_email: "owner@example.test".to_string(),
            message_kind: "freebusy".to_string(),
            subject: "Free/busy for owner@example.test".to_string(),
            body_text: "busy".to_string(),
            starts_at: None,
            ends_at: None,
            busy_status: None,
            payload_json: "{}".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }])
        .with_associated_configs(vec![crate::store::MapiAssociatedConfigRecord {
            id: stale_config_id,
            account_id,
            folder_id: FREEBUSY_DATA_FOLDER_ID,
            message_class: "IPM.Configuration.FreeBusy".to_string(),
            subject: "Stale FreeBusy associated config".to_string(),
            properties_json: serde_json::json!({}),
        }]);

    let selected =
        delegate_freebusy_message_for_open(&snapshot, FREEBUSY_DATA_FOLDER_ID, object_id);

    assert_eq!(
        selected.map(|message| message.message.subject.as_str()),
        Some("Free/busy for owner@example.test")
    );
}

#[test]
fn conversation_action_open_prefers_action_over_stale_associated_config_identity() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let action_id = Uuid::from_u128(0x636f6e76_6163_746e_8000_000000000001);
    let stale_config_id = Uuid::from_u128(0x636f6e66_6967_6361_8000_000000000001);
    let object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 312,
    );
    crate::mapi::identity::remember_mapi_identity(action_id, object_id);
    crate::mapi::identity::remember_mapi_identity(stale_config_id, object_id);

    let snapshot = MapiMailStoreSnapshot::empty()
        .with_conversation_actions(vec![lpe_storage::ConversationAction {
            id: action_id,
            conversation_id: action_id,
            subject: "Conversation Action".to_string(),
            categories_json: "[]".to_string(),
            move_folder_entry_id: None,
            move_store_entry_id: None,
            move_target_mailbox_id: None,
            max_delivery_time: None,
            last_applied_time: None,
            version: lpe_storage::CONVERSATION_ACTION_VERSION,
            processed: 0,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }])
        .with_associated_configs(vec![crate::store::MapiAssociatedConfigRecord {
            id: stale_config_id,
            account_id,
            folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            message_class: "IPM.Configuration.StaleConversationAction".to_string(),
            subject: "Stale Conversation Action associated config".to_string(),
            properties_json: serde_json::json!({}),
        }]);

    let selected = conversation_action_message_for_open(
        &snapshot,
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        object_id,
    );

    assert_eq!(
        selected.map(|message| message.action.subject),
        Some("Conversation Action".to_string())
    );
}

#[test]
fn common_views_open_projects_default_navigation_shortcut() {
    let shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
    let selected = navigation_shortcut_message_for_open(
        &MapiMailStoreSnapshot::empty(),
        COMMON_VIEWS_FOLDER_ID,
        shortcut_id,
    );

    assert_eq!(
        selected.map(|message| message.subject),
        Some("Inbox".to_string())
    );
}

#[test]
fn common_views_open_rejects_default_navigation_shortcut_from_wrong_folder() {
    let shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
    let selected = navigation_shortcut_message_for_open(
        &MapiMailStoreSnapshot::empty(),
        INBOX_FOLDER_ID,
        shortcut_id,
    );

    assert!(selected.is_none());
}

#[test]
fn common_views_open_rejects_default_named_view_from_wrong_folder() {
    let view_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
    let selected = common_view_named_view_message_for_open(
        &MapiMailStoreSnapshot::empty(),
        INBOX_FOLDER_ID,
        view_id,
    );

    assert!(selected.is_none());
}

#[test]
fn folder_default_named_view_open_materializes_for_inbox() {
    let selected = common_view_named_view_message_for_open(
        &MapiMailStoreSnapshot::empty(),
        INBOX_FOLDER_ID,
        crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
    );

    assert_eq!(
        selected.map(|message| (message.folder_id, message.name)),
        Some((INBOX_FOLDER_ID, "Compact".to_string()))
    );
}

#[test]
fn folder_default_named_view_open_materializes_for_supported_contact_folder() {
    let selected = common_view_named_view_message_for_open(
        &MapiMailStoreSnapshot::empty(),
        CONTACTS_FOLDER_ID,
        crate::mapi_store::outlook_default_folder_named_view_id(CONTACTS_FOLDER_ID),
    );

    assert_eq!(
        selected.map(|message| (message.folder_id, message.name)),
        Some((CONTACTS_FOLDER_ID, "Contacts".to_string()))
    );
    let legacy = common_view_named_view_message_for_open(
        &MapiMailStoreSnapshot::empty(),
        CONTACTS_FOLDER_ID,
        crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
    );
    assert_eq!(
        legacy.map(|message| message.id),
        Some(crate::mapi_store::outlook_default_folder_named_view_id(
            CONTACTS_FOLDER_ID
        ))
    );
}

#[test]
fn conversation_action_open_rejects_default_action_from_wrong_folder() {
    let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
    let selected = conversation_action_message_for_open(
        &MapiMailStoreSnapshot::empty(),
        COMMON_VIEWS_FOLDER_ID,
        action_id,
    );

    assert!(selected.is_none());
}

#[test]
fn virtual_default_conversation_action_set_properties_stages_pending_row() {
    let mut session = test_mapi_session();
    let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
    session.handles.insert(
        1,
        MapiObject::ConversationAction {
            folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            conversation_action_id: action_id,
        },
    );
    let handle_slots = vec![1];
    let request = RopRequest {
        rop_id: RopId::SetProperties.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let result = stage_virtual_conversation_action_property_values(
        &mut session,
        &handle_slots,
        &request,
        &MapiMailStoreSnapshot::empty(),
        vec![(
            PID_TAG_SUBJECT_W,
            MapiValue::String("Conversation Action Update".to_string()),
        )],
    );

    assert!(matches!(result, Some(Ok(()))));
    match session.handles.get(&1) {
        Some(MapiObject::PendingConversationAction { properties, .. }) => {
            assert_eq!(
                properties.get(&PID_TAG_SUBJECT_W),
                Some(&MapiValue::String("Conversation Action Update".to_string()))
            );
        }
        other => panic!("expected pending conversation action, got {other:?}"),
    }
}

#[test]
fn virtual_default_conversation_action_set_rejects_wrong_folder() {
    let mut session = test_mapi_session();
    let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
    session.handles.insert(
        1,
        MapiObject::ConversationAction {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            conversation_action_id: action_id,
        },
    );
    let handle_slots = vec![1];
    let request = RopRequest {
        rop_id: RopId::SetProperties.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let result = stage_virtual_conversation_action_property_values(
        &mut session,
        &handle_slots,
        &request,
        &MapiMailStoreSnapshot::empty(),
        vec![(
            PID_TAG_SUBJECT_W,
            MapiValue::String("Conversation Action Update".to_string()),
        )],
    );

    assert!(matches!(result, Some(Err(_))));
    assert!(matches!(
        session.handles.get(&1),
        Some(MapiObject::ConversationAction { .. })
    ));
}

#[test]
fn virtual_default_conversation_action_delete_properties_stages_pending_row() {
    let mut session = test_mapi_session();
    let action_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2);
    session.handles.insert(
        1,
        MapiObject::ConversationAction {
            folder_id: CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            conversation_action_id: action_id,
        },
    );
    let handle_slots = vec![1];
    let request = RopRequest {
        rop_id: RopId::DeleteProperties.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let result = stage_virtual_conversation_action_property_delete(
        &mut session,
        &handle_slots,
        &request,
        &MapiMailStoreSnapshot::empty(),
        &[PID_TAG_SUBJECT_W],
    );

    assert!(matches!(result, Some(Ok(()))));
    match session.handles.get(&1) {
        Some(MapiObject::PendingConversationAction { properties, .. }) => {
            assert!(!properties.contains_key(&PID_TAG_SUBJECT_W));
        }
        other => panic!("expected pending conversation action, got {other:?}"),
    }
}

#[test]
fn associated_config_stream_write_summary_names_roaming_xml() {
    let values = vec![
        (PID_TAG_ROAMING_DATATYPES, MapiValue::I32(2)),
        (
            PID_TAG_ROAMING_XML_STREAM,
            MapiValue::Binary(b"<xml/>".to_vec()),
        ),
        (0x685D_0003, MapiValue::I32(42)),
    ];

    let summary = associated_config_stream_write_summary(&values);

    assert!(summary.contains("PidTagRoamingDatatypes=i32"));
    assert!(summary.contains("PidTagRoamingXmlStream=binary:bytes=6"));
    assert!(summary.contains("OutlookConfigurationStamp=i32"));
}

#[test]
fn empty_inbox_message_list_settings_save_gets_persistable_stream_defaults() {
    let properties = HashMap::from([
        (
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String("IPM.Configuration.MessageListSettings".to_string()),
        ),
        (PID_TAG_ROAMING_DATATYPES, MapiValue::I32(0)),
    ]);

    assert!(is_empty_inbox_message_list_settings_placeholder(
        INBOX_FOLDER_ID,
        "IPM.Configuration.MessageListSettings",
        &properties
    ));

    let with_payload = HashMap::from([
        (
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String("IPM.Configuration.MessageListSettings".to_string()),
        ),
        (
            PID_TAG_ROAMING_DICTIONARY,
            MapiValue::Binary(b"<xml/>".to_vec()),
        ),
    ]);
    assert!(!is_empty_inbox_message_list_settings_placeholder(
        INBOX_FOLDER_ID,
        "IPM.Configuration.MessageListSettings",
        &with_payload
    ));

    let default = crate::mapi_store::outlook_inbox_message_list_settings_default();
    let persisted = message_list_settings_placeholder_persisted_properties(&default);
    assert_eq!(
        persisted
            .get(&PID_TAG_ROAMING_DATATYPES)
            .cloned()
            .and_then(MapiValue::into_u32),
        Some(0x0000_0004)
    );
    assert!(matches!(
        persisted.get(&PID_TAG_ROAMING_DICTIONARY),
        Some(MapiValue::Binary(bytes)) if !bytes.is_empty()
    ));
    assert!(!is_empty_inbox_message_list_settings_placeholder(
        INBOX_FOLDER_ID,
        "IPM.Configuration.MessageListSettings",
        &persisted
    ));
}

#[test]
fn associated_config_mutation_uses_saved_handle_when_snapshot_misses_row() {
    let config_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 219,
    );
    let saved = crate::mapi_store::MapiAssociatedConfigMessage {
        id: config_id,
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::from_u128(0x6d617069_6d6c_7343_8000_000000000219),
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        subject: "IPM.Configuration.MessageListSettings".to_string(),
        properties_json: serde_json::json!({
            "0x7c060003": {"type": "u32", "value": 4},
            "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
        }),
    };

    let resolved = associated_config_message_for_mutation(
        &MapiMailStoreSnapshot::empty(),
        INBOX_FOLDER_ID,
        config_id,
        Some(&saved),
    )
    .expect("saved handle fallback");

    assert_eq!(resolved.canonical_id, saved.canonical_id);
    assert_eq!(
        resolved.message_class,
        "IPM.Configuration.MessageListSettings"
    );
}

#[test]
fn associated_config_persist_normalizes_stale_configuration_roaming_dictionary() {
    let properties = HashMap::from([
        (
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String("IPM.Configuration.UMOLK.UserOptions".to_string()),
        ),
        (
            PID_TAG_ROAMING_DICTIONARY,
            MapiValue::Binary(b"<xml/>".to_vec()),
        ),
    ]);

    let normalized = normalized_associated_config_persisted_properties(
        "ipm.configuration.umolk.useroptions",
        &properties,
    );

    match normalized.get(&PID_TAG_ROAMING_DICTIONARY) {
        Some(MapiValue::Binary(value)) => {
            let text = std::str::from_utf8(value).expect("dictionary xml");
            assert!(text.contains("18-OLPrefsVersion"), "{text}");
            assert!(text.contains("9-1"), "{text}");
            assert_ne!(value.as_slice(), b"<xml/>");
        }
        other => panic!("unexpected dictionary value: {other:?}"),
    }
}

#[test]
fn associated_config_persist_normalizes_stale_umolk_minimal_dictionary() {
    let stale = br#"<?xml version="1.0" encoding="utf-8"?><UserConfiguration xmlns="dictionary.xsd"><Info version="LPE.1"/><Data><e k="18-OLPrefsVersion" v="9-0"/></Data></UserConfiguration>"#;
    let properties = HashMap::from([(
        PID_TAG_ROAMING_DICTIONARY,
        MapiValue::Binary(stale.to_vec()),
    )]);

    let normalized = normalized_associated_config_persisted_properties(
        "IPM.Configuration.UMOLK.UserOptions",
        &properties,
    );

    match normalized.get(&PID_TAG_ROAMING_DICTIONARY) {
        Some(MapiValue::Binary(value)) => {
            let text = std::str::from_utf8(value).expect("dictionary xml");
            assert!(text.contains(r#"Info version="Outlook.16""#), "{text}");
            assert!(text.contains(r#"v="9-1""#), "{text}");
            assert!(!text.contains(r#"Info version="LPE.1""#), "{text}");
            assert!(!text.contains(r#"v="9-0""#), "{text}");
        }
        other => panic!("unexpected dictionary value: {other:?}"),
    }
}

#[test]
fn associated_config_persist_leaves_non_configuration_roaming_dictionary_unchanged() {
    let properties = HashMap::from([(
        PID_TAG_ROAMING_DICTIONARY,
        MapiValue::Binary(b"<xml/>".to_vec()),
    )]);

    let normalized =
        normalized_associated_config_persisted_properties("IPM.Custom.Message", &properties);

    assert_eq!(
        normalized.get(&PID_TAG_ROAMING_DICTIONARY),
        Some(&MapiValue::Binary(b"<xml/>".to_vec()))
    );
}

#[test]
fn calendar_configuration_debug_contract_uses_roaming_properties() {
    let object = mapi_mailstore::SpecialMessageSyncFact {
        folder_id: CALENDAR_FOLDER_ID,
        item_id: 1,
        canonical_id: Uuid::nil(),
        associated: true,
        subject: "Calendar".to_string(),
        body_text: String::new(),
        message_class: "IPM.Configuration.Calendar".to_string(),
        last_modified_filetime: 0,
        message_size: 0,
        read_state: None,
        named_properties: vec![
            (
                PID_TAG_ROAMING_DATATYPES,
                mapi_mailstore::SpecialMessagePropertyValue::U32(4),
            ),
            (
                PID_TAG_ROAMING_DICTIONARY,
                mapi_mailstore::SpecialMessagePropertyValue::Binary(Vec::new()),
            ),
        ],
    };

    assert!(is_calendar_configuration_object(&object));
    let required_tags = format_calendar_required_property_tags(true, false);

    assert!(required_tags.contains("0x7c060003"));
    assert!(required_tags.contains("0x7c070102"));
    assert!(required_tags.contains("0x7c080102"));
    assert!(!required_tags.contains("0x00600040"));
    assert!(!required_tags.contains("0x820d0102"));
}

#[test]
fn inbox_associated_config_summary_reports_modeled_startup_rows() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let account_prefs_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000021);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000099999);
    let header_id = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
    crate::mapi::identity::remember_mapi_identity(
        account_prefs_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 997,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 999,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        header_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 998,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty()
        .with_associated_configs(vec![crate::store::MapiAssociatedConfigRecord {
            id: account_prefs_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.AccountPrefs".to_string(),
            subject: "IPM.Configuration.AccountPrefs".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        }])
        .with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: header_id,
                account_id,
                subject: "Mail".to_string(),
                target_folder_id: None,
                shortcut_type: 4,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0,
                group_header_id: Some(header_id),
                group_name: "Mail".to_string(),
            },
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Pinned Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 127,
                group_header_id: Some(header_id),
                group_name: "Mail".to_string(),
            },
        ]);

    let summary = format_inbox_associated_config_summary(INBOX_FOLDER_ID, true, &snapshot);

    assert!(
        summary.contains("class=IPM.Configuration.AccountPrefs"),
        "{summary}"
    );
    assert!(
        !summary.contains("class=IPM.Configuration.ELC"),
        "{summary}"
    );
    assert!(
        summary.contains("class=IPM.Configuration.MessageListSettings"),
        "{summary}"
    );
    assert!(
        !summary.contains("class=IPM.Configuration.UMOLK.UserOptions"),
        "{summary}"
    );
    assert!(
        !summary.contains("class=IPM.Microsoft.FolderDesign.NamedView"),
        "{summary}"
    );
    assert!(
        !summary.contains("class=IPM.Configuration.EAS"),
        "{summary}"
    );
    assert!(
        !summary.contains("class=IPM.Sharing.Configuration"),
        "{summary}"
    );
    assert!(!summary.contains("class=IPM.Sharing.Index"), "{summary}");
    assert!(!summary.contains("truncated="), "{summary}");
}

#[test]
fn ipm_configuration_contract_summary_reports_required_columns_and_streams() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let canonical_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        canonical_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 121,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: canonical_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "Message list settings".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
    ]);
    let columns = [
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_ROAMING_DATATYPES,
    ];
    let sort_orders = [
        MapiSortOrder {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            order: 0,
        },
        MapiSortOrder {
            property_tag: PID_TAG_LAST_MODIFICATION_TIME,
            order: 0,
        },
    ];

    let summary = format_ipm_configuration_contract_summary(
        INBOX_FOLDER_ID,
        true,
        &columns,
        &sort_orders,
        &snapshot,
    );

    assert!(summary.contains("not_selected_required_columns="));
    assert!(summary.contains("sort_by_message_class_then_lastmod=true"));
    assert!(summary.contains("row_issue_count=0"));
    assert!(summary.contains("datatypes=0x00000004"));
    assert!(summary.contains("has_dict=true"));
    assert!(summary.contains("associated_config_0e0b=binary:bytes="));
    assert!(!summary.contains("associated_config_0e0b=binary:bytes=0"));
}

#[test]
fn associated_config_wire_summary_uses_requested_position() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let first_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000011);
    let second_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000012);
    crate::mapi::identity::remember_mapi_identity(
        first_id,
        crate::mapi::identity::mapi_store_id(0x7011),
    );
    crate::mapi::identity::remember_mapi_identity(
        second_id,
        crate::mapi::identity::mapi_store_id(0x7012),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: first_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Custom.A".to_string(),
            subject: "A".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: second_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Custom.B".to_string(),
            subject: "B".to_string(),
            properties_json: serde_json::json!({
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
    ]);

    let summary = format_inbox_associated_wire_row_summary(
        account_id,
        INBOX_FOLDER_ID,
        true,
        1,
        true,
        1,
        &[],
        None,
        &[PID_TAG_MESSAGE_CLASS_W],
        &snapshot,
    );

    assert!(summary.contains("position=1"), "{summary}");
    assert!(summary.contains("class=IPM.Custom.B"), "{summary}");
    assert!(!summary.contains("class=IPM.Custom.A"), "{summary}");
}

#[test]
fn associated_config_debug_summaries_honor_table_restriction() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let eas_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000021);
    let umolk_id = Uuid::from_u128(0x6d617069_6970_6d43_8000_000000000022);
    crate::mapi::identity::remember_mapi_identity(
        eas_id,
        crate::mapi::identity::mapi_store_id(0x7021),
    );
    crate::mapi::identity::remember_mapi_identity(
        umolk_id,
        crate::mapi::identity::mapi_store_id(0x7022),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: eas_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.EAS".to_string(),
            subject: "IPM.Configuration.EAS".to_string(),
            properties_json: serde_json::json!({}),
        },
        crate::store::MapiAssociatedConfigRecord {
            id: umolk_id,
            account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            subject: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
            }),
        },
    ]);
    let restriction = MapiRestriction::Property {
        relop: 0x04,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String("IPM.Configuration.UMOLK.UserOptions".to_string()),
    };

    let window = format_inbox_associated_query_row_window(
        account_id,
        0,
        true,
        2,
        &[],
        Some(&restriction),
        &snapshot,
    );
    let values = format_outlook_query_row_values(
        account_id,
        INBOX_FOLDER_ID,
        true,
        0,
        true,
        2,
        &[],
        Some(&restriction),
        &[PID_TAG_MESSAGE_CLASS_W],
        &snapshot,
    );
    let wire = format_inbox_associated_wire_row_summary(
        account_id,
        INBOX_FOLDER_ID,
        true,
        0,
        true,
        2,
        &[],
        Some(&restriction),
        &[PID_TAG_MESSAGE_CLASS_W],
        &snapshot,
    );

    assert!(window.contains("total=1"), "{window}");
    assert!(
        window.contains("IPM.Configuration.UMOLK.UserOptions"),
        "{window}"
    );
    assert!(!window.contains("IPM.Configuration.EAS"), "{window}");
    assert!(
        values.contains("IPM.Configuration.UMOLK.UserOptions"),
        "{values}"
    );
    assert!(!values.contains("IPM.Configuration.EAS"), "{values}");
    assert!(
        wire.contains("IPM.Configuration.UMOLK.UserOptions"),
        "{wire}"
    );
    assert!(!wire.contains("IPM.Configuration.EAS"), "{wire}");
}

#[test]
fn inbox_associated_named_view_debug_summaries_report_folder_local_default_view() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = MapiMailStoreSnapshot::empty();
    let restriction = MapiRestriction::Property {
        relop: 0x04,
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: MapiValue::String("IPM.Microsoft.FolderDesign.NamedView".to_string()),
    };
    let columns = [
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_VIEW_DESCRIPTOR_VERSION,
    ];

    let window = format_inbox_associated_query_row_window(
        account_id,
        0,
        true,
        1,
        &[],
        Some(&restriction),
        &snapshot,
    );
    let values = format_outlook_query_row_values(
        account_id,
        INBOX_FOLDER_ID,
        true,
        0,
        true,
        1,
        &[],
        Some(&restriction),
        &columns,
        &snapshot,
    );
    let wire = format_inbox_associated_wire_row_summary(
        account_id,
        INBOX_FOLDER_ID,
        true,
        0,
        true,
        1,
        &[],
        Some(&restriction),
        &columns,
        &snapshot,
    );

    assert!(window.contains("total=1"), "{window}");
    assert!(
        values.contains("class=IPM.Microsoft.FolderDesign.NamedView"),
        "{values}"
    );
    assert!(
        wire.contains("class=IPM.Microsoft.FolderDesign.NamedView"),
        "{wire}"
    );
    let expected_id = format!(
        "id=0x{:016x}",
        crate::mapi_store::outlook_default_folder_named_view_id(INBOX_FOLDER_ID)
    );
    assert!(values.contains(&expected_id), "{values}");
    assert!(wire.contains(&expected_id), "{wire}");
}

#[test]
fn common_views_query_row_values_report_selected_wlink_columns() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000001);
    let shortcut_store_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 131,
    );
    crate::mapi::identity::remember_mapi_identity(shortcut_id, shortcut_store_id);
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
            ordinal: 0x10,
            group_header_id: Some(default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
    ]);

    let summary = format_outlook_query_row_values(
        account_id,
        COMMON_VIEWS_FOLDER_ID,
        true,
        0,
        true,
        20,
        &[],
        None,
        &[
            PID_TAG_FOLDER_ID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_SUBJECT_W,
            PID_TAG_WLINK_ENTRY_ID,
            PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
            PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
        ],
        &snapshot,
    );

    assert!(summary.contains("index=0"));
    assert!(summary.contains(&format!("0x67480014={COMMON_VIEWS_FOLDER_ID}")));
    assert!(summary.contains(&format!("0x674d0014={shortcut_store_id}")));
    assert!(summary.contains("0x674e0003=0"));
    assert!(summary.contains("0x0037001f=Pinned Inbox"));
    assert!(summary.contains("0x684c0102=binary:"));
    assert!(summary.contains("0x68910102=binary:"));
    assert!(summary.contains("0x80100102=binary:"));
}

#[test]
fn common_views_diagnostics_keep_named_views_for_wlink_columns() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = MapiMailStoreSnapshot::empty();
    let columns = default_navigation_shortcut_property_tags();

    let window = format_outlook_query_row_window(
        COMMON_VIEWS_FOLDER_ID,
        true,
        0,
        true,
        20,
        &[],
        None,
        &columns,
        account_id,
        &snapshot,
    );
    let values = format_outlook_query_row_values(
        account_id,
        COMMON_VIEWS_FOLDER_ID,
        true,
        0,
        true,
        20,
        &[],
        None,
        &columns,
        &snapshot,
    );

    assert!(window.contains("total=19"), "{window}");
    assert!(window.contains("FolderDesign.NamedView"), "{window}");
    assert!(window.contains("Sent To"), "{window}");
    assert!(values.contains("FolderDesign.NamedView"), "{values}");
}

#[test]
fn quick_step_associated_debug_summaries_report_custom_action_row() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let snapshot = MapiMailStoreSnapshot::empty();
    let columns = [PID_TAG_MESSAGE_CLASS_W, PID_TAG_ROAMING_XML_STREAM];

    assert_eq!(
        effective_contents_table_columns(QUICK_STEP_SETTINGS_FOLDER_ID, true, &[]),
        default_associated_config_columns()
    );

    let values = format_outlook_query_row_values(
        account_id,
        QUICK_STEP_SETTINGS_FOLDER_ID,
        true,
        0,
        true,
        1,
        &[],
        None,
        &columns,
        &snapshot,
    );
    let wire = format_inbox_associated_wire_row_summary(
        account_id,
        QUICK_STEP_SETTINGS_FOLDER_ID,
        true,
        0,
        true,
        1,
        &[],
        None,
        &columns,
        &snapshot,
    );

    assert!(values.contains("IPM.Microsoft.CustomAction"), "{values}");
    assert!(values.contains("0x7c080102=binary:bytes="), "{values}");
    assert!(wire.contains("class=IPM.Microsoft.CustomAction"), "{wire}");
    assert!(wire.contains("query_rows_len="), "{wire}");
}

#[test]
fn common_views_wlink_target_decoding_reports_inbox_match() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000001);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 131,
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
            ordinal: 0x10,
            group_header_id: Some(default_wlink_group_uuid()),
            group_name: "Mail".to_string(),
        },
    ]);

    let summary = format_common_views_wlink_target_decoding(account_id, &snapshot);

    assert!(summary.contains("subject=Pinned Inbox"));
    assert!(summary.contains(&format!("target_folder=0x{INBOX_FOLDER_ID:016x}")));
    assert!(summary.contains(&format!("entry_id_decoded=0x{INBOX_FOLDER_ID:016x}")));
    assert!(summary.contains("entry_id_matches_inbox=true"));
    assert!(summary.contains(&format!("source_key_decoded=0x{INBOX_FOLDER_ID:016x}")));
    assert!(summary.contains("source_key_matches_inbox=true"));
    assert!(summary.contains("store_entry_id=binary:bytes=46"));
    assert!(summary.contains("store_entry_id_decoded=0x0000000000010001"));
    assert!(summary.contains("store_entry_id_matches_private_store=true"));
    assert!(summary.contains("address_book_store_entry_id=binary:bytes=46"));
    assert!(summary.contains("address_book_store_entry_id_decoded=0x0000000000010001"));
    assert!(summary.contains("address_book_store_entry_id_matches_private_store=true"));
    assert!(summary.contains(&format!(
        "sharing_local_folder_id_decoded=0x{INBOX_FOLDER_ID:016x}"
    )));
    assert!(summary.contains("sharing_local_folder_id_matches_inbox=true"));
}

#[test]
fn common_views_wlink_contract_distinguishes_expected_link_defaults() {
    let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
    let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000088888);
    let header_id = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
    crate::mapi::identity::remember_mapi_identity(
        shortcut_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 888,
        ),
    );
    crate::mapi::identity::remember_mapi_identity(
        header_id,
        crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 887,
        ),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: header_id,
            account_id,
            subject: "Mail".to_string(),
            target_folder_id: None,
            shortcut_type: 4,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 0,
            group_header_id: Some(header_id),
            group_name: "Mail".to_string(),
        },
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id,
            subject: "Pinned Inbox".to_string(),
            target_folder_id: Some(INBOX_FOLDER_ID),
            shortcut_type: 0,
            flags: 0,
            save_stamp: 0,
            section: 1,
            ordinal: 127,
            group_header_id: Some(header_id),
            group_name: "Mail".to_string(),
        },
    ]);
    let columns = [
        PID_TAG_SUBJECT_W,
        PID_TAG_WLINK_ENTRY_ID,
        PID_TAG_WLINK_RECORD_KEY,
        PID_TAG_WLINK_STORE_ENTRY_ID,
        0x684f_0102,
        0x6850_0102,
        PID_TAG_WLINK_GROUP_NAME_W,
        PID_TAG_WLINK_SECTION,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_TYPE,
        PID_TAG_WLINK_FLAGS,
        PID_TAG_WLINK_SAVE_STAMP,
        0x6842_0102,
        PID_TAG_WLINK_CALENDAR_COLOR,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        PID_TAG_WLINK_CLIENT_ID,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        PID_TAG_WLINK_RO_GROUP_TYPE,
        0x6893_0102,
        PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
    ];

    let summary = format_common_views_wlink_contract_summary(&columns, &snapshot);

    assert!(summary.contains("link_rows=11"));
    assert!(summary.contains("header_rows=6"));
    assert!(summary.contains("not_selected_required_link_columns="));
    assert!(summary.contains("expected_link_default_columns=0x68530003"));
    assert!(!summary.contains("0x68420102"));
    assert!(summary.contains("0x68530003"));
    assert!(!summary.contains("0x68910102"));
    assert!(summary.contains("0x68930102"));
}
