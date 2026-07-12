use super::super::*;
use super::*;

#[test]
fn inbox_folder_type_getprops_probe_loads_store_snapshot() {
    let session = test_mapi_session();
    let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
    probe.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(INBOX_FOLDER_ID).unwrap(),
    );
    probe.push(0);
    probe.extend_from_slice(&[0x07, 0x00, 0x01]);
    probe.extend_from_slice(&4096u16.to_le_bytes());
    probe.extend_from_slice(&1u16.to_le_bytes());
    probe.extend_from_slice(&PID_TAG_FOLDER_TYPE.to_le_bytes());
    let probe = rop_buffer_with_response(probe, &[u32::MAX]);

    assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
}

#[test]
fn logon_getprops_are_outlook_surface_diagnostics() {
    assert!(should_log_outlook_surface_getprops_info(Some(
        &MapiObject::Logon
    )));
    assert!(should_log_outlook_surface_getprops_info(Some(
        &MapiObject::PublicFolderLogon
    )));
}

#[test]
fn deferred_action_folder_getprops_are_outlook_surface_diagnostics() {
    assert!(should_log_outlook_surface_getprops_info(Some(
        &MapiObject::Folder {
            folder_id: DEFERRED_ACTION_FOLDER_ID,
            properties: HashMap::new(),
        }
    )));
}

#[test]
fn calendar_folder_getprops_trace_summarizes_response_contract() {
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: {
            let mut payload = Vec::new();
            payload.extend_from_slice(&4096u16.to_le_bytes());
            payload.extend_from_slice(&2u16.to_le_bytes());
            payload.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
            payload.extend_from_slice(&PID_TAG_CONTAINER_CLASS_W.to_le_bytes());
            payload
        },
    };
    let object = MapiObject::Folder {
        folder_id: CALENDAR_FOLDER_ID,
        properties: HashMap::from([
            (
                PID_TAG_DISPLAY_NAME_W,
                MapiValue::String("Calendar".to_string()),
            ),
            (
                PID_TAG_CONTAINER_CLASS_W,
                MapiValue::String("IPF.Appointment".to_string()),
            ),
        ]),
    };
    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &test_principal(),
        &[],
        &[],
        &empty_snapshot(),
    );

    let trace = format_outlook_surface_folder_getprops_trace(
        "{REQ}:225",
        &request,
        Some(&object),
        &response,
    )
    .expect("calendar folder trace");

    assert!(trace.contains("getprops_folder:request_id={REQ}:225"));
    assert!(trace.contains("folder=0x0000000000100001"));
    assert!(trace.contains("role=calendar"));
    assert!(trace.contains("tags=0x3001001f,0x3613001f"));
    assert!(trace.contains("returned=0x3001001f,0x3613001f"));
    assert!(trace.contains("values=0x3001001f:string:chars=8"));
    assert!(trace.contains("response=0x00000000"));
}

#[test]
fn inbox_display_name_getprops_probe_loads_store_snapshot() {
    let session = test_mapi_session();
    let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
    probe.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(INBOX_FOLDER_ID).unwrap(),
    );
    probe.push(0);
    probe.extend_from_slice(&[0x07, 0x00, 0x01]);
    probe.extend_from_slice(&4096u16.to_le_bytes());
    probe.extend_from_slice(&1u16.to_le_bytes());
    probe.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
    let probe = rop_buffer_with_response(probe, &[u32::MAX]);

    assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
}

#[test]
fn root_folder_type_getprops_probe_stays_store_independent() {
    let session = test_mapi_session();
    let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
    probe.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
    );
    probe.push(0);
    probe.extend_from_slice(&[0x07, 0x00, 0x01]);
    probe.extend_from_slice(&4096u16.to_le_bytes());
    probe.extend_from_slice(&1u16.to_le_bytes());
    probe.extend_from_slice(&PID_TAG_FOLDER_TYPE.to_le_bytes());
    let probe = rop_buffer_with_response(probe, &[u32::MAX]);

    assert!(rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
}

#[test]
fn root_default_folder_entry_id_getprops_probe_loads_store_snapshot() {
    let session = test_mapi_session();
    let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
    probe.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
    );
    probe.push(0);
    probe.extend_from_slice(&[0x07, 0x00, 0x01]);
    probe.extend_from_slice(&4096u16.to_le_bytes());
    probe.extend_from_slice(&1u16.to_le_bytes());
    probe.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
    let probe = rop_buffer_with_response(probe, &[u32::MAX]);

    assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
}

#[test]
fn role_backed_special_folder_getprops_probes_load_store_snapshot() {
    for folder_id in [
        INBOX_FOLDER_ID,
        DRAFTS_FOLDER_ID,
        SENT_FOLDER_ID,
        TRASH_FOLDER_ID,
        OUTBOX_FOLDER_ID,
        CONTACTS_FOLDER_ID,
        SUGGESTED_CONTACTS_FOLDER_ID,
        QUICK_CONTACTS_FOLDER_ID,
        IM_CONTACT_LIST_FOLDER_ID,
        CONTACTS_SEARCH_FOLDER_ID,
        CALENDAR_FOLDER_ID,
        JOURNAL_FOLDER_ID,
        NOTES_FOLDER_ID,
        TASKS_FOLDER_ID,
        REMINDERS_FOLDER_ID,
        DOCUMENT_LIBRARIES_FOLDER_ID,
        SYNC_ISSUES_FOLDER_ID,
        CONFLICTS_FOLDER_ID,
        LOCAL_FAILURES_FOLDER_ID,
        SERVER_FAILURES_FOLDER_ID,
        JUNK_FOLDER_ID,
        RSS_FEEDS_FOLDER_ID,
        TRACKED_MAIL_PROCESSING_FOLDER_ID,
        TODO_SEARCH_FOLDER_ID,
        QUICK_STEP_SETTINGS_FOLDER_ID,
        ARCHIVE_FOLDER_ID,
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        CONVERSATION_HISTORY_FOLDER_ID,
    ] {
        let session = test_mapi_session();
        let mut probe = vec![0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01];
        probe.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(folder_id).unwrap(),
        );
        probe.push(0);
        probe.extend_from_slice(&[0x07, 0x00, 0x01]);
        probe.extend_from_slice(&4096u16.to_le_bytes());
        probe.extend_from_slice(&1u16.to_le_bytes());
        probe.extend_from_slice(&PID_TAG_FOLDER_TYPE.to_le_bytes());
        let probe = rop_buffer_with_response(probe, &[u32::MAX]);

        assert!(
            !rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session),
            "folder {folder_id:#018x} must load store state"
        );
    }
}

#[test]
fn special_folder_getprops_probe_rejects_custom_properties() {
    let session = test_mapi_session();
    let mut probe = vec![0x02, 0x00, 0x00, 0x01];
    probe.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
    );
    probe.push(0);
    probe.extend_from_slice(&[0x07, 0x00, 0x01]);
    probe.extend_from_slice(&4096u16.to_le_bytes());
    probe.extend_from_slice(&1u16.to_le_bytes());
    probe.extend_from_slice(&0x9000_0003u32.to_le_bytes());
    let probe = rop_buffer_with_response(probe, &[u32::MAX]);

    assert!(!rop_buffer_is_store_independent_special_folder_getprops_probe(&probe, &session));
}

#[test]
fn folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name() {
    let principal = test_principal();
    let inbox_id = Uuid::from_u128(0x1111);
    crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
    let inbox = JmapMailbox {
        id: inbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "INBOX".to_string(),
        sort_order: 0,
        modseq: 42,
        total_emails: 221,
        unread_emails: 17,
        size_octets: 0,
        is_subscribed: true,
    };

    let properties = folder_properties_for_open_from_mailboxes(
        &principal,
        INBOX_FOLDER_ID,
        &[inbox],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(
        properties.get(&PID_TAG_DISPLAY_NAME_W),
        Some(&MapiValue::String("Inbox".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTENT_COUNT),
        Some(&MapiValue::U32(221))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTENT_UNREAD_COUNT),
        Some(&MapiValue::U32(17))
    );
    assert_eq!(
        properties.get(&PID_TAG_ASSOCIATED_CONTENT_COUNT),
        Some(&MapiValue::U32(associated_folder_message_count(
            INBOX_FOLDER_ID,
            &MapiMailStoreSnapshot::empty()
        )))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTAINER_CLASS_W),
        Some(&MapiValue::String("IPF.Note".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(&MapiValue::String("IPM.Note".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_RIGHTS),
        Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
    );
    assert_eq!(
        properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
        Some(&MapiValue::Binary(vec![0x01, 0x04, 0x00, 0x00, 0x10, 0x00]))
    );
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_VIEW_ENTRY_ID),
        crate::mapi::identity::message_entry_id_from_object_ids(
            principal.account_id,
            INBOX_FOLDER_ID,
            crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
        )
        .map(MapiValue::Binary)
        .as_ref()
    );
    assert_eq!(
        properties.get(&PID_TAG_FOLDER_FORM_FLAGS),
        Some(&MapiValue::U32(0))
    );
    assert!(!properties.contains_key(&PID_TAG_FOLDER_WEBVIEWINFO));
    assert!(!properties.contains_key(&PID_TAG_FOLDER_XVIEWINFO_E));
    assert_eq!(
        properties.get(&PID_TAG_FOLDER_VIEWS_ONLY),
        Some(&MapiValue::U32(0))
    );
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_FORM_NAME_W),
        Some(&MapiValue::String(String::new()))
    );
    assert_eq!(
        properties.get(&PID_TAG_FOLDER_FORM_STORAGE),
        Some(&MapiValue::Binary(Vec::new()))
    );
    assert!(!properties.contains_key(&PID_TAG_ACL_MEMBER_NAME_W));
    assert_eq!(
        properties.get(&PID_TAG_FOLDER_VIEWLIST_FLAGS),
        Some(&MapiValue::U32(0))
    );
    assert!(!properties.contains_key(&PID_TAG_ARCHIVE_TAG));
    assert!(!properties.contains_key(&PID_TAG_POLICY_TAG));
    assert_eq!(
        properties.get(&PID_TAG_RETENTION_PERIOD),
        Some(&MapiValue::U32(0))
    );
    assert_eq!(
        properties.get(&PID_TAG_RETENTION_FLAGS),
        Some(&MapiValue::U32(0))
    );
    assert_eq!(
        properties.get(&PID_TAG_ARCHIVE_PERIOD),
        Some(&MapiValue::U32(0))
    );
    let expected_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        principal.account_id,
        INBOX_FOLDER_ID,
    )
    .unwrap();
    assert_eq!(
        properties.get(&PID_TAG_ENTRY_ID),
        Some(&MapiValue::Binary(expected_entry_id))
    );
    assert_eq!(
        properties.get(&PID_TAG_RECORD_KEY),
        Some(&MapiValue::Binary(mapi_mailstore::source_key_for_store_id(
            INBOX_FOLDER_ID
        )))
    );
}

#[test]
fn folder_properties_for_open_projects_search_folder_mail_class() {
    let principal = test_principal();

    let properties = folder_properties_for_open_from_mailboxes(
        &principal,
        SEARCH_FOLDER_ID,
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(
        properties.get(&PID_TAG_FOLDER_TYPE),
        Some(&MapiValue::U32(FOLDER_SEARCH))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTAINER_CLASS_W),
        Some(&MapiValue::String("IPF.Note".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(&MapiValue::String("IPM.Note".to_string()))
    );
}

#[test]
fn folder_properties_for_open_projects_persisted_search_folder_contract() {
    let principal = test_principal();
    let definition_id = Uuid::parse_str("aaaaaaaa-3333-4333-8333-aaaaaaaaaaaa").unwrap();
    let folder_id = crate::mapi::identity::mapi_store_id(333);
    crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
        SearchFolderDefinition {
            id: definition_id,
            account_id: principal.account_id,
            role: "category_red".to_string(),
            display_name: "Categorized Mail".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: json!({"kind": "mapi_bounded"}),
            restriction_json: json!({"kind": "mapi_bounded"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        },
    ]);

    let properties =
        folder_properties_for_open_from_mailboxes(&principal, folder_id, &[], &snapshot);

    assert_eq!(
        properties.get(&PID_TAG_FOLDER_TYPE),
        Some(&MapiValue::U32(FOLDER_SEARCH))
    );
    assert_eq!(
        properties.get(&PID_TAG_DISPLAY_NAME_W),
        Some(&MapiValue::String("Categorized Mail".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_PARENT_FOLDER_ID),
        Some(&MapiValue::U64(SEARCH_FOLDER_ID))
    );
    assert_eq!(
        properties.get(&PID_TAG_RIGHTS),
        Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
    );
    let mut expected_extended_flags = extended_folder_flags();
    expected_extended_flags.extend_from_slice(&[0x03, 0x04]);
    expected_extended_flags.extend_from_slice(&0xAAAA_AAAAu32.to_le_bytes());
    expected_extended_flags.extend_from_slice(&[0x02, 0x10]);
    expected_extended_flags.extend_from_slice(definition_id.as_bytes());
    assert_eq!(
        properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
        Some(&MapiValue::Binary(expected_extended_flags))
    );
}

#[test]
fn folder_properties_for_open_projects_collaboration_folder_contract() {
    let principal = test_principal();
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![lpe_storage::CollaborationCollection {
            id: "default".to_string(),
            kind: "calendar".to_string(),
            owner_account_id: principal.account_id,
            owner_email: principal.email.clone(),
            owner_display_name: principal.display_name.clone(),
            display_name: "Calendar".to_string(),
            is_owned: true,
            rights: lpe_storage::CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let properties =
        folder_properties_for_open_from_mailboxes(&principal, CALENDAR_FOLDER_ID, &[], &snapshot);

    assert_eq!(
        properties.get(&PID_TAG_DISPLAY_NAME_W),
        Some(&MapiValue::String("Calendar".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTAINER_CLASS_W),
        Some(&MapiValue::String("IPF.Appointment".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(&MapiValue::String("IPM.Appointment".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTENT_COUNT),
        Some(&MapiValue::U32(0))
    );
    assert_eq!(
        properties.get(&PID_TAG_PARENT_FOLDER_ID),
        Some(&MapiValue::U64(IPM_SUBTREE_FOLDER_ID))
    );
    assert_eq!(
        properties.get(&PID_TAG_RIGHTS),
        Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
    );
    assert_eq!(
        properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
        Some(&MapiValue::Binary(extended_folder_flags()))
    );
    assert_eq!(
        properties.get(&PID_TAG_ASSOCIATED_CONTENT_COUNT),
        Some(&MapiValue::U32(associated_folder_message_count(
            CALENDAR_FOLDER_ID,
            &snapshot
        )))
    );
    assert_eq!(
        properties.get(&PID_TAG_FOLDER_FORM_FLAGS),
        Some(&MapiValue::U32(0))
    );
    assert!(!properties.contains_key(&PID_TAG_FOLDER_WEBVIEWINFO));
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_FORM_NAME_W),
        Some(&MapiValue::String(String::new()))
    );
    assert!(!properties.contains_key(&PID_TAG_ARCHIVE_TAG));
    assert_eq!(
        properties.get(&PID_TAG_RETENTION_PERIOD),
        Some(&MapiValue::U32(0))
    );
}

#[test]
fn journal_getprops_flags_absent_web_view_properties() {
    let principal = test_principal();
    let properties = folder_properties_for_open_from_mailboxes(
        &principal,
        JOURNAL_FOLDER_ID,
        &[],
        &MapiMailStoreSnapshot::empty(),
    );
    let object = MapiObject::Folder {
        folder_id: JOURNAL_FOLDER_ID,
        properties,
    };
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: {
            let mut payload = Vec::new();
            payload.extend_from_slice(&4096u16.to_le_bytes());
            payload.extend_from_slice(&2u16.to_le_bytes());
            payload.extend_from_slice(&PID_TAG_FOLDER_WEBVIEWINFO.to_le_bytes());
            payload.extend_from_slice(&PID_TAG_FOLDER_XVIEWINFO_E.to_le_bytes());
            payload
        },
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    // [MS-OXCDATA] sections 2.8.1.2 and 2.11.5: absent selected
    // properties use a FlaggedPropertyRow and ecNotFound (0x8004010F).
    assert_eq!(
        response,
        vec![
            0x07, 0x02, 0, 0, 0, 0, 0x01, 0x0A, 0x0F, 0x01, 0x04, 0x80, 0x0A, 0x0F, 0x01, 0x04,
            0x80,
        ]
    );
}

#[test]
fn inbox_getprops_flags_absent_retention_identity_properties() {
    let principal = test_principal();
    let properties = folder_properties_for_open_from_mailboxes(
        &principal,
        INBOX_FOLDER_ID,
        &[],
        &MapiMailStoreSnapshot::empty(),
    );
    let object = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties,
    };
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: {
            let mut payload = Vec::new();
            payload.extend_from_slice(&4096u16.to_le_bytes());
            payload.extend_from_slice(&2u16.to_le_bytes());
            payload.extend_from_slice(&PID_TAG_POLICY_TAG.to_le_bytes());
            payload.extend_from_slice(&PID_TAG_ARCHIVE_TAG.to_le_bytes());
            payload
        },
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    // [MS-OXCMSG] sections 2.2.1.60.1 and 2.2.1.60.2 define these
    // binary properties as GUID identifiers. Without configured tags they
    // are absent, using [MS-OXCDATA] sections 2.8.1.2 and 2.11.5.
    assert_eq!(
        response,
        vec![
            0x07, 0x02, 0, 0, 0, 0, 0x01, 0x0A, 0x0F, 0x01, 0x04, 0x80, 0x0A, 0x0F, 0x01, 0x04,
            0x80,
        ]
    );
}

#[test]
fn inbox_getprops_flags_binary_acl_member_name_as_absent() {
    let principal = test_principal();
    let properties = folder_properties_for_open_from_mailboxes(
        &principal,
        INBOX_FOLDER_ID,
        &[],
        &MapiMailStoreSnapshot::empty(),
    );
    let object = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties,
    };
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: {
            let mut payload = Vec::new();
            payload.extend_from_slice(&4096u16.to_le_bytes());
            payload.extend_from_slice(&1u16.to_le_bytes());
            payload.extend_from_slice(&0x6672_0102u32.to_le_bytes());
            payload
        },
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    // [MS-OXCPERM] section 2.2.6 defines PidTagMemberName
    // as an ACL-table string column, not a PT_BINARY folder property.
    assert_eq!(
        response,
        vec![0x07, 0x02, 0, 0, 0, 0, 0x01, 0x0A, 0x0F, 0x01, 0x04, 0x80]
    );
}

#[test]
fn folder_properties_for_open_projects_public_folder_contract() {
    let principal = test_principal();
    let folder_id = PUBLIC_FOLDERS_ROOT_FOLDER_ID + 0x10000;
    let canonical_id = Uuid::from_u128(0x77777777_7777_4777_8777_777777777777);
    crate::mapi::identity::remember_mapi_identity(canonical_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::empty().with_public_folders(
        vec![lpe_storage::PublicFolder {
            id: canonical_id,
            tree_id: Uuid::from_u128(0x88888888_8888_4888_8888_888888888888),
            parent_folder_id: None,
            canonical_id,
            display_name: "Public Contacts".to_string(),
            folder_class: "IPF.Contact".to_string(),
            path: "/Public Contacts".to_string(),
            sort_order: 0,
            lifecycle_state: "active".to_string(),
            change_counter: 1,
            rights: lpe_storage::PublicFolderRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }],
        Vec::new(),
        Vec::new(),
    );

    let properties =
        folder_properties_for_open_from_mailboxes(&principal, folder_id, &[], &snapshot);

    assert_eq!(
        properties.get(&PID_TAG_DISPLAY_NAME_W),
        Some(&MapiValue::String("Public Contacts".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_PARENT_FOLDER_ID),
        Some(&MapiValue::U64(PUBLIC_FOLDERS_ROOT_FOLDER_ID))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTAINER_CLASS_W),
        Some(&MapiValue::String("IPF.Contact".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(&MapiValue::String("IPM.Contact".to_string()))
    );
    assert_eq!(
        properties.get(&PID_TAG_RIGHTS),
        Some(&MapiValue::U32(MAPI_FOLDER_ACCESS))
    );
    assert_eq!(
        properties.get(&PID_TAG_EXTENDED_FOLDER_FLAGS),
        Some(&MapiValue::Binary(extended_folder_flags()))
    );
    assert!(!properties.contains_key(&PID_TAG_FOLDER_WEBVIEWINFO));
    assert!(!properties.contains_key(&PID_TAG_ARCHIVE_TAG));
    assert_eq!(
        properties.get(&PID_TAG_RETENTION_PERIOD),
        Some(&MapiValue::U32(0))
    );
}

#[test]
fn folder_properties_for_open_projects_im_contact_list_default_post_class() {
    let principal = test_principal();

    let properties = folder_properties_for_open_from_mailboxes(
        &principal,
        IM_CONTACT_LIST_FOLDER_ID,
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(
        properties.get(&PID_TAG_CONTAINER_CLASS_W),
        Some(&MapiValue::String(
            "IPF.Contact.MOC.ImContactList".to_string()
        ))
    );
    assert_eq!(
        properties.get(&PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        Some(&MapiValue::String("IPM.Contact".to_string()))
    );
}

#[test]
fn advertised_special_folder_counts_snapshot_messages_when_mailbox_not_loaded() {
    let principal = test_principal();
    let draft_id = Uuid::from_u128(0x3333);
    crate::mapi::identity::remember_mapi_identity(draft_id, 0x0000_0000_01a4_0001);
    let draft = JmapEmail {
        id: draft_id,
        thread_id: Uuid::from_u128(0x4444),
        mailbox_ids: Vec::new(),
        mailbox_states: vec![test_mailbox_state(Uuid::from_u128(0x5555), "drafts")],
        mailbox_id: Uuid::from_u128(0x5555),
        mailbox_role: "drafts".to_string(),
        mailbox_name: "Drafts".to_string(),
        modseq: 1,
        received_at: "2026-06-11T13:41:41Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: None,
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Test Draft".to_string(),
        preview: String::new(),
        body_text: "Draft".to_string(),
        body_html_sanitized: None,
        unread: true,
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
        size_octets: 5,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        vec![draft],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let properties =
        folder_properties_for_open_from_mailboxes(&principal, DRAFTS_FOLDER_ID, &[], &snapshot);

    assert_eq!(
        properties.get(&PID_TAG_CONTENT_COUNT),
        Some(&MapiValue::U32(1))
    );
    assert_eq!(
        properties.get(&PID_TAG_CONTENT_UNREAD_COUNT),
        Some(&MapiValue::U32(1))
    );
}

#[test]
fn folder_properties_for_open_reports_inbox_associated_content_count() {
    let principal = test_principal();
    let inbox_id = Uuid::from_u128(0x2222);
    let config_id = Uuid::from_u128(0x3333);
    crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
    crate::mapi::identity::remember_mapi_identity(config_id, 0x7fff_ffff_fffb_0001);
    let inbox = JmapMailbox {
        id: inbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "INBOX".to_string(),
        sort_order: 0,
        modseq: 42,
        total_emails: 18,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(vec![
        crate::store::MapiAssociatedConfigRecord {
            id: config_id,
            account_id: principal.account_id,
            folder_id: INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.EAS".to_string(),
            subject: "IPM.Configuration.EAS".to_string(),
            properties_json: serde_json::json!({}),
        },
    ]);

    let properties =
        folder_properties_for_open_from_mailboxes(&principal, INBOX_FOLDER_ID, &[inbox], &snapshot);

    assert_eq!(
        properties.get(&PID_TAG_ASSOCIATED_CONTENT_COUNT),
        Some(&MapiValue::U32(associated_folder_message_count(
            INBOX_FOLDER_ID,
            &snapshot
        )))
    );
}

#[test]
fn outlook_special_folder_debug_classifiers_cover_configuration_folders() {
    assert_eq!(
        post_hierarchy_probe_folder_name(QUICK_STEP_SETTINGS_FOLDER_ID),
        "quick_step_settings"
    );
    assert_eq!(
        debug_container_class_for_folder_id(QUICK_STEP_SETTINGS_FOLDER_ID),
        "IPF.Configuration"
    );
    assert_eq!(
        debug_container_class_for_folder_id(CONVERSATION_ACTION_SETTINGS_FOLDER_ID),
        "IPF.Configuration"
    );
    assert_eq!(
        debug_container_class_for_folder_id(RSS_FEEDS_FOLDER_ID),
        "IPF.Note.OutlookHomepage"
    );
    assert_eq!(
        debug_container_class_for_folder_id(SEARCH_FOLDER_ID),
        "IPF.Note"
    );
    assert_eq!(
        expected_special_folder_container_class(SEARCH_FOLDER_ID),
        "IPF.Note"
    );
    assert_eq!(
        expected_special_folder_item_message_class(SEARCH_FOLDER_ID),
        "IPM.Note"
    );
}

#[test]
fn open_folder_debug_metadata_uses_real_dynamic_mailbox_values() {
    let mailbox_id = Uuid::from_u128(0x195);
    let folder_id = crate::mapi::identity::mapi_store_id(0x195);
    crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "other".to_string(),
        name: "Categories Rename Search Folder".to_string(),
        sort_order: 0,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };

    let (name, role, container_class) = debug_open_folder_metadata(folder_id, &[mailbox]);

    assert_eq!(name, "Categories Rename Search Folder");
    assert_eq!(role, "other");
    assert_eq!(container_class, "IPF.Note");
}

#[test]
fn private_create_folder_response_marks_existing_folder_reuse() {
    assert!(private_create_folder_is_existing_response_flag());
}

#[test]
fn deleted_advertised_quick_step_create_can_reuse_existing_real_folder() {
    let mut session = test_mapi_session();

    assert!(
        !create_folder_existing_mailbox_satisfies_deleted_advertised_request(
            &session,
            IPM_SUBTREE_FOLDER_ID,
            "Quick Step Settings",
        )
    );

    session.record_deleted_advertised_special_folder(QUICK_STEP_SETTINGS_FOLDER_ID);

    assert!(
        create_folder_existing_mailbox_satisfies_deleted_advertised_request(
            &session,
            IPM_SUBTREE_FOLDER_ID,
            "Quick Step Settings",
        )
    );
    assert!(
        !create_folder_existing_mailbox_satisfies_deleted_advertised_request(
            &session,
            IPM_SUBTREE_FOLDER_ID,
            "Ordinary Folder",
        )
    );
}

#[test]
fn advertised_contact_folders_use_noop_delete_acknowledgement() {
    for folder_id in [
        CONTACTS_FOLDER_ID,
        SUGGESTED_CONTACTS_FOLDER_ID,
        QUICK_CONTACTS_FOLDER_ID,
        IM_CONTACT_LIST_FOLDER_ID,
    ] {
        assert!(
            is_advertised_special_folder(folder_id),
            "expected advertised special folder {folder_id:#018x}"
        );
        assert!(
            !advertised_special_folder_delete_uses_session_tombstone(folder_id),
            "contact folder delete must not hide the folder in session {folder_id:#018x}"
        );
        assert!(
                advertised_special_folder_delete_is_noop(folder_id),
                "contact folder delete should be acknowledged as non-destructive no-op {folder_id:#018x}"
            );
    }
    assert!(!advertised_special_folder_delete_is_noop(
        QUICK_STEP_SETTINGS_FOLDER_ID
    ));
    assert!(advertised_special_folder_delete_uses_session_tombstone(
        QUICK_STEP_SETTINGS_FOLDER_ID
    ));
}

#[test]
fn inbox_folder_type_getprops_response_context_includes_wire_preview() {
    let context = format_inbox_folder_type_getprops_response_context(&[
        0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    ]);

    assert!(context.contains("response_bytes=11"));
    assert!(context.contains("return_value=0x00000000"));
    assert!(context.contains("row_bytes=5"));
    assert!(context.contains("row_preview=0001000000"));
}

#[test]
fn getprops_contract_response_summary_includes_access_value() {
    let summary = getprops_contract_response_summary(
        &[PID_TAG_ACCESS],
        &[
            0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0x00, 0x00, 0x00,
        ],
    );

    assert_eq!(summary.result, "0x00000000");
    assert_eq!(summary.returned_tags, "0x0ff40003");
    assert_eq!(summary.value_shapes, "0x0ff40003:0x0000003f");
}

#[test]
fn folder_set_property_problems_accepts_ipm_subtree_ostid_write() {
    let ipm_subtree = MapiObject::Folder {
        folder_id: IPM_SUBTREE_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let inbox = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };

    assert!(folder_set_property_problems(
        Some(&ipm_subtree),
        &[],
        &[(PID_TAG_OST_OSTID, MapiValue::Binary(vec![1; 40]))],
    )
    .is_empty());
    assert_eq!(
        folder_set_property_problems(
            Some(&ipm_subtree),
            &[],
            &[(PID_TAG_OST_OSTID, MapiValue::Binary(Vec::new()))],
        ),
        vec![(0, PID_TAG_OST_OSTID, 0x8004_0102)]
    );
    assert_eq!(
        folder_set_property_problems(
            Some(&ipm_subtree),
            &[],
            &[(PID_TAG_DISPLAY_NAME_W, MapiValue::String("IPM".to_string()))],
        ),
        vec![(0, PID_TAG_DISPLAY_NAME_W, 0x8004_0102)]
    );
    assert_eq!(
        folder_set_property_problems(
            Some(&inbox),
            &[],
            &[(PID_TAG_OST_OSTID, MapiValue::Binary(vec![1; 40]))],
        ),
        vec![(0, PID_TAG_OST_OSTID, 0x8004_0102)]
    );
}

#[test]
fn folder_set_property_problems_accepts_additional_ren_entry_ids_ex_on_root_and_inbox() {
    let root = MapiObject::Folder {
        folder_id: ROOT_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let inbox = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let ipm_subtree = MapiObject::Folder {
        folder_id: IPM_SUBTREE_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let value = MapiValue::Binary(vec![1; 490]);

    assert!(folder_set_property_problems(
        Some(&root),
        &[],
        &[(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, value.clone())],
    )
    .is_empty());
    assert!(folder_set_property_problems(
        Some(&inbox),
        &[],
        &[(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, value.clone())],
    )
    .is_empty());
    assert_eq!(
        folder_set_property_problems(
            Some(&ipm_subtree),
            &[],
            &[(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, value.clone())],
        ),
        vec![(0, PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, 0x8004_0102)]
    );
    assert_eq!(
        folder_set_property_problems(
            Some(&root),
            &[],
            &[(
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
                MapiValue::Binary(Vec::new())
            )],
        ),
        vec![(0, PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX, 0x8004_0102)]
    );
}

#[test]
fn folder_set_property_problems_accepts_hidden_write_on_quick_step_folder() {
    let quick_step = JmapMailbox {
        id: Uuid::parse_str("f54d192a-3149-4ff1-bde7-a8dac219c73b").unwrap(),
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
    let quick_step_folder = MapiObject::Folder {
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let regular_folder = MapiObject::Folder {
        folder_id: 0x0000_0000_1234_0001,
        properties: std::collections::HashMap::new(),
    };

    assert!(folder_set_property_problems(
        Some(&quick_step_folder),
        std::slice::from_ref(&quick_step),
        &[(PID_TAG_ATTRIBUTE_HIDDEN, MapiValue::Bool(true))],
    )
    .is_empty());
    assert!(folder_set_property_problems(
        Some(&quick_step_folder),
        std::slice::from_ref(&quick_step),
        &[(
            PID_TAG_CONTAINER_CLASS_W,
            MapiValue::String("IPF.Configuration".to_string())
        )],
    )
    .is_empty());
    assert_eq!(
        folder_set_property_problems(
            Some(&quick_step_folder),
            std::slice::from_ref(&quick_step),
            &[(
                PID_TAG_ATTRIBUTE_HIDDEN,
                MapiValue::String("true".to_string())
            )],
        ),
        vec![(0, PID_TAG_ATTRIBUTE_HIDDEN, 0x8004_0102)]
    );
    assert_eq!(
        folder_set_property_problems(
            Some(&quick_step_folder),
            std::slice::from_ref(&quick_step),
            &[(
                PID_TAG_CONTAINER_CLASS_W,
                MapiValue::String("IPF.Note".to_string())
            )],
        ),
        vec![(0, PID_TAG_CONTAINER_CLASS_W, 0x8004_0102)]
    );
    assert_eq!(
        folder_set_property_problems(
            Some(&regular_folder),
            std::slice::from_ref(&quick_step),
            &[(PID_TAG_ATTRIBUTE_HIDDEN, MapiValue::Bool(true))],
        ),
        vec![(0, PID_TAG_ATTRIBUTE_HIDDEN, 0x8004_0102)]
    );
    assert_eq!(
        folder_set_property_problems(
            Some(&regular_folder),
            std::slice::from_ref(&quick_step),
            &[(
                PID_TAG_CONTAINER_CLASS_W,
                MapiValue::String("IPF.Configuration".to_string())
            )],
        ),
        vec![(0, PID_TAG_CONTAINER_CLASS_W, 0x8004_0102)]
    );
}

#[test]
fn default_folder_entry_id_values_debug_decodes_additional_ren_entry_ids_ex() {
    let Some(MapiValue::Binary(value)) = special_folder_identification_property_value(
        test_principal().account_id,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
    ) else {
        panic!("expected AdditionalRenEntryIdsEx");
    };

    let debug = default_folder_entry_id_values_for_debug(&[(
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
        MapiValue::Binary(value),
    )]);

    assert!(debug.contains("PidTagAdditionalRenEntryIdsEx:bytes="));
    assert!(debug.contains("bytes=544"));
    assert!(debug.contains("entry_count=10"));
    assert!(debug.contains("persist_id=0x8006"));
    assert!(debug.contains("persist_name=conversation_actions"));
    assert!(debug.contains("decoded_name=conversation_action_settings"));
    assert!(debug.contains("persist_id=0x8007"));
    assert!(debug.contains("persist_name=quick_step_settings"));
    assert!(debug.contains("decoded_name=quick_step_settings"));
    assert!(debug.contains("persist_id=0x800f"));
    assert!(debug.contains("persist_name=archive"));
    assert!(debug.contains("decoded_name=archive"));
    assert!(debug.contains("matches_expected=true"));
}

#[test]
fn default_folder_entry_id_values_debug_decodes_default_view_entry_id() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let entry_id = default_folder_view_entry_id(mailbox_guid, INBOX_FOLDER_ID, "IPF.Note").unwrap();

    let debug =
        default_folder_entry_id_values_for_debug(&[(PID_TAG_DEFAULT_VIEW_ENTRY_ID, entry_id)]);

    assert!(debug.contains("PidTagDefaultViewEntryId:bytes=70"));
    assert!(debug.contains(&format!("decoded_folder_id=0x{INBOX_FOLDER_ID:016x}")));
    assert!(debug.contains("decoded_folder_name=inbox"));
    assert!(debug.contains(&format!(
        "decoded_message_id=0x{:016x}",
        crate::mapi_store::outlook_default_folder_named_view_id(INBOX_FOLDER_ID)
    )));
}

#[test]
fn getprops_view_response_values_debug_decodes_default_view_entry_id() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let MapiValue::Binary(entry_id) =
        default_folder_view_entry_id(mailbox_guid, SENT_FOLDER_ID, "IPF.Note").unwrap()
    else {
        panic!("default folder view entry id must be binary");
    };
    let mut response = vec![0x07, 0x01, 0, 0, 0, 0, 0];
    response.extend_from_slice(&(entry_id.len() as u16).to_le_bytes());
    response.extend_from_slice(&entry_id);

    let debug =
        get_properties_view_response_values_for_debug(&[PID_TAG_DEFAULT_VIEW_ENTRY_ID], &response);

    assert!(debug.contains("PidTagDefaultViewEntryId:bytes=70"));
    assert!(debug.contains(&format!(
        "decoded_folder_id=0x{COMMON_VIEWS_FOLDER_ID:016x}"
    )));
    assert!(debug.contains("decoded_folder_name=common_views"));
    assert!(debug.contains(&format!(
        "decoded_message_id=0x{:016x}",
        crate::mapi_store::OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID
    )));
}

#[test]
fn set_property_debug_names_cover_folder_special_properties() {
    assert_eq!(
        set_property_debug_name(PID_TAG_DISPLAY_NAME_W),
        "PidTagDisplayName"
    );
    assert_eq!(set_property_debug_name(PID_TAG_FOLDER_ID), "PidTagFolderId");
    assert_eq!(
        set_property_debug_name(PID_TAG_PARENT_FOLDER_ID),
        "PidTagParentFolderId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLDER_TYPE),
        "PidTagFolderType"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_CONTENT_COUNT),
        "PidTagContentCount"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_CONTENT_UNREAD_COUNT),
        "PidTagContentUnreadCount"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SUBFOLDERS),
        "PidTagSubfolders"
    );
    assert_eq!(set_property_debug_name(PID_TAG_PRIVATE), "PidTagPrivate");
    assert_eq!(
        set_property_debug_name(PID_TAG_RESOURCE_FLAGS),
        "PidTagResourceFlags"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SERIALIZED_REPLID_GUID_MAP),
        "PidTagSerializedReplidGuidMap"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_USER_ENTRY_ID),
        "PidTagUserEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_MAILBOX_OWNER_ENTRY_ID),
        "PidTagMailboxOwnerEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_MAILBOX_OWNER_NAME_W),
        "PidTagMailboxOwnerName"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_IPM_PUBLIC_FOLDERS_ENTRY_ID),
        "PidTagIpmPublicFoldersEntryId"
    );
    assert_eq!(set_property_debug_name(PID_TAG_USER_GUID), "PidTagUserGuid");
    assert_eq!(
        set_property_debug_name(PID_TAG_ASSOCIATED_SHARING_PROVIDER),
        "PidTagAssociatedSharingProvider"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SERVER_TYPE_DISPLAY_NAME_W),
        "PidTagServerTypeDisplayName"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_MESSAGE_SIZE_EXTENDED),
        "PidTagMessageSizeExtended"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_PROHIBIT_RECEIVE_QUOTA),
        "PidTagProhibitReceiveQuota"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_PROHIBIT_SEND_QUOTA),
        "PidTagProhibitSendQuota"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_STORAGE_QUOTA_LIMIT),
        "PidTagStorageQuotaLimit"
    );
    assert_eq!(set_property_debug_name(PID_TAG_PST_PATH_W), "PidTagPstPath");
    assert_eq!(
        set_property_debug_name(PID_TAG_CHANGE_KEY),
        "PidTagChangeKey"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_MESSAGE_FLAGS),
        "PidTagMessageFlags"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_MESSAGE_STATUS),
        "PidTagMessageStatus"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_MESSAGE_CLASS_W),
        "PidTagOriginalMessageClass"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ACCESS_LEVEL),
        "PidTagAccessLevel"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SEARCH_KEY),
        "PidTagSearchKey"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SENT_MAIL_SVR_EID),
        "PidTagSentMailSvrEID"
    );
    assert_eq!(
        set_property_debug_name(PID_NAME_CONTENT_CLASS_W_TAG),
        "PidNameContentClass"
    );
    assert_eq!(
        set_property_debug_name(PID_NAME_CONTENT_TYPE_W_TAG),
        "PidNameContentType"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_LAST_MODIFICATION_TIME),
        "PidTagLastModificationTime"
    );
    assert_eq!(
        set_property_debug_name(OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        "OutlookAssociatedConfigBinary0E0B"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_CLSID),
        "PidTagViewDescriptorCLSID"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_FLAGS),
        "PidTagViewDescriptorFlags"
    );
    assert_eq!(
        set_property_debug_name(OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835),
        "OutlookCommonViewDescriptorBinary6835"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_VERSION),
        "PidTagViewDescriptorVersion"
    );
    assert_eq!(
        set_property_debug_name(OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C),
        "OutlookCommonViewDescriptorStrings683C"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE),
        "PidTagViewDescriptorFolderType"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE),
        "PidTagViewDescriptorViewMode"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_BINARY),
        "PidTagViewDescriptorBinary"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_STRINGS_W),
        "PidTagViewDescriptorStrings"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_NAME_W),
        "PidTagViewDescriptorName"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL),
        "PidTagViewDescriptorVersionCanonical"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_CONTAINER_CLASS_W),
        "PidTagContainerClass"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_DEFAULT_VIEW_ENTRY_ID),
        "PidTagDefaultViewEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        "PidTagDefaultPostMessageClass"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_DEFAULT_FORM_NAME_W),
        "PidTagDefaultFormName"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX),
        "PidTagAdditionalRenEntryIdsEx"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLDER_FORM_FLAGS),
        "PidTagFolderFormFlags"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLDER_WEBVIEWINFO),
        "PidTagFolderWebViewInfo"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLDER_XVIEWINFO_E),
        "PidTagFolderXViewInfoE"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLDER_VIEWS_ONLY),
        "PidTagFolderViewsOnly"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLDER_FORM_STORAGE),
        "PidTagFolderFormStorage"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_EXTENDED_FOLDER_FLAGS),
        "PidTagExtendedFolderFlags"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FREE_BUSY_ENTRY_IDS),
        "PidTagFreeBusyEntryIds"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SUBJECT_PREFIX_W),
        "PidTagSubjectPrefix"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS),
        "PidTagExtendedRuleMessageActions"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SEARCH_FOLDER_ID),
        "PidTagSearchFolderId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SEARCH_FOLDER_STORAGE_TYPE),
        "PidTagSearchFolderStorageType"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SEARCH_FOLDER_EFP_FLAGS),
        "PidTagSearchFolderEfpFlags"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SEARCH_FOLDER_DEFINITION),
        "PidTagSearchFolderDefinition"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ARCHIVE_TAG),
        "PidTagArchiveTag"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_POLICY_TAG),
        "PidTagPolicyTag"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RETENTION_PERIOD),
        "PidTagRetentionPeriod"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RETENTION_FLAGS),
        "PidTagRetentionFlags"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ARCHIVE_PERIOD),
        "PidTagArchivePeriod"
    );
    assert_eq!(set_property_debug_name(PID_TAG_RIGHTS), "PidTagRights");
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLDER_VIEWLIST_FLAGS),
        "PidTagFolderViewListFlags"
    );
    assert_eq!(set_property_debug_name(PID_TAG_ACCESS), "PidTagAccess");
    assert_eq!(set_property_debug_name(0x6672_0102), "unknown");
    assert_eq!(
        set_property_debug_name(OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C),
        "OutlookUndocumentedFolderBinary120C"
    );
    assert_eq!(
        set_property_debug_name(PID_LID_PERCENT_COMPLETE_TAG),
        "PidLidPercentComplete"
    );
    assert_eq!(
        set_property_debug_name(PID_LID_TASK_START_DATE_TAG),
        "PidLidTaskStartDate"
    );
    assert_eq!(
        set_property_debug_name(PID_LID_TASK_DUE_DATE_TAG),
        "PidLidTaskDueDate"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RTF_IN_SYNC),
        "PidTagRtfInSync"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RTF_COMPRESSED),
        "PidTagRtfCompressed"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ATTACH_NUM),
        "PidTagAttachNumber"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_CONVERSATION_INDEX),
        "PidTagConversationIndex"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_CONVERSATION_INDEX_TRACKING),
        "PidTagConversationIndexTracking"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_SENSITIVITY),
        "PidTagOriginalSensitivity"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ALTERNATE_RECIPIENT_ALLOWED),
        "PidTagAlternateRecipientAllowed"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_AUTO_FORWARDED),
        "PidTagAutoForwarded"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_DEFERRED_DELIVERY_TIME),
        "PidTagDeferredDeliveryTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_DELETE_AFTER_SUBMIT),
        "PidTagDeleteAfterSubmit"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_EXPIRY_TIME),
        "PidTagExpiryTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_MESSAGE_SIZE),
        "PidTagMessageSize"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ICON_INDEX),
        "PidTagIconIndex"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_BLOCK_STATUS),
        "PidTagBlockStatus"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_INTERNET_MAIL_OVERRIDE_FORMAT),
        "PidTagInternetMailOverrideFormat"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_INTERNET_MESSAGE_ID_W),
        "PidTagInternetMessageId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_IN_REPLY_TO_ID_W),
        "PidTagInReplyToId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_LAST_MODIFIER_NAME_W),
        "PidTagLastModifierName"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_LAST_VERB_EXECUTED),
        "PidTagLastVerbExecuted"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_LAST_VERB_EXECUTION_TIME),
        "PidTagLastVerbExecutionTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_AUTHOR_ENTRY_ID),
        "PidTagOriginalAuthorEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_AUTHOR_NAME_W),
        "PidTagOriginalAuthorName"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_DISPLAY_BCC_W),
        "PidTagOriginalDisplayBcc"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_DISPLAY_CC_W),
        "PidTagOriginalDisplayCc"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_DISPLAY_TO_W),
        "PidTagOriginalDisplayTo"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_SUBJECT_W),
        "PidTagOriginalSubject"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINAL_SUBMIT_TIME),
        "PidTagOriginalSubmitTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_ORIGINATOR_DELIVERY_REPORT_REQUESTED),
        "PidTagOriginatorDeliveryReportRequested"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_PARENT_KEY),
        "PidTagParentKey"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_READ_RECEIPT_REQUESTED),
        "PidTagReadReceiptRequested"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RECIPIENT_REASSIGNMENT_PROHIBITED),
        "PidTagRecipientReassignmentProhibited"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_REPLY_REQUESTED),
        "PidTagReplyRequested"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_REPLY_RECIPIENT_ENTRIES),
        "PidTagReplyRecipientEntries"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_REPLY_RECIPIENT_NAMES_W),
        "PidTagReplyRecipientNames"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_REPLY_TIME),
        "PidTagReplyTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_REPORT_TAG),
        "PidTagReportTag"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_REPORT_TIME),
        "PidTagReportTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RESPONSE_REQUESTED),
        "PidTagResponseRequested"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_START_DATE),
        "PidTagStartDate"
    );
    assert_eq!(set_property_debug_name(PID_TAG_END_DATE), "PidTagEndDate");
    assert_eq!(
        set_property_debug_name(PID_TAG_OWNER_APPOINTMENT_ID),
        "PidTagOwnerAppointmentId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_MESSAGE_EDITOR_FORMAT),
        "PidTagMessageEditorFormat"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_PROCESSED),
        "PidTagProcessed"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_PRIMARY_SEND_ACCOUNT_W),
        "PidTagPrimarySendAccount"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_NEXT_SEND_ACCOUNT_W),
        "PidTagNextSendAcct"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_TRANSPORT_MESSAGE_HEADERS_W),
        "PidTagTransportMessageHeaders"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FLAG_STATUS),
        "PidTagFlagStatus"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FLAG_COMPLETE_TIME),
        "PidTagFlagCompleteTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_HAS_ATTACHMENTS),
        "PidTagHasAttachments"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_DEFERRED_SEND_TIME),
        "PidTagDeferredSendTime"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_FOLLOWUP_ICON),
        "PidTagFollowupIcon"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SENDER_ENTRY_ID),
        "PidTagSenderEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SENDER_SEARCH_KEY),
        "PidTagSenderSearchKey"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SENT_REPRESENTING_SEARCH_KEY),
        "PidTagSentRepresentingSearchKey"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W),
        "PidTagSentRepresentingSmtpAddress"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RECEIVED_BY_ENTRY_ID_ALT),
        "PidTagReceivedByEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_RECEIVED_REPRESENTING_ENTRY_ID),
        "PidTagReceivedRepresentingEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_WLINK_ENTRY_ID),
        "PidTagWlinkEntryId"
    );
    assert_eq!(
        set_property_debug_name(0x684F_0102),
        "PidTagWlinkFolderType"
    );
    assert_eq!(
        set_property_debug_name(0x6850_0102),
        "PidTagWlinkGroupClsid"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID),
        "PidTagWlinkAddressBookStoreEid"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_WLINK_CALENDAR_COLOR),
        "PidTagWlinkCalendarColor"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_WLINK_ADDRESS_BOOK_EID),
        "PidTagWlinkAddressBookEid"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_WLINK_RO_GROUP_TYPE),
        "PidTagWlinkRoGroupType"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_IPM_APPOINTMENT_ENTRY_ID),
        "PidTagIpmAppointmentEntryId"
    );
    assert_eq!(
        set_property_debug_name(PID_TAG_REM_ONLINE_ENTRY_ID),
        "PidTagRemOnlineEntryId"
    );
}

#[test]
fn default_folder_entry_id_values_debug_decodes_indexed_special_folder_ids() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let values = vec![
        crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, CONFLICTS_FOLDER_ID)
            .unwrap(),
        crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, SYNC_ISSUES_FOLDER_ID)
            .unwrap(),
        crate::mapi::identity::folder_entry_id_from_object_id(
            mailbox_guid,
            LOCAL_FAILURES_FOLDER_ID,
        )
        .unwrap(),
        crate::mapi::identity::folder_entry_id_from_object_id(
            mailbox_guid,
            SERVER_FAILURES_FOLDER_ID,
        )
        .unwrap(),
    ];

    let debug = default_folder_entry_id_values_for_debug(&[(
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
        MapiValue::MultiBinary(values),
    )]);

    assert!(debug.contains("PidTagAdditionalRenEntryIds:count=4"));
    assert!(debug.contains("index=0"));
    assert!(debug.contains("decoded_name=conflicts"));
    assert!(debug.contains("omitted_preserved_indexes=4"));
}

#[test]
fn bootstrap_query_rows_total_count_keeps_sync_issues_leaf_until_backed() {
    let object = MapiObject::HierarchyTable {
        folder_id: SYNC_ISSUES_FOLDER_ID,
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
    };

    assert_eq!(
        outlook_bootstrap_query_rows_total_count(
            Some(&object),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        ),
        Some(0)
    );
}

#[test]
fn bootstrap_query_rows_total_count_projects_common_views_navigation_shortcuts() {
    let object = MapiObject::ContentsTable {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        associated: true,
        columns: default_navigation_shortcut_property_tags(),
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
        outlook_bootstrap_query_rows_total_count(
            Some(&object),
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            Uuid::nil(),
        ),
        Some(6)
    );
}

#[test]
fn contents_table_open_row_count_projects_common_views_full_table() {
    let snapshot = MapiMailStoreSnapshot::empty();

    assert_eq!(
        associated_folder_message_count(COMMON_VIEWS_FOLDER_ID, &snapshot),
        6
    );
    assert_eq!(
        contents_table_open_row_count(COMMON_VIEWS_FOLDER_ID, true, &[], &[], &snapshot,),
        6
    );
}

#[test]
fn default_folder_entry_id_values_debug_decodes_freebusy_data_index() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let freebusy_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        mailbox_guid,
        FREEBUSY_DATA_FOLDER_ID,
    )
    .unwrap();

    let debug = default_folder_entry_id_values_for_debug(&[(
        PID_TAG_FREE_BUSY_ENTRY_IDS,
        MapiValue::MultiBinary(vec![Vec::new(), Vec::new(), Vec::new(), freebusy_entry_id]),
    )]);

    assert!(debug.contains("PidTagFreeBusyEntryIds:count=4"));
    assert!(debug.contains("index=3"));
    assert!(debug.contains("decoded_name=freebusy_data"));
    assert!(debug.contains("matches_expected=true"));
}

#[test]
fn default_folder_identification_values_do_not_shadow_canonical_inbox_projection() {
    let inbox = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let retained = default_folder_identification_safe_property_values(
        &test_principal(),
        Some(&inbox),
        vec![
            (
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                MapiValue::MultiBinary(vec![
                    vec![0xAA],
                    vec![0xBB],
                    vec![0xCC],
                    vec![0xDD],
                    vec![0xEE],
                    vec![0xFA, 0xCE],
                ]),
            ),
            (
                PID_TAG_DISPLAY_NAME_W,
                MapiValue::String("Inbox".to_string()),
            ),
        ],
    );

    assert_eq!(retained.len(), 2);
    let Some(MapiValue::MultiBinary(values)) = retained
        .iter()
        .find(|(tag, _)| *tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS)
        .map(|(_, value)| value)
    else {
        panic!("expected AdditionalRenEntryIds");
    };
    assert_eq!(values.len(), 6);
    assert_ne!(values[0], vec![0xAA]);
    assert_eq!(values[5], vec![0xFA, 0xCE]);
    assert_eq!(
        retained
            .iter()
            .find(|(tag, _)| *tag == PID_TAG_DISPLAY_NAME_W),
        Some(&(
            PID_TAG_DISPLAY_NAME_W,
            MapiValue::String("Inbox".to_string())
        ))
    );
}

#[test]
fn root_scalar_default_folder_entry_ids_do_not_shadow_canonical_projection() {
    let root = MapiObject::Folder {
        folder_id: ROOT_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        test_principal().account_id,
        CALENDAR_FOLDER_ID,
    )
    .unwrap();

    let retained = default_folder_identification_safe_property_values(
        &test_principal(),
        Some(&root),
        vec![
            (
                PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
                MapiValue::Binary(calendar_entry_id.clone()),
            ),
            (
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                MapiValue::MultiBinary(vec![Vec::new()]),
            ),
        ],
    );

    assert_eq!(
        retained,
        vec![(
            PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
            MapiValue::Binary(calendar_entry_id)
        )]
    );
}

#[test]
fn root_scalar_default_folder_entry_id_write_is_retained_as_canonical_session_state() {
    let mut root = MapiObject::Folder {
        folder_id: ROOT_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        test_principal().account_id,
        CALENDAR_FOLDER_ID,
    )
    .unwrap();

    let values = default_folder_identification_safe_property_values(
        &test_principal(),
        Some(&root),
        vec![(
            PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
            MapiValue::Binary(calendar_entry_id.clone()),
        )],
    );
    apply_mapi_property_values(Some(&mut root), values).unwrap();

    let MapiObject::Folder { properties, .. } = root else {
        panic!("expected folder object");
    };
    assert_eq!(
        properties.get(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID),
        Some(&MapiValue::Binary(calendar_entry_id))
    );
}

#[test]
fn ipm_subtree_ostid_write_is_retained_as_session_mutable_state() {
    let mut ipm_subtree = MapiObject::Folder {
        folder_id: IPM_SUBTREE_FOLDER_ID,
        properties: std::collections::HashMap::new(),
    };
    let client_ostid = vec![1; 40];

    apply_mapi_property_values(
        Some(&mut ipm_subtree),
        vec![(PID_TAG_OST_OSTID, MapiValue::Binary(client_ostid.clone()))],
    )
    .unwrap();

    let MapiObject::Folder { properties, .. } = ipm_subtree else {
        panic!("expected folder object");
    };
    assert_eq!(
        properties.get(&PID_TAG_OST_OSTID),
        Some(&MapiValue::Binary(client_ostid))
    );
}

#[test]
fn logon_special_folder_contract_reports_mismatched_inbox() {
    let mut folder_ids = PRIVATE_LOGON_SPECIAL_FOLDER_IDS.to_vec();
    folder_ids[4] = ROOT_FOLDER_ID;

    let issues = logon_special_folder_contract_issues(&folder_ids);

    assert!(issues.contains("4:inbox"));
    assert!(issues.contains(&format!("got=0x{ROOT_FOLDER_ID:016x}")));
    assert!(issues.contains(&format!("expected=0x{INBOX_FOLDER_ID:016x}")));
}

#[test]
fn default_folder_identification_contract_decodes_root_defaults() {
    let contract = default_folder_identification_contract_for_debug(&test_principal());

    assert!(contract.contains("PidTagValidFolderMask:0x000000ff"));
    assert!(contract.contains(&format!(
        "PidTagIpmSubtreeEntryId:bytes=46:decoded_folder_id=0x{IPM_SUBTREE_FOLDER_ID:016x}"
    )));
    assert!(contract.contains(&format!(
        "PidTagCommonViewsEntryId:bytes=46:decoded_folder_id=0x{COMMON_VIEWS_FOLDER_ID:016x}"
    )));
    assert!(contract.contains("PidTagAdditionalRenEntryIds:count=5"));
    assert!(contract.contains("PidTagFreeBusyEntryIds:count=4"));
}

#[test]
fn default_folder_hierarchy_projection_reports_calendar_and_contacts_identity() {
    let projection = default_folder_hierarchy_projection_for_debug(
        &test_principal(),
        &[],
        &[],
        &empty_snapshot(),
    );

    assert!(projection.contains(&format!(
        "calendar:tag=0x{PID_TAG_IPM_APPOINTMENT_ENTRY_ID:08x};folder=0x{CALENDAR_FOLDER_ID:016x}"
    )));
    assert!(projection.contains(&format!(
        "contacts:tag=0x{PID_TAG_IPM_CONTACT_ENTRY_ID:08x};folder=0x{CONTACTS_FOLDER_ID:016x}"
    )));
    assert!(projection.contains("entry_id_matches=true"));
    assert!(projection.contains("source_key_matches=true"));
}

#[test]
fn first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes() {
    let mut request_bytes = vec![0x02, 0x00, 0x00, 0x01];
    request_bytes.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(CALENDAR_FOLDER_ID).unwrap(),
    );
    request_bytes.push(0);
    request_bytes.extend_from_slice(&[0x07, 0x00, 0x01]);
    request_bytes.extend_from_slice(&4096u16.to_le_bytes());
    request_bytes.extend_from_slice(&2u16.to_le_bytes());
    request_bytes.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
    request_bytes.extend_from_slice(&PID_TAG_CONTENT_COUNT.to_le_bytes());
    let request_buffer = rop_buffer_with_response(request_bytes, &[1, u32::MAX]);

    let open_folder_request = RopRequest {
        rop_id: 0x02,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };
    let mut responses = rop_open_folder_response(&open_folder_request, false);
    responses.extend_from_slice(&[0x07, 0x01]);
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);
    responses.extend_from_slice(&super::utf16z_bytes("Calendar"));
    responses.extend_from_slice(&0u32.to_le_bytes());
    let response_buffer = rop_buffer_with_response(responses, &[1]);

    let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

    assert_eq!(summary.open_folder_request_count, 1);
    assert!(summary
        .open_folder_requests
        .contains(&format!("folder=0x{CALENDAR_FOLDER_ID:016x};name=calendar")));
    assert!(summary
        .open_folder_response_shapes
        .contains("result=0x00000000;has_rules=0;is_ghosted=0"));
    assert_eq!(summary.get_properties_specific_request_count, 1);
    assert!(summary
        .get_properties_specific_requests
        .contains("tags=0x3001001f,0x36020003"));
    assert!(summary
        .get_properties_specific_response_shapes
        .contains("result=0x00000000;row=standard"));
    assert!(summary.parse_error.is_empty());
}

#[test]
fn post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops() {
    let mut request_bytes = vec![0x07, 0x00, 0x01];
    request_bytes.extend_from_slice(&4096u16.to_le_bytes());
    request_bytes.extend_from_slice(&1u16.to_le_bytes());
    request_bytes.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
    let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

    let mut responses = vec![0x07, 0x01];
    responses.extend_from_slice(&0u32.to_le_bytes());
    responses.push(0);
    responses.extend_from_slice(&46u16.to_le_bytes());
    responses.extend_from_slice(&[0xAA; 46]);
    let response_buffer = rop_buffer_with_response(responses, &[1]);

    let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

    assert!(summary
        .get_properties_specific_response_shapes
        .contains("values=0x36d00102:binary:bytes=46"));
    assert!(summary.parse_error.is_empty());
}

#[test]
fn root_default_folder_getprops_uses_canonical_projection_not_setprops_state() {
    let reopened_root = MapiObject::Folder {
        folder_id: ROOT_FOLDER_ID,
        properties: HashMap::new(),
    };
    let request = get_properties_specific_request(&[PID_TAG_IPM_APPOINTMENT_ENTRY_ID]);
    let response = rop_get_properties_specific_response(
        &request,
        Some(&reopened_root),
        &test_principal(),
        &[],
        &[],
        &empty_snapshot(),
    );

    let mut cursor = Cursor::new(&response[7..]);
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_IPM_APPOINTMENT_ENTRY_ID).unwrap(),
        MapiValue::Binary(
            crate::mapi::identity::folder_entry_id_from_object_id(
                test_principal().account_id,
                CALENDAR_FOLDER_ID,
            )
            .unwrap()
        )
    );
    let MapiObject::Folder { properties, .. } = &reopened_root else {
        panic!("expected reopened root folder object");
    };
    assert!(properties.is_empty());
}

#[test]
fn first_post_hierarchy_probe_summary_identifies_set_properties_shapes() {
    let mut property_value = Vec::new();
    property_value.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
    property_value.extend_from_slice(&3u16.to_le_bytes());
    property_value.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
    let property_value_size = property_value.len() + 2;
    let mut request_bytes = vec![0x0A, 0x00, 0x01];
    request_bytes.extend_from_slice(&(property_value_size as u16).to_le_bytes());
    request_bytes.extend_from_slice(&1u16.to_le_bytes());
    request_bytes.extend_from_slice(&property_value);
    let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

    let request = RopRequest {
        rop_id: 0x0A,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let response_buffer = rop_buffer_with_response(rop_set_properties_response(&request), &[1]);

    let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

    assert_eq!(summary.set_properties_request_count, 1);
    assert!(summary
        .set_properties_requests
        .contains("tags=0x36d00102;values=0x36d00102:binary:bytes=3"));
    assert!(summary
        .set_properties_response_shapes
        .contains("result=0x00000000;property_problem_count=0"));
    assert!(summary.parse_error.is_empty());
}
