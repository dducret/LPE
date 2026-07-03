use super::*;

mod associated_config;

mod execute;
mod folders;

#[test]
fn debug_named_property_context_reports_session_and_unresolved_properties() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        0x801f,
        MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("custom field".to_string()),
        },
    );

    let context = format_debug_named_property_context(
        &session,
        &[0x801f_001f, PID_TAG_SUBJECT_W, 0x836b_001f],
    );

    assert!(context.contains("0x801f001f:id=0x801f:type=0x001f:source=session"));
    assert!(context.contains("name=custom field"));
    assert!(context.contains("0x836b001f:id=0x836b:type=0x001f:source=well_known"));
    assert!(context.contains("name=content-type"));
    assert!(!context.contains("0x0037001f"));
}

#[test]
fn contents_table_named_property_context_reports_selected_columns() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        0x801f,
        MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("view custom column".to_string()),
        },
    );
    let table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_SUBJECT_W, 0x801f_001f],
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

    let context = format_contents_table_named_property_context(&session, Some(&table));

    assert!(context.contains("0x801f001f:id=0x801f:type=0x001f:source=session"));
    assert!(context.contains("name=view custom column"));
    assert!(!context.contains("0x0037001f"));
}

#[test]
fn outlook_view_descriptor_probe_detection_tracks_collaboration_view_named_properties() {
    let properties = vec![
        MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_COMMON_START),
        },
        MapiNamedProperty {
            guid: PSETID_APPOINTMENT_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_BUSY_STATUS),
        },
        MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL1_EMAIL_ADDRESS),
        },
        MapiNamedProperty {
            guid: PSETID_TASK_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_TASK_DUE_DATE),
        },
        MapiNamedProperty {
            guid: PSETID_NOTE_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_NOTE_COLOR),
        },
        MapiNamedProperty {
            guid: PSETID_LOG_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_LOG_TYPE),
        },
    ];

    assert!(contains_outlook_view_descriptor_probe(&properties));
}

#[test]
fn outlook_view_descriptor_named_property_context_reports_calendar_lids() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        PID_LID_LOCATION as u16,
        MapiNamedProperty {
            guid: PSETID_APPOINTMENT_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_LOCATION),
        },
    );
    let context = format_debug_named_property_context(
        &session,
        &[
            PID_LID_COMMON_START_TAG,
            PID_LID_COMMON_END_TAG,
            PID_LID_LOCATION_W_TAG,
            PID_LID_BUSY_STATUS_TAG,
        ],
    );

    assert!(context.contains("0x85160040:id=0x8516:type=0x0040"));
    assert!(context.contains("lid=0x00008516"));
    assert!(context.contains("0x8208001f:id=0x8208:type=0x001f:source=session"));
    assert!(context.contains("0x82050003:id=0x8205:type=0x0003"));
}

#[test]
fn outlook_view_descriptor_named_property_context_reports_requested_folder_lids() {
    let session = test_mapi_session();
    let snapshot = MapiMailStoreSnapshot::empty();

    let contacts = format_outlook_view_descriptor_named_property_context(
        &session,
        CONTACTS_FOLDER_ID,
        &snapshot,
    );
    let tasks =
        format_outlook_view_descriptor_named_property_context(&session, TASKS_FOLDER_ID, &snapshot);
    let notes =
        format_outlook_view_descriptor_named_property_context(&session, NOTES_FOLDER_ID, &snapshot);
    let journal = format_outlook_view_descriptor_named_property_context(
        &session,
        JOURNAL_FOLDER_ID,
        &snapshot,
    );

    assert!(contacts.contains("0x8083001f"));
    assert!(tasks.contains("0x81050040"));
    assert!(tasks.contains("0x81040040"));
    assert!(tasks.contains("0x81020005"));
    assert!(notes.contains("0x8b000003"));
    assert!(journal.contains("0x87060040"));
    assert!(journal.contains("0x87070003"));
    assert!(journal.contains("0x8700001f"));
}

#[test]
fn smart_input_variant_resets_inbox_fai_cursor_before_query_rows() {
    let mut session = test_mapi_session();
    session.outlook_smart_input_variant = "fai_cursor_reset_before_query_rows".to_string();
    session.handles.insert(
        9,
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
            position: 3,
        },
    );
    let request = RopRequest {
        rop_id: RopId::QueryRows.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: vec![0x00, 0x01, 0x01, 0x00],
    };
    let handle_slots = vec![u32::MAX, u32::MAX, 9];

    let context = apply_outlook_smart_input_variant_before_query_rows(
        &mut session,
        &handle_slots,
        &request,
        "request:42",
        "QueryRows",
    )
    .expect("variant should apply");

    assert!(context.contains("previous_position=3"));
    assert!(session.outlook_smart_input_variant_applied);
    let Some(MapiObject::ContentsTable { position, .. }) = session.handles.get(&9) else {
        panic!("expected contents table");
    };
    assert_eq!(*position, 0);
}

#[test]
fn inbox_view_handoff_table_contract_reports_common_views_default_view() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let contract = format_outlook_view_handoff_table_contract(
        INBOX_FOLDER_ID,
        true,
        &default_associated_config_columns(),
        &snapshot,
    );

    assert!(contract.contains("folder_local_default_supported=true"));
    assert!(contract.contains("folder_local_default_visible_in_fai_table=false"));
    assert!(contract.contains(&format!(
        "advertised_default_view_folder_id=0x{COMMON_VIEWS_FOLDER_ID:016x}"
    )));
    assert!(contract.contains(&format!(
        "expected_view_message_id=0x{:016x}",
        crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID
    )));
}

#[test]
fn inbox_fai_handoff_visibility_context_separates_prefix_and_named_view_rows() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let prefix_restriction = MapiRestriction::Content {
        property_tag: PID_TAG_MESSAGE_CLASS_W,
        value: "IPM.Configuration.".to_string(),
        fuzzy_level_low: 0x0002,
        fuzzy_level_high: 0x0001,
    };

    let context = format_inbox_fai_handoff_visibility_context(
        &snapshot,
        Some(&prefix_restriction),
        Uuid::nil(),
    );

    assert!(context.contains(&format!(
        "advertised_default_view_folder_id=0x{COMMON_VIEWS_FOLDER_ID:016x}"
    )));
    assert!(context.contains(&format!(
        "default_view_id=0x{:016x}",
        crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID
    )));
    assert!(context.contains("current_count=1"));
    assert!(context.contains("exact_named_view_count=1"));
    assert!(context.contains("class=IPM.Microsoft.FolderDesign.NamedView"));
    assert!(context.contains("subject=Compact"));
}

#[test]
fn junk_view_handoff_table_contract_reports_folder_local_default_view() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let contract = format_outlook_view_handoff_table_contract(
        JUNK_FOLDER_ID,
        true,
        &default_associated_config_columns(),
        &snapshot,
    );

    assert!(contract.contains("folder_local_default_supported=true"));
    assert!(contract.contains("folder_local_default_visible_in_fai_table=true"));
    assert!(contract.contains(&format!(
        "advertised_default_view_folder_id=0x{JUNK_FOLDER_ID:016x}"
    )));
    assert!(contract.contains(&format!(
        "expected_view_message_id=0x{:016x}",
        crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID
    )));
}

#[test]
fn contacts_view_handoff_table_contract_reports_contact_default_view() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let contract = format_outlook_view_handoff_table_contract(
        CONTACTS_FOLDER_ID,
        true,
        &default_associated_config_columns(),
        &snapshot,
    );

    assert!(contract.contains("folder_local_default_supported=true"));
    assert!(contract.contains("folder_local_default_visible_in_fai_table=true"));
    assert!(contract.contains(
        "visible_column_tags=0x001a001f,0x3001001f,0x8083001f,0x3a1c001f,0x3a16001f,0x3a17001f"
    ));
    assert!(contract.contains("expected_view_message_id=0x7fffffffffe90001"));
}

#[test]
fn calendar_view_handoff_table_contract_reports_calendar_default_view() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let contract = format_outlook_view_handoff_table_contract(
        CALENDAR_FOLDER_ID,
        true,
        &default_associated_config_columns(),
        &snapshot,
    );

    assert!(contract.contains("folder_local_default_supported=true"));
    assert!(contract.contains("folder_local_default_visible_in_fai_table=true"));
    assert!(contract.contains(
        "visible_column_tags=0x001a001f,0x0037001f,0x85160040,0x85170040,0x8208001f,0x82050003"
    ));
    assert!(contract.contains("expected_view_message_id=0x7fffffffffe90001"));
}

#[test]
fn inbox_view_descriptor_set_columns_contract_reports_missing_descriptor_columns() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let contract = format_inbox_view_descriptor_set_columns_behavior_contract(
        INBOX_FOLDER_ID,
        false,
        &[PID_TAG_SUBJECT_W, PID_TAG_MESSAGE_DELIVERY_TIME],
        &snapshot,
    );

    assert!(contract.contains("phase=setcolumns"));
    assert!(contract.contains("default_view_id=0x7ffffffffff70001"));
    assert!(contract
        .contains("descriptor_columns=0x00170003,0x8514000b,0x001a001f,0x0e170003,0x0e1b000b"));
    assert!(!contract.contains("descriptor_columns=0x00040001"));
    assert!(contract.contains("selected_columns=0x0037001f,0x0e060040"));
    assert!(contract.ends_with("selected_missing_descriptor_columns="));
    assert!(!contract.contains("selected_missing_descriptor_columns=0x00040001"));
}

#[test]
fn inbox_compact_descriptor_matches_observed_visible_projection() {
    let snapshot = MapiMailStoreSnapshot::empty();
    let contract = format_inbox_view_descriptor_set_columns_behavior_contract(
        INBOX_FOLDER_ID,
        false,
        &[
            0x6748_0014,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_CREATION_TIME,
            PID_TAG_SUBJECT_W,
            PID_TAG_SENT_REPRESENTING_NAME_W,
            PID_TAG_MESSAGE_FLAGS,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_INTERNET_MESSAGE_ID_W,
            PID_TAG_IMPORTANCE,
            PID_TAG_HAS_ATTACHMENTS,
            PID_TAG_MESSAGE_STATUS,
            PID_LID_OUTLOOK_COMMON_8514_TAG,
            0x8017_000B,
            0x801F_001F,
            0x0041_0102,
            OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG,
            PID_TAG_MESSAGE_DELIVERY_TIME,
        ],
        &snapshot,
    );

    assert!(contract.contains("descriptor_columns=0x00170003"));
    assert!(contract.contains("0x0e1b000b"));
    assert!(contract.contains("0x0042001f"));
    assert!(
        contract.ends_with("selected_missing_descriptor_columns="),
        "{contract}"
    );
}

#[test]
fn calendar_content_sync_changed_ids_are_projected() {
    let changed_event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
    let changes = MapiSyncChangeSet {
        changed_calendar_event_ids: vec![changed_event_id],
        ..Default::default()
    };

    let changed_ids = changed_special_ids_for_folder(
        CALENDAR_FOLDER_ID,
        &MapiMailStoreSnapshot::empty(),
        &changes,
    );

    assert_eq!(changed_ids, vec![changed_event_id]);
}

#[test]
fn calendar_content_sync_changed_ids_include_associated_config() {
    let changed_event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
    let changed_config_id = Uuid::from_u128(0xc5a11c0ff1ce4c998b07111111111111);
    let changes = MapiSyncChangeSet {
        changed_calendar_event_ids: vec![changed_event_id],
        changed_associated_config_ids: vec![crate::store::MapiAssociatedConfigChange {
            folder_id: CALENDAR_FOLDER_ID,
            config_id: changed_config_id,
        }],
        ..Default::default()
    };

    let changed_ids = changed_special_ids_for_folder(
        CALENDAR_FOLDER_ID,
        &MapiMailStoreSnapshot::empty(),
        &changes,
    );

    assert_eq!(changed_ids, vec![changed_config_id, changed_event_id]);
}

#[test]
fn table_columns_normalize_stale_sharing_named_property_alias() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        0x8fff,
        MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name(
                "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
            ),
        },
    );

    let columns =
        normalize_table_property_tags_for_session(&session, vec![0x8fff_0102, PID_TAG_SUBJECT_W]);

    assert_eq!(
        columns,
        vec![
            PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
            PID_TAG_SUBJECT_W
        ]
    );
}

#[test]
fn table_columns_normalize_stale_sharing_alias_without_cached_mapping() {
    let session = test_mapi_session();

    let columns =
        normalize_table_property_tags_for_session(&session, vec![0x8fff_0102, PID_TAG_SUBJECT_W]);

    assert_eq!(
        columns,
        vec![
            PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
            PID_TAG_SUBJECT_W
        ]
    );
}

#[test]
fn table_columns_normalize_well_known_contact_email_named_property_alias() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        0x8022,
        MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_EMAIL1_EMAIL_ADDRESS),
        },
    );

    let columns =
        normalize_table_property_tags_for_session(&session, vec![0x8022_001f, PID_TAG_SUBJECT_W]);

    assert_eq!(
        columns,
        vec![PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG, PID_TAG_SUBJECT_W]
    );
}

#[test]
fn table_columns_normalize_outlook_contact_view_email_alias() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        0x8FFE,
        MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS),
        },
    );

    let columns =
        normalize_table_property_tags_for_session(&session, vec![0x8FFE_001f, PID_TAG_SUBJECT_W]);

    assert_eq!(
        columns,
        vec![
            PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS_W_TAG,
            PID_TAG_SUBJECT_W
        ]
    );
}

#[test]
fn table_columns_normalize_outlook_visible_inbox_appointment_alias() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        0x8017,
        MapiNamedProperty {
            guid: PSETID_APPOINTMENT_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_APPOINTMENT_8F07),
        },
    );

    let columns =
        normalize_table_property_tags_for_session(&session, vec![0x8017_000b, PID_TAG_SUBJECT_W]);

    assert_eq!(
        columns,
        vec![PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG, PID_TAG_SUBJECT_W]
    );
}

#[test]
fn table_columns_normalize_outlook_calendar_common_aliases() {
    let mut session = test_mapi_session();
    session.cache_named_property(
        0x8005,
        MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_SIDE_EFFECTS),
        },
    );
    session.cache_named_property(
        0x8013,
        MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_COMMON_8578),
        },
    );

    let columns = normalize_table_property_tags_for_session(
        &session,
        vec![0x8013_0003, 0x8005_0003, PID_TAG_SUBJECT_W],
    );

    assert_eq!(
        columns,
        vec![
            PID_LID_OUTLOOK_COMMON_8578_TAG,
            PID_LID_SIDE_EFFECTS_TAG,
            PID_TAG_SUBJECT_W
        ]
    );
}

#[test]
fn get_property_ids_from_names_returns_canonical_well_known_id_from_stale_mapping() {
    let mut session = test_mapi_session();
    let property = MapiNamedProperty {
        guid: PSETID_SHARING_GUID,
        kind: MapiNamedPropertyKind::Name(
            "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
        ),
    };

    let property_id =
        cache_named_property_mapping_and_return_property_id(&mut session, 0x8fff, property.clone());

    assert_eq!(property_id, 0x8010);
    assert_eq!(session.property_name_for_id(0x8fff), property);
    assert_eq!(session.property_id_for_name(property, false), Some(0x8010));
}

#[test]
fn get_property_ids_from_names_returns_canonical_contact_source_id_from_stale_mapping() {
    let mut session = test_mapi_session();
    let property = MapiNamedProperty {
        guid: PSETID_ADDRESS_GUID,
        kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_SOURCE_80E0),
    };

    let property_id =
        cache_named_property_mapping_and_return_property_id(&mut session, 0x80b8, property.clone());

    assert_eq!(property_id, PID_LID_OUTLOOK_CONTACT_SOURCE_80E0 as u16);
    assert_eq!(session.property_name_for_id(0x80b8), property);
    assert_eq!(
        session.property_id_for_name(property, false),
        Some(PID_LID_OUTLOOK_CONTACT_SOURCE_80E0 as u16)
    );
}

fn test_mailbox_state(mailbox_id: Uuid, role: &str) -> lpe_storage::JmapEmailMailboxState {
    lpe_storage::JmapEmailMailboxState {
        mailbox_id,
        role: role.to_string(),
        name: role.to_string(),
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
    }
}

#[test]
fn open_message_fallback_preserves_valid_requested_folder() {
    let inbox_id = Uuid::from_u128(0x1111);
    let sent_id = Uuid::from_u128(0x2222);
    crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
    crate::mapi::identity::remember_mapi_identity(sent_id, SENT_FOLDER_ID);

    let mailboxes = vec![
        JmapMailbox {
            id: inbox_id,
            parent_id: None,
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
        JmapMailbox {
            id: sent_id,
            parent_id: None,
            role: "sent".to_string(),
            name: "Sent".to_string(),
            sort_order: 1,
            modseq: 1,
            total_emails: 1,
            unread_emails: 0,
            size_octets: 0,
            is_subscribed: true,
        },
    ];
    let email = JmapEmail {
        id: Uuid::from_u128(0x3333),
        thread_id: Uuid::from_u128(0x4444),
        mailbox_ids: vec![sent_id, inbox_id],
        mailbox_states: vec![
            test_mailbox_state(sent_id, "sent"),
            test_mailbox_state(inbox_id, "inbox"),
        ],
        mailbox_id: sent_id,
        mailbox_role: "sent".to_string(),
        mailbox_name: "Sent".to_string(),
        modseq: 1,
        received_at: "2026-06-07T19:00:00Z".to_string(),
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
        subject: "Test".to_string(),
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
        size_octets: 0,
        internet_message_id: None,
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };

    assert_eq!(
        fallback_open_message_folder_id(INBOX_FOLDER_ID, &email, &mailboxes),
        INBOX_FOLDER_ID
    );
    assert_eq!(
        fallback_open_message_folder_id(TRASH_FOLDER_ID, &email, &mailboxes),
        SENT_FOLDER_ID
    );
}

#[test]
fn normal_inbox_query_row_summary_reports_message_shapes() {
    let inbox_id = Uuid::from_u128(0x5555);
    crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
    let mailbox = JmapMailbox {
        id: inbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 0,
        modseq: 1,
        total_emails: 1,
        unread_emails: 1,
        size_octets: 0,
        is_subscribed: true,
    };
    let email = JmapEmail {
        id: Uuid::from_u128(0x6666),
        thread_id: Uuid::from_u128(0x7777),
        mailbox_ids: vec![inbox_id],
        mailbox_states: vec![test_mailbox_state(inbox_id, "inbox")],
        mailbox_id: inbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 1,
        received_at: "2026-06-07T19:00:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "self".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Preview target".to_string(),
        preview: "Body text".to_string(),
        body_text: "Body text".to_string(),
        body_html_sanitized: Some("<p>Body text</p>".to_string()),
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
        size_octets: 128,
        internet_message_id: Some("<message@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "stored".to_string(),
    };
    crate::mapi::identity::remember_mapi_identity(
        email.id,
        crate::mapi::identity::mapi_store_id(0x6666),
    );

    let summary = format_normal_message_query_row_summary(
        INBOX_FOLDER_ID,
        false,
        0,
        true,
        50,
        &[],
        None,
        &[
            PID_TAG_MID,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_MESSAGE_FLAGS,
            PID_TAG_BODY_W,
            PID_TAG_RTF_COMPRESSED,
            PID_TAG_HTML_BINARY,
            PID_TAG_NATIVE_BODY,
            PID_TAG_INTERNET_MESSAGE_ID_W,
        ],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&email),
        &empty_snapshot(),
    );

    assert!(summary.contains("total=1"));
    assert!(summary.contains("returned=1"));
    assert!(summary.contains("class=IPM.Note"));
    assert!(summary.contains("body_text_len=9"));
    assert!(summary.contains("body_html_len=16"));
    assert!(summary.contains("0x001a001f=IPM.Note"));
    assert!(summary.contains("0x0e070003="));
    assert!(summary.contains("0x1000001f=Body text"));
    assert!(summary.contains("0x10090102=binary:"));
    assert!(summary.contains("0x10130102=binary:bytes=16"), "{summary}");
    assert!(summary.contains("0x10160003=3"));
    assert!(summary.contains("0x1035001f=<message@example.test>"));

    let restricted = format_normal_message_query_row_summary(
        INBOX_FOLDER_ID,
        false,
        0,
        true,
        50,
        &[],
        Some(&MapiRestriction::Bitmask {
            property_tag: PID_TAG_MESSAGE_FLAGS,
            mask: 0x0000_0001,
            must_be_nonzero: true,
        }),
        &[PID_TAG_SUBJECT_W],
        std::slice::from_ref(&mailbox),
        std::slice::from_ref(&email),
        &empty_snapshot(),
    );

    assert!(restricted.contains("total=0"));
    assert!(restricted.contains("returned=0"));
}

#[test]
fn hierarchy_query_rows_wire_summary_decodes_compact_folder_projection() {
    fn append_utf16z(row: &mut Vec<u8>, value: &str) {
        for unit in value.encode_utf16() {
            row.extend_from_slice(&unit.to_le_bytes());
        }
        row.extend_from_slice(&0u16.to_le_bytes());
    }

    let columns = vec![
        PID_TAG_FOLDER_ID,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_CONTENT_COUNT,
    ];
    let mut response = vec![0x15, 0, 0, 0, 0, 0, 0, 1, 0];
    response.push(0);
    response.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(INBOX_FOLDER_ID).unwrap(),
    );
    append_utf16z(&mut response, "IPF.Note");
    append_utf16z(&mut response, "Inbox");
    response.extend_from_slice(&3u32.to_le_bytes());

    let summary = format_hierarchy_query_rows_wire_summary(&response, &columns, 8);

    assert!(summary.contains("total=1"), "{summary}");
    assert!(summary.contains("decoded=1"), "{summary}");
    assert!(
        summary.contains(
            "index=0;row_status=0x00;id=0x0000000000050001;class=IPF.Note;name=Inbox;count=3"
        ),
        "{summary}"
    );
    assert!(summary.contains("remaining_bytes=0"), "{summary}");
}

#[test]
fn uploaded_state_delta_anchor_requires_idset_and_cnset_seen() {
    let idset_only = upload_state_marker_bit(0x4017_0003);
    assert!(!uploaded_state_has_delta_anchor(idset_only));

    let cnset_only = upload_state_marker_bit(0x6796_0102);
    assert!(!uploaded_state_has_delta_anchor(cnset_only));

    assert!(uploaded_state_has_delta_anchor(idset_only | cnset_only));
}

#[test]
fn uploaded_state_empty_stream_does_not_create_delta_anchor() {
    let mut marker_mask = 0;
    let uploaded_bytes = 0usize;

    if uploaded_bytes > 0 {
        mark_uploaded_state_stream(&mut marker_mask, 0x4017_0003);
        mark_uploaded_state_stream(&mut marker_mask, 0x6796_0102);
    }

    assert!(!uploaded_state_has_delta_anchor(marker_mask));
}

#[test]
fn inbox_open_loop_summary_requires_repeated_probe_without_contents_table() {
    let mut state = PostHierarchyActionState::default();
    state.inbox_open_folder_probe_count = 2;
    state.inbox_folder_type_getprops_probe_count = 2;
    state
        .recent_probe_actions
        .push("Release(in=1,handle=2,kind=folder,folder=0x1)".to_string());

    let summary = format_inbox_open_loop_summary(&state).unwrap();

    assert!(summary.contains(&format!("folder=0x{INBOX_FOLDER_ID:016x}")));
    assert!(summary.contains("open_folder_count=2"));
    assert!(summary.contains("folder_type_getprops_count=2"));
    assert!(summary.contains("normal_contents_table_observed=false"));
    assert!(summary.contains("next_debug_focus=inbox_open_folder_loop"));
    assert!(summary.contains("last_common_views_inbox_shortcut=none"));
    assert!(summary.contains("last_inbox_hierarchy_table=none"));
    assert!(summary.contains("last_inbox_hierarchy_query=none"));
    assert!(summary.contains("last_inbox_related_release=none"));
    assert!(summary.contains("recent_actions=Release("));

    state.inbox_associated_contents_table_observed = true;
    let summary = format_inbox_open_loop_summary(&state).unwrap();
    assert!(summary.contains("next_debug_focus=common_views_or_inbox_fai_handoff"));
    state.last_inbox_hierarchy_query_context =
        "input_index=0;row_count=0;expected_subfolders=false".to_string();
    let summary = format_inbox_open_loop_summary(&state).unwrap();
    assert!(summary.contains("next_debug_focus=inbox_hierarchy_handoff"));

    state.inbox_normal_contents_table_observed = true;
    assert_eq!(format_inbox_open_loop_summary(&state), None);
}

#[test]
fn inbox_post_fai_handoff_context_points_to_missing_contents_step() {
    let mut state = PostHierarchyActionState::default();
    state.inbox_associated_contents_table_observed = true;
    state.inbox_associated_query_rows_returned_non_empty = true;
    state.inbox_associated_query_rows_reached_end = true;
    state.last_inbox_associated_query_context = "values=row0".to_string();
    state.last_inbox_associated_non_empty_query_context = "returned=6".to_string();
    state.last_inbox_associated_end_query_context = "returned=0;origin=end".to_string();
    state.last_common_views_inbox_shortcut_context = "entry_id_matches_inbox=true".to_string();
    state
        .recent_probe_actions
        .push("Release(in=0,handle=17,kind=contents_table,folder=0x5)".to_string());

    let context = format_inbox_post_fai_handoff_context(&state);

    assert!(context.contains("associated_contents_table_observed=true"));
    assert!(context.contains("associated_query_rows_returned_non_empty=true"));
    assert!(context.contains("associated_query_rows_reached_end=true"));
    assert!(context.contains("normal_contents_table_observed=false"));
    assert!(context.contains("last_associated_query=values=row0"));
    assert!(context.contains("last_associated_non_empty_query=returned=6"));
    assert!(context.contains("last_associated_end_query=returned=0;origin=end"));
    assert!(context.contains("last_common_views_inbox_shortcut=entry_id_matches_inbox=true"));
    assert!(context.contains(
        "next_expected_client_step=open_inbox_associated_config_message_or_normal_contents_table"
    ));
}

#[test]
fn post_fai_hierarchy_release_context_reports_stop_before_inbox_contents() {
    let mut state = PostHierarchyActionState::default();
    state.inbox_associated_contents_table_observed = true;
    state.post_inbox_fai_handoff_logged = true;
    state.last_inbox_associated_query_context = "window=returned=6".to_string();
    state
        .recent_probe_actions
        .push("GetHierarchyTable(in=0,out=13,row_count=22)".to_string());
    let table = MapiObject::HierarchyTable {
        folder_id: IPM_SUBTREE_FOLDER_ID,
        columns: vec![
            PID_TAG_FOLDER_ID,
            PID_TAG_SUBFOLDERS,
            PID_TAG_CONTAINER_CLASS_W,
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
    };

    let context = format_post_fai_hierarchy_release_without_inbox_contents_context(
        Some(&table),
        Some(13),
        &state,
        &[],
        &MapiMailStoreSnapshot::empty(),
    )
    .unwrap();

    assert!(context.contains("handle=13"));
    assert!(context.contains(&format!("folder=0x{IPM_SUBTREE_FOLDER_ID:016x}")));
    assert!(context.contains("role=ipm_subtree"));
    assert!(context.contains("row_count=22"));
    assert!(context.contains("last_associated_query=window=returned=6"));
    assert!(context
        .contains("next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure"));
}

#[test]
fn post_fai_folder_type_probe_loop_context_requires_reopen_and_repeated_probes() {
    let mut state = PostHierarchyActionState::default();
    state.post_inbox_fai_handoff_logged = true;
    state.post_inbox_fai_reopen_logged = true;
    state.inbox_associated_contents_table_observed = true;
    state.inbox_open_folder_probe_count = 3;
    state.inbox_folder_type_getprops_probe_count = 2;
    state.last_inbox_open_folder_context = "output_handle=25".to_string();
    state.last_inbox_folder_type_getprops_context = "folder_type=1".to_string();
    state.last_inbox_associated_query_context = "window=returned=6".to_string();
    state.last_inbox_related_release_context = "handle=20;role=ipm_subtree".to_string();
    state
        .recent_probe_actions
        .push("OpenFolder(in=1,handle=8,out=25,folder=0x0000000000050001)".to_string());
    state
        .recent_probe_actions
        .push("GetPropertiesSpecific(in=2,handle=25,tags=0x36010003)".to_string());

    let context = format_post_fai_folder_type_probe_loop_context(&state).unwrap();

    assert!(context.contains("open_folder_count=3"));
    assert!(context.contains("folder_type_getprops_count=2"));
    assert!(context.contains("last_open=output_handle=25"));
    assert!(context.contains("last_folder_type_getprops=folder_type=1"));
    assert!(context.contains("last_associated_query=window=returned=6"));
    assert!(context.contains("last_inbox_related_release=handle=20;role=ipm_subtree"));
    assert!(context
        .contains("next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure"));

    state.inbox_normal_contents_table_observed = true;
    assert!(format_post_fai_folder_type_probe_loop_context(&state).is_none());
}

#[test]
fn inbox_release_context_flags_visible_table_setcolumns_without_query_rows() {
    let mut state = PostHierarchyActionState::default();
    state.inbox_normal_contents_table_observed = true;
    state.inbox_normal_contents_table_setcolumns_observed = true;
    state.last_inbox_normal_contents_table_setcolumns_handle = Some(17);
    state.last_inbox_normal_contents_table_setcolumns_context =
        "handle=17;columns=0x67480014,0x674a0014,0x0037001f".to_string();
    let table = MapiObject::ContentsTable {
        folder_id: INBOX_FOLDER_ID,
        associated: false,
        columns: vec![PID_TAG_FOLDER_ID, PID_TAG_MID, PID_TAG_SUBJECT_W],
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

    let context = format_inbox_related_release_context(
        Some(&table),
        Some(17),
        &state,
        &MapiMailStoreSnapshot::empty(),
    )
    .unwrap();

    assert!(context.contains("associated=false"));
    assert!(context.contains("normal_setcolumns_observed=true"));
    assert!(context.contains("normal_query_rows_observed=false"));
    assert!(context.contains("visible_inbox_release_without_query_rows=true"));
    assert!(context.contains("last_normal_setcolumns_handle=17"));
    assert!(context.contains("last_normal_query_rows_handle=none"));
}

#[test]
fn normal_message_column_support_covers_visible_inbox_probe_columns() {
    let summary = normal_message_table_column_support_summary(&[
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_SUBJECT_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
    ]);

    assert!(summary
        .contains("backed=0x67480014,0x674a0014,0x674d0014,0x674e0003,0x0037001f,0x0e060040"));
    assert!(summary.ends_with("defaulted=;named_or_dynamic="));
}

#[test]
fn normal_message_column_support_covers_outlook_mail_view_columns() {
    for view_name in ["Messages", "Compact", "Sent To"] {
        let columns = outlook_mail_view_definition(view_name)
            .columns
            .iter()
            .map(|column| column.property_tag)
            .collect::<Vec<_>>();
        let summary = normal_message_table_column_support_summary(&columns);

        assert!(
            summary.contains(";defaulted=;"),
            "{view_name} view has defaulted message-table columns: {summary}"
        );
        assert!(
            summary.ends_with(match view_name {
                "Compact" | "Messages" | "Sent To" => "named_or_dynamic=",
                _ => unreachable!(),
            }),
            "{view_name} view has unexpected named/dynamic columns: {summary}"
        );
    }
}

#[test]
fn normal_message_column_support_covers_observed_inbox_compact_projection() {
    let summary = normal_message_table_column_support_summary(&[
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_CREATION_TIME,
        PID_TAG_SUBJECT_W,
        PID_TAG_SENT_REPRESENTING_NAME_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_INTERNET_MESSAGE_ID_W,
        PID_TAG_IMPORTANCE,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_MESSAGE_STATUS,
        0x8514_000B,
        0x8017_000B,
        0x801F_001F,
        PID_TAG_SENT_REPRESENTING_ENTRY_ID,
        0x1213_0003,
        PID_TAG_MESSAGE_DELIVERY_TIME,
    ]);

    assert!(summary.contains("0x00410102"));
    assert!(!summary.contains("defaulted=0x00410102"));
    assert!(!summary.contains("defaulted=0x12130003"));
    assert!(summary.contains("0x8514000b"));
    assert!(summary.contains("0x8017000b"));
    assert!(summary.contains("0x801f001f"));
    assert!(summary.ends_with("named_or_dynamic="));
}

#[test]
fn normal_message_column_support_backs_outlook_auxiliary_flags() {
    let detail =
        normal_message_defaulted_column_detail(&[PID_TAG_SUBJECT_W, 0x1213_0003, 0x801f_001f]);

    assert!(!detail.contains("tag=0x12130003"));
    assert!(!detail.contains("0x0037001f"));
    assert!(!detail.contains("0x801f001f"));
}

#[test]
fn calendar_query_position_summary_projects_observed_outlook_columns() {
    let event_id = uuid::Uuid::from_u128(0x7174);
    crate::mapi::identity::remember_mapi_identity(
        event_id,
        crate::mapi::identity::mapi_store_id(0x7174),
    );
    let event = lpe_storage::AccessibleEvent {
        id: event_id,
        uid: "calendar-row".to_string(),
        collection_id: DEFAULT_CALENDAR_COLLECTION_ID.to_string(),
        owner_account_id: uuid::Uuid::from_u128(0x8184),
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test User".to_string(),
        rights: default_mapping_rights(),
        date: "2026-06-23".to_string(),
        time: "15:00".to_string(),
        time_zone: "Europe/Berlin".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Calendar row".to_string(),
        location: "Office".to_string(),
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

    let summary = format_calendar_event_query_position_summary(
        CALENDAR_FOLDER_ID,
        false,
        0,
        1,
        &[],
        None,
        &[
            PID_TAG_FOLDER_ID,
            PID_TAG_MID,
            PID_TAG_INST_ID,
            PID_TAG_INSTANCE_NUM,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_MESSAGE_FLAGS,
            PID_TAG_MESSAGE_STATUS,
            PID_LID_OUTLOOK_COMMON_8578_TAG,
            PID_LID_SIDE_EFFECTS_TAG,
        ],
        &snapshot,
    );

    assert!(summary.contains("event_total=1"));
    assert!(summary.contains("title=Calendar row"));
    assert!(summary.contains("0x85780003=0"));
    assert!(summary.contains("0x85100003=353"));
    assert!(!summary.contains("0x67480014=default"));
    assert!(!summary.contains("0x674d0014=default"));
    assert!(!summary.contains("0x674e0003=default"));
    assert!(!summary.contains("0x001a001f=default"));
    assert!(!summary.contains("0x0e170003=default"));
}

#[test]
fn associated_column_support_covers_inbox_view_descriptor_columns() {
    let summary = associated_contents_table_column_support_summary(&[
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_SUBJECT_W,
        PID_TAG_VIEW_DESCRIPTOR_FLAGS,
        PID_TAG_VIEW_DESCRIPTOR_CLSID,
        PID_TAG_VIEW_DESCRIPTOR_VERSION,
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
        0x6842_0102,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_MESSAGE_CLASS_W,
    ]);

    assert!(summary.contains("0x68340003"));
    assert!(summary.contains("0x68330048"));
    assert!(summary.contains("0x683a0003"));
    assert!(summary.contains("0x68410003"));
    assert!(summary.contains("0x68420102"));
    assert!(summary.ends_with("defaulted=;named_or_dynamic="));
}

#[test]
fn associated_column_support_covers_inbox_configuration_columns() {
    let summary = associated_contents_table_column_support_summary(&[
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_ROAMING_DATATYPES,
        PID_TAG_MESSAGE_CLASS_W,
        0x685D_0003,
        PID_TAG_LAST_MODIFICATION_TIME,
    ]);

    assert!(summary.contains("0x7c060003"));
    assert!(summary.contains("0x685d0003"));
    assert!(summary.ends_with("defaulted=;named_or_dynamic="));
}

#[test]
fn associated_column_support_covers_common_views_wlink_binary_variants() {
    let summary = associated_contents_table_column_support_summary(&[
        PID_TAG_FOLDER_ID,
        PID_TAG_MID,
        PID_TAG_INST_ID,
        PID_TAG_INSTANCE_NUM,
        PID_TAG_MESSAGE_CLASS_W,
        0x6842_0102,
        PID_TAG_WLINK_SAVE_STAMP,
        PID_TAG_SUBJECT_W,
        PID_TAG_WLINK_TYPE,
        PID_TAG_WLINK_FLAGS,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_ENTRY_ID,
        PID_TAG_WLINK_RECORD_KEY,
        PID_TAG_WLINK_CALENDAR_COLOR,
        PID_TAG_WLINK_STORE_ENTRY_ID,
        0x684F_0102,
        0x6850_0102,
        PID_TAG_WLINK_GROUP_NAME_W,
        PID_TAG_WLINK_SECTION,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        PID_TAG_WLINK_CLIENT_ID,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        PID_TAG_WLINK_RO_GROUP_TYPE,
        0x6893_0102,
        0x8010_0102,
    ]);

    assert!(summary.contains("0x684f0102"));
    assert!(summary.contains("0x68500102"));
    assert!(summary.contains("named_or_dynamic=0x80100102"));
    assert!(!summary.contains("defaulted=0x684f0102"));
    assert!(!summary.contains("defaulted=0x68500102"));
}

#[test]
fn inbox_post_fai_reopen_stall_requires_handoff_release_without_normal_contents() {
    let mut state = PostHierarchyActionState::default();
    state.post_inbox_fai_handoff_logged = true;
    state.inbox_associated_contents_table_observed = true;
    state.last_inbox_related_release_context =
        "handle=16;kind=contents_table;associated=true".to_string();

    assert!(inbox_post_fai_reopen_stall_observed(&state));

    state.inbox_normal_contents_table_observed = true;
    assert!(!inbox_post_fai_reopen_stall_observed(&state));

    state.inbox_normal_contents_table_observed = false;
    state.last_inbox_related_release_context.clear();
    assert!(!inbox_post_fai_reopen_stall_observed(&state));
}

#[test]
fn post_sync_release_flags_counts_outlook_close_handles() {
    let events = vec![
        PostHierarchyReleaseDebugEvent {
            input_handle_index: 0,
            handle: "1".to_string(),
            object_kind: "synchronization_source".to_string(),
            folder_id: "0x0000000000050001".to_string(),
            remaining_before: 4,
            remaining_after: 3,
            logon_before_content_sync: false,
        },
        PostHierarchyReleaseDebugEvent {
            input_handle_index: 1,
            handle: "2".to_string(),
            object_kind: "synchronization_collector".to_string(),
            folder_id: "0x0000000000050001".to_string(),
            remaining_before: 3,
            remaining_after: 2,
            logon_before_content_sync: false,
        },
        PostHierarchyReleaseDebugEvent {
            input_handle_index: 2,
            handle: "3".to_string(),
            object_kind: "notification_subscription".to_string(),
            folder_id: "none".to_string(),
            remaining_before: 2,
            remaining_after: 1,
            logon_before_content_sync: false,
        },
        PostHierarchyReleaseDebugEvent {
            input_handle_index: 3,
            handle: "4".to_string(),
            object_kind: "logon".to_string(),
            folder_id: "none".to_string(),
            remaining_before: 1,
            remaining_after: 0,
            logon_before_content_sync: false,
        },
    ];

    let flags = post_sync_release_flags(&events);

    assert!(flags.contains("logon=1"), "{flags}");
    assert!(flags.contains("synchronization_source=1"), "{flags}");
    assert!(flags.contains("synchronization_collector=1"), "{flags}");
    assert!(flags.contains("notification_subscription=1"), "{flags}");
    assert!(flags.contains("folder=0"), "{flags}");
}

#[test]
fn save_changes_success_response_updates_response_handle_slot() {
    let request = RopRequest {
        rop_id: 0x0c,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0],
    };
    let mut responses = Vec::new();
    let mut handle_slots = vec![77, u32::MAX];

    append_save_changes_message_response(
        &mut responses,
        &mut handle_slots,
        &request,
        77,
        0x0000_0000_0000_1234,
    );

    assert_eq!(handle_slots, vec![77, 77]);
    assert_eq!(responses[0], 0x0c);
    assert_eq!(responses[1], 1);
}

#[test]
fn builtin_search_criteria_fallback_covers_advertised_reminders_folder() {
    let (restriction, folder_ids, flags) =
        builtin_search_criteria_to_rop_for_folder_id(REMINDERS_FOLDER_ID)
            .expect("reminders built-in search criteria");

    assert!(restriction.is_empty());
    assert_eq!(folder_ids, vec![CALENDAR_FOLDER_ID, TASKS_FOLDER_ID]);
    assert_eq!(flags, SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG);
    assert_eq!(
        builtin_search_role_for_folder_id(REMINDERS_FOLDER_ID),
        Some("reminders")
    );
}

#[test]
fn builtin_search_criteria_fallback_covers_tracked_mail_processing_folder() {
    let (restriction, folder_ids, flags) =
        builtin_search_criteria_to_rop_for_folder_id(TRACKED_MAIL_PROCESSING_FOLDER_ID)
            .expect("tracked mail processing built-in search criteria");

    assert!(restriction.is_empty());
    assert_eq!(folder_ids, vec![IPM_SUBTREE_FOLDER_ID]);
    assert_eq!(flags, SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG);
    assert_eq!(
        builtin_search_role_for_folder_id(TRACKED_MAIL_PROCESSING_FOLDER_ID),
        Some("tracked_mail_processing")
    );
}

#[test]
fn search_criteria_debug_scope_reports_invalid_folder_ids() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&0u64.to_le_bytes());
    payload.extend_from_slice(&SEARCH_RUNNING_FLAG.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::SetSearchCriteria.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let context = format_debug_search_criteria_scope(&request);

    assert!(context.contains("parse=invalid_folder_id"));
    assert!(context.contains("folder_count=1"));
    assert!(context.contains("invalid:0000000000000000"));
    assert!(context.contains("flags=0x00000001"));
}

#[test]
fn blank_search_criteria_is_invalid() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&INBOX_FOLDER_ID.to_le_bytes());
    payload.extend_from_slice(&(SEARCH_RUNNING_FLAG | SEARCH_RECURSIVE_FLAG).to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::SetSearchCriteria.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let error = bounded_search_criteria_from_rop(&request, INBOX_FOLDER_ID, None, &[]).unwrap_err();

    assert_eq!(error, EC_SEARCH_INVALID_PARAMETER);
}

#[test]
fn event_19_candidate_detects_same_batch_save_getprops_not_found() {
    let request = RopRequestDebugSummary {
        ids: vec![
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x06, 0x0a, 0x0a, 0x0a, 0x0a, 0x0c,
            0x07,
        ],
        ..RopRequestDebugSummary::default()
    };
    let response = RopResponseDebugSummary {
        results_csv: "0x06:0x00000000,0x0a:0x00000000,0x0c:0x00000000,0x07:0x8004010f".to_string(),
        ..RopResponseDebugSummary::default()
    };

    assert!(execute_batch_has_same_save_getprops_not_found(
        &request, &response
    ));

    let response = RopResponseDebugSummary {
        results_csv: "0x06:0x00000000,0x0a:0x00000000,0x0c:0x00000000,0x07:0x00000000".to_string(),
        ..RopResponseDebugSummary::default()
    };

    assert!(!execute_batch_has_same_save_getprops_not_found(
        &request, &response
    ));
}

#[test]
fn long_term_id_from_id_rejects_unparsed_or_not_loaded_scope() {
    let object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 1,
    );
    let request = RopRequest {
        rop_id: RopId::LongTermIdFromId as u8,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: crate::mapi::identity::wire_id_bytes_from_object_id(object_id)
            .unwrap()
            .to_vec(),
    };

    assert_eq!(
        rop_long_term_id_from_id_response_for_scope(&request, None, "not_loaded"),
        vec![RopId::LongTermIdFromId as u8, 0x00, 0x0F, 0x01, 0x04, 0x80]
    );
    assert_eq!(
        &rop_long_term_id_from_id_response_for_scope(&request, None, "message")[..6],
        &[RopId::LongTermIdFromId as u8, 0x00, 0, 0, 0, 0]
    );
    assert_eq!(
        rop_long_term_id_from_id_response_for_scope(&request, None, "unparsed"),
        vec![RopId::LongTermIdFromId as u8, 0x00, 0x0F, 0x01, 0x04, 0x80]
    );
}

#[test]
fn logon_response_debug_summary_decodes_private_mailbox_fields() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let request = RopRequest {
        rop_id: 0xFE,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0x01],
    };
    let response_buffer =
        rop_buffer_with_response(rop_logon_response_body(&principal, &request), &[42]);

    let summary = summarize_logon_response_rop(&response_buffer, &[0xFE]);

    assert!(summary.present);
    assert_eq!(summary.output_handle_index, "1");
    assert_eq!(summary.error_code, "0x00000000");
    assert_eq!(summary.logon_flags, "0x01");
    assert!(summary
        .special_folder_ids
        .starts_with(&format!("{ROOT_FOLDER_ID:#018x}")));
    assert!(summary
        .special_folder_contract
        .contains(&format!("3:ipm_subtree=0x{IPM_SUBTREE_FOLDER_ID:016x}")));
    assert!(summary
        .special_folder_contract
        .contains(&format!("4:inbox=0x{INBOX_FOLDER_ID:016x}")));
    assert!(summary.special_folder_contract_issues.is_empty());
    assert_eq!(summary.response_flags, "0x07");
    assert_eq!(summary.mailbox_guid, principal.account_id.to_string());
    assert_eq!(summary.replid, "1");
    assert_eq!(summary.replica_guid.len(), 32);
    assert!(summary.parse_error.is_empty());
}

fn utf16z_bytes(value: &str) -> Vec<u8> {
    value
        .encode_utf16()
        .chain(std::iter::once(0))
        .flat_map(u16::to_le_bytes)
        .collect()
}

fn get_properties_specific_request(property_tags: &[u32]) -> RopRequest {
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
    for tag in property_tags {
        payload.extend_from_slice(&tag.to_le_bytes());
    }
    RopRequest {
        rop_id: 0x07,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    }
}

fn test_principal() -> AccountPrincipal {
    AccountPrincipal {
        tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    }
}

fn test_mapi_session() -> MapiSession {
    let principal = test_principal();
    MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: principal.tenant_id,
        account_id: principal.account_id,
        email: principal.email,
        created_at: SystemTime::UNIX_EPOCH,
        last_seen_at: SystemTime::UNIX_EPOCH,
        first_request_type: String::new(),
        first_request_id: String::new(),
        last_request_type: String::new(),
        last_request_id: String::new(),
        request_count: 0,
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
        inbox_associated_config_stream_handles: HashSet::new(),
        inbox_rule_organizer_stream_handles: HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    }
}

fn empty_snapshot() -> MapiMailStoreSnapshot {
    MapiMailStoreSnapshot::new(
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
}
