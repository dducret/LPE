use super::*;

fn principal() -> AccountPrincipal {
    AccountPrincipal {
        tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "user@example.test".to_string(),
        display_name: "User".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    }
}

#[test]
fn reconnect_session_rejects_active_context() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let _active = begin_active_session_request(&session_id).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        "cookie",
        HeaderValue::from_str(&format!("MapiContext={session_id}")).unwrap(),
    );

    let Err(response) = reconnect_session(
        MapiEndpoint::Emsmdb,
        &principal,
        &headers,
        "Connect",
        "{11111111-2222-3333-4444-555555555555}:1",
    ) else {
        panic!("active session reconnect should be rejected");
    };

    assert_eq!(
        response_header(&response, "x-requesttype").unwrap(),
        "Connect"
    );
    assert_eq!(
        response_header(&response, "x-requestid").unwrap(),
        "{11111111-2222-3333-4444-555555555555}:1"
    );
    assert_eq!(response_header(&response, "x-responsecode").unwrap(), "15");
    remove_session(&session_id);
}

#[test]
fn execute_replay_cache_evicts_oldest_inserted_request_id() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();

    for index in 0..=MAX_CACHED_EXECUTE_REQUESTS {
        cache_execute_response(
            &mut session,
            &format!("{{11111111-2222-3333-4444-555555555555}}:{index}"),
            index as u64,
            &[index as u8],
            format!("request-{index}"),
            format!("response-{index}"),
            format!("result-{index}"),
            index,
        );
    }

    assert!(!session
        .completed_execute_requests
        .contains_key("{11111111-2222-3333-4444-555555555555}:0"));
    assert!(session
        .completed_execute_requests
        .contains_key("{11111111-2222-3333-4444-555555555555}:1"));
    assert!(session.completed_execute_requests.contains_key(&format!(
        "{{11111111-2222-3333-4444-555555555555}}:{MAX_CACHED_EXECUTE_REQUESTS}"
    )));
    assert_eq!(
        session.completed_execute_requests.len(),
        MAX_CACHED_EXECUTE_REQUESTS
    );
}

#[test]
fn session_records_transport_request_lifetime() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();

    session.record_transport_request("Execute", "test:2");
    session.record_transport_request("Disconnect", "test:3");

    assert_eq!(session.first_request_type, "Connect");
    assert_eq!(session.first_request_id, "test:1");
    assert_eq!(session.last_request_type, "Disconnect");
    assert_eq!(session.last_request_id, "test:3");
    assert_eq!(session.request_count, 3);
    assert_eq!(session.execute_request_count, 1);
}

#[test]
fn session_detects_abandon_after_inbox_fai_query_rows_release() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();

    session.record_inbox_associated_contents_table();
    session.record_last_inbox_associated_query_context(
        "handle=19;folder=0x0000000000050001;associated=true;response_row_count=5".to_string(),
    );
    session.record_last_table_release_context(
        "phase=release;folder=0x0000000000050001;role=inbox;associated=true".to_string(),
    );

    assert!(session.abandoned_after_inbox_fai_query_rows());

    session.record_inbox_associated_findrow_returned_content();

    assert!(!session.abandoned_after_inbox_fai_query_rows());
}

#[test]
fn session_does_not_treat_findrow_delivered_fai_as_abandoned() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();

    session.record_inbox_associated_contents_table();
    session.record_inbox_associated_findrow_returned_content();
    session.record_last_inbox_associated_query_context(
        "handle=19;folder=0x0000000000050001;associated=true;response_row_count=0".to_string(),
    );
    session.record_last_table_release_context(
        "phase=release;folder=0x0000000000050001;role=inbox;associated=true".to_string(),
    );

    assert!(!session.abandoned_after_inbox_fai_query_rows());

    session.record_inbox_normal_contents_table();

    assert!(!session.abandoned_after_inbox_fai_query_rows());
}

#[test]
fn session_remembers_deleted_advertised_special_folder() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();

    assert!(!session.advertised_special_folder_was_deleted(QUICK_STEP_SETTINGS_FOLDER_ID));

    session.record_deleted_advertised_special_folder(QUICK_STEP_SETTINGS_FOLDER_ID);

    assert!(session.advertised_special_folder_was_deleted(QUICK_STEP_SETTINGS_FOLDER_ID));
}

#[test]
fn reset_table_state_removes_columns_sort_restriction_and_bookmarks() {
    let mut bookmarks = HashMap::new();
    bookmarks.insert(
        b"bookmark".to_vec(),
        TableBookmark {
            position: 7,
            row_key: Some(3),
        },
    );
    let mut collapsed_categories = HashSet::new();
    collapsed_categories.insert(0x1234);
    let mut table = MapiObject::ContentsTable {
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
        restriction: Some(MapiRestriction::Exist {
            property_tag: PID_TAG_SUBJECT_W,
        }),
        bookmarks,
        next_bookmark: 9,
        position: 4,
    };

    assert!(reset_table_state(&mut table));

    let MapiObject::ContentsTable {
        columns,
        columns_set,
        sort_orders,
        category_count,
        expanded_count,
        collapsed_categories,
        restriction,
        bookmarks,
        next_bookmark,
        position,
        ..
    } = table
    else {
        panic!("expected contents table");
    };
    assert!(columns.is_empty());
    assert!(!columns_set);
    assert!(sort_orders.is_empty());
    assert_eq!(category_count, 0);
    assert_eq!(expanded_count, 0);
    assert!(collapsed_categories.is_empty());
    assert!(restriction.is_none());
    assert!(bookmarks.is_empty());
    assert_eq!(next_bookmark, 9);
    assert_eq!(position, 0);
}

#[test]
fn session_remembers_saved_search_folder_definition() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let folder_id = 0x0000_0000_0157_0001;
    let definition = SearchFolderDefinition {
        id: Uuid::from_u128(0x157),
        account_id: principal.account_id,
        role: "custom".to_string(),
        display_name: "Unread Mail".to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "email".to_string(),
        scope_json: serde_json::json!({"kind": "mapi_bounded"}),
        restriction_json: serde_json::json!({"kind": "mapi_bounded", "all": []}),
        excluded_folder_roles: Vec::new(),
        is_builtin: false,
    };

    session.remember_search_folder_definition(folder_id, definition);

    assert_eq!(
        session
            .search_folder_definition(folder_id)
            .map(|definition| definition.display_name.as_str()),
        Some("Unread Mail")
    );
}

#[test]
fn session_remembers_deleted_saved_search_folder_definition() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let folder_id = 0x0000_0000_01db_0001;
    let definition = SearchFolderDefinition {
        id: Uuid::from_u128(0x1db),
        account_id: principal.account_id,
        role: "custom".to_string(),
        display_name: "Categories".to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "email".to_string(),
        scope_json: serde_json::json!({"kind": "mapi_bounded"}),
        restriction_json: serde_json::json!({"kind": "mapi_bounded", "all": []}),
        excluded_folder_roles: Vec::new(),
        is_builtin: false,
    };

    session.remember_search_folder_definition(folder_id, definition);
    assert!(!session.search_folder_definition_was_deleted(folder_id));

    assert_eq!(
        session
            .forget_search_folder_definition(folder_id)
            .map(|definition| definition.display_name),
        Some("Categories".to_string())
    );

    assert!(session.search_folder_definition(folder_id).is_none());
    assert!(session.search_folder_definition_was_deleted(folder_id));
}

#[test]
fn session_records_completed_sync_checkpoint_once() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();

    session.record_completed_sync_checkpoint(
        0x0000_0000_0010_0001,
        "calendar",
        "IPF.Appointment",
        "content",
        0x01,
        "ok",
    );
    session.record_completed_sync_checkpoint(
        0x0000_0000_0010_0001,
        "calendar",
        "IPF.Appointment",
        "content",
        0x01,
        "ok",
    );

    assert_eq!(
            session
                .post_hierarchy_actions
                .completed_sync_checkpoint_summaries,
            vec![
                "folder=0x0000000000100001;role=calendar;container=IPF.Appointment;kind=content;sync=0x01;status=ok"
                    .to_string()
            ]
        );
}

#[test]
fn response_handle_table_preserves_sparse_output_handle_indexes() {
    let handles = response_handle_table(&[10, 20, 30], &[20, 30], false);

    assert_eq!(handles, vec![10, 20, 30]);
}

#[test]
fn response_handle_table_can_echo_released_input_slots() {
    let handles = response_handle_table(&[u32::MAX], &[], true);

    assert_eq!(handles, vec![u32::MAX]);
}

#[test]
fn allocate_output_handle_prefers_free_low_output_slot_handle() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();

    let logon_handle = session.allocate_output_handle(Some(0), MapiObject::Logon);
    let source_handle = session.allocate_output_handle(
        Some(1),
        MapiObject::Folder {
            folder_id: 0x0000_0000_0004_0001,
            properties: HashMap::new(),
        },
    );

    assert_eq!(logon_handle, 1);
    assert_eq!(source_handle, 2);
}

#[test]
fn allocate_output_handle_skips_reserved_same_execute_handle() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    session.allocate_output_handle(Some(0), MapiObject::Logon);
    session.allocate_output_handle(
        Some(1),
        MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    let released_handle = session.allocate_output_handle(
        Some(2),
        MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    session.handles.remove(&released_handle);

    let handle = session.allocate_output_handle_avoiding(
        Some(2),
        MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        },
        &HashSet::from([released_handle]),
    );

    assert_eq!(released_handle, 3);
    assert_ne!(handle, released_handle);
    assert!(handle > released_handle);
}

#[test]
fn allocate_output_handle_does_not_reuse_old_low_slot_handle() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    session.allocate_output_handle(Some(0), MapiObject::Logon);
    session.allocate_output_handle(
        Some(1),
        MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    let released_handle = session.allocate_output_handle(
        Some(2),
        MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        },
    );
    session.handles.remove(&released_handle);

    let handle = session.allocate_output_handle(
        Some(2),
        MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: HashMap::new(),
        },
    );

    assert_eq!(released_handle, 3);
    assert_eq!(handle, 4);
}

#[test]
fn cached_named_property_updates_bidirectional_registry() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let property = MapiNamedProperty {
        guid: PSETID_COMMON_GUID,
        kind: MapiNamedPropertyKind::Name("custom-field".to_string()),
    };

    session.cache_named_property(0x9001, property.clone());

    assert_eq!(
        session.property_id_for_name(property.clone(), false),
        Some(0x9001)
    );
    assert_eq!(session.property_name_for_id(0x9001), property);
    assert_eq!(session.next_named_property_id, 0x9002);
}

#[test]
fn cached_calendar_named_property_preserves_registered_id() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let property = MapiNamedProperty {
        guid: PSETID_COMMON_GUID,
        kind: MapiNamedPropertyKind::Lid(PID_LID_SIDE_EFFECTS),
    };

    session.cache_named_property(0x8005, property.clone());

    assert_eq!(
        session.property_id_for_name(property.clone(), false),
        Some(0x8005)
    );
    assert_eq!(session.property_name_for_id(0x8005), property);
}

#[test]
fn dynamic_named_property_allocation_starts_at_project_dynamic_range() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let property = MapiNamedProperty {
        guid: PS_PUBLIC_STRINGS_GUID,
        kind: MapiNamedPropertyKind::Name("custom-low-range-guard".to_string()),
    };

    assert_eq!(
        session.property_id_for_name(property.clone(), true),
        Some(0x9001)
    );
    assert_eq!(
        session.property_id_for_name(property.clone(), false),
        Some(0x9001)
    );
    assert_eq!(session.property_name_for_id(0x9001), property);
    assert_eq!(session.next_named_property_id, 0x9002);
}

#[test]
fn cached_well_known_named_property_keeps_registered_dynamic_id() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let property = MapiNamedProperty {
        guid: PSETID_SHARING_GUID,
        kind: MapiNamedPropertyKind::Name(
            "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
        ),
    };

    session.cache_named_property(0x8fff, property.clone());

    assert_eq!(
        session.property_id_for_name(property.clone(), false),
        Some(0x8fff)
    );
    assert_eq!(session.property_name_for_id(0x8010), property);
    assert_eq!(session.property_name_for_id(0x8fff), property);
    assert_eq!(session.next_named_property_id, 0x9000);
}

#[test]
fn cached_well_known_named_property_keeps_registered_reserved_range_id() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let property = MapiNamedProperty {
        guid: PSETID_ADDRESS_GUID,
        kind: MapiNamedPropertyKind::Lid(PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS),
    };

    session.cache_named_property(PID_LID_EMAIL1_EMAIL_ADDRESS as u16, property.clone());

    assert_eq!(
        session.property_id_for_name(property.clone(), false),
        Some(PID_LID_EMAIL1_EMAIL_ADDRESS as u16)
    );
    assert_eq!(
        session.property_name_for_id(PID_LID_EMAIL1_EMAIL_ADDRESS as u16),
        property
    );
}

#[test]
fn cached_unknown_named_property_keeps_registered_reserved_range_id() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let property = MapiNamedProperty {
        guid: PS_PUBLIC_STRINGS_GUID,
        kind: MapiNamedPropertyKind::Name("custom-contact-shadow".to_string()),
    };

    session.cache_named_property(PID_LID_EMAIL1_DISPLAY_NAME as u16, property.clone());

    assert_eq!(
        session.property_id_for_name(property.clone(), false),
        Some(PID_LID_EMAIL1_DISPLAY_NAME as u16)
    );
    assert_eq!(
        session.property_name_for_id(PID_LID_EMAIL1_DISPLAY_NAME as u16),
        property
    );
}

#[test]
fn ps_mapi_lid_maps_directly_even_in_named_property_range() {
    let principal = principal();
    let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
    let mut session = remove_session(&session_id).unwrap();
    let property = MapiNamedProperty {
        guid: PS_MAPI_GUID,
        kind: MapiNamedPropertyKind::Lid(0x8503),
    };

    assert_eq!(session.property_id_for_name(property, false), Some(0x8503));
    assert_eq!(session.next_named_property_id, FIRST_NAMED_PROPERTY_ID);
}
