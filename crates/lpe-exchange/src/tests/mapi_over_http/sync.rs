use super::*;

#[tokio::test]
async fn mapi_over_http_connect_reestablishes_session_context_with_open_sync_handle() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        "48484848-4848-4848-4848-484848484848",
        mailbox_id,
        "inbox",
        "Reconnect sync context message",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let first_cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut configure_rops = Vec::new();
    append_rop_open_folder(&mut configure_rops, 0, 1, test_mapi_folder_id(5));
    configure_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
    ]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let configure_request = execute_body(&rop_buffer(&configure_rops, &[1, u32::MAX, u32::MAX]));
    let configure_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &configure_request)
        .await
        .unwrap();
    assert_eq!(configure_response.status(), StatusCode::OK);
    assert_eq!(
        configure_response.headers().get("x-responsecode").unwrap(),
        "0"
    );

    let mut reconnect_headers = mapi_headers("Connect");
    reconnect_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(reconnect.status(), StatusCode::OK);
    assert_eq!(reconnect.headers().get("x-responsecode").unwrap(), "0");
    let reconnected_cookie = reconnect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_ne!(reconnected_cookie, first_cookie);

    let mut get_buffer_rops = Vec::new();
    get_buffer_rops.extend_from_slice(&[0x4E, 0x00, 0x00]);
    get_buffer_rops.extend_from_slice(&4096u16.to_le_bytes());
    let mut reconnected_execute_headers = mapi_headers("Execute");
    reconnected_execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&reconnected_cookie).unwrap(),
    );
    let get_buffer_request = execute_body(&rop_buffer(&get_buffer_rops, &[3]));
    let get_buffer_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &reconnected_execute_headers,
            &get_buffer_request,
        )
        .await
        .unwrap();

    assert_eq!(get_buffer_response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(get_buffer_response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(
        &response_rops,
        b"Reconnect sync context message"
    ));
}

#[tokio::test]
async fn mapi_over_http_move_folder_updates_custom_canonical_mailbox_and_hierarchy_sync() {
    let source_id = Uuid::parse_str("31313131-3131-4131-8131-313131313131").unwrap();
    let target_parent_id = Uuid::parse_str("32323232-3232-4232-8232-323232323232").unwrap();
    let source_mapi_id = test_mapi_uuid_id(&source_id);
    let target_parent_mapi_id = test_mapi_uuid_id(&target_parent_id);
    crate::mapi::identity::remember_mapi_identity(source_id, source_mapi_id);
    crate::mapi::identity::remember_mapi_identity(target_parent_id, target_parent_mapi_id);
    let updated_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&source_id.to_string(), "custom", "Projects"),
            FakeStore::mailbox(&target_parent_id.to_string(), "custom", "Clients"),
        ])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([
            (source_id, source_mapi_id),
            (target_parent_id, target_parent_mapi_id),
        ]))),
        updated_mailboxes: updated_mailboxes.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_open_folder(&mut rops, 0, 2, target_parent_mapi_id);
    rops.extend_from_slice(&[
        0x35, 0x00, 0x01,
        0x02, // RopMoveFolder, source parent handle, destination parent handle
        0x00, // synchronous
        0x01, // Unicode name
    ]);
    append_mapi_wire_id(&mut rops, source_mapi_id);
    rops.extend_from_slice(&utf16z("Moved Projects"));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x35, 0x01, 0, 0, 0, 0, 0]));
    {
        let updated = updated_mailboxes.lock().unwrap();
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].mailbox_id, source_id);
        assert_eq!(updated[0].name.as_deref(), Some("Moved Projects"));
        assert_eq!(updated[0].parent_id, Some(Some(target_parent_id)));
    }

    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut hierarchy_headers = mapi_headers("Execute");
    hierarchy_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut hierarchy_rops = Vec::new();
    append_rop_open_folder(
        &mut hierarchy_rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut hierarchy_rops, 1, 2, 4096);
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &hierarchy_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;
    let hierarchy = strict_hierarchy_sync_transfer_from_response(&hierarchy_rops).unwrap();
    let moved = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Moved Projects")
        .expect("moved folder hierarchy row");
    assert_eq!(moved.parent_folder_id, Some(target_parent_mapi_id));
}

#[tokio::test]
async fn mapi_over_http_set_columns_rejects_invalid_microsoft_async_flags() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x02, // RopSetColumns with invalid SetColumnsFlags
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x12, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_create_set_save_message_imports_canonical_email() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "MAPI saved subject");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Body saved through MAPI");
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-save@example.test>",
    );
    let stream_body = utf16z("Body stream saved through MAPI");
    let html_stream_body = b"<p>HTML stream saved through MAPI</p>";

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream, create body stream
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[
        0x2F, 0x00, 0x03, // RopSetStreamSize
    ]);
    rops.extend_from_slice(&(stream_body.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x5E, 0x00, 0x03, // RopGetStreamSize
    ]);
    rops.extend_from_slice(&[
        0x2D, 0x00, 0x03, // RopWriteStream
    ]);
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(&stream_body);
    rops.extend_from_slice(&[
        0x5D, 0x00, 0x03, // RopCommitStream
    ]);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x04, // RopOpenStream, create HTML body stream
    ]);
    rops.extend_from_slice(&0x1013_0102u32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[
        0xA3, 0x00, 0x04, // RopWriteStreamExtended
    ]);
    rops.extend_from_slice(&(html_stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(html_stream_body);
    rops.extend_from_slice(&[
        0x5D, 0x00, 0x04, // RopCommitStream
    ]);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific on pending message
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
    ));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x06, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2B, 0x03, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x2F, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x5E, 0x03, 0, 0, 0, 0, stream_body.len() as u8, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2D, 0x03, 0, 0, 0, 0, stream_body.len() as u8, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x5D, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[
            0xA3,
            0x04,
            0,
            0,
            0,
            0,
            html_stream_body.len() as u8,
            0,
            0,
            0
        ]
    ));
    assert!(contains_bytes(response_rops, &[0x5D, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("MAPI saved subject")));
    assert!(contains_bytes(
        response_rops,
        &utf16z("Body stream saved through MAPI")
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));
    assert!(contains_bytes(
        response_rops,
        &test_mapi_message_id("99999999-9999-9999-9999-999999999999").to_le_bytes()
    ));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].source, "mapi-save-message");
    assert_eq!(recorded[0].from_address, "alice@example.test");
    assert_eq!(recorded[0].from_display.as_deref(), Some("Alice"));
    assert_eq!(recorded[0].subject, "MAPI saved subject");
    assert_eq!(recorded[0].body_text, "Body stream saved through MAPI");
    assert_eq!(
        recorded[0].body_html_sanitized.as_deref(),
        Some("<p>HTML stream saved through MAPI</p>")
    );
    assert_eq!(
        recorded[0].internet_message_id.as_deref(),
        Some("<mapi-save@example.test>")
    );
    assert!(recorded[0].to.is_empty());
    assert!(recorded[0].cc.is_empty());
    assert!(recorded[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email()
{
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "MS-OXCMSG 4.8 saved subject",
    );
    append_mapi_utf16_property(
        &mut property_values,
        0x1000_001F,
        "MS-OXCMSG 4.8 saved body",
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_save_changes_message_with_flags(&mut rops, 1, 2, 0x0A);
    append_rop_get_properties_specific(&mut rops, 2, &[0x0037_001F, 0x1000_001F]);
    rops.extend_from_slice(&[0x01, 0x00, 0x02]);
    append_rop_get_properties_specific(&mut rops, 2, &[0x0037_001F]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));
    assert!(contains_bytes(
        response_rops,
        &test_mapi_message_id("99999999-9999-9999-9999-999999999999").to_le_bytes()
    ));
    assert!(
        mapi_get_properties_specific_standard_row_offset(response_rops, 2).is_ok(),
        "KeepOpenReadWrite save must leave the message handle readable"
    );
    assert!(contains_bytes(
        response_rops,
        &utf16z("MS-OXCMSG 4.8 saved subject")
    ));
    assert!(contains_bytes(
        response_rops,
        &utf16z("MS-OXCMSG 4.8 saved body")
    ));
    assert!(
        contains_bytes(
            response_rops,
            &[0x07, 0x02, 0, 0, 0, 0, 1, 0x0A, 0x0F, 0x01, 0x04, 0x80]
        ),
        "response_rops={response_rops:02x?}"
    );

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].source, "mapi-save-message");
    assert_eq!(recorded[0].subject, "MS-OXCMSG 4.8 saved subject");
    assert_eq!(recorded[0].body_text, "MS-OXCMSG 4.8 saved body");
}

#[tokio::test]
async fn mapi_over_http_conversation_action_fai_persists_and_moves_existing_conversation() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let conversation_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
    let mut matching = FakeStore::email(
        "88888888-8888-4888-8888-888888888888",
        &inbox_id.to_string(),
        "inbox",
        "Conversation action target",
    );
    matching.thread_id = conversation_id;
    matching.received_at = "2026-05-22T12:00:00Z".to_string();
    let mut other = FakeStore::email(
        "99999999-9999-4999-8999-999999999999",
        &inbox_id.to_string(),
        "inbox",
        "Other conversation",
    );
    other.thread_id = Uuid::parse_str("99999999-9999-4999-8999-111111111111").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items"),
        ])),
        emails: Arc::new(Mutex::new(vec![matching.clone(), other])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let conversation_actions = store.conversation_actions.clone();
    let emails_state = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let target_folder_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        FakeStore::account().account_id,
        crate::mapi::identity::TRASH_FOLDER_ID,
    )
    .unwrap();
    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        0x0071_0102,
        &test_conversation_index(conversation_id),
    );
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Ignore thread");
    append_mapi_binary_property(&mut property_values, 0x85C6_0102, &target_folder_entry_id);
    append_mapi_i32_property(
        &mut property_values,
        0x85CB_0003,
        lpe_storage::CONVERSATION_ACTION_VERSION,
    );
    append_mapi_i32_property(&mut property_values, 0x85C9_0003, 1);
    append_mapi_multi_utf16_property(&mut property_values, 0x9000_101F, &["Red Category"]);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    );
    append_rop_create_associated_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 2, 6, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));

    let actions = conversation_actions.lock().unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].conversation_id, conversation_id);
    assert_eq!(actions[0].subject, "Ignore thread");
    assert_eq!(
        actions[0].move_folder_entry_id.as_deref(),
        Some(target_folder_entry_id.as_slice())
    );
    assert_eq!(actions[0].categories_json, "[\"Red Category\"]");
    drop(actions);

    let moved = moved_emails.lock().unwrap();
    assert_eq!(moved.as_slice(), &[(matching.id, trash_id)]);
    let emails = emails_state.lock().unwrap();
    let matching_after = emails.iter().find(|email| email.id == matching.id).unwrap();
    assert_eq!(matching_after.categories, vec!["Red Category".to_string()]);
}

#[tokio::test]
async fn mapi_over_http_conversation_action_fai_is_listed_and_openable() {
    let conversation_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
    let action = ConversationAction {
        id: conversation_id,
        conversation_id,
        subject: "Move thread".to_string(),
        categories_json: "[\"Blue Category\"]".to_string(),
        move_folder_entry_id: None,
        move_store_entry_id: None,
        move_target_mailbox_id: None,
        max_delivery_time: Some("2026-05-22T10:00:00Z".to_string()),
        last_applied_time: Some("2026-05-22T11:00:00Z".to_string()),
        version: lpe_storage::CONVERSATION_ACTION_VERSION,
        processed: 1,
        created_at: "2026-05-22T00:00:00Z".to_string(),
        updated_at: "2026-05-22T11:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        conversation_actions: Arc::new(Mutex::new(vec![action])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let message_id = test_mapi_message_id(&conversation_id.to_string());

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    );
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x02]); // associated contents table
    rops.extend_from_slice(&[0x15, 0x00, 0x02]); // RopQueryRows
    rops.extend_from_slice(&[0, 0, 1, 0]);
    rops.extend_from_slice(&[0x03, 0x00, 0x01, 0x03]); // RopOpenMessage
    rops.extend_from_slice(&0u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    );
    rops.push(0);
    append_mapi_wire_id(&mut rops, message_id);
    rops.extend_from_slice(&[0x07, 0x00, 0x03]); // GetPropertiesSpecific
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0071_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x85CA_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x85CB_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x9000_101Fu32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x05, 0x02, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.ConversationAction")
    ));
    assert!(contains_bytes(
        &response_rops,
        &test_conversation_index(conversation_id)
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Blue Category")));
    assert!(contains_bytes(
        &response_rops,
        &lpe_storage::CONVERSATION_ACTION_VERSION.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_modify_recipients_imports_pending_message_recipients() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "MAPI recipients");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Recipient body");

    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let cc_row = mapi_recipient_row("Carol", "carol@example.test", 0x02);
    let bcc_row = mapi_recipient_row("Hidden", "hidden@example.test", 0x03);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    for (row_id, recipient_type, row) in [
        (1u32, 0x01u8, to_row.as_slice()),
        (2u32, 0x02u8, cc_row.as_slice()),
        (3u32, 0x03u8, bcc_row.as_slice()),
    ] {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients from pending message
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Carol")));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
    assert_eq!(recorded[0].cc.len(), 1);
    assert_eq!(recorded[0].cc[0].address, "carol@example.test");
    assert_eq!(recorded[0].bcc.len(), 1);
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::png(), 0.8));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let microsoft_html = concat!(
        "<html>\r\n",
        "<head>\r\n",
        "<meta http-equiv=Content-Type content=\"text/html; charset=us-ascii\" />\r\n",
        "</head>\r\n",
        "<body lang=EN-US link=blue vlink=purple>\r\n",
        "<div>\r\n",
        "<p>This is a sample body text<o:p></o:p></p>\r\n",
        "<p><img width=174 height=152 id=\"Picture_x0020_2\"\r\n",
        "src=\"cid:image001.png@01C86E1C.F1954390\"\r\n",
        "alt=\"cid:image001.png@01C86E1C.F1954390\" /><o:p></o:p></p>\r\n",
        "</div>\r\n",
        "</body>\r\n",
        "</html>"
    );

    let mut message_properties = Vec::new();
    append_mapi_utf16_property(
        &mut message_properties,
        0x0037_001F,
        "MS-OXCMSG embedded image",
    );
    append_mapi_utf16_property(&mut message_properties, 0x1013_001F, microsoft_html);

    let mut attachment_properties = Vec::new();
    append_mapi_i32_property(&mut attachment_properties, 0x3705_0003, 1);
    append_mapi_i32_property(&mut attachment_properties, 0x370B_0003, -1);
    append_mapi_i32_property(&mut attachment_properties, 0x7FFD_0003, 0);
    append_mapi_utf16_property(&mut attachment_properties, 0x3001_001F, "image001.PNG");
    append_mapi_utf16_property(
        &mut attachment_properties,
        0x3712_001F,
        "image001.PNG@01C86E1C.F1954390",
    );
    append_mapi_utf16_property(&mut attachment_properties, 0x370E_001F, "image/PNG");
    append_mapi_i32_property(&mut attachment_properties, 0x7FFA_0003, 0);
    append_mapi_i32_property(&mut attachment_properties, 0x3714_0003, 4);
    append_mapi_bool_property(&mut attachment_properties, 0x7FFE_000B, true);
    append_mapi_utf16_property(&mut attachment_properties, 0x3707_001F, "image001.PNG");
    append_mapi_utf16_property(&mut attachment_properties, 0x3704_001F, "image001.PNG");
    append_mapi_utf16_property(&mut attachment_properties, 0x3703_001F, ".PNG");
    let image_bytes = b"\x89PNG-pending";

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x21, 0x00, 0x02, 0x03, 0x00]); // RopGetAttachmentTable
    rops.extend_from_slice(&[0x23, 0x00, 0x02, 0x04]); // RopCreateAttachment
    append_rop_set_properties(&mut rops, 4, 12, &attachment_properties);
    rops.extend_from_slice(&[0x2B, 0x00, 0x04, 0x05]); // RopOpenStream
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x2F, 0x00, 0x05]); // RopSetStreamSize
    rops.extend_from_slice(&(image_bytes.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[0x2D, 0x00, 0x05]); // RopWriteStream
    rops.extend_from_slice(&(image_bytes.len() as u16).to_le_bytes());
    rops.extend_from_slice(image_bytes);
    rops.extend_from_slice(&[0x5D, 0x00, 0x05]); // RopCommitStream
    rops.extend_from_slice(&[0x01, 0x00, 0x05]); // RopRelease stream handle
    rops.extend_from_slice(&[0x25, 0x00, 0x02, 0x04, 0x00]); // RopSaveChangesAttachment
    append_rop_set_properties(&mut rops, 2, 2, &message_properties);
    append_rop_save_changes_message(&mut rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x21, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x23, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x2B, 0x05, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x2F, 0x05, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x2D, 0x05, 0, 0, 0, 0, image_bytes.len() as u8, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x5D, 0x05, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));

    let imported = imported_emails.lock().unwrap();
    assert_eq!(imported.len(), 1);
    assert_eq!(imported[0].subject, "MS-OXCMSG embedded image");
    assert_eq!(
        imported[0].body_html_sanitized.as_deref(),
        Some(microsoft_html)
    );
    assert!(imported[0]
        .body_html_sanitized
        .as_deref()
        .unwrap()
        .contains("cid:image001.png@01C86E1C.F1954390"));
    assert_eq!(imported[0].attachments.len(), 1);
    assert_eq!(imported[0].attachments[0].file_name, "image001.PNG");
    assert_eq!(imported[0].attachments[0].media_type, "image/PNG");
    assert_eq!(
        imported[0].attachments[0].content_id.as_deref(),
        Some("image001.PNG@01C86E1C.F1954390")
    );
    assert_eq!(
        imported[0].attachments[0].disposition.as_deref(),
        Some("inline")
    );
    assert_eq!(imported[0].attachments[0].blob_bytes, image_bytes);
}

#[tokio::test]
async fn mapi_over_http_ipm_subtree_ost_identity_write_survives_store_failure() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        fail_mapi_ipm_subtree_ost_id_store: true,
        ..Default::default()
    };
    let stored_ost_id = store.mapi_ipm_subtree_ost_id.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let client_blob = [0x66; 40];
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x7C04_0102, &client_blob);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(4));
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on the IPM subtree
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on the same folder
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x7C04_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0x00, 0, 0, 0, 0, 0]
    ));
    let mut overridden = 40u16.to_le_bytes().to_vec();
    overridden.extend_from_slice(&client_blob);
    assert!(contains_bytes(&response_rops, &overridden));
    assert!(stored_ost_id.lock().unwrap().is_none());
}

#[tokio::test]
async fn mapi_over_http_sync_configure_returns_canonical_manifest_buffer() {
    let message_id = "40404040-4040-4040-4040-404040404040";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Sync manifest message",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(&response_rops, b"Sync manifest message"));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
}

#[tokio::test]
async fn mapi_over_http_content_sync_partial_item_uses_microsoft_full_item_fallback() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        "48484848-4848-4848-8848-484848484848",
        mailbox_id,
        "inbox",
        "Partial fallback subject",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x10, 0x00, 0x00, // content sync, PartialItem SendOptions
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].subject,
        "Partial fallback subject"
    );
    assert!(contains_bytes(
        &response_rops,
        &0x4012_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x4015_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x407B_0102u32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x407D_0003u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_content_sync_only_specified_properties_limits_message_properties() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        "70707070-7070-4070-8070-707070707070",
        mailbox_id,
        "inbox",
        "Only specified subject",
    );
    email.body_text = "Only specified body".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x80, 0x00, // content sync, OnlySpecifiedProperties
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x01, 0x00, // PropertyTagCount
    ]);
    rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    let message = &stream.message_changes[0];
    assert_eq!(message.subject, "Only specified subject");
    assert!(message.body_tags.contains(&PID_TAG_PARENT_SOURCE_KEY));
    assert!(message.body_tags.contains(&PID_TAG_SUBJECT_W));
    assert!(!message.body_tags.contains(&PID_TAG_MESSAGE_FLAGS));
    assert!(!message.body_tags.contains(&PID_TAG_FLAG_STATUS));
    assert!(!message.body_tags.contains(&PID_TAG_NORMALIZED_SUBJECT_A));
    assert!(!message.body_tags.contains(&PID_TAG_BODY_W));
}

#[tokio::test]
async fn mapi_over_http_content_sync_only_specified_body_returns_body_property() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        "72727272-7272-4272-8272-727272727272",
        mailbox_id,
        "inbox",
        "Only specified body subject",
    );
    email.body_text = "Only specified sync body".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x80, 0x00, // content sync, OnlySpecifiedProperties
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x01, 0x00, // PropertyTagCount
    ]);
    rops.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
    rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    let message = &stream.message_changes[0];
    assert_eq!(message.subject, "");
    assert!(message.body_tags.contains(&PID_TAG_PARENT_SOURCE_KEY));
    assert!(message.body_tags.contains(&PID_TAG_BODY_W));
    assert!(!message.body_tags.contains(&PID_TAG_SUBJECT_W));
    assert!(!message.body_tags.contains(&PID_TAG_MESSAGE_FLAGS));
    assert!(!message.body_tags.contains(&PID_TAG_FLAG_STATUS));
    assert!(!message.body_tags.contains(&PID_TAG_NORMALIZED_SUBJECT_A));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Only specified sync body")
    ));
}

#[tokio::test]
async fn mapi_over_http_content_sync_property_tags_exclude_message_properties_by_default() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        "71717171-7171-4171-8171-717171717171",
        mailbox_id,
        "inbox",
        "Excluded subject",
    );
    email.body_text = "Excluded sync body".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x02, 0x00, // PropertyTagCount
    ]);
    rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
    rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    let message = &stream.message_changes[0];
    assert!(message.body_tags.contains(&PID_TAG_PARENT_SOURCE_KEY));
    assert!(message.body_tags.contains(&PID_TAG_MESSAGE_FLAGS));
    assert!(message.body_tags.contains(&PID_TAG_FLAG_STATUS));
    assert!(message.body_tags.contains(&PID_TAG_NORMALIZED_SUBJECT_A));
    assert!(!message.body_tags.contains(&PID_TAG_SUBJECT_W));
    assert!(!message.body_tags.contains(&PID_TAG_BODY_W));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Excluded sync body")
    ));
}

#[tokio::test]
async fn mapi_over_http_common_views_sync_suppresses_lpe_search_definition_fai() {
    let account = FakeStore::account();
    let definition_id = Uuid::parse_str("73737373-7373-4373-8373-737373737373").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: definition_id,
            account_id: account.account_id,
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({
                "scope": "top_of_personal_folders",
                "recursive": true
            }),
            restriction_json: serde_json::json!({
                "kind": "exchange_reminders",
                "match": "reminder_set_or_recurring",
                "recurrenceHorizonDays": 90,
                "occurrenceDismissals": true
            }),
            excluded_folder_roles: exchange_reminder_excluded_folder_roles(),
            is_builtin: true,
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::COMMON_VIEWS_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid and CN
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.WunderBar.SFInfo")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &PID_TAG_SEARCH_FOLDER_DEFINITION.to_le_bytes()
    ));
    assert!(!contains_bytes(&response_rops, b"definitionKind"));
    assert!(!contains_bytes(&response_rops, b"restriction"));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.WunderBar.Link")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Shortcuts")));
}

#[tokio::test]
async fn mapi_over_http_common_views_observed_outlook_partial_sync_returns_no_sync_items() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 26,
        current_modseq: 26,
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x39, 0xA1, // content sync, observed Outlook flags 0xa139
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x09, 0x00, // PropertyTagCount
    ]);
    for tag in [
        0x1000_001F,
        0x1006_0003,
        0x1007_0003,
        0x1008_001F,
        0x1010_0003,
        0x1011_0003,
        0x3FF8_001F,
        0x3FF9_0102,
        0x300F_0102,
    ] {
        rops.extend_from_slice(&u32::to_le_bytes(tag));
    }
    for state_tag in [0x4017_0102u32, 0x6796_0102, 0x67DA_0102, 0x67D2_0102] {
        rops.extend_from_slice(&[0x75, 0x00, 0x02]); // RopSynchronizationUploadStateStreamBegin
        rops.extend_from_slice(&u32::to_le_bytes(state_tag));
        rops.extend_from_slice(&0u32.to_le_bytes());
        rops.extend_from_slice(&[0x77, 0x00, 0x02]); // RopSynchronizationUploadStateStreamEnd
    }
    rops.extend_from_slice(&[0x4E, 0x00, 0x02]); // RopFastTransferSourceGetBuffer
    rops.extend_from_slice(&31680u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.WunderBar.Link")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Shortcuts")));

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(
                    crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
                )
                .unwrap()
                .id,
            ),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .expect("Common Views content checkpoint");
    let current_changes = store.mapi_sync_changes.lock().unwrap().clone();
    assert_eq!(
        checkpoint.last_change_sequence,
        current_changes.current_change_sequence
    );
    assert_eq!(checkpoint.last_modseq, current_changes.current_modseq);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(|id| id.as_u64()),
        Some(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID)
    );
}

#[tokio::test]
async fn mapi_over_http_empty_root_adjacent_special_content_sync_uses_zero_length_state_sets() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };

    for folder_id in [
        crate::mapi::identity::SHORTCUTS_FOLDER_ID,
        crate::mapi::identity::VIEWS_FOLDER_ID,
    ] {
        let response_rops =
            content_sync_response_rops_for_store(store.clone(), folder_id, &[]).await;
        let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
        assert!(stream.message_changes.is_empty());
        assert!(stream.idset_given.is_empty());
        assert!(stream.cnset_seen.is_empty());
        assert!(stream.cnset_seen_fai.is_empty());
        assert!(stream.cnset_read.is_empty());
    }
}

#[tokio::test]
async fn mapi_over_http_common_views_create_associated_navigation_shortcut_persists() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    let shortcuts = store.navigation_shortcuts.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let inbox_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    )
    .unwrap();
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, PID_TAG_SUBJECT_W, "Persisted Inbox");
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_WLINK_ENTRY_ID,
        &inbox_entry_id,
    );
    append_mapi_i32_property(&mut property_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_binary_property(&mut property_values, PID_TAG_WLINK_ORDINAL, &[0x89]);
    append_mapi_guid_property(&mut property_values, PID_TAG_WLINK_GROUP_CLSID, [0x11; 16]);
    append_mapi_utf16_property(&mut property_values, PID_TAG_WLINK_GROUP_NAME_W, "Custom");

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 6, &property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x01, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let stored = shortcuts.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].subject, "Persisted Inbox");
    assert_eq!(
        stored[0].target_folder_id,
        Some(crate::mapi::identity::INBOX_FOLDER_ID)
    );
    assert_eq!(stored[0].shortcut_type, 0);
    assert_eq!(stored[0].ordinal, 0x89);
    assert_eq!(
        stored[0].group_header_id,
        Some(Uuid::from_bytes([0x11; 16]))
    );
    assert_eq!(stored[0].group_name, "Custom");
}

#[tokio::test]
async fn mapi_over_http_conversation_action_content_sync_exports_fai_rows() {
    let account = FakeStore::account();
    let conversation_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
    let action = ConversationAction {
        id: Uuid::parse_str("abababab-abab-4bab-8bab-abababababab").unwrap(),
        conversation_id,
        subject: "Sync conversation action".to_string(),
        categories_json: "[\"Sync Category\"]".to_string(),
        move_folder_entry_id: None,
        move_store_entry_id: None,
        move_target_mailbox_id: None,
        max_delivery_time: None,
        last_applied_time: None,
        version: lpe_storage::CONVERSATION_ACTION_VERSION,
        processed: 1,
        created_at: "2026-05-22T12:00:00Z".to_string(),
        updated_at: "2026-05-22T12:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(account),
        conversation_actions: Arc::new(Mutex::new(vec![action])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(
        &mut rops,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    );
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid and CN
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    let message = &stream.message_changes[0];
    assert!(message.associated);
    assert_eq!(message.subject, "Conv.Action: Sync conversation action");
    assert_eq!(
        message.parent_source_key,
        mapi_mailstore::source_key_for_store_id(
            crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        )
    );
    assert!(message.body_tags.contains(&0x0071_0102));
    assert!(message.body_tags.contains(&0x85CB_0003));
    assert!(message.body_tags.contains(&0x9000_101F));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.ConversationAction")
    ));
    assert!(contains_bytes(
        &response_rops,
        &test_conversation_index(conversation_id)
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Sync Category")));
}

#[tokio::test]
async fn mapi_over_http_conversation_action_content_sync_exports_deletes() {
    let action_id = Uuid::parse_str("abababab-abab-4bab-8bab-abababababac").unwrap();
    let conversation_action_checkpoint_id = mapi_mailstore::virtual_special_mailbox(
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    )
    .unwrap()
    .id;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(conversation_action_checkpoint_id),
            MapiCheckpointKind::Content,
            50,
            50,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 51,
        current_modseq: 51,
        deleted_conversation_action_ids: vec![action_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(
        store,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID >> 16,
        b"client-content-state",
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(contains_bytes(
        &response_rops,
        &mapi_deleted_message_idset_property(&[action_id])
    ));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(stream.deleted_idset.is_some());
}

#[tokio::test]
async fn mapi_over_http_associated_config_content_sync_exports_deletes() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555501").unwrap();
    let config_id = Uuid::parse_str("61616161-6161-4161-8161-616161616161").unwrap();
    let config_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 44,
    );
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 0;
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(config_id, config_object_id);
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            50,
            50,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 51,
        current_modseq: 51,
        deleted_associated_config_ids: vec![crate::store::MapiAssociatedConfigChange {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            config_id,
        }],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(stream.deleted_idset.is_some());
    assert!(strict_replid_globset_contains_counter(
        stream.deleted_idset.as_deref().unwrap(),
        &globcnt_bytes(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 44)
    )
    .unwrap());
}

#[tokio::test]
async fn mapi_over_http_associated_config_delete_does_not_allocate_identity() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555511").unwrap();
    let config_id = Uuid::parse_str("61616161-6161-4161-8161-616161616171").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 0;
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            50,
            50,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 51,
        current_modseq: 51,
        deleted_associated_config_ids: vec![crate::store::MapiAssociatedConfigChange {
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            config_id,
        }],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store.clone(), 5, b"client-content-state").await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(stream.deleted_idset.is_none());
    assert!(!store
        .mapi_identities
        .lock()
        .unwrap()
        .contains_key(&config_id));
}

#[tokio::test]
async fn mapi_over_http_content_sync_uses_mailbox_state_membership() {
    let inbox_id = Uuid::parse_str("97979797-9797-4797-9797-979797979797").unwrap();
    let sent_id = Uuid::parse_str("98989898-9898-4898-9898-989898989898").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let sent = FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent");
    let mut email = FakeStore::email(
        "99999999-9999-4999-9999-999999999999",
        &sent_id.to_string(),
        "sent",
        "Inbox membership sync",
    );
    email.mailbox_ids.push(inbox_id);
    email.mailbox_states.push(JmapEmailMailboxState {
        mailbox_id: inbox_id,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        modseq: 42,
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
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"Inbox membership sync"));
}

#[tokio::test]
async fn mapi_over_http_tell_version_accepts_fast_transfer_sync_context() {
    let message_id = "40404040-4040-4040-4040-404040404041";
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(message_id, mailbox_id, "inbox", "TellVersion sync message");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x86, 0x00, 0x02, // RopTellVersion
    ]);
    rops.extend_from_slice(&[15, 20, 0, 1, 0, 0]);
    rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x86, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"TellVersion sync message"));
}

#[tokio::test]
async fn mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let sent_id = "22222222-2222-2222-2222-222222222222";
    let mut inbox = FakeStore::mailbox(inbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut sent = FakeStore::mailbox(sent_id, "sent", "Sent");
    sent.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "41414141-4141-4141-4141-414141414141",
                inbox_id,
                "inbox",
                "Inbox scoped sync",
            ),
            FakeStore::email(
                "42424242-4242-4242-4242-424242424242",
                sent_id,
                "sent",
                "Sent scoped sync",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut content_rops = Vec::new();
    append_rop_open_folder(&mut content_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut content_rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let content_request = execute_body(&rop_buffer(&content_rops, &[1, u32::MAX, u32::MAX]));
    let content_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &content_request)
        .await
        .unwrap();
    let content_rops = response_rops_from_execute_response(content_response).await;

    assert_eq!(mapi_sync_manifest_counts(&content_rops), Some((0, 1)));
    assert!(contains_bytes(&content_rops, b"Inbox scoped sync"));
    assert!(!contains_bytes(&content_rops, b"Sent scoped sync"));

    let mut hierarchy_rops = Vec::new();
    append_rop_open_folder(&mut hierarchy_rops, 0, 1, test_mapi_folder_id(1));
    hierarchy_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x00, 0x00, 0x00, // hierarchy sync
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags, Eid
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    hierarchy_rops.extend_from_slice(&16384u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let hierarchy_request = execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX]));
    let hierarchy_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &hierarchy_request)
        .await
        .unwrap();
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;

    assert_eq!(mapi_sync_manifest_counts(&hierarchy_rops), Some((31, 0)));
    assert!(!contains_bytes(&hierarchy_rops, b"Inbox scoped sync"));
    assert!(!contains_bytes(&hierarchy_rops, b"Sent scoped sync"));
    for name in [
        "Quick Contacts",
        "IM Contact List",
        "Conversation Action Settings",
    ] {
        assert!(!contains_bytes(&hierarchy_rops, &utf16z(name)));
    }
}

#[tokio::test]
async fn mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let unchanged_id = Uuid::parse_str("41414141-4141-4141-4141-414141414141").unwrap();
    let changed_id = Uuid::parse_str("42424242-4242-4242-4242-424242424242").unwrap();
    let deleted_id = Uuid::parse_str("43434343-4343-4343-4343-434343434343").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &unchanged_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Checkpoint unchanged",
            ),
            FakeStore::email(
                &changed_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Checkpoint changed",
            ),
        ])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            10,
            4,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 12,
        current_modseq: 6,
        changed_message_ids: vec![changed_id],
        deleted_message_ids: vec![deleted_id],
        ..Default::default()
    };

    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer_with_state(&mut rops, 1, 2, 4096, b"client-content-state");
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Checkpoint changed"));
    assert!(!contains_bytes(&response_rops, b"Checkpoint unchanged"));
    assert!(contains_bytes(
        &response_rops,
        &0x4013_0003u32.to_le_bytes()
    ));
    let deleted_counter = test_mapi_message_id(&deleted_id.to_string()) >> 16;
    let mut deleted_idset = 1u16.to_le_bytes().to_vec();
    deleted_idset.push(0x52);
    deleted_idset.extend_from_slice(&globcnt_bytes(deleted_counter));
    deleted_idset.extend_from_slice(&globcnt_bytes(deleted_counter));
    deleted_idset.push(0);
    let mut deleted_property = 0x4018_0102u32.to_le_bytes().to_vec();
    deleted_property.extend_from_slice(&(deleted_idset.len() as u32).to_le_bytes());
    deleted_property.extend_from_slice(&deleted_idset);
    assert!(contains_bytes(&response_rops, &deleted_property));

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 12);
    assert_eq!(checkpoint.last_modseq, 6);

    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 12,
        current_modseq: 6,
        ..Default::default()
    };
    let restarted = ExchangeService::new(store.clone());
    let connect = restarted
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut restart_rops = Vec::new();
    append_rop_open_folder(&mut restart_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer_with_state(
        &mut restart_rops,
        1,
        2,
        4096,
        b"client-content-state",
    );
    let response = restarted
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&restart_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(!contains_bytes(&response_rops, b"Checkpoint changed"));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_content_sync_first_baseline_exports_all_current_messages() {
    let inbox_id = Uuid::parse_str("51515151-5151-5151-5151-515151515151").unwrap();
    let first_id = Uuid::parse_str("61616161-6161-6161-6161-616161616161").unwrap();
    let second_id = Uuid::parse_str("62626262-6262-6262-6262-626262626262").unwrap();
    let removed_id = Uuid::parse_str("63636363-6363-6363-6363-636363636363").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &first_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Baseline first",
            ),
            FakeStore::email(
                &second_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Baseline second",
            ),
        ])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 55,
        current_modseq: 41,
        changed_message_ids: vec![first_id],
        deleted_message_ids: vec![removed_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, &[]).await;

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops).map(|(_, messages)| messages),
        Some(2)
    );
    assert!(contains_bytes(&response_rops, b"Baseline first"));
    assert!(contains_bytes(&response_rops, b"Baseline second"));
    assert!(!contains_bytes(
        &response_rops,
        &META_TAG_IDSET_DELETED.to_le_bytes()
    ));
    assert_content_final_state_includes(&response_rops, &[first_id, second_id], &[41]);
}

#[tokio::test]
async fn mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes() {
    let inbox_id = Uuid::parse_str("51515151-5151-5151-5151-515151515152").unwrap();
    let first_id = Uuid::parse_str("61616161-6161-6161-6161-616161616162").unwrap();
    let second_id = Uuid::parse_str("62626262-6262-6262-6262-626262626263").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    inbox.unread_emails = 1;
    let mut first = FakeStore::email(
        &first_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Outlook first folder read",
    );
    first.unread = false;
    first.mailbox_states[0].unread = false;
    first.modseq = 48;
    first.mailbox_states[0].modseq = 48;
    let mut second = FakeStore::email(
        &second_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Outlook first folder unread",
    );
    second.unread = true;
    second.mailbox_states[0].unread = true;
    second.modseq = 49;
    second.mailbox_states[0].modseq = 49;
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![first, second])),
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, &[]).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();

    assert_eq!(stream.message_changes.len(), 2);
    assert!(stream.deleted_idset.is_none());
    assert!(stream.read_idset.is_some());
    assert!(stream.unread_idset.is_some());
    assert!(!stream.idset_given.is_empty());
    assert!(!stream.cnset_seen.is_empty());
    assert!(stream.cnset_seen_fai.is_empty());
    assert!(!stream.cnset_read.is_empty());
    assert!(strict_replid_globset_contains_counter(
        stream.read_idset.as_deref().unwrap(),
        &globcnt_bytes(mapi_message_global_counter(&first_id))
    )
    .unwrap());
    assert!(strict_replid_globset_contains_counter(
        stream.unread_idset.as_deref().unwrap(),
        &globcnt_bytes(mapi_message_global_counter(&second_id))
    )
    .unwrap());
    let inbox_source_key = mapi_mailstore::source_key_for_store_id(test_mapi_folder_id(5));
    for message in &stream.message_changes {
        assert_eq!(message.parent_source_key, inbox_source_key);
        assert!(message.mid.is_some());
        assert!(message.change_number.is_some());
        assert!(!message.associated);
        let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
            account.account_id,
            test_mapi_folder_id(5),
            message.mid.unwrap(),
        )
        .expect("message EntryID");
        assert_eq!(entry_id.len(), 70);
        assert!(contains_bytes(&response_rops, &entry_id));
        assert_eq!(
            message.predecessor_change_list,
            mapi_mailstore::predecessor_change_list(message.change_number.unwrap())
        );
    }
    assert!(stream
        .message_changes
        .iter()
        .any(|message| message.subject == "Outlook first folder read"));
    assert!(stream
        .message_changes
        .iter()
        .any(|message| message.subject == "Outlook first folder unread"));
}

#[tokio::test]
async fn mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding() {
    let inbox_id = Uuid::parse_str("52525252-5252-4525-9252-525252525201").unwrap();
    let message_id = Uuid::parse_str("65656565-6565-4565-9565-656565656501").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &inbox_id.to_string(),
            "inbox",
            "REPLGUID state encoding",
        )])),
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store.clone(), 5, &[]).await;
    let final_state = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    for value in [
        &final_state.idset_given,
        &final_state.cnset_seen,
        &final_state.cnset_read,
    ] {
        strict_validate_replguid_globset(value).unwrap();
        assert!(strict_validate_replid_globset(value).is_err());
    }
    assert!(final_state.cnset_seen_fai.is_empty());
    assert!(strict_replguid_globset_contains_counter(
        &final_state.idset_given,
        &globcnt_bytes(mapi_message_global_counter(&message_id))
    )
    .unwrap());

    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x28, 0x00, // content sync, ReadState | Normal
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(chunks.len(), 1);
    let checkpoint_state = strict_decode_content_sync_stream(&chunks[0].1).unwrap();
    for value in [
        &checkpoint_state.idset_given,
        &checkpoint_state.cnset_seen,
        &checkpoint_state.cnset_read,
    ] {
        strict_validate_replguid_globset(value).unwrap();
        assert!(strict_validate_replid_globset(value).is_err());
    }
    assert!(checkpoint_state.cnset_seen_fai.is_empty());
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape() {
    let inbox_id = Uuid::parse_str("52525252-5252-4525-9252-525252525204").unwrap();
    let message_id = Uuid::parse_str("65656565-6565-4565-9565-656565656504").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-4bab-8bab-ababababab04").unwrap();
    let deleted_id = Uuid::parse_str("67676767-6767-4767-9767-676767676704").unwrap();
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "MS-OXCFXICS 4.5 stream",
    );
    email.has_attachments = true;
    email.unread = false;
    email.mailbox_states[0].unread = false;
    email.to.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: "Embedded child.msg".to_string(),
                media_type: "application/vnd.ms-outlook".to_string(),
                disposition: None,
                content_id: None,
                size_octets: 91,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: "Embedded child.msg".to_string(),
                media_type: "application/vnd.ms-outlook".to_string(),
                blob_bytes: b"LPE-MAPI-EMBEDDED-MESSAGE\0Subject:Embedded 4.5 child\r\nBody-Length:14\r\nEmbedded body.\r\nHtml-Length:0\r\n".to_vec(),
            },
        )]))),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            60,
            60,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 61,
        current_modseq: 61,
        changed_message_ids: vec![message_id],
        deleted_message_ids: vec![deleted_id],
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, // content sync
        0x15, // SendOptions: Unicode | RecoverMode | PartialItem
    ]);
    rops.extend_from_slice(&0xA139u16.to_le_bytes()); // Unicode | ReadState | FAI | Normal | NoForeignIdentifiers | BestBody | Progress
    rops.extend_from_slice(&0u16.to_le_bytes()); // RestrictionDataSize
    rops.extend_from_slice(&0x0Du32.to_le_bytes()); // SynchronizationExtraFlags: Eid | CN | MessageSize
    rops.extend_from_slice(&0u16.to_le_bytes()); // PropertyTagCount
    rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    rops.extend_from_slice(&(b"client-content-state".len() as u32).to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(b"client-content-state".len() as u32).to_le_bytes());
    rops.extend_from_slice(b"client-content-state");
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    rops.extend_from_slice(&(b"client-content-state".len() as u32).to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(b"client-content-state".len() as u32).to_le_bytes());
    rops.extend_from_slice(b"client-content-state");
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(stream.message_changes[0].subject, "MS-OXCFXICS 4.5 stream");
    assert!(stream.deleted_idset.is_some());
    assert!(stream.read_idset.is_some());
    assert!(contains_bytes(
        &response_rops,
        &mapi_deleted_message_idset_property(&[deleted_id])
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_read_message_idset_property(&[message_id])
    ));
    assert_mapi_fast_transfer_marker_sequence(
        &mapi_fast_transfer_chunks(&response_rops)[0].1,
        &[
            FX_INCR_SYNC_PROGRESS_MODE,
            FX_INCR_SYNC_PROGRESS_PER_MSG,
            FX_INCR_SYNC_CHG,
            FX_INCR_SYNC_MESSAGE,
            FX_START_RECIP,
            FX_END_TO_RECIP,
            FX_NEW_ATTACH,
            FX_START_EMBED,
            FX_END_EMBED,
            FX_END_ATTACH,
            FX_INCR_SYNC_DEL,
            FX_INCR_SYNC_READ,
            FX_INCR_SYNC_STATE_BEGIN,
            FX_INCR_SYNC_STATE_END,
            FX_INCR_SYNC_END,
        ],
    );
}

#[tokio::test]
async fn mapi_over_http_content_sync_incremental_after_client_state_exports_delta() {
    let inbox_id = Uuid::parse_str("52525252-5252-5252-5252-525252525252").unwrap();
    let unchanged_id = Uuid::parse_str("64646464-6464-6464-6464-646464646464").unwrap();
    let changed_id = Uuid::parse_str("65656565-6565-6565-6565-656565656565").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &unchanged_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Incremental unchanged",
            ),
            FakeStore::email(
                &changed_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Incremental changed",
            ),
        ])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            20,
            40,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 21,
        current_modseq: 41,
        changed_message_ids: vec![changed_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Incremental changed"));
    assert!(!contains_bytes(&response_rops, b"Incremental unchanged"));
    assert_content_final_state_includes(&response_rops, &[unchanged_id, changed_id], &[41]);
}

#[tokio::test]
async fn mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change(
) {
    let inbox_id = Uuid::parse_str("53535353-5353-5353-5353-535353535353").unwrap();
    let archive_id = Uuid::parse_str("54545454-5454-5454-5454-545454545454").unwrap();
    let moved_id = Uuid::parse_str("66666666-6666-6666-8666-666666666666").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let mut archive = FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive");
    archive.total_emails = 1;
    let moved = FakeStore::email(
        &moved_id.to_string(),
        &archive_id.to_string(),
        "archive",
        "Moved canonical message",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        emails: Arc::new(Mutex::new(vec![moved])),
        ..Default::default()
    };
    for mailbox_id in [inbox_id, archive_id] {
        store
            .store_mapi_sync_checkpoint(
                FakeStore::account().account_id,
                Some(mailbox_id),
                MapiCheckpointKind::Content,
                30,
                40,
                serde_json::json!({"source": "previous-run"}),
            )
            .await
            .unwrap();
    }
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 31,
        current_modseq: 41,
        changed_message_ids: vec![moved_id],
        deleted_message_ids: vec![moved_id],
        ..Default::default()
    };

    let source_rops = content_sync_response_rops(store.clone(), 5, b"client-content-state").await;
    let target_rops = content_sync_response_rops(
        store.clone(),
        crate::mapi::identity::ARCHIVE_FOLDER_ID >> 16,
        b"client-content-state",
    )
    .await;
    let moved_counter = store
        .mapi_identities
        .lock()
        .unwrap()
        .get(&moved_id)
        .and_then(|object_id| crate::mapi::identity::global_counter_from_store_id(*object_id))
        .unwrap();

    assert_eq!(mapi_sync_manifest_counts(&source_rops), None);
    assert!(contains_bytes(
        &source_rops,
        &mapi_deleted_message_idset_property(&[moved_id])
    ));
    assert_content_final_state_includes(&source_rops, &[], &[]);
    assert_eq!(mapi_sync_manifest_counts(&target_rops), Some((0, 1)));
    assert!(contains_bytes(&target_rops, b"Moved canonical message"));
    assert_content_final_state_includes_counters(&target_rops, &[moved_counter], &[41]);
}

#[tokio::test]
async fn mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state() {
    let inbox_id = Uuid::parse_str("56565656-5656-5656-5656-565656565656").unwrap();
    let deleted_id = Uuid::parse_str("67676767-6767-6767-6767-676767676767").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(Vec::new())),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            40,
            40,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 41,
        current_modseq: 41,
        deleted_message_ids: vec![deleted_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(contains_bytes(
        &response_rops,
        &mapi_deleted_message_idset_property(&[deleted_id])
    ));
    assert_content_final_state_includes(&response_rops, &[], &[]);
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(stream.deleted_idset.is_some());
    assert!(strict_replid_globset_contains_counter(
        stream.deleted_idset.as_deref().unwrap(),
        &globcnt_bytes(mapi_message_global_counter(&deleted_id))
    )
    .unwrap());
}

#[tokio::test]
async fn mapi_over_http_content_sync_read_flag_update_exports_read_state() {
    let inbox_id = Uuid::parse_str("57575757-5757-5757-5757-575757575757").unwrap();
    let message_id = Uuid::parse_str("68686868-6868-6868-6868-686868686868").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Read flag canonical update",
    );
    email.unread = false;
    email.mailbox_states[0].unread = false;
    email.modseq = 47;
    email.mailbox_states[0].modseq = 47;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            46,
            46,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 47,
        current_modseq: 47,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(
        &response_rops,
        &0x402F_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_read_message_idset_property(&[message_id])
    ));
    assert!(contains_bytes(
        &response_rops,
        b"Read flag canonical update"
    ));
    assert_content_final_state_includes(&response_rops, &[message_id], &[47]);
    assert!(contains_bytes(
        &response_rops,
        &mapi_message_cnset_property(META_TAG_CNSET_READ, &[47])
    ));
}

#[tokio::test]
async fn mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc() {
    let inbox_id = Uuid::parse_str("58585858-5858-5858-5858-585858585858").unwrap();
    let message_id = Uuid::parse_str("69696969-6969-6969-6969-696969696969").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Protected Bcc sync",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden Bcc".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            50,
            40,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 51,
        current_modseq: 41,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Protected Bcc sync"));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
    assert!(!contains_bytes(&response_rops, b"Hidden Bcc"));
    assert_content_final_state_includes(&response_rops, &[message_id], &[41]);
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc() {
    let message_uuid = Uuid::parse_str("43434343-4343-4343-4343-434343434343").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Attachment sync message",
    );
    email.has_attachments = true;
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: None,
    });
    let file_reference = format!("attachment:{message_uuid}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: message_uuid,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: None,
                content_id: None,
                size_octets: 42,
                file_reference: file_reference.clone(),
            }],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Attachment sync message"));
    assert!(contains_bytes(&response_rops, &utf16z("brief.pdf")));
    assert!(contains_bytes(&response_rops, &utf16z("application/pdf")));
    assert!(!contains_bytes(&response_rops, file_reference.as_bytes()));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc() {
    let message_uuid = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Recipient sync message",
    );
    email.to.push(JmapEmailAddress {
        address: "to@example.test".to_string(),
        display_name: Some("Visible To".to_string()),
    });
    email.cc.push(JmapEmailAddress {
        address: "cc@example.test".to_string(),
        display_name: Some("Visible Cc".to_string()),
    });
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden Bcc".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Recipient sync message"));
    assert!(contains_bytes(&response_rops, &utf16z("to@example.test")));
    assert!(contains_bytes(&response_rops, &utf16z("Visible To")));
    assert!(contains_bytes(&response_rops, &utf16z("cc@example.test")));
    assert!(contains_bytes(&response_rops, &utf16z("Visible Cc")));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
    assert!(!contains_bytes(&response_rops, b"Hidden Bcc"));
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_canonical_read_flag_state() {
    let message_uuid = Uuid::parse_str("45454545-4545-4545-4545-454545454545").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Read flag sync message",
    );
    email.unread = false;
    email.flagged = true;
    email.has_attachments = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert_eq!(
        mapi_sync_manifest_message_state(&response_rops, "Read flag sync message"),
        Some((0x0000_0013, 2))
    );
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc() {
    let message_uuid = Uuid::parse_str("46464646-4646-4646-4646-464646464646").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Change key sync message",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden Bcc".to_string()),
    });
    let change_number = mapi_mailstore::canonical_message_change_number(&email);
    let change_key = mapi_mailstore::change_key_for_change_number(change_number);
    let predecessor_change_list = mapi_mailstore::predecessor_change_list(change_number);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Change key sync message"));
    assert!(contains_bytes(&response_rops, &change_key));
    assert!(contains_bytes(&response_rops, &predecessor_change_list));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
    assert!(!contains_bytes(&response_rops, b"Hidden Bcc"));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(inbox_id, "inbox", "Inbox");
    inbox.total_emails = 3;
    inbox.unread_emails = 1;
    let change_number = crate::mapi::identity::INBOX_FOLDER_COUNTER;
    let change_key = mapi_mailstore::change_key_for_change_number(change_number);
    let predecessor_change_list = mapi_mailstore::predecessor_change_list(change_number);
    let email = FakeStore::email(
        "57575757-5757-5757-5757-575757575757",
        inbox_id,
        "inbox",
        "Hierarchy aggregate message",
    );
    let message_change_number = mapi_mailstore::canonical_message_change_number(&email);
    assert_ne!(change_number, message_change_number);
    let local_commit_time_max = mapi_mailstore::filetime_from_change_number(message_change_number);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x00, 0x00, 0x00, // hierarchy sync
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags, Eid
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&8192u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );
    assert!(contains_bytes(&response_rops, &change_key));
    assert!(contains_bytes(&response_rops, &predecessor_change_list));
    let mut local_commit_time_property = 0x670A_0040u32.to_le_bytes().to_vec();
    local_commit_time_property.extend_from_slice(&(local_commit_time_max as i64).to_le_bytes());
    assert!(contains_bytes(&response_rops, &local_commit_time_property));
    assert!(contains_bytes(
        &response_rops,
        &0x670B_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x67A4_0014u32.to_le_bytes()
    ));
    let mut folder_type_property = 0x3601_0003u32.to_le_bytes().to_vec();
    folder_type_property.extend_from_slice(&1i32.to_le_bytes());
    assert!(contains_bytes(&response_rops, &folder_type_property));
    let final_cnset_seen = mapi_last_binary_property(&response_rops, 0x6796_0102).unwrap();
    assert!(strict_replguid_globset_contains_counter(
        final_cnset_seen,
        &globcnt_bytes(change_number)
    )
    .unwrap());
    assert!(!strict_replguid_globset_contains_counter(
        final_cnset_seen,
        &globcnt_bytes(message_change_number)
    )
    .unwrap());
}

#[tokio::test]
async fn mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        fail_query_jmap_email_ids: true,
        ..Default::default()
    };
    let queried_jmap_email_ids = store.queried_jmap_email_ids.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(queried_jmap_email_ids.load(Ordering::SeqCst), 0);
    let response_rops = response_rops_from_execute_response(response).await;
    let get_buffer_response_offset = response_rops
        .windows(6)
        .position(|window| window == [0x4E, 0x02, 0x00, 0x00, 0x00, 0x00])
        .unwrap();
    assert_eq!(
        u16::from_le_bytes(
            response_rops[get_buffer_response_offset + 6..get_buffer_response_offset + 8]
                .try_into()
                .unwrap()
        ),
        0x0003
    );
    assert_eq!(
        u16::from_le_bytes(
            response_rops[get_buffer_response_offset + 8..get_buffer_response_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        u16::from_le_bytes(
            response_rops[get_buffer_response_offset + 10..get_buffer_response_offset + 12]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(response_rops[get_buffer_response_offset + 12], 0);
    let transfer_buffer_size = u16::from_le_bytes(
        response_rops[get_buffer_response_offset + 13..get_buffer_response_offset + 15]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(transfer_buffer_size > 0);
    assert!(response_rops.len() >= get_buffer_response_offset + 15 + transfer_buffer_size);

    assert!(contains_bytes(
        &response_rops,
        &0x4012_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(
        &response_rops,
        &0x6749_0014u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_wire_id_bytes(test_mapi_folder_id(4))
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x3008_0040u32.to_le_bytes()
    ));
    let mut empty_local_commit_time_property = 0x670A_0040u32.to_le_bytes().to_vec();
    empty_local_commit_time_property.extend_from_slice(&0i64.to_le_bytes());
    assert!(!contains_bytes(
        &response_rops,
        &empty_local_commit_time_property
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x3613_001Fu32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x001A_001Fu32.to_le_bytes()
    ));
    let tag_position = |tag: u32| {
        let tag_bytes = tag.to_le_bytes();
        response_rops
            .windows(tag_bytes.len())
            .position(|window| window == tag_bytes)
            .unwrap()
    };
    assert!(
        tag_position(0x65E1_0102) < tag_position(0x65E0_0102)
            && tag_position(0x65E0_0102) < tag_position(0x3008_0040)
            && tag_position(0x3008_0040) < tag_position(0x65E2_0102)
            && tag_position(0x65E2_0102) < tag_position(0x65E3_0102)
            && tag_position(0x65E3_0102) < tag_position(0x3001_001F)
            && tag_position(0x3001_001F) < tag_position(0x6749_0014)
            && tag_position(0x6749_0014) < tag_position(0x3613_001F)
    );
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert_eq!(
        decoded
            .folder_changes
            .first()
            .map(|folder| folder.display_name.as_str()),
        Some("Top of Information Store")
    );
    assert_eq!(
        decoded
            .folder_changes
            .first()
            .and_then(|folder| folder.folder_id),
        Some(test_mapi_folder_id(4))
    );
    assert_eq!(
        decoded
            .folder_changes
            .first()
            .map(|folder| folder.parent_source_key.as_slice()),
        Some(mapi_mailstore::source_key_for_store_id(test_mapi_folder_id(1)).as_slice())
    );
    assert!(decoded
        .folder_changes
        .iter()
        .all(|folder| folder.folder_id.is_some()));
    assert!(decoded.folder_changes.iter().all(|folder| {
        let expected_parent = match folder.display_name.as_str() {
            "Top of Information Store" => test_mapi_folder_id(1),
            "Conflicts" | "Local Failures" | "Server Failures" => test_mapi_folder_id(26),
            _ => test_mapi_folder_id(4),
        };
        folder.parent_folder_id == Some(expected_parent)
    }));
    for tag in [0x0E08_0003u32, 0x3FE0_0102, 0x3FE1_0102, 0x0E27_0102] {
        assert!(!contains_bytes(&response_rops, &tag.to_le_bytes()));
    }
    assert!(contains_bytes(
        &response_rops,
        &0x65E1_0102u32.to_le_bytes()
    ));
    let parent_source_key =
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
    let mut ipm_child_parent_source_key = 0x65E1_0102u32.to_le_bytes().to_vec();
    ipm_child_parent_source_key.extend_from_slice(&(parent_source_key.len() as u32).to_le_bytes());
    ipm_child_parent_source_key.extend_from_slice(&parent_source_key);
    assert!(contains_bytes(&response_rops, &ipm_child_parent_source_key));
    let source_key_offset = tag_position(0x65E0_0102);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[source_key_offset + 4..source_key_offset + 8]
                .try_into()
                .unwrap()
        ),
        22
    );
    let change_key_offset = tag_position(0x65E2_0102);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[change_key_offset + 4..change_key_offset + 8]
                .try_into()
                .unwrap()
        ),
        22
    );
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Note")));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(&response_rops, &utf16z("Drafts")));
    assert!(contains_bytes(&response_rops, &utf16z("Outbox")));
    assert!(contains_bytes(&response_rops, &utf16z("Sent Items")));
    assert!(contains_bytes(&response_rops, &utf16z("Deleted Items")));
    assert!(contains_bytes(&response_rops, &utf16z("Contacts")));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("Journal")));
    assert!(contains_bytes(&response_rops, &utf16z("Notes")));
    assert!(contains_bytes(&response_rops, &utf16z("Tasks")));
    assert!(!contains_bytes(&response_rops, &utf16z("Reminders")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Conversation History")
    ));
    let mut folder_offsets = Vec::new();
    for name in [
        "Inbox",
        "Drafts",
        "Outbox",
        "Sent Items",
        "Deleted Items",
        "Contacts",
        "Calendar",
        "Journal",
        "Notes",
        "Tasks",
    ] {
        let name_bytes = utf16z(name);
        folder_offsets.push(
            response_rops
                .windows(name_bytes.len())
                .position(|window| window == name_bytes.as_slice())
                .unwrap(),
        );
    }
    assert!(folder_offsets.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Contact")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(
        &response_rops,
        &0x36E5_001Fu32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Journal")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.StickyNote")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Task")));
    assert!(!contains_bytes(&response_rops, &utf16z("Outlook.Reminder")));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::OUTBOX_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::DRAFTS_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::TRASH_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CONTACTS_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::JOURNAL_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::NOTES_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::TASKS_FOLDER_ID)
    ));
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    for folder_id in [
        crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
        crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
    ] {
        let counter = crate::mapi::identity::global_counter_from_store_id(folder_id)
            .expect("stable folder counter");
        assert!(
            !strict_replguid_globset_contains_counter(&decoded.idset_given, &globcnt_bytes(counter))
                .expect("hierarchy final IDSET"),
            "final hierarchy state should not acknowledge non-hierarchy special folder 0x{folder_id:016x}"
        );
    }
    let sync_issues = decoded
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Sync Issues")
        .expect("Sync Issues folderChange");
    assert_eq!(
        sync_issues.parent_source_key,
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    let sync_issues_source_key = sync_issues.source_key.clone();
    for name in ["Conflicts", "Local Failures", "Server Failures"] {
        let folder = decoded
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("{name} folderChange"));
        assert_eq!(folder.parent_source_key, sync_issues_source_key);
    }
}

#[test]
fn mapi_hierarchy_sync_projects_outlook_special_folder_display_names() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "INBOX");
    let sent = FakeStore::mailbox("66666666-6666-4666-8666-666666666666", "sent", "Sent");
    let trash = FakeStore::mailbox("77777777-7777-4777-8777-777777777777", "trash", "Trash");
    let mailboxes = vec![inbox, sent, trash];
    let buffer = mapi_mailstore::sync_manifest_buffer_with_final_state(
        Uuid::nil(),
        0x02,
        0x0101,
        0,
        &[
            PID_TAG_FOLDER_TYPE,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            0x0E08_0003,
            0x0FF4_0003,
            0x3FE0_0102,
            0x3FE1_0102,
            0x0E27_0102,
        ],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        &mailboxes,
        &[],
        &[],
        &[],
        &mailboxes,
        &mailboxes,
        &[],
        &[],
        &[],
        &[],
        1,
    );
    let decoded = strict_decode_hierarchy_sync_stream(&buffer).expect("strict hierarchy ICS");

    let names = decoded
        .folder_changes
        .iter()
        .map(|folder| folder.display_name.as_str())
        .collect::<Vec<_>>();
    assert!(names.contains(&"Inbox"));
    assert!(names.contains(&"Sent Items"));
    assert!(names.contains(&"Deleted Items"));
    assert!(!names.contains(&"INBOX"));
    assert!(!names.contains(&"Trash"));
}

#[tokio::test]
async fn mapi_over_http_real_conversation_history_mailbox_stays_out_of_startup_hierarchy_sync() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox(
                "73737373-7373-4373-8373-737373737373",
                "conversation_history",
                "Conversation History",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let conversation_history = decoded
        .folder_changes
        .iter()
        .filter(|folder| folder.display_name == "Conversation History")
        .collect::<Vec<_>>();

    assert!(conversation_history.is_empty());
}

#[tokio::test]
async fn mapi_over_http_default_folder_probe_after_hierarchy_sync_succeeds() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut hierarchy_rops = Vec::new();
    append_rop_open_folder(&mut hierarchy_rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut hierarchy_rops, 1, 2, 4096);
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&hierarchy_response);
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;
    assert_eq!(
        mapi_sync_manifest_counts(&hierarchy_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());

    let mut probe_rops = vec![0xFE, 0x00, 0x00, 0x01];
    probe_rops.extend_from_slice(&0u32.to_le_bytes());
    probe_rops.extend_from_slice(&0u32.to_le_bytes());
    probe_rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(
        &mut probe_rops,
        0,
        &[0x36D0_0102, 0x36D1_0102, 0x36D4_0102, 0x36D5_0102],
    );
    append_rop_open_folder(
        &mut probe_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_get_properties_specific(&mut probe_rops, 1, &[0x3001_001F, 0x3613_001F]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&probe_rops, &[u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x07, 0x00, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(!contains_bytes(&response_rops, &utf16z("Reminders")));
}

#[tokio::test]
async fn mapi_over_http_root_hierarchy_sync_keeps_parent_keys_root_relative() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(1));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 20000);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let top = decoded
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Top of Information Store")
        .expect("IPM subtree folderChange");
    assert!(top.parent_source_key.is_empty());
    for name in [
        "Deferred Action",
        "Spooler Queue",
        "Common Views",
        "Schedule",
        "Search",
        "Personal Views",
    ] {
        let folder = decoded
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("{name} folderChange"));
        assert!(folder.parent_source_key.is_empty());
        assert_eq!(folder.container_class.as_deref(), Some(""));
    }
    let shortcuts = decoded
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Shortcuts")
        .expect("Shortcuts folderChange");
    assert!(shortcuts.parent_source_key.is_empty());
    assert_eq!(
        shortcuts.container_class.as_deref(),
        Some("IPF.ShortcutFolder")
    );
    let ipm_source_key = mapi_mailstore::source_key_for_store_id(test_mapi_folder_id(4));
    for name in ["Inbox", "Outbox", "Sent Items", "Deleted Items"] {
        let folder = decoded
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("{name} folderChange"));
        assert_eq!(folder.parent_source_key, ipm_source_key);
    }
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys() {
    let parent_id = Uuid::parse_str("90909090-9090-4090-9090-909090909090").unwrap();
    let child_id = Uuid::parse_str("91919191-9191-4191-9191-919191919191").unwrap();
    let parent = FakeStore::mailbox(&parent_id.to_string(), "custom", "Projects");
    let mut child = FakeStore::mailbox(&child_id.to_string(), "custom", "Archive");
    child.parent_id = Some(parent_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![parent.clone(), child.clone()])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT + 2, 0))
    );
    assert!(contains_bytes(&response_rops, &utf16z("Projects")));
    assert!(contains_bytes(&response_rops, &utf16z("Archive")));
    let mut child_parent_source_key = 0x65E1_0102u32.to_le_bytes().to_vec();
    let parent_source_key = mapi_mailstore::source_key_for_mailbox_folder(&parent);
    child_parent_source_key.extend_from_slice(&(parent_source_key.len() as u32).to_le_bytes());
    child_parent_source_key.extend_from_slice(&parent_source_key);
    assert!(contains_bytes(&response_rops, &child_parent_source_key));
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert!(decoded.folder_changes.iter().any(|folder| {
        folder.display_name == "Archive" && folder.parent_source_key == parent_source_key
    }));

    let parent_folder_id = crate::mapi::identity::mapped_mapi_object_id(&parent_id).unwrap();
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut child_scope_headers = mapi_headers("Execute");
    child_scope_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut child_scope_rops = Vec::new();
    append_rop_open_folder(&mut child_scope_rops, 0, 1, parent_folder_id);
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut child_scope_rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &child_scope_headers,
            &execute_body(&rop_buffer(&child_scope_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((1, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Archive")));
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert_eq!(decoded.folder_changes.len(), 1);
    assert_eq!(decoded.folder_changes[0].display_name, "Archive");
    assert!(decoded.folder_changes[0].parent_source_key.is_empty());
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_fast_transfer_stream_decodes_strictly() {
    let parent_id = Uuid::parse_str("92929292-9292-4292-9292-929292929292").unwrap();
    let child_id = Uuid::parse_str("93939393-9393-4393-9393-939393939393").unwrap();
    let parent = FakeStore::mailbox(&parent_id.to_string(), "custom", "Projects");
    let mut child = FakeStore::mailbox(&child_id.to_string(), "custom", "Archive");
    child.parent_id = Some(parent_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![parent, child])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert_eq!(
        decoded.folder_changes.len(),
        OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT as usize + 2
    );
    let names = decoded
        .folder_changes
        .iter()
        .map(|folder| folder.display_name.as_str())
        .collect::<Vec<_>>();
    let projects = names
        .iter()
        .position(|name| *name == "Projects")
        .expect("Projects folderChange");
    let archive = decoded
        .folder_changes
        .iter()
        .position(|folder| {
            folder.display_name == "Archive"
                && folder.parent_source_key == decoded.folder_changes[projects].source_key
        })
        .expect("custom Archive folderChange");
    assert_eq!(
        decoded.folder_changes[projects].parent_source_key,
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert!(decoded.folder_changes[archive]
        .parent_source_key
        .eq(&decoded.folder_changes[projects].source_key));
    assert!(decoded
        .idset_given
        .starts_with(&mapi_mailstore::STORE_REPLICA_GUID));
    assert!(decoded
        .cnset_seen
        .starts_with(&mapi_mailstore::STORE_REPLICA_GUID));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_includes_content_activity_properties() {
    let inbox_id = Uuid::parse_str("94949494-9494-4494-9494-949494949494").unwrap();
    let sent_id = Uuid::parse_str("96969696-9696-4696-9696-969696969696").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let sent = FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent");
    let mut email = FakeStore::email(
        "95959595-9595-4595-9595-959595959595",
        &sent_id.to_string(),
        "sent",
        "Unread hierarchy count",
    );
    email.mailbox_ids.push(inbox_id);
    email.mailbox_states.push(JmapEmailMailboxState {
        mailbox_id: inbox_id,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        modseq: 42,
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
        draft: false,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x09, 0x01, 0x01, // hierarchy sync, Unicode + NoForeignIdentifiers
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x08, 0x00, // PropertyTagCount
        0x03, 0x00, 0x01, 0x36, // PidTagFolderType
        0x03, 0x00, 0x02, 0x36, // PidTagContentCount
        0x03, 0x00, 0x03, 0x36, // PidTagContentUnreadCount
        0x03, 0x00, 0x08, 0x0e, // PidTagMessageSize
        0x03, 0x00, 0xf4, 0x0f, // PidTagAccess
        0x02, 0x01, 0xe0, 0x3f, // PidTagMappingSignature
        0x02, 0x01, 0xe1, 0x3f, // PidTagRecordKey
        0x02, 0x01, 0x27, 0x0e, // PidTagOrdinalMost
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&8192u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let inbox = decoded
        .folder_changes
        .iter()
        .find(|folder| folder.display_name.eq_ignore_ascii_case("inbox"))
        .expect("Inbox folderChange");
    assert_eq!(
        inbox.folder_id,
        Some(crate::mapi::identity::INBOX_FOLDER_ID)
    );
    assert_eq!(
        inbox.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(inbox.content_count, None);
    assert_eq!(inbox.content_unread_count, None);
    assert!(inbox.local_commit_time_max.unwrap_or_default() > 0);
    assert_eq!(inbox.deleted_count_total, Some(0));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_manifest_ignores_stale_server_checkpoint() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 3;
    inbox.unread_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            None,
            MapiCheckpointKind::Hierarchy,
            99,
            9,
            serde_json::json!({"source": "emsmdb-ics-download"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 99,
        current_modseq: 9,
        ..Default::default()
    };

    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x00, 0x00, 0x00, // hierarchy sync
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags, Eid
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&8192u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            None,
            MapiCheckpointKind::Hierarchy,
            42,
            7,
            serde_json::json!({
                "source": "emsmdb-ics-download",
                "syncType": 2,
                "syncRootFolderId": test_mapi_folder_id(1),
                "hierarchySyncVersion": 2
            }),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 42,
        current_modseq: 7,
        ..Default::default()
    };

    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        &mut rops,
        1,
        2,
        4096,
        b"client-hierarchy-state",
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert!(decoded
        .folder_changes
        .iter()
        .any(|folder| folder.display_name == "Inbox"));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_checkpoint_resumes_after_completed_download() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 3;
    inbox.unread_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 42,
        current_modseq: 7,
        ..Default::default()
    };

    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            None,
            MapiCheckpointKind::Hierarchy,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 42);
    assert_eq!(checkpoint.last_modseq, 7);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("source")
            .and_then(serde_json::Value::as_str),
        Some("emsmdb-ics-download")
    );
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(serde_json::Value::as_u64),
        Some(test_mapi_folder_id(4))
    );
    assert_eq!(
        checkpoint
            .cursor_json
            .get("hierarchySyncVersion")
            .and_then(serde_json::Value::as_u64),
        Some(2)
    );

    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 42,
        current_modseq: 7,
        ..Default::default()
    };
    let restarted = ExchangeService::new(store.clone());
    let connect = restarted
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut restart_rops = Vec::new();
    append_rop_open_folder(&mut restart_rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        &mut restart_rops,
        1,
        2,
        4096,
        b"client-hierarchy-state",
    );
    let response = restarted
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&restart_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(!contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x4014_0003u32.to_le_bytes()
    ));

    let restarted = ExchangeService::new(store.clone());
    let connect = restarted
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut restart_rops = Vec::new();
    append_rop_open_folder(&mut restart_rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        &mut restart_rops,
        1,
        2,
        4096,
        &[],
    );
    let response = restarted
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&restart_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(!contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_to_message_returns_canonical_manifest_without_bcc() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "43434343-4343-4343-4343-434343434343";
    let mut email = FakeStore::email(message_id, inbox_id, "inbox", "CopyTo message");
    email.body_text = "CopyTo body from canonical mail".to_string();
    email.bcc.push(JmapEmailAddress {
        address: "hidden-copyto@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x03, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[0x4D, 0x00, 0x02, 0x03]);
    rops.push(0);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x4E, 0x00, 0x03]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4D, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"CopyTo message"));
    assert!(contains_bytes(
        &response_rops,
        b"CopyTo body from canonical mail"
    ));
    assert!(!contains_bytes(
        &response_rops,
        b"hidden-copyto@example.test"
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let child_id = "66666666-6666-6666-6666-666666666666";
    let child_uuid = Uuid::parse_str(child_id).unwrap();
    let child_folder_id = test_mapi_folder_id(600);
    let mut child_folder = FakeStore::mailbox(child_id, "custom", "Project");
    child_folder.parent_id = Some(Uuid::parse_str(inbox_id).unwrap());
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(inbox_id, "inbox", "Inbox"),
            child_folder,
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "46464646-4646-4646-4646-464646464646",
                inbox_id,
                "inbox",
                "Folder FastTransfer",
            ),
            FakeStore::email(
                "47474747-4747-4747-4747-474747474747",
                child_id,
                "custom",
                "Child FastTransfer",
            ),
        ])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(child_uuid, child_folder_id)]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x4C, 0x00, 0x01, 0x02]);
    rops.push(0);
    rops.push(0x01);
    rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4C, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x4009_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x400B_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x400A_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Folder FastTransfer")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Child FastTransfer")
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_properties_message_returns_canonical_manifest_without_bcc(
) {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "47474747-4747-4747-4747-474747474747";
    let mut email = FakeStore::email(message_id, inbox_id, "inbox", "CopyProperties message");
    email.body_text = "CopyProperties body from canonical mail".to_string();
    email.bcc.push(JmapEmailAddress {
        address: "hidden-copyprops@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x03, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[0x69, 0x00, 0x02, 0x03]);
    rops.push(0);
    rops.push(0);
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x4E, 0x00, 0x03]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x69, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"CopyProperties message"));
    assert!(contains_bytes(
        &response_rops,
        b"CopyProperties body from canonical mail"
    ));
    assert!(!contains_bytes(
        &response_rops,
        b"hidden-copyprops@example.test"
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_destination_upload_saves_canonical_email() {
    fn append_fast_transfer_utf16_property(values: &mut Vec<u8>, property_tag: u32, value: &str) {
        let bytes = utf16z(value);
        values.extend_from_slice(&property_tag.to_le_bytes());
        values.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        values.extend_from_slice(&bytes);
    }

    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut transfer_data = Vec::new();
    append_fast_transfer_utf16_property(
        &mut transfer_data,
        0x0037_001F,
        "FastTransfer uploaded subject",
    );
    append_fast_transfer_utf16_property(
        &mut transfer_data,
        0x1000_001F,
        "FastTransfer uploaded body",
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x53, 0x00, 0x02, 0x03, 0x01, 0x00]);
    rops.extend_from_slice(&[0x54, 0x00, 0x03]);
    rops.extend_from_slice(&(transfer_data.len() as u16).to_le_bytes());
    rops.extend_from_slice(&transfer_data);
    rops.extend_from_slice(&[0x86, 0x00, 0x03]);
    rops.extend_from_slice(&[15, 20, 0, 1, 0, 0]);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x02, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x53, 0x03, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[
            0x54,
            0x03,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            transfer_data.len() as u8,
            0x00,
        ]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x86, 0x03, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "FastTransfer uploaded subject");
    assert_eq!(recorded[0].body_text, "FastTransfer uploaded body");
}

#[tokio::test]
async fn mapi_over_http_microsoft_async_table_control_rops_return_rop_specific_protocol_errors() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x38, 0x00, 0x02]);
    rops.extend_from_slice(&[0x50, 0x00, 0x02, 0x01]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x05, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x38, 0x02, 0x14, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x50, 0x02, 0x00, 0x04, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));

    renew_mapi_request_id(&mut execute_headers);
    let mut invalid_progress = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut invalid_progress, test_mapi_folder_id(5));
    invalid_progress.push(0);
    invalid_progress.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);
    invalid_progress.extend_from_slice(&[0x50, 0x00, 0x02, 0x02]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&invalid_progress, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x50, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let first_message_id = Uuid::parse_str("88888888-8888-4888-8888-888888888881").unwrap();
    let second_message_id = Uuid::parse_str("88888888-8888-4888-8888-888888888882").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &first_message_id.to_string(),
                &trash_id.to_string(),
                "trash",
                "First sync deleted message",
            ),
            FakeStore::email(
                &second_message_id.to_string(),
                &trash_id.to_string(),
                "trash",
                "Second sync deleted message",
            ),
        ])),
        ..Default::default()
    };
    store.mapi_identities.lock().unwrap().insert(
        first_message_id,
        crate::mapi::identity::legacy_migration_object_id(&first_message_id),
    );
    store.mapi_identities.lock().unwrap().insert(
        second_message_id,
        crate::mapi::identity::legacy_migration_object_id(&second_message_id),
    );
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
            50,
            50,
            serde_json::json!({"source": "before-empty-folder"}),
        )
        .await
        .unwrap();
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut purge_rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut purge_rops, crate::mapi::identity::TRASH_FOLDER_ID);
    purge_rops.push(0);
    purge_rops.extend_from_slice(&[0x58, 0x00, 0x01, 0x00, 0x00]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&purge_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x58, 0x01, 0, 0, 0, 0, 0]));
    assert_eq!(
        store.deleted_emails.lock().unwrap().as_slice(),
        &[first_message_id, second_message_id]
    );

    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 51,
        current_modseq: 51,
        deleted_message_ids: vec![first_message_id, second_message_id],
        ..Default::default()
    };
    let response_rops = content_sync_response_rops(store.clone(), 8, b"trash-state").await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(stream.idset_given.is_empty());
    assert!(stream.cnset_seen.is_empty());
    assert!(stream.cnset_seen_fai.is_empty());
    assert!(stream.cnset_read.is_empty());
    assert_content_final_state_includes(&response_rops, &[], &[]);
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 51);
    assert_eq!(checkpoint.last_modseq, 51);
}

#[tokio::test]
async fn mapi_over_http_empty_folder_reports_partial_completion_when_membership_delete_fails() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let first_message_id = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    let failing_message_id = Uuid::parse_str("55555555-5555-4555-8555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &first_message_id.to_string(),
                &trash_id.to_string(),
                "trash",
                "First trash message",
            ),
            FakeStore::email(
                &failing_message_id.to_string(),
                &trash_id.to_string(),
                "trash",
                "Failing trash message",
            ),
        ])),
        failed_delete_email_ids: Arc::new(Mutex::new(vec![failing_message_id])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let canonical_emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x58, 0x00, 0x01, 0x00, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x58, 0x01, 0, 0, 0, 0, 1]));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[first_message_id]
    );
    let canonical = canonical_emails.lock().unwrap();
    assert!(canonical.iter().all(|email| email.id != first_message_id));
    assert!(canonical.iter().any(|email| email.id == failing_message_id));
}

#[tokio::test]
async fn mapi_over_http_sync_source_transfer_state_does_not_echo_uploaded_client_state() {
    let message_id = Uuid::parse_str("45454545-4545-4545-4545-454545454545").unwrap();
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &inbox_id.to_string(),
            "inbox",
            "Transfer state message",
        )])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 88,
        current_modseq: 44,
        changed_mailbox_ids: Vec::new(),
        changed_message_ids: vec![message_id],
        deleted_message_ids: Vec::new(),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let client_state = b"client-uploaded-content-state";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&META_TAG_IDSET_GIVEN.to_le_bytes());
    rops.extend_from_slice(&(client_state.len() as u32).to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(client_state.len() as u32).to_le_bytes());
    rops.extend_from_slice(client_state);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));
    assert!(!contains_bytes(&response_rops, client_state));
    assert!(!contains_bytes(
        &response_rops,
        &globcnt_bytes(mapi_message_global_counter(&message_id))
    ));
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap();
    assert!(checkpoint.is_none());
}

#[tokio::test]
async fn mapi_over_http_download_transfer_state_handle_cannot_regress_checkpoint() {
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555501").unwrap();
    let message_id = Uuid::parse_str("45454545-4545-4545-8545-454545454501").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &inbox_id.to_string(),
            "inbox",
            "No checkpoint regression",
        )])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            88,
            44,
            serde_json::json!({"source": "newer-download"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 55,
        current_modseq: 22,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x28, 0x00, // content sync, ReadState | Normal
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 88);
    assert_eq!(checkpoint.last_modseq, 44);
    assert_eq!(checkpoint.cursor_json["source"], "newer-download");
}

#[tokio::test]
async fn mapi_over_http_partial_scope_content_sync_does_not_advance_checkpoint() {
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555501").unwrap();
    let message_id = Uuid::parse_str("45454545-4545-4545-8545-454545454502").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &inbox_id.to_string(),
            "inbox",
            "Partial scope checkpoint",
        )])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            88,
            44,
            serde_json::json!({"source": "full-content-sync"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 89,
        current_modseq: 45,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 88);
    assert_eq!(checkpoint.last_modseq, 44);
    assert_eq!(checkpoint.cursor_json["source"], "full-content-sync");
}

#[tokio::test]
async fn mapi_over_http_partial_trash_content_scope_does_not_advance_checkpoint() {
    let trash_id = Uuid::parse_str("77777777-7777-4777-8777-777777777701").unwrap();
    let message_id = Uuid::parse_str("46464646-4646-4646-8646-464646464602").unwrap();
    let mut trash = FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items");
    trash.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![trash])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &trash_id.to_string(),
            "trash",
            "Partial trash scope checkpoint",
        )])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
            88,
            44,
            serde_json::json!({"source": "full-trash-content-sync"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 89,
        current_modseq: 45,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Partial trash scope checkpoint")
    ));

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 88);
    assert_eq!(checkpoint.last_modseq, 44);
    assert_eq!(checkpoint.cursor_json["source"], "full-trash-content-sync");
}

#[tokio::test]
async fn mapi_over_http_sync_upload_state_returns_server_transfer_state() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            55,
            22,
            serde_json::json!({"source": "existing-content-sync"}),
        )
        .await
        .unwrap();
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let state = b"client-uploaded-sync-state";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
    ]);
    rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x65E2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    rops.extend_from_slice(state);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));
    assert!(!contains_bytes(&response_rops, state));
    assert_content_final_state_includes(&response_rops, &[], &[]);
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 55);
    assert_eq!(checkpoint.last_modseq, 22);
}

#[tokio::test]
async fn mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints() {
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555502").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            55,
            22,
            serde_json::json!({"source": "existing-content-sync"}),
        )
        .await
        .unwrap();
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let state = b"collector-upload-state";
    let imported_message_id = test_mapi_message_id("42424242-4242-4242-8242-424242424242");
    let mut values = Vec::new();
    append_mapi_binary_property(
        &mut values,
        PID_TAG_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(imported_message_id),
    );
    append_mapi_utf16_property(&mut values, 0x0037_001F, "Collector import message");
    append_mapi_utf16_property(&mut values, PID_TAG_BODY_W, "Collector body");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&META_TAG_IDSET_GIVEN.to_le_bytes());
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    rops.extend_from_slice(state);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&values);
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x03, 0x00, // RopSaveChangesMessage
        0x82, 0x00, 0x02, 0x04, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x04, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x04, 0, 0, 0, 0]));
    assert!(!contains_bytes(&response_rops, state));
    assert_eq!(store.imported_emails.lock().unwrap().len(), 1);
    let imported_email = store.emails.lock().unwrap().last().unwrap().clone();
    assert_content_final_state_includes(
        &response_rops,
        &[imported_email.id],
        &[mapi_mailstore::canonical_message_change_number(
            &imported_email,
        )],
    );
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 55);
    assert_eq!(checkpoint.last_modseq, 22);
    assert_eq!(checkpoint.cursor_json["source"], "existing-content-sync");
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state() {
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555504").unwrap();
    let imported_message_id = test_mapi_message_id("42424242-4242-4242-8242-424242424243");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut values = Vec::new();
    append_mapi_binary_property(
        &mut values,
        PID_TAG_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(imported_message_id),
    );
    append_mapi_utf16_property(&mut values, PID_TAG_SUBJECT_W, "MS-OXCFXICS 4.2.1 subject");
    append_mapi_utf16_property(&mut values, PID_TAG_BODY_W, "MS-OXCFXICS 4.2.1 body");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
    ]);
    for tag in [
        META_TAG_CNSET_SEEN,
        META_TAG_CNSET_SEEN_FAI,
        META_TAG_CNSET_READ,
    ] {
        rops.extend_from_slice(&[0x75, 0x00, 0x02]); // UploadStateStreamBegin
        rops.extend_from_slice(&tag.to_le_bytes());
        rops.extend_from_slice(&0u32.to_le_bytes());
        rops.extend_from_slice(&[0x77, 0x00, 0x02]); // UploadStateStreamEnd
    }
    rops.extend_from_slice(&[
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&values);
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x03, 0x00, // RopSaveChangesMessage
        0x82, 0x00, 0x02, 0x04, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x04, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x04, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    let saved_email = emails.lock().unwrap().last().unwrap().clone();
    assert_content_final_state_includes_counters(
        &response_rops,
        &[crate::mapi::identity::global_counter_from_store_id(imported_message_id).unwrap()],
        &[mapi_mailstore::canonical_message_change_number(
            &saved_email,
        )],
    );

    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].source, "mapi-save-message");
    assert_eq!(recorded[0].subject, "MS-OXCFXICS 4.2.1 subject");
    assert_eq!(recorded[0].body_text, "MS-OXCFXICS 4.2.1 body");
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state() {
    let mailbox_id = "55555555-5555-4555-9555-555555555505";
    let message_id = "42424242-4242-4242-8242-424242424244";
    let email = FakeStore::email(message_id, mailbox_id, "inbox", "MS-OXCFXICS 4.2.2 delete");
    let expected_change_number = mapi_mailstore::canonical_message_change_number(&email);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
    ]);
    for tag in [
        META_TAG_CNSET_SEEN,
        META_TAG_CNSET_SEEN_FAI,
        META_TAG_CNSET_READ,
    ] {
        rops.extend_from_slice(&[0x75, 0x00, 0x02]); // UploadStateStreamBegin
        rops.extend_from_slice(&tag.to_le_bytes());
        rops.extend_from_slice(&0u32.to_le_bytes());
        rops.extend_from_slice(&[0x77, 0x00, 0x02]); // UploadStateStreamEnd
    }
    rops.extend_from_slice(&[
        0x74, 0x00, 0x02, // RopSynchronizationImportDeletes
        0x02, // hard delete
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));
    let state_chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(state_chunks.len(), 1);
    assert!(contains_bytes(
        &state_chunks[0].1,
        &globcnt_bytes(mapi_message_global_counter(
            &Uuid::parse_str(message_id).unwrap()
        ))
    ));
    assert!(contains_bytes(
        &state_chunks[0].1,
        &globcnt_bytes(expected_change_number)
    ));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[Uuid::parse_str(message_id).unwrap()]
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_partial_item_upload_updates_existing_message_without_import() {
    let mailbox_id = "55555555-5555-4555-9555-555555555503";
    let message_id = "49494949-4949-4949-8949-494949494949";
    let inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    let mut email = FakeStore::email(message_id, mailbox_id, "inbox", "Partial upload before");
    email.body_text = "Body before partial upload".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut set_values = Vec::new();
    append_mapi_utf16_property(&mut set_values, PID_TAG_SUBJECT_W, "Partial upload after");
    append_mapi_binary_property(
        &mut set_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        b"client-pcl",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
    ]);
    for tag in [
        META_TAG_CNSET_SEEN,
        META_TAG_CNSET_SEEN_FAI,
        META_TAG_CNSET_READ,
    ] {
        rops.extend_from_slice(&[0x75, 0x00, 0x02]); // UploadStateStreamBegin
        rops.extend_from_slice(&tag.to_le_bytes());
        rops.extend_from_slice(&0u32.to_le_bytes());
        rops.extend_from_slice(&[0x77, 0x00, 0x02]); // UploadStateStreamEnd
    }
    append_rop_open_message(
        &mut rops,
        1,
        3,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    rops.extend_from_slice(&[0x07, 0x00, 0x03]); // RopGetPropertiesSpecific
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_PREDECESSOR_CHANGE_LIST.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_CHANGE_KEY.to_le_bytes());
    rops.extend_from_slice(&[0x7A, 0x00, 0x03]); // RopDeletePropertiesNoReplicate
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
    rops.extend_from_slice(&[0x79, 0x00, 0x03]); // RopSetPropertiesNoReplicate
    rops.extend_from_slice(&((set_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&set_values);
    append_rop_save_changes_message(&mut rops, 3, 3);
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x04, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x04, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x07, 0x03, 0, 0, 0, 0]));
    assert!(
        contains_bytes(&response_rops, &[0x7A, 0x03, 0, 0, 0, 0, 0, 0]),
        "DeletePropertiesNoReplicate failed in response: {response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x79, 0x03, 0, 0, 0, 0, 0, 0]),
        "SetPropertiesNoReplicate failed in response: {response_rops:02x?}"
    );
    assert!(contains_bytes(&response_rops, &[0x0C, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x04, 0, 0, 0, 0]));
    assert_content_final_state_includes(&response_rops, &[], &[]);
    let updated = emails.lock().unwrap();
    assert_eq!(updated[0].subject, "Partial upload after");
    assert_eq!(updated[0].body_text, "");
}

#[tokio::test]
async fn mapi_over_http_sync_upload_state_does_not_echo_multiple_streams() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let first_state = b"client-idset-given";
    let second_state = b"client-cnset-seen";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(first_state.len() as u32).to_le_bytes());
    rops.extend_from_slice(first_state);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(second_state.len() as u32).to_le_bytes());
    rops.extend_from_slice(second_state);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!contains_bytes(&response_rops, first_state));
    assert!(!contains_bytes(&response_rops, second_state));
    assert_content_final_state_includes(&response_rops, &[], &[]);
}

#[tokio::test]
async fn mapi_over_http_set_local_replica_midset_deleted_round_trips_in_transfer_state() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let deleted_midset = b"deleted-local-replica-midset";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x93, 0x00, 0x02, // RopSetLocalReplicaMidsetDeleted
    ]);
    rops.extend_from_slice(&(deleted_midset.len() as u16).to_le_bytes());
    rops.extend_from_slice(deleted_midset);
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x93, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, deleted_midset));
}

#[tokio::test]
async fn mapi_over_http_sync_import_message_change_updates_canonical_flags() {
    let message_id = "41414141-4141-4141-4141-414141414141";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Import message change",
    );
    email.unread = true;
    email.flagged = false;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(test_mapi_message_id(message_id)),
    );
    append_mapi_i32_property(&mut property_values, 0x0E07_0003, 1);
    append_mapi_i32_property(&mut property_values, 0x1090_0003, 2);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_6_fail_on_conflict_uses_predecessor_change_list() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "49494949-4949-4949-8949-494949494950";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(message_id, mailbox_id, "inbox", "Server current subject");
    email.modseq = 70;
    email.mailbox_states[0].modseq = 70;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let stale_pcl = mapi_mailstore::predecessor_change_list(69);

    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(test_mapi_message_id(message_id)),
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &stale_pcl,
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "Client stale subject",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
        0x40, // FailOnConflict
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x72, 0x03, 0x09, 0x01, 0x04, 0x80]
    ));
    assert_eq!(emails.lock().unwrap()[0].subject, "Server current subject");
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_6_newer_predecessor_change_list_imports() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "49494949-4949-4949-8949-494949494951";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(message_id, mailbox_id, "inbox", "Server older subject");
    email.modseq = 70;
    email.mailbox_states[0].modseq = 70;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let newer_pcl = mapi_mailstore::predecessor_change_list(71);

    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(test_mapi_message_id(message_id)),
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &newer_pcl,
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "Client newer subject",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
        0x40, // FailOnConflict
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert_eq!(emails.lock().unwrap()[0].subject, "Client newer subject");
}

#[tokio::test]
async fn mapi_over_http_sync_import_new_message_saves_canonical_email() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "ICS imported subject");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "ICS imported body");
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-ics-import@example.test>",
    );

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x03, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].source, "mapi-save-message");
    assert_eq!(recorded[0].subject, "ICS imported subject");
    assert_eq!(recorded[0].body_text, "ICS imported body");
    assert_eq!(
        recorded[0].internet_message_id.as_deref(),
        Some("<mapi-ics-import@example.test>")
    );
    assert!(recorded[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_message_change_can_target_trash() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "ICS imported to Trash");
    let imported_message_id = crate::mapi::identity::mapi_store_id(0x1234);
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &crate::mapi::identity::source_key_for_object_id(imported_message_id),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x03, 0x00]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    let mut expected_save = vec![0x0C, 0x01, 0, 0, 0, 0, 0x03];
    expected_save.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(imported_message_id).unwrap(),
    );
    assert!(contains_bytes(&response_rops, &expected_save));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, trash_id);
    assert_eq!(recorded[0].subject, "ICS imported to Trash");
}

#[tokio::test]
async fn mapi_over_http_save_message_falls_back_when_import_source_key_is_already_used() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let imported_message_id = crate::mapi::identity::mapi_store_id(0x1234);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    store.mapi_identities.lock().unwrap().insert(
        Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
        imported_message_id,
    );
    let imported_emails = store.imported_emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "ICS duplicate source key",
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &crate::mapi::identity::source_key_for_object_id(imported_message_id),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x03, 0x00]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x0C, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    let allocated = mapi_identities.lock().unwrap();
    assert_eq!(allocated.len(), 2);
    assert!(allocated
        .values()
        .any(|object_id| *object_id != imported_message_id));
}

#[tokio::test]
async fn mapi_over_http_save_message_replaces_out_of_range_import_source_key() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let imported_source_key =
        crate::mapi::identity::source_key_for_object_id(crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 1,
        ));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let emails = store.emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let mapi_identity_source_keys = store.mapi_identity_source_keys.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "ICS out of range source key",
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &imported_source_key,
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x03, 0x00]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x0C, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert_eq!(imported_emails.lock().unwrap().len(), 1);
    let allocated = mapi_identities.lock().unwrap();
    let saved_id = emails.lock().unwrap().last().unwrap().id;
    assert!(!allocated
        .values()
        .any(
            |object_id| crate::mapi::identity::source_key_for_object_id(*object_id)
                == imported_source_key
        ));
    assert!(!mapi_identity_source_keys
        .lock()
        .unwrap()
        .contains_key(&saved_id));
}

#[tokio::test]
async fn mapi_over_http_save_message_acknowledges_trash_sync_metadata_only_import() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let out_of_range_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 1,
    );
    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &crate::mapi::identity::source_key_for_object_id(out_of_range_object_id),
    );
    append_mapi_i64_property(&mut property_values, PID_TAG_LAST_MODIFICATION_TIME, 0);
    append_mapi_binary_property(&mut property_values, PID_TAG_CHANGE_KEY, b"change");
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        b"pcl",
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x03, 0x00]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &mapi_wire_id_bytes(out_of_range_object_id)
    ));
    assert_eq!(imported_emails.lock().unwrap().len(), 0);
    assert!(!mapi_identities
        .lock()
        .unwrap()
        .values()
        .any(|object_id| *object_id == out_of_range_object_id));
}

#[tokio::test]
async fn mapi_over_http_associated_message_uploads_do_not_create_visible_items() {
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555501").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let events = store.events.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());

    let mut inbox_values = Vec::new();
    append_mapi_utf16_property(&mut inbox_values, 0x001A_001F, "IPM.Configuration");
    append_mapi_utf16_property(&mut inbox_values, PID_TAG_SUBJECT_W, "Outlook view state");
    let mut inbox_rops = Vec::new();
    append_rop_create_associated_message(&mut inbox_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_set_properties(&mut inbox_rops, 1, 2, &inbox_values);
    append_rop_save_changes_message(&mut inbox_rops, 1, 1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&inbox_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert_eq!(imported_emails.lock().unwrap().len(), 0);

    renew_mapi_request_id(&mut execute_headers);
    let mut calendar_values = Vec::new();
    append_mapi_utf16_property(
        &mut calendar_values,
        0x001A_001F,
        "IPM.Configuration.Calendar",
    );
    append_mapi_utf16_property(
        &mut calendar_values,
        PID_TAG_SUBJECT_W,
        "Calendar view state",
    );
    let mut calendar_rops = Vec::new();
    append_rop_create_associated_message(
        &mut calendar_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_set_properties(&mut calendar_rops, 1, 2, &calendar_values);
    append_rop_save_changes_message(&mut calendar_rops, 1, 1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&calendar_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(events.lock().unwrap().is_empty());

    renew_mapi_request_id(&mut execute_headers);
    let mut freebusy_values = Vec::new();
    append_mapi_utf16_property(
        &mut freebusy_values,
        0x001A_001F,
        "IPM.Microsoft.ScheduleData.FreeBusy",
    );
    append_mapi_utf16_property(
        &mut freebusy_values,
        PID_TAG_SUBJECT_W,
        "Calendar freebusy view state",
    );
    let mut freebusy_rops = Vec::new();
    append_rop_create_message(
        &mut freebusy_rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    append_rop_set_properties(&mut freebusy_rops, 1, 2, &freebusy_values);
    append_rop_save_changes_message(&mut freebusy_rops, 1, 1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&freebusy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x06, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert_eq!(imported_emails.lock().unwrap().len(), 0);
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_associated_message_persists_and_replays_fai() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 42,
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let associated_configs = store.associated_configs.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &crate::mapi::identity::source_key_for_object_id(associated_object_id),
    );
    append_mapi_utf16_property(&mut property_values, 0x001A_001F, "IPM.Configuration");
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "Outlook Inbox view state",
    );
    let outlook_prefs_dictionary = br#"<?xml version="1.0" encoding="utf-8"?><UserConfiguration xmlns="dictionary.xsd"><Info version="Outlook.16"/><Data><e k="OLPrefsVersion" v="9-7"/></Data></UserConfiguration>"#;
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_CHANGE_KEY,
        &crate::mapi::identity::source_key_for_object_id(associated_object_id),
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &strict_test_replid_globset(&[crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 42]),
    );
    append_mapi_binary_property(&mut property_values, 0x7C07_0102, outlook_prefs_dictionary);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0x10);
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, PID_TAG_BODY_W, "Client view payload");
    append_mapi_binary_property(&mut update_values, 0x7C08_0102, b"view-extra");
    append_rop_set_properties(&mut rops, 3, 2, &update_values);
    append_rop_save_changes_message(&mut rops, 3, 3);
    append_rop_get_properties_specific(&mut rops, 3, &[PID_TAG_SUBJECT_W, 0x7C08_0102]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[1, u32::MAX, u32::MAX, u32::MAX, 1, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0A, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x07, 0x03, 0, 0, 0, 0]));
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    assert!(imported_emails.lock().unwrap().is_empty());
    {
        let configs = associated_configs.lock().unwrap();
        let config = configs
            .iter()
            .find(|config| {
                config.message_class == "IPM.Configuration"
                    && config.subject == "Outlook Inbox view state"
            })
            .expect("imported associated config");
        assert_eq!(config.folder_id, crate::mapi::identity::INBOX_FOLDER_ID);
        assert_eq!(config.message_class, "IPM.Configuration");
        assert_eq!(config.subject, "Outlook Inbox view state");
        assert_eq!(
            config.properties_json["0x65e00102"]["value"],
            serde_json::Value::String(
                associated_source_key
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
        );
        assert_eq!(
            config.properties_json["0x7c070102"]["value"],
            serde_json::Value::String(
                outlook_prefs_dictionary
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
        );
        assert_eq!(
            config.properties_json["0x7c080102"]["value"],
            serde_json::Value::String("766965772d6578747261".to_string())
        );
    }

    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let reconnect_cookie = mapi_cookie_header(&reconnect);

    let mut sync_rops = Vec::new();
    append_rop_open_folder(&mut sync_rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    sync_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x02, 0x00, // PropertyTagCount
    ]);
    sync_rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    sync_rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    sync_rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    sync_rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut sync_headers = mapi_headers("Execute");
    sync_headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let sync_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &sync_headers,
            &execute_body(&rop_buffer(&sync_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(sync_response.status(), StatusCode::OK);
    let sync_response_rops = response_rops_from_execute_response(sync_response).await;
    let stream = strict_content_sync_transfer_from_response(&sync_response_rops)
        .unwrap_or_else(|error| panic!("{error}: {sync_response_rops:02x?}"));
    let message = stream
        .message_changes
        .iter()
        .find(|message| message.source_key == associated_source_key)
        .expect("imported associated config should replay in FAI sync");
    assert!(message.associated);
    assert_eq!(message.subject, "Outlook Inbox view state");
    assert!(message.mid.is_some());
    assert!(message.body_tags.contains(&0x7C08_0102));
    assert!(contains_bytes(&sync_response_rops, b"OLPrefsVersion"));
    assert!(contains_bytes(&sync_response_rops, b"9-7"));
    assert!(contains_bytes(&sync_response_rops, b"view-extra"));

    let mut table_rops = Vec::new();
    append_rop_open_folder(
        &mut table_rops,
        0,
        1,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    table_rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // normal contents table
    table_rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    table_rops.extend_from_slice(&1u16.to_le_bytes());
    table_rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    table_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    table_rops.extend_from_slice(&50u16.to_le_bytes());

    let mut table_headers = mapi_headers("Execute");
    table_headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let table_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &table_headers,
            &execute_body(&rop_buffer(&table_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(table_response.status(), StatusCode::OK);
    let table_response_rops = response_rops_from_execute_response(table_response).await;
    assert!(contains_bytes(
        &table_response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0, 0]
    ));
    assert!(!contains_bytes(
        &table_response_rops,
        &utf16z("Outlook Inbox view state")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai() {
    let account = FakeStore::account();
    let associated_configs = Arc::new(Mutex::new(Vec::new()));
    let mapi_identities = Arc::new(Mutex::new(HashMap::new()));
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        associated_configs: associated_configs.clone(),
        mapi_identities: mapi_identities.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());

    let dictionary_stream = br#"<?xml version="1.0"?><UserConfiguration><Info version="Outlook.12"/><Data><e k="18-piAutoProcess" v="3-True"/><e k="18-piRemindDefault" v="9-15"/><e k="18-piReminderUpgradeTime" v="9-212864507"/><e k="18-OLPrefsVersion" v="9-1"/></Data></UserConfiguration>"#;
    let work_hours_xml = br#"<?xml version="1.0"?><Root xmlns="WorkingHours.xsd"><WorkHoursVersion1><TimeZone><Bias>480</Bias><Standard><Bias>0</Bias><ChangeDate><Time>02:00:00</Time><Date>0000/11/01</Date><DayOfWeek>0</DayOfWeek></ChangeDate></Standard><DaylightSavings><Bias>-60</Bias><ChangeDate><Time>02:00:00</Time><Date>0000/03/02</Date><DayOfWeek>0</DayOfWeek></ChangeDate></DaylightSavings><Name>Pacific Standard Time</Name></TimeZone><TimeSlot><Start>09:00:00</Start><End>17:00:00</End></TimeSlot><WorkDays>Monday Tuesday Wednesday Thursday Friday</WorkDays></WorkHoursVersion1></Root>"#;
    let category_list_xml = br#"<?xml version="1.0"?><categories default="Red Category" lastSavedSession="5" lastSavedTime="2007-12-28T03:01:50.429" xmlns="CategoryList.xsd"><category name="Red Category" color="0" keyboardShortcut="0" usageCount="7" lastTimeUsedNotes="1601-01-01T00:00:00.000" lastTimeUsedJournal="1601-01-01T00:00:00.000" lastTimeUsedContacts="1601-01-01T00:00:00.000" lastTimeUsedTasks="1601-01-01T00:00:00.000" lastTimeUsedCalendar="2007-11-28T20:05:04.703" lastTimeUsedMail="1601-01-01T00:00:00.000" lastTimeUsed="2007-11-28T20:05:04.703" lastSessionUsed="3" guid="{2B7FC69C-7046-44A2-8FF3-007D7467DC82}"/><category name="Blue Category" color="7" keyboardShortcut="0" usageCount="6" lastTimeUsedNotes="1601-01-01T00:00:00.000" lastTimeUsedJournal="1601-01-01T00:00:00.000" lastTimeUsedContacts="1601-01-01T00:00:00.000" lastTimeUsedTasks="1601-01-01T00:00:00.000" lastTimeUsedCalendar="2007-12-28T03:00:07.102" lastTimeUsedMail="1601-01-01T00:00:00.000" lastTimeUsed="2007-12-28T03:00:07.102" lastSessionUsed="5" guid="{33A1EAE3-8E5E-4912-9580-69FC764FEA35}"/><category name="Purple Category" color="8" keyboardShortcut="0" usageCount="7" lastTimeUsedNotes="1601-01-01T00:00:00.000" lastTimeUsedJournal="1601-01-01T00:00:00.000" lastTimeUsedContacts="1601-01-01T00:00:00.000" lastTimeUsedTasks="1601-01-01T00:00:00.000" lastTimeUsedCalendar="2007-11-28T20:03:06.018" lastTimeUsedMail="1601-01-01T00:00:00.000" lastTimeUsed="2007-11-28T20:03:06.018" lastSessionUsed="3" guid="{58AB8B90-BB05-428A-B8D2-F1C93968C144}"/><category name="Orange Category" color="1" keyboardShortcut="0" usageCount="2" lastTimeUsedNotes="1601-01-01T00:00:00.000" lastTimeUsedJournal="1601-01-01T00:00:00.000" lastTimeUsedContacts="1601-01-01T00:00:00.000" lastTimeUsedTasks="1601-01-01T00:00:00.000" lastTimeUsedCalendar="1601-01-01T00:00:00.000" lastTimeUsedMail="1601-01-01T00:00:00.000" lastTimeUsed="2007-11-21T00:07:48.517" lastSessionUsed="0" guid="{F5F57BF3-A188-48D5-A096-863ACACB2D36}" renameOnFirstUse="1"/></categories>"#;
    let examples = [
        (
            "IPM.Configuration.Calendar",
            "MS-OXOCFG dictionary",
            0x0000_0004,
            0x7C07_0102,
            dictionary_stream.as_slice(),
        ),
        (
            "IPM.Configuration.WorkHours",
            "MS-OXOCFG working hours",
            0x0000_0002,
            0x7C08_0102,
            work_hours_xml.as_slice(),
        ),
        (
            "IPM.Configuration.CategoryList",
            "MS-OXOCFG category list",
            0x0000_0002,
            0x7C08_0102,
            category_list_xml.as_slice(),
        ),
    ];

    for (message_class, subject, roaming_datatypes, stream_tag, stream) in examples {
        let mut property_values = Vec::new();
        append_mapi_utf16_property(&mut property_values, 0x001A_001F, message_class);
        append_mapi_utf16_property(&mut property_values, PID_TAG_SUBJECT_W, subject);
        append_mapi_i32_property(&mut property_values, 0x7C06_0003, roaming_datatypes);
        append_mapi_binary_property(&mut property_values, stream_tag, stream);

        let mut rops = Vec::new();
        append_rop_create_associated_message(
            &mut rops,
            0,
            1,
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        );
        append_rop_set_properties(&mut rops, 1, 4, &property_values);
        append_rop_save_changes_message(&mut rops, 1, 1);
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response_rops = response_rops_from_execute_response(response).await;
        assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
        renew_mapi_request_id(&mut execute_headers);
    }

    {
        let configs = associated_configs.lock().unwrap();
        assert_eq!(configs.len(), 3);
        let hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        for (message_class, subject, roaming_datatypes, stream_tag, stream) in examples {
            let config = configs
                .iter()
                .find(|config| config.message_class == message_class)
                .unwrap_or_else(|| panic!("missing {message_class}"));
            assert_eq!(config.account_id, account.account_id);
            assert_eq!(config.folder_id, crate::mapi::identity::CALENDAR_FOLDER_ID);
            assert_eq!(config.subject, subject);
            assert_eq!(
                config.properties_json["0x7c060003"]["value"],
                serde_json::Value::Number(roaming_datatypes.into())
            );
            assert_eq!(
                config.properties_json[format!("0x{stream_tag:08x}")]["value"],
                serde_json::Value::String(hex(stream))
            );
        }
    }

    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let reconnect_cookie = mapi_cookie_header(&reconnect);

    let mut table_rops = Vec::new();
    append_rop_open_folder(
        &mut table_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    table_rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x02]);
    table_rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    table_rops.extend_from_slice(&5u16.to_le_bytes());
    for tag in [
        0x001A_001F,
        PID_TAG_SUBJECT_W,
        0x7C06_0003,
        0x7C07_0102,
        0x7C08_0102,
    ] {
        table_rops.extend_from_slice(&tag.to_le_bytes());
    }
    table_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    table_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut table_headers = mapi_headers("Execute");
    table_headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let table_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &table_headers,
            &execute_body(&rop_buffer(&table_rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(table_response.status(), StatusCode::OK);
    let table_response_rops = response_rops_from_execute_response(table_response).await;
    assert!(contains_bytes(
        &table_response_rops,
        &utf16z("IPM.Configuration.WorkHours")
    ));
    assert!(contains_bytes(
        &table_response_rops,
        &utf16z("IPM.Configuration.CategoryList")
    ));
    assert!(contains_bytes(&table_response_rops, b"18-piAutoProcess"));
    assert!(contains_bytes(&table_response_rops, b"WorkingHours.xsd"));
    assert!(contains_bytes(&table_response_rops, b"DaylightSavings"));
    assert!(contains_bytes(&table_response_rops, b"CategoryList.xsd"));

    let (work_hours_id, category_list_id) = {
        let configs = associated_configs.lock().unwrap();
        let work_hours_uuid = configs
            .iter()
            .find(|config| config.message_class == "IPM.Configuration.WorkHours")
            .expect("stored WorkHours config")
            .id;
        let category_list_uuid = configs
            .iter()
            .find(|config| config.message_class == "IPM.Configuration.CategoryList")
            .expect("stored CategoryList config")
            .id;
        let identities = mapi_identities.lock().unwrap();
        let work_hours_id = *identities
            .get(&work_hours_uuid)
            .expect("allocated WorkHours MAPI identity");
        let category_list_id = *identities
            .get(&category_list_uuid)
            .expect("allocated CategoryList MAPI identity");
        (work_hours_id, category_list_id)
    };

    let mut open_rops = Vec::new();
    append_rop_open_folder(
        &mut open_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_open_message(
        &mut open_rops,
        1,
        2,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        work_hours_id,
    );
    append_rop_get_properties_specific(
        &mut open_rops,
        2,
        &[0x001A_001F, PID_TAG_SUBJECT_W, 0x7C06_0003, 0x7C08_0102],
    );
    let mut open_headers = mapi_headers("Execute");
    open_headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &open_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(open_response.status(), StatusCode::OK);
    let open_response_rops = response_rops_from_execute_response(open_response).await;
    assert!(contains_bytes(
        &open_response_rops,
        &[0x03, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &open_response_rops,
        &[0x07, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &open_response_rops,
        &utf16z("IPM.Configuration.WorkHours")
    ));
    assert!(contains_bytes(
        &open_response_rops,
        b"Pacific Standard Time"
    ));

    let mut category_open_rops = Vec::new();
    append_rop_open_folder(
        &mut category_open_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_open_message(
        &mut category_open_rops,
        1,
        2,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        category_list_id,
    );
    append_rop_get_properties_specific(
        &mut category_open_rops,
        2,
        &[0x001A_001F, PID_TAG_SUBJECT_W, 0x7C06_0003, 0x7C08_0102],
    );
    let mut category_open_headers = mapi_headers("Execute");
    category_open_headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let category_open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &category_open_headers,
            &execute_body(&rop_buffer(&category_open_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(category_open_response.status(), StatusCode::OK);
    let category_open_response_rops =
        response_rops_from_execute_response(category_open_response).await;
    assert!(contains_bytes(
        &category_open_response_rops,
        &utf16z("IPM.Configuration.CategoryList")
    ));
    assert!(contains_bytes(
        &category_open_response_rops,
        b"Blue Category"
    ));
    assert!(contains_bytes(
        &category_open_response_rops,
        b"renameOnFirstUse"
    ));
}

#[tokio::test]
async fn mapi_over_http_inbox_fai_sync_exports_folder_local_default_view() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&16384u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops)
        .unwrap_or_else(|error| panic!("{error}: {response_rops:02x?}"));
    assert_eq!(stream.message_changes.len(), 1);
    assert!(stream
        .message_changes
        .iter()
        .all(|message| message.associated));

    let mut counters = Vec::new();
    for message in &stream.message_changes {
        assert!(!message.source_key.is_empty(), "{}", message.subject);
        assert!(!message.parent_source_key.is_empty(), "{}", message.subject);
        assert!(!message.entry_id.is_empty(), "{}", message.subject);
        assert_eq!(message.entry_id.len(), 70, "{}", message.subject);
        assert_eq!(&message.entry_id[4..20], account.account_id.as_bytes());
        assert!(contains_bytes(
            &message.entry_id,
            &message.source_key[16..22]
        ));
        assert!(strict_replguid_globset_contains_counter(
            &stream.idset_given,
            &message.source_key[16..22]
        )
        .unwrap());
        let change_number = message.change_number.expect("FAI change number");
        assert!(strict_replguid_globset_contains_counter(
            &stream.cnset_seen_fai,
            &globcnt_bytes(change_number)
        )
        .unwrap());
        counters.push(strict_globcnt_to_u64(&message.source_key[16..22]).unwrap());
    }

    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.FolderDesign.NamedView")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Compact")));
    assert!(!contains_bytes(&response_rops, &utf16z("Messages")));
    assert!(counters.contains(&0x7FFF_FFFF_FFE9));
    for suppressed_counter in [
        0x7FFF_FFFF_FFE3,
        0x7FFF_FFFF_FFED,
        0x7FFF_FFFF_FFF3,
        0x7FFF_FFFF_FFF5,
        0x7FFF_FFFF_FFF8,
        0x7FFF_FFFF_FFFB,
        0x7FFF_FFFF_FFFC,
        0x7FFF_FFFF_FFFD,
    ] {
        assert!(
            !counters.contains(&suppressed_counter),
            "suppressed Inbox FAI counter 0x{suppressed_counter:012x} was emitted"
        );
    }
}

#[tokio::test]
async fn mapi_over_http_open_associated_message_by_imported_source_key_id() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 42,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: Uuid::parse_str("e0fdf7ca-15f8-bc62-ff51-d543d69a14a5").unwrap(),
            account_id: account.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "Outlook Inbox view state".to_string(),
            properties_json: serde_json::json!({
                "0x001a001f": {
                    "type": "string",
                    "value": "IPM.Configuration.MessageListSettings"
                },
                "0x0037001f": {
                    "type": "string",
                    "value": "Outlook Inbox view state"
                },
                "0x1000001f": {
                    "type": "string",
                    "value": "Client view payload"
                },
                "0x65e00102": {
                    "type": "binary",
                    "value": associated_source_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
                }
            }),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let reconnect_cookie = mapi_cookie_header(&reconnect);

    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_open_message(
        &mut open_rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        associated_object_id,
    );
    append_rop_get_properties_specific(&mut open_rops, 2, &[PID_TAG_SUBJECT_W, PID_TAG_BODY_W]);

    let mut open_headers = mapi_headers("Execute");
    open_headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &open_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let open_response_rops = response_rops_from_execute_response(open_response).await;
    assert!(
        contains_bytes(&open_response_rops, &[0x03, 0x02, 0, 0, 0, 0]),
        "{open_response_rops:02x?}"
    );
    assert!(contains_bytes(
        &open_response_rops,
        &[0x07, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &open_response_rops,
        &utf16z("Outlook Inbox view state")
    ));
    assert!(contains_bytes(
        &open_response_rops,
        &utf16z("Client view payload")
    ));
}

#[tokio::test]
async fn mapi_over_http_missing_associated_config_identity_is_not_recreated() {
    let account = FakeStore::account();
    let config_id = Uuid::parse_str("e0fdf7ca-15f8-bc62-ff51-d543d69a14a8").unwrap();
    let config_object_id = crate::mapi::identity::mapi_store_id(302);
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(config_id, config_object_id)]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x001A_001F,
        "IPM.Configuration.MessageListSettings",
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "Recovered view state",
    );
    let stream_body = utf16z("Recovered body stream");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        config_object_id,
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    rops.extend_from_slice(&[0x2B, 0x00, 0x02, 0x03]);
    rops.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[0x2F, 0x00, 0x03]);
    rops.extend_from_slice(&(stream_body.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[0x2D, 0x00, 0x03]);
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(&stream_body);
    rops.extend_from_slice(&[0x5D, 0x00, 0x03]);
    append_rop_get_properties_specific(&mut rops, 2, &[PID_TAG_SUBJECT_W, PID_TAG_BODY_W]);

    let mut headers = mapi_headers("Execute");
    headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x03, 0x02, 0x0f, 0x01, 0x04, 0x80]),
        "{response_rops:02x?}"
    );
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Recovered view state")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Recovered body stream")
    ));
}

#[tokio::test]
async fn mapi_over_http_virtual_associated_config_write_preserves_default_class() {
    let account = FakeStore::account();
    let config_id = Uuid::from_u128(0x6d617069_6d6c_7343_8000_000000000001);
    let config_object_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF8);
    let associated_configs = Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
        id: config_id,
        account_id: account.account_id,
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        subject: "IPM.Configuration.MessageListSettings".to_string(),
        properties_json: serde_json::json!({
            "0x001a001f": {
                "type": "string",
                "value": "IPM.Configuration.MessageListSettings"
            },
            "0x0037001f": {
                "type": "string",
                "value": "IPM.Configuration.MessageListSettings"
            },
            "0x7c060003": {"type": "u32", "value": 4},
            "0x7c070102": {"type": "binary", "value": "3c786d6c2f3e"}
        }),
    }]));
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: associated_configs.clone(),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(config_id, config_object_id)]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let xml_stream = br#"<view-state />"#;
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x7C08_0102, xml_stream);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        config_object_id,
    );
    append_rop_set_properties(&mut rops, 2, 1, &property_values);

    let mut headers = mapi_headers("Execute");
    headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x03, 0x02, 0, 0, 0, 0]));
    assert!(response_rops.contains(&0x0A), "{response_rops:02x?}");

    let configs = associated_configs.lock().unwrap();
    assert_eq!(configs.len(), 1, "{response_rops:02x?}");
    assert_eq!(configs[0].account_id, account.account_id);
    assert_eq!(configs[0].folder_id, crate::mapi::identity::INBOX_FOLDER_ID);
    assert_eq!(
        configs[0].message_class,
        "IPM.Configuration.MessageListSettings"
    );
    assert_eq!(configs[0].subject, "IPM.Configuration.MessageListSettings");
    assert_eq!(
        configs[0].properties_json["0x7c080102"]["value"],
        serde_json::Value::String(
            xml_stream
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        )
    );
    let dictionary = configs[0].properties_json["0x7c070102"]["value"]
        .as_str()
        .expect("dictionary hex");
    assert_ne!(dictionary, "3c786d6c2f3e");
    assert!(dictionary.contains("392d30"), "{dictionary}");
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_to_associated_config_message_succeeds() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 44,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: Uuid::parse_str("e0fdf7ca-15f8-bc62-ff51-d543d69a14a7").unwrap(),
            account_id: account.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "Outlook Inbox view state".to_string(),
            properties_json: serde_json::json!({
                "0x001a001f": {
                    "type": "string",
                    "value": "IPM.Configuration.MessageListSettings"
                },
                "0x0037001f": {
                    "type": "string",
                    "value": "Outlook Inbox view state"
                },
                "0x1000001f": {
                    "type": "string",
                    "value": "Client view payload"
                },
                "0x65e00102": {
                    "type": "binary",
                    "value": associated_source_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
                },
                "0x7c080102": {
                    "type": "binary",
                    "value": "766965772d6578747261"
                }
            }),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let reconnect_cookie = mapi_cookie_header(&reconnect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        associated_object_id,
    );
    rops.extend_from_slice(&[0x4D, 0x00, 0x02, 0x03]);
    rops.push(0);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x4E, 0x00, 0x03]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut headers = mapi_headers("Execute");
    headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4D, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x4010_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x400D_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Outlook Inbox view state")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Client view payload")
    ));
    assert!(contains_bytes(&response_rops, b"view-extra"));
}

#[tokio::test]
async fn mapi_over_http_reads_empty_associated_config_body_stream() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 43,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: Uuid::parse_str("e0fdf7ca-15f8-bc62-ff51-d543d69a14a6").unwrap(),
            account_id: account.account_id,
            folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
            message_class: "IPM.Configuration.MessageListSettings".to_string(),
            subject: "Outlook empty view state".to_string(),
            properties_json: serde_json::json!({
                "0x001a001f": {
                    "type": "string",
                    "value": "IPM.Configuration.MessageListSettings"
                },
                "0x0037001f": {
                    "type": "string",
                    "value": "Outlook empty view state"
                },
                "0x65e00102": {
                    "type": "binary",
                    "value": associated_source_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
                }
            }),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let reconnect_cookie = mapi_cookie_header(&reconnect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        associated_object_id,
    );
    rops.extend_from_slice(&[0x2B, 0x00, 0x02, 0x03]);
    rops.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x2C, 0x00, 0x03]);
    rops.extend_from_slice(&16u16.to_le_bytes());

    let mut headers = mapi_headers("Execute");
    headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x03, 0x02, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x2B, 0x03, 0, 0, 0, 0, 2, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x2C, 0x03, 0, 0, 0, 0, 2, 0, 0, 0]),
        "{response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_save_message_acknowledges_foreign_trash_sync_upload_without_persisting() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let imported_source_key =
        crate::mapi::identity::source_key_for_object_id(crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 1,
        ));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &imported_source_key,
    );
    append_mapi_binary_property(&mut property_values, PID_TAG_CHANGE_KEY, b"change");
    append_mapi_utf16_property(&mut property_values, 0x001A_001F, "IPM.Note");
    append_mapi_utf16_property(&mut property_values, 0x0E1D_001F, "Client trash upload");

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x03, 0x00]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(imported_emails.lock().unwrap().is_empty());
    assert!(!mapi_identities.lock().unwrap().values().any(|object_id| {
        crate::mapi::identity::source_key_for_object_id(*object_id) == imported_source_key
    }));
}

#[tokio::test]
async fn mapi_over_http_replays_outlook_trash_collector_import_then_save() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let imported_source_key =
        crate::mapi::identity::source_key_for_object_id(crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 1,
        ));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());

    let mut import_values = Vec::new();
    append_mapi_binary_property(&mut import_values, PID_TAG_SOURCE_KEY, &imported_source_key);
    append_mapi_i64_property(&mut import_values, PID_TAG_LAST_MODIFICATION_TIME, 0);
    append_mapi_binary_property(&mut import_values, PID_TAG_CHANGE_KEY, b"outlook-change");
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        b"outlook-pcl",
    );
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
    ]);
    for state_tag in [
        META_TAG_IDSET_GIVEN,
        META_TAG_CNSET_SEEN,
        META_TAG_CNSET_SEEN_FAI,
    ] {
        rops.extend_from_slice(&[0x75, 0x00, 0x02]);
        rops.extend_from_slice(&state_tag.to_le_bytes());
        rops.extend_from_slice(&0u32.to_le_bytes());
        rops.extend_from_slice(&[0x77, 0x00, 0x02]);
    }
    rops.extend_from_slice(&[0x72, 0x00, 0x02, 0x03]);
    rops.push(0);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&import_values);
    let mut first_set_values = Vec::new();
    append_mapi_utf16_property(&mut first_set_values, 0x001A_001F, "IPM.Note");
    append_mapi_utf16_property(&mut first_set_values, 0x0037_001F, "Outlook trash upload");
    let mut second_set_values = Vec::new();
    append_mapi_utf16_property(&mut second_set_values, PID_TAG_BODY_W, "Saved after import");
    append_rop_set_properties(&mut rops, 3, 2, &first_set_values);
    append_rop_modify_recipients(&mut rops, 3, &[]);
    append_rop_set_properties(&mut rops, 3, 1, &second_set_values);
    append_rop_save_changes_message(&mut rops, 3, 3);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x00, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_SOURCE_KEY.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x07, 0x00, 0, 0, 0, 0]));

    assert!(imported_emails.lock().unwrap().is_empty());
    assert!(!mapi_identities.lock().unwrap().values().any(|object_id| {
        crate::mapi::identity::source_key_for_object_id(*object_id) == imported_source_key
    }));
}

#[tokio::test]
async fn mapi_over_http_sync_import_delete_and_read_state_use_canonical_store() {
    let read_message_id = "42424242-4242-4242-4242-424242424242";
    let delete_message_id = "43434343-4343-4343-4343-434343434343";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    inbox.unread_emails = 1;
    let mut read_email = FakeStore::email(
        read_message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Read import",
    );
    read_email.unread = true;
    let delete_email = FakeStore::email(
        delete_message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Delete import",
    );
    let emails = Arc::new(Mutex::new(vec![read_email, delete_email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x80, 0x00, 0x02, // RopSynchronizationImportReadStateChanges
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(read_message_id));
    rops.push(1);
    rops.extend_from_slice(&[
        0x74, 0x00, 0x02, // RopSynchronizationImportDeletes
        0x02,
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(delete_message_id));
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x80, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert_content_final_state_includes(
        &response_rops,
        &[Uuid::parse_str(read_message_id).unwrap()],
        &[41],
    );
    assert!(!emails.lock().unwrap()[0].unread);
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[Uuid::parse_str(delete_message_id).unwrap()]
    );
}

#[tokio::test]
async fn mapi_over_http_sync_import_read_state_requires_valid_handle() {
    let message_id = "42424242-4242-4242-4242-424242424250";
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Read import invalid handle",
    );
    email.unread = true;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![
        0x80, 0x00, 0x02, // RopSynchronizationImportReadStateChanges, invalid input handle 2
    ];
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.push(1);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();

    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x80, 0x02, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_sync_import_delete_ignores_transient_trash_artifact() {
    let out_of_range_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 43,
    );
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x74, 0x00, 0x02, // RopSynchronizationImportDeletes
        0x02,
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, out_of_range_object_id);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert!(deleted_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_read_state_ignores_transient_associated_artifact() {
    let out_of_range_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 44,
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x80, 0x00, 0x02, // RopSynchronizationImportReadStateChanges
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, out_of_range_object_id);
    rops.push(1);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x80, 0x02, 0, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_sync_import_hard_delete_reports_partial_when_retention_blocks_delete() {
    let message_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3";
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            &inbox_id.to_string(),
            "inbox",
            "Retained sync import delete",
        )])),
        failed_delete_email_ids: Arc::new(Mutex::new(vec![Uuid::parse_str(message_id).unwrap()])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let canonical_emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x7E, 0x00, 0x01, 0x02, 0x01]);
    rops.extend_from_slice(&[0x74, 0x00, 0x02, 0x02]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(
        contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 1]),
        "{response_rops:02x?}"
    );
    assert!(deleted_emails.lock().unwrap().is_empty());
    assert!(canonical_emails
        .lock()
        .unwrap()
        .iter()
        .any(|email| email.id == Uuid::parse_str(message_id).unwrap()));
}

#[tokio::test]
async fn mapi_over_http_sync_import_soft_delete_moves_to_trash() {
    let message_id = "45454545-4545-4545-4545-454545454545";
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            &inbox_id.to_string(),
            "inbox",
            "Soft delete import",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x74, 0x00, 0x02, // RopSynchronizationImportDeletes
        0x00,
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert_content_final_state_includes(&response_rops, &[], &[]);
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(Uuid::parse_str(message_id).unwrap(), trash_id)]
    );
}

#[tokio::test]
async fn mapi_over_http_sync_import_delete_from_trash_child_hard_deletes() {
    let message_id = "46464646-4646-4646-8646-464646464646";
    let trash_id = Uuid::parse_str("77777777-7777-4777-8777-777777777790").unwrap();
    let child_id = Uuid::parse_str("77777777-7777-4777-8777-777777777791").unwrap();
    let trash = FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted");
    let mut child = FakeStore::mailbox(&child_id.to_string(), "", "Child");
    child.parent_id = Some(trash_id);
    let child_folder_id = test_mapi_folder_id(0x1791);
    crate::mapi::identity::remember_mapi_identity(child_id, child_folder_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![trash, child])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            &child_id.to_string(),
            "",
            "Trash child sync delete",
        )])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(child_id, child_folder_id);
    let deleted_emails = store.deleted_emails.clone();
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, child_folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x7E, 0x00, 0x01, 0x02, 0x01]);
    rops.extend_from_slice(&[0x74, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[Uuid::parse_str(message_id).unwrap()]
    );
    assert!(moved_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_move_uses_canonical_store() {
    let message_id = "44444444-4444-4444-4444-444444444444";
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let archive = FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "archive", "Archive");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Move import",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ARCHIVE_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x78, 0x00, 0x02, // RopSynchronizationImportMessageMove
    ]);
    rops.extend_from_slice(&8u32.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(&8u32.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x78, 0x02, 0, 0, 0, 0]));
    assert_content_final_state_includes(&response_rops, &[], &[]);
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(
            Uuid::parse_str(message_id).unwrap(),
            Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap()
        )]
    );
}

#[tokio::test]
async fn mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox() {
    let parent_id = Uuid::parse_str("99999999-9999-4999-9999-999999999999").unwrap();
    let parent_folder_id = test_mapi_folder_id(0x1999);
    crate::mapi::identity::remember_mapi_identity(parent_id, parent_folder_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox(&parent_id.to_string(), "custom", "Projects"),
        ])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(parent_id, parent_folder_id);
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(
        &mut hierarchy_values,
        0x65E1_0102,
        &mapi_mailstore::source_key_for_store_id(parent_folder_id),
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        0x65E0_0102,
        b"local-folder-source-key",
    );
    append_mapi_i64_property(&mut hierarchy_values, 0x3008_0040, 0);
    append_mapi_binary_property(&mut hierarchy_values, 0x65E2_0102, b"change-key");
    append_mapi_binary_property(&mut hierarchy_values, 0x65E3_0102, b"pcl");
    append_mapi_utf16_property(&mut hierarchy_values, 0x3001_001F, "Imported Sync Folder");

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "Imported Sync Folder");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x73, 0x02, 0, 0, 0, 0]));
    assert_eq!(
        created_mailboxes.lock().unwrap()[0].name,
        "Imported Sync Folder"
    );
    assert_eq!(
        created_mailboxes.lock().unwrap()[0].parent_id,
        Some(parent_id)
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state() {
    let parent_id = Uuid::parse_str("99999999-9999-4999-9999-999999999998").unwrap();
    let parent_folder_id = test_mapi_folder_id(0x1998);
    crate::mapi::identity::remember_mapi_identity(parent_id, parent_folder_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox(&parent_id.to_string(), "custom", "Projects"),
        ])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(parent_id, parent_folder_id);
    let service = ExchangeService::new(store.clone());
    let created_mailboxes = store.created_mailboxes.clone();
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PARENT_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(parent_folder_id),
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_SOURCE_KEY,
        b"ms-oxcfxics-4-1-1-folder",
    );
    append_mapi_i64_property(&mut hierarchy_values, PID_TAG_LAST_MODIFICATION_TIME, 0);
    append_mapi_binary_property(&mut hierarchy_values, PID_TAG_CHANGE_KEY, b"change-key");
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        b"pcl",
    );
    append_mapi_utf16_property(
        &mut hierarchy_values,
        PID_TAG_DISPLAY_NAME_W,
        "MS-OXCFXICS 4.1.1 Folder",
    );

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_DISPLAY_NAME_W,
        "MS-OXCFXICS 4.1.1 Folder",
    );

    let mut upload_rops = Vec::new();
    append_rop_open_folder(&mut upload_rops, 0, 1, test_mapi_folder_id(5));
    upload_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
    ]);
    upload_rops.push(0x02); // hierarchy sync
    upload_rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    upload_rops.extend_from_slice(&META_TAG_CNSET_SEEN.to_le_bytes());
    upload_rops.extend_from_slice(&0u32.to_le_bytes());
    upload_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    upload_rops.extend_from_slice(&6u16.to_le_bytes());
    upload_rops.extend_from_slice(&hierarchy_values);
    upload_rops.extend_from_slice(&1u16.to_le_bytes());
    upload_rops.extend_from_slice(&property_values);
    upload_rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    upload_rops.extend_from_slice(&4096u16.to_le_bytes());

    let upload_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &upload_rops,
                &[1, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(upload_response.status(), StatusCode::OK);
    let upload_response_rops = response_rops_from_execute_response(upload_response).await;
    assert!(
        contains_bytes(&upload_response_rops, &[0x73, 0x02, 0, 0, 0, 0]),
        "{upload_response_rops:02x?}"
    );
    assert!(contains_bytes(
        &upload_response_rops,
        &[0x82, 0x03, 0, 0, 0, 0]
    ));
    let upload_state_chunks = mapi_fast_transfer_chunks(&upload_response_rops);
    assert_eq!(upload_state_chunks.len(), 1);
    assert!(contains_bytes(
        &upload_state_chunks[0].1,
        &FX_INCR_SYNC_STATE_BEGIN.to_le_bytes()
    ));
    assert!(contains_bytes(
        &upload_state_chunks[0].1,
        &FX_INCR_SYNC_STATE_END.to_le_bytes()
    ));
    assert!(contains_bytes(
        &upload_state_chunks[0].1,
        &META_TAG_IDSET_GIVEN.to_le_bytes()
    ));
    assert!(contains_bytes(
        &upload_state_chunks[0].1,
        &META_TAG_CNSET_SEEN.to_le_bytes()
    ));
    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].name, "MS-OXCFXICS 4.1.1 Folder");
    assert_eq!(created[0].parent_id, Some(parent_id));
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_1_2_hierarchy_delete_returns_transfer_state() {
    let folder_id = Uuid::parse_str("88888888-8888-4888-8888-888888888812").unwrap();
    let folder_mapi_id = test_mapi_uuid_id(&folder_id);
    crate::mapi::identity::remember_mapi_identity(folder_id, folder_mapi_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox(&folder_id.to_string(), "custom", "MS-OXCFXICS 4.1.2 Folder"),
        ])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(folder_id, folder_mapi_id);
    let destroyed_mailboxes = store.destroyed_mailboxes.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
    ]);
    rops.push(0x02); // hierarchy sync
    rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&META_TAG_CNSET_SEEN.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x74, 0x00, 0x02, // RopSynchronizationImportDeletes
        0x02, // hard delete
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, folder_mapi_id);
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));
    let state_chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(state_chunks.len(), 1);
    assert!(contains_bytes(
        &state_chunks[0].1,
        &FX_INCR_SYNC_STATE_BEGIN.to_le_bytes()
    ));
    assert!(contains_bytes(
        &state_chunks[0].1,
        &FX_INCR_SYNC_STATE_END.to_le_bytes()
    ));
    assert_eq!(destroyed_mailboxes.lock().unwrap().as_slice(), &[folder_id]);
}

#[tokio::test]
async fn mapi_over_http_sync_import_hierarchy_change_acknowledges_system_folder_reconciliation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(&mut hierarchy_values, 0x65E1_0102, &[]);
    append_mapi_binary_property(&mut hierarchy_values, 0x65E0_0102, b"system-source-key");
    append_mapi_utf16_property(&mut hierarchy_values, 0x3001_001F, "Sync Issues");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x73, 0x02, 0, 0, 0, 0]));
    assert!(created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_microsoft_failed_set_columns_invalidates_table_until_success() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "85858585-8585-8585-8585-858585858585",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Invalidated columns",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();

    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, test_mapi_folder_id(5));
    open_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    open_rops.extend_from_slice(&1u16.to_le_bytes());
    open_rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    open_rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows succeeds with the valid columns.
    ]);
    open_rops.extend_from_slice(&1u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(open_response.status(), StatusCode::OK);
    let open_cookie = mapi_cookie_header(&open_response);
    let open_body = response_bytes(open_response).await;
    let (open_response_rops, open_handles) =
        response_rops_and_handles_from_execute_body(&open_body);
    assert!(contains_bytes(
        &open_response_rops,
        &[0x12, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &open_response_rops,
        &utf16z("Invalidated columns")
    ));

    let mut invalid_rops = vec![0x12, 0x00, 0x02, 0x00]; // RopSetColumns
    invalid_rops.extend_from_slice(&1u16.to_le_bytes());
    invalid_rops.extend_from_slice(&0x0037_0000u32.to_le_bytes()); // PT_UNSPECIFIED is invalid.
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&open_cookie).unwrap());
    let invalid_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&invalid_rops, &open_handles)),
        )
        .await
        .unwrap();
    assert_eq!(invalid_response.status(), StatusCode::OK);
    let invalid_cookie = mapi_cookie_header(&invalid_response);
    let invalid_body = response_bytes(invalid_response).await;
    let (invalid_response_rops, invalid_handles) =
        response_rops_and_handles_from_execute_body(&invalid_body);
    assert!(contains_bytes(
        &invalid_response_rops,
        &[0x12, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));

    let mut query_rops = vec![0x15, 0x00, 0x02, 0x00, 0x01]; // RopQueryRows
    query_rops.extend_from_slice(&1u16.to_le_bytes());
    query_rops.extend_from_slice(&[
        0x12, 0x00, 0x02, 0x00, // RopSetColumns repairs the invalidated table.
    ]);
    query_rops.extend_from_slice(&1u16.to_le_bytes());
    query_rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    query_rops.extend_from_slice(&[
        0x18, 0x00, 0x02, 0x00, // RopSeekRow to BOOKMARK_BEGINNING.
    ]);
    query_rops.extend_from_slice(&0i32.to_le_bytes());
    query_rops.push(0);
    query_rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows succeeds again.
    ]);
    query_rops.extend_from_slice(&1u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&invalid_cookie).unwrap());
    let query_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&query_rops, &invalid_handles)),
        )
        .await
        .unwrap();
    assert_eq!(query_response.status(), StatusCode::OK);
    let query_response_rops = response_rops_from_execute_response(query_response).await;
    assert!(contains_bytes(
        &query_response_rops,
        &[0x15, 0x02, 0xB9, 0x04, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &query_response_rops,
        &[0x12, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &query_response_rops,
        &[0x18, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &query_response_rops,
        &utf16z("Invalidated columns")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_failed_sort_and_restrict_invalidate_table_until_success() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "85858585-8585-8585-8585-858585858586",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Invalidated sort and restriction",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();

    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, test_mapi_folder_id(5));
    open_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    open_rops.extend_from_slice(&1u16.to_le_bytes());
    open_rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(open_response.status(), StatusCode::OK);
    let open_cookie = mapi_cookie_header(&open_response);
    let open_body = response_bytes(open_response).await;
    let (open_response_rops, open_handles) =
        response_rops_and_handles_from_execute_body(&open_body);
    assert!(contains_bytes(
        &open_response_rops,
        &[0x12, 0x02, 0, 0, 0, 0]
    ));

    let mut invalid_sort_rops = vec![0x13, 0x00, 0x02, 0x00]; // RopSortTable
    invalid_sort_rops.extend_from_slice(&2u16.to_le_bytes());
    invalid_sort_rops.extend_from_slice(&0u16.to_le_bytes());
    invalid_sort_rops.extend_from_slice(&0u16.to_le_bytes());
    invalid_sort_rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    invalid_sort_rops.push(0x01);
    invalid_sort_rops.extend_from_slice(&0x0E06_0040u32.to_le_bytes());
    invalid_sort_rops.push(0x04); // MaximumCategory without a preceding category.
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&open_cookie).unwrap());
    let invalid_sort_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&invalid_sort_rops, &open_handles)),
        )
        .await
        .unwrap();
    let invalid_sort_cookie = mapi_cookie_header(&invalid_sort_response);
    let invalid_sort_body = response_bytes(invalid_sort_response).await;
    let (invalid_sort_response_rops, invalid_sort_handles) =
        response_rops_and_handles_from_execute_body(&invalid_sort_body);
    assert!(contains_bytes(
        &invalid_sort_response_rops,
        &[0x13, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));

    let mut repair_sort_rops = vec![0x15, 0x00, 0x02, 0x00, 0x01]; // RopQueryRows fails while sort is invalid.
    repair_sort_rops.extend_from_slice(&1u16.to_le_bytes());
    repair_sort_rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable repairs the invalidated sort.
    ]);
    repair_sort_rops.extend_from_slice(&1u16.to_le_bytes());
    repair_sort_rops.extend_from_slice(&0u16.to_le_bytes());
    repair_sort_rops.extend_from_slice(&0u16.to_le_bytes());
    repair_sort_rops.extend_from_slice(&0x0E06_0040u32.to_le_bytes());
    repair_sort_rops.push(0x01);
    repair_sort_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    repair_sort_rops.extend_from_slice(&1u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&invalid_sort_cookie).unwrap(),
    );
    let repair_sort_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&repair_sort_rops, &invalid_sort_handles)),
        )
        .await
        .unwrap();
    let repair_sort_cookie = mapi_cookie_header(&repair_sort_response);
    let repair_sort_body = response_bytes(repair_sort_response).await;
    let (repair_sort_response_rops, repair_sort_handles) =
        response_rops_and_handles_from_execute_body(&repair_sort_body);
    assert!(contains_bytes(
        &repair_sort_response_rops,
        &[0x15, 0x02, 0xB9, 0x04, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &repair_sort_response_rops,
        &[0x13, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &repair_sort_response_rops,
        &utf16z("Invalidated sort and restriction")
    ));

    let invalid_restrict_rops = vec![
        0x14, 0x00, 0x02, 0x02, 0x00, 0x00, // RopRestrict with invalid async flags.
    ];
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&repair_sort_cookie).unwrap(),
    );
    let invalid_restrict_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&invalid_restrict_rops, &repair_sort_handles)),
        )
        .await
        .unwrap();
    let invalid_restrict_cookie = mapi_cookie_header(&invalid_restrict_response);
    let invalid_restrict_body = response_bytes(invalid_restrict_response).await;
    let (invalid_restrict_response_rops, invalid_restrict_handles) =
        response_rops_and_handles_from_execute_body(&invalid_restrict_body);
    assert!(contains_bytes(
        &invalid_restrict_response_rops,
        &[0x14, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));

    let mut repair_restrict_rops = vec![0x15, 0x00, 0x02, 0x00, 0x01]; // RopQueryRows fails while restriction is invalid.
    repair_restrict_rops.extend_from_slice(&1u16.to_le_bytes());
    repair_restrict_rops.extend_from_slice(&[
        0x14, 0x00, 0x02, 0x00, 0x00, 0x00, // RopRestrict repairs with no restriction.
        0x18, 0x00, 0x02, 0x00, // RopSeekRow to BOOKMARK_BEGINNING.
    ]);
    repair_restrict_rops.extend_from_slice(&0i32.to_le_bytes());
    repair_restrict_rops.push(0);
    repair_restrict_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    repair_restrict_rops.extend_from_slice(&1u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&invalid_restrict_cookie).unwrap(),
    );
    let repair_restrict_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &repair_restrict_rops,
                &invalid_restrict_handles,
            )),
        )
        .await
        .unwrap();
    let repair_restrict_response_rops =
        response_rops_from_execute_response(repair_restrict_response).await;
    assert!(contains_bytes(
        &repair_restrict_response_rops,
        &[0x15, 0x02, 0xB9, 0x04, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &repair_restrict_response_rops,
        &[0x14, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &repair_restrict_response_rops,
        &[0x18, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &repair_restrict_response_rops,
        &utf16z("Invalidated sort and restriction")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance(
) {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555507").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343491").unwrap();
    let search_folder_mapi_id = test_mapi_uuid_id(&search_folder_id);
    crate::mapi::identity::remember_mapi_identity(search_folder_id, search_folder_mapi_id);
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: search_folder_id,
            account_id: account.account_id,
            role: "custom".to_string(),
            display_name: "High importance mail".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({}),
            restriction_json: serde_json::json!({}),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        }])),
        ..Default::default()
    };
    let stored_search_folders = store.search_folders.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut restriction = vec![0x00];
    restriction.extend_from_slice(&2u16.to_le_bytes());
    restriction.push(0x00);
    restriction.extend_from_slice(&7u16.to_le_bytes());
    for (message_class, fuzzy_level_low) in [
        ("IPM.Appointment", 0x0002u16),
        ("IPM.Contact", 0x0002),
        ("IPM.DistList", 0x0002),
        ("IPM.Activity", 0x0002),
        ("IPM.StickyNote", 0x0002),
        ("IPM.Task", 0x0000),
        ("IPM.Task.", 0x0002),
    ] {
        restriction.extend_from_slice(&[0x02, 0x03]);
        restriction.extend_from_slice(&fuzzy_level_low.to_le_bytes());
        restriction.extend_from_slice(&0x0001u16.to_le_bytes());
        restriction.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
        append_mapi_utf16_property(&mut restriction, 0x001A_001F, message_class);
    }
    restriction.push(0x00);
    restriction.extend_from_slice(&1u16.to_le_bytes());
    append_search_property_u32(&mut restriction, 0x0017_0003, 0x04, 2);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[test_mapi_folder_id(5)],
        0x0002_002A,
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x30, 0x01, 0, 0, 0, 0]));
    {
        let stored = stored_search_folders.lock().unwrap();
        assert_eq!(
            stored[0].restriction_json,
            serde_json::json!({
                "kind": "mapi_bounded",
                "all": [
                    {"field": "messageClass", "notPrefix": "IPM.Appointment"},
                    {"field": "messageClass", "notPrefix": "IPM.Contact"},
                    {"field": "messageClass", "notPrefix": "IPM.DistList"},
                    {"field": "messageClass", "notPrefix": "IPM.Activity"},
                    {"field": "messageClass", "notPrefix": "IPM.StickyNote"},
                    {"field": "messageClass", "notEquals": "IPM.Task"},
                    {"field": "messageClass", "notPrefix": "IPM.Task."},
                    {"field": "importance", "equals": 2}
                ]
            })
        );
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut get_rops = Vec::new();
    append_rop_open_folder(&mut get_rops, 0, 1, search_folder_mapi_id);
    get_rops.extend_from_slice(&[0x31, 0x00, 0x01, 0x01, 0x01, 0x00]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&get_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x31, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x001A_001Fu32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(
        &response_rops,
        &0x0017_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &2u32.to_le_bytes()));
    let get_offset = response_rops
        .windows(6)
        .position(|window| window == [0x31, 0x01, 0, 0, 0, 0])
        .unwrap();
    let restriction_size = u16::from_le_bytes(
        response_rops[get_offset + 6..get_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    assert_eq!(restriction_size, 0x0129);
    assert_eq!(
        &response_rops[get_offset + 8..get_offset + 8 + restriction_size],
        restriction.as_slice()
    );
    assert_eq!(response_rops[get_offset + 8 + restriction_size], 0);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[get_offset + 9 + restriction_size..get_offset + 11 + restriction_size]
                .try_into()
                .unwrap()
        ),
        0
    );

    renew_mapi_request_id(&mut execute_headers);
    let mut get_with_folders_rops = Vec::new();
    append_rop_open_folder(&mut get_with_folders_rops, 0, 1, search_folder_mapi_id);
    append_rop_get_search_criteria(&mut get_with_folders_rops, 1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&get_with_folders_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x31, 0x01, 0, 0, 0, 0]));
    let get_offset = response_rops
        .windows(6)
        .position(|window| window == [0x31, 0x01, 0, 0, 0, 0])
        .unwrap();
    let restriction_size = u16::from_le_bytes(
        response_rops[get_offset + 6..get_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    assert_eq!(restriction_size, 0x0129);
    assert_eq!(
        &response_rops[get_offset + 8..get_offset + 8 + restriction_size],
        restriction.as_slice()
    );
    let folders_offset = get_offset + 9 + restriction_size;
    assert_eq!(
        u16::from_le_bytes(
            response_rops[folders_offset..folders_offset + 2]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        &response_rops[folders_offset + 2..folders_offset + 10],
        &crate::mapi::identity::wire_id_bytes_from_object_id(test_mapi_folder_id(5)).unwrap()
    );

    renew_mapi_request_id(&mut execute_headers);
    let mut get_string8_rops = Vec::new();
    append_rop_open_folder(&mut get_string8_rops, 0, 1, search_folder_mapi_id);
    get_string8_rops.extend_from_slice(&[0x31, 0x00, 0x01, 0x00, 0x01, 0x00]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&get_string8_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x31, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x001A_001Eu32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, b"IPM.Appointment\0"));
    assert!(contains_bytes(
        &response_rops,
        &0x0017_0003u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_unknown_sync_type_terminates_current_buffer() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x70, 0x00, 0x01, 0x02]); // RopSynchronizationConfigure
    rops.extend_from_slice(&[0x99, 0x00]); // Unknown sync type, SendOptions.
    rops.extend_from_slice(&0u16.to_le_bytes()); // SynchronizationFlags.
    rops.extend_from_slice(&0u16.to_le_bytes()); // RestrictionDataSize.
    rops.extend_from_slice(&0u32.to_le_bytes()); // SynchronizationExtraFlags.
    rops.extend_from_slice(&0u16.to_le_bytes()); // PropertyTagCount.
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_unknown_fasttransfer_marker_terminates_current_buffer() {
    let mut rops = vec![0x54, 0x00, 0x01]; // RopFastTransferDestinationPutBuffer
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x54, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}
