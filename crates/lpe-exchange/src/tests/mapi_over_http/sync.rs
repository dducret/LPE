use super::*;

fn append_rop_sync_import_deletes(
    rops: &mut Vec<u8>,
    input_handle_index: u8,
    import_delete_flags: u8,
    object_ids: &[u64],
) {
    rops.extend_from_slice(&[0x74, 0x00, input_handle_index, import_delete_flags]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0000_1102u32.to_le_bytes());
    rops.extend_from_slice(&(object_ids.len() as u32).to_le_bytes());
    for object_id in object_ids {
        let source_key = crate::mapi::identity::source_key_for_object_id(*object_id);
        rops.extend_from_slice(&(source_key.len() as u16).to_le_bytes());
        rops.extend_from_slice(&source_key);
    }
}

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
        &utf16z("IPM.Microsoft.FolderDesign.NamedView")
    ));
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
async fn mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai() {
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
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN | OrderByDeliveryTime
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
        &utf16z("IPM.Microsoft.FolderDesign.NamedView")
    ));
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
    let mail_folder_type = [
        0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(&mut property_values, PID_TAG_SUBJECT_W, "Persisted Inbox");
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_WLINK_ENTRY_ID,
        &inbox_entry_id,
    );
    append_mapi_i32_property(&mut property_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(&mut property_values, 0x684A_0003, 0);
    append_mapi_i32_property(&mut property_values, 0x6847_0003, 1_537_819_608);
    append_mapi_i32_property(&mut property_values, 0x6852_0003, 1);
    append_mapi_binary_property(&mut property_values, PID_TAG_WLINK_ORDINAL, &[0x89]);
    append_mapi_binary_property(&mut property_values, PID_TAG_WLINK_GROUP_CLSID, &[0x11; 16]);
    append_mapi_utf16_property(&mut property_values, PID_TAG_WLINK_GROUP_NAME_W, "Custom");
    append_mapi_binary_property(&mut property_values, 0x684F_0102, &mail_folder_type);

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 11, &property_values);
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
    assert_eq!(stored[0].ordinal, vec![0x89]);
    assert_eq!(
        stored[0].group_header_id,
        Some(Uuid::from_bytes([0x11; 16]))
    );
    assert_eq!(stored[0].group_name, "Custom");
}

#[tokio::test]
async fn mapi_over_http_common_views_rejects_incomplete_wlink_without_synthetic_defaults() {
    // [MS-OXOCFG] sections 3.1.4.10.1 and 3.1.4.10.2 enumerate the
    // properties of group-header and shortcut WLinks. Saving only the class
    // and subject must not invent type=2, section=0, group="Mail", or ordinal=01.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account),
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(&mut property_values, PID_TAG_SUBJECT_W, "Incomplete WLink");
    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 2, &property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x01, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(
        contains_bytes(&response_rops, &[0x0C, 0x01, 0x0F, 0x01, 0x04, 0x80]),
        "{response_rops:02x?}"
    );
    assert!(shortcuts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_common_views_accepts_outlook_calendar_group_header_without_group_name() {
    // RR 202607141822 and logs/LPE_last_202607141822.log lines 308-309:
    // Outlook saves My Calendars with the normative PtypBinary
    // PidTagWlinkGroupHeaderID and PidTagWlinkFolderType properties, but no
    // PidTagWlinkGroupName. [MS-OXOCFG] sections 2.2.9.3, 2.2.9.11, and
    // 3.1.4.10.1 define that exact group-header shape.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account),
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

    let group_id = Uuid::parse_str("b7f00600-0000-0000-c000-000000000046").unwrap();
    let calendar_folder_type = [
        0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "My Calendars",
    );
    append_mapi_binary_property(&mut property_values, 0x6842_0102, group_id.as_bytes());
    append_mapi_i32_property(&mut property_values, 0x6847_0003, 0x4F30_48F7);
    append_mapi_i32_property(&mut property_values, PID_TAG_WLINK_TYPE, 4);
    append_mapi_i32_property(&mut property_values, 0x684A_0003, 0);
    append_mapi_binary_property(&mut property_values, PID_TAG_WLINK_ORDINAL, &[0x7F]);
    append_mapi_binary_property(&mut property_values, 0x684F_0102, &calendar_folder_type);
    append_mapi_i32_property(&mut property_values, 0x6852_0003, 3);

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 9, &property_values);
    append_rop_save_changes_message_with_flags(&mut rops, 0, 1, 0x08);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let stored = shortcuts.lock().unwrap();

    assert!(
        contains_bytes(&response_rops, &[0x0C, 0x00, 0, 0, 0, 0]),
        "Outlook's group header must save without PidTagWlinkGroupName: {response_rops:02x?}"
    );
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].subject, "My Calendars");
    assert_eq!(stored[0].group_name, "My Calendars");
    assert_eq!(stored[0].group_header_id, Some(group_id));
    assert_eq!(stored[0].target_folder_id, None);
}

#[tokio::test]
async fn mapi_over_http_common_views_online_create_ignores_client_source_key_in_postgresql(
) -> anyhow::Result<()> {
    // [MS-OXCFXICS] section 3.3.5.2.1 reserves client-chosen local IDs for
    // ImportMessageChange, while section 3.3.5.2.2 requires the server to
    // allocate the identity for an online ROP create. The SourceKey below is
    // deliberately valid and reserved, but must remain only an ignored
    // read-only property on this RopCreateMessage/RopSaveChangesMessage path.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let reserved_start = storage
        .reserve_mapi_local_replica_ids(fixture.account_id, 0x0001_0000)
        .await?;
    let colliding_id = crate::mapi::identity::mapi_store_id(reserved_start + 0x0200);
    let colliding_source_key = crate::mapi::identity::source_key_for_object_id(colliding_id);
    storage
        .upsert_mapi_special_folder_aliases(
            fixture.account_id,
            &[MapiSpecialFolderAlias {
                alias_folder_id: colliding_id,
                canonical_folder_id: crate::mapi::identity::JUNK_FOLDER_ID,
                source_key: colliding_source_key.clone(),
            }],
        )
        .await?;

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let inbox_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        fixture.account_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    )
    .unwrap();
    let mail_folder_type = [
        0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &colliding_source_key,
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "Atomic native WLink",
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_WLINK_ENTRY_ID,
        &inbox_entry_id,
    );
    append_mapi_i32_property(&mut property_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(&mut property_values, 0x684A_0003, 0);
    append_mapi_i32_property(&mut property_values, 0x6847_0003, 1_537_819_608);
    append_mapi_i32_property(&mut property_values, 0x6852_0003, 1);
    append_mapi_binary_property(&mut property_values, PID_TAG_WLINK_ORDINAL, &[0x89]);
    append_mapi_binary_property(&mut property_values, PID_TAG_WLINK_GROUP_CLSID, &[0x11; 16]);
    append_mapi_utf16_property(&mut property_values, PID_TAG_WLINK_GROUP_NAME_W, "Atomic");
    append_mapi_binary_property(&mut property_values, 0x684F_0102, &mail_folder_type);
    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 12, &property_values);
    append_rop_save_changes_message_with_flags(&mut rops, 0, 1, 0x01);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await?;
    let response_rops = response_rops_from_execute_response(response).await;
    let save_succeeded = contains_bytes(&response_rops, &[0x0C, 0x00, 0, 0, 0, 0]);
    let saved_content = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_navigation_shortcuts
        WHERE account_id = $1 AND subject = 'Atomic native WLink'
        "#,
    )
    .bind(fixture.account_id)
    .fetch_one(storage.pool())
    .await?;
    let saved_identity = sqlx::query_as::<_, (i64, Vec<u8>)>(
        r#"
        SELECT identity.mapi_object_id, identity.source_key
        FROM mapi_object_identities identity
        JOIN mapi_navigation_shortcuts shortcut
          ON shortcut.tenant_id = identity.tenant_id
         AND shortcut.account_id = identity.account_id
         AND shortcut.id = identity.canonical_id
        WHERE identity.account_id = $1
          AND identity.object_kind = 'navigation_shortcut'
          AND shortcut.subject = 'Atomic native WLink'
        "#,
    )
    .bind(fixture.account_id)
    .fetch_optional(storage.pool())
    .await?;
    let saved_changes = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE account_id = $1 AND object_kind = 'navigation_shortcut'
        "#,
    )
    .bind(fixture.account_id)
    .fetch_one(storage.pool())
    .await?;
    fixture.cleanup().await?;

    assert!(save_succeeded);
    assert_eq!((saved_content, saved_changes), (1, 1));
    let (saved_object_id, saved_source_key) =
        saved_identity.expect("online WLink content and identity commit atomically");
    assert_ne!(saved_object_id as u64, colliding_id);
    assert_ne!(saved_source_key, colliding_source_key);
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_online_common_views_wlink_accepts_later_ics_update_without_local_reservation(
) -> anyhow::Result<()> {
    // [MS-OXCFXICS] sections 3.3.5.2.1 and 3.3.5.2.2 distinguish an
    // imported client-chosen local ID from the server-assigned identity of an
    // online RopCreateMessage. Section 3.2.5.9.4.2 then permits a later
    // ImportMessageChange to update the downloaded server object without a
    // GetLocalReplicaIds reservation for that already-durable identity.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let contacts_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        fixture.account_id,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
    )
    .ok_or_else(|| anyhow::anyhow!("Contacts EntryID could not be encoded"))?;
    let group_id = Uuid::parse_str("b7f00600-0000-0000-c000-000000000046")?;
    let contacts_folder_type = [
        0x01, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let mut create_values = Vec::new();
    append_mapi_utf16_property(
        &mut create_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(
        &mut create_values,
        PID_TAG_SUBJECT_W,
        "Online Contacts WLink",
    );
    append_mapi_binary_property(
        &mut create_values,
        PID_TAG_WLINK_ENTRY_ID,
        &contacts_entry_id,
    );
    append_mapi_i32_property(&mut create_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(&mut create_values, 0x684A_0003, 0x0010_0000);
    append_mapi_i32_property(&mut create_values, 0x6847_0003, 1_537_819_608);
    append_mapi_i32_property(&mut create_values, 0x6852_0003, 4);
    append_mapi_binary_property(&mut create_values, PID_TAG_WLINK_ORDINAL, &[0x7F]);
    append_mapi_binary_property(
        &mut create_values,
        PID_TAG_WLINK_GROUP_CLSID,
        group_id.as_bytes(),
    );
    append_mapi_utf16_property(
        &mut create_values,
        PID_TAG_WLINK_GROUP_NAME_W,
        "My Contacts",
    );
    append_mapi_binary_property(&mut create_values, 0x684F_0102, &contacts_folder_type);
    let mut create_rops = Vec::new();
    append_rop_create_associated_message(
        &mut create_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut create_rops, 1, 11, &create_values);
    append_rop_save_changes_message_with_flags(&mut create_rops, 0, 1, 0x01);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&create_rops, &[1, u32::MAX])),
        )
        .await?;
    let create_response = response_rops_from_execute_response(response).await;
    let save_offset = create_response
        .windows(6)
        .position(|window| window == [0x0C, 0x00, 0, 0, 0, 0])
        .ok_or_else(|| anyhow::anyhow!("online WLink SaveChangesMessage failed"))?;
    let online_message_id = crate::mapi::identity::object_id_from_wire_id(
        &create_response[save_offset + 7..save_offset + 15],
    )
    .ok_or_else(|| anyhow::anyhow!("online WLink SaveChangesMessage omitted its MID"))?;
    let source_counter = crate::mapi::identity::global_counter_from_store_id(online_message_id)
        .ok_or_else(|| anyhow::anyhow!("online WLink MID is not a store ID"))?;
    let online_canonical_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT canonical_id
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'navigation_shortcut'
          AND mapi_object_id = $2
          AND deleted_at IS NULL
        "#,
    )
    .bind(fixture.account_id)
    .bind(online_message_id as i64)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM mapi_local_replica_id_ranges
            WHERE account_id = $1
              AND replica_guid = $2
              AND first_global_counter <= $3
              AND end_global_counter_exclusive > $3
            "#,
        )
        .bind(fixture.account_id)
        .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
        .bind(source_counter as i64)
        .fetch_one(storage.pool())
        .await?,
        0,
        "an online-created WLink identity is server-assigned, not locally reserved"
    );

    let mut download_rops = Vec::new();
    append_rop_open_folder(
        &mut download_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    download_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure.
        0x01, 0x00, 0x39, 0xA1, // Content sync, observed Outlook FAI flags.
        0x00, 0x00, // RestrictionDataSize.
        0x0D, 0x00, 0x00, 0x00, // Eid | CN | OrderByDeliveryTime.
        0x00, 0x00, // PropertyTagCount.
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer.
    ]);
    download_rops.extend_from_slice(&31_680u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&download_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let download_response = response_rops_from_execute_response(response).await;
    let download = strict_content_sync_transfer_from_response(&download_response)
        .map_err(anyhow::Error::msg)?;
    let downloaded = download
        .message_changes
        .iter()
        .find(|message| message.subject == "Online Contacts WLink")
        .ok_or_else(|| anyhow::anyhow!("online WLink was not downloaded through ICS"))?;
    assert!(downloaded.associated);
    assert_eq!(downloaded.mid, Some(online_message_id));
    assert_eq!(
        downloaded.source_key,
        crate::mapi::identity::source_key_for_object_id(online_message_id)
    );
    let initial_change_number = downloaded
        .change_number
        .ok_or_else(|| anyhow::anyhow!("downloaded WLink omitted its change number"))?;
    let initial_change_key = downloaded.change_key.clone();
    let initial_predecessor_change_list = downloaded.predecessor_change_list.clone();
    let imported_change_key = vec![
        0x51, 0xA1, 0x66, 0x72, 0x14, 0x93, 0x5C, 0x48, 0xAA, 0x14, 0xE7, 0xDC, 0xB0, 0x5E, 0x0D,
        0xA6, 0x00, 0x00, 0x04, 0x21,
    ];
    let mut predecessor_keys = vec![initial_change_key.clone(), imported_change_key.clone()];
    predecessor_keys.sort_by(|left, right| left[..16].cmp(&right[..16]));
    let mut imported_predecessor_change_list = Vec::new();
    for predecessor in predecessor_keys {
        imported_predecessor_change_list.push(predecessor.len() as u8);
        imported_predecessor_change_list.extend_from_slice(&predecessor);
    }
    let imported_last_modification_time = downloaded
        .last_modification_time
        .ok_or_else(|| anyhow::anyhow!("downloaded WLink omitted its modification time"))?
        + 10_000_000;

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // OpenCollector, contents.
    ]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let response_body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&response_body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7E, 0x02, 0, 0, 0, 0]
    ));

    let mut identity_values = Vec::new();
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_SOURCE_KEY,
        &downloaded.source_key,
    );
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time as i64,
    );
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_CHANGE_KEY,
        &imported_change_key,
    );
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_predecessor_change_list,
    );
    let mut import_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x10, // ImportMessageChange, associated FAI.
    ];
    import_rops.extend_from_slice(&4u16.to_le_bytes());
    import_rops.extend_from_slice(&identity_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_rops, &[collector_handles[2], u32::MAX])),
        )
        .await?;
    let response_body = response_bytes(response).await;
    let (import_response, import_handles) =
        response_rops_and_handles_from_execute_body(&response_body);
    assert!(contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]));

    let mut update_values = Vec::new();
    append_mapi_utf16_property(
        &mut update_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(
        &mut update_values,
        PID_TAG_SUBJECT_W,
        "Online Contacts WLink updated by ICS",
    );
    // [MS-OXCFXICS] sections 2.2.3.2.4.2, 3.2.5.9.4.2, and
    // 3.3.4.3.3.2.2.1 require an existing-message import to upload the
    // complete client-settable Message rather than a property delta.
    append_mapi_binary_property(
        &mut update_values,
        PID_TAG_WLINK_ENTRY_ID,
        &contacts_entry_id,
    );
    append_mapi_i32_property(&mut update_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(&mut update_values, 0x684A_0003, 0x0010_0000);
    append_mapi_i32_property(&mut update_values, 0x6847_0003, 1_537_819_608);
    append_mapi_i32_property(&mut update_values, 0x6852_0003, 4);
    append_mapi_binary_property(&mut update_values, PID_TAG_WLINK_ORDINAL, &[0x81]);
    append_mapi_binary_property(
        &mut update_values,
        PID_TAG_WLINK_GROUP_CLSID,
        group_id.as_bytes(),
    );
    append_mapi_utf16_property(
        &mut update_values,
        PID_TAG_WLINK_GROUP_NAME_W,
        "My Contacts",
    );
    append_mapi_binary_property(&mut update_values, 0x684F_0102, &contacts_folder_type);
    let mut set_rops = Vec::new();
    append_rop_set_properties(&mut set_rops, 1, 11, &update_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&set_rops, &import_handles)),
        )
        .await?;
    assert!(contains_bytes(
        &response_rops_from_execute_response(response).await,
        &[0x0A, 0x01, 0, 0, 0, 0]
    ));

    let staged_state = sqlx::query_as::<_, (String, i64, Vec<u8>, Vec<u8>, i64)>(
        r#"
        SELECT shortcut.subject, identity.mapi_change_number,
               identity.change_key, identity.predecessor_change_list,
               (SELECT COUNT(*) FROM mail_change_log changes
                WHERE changes.account_id = shortcut.account_id
                  AND changes.object_kind = 'navigation_shortcut'
                  AND changes.object_id = shortcut.id)
        FROM mapi_navigation_shortcuts shortcut
        JOIN mapi_object_identities identity
          ON identity.tenant_id = shortcut.tenant_id
         AND identity.account_id = shortcut.account_id
         AND identity.canonical_id = shortcut.id
        WHERE shortcut.account_id = $1
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.mapi_object_id = $2
        "#,
    )
    .bind(fixture.account_id)
    .bind(online_message_id as i64)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(staged_state.0, "Online Contacts WLink");
    assert_eq!(staged_state.1 as u64, initial_change_number);
    assert_eq!(staged_state.2, initial_change_key);
    assert_eq!(staged_state.3, initial_predecessor_change_list);
    assert_eq!(staged_state.4, 1);

    let mut save_rops = Vec::new();
    append_rop_save_changes_message_with_flags(&mut save_rops, 0, 1, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &import_handles)),
        )
        .await?;
    let save_response = response_rops_from_execute_response(response).await;
    let mut expected_save = vec![0x0C, 0x00, 0, 0, 0, 0, 0x01];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(online_message_id));
    assert!(
        contains_bytes(&save_response, &expected_save),
        "ICS SaveChangesMessage must preserve the online-created WLink identity: {save_response:02x?}"
    );

    let saved_state =
        sqlx::query_as::<_, (Uuid, String, i64, Vec<u8>, i64, Vec<u8>, Vec<u8>, i64)>(
            r#"
        SELECT shortcut.id, shortcut.subject, identity.mapi_object_id,
               identity.source_key, identity.mapi_change_number,
               identity.change_key, identity.predecessor_change_list,
               (SELECT COUNT(*) FROM mail_change_log changes
                WHERE changes.account_id = shortcut.account_id
                  AND changes.object_kind = 'navigation_shortcut'
                  AND changes.object_id = shortcut.id)
        FROM mapi_navigation_shortcuts shortcut
        JOIN mapi_object_identities identity
          ON identity.tenant_id = shortcut.tenant_id
         AND identity.account_id = shortcut.account_id
         AND identity.canonical_id = shortcut.id
        WHERE shortcut.account_id = $1
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.mapi_object_id = $2
        "#,
        )
        .bind(fixture.account_id)
        .bind(online_message_id as i64)
        .fetch_one(storage.pool())
        .await?;
    assert_eq!(saved_state.0, online_canonical_id);
    assert_eq!(saved_state.1, "Online Contacts WLink updated by ICS");
    assert_eq!(saved_state.2 as u64, online_message_id);
    assert_eq!(saved_state.3, downloaded.source_key);
    assert!(saved_state.4 as u64 > initial_change_number);
    assert_eq!(saved_state.5, imported_change_key);
    assert!(test_mapi_pcl_includes_change_key(
        &saved_state.6,
        &initial_change_key
    ));
    assert!(test_mapi_pcl_includes_change_key(
        &saved_state.6,
        &saved_state.5
    ));
    assert_eq!(saved_state.7, 2);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql(
) -> anyhow::Result<()> {
    // [MS-OXOCFG] section 3.1.4.10 requires the existing WLink sequence
    // RopOpenMessage(ReadWrite), RopSetProperties, then RopSaveChangesMessage.
    // [MS-OXCROPS] sections 2.2.6.1, 2.2.8.6, and 2.2.6.3 define those
    // ROPs. SetProperties alone must not publish canonical Common Views state.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let group_id = Uuid::from_bytes([0x44; 16]);
    let initial = storage
        .commit_mapi_navigation_shortcut_create(
            crate::store::CommitMapiNavigationShortcutCreateInput {
                shortcut: crate::store::UpsertMapiNavigationShortcutInput {
                    id: Some(Uuid::new_v4()),
                    account_id: fixture.account_id,
                    subject: "Contacts before staged update".to_string(),
                    target_folder_id: Some(crate::mapi::identity::CONTACTS_FOLDER_ID),
                    shortcut_type: 0,
                    flags: 0x0010_0000,
                    save_stamp: 0x1234_5678,
                    section: 4,
                    ordinal: vec![0x80],
                    group_header_id: Some(group_id),
                    group_name: "My Contacts".to_string(),
                    client_properties:
                        crate::store::MapiNavigationShortcutClientProperties::default(),
                },
            },
        )
        .await?;

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let mut staged_values = Vec::new();
    append_mapi_utf16_property(
        &mut staged_values,
        PID_TAG_SUBJECT_W,
        "Contacts after atomic save",
    );
    append_mapi_binary_property(&mut staged_values, PID_TAG_WLINK_ORDINAL, &[0x91]);
    let mut stage_rops = Vec::new();
    append_rop_open_folder(
        &mut stage_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_open_message_with_flags(
        &mut stage_rops,
        1,
        2,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        initial.identity.object_id,
        0x01,
    );
    append_rop_set_properties(&mut stage_rops, 2, 2, &staged_values);
    append_rop_get_properties_specific(
        &mut stage_rops,
        2,
        &[PID_TAG_SUBJECT_W, PID_TAG_WLINK_ORDINAL],
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&stage_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let body = response_bytes(response).await;
    let (response_rops, handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&response_rops, &[0x0A, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x07, 0x02, 0, 0, 0, 0]));
    assert!(
        contains_bytes(&response_rops, &utf16z("Contacts after atomic save")),
        "GetPropertiesSpecific on the open handle must expose the staged WLink value"
    );

    let staged_state = sqlx::query(
        r#"
        SELECT shortcut.subject, shortcut.ordinal, shortcut.group_header_id,
               shortcut.group_name, identity.mapi_change_number,
               identity.change_key, identity.predecessor_change_list,
               to_char(identity.updated_at AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
        FROM mapi_navigation_shortcuts shortcut
        JOIN mapi_object_identities identity
          ON identity.tenant_id = shortcut.tenant_id
         AND identity.account_id = shortcut.account_id
         AND identity.canonical_id = shortcut.id
        WHERE shortcut.account_id = $1
          AND shortcut.id = $2
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.deleted_at IS NULL
        "#,
    )
    .bind(fixture.account_id)
    .bind(initial.shortcut.id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(
        staged_state.get::<String, _>("subject"),
        "Contacts before staged update",
        "RopSetProperties must not persist an existing WLink before SaveChangesMessage"
    );
    assert_eq!(staged_state.get::<Vec<u8>, _>("ordinal"), vec![0x80]);
    assert_eq!(
        staged_state.get::<i64, _>("mapi_change_number") as u64,
        initial.identity.change_number
    );
    assert_eq!(
        staged_state.get::<Vec<u8>, _>("change_key"),
        initial.identity.change_key
    );
    assert_eq!(
        staged_state.get::<Vec<u8>, _>("predecessor_change_list"),
        initial.identity.predecessor_change_list
    );

    let mut save_rops = Vec::new();
    append_rop_save_changes_message_with_flags(&mut save_rops, 2, 2, 0x0A);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &handles)),
        )
        .await?;
    let save_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&save_response, &[0x0C, 0x02, 0, 0, 0, 0]));

    let saved_state = sqlx::query(
        r#"
        SELECT shortcut.subject, shortcut.ordinal, shortcut.group_header_id,
               shortcut.group_name, identity.mapi_object_id,
               identity.mapi_change_number, identity.source_key,
               identity.change_key, identity.predecessor_change_list,
               to_char(identity.updated_at AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
        FROM mapi_navigation_shortcuts shortcut
        JOIN mapi_object_identities identity
          ON identity.tenant_id = shortcut.tenant_id
         AND identity.account_id = shortcut.account_id
         AND identity.canonical_id = shortcut.id
        WHERE shortcut.account_id = $1
          AND shortcut.id = $2
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.deleted_at IS NULL
        "#,
    )
    .bind(fixture.account_id)
    .bind(initial.shortcut.id)
    .fetch_one(storage.pool())
    .await?;
    let saved_change_number = saved_state.get::<i64, _>("mapi_change_number") as u64;
    let saved_change_key = saved_state.get::<Vec<u8>, _>("change_key");
    let saved_predecessors = saved_state.get::<Vec<u8>, _>("predecessor_change_list");
    assert_eq!(
        saved_state.get::<String, _>("subject"),
        "Contacts after atomic save"
    );
    assert_eq!(saved_state.get::<Vec<u8>, _>("ordinal"), vec![0x91]);
    assert_eq!(
        saved_state.get::<Option<Uuid>, _>("group_header_id"),
        Some(group_id)
    );
    assert_eq!(saved_state.get::<String, _>("group_name"), "My Contacts");
    assert_eq!(
        saved_state.get::<i64, _>("mapi_object_id") as u64,
        initial.identity.object_id
    );
    assert_eq!(
        saved_state.get::<Vec<u8>, _>("source_key"),
        initial.identity.source_key
    );
    assert!(saved_change_number > initial.identity.change_number);
    assert_eq!(
        saved_change_key,
        crate::mapi::identity::change_key_for_change_number(saved_change_number)
    );
    assert_ne!(saved_predecessors, initial.identity.predecessor_change_list);
    assert!(contains_bytes(&saved_predecessors, &saved_change_key));
    assert_ne!(
        saved_state.get::<String, _>("updated_at"),
        staged_state.get::<String, _>("updated_at"),
        "SaveChangesMessage must advance PidTagLastModificationTime"
    );

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql(
) -> anyhow::Result<()> {
    // [MS-OXCROPS] sections 2.2.8.8 and 2.2.6.3 require property deletion
    // on the open Message to remain pending until SaveChangesMessage.
    // [MS-OXOCFG] sections 2.2.9.8 and 3.1.4.10.2 require a shortcut
    // EntryID, so this realistic edit replaces rather than removes its target.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let initial = storage
        .commit_mapi_navigation_shortcut_create(
            crate::store::CommitMapiNavigationShortcutCreateInput {
                shortcut: crate::store::UpsertMapiNavigationShortcutInput {
                    id: Some(Uuid::new_v4()),
                    account_id: fixture.account_id,
                    subject: "Contacts target deletion".to_string(),
                    target_folder_id: Some(crate::mapi::identity::CONTACTS_FOLDER_ID),
                    shortcut_type: 0,
                    flags: 0x0010_0000,
                    save_stamp: 0x1234_5678,
                    section: 4,
                    ordinal: vec![0x80],
                    group_header_id: Some(Uuid::from_bytes([0x45; 16])),
                    group_name: "My Contacts".to_string(),
                    client_properties:
                        crate::store::MapiNavigationShortcutClientProperties::default(),
                },
            },
        )
        .await?;
    let initial_change_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE account_id = $1
          AND object_kind = 'navigation_shortcut'
          AND object_id = $2
        "#,
    )
    .bind(fixture.account_id)
    .bind(initial.shortcut.id)
    .fetch_one(storage.pool())
    .await?;

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let mut open_rops = Vec::new();
    append_rop_open_folder(
        &mut open_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_open_message_with_flags(
        &mut open_rops,
        1,
        2,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        initial.identity.object_id,
        0x01,
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let body = response_bytes(response).await;
    let (open_response, handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&open_response, &[0x03, 0x02, 0, 0, 0, 0]));

    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        fixture.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let mut replacement = Vec::new();
    append_mapi_binary_property(&mut replacement, PID_TAG_WLINK_ENTRY_ID, &calendar_entry_id);
    let mut stage_rops = Vec::new();
    append_rop_delete_properties(&mut stage_rops, 2, &[PID_TAG_WLINK_ENTRY_ID]);
    append_rop_get_properties_specific(&mut stage_rops, 2, &[PID_TAG_WLINK_ENTRY_ID]);
    append_rop_set_properties(&mut stage_rops, 2, 1, &replacement);
    append_rop_get_properties_specific(&mut stage_rops, 2, &[PID_TAG_WLINK_ENTRY_ID]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&stage_rops, &handles)),
        )
        .await?;
    let stage_response = response_rops_from_execute_response(response).await;
    assert_eq!(
        stage_response
            .windows(4)
            .filter(|window| *window == 0x8004_010Fu32.to_le_bytes())
            .count(),
        1,
        "the staged deletion must read as ecNotFound on the same WLink handle: {stage_response:02x?}"
    );
    assert!(
        contains_bytes(&stage_response, &calendar_entry_id),
        "SetProperties after DeleteProperties must cancel the staged deletion for the same tag"
    );

    let staged_state = sqlx::query_as::<_, (Option<i64>, i64, Vec<u8>, Vec<u8>, Vec<u8>, i64)>(
        r#"
        SELECT shortcut.target_folder_id, identity.mapi_object_id,
               identity.source_key, identity.change_key,
               identity.predecessor_change_list,
               (SELECT COUNT(*)
                FROM mail_change_log changes
                WHERE changes.account_id = shortcut.account_id
                  AND changes.object_kind = 'navigation_shortcut'
                  AND changes.object_id = shortcut.id)
        FROM mapi_navigation_shortcuts shortcut
        JOIN mapi_object_identities identity
          ON identity.tenant_id = shortcut.tenant_id
         AND identity.account_id = shortcut.account_id
         AND identity.canonical_id = shortcut.id
        WHERE shortcut.account_id = $1
          AND shortcut.id = $2
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.deleted_at IS NULL
        "#,
    )
    .bind(fixture.account_id)
    .bind(initial.shortcut.id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(
        staged_state,
        (
            Some(crate::mapi::identity::CONTACTS_FOLDER_ID as i64),
            initial.identity.object_id as i64,
            initial.identity.source_key.clone(),
            initial.identity.change_key.clone(),
            initial.identity.predecessor_change_list.clone(),
            initial_change_count,
        ),
        "DeleteProperties and SetProperties must not publish WLink state before SaveChangesMessage"
    );

    let mut save_rops = Vec::new();
    append_rop_save_changes_message_with_flags(&mut save_rops, 2, 2, 0x0A);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &handles)),
        )
        .await?;
    let save_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&save_response, &[0x0C, 0x02, 0, 0, 0, 0]));

    let saved_state = sqlx::query_as::<_, (Option<i64>, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>, i64)>(
        r#"
        SELECT shortcut.target_folder_id, identity.mapi_object_id,
               identity.mapi_change_number, identity.source_key,
               identity.change_key, identity.predecessor_change_list,
               (SELECT COUNT(*)
                FROM mail_change_log changes
                WHERE changes.account_id = shortcut.account_id
                  AND changes.object_kind = 'navigation_shortcut'
                  AND changes.object_id = shortcut.id)
        FROM mapi_navigation_shortcuts shortcut
        JOIN mapi_object_identities identity
          ON identity.tenant_id = shortcut.tenant_id
         AND identity.account_id = shortcut.account_id
         AND identity.canonical_id = shortcut.id
        WHERE shortcut.account_id = $1
          AND shortcut.id = $2
          AND identity.object_kind = 'navigation_shortcut'
          AND identity.deleted_at IS NULL
        "#,
    )
    .bind(fixture.account_id)
    .bind(initial.shortcut.id)
    .fetch_one(storage.pool())
    .await?;
    let saved_change_number = saved_state.2 as u64;
    assert_eq!(
        saved_state.0,
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID as i64)
    );
    assert_eq!(saved_state.1 as u64, initial.identity.object_id);
    assert_eq!(saved_state.3, initial.identity.source_key);
    assert!(saved_change_number > initial.identity.change_number);
    assert_eq!(
        saved_state.4,
        crate::mapi::identity::change_key_for_change_number(saved_change_number)
    );
    assert_ne!(saved_state.5, initial.identity.predecessor_change_list);
    assert!(contains_bytes(&saved_state.5, &saved_state.4));
    assert_eq!(saved_state.6, initial_change_count + 1);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_common_views_import_classifies_non_wlink_fai_at_save() {
    // [MS-OXCFXICS] sections 3.2.5.9.4.2 and 3.3.5.8.7: the associated
    // Message returned by ImportMessageChange is populated by later ROPs and
    // remains uncommitted until SaveChangesMessage. [MS-OXOCFG] section 2.2.9
    // limits the navigation-shortcut contract to WunderBar.Link messages.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    *store.next_mapi_global_counter.lock().unwrap() = 0x0200_AF;
    store
        .reserve_mapi_local_replica_ids(account.account_id, 0x0001_0000)
        .await
        .unwrap();
    let shortcuts = store.navigation_shortcuts.clone();
    let associated_configs = store.associated_configs.clone();
    let identities = store.mapi_identities.clone();
    let identity_source_keys = store.mapi_identity_source_keys.clone();
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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7E, 0x02, 0, 0, 0, 0]
    ));

    let message_id = crate::mapi::identity::mapi_store_id(0x0206_B6);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let change_key = vec![
        0xA2, 0xD1, 0xCC, 0x5A, 0x17, 0xAB, 0x87, 0x4F, 0xB7, 0x18, 0xA2, 0xE4, 0xB8, 0xAB, 0x0A,
        0xC2, 0x00, 0x00, 0x08, 0x22,
    ];
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);
    let mut identity_values = Vec::new();
    append_mapi_binary_property(&mut identity_values, PID_TAG_SOURCE_KEY, &source_key);
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-19", "14:00"),
    );
    append_mapi_binary_property(&mut identity_values, PID_TAG_CHANGE_KEY, &change_key);
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &predecessor_change_list,
    );
    let mut import_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x10, // ImportMessageChange, associated FAI.
    ];
    import_rops.extend_from_slice(&4u16.to_le_bytes());
    import_rops.extend_from_slice(&identity_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_rops, &[collector_handles[2], u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (import_response, import_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]));
    assert!(shortcuts.lock().unwrap().is_empty());
    assert!(associated_configs.lock().unwrap().is_empty());

    let mut config_values = Vec::new();
    append_mapi_utf16_property(
        &mut config_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Configuration.CommonViews",
    );
    append_mapi_utf16_property(
        &mut config_values,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "Common Views settings",
    );
    append_mapi_i32_property(
        &mut config_values,
        0x7C06_0003, // PidTagRoamingDatatypes.
        4,
    );
    append_mapi_binary_property(
        &mut config_values,
        0x7C07_0102, // PidTagRoamingDictionary.
        b"<xml/>",
    );
    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 1, 4, &config_values);
    append_rop_save_changes_message_with_flags(&mut save_rops, 0, 1, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &import_handles)),
        )
        .await
        .unwrap();
    let save_response = response_rops_from_execute_response(response).await;
    let mut expected_save = vec![0x0C, 0x00, 0, 0, 0, 0, 0x01];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(message_id));
    assert!(contains_bytes(&save_response, &expected_save));

    assert!(
        shortcuts.lock().unwrap().is_empty(),
        "a non-WunderBar Common Views FAI must never enter mapi_navigation_shortcuts"
    );
    let configs = associated_configs.lock().unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].message_class, "IPM.Configuration.CommonViews");
    assert_eq!(configs[0].subject, "Common Views settings");
    assert!(configs[0].properties_json.get("0x65e00102").is_none());
    assert!(configs[0].properties_json.get("0x65e20102").is_none());
    assert!(configs[0].properties_json.get("0x65e30102").is_none());
    assert_eq!(identities.lock().unwrap()[&configs[0].id], message_id);
    assert_eq!(
        identity_source_keys.lock().unwrap()[&configs[0].id],
        source_key
    );
}

#[tokio::test]
async fn mapi_over_http_common_views_keeps_identical_online_fai_messages_distinct() {
    // [MS-OXCFOLD] section 2.2.1.14 exposes Message objects in the associated
    // contents table. Equal class, subject, and payload are not an identity
    // key and therefore must not collapse two RopCreateMessage operations.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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
    let mut values = Vec::new();
    append_mapi_utf16_property(
        &mut values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Configuration.CommonViews",
    );
    append_mapi_utf16_property(
        &mut values,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "Two distinct but equal FAI messages",
    );
    append_mapi_i32_property(&mut values, 0x7C06_0003, 4);

    for _ in 0..2 {
        let mut rops = Vec::new();
        append_rop_create_associated_message(
            &mut rops,
            0,
            1,
            crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        );
        append_rop_set_properties(&mut rops, 1, 3, &values);
        append_rop_save_changes_message(&mut rops, 1, 1);
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
            )
            .await
            .unwrap();
        let response_rops = response_rops_from_execute_response(response).await;
        assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
        renew_mapi_request_id(&mut execute_headers);
    }

    let configs = store.associated_configs.lock().unwrap().clone();
    assert_eq!(configs.len(), 2);
    assert_ne!(configs[0].id, configs[1].id);
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let messages = snapshot
        .associated_config_messages_for_folder(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID)
        .into_iter()
        .filter(|message| {
            message.message_class == "IPM.Configuration.CommonViews"
                && message.subject == "Two distinct but equal FAI messages"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        messages.len(),
        2,
        "snapshot must not deduplicate equal FAI payloads"
    );
    assert_ne!(messages[0].id, messages[1].id);
}

#[tokio::test]
async fn mapi_over_http_common_views_non_wlink_fai_import_round_trips_durable_ics_identity_in_postgresql(
) -> anyhow::Result<()> {
    // [MS-OXCFOLD] section 2.2.1.14 and [MS-OXCFXICS] sections
    // 3.2.5.9.4.2 and 3.3.5.8.7: Common Views contains FAI messages, and
    // ImportMessageChange returns an uncommitted Message that Outlook can
    // classify through later SetProperties before SaveChangesMessage.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let writer = fixture.storage.clone();
    let reader = fixture.storage.clone();
    let account_id = fixture.account_id;
    let reserved_start = writer
        .reserve_mapi_local_replica_ids(account_id, 0x0001_0000)
        .await?;
    let source_counter = reserved_start + 0x06B6;
    let message_id = crate::mapi::identity::mapi_store_id(source_counter);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let imported_change_key = vec![
        0xA2, 0xD1, 0xCC, 0x5A, 0x17, 0xAB, 0x87, 0x4F, 0xB7, 0x18, 0xA2, 0xE4, 0xB8, 0xAB, 0x0A,
        0xC2, 0x00, 0x00, 0x08, 0x22,
    ];
    let mut imported_pcl = vec![imported_change_key.len() as u8];
    imported_pcl.extend_from_slice(&imported_change_key);
    let imported_last_modification_time = test_filetime("2026-07-19", "14:00");
    let baseline_cursor = reader
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);

    let service = ExchangeService::new(writer.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7E, 0x02, 0, 0, 0, 0]
    ));

    let mut identity_values = Vec::new();
    append_mapi_binary_property(&mut identity_values, PID_TAG_SOURCE_KEY, &source_key);
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time,
    );
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_CHANGE_KEY,
        &imported_change_key,
    );
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_pcl,
    );
    let mut import_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x10, // ImportMessageChange, associated FAI.
    ];
    import_rops.extend_from_slice(&4u16.to_le_bytes());
    import_rops.extend_from_slice(&identity_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_rops, &[collector_handles[2], u32::MAX])),
        )
        .await?;
    let body = response_bytes(response).await;
    let (import_response, import_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mapi_associated_config_messages WHERE account_id = $1"
        )
        .bind(account_id)
        .fetch_one(writer.pool())
        .await?,
        0,
        "ImportMessageChange must not publish an unclassified FAI before SaveChangesMessage"
    );

    let mut config_values = Vec::new();
    append_mapi_utf16_property(
        &mut config_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Configuration.CommonViews",
    );
    append_mapi_utf16_property(
        &mut config_values,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "Durable Common Views settings",
    );
    append_mapi_i32_property(&mut config_values, 0x7C06_0003, 4);
    append_mapi_binary_property(&mut config_values, 0x7C07_0102, b"<xml/>");
    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 1, 4, &config_values);
    append_rop_save_changes_message_with_flags(&mut save_rops, 0, 1, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &import_handles)),
        )
        .await?;
    let save_response = response_rops_from_execute_response(response).await;
    let mut expected_save = vec![0x0C, 0x00, 0, 0, 0, 0, 0x01];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(message_id));
    assert!(contains_bytes(&save_response, &expected_save));

    let durable = sqlx::query(
        r#"
        SELECT config.id, config.folder_id, config.message_class, config.subject,
               config.properties_json,
               identity.mapi_object_id, identity.mapi_change_number,
               identity.source_key, identity.change_key,
               identity.predecessor_change_list,
               ((EXTRACT(EPOCH FROM (identity.updated_at - TIMESTAMPTZ '1601-01-01 00:00:00+00'))
                   * 10000000)::bigint) AS last_modification_time
        FROM mapi_associated_config_messages config
        JOIN mapi_object_identities identity
          ON identity.tenant_id = config.tenant_id
         AND identity.account_id = config.account_id
         AND identity.object_kind = 'associated_config'
         AND identity.canonical_id = config.id
         AND identity.deleted_at IS NULL
        WHERE config.account_id = $1
          AND config.folder_id = $2
          AND config.message_class = 'IPM.Configuration.CommonViews'
          AND config.subject = 'Durable Common Views settings'
        "#,
    )
    .bind(account_id)
    .bind(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64)
    .fetch_one(writer.pool())
    .await?;
    let canonical_id = durable.get::<Uuid, _>("id");
    let content_properties = durable.get::<serde_json::Value, _>("properties_json");
    for identity_tag in [
        "0x674a0014",
        "0x65e00102",
        "0x65e20102",
        "0x65e30102",
        "0x67a40014",
        "0x30080040",
    ] {
        assert!(
            content_properties.get(identity_tag).is_none(),
            "{identity_tag} must not duplicate the canonical identity table in content JSON"
        );
    }
    let server_change_number = durable.get::<i64, _>("mapi_change_number") as u64;
    assert_eq!(durable.get::<i64, _>("mapi_object_id") as u64, message_id);
    assert_ne!(
        server_change_number, source_counter,
        "[MS-OXCFXICS] section 3.3.5.2.1 requires a server CN distinct from the client-local MID"
    );
    assert_eq!(durable.get::<Vec<u8>, _>("source_key"), source_key);
    assert_eq!(durable.get::<Vec<u8>, _>("change_key"), imported_change_key);
    assert_eq!(
        durable.get::<Vec<u8>, _>("predecessor_change_list"),
        imported_pcl
    );
    assert_eq!(
        durable.get::<i64, _>("last_modification_time") as u64,
        imported_last_modification_time as u64
    );

    let reloaded = reader.load_mapi_mail_store(account_id, 500).await?;
    let table_rows = reloaded
        .associated_config_messages_for_folder(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID)
        .into_iter()
        .filter(|message| message.canonical_id == canonical_id)
        .collect::<Vec<_>>();
    let sync_rows = reloaded
        .associated_config_sync_messages_for_folder(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID)
        .into_iter()
        .filter(|message| message.canonical_id == canonical_id)
        .collect::<Vec<_>>();
    assert_eq!(table_rows, sync_rows);
    assert_eq!(table_rows.len(), 1);
    assert_eq!(table_rows[0].id, message_id);

    let sync_response = content_sync_response_rops_for_store_with_flags(
        reader.clone(),
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        0x0010,
    )
    .await;
    let sync =
        strict_content_sync_transfer_from_response(&sync_response).map_err(anyhow::Error::msg)?;
    let downloaded = sync
        .message_changes
        .iter()
        .find(|message| message.subject == "Durable Common Views settings")
        .ok_or_else(|| anyhow::anyhow!("persisted Common Views FAI was absent from ICS"))?;
    assert!(downloaded.associated);
    assert_eq!(downloaded.mid, Some(message_id));
    assert_eq!(downloaded.source_key, source_key);
    assert_eq!(downloaded.change_key, imported_change_key);
    assert_eq!(downloaded.predecessor_change_list, imported_pcl);
    assert_eq!(downloaded.change_number, Some(server_change_number));
    assert_eq!(
        downloaded.last_modification_time,
        Some(imported_last_modification_time as u64)
    );
    assert!(strict_replguid_globset_contains_counter(
        &sync.cnset_seen_fai,
        &globcnt_bytes(server_change_number),
    )
    .map_err(anyhow::Error::msg)?);
    assert!(!strict_replguid_globset_contains_counter(
        &sync.cnset_seen_fai,
        &globcnt_bytes(source_counter),
    )
    .map_err(anyhow::Error::msg)?);

    let created = reader
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    assert!(created.event_pending);
    assert_eq!(created.events.len(), 1);
    assert_eq!(
        created.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0004,
            crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            Some(message_id),
            None,
            None,
            Some("associated_config"),
        )
    );

    let conflict_change_key = vec![
        0xB2, 0xD1, 0xCC, 0x5A, 0x17, 0xAB, 0x87, 0x4F, 0xB7, 0x18, 0xA2, 0xE4, 0xB8, 0xAB, 0x0A,
        0xC2, 0x00, 0x00, 0x09, 0x33,
    ];
    let mut conflict_pcl = vec![conflict_change_key.len() as u8];
    conflict_pcl.extend_from_slice(&conflict_change_key);
    let mut conflict_values = Vec::new();
    append_mapi_binary_property(&mut conflict_values, PID_TAG_SOURCE_KEY, &source_key);
    append_mapi_i64_property(
        &mut conflict_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time + 10_000_000,
    );
    append_mapi_binary_property(
        &mut conflict_values,
        PID_TAG_CHANGE_KEY,
        &conflict_change_key,
    );
    append_mapi_binary_property(
        &mut conflict_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &conflict_pcl,
    );
    let mut conflict_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x50, // associated FAI + FailOnConflict.
    ];
    conflict_rops.extend_from_slice(&4u16.to_le_bytes());
    conflict_rops.extend_from_slice(&conflict_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &conflict_rops,
                &[collector_handles[2], u32::MAX],
            )),
        )
        .await?;
    let body = response_bytes(response).await;
    let (conflict_response, conflict_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &conflict_response,
        &[0x72, 0x01, 0x02, 0x08, 0x04, 0x80]
    ));
    assert_eq!(conflict_handles[1], u32::MAX);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_common_views_fai_table_open_and_ics_share_canonical_identity() {
    // [MS-OXCFOLD] section 2.2.1.14 exposes every FAI Message in the
    // associated contents table. [MS-OXCFXICS] sections 2.2.1.2.1 and
    // 3.2.5.3 require the same messages and identities in FAI content sync.
    let account = FakeStore::account();
    let config_canonical_id = Uuid::parse_str("67676767-6767-4767-8767-676767676767").unwrap();
    let search_canonical_id = Uuid::parse_str("68686868-6868-4868-8868-686868686868").unwrap();
    let mut definition_blob = vec![
        0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    ];
    definition_blob.extend_from_slice(&1u32.to_le_bytes());
    definition_blob.push(0xAA);
    definition_blob.extend_from_slice(&0u32.to_le_bytes());
    definition_blob.push(0xBB);
    definition_blob.extend_from_slice(&0u32.to_le_bytes());

    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: search_canonical_id,
            account_id: account.account_id,
            role: "unread_mail".to_string(),
            display_name: "Canonical search FAI".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({
                "scope": "top_of_personal_folders",
                "recursive": true
            }),
            restriction_json: serde_json::json!({
                "kind": "exchange_unread_mail",
                "pidTagSearchFolderDefinition": BASE64_STANDARD.encode(&definition_blob)
            }),
            excluded_folder_roles: vec!["trash".to_string(), "junk".to_string()],
            is_builtin: true,
        }])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: config_canonical_id,
            account_id: account.account_id,
            folder_id: crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            message_class: "IPM.Configuration.CommonViews".to_string(),
            subject: "Canonical config FAI".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "01020304"}
            }),
        }])),
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

    let identity_tags = [
        PID_TAG_MID,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_SUBJECT_W,
        PID_TAG_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ];
    let mut table_rops = Vec::new();
    append_rop_open_folder(
        &mut table_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    table_rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x02]);
    table_rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    table_rops.extend_from_slice(&(identity_tags.len() as u16).to_le_bytes());
    for tag in identity_tags {
        table_rops.extend_from_slice(&tag.to_le_bytes());
    }
    table_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    table_rops.extend_from_slice(&20u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&table_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let table_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&table_response, &[0x15, 0x02, 0, 0, 0, 0]));

    let expected_identity = |canonical_id: Uuid| {
        let object_id = store.mapi_identities.lock().unwrap()[&canonical_id];
        let source_key = store
            .mapi_identity_source_keys
            .lock()
            .unwrap()
            .get(&canonical_id)
            .cloned()
            .unwrap_or_else(|| crate::mapi::identity::source_key_for_object_id(object_id));
        let change_number = store.mapi_identity_change_numbers.lock().unwrap()[&canonical_id];
        let change_key = store.mapi_identity_change_keys.lock().unwrap()[&canonical_id].clone();
        let predecessor_change_list =
            store.mapi_identity_predecessor_change_lists.lock().unwrap()[&canonical_id].clone();
        (
            object_id,
            source_key,
            change_key,
            predecessor_change_list,
            change_number,
        )
    };
    let config_identity = expected_identity(config_canonical_id);
    let search_identity = expected_identity(search_canonical_id);
    let expected_table_row =
        |identity: &(u64, Vec<u8>, Vec<u8>, Vec<u8>, u64), message_class: &str, subject: &str| {
            let mut row = vec![0];
            row.extend_from_slice(&mapi_wire_id_bytes(identity.0));
            row.extend_from_slice(&utf16z(message_class));
            row.extend_from_slice(&utf16z(subject));
            for value in [&identity.1, &identity.2, &identity.3] {
                row.extend_from_slice(&(value.len() as u16).to_le_bytes());
                row.extend_from_slice(value);
            }
            row.extend_from_slice(&mapi_wire_id_bytes(crate::mapi::identity::mapi_store_id(
                identity.4,
            )));
            row
        };
    assert!(contains_bytes(
        &table_response,
        &expected_table_row(
            &config_identity,
            "IPM.Configuration.CommonViews",
            "Canonical config FAI",
        )
    ));
    assert!(contains_bytes(
        &table_response,
        &expected_table_row(
            &search_identity,
            "IPM.Microsoft.WunderBar.SFInfo",
            "Canonical search FAI",
        )
    ));

    renew_mapi_request_id(&mut execute_headers);
    let mut open_rops = Vec::new();
    append_rop_open_folder(
        &mut open_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_open_message(
        &mut open_rops,
        1,
        2,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        config_identity.0,
    );
    append_rop_get_properties_specific(
        &mut open_rops,
        2,
        &[
            PID_TAG_MID,
            PID_TAG_SOURCE_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_CHANGE_NUMBER,
        ],
    );
    append_rop_open_message(
        &mut open_rops,
        1,
        3,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        search_identity.0,
    );
    append_rop_get_properties_specific(
        &mut open_rops,
        3,
        &[
            PID_TAG_MID,
            PID_TAG_SOURCE_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_CHANGE_NUMBER,
        ],
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let open_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&open_response, &[0x03, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&open_response, &[0x03, 0x03, 0, 0, 0, 0]));
    for (handle, identity) in [(2, &config_identity), (3, &search_identity)] {
        let mut expected = vec![0];
        expected.extend_from_slice(&mapi_wire_id_bytes(identity.0));
        for value in [&identity.1, &identity.2, &identity.3] {
            expected.extend_from_slice(&(value.len() as u16).to_le_bytes());
            expected.extend_from_slice(value);
        }
        expected.extend_from_slice(&mapi_wire_id_bytes(crate::mapi::identity::mapi_store_id(
            identity.4,
        )));
        let row_offset =
            mapi_get_properties_specific_standard_row_offset(&open_response, handle).unwrap();
        assert_eq!(
            &open_response[row_offset..row_offset + expected.len()],
            expected.as_slice()
        );
    }

    let sync_response = content_sync_response_rops_for_store_with_flags(
        store,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        0x0010,
    )
    .await;
    let sync = strict_content_sync_transfer_from_response(&sync_response).unwrap();
    for (subject, identity) in [
        ("Canonical config FAI", &config_identity),
        ("Canonical search FAI", &search_identity),
    ] {
        let message = sync
            .message_changes
            .iter()
            .find(|message| message.subject == subject)
            .unwrap_or_else(|| panic!("Common Views ICS omitted {subject}"));
        assert!(message.associated);
        assert_eq!(message.mid, Some(identity.0));
        assert_eq!(message.source_key, identity.1);
        assert_eq!(message.change_key, identity.2);
        assert_eq!(message.predecessor_change_list, identity.3);
        assert_eq!(message.change_number, Some(identity.4));
    }
}

#[tokio::test]
async fn mapi_over_http_outlook_mail_favorite_import_without_group_properties_persists() {
    // Outlook 16.0.20131.20044 trace 202607201648 imported this exact
    // type=0, section=1 Mail-folder WLink without PidTagWlinkGroupClsid or
    // PidTagWlinkGroupName. [MS-OXOCFG] sections 2.2.9.11-2.2.9.14 and
    // 3.1.4.10.2 still describe those group properties; this fixture records
    // the observed omission without treating it as normative permission.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    *store.next_mapi_global_counter.lock().unwrap() = 0x0200_AF;
    store
        .reserve_mapi_local_replica_ids(account.account_id, 0x0001_0000)
        .await
        .unwrap();
    let shortcuts = store.navigation_shortcuts.clone();
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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7E, 0x02, 0, 0, 0, 0]
    ));

    let message_id = crate::mapi::identity::mapi_store_id(0x0206_B4);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let change_key = vec![
        0xA2, 0xD1, 0xCC, 0x5A, 0x17, 0xAB, 0x87, 0x4F, 0xB7, 0x18, 0xA2, 0xE4, 0xB8, 0xAB, 0x0A,
        0xC2, 0x00, 0x00, 0x08, 0x20,
    ];
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);
    let mut identity_values = Vec::new();
    append_mapi_binary_property(&mut identity_values, PID_TAG_SOURCE_KEY, &source_key);
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-20", "16:48"),
    );
    append_mapi_binary_property(&mut identity_values, PID_TAG_CHANGE_KEY, &change_key);
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &predecessor_change_list,
    );
    let mut import_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x10, // ImportMessageChange, associated FAI.
    ];
    import_rops.extend_from_slice(&4u16.to_le_bytes());
    import_rops.extend_from_slice(&identity_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_rops, &[collector_handles[2], u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (import_response, import_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]));
    assert!(shortcuts.lock().unwrap().is_empty());

    let inbox_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    )
    .unwrap();
    let mail_folder_type = [
        0x00, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let mut first_batch = Vec::new();
    append_mapi_utf16_property(
        &mut first_batch,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(&mut first_batch, PID_TAG_NORMALIZED_SUBJECT_W, "Inbox");
    append_mapi_i32_property(
        &mut first_batch,
        0x6847_0003, // PidTagWlinkSaveStamp.
        1_537_819_608,
    );
    let mut second_batch = Vec::new();
    append_mapi_i32_property(&mut second_batch, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(
        &mut second_batch,
        0x684A_0003, // PidTagWlinkFlags.
        0x0010_8000,
    );
    append_mapi_binary_property(&mut second_batch, PID_TAG_WLINK_ORDINAL, &[0x7F]);
    let mut third_batch = Vec::new();
    append_mapi_binary_property(&mut third_batch, PID_TAG_WLINK_ENTRY_ID, &inbox_entry_id);
    append_mapi_binary_property(
        &mut third_batch,
        0x684F_0102, // PidTagWlinkFolderType.
        &mail_folder_type,
    );
    append_mapi_i32_property(
        &mut third_batch,
        0x6852_0003, // PidTagWlinkSection.
        1,
    );

    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 1, 3, &first_batch);
    append_rop_set_properties(&mut save_rops, 1, 3, &second_batch);
    append_rop_set_properties(&mut save_rops, 1, 3, &third_batch);
    append_rop_save_changes_message_with_flags(&mut save_rops, 0, 1, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &import_handles)),
        )
        .await
        .unwrap();
    let save_response = response_rops_from_execute_response(response).await;
    let mut expected_save = vec![0x0C, 0x00, 0, 0, 0, 0, 0x01];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(message_id));
    assert!(
        contains_bytes(&save_response, &expected_save),
        "Outlook's group-less Mail favorite must retain its imported MID: {save_response:02x?}"
    );

    let stored = shortcuts.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].subject, "Inbox");
    assert_eq!(
        stored[0].target_folder_id,
        Some(crate::mapi::identity::INBOX_FOLDER_ID)
    );
    assert_eq!(stored[0].shortcut_type, 0);
    assert_eq!(stored[0].section, 1);
    assert_eq!(stored[0].group_name, "");
    assert_eq!(stored[0].group_header_id, None);
    let canonical_id = stored[0].id;
    drop(stored);

    let snapshot = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let reconstructed = snapshot
        .navigation_shortcut_messages()
        .into_iter()
        .find(|shortcut| shortcut.canonical_id == canonical_id)
        .expect("saved Mail favorite after snapshot reconstruction");
    assert_eq!(reconstructed.group_header_id, None);
    assert_eq!(reconstructed.group_name, "");

    let sync_response = content_sync_response_rops_for_store_with_flags(
        store,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        0x0010,
    )
    .await;
    let sync = strict_content_sync_transfer_from_response(&sync_response).unwrap();
    let downloaded = sync
        .message_changes
        .iter()
        .find(|message| message.subject == "Inbox")
        .expect("saved Mail favorite in reconstructed Common Views FAI sync");
    assert!(downloaded.body_tags.contains(&0x684F_0102));
    assert!(!downloaded.body_tags.contains(&PID_TAG_WLINK_GROUP_CLSID));
    assert!(!downloaded.body_tags.contains(&PID_TAG_WLINK_GROUP_NAME_W));
}

#[tokio::test]
async fn mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save() {
    // This fixture replays the two Contacts WLinks selected from the four
    // Common Views FAI messages imported by Outlook trace 202607190710. Outlook
    // populated each selected Message object through three
    // RopSetProperties calls before RopSaveChangesMessage. [MS-OXCFXICS]
    // sections 3.2.5.9.4.2 and 3.3.5.8.7 require the imported Message object
    // to remain uncommitted until Save. [MS-OXOCFG] sections 2.2.9 and
    // 3.1.4.9-3.1.4.10 define these FAI messages as navigation shortcuts in
    // Common Views.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    // RR 202607190710 reserves 0x10000 local replica IDs beginning at
    // 0x0200af before importing the adjacent WLink MIDs 0x0206b4/0x0206b5.
    *store.next_mapi_global_counter.lock().unwrap() = 0x0200_AF;
    assert_eq!(
        store
            .reserve_mapi_local_replica_ids(account.account_id, 0x0001_0000)
            .await
            .unwrap(),
        0x0200_AF
    );
    let shortcuts = store.navigation_shortcuts.clone();
    let identities = store.mapi_identities.clone();
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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7E, 0x02, 0, 0, 0, 0]
    ));

    let group_id = Uuid::parse_str("b7f00600-0000-0000-c000-000000000046").unwrap();
    let suggested_contacts_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
    )
    .unwrap();
    let imported_shortcuts = [
        (
            crate::mapi::identity::mapi_store_id(0x0206_B4),
            "My Contacts",
            4u32,
            0u32,
            127u8,
            None,
        ),
        (
            crate::mapi::identity::mapi_store_id(0x0206_B5),
            "Suggested Contacts",
            0u32,
            1_048_576u32,
            127u8,
            Some(suggested_contacts_entry_id.as_slice()),
        ),
    ];

    for (index, (message_id, subject, shortcut_type, flags, ordinal, entry_id)) in
        imported_shortcuts.iter().enumerate()
    {
        let source_key = crate::mapi::identity::source_key_for_object_id(*message_id);
        let mut change_key = vec![
            0xA2, 0xD1, 0xCC, 0x5A, 0x17, 0xAB, 0x87, 0x4F, 0xB7, 0x18, 0xA2, 0xE4, 0xB8, 0xAB,
            0x0A, 0xC2, 0x00, 0x00, 0x08, 0x20,
        ];
        change_key[19] += index as u8;
        let mut predecessor_change_list = vec![change_key.len() as u8];
        predecessor_change_list.extend_from_slice(&change_key);
        let last_modification_time = test_filetime("2026-07-19", "07:10") as u64;
        let mut identity_values = Vec::new();
        append_mapi_binary_property(&mut identity_values, PID_TAG_SOURCE_KEY, &source_key);
        append_mapi_i64_property(
            &mut identity_values,
            PID_TAG_LAST_MODIFICATION_TIME,
            last_modification_time as i64,
        );
        append_mapi_binary_property(&mut identity_values, PID_TAG_CHANGE_KEY, &change_key);
        append_mapi_binary_property(
            &mut identity_values,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            &predecessor_change_list,
        );
        let mut import_rops = vec![
            0x72, 0x00, 0x00, 0x01, 0x10, // ImportMessageChange, associated FAI.
        ];
        import_rops.extend_from_slice(&4u16.to_le_bytes());
        import_rops.extend_from_slice(&identity_values);

        renew_mapi_request_id(&mut execute_headers);
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(&import_rops, &[collector_handles[2], u32::MAX])),
            )
            .await
            .unwrap();
        let body = response_bytes(response).await;
        let (import_response, import_handles) = response_rops_and_handles_from_execute_body(&body);
        assert!(contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]));
        assert_eq!(
            shortcuts.lock().unwrap().len(),
            index,
            "ImportMessageChange must not persist a partial WLink before SaveChangesMessage"
        );

        let mut first_batch = Vec::new();
        append_mapi_utf16_property(
            &mut first_batch,
            PID_TAG_MESSAGE_CLASS_W,
            "IPM.Microsoft.WunderBar.Link",
        );
        append_mapi_utf16_property(&mut first_batch, PID_TAG_NORMALIZED_SUBJECT_W, subject);
        append_mapi_i32_property(
            &mut first_batch,
            0x6847_0003, // PidTagWlinkSaveStamp
            1_537_819_608,
        );
        let mut second_batch = Vec::new();
        append_mapi_i32_property(&mut second_batch, PID_TAG_WLINK_TYPE, *shortcut_type as i32);
        append_mapi_i32_property(
            &mut second_batch,
            0x684A_0003, // PidTagWlinkFlags
            *flags as i32,
        );
        append_mapi_binary_property(&mut second_batch, PID_TAG_WLINK_ORDINAL, &[*ordinal]);
        let mut third_batch = Vec::new();
        append_mapi_binary_property(
            &mut third_batch,
            if *shortcut_type == 4 {
                0x6842_0102 // PidTagWlinkGroupHeaderId, trace binary variant.
            } else {
                0x6850_0102 // PidTagWlinkGroupClsid, trace binary variant.
            },
            group_id.as_bytes(),
        );
        if *shortcut_type != 4 {
            append_mapi_utf16_property(&mut third_batch, PID_TAG_WLINK_GROUP_NAME_W, "My Contacts");
        }
        append_mapi_i32_property(
            &mut third_batch,
            0x6852_0003, // PidTagWlinkSection
            4,
        );
        append_mapi_binary_property(
            &mut third_batch,
            0x684F_0102, // PidTagWlinkFolderType, trace binary variant.
            &[
                0x01, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x46,
            ],
        );
        if let Some(entry_id) = entry_id {
            append_mapi_binary_property(&mut third_batch, PID_TAG_WLINK_ENTRY_ID, entry_id);
        }

        let mut save_rops = Vec::new();
        append_rop_set_properties(&mut save_rops, 1, 3, &first_batch);
        append_rop_set_properties(&mut save_rops, 1, 3, &second_batch);
        append_rop_set_properties(
            &mut save_rops,
            1,
            if entry_id.is_some() { 5 } else { 3 },
            &third_batch,
        );
        // RR 202607190710 sends the unlisted/ignored bit 0x08, then reads the
        // imported ChangeKey from the same Message handle in the same Execute.
        append_rop_save_changes_message_with_flags(&mut save_rops, 0, 1, 0x08);
        save_rops.extend_from_slice(&[0x07, 0x00, 0x01]);
        save_rops.extend_from_slice(&0u16.to_le_bytes()); // PropertySizeLimit
        save_rops.extend_from_slice(&0u16.to_le_bytes()); // WantUnicode = false
        save_rops.extend_from_slice(&1u16.to_le_bytes());
        save_rops.extend_from_slice(&PID_TAG_CHANGE_KEY.to_le_bytes());

        renew_mapi_request_id(&mut execute_headers);
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(&save_rops, &import_handles)),
            )
            .await
            .unwrap();
        let save_response = response_rops_from_execute_response(response).await;
        assert_eq!(
            save_response
                .windows(8)
                .filter(|window| *window == [0x0A, 0x01, 0, 0, 0, 0, 0, 0])
                .count(),
            3,
            "the three SetProperties calls must be processed"
        );
        let mut expected_save = vec![0x0C, 0x00, 0, 0, 0, 0, 0x01];
        expected_save.extend_from_slice(&mapi_wire_id_bytes(*message_id));
        assert!(
            contains_bytes(&save_response, &expected_save),
            "SaveChangesMessage must preserve the imported WLink MID: {save_response:02x?}"
        );
        let mut expected_get_properties = vec![0x07, 0x01, 0, 0, 0, 0, 0];
        expected_get_properties.extend_from_slice(&(change_key.len() as u16).to_le_bytes());
        expected_get_properties.extend_from_slice(&change_key);
        assert!(
            contains_bytes(&save_response, &expected_get_properties),
            "GetPropertiesSpecific after Save must return the imported WLink ChangeKey"
        );
        let canonical_id = shortcuts.lock().unwrap()[index].id;
        assert_eq!(
            store.mapi_identity_change_keys.lock().unwrap()[&canonical_id],
            change_key
        );
        assert_eq!(
            store.mapi_identity_predecessor_change_lists.lock().unwrap()[&canonical_id],
            predecessor_change_list
        );
        assert_eq!(
            store.mapi_identity_last_modification_times.lock().unwrap()[&canonical_id],
            last_modification_time
        );
    }
    let imported_server_change_numbers = shortcuts
        .lock()
        .unwrap()
        .iter()
        .map(|shortcut| store.mapi_identity_change_numbers.lock().unwrap()[&shortcut.id])
        .collect::<Vec<_>>();

    // [MS-OXCFXICS] sections 2.2.3.2.4.2.1 and 3.2.5.9.4.2:
    // FailOnConflict is evaluated by ImportMessageChange itself. A conflicting
    // WLink must return SyncConflict and must not expose a Message handle whose
    // later Save could overwrite the durable Common Views FAI content.
    let conflict_message_id = imported_shortcuts[0].0;
    let conflict_source_key = crate::mapi::identity::source_key_for_object_id(conflict_message_id);
    let mut conflict_change_key = vec![
        0xA2, 0xD1, 0xCC, 0x5A, 0x17, 0xAB, 0x87, 0x4F, 0xB7, 0x18, 0xA2, 0xE4, 0xB8, 0xAB, 0x0A,
        0xC2, 0x00, 0x00, 0x08, 0x20,
    ];
    conflict_change_key[0] = 0xB2;
    conflict_change_key[19] = 0x33;
    let mut conflict_pcl = vec![conflict_change_key.len() as u8];
    conflict_pcl.extend_from_slice(&conflict_change_key);
    let mut conflict_values = Vec::new();
    append_mapi_binary_property(
        &mut conflict_values,
        PID_TAG_SOURCE_KEY,
        &conflict_source_key,
    );
    append_mapi_i64_property(
        &mut conflict_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-19", "07:11"),
    );
    append_mapi_binary_property(
        &mut conflict_values,
        PID_TAG_CHANGE_KEY,
        &conflict_change_key,
    );
    append_mapi_binary_property(
        &mut conflict_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &conflict_pcl,
    );
    let mut conflict_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x50, // associated FAI + FailOnConflict.
    ];
    conflict_rops.extend_from_slice(&4u16.to_le_bytes());
    conflict_rops.extend_from_slice(&conflict_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &conflict_rops,
                &[collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    let conflict_body = response_bytes(response).await;
    let (conflict_response, conflict_handles) =
        response_rops_and_handles_from_execute_body(&conflict_body);
    assert!(contains_bytes(
        &conflict_response,
        &[0x72, 0x01, 0x02, 0x08, 0x04, 0x80]
    ));
    assert_eq!(
        conflict_handles[1],
        u32::MAX,
        "FailOnConflict must not expose an OutputServerObject handle"
    );

    let reserved_message_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER,
    );
    let reserved_source_key = crate::mapi::identity::source_key_for_object_id(reserved_message_id);
    let reserved_change_key = vec![0xE2; 20];
    let mut reserved_pcl = vec![reserved_change_key.len() as u8];
    reserved_pcl.extend_from_slice(&reserved_change_key);
    let mut reserved_values = Vec::new();
    append_mapi_binary_property(
        &mut reserved_values,
        PID_TAG_SOURCE_KEY,
        &reserved_source_key,
    );
    append_mapi_i64_property(
        &mut reserved_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-19", "07:12"),
    );
    append_mapi_binary_property(
        &mut reserved_values,
        PID_TAG_CHANGE_KEY,
        &reserved_change_key,
    );
    append_mapi_binary_property(
        &mut reserved_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &reserved_pcl,
    );
    let mut reserved_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x10, // associated FAI.
    ];
    reserved_rops.extend_from_slice(&4u16.to_le_bytes());
    reserved_rops.extend_from_slice(&reserved_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reserved_rops,
                &[collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    let reserved_body = response_bytes(response).await;
    let (reserved_response, reserved_handles) =
        response_rops_and_handles_from_execute_body(&reserved_body);
    assert!(contains_bytes(
        &reserved_response,
        &[0x72, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert_eq!(reserved_handles[1], u32::MAX);
    assert_eq!(shortcuts.lock().unwrap().len(), 2);
    assert!(shortcuts
        .lock()
        .unwrap()
        .iter()
        .any(|shortcut| shortcut.subject == "My Contacts"));

    // [MS-OXCFXICS] sections 3.1.5.6.2.2 and 3.2.5.9.4.2: an older
    // conflicting FAI import is accepted without FailOnConflict, but the
    // last-writer-wins server version remains canonical after Save.
    let my_contacts_id = shortcuts
        .lock()
        .unwrap()
        .iter()
        .find(|shortcut| shortcut.subject == "My Contacts")
        .unwrap()
        .id;
    let server_change_key_before_conflict =
        store.mapi_identity_change_keys.lock().unwrap()[&my_contacts_id].clone();
    let server_change_number_before_conflict =
        store.mapi_identity_change_numbers.lock().unwrap()[&my_contacts_id];
    let mut older_conflict_change_key = server_change_key_before_conflict.clone();
    older_conflict_change_key[0] = 0x92;
    older_conflict_change_key[19] = 0x44;
    let mut older_conflict_pcl = vec![older_conflict_change_key.len() as u8];
    older_conflict_pcl.extend_from_slice(&older_conflict_change_key);
    let mut older_conflict_identity_values = Vec::new();
    append_mapi_binary_property(
        &mut older_conflict_identity_values,
        PID_TAG_SOURCE_KEY,
        &conflict_source_key,
    );
    append_mapi_i64_property(
        &mut older_conflict_identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-19", "07:09"),
    );
    append_mapi_binary_property(
        &mut older_conflict_identity_values,
        PID_TAG_CHANGE_KEY,
        &older_conflict_change_key,
    );
    append_mapi_binary_property(
        &mut older_conflict_identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &older_conflict_pcl,
    );
    let mut older_conflict_import_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x10, // associated FAI, accept conflicts.
    ];
    older_conflict_import_rops.extend_from_slice(&4u16.to_le_bytes());
    older_conflict_import_rops.extend_from_slice(&older_conflict_identity_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &older_conflict_import_rops,
                &[collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    let older_conflict_body = response_bytes(response).await;
    let (older_conflict_import_response, older_conflict_handles) =
        response_rops_and_handles_from_execute_body(&older_conflict_body);
    assert!(contains_bytes(
        &older_conflict_import_response,
        &[0x72, 0x01, 0, 0, 0, 0]
    ));

    let mut older_conflict_properties = Vec::new();
    append_mapi_utf16_property(
        &mut older_conflict_properties,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(
        &mut older_conflict_properties,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "Older client copy must not replace server",
    );
    append_mapi_i32_property(
        &mut older_conflict_properties,
        0x6847_0003, // PidTagWlinkSaveStamp.
        1_537_819_608,
    );
    append_mapi_i32_property(&mut older_conflict_properties, PID_TAG_WLINK_TYPE, 4);
    append_mapi_i32_property(
        &mut older_conflict_properties,
        0x684A_0003, // PidTagWlinkFlags.
        0,
    );
    append_mapi_binary_property(
        &mut older_conflict_properties,
        PID_TAG_WLINK_ORDINAL,
        &[127],
    );
    append_mapi_binary_property(
        &mut older_conflict_properties,
        0x6842_0102, // PidTagWlinkGroupHeaderId.
        group_id.as_bytes(),
    );
    append_mapi_binary_property(
        &mut older_conflict_properties,
        0x684F_0102, // PidTagWlinkFolderType, CLSID_ContactFolder.
        &[
            0x01, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ],
    );
    append_mapi_i32_property(
        &mut older_conflict_properties,
        0x6852_0003, // PidTagWlinkSection.
        4,
    );
    let mut older_conflict_save_rops = Vec::new();
    append_rop_set_properties(
        &mut older_conflict_save_rops,
        1,
        9,
        &older_conflict_properties,
    );
    append_rop_save_changes_message_with_flags(&mut older_conflict_save_rops, 0, 1, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &older_conflict_save_rops,
                &older_conflict_handles,
            )),
        )
        .await
        .unwrap();
    let older_conflict_save_response = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&older_conflict_save_response, &[0x0C, 0x00, 0, 0, 0, 0]),
        "older conflicting WLink Save response: {older_conflict_save_response:02x?}"
    );
    assert!(shortcuts
        .lock()
        .unwrap()
        .iter()
        .any(|shortcut| shortcut.id == my_contacts_id && shortcut.subject == "My Contacts"));
    assert!(!shortcuts
        .lock()
        .unwrap()
        .iter()
        .any(|shortcut| shortcut.subject == "Older client copy must not replace server"));
    let resolved_server_change_number =
        store.mapi_identity_change_numbers.lock().unwrap()[&my_contacts_id];
    assert_ne!(
        resolved_server_change_number,
        server_change_number_before_conflict
    );
    assert_eq!(
        store.mapi_identity_change_keys.lock().unwrap()[&my_contacts_id],
        server_change_key_before_conflict
    );
    assert!(test_mapi_pcl_includes_change_key(
        &store.mapi_identity_predecessor_change_lists.lock().unwrap()[&my_contacts_id],
        &older_conflict_change_key,
    ));

    let mut upload_state_rops = vec![
        0x82, 0x00, 0x00, 0x01, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x01, // RopFastTransferSourceGetBuffer
    ];
    upload_state_rops.extend_from_slice(&4096u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &upload_state_rops,
                &[collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    let upload_state_response = response_rops_from_execute_response(response).await;
    assert_content_upload_final_state_includes(
        &upload_state_response,
        &[],
        &imported_server_change_numbers,
        &[],
    );
    let upload_state = &mapi_fast_transfer_chunks(&upload_state_response)[0].1;
    let cnset_seen_fai = mapi_binary_property_value(upload_state, META_TAG_CNSET_SEEN_FAI);
    for (message_id, ..) in imported_shortcuts {
        let message_counter =
            crate::mapi::identity::global_counter_from_store_id(message_id).unwrap();
        assert!(
            !strict_replguid_globset_contains_counter(
                cnset_seen_fai,
                &globcnt_bytes(message_counter),
            )
            .unwrap(),
            "CnsetSeenFAI must contain the server CN, not the imported MID counter"
        );
    }
    assert!(
        !strict_replguid_globset_contains_counter(
            cnset_seen_fai,
            &globcnt_bytes(resolved_server_change_number),
        )
        .unwrap(),
        "a server-winning conflict CN is not reflected in the client replica and must remain unseen"
    );

    // [MS-OXCFXICS] sections 2.2.1.1.3 and 3.2.5.3: replay the
    // collector's returned state as the next content-download state. The
    // server-winning WLink resolution remains eligible and is downloaded.
    let uploaded_download_state = [
        (META_TAG_IDSET_GIVEN, Vec::new()),
        (
            META_TAG_CNSET_SEEN,
            mapi_binary_property_value(upload_state, META_TAG_CNSET_SEEN).to_vec(),
        ),
        (META_TAG_CNSET_SEEN_FAI, cnset_seen_fai.to_vec()),
        (
            META_TAG_CNSET_READ,
            mapi_binary_property_value(upload_state, META_TAG_CNSET_READ).to_vec(),
        ),
    ];
    let mut resolution_download_rops = Vec::new();
    append_rop_open_folder(
        &mut resolution_download_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    resolution_download_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x39, 0xA1, // content sync, observed Outlook flags 0xa139
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN | OrderByDeliveryTime
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
        resolution_download_rops.extend_from_slice(&u32::to_le_bytes(tag));
    }
    for (state_tag, state_value) in uploaded_download_state {
        resolution_download_rops.extend_from_slice(&[0x75, 0x00, 0x02]);
        resolution_download_rops.extend_from_slice(&state_tag.to_le_bytes());
        resolution_download_rops.extend_from_slice(&(state_value.len() as u32).to_le_bytes());
        if !state_value.is_empty() {
            resolution_download_rops.extend_from_slice(&[0x76, 0x00, 0x02]);
            resolution_download_rops.extend_from_slice(&(state_value.len() as u32).to_le_bytes());
            resolution_download_rops.extend_from_slice(&state_value);
        }
        resolution_download_rops.extend_from_slice(&[0x77, 0x00, 0x02]);
    }
    resolution_download_rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    resolution_download_rops.extend_from_slice(&31_680u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &resolution_download_rops,
                &[1, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let resolution_download_response = response_rops_from_execute_response(response).await;
    let resolution_download =
        strict_content_sync_transfer_from_response(&resolution_download_response).unwrap();
    let downloaded_resolution = resolution_download
        .message_changes
        .iter()
        .find(|message| message.subject == "My Contacts")
        .expect("server-winning WLink resolution is downloaded");
    assert_eq!(
        downloaded_resolution.change_number,
        Some(resolved_server_change_number)
    );
    assert_eq!(
        downloaded_resolution.change_key,
        server_change_key_before_conflict
    );
    assert!(test_mapi_pcl_includes_change_key(
        &downloaded_resolution.predecessor_change_list,
        &older_conflict_change_key,
    ));

    let stored = shortcuts.lock().unwrap().clone();
    assert_eq!(stored.len(), 2);
    let contacts = stored
        .iter()
        .find(|shortcut| shortcut.subject == "Suggested Contacts")
        .unwrap();
    assert_eq!(
        contacts.target_folder_id,
        Some(crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID)
    );
    assert_eq!(contacts.shortcut_type, 0);
    assert_eq!(contacts.flags, 1_048_576);
    assert_eq!(contacts.section, 4);
    assert_eq!(contacts.ordinal, vec![127]);
    assert_eq!(contacts.group_header_id, Some(group_id));
    assert_eq!(contacts.group_name, "My Contacts");
    let mapped_ids = identities
        .lock()
        .unwrap()
        .values()
        .copied()
        .collect::<Vec<_>>();
    assert!(imported_shortcuts
        .iter()
        .all(|(message_id, ..)| mapped_ids.contains(message_id)));

    let mut sync_rops = Vec::new();
    append_rop_open_folder(
        &mut sync_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    sync_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x39, 0xA1, // content sync, observed Outlook flags 0xa139
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN | OrderByDeliveryTime
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
        sync_rops.extend_from_slice(&u32::to_le_bytes(tag));
    }
    for state_tag in [0x4017_0102u32, 0x6796_0102, 0x67DA_0102, 0x67D2_0102] {
        sync_rops.extend_from_slice(&[0x75, 0x00, 0x02]);
        sync_rops.extend_from_slice(&u32::to_le_bytes(state_tag));
        sync_rops.extend_from_slice(&0u32.to_le_bytes());
        sync_rops.extend_from_slice(&[0x77, 0x00, 0x02]);
    }
    sync_rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    sync_rops.extend_from_slice(&31_680u16.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&sync_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    for (message_id, subject, ..) in imported_shortcuts {
        let change = stream
            .message_changes
            .iter()
            .find(|message| message.subject == subject)
            .unwrap();
        let canonical_id = stored
            .iter()
            .find(|shortcut| shortcut.subject == subject)
            .map(|shortcut| shortcut.id)
            .unwrap();
        let expected_change_number =
            store.mapi_identity_change_numbers.lock().unwrap()[&canonical_id];
        let expected_change_key =
            store.mapi_identity_change_keys.lock().unwrap()[&canonical_id].clone();
        let expected_predecessor_change_list =
            store.mapi_identity_predecessor_change_lists.lock().unwrap()[&canonical_id].clone();
        let expected_last_modification_time =
            store.mapi_identity_last_modification_times.lock().unwrap()[&canonical_id];
        assert!(change.associated);
        assert_eq!(change.mid, Some(message_id));
        assert_eq!(
            change.source_key,
            crate::mapi::identity::source_key_for_object_id(message_id)
        );
        assert_eq!(change.change_number, Some(expected_change_number));
        assert_eq!(change.change_key, expected_change_key);
        assert_eq!(
            change.predecessor_change_list,
            expected_predecessor_change_list
        );
        assert_eq!(
            change.last_modification_time,
            Some(expected_last_modification_time)
        );
    }
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
async fn mapi_over_http_empty_conversation_action_sync_is_state_only() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, // Contents
        0x1D, // Unicode | RecoverMode | ForceUnicode | PartialItem
    ]);
    rops.extend_from_slice(&0xA139u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes()); // RestrictionDataSize
    rops.extend_from_slice(&0x0000_000Du32.to_le_bytes());
    let excluded_tags: [u32; 9] = [
        0x1000_001F,
        0x1006_0003,
        0x1007_0003,
        0x1008_001F,
        0x1010_0003,
        0x1011_0003,
        0x3FF8_001F,
        0x3FF9_0102,
        0x300F_0102,
    ];
    rops.extend_from_slice(&(excluded_tags.len() as u16).to_le_bytes());
    for tag in excluded_tags {
        rops.extend_from_slice(&tag.to_le_bytes());
    }
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

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(
        stream.message_changes.is_empty(),
        "an empty canonical store must not synthesize an IPM.ConversationAction FAI"
    );
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

    let client_state = outlook_content_sync_state_properties(
        &[mapi_message_global_counter(&action_id)],
        &[],
        &[],
        &[],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        &client_state,
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

    let client_state =
        outlook_content_sync_state_properties(&[config_object_id >> 16], &[], &[], &[]);
    let response_rops = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;

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

    let client_state = outlook_content_sync_state_properties(&[], &[], &[], &[]);
    let response_rops = outlook_content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;

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

    let decoded_hierarchy = strict_hierarchy_sync_transfer_from_response(&hierarchy_rops).unwrap();
    assert_eq!(decoded_hierarchy.folder_changes.len(), 32);
    assert_eq!(mapi_sync_manifest_counts(&hierarchy_rops), Some((32, 0)));
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
    let mut unchanged = FakeStore::email(
        &unchanged_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Checkpoint unchanged",
    );
    unchanged.modseq = 40;
    unchanged.mailbox_states[0].modseq = 40;
    let mut changed = FakeStore::email(
        &changed_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Checkpoint changed",
    );
    changed.modseq = 41;
    changed.mailbox_states[0].modseq = 41;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![unchanged, changed])),
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

    let client_state = outlook_content_sync_state_properties(
        &[
            mapi_message_global_counter(&unchanged_id),
            mapi_message_global_counter(&changed_id),
            mapi_message_global_counter(&deleted_id),
        ],
        &[40],
        &[],
        &[40],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
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
    let mut deleted_property = 0x67E5_0102u32.to_le_bytes().to_vec();
    deleted_property.extend_from_slice(&(deleted_idset.len() as u32).to_le_bytes());
    deleted_property.extend_from_slice(&deleted_idset);
    assert!(contains_bytes(&response_rops, &deleted_property));
    let first_stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(first_stream.message_changes.len(), 1);
    assert_eq!(
        first_stream.message_changes[0].mid.unwrap() >> 16,
        mapi_message_global_counter(&changed_id)
    );
    assert_eq!(first_stream.message_changes[0].change_number, Some(41));
    assert!(first_stream.read_idset.is_none());
    assert!(first_stream.unread_idset.is_none());
    let restart_state = vec![
        (META_TAG_IDSET_GIVEN, first_stream.idset_given.clone()),
        (META_TAG_CNSET_SEEN, first_stream.cnset_seen.clone()),
        (META_TAG_CNSET_SEEN_FAI, first_stream.cnset_seen_fai.clone()),
        (META_TAG_CNSET_READ, first_stream.cnset_read.clone()),
    ];

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
    let response_rops = outlook_content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::INBOX_FOLDER_ID,
        &restart_state,
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    let restarted_stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(restarted_stream.message_changes.is_empty());
    assert!(restarted_stream.deleted_idset.is_none());
    assert!(restarted_stream.read_idset.is_none());
    assert!(restarted_stream.unread_idset.is_none());
    assert_eq!(restarted_stream.idset_given, first_stream.idset_given);
    assert_eq!(restarted_stream.cnset_seen, first_stream.cnset_seen);
    assert_eq!(restarted_stream.cnset_seen_fai, first_stream.cnset_seen_fai);
    assert_eq!(restarted_stream.cnset_read, first_stream.cnset_read);
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
    // [MS-OXCFXICS] section 3.2.5.3 forbids a message from appearing in
    // both messageChange and readStateChanges during the same download.
    assert!(stream.read_idset.is_none());
    assert!(stream.unread_idset.is_none());
    assert!(!stream.idset_given.is_empty());
    assert!(!stream.cnset_seen.is_empty());
    assert!(stream.cnset_seen_fai.is_empty());
    assert!(stream.cnset_read.is_empty());
    let inbox_source_key = mapi_mailstore::source_key_for_store_id(test_mapi_folder_id(5));
    for message in &stream.message_changes {
        assert!(message.body_tags.contains(&PID_TAG_MESSAGE_FLAGS));
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
    let deleted_id = Uuid::parse_str("67676767-6767-4767-9767-676767676704").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "MS-OXCFXICS 4.5 stream",
    );
    email.modseq = 61;
    email.mailbox_states[0].modseq = 61;
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
    let client_state = outlook_content_sync_state_properties(
        &[
            mapi_message_global_counter(&message_id),
            mapi_message_global_counter(&deleted_id),
        ],
        &[60],
        &[],
        &[],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].mid.unwrap() >> 16,
        mapi_message_global_counter(&message_id)
    );
    assert_eq!(stream.message_changes[0].change_number, Some(61));
    assert!(stream.deleted_idset.is_some());
    // [MS-OXCFXICS] section 3.2.5.3: an object exported as a
    // messageChange cannot also appear in readStateChanges.
    assert!(stream.read_idset.is_none());
    assert!(stream.unread_idset.is_none());
    assert!(stream.cnset_read.is_empty());
    assert!(contains_bytes(
        &response_rops,
        &mapi_deleted_message_idset_property(&[deleted_id])
    ));
    assert_mapi_fast_transfer_marker_sequence(
        &mapi_fast_transfer_chunks(&response_rops)[0].1,
        &[
            FX_INCR_SYNC_PROGRESS_MODE,
            FX_INCR_SYNC_PROGRESS_PER_MSG,
            FX_INCR_SYNC_CHG,
            FX_INCR_SYNC_MESSAGE,
            FX_INCR_SYNC_DEL,
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
    let mut unchanged = FakeStore::email(
        &unchanged_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Incremental unchanged",
    );
    unchanged.modseq = 40;
    unchanged.mailbox_states[0].modseq = 40;
    let mut changed = FakeStore::email(
        &changed_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Incremental changed",
    );
    changed.modseq = 41;
    changed.mailbox_states[0].modseq = 41;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![unchanged, changed])),
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

    let client_state = outlook_content_sync_state_properties(
        &[
            mapi_message_global_counter(&unchanged_id),
            mapi_message_global_counter(&changed_id),
        ],
        &[40],
        &[],
        &[40],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].mid.unwrap() >> 16,
        mapi_message_global_counter(&changed_id)
    );
    assert_eq!(stream.message_changes[0].change_number, Some(41));
    assert!(stream.read_idset.is_none());
    assert!(stream.unread_idset.is_none());
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

    let moved_counter = mapi_message_global_counter(&moved_id);
    let source_state = outlook_content_sync_state_properties(&[moved_counter], &[], &[], &[]);
    let target_state = outlook_content_sync_state_properties(&[], &[], &[], &[]);
    let source_rops = outlook_content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::INBOX_FOLDER_ID,
        &source_state,
    )
    .await;
    let target_rops = outlook_content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::ARCHIVE_FOLDER_ID,
        &target_state,
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&source_rops), None);
    assert!(contains_bytes(
        &source_rops,
        &mapi_deleted_message_idset_property(&[moved_id])
    ));
    assert_content_final_state_includes(&source_rops, &[], &[]);
    assert_eq!(mapi_sync_manifest_counts(&target_rops), Some((0, 1)));
    let target_stream = strict_content_sync_transfer_from_response(&target_rops).unwrap();
    assert_eq!(target_stream.message_changes.len(), 1);
    assert_eq!(
        target_stream.message_changes[0].mid.unwrap() >> 16,
        moved_counter
    );
    assert_eq!(target_stream.message_changes[0].change_number, Some(41));
    assert!(target_stream.read_idset.is_none());
    assert!(target_stream.unread_idset.is_none());
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

    let client_state = outlook_content_sync_state_properties(
        &[mapi_message_global_counter(&deleted_id)],
        &[],
        &[],
        &[],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;

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
async fn mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state() {
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

    let client_state = outlook_content_sync_state_properties(
        &[mapi_message_global_counter(&message_id)],
        &[46],
        &[],
        &[46],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].mid.unwrap() >> 16,
        mapi_message_global_counter(&message_id)
    );
    assert_eq!(stream.message_changes[0].change_number, Some(47));
    // [MS-OXCFXICS] section 3.2.5.3: the canonical read transition is
    // represented by this new object CN, not a duplicate IncrSyncRead row.
    assert!(stream.read_idset.is_none());
    assert!(stream.unread_idset.is_none());
    assert_content_final_state_includes(&response_rops, &[message_id], &[47]);
    assert!(contains_bytes(
        &response_rops,
        &mapi_message_cnset_property(META_TAG_CNSET_READ, &[46])
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

    let client_state = outlook_content_sync_state_properties(
        &[mapi_message_global_counter(&message_id)],
        &[40],
        &[],
        &[40],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &client_state,
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].mid.unwrap() >> 16,
        mapi_message_global_counter(&message_id)
    );
    assert_eq!(stream.message_changes[0].change_number, Some(41));
    assert!(stream.read_idset.is_none());
    assert!(stream.unread_idset.is_none());
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
    let folder_id_counter = crate::mapi::identity::INBOX_FOLDER_COUNTER;
    let email = FakeStore::email(
        "57575757-5757-5757-5757-575757575757",
        inbox_id,
        "inbox",
        "Hierarchy aggregate message",
    );
    let message_change_number = mapi_mailstore::canonical_message_change_number(&email);
    assert_ne!(folder_id_counter, message_change_number);
    let local_commit_time_max = mapi_mailstore::filetime_from_change_number(message_change_number);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let folder_change_numbers = store.mapi_identity_change_numbers.clone();
    let folder_change_keys = store.mapi_identity_change_keys.clone();
    let folder_predecessor_change_lists = store.mapi_identity_predecessor_change_lists.clone();
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
    let inbox_uuid = Uuid::parse_str(inbox_id).unwrap();
    let change_number = folder_change_numbers.lock().unwrap()[&inbox_uuid];
    let change_key = folder_change_keys.lock().unwrap()[&inbox_uuid].clone();
    let predecessor_change_list =
        folder_predecessor_change_lists.lock().unwrap()[&inbox_uuid].clone();
    assert_ne!(
        change_number, folder_id_counter,
        "the hierarchy change CN must be distinct from the Inbox FID"
    );
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
        &globcnt_bytes(folder_id_counter)
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
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        FakeStore::account().account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let mut calendar_identification_property = 0x36D0_0102u32.to_le_bytes().to_vec();
    calendar_identification_property
        .extend_from_slice(&(calendar_entry_id.len() as u32).to_le_bytes());
    calendar_identification_property.extend_from_slice(&calendar_entry_id);
    assert!(contains_bytes(
        &response_rops,
        &calendar_identification_property
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
        Some("Inbox")
    );
    assert_eq!(
        decoded
            .folder_changes
            .first()
            .and_then(|folder| folder.folder_id),
        Some(test_mapi_folder_id(5))
    );
    assert_eq!(
        decoded
            .folder_changes
            .first()
            .map(|folder| folder.parent_source_key.as_slice()),
        Some(&[][..])
    );
    assert!(decoded
        .folder_changes
        .iter()
        .all(|folder| folder.display_name != "Top of Information Store"));
    assert!(decoded
        .folder_changes
        .iter()
        .all(|folder| folder.folder_id.is_some()));
    assert!(decoded.folder_changes.iter().all(|folder| {
        let expected_parent = match folder.display_name.as_str() {
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
    let mut ipm_child_parent_source_key = 0x65E1_0102u32.to_le_bytes().to_vec();
    ipm_child_parent_source_key.extend_from_slice(&0u32.to_le_bytes());
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
    assert!(sync_issues.parent_source_key.is_empty());
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
    assert!(decoded.folder_changes[projects]
        .parent_source_key
        .is_empty());
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
    assert_eq!(inbox.folder_id, None);
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
async fn mapi_over_http_hierarchy_sync_replays_version_2_server_checkpoint() {
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
            serde_json::json!({
                "source": "emsmdb-ics-download",
                "syncType": 2,
                "syncRootFolderId": test_mapi_folder_id(4),
                "hierarchySyncVersion": 2
            }),
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
                "hierarchySyncVersion": 3
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
        &[],
        &[],
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
async fn mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download() {
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
    let completed_state = strict_hierarchy_sync_transfer_from_response(&response_rops)
        .expect("completed hierarchy state");
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
        Some(3)
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
        &completed_state.idset_given,
        &completed_state.cnset_seen,
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

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops),
        Some((OUTLOOK_IPM_HIERARCHY_FOLDER_COUNT, 0))
    );
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_to_message_returns_message_content_without_bcc() {
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
    let chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(chunks.len(), 1, "{response_rops:02x?}");
    let transfer = &chunks[0].1;
    assert!(transfer.starts_with(&PID_TAG_SUBJECT_W.to_le_bytes()));
    assert!(!contains_bytes(transfer, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(transfer, &utf16z("CopyTo message")));
    assert!(contains_bytes(
        transfer,
        &utf16z("CopyTo body from canonical mail")
    ));
    assert!(!contains_bytes(transfer, b"hidden-copyto@example.test"));
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
async fn mapi_over_http_fast_transfer_copy_properties_message_returns_message_content_without_bcc()
{
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
    let chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(chunks.len(), 1, "{response_rops:02x?}");
    let transfer = &chunks[0].1;
    assert!(transfer.starts_with(&PID_TAG_SUBJECT_W.to_le_bytes()));
    assert!(!contains_bytes(transfer, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(transfer, &utf16z("CopyProperties message")));
    assert!(contains_bytes(
        transfer,
        &utf16z("CopyProperties body from canonical mail")
    ));
    assert!(!contains_bytes(transfer, b"hidden-copyprops@example.test"));
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
    let client_state = outlook_content_sync_state_properties(
        &[
            mapi_message_global_counter(&first_message_id),
            mapi_message_global_counter(&second_message_id),
        ],
        &[],
        &[],
        &[],
    );
    let response_rops = outlook_content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::TRASH_FOLDER_ID,
        &client_state,
    )
    .await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    let deleted = stream.deleted_idset.as_deref().expect("deleted IDSET");
    for message_id in [first_message_id, second_message_id] {
        assert!(strict_replid_globset_contains_counter(
            deleted,
            &globcnt_bytes(mapi_message_global_counter(&message_id))
        )
        .unwrap());
    }
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
async fn mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state() {
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

    const STALE_CLIENT_COUNTER: u64 = 0x0000_0000_4321;
    let client_state =
        outlook_content_sync_state_properties(&[STALE_CLIENT_COUNTER], &[], &[], &[]);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_outlook_content_sync_manifest_get_buffer_with_state(
        &mut rops,
        1,
        2,
        31_680,
        &client_state,
    );
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&31_680u16.to_le_bytes());

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
    let chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(chunks.len(), 2);
    let final_state = &chunks[1].1;
    let idset_given = mapi_binary_property_value(final_state, META_TAG_IDSET_GIVEN);
    assert!(strict_replguid_globset_contains_counter(
        idset_given,
        &globcnt_bytes(mapi_message_global_counter(&message_id))
    )
    .unwrap());
    assert!(!strict_replguid_globset_contains_counter(
        idset_given,
        &globcnt_bytes(STALE_CLIENT_COUNTER)
    )
    .unwrap());
    let cnset_seen = mapi_binary_property_value(final_state, META_TAG_CNSET_SEEN);
    assert!(strict_replguid_globset_contains_counter(cnset_seen, &globcnt_bytes(41)).unwrap());
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
    assert_content_upload_final_state_includes(&response_rops, &[], &[], &[]);
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
async fn mapi_over_http_invalid_download_state_upload_poison_context_before_get_buffer() {
    let mailbox_id = "55555555-5555-4555-9555-555555555531";
    let subject = "Invalid upload must not leak this manifest";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "42424242-4242-4242-8242-424242424531",
            mailbox_id,
            "inbox",
            subject,
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

    let mut malformed_cnset = mapi_mailstore::STORE_REPLICA_GUID.to_vec();
    malformed_cnset.extend_from_slice(&[0x52, 0x01]); // truncated GLOBSET Range command
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, // Contents, SendOptions
        0x20, 0x00, // SynchronizationFlags: Normal
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&META_TAG_CNSET_SEEN.to_le_bytes());
    rops.extend_from_slice(&(malformed_cnset.len() as u32).to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(malformed_cnset.len() as u32).to_le_bytes());
    rops.extend_from_slice(&malformed_cnset);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer on the same context
    ]);
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

    // [MS-OXCFXICS] sections 3.1.5.4.3.2, 3.1.5.4.3.2.4, and 3.2.5.2:
    // a malformed Range is not a valid basis for a download context.
    assert!(contains_bytes(
        &response_rops,
        &[0x77, 0x02, 0xB6, 0x04, 0x00, 0x00]
    ));
    assert!(
        contains_bytes(&response_rops, &[0x4E, 0x02, 0xB6, 0x04, 0x00, 0x00]),
        "GetBuffer reused an ICS context after RpcFormat: {response_rops:02x?}"
    );
    assert!(
        mapi_fast_transfer_chunks(&response_rops).is_empty(),
        "an invalidated download context must not return FastTransfer payload"
    );
    assert!(!contains_bytes(&response_rops, &utf16z(subject)));
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
    let imported_change_number = mapi_mailstore::canonical_message_change_number(&imported_email);
    assert_content_upload_final_state_includes(
        &response_rops,
        &[imported_change_number],
        &[],
        &[imported_change_number],
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
    let saved_change_number = mapi_mailstore::canonical_message_change_number(&saved_email);
    assert_content_upload_final_state_includes(
        &response_rops,
        &[saved_change_number],
        &[],
        &[saved_change_number],
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
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x02, &[test_mapi_message_id(message_id)]);
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
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));
    let state_chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(state_chunks.len(), 1);
    assert!(
        !contains_bytes(&state_chunks[0].1, &META_TAG_IDSET_GIVEN.to_le_bytes()),
        "[MS-OXCFXICS] section 3.2.5.2.1 forbids returning MetaTagIdsetGiven from upload state"
    );
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
    assert_content_upload_final_state_includes(&response_rops, &[], &[], &[]);
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
    assert_content_upload_final_state_includes(&response_rops, &[], &[], &[]);
}

#[tokio::test]
async fn mapi_over_http_set_local_replica_midset_deleted_persists_folder_scoped_ranges(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon_response.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let reservation_start = storage
        .reserve_mapi_local_replica_ids(fixture.account_id, 0x0100)
        .await?;
    let min_counter = reservation_start + 0x20;
    let max_counter = reservation_start + 0x2f;
    let min_long_term_id = crate::mapi::identity::long_term_id_from_object_id(
        crate::mapi::identity::mapi_store_id(min_counter),
    )
    .ok_or_else(|| anyhow::anyhow!("invalid minimum LongTermID fixture"))?;
    let max_long_term_id = crate::mapi::identity::long_term_id_from_object_id(
        crate::mapi::identity::mapi_store_id(max_counter),
    )
    .ok_or_else(|| anyhow::anyhow!("invalid maximum LongTermID fixture"))?;

    let mut deleted_ranges = 1u32.to_le_bytes().to_vec();
    deleted_ranges.extend_from_slice(&min_long_term_id);
    deleted_ranges.extend_from_slice(&max_long_term_id);
    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x93, 0x00, 0x01, // RopSetLocalReplicaMidsetDeleted on Folder
    ]);
    rops.extend_from_slice(&(deleted_ranges.len() as u16).to_le_bytes());
    rops.extend_from_slice(&deleted_ranges);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // OpenCollector from the Folder.
        0x93, 0x00, 0x02, // SetLocalReplicaMidsetDeleted on Collector: invalid.
    ]);
    rops.extend_from_slice(&(deleted_ranges.len() as u16).to_le_bytes());
    rops.extend_from_slice(&deleted_ranges);

    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x93, 0x01, 0, 0, 0, 0]));
    assert!(
        contains_bytes(&response_rops, &[0x93, 0x02, 0x02, 0x01, 0x04, 0x80]),
        "[MS-OXCFXICS] section 2.2.3.2.4.8.1 requires a Folder input object: {response_rops:02x?}"
    );
    let persisted = sqlx::query_as::<_, (i64, Uuid, i64, i64)>(
        r#"
        SELECT folder_id, replica_guid, min_global_counter, max_global_counter
        FROM mapi_local_replica_deleted_ranges
        WHERE account_id = $1
        "#,
    )
    .bind(fixture.account_id)
    .fetch_all(storage.pool())
    .await?;
    assert_eq!(
        persisted,
        vec![(
            crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64,
            Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID),
            min_counter as i64,
            max_counter as i64,
        )]
    );

    let source_key = crate::mapi::identity::source_key_for_object_id(
        crate::mapi::identity::mapi_store_id(min_counter),
    );
    let change_key = vec![0xB8; 20];
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);
    let replay = storage
        .commit_mapi_navigation_shortcut_import(
            crate::store::CommitMapiNavigationShortcutImportInput {
                shortcut: crate::store::UpsertMapiNavigationShortcutInput {
                    id: None,
                    account_id: fixture.account_id,
                    subject: "Deleted range must not import".to_string(),
                    target_folder_id: None,
                    shortcut_type: 4,
                    flags: 0,
                    save_stamp: 1_537_819_608,
                    section: 4,
                    ordinal: vec![127],
                    group_header_id: Some(Uuid::parse_str("b7f00600-0000-0000-c000-000000000046")?),
                    group_name: "My Contacts".to_string(),
                    client_properties:
                        crate::store::MapiNavigationShortcutClientProperties::default(),
                },
                identity: crate::store::MapiFaiImportedIdentity {
                    source_key: source_key.clone(),
                    change_key,
                    predecessor_change_list,
                    last_modification_time: test_filetime("2026-07-19", "14:00") as u64,
                },
                fail_on_conflict: false,
            },
        )
        .await;
    assert!(replay
        .as_ref()
        .err()
        .is_some_and(|error| { error.is::<crate::store::MapiFaiImportObjectDeleted>() }));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT
                (SELECT COUNT(*) FROM mapi_navigation_shortcuts
                 WHERE account_id = $1)
              + (SELECT COUNT(*) FROM mapi_object_identities
                 WHERE account_id = $1 AND object_kind = 'navigation_shortcut'
                   AND source_key = $2)
              + (SELECT COUNT(*) FROM mail_change_log
                 WHERE account_id = $1 AND object_kind = 'navigation_shortcut')
            "#,
        )
        .bind(fixture.account_id)
        .bind(&source_key)
        .fetch_one(storage.pool())
        .await?,
        0
    );

    fixture.cleanup().await?;
    Ok(())
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
    let conflicting_canonical_id = Uuid::parse_str("abababab-abab-4bab-8bab-abababababab").unwrap();
    let imported_canonical_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(conflicting_canonical_id, imported_message_id);
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
    let save_offset = response_rops
        .windows(6)
        .position(|window| window == [0x0C, 0x01, 0, 0, 0, 0])
        .unwrap();
    let saved_message_id = crate::mapi::identity::object_id_from_wire_id(
        &response_rops[save_offset + 7..save_offset + 15],
    )
    .unwrap();
    assert_ne!(
        saved_message_id, imported_message_id,
        "the new canonical message must receive a non-conflicting MAPI identity"
    );
    let allocated = mapi_identities.lock().unwrap();
    assert_eq!(allocated[&conflicting_canonical_id], imported_message_id);
    assert_eq!(allocated[&imported_canonical_id], saved_message_id);
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
    *store.next_mapi_global_counter.lock().unwrap() =
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 42;
    let reserved_start = store
        .reserve_mapi_local_replica_ids(account.account_id, 1)
        .await
        .unwrap();
    let associated_object_id = crate::mapi::identity::mapi_store_id(reserved_start);
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let imported_change_key = associated_source_key.clone();
    let mut imported_pcl = vec![imported_change_key.len() as u8];
    imported_pcl.extend_from_slice(&imported_change_key);
    let imported_last_modification_time = test_filetime("2026-07-18", "15:16");
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
        &associated_source_key,
    );
    append_mapi_i64_property(
        &mut property_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time,
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
        &imported_change_key,
    );
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_pcl,
    );
    append_mapi_binary_property(&mut property_values, 0x7C07_0102, outlook_prefs_dictionary);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0x10);
    rops.extend_from_slice(&7u16.to_le_bytes());
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
        assert!(config.properties_json.get("0x65e00102").is_none());
        assert!(config.properties_json.get("0x65e20102").is_none());
        assert!(config.properties_json.get("0x65e30102").is_none());
        assert!(config.properties_json.get("0x30080040").is_none());
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
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN | OrderByDeliveryTime
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
    assert_eq!(message.mid, Some(associated_object_id));
    assert_eq!(message.change_key, imported_change_key);
    assert_eq!(message.predecessor_change_list, imported_pcl);
    assert_eq!(
        message.last_modification_time,
        Some(imported_last_modification_time as u64)
    );
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
async fn mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content() {
    // Outlook traces 202607181515 and 202607211751 import the Inbox FAI and
    // save MessageListSettings with PidTagRoamingDatatypes explicitly set to
    // zero and no roaming stream. MS-OXCFXICS section 2.2.3.2.4.2.1 defines
    // that upload, section 3.1.5.3 defines the server identity handling, and
    // section 3.3.5.8.7 describes the corresponding client workflow.
    // MS-OXCPRPT section 3.2.5.4 and MS-OXCMSG section 3.2.5.3 require the
    // accepted property change to be committed, while MS-OXOCFG sections
    // 2.2.2.1 and 2.2.5.1 define zero as no dictionary stream.
    let imported_change_key = vec![
        0x51, 0xa1, 0x66, 0x72, 0x14, 0x93, 0x5c, 0x48, 0xaa, 0x14, 0xe7, 0xdc, 0xb0, 0x5e, 0x0d,
        0x31, 0x00, 0x00, 0x00, 0x00, 0x04, 0x15,
    ];
    let mut imported_pcl = vec![imported_change_key.len() as u8];
    imported_pcl.extend_from_slice(&imported_change_key);
    let imported_last_modification_time = test_filetime("2026-07-18", "15:15");

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
    *store.next_mapi_global_counter.lock().unwrap() = 0x023e;
    let reserved_start = store
        .reserve_mapi_local_replica_ids(account.account_id, 1)
        .await
        .unwrap();
    assert_eq!(reserved_start, 0x023e);
    let imported_message_id = crate::mapi::identity::mapi_store_id(reserved_start);
    let imported_source_key = crate::mapi::identity::source_key_for_object_id(imported_message_id);
    let associated_configs = store.associated_configs.clone();
    let mapi_identities = store.mapi_identities.clone();
    let mapi_identity_source_keys = store.mapi_identity_source_keys.clone();
    let mapi_identity_change_keys = store.mapi_identity_change_keys.clone();
    let mapi_identity_predecessor_change_lists =
        store.mapi_identity_predecessor_change_lists.clone();
    let mapi_identity_last_modification_times = store.mapi_identity_last_modification_times.clone();
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut import_values = Vec::new();
    append_mapi_binary_property(&mut import_values, PID_TAG_SOURCE_KEY, &imported_source_key);
    append_mapi_i64_property(
        &mut import_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time,
    );
    append_mapi_binary_property(&mut import_values, PID_TAG_CHANGE_KEY, &imported_change_key);
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_pcl,
    );
    let mut save_values = Vec::new();
    append_mapi_utf16_property(
        &mut save_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Configuration.MessageListSettings",
    );
    append_mapi_i32_property(&mut save_values, 0x7C06_0003, 0); // PidTagRoamingDatatypes.
    append_mapi_i32_property(&mut save_values, PID_TAG_MESSAGE_FLAGS, 0x40);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange.
        0x50, // ImportFlagAssociated | ImportFlagFailOnConflict.
    ]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&import_values);
    append_rop_set_properties(&mut rops, 3, 3, &save_values);
    append_rop_save_changes_message_with_flags(&mut rops, 3, 3, 0x08);

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
    assert!(contains_bytes(&response_rops, &[0x0A, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x03, 0, 0, 0, 0]));
    assert!(
        contains_bytes(&response_rops, &mapi_wire_id_bytes(imported_message_id)),
        "SaveChangesMessage must acknowledge imported MID 0x23e: {response_rops:02x?}"
    );

    let config = associated_configs
        .lock()
        .unwrap()
        .iter()
        .find(|config| config.message_class == "IPM.Configuration.MessageListSettings")
        .cloned()
        .expect("persisted MessageListSettings FAI");
    assert_eq!(
        mapi_identities.lock().unwrap().get(&config.id).copied(),
        Some(imported_message_id)
    );
    assert_eq!(
        mapi_identity_source_keys
            .lock()
            .unwrap()
            .get(&config.id)
            .cloned(),
        Some(imported_source_key.clone())
    );
    assert_eq!(
        mapi_identity_change_keys
            .lock()
            .unwrap()
            .get(&config.id)
            .cloned(),
        Some(imported_change_key.clone())
    );
    assert_eq!(
        mapi_identity_predecessor_change_lists
            .lock()
            .unwrap()
            .get(&config.id)
            .cloned(),
        Some(imported_pcl.clone())
    );
    assert_eq!(
        mapi_identity_last_modification_times
            .lock()
            .unwrap()
            .get(&config.id)
            .copied(),
        Some(imported_last_modification_time as u64)
    );
    for identity_tag in ["0x65e00102", "0x65e20102", "0x65e30102", "0x30080040"] {
        assert!(
            config.properties_json.get(identity_tag).is_none(),
            "{identity_tag} must exist only in the durable identity record"
        );
    }
    assert_eq!(
        config.properties_json["0x7c060003"],
        serde_json::json!({"type": "i32", "value": 0})
    );
    assert!(config.properties_json.get("0x7c070102").is_none());
    assert!(config.properties_json.get("0x7c080102").is_none());
    assert!(config.properties_json.get("0x0e0b0102").is_none());

    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut reopen_headers = mapi_headers("Execute");
    reopen_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&reconnect)).unwrap(),
    );
    let mut reopen_rops = Vec::new();
    append_rop_open_folder(
        &mut reopen_rops,
        0,
        1,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    append_rop_open_message(
        &mut reopen_rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        imported_message_id,
    );
    append_rop_get_properties_specific(
        &mut reopen_rops,
        2,
        &[
            PID_TAG_SOURCE_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
        ],
    );
    let reopen_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &reopen_headers,
            &execute_body(&rop_buffer(&reopen_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let reopen_response_body = response_bytes(reopen_response).await;
    let (reopen_response_rops, reopen_response_handles) =
        response_rops_and_handles_from_execute_body(&reopen_response_body);
    assert_eq!(reopen_response_handles.len(), 3);
    assert_ne!(reopen_response_handles[2], u32::MAX);
    assert!(
        contains_bytes(&reopen_response_rops, &[0x03, 0x02, 0, 0, 0, 0]),
        "OpenMessage for imported MID 0x23e failed: {reopen_response_rops:02x?}"
    );
    assert!(contains_bytes(&reopen_response_rops, &imported_source_key));
    assert!(contains_bytes(&reopen_response_rops, &imported_change_key));
    assert!(contains_bytes(&reopen_response_rops, &imported_pcl));

    // Exact second Execute from trace 202607211751: the Message handle opened
    // above is reused for direct messageContent CopyTo with BestBody and
    // ForceUnicode, followed by the extended 0x7BC0-byte GetBuffer form.
    let mut copy_rops = Vec::new();
    copy_rops.extend_from_slice(&[0x4D, 0x00, 0x02, 0x03]);
    copy_rops.push(0);
    copy_rops.extend_from_slice(&0x0000_2000u32.to_le_bytes());
    copy_rops.push(0x09);
    copy_rops.extend_from_slice(&0u16.to_le_bytes());
    copy_rops.extend_from_slice(&[0x4E, 0x00, 0x03]);
    copy_rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    copy_rops.extend_from_slice(&0x7BC0u16.to_le_bytes());
    renew_mapi_request_id(&mut reopen_headers);
    let copy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &reopen_headers,
            &execute_body(&rop_buffer(
                &copy_rops,
                &[
                    reopen_response_handles[0],
                    reopen_response_handles[1],
                    reopen_response_handles[2],
                    u32::MAX,
                ],
            )),
        )
        .await
        .unwrap();
    let copy_response_body = response_bytes(copy_response).await;
    let (copy_response_rops, copy_response_handles) =
        response_rops_and_handles_from_execute_body(&copy_response_body);
    assert_eq!(copy_response_handles.len(), 4);
    assert_ne!(copy_response_handles[3], u32::MAX);
    let chunks = mapi_fast_transfer_chunks(&copy_response_rops);
    assert_eq!(chunks.len(), 1, "{copy_response_rops:02x?}");
    assert_eq!(chunks[0].0, 0x0003);
    let transfer = &chunks[0].1;
    let mut offset = 0;
    let mut properties = Vec::new();
    while offset < transfer.len() {
        let property = strict_parse_fast_transfer_property(transfer, offset)
            .unwrap_or_else(|error| panic!("{error}: {transfer:02x?}"));
        offset = property.next_offset;
        properties.push(property);
    }
    let roaming_datatypes = properties
        .iter()
        .filter(|property| property.tag == 0x7C06_0003)
        .collect::<Vec<_>>();
    assert_eq!(roaming_datatypes.len(), 1, "{transfer:02x?}");
    assert_eq!(strict_decode_i32_property(roaming_datatypes[0]).unwrap(), 0);
    assert!(!properties
        .iter()
        .any(|property| property.tag == 0x7C07_0102));
    assert!(!properties
        .iter()
        .any(|property| property.tag == 0x7C08_0102));
    assert!(!properties
        .iter()
        .any(|property| property.tag == 0x0E0B_0102));
    assert!(!properties
        .iter()
        .any(|property| property.tag == 0x801F_001F));
    assert!(!properties
        .iter()
        .any(|property| property.tag == 0x836B_001F));

    // Exact third Execute from trace 202607211931: Outlook releases the
    // FastTransfer handle, then directly reads the persisted Message handle.
    // The client did not write 0x0E0B0102, so the requested property is absent
    // and must be returned as ecNotFound in a FlaggedPropertyRow, following
    // [MS-OXCPRPT] section 3.2.5.1 and [MS-OXCDATA] sections 2.4.2,
    // 2.8.1.2, and 2.11.5.
    let direct_tags = [
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_LAST_MODIFICATION_TIME,
        0x0E0B_0102,
        PID_TAG_MESSAGE_CLASS_W,
    ];
    let mut direct_get_rops = vec![0x01, 0x00, 0x00]; // RopRelease FastTransfer handle.
    direct_get_rops.extend_from_slice(&[0x07, 0x00, 0x01]);
    direct_get_rops.extend_from_slice(&0u16.to_le_bytes()); // PropertySizeLimit.
    direct_get_rops.extend_from_slice(&0u16.to_le_bytes()); // WantUnicode.
    direct_get_rops.extend_from_slice(&(direct_tags.len() as u16).to_le_bytes());
    for tag in direct_tags {
        direct_get_rops.extend_from_slice(&tag.to_le_bytes());
    }
    renew_mapi_request_id(&mut reopen_headers);
    let direct_get_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &reopen_headers,
            &execute_body(&rop_buffer(
                &direct_get_rops,
                &[copy_response_handles[3], reopen_response_handles[2]],
            )),
        )
        .await
        .unwrap();
    let direct_get_response_rops = response_rops_from_execute_response(direct_get_response).await;
    assert_eq!(
        direct_get_response_rops.get(..6),
        Some(&[0x07, 0x01, 0, 0, 0, 0][..]),
        "GetPropertiesSpecific must use the success response from [MS-OXCROPS] section 2.2.8.3.2: {direct_get_response_rops:02x?}"
    );
    assert_eq!(
        direct_get_response_rops.get(6),
        Some(&1),
        "GetPropertiesSpecific must return a FlaggedPropertyRow: {direct_get_response_rops:02x?}"
    );
    let mut cell_offset = 7;
    for tag in [PID_TAG_CHANGE_KEY, PID_TAG_PREDECESSOR_CHANGE_LIST] {
        assert_eq!(
            direct_get_response_rops[cell_offset], 0,
            "{tag:#010x} must have a value: {direct_get_response_rops:02x?}"
        );
        cell_offset += 1;
        let value_len = u16::from_le_bytes(
            direct_get_response_rops[cell_offset..cell_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        cell_offset += 2 + value_len;
    }
    assert_eq!(
        direct_get_response_rops[cell_offset], 0,
        "PidTagLastModificationTime must have a value: {direct_get_response_rops:02x?}"
    );
    cell_offset += 1 + 8;
    assert_eq!(
        direct_get_response_rops[cell_offset], 0x0A,
        "absent 0x0E0B0102 must be a flagged error: {direct_get_response_rops:02x?}"
    );
    assert_eq!(
        u32::from_le_bytes(
            direct_get_response_rops[cell_offset + 1..cell_offset + 5]
                .try_into()
                .unwrap(),
        ),
        0x8004_010F,
        "absent 0x0E0B0102 must be ecNotFound: {direct_get_response_rops:02x?}"
    );
    assert!(!contains_bytes(
        &direct_get_response_rops,
        b"OLPrefsVersion"
    ));

    let sync_response = content_sync_response_rops_for_store_with_flags(
        store.clone(),
        crate::mapi::identity::INBOX_FOLDER_ID,
        &[],
        0x0010,
    )
    .await;
    let sync = strict_content_sync_transfer_from_response(&sync_response)
        .unwrap_or_else(|error| panic!("{error}: {sync_response:02x?}"));
    let downloaded = sync
        .message_changes
        .iter()
        .find(|message| message.source_key == imported_source_key)
        .expect("imported MessageListSettings FAI in Inbox ICS");
    assert!(downloaded.associated);
    assert_eq!(downloaded.mid, Some(imported_message_id));
    assert_eq!(downloaded.change_key, imported_change_key);
    assert_eq!(downloaded.predecessor_change_list, imported_pcl);
    assert_eq!(
        downloaded.last_modification_time,
        Some(imported_last_modification_time as u64)
    );
    assert!(downloaded.body_tags.contains(&0x7C06_0003));
    for absent_tag in [
        0x7C07_0102,
        0x7C08_0102,
        0x0E0B_0102,
        0x801F_001F,
        0x836B_001F,
    ] {
        assert!(!downloaded.body_tags.contains(&absent_tag));
    }

    let change_sequence_before_conflict = store
        .mapi_sync_changes
        .lock()
        .unwrap()
        .current_change_sequence;
    let conflicting_change_key = vec![
        0x61, 0xa1, 0x66, 0x72, 0x14, 0x93, 0x5c, 0x48, 0xaa, 0x14, 0xe7, 0xdc, 0xb0, 0x5e, 0x0d,
        0x31, 0x00, 0x00, 0x00, 0x00, 0x04, 0x16,
    ];
    let mut conflicting_pcl = vec![conflicting_change_key.len() as u8];
    conflicting_pcl.extend_from_slice(&conflicting_change_key);
    let mut conflicting_import_values = Vec::new();
    append_mapi_binary_property(
        &mut conflicting_import_values,
        PID_TAG_SOURCE_KEY,
        &imported_source_key,
    );
    append_mapi_i64_property(
        &mut conflicting_import_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time + 10_000_000,
    );
    append_mapi_binary_property(
        &mut conflicting_import_values,
        PID_TAG_CHANGE_KEY,
        &conflicting_change_key,
    );
    append_mapi_binary_property(
        &mut conflicting_import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &conflicting_pcl,
    );
    let mut conflicting_save_values = Vec::new();
    append_mapi_utf16_property(
        &mut conflicting_save_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Configuration.MessageListSettings",
    );
    append_mapi_utf16_property(
        &mut conflicting_save_values,
        PID_TAG_SUBJECT_W,
        "FailOnConflict must not overwrite Inbox FAI",
    );
    let mut conflicting_rops = Vec::new();
    append_rop_open_folder(
        &mut conflicting_rops,
        0,
        1,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    conflicting_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange.
        0x50, // ImportFlagAssociated | ImportFlagFailOnConflict.
    ]);
    conflicting_rops.extend_from_slice(&4u16.to_le_bytes());
    conflicting_rops.extend_from_slice(&conflicting_import_values);
    append_rop_set_properties(&mut conflicting_rops, 3, 2, &conflicting_save_values);
    append_rop_save_changes_message_with_flags(&mut conflicting_rops, 3, 3, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &conflicting_rops,
                &[1, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let conflict_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &conflict_response,
        &[0x0C, 0x03, 0x09, 0x01, 0x04, 0x80]
    ));
    assert_eq!(associated_configs.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .mapi_sync_changes
            .lock()
            .unwrap()
            .current_change_sequence,
        change_sequence_before_conflict,
        "a rejected FAI import must not publish content or journal state"
    );
    assert_eq!(
        mapi_identity_change_keys
            .lock()
            .unwrap()
            .get(&config.id)
            .cloned(),
        Some(imported_change_key)
    );
    assert_eq!(
        mapi_identity_predecessor_change_lists
            .lock()
            .unwrap()
            .get(&config.id)
            .cloned(),
        Some(imported_pcl)
    );
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
async fn mapi_over_http_empty_inbox_fai_sync_exports_no_default_view() {
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
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN | OrderByDeliveryTime
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
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.FolderDesign.NamedView")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Compact")));
    assert!(!contains_bytes(&response_rops, &utf16z("Messages")));
}

#[tokio::test]
async fn mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset() {
    let account = FakeStore::account();
    let config_id = Uuid::parse_str("e0fdf7ca-15f8-4c62-bf51-d543d69a1401").unwrap();
    let config_object_id = crate::mapi::identity::mapi_store_id(197_401);
    let config_change_number = 262_425;
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: config_id,
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
                }
            }),
        }])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(config_id, config_object_id)]))),
        mapi_identity_change_numbers: Arc::new(Mutex::new(HashMap::from([(
            config_id,
            config_change_number,
        )]))),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 88,
        current_modseq: 88,
        ..Default::default()
    };
    let empty_state = vec![
        (META_TAG_IDSET_GIVEN, Vec::new()),
        (META_TAG_CNSET_SEEN, Vec::new()),
        (META_TAG_CNSET_SEEN_FAI, Vec::new()),
        (META_TAG_CNSET_READ, Vec::new()),
    ];
    let initial_response = outlook_content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::INBOX_FOLDER_ID,
        &empty_state,
    )
    .await;
    let initial_stream = strict_content_sync_transfer_from_response(&initial_response).unwrap();
    assert!(!initial_stream.message_changes.is_empty());
    assert!(initial_stream
        .message_changes
        .iter()
        .all(|message| message.associated));
    assert!(initial_stream.cnset_seen.is_empty());
    assert!(initial_stream.cnset_read.is_empty());
    let uploaded_state = vec![
        (META_TAG_IDSET_GIVEN, initial_stream.idset_given.clone()),
        (META_TAG_CNSET_SEEN, initial_stream.cnset_seen.clone()),
        (
            META_TAG_CNSET_SEEN_FAI,
            initial_stream.cnset_seen_fai.clone(),
        ),
        (META_TAG_CNSET_READ, initial_stream.cnset_read.clone()),
    ];

    // Outlook legitimately uploads zero-length normal/read CNSET streams for
    // this FAI-only scope. Their presence does not make the FAI CNSET optional.
    store.mapi_checkpoints.lock().unwrap().clear();
    let response = outlook_content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::INBOX_FOLDER_ID,
        &uploaded_state,
    )
    .await;
    let stream = strict_content_sync_transfer_from_response(&response).unwrap();

    assert!(
        stream.message_changes.is_empty(),
        "Outlook already reported every Inbox FAI change in MetaTagCnsetSeenFAI"
    );
    assert!(stream.deleted_idset.is_none());
    assert!(stream.read_idset.is_none());
    assert!(stream.unread_idset.is_none());
    assert_eq!(
        stream.idset_given.as_slice(),
        uploaded_state[0].1.as_slice()
    );
    assert_eq!(stream.cnset_seen.as_slice(), uploaded_state[1].1.as_slice());
    assert_eq!(
        stream.cnset_seen_fai.as_slice(),
        uploaded_state[2].1.as_slice()
    );
    assert_eq!(stream.cnset_read.as_slice(), uploaded_state[3].1.as_slice());
}

#[tokio::test]
async fn mapi_over_http_empty_contacts_fai_sync_exports_no_default_view() {
    // [MS-OXOCFG] sections 2.2.6 and 3.1.4.3 make view definitions
    // client-created FAI messages. [MS-OXCFXICS] section 3.2.5.3 exports
    // only FAI messages that exist in the canonical folder state.
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CONTACTS_FOLDER_ID);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN | OrderByDeliveryTime
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
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.FolderDesign.NamedView")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Contacts")));
}

#[tokio::test]
async fn mapi_over_http_open_associated_message_by_imported_source_key_id() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 42,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let associated_config_id = Uuid::parse_str("e0fdf7ca-15f8-bc62-ff51-d543d69a14a5").unwrap();
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: associated_config_id,
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
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            associated_config_id,
            associated_object_id,
        )]))),
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
async fn mapi_over_http_persisted_associated_config_write_preserves_class_on_save() {
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
    append_rop_save_changes_message(&mut rops, 2, 2);

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
    assert!(contains_bytes(&response_rops, &[0x0C, 0x02, 0, 0, 0, 0]));

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
    assert!(dictionary.contains("392d31"), "{dictionary}");
    assert!(!dictionary.contains("392d30"), "{dictionary}");
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_to_associated_config_message_succeeds() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 44,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let associated_config_id = Uuid::parse_str("e0fdf7ca-15f8-bc62-ff51-d543d69a14a7").unwrap();
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: associated_config_id,
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
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            associated_config_id,
            associated_object_id,
        )]))),
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
    rops.push(0); // Level: include subobjects.
    rops.extend_from_slice(&0x0000_2000u32.to_le_bytes()); // BestBody.
    rops.push(0x09); // Unicode | ForceUnicode, as sent by Outlook.
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
    let chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(chunks.len(), 1, "{response_rops:02x?}");
    let transfer = &chunks[0].1;
    assert!(transfer.starts_with(&PID_TAG_PARENT_SOURCE_KEY.to_le_bytes()));
    assert!(!contains_bytes(transfer, &0x4010_0003u32.to_le_bytes()));
    assert!(!contains_bytes(transfer, &0x400D_0003u32.to_le_bytes()));
    assert!(!contains_bytes(transfer, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(!contains_bytes(transfer, &0x67AA_000Bu32.to_le_bytes()));
    assert!(!contains_bytes(transfer, &0x674A_0014u32.to_le_bytes()));
    assert!(contains_bytes(
        transfer,
        &utf16z("Outlook Inbox view state")
    ));
    assert!(contains_bytes(transfer, &utf16z("Client view payload")));
    assert!(contains_bytes(transfer, b"view-extra"));
    let normalized_subject_value = utf16z("Outlook Inbox view state");
    let mut normalized_subject = PID_TAG_NORMALIZED_SUBJECT_W.to_le_bytes().to_vec();
    normalized_subject.extend_from_slice(&(normalized_subject_value.len() as u32).to_le_bytes());
    normalized_subject.extend_from_slice(&normalized_subject_value);
    assert!(
        contains_bytes(transfer, &normalized_subject),
        "missing Unicode normalized subject in {transfer:02x?}"
    );
    assert!(!contains_bytes(
        transfer,
        &PID_TAG_NORMALIZED_SUBJECT_A.to_le_bytes()
    ));
    let mut message_flags = PID_TAG_MESSAGE_FLAGS.to_le_bytes().to_vec();
    message_flags.extend_from_slice(&0x0000_0040u32.to_le_bytes());
    assert!(contains_bytes(transfer, &message_flags));
    assert!(transfer.ends_with(&[
        0x03, 0x00, 0x16, 0x40, // MetaTagFXDelProp.
        0x0D, 0x00, 0x12, 0x0E, // PidTagMessageRecipients.
        0x03, 0x00, 0x16, 0x40, // MetaTagFXDelProp.
        0x0D, 0x00, 0x13, 0x0E, // PidTagMessageAttachments.
    ]));
}

#[tokio::test]
async fn mapi_over_http_reads_empty_associated_config_body_stream() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 43,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let associated_config_id = Uuid::parse_str("e0fdf7ca-15f8-bc62-ff51-d543d69a14a6").unwrap();
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
            id: associated_config_id,
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
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            associated_config_id,
            associated_object_id,
        )]))),
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
async fn mapi_over_http_replays_outlook_calendar_sync_import_then_save() {
    // Outlook trace 202607171012, requests :257 and :261.
    // [MS-OXCFXICS] sections 2.2.3.2.4.2.1 and 3.3.4.3.3.2.2.1 require the
    // ImportMessageChange -> SetProperties -> SaveChangesMessage upload sequence.
    // [MS-OXCROPS] section 2.2.6.3 and [MS-OXCMSG] section 3.2.5.3 require a
    // successful save to commit the Message object and return its imported MID.
    let imported_message_id = crate::mapi::identity::mapi_store_id(0x0df8_974b_7f66);
    let imported_source_key = crate::mapi::identity::source_key_for_object_id(imported_message_id);
    let change_xid = [
        0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2,
        0xc1, 0x00, 0x00, 0x04, 0x57,
    ];
    let mut predecessor_change_list = vec![change_xid.len() as u8];
    predecessor_change_list.extend_from_slice(&change_xid);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let events = store.events.clone();
    let emails = store.emails.clone();
    let imported_emails = store.imported_emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let mapi_event_identity_versions = store.mapi_event_identity_versions.clone();
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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7e, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7e, 0x02, 0, 0, 0, 0]
    ));
    assert_ne!(collector_handles[2], u32::MAX);

    // Exact uncompressed ROP sequence from request :257, excluding its handle table.
    let import_rops = [
        0x75, 0x00, 0x00, 0x02, 0x01, 0x96, 0x67, 0x00, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x75,
        0x00, 0x00, 0x02, 0x01, 0xda, 0x67, 0x1e, 0x00, 0x00, 0x00, 0x76, 0x00, 0x00, 0x1e, 0x00,
        0x00, 0x00, 0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45,
        0x1c, 0x8f, 0x10, 0x52, 0x0d, 0xf8, 0x97, 0x4b, 0x7f, 0x63, 0x0d, 0xf8, 0x97, 0x4b, 0x7f,
        0x65, 0x00, 0x77, 0x00, 0x00, 0x75, 0x00, 0x00, 0x02, 0x01, 0xd2, 0x67, 0x00, 0x00, 0x00,
        0x00, 0x77, 0x00, 0x00, 0x72, 0x00, 0x00, 0x01, 0x00, 0x04, 0x00, 0x02, 0x01, 0xe0, 0x65,
        0x16, 0x00, 0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45,
        0x1c, 0x8f, 0x10, 0x0d, 0xf8, 0x97, 0x4b, 0x7f, 0x66, 0x40, 0x00, 0x08, 0x30, 0x00, 0x49,
        0xaa, 0x9a, 0xc3, 0x15, 0xdd, 0x01, 0x02, 0x01, 0xe2, 0x65, 0x14, 0x00, 0x67, 0x45, 0x48,
        0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2, 0xc1, 0x00, 0x00,
        0x04, 0x57, 0x02, 0x01, 0xe3, 0x65, 0x15, 0x00, 0x14, 0x67, 0x45, 0x48, 0x20, 0x69, 0x60,
        0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2, 0xc1, 0x00, 0x00, 0x04, 0x57,
    ];

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_rops, &[collector_handles[2], u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (import_response, import_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(
        contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]),
        "RopSynchronizationImportMessageChange 0x72 failed: {import_response:02x?}"
    );
    assert_ne!(import_handles[1], u32::MAX);

    let mut appointment_values = Vec::new();
    append_mapi_utf16_property(
        &mut appointment_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Appointment",
    );
    append_mapi_utf16_property(
        &mut appointment_values,
        PID_TAG_SUBJECT_W,
        "Delete 20:52 - été",
    );
    append_mapi_i64_property(
        &mut appointment_values,
        0x0060_0040,
        test_filetime("2026-07-16", "20:52"),
    );
    append_mapi_i64_property(
        &mut appointment_values,
        0x0061_0040,
        test_filetime("2026-07-16", "21:22"),
    );
    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 1, 4, &appointment_values);
    append_rop_save_changes_message_with_flags(&mut save_rops, 1, 1, 0x08);

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &import_handles)),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let save_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&save_response, &[0x0a, 0x01, 0, 0, 0, 0]));
    let mut expected_save = vec![0x0c, 0x01, 0, 0, 0, 0, 0x01];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(imported_message_id));
    assert!(
        contains_bytes(&save_response, &expected_save),
        "RopSaveChangesMessage 0x0c must commit the imported Calendar MID; expected {expected_save:02x?}, got {save_response:02x?}"
    );

    let events = events.lock().unwrap();
    assert_eq!(
        events.len(),
        1,
        "one canonical Calendar event must be saved"
    );
    assert_eq!(events[0].title, "Delete 20:52 - été");
    let event_id = events[0].id;
    drop(events);
    assert!(emails.lock().unwrap().is_empty());
    assert!(imported_emails.lock().unwrap().is_empty());
    let canonical_message_id = mapi_identities.lock().unwrap()[&event_id];
    assert_eq!(canonical_message_id, imported_message_id);
    assert_eq!(
        crate::mapi::identity::source_key_for_object_id(canonical_message_id),
        imported_source_key
    );
    let canonical_version = mapi_event_identity_versions.lock().unwrap()[&event_id].clone();
    assert_eq!(canonical_version.change_key, change_xid);
    assert_eq!(
        canonical_version.predecessor_change_list,
        predecessor_change_list
    );

    // [MS-OXCFXICS] sections 3.1.5.3 and 3.2.5.9.3.1: the upload
    // collector acknowledges the imported MID with the fresh server CN. A
    // Calendar content save is neither an FAI nor a read-state mutation.
    let mut transfer_state_rops = vec![
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4e, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ];
    transfer_state_rops.extend_from_slice(&4096u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &transfer_state_rops,
                &[1, u32::MAX, collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let transfer_state = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&transfer_state, &[0x82, 0x03, 0, 0, 0, 0]));
    let imported_message_counter = mapi_mailstore::change_number_for_store_id(imported_message_id);
    assert_ne!(
        imported_message_counter, canonical_version.change_number,
        "the imported MID and fresh server CN exercise distinct ICS sets"
    );
    assert_content_upload_final_state_includes(
        &transfer_state,
        &[canonical_version.change_number],
        &[],
        &[],
    );
}

#[tokio::test]
async fn mapi_over_http_replays_outlook_calendar_import_move_to_deleted_items() {
    // Outlook trace 202607171012, request :268. [MS-OXCFXICS] sections
    // 2.2.3.2.4.4.1, 3.2.5.9.4.4, and 3.3.4.3.3.2.1.1 define the five
    // length-prefixed fields used to import the Calendar move into Deleted Items.
    let source_folder_gid = [
        0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f,
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
    ];
    let source_message_gid = [
        0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f,
        0x10, 0x0d, 0xf8, 0x97, 0x4b, 0x7f, 0x66,
    ];
    let change_xid = [
        0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2,
        0xc1, 0x00, 0x00, 0x04, 0x57,
    ];
    let mut predecessor_change_list = vec![change_xid.len() as u8];
    predecessor_change_list.extend_from_slice(&change_xid);
    let destination_message_gid = [
        0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f,
        0x10, 0x0d, 0xf8, 0x97, 0x4b, 0x77, 0x6d,
    ];
    let source_message_id =
        crate::mapi::identity::object_id_from_source_key(&source_message_gid).unwrap();
    let destination_message_id =
        crate::mapi::identity::object_id_from_source_key(&destination_message_gid).unwrap();
    assert_eq!(
        crate::mapi::identity::object_id_from_source_key(&source_folder_gid),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );

    let account = FakeStore::account();
    let event_id = Uuid::parse_str("20260716-2052-4078-8000-000000000268").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-4777-8777-777777777268").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-07-16".to_string(),
            time: "20:52".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Delete 20:52 - été".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        event_versions: Arc::new(Mutex::new(HashMap::from([(event_id, 1)]))),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(event_id, source_message_id)]))),
        mapi_identity_source_keys: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            source_message_gid.to_vec(),
        )]))),
        mapi_event_identity_versions: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            MapiEventVersion {
                event_id,
                canonical_modseq: 1,
                change_number: mapi_mailstore::change_number_for_store_id(source_message_id),
                change_key: change_xid.to_vec(),
                predecessor_change_list: predecessor_change_list.clone(),
                updated_at: "2026-07-16T20:52:00Z".to_string(),
            },
        )]))),
        ..Default::default()
    };
    let events = store.events.clone();
    let deleted_events = store.deleted_events.clone();
    let deleted_calendar_events = store.deleted_calendar_events.clone();
    let emails = store.emails.clone();
    let imported_emails = store.imported_emails.clone();
    let moved_emails = store.moved_emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let mapi_identity_source_keys = store.mapi_identity_source_keys.clone();
    let mapi_event_identity_versions = store.mapi_event_identity_versions.clone();
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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::TRASH_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7e, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7e, 0x02, 0, 0, 0, 0]
    ));
    assert_ne!(collector_handles[2], u32::MAX);

    // Exact five length-prefixed fields from Outlook request :268. The input
    // handle index is also kept at 1, as observed in the captured ROP.
    let mut import_move_rops = vec![0x78, 0x00, 0x01];
    for field in [
        source_folder_gid.as_slice(),
        source_message_gid.as_slice(),
        predecessor_change_list.as_slice(),
        destination_message_gid.as_slice(),
        change_xid.as_slice(),
    ] {
        import_move_rops.extend_from_slice(&(field.len() as u32).to_le_bytes());
        import_move_rops.extend_from_slice(field);
    }
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_move_rops, &[1, collector_handles[2]])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x78, 0x01, 0, 0, 0, 0]),
        "RopSynchronizationImportMessageMove 0x78 failed: {response_rops:02x?}"
    );

    assert!(events.lock().unwrap().is_empty());
    assert_eq!(deleted_events.lock().unwrap().as_slice(), &[event_id]);
    let deleted = deleted_calendar_events.lock().unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].id, event_id);
    assert_eq!(deleted[0].title, "Delete 20:52 - été");
    drop(deleted);
    assert!(emails.lock().unwrap().is_empty());
    assert!(imported_emails.lock().unwrap().is_empty());
    assert!(moved_emails.lock().unwrap().is_empty());
    assert_eq!(
        mapi_identities.lock().unwrap()[&event_id],
        destination_message_id
    );
    assert_eq!(
        mapi_identity_source_keys.lock().unwrap()[&event_id],
        destination_message_gid
    );
    let destination_version = mapi_event_identity_versions.lock().unwrap()[&event_id].clone();
    assert_eq!(destination_version.change_key, change_xid);
    assert_eq!(
        destination_version.predecessor_change_list,
        predecessor_change_list
    );
}

#[tokio::test]
async fn mapi_over_http_replays_outlook_calendar_move_then_modifies_deleted_event() {
    // Outlook first imports the inter-folder move into Deleted Items, then can
    // upload another version against the destination MID. [MS-OXCFXICS]
    // section 3.3.4.3.3.2.1.1 defines the successful 0x78 transition, while
    // section 3.1.5.3 requires the destination GID to remain stable and the
    // imported CK/PCL to coexist with a newly allocated server-internal CN.
    let source_folder_gid = [
        0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f,
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
    ];
    let source_message_gid = [
        0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f,
        0x10, 0x0d, 0xf8, 0x97, 0x4b, 0x7f, 0x66,
    ];
    let destination_message_gid = [
        0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f,
        0x10, 0x0d, 0xf8, 0x97, 0x4b, 0x77, 0x6d,
    ];
    let move_change_xid = [
        0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2,
        0xc1, 0x00, 0x00, 0x04, 0x57,
    ];
    let mut move_predecessor_change_list = vec![move_change_xid.len() as u8];
    move_predecessor_change_list.extend_from_slice(&move_change_xid);
    let update_change_xid = [
        0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2,
        0xc1, 0x00, 0x00, 0x04, 0x58,
    ];
    let mut update_predecessor_change_list = vec![update_change_xid.len() as u8];
    update_predecessor_change_list.extend_from_slice(&update_change_xid);
    let source_message_id =
        crate::mapi::identity::object_id_from_source_key(&source_message_gid).unwrap();
    let destination_message_id =
        crate::mapi::identity::object_id_from_source_key(&destination_message_gid).unwrap();
    assert_eq!(
        crate::mapi::identity::object_id_from_source_key(&source_folder_gid),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );

    let account = FakeStore::account();
    let event_id = Uuid::parse_str("20260716-2052-4078-8000-000000000272").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-4777-8777-777777777272").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-07-16".to_string(),
            time: "20:52".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Delete 20:52 - été".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        event_versions: Arc::new(Mutex::new(HashMap::from([(event_id, 1)]))),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(event_id, source_message_id)]))),
        mapi_identity_source_keys: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            source_message_gid.to_vec(),
        )]))),
        mapi_event_identity_versions: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            MapiEventVersion {
                event_id,
                canonical_modseq: 1,
                change_number: mapi_mailstore::change_number_for_store_id(source_message_id),
                change_key: move_change_xid.to_vec(),
                predecessor_change_list: move_predecessor_change_list.clone(),
                updated_at: "2026-07-16T20:52:00Z".to_string(),
            },
        )]))),
        ..Default::default()
    };
    let events = store.events.clone();
    let deleted_events = store.deleted_events.clone();
    let deleted_calendar_events = store.deleted_calendar_events.clone();
    let emails = store.emails.clone();
    let imported_emails = store.imported_emails.clone();
    let moved_emails = store.moved_emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let mapi_identity_source_keys = store.mapi_identity_source_keys.clone();
    let mapi_event_identity_versions = store.mapi_event_identity_versions.clone();
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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::TRASH_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7e, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7e, 0x02, 0, 0, 0, 0]
    ));
    assert_ne!(collector_handles[2], u32::MAX);

    let mut import_move_rops = vec![0x78, 0x00, 0x01];
    for field in [
        source_folder_gid.as_slice(),
        source_message_gid.as_slice(),
        move_predecessor_change_list.as_slice(),
        destination_message_gid.as_slice(),
        move_change_xid.as_slice(),
    ] {
        import_move_rops.extend_from_slice(&(field.len() as u32).to_le_bytes());
        import_move_rops.extend_from_slice(field);
    }
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_move_rops, &[1, collector_handles[2]])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let move_response = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&move_response, &[0x78, 0x01, 0, 0, 0, 0]),
        "RopSynchronizationImportMessageMove 0x78 failed: {move_response:02x?}"
    );
    let moved_change_number = mapi_event_identity_versions.lock().unwrap()[&event_id].change_number;

    let mut import_values = Vec::new();
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_SOURCE_KEY,
        &destination_message_gid,
    );
    append_mapi_i64_property(
        &mut import_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-17", "08:35"),
    );
    append_mapi_binary_property(&mut import_values, PID_TAG_CHANGE_KEY, &update_change_xid);
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &update_predecessor_change_list,
    );
    let mut update_values = Vec::new();
    append_mapi_utf16_property(
        &mut update_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Appointment",
    );
    append_mapi_utf16_property(
        &mut update_values,
        PID_TAG_SUBJECT_W,
        "Deleted event modified after move",
    );
    append_mapi_i64_property(
        &mut update_values,
        0x0060_0040,
        test_filetime("2026-07-17", "06:35"),
    );
    append_mapi_i64_property(
        &mut update_values,
        0x0061_0040,
        test_filetime("2026-07-17", "07:05"),
    );
    let mut update_rops = vec![
        0x72, 0x00, 0x01, 0x02, // RopSynchronizationImportMessageChange.
        0x00, // ImportFlag.
    ];
    update_rops.extend_from_slice(&4u16.to_le_bytes());
    update_rops.extend_from_slice(&import_values);
    append_rop_set_properties(&mut update_rops, 2, 4, &update_values);
    append_rop_save_changes_message_with_flags(&mut update_rops, 2, 2, 0x08);

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &update_rops,
                &[1, collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let update_response = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&update_response, &[0x72, 0x02, 0, 0, 0, 0]),
        "RopSynchronizationImportMessageChange 0x72 failed: {update_response:02x?}"
    );
    assert!(contains_bytes(&update_response, &[0x0a, 0x02, 0, 0, 0, 0]));
    let mut expected_save = vec![0x0c, 0x02, 0, 0, 0, 0, 0x02];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(destination_message_id));
    assert!(
        contains_bytes(&update_response, &expected_save),
        "RopSaveChangesMessage 0x0c must keep the destination Calendar MID; expected {expected_save:02x?}, got {update_response:02x?}"
    );

    assert!(events.lock().unwrap().is_empty());
    assert_eq!(deleted_events.lock().unwrap().as_slice(), &[event_id]);
    assert!(
        emails.lock().unwrap().is_empty(),
        "updating a deleted Calendar event must not create a parallel generic email"
    );
    assert!(imported_emails.lock().unwrap().is_empty());
    assert!(moved_emails.lock().unwrap().is_empty());
    let deleted = deleted_calendar_events.lock().unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].id, event_id);
    assert_eq!(deleted[0].title, "Deleted event modified after move");
    assert_eq!(deleted[0].date, "2026-07-17");
    assert_eq!(deleted[0].time, "08:35");
    assert_eq!(deleted[0].duration_minutes, 30);
    drop(deleted);
    assert_eq!(
        mapi_identities.lock().unwrap()[&event_id],
        destination_message_id
    );
    assert_eq!(
        mapi_identity_source_keys.lock().unwrap()[&event_id],
        destination_message_gid
    );
    let updated_version = mapi_event_identity_versions.lock().unwrap()[&event_id].clone();
    assert!(
        updated_version.change_number > moved_change_number,
        "the server must allocate a fresh internal CN for the imported update"
    );
    assert_eq!(updated_version.change_key, update_change_xid);
    assert_eq!(
        updated_version.predecessor_change_list,
        update_predecessor_change_list
    );
}

fn calendar_sync_conflict_xid(replica_byte: u8, counter: u64) -> Vec<u8> {
    let mut xid = vec![replica_byte; 16];
    xid.extend_from_slice(&counter.to_be_bytes()[2..]);
    xid
}

fn calendar_sync_conflict_pcl(xids: &[&[u8]]) -> Vec<u8> {
    let mut pcl = Vec::new();
    for xid in xids {
        pcl.push(xid.len() as u8);
        pcl.extend_from_slice(xid);
    }
    pcl
}

fn calendar_sync_conflict_store(
    event_id: Uuid,
    message_id: u64,
    change_key: Vec<u8>,
    predecessor_change_list: Vec<u8>,
    updated_at: &str,
) -> FakeStore {
    let account = FakeStore::account();
    FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-07-17".to_string(),
            time: "10:00".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Server version".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        event_versions: Arc::new(Mutex::new(HashMap::from([(event_id, 1)]))),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(event_id, message_id)]))),
        mapi_identity_source_keys: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            crate::mapi::identity::source_key_for_object_id(message_id),
        )]))),
        mapi_event_identity_versions: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            MapiEventVersion {
                event_id,
                canonical_modseq: 1,
                change_number: mapi_mailstore::change_number_for_store_id(message_id),
                change_key,
                predecessor_change_list,
                updated_at: updated_at.to_string(),
            },
        )]))),
        ..Default::default()
    }
}

async fn execute_existing_calendar_sync_import(
    store: FakeStore,
    source_key: &[u8],
    last_modification_time: i64,
    change_key: &[u8],
    predecessor_change_list: &[u8],
    import_flag: u8,
    subject: Option<&str>,
    request_transfer_state: bool,
) -> Vec<u8> {
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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7e, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (_, collector_handles) = response_rops_and_handles_from_execute_body(&body);

    let mut import_values = Vec::new();
    append_mapi_binary_property(&mut import_values, PID_TAG_SOURCE_KEY, source_key);
    append_mapi_i64_property(
        &mut import_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        last_modification_time,
    );
    append_mapi_binary_property(&mut import_values, PID_TAG_CHANGE_KEY, change_key);
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        predecessor_change_list,
    );
    let mut rops = vec![
        0x72,
        0x00,
        0x01,
        0x02, // RopSynchronizationImportMessageChange.
        import_flag,
    ];
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&import_values);
    if let Some(subject) = subject {
        let mut update_values = Vec::new();
        append_mapi_utf16_property(&mut update_values, PID_TAG_SUBJECT_W, subject);
        append_rop_set_properties(&mut rops, 2, 1, &update_values);
        append_rop_save_changes_message_with_flags(&mut rops, 2, 2, 0x08);
    }
    if request_transfer_state {
        rops.extend_from_slice(&[
            0x82, 0x00, 0x01, 0x03, // RopSynchronizationGetTransferState.
            0x4e, 0x00, 0x03, // RopFastTransferSourceGetBuffer.
        ]);
        rops.extend_from_slice(&4096u16.to_le_bytes());
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut request_handles = vec![1, collector_handles[2], u32::MAX];
    if request_transfer_state {
        request_handles.push(u32::MAX);
    }
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &request_handles)),
        )
        .await
        .unwrap();
    response_rops_from_execute_response(response).await
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_import_ignores_an_older_client_version_at_save() {
    // [MS-OXCFXICS] section 3.1.5.6.1: when the server PCL includes the
    // client PCL, the imported version is older and MUST be ignored.
    let event_id = Uuid::parse_str("20260717-1012-4078-8000-000000000301").unwrap();
    let message_id = crate::mapi::identity::mapi_store_id(0x0df8_974b_8031);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let client_change_key = calendar_sync_conflict_xid(0x31, 10);
    let server_change_key = calendar_sync_conflict_xid(0x31, 11);
    let client_pcl = calendar_sync_conflict_pcl(&[&client_change_key]);
    let server_pcl = calendar_sync_conflict_pcl(&[&server_change_key]);
    let store = calendar_sync_conflict_store(
        event_id,
        message_id,
        server_change_key.clone(),
        server_pcl.clone(),
        "2026-07-17T10:00:00Z",
    );
    let events = store.events.clone();
    let versions = store.mapi_event_identity_versions.clone();

    let response = execute_existing_calendar_sync_import(
        store,
        &source_key,
        test_filetime("2026-07-17", "09:00"),
        &client_change_key,
        &client_pcl,
        0,
        Some("Older client version"),
        false,
    )
    .await;

    assert!(contains_bytes(&response, &[0x72, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response, &[0x0c, 0x02, 0, 0, 0, 0]));
    assert_eq!(events.lock().unwrap()[0].title, "Server version");
    let version = versions.lock().unwrap()[&event_id].clone();
    assert_eq!(version.change_key, server_change_key);
    assert_eq!(version.predecessor_change_list, server_pcl);
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_import_fail_on_conflict_returns_sync_conflict() {
    // [MS-OXCFXICS] sections 2.2.3.2.4.2.2 and 3.2.5.9.4.2 require
    // SyncConflict (0x80040802) and no imported data when FailOnConflict is set.
    let event_id = Uuid::parse_str("20260717-1012-4078-8000-000000000302").unwrap();
    let message_id = crate::mapi::identity::mapi_store_id(0x0df8_974b_8032);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let server_change_key = calendar_sync_conflict_xid(0x11, 5);
    let client_change_key = calendar_sync_conflict_xid(0x22, 7);
    let server_pcl = calendar_sync_conflict_pcl(&[&server_change_key]);
    let client_pcl = calendar_sync_conflict_pcl(&[&client_change_key]);
    let store = calendar_sync_conflict_store(
        event_id,
        message_id,
        server_change_key,
        server_pcl,
        "2026-07-17T10:00:00Z",
    );
    let events = store.events.clone();

    let response = execute_existing_calendar_sync_import(
        store,
        &source_key,
        test_filetime("2026-07-17", "11:00"),
        &client_change_key,
        &client_pcl,
        0x40,
        None,
        false,
    )
    .await;

    assert!(contains_bytes(
        &response,
        &[0x72, 0x02, 0x02, 0x08, 0x04, 0x80]
    ));
    assert_eq!(events.lock().unwrap()[0].title, "Server version");
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists() {
    // [MS-OXCFXICS] sections 3.1.5.6.1, 3.1.5.6.2 and 3.2.5.9.4.2:
    // an accepted conflict resolves to a PCL that succeeds both replicas.
    let event_id = Uuid::parse_str("20260717-1012-4078-8000-000000000303").unwrap();
    let message_id = crate::mapi::identity::mapi_store_id(0x0df8_974b_8033);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let server_change_key = calendar_sync_conflict_xid(0x11, 5);
    let client_change_key = calendar_sync_conflict_xid(0x22, 7);
    let server_pcl = calendar_sync_conflict_pcl(&[&server_change_key]);
    let client_pcl = calendar_sync_conflict_pcl(&[&client_change_key]);
    let merged_pcl = calendar_sync_conflict_pcl(&[&server_change_key, &client_change_key]);
    let store = calendar_sync_conflict_store(
        event_id,
        message_id,
        server_change_key,
        server_pcl,
        "2026-07-17T10:00:00Z",
    );
    let events = store.events.clone();
    let versions = store.mapi_event_identity_versions.clone();
    let previous_change_number = versions.lock().unwrap()[&event_id].change_number;

    let response = execute_existing_calendar_sync_import(
        store,
        &source_key,
        test_filetime("2026-07-17", "11:00"),
        &client_change_key,
        &client_pcl,
        0,
        Some("Resolved client version"),
        true,
    )
    .await;

    assert!(contains_bytes(&response, &[0x72, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response, &[0x0c, 0x02, 0, 0, 0, 0]));
    assert_eq!(events.lock().unwrap()[0].title, "Resolved client version");
    let version = versions.lock().unwrap()[&event_id].clone();
    assert!(version.change_number > previous_change_number);
    assert_eq!(version.change_key, client_change_key);
    assert_eq!(version.predecessor_change_list, merged_pcl);
    // [MS-OXCFXICS] sections 2.2.1.1.4 and 3.2.5.6: an Event content
    // update advances the normal CNSET, not the FAI or read-state CNSET.
    assert_content_upload_final_state_includes(&response, &[version.change_number], &[], &[]);
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_import_conflict_keeps_the_newer_server_content() {
    // [MS-OXCFXICS] sections 3.1.5.6.2 and 3.1.5.6.2.2: LWW can keep
    // the server contents, but the resolved PCL still succeeds both versions.
    let event_id = Uuid::parse_str("20260717-1012-4078-8000-000000000304").unwrap();
    let message_id = crate::mapi::identity::mapi_store_id(0x0df8_974b_8034);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let server_change_key = calendar_sync_conflict_xid(0x11, 5);
    let client_change_key = calendar_sync_conflict_xid(0x22, 7);
    let server_pcl = calendar_sync_conflict_pcl(&[&server_change_key]);
    let client_pcl = calendar_sync_conflict_pcl(&[&client_change_key]);
    let merged_pcl = calendar_sync_conflict_pcl(&[&server_change_key, &client_change_key]);
    let store = calendar_sync_conflict_store(
        event_id,
        message_id,
        server_change_key.clone(),
        server_pcl,
        "2026-07-17T10:00:00.000000Z",
    );
    let events = store.events.clone();
    let versions = store.mapi_event_identity_versions.clone();
    let previous_change_number = versions.lock().unwrap()[&event_id].change_number;

    let response = execute_existing_calendar_sync_import(
        store,
        &source_key,
        test_filetime("2026-07-17", "09:00"),
        &client_change_key,
        &client_pcl,
        0,
        Some("Losing client version"),
        false,
    )
    .await;

    assert!(contains_bytes(&response, &[0x72, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response, &[0x0c, 0x02, 0, 0, 0, 0]));
    assert_eq!(events.lock().unwrap()[0].title, "Server version");
    let version = versions.lock().unwrap()[&event_id].clone();
    assert!(version.change_number > previous_change_number);
    assert_eq!(version.change_key, server_change_key);
    assert_eq!(version.predecessor_change_list, merged_pcl);
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
    append_rop_sync_import_deletes(
        &mut rops,
        0x02,
        0x02,
        &[test_mapi_message_id(delete_message_id)],
    );
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
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
    assert_content_upload_final_state_includes(&response_rops, &[41], &[], &[41]);
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
    ]);
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x02, &[out_of_range_object_id]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
    assert!(deleted_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_deletes_removes_fai_by_outlook_source_key() {
    let account = FakeStore::account();
    let outlook_source_object_id = crate::mapi::identity::mapi_store_id(0xa00a_5207_3216);
    let config_id = Uuid::parse_str("16161616-1616-4616-8616-161616161616").unwrap();
    let associated_configs = Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
        id: config_id,
        account_id: account.account_id,
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        message_class: "IPM.Configuration.AccountPrefs".to_string(),
        subject: "IPM.Configuration.AccountPrefs".to_string(),
        properties_json: serde_json::json!({
            "0x65e00102": {
                "type": "binary",
                "value": "741f6fd38e1a654f9d422dfb451c8f10a00a52073216"
            }
        }),
    }]));
    let store = FakeStore {
        session: Some(account),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        associated_configs: associated_configs.clone(),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            config_id,
            outlook_source_object_id,
        )]))),
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
    ]);
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x00, &[outlook_source_object_id]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_body = response_bytes(response).await;
    let (response_rops, response_handles) =
        response_rops_and_handles_from_execute_body(&response_body);
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
    assert_eq!(response_handles.len(), 3);
    assert!(associated_configs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads() {
    let account = FakeStore::account();
    let shortcut_id = Uuid::parse_str("adbbec46-a698-4684-a317-1ea7fd49067d").unwrap();
    let shortcuts = Arc::new(Mutex::new(vec![
        crate::store::MapiNavigationShortcutRecord {
            id: shortcut_id,
            account_id: account.account_id,
            subject: "My Contacts".to_string(),
            target_folder_id: None,
            shortcut_type: 4,
            flags: 0,
            save_stamp: 1_537_819_608,
            section: 4,
            ordinal: vec![127],
            group_header_id: Some(Uuid::parse_str("b7f00600-0000-0000-c000-000000000046").unwrap()),
            group_name: "My Contacts".to_string(),
            client_properties: crate::store::MapiNavigationShortcutClientProperties::default(),
        },
    ]));
    let store = FakeStore {
        session: Some(account.clone()),
        navigation_shortcuts: shortcuts.clone(),
        ..Default::default()
    };
    let reservation_start = store
        .reserve_mapi_local_replica_ids(account.account_id, 0x0100)
        .await
        .unwrap();
    let source_counter = reservation_start + 0x074;
    let message_id = crate::mapi::identity::mapi_store_id(source_counter);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);
    let identity = store
        .fetch_or_allocate_mapi_identities(
            account.account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::NavigationShortcut,
                canonical_id: shortcut_id,
                reserved_global_counter: Some(source_counter),
                source_key: Some(source_key.clone()),
            }],
        )
        .await
        .unwrap()
        .remove(0);

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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_body = response_bytes(response).await;
    let (_, collector_handles) = response_rops_and_handles_from_execute_body(&response_body);

    // Outlook supplies PidTagSourceKey to RopSynchronizationImportDeletes.
    // [MS-OXCFXICS] sections 2.2.3.2.4.5 and 3.3.4.3.3.2.3 require the
    // Common Views FAI object to be resolved and deleted by that identity.
    let mut delete_rops = Vec::new();
    append_rop_sync_import_deletes(&mut delete_rops, 0x00, 0x02, &[message_id]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[collector_handles[2]])),
        )
        .await
        .unwrap();
    assert_eq!(
        response_rops_from_execute_response(response).await,
        [0x74, 0x00, 0, 0, 0, 0],
        "[MS-OXCROPS] section 2.2.13.5.2 has no PartialCompletion field"
    );
    assert!(shortcuts.lock().unwrap().is_empty());

    let mut checkpoint_rops = vec![
        0x82, 0x00, 0x00, 0x01, // RopSynchronizationGetTransferState.
        0x4E, 0x00, 0x01, // RopFastTransferSourceGetBuffer.
    ];
    checkpoint_rops.extend_from_slice(&4096u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &checkpoint_rops,
                &[collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    let checkpoint_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &checkpoint_response,
        &[0x82, 0x01, 0, 0, 0, 0]
    ));
    assert_eq!(mapi_fast_transfer_chunks(&checkpoint_response).len(), 1);

    let reloaded = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    assert!(reloaded
        .navigation_shortcut_message_for_id(message_id)
        .is_none());

    let mut import_values = Vec::new();
    append_mapi_binary_property(&mut import_values, PID_TAG_SOURCE_KEY, &source_key);
    append_mapi_i64_property(
        &mut import_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        identity.last_modification_time as i64,
    );
    append_mapi_binary_property(&mut import_values, PID_TAG_CHANGE_KEY, &identity.change_key);
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &identity.predecessor_change_list,
    );
    let mut import_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x10, // ImportMessageChange, associated FAI.
    ];
    import_rops.extend_from_slice(&4u16.to_le_bytes());
    import_rops.extend_from_slice(&import_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&import_rops, &[collector_handles[2], u32::MAX])),
        )
        .await
        .unwrap();
    let response_body = response_bytes(response).await;
    let (import_response, import_handles) =
        response_rops_and_handles_from_execute_body(&response_body);
    assert!(contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]));

    let mut wlink_values = Vec::new();
    append_mapi_utf16_property(
        &mut wlink_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(
        &mut wlink_values,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "My Contacts",
    );
    append_mapi_i32_property(&mut wlink_values, PID_TAG_WLINK_TYPE, 4);
    append_mapi_i32_property(&mut wlink_values, 0x6847_0003, 1_537_819_608);
    append_mapi_i32_property(&mut wlink_values, 0x684A_0003, 0);
    append_mapi_binary_property(&mut wlink_values, PID_TAG_WLINK_ORDINAL, &[0x7F]);
    append_mapi_i32_property(&mut wlink_values, 0x6852_0003, 4); // PidTagWlinkSection.
    append_mapi_binary_property(
        &mut wlink_values,
        0x6842_0102, // PidTagWlinkGroupHeaderId.
        Uuid::parse_str("b7f00600-0000-0000-c000-000000000046")
            .unwrap()
            .as_bytes(),
    );
    append_mapi_binary_property(
        &mut wlink_values,
        0x684F_0102, // PidTagWlinkFolderType, CLSID_ContactFolder.
        &[
            0x01, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ],
    );
    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 1, 9, &wlink_values);
    append_rop_save_changes_message_with_flags(&mut save_rops, 0, 1, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &import_handles)),
        )
        .await
        .unwrap();
    let save_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &save_response,
        &[0x0C, 0x00, 0x0A, 0x01, 0x04, 0x80]
    ), "[MS-OXCFXICS] section 3.3.4.3.3.2.2.1 and [MS-OXCDATA] section 2.4 permit ecObjectDeleted on RopSaveChangesMessage and direct the client to ignore this warning: {save_response:02x?}");
    assert!(shortcuts.lock().unwrap().is_empty());
    assert_eq!(
        store
            .deleted_navigation_shortcut_ids
            .lock()
            .unwrap()
            .as_slice(),
        &[shortcut_id]
    );
}

#[tokio::test]
async fn mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink(
) -> anyhow::Result<()> {
    // [MS-OXCFXICS] section 3.3.5.2.2 requires the server to allocate the
    // identity of an online-created Message. Unlike an imported local MID,
    // that identity is not backed by a RopGetLocalReplicaIds reservation.
    // Section 3.2.5.9.4.5 requires a retry after deletion to be ignored.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let committed = storage
        .commit_mapi_navigation_shortcut_create(
            crate::store::CommitMapiNavigationShortcutCreateInput {
                shortcut: crate::store::UpsertMapiNavigationShortcutInput {
                    id: Some(Uuid::new_v4()),
                    account_id: fixture.account_id,
                    subject: "Online WLink delete retry".to_string(),
                    target_folder_id: Some(crate::mapi::identity::CONTACTS_FOLDER_ID),
                    shortcut_type: 0,
                    flags: 0x0010_0000,
                    save_stamp: 1_537_819_608,
                    section: 4,
                    ordinal: vec![127],
                    group_header_id: Some(Uuid::parse_str("b7f00600-0000-0000-c000-000000000046")?),
                    group_name: "My Contacts".to_string(),
                    client_properties:
                        crate::store::MapiNavigationShortcutClientProperties::default(),
                },
            },
        )
        .await?;
    let source_counter =
        crate::mapi::identity::global_counter_from_store_id(committed.identity.object_id)
            .ok_or_else(|| anyhow::anyhow!("online WLink identity is not a store ID"))?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM mapi_local_replica_id_ranges
            WHERE account_id = $1
              AND replica_guid = $2
              AND first_global_counter <= $3
              AND end_global_counter_exclusive > $3
            "#,
        )
        .bind(fixture.account_id)
        .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
        .bind(source_counter as i64)
        .fetch_one(storage.pool())
        .await?,
        0,
        "an online-created WLink must not consume a local-ID reservation"
    );

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon_response.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // OpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&collector_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let response_body = response_bytes(response).await;
    let (_, collector_handles) = response_rops_and_handles_from_execute_body(&response_body);

    let mut delete_rops = Vec::new();
    append_rop_sync_import_deletes(
        &mut delete_rops,
        0x00,
        0x02,
        &[committed.identity.object_id],
    );
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[collector_handles[2]])),
        )
        .await?;
    assert_eq!(
        response_rops_from_execute_response(response).await,
        [0x74, 0x00, 0, 0, 0, 0]
    );

    let state_after_delete = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM mapi_navigation_shortcuts
             WHERE account_id = $1 AND id = $2),
            (SELECT COUNT(*) FROM mapi_object_identities
             WHERE account_id = $1
               AND object_kind = 'navigation_shortcut'
               AND canonical_id = $2
               AND source_key = $3
               AND deleted_at IS NOT NULL),
            (SELECT COUNT(*) FROM mail_change_log
             WHERE account_id = $1
               AND object_kind = 'navigation_shortcut'
               AND object_id = $2),
            (SELECT COUNT(*) FROM mapi_local_replica_deleted_ranges
             WHERE account_id = $1
               AND folder_id = $4
               AND min_global_counter <= $5
               AND max_global_counter >= $5)
        "#,
    )
    .bind(fixture.account_id)
    .bind(committed.shortcut.id)
    .bind(&committed.identity.source_key)
    .bind(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64)
    .bind(source_counter as i64)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(state_after_delete, (0, 1, 2, 0));

    // Simulate Outlook retrying after losing the first successful response.
    // The refreshed snapshot no longer contains the WLink content, so this
    // exercises the unknown-content tombstone path with the durable identity.
    renew_mapi_request_id(&mut execute_headers);
    let retry_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[collector_handles[2]])),
        )
        .await?;
    assert_eq!(
        response_rops_from_execute_response(retry_response).await,
        [0x74, 0x00, 0, 0, 0, 0],
        "an already-deleted online WLink must be an idempotent success"
    );
    let state_after_retry = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM mapi_navigation_shortcuts
             WHERE account_id = $1 AND id = $2),
            (SELECT COUNT(*) FROM mapi_object_identities
             WHERE account_id = $1
               AND object_kind = 'navigation_shortcut'
               AND canonical_id = $2
               AND source_key = $3
               AND deleted_at IS NOT NULL),
            (SELECT COUNT(*) FROM mail_change_log
             WHERE account_id = $1
               AND object_kind = 'navigation_shortcut'
               AND object_id = $2),
            (SELECT COUNT(*) FROM mapi_local_replica_deleted_ranges
             WHERE account_id = $1
               AND folder_id = $4
               AND min_global_counter <= $5
               AND max_global_counter >= $5)
        "#,
    )
    .bind(fixture.account_id)
    .bind(committed.shortcut.id)
    .bind(&committed.identity.source_key)
    .bind(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64)
    .bind(source_counter as i64)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(state_after_retry, state_after_delete);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let reservation_start = storage
        .reserve_mapi_local_replica_ids(fixture.account_id, 0x0100)
        .await?;
    let source_counter = reservation_start + 0x33;
    let message_id = crate::mapi::identity::mapi_store_id(source_counter);
    let source_key = crate::mapi::identity::source_key_for_object_id(message_id);

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon_response.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // OpenCollector, contents.
    ]);
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x02, &[message_id]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));

    // [MS-OXCFXICS] section 3.2.5.9.4.5 recommends retaining a deletion
    // for an object that never existed so a later upload cannot resurrect it.
    let tombstone = sqlx::query_as::<_, (Uuid, i64, i64, Vec<u8>, bool)>(
        r#"
        SELECT canonical_id, mapi_object_id, mapi_change_number,
               source_key, deleted_at IS NOT NULL
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'navigation_shortcut'
          AND source_key = $2
        "#,
    )
    .bind(fixture.account_id)
    .bind(&source_key)
    .fetch_optional(storage.pool())
    .await?;
    let (canonical_id, persisted_object_id, persisted_change_number, persisted_source_key, deleted) =
        tombstone
            .ok_or_else(|| anyhow::anyhow!("unknown reserved WLink deletion was not tombstoned"))?;
    assert_eq!(persisted_object_id as u64, message_id);
    assert_ne!(persisted_change_number as u64, source_counter);
    assert_eq!(persisted_source_key, source_key);
    assert!(deleted);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM mapi_local_replica_deleted_ranges
            WHERE account_id = $1
              AND folder_id = $2
              AND replica_guid = $3
              AND min_global_counter = $4
              AND max_global_counter = $4
            "#,
        )
        .bind(fixture.account_id)
        .bind(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64)
        .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
        .bind(source_counter as i64)
        .fetch_one(storage.pool())
        .await?,
        1,
        "the unknown reserved SourceKey must enter the Common Views deleted-item list"
    );
    let protocol_parallel_writes = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM mapi_navigation_shortcuts
             WHERE account_id = $1 AND id = $2)
          + (SELECT COUNT(*) FROM mail_change_log
             WHERE account_id = $1 AND object_kind = 'navigation_shortcut'
               AND object_id = $2)
        "#,
    )
    .bind(fixture.account_id)
    .bind(canonical_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(protocol_parallel_writes, 0);

    let change_key = vec![0xA7; 20];
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);
    let replay = storage
        .commit_mapi_navigation_shortcut_import(
            crate::store::CommitMapiNavigationShortcutImportInput {
                shortcut: crate::store::UpsertMapiNavigationShortcutInput {
                    id: None,
                    account_id: fixture.account_id,
                    subject: "Must stay deleted".to_string(),
                    target_folder_id: None,
                    shortcut_type: 4,
                    flags: 0,
                    save_stamp: 1_537_819_608,
                    section: 4,
                    ordinal: vec![127],
                    group_header_id: Some(Uuid::parse_str("b7f00600-0000-0000-c000-000000000046")?),
                    group_name: "My Contacts".to_string(),
                    client_properties:
                        crate::store::MapiNavigationShortcutClientProperties::default(),
                },
                identity: crate::store::MapiFaiImportedIdentity {
                    source_key,
                    change_key,
                    predecessor_change_list,
                    last_modification_time: test_filetime("2026-07-19", "14:00") as u64,
                },
                fail_on_conflict: false,
            },
        )
        .await;
    assert!(replay
        .as_ref()
        .err()
        .is_some_and(|error| { error.is::<crate::store::MapiFaiImportObjectDeleted>() }));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT
                (SELECT COUNT(*) FROM mapi_navigation_shortcuts
                 WHERE account_id = $1 AND id = $2)
              + (SELECT COUNT(*) FROM mail_change_log
                 WHERE account_id = $1 AND object_kind = 'navigation_shortcut'
                   AND object_id = $2)
            "#,
        )
        .bind(fixture.account_id)
        .bind(canonical_id)
        .fetch_one(storage.pool())
        .await?,
        0
    );

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let committed = storage
        .commit_mapi_navigation_shortcut_create(
            crate::store::CommitMapiNavigationShortcutCreateInput {
                shortcut: crate::store::UpsertMapiNavigationShortcutInput {
                    id: Some(Uuid::parse_str("83a302f4-5b88-4687-96fe-4bef3d3cb5d2")?),
                    account_id: fixture.account_id,
                    subject: "Atomic ImportDeletes WLink".to_string(),
                    target_folder_id: Some(crate::mapi::identity::CONTACTS_FOLDER_ID),
                    shortcut_type: 0,
                    flags: 0x0010_0000,
                    save_stamp: 1_537_819_608,
                    section: 4,
                    ordinal: vec![127],
                    group_header_id: Some(Uuid::parse_str("b7f00600-0000-0000-c000-000000000046")?),
                    group_name: "My Contacts".to_string(),
                    client_properties:
                        crate::store::MapiNavigationShortcutClientProperties::default(),
                },
            },
        )
        .await?;
    let unreserved_counter = crate::mapi::identity::FIRST_RESERVED_HIGH_GLOBAL_COUNTER - 0x100;
    let unreserved_object_id = crate::mapi::identity::mapi_store_id(unreserved_counter);
    let unreserved_source_key =
        crate::mapi::identity::source_key_for_object_id(unreserved_object_id);
    let unreserved_precondition = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM mapi_local_replica_id_ranges
             WHERE account_id = $1
               AND replica_guid = $2
               AND first_global_counter <= $3
               AND end_global_counter_exclusive > $3),
            (SELECT COUNT(*) FROM mapi_object_identities
             WHERE account_id = $1
               AND (mapi_object_id = $4 OR source_key = $5))
        "#,
    )
    .bind(fixture.account_id)
    .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
    .bind(unreserved_counter as i64)
    .bind(unreserved_object_id as i64)
    .bind(&unreserved_source_key)
    .fetch_one(storage.pool())
    .await?;

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    let logon_succeeded = logon_response.status() == StatusCode::OK;
    renew_mapi_request_id(&mut execute_headers);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // OpenCollector, contents.
    ]);
    append_rop_sync_import_deletes(
        &mut rops,
        0x02,
        0x02,
        &[committed.identity.object_id, unreserved_object_id],
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let response_rops = response_rops_from_execute_response(response).await;
    let batch_failed = contains_bytes(&response_rops, &[0x74, 0x02, 0x05, 0x40, 0, 0x80]);
    let state_after = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM mapi_navigation_shortcuts
             WHERE account_id = $1 AND id = $2),
            (SELECT COUNT(*) FROM mapi_object_identities
             WHERE account_id = $1
               AND object_kind = 'navigation_shortcut'
               AND canonical_id = $2
               AND deleted_at IS NULL),
            (SELECT COUNT(*) FROM mail_change_log
             WHERE account_id = $1
               AND object_kind = 'navigation_shortcut'
               AND object_id = $2
               AND change_kind = 'destroyed'),
            (SELECT COUNT(*) FROM mapi_object_identities
             WHERE account_id = $1
               AND (mapi_object_id = $3 OR source_key = $4)),
            (SELECT COUNT(*) FROM mapi_local_replica_deleted_ranges
             WHERE account_id = $1
               AND folder_id = $5
               AND min_global_counter <= $6
               AND max_global_counter >= $6)
        "#,
    )
    .bind(fixture.account_id)
    .bind(committed.shortcut.id)
    .bind(unreserved_object_id as i64)
    .bind(&unreserved_source_key)
    .bind(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64)
    .bind(unreserved_counter as i64)
    .fetch_one(storage.pool())
    .await?;
    fixture.cleanup().await?;

    assert!(logon_succeeded);
    assert_eq!(unreserved_precondition, (0, 0));
    assert!(batch_failed, "{response_rops:02x?}");
    assert_eq!(
        state_after,
        (1, 1, 0, 0, 0),
        "[MS-OXCFXICS] section 3.2.5.9.4.5 recommends rejecting a predictable failure before any deletion"
    );
    Ok(())
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
async fn mapi_over_http_import_deletes_rejects_unknown_flags_before_mutation() {
    let message_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaad0";
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
            "Unknown ImportDeleteFlags",
        )])),
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x82, &[test_mapi_message_id(message_id)]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x74, 0x02, 0x05, 0x40, 0, 0x80]
    ));
    assert!(
        deleted_emails.lock().unwrap().is_empty(),
        "[MS-OXCFXICS] section 3.2.5.9.4.5 recommends rejecting unknown flag bits before mutation"
    );
    assert_eq!(canonical_emails.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mapi_over_http_import_deletes_deduplicates_source_keys_before_mutation() {
    let message_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaad1";
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
            "Duplicate ImportDeletes SourceKey",
        )])),
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

    let message_object_id = test_mapi_message_id(message_id);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    append_rop_sync_import_deletes(
        &mut rops,
        0x02,
        0x02,
        &[message_object_id, message_object_id],
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

    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[Uuid::parse_str(message_id).unwrap()],
        "[MS-OXCFXICS] section 3.2.5.9.4.5 makes a repeated deletion an idempotent no-op"
    );
    assert!(canonical_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_hard_delete_returns_failure_when_retention_blocks_delete() {
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
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x02, &[test_mapi_message_id(message_id)]);

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
        contains_bytes(&response_rops, &[0x74, 0x02, 0x05, 0x40, 0, 0x80]),
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
    let deleted_emails = store.deleted_emails.clone();
    let canonical_emails = store.emails.clone();
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
    ]);
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x00, &[test_mapi_message_id(message_id)]);
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
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
    let upload_state_chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(upload_state_chunks.len(), 1);
    assert!(contains_bytes(
        &upload_state_chunks[0].1,
        &META_TAG_CNSET_SEEN.to_le_bytes()
    ));
    assert!(
        !contains_bytes(
            &upload_state_chunks[0].1,
            &META_TAG_IDSET_GIVEN.to_le_bytes()
        ),
        "[MS-OXCFXICS] section 3.2.5.2.1 forbids MetaTagIdsetGiven in upload state"
    );
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(Uuid::parse_str(message_id).unwrap(), trash_id)]
    );

    let mut retry_headers = execute_headers.clone();
    renew_mapi_request_id(&mut retry_headers);
    let retry_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &retry_headers, &request)
        .await
        .unwrap();
    let retry_response_rops = response_rops_from_execute_response(retry_response).await;
    assert!(
        contains_bytes(&retry_response_rops, &[0x74, 0x02, 0, 0, 0, 0]),
        "[MS-OXCFXICS] section 3.2.5.9.4.5 requires an already-deleted object to be ignored: {retry_response_rops:02x?}"
    );
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(Uuid::parse_str(message_id).unwrap(), trash_id)],
        "an idempotent retry must not move the message a second time"
    );
    assert!(
        deleted_emails.lock().unwrap().is_empty(),
        "an idempotent soft-delete retry must not hard-delete the Trash copy"
    );
    assert!(
        canonical_emails
            .lock()
            .unwrap()
            .iter()
            .any(|email| email.id == Uuid::parse_str(message_id).unwrap()),
        "the canonical Trash copy must remain visible after an idempotent retry"
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
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x00, &[test_mapi_message_id(message_id)]);

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
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[Uuid::parse_str(message_id).unwrap()]
    );
    assert!(moved_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_import_move_uses_canonical_store() {
    let message_id = "44444444-4444-4444-4444-444444444444";
    let source_folder_id = test_mapi_folder_id(5);
    let source_message_id = test_mapi_message_id(message_id);
    let destination_message_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 0x178,
    );
    let source_folder_gid = crate::mapi::identity::source_key_for_object_id(source_folder_id);
    let source_message_gid = crate::mapi::identity::source_key_for_object_id(source_message_id);
    let destination_message_gid =
        crate::mapi::identity::source_key_for_object_id(destination_message_id);
    let destination_change_number =
        mapi_mailstore::change_number_for_store_id(destination_message_id);
    let predecessor_change_list =
        mapi_mailstore::predecessor_change_list(destination_change_number);
    let change_xid = mapi_mailstore::change_key_for_change_number(destination_change_number);
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
    // [MS-OXCFXICS] section 2.2.3.2.4.4.1: SourceFolderId,
    // SourceMessageId, PCL, DestinationMessageId, and ChangeNumber are five
    // non-empty length-prefixed fields. The first, second, and fourth are GIDs.
    for field in [
        source_folder_gid.as_slice(),
        source_message_gid.as_slice(),
        predecessor_change_list.as_slice(),
        destination_message_gid.as_slice(),
        change_xid.as_slice(),
    ] {
        rops.extend_from_slice(&(field.len() as u32).to_le_bytes());
        rops.extend_from_slice(field);
    }
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
    assert_content_upload_final_state_includes(&response_rops, &[], &[], &[]);
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
    let first_reserved_counter = store
        .reserve_mapi_local_replica_ids(FakeStore::account().account_id, 0x1_0000)
        .await
        .unwrap();
    let imported_folder_id = crate::mapi::identity::mapi_store_id(first_reserved_counter + 0x200);
    let imported_source_key = mapi_mailstore::source_key_for_store_id(imported_folder_id);
    let created_mailboxes = store.created_mailboxes.clone();
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

    let imported_change_xid = [
        0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2,
        0xc1, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01,
    ];
    let mut imported_predecessor_change_list = vec![imported_change_xid.len() as u8];
    imported_predecessor_change_list.extend_from_slice(&imported_change_xid);
    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(
        &mut hierarchy_values,
        0x65E1_0102,
        &mapi_mailstore::source_key_for_store_id(parent_folder_id),
    );
    append_mapi_binary_property(&mut hierarchy_values, 0x65E0_0102, &imported_source_key);
    append_mapi_i64_property(&mut hierarchy_values, 0x3008_0040, 0);
    append_mapi_binary_property(&mut hierarchy_values, 0x65E2_0102, &imported_change_xid);
    append_mapi_binary_property(
        &mut hierarchy_values,
        0x65E3_0102,
        &imported_predecessor_change_list,
    );
    append_mapi_utf16_property(&mut hierarchy_values, 0x3001_001F, "Imported Sync Folder");

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "Imported Sync Folder");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x00, // RopSynchronizationOpenCollector, hierarchy
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
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
    let retry_cookie = mapi_cookie_header(&response);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x73, 0x02, 0, 0, 0, 0]),
        "system-folder reconciliation response: {response_rops:02x?}"
    );
    let created_mailbox_id = store
        .mailboxes
        .lock()
        .unwrap()
        .iter()
        .find(|mailbox| mailbox.name == "Imported Sync Folder")
        .unwrap()
        .id;
    let imported_change_number =
        store.mapi_identity_change_numbers.lock().unwrap()[&created_mailbox_id];
    assert!(
        imported_change_number >= first_reserved_counter + 0x1_0000,
        "the imported folder must receive a server CN outside Outlook's reserved FID range"
    );
    let state_chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(state_chunks.len(), 1);
    let cnset_seen = mapi_binary_property_value(&state_chunks[0].1, META_TAG_CNSET_SEEN);
    assert!(
        strict_replguid_globset_contains_counter(
            cnset_seen,
            &globcnt_bytes(imported_change_number),
        )
        .unwrap(),
        "ImportHierarchyChange must add a newly allocated server CN"
    );
    assert!(
        !strict_replguid_globset_contains_counter(
            cnset_seen,
            &globcnt_bytes(first_reserved_counter + 0x200),
        )
        .unwrap(),
        "the client-assigned FID must not be reused as a change number"
    );
    assert_eq!(
        created_mailboxes.lock().unwrap()[0].name,
        "Imported Sync Folder"
    );
    assert_eq!(
        created_mailboxes.lock().unwrap()[0].parent_id,
        Some(parent_id)
    );
    assert_eq!(
        store.mapi_identities.lock().unwrap()[&created_mailbox_id],
        imported_folder_id,
        "ImportHierarchyChange must preserve the FID assigned from the reserved local replica range"
    );
    assert_eq!(
        store.mapi_identity_source_keys.lock().unwrap()[&created_mailbox_id],
        imported_source_key
    );
    let mut retry_headers = mapi_headers("Execute");
    retry_headers.insert("cookie", HeaderValue::from_str(&retry_cookie).unwrap());
    let retry_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &retry_headers, &request)
        .await
        .unwrap();
    assert_eq!(retry_response.status(), StatusCode::OK);
    let retry_response_rops = response_rops_from_execute_response(retry_response).await;
    assert!(contains_bytes(
        &retry_response_rops,
        &[0x73, 0x02, 0, 0, 0, 0]
    ));
    assert_eq!(
        created_mailboxes.lock().unwrap().len(),
        1,
        "a retry after a lost response must reuse the canonical mailbox"
    );
    let retry_state_chunks = mapi_fast_transfer_chunks(&retry_response_rops);
    assert_eq!(retry_state_chunks.len(), 1);
    let retry_cnset_seen =
        mapi_binary_property_value(&retry_state_chunks[0].1, META_TAG_CNSET_SEEN);
    assert!(
        strict_replguid_globset_contains_counter(
            retry_cnset_seen,
            &globcnt_bytes(imported_change_number),
        )
        .unwrap(),
        "the idempotent retry must retain the server-allocated CN"
    );
    assert!(
        !strict_replguid_globset_contains_counter(
            retry_cnset_seen,
            &globcnt_bytes(first_reserved_counter + 0x200),
        )
        .unwrap(),
        "the idempotent retry must not turn the client FID into a CN"
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
    let first_reserved_counter = store
        .reserve_mapi_local_replica_ids(FakeStore::account().account_id, 0x0001_0000)
        .await
        .unwrap();
    // Model a server CN already emitted after the client-reserved range; using
    // Inbox's FID counter here would hide the FID/CN confusion under test.
    let uploaded_change_number = first_reserved_counter + 0x0001_0000;
    *store.next_mapi_global_counter.lock().unwrap() = uploaded_change_number + 1;
    let imported_global_counter = first_reserved_counter + 0x200;
    let imported_source_key = crate::mapi::identity::source_key_for_object_id(
        crate::mapi::identity::mapi_store_id(imported_global_counter),
    );
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

    let imported_change_xid = [
        0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2,
        0xc1, 0x00, 0x00, 0x00, 0x00, 0x02, 0x02,
    ];
    let mut imported_predecessor_change_list = vec![imported_change_xid.len() as u8];
    imported_predecessor_change_list.extend_from_slice(&imported_change_xid);
    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PARENT_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(parent_folder_id),
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_SOURCE_KEY,
        &imported_source_key,
    );
    append_mapi_i64_property(&mut hierarchy_values, PID_TAG_LAST_MODIFICATION_TIME, 0);
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_CHANGE_KEY,
        &imported_change_xid,
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_predecessor_change_list,
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
    let uploaded_cnset_seen = strict_test_replguid_globset(&[uploaded_change_number]);

    let mut upload_rops = Vec::new();
    append_rop_open_folder(&mut upload_rops, 0, 1, test_mapi_folder_id(5));
    upload_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
    ]);
    // [MS-OXCFXICS] section 2.2.3.2.4.1.1: IsContentsCollector is a
    // PtypBoolean; 0x00 selects a hierarchy upload collector.
    upload_rops.push(0x00);
    upload_rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    upload_rops.extend_from_slice(&META_TAG_CNSET_SEEN.to_le_bytes());
    upload_rops.extend_from_slice(&(uploaded_cnset_seen.len() as u32).to_le_bytes());
    upload_rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    upload_rops.extend_from_slice(&(uploaded_cnset_seen.len() as u32).to_le_bytes());
    upload_rops.extend_from_slice(&uploaded_cnset_seen);
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
    assert!(
        !contains_bytes(
            &upload_state_chunks[0].1,
            &META_TAG_IDSET_GIVEN.to_le_bytes()
        ),
        "[MS-OXCFXICS] section 3.2.5.2.1 forbids returning MetaTagIdsetGiven from upload state"
    );
    assert!(contains_bytes(
        &upload_state_chunks[0].1,
        &META_TAG_CNSET_SEEN.to_le_bytes()
    ));
    let returned_cnset_seen =
        mapi_binary_property_value(&upload_state_chunks[0].1, META_TAG_CNSET_SEEN);
    assert!(
        strict_replguid_globset_contains_counter(
            returned_cnset_seen,
            &globcnt_bytes(uploaded_change_number),
        )
        .unwrap(),
        "GetTransferState discarded the CnsetSeen uploaded before ImportHierarchyChange"
    );
    let imported_mailbox_id = store
        .mailboxes
        .lock()
        .unwrap()
        .iter()
        .find(|mailbox| mailbox.name == "MS-OXCFXICS 4.1.1 Folder")
        .unwrap()
        .id;
    let imported_change_number = *store
        .mapi_identity_change_numbers
        .lock()
        .unwrap()
        .get(&imported_mailbox_id)
        .unwrap();
    assert!(
        strict_replguid_globset_contains_counter(
            returned_cnset_seen,
            &globcnt_bytes(imported_change_number),
        )
        .unwrap(),
        "GetTransferState omitted the change number created by ImportHierarchyChange"
    );
    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].name, "MS-OXCFXICS 4.1.1 Folder");
    assert_eq!(created[0].parent_id, Some(parent_id));
}

#[tokio::test]
async fn mapi_over_http_import_deletes_prevalidates_hierarchy_batch_before_mutation() {
    let custom_folder_id = Uuid::parse_str("88888888-8888-4888-8888-8888888888d2").unwrap();
    let custom_folder_object_id = test_mapi_uuid_id(&custom_folder_id);
    crate::mapi::identity::remember_mapi_identity(custom_folder_id, custom_folder_object_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox(
                &custom_folder_id.to_string(),
                "custom",
                "Valid batch folder",
            ),
        ])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(custom_folder_id, custom_folder_object_id);
    let destroyed_mailboxes = store.destroyed_mailboxes.clone();
    let canonical_mailboxes = store.mailboxes.clone();
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
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x00, // RopSynchronizationOpenCollector, hierarchy.
    ]);
    append_rop_sync_import_deletes(
        &mut rops,
        0x02,
        0x03,
        &[
            custom_folder_object_id,
            crate::mapi::identity::INBOX_FOLDER_ID,
        ],
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

    assert!(contains_bytes(
        &response_rops,
        &[0x74, 0x02, 0x05, 0x40, 0, 0x80]
    ));
    assert!(
        destroyed_mailboxes.lock().unwrap().is_empty(),
        "[MS-OXCFXICS] section 3.2.5.9.4.5 requires predictable batch failure before mutation"
    );
    assert!(canonical_mailboxes
        .lock()
        .unwrap()
        .iter()
        .any(|mailbox| mailbox.id == custom_folder_id));
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
    // [MS-OXCFXICS] section 2.2.3.2.4.1.1: 0x00 selects hierarchy.
    rops.push(0x00);
    rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&META_TAG_CNSET_SEEN.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
    ]);
    append_rop_sync_import_deletes(&mut rops, 0x02, 0x02, &[folder_mapi_id]);
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0]));
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
    let cnset_seen = mapi_binary_property_value(&state_chunks[0].1, META_TAG_CNSET_SEEN);
    assert!(
        !strict_replguid_globset_contains_counter(
            cnset_seen,
            &globcnt_bytes(
                crate::mapi::identity::global_counter_from_store_id(folder_mapi_id).unwrap()
            ),
        )
        .unwrap(),
        "[MS-OXCFXICS] section 3.2.5.9.4.5 does not turn a deleted FID into a CN"
    );

    let mut retry_headers = execute_headers.clone();
    renew_mapi_request_id(&mut retry_headers);
    let retry_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &retry_headers, &request)
        .await
        .unwrap();
    let retry_response_rops = response_rops_from_execute_response(retry_response).await;
    assert!(
        contains_bytes(&retry_response_rops, &[0x74, 0x02, 0, 0, 0, 0]),
        "[MS-OXCFXICS] section 3.2.5.9.4.5 requires an already-deleted folder to be ignored: {retry_response_rops:02x?}"
    );
    assert_eq!(destroyed_mailboxes.lock().unwrap().as_slice(), &[folder_id]);
}

#[tokio::test]
async fn mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items() {
    let deleted_items_id = Uuid::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox(
                "88888888-8888-4888-8888-888888888888",
                "trash",
                "Deleted Items",
            ),
        ])),
        ..Default::default()
    };
    store
        .load_mapi_mail_store(FakeStore::account().account_id, 500)
        .await
        .unwrap();
    let current_server_change_number =
        store.mapi_identity_change_numbers.lock().unwrap()[&deleted_items_id];
    let created_mailboxes = store.created_mailboxes.clone();
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

    // Shape captured from Outlook 16.0 run 202607181515: the existing Deleted
    // Items folder uses the canonical SourceKey and IPM subtree parent. The
    // PCL carries the durable ChangeKey that the corrected server advertises.
    let imported_change_xid = [
        0x51, 0xa1, 0x66, 0x72, 0x14, 0x93, 0x5c, 0x48, 0xaa, 0x14, 0xe7, 0xdc, 0xb0, 0x5e, 0x0d,
        0xa6, 0x00, 0x00, 0x04, 0x15,
    ];
    let mut imported_predecessor_change_list = vec![imported_change_xid.len() as u8];
    imported_predecessor_change_list.extend_from_slice(&imported_change_xid);
    let existing_change_key =
        crate::mapi::identity::change_key_for_change_number(current_server_change_number);
    imported_predecessor_change_list.push(existing_change_key.len() as u8);
    imported_predecessor_change_list.extend_from_slice(&existing_change_key);
    let imported_last_modification_time = mapi_mailstore::filetime_from_change_number(17_100_000);
    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PARENT_SOURCE_KEY,
        &crate::mapi::identity::source_key_for_object_id(
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        ),
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_SOURCE_KEY,
        &crate::mapi::identity::source_key_for_object_id(crate::mapi::identity::TRASH_FOLDER_ID),
    );
    append_mapi_i64_property(
        &mut hierarchy_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time as i64,
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_CHANGE_KEY,
        &imported_change_xid,
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_predecessor_change_list,
    );
    append_mapi_utf16_property(
        &mut hierarchy_values,
        PID_TAG_DISPLAY_NAME_W,
        "Deleted Items",
    );

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_DISPLAY_NAME_W,
        "Ignored duplicate name",
    );
    append_mapi_utf16_property(&mut property_values, 0x3613_001F, "IPF.Note");

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x00, // RopSynchronizationOpenCollector, hierarchy
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    append_rop_open_folder(&mut rops, 0, 4, crate::mapi::identity::TRASH_FOLDER_ID);
    append_rop_get_properties_specific(
        &mut rops,
        4,
        &[
            PID_TAG_CHANGE_NUMBER,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_LOCAL_COMMIT_TIME_MAX,
        ],
    );

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
    assert!(
        contains_bytes(&response_rops, &[0x73, 0x02, 0, 0, 0, 0]),
        "[MS-OXCFXICS] sections 2.2.3.2.4.3 and 3.2.5.9.4.3 require an existing system-folder hierarchy change to be acknowledged: {response_rops:02x?}"
    );
    let state_chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(state_chunks.len(), 1);
    let cnset_seen = mapi_binary_property_value(&state_chunks[0].1, META_TAG_CNSET_SEEN);
    let allocated_change_number = *store.next_mapi_global_counter.lock().unwrap() - 1;
    let mut same_execute_offset =
        mapi_get_properties_specific_standard_row_offset(&response_rops, 4).unwrap() + 1;
    let same_execute_change_number = crate::mapi::identity::object_id_from_wire_id(
        &response_rops[same_execute_offset..same_execute_offset + 8],
    )
    .and_then(crate::mapi::identity::global_counter_from_store_id)
    .unwrap();
    same_execute_offset += 8;
    let same_execute_change_key =
        read_rop_binary_u16(&response_rops, &mut same_execute_offset).unwrap();
    let same_execute_predecessor_change_list =
        read_rop_binary_u16(&response_rops, &mut same_execute_offset).unwrap();
    let same_execute_last_modification_time = u64::from_le_bytes(
        response_rops[same_execute_offset..same_execute_offset + 8]
            .try_into()
            .unwrap(),
    );
    same_execute_offset += 8;
    let same_execute_local_commit_time_max = u64::from_le_bytes(
        response_rops[same_execute_offset..same_execute_offset + 8]
            .try_into()
            .unwrap(),
    );
    assert_eq!(same_execute_change_number, allocated_change_number);
    assert_eq!(same_execute_change_key, imported_change_xid);
    assert_eq!(
        same_execute_predecessor_change_list,
        imported_predecessor_change_list
    );
    assert_eq!(
        same_execute_last_modification_time,
        imported_last_modification_time
    );
    assert_eq!(
        same_execute_local_commit_time_max,
        mapi_mailstore::filetime_from_change_number(40),
        "[MS-OXCFOLD] section 2.2.2.2.1.14 ties LocalCommitTimeMax to top-level content changes, not hierarchy imports"
    );
    assert!(
        allocated_change_number > crate::mapi::identity::TRASH_FOLDER_COUNTER,
        "the accepted hierarchy change must receive a distinct server CN"
    );
    assert!(
        strict_replguid_globset_contains_counter(
            cnset_seen,
            &globcnt_bytes(allocated_change_number),
        )
        .unwrap(),
        "GetTransferState must return the newly allocated server CN"
    );
    assert!(
        !strict_replguid_globset_contains_counter(
            cnset_seen,
            &globcnt_bytes(crate::mapi::identity::TRASH_FOLDER_COUNTER),
        )
        .unwrap(),
        "the Deleted Items FID must not be reused as its change number"
    );
    assert!(created_mailboxes.lock().unwrap().is_empty());

    assert_eq!(
        store.mapi_identity_change_keys.lock().unwrap()[&deleted_items_id],
        imported_change_xid
    );
    assert_eq!(
        store.mapi_identity_predecessor_change_lists.lock().unwrap()[&deleted_items_id],
        imported_predecessor_change_list
    );

    let mut reopen_rops = Vec::new();
    append_rop_open_folder(
        &mut reopen_rops,
        0,
        1,
        crate::mapi::identity::TRASH_FOLDER_ID,
    );
    append_rop_get_properties_specific(
        &mut reopen_rops,
        1,
        &[
            PID_TAG_CHANGE_NUMBER,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_LOCAL_COMMIT_TIME_MAX,
        ],
    );
    renew_mapi_request_id(&mut execute_headers);
    let reopen_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&reopen_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let reopen_response_rops = response_rops_from_execute_response(reopen_response).await;
    let mut row_offset =
        mapi_get_properties_specific_standard_row_offset(&reopen_response_rops, 1).unwrap() + 1;
    let reopened_change_number = crate::mapi::identity::object_id_from_wire_id(
        &reopen_response_rops[row_offset..row_offset + 8],
    )
    .and_then(crate::mapi::identity::global_counter_from_store_id)
    .unwrap();
    row_offset += 8;
    let reopened_change_key = read_rop_binary_u16(&reopen_response_rops, &mut row_offset).unwrap();
    let reopened_predecessor_change_list =
        read_rop_binary_u16(&reopen_response_rops, &mut row_offset).unwrap();
    let reopened_last_modification_time = u64::from_le_bytes(
        reopen_response_rops[row_offset..row_offset + 8]
            .try_into()
            .unwrap(),
    );
    row_offset += 8;
    let reopened_local_commit_time_max = u64::from_le_bytes(
        reopen_response_rops[row_offset..row_offset + 8]
            .try_into()
            .unwrap(),
    );
    assert_eq!(reopened_change_number, allocated_change_number);
    assert_eq!(reopened_change_key, imported_change_xid);
    assert_eq!(
        reopened_predecessor_change_list,
        imported_predecessor_change_list
    );
    assert_eq!(
        reopened_last_modification_time,
        imported_last_modification_time
    );
    assert_eq!(
        reopened_local_commit_time_max,
        mapi_mailstore::filetime_from_change_number(40),
        "LocalCommitTimeMax must remain the canonical content watermark after reloading the imported hierarchy version"
    );
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
    let first_reserved_counter = store
        .reserve_mapi_local_replica_ids(FakeStore::account().account_id, 0x1_0000)
        .await
        .unwrap();
    let alias_id = crate::mapi::identity::mapi_store_id(first_reserved_counter + 0x203);
    let alias_source_key = crate::mapi::identity::source_key_for_object_id(alias_id);
    let created_mailboxes = store.created_mailboxes.clone();
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

    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PARENT_SOURCE_KEY,
        &crate::mapi::identity::source_key_for_object_id(
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        ),
    );
    append_mapi_binary_property(&mut hierarchy_values, PID_TAG_SOURCE_KEY, &alias_source_key);
    let alias_change_key = crate::mapi::identity::change_key_for_change_number(900);
    append_mapi_i64_property(
        &mut hierarchy_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        mapi_mailstore::filetime_from_change_number(900) as i64,
    );
    append_mapi_binary_property(&mut hierarchy_values, PID_TAG_CHANGE_KEY, &alias_change_key);
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &mapi_mailstore::predecessor_change_list(900),
    );
    append_mapi_utf16_property(&mut hierarchy_values, 0x3001_001F, "Sync Issues");

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "Sync Issues");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x00, // RopSynchronizationOpenCollector, hierarchy
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
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
    assert!(
        contains_bytes(&response_rops, &[0x73, 0x02, 0, 0, 0, 0]),
        "system-folder reconciliation response: {response_rops:02x?}"
    );
    let state_chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(state_chunks.len(), 1);
    let cnset_seen = mapi_binary_property_value(&state_chunks[0].1, META_TAG_CNSET_SEEN);
    let alias_change_number = store
        .mapi_special_folder_alias_change_numbers
        .lock()
        .unwrap()[&alias_id];
    assert!(alias_change_number >= first_reserved_counter + 0x1_0000);
    assert!(
        strict_replguid_globset_contains_counter(cnset_seen, &globcnt_bytes(alias_change_number),)
            .unwrap(),
        "special-folder reconciliation must add a server CN after the client-reserved FID range"
    );
    assert!(
        !strict_replguid_globset_contains_counter(cnset_seen, &globcnt_bytes(26)).unwrap(),
        "the canonical Sync Issues FID must not be reused as a change number"
    );
    assert!(
        !strict_replguid_globset_contains_counter(
            cnset_seen,
            &globcnt_bytes(first_reserved_counter + 0x203),
        )
        .unwrap(),
        "the client-reserved alias FID must not be reused as a change number"
    );
    assert!(created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_sync_imported_special_folder_alias_survives_a_new_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let first_reserved_counter = store
        .reserve_mapi_local_replica_ids(FakeStore::account().account_id, 0x1_0000)
        .await
        .unwrap();
    let alias_id = crate::mapi::identity::mapi_store_id(first_reserved_counter + 0x204);
    let alias_source_key = crate::mapi::identity::source_key_for_object_id(alias_id);
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);

    let first_connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut first_headers = mapi_headers("Execute");
    first_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&first_connect)).unwrap(),
    );
    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PARENT_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(test_mapi_folder_id(4)),
    );
    append_mapi_binary_property(&mut hierarchy_values, PID_TAG_SOURCE_KEY, &alias_source_key);
    append_mapi_i64_property(
        &mut hierarchy_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        mapi_mailstore::filetime_from_change_number(901) as i64,
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_CHANGE_KEY,
        &crate::mapi::identity::change_key_for_change_number(901),
    );
    append_mapi_binary_property(
        &mut hierarchy_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &mapi_mailstore::predecessor_change_list(901),
    );
    append_mapi_utf16_property(&mut hierarchy_values, PID_TAG_DISPLAY_NAME_W, "Junk E-mail");
    let mut import_rops = Vec::new();
    append_rop_open_folder(&mut import_rops, 0, 1, test_mapi_folder_id(4));
    import_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x00, // RopSynchronizationOpenCollector, hierarchy
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    import_rops.extend_from_slice(&6u16.to_le_bytes());
    import_rops.extend_from_slice(&hierarchy_values);
    import_rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_utf16_property(&mut import_rops, PID_TAG_DISPLAY_NAME_W, "Junk E-mail");
    let import_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &first_headers,
            &execute_body(&rop_buffer(&import_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let import_response_rops = response_rops_from_execute_response(import_response).await;
    assert!(contains_bytes(
        &import_response_rops,
        &[0x73, 0x02, 0, 0, 0, 0]
    ));

    let second_connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut second_headers = mapi_headers("Execute");
    second_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&second_connect)).unwrap(),
    );
    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, alias_id);
    append_rop_get_properties_specific(&mut open_rops, 1, &[PID_TAG_DISPLAY_NAME_W]);
    let open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &second_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let open_response_rops = response_rops_from_execute_response(open_response).await;
    assert!(
        contains_bytes(&open_response_rops, &[0x02, 0x01, 0, 0, 0, 0]),
        "a second EMSMDB session must resolve the FID reconciled by the first session: {open_response_rops:02x?}"
    );
    assert!(contains_bytes(&open_response_rops, &utf16z("Junk E-mail")));
    assert!(created_mailboxes.lock().unwrap().is_empty());

    renew_mapi_request_id(&mut second_headers);
    let alias_counter =
        crate::mapi::identity::global_counter_from_store_id(alias_id).expect("alias GLOBCNT");
    let alias_idset_given = strict_test_replguid_globset(&[alias_counter]);
    let mut hierarchy_rops = Vec::new();
    append_rop_open_folder(
        &mut hierarchy_rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        &mut hierarchy_rops,
        1,
        2,
        20_000,
        &alias_idset_given,
        &[],
    );
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &second_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let hierarchy_response_rops = response_rops_from_execute_response(hierarchy_response).await;
    let hierarchy_chunks = mapi_fast_transfer_chunks(&hierarchy_response_rops);
    assert_eq!(hierarchy_chunks.len(), 1);
    let hierarchy_stream = &hierarchy_chunks[0].1;
    assert!(
        !contains_bytes(hierarchy_stream, &META_TAG_IDSET_DELETED.to_le_bytes()),
        "a successful durable special-folder alias must not be reported deleted"
    );
    let final_idset_given = mapi_binary_property_value(hierarchy_stream, META_TAG_IDSET_GIVEN);
    assert!(
        strict_replguid_globset_contains_counter(final_idset_given, &globcnt_bytes(alias_counter))
            .unwrap(),
        "the active alias must remain in hierarchy MetaTagIdsetGiven"
    );
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
