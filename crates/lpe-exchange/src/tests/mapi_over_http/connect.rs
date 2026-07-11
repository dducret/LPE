use super::*;

#[tokio::test]
async fn mapi_over_http_execute_accepts_release_rop() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 6);
    assert_eq!(&body[16..18], &[0, 0]);
    assert_eq!(&body[18..22], &u32::MAX.to_le_bytes());
}

#[tokio::test]
async fn mapi_over_http_empty_extended_execute_returns_success() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rpc_proxy_wrapped_rop_buffer(&[], &[])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    assert_eq!(rop_buffer_size, 10);
    assert_eq!(&body[16..26], &[0, 0, 4, 0, 2, 0, 2, 0, 2, 0]);
}

#[tokio::test]
async fn mapi_over_http_execute_sets_columns_and_queries_empty_rows() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x6748_0014u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_create_save_message_can_target_trash() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "MAPI saved to Trash");

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x0A, 0x00, 0x02]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x02, 0x00]);

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
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, trash_id);
    assert_eq!(recorded[0].subject, "MAPI saved to Trash");
}

#[tokio::test]
async fn mapi_over_http_conversation_action_applies_to_future_matching_message() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let conversation_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
    let target_folder_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        FakeStore::account().account_id,
        crate::mapi::identity::TRASH_FOLDER_ID,
    )
    .unwrap();
    let action = ConversationAction {
        id: Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(),
        conversation_id,
        subject: "Ignore future".to_string(),
        categories_json: "[\"Green Category\"]".to_string(),
        move_folder_entry_id: Some(target_folder_entry_id),
        move_store_entry_id: None,
        move_target_mailbox_id: Some(trash_id),
        max_delivery_time: None,
        last_applied_time: None,
        version: lpe_storage::CONVERSATION_ACTION_VERSION,
        processed: 1,
        created_at: "2026-05-22T12:00:00Z".to_string(),
        updated_at: "2026-05-22T12:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items"),
        ])),
        conversation_actions: Arc::new(Mutex::new(vec![action])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let imported_emails = store.imported_emails.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Future matching message");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Body");
    append_mapi_binary_property(
        &mut property_values,
        0x0071_0102,
        &test_conversation_index(conversation_id),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[0x06, 0x01, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.push(0);
    append_rop_set_properties(&mut rops, 2, 3, &property_values);
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
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].thread_id, Some(conversation_id));
    drop(recorded);

    let imported_message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(imported_message_id, trash_id)]
    );
    let emails = emails_state.lock().unwrap();
    let imported = emails
        .iter()
        .find(|email| email.id == imported_message_id)
        .unwrap();
    assert_eq!(imported.mailbox_id, trash_id);
    assert_eq!(imported.categories, vec!["Green Category".to_string()]);
}

#[tokio::test]
async fn mapi_over_http_conversation_action_cross_store_keeps_local_message_in_place() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let conversation_id = Uuid::parse_str("77777777-7777-4777-8777-777777777778").unwrap();
    let action = ConversationAction {
        id: Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaab").unwrap(),
        conversation_id,
        subject: "Cross store".to_string(),
        categories_json: "[\"CrossStore\"]".to_string(),
        move_folder_entry_id: Some(vec![1, 2, 3]),
        move_store_entry_id: Some(vec![4, 5, 6]),
        move_target_mailbox_id: Some(trash_id),
        max_delivery_time: None,
        last_applied_time: None,
        version: lpe_storage::CONVERSATION_ACTION_VERSION,
        processed: 1,
        created_at: "2026-05-22T12:00:00Z".to_string(),
        updated_at: "2026-05-22T12:00:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items"),
        ])),
        conversation_actions: Arc::new(Mutex::new(vec![action])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Cross-store message");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Body");
    append_mapi_binary_property(
        &mut property_values,
        0x0071_0102,
        &test_conversation_index(conversation_id),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[0x06, 0x01, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.push(0);
    append_rop_set_properties(&mut rops, 2, 3, &property_values);
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
    assert!(moved_emails.lock().unwrap().is_empty());
    let imported_message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let emails = emails_state.lock().unwrap();
    let imported = emails
        .iter()
        .find(|email| email.id == imported_message_id)
        .unwrap();
    assert_eq!(imported.mailbox_id, inbox_id);
    assert_eq!(imported.categories, vec!["CrossStore".to_string()]);
}

#[tokio::test]
async fn mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end() {
    let drafts_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let sent_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&drafts_id.to_string(), "drafts", "Drafts"),
            FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
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

    let lifecycle_subject = "Outlook day-two canonical draft";
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, lifecycle_subject);
    append_mapi_utf16_property(
        &mut property_values,
        0x1000_001F,
        "Created through EMSMDB and submitted through canonical LPE",
    );
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-lifecycle@example.test>",
    );
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let bcc_row = mapi_recipient_row("Hidden", "hidden@example.test", 0x03);

    let mut create_rops = Vec::new();
    append_rop_open_folder(&mut create_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_create_message(&mut create_rops, 1, 2, test_mapi_folder_id(14));
    append_rop_set_properties(&mut create_rops, 2, 3, &property_values);
    append_rop_modify_recipients(
        &mut create_rops,
        2,
        &[(1, 0x01, to_row.as_slice()), (2, 0x03, bcc_row.as_slice())],
    );
    append_rop_save_changes_message(&mut create_rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let create_request = execute_body(&rop_buffer(&create_rops, &[1, u32::MAX, u32::MAX]));
    let create_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &create_request)
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::OK);
    let create_response_rops = response_rops_from_execute_response(create_response).await;
    assert!(contains_bytes(
        &create_response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));
    assert_eq!(imported_emails.lock().unwrap().len(), 1);
    let draft_message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let draft_mapi_message_id = test_mapi_message_id(&draft_message_id.to_string());
    {
        let canonical = emails.lock().unwrap();
        let draft = canonical
            .iter()
            .find(|email| email.id == draft_message_id)
            .expect("saved draft is visible in canonical store");
        assert_eq!(draft.mailbox_id, drafts_id);
        assert_eq!(draft.mailbox_role, "drafts");
        assert_eq!(draft.subject, lifecycle_subject);
        assert_eq!(draft.to[0].address, "bob@example.test");
        assert_eq!(draft.bcc[0].address, "hidden@example.test");
    }

    let mut sync_rops = Vec::new();
    append_rop_open_folder(&mut sync_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_sync_manifest_get_buffer(&mut sync_rops, 1, 2, 4096);
    renew_mapi_request_id(&mut execute_headers);
    let sync_request = execute_body(&rop_buffer(&sync_rops, &[1, u32::MAX, u32::MAX]));
    let sync_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &sync_request)
        .await
        .unwrap();

    assert_eq!(sync_response.status(), StatusCode::OK);
    let sync_response_rops = response_rops_from_execute_response(sync_response).await;
    assert!(!contains_bytes(&sync_response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(
        &sync_response_rops,
        lifecycle_subject.as_bytes()
    ));
    assert!(!contains_bytes(&sync_response_rops, b"hidden@example.test"));

    let mut flag_rops = Vec::new();
    append_rop_open_folder(&mut flag_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_set_read_flags(&mut flag_rops, 1, 0x05, &[draft_mapi_message_id]);
    renew_mapi_request_id(&mut execute_headers);
    let flag_request = execute_body(&rop_buffer(&flag_rops, &[1, u32::MAX]));
    let flag_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &flag_request)
        .await
        .unwrap();

    assert_eq!(flag_response.status(), StatusCode::OK);
    let flag_response_rops = response_rops_from_execute_response(flag_response).await;
    assert!(contains_bytes(
        &flag_response_rops,
        &[0x66, 0x01, 0, 0, 0, 0, 0]
    ));
    assert!(
        emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == draft_message_id)
            .unwrap()
            .unread
    );

    let mut submit_rops = Vec::new();
    append_rop_open_folder(&mut submit_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut submit_rops,
        1,
        2,
        test_mapi_folder_id(14),
        draft_mapi_message_id,
    );
    append_rop_submit_message(&mut submit_rops, 2);
    renew_mapi_request_id(&mut execute_headers);
    let submit_request = execute_body(&rop_buffer(&submit_rops, &[1, u32::MAX, u32::MAX]));
    let submit_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &submit_request)
        .await
        .unwrap();

    assert_eq!(submit_response.status(), StatusCode::OK);
    let submit_response_rops = response_rops_from_execute_response(submit_response).await;
    assert!(contains_bytes(
        &submit_response_rops,
        &[0x32, 0x02, 0, 0, 0, 0]
    ));
    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, Some(draft_message_id));
        assert_eq!(recorded[0].subject, lifecycle_subject);
        assert_eq!(recorded[0].to[0].address, "bob@example.test");
        assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    }
    {
        let canonical = emails.lock().unwrap();
        assert!(canonical.iter().all(|email| email.id != draft_message_id));
        assert_eq!(
            canonical
                .iter()
                .filter(|email| email.mailbox_role == "sent")
                .count(),
            1
        );
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        let sent = canonical
            .iter()
            .find(|email| email.mailbox_role == "sent")
            .expect("submitted message is visible in canonical Sent");
        assert_eq!(sent.mailbox_id, sent_id);
        assert_eq!(sent.subject, lifecycle_subject);
        assert!(sent.unread);
    }

    let mut sent_table_rops = Vec::new();
    append_rop_open_folder(&mut sent_table_rops, 0, 1, test_mapi_folder_id(7));
    append_rop_query_subject_rows(&mut sent_table_rops, 1, 2, 10);
    renew_mapi_request_id(&mut execute_headers);
    let sent_table_request = execute_body(&rop_buffer(&sent_table_rops, &[1, u32::MAX, u32::MAX]));
    let sent_table_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &sent_table_request)
        .await
        .unwrap();

    assert_eq!(sent_table_response.status(), StatusCode::OK);
    let sent_table_response_rops = response_rops_from_execute_response(sent_table_response).await;
    assert!(contains_bytes(
        &sent_table_response_rops,
        &utf16z(lifecycle_subject)
    ));
    assert!(!contains_bytes(
        &sent_table_response_rops,
        &utf16z("hidden@example.test")
    ));
}

#[tokio::test]
async fn mapi_over_http_reload_cached_information_returns_pending_message_summary() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Cached pending");
    let row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x0A, 0x00, 0x02]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0E, 0x00, 0x02]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
    rops.extend_from_slice(&row);
    rops.extend_from_slice(&[0x10, 0x00, 0x02, 0x00, 0x00]); // RopReloadCachedInformation

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
    let reload_offset = response_rops
        .windows(6)
        .position(|window| window == [0x10, 0x02, 0, 0, 0, 0].as_slice())
        .unwrap();
    assert_eq!(response_rops[reload_offset + 6], 0);
    assert_eq!(response_rops[reload_offset + 7], 0x01);
    assert_eq!(response_rops[reload_offset + 8], 0x04);
    let subject = utf16z("Cached pending");
    assert_eq!(
        &response_rops[reload_offset + 9..reload_offset + 9 + subject.len()],
        subject.as_slice()
    );
    let recipient_count_offset = reload_offset + 9 + subject.len();
    assert_eq!(
        u16::from_le_bytes(
            response_rops[recipient_count_offset..recipient_count_offset + 2]
                .try_into()
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_reload_cached_information_rejects_nonzero_reserved() {
    let message_id = Uuid::parse_str("10101010-1010-4010-9010-101010101019").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Invalid reload reserved",
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
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x03, 0x00, 0x01, 0x02]); // RopOpenMessage
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_uuid_id(&message_id));
    rops.extend_from_slice(&[
        0x10, 0x00, 0x02, 0x01, 0x00, // RopReloadCachedInformation with nonzero Reserved.
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ]);

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
        &[0x10, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_move_copy_messages_uses_canonical_store() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let archive_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let move_message_id = Uuid::parse_str("9b9b9b9b-9b9b-9b9b-9b9b-9b9b9b9b9b9b").unwrap();
    let copy_message_id = Uuid::parse_str("9c9c9c9c-9c9c-9c9c-9c9c-9c9c9c9c9c9c").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &move_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Move through MAPI",
            ),
            FakeStore::email(
                &copy_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Copy through MAPI",
            ),
        ])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let copied_emails = store.copied_emails.clone();
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x02, 0x00, 0x00, 0x02, // RopOpenFolder, Archive
    ]);
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ARCHIVE_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, move
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&move_message_id.to_string()),
    );
    rops.push(0);
    rops.push(0);
    rops.extend_from_slice(&[
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, copy
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&copy_message_id.to_string()),
    );
    rops.push(0);
    rops.push(1);

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

    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(move_message_id, archive_id)]
    );
    assert_eq!(
        copied_emails.lock().unwrap().as_slice(),
        &[(copy_message_id, archive_id)]
    );
    {
        let canonical = canonical_emails.lock().unwrap();
        let moved = canonical
            .iter()
            .find(|email| email.id == move_message_id)
            .expect("moved message remains canonical");
        assert_eq!(moved.mailbox_id, archive_id);
        assert_eq!(moved.mailbox_role, "archive");
        let copied = canonical
            .iter()
            .find(|email| email.id != copy_message_id && email.subject == "Copy through MAPI")
            .expect("copied message is created through canonical copy");
        assert_eq!(copied.mailbox_id, archive_id);
        assert_eq!(copied.mailbox_role, "archive");
        assert_eq!(
            canonical
                .iter()
                .filter(|email| email.subject == "Copy through MAPI")
                .count(),
            2
        );
    }
    assert_eq!(
        response_rops
            .windows(7)
            .filter(|window| *window == [0x33, 0x01, 0, 0, 0, 0, 0])
            .count(),
        2
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_move_copy_messages_accepts_nonzero_boolean_fields() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let archive_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let move_message_id = Uuid::parse_str("9e9e9e91-9e91-4e91-9e91-9e9e9e9e9e91").unwrap();
    let copy_message_id = Uuid::parse_str("9e9e9e92-9e92-4e92-9e92-9e9e9e9e9e92").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &move_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "MoveCopy nonzero move",
            ),
            FakeStore::email(
                &copy_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "MoveCopy nonzero copy",
            ),
        ])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let copied_emails = store.copied_emails.clone();
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

    for (message_id, want_async, want_copy) in
        [(move_message_id, 0x02, 0x00), (copy_message_id, 0x00, 0x02)]
    {
        let mut rops = Vec::new();
        append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
        append_rop_open_folder(&mut rops, 0, 2, crate::mapi::identity::ARCHIVE_FOLDER_ID);
        rops.extend_from_slice(&[
            0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages.
        ]);
        rops.extend_from_slice(&1u16.to_le_bytes());
        append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));
        rops.push(want_async);
        rops.push(want_copy);

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
            &[0x33, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
        ));
        renew_mapi_request_id(&mut execute_headers);
    }

    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(move_message_id, archive_id)]
    );
    assert_eq!(
        copied_emails.lock().unwrap().as_slice(),
        &[(copy_message_id, archive_id)]
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_delete_messages_uses_trash_and_hard_delete() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let soft_message_id = Uuid::parse_str("9d9d9d9d-9d9d-9d9d-9d9d-9d9d9d9d9d9d").unwrap();
    let hard_message_id = Uuid::parse_str("9f9f9f9f-9f9f-9f9f-9f9f-9f9f9f9f9f9f").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &soft_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Soft delete through MAPI",
            ),
            FakeStore::email(
                &hard_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Hard delete through MAPI",
            ),
        ])),
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x1E, 0x00, 0x01, 0x00, 0x00, // RopDeleteMessages
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&soft_message_id.to_string()),
    );
    rops.extend_from_slice(&[
        0x91, 0x00, 0x01, 0x00, 0x00, // RopHardDeleteMessages
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&hard_message_id.to_string()),
    );

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
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

    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(soft_message_id, trash_id)]
    );
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[hard_message_id]
    );
    {
        let canonical = canonical_emails.lock().unwrap();
        let soft_deleted = canonical
            .iter()
            .find(|email| email.id == soft_message_id)
            .expect("soft-deleted message is moved through canonical store");
        assert_eq!(soft_deleted.mailbox_id, trash_id);
        assert_eq!(soft_deleted.mailbox_role, "trash");
        assert!(canonical.iter().all(|email| email.id != hard_message_id));
    }
    assert_eq!(
        decode_partial_completion_response(&response_rops[8..15]),
        (0x1E, 0)
    );
    assert_eq!(
        decode_partial_completion_response(&response_rops[15..22]),
        (0x91, 0)
    );
}

#[tokio::test]
async fn mapi_over_http_delete_messages_reports_partial_only_for_mixed_delete_failure() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let deleted_message_id = Uuid::parse_str("9a9a9a91-9a91-4a91-9a91-9a9a9a9a9a91").unwrap();
    let failed_message_id = Uuid::parse_str("9a9a9a92-9a92-4a92-9a92-9a9a9a9a9a92").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &deleted_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Deleted through MAPI",
            ),
            FakeStore::email(
                &failed_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Delete failure through MAPI",
            ),
        ])),
        failed_delete_email_ids: Arc::new(Mutex::new(vec![failed_message_id])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let canonical_emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x1E, 0x00, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&deleted_message_id.to_string()),
    );
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&failed_message_id.to_string()),
    );

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
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

    assert_eq!(
        decode_partial_completion_response(&response_rops[8..15]),
        (0x1E, 1)
    );
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[deleted_message_id]
    );
    let canonical = canonical_emails.lock().unwrap();
    assert!(canonical.iter().all(|email| email.id != deleted_message_id));
    assert!(canonical.iter().any(|email| email.id == failed_message_id));
}

#[tokio::test]
async fn mapi_over_http_microsoft_delete_messages_accepts_nonzero_boolean_fields() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let first_message_id = Uuid::parse_str("9a9a9a91-9a91-4a91-9a91-9a9a9a9a9a91").unwrap();
    let second_message_id = Uuid::parse_str("9a9a9a92-9a92-4a92-9a92-9a9a9a9a9a92").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &first_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "DeleteMessages nonzero async",
            ),
            FakeStore::email(
                &second_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "DeleteMessages nonzero notify",
            ),
        ])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
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

    for (message_id, want_async, notify_non_read) in [
        (first_message_id, 0x02, 0x00),
        (second_message_id, 0x00, 0x02),
    ] {
        let mut rops = Vec::new();
        append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
        rops.extend_from_slice(&[
            0x1E,
            0x00,
            0x01, // RopDeleteMessages.
            want_async,
            notify_non_read,
        ]);
        rops.extend_from_slice(&1u16.to_le_bytes());
        append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));

        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
            )
            .await
            .unwrap();
        let response_rops = response_rops_from_execute_response(response).await;
        assert!(contains_bytes(
            &response_rops,
            &[0x1E, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
        ));
        renew_mapi_request_id(&mut execute_headers);
    }

    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(first_message_id, trash_id), (second_message_id, trash_id)]
    );
    assert!(deleted_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_delete_messages_from_trash_child_hard_deletes() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let child_id = Uuid::parse_str("78787878-7878-4787-8787-787878787878").unwrap();
    let message_id = Uuid::parse_str("89898989-8989-4989-8989-898989898989").unwrap();
    let mut child = FakeStore::mailbox(&child_id.to_string(), "", "Trash Child");
    child.parent_id = Some(trash_id);
    let child_folder_id = test_mapi_folder_id(0x2772);
    crate::mapi::identity::remember_mapi_identity(child_id, child_folder_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items"),
            child,
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &child_id.to_string(),
            "",
            "Trash child delete",
        )])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(child_id, child_folder_id);
    let moved_emails = store.moved_emails.clone();
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
    append_rop_open_folder(&mut rops, 0, 1, child_folder_id);
    rops.extend_from_slice(&[0x1E, 0x00, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x1E, 0x01, 0, 0, 0, 0, 0]));
    assert!(moved_emails.lock().unwrap().is_empty());
    assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
}

#[tokio::test]
async fn mapi_over_http_hard_delete_messages_reports_partial_when_retention_blocks_delete() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let message_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &inbox_id.to_string(),
            "inbox",
            "Retained hard delete",
        )])),
        failed_delete_email_ids: Arc::new(Mutex::new(vec![message_id])),
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
    rops.extend_from_slice(&[0x91, 0x00, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        crate::mapi::identity::legacy_migration_object_id(&message_id),
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

    assert!(contains_bytes(&response_rops, &[0x91, 0x01, 0, 0, 0, 0, 1]));
    assert!(deleted_emails.lock().unwrap().is_empty());
    assert!(canonical_emails
        .lock()
        .unwrap()
        .iter()
        .any(|email| email.id == message_id));
}

#[tokio::test]
async fn mapi_over_http_delete_messages_from_trash_reports_partial_when_retention_blocks_delete() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let message_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &trash_id.to_string(),
            "trash",
            "Retained Trash delete",
        )])),
        failed_delete_email_ids: Arc::new(Mutex::new(vec![message_id])),
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
    rops.extend_from_slice(&[0x1E, 0x00, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x1E, 0x01, 0, 0, 0, 0, 1]));
    assert!(deleted_emails.lock().unwrap().is_empty());
    assert!(canonical_emails
        .lock()
        .unwrap()
        .iter()
        .any(|email| email.id == message_id));
}

#[tokio::test]
async fn mapi_over_http_open_message_uses_targeted_store_lookup() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let target = FakeStore::email(
        "87878787-8787-8787-8787-878787878787",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Targeted open",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            target.clone(),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Unopened message",
            ),
        ])),
        ..Default::default()
    };
    let queried_jmap_email_ids = store.queried_jmap_email_ids.clone();
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
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(&target.id.to_string()),
    );
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &utf16z("Targeted open")));
    assert_eq!(queried_jmap_email_ids.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mapi_over_http_save_changes_attachment_rejects_conflicting_microsoft_save_flags_without_batch_drift(
) {
    let message_id = "34343434-3434-3434-3434-343434343436";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Invalid attachment save flags message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
        0x25, 0x00, 0x02, 0x03, 0x03, // RopSaveChangesAttachment with conflicting SaveFlags.
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ]);

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
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x23, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x25, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_create_inline_attachment_preserves_content_id_metadata() {
    let message_id = "37373737-3737-3737-3737-373737373738";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let canonical_emails = Arc::new(Mutex::new(vec![FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MAPI inline attachment message",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: canonical_emails,
        created_attachments: created_attachments.clone(),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::png(), 0.8));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_i32_property(&mut property_values, 0x3705_0003, 1);
    append_mapi_i32_property(&mut property_values, 0x370B_0003, -1);
    append_mapi_i32_property(&mut property_values, 0x7FFD_0003, 0);
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "image001.PNG");
    append_mapi_utf16_property(
        &mut property_values,
        0x3712_001F,
        "image001.PNG@01C86E1C.F1954390",
    );
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "image/png");
    append_mapi_i32_property(&mut property_values, 0x7FFA_0003, 0);
    append_mapi_i32_property(&mut property_values, 0x3714_0003, 4);
    append_mapi_bool_property(&mut property_values, 0x7FFE_000B, true);
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "image001.PNG");
    append_mapi_utf16_property(&mut property_values, 0x3704_001F, "image001.PNG");
    append_mapi_utf16_property(&mut property_values, 0x3703_001F, ".PNG");
    append_mapi_binary_property(&mut property_values, 0x3701_0102, b"\x89PNG-mapi");

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x03, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[0x23, 0x00, 0x02, 0x03, 0x0A, 0x00, 0x03]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&13u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x25, 0x00, 0x02, 0x03, 0x00]);

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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x23, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "image001.PNG");
    assert_eq!(created[0].media_type, "image/png");
    assert_eq!(created[0].disposition.as_deref(), Some("inline"));
    assert_eq!(
        created[0].content_id.as_deref(),
        Some("image001.PNG@01C86E1C.F1954390")
    );
    assert_eq!(created[0].blob_bytes, b"\x89PNG-mapi");
}

#[tokio::test]
async fn mapi_over_http_microsoft_delete_attachment_is_committed_by_save_message() {
    let message_id = "39393939-3939-3939-3939-393939393939";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment delete message",
    );
    email.has_attachments = true;
    let attachments = Arc::new(Mutex::new(HashMap::from([(
        message_uuid,
        vec![ActiveSyncAttachment {
            id: attachment_id,
            message_id: message_uuid,
            file_name: "delete.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            disposition: None,
            content_id: None,
            size_octets: 11,
            file_reference: format!("attachment:{message_uuid}:{attachment_id}"),
        }],
    )])));
    let canonical_emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: canonical_emails.clone(),
        attachments: attachments.clone(),
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
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x24, 0x00, 0x02, // RopDeleteAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());

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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x24, 0x02, 0, 0, 0, 0]));
    assert_eq!(attachments.lock().unwrap()[&message_uuid].len(), 1);
    assert!(
        canonical_emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == message_uuid)
            .expect("message remains canonical after pending attachment delete")
            .has_attachments
    );

    let mut save_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut save_rops, test_mapi_folder_id(5));
    save_rops.push(0);
    save_rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    save_rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut save_rops, test_mapi_folder_id(5));
    save_rops.push(0);
    append_mapi_wire_id(&mut save_rops, test_mapi_message_id(message_id));
    append_rop_save_changes_message(&mut save_rops, 2, 2);

    renew_mapi_request_id(&mut execute_headers);
    let save_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(save_response.status(), StatusCode::OK);
    let save_response_rops = response_rops_from_execute_response(save_response).await;
    assert!(contains_bytes(
        &save_response_rops,
        &[0x0C, 0x02, 0, 0, 0, 0]
    ));
    assert!(attachments.lock().unwrap()[&message_uuid].is_empty());
    let canonical = canonical_emails.lock().unwrap();
    assert!(
        !canonical
            .iter()
            .find(|email| email.id == message_uuid)
            .expect("message remains canonical after saved attachment delete")
            .has_attachments
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode() {
    let message_id = "41414141-4141-4141-4141-414141414141";
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Embedded parent",
    );
    let canonical_emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: canonical_emails.clone(),
        created_attachments: created_attachments.clone(),
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

    let mut attachment_properties = Vec::new();
    append_mapi_i32_property(&mut attachment_properties, 0x3705_0003, 5);
    append_mapi_utf16_property(
        &mut attachment_properties,
        0x3707_001F,
        "embedded-child.msg",
    );
    let mut embedded_properties = Vec::new();
    append_mapi_utf16_property(&mut embedded_properties, 0x0037_001F, "Embedded child");
    append_mapi_utf16_property(&mut embedded_properties, 0x1000_001F, "Embedded body");
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
    ]);
    append_rop_set_properties(&mut rops, 3, 1, &attachment_properties);
    rops.extend_from_slice(&[
        0x46, 0x00, 0x03, 0x04, // RopOpenEmbeddedMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.push(0x00);
    append_rop_set_properties(&mut rops, 4, 1, &embedded_properties);
    append_rop_save_changes_message(&mut rops, 4, 4);
    rops.extend_from_slice(&[
        0x25, 0x00, 0x03, 0x03, 0x0A, // RopSaveChangesAttachment KeepOpenReadWrite
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert!(contains_bytes(&response_rops, &[0x46, 0x04, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x04, 0, 0, 0, 0, 0x04]
    ));
    assert!(
        contains_bytes(&response_rops, &[0x25, 0x03, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    let canonical = canonical_emails.lock().unwrap();
    assert_eq!(canonical.len(), 1);
    assert_eq!(canonical[0].subject, "Embedded parent");
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "Embedded child.msg");
    assert_eq!(created[0].media_type, "application/vnd.ms-outlook");
    assert!(created[0]
        .blob_bytes
        .windows(b"Subject:Embedded child".len())
        .any(|window| window == b"Subject:Embedded child"));
}

#[tokio::test]
async fn mapi_over_http_get_valid_attachments_lists_canonical_attachment_numbers() {
    let message_id = "40404040-4040-4040-4040-404040404040";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let first_attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let second_attachment_id = Uuid::parse_str("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Valid attachments message",
    );
    email.has_attachments = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![
                ActiveSyncAttachment {
                    id: first_attachment_id,
                    message_id: message_uuid,
                    file_name: "first.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    disposition: None,
                    content_id: None,
                    size_octets: 11,
                    file_reference: format!("attachment:{message_uuid}:{first_attachment_id}"),
                },
                ActiveSyncAttachment {
                    id: second_attachment_id,
                    message_id: message_uuid,
                    file_name: "second.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    disposition: None,
                    content_id: None,
                    size_octets: 22,
                    file_reference: format!("attachment:{message_uuid}:{second_attachment_id}"),
                },
            ],
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x52, 0x00, 0x02, // RopGetValidAttachments
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x52, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_stale_message_handle_requires_force_save() {
    let message_id = "68686868-6868-6868-6868-686868686868";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Concurrent message transaction",
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

    let mut first_property_values = Vec::new();
    append_mapi_i32_property(&mut first_property_values, 0x0E07_0003, 1);
    let mut second_property_values = Vec::new();
    append_mapi_i32_property(&mut second_property_values, 0x1090_0003, 2);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage, first handle
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x03, // RopOpenMessage, second handle
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    append_rop_set_properties(&mut rops, 2, 1, &first_property_values);
    append_rop_save_changes_message(&mut rops, 2, 2);
    append_rop_set_properties(&mut rops, 3, 1, &second_property_values);
    append_rop_save_changes_message(&mut rops, 3, 3);

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
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
    assert!(contains_bytes(
        response_rops,
        &[0x0C, 0x02, 0, 0, 0, 0, 0x02]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x0C, 0x03, 0x09, 0x01, 0x04, 0x80]
    ));
    let staged = emails.lock().unwrap()[0].clone();
    assert!(!staged.unread);
    assert!(!staged.flagged);

    let handle_table = &response_rop_buffer[2 + response_rop_size..];
    let folder_handle = u32::from_le_bytes(handle_table[4..8].try_into().unwrap());
    let first_message_handle = u32::from_le_bytes(handle_table[8..12].try_into().unwrap());
    let second_message_handle = u32::from_le_bytes(handle_table[12..16].try_into().unwrap());
    let force_save_rops = vec![0x0C, 0x00, 0x03, 0x03, 0x04];
    let mut force_save_headers = mapi_headers("Execute");
    force_save_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let force_save_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &force_save_headers,
            &execute_body(&rop_buffer(
                &force_save_rops,
                &[
                    1,
                    folder_handle,
                    first_message_handle,
                    second_message_handle,
                ],
            )),
        )
        .await
        .unwrap();

    assert_eq!(force_save_response.status(), StatusCode::OK);
    let force_save_response_rops = response_rops_from_execute_response(force_save_response).await;
    assert!(contains_bytes(
        &force_save_response_rops,
        &[0x0C, 0x03, 0, 0, 0, 0, 0x03]
    ));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfxics_4_3_2_partial_item_download_uses_full_item_fallback() {
    let mailbox_id = "55555555-5555-4555-9555-555555555506";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        "48484848-4848-4848-8848-484848484849",
        mailbox_id,
        "inbox",
        "MS-OXCFXICS 4.3.2 fallback",
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
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x10, 0x00, 0x00, // content sync, PartialItem SendOptions
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
    ]);
    for tag in [
        META_TAG_IDSET_GIVEN,
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
    assert!(contains_bytes(&response_rops, &[0x70, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x4E, 0x02, 0, 0, 0, 0]));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].subject,
        "MS-OXCFXICS 4.3.2 fallback"
    );
    assert!(contains_bytes(
        &response_rops,
        &META_TAG_IDSET_GIVEN.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &META_TAG_CNSET_SEEN.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &META_TAG_CNSET_SEEN_FAI.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &META_TAG_CNSET_READ.to_le_bytes()
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
async fn mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut class_match = vec![0x04, 0x04];
    class_match.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    append_mapi_utf16_property(
        &mut class_match,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.FolderDesign.NamedView",
    );
    let mut version_match = vec![0x04, 0x04];
    version_match.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
    append_mapi_i32_property(&mut version_match, PID_TAG_VIEW_DESCRIPTOR_VERSION, 8);
    let mut name_match = vec![0x04, 0x04];
    name_match.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_NAME_W.to_le_bytes());
    append_mapi_utf16_property(&mut name_match, PID_TAG_VIEW_DESCRIPTOR_NAME_W, "Compact");
    let mut restriction = vec![0x00];
    restriction.extend_from_slice(&3u16.to_le_bytes());
    restriction.extend_from_slice(&class_match);
    restriction.extend_from_slice(&version_match);
    restriction.extend_from_slice(&name_match);

    let descriptor = b"MS-OXOCFG test view descriptor";
    let descriptor_strings =
        utf16z("\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nFrom\nSubject\nReceived\nSize\nCategories\n");
    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x02, // RopGetContentsTable, Associated.
        0x12, 0x00, 0x02, 0x00, // RopSetColumns.
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_FOLDER_ID.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_MID.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_NAME_W.to_le_bytes());
    rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable.
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_VERSION.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_NAME_W.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x4F, 0x00, 0x02, 0x00]); // RopFindRow.
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, 0x01, 0x00, // RopQueryRows, one row.
    ]);
    append_rop_open_message(
        &mut rops,
        1,
        3,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID,
    );
    rops.extend_from_slice(&[0x2B, 0x00, 0x03, 0x04]); // RopOpenStream.
    rops.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_BINARY.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x2F, 0x00, 0x04]); // RopSetStreamSize.
    rops.extend_from_slice(&(descriptor.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[0x2D, 0x00, 0x04]); // RopWriteStream.
    rops.extend_from_slice(&(descriptor.len() as u16).to_le_bytes());
    rops.extend_from_slice(descriptor);
    rops.extend_from_slice(&[0x5D, 0x00, 0x04]); // RopCommitStream.
    rops.extend_from_slice(&[0x2B, 0x00, 0x03, 0x05]); // RopOpenStream.
    rops.extend_from_slice(&PID_TAG_VIEW_DESCRIPTOR_STRINGS_W.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x2F, 0x00, 0x05]); // RopSetStreamSize.
    rops.extend_from_slice(&(descriptor_strings.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[0x2D, 0x00, 0x05]); // RopWriteStream.
    rops.extend_from_slice(&(descriptor_strings.len() as u16).to_le_bytes());
    rops.extend_from_slice(&descriptor_strings);
    rops.extend_from_slice(&[0x5D, 0x00, 0x05]); // RopCommitStream.
    append_rop_save_changes_message(&mut rops, 3, 3);

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

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x2B, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x2D, 0x04, 0, 0, 0, 0, descriptor.len() as u8, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x5D, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x2B, 0x05, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x2D, 0x05, 0, 0, 0, 0, descriptor_strings.len() as u8, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x5D, 0x05, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x03, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxocfg_default_named_views_expose_descriptor_columns() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut class_match = vec![0x04, 0x04];
    class_match.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    append_mapi_utf16_property(
        &mut class_match,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.FolderDesign.NamedView",
    );

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x02, // RopGetContentsTable, Associated.
        0x12, 0x00, 0x02, 0x00, // RopSetColumns.
    ]);
    rops.extend_from_slice(&6u16.to_le_bytes());
    for tag in [
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_VIEW_DESCRIPTOR_NAME_W,
        PID_TAG_VIEW_DESCRIPTOR_VERSION,
        PID_TAG_VIEW_DESCRIPTOR_BINARY,
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
        PID_TAG_MESSAGE_SIZE,
    ] {
        rops.extend_from_slice(&tag.to_le_bytes());
    }
    rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]); // RopRestrict.
    rops.extend_from_slice(&(class_match.len() as u16).to_le_bytes());
    rops.extend_from_slice(&class_match);
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows.
    rops.extend_from_slice(&4u16.to_le_bytes());

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
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x14, 0x02, 0, 0, 0, 0]));
    assert!(
        contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0x02, 0]),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.FolderDesign.NamedView")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Compact")));
    assert!(contains_bytes(&response_rops, &utf16z("Sent To")));
    assert!(contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nFrom\nSubject\nReceived\nSize\nCategories\n")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("\nImportance\nReminder\nIcon\nFlag Status\nAttachment\nTo\nSubject\nSent\nSize\nCategories\n")
    ));
}

#[tokio::test]
async fn mapi_over_http_common_views_delete_messages_deletes_navigation_shortcut() {
    let account = FakeStore::account();
    let shortcut_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let shortcut_mapi_id = test_mapi_uuid_id(&shortcut_id);
    let store = FakeStore {
        session: Some(account.clone()),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(shortcut_id, shortcut_mapi_id)]))),
        navigation_shortcuts: Arc::new(Mutex::new(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id: account.account_id,
                subject: "Shortcut".to_string(),
                target_folder_id: Some(crate::mapi::identity::INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 0,
                ordinal: 1,
                group_header_id: None,
                group_name: "Mail".to_string(),
            },
        ])),
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

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_delete_messages(&mut rops, 1, &[shortcut_mapi_id]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x1E, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(shortcuts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads() {
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

    let group_id = [0x22; 16];
    let mut group_values = Vec::new();
    append_mapi_utf16_property(&mut group_values, PID_TAG_SUBJECT_W, "Projects");
    append_mapi_guid_property(&mut group_values, PID_TAG_WLINK_GROUP_HEADER_ID, group_id);
    append_mapi_i32_property(&mut group_values, PID_TAG_WLINK_TYPE, 4);
    append_mapi_binary_property(&mut group_values, PID_TAG_WLINK_ORDINAL, &[0x90]);

    let inbox_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    )
    .unwrap();
    let mut link_values = Vec::new();
    append_mapi_utf16_property(&mut link_values, PID_TAG_SUBJECT_W, "Project Inbox");
    append_mapi_binary_property(&mut link_values, PID_TAG_WLINK_ENTRY_ID, &inbox_entry_id);
    append_mapi_i32_property(&mut link_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_binary_property(&mut link_values, PID_TAG_WLINK_ORDINAL, &[0x91]);
    append_mapi_guid_property(&mut link_values, PID_TAG_WLINK_GROUP_CLSID, group_id);
    append_mapi_utf16_property(&mut link_values, PID_TAG_WLINK_GROUP_NAME_W, "Projects");

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 4, &group_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x01, 0x01, 0x00]);
    append_rop_create_associated_message(
        &mut rops,
        0,
        2,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 2, 6, &link_values);
    rops.extend_from_slice(&[0x0C, 0x00, 0x02, 0x02, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let stored = shortcuts.lock().unwrap().clone();
    let group = stored
        .iter()
        .find(|shortcut| shortcut.shortcut_type == 4 && shortcut.subject == "Projects")
        .expect("stored group header");
    let link = stored
        .iter()
        .find(|shortcut| shortcut.shortcut_type == 0 && shortcut.subject == "Project Inbox")
        .expect("stored linked shortcut");
    assert_eq!(group.group_header_id, Some(Uuid::from_bytes(group_id)));
    assert_eq!(link.group_header_id, group.group_header_id);
    assert_eq!(link.group_name, "Projects");
    assert_eq!(
        link.target_folder_id,
        Some(crate::mapi::identity::INBOX_FOLDER_ID)
    );

    let snapshot =
        crate::mapi_store::MapiMailStoreSnapshot::empty().with_navigation_shortcuts(stored);
    let reloaded_group = snapshot
        .navigation_shortcut_messages()
        .into_iter()
        .find(|shortcut| shortcut.subject == "Projects")
        .expect("reloaded group header");
    assert_eq!(
        reloaded_group.group_header_id,
        Some(Uuid::from_bytes(group_id))
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxocfg_navigation_shortcut_examples_round_trip() {
    let account = FakeStore::account();
    let superseded_calendar_shortcut_id = Uuid::from_u128(0xd49ca8a0_dc7c_469f_8c82_4ec37f03bdf8);
    let store = FakeStore {
        session: Some(account.clone()),
        navigation_shortcuts: Arc::new(Mutex::new(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: superseded_calendar_shortcut_id,
                account_id: account.account_id,
                subject: "Calendar".to_string(),
                target_folder_id: Some(crate::mapi::identity::CALENDAR_FOLDER_ID),
                shortcut_type: 0,
                flags: 0x0010_0000,
                save_stamp: 0x4f30_48f7,
                section: 3,
                ordinal: 127,
                group_header_id: Some(Uuid::from_u128(0xb7f00600_0000_0000_c000_000000000046)),
                group_name: "My Calendars".to_string(),
            },
        ])),
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

    let group_id = [
        0x5B, 0xA9, 0x43, 0xD8, 0xDA, 0xAA, 0x46, 0x2C, 0xA6, 0x3E, 0x91, 0x36, 0xF6, 0x5C, 0x86,
        0x81,
    ];
    let calendar_folder_type = [
        0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let calendar_record_key =
        crate::mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID);
    let store_entry_id = crate::mapi::identity::mailbox_store_object_entry_id(
        &account.email,
        &test_account_legacy_dn(&account.email),
    );

    let mut group_values = Vec::new();
    append_mapi_utf16_property(
        &mut group_values,
        0x001A_001F,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(&mut group_values, 0x0E1D_001F, "My Work Calendars");
    append_mapi_guid_property(&mut group_values, PID_TAG_WLINK_GROUP_HEADER_ID, group_id);
    append_mapi_i32_property(&mut group_values, 0x6847_0003, 0x1234_5678);
    append_mapi_i32_property(&mut group_values, PID_TAG_WLINK_TYPE, 4);
    append_mapi_i32_property(&mut group_values, 0x684A_0003, 0);
    append_mapi_binary_property(&mut group_values, PID_TAG_WLINK_ORDINAL, &[0x80]);
    append_mapi_guid_property(&mut group_values, 0x684F_0048, calendar_folder_type);
    append_mapi_i32_property(&mut group_values, 0x6852_0003, 3);

    let mut link_values = Vec::new();
    append_mapi_utf16_property(
        &mut link_values,
        0x001A_001F,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(&mut link_values, 0x0E1D_001F, "Meetings");
    append_mapi_i32_property(&mut link_values, 0x6847_0003, 0x1234_5678);
    append_mapi_i32_property(&mut link_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(&mut link_values, 0x684A_0003, 0);
    append_mapi_binary_property(&mut link_values, PID_TAG_WLINK_ORDINAL, &[0x80]);
    append_mapi_binary_property(&mut link_values, PID_TAG_WLINK_ENTRY_ID, &calendar_entry_id);
    append_mapi_binary_property(&mut link_values, 0x684D_0102, &calendar_record_key);
    append_mapi_binary_property(&mut link_values, 0x684E_0102, &store_entry_id);
    append_mapi_guid_property(&mut link_values, 0x684F_0048, calendar_folder_type);
    append_mapi_guid_property(&mut link_values, PID_TAG_WLINK_GROUP_CLSID, group_id);
    append_mapi_utf16_property(
        &mut link_values,
        PID_TAG_WLINK_GROUP_NAME_W,
        "My Work Calendars",
    );
    append_mapi_i32_property(&mut link_values, 0x6852_0003, 3);

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 9, &group_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_create_associated_message(
        &mut rops,
        0,
        2,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 2, 13, &link_values);
    append_rop_save_changes_message(&mut rops, 2, 2);

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
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x02, 0, 0, 0, 0]));

    let stored = shortcuts.lock().unwrap().clone();
    let group = stored
        .iter()
        .find(|shortcut| shortcut.shortcut_type == 4 && shortcut.subject == "My Work Calendars")
        .expect("MS-OXOCFG group header");
    let link = stored
        .iter()
        .find(|shortcut| shortcut.shortcut_type == 0 && shortcut.subject == "Meetings")
        .expect("MS-OXOCFG navigation shortcut");
    assert_ne!(
        link.id, superseded_calendar_shortcut_id,
        "RopCreateMessage must not return the MID of the Common Views row already read by Outlook"
    );
    assert_eq!(
        stored
            .iter()
            .filter(|shortcut| {
                shortcut.target_folder_id == Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    && shortcut.shortcut_type == 0
                    && shortcut.section == 3
            })
            .count(),
        1,
        "the new Calendar shortcut supersedes the old logical WLink"
    );

    assert_eq!(group.group_header_id, Some(Uuid::from_bytes(group_id)));
    assert_eq!(group.save_stamp, 0x1234_5678);
    assert_eq!(group.flags, 0);
    assert_eq!(group.section, 3);
    assert_eq!(group.ordinal, 0x80);
    assert_eq!(group.target_folder_id, None);
    assert_eq!(
        link.target_folder_id,
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    assert_eq!(link.group_header_id, group.group_header_id);
    assert_eq!(link.group_name, "My Work Calendars");
    assert_eq!(link.save_stamp, 0x1234_5678);
    assert_eq!(link.flags, 0);
    assert_eq!(link.section, 3);
    assert_eq!(link.ordinal, 0x80);

    let snapshot =
        crate::mapi_store::MapiMailStoreSnapshot::empty().with_navigation_shortcuts(stored);
    let reloaded = snapshot.navigation_shortcut_messages();
    assert!(reloaded.iter().any(|shortcut| {
        shortcut.shortcut_type == 4 && shortcut.subject == "My Work Calendars"
    }));
    assert!(reloaded.iter().any(|shortcut| {
        shortcut.shortcut_type == 0
            && shortcut.subject == "Meetings"
            && shortcut.group_header_id == Some(Uuid::from_bytes(group_id))
    }));
}

#[tokio::test]
async fn mapi_over_http_ics_transient_deleted_read_and_unread_sets_use_replid_encoding() {
    let inbox_id = Uuid::parse_str("52525252-5252-4525-9252-525252525202").unwrap();
    let read_id = Uuid::parse_str("65656565-6565-4565-9565-656565656502").unwrap();
    let unread_id = Uuid::parse_str("66666666-6666-4666-9666-666666666602").unwrap();
    let deleted_id = Uuid::parse_str("67676767-6767-4767-9767-676767676702").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    inbox.unread_emails = 1;
    let mut read = FakeStore::email(
        &read_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Read transient state",
    );
    read.unread = false;
    read.mailbox_states[0].unread = false;
    let mut unread = FakeStore::email(
        &unread_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Unread transient state",
    );
    unread.unread = true;
    unread.mailbox_states[0].unread = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![read, unread])),
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
        changed_message_ids: vec![read_id, unread_id],
        deleted_message_ids: vec![deleted_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    for value in [
        stream.deleted_idset.as_deref().unwrap(),
        stream.read_idset.as_deref().unwrap(),
        stream.unread_idset.as_deref().unwrap(),
    ] {
        strict_validate_replid_globset(value).unwrap();
        assert!(strict_validate_replguid_globset(value).is_err());
    }
    assert!(contains_bytes(
        &response_rops,
        &mapi_deleted_message_idset_property(&[deleted_id])
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_read_message_idset_property(&[read_id])
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_unread_message_idset_property(&[unread_id])
    ));
}

#[tokio::test]
async fn mapi_over_http_ics_client_state_controls_baseline_versus_delta_selection() {
    let inbox_id = Uuid::parse_str("52525252-5252-4525-9252-525252525203").unwrap();
    let unchanged_id = Uuid::parse_str("64646464-6464-4646-9646-646464646403").unwrap();
    let changed_id = Uuid::parse_str("65656565-6565-4565-9565-656565656503").unwrap();
    let deleted_id = Uuid::parse_str("67676767-6767-4767-9767-676767676703").unwrap();
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
                "Client state baseline unchanged",
            ),
            FakeStore::email(
                &changed_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Client state delta changed",
            ),
        ])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            30,
            40,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 31,
        current_modseq: 41,
        changed_message_ids: vec![changed_id],
        deleted_message_ids: vec![deleted_id],
        ..Default::default()
    };

    let baseline_rops = content_sync_response_rops(store.clone(), 5, &[]).await;
    assert_eq!(mapi_sync_manifest_counts(&baseline_rops), Some((0, 2)));
    assert!(contains_bytes(
        &baseline_rops,
        b"Client state baseline unchanged"
    ));
    assert!(contains_bytes(
        &baseline_rops,
        b"Client state delta changed"
    ));
    assert!(!contains_bytes(
        &baseline_rops,
        &mapi_deleted_message_idset_property(&[deleted_id])
    ));

    let delta_rops = content_sync_response_rops(store, 5, b"client-content-state").await;
    assert_eq!(mapi_sync_manifest_counts(&delta_rops), Some((0, 1)));
    assert!(!contains_bytes(
        &delta_rops,
        b"Client state baseline unchanged"
    ));
    assert!(contains_bytes(&delta_rops, b"Client state delta changed"));
    assert!(contains_bytes(
        &delta_rops,
        &mapi_deleted_message_idset_property(&[deleted_id])
    ));
}

#[tokio::test]
async fn mapi_over_http_open_conversation_history_requires_real_mailbox() {
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
        crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
    );

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_real_conversation_history_open_props_contents_and_notifications_succeed() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "73737373-7373-4373-8373-737373737373",
            "conversation_history",
            "Conversation History",
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
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
    );
    append_rop_get_properties_specific(
        &mut rops,
        1,
        &[
            0x3601_0003, // PidTagFolderType
            0x6749_0014, // PidTagParentFolderId
            0x0FF4_0003, // PidTagAccess
            0x6639_0003, // PidTagRights
            0x3001_001F, // PidTagDisplayName
            0x3602_0003, // PidTagContentCount
            0x3603_0003, // PidTagContentUnreadCount
            0x360A_000B, // PidTagSubfolders
            0x3613_001F, // PidTagContainerClass
            0x36E5_001F, // PidTagDefaultPostMessageClass
            0x36DA_0102, // PidTagExtendedFolderFlags
        ],
    );
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x03]); // RopRegisterNotification
    rops.extend_from_slice(&0x0401u16.to_le_bytes());
    rops.push(0);
    rops.push(0);
    append_mapi_wire_id(
        &mut rops,
        crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
    );
    rops.extend_from_slice(&0u64.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("Conversation History GetProps should return a standard row");
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Conversation History")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Note")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Note")));
    assert!(contains_bytes(
        &response_rops,
        &mapi_wire_id_bytes(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    ));
    assert!(contains_bytes(&response_rops, &[0x05, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x05, 0x02, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x29, 0x03, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_messages_filters_to_requested_canonical_messages() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let selected_id = "44444444-4444-4444-4444-444444444444";
    let other_id = "45454545-4545-4545-4545-454545454545";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(selected_id, inbox_id, "inbox", "Selected FastTransfer"),
            FakeStore::email(other_id, inbox_id, "inbox", "Unrequested FastTransfer"),
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

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x4B, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(selected_id));
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
    assert!(contains_bytes(&response_rops, &[0x4B, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x400C_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x400D_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Selected FastTransfer")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Unrequested FastTransfer")
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_destination_put_buffer_extended_is_parseable() {
    let mut transfer_data = Vec::new();
    let subject = utf16z("FastTransfer extended subject");
    transfer_data.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    transfer_data.extend_from_slice(&(subject.len() as u32).to_le_bytes());
    transfer_data.extend_from_slice(&subject);

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x53, 0x00, 0x02, 0x03, 0x01, 0x00]);
    rops.extend_from_slice(&[0x9D, 0x00, 0x03]);
    rops.extend_from_slice(&(transfer_data.len() as u16).to_le_bytes());
    rops.extend_from_slice(&transfer_data);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x9D, 0x03, 0, 0, 0, 0, transfer_data.len() as u8, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_destination_rejects_wrong_target_handle() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x53, 0x00, 0x01, 0x02, 0x01, 0x00]);
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // The batch stays aligned after rejection.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x53, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_modify_rules_writes_bounded_canonical_sieve_rule() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let active_sieve = store.active_sieve_script.clone();
    let mailbox_rules = store.mailbox_rules.clone();
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
    rops.extend_from_slice(&[0x41, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&3u16.to_le_bytes());
    append_mapi_utf16_property(&mut rops, 0x6682_001F, "Move invoices");
    rops.extend_from_slice(&0x6677_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    let provider_data = serde_json::json!({
        "condition": {"kind": "subjectContains", "value": "invoice"},
        "actions": [{"type": "move", "folder": "Invoices"}],
        "stopProcessing": true
    })
    .to_string();
    rops.extend_from_slice(&0x6684_0102u32.to_le_bytes());
    rops.extend_from_slice(&(provider_data.len() as u16).to_le_bytes());
    rops.extend_from_slice(provider_data.as_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x41, 0x01, 0, 0, 0, 0]));
    let sieve = active_sieve.lock().unwrap().clone().unwrap();
    assert!(sieve.contains(r#"header :contains "Subject" "invoice""#));
    assert!(sieve.contains(r#"fileinto "Invoices";"#));
    assert!(sieve.contains("stop;"));
    assert_eq!(mailbox_rules.lock().unwrap()[0].name, "Move invoices");
}

#[tokio::test]
async fn mapi_over_http_modify_rules_accepts_bounded_sieve_actions() {
    let cases = [
        (
            "Delete reports",
            serde_json::json!({
                "condition": {"kind": "subjectContains", "value": "report"},
                "actions": [{"type": "delete"}]
            }),
            ["discard;", ""],
        ),
        (
            "Forward alerts",
            serde_json::json!({
                "condition": {"kind": "fromContains", "value": "alerts@example.test"},
                "actions": [{"type": "forward", "address": "ops@example.test"}]
            }),
            [
                r#"address :contains "From" "alerts@example.test""#,
                r#"redirect "ops@example.test";"#,
            ],
        ),
        (
            "Redirect alerts",
            serde_json::json!({
                "condition": {"kind": "fromContains", "value": "alerts@example.test"},
                "actions": [{"type": "redirect", "address": "archive@example.test"}]
            }),
            [
                r#"address :contains "From" "alerts@example.test""#,
                r#"redirect "archive@example.test";"#,
            ],
        ),
        (
            "Mark read",
            serde_json::json!({
                "condition": {"kind": "subjectContains", "value": "notice"},
                "actions": [{"type": "markRead"}]
            }),
            [r#"header :contains "Subject" "notice""#, "keep;"],
        ),
        (
            "Stop processing",
            serde_json::json!({
                "condition": {"kind": "always"},
                "actions": [{"type": "delete"}],
                "stopProcessing": true
            }),
            ["discard;", "stop;"],
        ),
    ];

    for (name, provider_data, expected_fragments) in cases {
        let (response_rops, sieve) = modify_rules_response(name, provider_data).await;
        assert!(
            contains_bytes(&response_rops, &[0x41, 0x01, 0, 0, 0, 0]),
            "{name}: {response_rops:02x?}"
        );
        let sieve = sieve.expect(name);
        for fragment in expected_fragments {
            if !fragment.is_empty() {
                assert!(sieve.contains(fragment), "{name}: {sieve}");
            }
        }
    }
}

#[tokio::test]
async fn mapi_over_http_modify_rules_rejects_exchange_rule_blobs() {
    let cases = [
        ("Client-only", serde_json::json!({"clientOnly": true})),
        ("Delegate", serde_json::json!({"delegate": true})),
        (
            "Delegate template",
            serde_json::json!({"delegateTemplate": {"messageClass": "IPM.Rule.Version2.Message"}}),
        ),
        ("Deferred", serde_json::json!({"deferredAction": true})),
        (
            "Exchange blob",
            serde_json::json!({"exchangeBlob": "AQIDBA=="}),
        ),
        (
            "Provider predicate",
            serde_json::json!({
                "condition": {"kind": "providerPredicate", "value": "x"},
                "actions": [{"type": "delete"}]
            }),
        ),
        (
            "Unknown action",
            serde_json::json!({
                "condition": {"kind": "always"},
                "actions": [{"type": "defer"}]
            }),
        ),
    ];

    for (name, provider_data) in cases {
        let (response_rops, sieve) = modify_rules_response(name, provider_data).await;
        assert!(
            contains_bytes(&response_rops, &[0x41, 0x01, 0x02, 0x01, 0x04, 0x80]),
            "{name}: {response_rops:02x?}"
        );
        assert!(sieve.is_none(), "{name}: {sieve:?}");
    }
}

#[tokio::test]
async fn mapi_over_http_update_deferred_action_messages_rejects_without_sieve_side_effect() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let active_sieve = store.active_sieve_script.clone();
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
    rops.extend_from_slice(&[0x57, 0x00, 0x01]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(b"SRVR");
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(b"CLIENT");
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
    assert!(contains_bytes(
        &response_rops,
        &[0x57, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(active_sieve.lock().unwrap().is_none());
}

#[tokio::test]
async fn mapi_over_http_search_criteria_rops_return_rop_specific_protocol_errors() {
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
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(11));
    rops.push(0);
    rops.extend_from_slice(&[0x30, 0x00, 0x01]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(&0x0004_0002u32.to_le_bytes());
    rops.extend_from_slice(&[0x31, 0x00, 0x01, 0x01, 0x01, 0x01]);

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
    assert!(contains_bytes(
        &response_rops,
        &[0x30, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x31, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_register_notification_returns_protocol_success_handles() {
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
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0001u16.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x03]);
    rops.extend_from_slice(&0x0401u16.to_le_bytes());
    rops.push(0);
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x04]);
    rops.extend_from_slice(&0xffffu16.to_le_bytes());
    rops.push(0);
    rops.push(1);

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
    assert!(contains_bytes(&response_rops, &[0x29, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x29, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x29, 0x04, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_notification_wait_reports_content_event_after_registered_delete() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "99999999-9999-9999-9999-999999999999";
    let mut inbox = FakeStore::mailbox(inbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "Notification target",
        )])),
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
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0008u16.to_le_bytes());
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&[0x1E, 0x00, 0x01, 0x00, 0x01]);
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

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 1);
    assert_eq!(u16::from_le_bytes(body[16..18].try_into().unwrap()), 0x0100);
    assert_eq!(body[18], 1);
    assert!(contains_bytes(
        &body,
        &mapi_wire_id_bytes(test_mapi_folder_id(5))
    ));
}

#[tokio::test]
async fn mapi_over_http_notification_wait_reports_content_event_after_registered_save() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Notification save");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Notification body");

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0004u16.to_le_bytes());
    rops.push(1);
    append_rop_create_message(&mut rops, 1, 3, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 3, 2, &property_values);
    append_rop_save_changes_message(&mut rops, 3, 3);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(imported_emails.lock().unwrap().len(), 1);

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 1);
    assert_eq!(u16::from_le_bytes(body[16..18].try_into().unwrap()), 0x0100);
    assert_eq!(body[18], 1);
    assert!(contains_bytes(
        &body,
        &mapi_wire_id_bytes(test_mapi_folder_id(5))
    ));
}

#[tokio::test]
async fn mapi_over_http_notification_wait_polls_canonical_change_cursor() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        mapi_notification_polls: Arc::new(Mutex::new(vec![MapiNotificationPoll {
            event_pending: true,
            cursor: Some(8),
            events: Vec::new(),
        }])),
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
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0010u16.to_le_bytes());
    rops.push(1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 0);
}

#[tokio::test]
async fn mapi_over_http_notification_wait_serializes_canonical_change_details() {
    let folder_id = test_mapi_folder_id(5);
    let message_id = test_mapi_message_id("99999999-9999-9999-9999-999999999999");
    let source_folder_id = test_mapi_folder_id(14);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        mapi_notification_polls: Arc::new(Mutex::new(vec![MapiNotificationPoll {
            event_pending: true,
            cursor: Some(8),
            events: vec![
                crate::mapi::notifications::MapiNotificationEvent::canonical(
                    crate::mapi::notifications::MapiNotificationKind::Content,
                    0x0020,
                    folder_id,
                    Some(message_id),
                    Some(source_folder_id),
                    8,
                    44,
                    Some(12),
                    Some(3),
                    "moved".to_string(),
                    Some("Archive".to_string()),
                    Some("Inbox".to_string()),
                    Some("quarterly report".to_string()),
                ),
            ],
        }])),
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
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0020u16.to_le_bytes());
    rops.push(1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 1);
    assert_eq!(u16::from_le_bytes(body[16..18].try_into().unwrap()), 0x0020);
    assert_eq!(body[18], 1);
    assert_eq!(body[19], 0b0000_1111);
    assert_eq!(&body[20..28], &mapi_wire_id_bytes(folder_id));
    assert_eq!(&body[28..36], &mapi_wire_id_bytes(message_id));
    assert_eq!(&body[36..44], &mapi_wire_id_bytes(source_folder_id));
    assert_eq!(u64::from_le_bytes(body[44..52].try_into().unwrap()), 8);
    assert_eq!(u64::from_le_bytes(body[52..60].try_into().unwrap()), 44);
    assert_eq!(u32::from_le_bytes(body[60..64].try_into().unwrap()), 12);
    assert_eq!(u32::from_le_bytes(body[64..68].try_into().unwrap()), 3);
    let details = notification_detail_strings(&body[68..]);
    assert_eq!(
        details,
        vec![
            "mailbox_message",
            "moved",
            "Archive",
            "Inbox",
            "quarterly report"
        ]
    );
}

#[tokio::test]
async fn mapi_over_http_long_term_id_round_trips_canonical_replica_ids() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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

    let object_id = test_mapi_folder_id(5);
    let mut long_term_id = [0; 24];
    long_term_id[..16].copy_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    long_term_id[16..22].copy_from_slice(&globcnt_bytes(5));
    let mut invalid_long_term_id = long_term_id;
    invalid_long_term_id[0] ^= 0xFF;
    invalid_long_term_id[16..22].copy_from_slice(&globcnt_bytes(5_000));
    let mut account_guid_long_term_id = long_term_id;
    account_guid_long_term_id[..16].copy_from_slice(account.account_id.as_bytes());
    let mut account_guid_le_long_term_id = long_term_id;
    account_guid_le_long_term_id[..16].copy_from_slice(&account.account_id.to_bytes_le());
    let stale_calendar_long_term_id = {
        let mut value = [0xA5; 24];
        value[16..22].copy_from_slice(&globcnt_bytes(16));
        value[22..24].copy_from_slice(&[0, 0]);
        value
    };
    let stale_calendar_short_id = {
        let mut value = [0; 8];
        value[..2].copy_from_slice(&0x7777_u16.to_le_bytes());
        value[2..8].copy_from_slice(&globcnt_bytes(16));
        value
    };
    let stale_calendar_little_endian_short_id = {
        let mut value = [0; 8];
        value[..2].copy_from_slice(&0x7777_u16.to_le_bytes());
        value[2..8].copy_from_slice(&16_u64.to_le_bytes()[..6]);
        value
    };
    let dynamic_bare_little_endian_short_id = {
        let mut value = [0; 8];
        value[..6].copy_from_slice(&75_u64.to_le_bytes()[..6]);
        value
    };

    let mut rops = vec![0x43, 0x00, 0x00];
    append_mapi_wire_id(&mut rops, object_id);
    rops.extend_from_slice(&[0x43, 0x00, 0x00]);
    append_mapi_trailing_replid_wire_id(&mut rops, 5);
    rops.extend_from_slice(&[0x43, 0x00, 0x00]);
    rops.extend_from_slice(&stale_calendar_short_id);
    rops.extend_from_slice(&[0x43, 0x00, 0x00]);
    rops.extend_from_slice(&stale_calendar_little_endian_short_id);
    rops.extend_from_slice(&[0x43, 0x00, 0x00]);
    rops.extend_from_slice(&dynamic_bare_little_endian_short_id);
    rops.extend_from_slice(&[0x43, 0x00, 0x00]);
    rops.extend_from_slice(&[0; 8]);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(&long_term_id);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(&account_guid_long_term_id);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(&account_guid_le_long_term_id);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(&stale_calendar_long_term_id);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(&invalid_long_term_id);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(
        &crate::mapi::identity::long_term_id_from_object_id(
            crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        )
        .unwrap(),
    );
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(
        &crate::mapi::identity::long_term_id_from_object_id(crate::mapi::identity::VIEWS_FOLDER_ID)
            .unwrap(),
    );

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let mut long_term_response = vec![0x43, 0x00, 0, 0, 0, 0];
    long_term_response.extend_from_slice(&long_term_id);
    assert!(contains_bytes(&response_rops, &long_term_response));
    let mut trailing_replid_response = vec![0x43, 0x00, 0, 0, 0, 0];
    trailing_replid_response.extend_from_slice(&long_term_id);
    assert!(contains_bytes(&response_rops, &trailing_replid_response));
    let mut stale_short_id_response = vec![0x43, 0x00, 0, 0, 0, 0];
    stale_short_id_response.extend_from_slice(
        &crate::mapi::identity::long_term_id_from_object_id(test_mapi_folder_id(16)).unwrap(),
    );
    assert_eq!(
        response_rops
            .windows(stale_short_id_response.len())
            .filter(|window| *window == stale_short_id_response)
            .count(),
        2
    );
    let mut dynamic_short_id_response = vec![0x43, 0x00, 0, 0, 0, 0];
    dynamic_short_id_response.extend_from_slice(
        &crate::mapi::identity::long_term_id_from_object_id(test_mapi_folder_id(75)).unwrap(),
    );
    assert!(contains_bytes(&response_rops, &dynamic_short_id_response));
    assert!(contains_bytes(
        &response_rops,
        &[0x43, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x43, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
    let mut object_id_response = vec![0x44, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut object_id_response, object_id);
    assert_eq!(
        response_rops
            .windows(object_id_response.len())
            .filter(|window| *window == object_id_response)
            .count(),
        3
    );
    let mut stale_calendar_response = vec![0x44, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut stale_calendar_response, test_mapi_folder_id(16));
    assert!(contains_bytes(&response_rops, &stale_calendar_response));
    assert!(contains_bytes(
        &response_rops,
        &[0x44, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
    let mut common_views_response = vec![0x44, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(
        &mut common_views_response,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    assert!(contains_bytes(&response_rops, &common_views_response));
    let mut personal_views_response = vec![0x44, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(
        &mut personal_views_response,
        crate::mapi::identity::VIEWS_FOLDER_ID,
    );
    assert!(contains_bytes(&response_rops, &personal_views_response));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        "47474747-4747-4747-4747-474747474747",
        mailbox_id,
        "inbox",
        "Chunked FastTransfer sync message",
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

    let mut first_rops = Vec::new();
    append_rop_open_folder(&mut first_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut first_rops, 1, 2, 32);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let first_request = execute_body(&rop_buffer(&first_rops, &[1, u32::MAX, u32::MAX]));
    let first_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &first_request)
        .await
        .unwrap();

    assert_eq!(first_response.status(), StatusCode::OK);
    let first_response_rops = response_rops_from_execute_response(first_response).await;
    let first_chunks = mapi_fast_transfer_chunks(&first_response_rops);
    assert_eq!(first_chunks.len(), 1);
    assert_eq!(first_chunks[0].0, 0x0001);
    assert_eq!(first_chunks[0].1.len(), 32);

    let mut second_rops = Vec::new();
    second_rops.extend_from_slice(&[0x4E, 0x00, 0x00]);
    second_rops.extend_from_slice(&4096u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let second_request = execute_body(&rop_buffer(&second_rops, &[3]));
    let second_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &second_request)
        .await
        .unwrap();

    assert_eq!(second_response.status(), StatusCode::OK);
    let second_response_rops = response_rops_from_execute_response(second_response).await;
    let second_chunks = mapi_fast_transfer_chunks(&second_response_rops);
    assert_eq!(second_chunks.len(), 1);
    assert_eq!(second_chunks[0].0, 0x0003);

    let mut transfer = Vec::new();
    transfer.extend_from_slice(&first_chunks[0].1);
    transfer.extend_from_slice(&second_chunks[0].1);
    assert_eq!(mapi_sync_manifest_counts(&transfer), Some((0, 1)));
    assert!(contains_bytes(
        &transfer,
        b"Chunked FastTransfer sync message"
    ));
}

#[tokio::test]
async fn mapi_over_http_get_local_replica_ids_returns_replica_guid() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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
        0x7F, 0x00, 0x00, // RopGetLocalReplicaIds
    ];
    rops.extend_from_slice(&4u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(response_rops[0], 0x7F);
    assert_eq!(response_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(&response_rops[6..22], &mapi_mailstore::STORE_REPLICA_GUID);
    let (first_global_counter, _) =
        mapi_mailstore::local_replica_id_range(account.account_id, 4, 1);
    assert_eq!(&response_rops[22..28], &globcnt_bytes(first_global_counter));
    assert_eq!(response_rops.len(), 28);
    assert!(response_rops[22..28].iter().any(|byte| *byte != 0));
}

#[tokio::test]
async fn mapi_over_http_quick_step_config_0e0b_defaults_to_empty_binary() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
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
    let quick_step_config_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
    );
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
        quick_step_config_id,
    );
    append_rop_get_properties_specific(&mut rops, 2, &[0x7C08_0102, 0x0E0B_0102, 0x001A_001F]);

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
    let marker = [0x07, 0x02, 0, 0, 0, 0];
    let offset = response_rops
        .windows(marker.len())
        .position(|window| window == marker)
        .unwrap_or_else(|| panic!("missing GetPropertiesSpecific: {response_rops:02x?}"));
    assert_eq!(response_rops[offset + marker.len()], 0);
    assert!(!contains_bytes(
        &response_rops[offset..],
        &[0x0A, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops[offset..], &[0x00, 0x00]));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.CustomAction")
    ));
}

#[tokio::test]
async fn mapi_over_http_same_execute_additional_ren_junk_alias_opens_junk() {
    let account = FakeStore::account();
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let stale_junk_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 77,
    );
    let conflicts = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CONFLICTS_FOLDER_ID,
    )
    .unwrap();
    let sync_issues = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
    )
    .unwrap();
    let local_failures = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID,
    )
    .unwrap();
    let server_failures = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::SERVER_FAILURES_FOLDER_ID,
    )
    .unwrap();
    let stale_junk =
        crate::mapi::identity::folder_entry_id_from_object_id(account.account_id, stale_junk_id)
            .unwrap();
    let mut property_values = Vec::new();
    append_mapi_multi_binary_property(
        &mut property_values,
        0x36D8_1102,
        &[
            &conflicts,
            &sync_issues,
            &local_failures,
            &server_failures,
            &stale_junk,
        ],
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_set_properties(&mut rops, 1, 1, &property_values);
    append_rop_open_folder(&mut rops, 0, 2, stale_junk_id);
    append_rop_get_properties_specific(&mut rops, 2, &[0x3001_001F, 0x3613_001F]);

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
        &[0x0A, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Junk E-mail")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Note")));
}

#[tokio::test]
async fn mapi_over_http_unknown_and_reserved_rops_terminate_current_buffer() {
    for rop_id in [0xAA, 0x28] {
        let rops = [
            0x7B, 0x00, 0x00, // RopGetStoreState succeeds first.
            rop_id, 0x00, 0x00, // Unknown or reserved terminal ROP.
            0x7B, 0x00, 0x00, // Must not execute.
        ];
        let response_rops = execute_rops_response_rops(&rops, &[1]).await;

        assert_eq!(
            response_rops
                .windows(10)
                .filter(|bytes| *bytes == [0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0])
                .count(),
            1
        );
        assert!(contains_bytes(
            &response_rops,
            &[rop_id, 0x00, 0x02, 0x01, 0x04, 0x80]
        ));
    }
}

#[tokio::test]
async fn mapi_over_http_microsoft_transport_info_rops_reject_missing_input_handle_without_batch_drift(
) {
    let mut rops = vec![
        0x49, 0x00, 0x01, // RopGetAddressTypes on missing handle 1.
        0x6D, 0x00, 0x01, // RopGetTransportFolder on missing handle 1.
        0x6F, 0x00, 0x01, // RopOptionsData on missing handle 1.
    ];
    rops.extend_from_slice(b"SMTP\0");
    rops.push(0);
    rops.extend_from_slice(&[
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x49, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x6D, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x6F, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_malformed_rop_terminates_current_buffer() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // Truncated RopOpenFolder.
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1]).await;

    assert_eq!(response_rops, vec![0x00, 0x00, 0x02, 0x01, 0x04, 0x80]);
}

#[tokio::test]
async fn mapi_over_http_set_get_search_criteria_round_trips_received_date_bounds() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555502").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343497").unwrap();
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
            display_name: "Received range".to_string(),
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

    let lower_bound = "2026-05-01T00:00:00Z";
    let upper_bound = "2026-05-31T23:59:00Z";
    let mut restriction = vec![0x00];
    restriction.extend_from_slice(&2u16.to_le_bytes());
    append_search_property_i64(
        &mut restriction,
        0x0E06_0040,
        0x03,
        mapi_mailstore::filetime_from_rfc3339_utc(lower_bound) as i64,
    );
    append_search_property_i64(
        &mut restriction,
        0x0E06_0040,
        0x01,
        mapi_mailstore::filetime_from_rfc3339_utc(upper_bound) as i64,
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[test_mapi_folder_id(5)],
        0x0000_0005,
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
                    {"field": "receivedAt", "afterOrAt": lower_bound},
                    {"field": "receivedAt", "beforeOrAt": upper_bound}
                ]
            })
        );
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut get_rops = Vec::new();
    append_rop_open_folder(&mut get_rops, 0, 1, search_folder_mapi_id);
    append_rop_get_search_criteria(&mut get_rops, 1);
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
        &mapi_mailstore::filetime_from_rfc3339_utc(lower_bound).to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::filetime_from_rfc3339_utc(upper_bound).to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_set_get_search_criteria_round_trips_attachment_exists() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555503").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343495").unwrap();
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
            display_name: "Has attachments".to_string(),
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

    let mut restriction = Vec::new();
    append_search_exists(&mut restriction, 0x0E1B_000B);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[test_mapi_folder_id(5)],
        0x0000_0005,
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
                    {"field": "hasAttachment", "equals": true}
                ]
            })
        );
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut get_rops = Vec::new();
    append_rop_open_folder(&mut get_rops, 0, 1, search_folder_mapi_id);
    append_rop_get_search_criteria(&mut get_rops, 1);
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
        &0x0E1B_000Bu32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_set_get_search_criteria_round_trips_read_bitmask() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555505").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343493").unwrap();
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
            display_name: "Unread bitmask".to_string(),
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

    let mut restriction = Vec::new();
    append_search_bitmask(&mut restriction, 0x0E07_0003, false, 0x0000_0001);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[test_mapi_folder_id(5)],
        0x0000_0005,
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
                    {"field": "unread", "equals": true}
                ]
            })
        );
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut get_rops = Vec::new();
    append_rop_open_folder(&mut get_rops, 0, 1, search_folder_mapi_id);
    append_rop_get_search_criteria(&mut get_rops, 1);
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
        &0x0E69_000Bu32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_set_get_search_criteria_round_trips_string8_body_content() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555506").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343492").unwrap();
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
            display_name: "String8 body".to_string(),
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

    let mut restriction = Vec::new();
    append_search_content_string8(&mut restriction, 0x1000_001E, "plain text body");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[test_mapi_folder_id(5)],
        0x0000_0005,
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
                    {"field": "body", "contains": "plain text body"}
                ]
            })
        );
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut get_rops = Vec::new();
    append_rop_open_folder(&mut get_rops, 0, 1, search_folder_mapi_id);
    append_rop_get_search_criteria(&mut get_rops, 1);
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
    assert!(contains_bytes(&response_rops, &utf16z("plain text body")));
}

#[tokio::test]
async fn mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555504").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343494").unwrap();
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
            display_name: "Canonical supported".to_string(),
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
    restriction.extend_from_slice(&6u16.to_le_bytes());
    append_search_property_u32(&mut restriction, 0x1090_0003, 0x04, 2);
    append_search_property_multi_string(&mut restriction, 0x9000_101F, 0x04, &["Finance"]);
    append_search_property_tagged_string(&mut restriction, 0x9000_101F, 0x9000_001F, 0x04, "Legal");
    append_search_property_string(&mut restriction, 0x0C1F_001F, 0x04, "alice@example.test");
    append_search_property_string(&mut restriction, 0x1000_001F, 0x04, "quarterly report");
    append_search_content(&mut restriction, 0x1000_001F, "approval");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[test_mapi_folder_id(5)],
        0x0000_0005,
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
                    {"field": "flagged", "equals": true},
                    {"field": "category", "equals": "Finance"},
                    {"field": "category", "equals": "Legal"},
                    {"field": "sender", "equals": "alice@example.test"},
                    {"field": "body", "equals": "quarterly report"},
                    {"field": "body", "contains": "approval"}
                ]
            })
        );
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut get_rops = Vec::new();
    append_rop_open_folder(&mut get_rops, 0, 1, search_folder_mapi_id);
    append_rop_get_search_criteria(&mut get_rops, 1);
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
        &0x1090_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x9000_101Fu32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Finance")));
    assert!(contains_bytes(&response_rops, &utf16z("Legal")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("alice@example.test")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("quarterly report")));
    assert!(contains_bytes(&response_rops, &utf16z("approval")));
}

#[tokio::test]
async fn mapi_over_http_get_search_criteria_rejects_exchange_only_blob_definition() {
    let account = FakeStore::account();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343496").unwrap();
    let search_folder_mapi_id = test_mapi_uuid_id(&search_folder_id);
    crate::mapi::identity::remember_mapi_identity(search_folder_id, search_folder_mapi_id);
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: search_folder_id,
            account_id: account.account_id,
            role: "custom".to_string(),
            display_name: "Exchange blob".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"kind": "exchange_blob"}),
            restriction_json: serde_json::json!({
                "kind": "exchange_blob",
                "pidTagSearchFolderDefinition": "AQIDBA=="
            }),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        }])),
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
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_get_search_criteria(&mut rops, 1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x31, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_bind_accepts_rca_bare_guid_headers() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = mapi_headers("Bind");
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("45"),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_static("8efcc291-b798-442e-b608-bd3f6c67b78b:1"),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_static("c9a1f6bb-76d3-41a1-8abb-fc60106a4a97:1"),
    );
    let request = [0u8; 45];

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(
        response.headers().get("x-requestid").unwrap(),
        "8efcc291-b798-442e-b608-bd3f6c67b78b:1"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_get_matches_uses_complete_utf16_lookup_value() {
    let mut principal = FakeStore::account();
    principal.account_id = Uuid::parse_str("f732c3ed-7780-4011-8c67-36b9215bd913").unwrap();
    principal.email = "test@l-p-e.ch".to_string();
    principal.display_name = "test".to_string();

    let mut same_domain = FakeStore::account();
    same_domain.account_id = Uuid::parse_str("315383c4-0000-0000-0000-000000000000").unwrap();
    same_domain.email = "fabien@l-p-e.ch".to_string();
    same_domain.display_name = "Fabien".to_string();

    let store = FakeStore {
        session: Some(principal.clone()),
        directory_accounts: Arc::new(Mutex::new(vec![same_domain, principal])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = hex_bytes(
        "00000000ff000000000000000000000000000000000000000088130000e40400000904000009080000\
         0000000000ff04041f000c36ff1f000c36ff740065007300740040006c002d0070002d0065002e\
         006300680000000088130000ff0f0000001e0001301e00173a1e00083a1e00193a1e00183a\
         1e00fe391e00163a1e00003a1e0002300201ff0f0300fe0f03000039030005390201f60f\
         1e00033000000000",
    );
    let headers = nspi_bound_headers(&service, "GetMatches").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(body[8], 0);
    assert_eq!(body[9], 1);
    assert_eq!(u32::from_le_bytes(body[10..14].try_into().unwrap()), 1);
    let matched_id = u32::from_le_bytes(body[14..18].try_into().unwrap());
    assert_ne!(matched_id, 0);
    assert_eq!(matched_id & 0x8000_0000, 0x8000_0000);
    assert_ne!(matched_id, 0xedc3_32f7);
    assert_eq!(body[18], 1);
    assert!(contains_bytes(&body, &utf16z("test@l-p-e.ch")));
    assert!(!contains_bytes(&body, &utf16z("fabien@l-p-e.ch")));
    assert!(!contains_bytes(&body, &utf16z("Fabien")));
}
