use super::*;

#[tokio::test]
async fn mapi_over_http_folder_and_collector_rops_reject_wrong_live_objects() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "89898989-8989-8989-8989-898989898989";
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        message_id,
        inbox_id,
        "inbox",
        "Wrong input object",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::clone(&emails),
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

    let folder_id = crate::mapi::identity::INBOX_FOLDER_ID;
    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x04, 0x00, 0x02, 0x03, 0x00, // RopGetHierarchyTable on Message
        0x72, 0x00, 0x01, 0x04, 0x00, 0x00, 0x00, // ImportMessageChange on Folder
    ]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);

    assert_ne!(response_handles[1], u32::MAX, "folder must open");
    assert_ne!(response_handles[2], u32::MAX, "message must open");
    for (rop_id, output_index) in [(0x04, 3), (0x72, 4)] {
        assert!(contains_bytes(
            &response_rops,
            &[rop_id, output_index, 0x02, 0x01, 0x04, 0x80],
        ));
        assert_eq!(
            response_handles
                .get(usize::from(output_index))
                .copied()
                .unwrap_or(u32::MAX),
            u32::MAX,
            "a rejected ROP must not bind its output handle"
        );
    }
    assert_eq!(emails.lock().unwrap().len(), 1, "mailbox must not mutate");
}

#[tokio::test]
async fn mapi_over_http_property_mutations_reject_live_table_object() {
    let custom_properties = Arc::new(Mutex::new(HashMap::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mapi_custom_property_values: Arc::clone(&custom_properties),
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

    let custom_tag = 0x8001_001F;
    let mut custom_value = Vec::new();
    append_mapi_utf16_property(&mut custom_value, custom_tag, "must not persist");
    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x00, // RopGetHierarchyTable
    ]);
    append_rop_set_properties(&mut rops, 2, 1, &custom_value);
    append_rop_delete_properties(&mut rops, 2, &[custom_tag]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);

    assert_ne!(
        response_handles[2],
        u32::MAX,
        "hierarchy table must be live"
    );
    for rop_id in [0x0A, 0x0B] {
        assert!(contains_bytes(
            &response_rops,
            &[rop_id, 0x02, 0x02, 0x01, 0x04, 0x80],
        ));
    }
    assert!(
        custom_properties.lock().unwrap().is_empty(),
        "rejected property mutations must not touch custom storage"
    );
}

#[tokio::test]
async fn mapi_over_http_named_property_rops_reject_live_table_without_mapping() {
    let named_properties = Arc::new(Mutex::new(FakeMapiNamedProperties::default()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mapi_named_properties: Arc::clone(&named_properties),
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

    let named_header = utf16z("X-LPE-Wrong-Named-Property-Object");
    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x00, // RopGetHierarchyTable
        0x56, 0x00, 0x02, 0x02, // RopGetPropertyIdsFromNames, create missing
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&FAKE_PS_INTERNET_HEADERS_GUID);
    rops.push(named_header.len() as u8);
    rops.extend_from_slice(&named_header);
    rops.extend_from_slice(&[
        0x55, 0x00, 0x02, 0x01, 0x00, 0x01, 0x80, // RopGetNamesFromPropertyIds
        0x5F, 0x00, 0x02, 0x00, 0x00, // RopQueryNamedProperties
    ]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);

    assert_ne!(
        response_handles[2],
        u32::MAX,
        "hierarchy table must be live"
    );
    for rop_id in [0x56, 0x55, 0x5F] {
        assert!(contains_bytes(
            &response_rops,
            &[rop_id, 0x02, 0x02, 0x01, 0x04, 0x80],
        ));
    }
    let named_properties = named_properties.lock().unwrap();
    assert!(named_properties.by_property.is_empty());
    assert!(named_properties.by_id.is_empty());
}

#[tokio::test]
async fn mapi_over_http_property_copies_reject_incompatible_live_object_families() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "34343434-3434-3434-3434-343434343434";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc").unwrap();
    let custom_tag = 0x8001_001F;
    let source_key = (
        FakeStore::account().account_id,
        MapiCustomPropertyObjectKind::Attachment,
        attachment_id,
        custom_tag,
        0x001F,
    );
    let custom_properties = Arc::new(Mutex::new(HashMap::from([(
        source_key,
        utf16z("attachment only"),
    )])));
    let mut email = FakeStore::email(message_id, inbox_id, "inbox", "Copy contract");
    email.has_attachments = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: message_uuid,
                file_name: "copy-source.txt".to_string(),
                media_type: "text/plain".to_string(),
                disposition: Some("attachment".to_string()),
                content_id: None,
                size_octets: 15,
                file_reference: format!("attachment:{message_uuid}:{attachment_id}"),
            }],
        )]))),
        mapi_custom_property_values: Arc::clone(&custom_properties),
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

    let folder_id = crate::mapi::identity::INBOX_FOLDER_ID;
    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[0x22, 0x00, 0x02, 0x03, 0x00]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x39, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, // Attachment -> Message CopyTo
        0x67, 0x00, 0x03, 0x02, 0x00, 0x00, 0x00,
        0x00, // Attachment -> Message CopyProperties
    ]);

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
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);

    assert_ne!(response_handles[2], u32::MAX, "message must be live");
    assert_ne!(response_handles[3], u32::MAX, "attachment must be live");
    for rop_id in [0x39, 0x67] {
        assert!(contains_bytes(
            &response_rops,
            &[rop_id, 0x03, 0x02, 0x01, 0x04, 0x80],
        ));
    }
    let stored = custom_properties.lock().unwrap();
    assert_eq!(
        stored.len(),
        1,
        "rejected copies must not create a destination value"
    );
    assert_eq!(stored.get(&source_key), Some(&utf16z("attachment only")));
}

#[tokio::test]
async fn mapi_over_http_private_logon_rops_reject_live_message_without_cancellation() {
    let sent_id = "22222222-2222-2222-2222-222222222222";
    let message_id = "87878787-8787-8787-8787-878787878787";
    let mut email = FakeStore::email(message_id, sent_id, "sent", "Wrong logon object");
    email.delivery_status = "queued".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            sent_id, "sent", "Sent",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let cancelled_submissions = Arc::clone(&store.cancelled_submissions);
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

    let folder_id = crate::mapi::identity::SENT_FOLDER_ID;
    let mapi_message_id = test_mapi_message_id(message_id);
    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, mapi_message_id);
    rops.extend_from_slice(&[
        0x6D, 0x00, 0x02, // RopGetTransportFolder on Message
        0x47, 0x00, 0x02, // RopSetSpooler on Message
        0x48, 0x00, 0x02, // RopSpoolerLockMessage on Message
    ]);
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x51, 0x00, 0x02]); // RopTransportNewMail on Message
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    append_mapi_wire_id(&mut rops, folder_id);
    rops.extend_from_slice(b"IPM.Note\0");
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[0x57, 0x00, 0x02]); // RopUpdateDeferredActionMessages on Message
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x02);
    rops.extend_from_slice(&[0x34, 0x00, 0x02]); // RopAbortSubmit on Message
    append_mapi_wire_id(&mut rops, folder_id);
    append_mapi_wire_id(&mut rops, mapi_message_id);
    rops.extend_from_slice(&[0x7B, 0x00, 0x02]); // RopGetStoreState on Message

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);

    assert_ne!(response_handles[2], u32::MAX, "message must be live");
    for rop_id in [0x6D, 0x47, 0x48, 0x51, 0x57, 0x34, 0x7B] {
        assert!(
            contains_bytes(&response_rops, &[rop_id, 0x02, 0x02, 0x01, 0x04, 0x80],),
            "ROP {rop_id:#04x} must reject the Message handle: {response_rops:02x?}"
        );
    }
    assert!(
        cancelled_submissions.lock().unwrap().is_empty(),
        "RopAbortSubmit must reject the wrong object before cancellation"
    );
}

#[tokio::test]
async fn mapi_over_http_save_attachment_requires_and_preserves_containing_message_handle() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "85858585-8585-8585-8585-858585858585";
    let mut inbox = FakeStore::mailbox(inbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let canonical_emails = Arc::new(Mutex::new(vec![FakeStore::email(
        message_id,
        inbox_id,
        "inbox",
        "Attachment parent contract",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::clone(&canonical_emails),
        created_attachments: Arc::clone(&created_attachments),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = crate::mapi::identity::INBOX_FOLDER_ID;
    let mut attachment_properties = Vec::new();
    append_mapi_utf16_property(
        &mut attachment_properties,
        0x3707_001F,
        "parent-contract.pdf",
    );
    append_mapi_utf16_property(&mut attachment_properties, 0x370E_001F, "application/pdf");
    append_mapi_binary_property(
        &mut attachment_properties,
        0x3701_0102,
        b"%PDF-parent-contract",
    );
    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[0x23, 0x00, 0x02, 0x03]); // RopCreateAttachment
    append_rop_set_properties(&mut rops, 3, 3, &attachment_properties);
    rops.extend_from_slice(&[0x25, 0x00, 0x01, 0x03, 0x00]); // Folder response slot

    let wrong_parent_response = service
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
    assert_eq!(wrong_parent_response.status(), StatusCode::OK);
    let next_cookie = mapi_cookie_header(&wrong_parent_response);
    let body = response_bytes(wrong_parent_response).await;
    let (wrong_parent_rops, handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &wrong_parent_rops,
        &[0x25, 0x01, 0x02, 0x01, 0x04, 0x80],
    ));
    assert_ne!(handles[1], u32::MAX, "folder must be live");
    assert_ne!(handles[2], u32::MAX, "message must be live");
    assert_ne!(handles[3], u32::MAX, "pending attachment must remain live");
    assert!(
        created_attachments.lock().unwrap().is_empty(),
        "wrong parent must be rejected before canonical attachment creation"
    );
    assert!(
        !canonical_emails.lock().unwrap()[0].has_attachments,
        "wrong parent must not update the containing message"
    );

    execute_headers.insert("cookie", HeaderValue::from_str(&next_cookie).unwrap());
    renew_mapi_request_id(&mut execute_headers);
    let correct_parent_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&[0x25, 0x00, 0x02, 0x03, 0x00], &handles)),
        )
        .await
        .unwrap();
    assert_eq!(correct_parent_response.status(), StatusCode::OK);
    let body = response_bytes(correct_parent_response).await;
    let (correct_parent_rops, saved_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &correct_parent_rops,
        &[0x25, 0x02, 0, 0, 0, 0],
    ));
    assert_eq!(
        saved_handles[2], handles[2],
        "success must preserve the containing Message in the response slot"
    );
    assert_ne!(
        saved_handles[2], handles[3],
        "the response slot must not be rebound to the saved Attachment"
    );
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "parent-contract.pdf");
    assert_eq!(created[0].blob_bytes, b"%PDF-parent-contract");
}
