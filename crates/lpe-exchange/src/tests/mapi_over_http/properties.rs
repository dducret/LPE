use super::*;

#[tokio::test]
async fn mapi_over_http_custom_named_property_round_trips_on_supported_object() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "RCA Contact",
            "rca@example.test",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let custom_tag = 0x8001_001F;
    let mut custom_values = Vec::new();
    append_mapi_utf16_property(&mut custom_values, custom_tag, "opaque outlook value");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    append_rop_set_properties(&mut rops, 2, 1, &custom_values);
    append_rop_get_properties_specific(&mut rops, 2, &[custom_tag]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());
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
        &utf16z("opaque outlook value")
    ));

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut delete_rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    delete_rops.extend_from_slice(&[0x0B, 0x00, 0x02]);
    delete_rops.extend_from_slice(&1u16.to_le_bytes());
    delete_rops.extend_from_slice(&custom_tag.to_le_bytes());
    append_rop_get_properties_specific(&mut delete_rops, 2, &[custom_tag]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0B, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("opaque outlook value")
    ));
}

#[tokio::test]
async fn mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds() {
    let account = FakeStore::account();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "34343434-3434-3434-3434-343434343434";
    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let task_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let note_id = Uuid::parse_str("f1f1f1f1-f1f1-f1f1-f1f1-f1f1f1f1f1f1").unwrap();
    let journal_id = Uuid::parse_str("f2f2f2f2-f2f2-f2f2-f2f2-f2f2f2f2f2f2").unwrap();
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            mailbox_id,
            "inbox",
            "Custom message",
        )])),
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
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Custom event".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        task_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "tasks", "Tasks",
        )])),
        tasks: Arc::new(Mutex::new(vec![ClientTask {
            id: task_id,
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            is_owned: true,
            rights: FakeStore::rights(),
            task_list_id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
            task_list_sort_order: 0,
            title: "Custom task".to_string(),
            description: String::new(),
            status: "needs-action".to_string(),
            due_at: None,
            completed_at: None,
            recurrence_rule: String::new(),
            sort_order: 0,
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        }])),
        notes: Arc::new(Mutex::new(vec![ClientNote {
            id: note_id,
            title: "Custom note".to_string(),
            body_text: String::new(),
            color: "yellow".to_string(),
            categories_json: "[]".to_string(),
            created_at: "2026-05-05T08:00:00Z".to_string(),
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        }])),
        journal_entries: Arc::new(Mutex::new(vec![JournalEntry {
            id: journal_id,
            subject: "Custom journal".to_string(),
            body_text: String::new(),
            entry_type: "phone_call".to_string(),
            message_class: "IPM.Activity".to_string(),
            starts_at: None,
            ends_at: None,
            occurred_at: None,
            companies_json: "[]".to_string(),
            contacts_json: "[]".to_string(),
            created_at: "2026-05-05T08:00:00Z".to_string(),
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        }])),
        ..Default::default()
    };
    let stored_custom_values = store.mapi_custom_property_values.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let cases = [
        (
            test_mapi_folder_id(5),
            test_mapi_message_id(message_id),
            0x8001_001F,
            "message opaque value",
        ),
        (
            test_mapi_folder_id(16),
            test_mapi_uuid_id(&event_id),
            0x8001_001F,
            "event opaque value",
        ),
        (
            test_mapi_folder_id(19),
            test_mapi_uuid_id(&task_id),
            0x8001_001F,
            "task opaque value",
        ),
        (
            test_mapi_folder_id(18),
            test_mapi_uuid_id(&note_id),
            0x8001_001F,
            "note opaque value",
        ),
        (
            test_mapi_folder_id(17),
            test_mapi_uuid_id(&journal_id),
            0x8001_001F,
            "journal opaque value",
        ),
    ];

    let mut set_rops = Vec::new();
    for (index, (folder_id, object_id, tag, value)) in cases.iter().enumerate() {
        let folder_handle = 1 + (index as u8) * 2;
        let object_handle = folder_handle + 1;
        let mut property_values = Vec::new();
        append_mapi_utf16_property(&mut property_values, *tag, value);
        append_rop_open_folder(&mut set_rops, 0, folder_handle, *folder_id);
        append_rop_open_message_with_flags(
            &mut set_rops,
            folder_handle,
            object_handle,
            *folder_id,
            *object_id,
            0x01,
        );
        append_rop_set_properties(&mut set_rops, object_handle, 1, &property_values);
        append_rop_save_changes_message(&mut set_rops, folder_handle, object_handle);
    }
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &set_rops,
                &[
                    1,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                ],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    {
        let stored_values = stored_custom_values.lock().unwrap();
        for (_, _, _, value) in cases.iter() {
            assert!(
                stored_values
                    .values()
                    .any(|stored| contains_bytes(stored, &utf16z(value))),
                "missing stored custom value {value}"
            );
        }
        assert_eq!(stored_values.len(), cases.len());
    }

    let mut delete_rops = Vec::new();
    for (index, (folder_id, object_id, tag, _value)) in cases.iter().enumerate() {
        let folder_handle = 1 + (index as u8) * 2;
        let object_handle = folder_handle + 1;
        append_rop_open_folder(&mut delete_rops, 0, folder_handle, *folder_id);
        append_rop_open_message_with_flags(
            &mut delete_rops,
            folder_handle,
            object_handle,
            *folder_id,
            *object_id,
            0x01,
        );
        append_rop_delete_properties(&mut delete_rops, object_handle, &[*tag]);
        append_rop_save_changes_message(&mut delete_rops, folder_handle, object_handle);
    }
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &delete_rops,
                &[
                    1,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                ],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    assert!(stored_custom_values.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_custom_named_property_set_before_save_persists_on_created_item() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let custom_tag = 0x8001_001F;
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "Created Custom Contact");
    append_mapi_utf16_property(&mut property_values, 0x39FE_001F, "created@example.test");
    append_mapi_utf16_property(&mut property_values, custom_tag, "created opaque value");
    let mut create_rops = Vec::new();
    append_rop_create_message(&mut create_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_set_properties(&mut create_rops, 1, 3, &property_values);
    append_rop_save_changes_message(&mut create_rops, 1, 1);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&create_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));

    let mut read_rops = Vec::new();
    append_rop_open_folder(&mut read_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut read_rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    append_rop_get_properties_specific(&mut read_rops, 2, &[0x3001_001F, custom_tag]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&read_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Created Custom Contact")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("created opaque value")
    ));
}

#[tokio::test]
async fn mapi_over_http_accepts_outlook_octet_stream_bind_probe() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Nspi,
            &mapi_headers_with_content_type("Bind", "application/octet-stream"),
            &[0; 45],
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/mapi-http"
    );
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert!(response
        .headers()
        .get("x-clientinfo")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}:"));
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    let set_cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.starts_with("MapiContext="));
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=")));
}

#[tokio::test]
async fn mapi_over_http_accepts_rca_octet_stream_emsmdb_connect() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = mapi_headers_with_content_type("Connect", "application/octet-stream");
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("214"),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_static("3e93d512-7b7b-495a-9eb5-40b5adc4696a:1"),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_static("c9a1f6bb-76d3-41a1-8abb-fc60106a4a97:1"),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &headers, &[0; 214])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/mapi-http"
    );
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
}

#[tokio::test]
async fn mapi_over_http_execute_returns_logon_owner_and_status_properties() {
    let mut account = FakeStore::account();
    account.account_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    account.email = "bob@example.test".to_string();
    account.display_name = "Bob Store".to_string();
    let search_folder_id = Uuid::parse_str("11111111-2222-4333-8444-666666666666").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: search_folder_id,
            account_id: account.account_id,
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
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
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_request = hex_bytes(
        "0200000063000000000004005b005b005700fe0000010c0400210000000047002f6f3d4c50452f6f753d45786368616e67652041646d696e6973747261746976652047726f75702f636e3d526563697069656e74732f636e3d746573742d6c2d702d652d636800ffffffff0780000000000000",
    );
    let logon_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &logon_request)
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    assert_eq!(logon_response.headers().get("x-responsecode").unwrap(), "0");

    renew_mapi_request_id(&mut execute_headers);
    let store_properties_request = hex_bytes(
        "0200000037000000000004002f002f002b000700000000000008001f001c6602011b661f001d3402011e3402011f340b005c0e03006f3402010767010000000780000000000000",
    );
    let store_properties_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &store_properties_request,
        )
        .await
        .unwrap();

    assert_eq!(store_properties_response.status(), StatusCode::OK);
    assert_eq!(
        store_properties_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    let body = response_bytes(store_properties_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rops = &payload[2..response_rop_size];

    assert_eq!(response_rops[0], 0x07);
    assert_eq!(response_rops[1], 0);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    let mut offset = 6;
    assert_eq!(response_rops[offset], 0);
    offset += 1;

    let owner_name = utf16z("Bob Store");
    assert_eq!(
        &response_rops[offset..offset + owner_name.len()],
        owner_name.as_slice()
    );
    offset += owner_name.len();

    let entry_id_len =
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()) as usize;
    assert!(entry_id_len > 0);
    offset += 2 + entry_id_len;

    let server_name = utf16z("LPE");
    assert_eq!(
        &response_rops[offset..offset + server_name.len()],
        server_name.as_slice()
    );
    offset += server_name.len();

    let connected_icon_len =
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()) as usize;
    assert!(connected_icon_len > 0);
    offset += 2 + connected_icon_len;

    let account_icon_len =
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()) as usize;
    assert!(account_icon_len > 0);
    offset += 2 + account_icon_len;

    assert_eq!(response_rops[offset], 1);
    offset += 1;

    assert_eq!(
        u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap()),
        0
    );
    offset += 4;

    assert_eq!(
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()),
        16
    );
    offset += 2;
    assert_eq!(
        &response_rops[offset..offset + 16],
        account.account_id.as_bytes()
    );
    offset += 16;
    assert_eq!(offset, response_rops.len());

    assert!(contains_bytes(&response_rops, &utf16z("Bob Store")));
    assert!(contains_bytes(&response_rops, b"bob-example-test\0"));
    assert!(!contains_bytes(&response_rops, b"acct-bob-example-test\0"));
    assert!(contains_bytes(&response_rops, &utf16z("LPE")));
    assert!(contains_bytes(
        &response_rops,
        account.account_id.as_bytes().as_slice()
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body() {
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "MS-OXCMSG HTML CID");
    append_mapi_utf16_property(&mut property_values, 0x1013_001F, microsoft_html);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
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
    assert!(contains_bytes(&response_rops, &[0x0A, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].subject, "MS-OXCMSG HTML CID");
    assert_eq!(
        recorded[0].body_html_sanitized.as_deref(),
        Some(microsoft_html)
    );
    assert!(recorded[0].body_text.contains("This is a sample body text"));
    assert!(recorded[0]
        .body_html_sanitized
        .as_deref()
        .unwrap()
        .contains("cid:image001.png@01C86E1C.F1954390"));
}

#[tokio::test]
async fn mapi_over_http_get_properties_sees_message_saved_in_same_execute() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Visible after save");

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
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific on saved message
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());

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

    assert!(!contains_bytes(
        response_rops,
        &[0x07, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(response_rops, &utf16z("Visible after save")));
}

#[tokio::test]
async fn mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property() {
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
    append_mapi_string8_property(&mut property_values, 0x0037_001E, "String8 subject");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    rops.extend_from_slice(&[0x07, 0x00, 0x02]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    append_rop_save_changes_message(&mut rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"String8 subject\0"));
    assert!(contains_bytes(&response_rops, &utf16z("String8 subject")));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "String8 subject");
}

#[tokio::test]
async fn mapi_over_http_microsoft_create_message_initializes_documented_properties() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_get_properties_specific(
        &mut rops,
        2,
        &[
            0x001A_001F, // PidTagMessageClass.
            0x0017_0003, // PidTagImportance.
            0x0036_0003, // PidTagSensitivity.
            0x0E02_001F, // PidTagDisplayBcc.
            0x0E03_001F, // PidTagDisplayCc.
            0x0E04_001F, // PidTagDisplayTo.
            0x0E07_0003, // PidTagMessageFlags.
            0x0E1B_000B, // PidTagHasAttachments.
            0x0E79_0003, // PidTagTrustSender.
            0x0FF7_0003, // PidTagAccessLevel.
            0x3007_0040, // PidTagCreationTime.
            0x3008_0040, // PidTagLastModificationTime.
            0x300B_0102, // PidTagSearchKey.
            0x3FF8_001F, // PidTagCreatorName.
            0x3FF9_0102, // PidTagCreatorEntryId.
            0x3FFA_001F, // PidTagLastModifierName.
            0x3FFB_0102, // PidTagLastModifierEntryId.
            0x3FF1_0003, // PidTagMessageLocaleId.
            0x664A_000B, // PidTagHasNamedProperties.
            0x66A1_0003, // PidTagLocaleId.
        ],
    );

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x06, 0x02, 0, 0, 0, 0]));
    let row_offset = mapi_get_properties_specific_standard_row_offset(&response_rops, 2)
        .expect("pending message GetProps should return a standard row");
    assert_eq!(response_rops[row_offset], 0);
    let mut offset = row_offset + 1;
    let message_class = read_rop_utf16z(&response_rops, &mut offset).unwrap();
    let importance = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let sensitivity = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let display_bcc = read_rop_utf16z(&response_rops, &mut offset).unwrap();
    let display_cc = read_rop_utf16z(&response_rops, &mut offset).unwrap();
    let display_to = read_rop_utf16z(&response_rops, &mut offset).unwrap();
    let message_flags = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let has_attachments = response_rops[offset];
    offset += 1;
    let trust_sender = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let access_level = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let creation_time = u64::from_le_bytes(response_rops[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let last_modification_time =
        u64::from_le_bytes(response_rops[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let search_key = read_rop_binary_u16(&response_rops, &mut offset).unwrap();
    let creator_name = read_rop_utf16z(&response_rops, &mut offset).unwrap();
    let creator_entry_id = read_rop_binary_u16(&response_rops, &mut offset).unwrap();
    let last_modifier_name = read_rop_utf16z(&response_rops, &mut offset).unwrap();
    let last_modifier_entry_id = read_rop_binary_u16(&response_rops, &mut offset).unwrap();
    let message_locale_id =
        u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let has_named_properties = response_rops[offset];
    offset += 1;
    let locale_id = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());

    assert_eq!(message_class, "IPM.Note");
    assert_eq!(importance, 1);
    assert_eq!(sensitivity, 0);
    assert_eq!(display_bcc, "");
    assert_eq!(display_cc, "");
    assert_eq!(display_to, "");
    assert_eq!(message_flags, 0x0000_0009);
    assert_eq!(has_attachments, 0);
    assert_eq!(trust_sender, 1);
    assert_eq!(access_level, 1);
    assert_ne!(creation_time, 0);
    assert_eq!(last_modification_time, creation_time);
    assert!(!search_key.is_empty());
    assert_eq!(creator_name, FakeStore::account().display_name);
    assert!(!creator_entry_id.is_empty());
    assert_eq!(last_modifier_name, creator_name);
    assert_eq!(last_modifier_entry_id, creator_entry_id);
    assert_eq!(message_locale_id, 0x0409);
    assert_eq!(has_named_properties, 0);
    assert_eq!(locale_id, 0x0409);
}

#[tokio::test]
async fn mapi_over_http_pending_message_display_recipients_follow_modify_recipients() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(14));
    let to_row = mapi_wrapped_recipient_row("Denis Ducret", "denis.ducret@sdic.ch", 0x01);
    let cc_row = mapi_wrapped_recipient_row("Sandra Ducret", "sandra.ducret@sdic.ch", 0x02);
    append_rop_modify_recipients(
        &mut rops,
        2,
        &[(1, 0x01, to_row.as_slice()), (2, 0x02, cc_row.as_slice())],
    );
    append_rop_get_properties_specific(
        &mut rops,
        2,
        &[
            0x0E04_001F, // PidTagDisplayTo.
            0x0E03_001F, // PidTagDisplayCc.
        ],
    );

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;
    let row_offset = mapi_get_properties_specific_standard_row_offset(&response_rops, 2)
        .expect("pending message GetProps should return a standard row");
    assert_eq!(response_rops[row_offset], 0);
    let mut offset = row_offset + 1;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut offset).unwrap(),
        "Denis Ducret"
    );
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut offset).unwrap(),
        "Sandra Ducret"
    );
}

#[tokio::test]
async fn mapi_over_http_delete_properties_no_replicate_clears_pending_message_properties() {
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Temporary subject");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Temporary body");

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
        0x79, 0x00, 0x02, // RopSetPropertiesNoReplicate
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x7A, 0x00, 0x02, // RopDeletePropertiesNoReplicate
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());

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
        &[0x79, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Temporary subject")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Temporary body")));
}

#[tokio::test]
async fn mapi_over_http_named_property_bootstrap_maps_session_property_ids() {
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

    let ps_mapi_guid = [
        0x28, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let ps_internet_headers_guid = [
        0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let named_header = utf16z("X-LPE-Test");

    let mut rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x56, 0x00, 0x00, 0x02, // RopGetPropertyIdsFromNames, create missing
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.push(0x00);
    rops.extend_from_slice(&ps_mapi_guid);
    rops.extend_from_slice(&0x8503u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&ps_internet_headers_guid);
    rops.push(named_header.len() as u8);
    rops.extend_from_slice(&named_header);
    rops.extend_from_slice(&[
        0x55, 0x00, 0x00, // RopGetNamesFromPropertyIds
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x8503u16.to_le_bytes());
    rops.extend_from_slice(&0x9001u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x5F, 0x00, 0x00, 0x00, 0x00, // RopQueryNamedProperties
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x56, 0x00, 0, 0, 0, 0, 2, 0, 0x03, 0x85, 0x01, 0x90]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("x-lpe-test")));
    assert!(contains_bytes(
        &response_rops,
        &[0x5F, 0x00, 0, 0, 0, 0, 1, 0, 0x01, 0x90]
    ));
}

#[tokio::test]
async fn mapi_over_http_named_property_no_create_missing_returns_not_found() {
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
    let named_header = utf16z("X-LPE-Missing-NoCreate");

    let mut rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x56, 0x00, 0x00, 0x00, // RopGetPropertyIdsFromNames, do not create missing
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&FAKE_PS_INTERNET_HEADERS_GUID);
    rops.push(named_header.len() as u8);
    rops.extend_from_slice(&named_header);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
        &[0x56, 0x00, 0x0f, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x56, 0x00, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_named_property_mapping_survives_restart_style_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let ps_internet_headers_guid = FAKE_PS_INTERNET_HEADERS_GUID;
    let named_header = utf16z("X-LPE-Restart");

    let first_service = ExchangeService::new(store.clone());
    let first_connect = first_service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let first_cookie = first_connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let mut first_rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    first_rops.extend_from_slice(&0u32.to_le_bytes());
    first_rops.extend_from_slice(&0u32.to_le_bytes());
    first_rops.extend_from_slice(&0u16.to_le_bytes());
    first_rops.extend_from_slice(&[
        0x56, 0x00, 0x00, 0x02, // RopGetPropertyIdsFromNames, create missing
    ]);
    first_rops.extend_from_slice(&1u16.to_le_bytes());
    first_rops.push(0x01);
    first_rops.extend_from_slice(&ps_internet_headers_guid);
    first_rops.push(named_header.len() as u8);
    first_rops.extend_from_slice(&named_header);

    let mut first_headers = mapi_headers("Execute");
    first_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let first_response = first_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &first_headers,
            &execute_body(&rop_buffer(&first_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    let first_response_rops = response_rops_from_execute_response(first_response).await;
    assert!(contains_bytes(
        &first_response_rops,
        &[0x56, 0x00, 0, 0, 0, 0, 1, 0, 0x01, 0x90]
    ));

    let restarted_service = ExchangeService::new(store.clone());
    let restarted_connect = restarted_service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let restarted_cookie = restarted_connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let mut restarted_rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    restarted_rops.extend_from_slice(&0u32.to_le_bytes());
    restarted_rops.extend_from_slice(&0u32.to_le_bytes());
    restarted_rops.extend_from_slice(&0u16.to_le_bytes());
    restarted_rops.extend_from_slice(&[
        0x55, 0x00, 0x00, // RopGetNamesFromPropertyIds
    ]);
    restarted_rops.extend_from_slice(&1u16.to_le_bytes());
    restarted_rops.extend_from_slice(&0x9001u16.to_le_bytes());
    restarted_rops.extend_from_slice(&[
        0x5F, 0x00, 0x00, 0x00, 0x00, // RopQueryNamedProperties
    ]);

    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert("cookie", HeaderValue::from_str(&restarted_cookie).unwrap());
    let restarted_response = restarted_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&restarted_rops, &[u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(restarted_response.status(), StatusCode::OK);
    let restarted_response_rops = response_rops_from_execute_response(restarted_response).await;
    assert!(contains_bytes(
        &restarted_response_rops,
        &utf16z("x-lpe-restart")
    ));
    assert!(contains_bytes(
        &restarted_response_rops,
        &[0x5F, 0x00, 0, 0, 0, 0, 1, 0, 0x01, 0x90]
    ));

    let zero_count_service = ExchangeService::new(store);
    let zero_count_connect = zero_count_service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let zero_count_cookie = mapi_cookie_header(&zero_count_connect);
    let mut zero_count_rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    zero_count_rops.extend_from_slice(&0u32.to_le_bytes());
    zero_count_rops.extend_from_slice(&0u32.to_le_bytes());
    zero_count_rops.extend_from_slice(&0u16.to_le_bytes());
    zero_count_rops.extend_from_slice(&[
        0x56, 0x00, 0x00, 0x02, // RopGetPropertyIdsFromNames, enumerate on Logon
    ]);
    zero_count_rops.push(0x00);
    zero_count_rops.extend_from_slice(&0u16.to_le_bytes());

    let mut zero_count_headers = mapi_headers("Execute");
    zero_count_headers.insert("cookie", HeaderValue::from_str(&zero_count_cookie).unwrap());
    let zero_count_response = zero_count_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &zero_count_headers,
            &execute_body(&rop_buffer(&zero_count_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    let zero_count_response_rops = response_rops_from_execute_response(zero_count_response).await;
    assert!(contains_bytes(
        &zero_count_response_rops,
        &[0x56, 0x00, 0, 0, 0, 0, 1, 0, 0x01, 0x90]
    ));
}

#[tokio::test]
async fn mapi_over_http_message_status_is_session_local() {
    let message_id = Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Status message",
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

    let mapi_message_id = test_mapi_message_id(&message_id.to_string());
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    rops.extend_from_slice(&[0x20, 0x00, 0x01]); // RopSetMessageStatus
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
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
    assert_eq!(
        response_rops
            .windows(10)
            .filter(|window| *window == [0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0].as_slice())
            .count(),
        2
    );
    assert!(contains_bytes(
        response_rops,
        &[0x20, 0x01, 0, 0, 0, 0, 0x20, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_message_status_requires_folder_handle_and_set_opcode() {
    let message_id = Uuid::parse_str("10101010-1010-1010-1010-101010101011").unwrap();
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            inbox_id,
            "inbox",
            "Status handle",
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

    let folder_id = test_mapi_folder_id(5);
    let mapi_message_id = test_mapi_message_id(&message_id.to_string());
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, mapi_message_id);
    rops.extend_from_slice(&[0x1F, 0x00, 0x02]); // RopGetMessageStatus on message handle.
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    rops.extend_from_slice(&[0x20, 0x00, 0x02]); // RopSetMessageStatus on message handle.
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus for a missing message.
    rops.extend_from_slice(
        &test_mapi_message_id("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").to_le_bytes(),
    );
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus succeeds on folder handle.
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());

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
        &[0x20, 0x02, 0xB9, 0x04, 0x00, 0x00]
    ));
    assert_eq!(
        response_rops
            .windows(6)
            .filter(|window| *window == [0x20, 0x02, 0xB9, 0x04, 0x00, 0x00].as_slice())
            .count(),
        2
    );
    assert!(contains_bytes(
        &response_rops,
        &[0x20, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(&response_rops, &[0x1F, 0x01]));
    assert!(!contains_bytes(&response_rops, &[0x1F, 0x02]));
}

#[tokio::test]
async fn mapi_over_http_get_properties_specific_allows_known_unsupported_types() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let target = FakeStore::email(
        "87878787-8787-8787-8787-878787878787",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Reading pane probe",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![target.clone()])),
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
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(&target.id.to_string()),
    );
    append_rop_get_properties_specific(&mut rops, 2, &[0x0037_001F, 0x3701_000D]);

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
    assert!(contains_bytes(&response_rops, &[0x03, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x07, 0x02, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x07, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Reading pane probe")
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_delete_properties_no_replicate_is_best_effort_for_persisted_message() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let target = FakeStore::email(
        "87878787-8787-8787-8787-878787878787",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Delete properties probe",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![target.clone()])),
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
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(&target.id.to_string()),
    );
    rops.extend_from_slice(&[0x7A, 0x00, 0x02]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
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
    assert!(contains_bytes(&response_rops, &[0x03, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x7A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x07, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Delete properties probe")
    ));
}

#[tokio::test]
async fn mapi_over_http_set_properties_accepts_ptyp_server_id_on_pending_message() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "66666666-6666-6666-6666-666666666666",
            "outbox",
            "Outbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x6740_00FB, &[1, 2, 3, 4]);

    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(6));
    append_rop_set_properties(&mut rops, 1, 1, &property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x06, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_open_message_then_gets_canonical_message_properties() {
    let message_id = "11111111-1111-1111-1111-111111111111";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
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
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C1F_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E08_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x0E07_0003u32.to_le_bytes());

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

    let open_message_offset = 8;
    assert_eq!(response_rops[open_message_offset], 0x03);
    assert_eq!(response_rops[open_message_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[open_message_offset + 2..open_message_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    let get_props_offset = response_rops
        .iter()
        .enumerate()
        .skip(open_message_offset + 6)
        .find_map(|(offset, byte)| (*byte == 0x07).then_some(offset))
        .unwrap();
    assert_eq!(response_rops[get_props_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[get_props_offset + 2..get_props_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox message")));
    assert!(contains_bytes(response_rops, &utf16z("Hello")));
    assert!(contains_bytes(response_rops, &utf16z("alice@example.test")));
    assert!(contains_bytes(response_rops, &128u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcdata_property_row_example_streams_oversized_body() {
    let message_id = "31313131-3131-3131-3131-313131313131";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Hello",
    );
    email.body_text = "Large body ".repeat(16);
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
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    rops.extend_from_slice(&[0x07, 0x00, 0x02]); // RopGetPropertiesSpecific.
    rops.extend_from_slice(&16u16.to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x0E07_0003u32.to_le_bytes()); // PidTagMessageFlags.
    rops.extend_from_slice(&0x0037_0001u32.to_le_bytes()); // PidTagSubject, PtypUnspecified.
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes()); // PidTagBody.

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
    let marker = [0x07, 0x02, 0, 0, 0, 0, 1];
    let mut offset = response_rops
        .windows(marker.len())
        .position(|window| window == marker)
        .map(|offset| offset + marker.len())
        .expect("missing flagged RopGetPropertiesSpecific response");
    assert_eq!(response_rops[offset], 0);
    offset += 1;
    offset += 4;
    assert_eq!(
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()),
        0x001F
    );
    offset += 2;
    assert_eq!(response_rops[offset], 0);
    offset += 1;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut offset).unwrap(),
        "Hello"
    );
    assert_eq!(response_rops[offset], 0x0A);
    offset += 1;
    assert_eq!(
        u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap()),
        0x8007_000E
    );
}

#[tokio::test]
async fn mapi_over_http_get_properties_all_returns_message_projection() {
    let message_id = "24242424-2424-2424-2424-242424242424";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "All properties message",
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
        0x08, 0x00, 0x02, // RopGetPropertiesAll
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());

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

    assert!(contains_bytes(response_rops, &[0x08, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &0x0037_001Fu32.to_le_bytes()));
    assert!(contains_bytes(
        response_rops,
        &utf16z("All properties message")
    ));
    assert!(contains_bytes(response_rops, &0x1000_001Fu32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &utf16z("Hello")));
}

#[tokio::test]
async fn mapi_over_http_open_attachment_returns_canonical_attachment_properties() {
    let message_id = "34343434-3434-3434-3434-343434343434";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment open message",
    );
    email.has_attachments = true;
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
                file_name: "brief-open.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: Some("inline".to_string()),
                content_id: Some("image001.PNG@01C86E1C.F1954390".to_string()),
                size_octets: 9,
                file_reference,
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
        0x22, 0x00, 0x02, 0x03, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x07, 0x00, 0x03, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&7u16.to_le_bytes());
    rops.extend_from_slice(&0x0E21_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3707_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x370E_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E20_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3712_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3714_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x7FFE_000Bu32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
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

    assert!(contains_bytes(response_rops, &[0x22, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("brief-open.pdf")));
    assert!(contains_bytes(response_rops, &utf16z("application/pdf")));
    assert!(contains_bytes(
        response_rops,
        &utf16z("image001.PNG@01C86E1C.F1954390")
    ));
    assert!(contains_bytes(response_rops, &4u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &9u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_attachment_custom_properties_survive_restart_style_session() {
    let message_id = "34343434-3434-3434-3434-343434343434";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment custom property message",
    );
    email.has_attachments = true;
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
                file_name: "custom.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: None,
                content_id: None,
                size_octets: 9,
                file_reference,
            }],
        )]))),
        ..Default::default()
    };
    let custom_tag = 0x8004_001F;
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, custom_tag, "attachment opaque value");

    let first_service = ExchangeService::new(store.clone());
    let first_connect = first_service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let first_cookie = mapi_cookie_header(&first_connect);
    let mut first_rops = Vec::new();
    append_rop_open_folder(&mut first_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_open_message(
        &mut first_rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    first_rops.extend_from_slice(&[0x22, 0x00, 0x02, 0x03, 0x00]);
    first_rops.extend_from_slice(&0u32.to_le_bytes());
    append_rop_set_properties(&mut first_rops, 3, 1, &property_values);

    let mut first_headers = mapi_headers("Execute");
    first_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let first_response = first_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &first_headers,
            &execute_body(&rop_buffer(&first_rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(first_response.status(), StatusCode::OK);
    let first_response_rops = response_rops_from_execute_response(first_response).await;
    assert!(contains_bytes(
        &first_response_rops,
        &[0x0A, 0x03, 0, 0, 0, 0, 0]
    ));

    let restarted_service = ExchangeService::new(store);
    let restarted_connect = restarted_service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let restarted_cookie = mapi_cookie_header(&restarted_connect);
    let mut restarted_rops = Vec::new();
    append_rop_open_folder(&mut restarted_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_open_message(
        &mut restarted_rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    restarted_rops.extend_from_slice(&[0x22, 0x00, 0x02, 0x03, 0x00]);
    restarted_rops.extend_from_slice(&0u32.to_le_bytes());
    restarted_rops.extend_from_slice(&[0x07, 0x00, 0x03]);
    restarted_rops.extend_from_slice(&4096u16.to_le_bytes());
    restarted_rops.extend_from_slice(&1u16.to_le_bytes());
    restarted_rops.extend_from_slice(&custom_tag.to_le_bytes());

    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert("cookie", HeaderValue::from_str(&restarted_cookie).unwrap());
    let restarted_response = restarted_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(
                &restarted_rops,
                &[1, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let restarted_response_rops = response_rops_from_execute_response(restarted_response).await;
    assert!(contains_bytes(
        &restarted_response_rops,
        &utf16z("attachment opaque value")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_stream_region_rops_succeed_on_stream_handles() {
    let message_id = "35353535-3535-3535-3535-353535353535";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment stream message",
    );
    email.has_attachments = true;
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
                file_name: "stream.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: None,
                content_id: None,
                size_octets: 11,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: "stream.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"hello-world".to_vec(),
            },
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
        0x22, 0x00, 0x02, 0x03, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x3B, 0x00, 0x04, 0x05, // RopCloneStream
    ]);
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x04, // RopReadStream
    ]);
    rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    rops.extend_from_slice(&5u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x05, // RopReadStream from cloned stream
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x2E, 0x00, 0x04, 0x00, // RopSeekStream, stream beginning
    ]);
    rops.extend_from_slice(&6i64.to_le_bytes());
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x04, // RopReadStream
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x5B, 0x00, 0x04, // RopLockRegionStream
    ]);
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&5u64.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x5C, 0x00, 0x04, // RopUnlockRegionStream
    ]);
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&5u64.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
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

    assert!(contains_bytes(
        response_rops,
        &[0x2B, 0x04, 0, 0, 0, 0, 11, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x3B, 0x05, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x2C, 0x04, 0, 0, 0, 0, 5, 0, b'h', b'e', b'l', b'l', b'o']
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2C, 0x05, 0, 0, 0, 0, 5, 0, b'h', b'e', b'l', b'l', b'o']
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2E, 0x04, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2C, 0x04, 0, 0, 0, 0, 5, 0, b'w', b'o', b'r', b'l', b'd']
    ));
    assert!(contains_bytes(response_rops, &[0x5B, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &[0x5C, 0x04, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_reads_canonical_message_body_stream() {
    let message_id = "42424242-4242-4242-4242-424242424242";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message body stream",
    );
    email.body_text = "Canonical body stream".to_string();
    email.body_html_sanitized = Some("<p>Canonical <b>HTML</b> stream</p>".to_string());
    let body_bytes = utf16z(&email.body_text);
    let html_bytes = email.body_html_sanitized.clone().unwrap().into_bytes();
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
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x03, // RopReadStream
    ]);
    rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    rops.extend_from_slice(&(body_bytes.len() as u32).to_le_bytes());
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x04, // RopOpenStream, PidTagHtml
    ]);
    rops.extend_from_slice(&0x1013_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x04, // RopReadStream
    ]);
    rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    rops.extend_from_slice(&(html_bytes.len() as u32).to_le_bytes());

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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x2B, 0x03, 0, 0, 0, 0, body_bytes.len() as u8, 0, 0, 0]
    ));
    let mut read_response = vec![0x2C, 0x03, 0, 0, 0, 0];
    read_response.extend_from_slice(&(body_bytes.len() as u16).to_le_bytes());
    read_response.extend_from_slice(&body_bytes);
    assert!(contains_bytes(&response_rops, &read_response));
    assert!(contains_bytes(
        &response_rops,
        &[0x2B, 0x04, 0, 0, 0, 0, html_bytes.len() as u8, 0, 0, 0]
    ));
    let mut html_response = vec![0x2C, 0x04, 0, 0, 0, 0];
    html_response.extend_from_slice(&(html_bytes.len() as u16).to_le_bytes());
    html_response.extend_from_slice(&html_bytes);
    assert!(contains_bytes(&response_rops, &html_response));
}

#[tokio::test]
async fn mapi_over_http_string8_body_stream_writes_canonical_message_body() {
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
    let stream_body = b"String8 body stream\0";

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream, PidTagBody String8
    ]);
    rops.extend_from_slice(&0x1000_001Eu32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[0x2D, 0x00, 0x03]); // RopWriteStream
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(stream_body);
    rops.extend_from_slice(&[
        0x2E, 0x00, 0x03, 0x00, // RopSeekStream, stream beginning
    ]);
    rops.extend_from_slice(&0i64.to_le_bytes());
    rops.extend_from_slice(&[0x2C, 0x00, 0x03]); // RopReadStream
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(&[0x5D, 0x00, 0x03]); // RopCommitStream
    append_rop_save_changes_message(&mut rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, stream_body));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].body_text, "String8 body stream");
}

#[tokio::test]
async fn mapi_over_http_copy_to_stream_saves_canonical_message_body() {
    let source_message_id = "43434343-4343-4343-4343-434343434343";
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut source = FakeStore::email(
        source_message_id,
        &inbox_id.to_string(),
        "inbox",
        "Source message body",
    );
    source.body_text = "Copied canonical body stream".to_string();
    let source_body = utf16z(&source.body_text);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![source])),
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

    let mut subject_values = Vec::new();
    append_mapi_utf16_property(&mut subject_values, 0x0037_001F, "Copied body destination");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage, source
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(source_message_id));
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream, source body
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x04, // RopCreateMessage, destination
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x04, // RopSetProperties, destination subject
    ]);
    rops.extend_from_slice(&((subject_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&subject_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x04, 0x05, // RopOpenStream, destination body
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[
        0x3A, 0x00, 0x03, 0x05, // RopCopyToStream
    ]);
    rops.extend_from_slice(&(source_body.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x04, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
    ));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[
            0x3A,
            0x03,
            0,
            0,
            0,
            0,
            source_body.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            source_body.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
    ));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].subject, "Copied body destination");
    assert_eq!(recorded[0].body_text, "Copied canonical body stream");
}

#[tokio::test]
async fn mapi_over_http_microsoft_create_attachment_initializes_documented_properties() {
    let message_id = "38383838-3838-3838-3838-383838383838";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "MAPI attachment defaults",
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
    append_rop_get_properties_specific(
        &mut rops,
        3,
        &[
            0x0E21_0003, // PidTagAttachNumber.
            0x0E20_0003, // PidTagAttachSize.
            0x0FF7_0003, // PidTagAccessLevel.
            0x370B_0003, // PidTagRenderingPosition.
            0x3007_0040, // PidTagCreationTime.
            0x3008_0040, // PidTagLastModificationTime.
        ],
    );

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
        &[0x23, 0x03, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    let row_offset = mapi_get_properties_specific_standard_row_offset(&response_rops, 3).unwrap();
    assert_eq!(response_rops[row_offset], 0);
    let mut offset = row_offset + 1;
    let attach_num = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let attach_size = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let access_level = u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let rendering_position =
        u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap());
    offset += 4;
    let creation_time = u64::from_le_bytes(response_rops[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let last_modification_time =
        u64::from_le_bytes(response_rops[offset..offset + 8].try_into().unwrap());

    assert_eq!(attach_num, 0);
    assert_eq!(attach_size, 0);
    assert_eq!(access_level, 0);
    assert_eq!(rendering_position, u32::MAX);
    assert_ne!(creation_time, 0);
    assert_eq!(last_modification_time, creation_time);
}

#[tokio::test]
async fn mapi_over_http_create_attachment_saves_canonical_attachment_from_properties() {
    let message_id = "37373737-3737-3737-3737-373737373737";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let canonical_emails = Arc::new(Mutex::new(vec![FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MAPI attachment message",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: canonical_emails.clone(),
        created_attachments: created_attachments.clone(),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
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
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "mapi-upload.pdf");
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "application/pdf");
    append_mapi_binary_property(&mut property_values, 0x3701_0102, b"%PDF-mapi");

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
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream, read-only
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x2D, 0x00, 0x04, // RopWriteStream
    ]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(b"fake");
    rops.extend_from_slice(&[
        0x25, 0x00, 0x02, 0x03, 0x00, // RopSaveChangesAttachment
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

    assert!(contains_bytes(
        response_rops,
        &[0x23, 0x03, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2B, 0x04, 0, 0, 0, 0, 9, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2D, 0x04, 0x05, 0x00, 0x03, 0x80]
    ));
    assert!(contains_bytes(response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "mapi-upload.pdf");
    assert_eq!(created[0].media_type, "application/pdf");
    assert_eq!(created[0].blob_bytes, b"%PDF-mapi");
    let canonical = canonical_emails.lock().unwrap();
    assert!(
        canonical
            .iter()
            .find(|email| email.id == Uuid::parse_str(message_id).unwrap())
            .expect("message remains canonical after attachment save")
            .has_attachments
    );
}

#[tokio::test]
async fn mapi_over_http_write_stream_saves_canonical_attachment() {
    let message_id = "38383838-3838-3838-3838-383838383838";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let canonical_emails = Arc::new(Mutex::new(vec![FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MAPI stream attachment message",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: canonical_emails.clone(),
        created_attachments: created_attachments.clone(),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
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
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "stream-upload.pdf");
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "application/pdf");

    let stream_bytes = b"%PDF-stream";
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
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x2F, 0x00, 0x04, // RopSetStreamSize
    ]);
    rops.extend_from_slice(&(stream_bytes.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x5E, 0x00, 0x04, // RopGetStreamSize
    ]);
    rops.extend_from_slice(&[
        0x2D, 0x00, 0x04, // RopWriteStream
    ]);
    rops.extend_from_slice(&(stream_bytes.len() as u16).to_le_bytes());
    rops.extend_from_slice(stream_bytes);
    rops.extend_from_slice(&[
        0x5D, 0x00, 0x04, // RopCommitStream
        0x25, 0x00, 0x02, 0x03, 0x00, // RopSaveChangesAttachment
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

    assert!(contains_bytes(response_rops, &[0x2F, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x5E, 0x04, 0, 0, 0, 0, stream_bytes.len() as u8, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2D, 0x04, 0, 0, 0, 0, stream_bytes.len() as u8, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x5D, 0x04, 0, 0, 0, 0]));
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "stream-upload.pdf");
    assert_eq!(created[0].blob_bytes, stream_bytes);
    let canonical = canonical_emails.lock().unwrap();
    assert!(
        canonical
            .iter()
            .find(|email| email.id == Uuid::parse_str(message_id).unwrap())
            .expect("message remains canonical after streamed attachment save")
            .has_attachments
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment() {
    let message_id = "39393939-3939-3939-3939-393939393939";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let canonical_emails = Arc::new(Mutex::new(vec![FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MS-OXCMSG text attachment message",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: canonical_emails.clone(),
        created_attachments: created_attachments.clone(),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::text(), 0.8));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_i32_property(&mut property_values, 0x3705_0003, 1);
    append_mapi_i32_property(&mut property_values, 0x370B_0003, -1);
    append_mapi_i32_property(&mut property_values, 0x7FFD_0003, 0);
    append_mapi_utf16_property(&mut property_values, PID_TAG_DISPLAY_NAME_W, "test.txt");
    append_mapi_i32_property(&mut property_values, 0x7FFA_0003, 0);
    append_mapi_i32_property(&mut property_values, 0x3714_0003, 0);
    append_mapi_bool_property(&mut property_values, 0x7FFE_000B, false);
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "test.txt");
    append_mapi_utf16_property(&mut property_values, 0x3704_001F, "test.txt");
    append_mapi_utf16_property(&mut property_values, 0x3703_001F, ".txt");
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "text/plain");
    append_mapi_binary_property(&mut property_values, 0x3709_0102, b"wmf-placeholder");

    let stream_bytes = b"hello from MS-OXCMSG 4.5";
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&12u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream: PidTagAttachDataBinary
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x2F, 0x00, 0x04]); // RopSetStreamSize
    rops.extend_from_slice(&(stream_bytes.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[0x2D, 0x00, 0x04]); // RopWriteStream
    rops.extend_from_slice(&(stream_bytes.len() as u16).to_le_bytes());
    rops.extend_from_slice(stream_bytes);
    rops.extend_from_slice(&[
        0x01, 0x00, 0x04, // RopRelease stream handle
        0x25, 0x00, 0x02, 0x03, 0x0A, // RopSaveChangesAttachment KeepOpenReadWrite
    ]);
    append_rop_get_properties_specific(&mut rops, 3, &[0x3707_001F, 0x370E_001F]);
    rops.extend_from_slice(&[0x01, 0x00, 0x03]); // RopRelease attachment handle
    append_rop_get_properties_specific(&mut rops, 3, &[0x3707_001F]);

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
    assert!(contains_bytes(&response_rops, &[0x23, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x2F, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x2D, 0x04, 0, 0, 0, 0, stream_bytes.len() as u8, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    assert!(
        mapi_get_properties_specific_standard_row_offset(&response_rops, 3).is_ok(),
        "KeepOpenReadWrite save must leave the attachment handle readable"
    );
    assert!(contains_bytes(&response_rops, &utf16z("test.txt")));
    assert!(contains_bytes(&response_rops, &utf16z("text/plain")));
    assert!(
        contains_bytes(
            &response_rops,
            &[0x07, 0x03, 0, 0, 0, 0, 1, 0x0A, 0x0F, 0x01, 0x04, 0x80]
        ),
        "response_rops={response_rops:02x?}"
    );
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "test.txt");
    assert_eq!(created[0].media_type, "text/plain");
    assert_eq!(created[0].disposition.as_deref(), Some("attachment"));
    assert_eq!(created[0].content_id, None);
    assert_eq!(created[0].blob_bytes, stream_bytes);
    let canonical = canonical_emails.lock().unwrap();
    assert!(
        canonical
            .iter()
            .find(|email| email.id == Uuid::parse_str(message_id).unwrap())
            .expect("message remains canonical after text attachment save")
            .has_attachments
    );
}

#[tokio::test]
async fn mapi_over_http_copy_to_stream_saves_canonical_attachment() {
    let message_id = "41414141-4141-4141-4141-414141414141";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let source_attachment_id = Uuid::parse_str("dadadada-dada-dada-dada-dadadadadada").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MAPI stream copy attachment message",
    );
    email.has_attachments = true;
    let source_reference = format!("attachment:{message_uuid}:{source_attachment_id}");
    let source_bytes = b"%PDF-copy-source";
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let canonical_emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: canonical_emails.clone(),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: source_attachment_id,
                message_id: message_uuid,
                file_name: "source.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: None,
                content_id: None,
                size_octets: source_bytes.len() as u64,
                file_reference: source_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            source_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: source_reference,
                file_name: "source.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: source_bytes.to_vec(),
            },
        )]))),
        created_attachments: created_attachments.clone(),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
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
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "copied-stream.pdf");
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "application/pdf");

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
        0x22, 0x00, 0x02, 0x03, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream, source
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x05, // RopCreateAttachment
        0x0A, 0x00, 0x05, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x05, 0x06, // RopOpenStream, destination
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x3A, 0x00, 0x04, 0x06, // RopCopyToStream
    ]);
    rops.extend_from_slice(&(source_bytes.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x25, 0x00, 0x02, 0x05, 0x00, // RopSaveChangesAttachment
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[
            1,
            u32::MAX,
            u32::MAX,
            u32::MAX,
            u32::MAX,
            u32::MAX,
            u32::MAX,
        ],
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

    assert!(contains_bytes(
        response_rops,
        &[
            0x3A,
            0x04,
            0,
            0,
            0,
            0,
            source_bytes.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            source_bytes.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
    ));
    assert!(contains_bytes(response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "copied-stream.pdf");
    assert_eq!(created[0].blob_bytes, source_bytes);
    let canonical = canonical_emails.lock().unwrap();
    assert!(
        canonical
            .iter()
            .find(|email| email.id == message_uuid)
            .expect("message remains canonical after copied attachment save")
            .has_attachments
    );
}

#[tokio::test]
async fn mapi_over_http_outlook_set_read_flags_updates_state_and_notifies_table() {
    let message_id = "36363636-3636-3636-3636-363636363636";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Read flag message",
    );
    email.unread = true;
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x00, 0x02]); // RopRegisterNotification
    rops.extend_from_slice(&0x0178u16.to_le_bytes());
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&[
        // Outlook 16 trace 202607131640 request :264: asynchronous and
        // rfReserved (0x0A), whose ignored bits leave rfDefault behavior.
        0x66, 0x00, 0x01, 0x01, 0x0A,
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));

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

    assert!(contains_bytes(response_rops, &[0x66, 0x01, 0, 0, 0, 0, 0]));
    assert!(!emails.lock().unwrap()[0].unread);
    assert!(contains_bytes(
        response_rops,
        &[0x2A, 0x03, 0, 0, 0, 0, 0x00, 0x01, 0x01, 0x00]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_set_read_flags_rejects_invalid_parameters() {
    let message_id = "36363636-3636-3636-3636-363636363637";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Invalid read flags",
    );
    email.unread = true;
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

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x66, 0x00, 0x01, 0x00, 0x80, // RopSetReadFlags, unknown ReadFlags bit.
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());

    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x66, 0x01, 0x57, 0, 0x07, 0x80]
    ));
    assert!(emails.lock().unwrap()[0].unread);

    renew_mapi_request_id(&mut execute_headers);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x66, 0x00, 0x01, 0x02, 0x01, // RopSetReadFlags, nonzero WantAsynchronous.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));

    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x66, 0x01, 0x00, 0, 0x00, 0x00, 0x00]
    ));
    assert!(!emails.lock().unwrap()[0].unread);

    renew_mapi_request_id(&mut execute_headers);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x66, 0x00, 0x01, 0x00, 0x04, // RopSetReadFlags, rfClearReadFlag.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));

    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x66, 0x01, 0x00, 0, 0x00, 0x00, 0x00]
    ));
    assert!(emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_set_message_read_flag_updates_open_message_state() {
    let message_id = "37373737-3737-3737-3737-373737373737";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message read flag",
    );
    email.unread = true;
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
        0x11, 0x00, 0x02, 0x02, 0x01, // RopSetMessageReadFlag
    ]);

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

    assert!(contains_bytes(response_rops, &[0x11, 0x02, 0, 0, 0, 0, 0]));
    assert!(!emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_outlook_set_message_read_flag_accepts_default_flag() {
    let message_id = "37373737-3737-3737-3737-373737373738";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Default message read flag",
    );
    email.unread = true;
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

    let mut open_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut open_rops, test_mapi_folder_id(5));
    open_rops.push(0);
    open_rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    open_rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut open_rops, test_mapi_folder_id(5));
    open_rops.push(0);
    append_mapi_wire_id(&mut open_rops, test_mapi_message_id(message_id));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX]));
    let open_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let open_cookie = mapi_cookie_header(&open_response);
    let open_body = response_bytes(open_response).await;
    let (_, open_handles) = response_rops_and_handles_from_execute_body(&open_body);

    // Outlook 16 trace 202607131609 requests :294 and :308 use this exact
    // RopSetMessageReadFlag shape: response/input handle 0 and rfDefault.
    let read_rops = [0x11, 0x00, 0x00, 0x00, 0x00];
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&open_cookie).unwrap());
    let request = execute_body(&rop_buffer(&read_rops, &[open_handles[2]]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x11, 0x00, 0, 0, 0, 0, 0]));
    assert!(!emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_outlook_set_message_read_flag_accepts_clear_read_flag() {
    let message_id = "37373737-3737-3737-3737-373737373739";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Clear message read flag",
    );
    email.unread = false;
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

    let mut open_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut open_rops, test_mapi_folder_id(5));
    open_rops.push(0);
    open_rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    open_rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut open_rops, test_mapi_folder_id(5));
    open_rops.push(0);
    append_mapi_wire_id(&mut open_rops, test_mapi_message_id(message_id));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&open_rops, &[1, u32::MAX, u32::MAX]));
    let open_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let open_cookie = mapi_cookie_header(&open_response);
    let open_body = response_bytes(open_response).await;
    let (_, open_handles) = response_rops_and_handles_from_execute_body(&open_body);

    // Outlook 16 trace 202607131718 request :240 uses this exact
    // RopSetMessageReadFlag shape to mark a message unread.
    let read_rops = [0x11, 0x00, 0x00, 0x00, 0x04];
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&open_cookie).unwrap());
    let request = execute_body(&rop_buffer(&read_rops, &[open_handles[2]]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x11, 0x00, 0, 0, 0, 0, 0]));
    assert!(emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_microsoft_message_properties_commit_on_save_changes() {
    let message_id = "38383838-3838-3838-3838-383838383838";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message property flags",
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
    append_mapi_i32_property(&mut property_values, 0x0E07_0003, 1);
    append_mapi_i32_property(&mut property_values, 0x1090_0003, 2);

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
        0x0A, 0x00, 0x02, // RopSetProperties on opened message
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

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
    assert!(contains_bytes(
        response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    let staged = emails.lock().unwrap()[0].clone();
    assert!(staged.unread);
    assert!(!staged.flagged);

    let handle_table = &rop_buffer[2 + response_rop_size..];
    let message_handle = u32::from_le_bytes(handle_table[8..12].try_into().unwrap());
    let mut save_rops = Vec::new();
    append_rop_save_changes_message(&mut save_rops, 2, 2);
    let save_request = execute_body(&super::rop_buffer(
        &save_rops,
        &[1, u32::MAX, message_handle],
    ));
    let mut save_headers = mapi_headers("Execute");
    save_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let save_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &save_headers, &save_request)
        .await
        .unwrap();

    assert_eq!(save_response.status(), StatusCode::OK);
    let save_response_rops = response_rops_from_execute_response(save_response).await;
    assert!(contains_bytes(
        &save_response_rops,
        &[0x0C, 0x02, 0, 0, 0, 0, 0x02]
    ));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn mapi_over_http_set_properties_validates_swapped_todo_data() {
    let message_id = "49494949-4949-4949-4949-494949494949";
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Swapped To-Do data",
    );
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

    let valid_data = valid_swapped_todo_data();
    let mut valid_values = Vec::new();
    append_mapi_binary_property(&mut valid_values, 0x0E2D_0102, &valid_data);
    let mut valid_rops = Vec::new();
    append_rop_open_folder(&mut valid_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_open_message(
        &mut valid_rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    append_rop_set_properties(&mut valid_rops, 2, 1, &valid_values);
    append_rop_save_changes_message(&mut valid_rops, 2, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&valid_rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x02, 0, 0, 0, 0, 0x02]
    ));
    assert_eq!(
        emails.lock().unwrap()[0].swapped_todo_data.as_deref(),
        Some(valid_data.as_slice())
    );

    let mut invalid_values = Vec::new();
    append_mapi_binary_property(&mut invalid_values, 0x0E2D_0102, &[1, 2, 3, 4]);
    let mut invalid_rops = Vec::new();
    append_rop_open_message(
        &mut invalid_rops,
        0,
        1,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    append_rop_set_properties(&mut invalid_rops, 1, 1, &invalid_values);

    renew_mapi_request_id(&mut execute_headers);
    let request = execute_body(&rop_buffer(&invalid_rops, &[u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert_eq!(
        emails.lock().unwrap()[0].swapped_todo_data.as_deref(),
        Some(valid_data.as_slice())
    );
}

#[tokio::test]
async fn mapi_over_http_cached_mode_properties_include_canonical_change_keys() {
    let message_id = "39393939-3939-3939-3939-393939393939";
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let folder_change = mapi_mailstore::canonical_folder_change_number(&inbox);
    let mut email = FakeStore::email(message_id, mailbox_id, "inbox", "Cached mode message");
    email.flagged = true;
    let message_change_number = mapi_mailstore::canonical_message_change_number(&email);
    let message_commit_time = mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at);
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
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on the folder
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x6709_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x670A_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x663E_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x4082_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x65E2_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&0x65E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E1_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x67A4_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6709_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x1090_0003u32.to_le_bytes());

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
        &mapi_mailstore::STORE_REPLICA_GUID
    ));
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let message_source_key = mapi_mailstore::source_key_for_uuid(&message_uuid);
    let message_change_key = mapi_mailstore::change_key_for_change_number(message_change_number);
    let mut source_key_wire_value = 22u16.to_le_bytes().to_vec();
    source_key_wire_value.extend_from_slice(&message_source_key);
    let mut change_key_wire_value = 22u16.to_le_bytes().to_vec();
    change_key_wire_value.extend_from_slice(&message_change_key);
    assert!(contains_bytes(&response_rops, &source_key_wire_value));
    assert!(contains_bytes(&response_rops, &change_key_wire_value));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::filetime_from_change_number(folder_change).to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &(folder_change.min(u64::from(u32::MAX)) as u32).to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &message_commit_time.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &2i32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxosrch_search_definition_message_properties_are_exposed() {
    let account = FakeStore::account();
    let definition_id = Uuid::parse_str("757154c8-c1df-c14c-91de-09c2044d2d1c").unwrap();
    let microsoft_unread_mail_definition = "BBAAAEgAAAAAAAAAAAAAAAABAAAAAD4AAAABAAAAvM2HGC4AAADEzYcYAAAAAAoZ1rzItEpMv132OpIuFwwBABTiABTuh5JDoagpsGINvYkAAAIN76gAAAAAAAAAAAAAAgAAAAAAAAAHAAAAAgAAAAMAAAACAAEAHgAaAB4AGgAQAElQTS5BcHBvaW50bWVudAACAAAAAwAAAAIAAQAeABoAHgAaAAwASVBNLkNvbnRhY3QAAgAAAAMAAAACAAEAHgAaAB4AGgANAElQTS5EaXN0TGlzdAACAAAAAwAAAAIAAQAeABoAHgAaAA0ASVBNLkFjdGl2aXR5AAIAAAADAAAAAgABAB4AGgAeABoADwBJUE0uU3RpY2t5Tm90ZQACAAAAAwAAAAAAAQAeABoAHgAaAAkASVBNLlRhc2sAAgAAAAMAAAACAAEAHgAaAB4AGgAKAElQTS5UYXNrLgAAAAAAAgAAAAAAAAAIAAAABAAAAAUAAAACAQkOAgEJDi4AAAAAAAoZ1rzItEpMv132OpIuFwwBABTiABTuh5JDoagpsGINvYkAAAIN764AAAQAAAAFAAAAAgEJDgIBCQ4uAAAAAAAKGda8yLRKTL9d9jqSLhcMAQBKC7nZLCyoRrM1V1y78FSSAAABZAACAAAEAAAABQAAAAIBCQ4CAQkOLgAAAAAAChnWvMi0Sky/XfY6ki4XDAEAFOIAFO6HkkOhqCmwYg29iQAAAg3RMwAABAAAAAUAAAACAQkOAgEJDi4AAAAAAAoZ1rzItEpMv132OpIuFwwBABTiABTuh5JDoagpsGINvYkAAAIN76wAAAQAAAAFAAAAAgEJDgIBCQ4uAAAAAAAKGda8yLRKTL9d9jqSLhcMAQAU4gAU7oeSQ6GoKbBiDb2JAAACELTSAAAEAAAABQAAAAIBCQ4CAQkOLgAAAAAAChnWvMi0Sky/XfY6ki4XDAEAFOIAFO6HkkOhqCmwYg29iQAAAhC00wAABAAAAAUAAAACAQkOAgEJDi4AAAAAAAoZ1rzItEpMv132OpIuFwwBABTiABTuh5JDoagpsGINvYkAAAIQtNQAAAQAAAAFAAAAAgEJDgIBCQ4uAAAAAAAKGda8yLRKTL9d9jqSLhcMAQAU4gAU7oeSQ6GoKbBiDb2JAAACELTRAAABAAAAAgAAAAYAAAAAAAAAAwAHDgEAAAAGAAAAAQAAAAMAlxABAAAAAAAAAA==";
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: definition_id,
            account_id: account.account_id,
            role: "unread_mail".to_string(),
            display_name: "Unread Mail".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({
                "scope": "top_of_personal_folders",
                "recursive": true
            }),
            restriction_json: serde_json::json!({
                "kind": "exchange_unread_mail",
                "pidTagSearchFolderTag": 1045439171u32,
                "pidTagSearchFolderDefinition": microsoft_unread_mail_definition
            }),
            excluded_folder_roles: vec![
                "trash".to_string(),
                "junk".to_string(),
                "drafts".to_string(),
                "outbox".to_string(),
            ],
            is_builtin: true,
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
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
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x02]); // associated contents table
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&8u16.to_le_bytes());
    for tag in [
        0x674A_0014u32, // PidTagMid
        0x001A_001F,    // PidTagMessageClass
        0x3001_001F,    // PidTagDisplayName
        0x6841_0003,    // PidTagSearchFolderTemplateId
        0x6846_0003,    // PidTagSearchFolderStorageType
        0x6847_0003,    // PidTagSearchFolderTag
        0x6848_0003,    // PidTagSearchFolderEfpFlags
        0x6842_0102,    // PidTagSearchFolderId
    ] {
        rops.extend_from_slice(&tag.to_le_bytes());
    }
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows
    rops.extend_from_slice(&20u16.to_le_bytes());
    append_rop_open_message(
        &mut rops,
        1,
        3,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        test_mapi_uuid_id(&definition_id),
    );
    append_rop_get_properties_specific(
        &mut rops,
        3,
        &[
            0x001A_001F, // PidTagMessageClass
            0x3001_001F, // PidTagDisplayName
            0x6841_0003, // PidTagSearchFolderTemplateId
            0x6834_0003, // PidTagSearchFolderLastUsed
            0x683A_0003, // PidTagSearchFolderExpiration
            0x6846_0003, // PidTagSearchFolderStorageType
            0x6847_0003, // PidTagSearchFolderTag
            0x6848_0003, // PidTagSearchFolderEfpFlags
            0x6842_0102, // PidTagSearchFolderId
            0x6845_0102, // PidTagSearchFolderDefinition
        ],
    );

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
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
    assert!(
        contains_bytes(&response_rops, &[0x03, 0x03, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    assert!(
        mapi_get_properties_specific_standard_row_offset(&response_rops, 3).is_ok(),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.WunderBar.SFInfo")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Unread Mail")));
    assert!(contains_bytes(&response_rops, definition_id.as_bytes()));
    assert!(contains_bytes(&response_rops, &2u32.to_le_bytes()));
    assert!(contains_bytes(
        &response_rops,
        &214_089_600u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &214_089_641u32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &0x48u32.to_le_bytes()));
    assert!(contains_bytes(&response_rops, &0u32.to_le_bytes()));
    assert!(contains_bytes(
        &response_rops,
        &[0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00]
    ));
    let mut row_offset =
        mapi_get_properties_specific_standard_row_offset(&response_rops, 3).unwrap() + 1;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut row_offset).unwrap(),
        "IPM.Microsoft.WunderBar.SFInfo"
    );
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut row_offset).unwrap(),
        "Unread Mail"
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        2
    );
    row_offset += 4;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        214_089_600
    );
    row_offset += 4;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        214_089_641
    );
    row_offset += 4;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        0x48
    );
    row_offset += 4;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        1_045_439_171
    );
    row_offset += 4;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        0
    );
    row_offset += 4;
    assert_eq!(
        read_rop_binary_u16(&response_rops, &mut row_offset).unwrap(),
        definition_id.as_bytes()
    );
    let definition_blob = read_rop_binary_u16(&response_rops, &mut row_offset).unwrap();
    assert_eq!(definition_blob.len(), 922);
    assert_eq!(
        &definition_blob[..8],
        &[0x04, 0x10, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00]
    );
    assert_eq!(&definition_blob[17..21], &[0x01, 0x00, 0x00, 0x00]);
    assert_eq!(
        &definition_blob[definition_blob.len() - 32..],
        &[
            0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x07, 0x0E, 0x01, 0x00, 0x00, 0x00, 0x06, 0x00,
            0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x97, 0x10, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ]
    );
}

#[tokio::test]
async fn mapi_over_http_common_view_named_view_accepts_microsoft_descriptor_write_stream_batch() {
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
    let descriptor = b"view";

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        crate::mapi_store::OUTLOOK_COMMON_VIEWS_COMPACT_NAMED_VIEW_ID,
    );
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream
    ]);
    rops.extend_from_slice(&0x7001_0102u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x2F, 0x00, 0x03, // RopSetStreamSize
    ]);
    rops.extend_from_slice(&(descriptor.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x2D, 0x00, 0x03, // RopWriteStream
    ]);
    rops.extend_from_slice(&(descriptor.len() as u16).to_le_bytes());
    rops.extend_from_slice(descriptor);
    rops.extend_from_slice(&[
        0x5D, 0x00, 0x03, // RopCommitStream
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
    assert!(contains_bytes(&response_rops, &[0x2B, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x2F, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x2D, 0x03, 0, 0, 0, 0, descriptor.len() as u8, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x5D, 0x03, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_destination_rejects_marker_and_subobject_streams() {
    for marker in [0x400C_0003u32, 0x4001_0003u32] {
        let mut rops = vec![0x02, 0x00, 0x00, 0x01];
        append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
        rops.push(0);
        rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
        rops.extend_from_slice(&0u16.to_le_bytes());
        append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
        rops.push(0);
        rops.extend_from_slice(&[0x53, 0x00, 0x02, 0x03, 0x01, 0x00]);
        rops.extend_from_slice(&[0x54, 0x00, 0x03]);
        rops.extend_from_slice(&4u16.to_le_bytes());
        rops.extend_from_slice(&marker.to_le_bytes());
        rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // Must not execute.

        let response_rops =
            execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]).await;

        assert!(contains_bytes(
            &response_rops,
            &[0x54, 0x03, 0x02, 0x01, 0x04, 0x80]
        ));
        assert!(!contains_bytes(
            &response_rops,
            &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
        ));
    }
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_destination_rejects_unsupported_property_type() {
    let mut transfer_data = Vec::new();
    transfer_data.extend_from_slice(&0x0037_000Du32.to_le_bytes()); // PtypObject.

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
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x54, 0x03, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_destination_rejects_partial_property_buffer() {
    let mut transfer_data = Vec::new();
    transfer_data.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    transfer_data.extend_from_slice(&12u32.to_le_bytes());
    transfer_data.extend_from_slice(&[b'L', 0]);

    let store = FakeStore {
        session: Some(FakeStore::account()),
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
    append_rop_save_changes_message(&mut rops, 2, 2); // Must not execute.

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x54, 0x03, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(&response_rops, &[0x0C, 0x02, 0, 0, 0, 0]));
    assert!(imported_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxocfg_release_persists_configuration_stream() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 67,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let account = FakeStore::account();
    let associated_configs = Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
        id: Uuid::parse_str("a0fdf7ca-15f8-bc62-ff51-d543d69a14a5").unwrap(),
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
            "0x65e00102": {
                "type": "binary",
                "value": associated_source_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            }
        }),
    }]));
    let store = FakeStore {
        session: Some(account.clone()),
        associated_configs: associated_configs.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let dictionary_stream =
        br#"<?xml version="1.0" encoding="utf-8"?><UserConfiguration xmlns="dictionary.xsd"><Info version="Outlook.16"/><Data><e k="18-OLPrefsVersion" v="9-1"/></Data></UserConfiguration>"#;

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        associated_object_id,
    );
    rops.extend_from_slice(&[0x2B, 0x00, 0x02, 0x03]); // RopOpenStream.
    rops.extend_from_slice(&0x7C07_0102u32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[0x2F, 0x00, 0x03]); // RopSetStreamSize.
    rops.extend_from_slice(&(dictionary_stream.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[0x2D, 0x00, 0x03]); // RopWriteStream.
    rops.extend_from_slice(&(dictionary_stream.len() as u16).to_le_bytes());
    rops.extend_from_slice(dictionary_stream);
    rops.extend_from_slice(&[0x01, 0x00, 0x03]); // RopRelease stream handle.
    append_rop_save_changes_message(&mut rops, 2, 2);

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
    assert!(contains_bytes(&response_rops, &[0x2B, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x2F, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x2D, 0x03, 0, 0, 0, 0, dictionary_stream.len() as u8, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x02, 0, 0, 0, 0]));

    let expected_stream_hex = dictionary_stream
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let configs = associated_configs.lock().unwrap();
    let config = configs
        .iter()
        .find(|config| config.message_class == "IPM.Configuration.MessageListSettings")
        .expect("updated associated config");
    assert_eq!(
        config.properties_json["0x7c070102"]["value"],
        serde_json::Value::String(expected_stream_hex)
    );
}

#[tokio::test]
async fn mapi_over_http_associated_config_mutations_are_cumulative_until_save() {
    let associated_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 68,
    );
    let associated_source_key =
        crate::mapi::identity::source_key_for_object_id(associated_object_id);
    let account = FakeStore::account();
    let associated_configs = Arc::new(Mutex::new(vec![crate::store::MapiAssociatedConfigRecord {
        id: Uuid::parse_str("4f3d59dd-2918-4ec6-86f5-f8ca0db18dc2").unwrap(),
        account_id: account.account_id,
        folder_id: crate::mapi::identity::INBOX_FOLDER_ID,
        message_class: "IPM.Configuration.MessageListSettings".to_string(),
        subject: "IPM.Configuration.MessageListSettings".to_string(),
        properties_json: serde_json::json!({
            "0x001a001f": {
                "type": "string",
                "value": "IPM.Configuration.MessageListSettings"
            },
            "0x65e00102": {
                "type": "binary",
                "value": associated_source_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            },
            "0x7c070102": {"type": "binary", "value": "0102"}
        }),
    }]));
    let store = FakeStore {
        session: Some(account),
        associated_configs: associated_configs.clone(),
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
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        associated_object_id,
    );
    for _ in 0..2 {
        rops.extend_from_slice(&[0x0A, 0x00, 0x02]); // RopSetProperties.
        rops.extend_from_slice(&12u16.to_le_bytes());
        rops.extend_from_slice(&1u16.to_le_bytes());
        rops.extend_from_slice(&0x7C06_0003u32.to_le_bytes());
        rops.extend_from_slice(&4i32.to_le_bytes());
    }
    rops.extend_from_slice(&[0x7A, 0x00, 0x02]); // RopDeletePropertiesNoReplicate.
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x7C08_0102u32.to_le_bytes());
    append_rop_save_changes_message(&mut rops, 2, 2);

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
    assert!(contains_bytes(&response_rops, &[0x0C, 0x02, 0, 0, 0, 0]));
    let configs = associated_configs.lock().unwrap();
    let config = configs
        .iter()
        .find(|config| config.message_class == "IPM.Configuration.MessageListSettings")
        .expect("updated associated config");
    assert_eq!(config.properties_json["0x7c060003"]["value"], 4);
    assert_eq!(config.properties_json["0x7c070102"]["value"], "0102");
    assert!(config.properties_json.get("0x7c080102").is_none());
}

#[tokio::test]
async fn mapi_over_http_get_properties_specific_returns_folder_properties() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 7;
    inbox.unread_emails = 2;
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3602_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3603_0003u32.to_le_bytes());

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
    let get_props_offset = 8;

    assert_eq!(response_rops[get_props_offset], 0x07);
    assert_eq!(response_rops[get_props_offset + 1], 0x01);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[get_props_offset + 2..get_props_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(response_rops, &7u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &2u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_folder_set_properties_rejects_protocol_local_values() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let state = [0x11, 0x22, 0x33, 0x44, 0x55];
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36D0_0102, &state);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on opened folder
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on same folder
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[
            0x0A, 0x01, 0, 0, 0, 0, 1, 0, // one PropertyProblem
            0, 0, // index 0
            0x02, 0x01, 0xD0, 0x36, // property tag
            0x02, 0x01, 0x04, 0x80, // MAPI_E_NO_SUPPORT
        ]
    ));
    assert!(!contains_bytes(&response_rops, &state));
}

#[tokio::test]
async fn mapi_over_http_folder_set_properties_accepts_additional_ren_entry_ids() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let conflicts = test_mapi_folder_id(0x1b).to_le_bytes();
    let local_failures = test_mapi_folder_id(0x1c).to_le_bytes();
    let server_failures = test_mapi_folder_id(0x1d).to_le_bytes();
    let junk = test_mapi_folder_id(0x1e).to_le_bytes();
    let mut property_values = Vec::new();
    append_mapi_multi_binary_property(
        &mut property_values,
        0x36D8_1102,
        &[&conflicts, &local_failures, &server_failures, &junk],
    );

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on opened folder
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_folder_set_properties_accepts_additional_ren_entry_ids_ex() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let client_additional_ren_ex = vec![0x7a; 490];
    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
        &client_additional_ren_ex,
    );

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, folder_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on opened folder
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_folder_open_stream_reads_computed_binary_property() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let property_tag: u32 = 0x36D9_0102;
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    rops.extend_from_slice(&[0x2B, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&property_tag.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x2C, 0x00, 0x02]);
    rops.extend_from_slice(&32u16.to_le_bytes());

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
    assert!(
        contains_bytes(&response_rops, &[0x2B, 0x02, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x2C, 0x02, 0, 0, 0, 0, 32, 0]),
        "{response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_folder_open_stream_returns_empty_missing_binary_property() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let property_tag: u32 = 0x66AB_0102;
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    rops.extend_from_slice(&[0x2B, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&property_tag.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x2C, 0x00, 0x02]);
    rops.extend_from_slice(&32u16.to_le_bytes());

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
    assert!(contains_bytes(
        &response_rops,
        &[0x2B, 0x02, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x2C, 0x02, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_root_set_properties_accepts_additional_ren_entry_ids_as_cache_write() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let root_folder_id = test_mapi_folder_id(1);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let conflicts = test_mapi_folder_id(0x1b).to_le_bytes();
    let sync_issues = test_mapi_folder_id(0x1a).to_le_bytes();
    let local_failures = test_mapi_folder_id(0x1c).to_le_bytes();
    let server_failures = test_mapi_folder_id(0x1d).to_le_bytes();
    let mut property_values = Vec::new();
    append_mapi_multi_binary_property(
        &mut property_values,
        0x36D8_1102,
        &[&conflicts, &sync_issues, &local_failures, &server_failures],
    );

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, root_folder_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on opened folder
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_folder_set_properties_do_not_survive_as_protocol_state() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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

    let state = [0x21, 0x32, 0x43, 0x54, 0x65];
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36D0_0102, &state);

    let mut set_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut set_rops, folder_id);
    set_rops.push(0);
    set_rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on opened folder
    ]);
    set_rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    set_rops.extend_from_slice(&1u16.to_le_bytes());
    set_rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&set_rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");

    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = reconnect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut get_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut get_rops, folder_id);
    get_rops.push(0);
    get_rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on reopened folder
    ]);
    get_rops.extend_from_slice(&4096u16.to_le_bytes());
    get_rops.extend_from_slice(&1u16.to_le_bytes());
    get_rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&get_rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!contains_bytes(&response_rops, &state));
}

#[tokio::test]
async fn mapi_over_http_root_default_folder_set_properties_do_not_override_computed_defaults() {
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

    let default_calendar_eid = [0xCA, 0x1E, 0xAD, 0xA7, 0x01, 0x02];
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36D0_0102, &default_calendar_eid);

    let mut set_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut set_rops, crate::mapi::identity::ROOT_FOLDER_ID);
    set_rops.push(0);
    set_rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on root
    ]);
    set_rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    set_rops.extend_from_slice(&1u16.to_le_bytes());
    set_rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&set_rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");

    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = reconnect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut get_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut get_rops, crate::mapi::identity::ROOT_FOLDER_ID);
    get_rops.push(0);
    get_rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on reopened root
    ]);
    get_rops.extend_from_slice(&4096u16.to_le_bytes());
    get_rops.extend_from_slice(&1u16.to_le_bytes());
    get_rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&get_rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let expected_calendar_eid = crate::mapi::identity::long_term_id_from_object_id(
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap()
    .to_vec();
    assert!(contains_bytes(&response_rops, &expected_calendar_eid));
    assert!(!contains_bytes(&response_rops, &default_calendar_eid));
}

#[tokio::test]
async fn mapi_over_http_root_default_folder_set_properties_reject_invalid_entry_ids() {
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

    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36D0_0102, &[0xCA, 0x1E, 0xAD, 0xA7]);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on root
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[
            0x0A, 0x01, 0, 0, 0, 0, 1, 0, // one PropertyProblem
            0, 0, // index 0
            0x02, 0x01, 0xD0, 0x36, // PidTagIpmAppointmentEntryId
            0x02, 0x01, 0x04, 0x80, // InvalidParameter
        ]
    ));
}

#[tokio::test]
async fn mapi_over_http_root_default_folder_set_properties_accept_valid_entry_ids() {
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

    let calendar_eid = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36D0_0102, &calendar_eid);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on root
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0x00, 0x00, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_root_default_folder_set_properties_accepts_cached_rem_online_entry_id() {
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

    let reminders_eid = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::REMINDERS_FOLDER_ID,
    )
    .unwrap();
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36D5_0102, &reminders_eid);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on root
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0x00, 0x00, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_root_default_folder_get_properties_returns_canonical_entry_ids() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&16u16.to_le_bytes());
    rops.extend_from_slice(&0x3601_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x35E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x35E2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x35E3_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x35E4_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x35E5_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x35E6_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x35E7_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x35FF_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x36D1_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x36D2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x36D3_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x36D4_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x36D7_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let get_props_offset = 8;
    assert_eq!(response_rops[get_props_offset], 0x07);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[get_props_offset + 2..get_props_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(&response_rops, b"R\0o\0o\0t\0\0\0"));
    for folder_id in [
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        crate::mapi::identity::OUTBOX_FOLDER_ID,
        crate::mapi::identity::TRASH_FOLDER_ID,
        crate::mapi::identity::SENT_FOLDER_ID,
        crate::mapi::identity::VIEWS_FOLDER_ID,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        crate::mapi::identity::SEARCH_FOLDER_ID,
        crate::mapi::identity::ARCHIVE_FOLDER_ID,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        crate::mapi::identity::JOURNAL_FOLDER_ID,
        crate::mapi::identity::NOTES_FOLDER_ID,
        crate::mapi::identity::TASKS_FOLDER_ID,
        crate::mapi::identity::DRAFTS_FOLDER_ID,
    ] {
        let entry_id = crate::mapi::identity::long_term_id_from_object_id(folder_id)
            .unwrap()
            .to_vec();
        assert!(contains_bytes(&response_rops, &entry_id));
    }
}

#[tokio::test]
async fn mapi_over_http_folder_get_properties_specific_flags_unknown_properties() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x66AA_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let get_props_offset = 8;
    assert_eq!(response_rops[get_props_offset], 0x07);
    assert_eq!(response_rops[get_props_offset + 6], 1);
    assert_eq!(response_rops[get_props_offset + 7], 0x0A);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[get_props_offset + 8..get_props_offset + 12]
                .try_into()
                .unwrap()
        ),
        0x8004_010F
    );
}

#[tokio::test]
async fn mapi_over_http_folder_get_properties_ignores_stale_protocol_local_folder_state() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        stale_protocol_local_folder_properties: Arc::new(Mutex::new(HashMap::from([(
            (
                crate::mapi::identity::INBOX_FOLDER_ID,
                PID_TAG_DISPLAY_NAME_W,
            ),
            utf16z("Stale Protocol Name"),
        )]))),
        ..Default::default()
    };
    assert!(store
        .stale_protocol_local_folder_properties
        .lock()
        .unwrap()
        .contains_key(&(
            crate::mapi::identity::INBOX_FOLDER_ID,
            PID_TAG_DISPLAY_NAME_W
        )));
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Stale Protocol Name")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_copy_to_null_destination_response_keeps_batch_aligned() {
    let rops = [
        0x39, 0x00, 0x00, 0x01, // RopCopyTo: source handle 0, destination handle 1.
        0x00, 0x00, 0x00, // WantAsynchronous, WantSubObjects, CopyFlags.
        0x00, 0x00, // ExcludedTagCount.
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ];
    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x39, 0x00, 0x03, 0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_copy_to_copies_custom_values_excluding_tags() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let source_message_id = Uuid::parse_str("47474747-4747-4747-4747-474747474751").unwrap();
    let destination_message_id = Uuid::parse_str("57575757-5757-5757-5757-575757575752").unwrap();
    let source_mapi_id = test_mapi_uuid_id(&source_message_id);
    let destination_mapi_id = test_mapi_uuid_id(&destination_message_id);
    crate::mapi::identity::remember_mapi_identity(source_message_id, source_mapi_id);
    crate::mapi::identity::remember_mapi_identity(destination_message_id, destination_mapi_id);
    let copied_tag = 0x8001_001F_u32;
    let excluded_tag = 0x8002_001F_u32;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &source_message_id.to_string(),
                inbox_id,
                "inbox",
                "CopyTo source",
            ),
            FakeStore::email(
                &destination_message_id.to_string(),
                inbox_id,
                "inbox",
                "CopyTo destination",
            ),
        ])),
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

    let folder_id = test_mapi_folder_id(5);
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, copied_tag, "copied by CopyTo");
    append_mapi_utf16_property(&mut property_values, excluded_tag, "excluded by CopyTo");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, source_mapi_id);
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 2);
    append_rop_open_message(&mut rops, 1, 3, folder_id, destination_mapi_id);
    rops.extend_from_slice(&[
        0x39, 0x00, 0x02, 0x03, // RopCopyTo: source handle 2, destination handle 3.
        0x00, 0x00, 0x00, // WantAsynchronous, WantSubObjects, CopyFlags.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&excluded_tag.to_le_bytes());

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
    assert!(
        contains_bytes(&response_rops, &[0x39, 0x02, 0, 0, 0, 0, 0, 0]),
        "response_rops={response_rops:02x?}"
    );
    let copied = store
        .fetch_mapi_custom_property_values(
            FakeStore::account().account_id,
            MapiCustomPropertyObjectKind::Message,
            destination_message_id,
            &[copied_tag, excluded_tag],
        )
        .await
        .unwrap();
    assert_eq!(copied.len(), 1);
    assert_eq!(copied[0].property_tag, copied_tag);
    assert_eq!(copied[0].property_value, utf16z("copied by CopyTo"));
}

#[tokio::test]
async fn mapi_over_http_microsoft_copy_properties_null_destination_response_keeps_batch_aligned() {
    let mut rops = vec![
        0x67, 0x00, 0x00, 0x01, // RopCopyProperties: source handle 0, destination handle 1.
        0x00, 0x00, // WantAsynchronous, CopyFlags.
    ];
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // RopGetStoreState proves the batch stayed aligned.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x67, 0x00, 0x03, 0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_copy_properties_empty_tag_list_succeeds_as_noop() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x67, 0x00, 0x00, 0x01, // RopCopyProperties: source handle 0, destination handle 1.
        0x00, 0x00, // WantAsynchronous, CopyFlags.
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // RopGetStoreState proves the batch stayed aligned.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x67, 0x00, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_copy_properties_copies_custom_values_and_reports_missing_tags() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let source_message_id = "47474747-4747-4747-4747-474747474741";
    let destination_message_id = "48484848-4848-4848-4848-484848484842";
    let source_message_uuid = Uuid::parse_str(source_message_id).unwrap();
    let destination_message_uuid = Uuid::parse_str(destination_message_id).unwrap();
    let source_mapi_id = test_mapi_uuid_id(&source_message_uuid);
    let destination_mapi_id = test_mapi_uuid_id(&destination_message_uuid);
    crate::mapi::identity::remember_mapi_identity(source_message_uuid, source_mapi_id);
    crate::mapi::identity::remember_mapi_identity(destination_message_uuid, destination_mapi_id);
    let custom_tag = 0x8001_001F_u32;
    let missing_custom_tag = 0x8002_001F_u32;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(source_message_id, inbox_id, "inbox", "Copy source"),
            FakeStore::email(
                destination_message_id,
                inbox_id,
                "inbox",
                "Copy destination",
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

    let folder_id = test_mapi_folder_id(5);
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, custom_tag, "copied opaque value");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, source_mapi_id);
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 2);
    append_rop_open_message(&mut rops, 1, 3, folder_id, destination_mapi_id);
    rops.extend_from_slice(&[
        0x67, 0x00, 0x02, 0x03, // RopCopyProperties: source handle 2, destination handle 3.
        0x00, 0x00, // WantAsynchronous, CopyFlags.
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&custom_tag.to_le_bytes());
    rops.extend_from_slice(&missing_custom_tag.to_le_bytes());
    append_rop_get_properties_specific(&mut rops, 3, &[custom_tag]);

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
    let mut expected_problem = vec![
        0x67, 0x02, 0, 0, 0, 0, // RopCopyProperties success.
        1, 0, // one PropertyProblem.
        1, 0, // index of missing_custom_tag in PropertyTags.
    ];
    expected_problem.extend_from_slice(&missing_custom_tag.to_le_bytes());
    expected_problem.extend_from_slice(&0x8004_010Fu32.to_le_bytes());
    assert!(
        contains_bytes(&response_rops, &expected_problem),
        "response_rops={response_rops:02x?}; expected_problem={expected_problem:02x?}"
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("copied opaque value")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_copy_properties_copies_message_followup_values() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let source_message_id = "61616161-6161-6161-6161-616161616161";
    let destination_message_id = "62626262-6262-6262-6262-626262626262";
    let destination_uuid = Uuid::parse_str(destination_message_id).unwrap();
    let mut source = FakeStore::email(source_message_id, inbox_id, "inbox", "Follow-up source");
    source.unread = true;
    source.flagged = true;
    let mut destination = FakeStore::email(
        destination_message_id,
        inbox_id,
        "inbox",
        "Follow-up destination",
    );
    destination.unread = false;
    destination.flagged = false;
    let emails = Arc::new(Mutex::new(vec![source, destination]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
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

    let folder_id = test_mapi_folder_id(5);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        folder_id,
        test_mapi_message_id(source_message_id),
    );
    append_rop_open_message(
        &mut rops,
        1,
        3,
        folder_id,
        test_mapi_message_id(destination_message_id),
    );
    rops.extend_from_slice(&[
        0x67, 0x00, 0x02, 0x03, // RopCopyProperties: source handle 2, destination handle 3.
        0x00, 0x00, // WantAsynchronous, CopyFlags.
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_MESSAGE_FLAGS.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_FLAG_STATUS.to_le_bytes());

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
    assert!(
        contains_bytes(&response_rops, &[0x67, 0x02, 0, 0, 0, 0, 0, 0]),
        "response_rops={response_rops:02x?}"
    );
    let stored = emails.lock().unwrap();
    let copied = stored
        .iter()
        .find(|email| email.id == destination_uuid)
        .unwrap();
    assert!(copied.unread);
    assert!(copied.flagged);
}

#[tokio::test]
async fn mapi_over_http_microsoft_property_rops_reject_missing_input_handle_without_batch_drift() {
    let mut rops = vec![
        0x08, 0x00, 0x01, // RopGetPropertiesAll on missing handle 1.
    ];
    rops.extend_from_slice(&0x1000u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x09, 0x00, 0x01, // RopGetPropertiesList on missing handle 1.
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x08, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x09, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_unknown_property_type_terminates_current_buffer() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_2222u32.to_le_bytes()); // Unknown property type.
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01, 0x01, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x05, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x12, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(!contains_bytes(&response_rops, &[0x15, 0x02]));
}
