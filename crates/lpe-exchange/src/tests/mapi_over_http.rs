use super::*;

fn exchange_reminder_excluded_folder_roles() -> Vec<String> {
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

fn test_account_legacy_dn(email: &str) -> String {
    test_legacy_dn(email)
}

fn test_contact_legacy_dn(email: &str, id: &str) -> String {
    test_legacy_dn(&format!("{email}-{id}"))
}

fn test_legacy_dn(source: &str) -> String {
    let legacy_cn = source
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={legacy_cn}")
}

fn test_category_id(folder_id: u64, property_tag: u32, value: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in folder_id
        .to_le_bytes()
        .into_iter()
        .chain(property_tag.to_le_bytes())
        .chain(value.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash | 0x8000_0000_0000_0000
}

fn test_collapse_state(folder_id: u64, row_id: u64, position: u32, category_id: u64) -> Vec<u8> {
    let mut state = Vec::new();
    state.extend_from_slice(b"LPECS1");
    state.extend_from_slice(&folder_id.to_le_bytes());
    state.extend_from_slice(&row_id.to_le_bytes());
    state.extend_from_slice(&0u32.to_le_bytes());
    state.extend_from_slice(&position.to_le_bytes());
    state.extend_from_slice(&1u16.to_le_bytes());
    state.extend_from_slice(&1u16.to_le_bytes());
    state.extend_from_slice(&1u16.to_le_bytes());
    state.extend_from_slice(&category_id.to_le_bytes());
    state
}

#[tokio::test]
async fn mapi_over_http_contact_crud_uses_canonical_contacts() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let contacts = store.contacts.clone();
    let deleted_contacts = store.deleted_contacts.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "RCA Contact");
    append_mapi_utf16_property(&mut property_values, 0x39FE_001F, "rca@example.test");
    append_mapi_utf16_property(&mut property_values, 0x3A1C_001F, "+49 30 123456");
    append_mapi_utf16_property(&mut property_values, 0x3A16_001F, "Interop Team");
    append_mapi_utf16_property(&mut property_values, 0x3A17_001F, "Coordinator");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Created through MAPI");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(15));
    append_rop_set_properties(&mut rops, 1, 6, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x3001_001F, 0x39FE_001F, 0x3A1C_001F]);
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
    let _response_rops = response_rops_from_execute_response(response).await;
    {
        let stored = contacts.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, "RCA Contact");
        assert_eq!(stored[0].email, "rca@example.test");
        assert_eq!(stored[0].phone, "+49 30 123456");
        assert_eq!(stored[0].team, "Interop Team");
        assert_eq!(stored[0].role, "Coordinator");
        assert_eq!(stored[0].notes, "Created through MAPI");
    }

    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x3001_001F, "Updated RCA Contact");
    append_mapi_utf16_property(&mut update_values, 0x39FE_001F, "updated@example.test");
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&update_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    {
        let stored = contacts.lock().unwrap();
        assert_eq!(stored[0].name, "Updated RCA Contact");
        assert_eq!(stored[0].email, "updated@example.test");
    }

    let mut read_rops = Vec::new();
    append_rop_open_folder(&mut read_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut read_rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    append_rop_get_properties_specific(&mut read_rops, 2, &[0x3001_001F, 0x39FE_001F]);
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
        &utf16z("Updated RCA Contact")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("updated@example.test")
    ));

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&contact_id)]);
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
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    assert!(contacts.lock().unwrap().is_empty());
    assert_eq!(deleted_contacts.lock().unwrap().as_slice(), &[contact_id]);
}

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
        append_rop_open_message(
            &mut set_rops,
            folder_handle,
            object_handle,
            *folder_id,
            *object_id,
        );
        append_rop_set_properties(&mut set_rops, object_handle, 1, &property_values);
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
    let stored_values = stored_custom_values.lock().unwrap();
    assert_eq!(stored_values.len(), cases.len());
    for (_, _, _, value) in cases.iter() {
        assert!(
            stored_values
                .values()
                .any(|stored| contains_bytes(stored, &utf16z(*value))),
            "missing stored custom value {value}"
        );
    }
    drop(stored_values);

    let mut delete_rops = Vec::new();
    for (index, (folder_id, object_id, tag, _value)) in cases.iter().enumerate() {
        let folder_handle = 1 + (index as u8) * 2;
        let object_handle = folder_handle + 1;
        append_rop_open_folder(&mut delete_rops, 0, folder_handle, *folder_id);
        append_rop_open_message(
            &mut delete_rops,
            folder_handle,
            object_handle,
            *folder_id,
            *object_id,
        );
        append_rop_delete_properties(&mut delete_rops, object_handle, &[*tag]);
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
async fn mapi_over_http_set_properties_rejects_unsupported_canonical_contact_property() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let contacts = Arc::new(Mutex::new(vec![FakeStore::contact(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "RCA Contact",
        "rca@example.test",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: contacts.clone(),
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3613_001F, "IPF.Note");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie);
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
        &[0x0A, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert_eq!(contacts.lock().unwrap()[0].name, "RCA Contact");
}

#[tokio::test]
async fn mapi_over_http_calendar_crud_uses_canonical_events() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let events = store.events.clone();
    let deleted_events = store.deleted_events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "RCA Calendar");
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-05-04", "09:30"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0061_0040,
        test_filetime("2026-05-04", "10:15"),
    );
    append_mapi_utf16_property(&mut property_values, 0x3FFB_001F, "Room 1");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Agenda");
    append_mapi_i32_property(&mut property_values, 0x8205_0003, 1);
    append_mapi_utf16_property(&mut property_values, 0x0C1A_001F, "Alice Organizer");
    append_mapi_utf16_property(
        &mut property_values,
        0x0C1F_001F,
        "Alice.Organizer@Example.Test",
    );
    append_mapi_utf16_property(&mut property_values, 0x0E04_001F, "Bob Attendee");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_set_properties(&mut rops, 1, 9, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x0037_001F, 0x0060_0040, 0x0061_0040]);
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
    let _response_rops = response_rops_from_execute_response(response).await;
    {
        let stored = events.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].title, "RCA Calendar");
        assert_eq!(stored[0].date, "2026-05-04");
        assert_eq!(stored[0].time, "09:30");
        assert_eq!(stored[0].duration_minutes, 45);
        assert_eq!(stored[0].location, "Room 1");
        assert_eq!(stored[0].notes, "Agenda");
        assert_eq!(stored[0].status, "tentative");
        assert_eq!(stored[0].attendees, "Bob Attendee");
        assert!(stored[0]
            .organizer_json
            .contains("alice.organizer@example.test"));
        assert!(stored[0].attendees_json.contains("Bob Attendee"));
        assert!(stored[0].recurrence_rule.is_empty());
    }

    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x0037_001F, "Updated RCA Calendar");
    append_mapi_utf16_property(&mut update_values, 0x3FFB_001F, "Room 2");
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&update_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    {
        let stored = events.lock().unwrap();
        assert_eq!(stored[0].title, "Updated RCA Calendar");
        assert_eq!(stored[0].location, "Room 2");
    }

    let mut read_rops = Vec::new();
    append_rop_open_folder(&mut read_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut read_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_get_properties_specific(&mut read_rops, 2, &[0x0037_001F, 0x3FFB_001F]);
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
        &utf16z("Updated RCA Calendar")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Room 2")));

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&event_id)]);
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
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    assert!(events.lock().unwrap().is_empty());
    assert_eq!(deleted_events.lock().unwrap().as_slice(), &[event_id]);
}

#[tokio::test]
async fn mapi_over_http_calendar_attachment_save_uses_canonical_event_attachments() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
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
            title: "Attached Calendar".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let calendar_attachments = store.calendar_attachments.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
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

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());

    let mut attachment_properties = Vec::new();
    append_mapi_utf16_property(&mut attachment_properties, 0x3707_001F, "agenda.pdf");
    append_mapi_utf16_property(&mut attachment_properties, 0x370E_001F, "application/pdf");
    append_mapi_binary_property(&mut attachment_properties, 0x3701_0102, b"%PDF-calendar");
    let mut attachment_rops = Vec::new();
    append_rop_open_folder(&mut attachment_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut attachment_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    attachment_rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    attachment_rops.extend_from_slice(&((attachment_properties.len() + 2) as u16).to_le_bytes());
    attachment_rops.extend_from_slice(&3u16.to_le_bytes());
    attachment_rops.extend_from_slice(&attachment_properties);
    attachment_rops.extend_from_slice(&[
        0x25, 0x00, 0x02, 0x03, 0x00, // RopSaveChangesAttachment
    ]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &attachment_rops,
                &[1, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x23, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x25, 0x02, 0, 0, 0, 0]));

    let stored = calendar_attachments.lock().unwrap();
    let event_attachments = stored.get(&event_id).unwrap();
    assert_eq!(event_attachments.len(), 1);
    assert_eq!(event_attachments[0].file_name, "agenda.pdf");
    assert_eq!(event_attachments[0].media_type, "application/pdf");
    assert!(event_attachments[0]
        .file_reference
        .starts_with("calendar-attachment:"));
}

#[tokio::test]
async fn mapi_over_http_task_crud_uses_canonical_tasks() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "tasks", "Tasks",
        )])),
        ..Default::default()
    };
    let tasks = store.tasks.clone();
    let deleted_tasks = store.deleted_tasks.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "RCA Task");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Created through MAPI");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(19));
    append_rop_set_properties(&mut rops, 1, 2, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x0037_001F, 0x1000_001F]);
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
    let _response_rops = response_rops_from_execute_response(response).await;
    {
        let stored = tasks.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].title, "RCA Task");
        assert_eq!(stored[0].description, "Created through MAPI");
        assert_eq!(stored[0].status, "needs-action");
    }

    let task_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x0037_001F, "Updated RCA Task");
    append_mapi_i32_property(&mut update_values, 0x1090_0003, 1);
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(19));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(19),
        test_mapi_uuid_id(&task_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&update_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    {
        let stored = tasks.lock().unwrap();
        assert_eq!(stored[0].title, "Updated RCA Task");
        assert_eq!(stored[0].status, "completed");
    }

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(19));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&task_id)]);
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
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    assert!(tasks.lock().unwrap().is_empty());
    assert_eq!(deleted_tasks.lock().unwrap().as_slice(), &[task_id]);
}

#[tokio::test]
async fn mapi_over_http_task_contents_table_lists_canonical_tasks() {
    let task_list_id = "99999999-9999-9999-9999-999999999999";
    let task = FakeStore::task(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        task_list_id,
        "Existing RCA Task",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "tasks", "Tasks",
        )])),
        tasks: Arc::new(Mutex::new(vec![task])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut cookie = mapi_cookie_header(&connect);
    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x01];
    let legacy_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}\0",
        FakeStore::account().email
    );
    logon_rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rops.extend_from_slice(legacy_dn.as_bytes());
    let mut logon_headers = mapi_headers("Execute");
    logon_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &logon_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    cookie = mapi_cookie_header(&logon_response);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(19));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&10u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &utf16z("Existing RCA Task")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Task")));
}

#[tokio::test]
async fn mapi_over_http_connect_creates_emsmdb_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/mapi-http"
    );
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("x-serverapplication").unwrap(),
        "Exchange/15.20.0485.000"
    );
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
    assert_eq!(response.headers().get("x-pendingperiod").unwrap(), "15000");
    let content_length = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let set_cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.starts_with("MapiContext="));
    assert!(set_cookie.contains("Max-Age=1800"));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("Secure"));
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

    let raw_body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(content_length, raw_body.len());
    assert!(raw_body.starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 0\r\n"));
    let body = strip_mapi_http_envelope(raw_body);
    assert_eq!(&body[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(&body[8..12], &60_000u32.to_le_bytes());
    assert_eq!(&body[12..16], &6u32.to_le_bytes());
    assert_eq!(&body[16..20], &10_000u32.to_le_bytes());
    assert!(body[20..].starts_with(b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=\0"));
    assert_eq!(
        &body[body.len() - 20..body.len() - 16],
        &16u32.to_le_bytes()
    );
    assert_eq!(
        &body[body.len() - 16..],
        &[
            0x00, 0x00, // RPC_HEADER_EXT Version
            0x04, 0x00, // Last flag
            0x08, 0x00, // Payload size
            0x08, 0x00, // Uncompressed payload size
            0x08, 0x00, // AUX_HEADER Size
            0x01, // AUX_HEADER Version
            0x17, // AUX_EXORGINFO
            0x00, 0x00, 0x00, 0x00, // OrgFlags
        ]
    );
}

#[tokio::test]
async fn mapi_over_http_transport_echoes_request_id_and_client_info() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request_id = "{11111111-2222-3333-4444-555555555555}:7001";
    let client_info = "{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}:7002";
    let mut headers = mapi_headers("Connect");
    headers.insert("x-requestid", HeaderValue::from_static(request_id));
    headers.insert("x-clientinfo", HeaderValue::from_static(client_info));

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requestid").unwrap(), request_id);
    assert_eq!(response.headers().get("x-clientinfo").unwrap(), client_info);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_transport_maps_response_code_to_header_and_envelope() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_with_request_id("Connect", "not-a-guid-counter"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let content_length = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let raw_body = raw_response_bytes(response).await;
    assert_eq!(content_length, raw_body.len());
    assert!(raw_body.starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 4\r\n"));
    assert!(String::from_utf8_lossy(&raw_body).contains("invalid MAPI X-RequestId header"));
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
async fn mapi_over_http_connect_ignores_mismatched_sequence_cookie_on_reconnect() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut reconnect_headers = mapi_headers("Connect");
    reconnect_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header_with_mismatched_sequence(&connect)).unwrap(),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
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
async fn mapi_over_http_connect_preserves_previous_cookie_for_follow_up_execute() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let previous_cookie = mapi_cookie_header(&connect);

    let mut reconnect_headers = mapi_headers("Connect");
    reconnect_headers.insert("cookie", HeaderValue::from_str(&previous_cookie).unwrap());
    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(reconnect.headers().get("x-responsecode").unwrap(), "0");

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rop.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&previous_cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rop, &[])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_execute_prefers_latest_duplicate_session_cookie() {
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
    let stale_id = "00000000-0000-0000-0000-000000000000";
    let duplicate_cookie = format!("MapiContext={stale_id}; MapiSequence={stale_id}; {cookie}");

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rop.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&duplicate_cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rop, &[])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_execute_prefers_latest_cookie_header() {
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
    let stale_id = "00000000-0000-0000-0000-000000000000";

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rop.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.append(
        "cookie",
        HeaderValue::from_str(&format!("MapiContext={stale_id}; MapiSequence={stale_id}")).unwrap(),
    );
    execute_headers.append("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rop, &[])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_request_id_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_request_id("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI X-RequestId header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_request_type_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_request_type(),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Unknown");
    assert!(response
        .headers()
        .get("x-requestid")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("{11111111-2222-3333-4444-555555555555}:"));
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI X-RequestType header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_unknown_request_type_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("BogusRequest"), b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "BogusRequest"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "5");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI X-RequestType header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_client_info_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_client_info("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert!(response
        .headers()
        .get("x-requestid")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("{11111111-2222-3333-4444-555555555555}:"));
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    assert!(response.headers().get("x-clientinfo").is_none());
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI X-ClientInfo header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_invalid_client_info_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_with_client_info("Connect", "not-a-guid-counter"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    assert_eq!(
        response.headers().get("x-clientinfo").unwrap(),
        "not-a-guid-counter"
    );
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI X-ClientInfo header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_host_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_host("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI Host header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_content_length_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_content_length("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI Content-Length header"));
}

#[tokio::test]
async fn mapi_over_http_response_content_length_covers_full_mapi_envelope() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();

    let content_length = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let raw_body = raw_response_bytes(response).await;
    assert_eq!(content_length, raw_body.len());
    assert!(raw_body.starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 0\r\n"));
}

#[tokio::test]
async fn mapi_over_http_rejects_invalid_content_length_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_with_content_length("Connect", "not-a-length"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI Content-Length header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_mismatched_content_length_without_canonical_mutation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let request = mapi_submit_execute_body("Mismatched length submit");

    let mut execute_headers = mapi_headers_with_content_length("Execute", "1");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("Content-Length header does not match request body length"));
    assert!(submitted_messages.lock().unwrap().is_empty());
    assert!(emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_rejects_invalid_request_id_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_with_request_id("Connect", "not-a-guid-counter"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(
        response.headers().get("x-requestid").unwrap(),
        "not-a-guid-counter"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI X-RequestId header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_content_type() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_content_type("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = response_bytes(response).await;
    let message = String::from_utf8_lossy(&body);
    assert!(message.contains("Content-Type application/mapi-http"));
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
async fn mapi_over_http_accepts_rca_octet_stream_resolve_names_probe() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = nspi_bound_headers(&service, "ResolveNames").await;
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("103"),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_static("520bfd13-f3a9-45c4-abec-6ef0a2541db9:2"),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_static("c9a1f6bb-76d3-41a1-8abb-fc60106a4a97:1"),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 103])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "ResolveNames"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_disconnect_consumes_emsmdb_session() {
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

    let mut disconnect_headers = mapi_headers("Disconnect");
    disconnect_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &disconnect_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "Disconnect"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert!(response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
    let body = response_bytes(response).await;
    assert_eq!(body.len(), 12);
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 0);
}

#[tokio::test]
async fn mapi_over_http_execute_rejects_missing_and_malformed_session_cookies() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let request = mapi_submit_execute_body("Rejected cookie submit");

    let missing = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Execute"), &request)
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::OK);
    assert_eq!(missing.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(missing.headers().get("x-responsecode").unwrap(), "13");
    assert!(String::from_utf8(response_bytes(missing).await)
        .unwrap()
        .contains("missing MAPI session cookie"));

    let mut malformed_headers = mapi_headers("Execute");
    malformed_headers.insert(
        "cookie",
        HeaderValue::from_static("MapiContext=; MapiSequence="),
    );
    let malformed = service
        .handle_mapi(MapiEndpoint::Emsmdb, &malformed_headers, &request)
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::OK);
    assert_eq!(malformed.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(malformed.headers().get("x-responsecode").unwrap(), "13");
    assert!(String::from_utf8(response_bytes(malformed).await)
        .unwrap()
        .contains("missing MAPI session cookie"));
    assert!(submitted_messages.lock().unwrap().is_empty());
    assert!(emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_disconnect_rejects_stale_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut disconnect_headers = mapi_headers("Disconnect");
    disconnect_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let disconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &disconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(disconnect.headers().get("x-responsecode").unwrap(), "0");

    let mut stale_headers = mapi_headers("Disconnect");
    stale_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let stale = service
        .handle_mapi(MapiEndpoint::Emsmdb, &stale_headers, b"")
        .await
        .unwrap();

    assert_eq!(stale.status(), StatusCode::OK);
    assert_eq!(stale.headers().get("x-requesttype").unwrap(), "Disconnect");
    assert_eq!(stale.headers().get("x-responsecode").unwrap(), "10");
    assert!(String::from_utf8(response_bytes(stale).await)
        .unwrap()
        .contains("MAPI session context not found"));
    assert!(submitted_messages.lock().unwrap().is_empty());
    assert!(emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_notification_wait_refreshes_emsmdb_session_cookie() {
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

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "NotificationWait"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=") && cookie.contains("Max-Age=1800")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=") && cookie.contains("Max-Age=1800")));
    let body = response_bytes(response).await;
    assert_eq!(body.len(), 16);
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 0);
}

#[tokio::test]
async fn mapi_over_http_ping_requires_and_refreshes_session_cookie() {
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

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    assert!(response_bytes(response).await.is_empty());

    let missing_cookie = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("PING"), b"")
        .await
        .unwrap();
    assert_eq!(
        missing_cookie.headers().get("x-responsecode").unwrap(),
        "13"
    );
    assert!(String::from_utf8(response_bytes(missing_cookie).await)
        .unwrap()
        .contains("missing MAPI session cookie"));

    let mut invalid_body_headers = mapi_headers("PING");
    invalid_body_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let invalid_body = service
        .handle_mapi(MapiEndpoint::Emsmdb, &invalid_body_headers, b"not-empty")
        .await
        .unwrap();
    assert_eq!(invalid_body.headers().get("x-responsecode").unwrap(), "12");
}

#[tokio::test]
async fn mapi_over_http_ping_rejects_mismatched_sequence_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let bad_cookie = mapi_cookie_header_with_mismatched_sequence(&connect);

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&bad_cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "6");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI request sequence cookie"));
}

#[tokio::test]
async fn mapi_over_http_ping_rejects_nonzero_content_length() {
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

    let mut ping_headers = mapi_headers_with_content_length("PING", "1");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("PING requests must use Content-Length 0"));
}

#[tokio::test]
async fn mapi_over_http_ping_refreshes_nspi_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&bind);

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    assert!(response_bytes(response).await.is_empty());
}

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
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 2);
    assert_eq!(&body[16..18], &[0, 0]);
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
async fn mapi_over_http_execute_and_replay_refresh_session_cookies() {
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
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let execute_cookie = mapi_cookie_header(&response);
    assert!(execute_cookie.contains("MapiContext="));
    assert!(execute_cookie.contains("MapiSequence="));

    let mut replay_headers = execute_headers;
    replay_headers.insert("cookie", HeaderValue::from_str(&execute_cookie).unwrap());
    let replay = service
        .handle_mapi(MapiEndpoint::Emsmdb, &replay_headers, &request)
        .await
        .unwrap();
    assert_eq!(replay.headers().get("x-responsecode").unwrap(), "0");
    let replay_cookie = mapi_cookie_header(&replay);
    assert!(replay_cookie.contains("MapiContext="));
    assert!(replay_cookie.contains("MapiSequence="));
}

#[tokio::test]
async fn mapi_over_http_replays_duplicate_execute_request_without_rerunning_rops() {
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
    let request_id = execute_headers
        .get("x-requestid")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let refreshed_cookie = mapi_cookie_header(&response);
    let response_body = response_bytes(response).await;

    let mut replay_headers = execute_headers;
    replay_headers.insert("cookie", HeaderValue::from_str(&refreshed_cookie).unwrap());
    replay_headers.insert("x-requestid", HeaderValue::from_str(&request_id).unwrap());
    let replay = service
        .handle_mapi(MapiEndpoint::Emsmdb, &replay_headers, &request)
        .await
        .unwrap();

    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay
            .headers()
            .get("x-requestid")
            .unwrap()
            .to_str()
            .unwrap(),
        request_id
    );
    assert_eq!(replay.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(response_bytes(replay).await, response_body);
}

#[tokio::test]
async fn mapi_over_http_rejects_duplicate_execute_request_id_with_different_body() {
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
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let refreshed_cookie = mapi_cookie_header(&response);

    let mut repeated_headers = execute_headers;
    repeated_headers.insert("cookie", HeaderValue::from_str(&refreshed_cookie).unwrap());
    let different_request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00, 0x01], &[1]));
    let repeated = service
        .handle_mapi(MapiEndpoint::Emsmdb, &repeated_headers, &different_request)
        .await
        .unwrap();

    assert_eq!(repeated.status(), StatusCode::OK);
    assert_eq!(repeated.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(repeated.headers().get("x-responsecode").unwrap(), "12");
    assert!(String::from_utf8(response_bytes(repeated).await)
        .unwrap()
        .contains("reused MAPI Execute request id with a different ROP payload"));
}

#[tokio::test]
async fn mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence() {
    let load_started = Arc::new(tokio::sync::Notify::new());
    let load_continue = Arc::new(tokio::sync::Notify::new());
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mapi_mail_store_load_started: Some(load_started.clone()),
        mapi_mail_store_load_continue: Some(load_continue.clone()),
        ..Default::default()
    };
    let service = Arc::new(ExchangeService::new(store));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let execute_service = service.clone();
    let first_execute = tokio::spawn(async move {
        execute_service
            .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
            .await
            .unwrap()
    });
    load_started.notified().await;

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let ping = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(ping.status(), StatusCode::OK);
    assert_eq!(ping.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(ping.headers().get("x-responsecode").unwrap(), "15");
    let body = String::from_utf8(response_bytes(ping).await).unwrap();
    assert!(body.contains("MAPI session already has an active request"));

    load_continue.notify_waiters();
    let execute = first_execute.await.unwrap();
    assert_eq!(execute.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(execute.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_execute_returns_private_mailbox_logon() {
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

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rop.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&logon_rop, &[]));
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
    let response_rop = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rop[0], 0xFE);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6], 0x01);
    let response_flags_offset = 7 + PRIVATE_LOGON_SPECIAL_FOLDER_ID_COUNT * 8;
    assert_eq!(response_rop[response_flags_offset], 0x07);
    assert_eq!(
        response_rop_size,
        62 + PRIVATE_LOGON_SPECIAL_FOLDER_ID_COUNT * 8
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[2 + response_rop_size..6 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn mapi_over_http_public_folder_logon_is_deferred_without_store_handle() {
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

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Public Folders\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x00];
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rop, &[])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rop = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rop, &[0xFE, 0x00, 0x02, 0x01, 0x04, 0x80]);
    assert_eq!(rop_buffer.len(), 2 + response_rop_size);
}

#[tokio::test]
async fn mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon() {
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
    let request = rca_wrapped_private_logon_execute_body(
        "alice@example.test",
        "Client=MS Connectivity Analyzer",
    );
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()), 0);
    assert_eq!(
        u16::from_le_bytes(rop_buffer[2..4].try_into().unwrap()),
        0x0004
    );
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    assert_eq!(
        u16::from_le_bytes(rop_buffer[6..8].try_into().unwrap()) as usize,
        payload_size
    );
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0xFE);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6] & 0x01, 0x01);
    let response_flags_offset = 7 + PRIVATE_LOGON_SPECIAL_FOLDER_ID_COUNT * 8;
    let mailbox_guid_offset = response_flags_offset + 1;
    let replid_offset = mailbox_guid_offset + 16;
    let replica_guid_offset = replid_offset + 2;
    assert_eq!(response_rop[response_flags_offset], 0x07);
    assert_eq!(
        &response_rop[mailbox_guid_offset..mailbox_guid_offset + 16],
        &FakeStore::account().account_id.to_bytes_le()
    );
    assert_eq!(
        &response_rop[replid_offset..replid_offset + 2],
        &1u16.to_le_bytes()
    );
    assert_eq!(
        &response_rop[replica_guid_offset..replica_guid_offset + 16],
        &mapi_mailstore::STORE_REPLICA_GUID
    );
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        u32::from_le_bytes(
            payload[response_rop_size..response_rop_size + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn mapi_over_http_execute_returns_logon_replid_guid_map_for_outlook_bootstrap() {
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
    let replid_request = hex_bytes(
        "020000001b00000000000400130013000f0007000000000000010002013866010000000780000000000000",
    );
    let replid_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &replid_request)
        .await
        .unwrap();

    assert_eq!(replid_response.status(), StatusCode::OK);
    assert_eq!(
        replid_response.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let body = response_bytes(replid_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x07);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6], 0);
    assert_eq!(
        u16::from_le_bytes(response_rop[7..9].try_into().unwrap()),
        18
    );
    assert_eq!(&response_rop[9..11], &1u16.to_le_bytes());
    assert_eq!(&response_rop[11..27], &mapi_mailstore::STORE_REPLICA_GUID);
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let named_property_request = hex_bytes(
        "020000003e00000000000400360036003200560000020200000820060000000000c00000000000004680850000000820060000000000c00000000000004681850000010000000780000000000000",
    );
    let named_property_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &named_property_request,
        )
        .await
        .unwrap();

    assert_eq!(named_property_response.status(), StatusCode::OK);
    let body = response_bytes(named_property_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x56);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        2
    );
    assert_eq!(&response_rop[8..10], &0x8003u16.to_le_bytes());
    assert_eq!(&response_rop[10..12], &0x8004u16.to_le_bytes());
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let ipm_subtree_property_request = hex_bytes(
        "020000002c00000000000400240024001c00020000010100000000000004000700010000000001000201047c01000000ffffffff0780000000000000",
    );
    let ipm_subtree_property_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &ipm_subtree_property_request,
        )
        .await
        .unwrap();

    assert_eq!(ipm_subtree_property_response.status(), StatusCode::OK);
    let body = response_bytes(ipm_subtree_property_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rops = &payload[2..response_rop_size];

    assert_eq!(response_rops[0], 0x02);
    assert_eq!(response_rops[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[8], 0x07);
    assert_eq!(response_rops[9], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[10..14].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop_size, response_rops.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[1, 0, 0, 0, 2, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let folder_set_properties_request = hex_bytes(
        "020000002f000000000004002700270023000a00001c0001000201047c14003bccd33e05e40d41a4e87c7d9d249ff501000000020000000780000000000000",
    );
    let folder_set_properties_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &folder_set_properties_request,
        )
        .await
        .unwrap();

    assert_eq!(folder_set_properties_response.status(), StatusCode::OK);
    let body = response_bytes(folder_set_properties_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x0A);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &2u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let release_and_local_replica_ids_request = hex_bytes(
        "020000001c00000000000400140014000c000100007f00010000010002000000010000000780000000000000",
    );
    let release_and_local_replica_ids_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &release_and_local_replica_ids_request,
        )
        .await
        .unwrap();

    assert_eq!(
        release_and_local_replica_ids_response.status(),
        StatusCode::OK
    );
    let body = response_bytes(release_and_local_replica_ids_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x7F);
    assert_eq!(response_rop[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(&response_rop[6..22], &mapi_mailstore::STORE_REPLICA_GUID);
    assert_eq!(response_rop.len(), 28);
    assert!(response_rop[22..28].iter().any(|byte| *byte != 0));
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let release_and_receive_folder_request = hex_bytes(
        "0200000019000000000004001100110009000100002700010003000000010000000780000000000000",
    );
    let release_and_receive_folder_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &release_and_receive_folder_request,
        )
        .await
        .unwrap();

    assert_eq!(release_and_receive_folder_response.status(), StatusCode::OK);
    let body = response_bytes(release_and_receive_folder_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x27);
    assert_eq!(response_rop[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response_rop[6..14]).unwrap(),
        test_mapi_folder_id(5)
    );
    assert_eq!(&response_rop[14..], b"\0");
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let mut hierarchy_sync_rops = Vec::new();
    append_rop_open_folder(&mut hierarchy_sync_rops, 0, 1, test_mapi_folder_id(1));
    hierarchy_sync_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x09, 0x01, 0x01, // hierarchy sync, Unicode send/options
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
    ]);
    let sync_property_tags = [
        0x3601_0003u32,
        0x3602_0003,
        0x3603_0003,
        0x0E08_0003,
        0x0FF4_0003,
        0x3FE0_0102,
        0x3FE1_0102,
        0x0E27_0102,
    ];
    hierarchy_sync_rops.extend_from_slice(&(sync_property_tags.len() as u16).to_le_bytes());
    for tag in sync_property_tags {
        hierarchy_sync_rops.extend_from_slice(&tag.to_le_bytes());
    }
    hierarchy_sync_rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    hierarchy_sync_rops.extend_from_slice(&0x4017_0003u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    hierarchy_sync_rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
    ]);
    hierarchy_sync_rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    hierarchy_sync_rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0x7BC0u16.to_le_bytes());
    let hierarchy_sync_configure_request = execute_body(&crate::tests::rop_buffer(
        &hierarchy_sync_rops,
        &[1, u32::MAX, u32::MAX],
    ));
    let hierarchy_sync_configure_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &hierarchy_sync_configure_request,
        )
        .await
        .unwrap();

    assert_eq!(hierarchy_sync_configure_response.status(), StatusCode::OK);
    let response_rops =
        response_rops_from_execute_response(hierarchy_sync_configure_response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00, 0x75, 0x02, 0x00, 0x00, 0x00, 0x00,]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x75,]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x75, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x77, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x4E, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(!contains_bytes(&response_rops, &[0x02, 0x01, 0x04, 0x80]));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &META_TAG_IDSET_GIVEN.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x6796_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x403B_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x4014_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));

    renew_mapi_request_id(&mut execute_headers);
    let address_types_request =
        hex_bytes("020000001100000000000400090009000500490000010000000780000000000000");
    let address_types_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &address_types_request,
        )
        .await
        .unwrap();

    assert_eq!(address_types_response.status(), StatusCode::OK);
    let body = response_bytes(address_types_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x49);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        2
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[8..10].try_into().unwrap()) as usize,
        b"EX\0SMTP\0".len()
    );
    assert_eq!(&response_rop[10..], b"EX\0SMTP\0");
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());
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

    for _ in 0..2 {
        let icon_len =
            u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        assert!(icon_len > 22);
        assert_eq!(&response_rops[offset..offset + 4], &[0, 0, 1, 0]);
        assert_eq!(response_rops[offset + 6], 16);
        assert_eq!(response_rops[offset + 7], 16);
        offset += icon_len;
    }

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
async fn mapi_over_http_execute_opens_root_folder_and_gets_special_hierarchy_table() {
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
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
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

    assert_eq!(response_rops[0], 0x02);
    assert_eq!(response_rops[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[8], 0x04);
    assert_eq!(response_rops[9], 0x02);
    assert_eq!(
        u32::from_le_bytes(response_rops[10..14].try_into().unwrap()),
        0
    );
    assert_eq!(
        u32::from_le_bytes(response_rops[14..18].try_into().unwrap()),
        13
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[2 + response_rop_size..6 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[6 + response_rop_size..10 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        2
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[10 + response_rop_size..14 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        3
    );
}

#[tokio::test]
async fn mapi_over_http_execute_opens_compat_shortcuts_folder() {
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::SHORTCUTS_FOLDER_ID);
    rops.push(0);
    append_rop_get_properties_specific(
        &mut rops,
        1,
        &[
            PID_TAG_LOCAL_COMMIT_TIME_MAX,
            PID_TAG_DELETED_COUNT_TOTAL,
            PID_TAG_CONTENT_UNREAD_COUNT,
            PID_TAG_CONTENT_COUNT,
        ],
    );

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x07, 0x01, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_execute_opens_freebusy_data_folder() {
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID);
    rops.push(0);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x02, 0x01, 0x0f, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_query_columns_all_reports_canonical_table_columns() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(7));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x37, 0x00, 0x02, // RopQueryColumnsAll
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

    let query_columns_offset = 18;
    assert_eq!(response_rops[query_columns_offset], 0x37);
    assert_eq!(response_rops[query_columns_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[query_columns_offset + 2..query_columns_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    let column_count = u16::from_le_bytes(
        response_rops[query_columns_offset + 6..query_columns_offset + 8]
            .try_into()
            .unwrap(),
    );
    assert!(column_count >= 10);
    assert!(contains_bytes(response_rops, &0x0037_001Fu32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x65E0_0102u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x65E2_0102u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_query_columns_all_folder_columns_omit_note_geometry() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, root
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x00, // RopGetHierarchyTable
        0x37, 0x00, 0x02, // RopQueryColumnsAll
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
    let response_rops = response_rops_from_execute_response(response).await;
    let query_columns_offset = 18;
    assert_eq!(response_rops[query_columns_offset], 0x37);
    assert!(contains_bytes(
        &response_rops,
        &0x3001_001Fu32.to_le_bytes()
    ));
    for note_geometry_tag in [
        0x8B00_0003u32,
        0x8B02_0003u32,
        0x8B03_0003u32,
        0x8B04_0003u32,
        0x8B05_0003u32,
    ] {
        assert!(!contains_bytes(
            &response_rops[query_columns_offset..],
            &note_geometry_tag.to_le_bytes()
        ));
    }
}

#[tokio::test]
async fn mapi_over_http_ipm_subtree_reports_distinct_folder_identity() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, IPM subtree
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(4));
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&7u16.to_le_bytes());
    for tag in [
        0x3001_001F,
        0x6748_0014,
        0x6749_0014,
        0x3613_001F,
        0x001A_001F,
        0x65E0_0102,
        0x65E1_0102,
    ] as [u32; 7]
    {
        rops.extend_from_slice(&tag.to_le_bytes());
    }

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
    let properties = &response_rops[8..];

    assert_eq!(properties[0], 0x07);
    assert_eq!(properties[1], 0x01);
    assert_eq!(u32::from_le_bytes(properties[2..6].try_into().unwrap()), 0);
    assert!(contains_bytes(
        properties,
        &utf16z("Top of Information Store")
    ));
    assert!(contains_bytes(
        properties,
        &mapi_wire_id_bytes(test_mapi_folder_id(4))
    ));
    assert!(contains_bytes(
        properties,
        &mapi_wire_id_bytes(test_mapi_folder_id(1))
    ));
    assert!(contains_bytes(properties, &utf16z("IPF.Note")));
}

#[tokio::test]
async fn mapi_over_http_advertised_special_folder_reports_own_identity() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Outbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(6));
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    for tag in [
        0x3001_001F,
        0x6748_0014,
        0x6749_0014,
        0x3613_001F,
        0x001A_001F,
    ] as [u32; 5]
    {
        rops.extend_from_slice(&tag.to_le_bytes());
    }

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
    let properties = &response_rops[8..];

    assert_eq!(properties[0], 0x07);
    assert_eq!(properties[1], 0x01);
    assert_eq!(u32::from_le_bytes(properties[2..6].try_into().unwrap()), 0);
    assert!(contains_bytes(properties, &utf16z("Outbox")));
    assert!(contains_bytes(
        properties,
        &mapi_wire_id_bytes(test_mapi_folder_id(6))
    ));
    assert!(contains_bytes(
        properties,
        &mapi_wire_id_bytes(test_mapi_folder_id(4))
    ));
    assert!(contains_bytes(properties, &utf16z("IPF.Note")));
}

#[tokio::test]
async fn mapi_over_http_create_folder_creates_canonical_mailbox() {
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        created_mailboxes: created_mailboxes.clone(),
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("MAPI Projects"));
    rops.extend_from_slice(&utf16z(""));

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
    let create = &response_rops[8..];

    assert_eq!(create[0], 0x1C);
    assert_eq!(create[1], 0x02);
    assert_eq!(u32::from_le_bytes(create[2..6].try_into().unwrap()), 0);
    assert_eq!(
        u64::from_le_bytes(create[6..14].try_into().unwrap()),
        test_mapi_uuid_id(&Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap())
    );
    assert_eq!(create[14], 0);
    assert_eq!(create[15], 0);

    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].account_id, FakeStore::account().account_id);
    assert_eq!(created[0].name, "MAPI Projects");
}

#[tokio::test]
async fn mapi_over_http_delete_folder_removes_custom_canonical_mailbox() {
    let custom_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let destroyed_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "66666666-6666-6666-6666-666666666666",
            "custom",
            "Archive",
        )])),
        destroyed_mailboxes: destroyed_mailboxes.clone(),
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // deletion flags
    ]);
    append_mapi_wire_id(&mut rops, test_mapi_uuid_id(&custom_id));

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
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(u32::from_le_bytes(delete[2..6].try_into().unwrap()), 0);
    assert_eq!(delete[6], 0);
    assert_eq!(destroyed_mailboxes.lock().unwrap().as_slice(), &[custom_id]);
}

#[tokio::test]
async fn mapi_over_http_delete_folder_rejects_system_mailbox() {
    let destroyed_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        destroyed_mailboxes: destroyed_mailboxes.clone(),
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // deletion flags
    ]);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));

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
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(delete[2..6].try_into().unwrap()),
        0x8007_0005
    );
    assert!(destroyed_mailboxes.lock().unwrap().is_empty());
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
    let updated = updated_mailboxes.lock().unwrap();
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].mailbox_id, source_id);
    assert_eq!(updated[0].name.as_deref(), Some("Moved Projects"));
    assert_eq!(updated[0].parent_id, Some(Some(target_parent_id)));

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
async fn mapi_over_http_copy_folder_creates_custom_canonical_mailbox() {
    let source_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let target_parent_id = Uuid::parse_str("34343434-3434-4434-8434-343434343434").unwrap();
    let source_mapi_id = test_mapi_uuid_id(&source_id);
    let target_parent_mapi_id = test_mapi_uuid_id(&target_parent_id);
    crate::mapi::identity::remember_mapi_identity(source_id, source_mapi_id);
    crate::mapi::identity::remember_mapi_identity(target_parent_id, target_parent_mapi_id);
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
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
        created_mailboxes: created_mailboxes.clone(),
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
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_open_folder(&mut rops, 0, 2, target_parent_mapi_id);
    rops.extend_from_slice(&[
        0x36, 0x00, 0x01, 0x02, // RopCopyFolder
        0x00, // synchronous
        0x01, // recursive
        0x00, // multibyte name
    ]);
    append_mapi_wire_id(&mut rops, source_mapi_id);
    rops.extend_from_slice(b"Copied Projects\0");

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
    assert!(contains_bytes(&response_rops, &[0x36, 0x01, 0, 0, 0, 0, 0]));
    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].name, "Copied Projects");
    assert_eq!(created[0].parent_id, Some(target_parent_id));
}

#[tokio::test]
async fn mapi_over_http_folder_move_copy_reject_system_mailbox_sources() {
    let inbox_id = Uuid::parse_str("35353535-3535-4535-8535-353535353535").unwrap();
    let inbox_mapi_id = crate::mapi::identity::INBOX_FOLDER_ID;
    let updated_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        updated_mailboxes: updated_mailboxes.clone(),
        created_mailboxes: created_mailboxes.clone(),
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
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_open_folder(
        &mut rops,
        0,
        2,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[0x35, 0x00, 0x01, 0x02, 0x00, 0x01]);
    append_mapi_wire_id(&mut rops, inbox_mapi_id);
    rops.extend_from_slice(&utf16z("Moved Inbox"));
    rops.extend_from_slice(&[0x36, 0x00, 0x01, 0x02, 0x00, 0x01, 0x01]);
    append_mapi_wire_id(&mut rops, inbox_mapi_id);
    rops.extend_from_slice(&utf16z("Copied Inbox"));

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
        &[0x35, 0x01, 0x05, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x36, 0x01, 0x05, 0x00, 0x07, 0x80]
    ));
    assert!(updated_mailboxes.lock().unwrap().is_empty());
    assert!(created_mailboxes.lock().unwrap().is_empty());
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
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_query_rows_lists_root_and_canonical_mailbox_folders() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 7;
    inbox.unread_emails = 2;
    let mut archive =
        FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "custom", "Archive");
    archive.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
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
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3602_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3603_0003u32.to_le_bytes());
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
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let query_offset = 8 + 10 + 7;

    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        15
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(response_rops, &utf16z("Archive")));
}

#[tokio::test]
async fn mapi_over_http_sort_table_orders_combined_hierarchy_rows() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Zulu Mail",
        )])),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default",
            "contacts",
            "Alpha Contacts",
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
    let alpha = utf16z("Alpha Contacts");
    let zulu = utf16z("Zulu Mail");
    let alpha_offset = response_rops
        .windows(alpha.len())
        .position(|window| window == alpha)
        .unwrap();
    let zulu_offset = response_rops
        .windows(zulu.len())
        .position(|window| window == zulu)
        .unwrap();

    assert!(alpha_offset < zulu_offset);
}

#[tokio::test]
async fn mapi_over_http_contents_table_lists_canonical_messages() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let archive = FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "custom", "Archive");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Inbox message",
            ),
            FakeStore::email(
                "99999999-9999-9999-9999-999999999999",
                "66666666-6666-6666-6666-666666666666",
                "custom",
                "Archive message",
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&7u16.to_le_bytes());
    rops.extend_from_slice(&0x674A_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C1F_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E04_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E08_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x0E07_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x0E1B_000Bu32.to_le_bytes());
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
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    let contents_offset = 8;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    let query_offset = contents_offset + 10 + 7;
    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox message")));
    assert!(contains_bytes(response_rops, &utf16z("alice@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Bob")));
    assert!(contains_bytes(response_rops, &128u32.to_le_bytes()));
    assert!(!contains_bytes(response_rops, &utf16z("Archive message")));
}

#[tokio::test]
async fn mapi_over_http_query_rows_advances_table_position() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "81818181-8181-8181-8181-818181818181",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "First page message",
            ),
            FakeStore::email(
                "82828282-8282-8282-8282-828282828282",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Second page message",
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
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x81, 0x00, 0x02, // RopResetTable
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
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
    let query_offsets = response_rops
        .windows(7)
        .enumerate()
        .filter_map(|(offset, window)| (window == [0x15, 0x02, 0, 0, 0, 0, 0x02]).then_some(offset))
        .collect::<Vec<_>>();

    assert_eq!(query_offsets.len(), 3);
    let first_query = &response_rops[query_offsets[0]..query_offsets[1]];
    let second_query = &response_rops[query_offsets[1]..query_offsets[2]];
    let reset_query = &response_rops[query_offsets[2]..];
    assert_eq!(u16::from_le_bytes(first_query[7..9].try_into().unwrap()), 1);
    assert_eq!(
        u16::from_le_bytes(second_query[7..9].try_into().unwrap()),
        1
    );
    assert!(contains_bytes(first_query, &utf16z("First page message")));
    assert!(!contains_bytes(first_query, &utf16z("Second page message")));
    assert!(contains_bytes(second_query, &utf16z("Second page message")));
    assert!(contains_bytes(reset_query, &utf16z("First page message")));
    assert!(!contains_bytes(reset_query, &utf16z("Second page message")));
}

#[tokio::test]
async fn mapi_over_http_query_position_reports_table_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "83838383-8383-8383-8383-838383838383",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Position first",
            ),
            FakeStore::email(
                "84848484-8484-8484-8484-848484848484",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Position second",
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
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

    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_seek_row_fractional_moves_table_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 4;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "87878787-8787-8787-8787-878787878787",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional first",
            ),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional second",
            ),
            FakeStore::email(
                "89898989-8989-8989-8989-898989898989",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional third",
            ),
            FakeStore::email(
                "8a8a8a8a-8a8a-8a8a-8a8a-8a8a8a8a8a8a",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional fourth",
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x1A, 0x00, 0x02, // RopSeekRowFractional
    ]);
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.extend_from_slice(&2u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
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
    assert!(contains_bytes(&response_rops, &[0x1A, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_categorized_table_rops_use_bounded_table_state() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let mut first = FakeStore::email(
        "87878787-8787-8787-8787-878787878787",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Categorized first",
    );
    first.categories = vec!["Project".to_string()];
    let mut second = FakeStore::email(
        "88888888-8888-8888-8888-888888888888",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Categorized second",
    );
    second.categories = vec!["Project".to_string()];
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![first, second])),
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
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x674D_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x9000_101Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x13, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x9000_101Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&10u16.to_le_bytes());
    let category_id = test_category_id(test_mapi_folder_id(5), 0x9000_101F, "Project");
    rops.extend_from_slice(&[0x5A, 0x00, 0x02]);
    rops.extend_from_slice(&category_id.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&10u16.to_le_bytes());
    rops.extend_from_slice(&[0x59, 0x00, 0x02]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&category_id.to_le_bytes());
    rops.extend_from_slice(&[0x6B, 0x00, 0x02]);
    rops.extend_from_slice(&category_id.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    let state = test_collapse_state(test_mapi_folder_id(5), category_id, 0, category_id);
    rops.extend_from_slice(&[0x6C, 0x00, 0x02]);
    rops.extend_from_slice(&(state.len() as u16).to_le_bytes());
    rops.extend_from_slice(&state);

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
    assert!(contains_bytes(&response_rops, &[0x13, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &utf16z("Project")));
    assert!(contains_bytes(&response_rops, &utf16z("Categorized first")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Categorized second")
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x5A, 0x02, 0, 0, 0, 0, 2, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x59, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x6B, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"LPECS1"));
    assert!(contains_bytes(
        &response_rops,
        &[0x6C, 0x02, 0, 0, 0, 0, 4, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_query_rows_no_advance_preserves_table_position() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "85858585-8585-8585-8585-858585858585",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "No advance first",
            ),
            FakeStore::email(
                "86868686-8686-8686-8686-868686868686",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "No advance second",
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x15, 0x00, 0x02, 0x01, 0x01, // RopQueryRows, NoAdvance
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
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

    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_sort_table_orders_contents_rows() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "89898989-8989-8989-8989-898989898989",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Zulu sort",
            ),
            FakeStore::email(
                "90909090-9090-9090-9090-909090909090",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Alpha sort",
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
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());

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
    let alpha = utf16z("Alpha sort");
    let zulu = utf16z("Zulu sort");
    let alpha_offset = response_rops
        .windows(alpha.len())
        .position(|window| window == alpha)
        .unwrap();
    let zulu_offset = response_rops
        .windows(zulu.len())
        .position(|window| window == zulu)
        .unwrap();

    assert!(contains_bytes(response_rops, &[0x13, 0x02, 0, 0, 0, 0, 0]));
    assert!(alpha_offset < zulu_offset);
}

#[tokio::test]
async fn mapi_over_http_query_rows_reads_backward_from_table_position() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "91919191-9191-9191-9191-919191919191",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Backward first",
            ),
            FakeStore::email(
                "92929292-9292-9292-9292-929292929292",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Backward second",
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
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows, forward
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x00, // RopQueryRows, backward
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
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
    let query_offsets = response_rops
        .windows(7)
        .enumerate()
        .filter_map(|(offset, window)| (window == [0x15, 0x02, 0, 0, 0, 0, 0x02]).then_some(offset))
        .collect::<Vec<_>>();

    assert_eq!(query_offsets.len(), 2);
    let backward_query = &response_rops[query_offsets[1]..];
    assert!(contains_bytes(backward_query, &utf16z("Backward second")));
    assert!(!contains_bytes(backward_query, &utf16z("Backward first")));
    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_restrict_filters_contents_table_rows() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "93939393-9393-9393-9393-939393939393",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Quarter planning",
            ),
            FakeStore::email(
                "94949494-9494-9494-9494-949494949494",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Budget review",
            ),
            FakeStore::email(
                "95959595-9595-9595-9595-959595959595",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Planning followup",
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
    let restriction = mapi_content_restriction(0x0037_001F, "planning");

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
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x14, 0x00, 0x02, 0x00, // RopRestrict
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
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

    assert!(contains_bytes(response_rops, &[0x14, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("Quarter planning")));
    assert!(!contains_bytes(response_rops, &utf16z("Budget review")));
    assert!(contains_bytes(response_rops, &utf16z("Planning followup")));
    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_find_row_returns_matching_contents_row() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "96969696-9696-9696-9696-969696969696",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Find first",
            ),
            FakeStore::email(
                "97979797-9797-9797-9797-979797979797",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Needle target",
            ),
            FakeStore::email(
                "98989898-9898-9898-9898-989898989898",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Find last",
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
    let restriction = mapi_content_restriction(0x0037_001F, "needle");

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
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(!contains_bytes(response_rops, &utf16z("Find first")));
    assert!(contains_bytes(response_rops, &utf16z("Needle target")));
    assert!(!contains_bytes(response_rops, &utf16z("Find last")));
}

#[tokio::test]
async fn mapi_over_http_table_bookmarks_restore_contents_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "99999999-9999-9999-9999-999999999999",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Bookmark first",
            ),
            FakeStore::email(
                "9a9a9a9a-9a9a-9a9a-9a9a-9a9a9a9a9a9a",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Bookmark second",
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

    let mut first_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut first_rops, test_mapi_folder_id(5));
    first_rops.push(0);
    first_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    first_rops.extend_from_slice(&1u16.to_le_bytes());
    first_rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    first_rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    first_rops.extend_from_slice(&1u16.to_le_bytes());
    first_rops.extend_from_slice(&[
        0x1B, 0x00, 0x02, // RopCreateBookmark
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let first_request = execute_body(&rop_buffer(&first_rops, &[1, u32::MAX, u32::MAX]));
    let first_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &first_request)
        .await
        .unwrap();
    let first_body = response_bytes(first_response).await;
    let first_rop_buffer_size = u32::from_le_bytes(first_body[12..16].try_into().unwrap()) as usize;
    let first_rop_buffer = &first_body[16..16 + first_rop_buffer_size];
    let first_response_rop_size =
        u16::from_le_bytes(first_rop_buffer[0..2].try_into().unwrap()) as usize;
    let first_response_rops = &first_rop_buffer[2..2 + first_response_rop_size];

    assert!(contains_bytes(
        first_response_rops,
        &utf16z("Bookmark first")
    ));
    assert!(contains_bytes(
        first_response_rops,
        &[0x1B, 0x02, 0, 0, 0, 0, 4, 0, 1, 0, 0, 0]
    ));

    let bookmark = 1u32.to_le_bytes();
    let mut second_rops = vec![0x19, 0x00, 0x02]; // RopSeekRowBookmark
    second_rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    second_rops.extend_from_slice(&bookmark);
    second_rops.extend_from_slice(&0i32.to_le_bytes());
    second_rops.push(1);
    second_rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    second_rops.extend_from_slice(&1u16.to_le_bytes());
    second_rops.extend_from_slice(&[
        0x89, 0x00, 0x02, // RopFreeBookmark
    ]);
    second_rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    second_rops.extend_from_slice(&bookmark);

    renew_mapi_request_id(&mut execute_headers);
    let second_request = execute_body(&rop_buffer(&second_rops, &[u32::MAX, u32::MAX, 3]));
    let second_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &second_request)
        .await
        .unwrap();

    assert_eq!(second_response.status(), StatusCode::OK);
    assert_eq!(
        second_response.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let second_body = response_bytes(second_response).await;
    let second_rop_buffer_size =
        u32::from_le_bytes(second_body[12..16].try_into().unwrap()) as usize;
    let second_rop_buffer = &second_body[16..16 + second_rop_buffer_size];
    let second_response_rop_size =
        u16::from_le_bytes(second_rop_buffer[0..2].try_into().unwrap()) as usize;
    let second_response_rops = &second_rop_buffer[2..2 + second_response_rop_size];

    assert!(contains_bytes(
        second_response_rops,
        &[0x19, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        second_response_rops,
        &utf16z("Bookmark first")
    ));
    assert!(contains_bytes(
        second_response_rops,
        &utf16z("Bookmark second")
    ));
    assert!(contains_bytes(
        second_response_rops,
        &[0x89, 0x02, 0, 0, 0, 0]
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
    rops.extend_from_slice(&0x8003u16.to_le_bytes());
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
        &[0x56, 0x00, 0, 0, 0, 0, 2, 0, 0x03, 0x85, 0x03, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("x-lpe-test")));
    assert!(contains_bytes(
        &response_rops,
        &[0x5F, 0x00, 0, 0, 0, 0, 1, 0, 0x03, 0x80]
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
        &[0x56, 0x00, 0, 0, 0, 0, 1, 0, 0x03, 0x80]
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
    restarted_rops.extend_from_slice(&0x8003u16.to_le_bytes());
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
        &[0x5F, 0x00, 0, 0, 0, 0, 1, 0, 0x03, 0x80]
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
        &[0x56, 0x00, 0, 0, 0, 0, 1, 0, 0x03, 0x80]
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
async fn mapi_over_http_modify_recipients_string8_rows_save_canonically() {
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

    let mut to_row = Vec::new();
    to_row.extend_from_slice(b"Bob\0");
    to_row.extend_from_slice(b"bob@example.test\0");
    to_row.extend_from_slice(&1i32.to_le_bytes());
    let mut bcc_row = Vec::new();
    bcc_row.extend_from_slice(b"Hidden\0");
    bcc_row.extend_from_slice(b"hidden@example.test\0");
    bcc_row.extend_from_slice(&3i32.to_le_bytes());

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "String8 recipients");

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
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    for (row_id, recipient_type, row) in [
        (1u32, 0x01u8, to_row.as_slice()),
        (2u32, 0x03u8, bcc_row.as_slice()),
    ] {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
    assert_eq!(recorded[0].bcc.len(), 1);
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    assert_eq!(recorded[0].bcc[0].display_name.as_deref(), Some("Hidden"));
}

#[tokio::test]
async fn mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Wrapped recipients");
    let to_row = mapi_wrapped_recipient_row("Bob", "bob@example.test", 0x01);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_save_changes_message(&mut rops, 2, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
}

#[tokio::test]
async fn mapi_over_http_modify_recipients_x500_rows_save_canonically() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "X500 recipients");
    let legacy_dn = "O=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test";
    let to_row = mapi_wrapped_x500_recipient_row("Bob", legacy_dn);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_modify_recipients_with_columns(
        &mut rops,
        2,
        &[0x0FFE_0003, 0x3900_0003, 0x39FF_001F],
        &[(1, 0x01, to_row.as_slice())],
    );
    append_rop_save_changes_message(&mut rops, 2, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "alice@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
}

#[tokio::test]
async fn mapi_over_http_remove_all_recipients_clears_pending_message_recipients() {
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
    rops.extend_from_slice(&[
        0x0D, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, // RopRemoveAllRecipients
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

    assert!(contains_bytes(response_rops, &[0x0D, 0x02, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert!(recorded[0].to.is_empty());
    assert!(recorded[0].cc.is_empty());
    assert!(recorded[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_submit_pending_message_uses_canonical_submission() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Submit from MAPI");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Canonical submit body");
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-submit@example.test>",
    );

    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
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
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    for (row_id, recipient_type, row) in [
        (1u32, 0x01u8, to_row.as_slice()),
        (2u32, 0x03u8, bcc_row.as_slice()),
    ] {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
    rops.extend_from_slice(&[
        0x32, 0x00, 0x02, 0x00, // RopSubmitMessage
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

    assert!(contains_bytes(response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, None);
        assert_eq!(recorded[0].subject, "Submit from MAPI");
        assert_eq!(recorded[0].body_text, "Canonical submit body");
        assert_eq!(recorded[0].from_address, "alice@example.test");
        assert_eq!(recorded[0].to.len(), 1);
        assert_eq!(recorded[0].to[0].address, "bob@example.test");
        assert_eq!(recorded[0].bcc.len(), 1);
        assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
        assert_eq!(
            recorded[0].internet_message_id.as_deref(),
            Some("<mapi-submit@example.test>")
        );
    }

    let sent = {
        let canonical = emails.lock().unwrap();
        let sent = canonical
            .iter()
            .filter(|email| email.mailbox_role == "sent" && email.subject == "Submit from MAPI")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(
        sent.mailbox_id,
        Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
    );
    assert_eq!(sent.mailbox_ids, vec![sent.mailbox_id]);
    assert_eq!(sent.mailbox_states.len(), 1);
    assert_eq!(sent.mailbox_states[0].mailbox_id, sent.mailbox_id);
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    assert_eq!(sent.delivery_status, "queued");

    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].mailbox_role, "sent");
    assert!(visible[0].bcc.is_empty());
    let protected = projection_store
        .fetch_jmap_emails_with_protected_bcc(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(protected[0].bcc[0].address, "hidden@example.test");
    let hidden_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent.mailbox_id),
            Some("hidden@example.test"),
            0,
            10,
        )
        .await
        .unwrap();
    assert!(hidden_search.ids.is_empty());
    let subject_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent.mailbox_id),
            Some("Submit from MAPI"),
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(subject_search.ids, vec![sent.id]);
}

#[tokio::test]
async fn mapi_over_http_transport_send_uses_canonical_submission() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
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
        "Transport send from MAPI",
    );
    append_mapi_utf16_property(
        &mut property_values,
        0x1000_001F,
        "Canonical transport body",
    );
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_transport_send(&mut rops, 2);

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
        &[0x4A, 0x02, 0, 0, 0, 0, 0, 0]
    ));

    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "mapi-submit-message");
    assert_eq!(recorded[0].draft_message_id, None);
    assert_eq!(recorded[0].subject, "Transport send from MAPI");
    assert_eq!(recorded[0].body_text, "Canonical transport body");
    assert_eq!(recorded[0].from_address, "alice@example.test");
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    drop(recorded);

    let sent = {
        let canonical = emails.lock().unwrap();
        let sent = canonical
            .iter()
            .filter(|email| {
                email.mailbox_role == "sent" && email.subject == "Transport send from MAPI"
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(sent.delivery_status, "queued");
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible[0].mailbox_role, "sent");
    assert!(visible[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards()
{
    let draft_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let sent_mailbox_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Transport saved draft",
    );
    draft.body_text = "Draft body for transport send".to_string();
    draft.bcc.push(JmapEmailAddress {
        address: "transport-hidden@example.test".to_string(),
        display_name: Some("Transport Hidden".to_string()),
    });
    draft.has_attachments = true;
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let attachment_reference = format!("attachment:{draft_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&draft_mailbox_id.to_string(), "drafts", "Drafts"),
            FakeStore::mailbox(&sent_mailbox_id.to_string(), "sent", "Sent"),
        ])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: draft_id,
                file_name: "transport.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 7,
                file_reference: attachment_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            attachment_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: attachment_reference,
                file_name: "transport.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"PDFDATA".to_vec(),
            },
        )]))),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_transport_send(&mut rops, 2);

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
        &[0x4A, 0x02, 0, 0, 0, 0, 0, 0]
    ));

    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, Some(draft_id));
        assert_eq!(recorded[0].subject, "Transport saved draft");
        assert_eq!(recorded[0].bcc[0].address, "transport-hidden@example.test");
        assert_eq!(recorded[0].attachments.len(), 1);
        assert_eq!(recorded[0].attachments[0].file_name, "transport.pdf");
        assert_eq!(recorded[0].attachments[0].media_type, "application/pdf");
        assert_eq!(recorded[0].attachments[0].blob_bytes, b"PDFDATA");
    }

    let sent = {
        let canonical = emails.lock().unwrap();
        assert!(canonical.iter().all(|email| email.id != draft_id));
        let sent = canonical
            .iter()
            .filter(|email| {
                email.mailbox_role == "sent" && email.subject == "Transport saved draft"
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(sent.mailbox_id, sent_mailbox_id);
    assert_eq!(sent.mailbox_ids, vec![sent_mailbox_id]);
    assert_eq!(sent.mailbox_states[0].mailbox_id, sent_mailbox_id);
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    assert!(sent.has_attachments);
    assert_eq!(sent.delivery_status, "queued");

    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert!(visible[0].bcc.is_empty());
    assert!(visible[0].has_attachments);
    let protected = projection_store
        .fetch_jmap_emails_with_protected_bcc(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(protected[0].bcc[0].address, "transport-hidden@example.test");
    let hidden_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent_mailbox_id),
            Some("transport-hidden@example.test"),
            0,
            10,
        )
        .await
        .unwrap();
    assert!(hidden_search.ids.is_empty());
    let subject_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent_mailbox_id),
            Some("Transport saved draft"),
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(subject_search.ids, vec![sent.id]);
}

#[tokio::test]
async fn mapi_over_http_replayed_execute_request_id_does_not_resubmit_message() {
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
    let submitted_messages = store.submitted_messages.clone();
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Retry-safe submit");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Retry body");
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_submit_message(&mut rops, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    execute_headers.insert(
        "x-requestid",
        HeaderValue::from_static("{11111111-2222-3333-4444-555555555555}:999999"),
    );
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let first = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let first_body = response_bytes(first).await;
    let second = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let second_body = response_bytes(second).await;

    assert_eq!(first_body, second_body);
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "Retry-safe submit");
}

#[tokio::test]
async fn mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message()
{
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    execute_headers.insert(
        "x-requestid",
        HeaderValue::from_static("{11111111-2222-3333-4444-555555555555}:999998"),
    );
    let first_request = mapi_submit_execute_body("Duplicate id first submit");
    let first = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &first_request)
        .await
        .unwrap();
    assert_eq!(first.headers().get("x-responsecode").unwrap(), "0");
    let refreshed_cookie = mapi_cookie_header(&first);

    let mut repeated_headers = execute_headers;
    repeated_headers.insert("cookie", HeaderValue::from_str(&refreshed_cookie).unwrap());
    let repeated = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &repeated_headers,
            &mapi_submit_execute_body("Duplicate id rejected submit"),
        )
        .await
        .unwrap();

    assert_eq!(repeated.status(), StatusCode::OK);
    assert_eq!(repeated.headers().get("x-responsecode").unwrap(), "12");
    assert!(String::from_utf8(response_bytes(repeated).await)
        .unwrap()
        .contains("reused MAPI Execute request id with a different ROP payload"));
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "Duplicate id first submit");
    let sent = emails.lock().unwrap();
    assert_eq!(
        sent.iter()
            .filter(|email| email.subject == "Duplicate id first submit")
            .count(),
        1
    );
    assert_eq!(
        sent.iter()
            .filter(|email| email.subject == "Duplicate id rejected submit")
            .count(),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_submit_opened_draft_uses_source_draft_id() {
    let draft_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Saved MAPI draft",
    );
    draft.body_text = "Draft body".to_string();
    draft.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    draft.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    });
    draft.has_attachments = true;
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let attachment_reference = format!("attachment:{draft_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: draft_id,
                file_name: "draft.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 7,
                file_reference: attachment_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            attachment_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: attachment_reference,
                file_name: "draft.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"PDFDATA".to_vec(),
            },
        )]))),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Drafts
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(14));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(14));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&draft_id.to_string()));
    rops.extend_from_slice(&[
        0x32, 0x00, 0x02, 0x00, // RopSubmitMessage
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

    assert!(contains_bytes(response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "mapi-submit-message");
    assert_eq!(recorded[0].draft_message_id, Some(draft_id));
    assert_eq!(recorded[0].subject, "Saved MAPI draft");
    assert_eq!(recorded[0].body_text, "Draft body");
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].cc[0].address, "carol@example.test");
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    assert_eq!(recorded[0].attachments.len(), 1);
    assert_eq!(recorded[0].attachments[0].file_name, "draft.pdf");
    assert_eq!(recorded[0].attachments[0].media_type, "application/pdf");
    assert_eq!(recorded[0].attachments[0].blob_bytes, b"PDFDATA");
    let canonical = emails.lock().unwrap();
    let sent = canonical
        .iter()
        .find(|email| email.mailbox_role == "sent")
        .expect("submitted draft is visible in canonical Sent");
    assert!(sent.has_attachments);
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
    append_rop_set_read_flags(&mut flag_rops, 1, 0x04, &[draft_mapi_message_id]);
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
async fn mapi_over_http_delete_messages_uses_trash_and_hard_delete() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let soft_message_id = Uuid::parse_str("9d9d9d9d-9d9d-9d9d-9d9d-9d9d9d9d9d9d").unwrap();
    let legacy_hard_message_id = Uuid::parse_str("9e9e9e9e-9e9e-9e9e-9e9e-9e9e9e9e9e9e").unwrap();
    let extended_hard_message_id = Uuid::parse_str("9f9f9f9f-9f9f-9f9f-9f9f-9f9f9f9f9f9f").unwrap();
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
                &legacy_hard_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Legacy hard delete through MAPI",
            ),
            FakeStore::email(
                &extended_hard_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Extended hard delete through MAPI",
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
        0x59, 0x00, 0x01, // RopHardDeleteMessages
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&legacy_hard_message_id.to_string()),
    );
    rops.extend_from_slice(&[
        0x91, 0x00, 0x01, 0x00, 0x00, // RopHardDeleteMessagesExtended
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        test_mapi_message_id(&extended_hard_message_id.to_string()),
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
        &[legacy_hard_message_id, extended_hard_message_id]
    );
    {
        let canonical = canonical_emails.lock().unwrap();
        let soft_deleted = canonical
            .iter()
            .find(|email| email.id == soft_message_id)
            .expect("soft-deleted message is moved through canonical store");
        assert_eq!(soft_deleted.mailbox_id, trash_id);
        assert_eq!(soft_deleted.mailbox_role, "trash");
        assert!(canonical.iter().all(
            |email| email.id != legacy_hard_message_id && email.id != extended_hard_message_id
        ));
    }
    assert!(contains_bytes(response_rops, &[0x1E, 0x01, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &[0x59, 0x01, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &[0x91, 0x01, 0, 0, 0, 0, 0]));
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
    rops.extend_from_slice(&[0x59, 0x00, 0x01]);
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

    assert!(contains_bytes(&response_rops, &[0x59, 0x01, 0, 0, 0, 0, 1]));
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
async fn mapi_over_http_guessed_recoverable_items_folder_id_is_not_opened() {
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(0x7000));
    rops.push(0);

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
        &[0x02, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_browses_recoverable_items_virtual_folder() {
    let item = FakeStore::recoverable_item(
        "abababab-abab-abab-abab-abababababab",
        "deletions",
        "Recoverable browse subject",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        recoverable_items: Arc::new(Mutex::new(vec![item])),
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
        crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&10u16.to_le_bytes());

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
        &utf16z("Recoverable browse subject")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Note")));
}

#[tokio::test]
async fn mapi_over_http_restores_recoverable_item_through_canonical_store() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let item_id = Uuid::parse_str("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc").unwrap();
    let item = FakeStore::recoverable_item(&item_id.to_string(), "deletions", "Restore me");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        recoverable_items: Arc::new(Mutex::new(vec![item])),
        ..Default::default()
    };
    let restored = store.restored_recoverable_items.clone();
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
        crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID,
    );
    append_rop_open_folder(&mut rops, 0, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, move
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        crate::mapi_store::mapi_recoverable_item_id(&item_id),
    );
    rops.push(0);
    rops.push(0);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, 2, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(
        restored.lock().unwrap().as_slice(),
        &[(item_id, Some(inbox_id))]
    );
    assert!(contains_bytes(&response_rops, &[0x33, 0x01, 0, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_recoverable_purge_reports_partial_when_canonical_store_blocks() {
    let item_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
    let mut item = FakeStore::recoverable_item(&item_id.to_string(), "deletions", "Held item");
    item.legal_hold = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        recoverable_items: Arc::new(Mutex::new(vec![item])),
        failed_purge_recoverable_item_ids: Arc::new(Mutex::new(vec![item_id])),
        ..Default::default()
    };
    let purged = store.purged_recoverable_items.clone();
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
        crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x59, 0x00, 0x01, // RopHardDeleteMessages
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        crate::mapi_store::mapi_recoverable_item_id(&item_id),
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

    assert!(purged.lock().unwrap().is_empty());
    assert!(contains_bytes(&response_rops, &[0x59, 0x01, 0, 0, 0, 0, 1]));
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
async fn mapi_over_http_query_rows_uses_paged_content_table_lookup() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "87878787-8787-8787-8787-878787878787",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Paged first",
            ),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Paged second",
            ),
            FakeStore::email(
                "89898989-8989-8989-8989-898989898989",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Paged third",
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
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_query_subject_rows(&mut rops, 1, 2, 1);

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
    assert!(contains_bytes(&response_rops, &utf16z("Paged first")));
    assert!(!contains_bytes(&response_rops, &utf16z("Paged second")));
    assert_eq!(queried_jmap_email_ids.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mapi_over_http_seek_row_moves_contents_table_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "87878787-8787-8787-8787-878787878787",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Seek first",
            ),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Seek second",
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
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x18, 0x00, 0x02, 0x00, // RopSeekRow, BOOKMARK_BEGINNING
    ]);
    rops.extend_from_slice(&1i32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
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

    assert!(contains_bytes(
        response_rops,
        &[0x18, 0x02, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]
    ));
    assert!(!contains_bytes(response_rops, &utf16z("Seek first")));
    assert!(contains_bytes(response_rops, &utf16z("Seek second")));
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
async fn mapi_over_http_read_recipients_returns_canonical_message_recipients() {
    let message_id = "22222222-2222-2222-2222-222222222222";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Recipient message",
    );
    email.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    email.bcc.push(JmapEmailAddress {
        address: "erin@example.test".to_string(),
        display_name: Some("Erin".to_string()),
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
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());

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
    let read_recipients_offset = response_rops
        .iter()
        .enumerate()
        .find_map(|(offset, byte)| (*byte == 0x0F).then_some(offset))
        .unwrap();

    assert_eq!(response_rops[read_recipients_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[read_recipients_offset + 2..read_recipients_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(response_rops, &0u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &1u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Bob")));
    assert!(contains_bytes(response_rops, &utf16z("carol@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Carol")));
    assert!(!contains_bytes(response_rops, &utf16z("erin@example.test")));
}

#[tokio::test]
async fn mapi_over_http_read_recipients_hides_sent_message_bcc_by_default() {
    let message_id = "23232323-2323-2323-2323-232323232323";
    let mut sent = FakeStore::mailbox("77777777-7777-7777-7777-777777777777", "sent", "Sent");
    sent.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "77777777-7777-7777-7777-777777777777",
        "sent",
        "Sent recipient message",
    );
    email.bcc.push(JmapEmailAddress {
        address: "erin@example.test".to_string(),
        display_name: Some("Erin".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![sent])),
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
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(7));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(7));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());

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

    assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
    assert!(!contains_bytes(response_rops, &utf16z("erin@example.test")));
    assert!(!contains_bytes(response_rops, &utf16z("Erin")));
}

#[tokio::test]
async fn mapi_over_http_attachment_table_lists_canonical_message_attachments() {
    let message_id = "33333333-3333-3333-3333-333333333333";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment message",
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
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 5,
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
        0x21, 0x00, 0x02, 0x03, 0x00, // RopGetAttachmentTable
        0x12, 0x00, 0x03, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x0E21_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3707_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x370E_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E20_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3705_0003u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x03, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

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

    assert!(contains_bytes(
        response_rops,
        &[0x21, 0x03, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &utf16z("brief.pdf")));
    assert!(contains_bytes(response_rops, &utf16z("application/pdf")));
    assert!(contains_bytes(response_rops, &5u32.to_le_bytes()));
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
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&0x0E21_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3707_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x370E_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E20_0003u32.to_le_bytes());

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
async fn mapi_over_http_reads_canonical_attachment_data_stream() {
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
    assert!(contains_bytes(
        response_rops,
        &[0x5B, 0x04, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x5C, 0x04, 0x02, 0x01, 0x04, 0x80]
    ));
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
async fn mapi_over_http_delete_attachment_removes_canonical_attachment() {
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
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x24, 0x02, 0, 0, 0, 0]));
    assert!(attachments.lock().unwrap()[&message_uuid].is_empty());
    let canonical = canonical_emails.lock().unwrap();
    assert!(
        !canonical
            .iter()
            .find(|email| email.id == message_uuid)
            .expect("message remains canonical after attachment delete")
            .has_attachments
    );
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
                    size_octets: 11,
                    file_reference: format!("attachment:{message_uuid}:{first_attachment_id}"),
                },
                ActiveSyncAttachment {
                    id: second_attachment_id,
                    message_id: message_uuid,
                    file_name: "second.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
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
async fn mapi_over_http_set_read_flags_updates_canonical_message_state() {
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
    rops.extend_from_slice(&[
        0x66, 0x00, 0x01, 0x00, 0x01, // RopSetReadFlags, sync, rfSuppressReceipt
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));

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

    assert!(contains_bytes(response_rops, &[0x66, 0x01, 0, 0, 0, 0, 0]));
    assert!(!emails.lock().unwrap()[0].unread);
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

    assert!(contains_bytes(response_rops, &[0x11, 0x02, 0, 0, 0, 0, 1]));
    assert!(!emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_set_properties_updates_open_message_flags() {
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn mapi_over_http_set_properties_updates_canonical_mail_reminder_state() {
    let message_id = "39393939-3939-3939-3939-393939393939";
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message reminder",
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

    let reminder_at = "2026-05-21T12:30:00Z";
    let mut property_values = Vec::new();
    property_values.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    property_values.push(1);
    append_mapi_i64_property(
        &mut property_values,
        0x8560_0040,
        mapi_mailstore::filetime_from_rfc3339_utc(reminder_at) as i64,
    );

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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(updated.reminder_set);
    assert_eq!(updated.reminder_at.as_deref(), Some(reminder_at));
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

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_set_properties_updates_canonical_event_and_task_reminders() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let task_list = FakeStore::collection("default", "task", "Tasks");
    let event_id = Uuid::parse_str("31313131-3131-3131-3131-313131313131").unwrap();
    let task_id = Uuid::parse_str("32323232-3232-3232-3232-323232323232").unwrap();
    let reminders = Arc::new(Mutex::new(vec![ClientReminder {
        source_type: "task".to_string(),
        source_id: task_id,
        occurrence_start_at: None,
        title: "Task reminder source".to_string(),
        due_at: Some("2026-05-21T12:00:00Z".to_string()),
        reminder_at: "2026-05-21T11:30:00Z".to_string(),
        dismissed_at: None,
        completed_at: None,
        status: "pending".to_string(),
    }]));
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar.clone()])),
        task_collections: Arc::new(Mutex::new(vec![task_list.clone()])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: calendar.id,
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar reminder source".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        tasks: Arc::new(Mutex::new(vec![FakeStore::task(
            &task_id.to_string(),
            "33333333-3333-3333-3333-333333333333",
            "Task reminder source",
        )])),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: Uuid::parse_str("34343434-3434-3434-3434-343434343434").unwrap(),
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
        reminders: reminders.clone(),
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

    let calendar_reminder_at = "2026-05-21T08:45:00Z";
    let task_reminder_at = "2026-05-21T11:45:00Z";
    let mut event_values = Vec::new();
    event_values.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    event_values.push(1);
    append_mapi_i64_property(
        &mut event_values,
        0x8560_0040,
        mapi_mailstore::filetime_from_rfc3339_utc(calendar_reminder_at) as i64,
    );
    let mut task_values = Vec::new();
    task_values.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    task_values.push(1);
    append_mapi_i64_property(
        &mut task_values,
        0x8560_0040,
        mapi_mailstore::filetime_from_rfc3339_utc(task_reminder_at) as i64,
    );

    let mut rops = Vec::new();
    append_rop_open_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        crate::mapi::identity::legacy_migration_object_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 1, 2, &event_values);
    append_rop_open_folder(&mut rops, 0, 3, crate::mapi::identity::TASKS_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        3,
        4,
        crate::mapi::identity::TASKS_FOLDER_ID,
        test_mapi_uuid_id(&task_id),
    );
    append_rop_set_properties(&mut rops, 4, 2, &task_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie);
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x04, 0, 0, 0, 0, 0, 0]
    ));
    let reminders = reminders.lock().unwrap();
    assert!(reminders.iter().any(|reminder| {
        reminder.source_type == "calendar"
            && reminder.source_id == event_id
            && reminder.reminder_at == calendar_reminder_at
    }));
    assert!(reminders.iter().any(|reminder| {
        reminder.source_type == "task"
            && reminder.source_id == task_id
            && reminder.reminder_at == task_reminder_at
    }));
}

#[tokio::test]
async fn mapi_over_http_ipm_subtree_returns_stable_ost_identity() {
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
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(4));
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on the IPM subtree
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
    let mut expected = 20u16.to_le_bytes().to_vec();
    expected.extend_from_slice(account.account_id.as_bytes());
    expected.extend_from_slice(&1u32.to_le_bytes());
    assert!(contains_bytes(&response_rops, &expected));
}

#[tokio::test]
async fn mapi_over_http_ipm_subtree_ost_identity_retains_client_session_blob() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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

    let client_blob = [0x44; 20];
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
    let mut overridden = 20u16.to_le_bytes().to_vec();
    overridden.extend_from_slice(&client_blob);
    assert!(contains_bytes(&response_rops, &overridden));

    let mut canonical = 20u16.to_le_bytes().to_vec();
    canonical.extend_from_slice(account.account_id.as_bytes());
    canonical.extend_from_slice(&1u32.to_le_bytes());
    assert!(!contains_bytes(&response_rops, &canonical));
    assert_eq!(
        stored_ost_id.lock().unwrap().as_deref(),
        Some(client_blob.as_slice())
    );
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
async fn mapi_over_http_ipm_subtree_ost_identity_survives_reconnect() {
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

    let client_blob = [0x55; 20];
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x7C04_0102, &client_blob);

    let mut set_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut set_rops, test_mapi_folder_id(4));
    set_rops.push(0);
    set_rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on the IPM subtree
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
    append_mapi_wire_id(&mut get_rops, test_mapi_folder_id(4));
    get_rops.push(0);
    get_rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on the reopened IPM subtree
    ]);
    get_rops.extend_from_slice(&4096u16.to_le_bytes());
    get_rops.extend_from_slice(&1u16.to_le_bytes());
    get_rops.extend_from_slice(&0x7C04_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&get_rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let mut persisted = 20u16.to_le_bytes().to_vec();
    persisted.extend_from_slice(&client_blob);
    assert!(contains_bytes(&response_rops, &persisted));

    let mut canonical = 20u16.to_le_bytes().to_vec();
    canonical.extend_from_slice(account.account_id.as_bytes());
    canonical.extend_from_slice(&1u32.to_le_bytes());
    assert!(!contains_bytes(&response_rops, &canonical));
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
    assert_eq!(message.subject, "");
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
async fn mapi_over_http_common_views_observed_outlook_partial_sync_returns_no_synthetic_items() {
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
    assert_eq!(checkpoint.last_change_sequence, 26);
    assert_eq!(checkpoint.last_modseq, 26);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(|id| id.as_u64()),
        Some(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID)
    );
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
    assert_eq!(message.subject, "Sync conversation action");
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
async fn mapi_over_http_contact_content_sync_exports_deletes() {
    let contact_id = Uuid::parse_str("fb129372-d6b6-4d69-99f7-977ab2a8093f").unwrap();
    let contacts_checkpoint_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CONTACTS_FOLDER_ID)
            .unwrap()
            .id;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(contacts_checkpoint_id),
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
        deleted_contact_ids: vec![contact_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        b"client-content-state",
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(contains_bytes(
        &response_rops,
        &mapi_deleted_message_idset_property(&[contact_id])
    ));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(stream.deleted_idset.is_some());
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

    assert_eq!(mapi_sync_manifest_counts(&hierarchy_rops), Some((35, 0)));
    assert!(!contains_bytes(&hierarchy_rops, b"Inbox scoped sync"));
    assert!(!contains_bytes(&hierarchy_rops, b"Sent scoped sync"));
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
async fn mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let event_id = Uuid::parse_str("71717171-7171-7171-7171-717171717171").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: FakeStore::rights(),
            date: "2026-05-25".to_string(),
            time: "14:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 45,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar sync appointment".to_string(),
            location: "Conference room".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: "Calendar sync body".to_string(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 55,
        current_modseq: 41,
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store.clone(), 16, &[]).await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].subject,
        "Calendar sync appointment"
    );
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("Conference room")));
    assert!(contains_bytes(
        &response_rops,
        &0x0060_0040u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x0061_0040u32.to_le_bytes()
    ));
    for property_tag in [
        0x8001_0102u32,
        0x8002_0102,
        0x820D_0040,
        0x820E_0040,
        0x8215_000B,
        0x8217_0003,
        0x8233_0102,
        0x8234_001F,
        0x825E_0102,
        0x825F_0102,
    ] {
        assert!(contains_bytes(&response_rops, &property_tag.to_le_bytes()));
    }
    assert!(contains_bytes(&response_rops, &utf16z("UTC")));
    assert!(contains_bytes(
        &response_rops,
        &[
            0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82,
            0xE0, 0x08,
        ]
    ));
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    .unwrap()
                    .id,
            ),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap();
    let checkpoint = checkpoint.unwrap();
    assert_eq!(checkpoint.last_change_sequence, 55);
    assert_eq!(checkpoint.last_modseq, 41);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(|id| id.as_u64()),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
}

#[tokio::test]
async fn mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    .unwrap()
                    .id,
            ),
            MapiCheckpointKind::Content,
            4,
            5,
            serde_json::json!({
                "source": "emsmdb-ics-download",
                "syncRootFolderId": crate::mapi::identity::CALENDAR_FOLDER_ID
            }),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 4,
        current_modseq: 5,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x67DA_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x67D2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
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
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    assert!(!contains_bytes(&response_rops, b"<UserConfiguration>"));
    assert!(!contains_bytes(&response_rops, b"18-OLPrefsVersion"));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.CategoryList")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.WorkHours")
    ));
    assert!(!contains_bytes(&response_rops, b"CategoryList.xsd"));
    assert!(!contains_bytes(&response_rops, b"WorkingHours.xsd"));
}

#[tokio::test]
async fn mapi_over_http_calendar_associated_contents_columns_include_configuration_properties() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let store = FakeStore {
        session: Some(account),
        calendar_collections: Arc::new(Mutex::new(vec![calendar])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x02, // RopGetContentsTable, associated contents
        0x37, 0x00, 0x02, // RopQueryColumnsAll
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

    let query_columns_offset = 18;
    assert_eq!(response_rops[query_columns_offset], 0x37);
    assert!(contains_bytes(
        &response_rops,
        &0x67AA_000Bu32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x7C06_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x7C07_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x7C08_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint() {
    let account = FakeStore::account();
    let contacts = FakeStore::collection("default", "contacts", "Contacts");
    let first = FakeStore::contact(
        "71717171-7171-7171-7171-717171717171",
        "Contact Sync One",
        "one@example.test",
    );
    let second = FakeStore::contact(
        "72727272-7272-7272-7272-727272727272",
        "Contact Sync Two",
        "two@example.test",
    );
    let store = FakeStore {
        session: Some(account.clone()),
        contact_collections: Arc::new(Mutex::new(vec![contacts])),
        contacts: Arc::new(Mutex::new(vec![first, second])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 56,
        current_modseq: 42,
        ..Default::default()
    };

    let response_rops = content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        &[],
    )
    .await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 2);
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Contact")));
    assert!(contains_bytes(&response_rops, &utf16z("Contact Sync One")));
    assert!(contains_bytes(&response_rops, &utf16z("one@example.test")));
    assert!(contains_bytes(&response_rops, &utf16z("Contact Sync Two")));
    assert!(contains_bytes(&response_rops, &utf16z("two@example.test")));

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CONTACTS_FOLDER_ID)
                    .unwrap()
                    .id,
            ),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap();
    let checkpoint = checkpoint.unwrap();
    assert_eq!(checkpoint.last_change_sequence, 56);
    assert_eq!(checkpoint.last_modseq, 42);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(|id| id.as_u64()),
        Some(crate::mapi::identity::CONTACTS_FOLDER_ID)
    );
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_projects_postgresql_canonical_event_properties(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    storage
        .upsert_client_event(UpsertClientEventInput {
            id: Some(Uuid::parse_str("71717171-7171-4171-9171-717171717171").unwrap()),
            account_id,
            uid: "mapi-calendar-postgres".to_string(),
            date: "2026-05-25".to_string(),
            time: "14:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 0,
            all_day: true,
            status: "tentative".to_string(),
            sequence: 7,
            recurrence_rule: "FREQ=DAILY;COUNT=2".to_string(),
            recurrence_json: r#"{"frequency":"daily","count":2}"#.to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "PostgreSQL calendar appointment".to_string(),
            location: "Room 420".to_string(),
            organizer_json: r#"{"email":"alice@example.test","common_name":"Alice Calendar"}"#.to_string(),
            attendees: "bob@example.test".to_string(),
            attendees_json: r#"{"organizer":{"email":"alice@example.test","common_name":"Alice Calendar"},"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"accepted","rsvp":false}]}"#.to_string(),
            notes: "Canonical body text".to_string(),
            body_html: "<p>Canonical body text</p>".to_string(),
        })
        .await?;

    let response_rops = content_sync_response_rops_for_store(
        storage,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
    )
    .await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].subject,
        "PostgreSQL calendar appointment"
    );
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("Room 420")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Canonical body text")
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x8205_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &1i32.to_le_bytes()));
    assert!(contains_bytes(
        &response_rops,
        &0x8215_000Bu32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &[1]));
    assert!(contains_bytes(
        &response_rops,
        &0x8217_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &1i32.to_le_bytes()));
    assert!(contains_bytes(
        &response_rops,
        &0x8001_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x8002_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::filetime_from_rfc3339_utc("2026-05-25T14:30:00Z").to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::filetime_from_rfc3339_utc("2026-05-25T14:31:00Z").to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x0E1B_000Bu32.to_le_bytes()
    ));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_calendar_contents_table_projects_postgresql_canonical_event_properties(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    storage
        .upsert_client_event(UpsertClientEventInput {
            id: Some(Uuid::parse_str("72727272-7272-4272-9272-727272727272").unwrap()),
            account_id: fixture.account_id,
            uid: "mapi-calendar-table-postgres".to_string(),
            date: "2026-05-25".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 45,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 1,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Contents table appointment".to_string(),
            location: "Room 421".to_string(),
            organizer_json: r#"{"email":"alice@example.test","common_name":"Alice Calendar"}"#
                .to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: "Contents table body".to_string(),
            body_html: String::new(),
        })
        .await?;

    let service = ExchangeService::new(storage);
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
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    let columns = [
        0x001A_001Fu32, // PidTagMessageClass
        0x0037_001Fu32, // PidTagSubject
        0x1000_001Fu32, // PidTagBody
        0x820D_0040u32, // PidLidAppointmentStartWhole
        0x820E_0040u32, // PidLidAppointmentEndWhole
        0x8205_0003u32, // PidLidBusyStatus
        0x8215_000Bu32, // PidLidAppointmentSubType
        0x8217_0003u32, // PidLidAppointmentStateFlags
        0x8001_0102u32, // PidLidGlobalObjectId
        0x8002_0102u32, // PidLidCleanGlobalObjectId
        0x3FFB_001Fu32, // PidTagLocation
        0x0E1B_000Bu32, // PidTagHasAttachments
    ];
    rops.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        rops.extend_from_slice(&column.to_le_bytes());
    }
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
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
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Contents table appointment")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Contents table body")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Room 421")));
    assert!(contains_bytes(&response_rops, &2i32.to_le_bytes()));
    assert!(contains_bytes(&response_rops, &[0]));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_projects_postgresql_custom_calendar_collection(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let collection = storage
        .create_accessible_calendar_collection(fixture.account_id, "Outlook Custom Calendar")
        .await?;
    let event = storage
        .create_accessible_event(
            fixture.account_id,
            Some(&collection.id),
            UpsertClientEventInput {
                id: Some(Uuid::parse_str("73737373-7373-4373-9373-737373737373").unwrap()),
                account_id: fixture.account_id,
                uid: "mapi-calendar-custom-postgres".to_string(),
                date: "2026-05-25".to_string(),
                time: "11:00".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 30,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 1,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Custom calendar appointment".to_string(),
                location: "Room 422".to_string(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: "{}".to_string(),
                notes: "Custom calendar body".to_string(),
                body_html: String::new(),
            },
        )
        .await?;
    assert_eq!(event.collection_id, collection.id);
    let canonical_events = storage
        .fetch_accessible_events_in_collection(fixture.account_id, &collection.id)
        .await?;
    assert_eq!(canonical_events.len(), 1);

    let snapshot = storage
        .load_mapi_mail_store(fixture.account_id, 500)
        .await?;
    let folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.display_name == "Outlook Custom Calendar")
        .expect("custom calendar folder projected");
    assert_ne!(folder.id, crate::mapi::identity::CALENDAR_FOLDER_ID);
    assert_eq!(snapshot.events_for_folder(folder.id).len(), 1);

    let response_rops = content_sync_response_rops_for_store(storage, folder.id, &[]).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(
        stream.message_changes[0].subject,
        "Custom calendar appointment"
    );
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("Room 422")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Custom calendar body")
    ));

    fixture.cleanup().await?;
    Ok(())
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
    assert!(!stream.cnset_seen_fai.is_empty());
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
        &final_state.cnset_seen_fai,
        &final_state.cnset_read,
    ] {
        strict_validate_replguid_globset(value).unwrap();
        assert!(strict_validate_replid_globset(value).is_err());
    }
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
        &checkpoint_state.cnset_seen_fai,
        &checkpoint_state.cnset_read,
    ] {
        strict_validate_replguid_globset(value).unwrap();
        assert!(strict_validate_replid_globset(value).is_err());
    }
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
    assert!(contains_bytes(&response_rops, b"brief.pdf"));
    assert!(contains_bytes(&response_rops, b"application/pdf"));
    assert!(contains_bytes(&response_rops, file_reference.as_bytes()));
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
    assert!(contains_bytes(&response_rops, b"to@example.test"));
    assert!(contains_bytes(&response_rops, b"Visible To"));
    assert!(contains_bytes(&response_rops, b"cc@example.test"));
    assert!(contains_bytes(&response_rops, b"Visible Cc"));
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
        Some((0x0000_0011, 2))
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
        Some("Inbox")
    );
    assert_eq!(
        decoded
            .folder_changes
            .first()
            .and_then(|folder| folder.folder_id),
        Some(test_mapi_folder_id(5))
    );
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
async fn mapi_over_http_hierarchy_sync_does_not_publish_recoverable_items() {
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
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let names = decoded
        .folder_changes
        .iter()
        .map(|folder| folder.display_name.as_str())
        .collect::<Vec<_>>();

    for name in [
        "Recoverable Items",
        "Deletions",
        "Purges",
        "Versions",
        "DiscoveryHolds",
    ] {
        assert!(!names.contains(&name));
        assert!(!contains_bytes(&response_rops, &utf16z(name)));
    }
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
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    ] {
        let counter = crate::mapi::identity::global_counter_from_store_id(folder_id)
            .expect("stable folder counter");
        assert!(
            strict_replguid_globset_contains_counter(&decoded.idset_given, &globcnt_bytes(counter))
                .expect("hierarchy final IDSET"),
            "final hierarchy state should acknowledge advertised stable folder 0x{folder_id:016x}"
        );
    }
    let sync_issues = decoded
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Sync Issues")
        .expect("Sync Issues folderChange");
    assert_eq!(sync_issues.parent_source_key, Vec::<u8>::new());
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
fn mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy() {
    let reminders =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::REMINDERS_FOLDER_ID)
            .expect("Reminders mailbox");
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
        std::slice::from_ref(&reminders),
        &[],
        &[],
        &[],
        std::slice::from_ref(&reminders),
        std::slice::from_ref(&reminders),
        &[],
        &[],
        &[],
        &[],
        1,
    );
    let decoded = strict_decode_hierarchy_sync_stream(&buffer).expect("strict hierarchy ICS");
    assert_eq!(decoded.folder_changes.len(), 1);
    assert_eq!(decoded.folder_changes[0].display_name, "Reminders");
    assert_eq!(decoded.folder_changes[0].folder_type, None);
    assert_eq!(
        decoded.folder_changes[0].parent_source_key,
        Vec::<u8>::new()
    );
    assert_eq!(
        decoded.folder_changes[0].parent_folder_id,
        Some(crate::mapi::identity::ROOT_FOLDER_ID)
    );
    assert!(contains_bytes(&buffer, &utf16z("Outlook.Reminder")));
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
async fn mapi_over_http_hierarchy_table_includes_default_ipm_special_folders() {
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3613_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x65E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

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
    assert_eq!(response_rops[8], 0x04);
    assert_eq!(
        u32::from_le_bytes(response_rops[14..18].try_into().unwrap()),
        OUTLOOK_IPM_HIERARCHY_TABLE_FOLDER_COUNT
    );
    let query_offset = 8 + 10 + 7;
    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        OUTLOOK_IPM_HIERARCHY_TABLE_FOLDER_COUNT as u16
    );

    for (name, class, folder_id) in [
        (
            "Calendar",
            "IPF.Appointment",
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        ),
        (
            "Contacts",
            "IPF.Contact",
            crate::mapi::identity::CONTACTS_FOLDER_ID,
        ),
        (
            "Suggested Contacts",
            "IPF.Contact",
            crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        ),
        (
            "Quick Contacts",
            "IPF.Contact.MOC.QuickContacts",
            crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
        ),
        (
            "IM Contact List",
            "IPF.Contact.MOC.ImContactList",
            crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
        ),
        (
            "Contacts Search",
            "IPF.Contact",
            crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
        ),
        (
            "Journal",
            "IPF.Journal",
            crate::mapi::identity::JOURNAL_FOLDER_ID,
        ),
        (
            "Notes",
            "IPF.StickyNote",
            crate::mapi::identity::NOTES_FOLDER_ID,
        ),
        ("Tasks", "IPF.Task", crate::mapi::identity::TASKS_FOLDER_ID),
        (
            "Sync Issues",
            "IPF.Note",
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
        ),
        (
            "Conflicts",
            "IPF.Note",
            crate::mapi::identity::CONFLICTS_FOLDER_ID,
        ),
        (
            "Local Failures",
            "IPF.Note",
            crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID,
        ),
        (
            "Server Failures",
            "IPF.Note",
            crate::mapi::identity::SERVER_FAILURES_FOLDER_ID,
        ),
        (
            "Junk E-mail",
            "IPF.Note",
            crate::mapi::identity::JUNK_FOLDER_ID,
        ),
        (
            "RSS Feeds",
            "IPF.Note.OutlookHomepage",
            crate::mapi::identity::RSS_FEEDS_FOLDER_ID,
        ),
        (
            "Conversation Action Settings",
            "IPF.Configuration",
            crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        ),
        (
            "Archive",
            "IPF.Note",
            crate::mapi::identity::ARCHIVE_FOLDER_ID,
        ),
        (
            "Conversation History",
            "IPF.Note",
            crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
        ),
    ] {
        assert!(contains_bytes(&response_rops, &utf16z(name)));
        assert!(contains_bytes(&response_rops, &utf16z(class)));
        assert!(contains_bytes(
            &response_rops,
            &mapi_mailstore::source_key_for_store_id(folder_id)
        ));
    }
}

#[tokio::test]
async fn mapi_over_http_root_hierarchy_table_uses_none_container_classes_for_root_children() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(
                "11111111-1111-4111-8111-111111111111",
                "__mapi_deferred_action",
                "Deferred Action",
            ),
            FakeStore::mailbox(
                "22222222-2222-4222-8222-222222222222",
                "__mapi_spooler_queue",
                "Spooler Queue",
            ),
            FakeStore::mailbox(
                "33333333-3333-4333-8333-333333333333",
                "__mapi_common_views",
                "Common Views",
            ),
            FakeStore::mailbox(
                "44444444-4444-4444-8444-444444444444",
                "__mapi_views",
                "Personal Views",
            ),
            FakeStore::mailbox(
                "55555555-5555-4555-8555-555555555555",
                "__mapi_freebusy_data",
                "FreeBusy Data",
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_CONTAINER_CLASS_W.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

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
    let rows = hierarchy_query_display_container_rows(&response_rops, 8 + 10 + 7)
        .expect("root hierarchy rows");

    for name in [
        "Deferred Action",
        "Spooler Queue",
        "Common Views",
        "Schedule",
        "Personal Views",
        "FreeBusy Data",
    ] {
        let (_, container_class) = rows
            .iter()
            .find(|(display_name, _)| display_name == name)
            .unwrap_or_else(|| panic!("{name} hierarchy row"));
        assert_eq!(container_class, "");
    }
    let (_, shortcuts_container_class) = rows
        .iter()
        .find(|(display_name, _)| display_name == "Shortcuts")
        .expect("Shortcuts hierarchy row");
    assert_eq!(shortcuts_container_class, "IPF.ShortcutFolder");
    assert!(!contains_bytes(&response_rops, &utf16z("IPF.Root")));
}

#[tokio::test]
async fn mapi_over_http_logon_advertises_openable_additional_ren_entryids_ex() {
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

    let mut rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut rops, 1, &[PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let get_props_offset = response_rops
        .windows(7)
        .rposition(|window| window == [0x07, 0x01, 0, 0, 0, 0, 0])
        .expect("AdditionalRenEntryIdsEx GetProperties response");
    let value_offset = get_props_offset + 7;
    let value_len = u16::from_le_bytes(
        response_rops[value_offset..value_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let value = &response_rops[value_offset + 2..value_offset + 2 + value_len];
    let entries = additional_ren_entry_ids_ex_entries(value);
    let expected = vec![
        (
            0x8001,
            crate::mapi::identity::RSS_FEEDS_FOLDER_ID,
            "RSS Feeds",
            "IPF.Note.OutlookHomepage",
        ),
        (
            0x8002,
            crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID,
            "Tracked Mail Processing",
            "IPF.Note",
        ),
        (
            0x8004,
            crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
            "To-Do",
            "IPF.Task",
        ),
        (
            0x8006,
            crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            "Conversation Action Settings",
            "IPF.Configuration",
        ),
        (
            0x8008,
            crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
            "Suggested Contacts",
            "IPF.Contact",
        ),
        (
            0x8009,
            crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
            "Contacts Search",
            "IPF.Contact",
        ),
        (
            0x800A,
            crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
            "IM Contact List",
            "IPF.Contact.MOC.ImContactList",
        ),
        (
            0x800B,
            crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
            "Quick Contacts",
            "IPF.Contact.MOC.QuickContacts",
        ),
    ];
    assert_eq!(
        entries,
        expected
            .iter()
            .map(|(persist_id, folder_id, _, _)| (*persist_id, *folder_id))
            .collect::<Vec<_>>()
    );

    for (_, folder_id, display_name, container_class) in expected {
        renew_mapi_request_id(&mut execute_headers);
        let mut rops = Vec::new();
        append_rop_open_folder(&mut rops, 0, 1, folder_id);
        append_rop_get_properties_specific(
            &mut rops,
            1,
            &[PID_TAG_DISPLAY_NAME_W, PID_TAG_CONTAINER_CLASS_W],
        );
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
        assert_eq!(response_rops[0], 0x02);
        assert_eq!(
            u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
            0
        );
        assert!(contains_bytes(&response_rops, &utf16z(display_name)));
        assert!(contains_bytes(&response_rops, &utf16z(container_class)));
    }
}

#[tokio::test]
async fn mapi_over_http_default_ipm_entry_ids_open_expected_folders() {
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

    let default_properties = [
        (
            0x36D0_0102,
            "Calendar",
            "IPF.Appointment",
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        ),
        (
            0x36D1_0102,
            "Contacts",
            "IPF.Contact",
            crate::mapi::identity::CONTACTS_FOLDER_ID,
        ),
        (
            0x36D2_0102,
            "Journal",
            "IPF.Journal",
            crate::mapi::identity::JOURNAL_FOLDER_ID,
        ),
        (
            0x36D3_0102,
            "Notes",
            "IPF.StickyNote",
            crate::mapi::identity::NOTES_FOLDER_ID,
        ),
        (
            0x36D4_0102,
            "Tasks",
            "IPF.Task",
            crate::mapi::identity::TASKS_FOLDER_ID,
        ),
    ];

    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(
        &mut rops,
        0,
        &default_properties
            .iter()
            .map(|(property_tag, _, _, _)| *property_tag)
            .collect::<Vec<_>>(),
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
    let mut cookie = mapi_cookie_header(&response);
    let response_rops = response_rops_from_execute_response(response).await;
    let get_props_offset = response_rops
        .windows(6)
        .rposition(|window| window == [0x07, 0x00, 0, 0, 0, 0].as_slice())
        .unwrap();
    let mut values_offset = get_props_offset + 7;
    let mut entry_ids = Vec::new();
    for (_, _, _, folder_id) in default_properties {
        let len = u16::from_le_bytes(
            response_rops[values_offset..values_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        values_offset += 2;
        let entry_id = response_rops[values_offset..values_offset + len].to_vec();
        values_offset += len;
        assert!(!entry_id.is_empty());
        assert_eq!(
            crate::mapi::identity::object_id_from_folder_identifier_bytes(&entry_id),
            Some(folder_id)
        );
        entry_ids.push(entry_id);
    }

    for ((_, display_name, container_class, folder_id), _entry_id) in
        default_properties.into_iter().zip(entry_ids)
    {
        let mut rops = Vec::new();
        append_rop_open_folder(&mut rops, 0, 1, folder_id);
        append_rop_get_properties_specific(&mut rops, 1, &[0x3001_001F, 0x3613_001F, 0x65E0_0102]);

        let mut execute_headers = mapi_headers("Execute");
        execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
            )
            .await
            .unwrap();
        cookie = mapi_cookie_header(&response);
        let response_rops = response_rops_from_execute_response(response).await;
        assert!(contains_bytes(
            &response_rops,
            &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
        ));
        assert!(contains_bytes(&response_rops, &utf16z(display_name)));
        assert!(contains_bytes(&response_rops, &utf16z(container_class)));
        assert!(contains_bytes(
            &response_rops,
            &mapi_mailstore::source_key_for_store_id(folder_id)
        ));
    }
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
async fn mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
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
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;
    let hierarchy = strict_hierarchy_sync_transfer_from_response(&hierarchy_rops).unwrap();
    let contacts = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Contacts")
        .expect("Contacts hierarchy row");
    assert_eq!(
        contacts.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(contacts.container_class.as_deref(), Some("IPF.Contact"));
    let calendar = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Calendar")
        .expect("Calendar hierarchy row");
    assert_eq!(
        calendar.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(calendar.container_class.as_deref(), Some("IPF.Appointment"));

    let contacts_rops = content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        &[],
    )
    .await;
    let contacts_stream = strict_content_sync_transfer_from_response(&contacts_rops).unwrap();
    assert!(contacts_stream.message_changes.is_empty());
    let contacts_checkpoint_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CONTACTS_FOLDER_ID)
            .unwrap()
            .id;
    assert!(store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(contacts_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .is_some());

    let calendar_rops = content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
    )
    .await;
    let calendar_stream = strict_content_sync_transfer_from_response(&calendar_rops).unwrap();
    assert!(calendar_stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &calendar_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    let calendar_checkpoint_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
            .unwrap()
            .id;
    assert!(store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .is_some());
    assert!(store.contact_collections.lock().unwrap().is_empty());
    assert!(store.calendar_collections.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555501").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-4777-8777-777777777701").unwrap();
    let message_id = Uuid::parse_str("46464646-4646-4646-8646-464646464602").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let mut trash = FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items");
    trash.total_emails = 1;
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![inbox.clone(), trash.clone()])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &trash_id.to_string(),
            "trash",
            "Observed startup trash message",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        search_folders: Arc::new(Mutex::new(vec![
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343401").unwrap(),
                account_id: account.account_id,
                role: "reminders".to_string(),
                display_name: "Reminders".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "mixed".to_string(),
                scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
                restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
                excluded_folder_roles: exchange_reminder_excluded_folder_roles(),
                is_builtin: true,
            },
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343402").unwrap(),
                account_id: account.account_id,
                role: "contacts_search".to_string(),
                display_name: "Contacts Search".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "contact".to_string(),
                scope_json: serde_json::json!({"scope": "contacts"}),
                restriction_json: serde_json::json!({"kind": "contacts_search"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343403").unwrap(),
                account_id: account.account_id,
                role: "tracked_mail_processing".to_string(),
                display_name: "Tracked Mail Processing".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "message".to_string(),
                scope_json: serde_json::json!({"scope": "mail"}),
                restriction_json: serde_json::json!({"kind": "tracked_mail_processing"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343404").unwrap(),
                account_id: account.account_id,
                role: "todo_search".to_string(),
                display_name: "To-Do".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "task".to_string(),
                scope_json: serde_json::json!({"scope": "tasks"}),
                restriction_json: serde_json::json!({"kind": "todo_search"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
        ])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
            88,
            44,
            serde_json::json!({"source": "full-trash-content-sync"}),
        )
        .await
        .unwrap();
    let calendar_checkpoint_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
            .unwrap()
            .id;
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
            89,
            45,
            serde_json::json!({
                "source": "emsmdb-ics-download",
                "syncRootFolderId": crate::mapi::identity::CALENDAR_FOLDER_ID
            }),
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

    let nspi_headers = nspi_bound_headers(&service, "DNToMId").await;
    let nspi_dn_to_mid = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, b"alice@example.test\0")
        .await
        .unwrap();
    assert_eq!(nspi_dn_to_mid.headers().get("x-responsecode").unwrap(), "0");
    let nspi_dn_to_mid_body = response_bytes(nspi_dn_to_mid).await;
    let principal_mid = u32::from_le_bytes(nspi_dn_to_mid_body[13..17].try_into().unwrap());
    assert_ne!(principal_mid, 0);

    let mut nspi_get_props_request = Vec::new();
    nspi_get_props_request.extend_from_slice(&principal_mid.to_le_bytes());
    nspi_get_props_request.extend_from_slice(&0x8C6D_0102u32.to_le_bytes());
    let nspi_headers = nspi_bound_headers(&service, "GetProps").await;
    let nspi_guid = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_get_props_request)
        .await
        .unwrap();
    let nspi_guid_body = response_bytes(nspi_guid).await;
    assert!(contains_bytes(
        &nspi_guid_body,
        account.account_id.to_bytes_le().as_slice()
    ));

    let mut nspi_smtp_request = Vec::new();
    nspi_smtp_request.extend_from_slice(&principal_mid.to_le_bytes());
    nspi_smtp_request.extend_from_slice(&0x39FE_001Fu32.to_le_bytes());
    let nspi_smtp = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_smtp_request)
        .await
        .unwrap();
    let nspi_smtp_body = response_bytes(nspi_smtp).await;
    assert!(contains_bytes(
        &nspi_smtp_body,
        &utf16z("alice@example.test")
    ));

    let mut nspi_email_request = Vec::new();
    nspi_email_request.extend_from_slice(&principal_mid.to_le_bytes());
    nspi_email_request.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    let nspi_email = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_email_request)
        .await
        .unwrap();
    let nspi_email_body = response_bytes(nspi_email).await;
    assert!(contains_bytes(
        &nspi_email_body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));
    assert!(!contains_bytes(
        &nspi_email_body,
        &utf16z("alice@example.test")
    ));

    let mut nspi_query_rows_request = hex_bytes(
        "00000000ff000000000000000000000000000000000000000000000000e40400000904000009080000\
         010000002600008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a\
         1f0003301f0002300b00403a1f00ff391f00",
    );
    nspi_query_rows_request[49..53].copy_from_slice(&principal_mid.to_le_bytes());
    let nspi_headers = nspi_bound_headers(&service, "QueryRows").await;
    let nspi_query_rows = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_query_rows_request)
        .await
        .unwrap();
    let nspi_query_rows_body = response_bytes(nspi_query_rows).await;
    assert!(contains_bytes(
        &nspi_query_rows_body,
        &utf16z("alice@example.test")
    ));
    assert!(contains_bytes(
        &nspi_query_rows_body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));
    assert!(contains_bytes(&nspi_query_rows_body, &utf16z("alice")));
    assert!(contains_bytes(&nspi_query_rows_body, &utf16z("EX")));
    assert!(!contains_bytes(&nspi_query_rows_body, &utf16z("SMTP")));

    let bootstrap_connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    assert_eq!(
        bootstrap_connect.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&bootstrap_connect)).unwrap(),
    );
    let mut bootstrap_rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    bootstrap_rops.extend_from_slice(&0u32.to_le_bytes());
    bootstrap_rops.extend_from_slice(&0u32.to_le_bytes());
    bootstrap_rops.extend_from_slice(&0u16.to_le_bytes());
    let bootstrap_logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_logon.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_logon.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let mut bootstrap_cookie = mapi_cookie_header(&bootstrap_logon);
    let bootstrap_logon_rops = response_rops_from_execute_response(bootstrap_logon).await;
    assert!(contains_bytes(
        &bootstrap_logon_rops,
        &[0xFE, 0x00, 0, 0, 0, 0]
    ));

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let mut bootstrap_getprops = Vec::new();
    append_rop_get_properties_specific(
        &mut bootstrap_getprops,
        0,
        &[
            0x6638_0102, // PidTagSerializedReplidGuidMap
        ],
    );
    let bootstrap_getprops_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_getprops, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_getprops_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_getprops_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_getprops_response);
    let bootstrap_getprops_rops =
        response_rops_from_execute_response(bootstrap_getprops_response).await;
    assert!(contains_bytes(
        &bootstrap_getprops_rops,
        &[0x07, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &bootstrap_getprops_rops,
        &crate::mapi::identity::STORE_REPLICA_GUID[..4]
    ));

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let mut bootstrap_named_props = vec![
        0x56, 0x00, 0x00, 0x02, // RopGetPropertyIdsFromNames, create missing on Logon
    ];
    bootstrap_named_props.extend_from_slice(&2u16.to_le_bytes());
    for lid in [0x8580u32, 0x8581u32] {
        bootstrap_named_props.push(0x00);
        bootstrap_named_props.extend_from_slice(&[
            0x08, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ]);
        bootstrap_named_props.extend_from_slice(&lid.to_le_bytes());
    }
    let bootstrap_named_props_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_named_props, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_named_props_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_named_props_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_named_props_response);
    let bootstrap_named_props_rops =
        response_rops_from_execute_response(bootstrap_named_props_response).await;
    assert!(contains_bytes(
        &bootstrap_named_props_rops,
        &[0x56, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &bootstrap_named_props_rops,
        &[0x02, 0x00, 0x03, 0x80, 0x04, 0x80]
    ));

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let bootstrap_address_types = vec![
        0x49, 0x00, 0x00, // RopGetAddressTypes
    ];
    let bootstrap_address_types_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_address_types, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_address_types_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_address_types_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_address_types_response);
    let bootstrap_address_types_rops =
        response_rops_from_execute_response(bootstrap_address_types_response).await;
    assert!(contains_bytes(
        &bootstrap_address_types_rops,
        &[0x49, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&bootstrap_address_types_rops, b"EX\0SMTP\0"));

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let mut bootstrap_store_props = Vec::new();
    append_rop_get_properties_specific(
        &mut bootstrap_store_props,
        0,
        &[
            0x661C_001F, // PidTagMailboxOwnerName
            0x661B_0102, // PidTagMailboxOwnerEntryId
            0x341D_001F, // PidTagServerTypeDisplayName
            0x341E_0102, // PidTagServerConnectedIcon
            0x341F_0102, // PidTagServerAccountIcon
            0x0E5C_000B, // PidTagPrivate
            0x346F_0003, // PidTagOutlookStoreState
            0x6707_0102, // PidTagUserGuid
        ],
    );
    let bootstrap_store_props_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_store_props, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_store_props_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_store_props_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_store_props_response);
    let bootstrap_store_props_rops =
        response_rops_from_execute_response(bootstrap_store_props_response).await;
    assert!(contains_bytes(
        &bootstrap_store_props_rops,
        &[0x07, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &bootstrap_store_props_rops,
        &utf16z("Alice")
    ));
    assert!(contains_bytes(&bootstrap_store_props_rops, &utf16z("LPE")));

    let mut cookie = bootstrap_cookie;

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&hierarchy_response);
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;
    let hierarchy = strict_hierarchy_sync_transfer_from_response(&hierarchy_rops).unwrap();
    let calendar = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Calendar")
        .expect("Calendar hierarchy row");
    let calendar_source_key = calendar.source_key.clone();
    assert_eq!(
        calendar.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(calendar.container_class.as_deref(), Some("IPF.Appointment"));
    assert!(!hierarchy
        .folder_changes
        .iter()
        .any(|folder| folder.display_name == "Reminders"));

    let hierarchy_checkpoint = store
        .fetch_mapi_sync_checkpoint(account.account_id, None, MapiCheckpointKind::Hierarchy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hierarchy_checkpoint.last_change_sequence, 89);
    assert_eq!(hierarchy_checkpoint.last_modseq, 45);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut max_submit_rops = Vec::new();
    append_rop_get_properties_specific(
        &mut max_submit_rops,
        0,
        &[0x666D_0003], // PidTagMaxSubmitMessageSize
    );
    let max_submit_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&max_submit_rops, &[1])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&max_submit_response);
    let max_submit_rops = response_rops_from_execute_response(max_submit_response).await;
    assert!(contains_bytes(&max_submit_rops, &[0x07, 0x00, 0, 0, 0, 0]));
    assert!(contains_bytes(&max_submit_rops, &35840u32.to_le_bytes()));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut default_probe_rops = vec![0xFE, 0x00, 0x00, 0x01];
    default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    default_probe_rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut default_probe_rops, 0, &[0x36D0_0102]);
    append_rop_open_folder(
        &mut default_probe_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_get_properties_specific(&mut default_probe_rops, 1, &[0x3001_001F, 0x3613_001F]);
    let default_probe = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&default_probe_rops, &[u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&default_probe);
    let default_probe_rops = response_rops_from_execute_response(default_probe).await;
    assert!(contains_bytes(
        &default_probe_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&default_probe_rops, &utf16z("Calendar")));
    assert!(contains_bytes(
        &default_probe_rops,
        &utf16z("IPF.Appointment")
    ));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut calendar_rops = Vec::new();
    append_rop_open_folder(
        &mut calendar_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    calendar_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x67DA_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x67D2_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    calendar_rops.extend_from_slice(&4096u16.to_le_bytes());
    let calendar_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&calendar_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&calendar_response);
    let calendar_rops = response_rops_from_execute_response(calendar_response).await;
    let calendar_stream = strict_content_sync_transfer_from_response(&calendar_rops).unwrap();
    assert!(calendar_stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &calendar_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    assert!(store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .is_some());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut trash_rops = Vec::new();
    append_rop_open_folder(
        &mut trash_rops,
        0,
        1,
        crate::mapi::identity::TRASH_FOLDER_ID,
    );
    trash_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x82, 0x00, 0x02, 0x03, 0x4E, 0x00, 0x03,
    ]);
    trash_rops.extend_from_slice(&4096u16.to_le_bytes());
    let trash_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&trash_rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&trash_response);
    let trash_rops = response_rops_from_execute_response(trash_response).await;
    assert!(!contains_bytes(
        &trash_rops,
        &utf16z("Observed startup trash message")
    ));
    let trash_checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trash_checkpoint.last_change_sequence, 88);
    assert_eq!(trash_checkpoint.last_modseq, 44);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut root_rops = Vec::new();
    append_rop_open_folder(&mut root_rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut root_rops, 1, 2, 20000);
    let root_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&root_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&root_response);
    let root_rops = response_rops_from_execute_response(root_response).await;
    let root_hierarchy = strict_hierarchy_sync_transfer_from_response(&root_rops).unwrap();
    for name in ["Reminders", "Tracked Mail Processing", "To-Do"] {
        let folder = root_hierarchy
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("{name} hierarchy row"));
        assert_eq!(
            folder.parent_folder_id,
            Some(crate::mapi::identity::ROOT_FOLDER_ID)
        );
    }

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut common_views_rops = Vec::new();
    append_rop_open_folder(
        &mut common_views_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    common_views_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x39, 0xA1, // content sync, observed Outlook flags 0xa139
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x00, 0x00, // PropertyTagCount
    ]);
    for state_tag in [0x4017_0102u32, 0x6796_0102, 0x67DA_0102, 0x67D2_0102] {
        common_views_rops.extend_from_slice(&[0x75, 0x00, 0x02]);
        common_views_rops.extend_from_slice(&state_tag.to_le_bytes());
        common_views_rops.extend_from_slice(&0u32.to_le_bytes());
        common_views_rops.extend_from_slice(&[0x77, 0x00, 0x02]);
    }
    common_views_rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    common_views_rops.extend_from_slice(&31680u16.to_le_bytes());
    let common_views_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&common_views_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&common_views_response);
    let common_views_rops = response_rops_from_execute_response(common_views_response).await;
    let common_views_stream =
        strict_content_sync_transfer_from_response(&common_views_rops).unwrap();
    assert!(common_views_stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &common_views_rops,
        &utf16z("IPM.Microsoft.WunderBar.SFInfo")
    ));

    let mut disconnect_headers = mapi_headers("Disconnect");
    disconnect_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let disconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &disconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(disconnect.status(), StatusCode::OK);
    assert_eq!(disconnect.headers().get("x-responsecode").unwrap(), "0");

    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    assert_eq!(reconnect.status(), StatusCode::OK);
    assert_eq!(reconnect.headers().get("x-responsecode").unwrap(), "0");
    cookie = mapi_cookie_header(&reconnect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_hierarchy_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_hierarchy_rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(
        &mut reconnect_hierarchy_rops,
        1,
        2,
        4096,
    );
    let reconnect_hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_hierarchy_rops,
                &[1, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_hierarchy_response);
    let reconnect_hierarchy_rops =
        response_rops_from_execute_response(reconnect_hierarchy_response).await;
    let reconnect_hierarchy =
        strict_hierarchy_sync_transfer_from_response(&reconnect_hierarchy_rops).unwrap();
    let reconnect_calendar = reconnect_hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Calendar")
        .expect("reconnected Calendar hierarchy row");
    assert_eq!(reconnect_calendar.source_key, calendar_source_key);
    assert_eq!(
        reconnect_calendar.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(
        reconnect_calendar.container_class.as_deref(),
        Some("IPF.Appointment")
    );

    let reconnect_hierarchy_checkpoint = store
        .fetch_mapi_sync_checkpoint(account.account_id, None, MapiCheckpointKind::Hierarchy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconnect_hierarchy_checkpoint.last_change_sequence, 89);
    assert_eq!(reconnect_hierarchy_checkpoint.last_modseq, 45);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_default_probe_rops = vec![0xFE, 0x00, 0x00, 0x01];
    reconnect_default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_default_probe_rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut reconnect_default_probe_rops, 0, &[0x36D0_0102]);
    append_rop_open_folder(
        &mut reconnect_default_probe_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_get_properties_specific(
        &mut reconnect_default_probe_rops,
        1,
        &[0x3001_001F, 0x3613_001F],
    );
    let reconnect_default_probe = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_default_probe_rops,
                &[u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_default_probe);
    let reconnect_default_probe_rops =
        response_rops_from_execute_response(reconnect_default_probe).await;
    assert!(contains_bytes(
        &reconnect_default_probe_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &reconnect_default_probe_rops,
        &utf16z("Calendar")
    ));
    assert!(contains_bytes(
        &reconnect_default_probe_rops,
        &utf16z("IPF.Appointment")
    ));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_calendar_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_calendar_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    reconnect_calendar_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x75, 0x00, 0x02,
    ]);
    reconnect_calendar_rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x75, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x75, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&0x67DA_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x75, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&0x67D2_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x4E, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&4096u16.to_le_bytes());
    let reconnect_calendar_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_calendar_rops,
                &[1, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_calendar_response);
    let reconnect_calendar_rops =
        response_rops_from_execute_response(reconnect_calendar_response).await;
    let reconnect_calendar_stream =
        strict_content_sync_transfer_from_response(&reconnect_calendar_rops).unwrap();
    assert!(reconnect_calendar_stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &reconnect_calendar_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    let reconnected_calendar_checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconnected_calendar_checkpoint.last_change_sequence, 89);
    assert_eq!(reconnected_calendar_checkpoint.last_modseq, 45);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_trash_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_trash_rops,
        0,
        1,
        crate::mapi::identity::TRASH_FOLDER_ID,
    );
    reconnect_trash_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x82, 0x00, 0x02, 0x03, 0x4E, 0x00, 0x03,
    ]);
    reconnect_trash_rops.extend_from_slice(&4096u16.to_le_bytes());
    let reconnect_trash_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_trash_rops,
                &[1, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_trash_response);
    let reconnect_trash_rops = response_rops_from_execute_response(reconnect_trash_response).await;
    assert!(!contains_bytes(
        &reconnect_trash_rops,
        &utf16z("Observed startup trash message")
    ));
    let reconnect_trash_checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconnect_trash_checkpoint.last_change_sequence, 88);
    assert_eq!(reconnect_trash_checkpoint.last_modseq, 44);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_root_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_root_rops,
        0,
        1,
        crate::mapi::identity::ROOT_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut reconnect_root_rops, 1, 2, 20000);
    let reconnect_root_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&reconnect_root_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_root_response);
    let reconnect_root_rops = response_rops_from_execute_response(reconnect_root_response).await;
    let reconnect_root_hierarchy =
        strict_hierarchy_sync_transfer_from_response(&reconnect_root_rops).unwrap();
    for name in ["Reminders", "Tracked Mail Processing", "To-Do"] {
        let folder = reconnect_root_hierarchy
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("reconnected {name} hierarchy row"));
        assert_eq!(
            folder.parent_folder_id,
            Some(crate::mapi::identity::ROOT_FOLDER_ID)
        );
    }

    let mut reconnect_disconnect_headers = mapi_headers("Disconnect");
    reconnect_disconnect_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let reconnect_disconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_disconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(reconnect_disconnect.status(), StatusCode::OK);
    assert_eq!(
        reconnect_disconnect
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
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
        Vec::<u8>::new()
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
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"Selected FastTransfer"));
    assert!(!contains_bytes(&response_rops, b"Unrequested FastTransfer"));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "46464646-4646-4646-4646-464646464646",
            inbox_id,
            "inbox",
            "Folder FastTransfer",
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
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"inbox"));
    assert!(contains_bytes(&response_rops, b"Inbox"));
    assert!(contains_bytes(&response_rops, b"Folder FastTransfer"));
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
async fn mapi_over_http_per_user_information_rops_return_rop_specific_protocol_errors() {
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
    rops.extend_from_slice(&[0x60, 0x00, 0x01]);
    rops.extend_from_slice(&[0x11; 16]);
    rops.extend_from_slice(&[0x61, 0x00, 0x01]);
    rops.extend_from_slice(&[0x22; 24]);
    rops.extend_from_slice(&[0x63, 0x00, 0x01]);
    rops.extend_from_slice(&[0x33; 24]);
    rops.push(0);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&512u16.to_le_bytes());
    rops.extend_from_slice(&[0x64, 0x00, 0x01]);
    rops.extend_from_slice(&[0x44; 24]);
    rops.push(1);
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(b"LPE");

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
        &[0x60, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x61, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x63, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x64, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_rules_table_projects_canonical_sieve_rules() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let rule_id = Uuid::parse_str("aaaaaaaa-4444-4111-8111-aaaaaaaaaaaa").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mailbox_rules: Arc::new(Mutex::new(vec![MailboxRule {
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x3F, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[0x41, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x57, 0x00, 0x01]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(b"SRVR");
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(b"CLIENT");
    rops.extend_from_slice(&[0x7B, 0x00, 0x01]);

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
    assert!(contains_bytes(&response_rops, &[0x3F, 0x02, 0, 0, 0, 0]));
    let rule_name = "Reports"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    assert!(response_rops
        .windows(rule_name.len())
        .any(|window| window == rule_name));
    assert!(contains_bytes(&response_rops, &[0x41, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x57, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
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
async fn mapi_over_http_modify_rules_rejects_exchange_rule_blobs() {
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
    rops.extend_from_slice(&[0x41, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&3u16.to_le_bytes());
    append_mapi_utf16_property(&mut rops, 0x6682_001F, "Client-only");
    rops.extend_from_slice(&0x6677_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    let provider_data = serde_json::json!({"clientOnly": true}).to_string();
    rops.extend_from_slice(&0x6684_0102u32.to_le_bytes());
    rops.extend_from_slice(&(provider_data.len() as u16).to_le_bytes());
    rops.extend_from_slice(provider_data.as_bytes());

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
        &[0x41, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(active_sieve.lock().unwrap().is_none());
}

#[tokio::test]
async fn mapi_over_http_permissions_table_maps_delegate_folder_access() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let delegate_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![
            crate::mapi::permissions::owner_permission(
                Uuid::parse_str(inbox_id).unwrap(),
                &AccountPrincipal {
                    tenant_id: FakeStore::account().tenant_id,
                    account_id: FakeStore::account().account_id,
                    email: FakeStore::account().email,
                    display_name: FakeStore::account().display_name,
                },
            ),
            MapiFolderPermission {
                mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
                member_account_id: Some(delegate_id),
                member_name: "Bob Delegate".to_string(),
                rights: crate::mapi::permissions::rights_from_grant(true, true, false, false),
            },
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x3E, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6672_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&8u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x3E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &utf16z("Bob Delegate")));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::permissions::rights_from_grant(true, true, false, false).to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_modify_permissions_maps_acl_rows_to_canonical_grants() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(80);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        ..Default::default()
    };
    let observed_permissions = store.mapi_folder_permissions.clone();
    let observed_audits = store.mapi_folder_permission_audits.clone();
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
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, true, false) as i32)
            .to_le_bytes(),
    );

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
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));
    let permissions = observed_permissions.lock().unwrap();
    let delegate_permission = permissions
        .iter()
        .find(|permission| permission.member_account_id == Some(delegate.account_id))
        .expect("delegate permission was not written");
    assert_eq!(delegate_permission.mailbox_id, inbox_id);
    assert_eq!(
        delegate_permission.rights,
        crate::mapi::permissions::rights_from_grant(true, true, true, false)
    );
    let audits = observed_audits.lock().unwrap();
    assert_eq!(audits[0].action, "mapi-modify-permissions");
}

#[tokio::test]
async fn mapi_over_http_denies_mutation_without_folder_write_permission() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: crate::mapi::permissions::rights_from_grant(true, false, false, false),
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));

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
        &[0x06, 0x02, 0x05, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_denies_contents_table_without_folder_read_permission() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: crate::mapi::permissions::rights_from_grant(false, false, false, false),
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);

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
        &[0x05, 0x02, 0x05, 0x00, 0x07, 0x80]
    ));
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
    assert!(contains_bytes(&response_rops, &[0x29, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x29, 0x03, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_options_handler_reports_transport_session_ready() {
    let response = mapi_options_handler().await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("allow").unwrap(), "OPTIONS, POST");
    assert_eq!(
        response.headers().get("x-lpe-mapi-status").unwrap(),
        "transport-session-ready"
    );
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
async fn mapi_over_http_notification_wait_serializes_canonical_hierarchy_details() {
    let parent_folder_id = test_mapi_folder_id(4);
    let changed_folder_id = test_mapi_folder_id(71);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(11))),
        mapi_notification_polls: Arc::new(Mutex::new(vec![MapiNotificationPoll {
            event_pending: true,
            cursor: Some(12),
            events: vec![
                crate::mapi::notifications::MapiNotificationEvent::canonical(
                    crate::mapi::notifications::MapiNotificationKind::Hierarchy,
                    0x0010,
                    parent_folder_id,
                    Some(changed_folder_id),
                    None,
                    12,
                    45,
                    Some(0),
                    Some(0),
                    "updated".to_string(),
                    Some("Moved Projects".to_string()),
                    Some("Clients".to_string()),
                    None,
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
    append_mapi_wire_id(&mut rops, parent_folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0110u16.to_le_bytes());
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
    assert_eq!(u16::from_le_bytes(body[16..18].try_into().unwrap()), 0x0010);
    assert_eq!(body[18], 2);
    assert_eq!(body[19], 0b0000_1101);
    assert_eq!(&body[20..28], &mapi_wire_id_bytes(parent_folder_id));
    assert_eq!(&body[28..36], &mapi_wire_id_bytes(changed_folder_id));
    assert_eq!(u64::from_le_bytes(body[44..52].try_into().unwrap()), 12);
    assert_eq!(u64::from_le_bytes(body[52..60].try_into().unwrap()), 45);
    assert_eq!(u32::from_le_bytes(body[60..64].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[64..68].try_into().unwrap()), 0);
    let details = notification_detail_strings(&body[68..]);
    assert_eq!(
        details,
        vec!["mailbox", "updated", "Moved Projects", "Clients", ""]
    );
}

#[tokio::test]
async fn mapi_over_http_notification_wait_reports_hierarchy_event_after_registered_create_folder() {
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        created_mailboxes: created_mailboxes.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0104u16.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x1C, 0x00, 0x01, 0x03, 0x01, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&utf16z("MAPI Notifications"));
    rops.extend_from_slice(&utf16z(""));

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
    assert_eq!(created_mailboxes.lock().unwrap().len(), 1);

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
    assert_eq!(body[18], 2);
    assert!(contains_bytes(
        &body,
        &mapi_wire_id_bytes(test_mapi_folder_id(1))
    ));
}

#[tokio::test]
async fn mapi_over_http_async_table_control_rops_return_rop_specific_protocol_errors() {
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
        &[0x38, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x50, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_empty_folder_hard_deletes_deleted_items_contents() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let first_message_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let second_message_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
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
                &second_message_id.to_string(),
                &trash_id.to_string(),
                "trash",
                "Second trash message",
            ),
        ])),
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
    assert!(contains_bytes(&response_rops, &[0x58, 0x01, 0, 0, 0, 0, 0]));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[first_message_id, second_message_id]
    );
    assert!(canonical_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_hard_delete_messages_and_subfolders_hard_deletes_trash_contents() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let message_id = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
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
            "Trash purge message",
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x92, 0x00, 0x01, 0x00, 0x00]);

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
    assert!(contains_bytes(&response_rops, &[0x92, 0x01, 0, 0, 0, 0, 0]));
    assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
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
async fn mapi_over_http_empty_folder_reports_partial_when_retention_blocks_one_message() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let deletable_message_id = Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1").unwrap();
    let retained_message_id = Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &deletable_message_id.to_string(),
                &trash_id.to_string(),
                "trash",
                "Deletable trash message",
            ),
            FakeStore::email(
                &retained_message_id.to_string(),
                &trash_id.to_string(),
                "trash",
                "Retained trash message",
            ),
        ])),
        failed_delete_email_ids: Arc::new(Mutex::new(vec![retained_message_id])),
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

    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x58, 0x01, 0, 0, 0, 0, 1]));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[deletable_message_id]
    );
    let canonical = canonical_emails.lock().unwrap();
    assert!(canonical
        .iter()
        .all(|email| email.id != deletable_message_id));
    assert!(canonical
        .iter()
        .any(|email| email.id == retained_message_id));
}

#[tokio::test]
async fn mapi_over_http_empty_folder_reports_success_when_already_empty() {
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
    assert!(contains_bytes(&response_rops, &[0x58, 0x01, 0, 0, 0, 0, 0]));
    assert!(deleted_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_empty_folder_notifies_when_partial_purge_changes_contents() {
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
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0008u16.to_le_bytes());
    rops.push(0);
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&[0x58, 0x00, 0x01, 0x00, 0x00]);

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
    assert!(contains_bytes(&response_rops, &[0x58, 0x01, 0, 0, 0, 0, 1]));

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
}

#[tokio::test]
async fn mapi_over_http_replayed_empty_folder_request_id_does_not_repeat_delete() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let message_id = Uuid::parse_str("66666666-6666-4666-8666-666666666666").unwrap();
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
            "Replay trash message",
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
        "x-requestid",
        HeaderValue::from_static("{11111111-1111-1111-1111-111111111111}:77"),
    );
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x58, 0x00, 0x01, 0x00, 0x00]);
    let body = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));

    let first = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &body)
        .await
        .unwrap();
    let first_rops = response_rops_from_execute_response(first).await;
    let replay = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &body)
        .await
        .unwrap();
    let replay_rops = response_rops_from_execute_response(replay).await;

    assert_eq!(first_rops, replay_rops);
    assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
}

#[tokio::test]
async fn mapi_over_http_hard_delete_messages_and_subfolders_keeps_child_folder_contents() {
    let parent_id = Uuid::parse_str("77777777-7777-4777-8777-777777777770").unwrap();
    let child_id = Uuid::parse_str("77777777-7777-4777-8777-777777777771").unwrap();
    let parent_message_id = Uuid::parse_str("77777777-7777-4777-8777-777777777772").unwrap();
    let child_message_id = Uuid::parse_str("77777777-7777-4777-8777-777777777773").unwrap();
    let parent = FakeStore::mailbox(&parent_id.to_string(), "", "Parent");
    let mut child = FakeStore::mailbox(&child_id.to_string(), "", "Child");
    child.parent_id = Some(parent_id);
    let parent_folder_id = test_mapi_folder_id(0x1771);
    let child_folder_id = test_mapi_folder_id(0x1772);
    crate::mapi::identity::remember_mapi_identity(parent_id, parent_folder_id);
    crate::mapi::identity::remember_mapi_identity(child_id, child_folder_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![parent, child])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &parent_message_id.to_string(),
                &parent_id.to_string(),
                "",
                "Parent message",
            ),
            FakeStore::email(
                &child_message_id.to_string(),
                &child_id.to_string(),
                "",
                "Child message",
            ),
        ])),
        ..Default::default()
    };
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(parent_id, parent_folder_id);
    store
        .mapi_identities
        .lock()
        .unwrap()
        .insert(child_id, child_folder_id);
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
    append_mapi_wire_id(&mut rops, parent_folder_id);
    rops.push(0);
    rops.extend_from_slice(&[0x92, 0x00, 0x01, 0x00, 0x00]);

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
    assert!(contains_bytes(&response_rops, &[0x92, 0x01, 0, 0, 0, 0, 0]));
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[parent_message_id]
    );
    let canonical = canonical_emails.lock().unwrap();
    assert!(canonical.iter().all(|email| email.id != parent_message_id));
    assert!(canonical.iter().any(|email| email.id == child_message_id));
}

#[tokio::test]
async fn mapi_over_http_empty_folder_rejects_unsupported_and_permission_denied_targets() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: trash_id,
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: rights_from_grant(true, true, false, false),
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x58, 0x00, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&[0x02, 0x00, 0x00, 0x02]);
    append_mapi_wire_id(&mut rops, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x92, 0x00, 0x02, 0x00, 0x00]);
    for (slot, folder_id) in [
        (3, crate::mapi::identity::CONTACTS_FOLDER_ID),
        (4, crate::mapi::identity::TASKS_FOLDER_ID),
        (5, crate::mapi::identity::NOTES_FOLDER_ID),
        (6, crate::mapi::identity::JOURNAL_FOLDER_ID),
    ] {
        rops.extend_from_slice(&[0x02, 0x00, 0x00, slot]);
        append_mapi_wire_id(&mut rops, folder_id);
        rops.push(0);
        rops.extend_from_slice(&[0x58, 0x00, slot, 0x00, 0x00]);
    }

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
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

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x58, 0x01, 0x05, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x92, 0x02, 0x0F, 0x01, 0x04, 0x80]
    ));
    for handle in [3, 4, 5, 6] {
        assert!(contains_bytes(
            &response_rops,
            &[0x58, handle, 0x0F, 0x01, 0x04, 0x80]
        ));
    }
}

#[tokio::test]
async fn mapi_over_http_public_folder_replica_rops_return_rop_specific_protocol_errors() {
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

    let mut rops = vec![0x42, 0x00, 0x00];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x45, 0x00, 0x00]);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]);

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
        &[0x42, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x45, 0x00, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
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
async fn mapi_over_http_save_message_rejects_sync_metadata_only_import() {
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
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
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
async fn mapi_over_http_save_message_persists_foreign_trash_sync_upload() {
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
    rops.extend_from_slice(&[
        0x07, 0x00, 0x03, // RopGetPropertiesSpecific on pending message
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_CHANGE_KEY.to_le_bytes());

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
        &imported_source_key[16..22]
    ));
    assert!(contains_bytes(&response_rops, &[0x07, 0x03, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, trash_id);
    assert_eq!(recorded[0].subject, "Client trash upload");
    let saved_id = emails.lock().unwrap().last().unwrap().id;
    let saved_object_id = mapi_identities
        .lock()
        .unwrap()
        .get(&saved_id)
        .copied()
        .unwrap();
    assert_ne!(
        crate::mapi::identity::source_key_for_object_id(saved_object_id),
        imported_source_key
    );
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
    let emails = store.emails.clone();
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

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, trash_id);
    assert_eq!(recorded[0].subject, "Outlook trash upload");
    let saved_id = emails.lock().unwrap().last().unwrap().id;
    let saved_object_id = mapi_identities
        .lock()
        .unwrap()
        .get(&saved_id)
        .copied()
        .unwrap();
    assert_ne!(
        crate::mapi::identity::source_key_for_object_id(saved_object_id),
        imported_source_key
    );
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

    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 1]));
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
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x78, 0x00, 0x02, // RopSynchronizationImportMessageMove
    ]);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ARCHIVE_FOLDER_ID);
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
}

#[tokio::test]
async fn mapi_over_http_sync_import_hierarchy_change_rejects_system_folder_mutation() {
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
    append_mapi_utf16_property(&mut hierarchy_values, 0x3001_001F, "Inbox");

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
    assert!(contains_bytes(
        &response_rops,
        &[0x73, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(created_mailboxes.lock().unwrap().is_empty());
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
    rops.extend_from_slice(&14u16.to_le_bytes());
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
    assert_eq!(response_rops[get_props_offset + 6], 0);
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
        0x8004_0102
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

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_reminders_folder_open_uses_canonical_search_projection() {
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::REMINDERS_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x3601_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3613_001Fu32.to_le_bytes());

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
    assert_eq!(&response_rops[0..8], &[0x02, 0x01, 0, 0, 0, 0, 0, 0]);
    assert!(contains_bytes(&response_rops, &2u32.to_le_bytes()));
    assert!(contains_bytes(&response_rops, &utf16z("Outlook.Reminder")));
}

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_root_rem_online_entry_id_projects_reminders_folder() {
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
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x36D5_0102u32.to_le_bytes());

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
    let entry_id = crate::mapi::identity::long_term_id_from_object_id(
        crate::mapi::identity::REMINDERS_FOLDER_ID,
    )
    .unwrap()
    .to_vec();
    assert!(contains_bytes(&response_rops, &entry_id));
}

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_reminders_table_projects_canonical_mixed_rows() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("calendar", "calendar", "Calendar");
    let task_list_id = "22222222-2222-2222-2222-222222222222";
    let task_list = FakeStore::collection(task_list_id, "task", "Tasks");
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let event_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let task_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let mail_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let mut email = FakeStore::email(
        &mail_id.to_string(),
        &inbox.id.to_string(),
        "inbox",
        "Mail reminder",
    );
    email.reminder_set = true;
    email.reminder_at = Some("2026-05-21T12:00:00Z".to_string());
    email.mailbox_states[0].reminder_set = true;
    email.mailbox_states[0].reminder_at = email.reminder_at.clone();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar.clone()])),
        task_collections: Arc::new(Mutex::new(vec![task_list.clone()])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: calendar.id.clone(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar reminder".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        tasks: Arc::new(Mutex::new(vec![FakeStore::task(
            &task_id.to_string(),
            task_list_id,
            "Task reminder",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: account.account_id,
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_reminder_excluded_folder_roles(),
            is_builtin: true,
        }])),
        reminders: Arc::new(Mutex::new(vec![
            ClientReminder {
                source_type: "calendar".to_string(),
                source_id: event_id,
                occurrence_start_at: None,
                title: "Calendar reminder".to_string(),
                due_at: Some("2026-05-21T09:00:00Z".to_string()),
                reminder_at: "2026-05-21T08:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "task".to_string(),
                source_id: task_id,
                occurrence_start_at: None,
                title: "Task reminder".to_string(),
                due_at: Some("2026-05-21T12:00:00Z".to_string()),
                reminder_at: "2026-05-21T11:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "mail".to_string(),
                source_id: mail_id,
                occurrence_start_at: None,
                title: "Mail reminder".to_string(),
                due_at: Some("2026-05-21T12:00:00Z".to_string()),
                reminder_at: "2026-05-21T12:00:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::REMINDERS_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x37, 0x00, 0x02, // RopQueryColumnsAll
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x674A_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&10u16.to_le_bytes());
    append_rop_open_message(
        &mut rops,
        0,
        3,
        crate::mapi::identity::REMINDERS_FOLDER_ID,
        crate::mapi::identity::legacy_migration_object_id(&mail_id),
    );

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let contents_offset = 8;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        3
    );
    assert!(contains_bytes(
        &response_rops,
        &0x8503_000Bu32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x8502_0040u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x8560_0040u32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x0C1A_001Fu32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar reminder")));
    assert!(contains_bytes(&response_rops, &utf16z("Task reminder")));
    assert!(contains_bytes(&response_rops, &utf16z("Mail reminder")));
    let calendar_name = utf16z("Calendar reminder");
    let mail_name = utf16z("Mail reminder");
    let task_name = utf16z("Task reminder");
    let calendar_offset = response_rops
        .windows(calendar_name.len())
        .position(|window| window == calendar_name.as_slice())
        .unwrap();
    let mail_offset = response_rops
        .windows(mail_name.len())
        .position(|window| window == mail_name.as_slice())
        .unwrap();
    let task_offset = response_rops
        .windows(task_name.len())
        .position(|window| window == task_name.as_slice())
        .unwrap();
    assert!(calendar_offset < mail_offset);
    assert!(mail_offset < task_offset);
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Task")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Note")));
    assert!(contains_bytes(&response_rops, &[0x03, 0x03, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_execute_handles_mailbox_store_bootstrap_rops() {
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
        0x09, 0x00, 0x01, // RopGetPropertiesList
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x16, 0x00, 0x02, // RopGetStatus
        0x17, 0x00, 0x02, // RopQueryPosition
        0x81, 0x00, 0x02, // RopResetTable
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
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    let props_list_offset = 8;
    assert_eq!(response_rops[props_list_offset], 0x09);
    let folder_property_count = u16::from_le_bytes(
        response_rops[props_list_offset + 6..props_list_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(contains_bytes(response_rops, &0x6748_0014u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x3601_0003u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x0FF4_0003u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x3613_001Fu32.to_le_bytes()));

    let contents_offset = props_list_offset + 8 + folder_property_count * 4;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(response_rops[contents_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 2..contents_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        0
    );

    let status_offset = contents_offset + 10;
    assert_eq!(
        &response_rops[status_offset..status_offset + 7],
        &[0x16, 0x02, 0, 0, 0, 0, 0]
    );
    let position_offset = status_offset + 7;
    assert_eq!(response_rops[position_offset], 0x17);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[position_offset + 2..position_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    let reset_offset = position_offset + 14;
    assert_eq!(
        &response_rops[reset_offset..reset_offset + 6],
        &[0x81, 0x02, 0, 0, 0, 0]
    );
    let query_offset = reset_offset + 6;
    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_table_control_rops_require_table_handles() {
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
        0x12, 0x00, 0x01, 0x00, // RopSetColumns on the folder handle.
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x16, 0x00, 0x01, // RopGetStatus on the folder handle.
        0x17, 0x00, 0x01, // RopQueryPosition on the folder handle.
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows on the folder handle.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x81, 0x00, 0x01, // RopResetTable on the folder handle.
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns on the contents table handle.
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x16, 0x00, 0x02, // RopGetStatus on the contents table handle.
        0x17, 0x00, 0x02, // RopQueryPosition on the contents table handle.
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows on the contents table handle.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x81, 0x00, 0x02, // RopResetTable on the contents table handle.
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
        &[0x12, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x16, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x17, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x81, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x16, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x81, 0x02, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_execute_returns_receive_folder_and_store_state() {
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

    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Note\0");
    rops.extend_from_slice(&[
        0x68, 0x00, 0x00, // RopGetReceiveFolderTable
        0x7B, 0x00, 0x00, // RopGetStoreState
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
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

    assert_eq!(response_rops[0], 0x27);
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response_rops[6..14]).unwrap(),
        test_mapi_folder_id(5)
    );
    assert!(contains_bytes(response_rops, b"IPM.Note\0"));

    let table_offset = 23;
    assert_eq!(response_rops[table_offset], 0x68);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[table_offset + 6..table_offset + 10]
                .try_into()
                .unwrap()
        ),
        3
    );
    assert!(contains_bytes(response_rops, &utf16z("IPM")));
    assert!(contains_bytes(response_rops, &utf16z("IPM.Note")));
    assert!(contains_bytes(response_rops, &utf16z("IPM.Appointment")));

    let store_offset = response_rops.len() - 10;
    assert_eq!(response_rops[store_offset], 0x7B);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[store_offset + 6..store_offset + 10]
                .try_into()
                .unwrap()
        ),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_uses_message_class_matching() {
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

    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Note.Custom\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"IPM.Appointment\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"IPM.MY.Class\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"MY.Class\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b".Invalid\0");

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"IPM.Note\0"));
    let mut calendar_response = vec![0x27, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut calendar_response, test_mapi_folder_id(16));
    calendar_response.extend_from_slice(b"IPM.Appointment\0");
    assert!(contains_bytes(&response_rops, calendar_response.as_slice()));
    let mut ipm_response = vec![0x27, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut ipm_response, test_mapi_folder_id(5));
    ipm_response.extend_from_slice(b"IPM\0");
    assert!(contains_bytes(&response_rops, ipm_response.as_slice()));
    let mut unmatched_response = vec![0x27, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut unmatched_response, test_mapi_folder_id(5));
    unmatched_response.push(0);
    assert!(contains_bytes(
        &response_rops,
        unmatched_response.as_slice()
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x27, 0x00, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_empty_class_returns_empty_explicit_message_class() {
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

    let rops = vec![0x27, 0x00, 0x00, 0x00];
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(response_rops[0], 0x27);
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response_rops[6..14]).unwrap(),
        test_mapi_folder_id(5)
    );
    assert_eq!(&response_rops[14..], b"\0");
}

#[tokio::test]
async fn mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state() {
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

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn);
    rops.extend_from_slice(&[
        0x6D, 0x00, 0x01, // RopGetTransportFolder against the logon handle.
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let transport_offset = response_rops.len() - 14;
    assert_eq!(response_rops[transport_offset], 0x6D);
    assert_eq!(response_rops[transport_offset + 1], 0x01);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[transport_offset + 2..transport_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(
            &response_rops[transport_offset + 6..transport_offset + 14]
        )
        .unwrap(),
        test_mapi_folder_id(6)
    );
}

#[tokio::test]
async fn mapi_over_http_transport_spooler_rops_return_parseable_errors_without_corrupting_batch() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let saved_drafts = store.saved_drafts.clone();
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let message_id = test_mapi_message_id("87878787-8787-8787-8787-878787878787");
    let folder_id = test_mapi_folder_id(5);

    let mut rops = Vec::new();
    rops.extend_from_slice(&[0x34, 0x00, 0x00]); // RopAbortSubmit.
    append_mapi_wire_id(&mut rops, folder_id);
    append_mapi_wire_id(&mut rops, message_id);
    rops.extend_from_slice(&[0x47, 0x00, 0x00]); // RopSetSpooler.
    rops.extend_from_slice(&[0x48, 0x00, 0x00]); // RopSpoolerLockMessage.
    rops.extend_from_slice(&message_id.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x51, 0x00, 0x00]); // RopTransportNewMail.
    rops.extend_from_slice(&message_id.to_le_bytes());
    append_mapi_wire_id(&mut rops, folder_id);
    rops.extend_from_slice(b"IPM.Note\0");
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // RopGetStoreState proves the batch stayed aligned.

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
        &[0x34, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
    for rop_id in [0x47, 0x48, 0x51] {
        assert!(contains_bytes(
            &response_rops,
            &[rop_id, 0x00, 0x02, 0x01, 0x04, 0x80]
        ));
    }
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(submitted_messages.lock().unwrap().is_empty());
    assert!(saved_drafts.lock().unwrap().is_empty());
    assert!(imported_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_abort_submit_cancels_pre_handoff_submission() {
    for status in ["queued", "ready", "deferred", "cancelled"] {
        let (response_rops, cancelled) = abort_submit_response(status).await;
        assert_eq!(cancelled.len(), 1);
        assert!(
            contains_bytes(&response_rops, &[0x34, 0x00, 0, 0, 0, 0]),
            "{response_rops:02x?}; cancelled={cancelled:?}"
        );
    }
}

#[tokio::test]
async fn mapi_over_http_abort_submit_rejects_handed_off_and_terminal_submissions() {
    for status in ["handed_off", "relayed", "bounced", "failed"] {
        let (response_rops, cancelled) = abort_submit_response(status).await;
        assert_eq!(cancelled.len(), 1);
        assert!(
            contains_bytes(&response_rops, &[0x34, 0x00, 0x02, 0x01, 0x04, 0x80]),
            "{response_rops:02x?}; cancelled={cancelled:?}"
        );
    }
}

async fn abort_submit_response(status: &str) -> (Vec<u8>, Vec<Uuid>) {
    let sent_mailbox = FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent");
    let message_id = Uuid::parse_str("87878787-8787-8787-8787-878787878787").unwrap();
    let mapi_message_id = crate::mapi::identity::legacy_migration_object_id(&message_id);
    crate::mapi::identity::remember_mapi_identity(message_id, mapi_message_id);
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &sent_mailbox.id.to_string(),
        "sent",
        "Abort submit",
    );
    email.delivery_status = status.to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![sent_mailbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(message_id, mapi_message_id)]))),
        ..Default::default()
    };
    let cancelled_submissions = store.cancelled_submissions.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut rops = Vec::new();
    rops.extend_from_slice(&[0x34, 0x00, 0x00]);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(7));
    append_mapi_wire_id(&mut rops, mapi_message_id);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let cancelled = cancelled_submissions.lock().unwrap().clone();
    (response_rops, cancelled)
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
async fn mapi_over_http_malformed_rop_terminates_current_buffer() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // Truncated RopOpenFolder.
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1]).await;

    assert_eq!(response_rops, vec![0x00, 0x00, 0x02, 0x01, 0x04, 0x80]);
}

#[tokio::test]
async fn mapi_over_http_unknown_property_type_terminates_current_buffer() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_000Du32.to_le_bytes()); // Unknown property type.
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01, 0x01, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x05, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x12, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(&response_rops, &[0x15, 0x02]));
}

#[tokio::test]
async fn mapi_over_http_unknown_restriction_type_terminates_current_buffer() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]); // RopRestrict
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0xEE); // Unknown restriction type.
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01, 0x01, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x14, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(&response_rops, &[0x15, 0x02]));
}

#[tokio::test]
async fn mapi_over_http_set_get_search_criteria_updates_canonical_search_folder() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555501").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343499").unwrap();
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
            display_name: "Unread invoices".to_string(),
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
    restriction.extend_from_slice(&3u16.to_le_bytes());
    append_search_property_bool(&mut restriction, 0x0E69_000B, 0x04, false);
    append_search_property_bool(&mut restriction, 0x0E1B_000B, 0x04, true);
    append_search_content(&mut restriction, 0x0037_001F, "invoice");
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
    let stored = stored_search_folders.lock().unwrap();
    assert_eq!(
        stored[0].scope_json,
        serde_json::json!({
            "kind": "mapi_bounded",
            "scope": "folders",
            "recursive": true,
            "folderIds": [inbox_id.to_string()],
            "folderRoles": ["inbox"]
        })
    );
    assert_eq!(
        stored[0].restriction_json,
        serde_json::json!({
            "kind": "mapi_bounded",
            "all": [
                {"field": "unread", "equals": true},
                {"field": "hasAttachment", "equals": true},
                {"field": "subject", "contains": "invoice"}
            ]
        })
    );
    drop(stored);

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
    assert!(contains_bytes(&response_rops, &utf16z("invoice")));
}

#[tokio::test]
async fn mapi_over_http_set_search_criteria_rejects_unsupported_restriction() {
    let account = FakeStore::account();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343498").unwrap();
    let search_folder_mapi_id = test_mapi_uuid_id(&search_folder_id);
    crate::mapi::identity::remember_mapi_identity(search_folder_id, search_folder_mapi_id);
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: search_folder_id,
            account_id: account.account_id,
            role: "custom".to_string(),
            display_name: "Unsupported".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({}),
            restriction_json: serde_json::json!({}),
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

    let mut unsupported_or_restriction = vec![0x01];
    unsupported_or_restriction.extend_from_slice(&2u16.to_le_bytes());
    append_search_property_bool(&mut unsupported_or_restriction, 0x0E1B_000B, 0x04, true);
    append_search_property_bool(&mut unsupported_or_restriction, 0x0E69_000B, 0x04, false);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &unsupported_or_restriction, &[], 0);
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
        &[0x30, 0x01, 0x02, 0x01, 0x04, 0x80]
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

#[tokio::test]
async fn mapi_over_http_public_folder_logon_is_unsupported_and_terminal() {
    let mut rops = vec![0xFE, 0x00, 0x01, 0x00]; // Public-folder RopLogon.
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // Must not execute.

    let response_rops = execute_rops_response_rops(&rops, &[u32::MAX]).await;

    assert_eq!(response_rops, vec![0xFE, 0x01, 0x02, 0x01, 0x04, 0x80]);
}

#[tokio::test]
async fn mapi_over_http_set_receive_folder_returns_rop_specific_protocol_error() {
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

    let mut rops = vec![0x26, 0x00, 0x00];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.extend_from_slice(b"IPM.Note\0");

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
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

    assert_eq!(response_rops[0], 0x26);
    assert_eq!(response_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0x8004_0102
    );
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_maps_appointments_to_calendar() {
    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Appointment\0");

    let response_rops = execute_rops_response_rops(&rops, &[1]).await;

    assert_eq!(&response_rops[..6], &[0x27, 0x00, 0, 0, 0, 0]);
    assert_eq!(
        &response_rops[6..14],
        &mapi_wire_id_bytes(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    assert_eq!(&response_rops[14..], b"IPM.Appointment\0");
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_preserves_ipm_note_inbox_mapping() {
    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Note\0");

    let response_rops = execute_rops_response_rops(&rops, &[1]).await;

    assert_eq!(&response_rops[..6], &[0x27, 0x00, 0, 0, 0, 0]);
    assert_eq!(
        &response_rops[6..14],
        &mapi_wire_id_bytes(crate::mapi::identity::INBOX_FOLDER_ID)
    );
    assert_eq!(&response_rops[14..], b"IPM.Note\0");
}

#[tokio::test]
async fn mapi_over_http_execute_returns_empty_transport_options_data() {
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

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn);
    rops.extend_from_slice(&[
        0x49, 0x00, 0x01, // RopGetAddressTypes
        0x6F, 0x00, 0x01, // RopOptionsData
    ]);
    rops.extend_from_slice(b"SMTP\0");
    rops.push(0);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"EX\0SMTP\0"));
    assert_eq!(
        &response_rops[response_rops.len() - 11..],
        &[0x6F, 0x01, 0, 0, 0, 0, 1, 0, 0, 0, 0]
    );
}

#[tokio::test]
async fn mapi_over_http_bind_creates_nspi_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert!(response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("MapiContext="));

    let body = response_bytes(response).await;
    assert_eq!(body.len(), 28);
    assert_eq!(&body[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
    assert_ne!(&body[8..24], &[0; 16]);
    assert_eq!(body[15] & 0xf0, 0x40);
    assert_eq!(body[16] & 0xc0, 0x80);
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
async fn mapi_over_http_bind_reestablishes_nspi_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let first_cookie = bind
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rebind_headers = mapi_headers("Bind");
    rebind_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let rebind = service
        .handle_mapi(MapiEndpoint::Nspi, &rebind_headers, b"")
        .await
        .unwrap();

    assert_eq!(rebind.status(), StatusCode::OK);
    assert_eq!(rebind.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(rebind.headers().get("x-responsecode").unwrap(), "0");
    let reconnected_cookie = rebind
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

    let mut old_unbind_headers = mapi_headers("Unbind");
    old_unbind_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let old_unbind = service
        .handle_mapi(MapiEndpoint::Nspi, &old_unbind_headers, b"")
        .await
        .unwrap();
    assert_eq!(old_unbind.headers().get("x-responsecode").unwrap(), "0");

    let mut new_unbind_headers = mapi_headers("Unbind");
    new_unbind_headers.insert(
        "cookie",
        HeaderValue::from_str(&reconnected_cookie).unwrap(),
    );
    let new_unbind = service
        .handle_mapi(MapiEndpoint::Nspi, &new_unbind_headers, b"")
        .await
        .unwrap();
    assert_eq!(new_unbind.headers().get("x-responsecode").unwrap(), "0");
    assert!(new_unbind
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
}

#[tokio::test]
async fn mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();

    let mut rebind_headers = mapi_headers("Bind");
    rebind_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header_with_mismatched_sequence(&bind)).unwrap(),
    );
    let rebind = service
        .handle_mapi(MapiEndpoint::Nspi, &rebind_headers, b"")
        .await
        .unwrap();

    assert_eq!(rebind.status(), StatusCode::OK);
    assert_eq!(rebind.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(rebind.headers().get("x-responsecode").unwrap(), "0");
    let set_cookies = rebind
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
async fn mapi_over_http_nspi_operation_requires_bound_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("QueryRows"), &[0; 32])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "QueryRows"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "13");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI session cookie"));
}

#[tokio::test]
async fn mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let mut headers = mapi_headers("QueryRows");
    headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header_with_mismatched_sequence(&bind)).unwrap(),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 32])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "QueryRows"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "6");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI request sequence cookie"));
}

#[tokio::test]
async fn mapi_over_http_nspi_bootstrap_requests_handle_stale_cleanup_and_reject_stateful_cookies() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let stale_cookie = mapi_cookie_header(&bind);
    let mut unbind_headers = mapi_headers("Unbind");
    unbind_headers.insert("cookie", HeaderValue::from_str(&stale_cookie).unwrap());
    let unbind = service
        .handle_mapi(MapiEndpoint::Nspi, &unbind_headers, b"")
        .await
        .unwrap();
    assert_eq!(unbind.headers().get("x-responsecode").unwrap(), "0");

    let repeated_unbind = service
        .handle_mapi(MapiEndpoint::Nspi, &unbind_headers, b"")
        .await
        .unwrap();
    assert_eq!(
        repeated_unbind.headers().get("x-requesttype").unwrap(),
        "Unbind"
    );
    assert_eq!(
        repeated_unbind.headers().get("x-responsecode").unwrap(),
        "0"
    );

    let mut dn_to_mid_headers = mapi_headers("DNToMId");
    dn_to_mid_headers.insert("cookie", HeaderValue::from_str(&stale_cookie).unwrap());
    let dn_to_mid = service
        .handle_mapi(
            MapiEndpoint::Nspi,
            &dn_to_mid_headers,
            b"alice@example.test\0",
        )
        .await
        .unwrap();
    assert_eq!(dn_to_mid.status(), StatusCode::OK);
    assert_eq!(dn_to_mid.headers().get("x-requesttype").unwrap(), "DNToMId");
    assert_eq!(dn_to_mid.headers().get("x-responsecode").unwrap(), "0");

    for request_type in [
        "GetProps",
        "GetSpecialTable",
        "GetMatches",
        "ResolveNames",
        "GetMailboxUrl",
        "GetAddressBookUrl",
    ] {
        let mut headers = mapi_headers(request_type);
        headers.insert("cookie", HeaderValue::from_str(&stale_cookie).unwrap());
        let response = service
            .handle_mapi(MapiEndpoint::Nspi, &headers, b"alice@example.test\0")
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{request_type}");
        assert_eq!(
            response.headers().get("x-requesttype").unwrap(),
            request_type,
            "{request_type}"
        );
        assert_eq!(
            response.headers().get("x-responsecode").unwrap(),
            "10",
            "{request_type}"
        );
        let body = String::from_utf8(response_bytes(response).await).unwrap();
        assert!(
            body.contains("MAPI session context not found"),
            "{request_type}"
        );
    }

    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut headers = mapi_headers("GetProps");
    headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, b"alice@example.test\0")
        .await
        .unwrap();
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "10");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("MAPI authentication context changed"));
}

#[tokio::test]
async fn mapi_over_http_returns_nspi_and_mailbox_urls() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = nspi_bound_headers(&service, "GetAddressBookUrl").await;
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "GetAddressBookUrl"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(
        utf16z_string_bytes(&body[8..]),
        b"https://mail.example.test/mapi/nspi/".to_vec()
    );
    assert!(body.ends_with(&[0, 0, 0, 0]));

    headers.insert("x-requesttype", HeaderValue::from_static("GetMailboxUrl"));
    renew_mapi_request_id(&mut headers);
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, b"")
        .await
        .unwrap();
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "GetMailboxUrl"
    );
    let body = response_bytes(response).await;
    assert_eq!(
        utf16z_string_bytes(&body[8..]),
        b"https://mail.example.test/mapi/emsmdb/".to_vec()
    );
}

#[tokio::test]
async fn mapi_over_http_resolve_names_resolves_authenticated_mailbox() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 103])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "ResolveNames"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert_eq!(body[21], 1);
    assert_eq!(u32::from_le_bytes(body[22..26].try_into().unwrap()), 8);
    assert_eq!(u32::from_le_bytes(body[58..62].try_into().unwrap()), 1);
    assert_eq!(body[62], 0);
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("Alice")));
    assert!(contains_bytes(&body, &utf16z("EX")));
}

#[tokio::test]
async fn mapi_over_http_resolve_names_honors_requested_rca_columns() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("alice@example.test", &[0x3003_001F, 0x3001_001F]);
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert_eq!(body[21], 1);
    assert_eq!(u32::from_le_bytes(body[22..26].try_into().unwrap()), 2);
    assert_eq!(
        u32::from_le_bytes(body[26..30].try_into().unwrap()),
        0x3003_001F
    );
    assert_eq!(
        u32::from_le_bytes(body[30..34].try_into().unwrap()),
        0x3001_001F
    );
    assert_eq!(u32::from_le_bytes(body[34..38].try_into().unwrap()), 1);
    assert_eq!(body[38], 0);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));
    assert!(contains_bytes(&body, &utf16z("Alice")));
    assert!(!contains_bytes(&body, &utf16z("SMTP")));
    assert!(body.ends_with(&[0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_resolve_names_falls_back_to_authenticated_mailbox_for_rca() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        omit_principal_from_directory: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("alice@example.test", &[0x3003_001F, 0x3001_001F]);
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert_eq!(body[21], 1);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));
    assert!(contains_bytes(&body, &utf16z("Alice")));
}

#[tokio::test]
async fn mapi_over_http_resolve_names_resolves_canonical_contact() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "Bob Contact",
            "bob@example.test",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("bob@example.test", &[0x3003_001F, 0x3001_001F]);
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_contact_legacy_dn(
            "bob@example.test",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ))
    ));
    assert!(contains_bytes(&body, &utf16z("Bob Contact")));
}

#[tokio::test]
async fn mapi_over_http_nspi_bootstrap_sequence_sees_only_visible_contacts() {
    let mut visible_contact = FakeStore::contact(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "Bob Contact",
        "bob.contact@example.test",
    );
    visible_contact.collection_id = "shared".to_string();
    let mut visible_collection = FakeStore::collection("shared", "contacts", "Shared Contacts");
    visible_collection.owner_account_id =
        Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    visible_collection.rights.may_read = true;

    let mut hidden_contact = FakeStore::contact(
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
        "Carol Hidden",
        "carol.hidden@example.test",
    );
    hidden_contact.collection_id = "private".to_string();
    hidden_contact.owner_account_id =
        Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let mut hidden_collection = FakeStore::collection("private", "contacts", "Private Contacts");
    hidden_collection.owner_account_id = hidden_contact.owner_account_id;
    hidden_collection.rights.may_read = false;

    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![visible_collection, hidden_collection])),
        contacts: Arc::new(Mutex::new(vec![visible_contact, hidden_contact])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let visible_lookup = b"bob.contact@example.test\0";
    let hidden_lookup = b"carol.hidden@example.test\0";

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, visible_lookup)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &utf16z("bob.contact@example.test")));
    assert!(contains_bytes(&body, &utf16z("Bob Contact")));
    assert!(!contains_bytes(&body, &utf16z("carol.hidden@example.test")));

    let matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &matches_headers, visible_lookup)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[9], 1);
    let visible_mid = u32::from_le_bytes(body[14..18].try_into().unwrap());
    assert_ne!(visible_mid, 0);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_contact_legacy_dn(
            "bob.contact@example.test",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ))
    ));
    assert!(!contains_bytes(&body, &utf16z("carol.hidden@example.test")));

    let resolve_request =
        resolve_names_request("bob.contact@example.test", &[0x3003_001F, 0x3001_001F]);
    let resolve_headers = nspi_bound_headers(&service, "ResolveNames").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &resolve_headers, &resolve_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_contact_legacy_dn(
            "bob.contact@example.test",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ))
    ));
    assert!(!contains_bytes(&body, &utf16z("carol.hidden@example.test")));

    let dn_to_mid_headers = nspi_bound_headers(&service, "DNToMId").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &dn_to_mid_headers, visible_lookup)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(
        u32::from_le_bytes(body[13..17].try_into().unwrap()),
        visible_mid
    );

    let mut props_request = Vec::new();
    props_request.extend_from_slice(&visible_mid.to_le_bytes());
    props_request.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    props_request.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &props_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_contact_legacy_dn(
            "bob.contact@example.test",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ))
    ));
    assert!(contains_bytes(&body, &utf16z("Bob Contact")));
    assert!(!contains_bytes(&body, &utf16z("carol.hidden@example.test")));

    let special_headers = nspi_bound_headers(&service, "GetSpecialTable").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &special_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &utf16z("Global Address List")));
    assert!(!contains_bytes(&body, &utf16z("carol.hidden@example.test")));

    let hidden_matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &hidden_matches_headers, hidden_lookup)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[9], 0);
    assert!(!contains_bytes(&body, &utf16z("carol.hidden@example.test")));
}

#[tokio::test]
async fn mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions() {
    let contact_id = Uuid::parse_str("d0d0d0d0-d0d0-d0d0-d0d0-d0d0d0d0d0d0").unwrap();
    let contact = FakeStore::contact(
        "d0d0d0d0-d0d0-d0d0-d0d0-d0d0d0d0d0d0",
        "Cache Collision Contact",
        "cache.collision@example.test",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![contact])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let poisoned_object_id = crate::mapi::identity::mapi_store_id(22);
    let poisoned_mid = 0x4000_0016;
    crate::mapi::identity::remember_mapi_identity(contact_id, poisoned_object_id);

    let lookup = b"cache.collision@example.test\0";
    let matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &matches_headers, lookup)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[9], 1);
    let visible_mid = u32::from_le_bytes(body[14..18].try_into().unwrap());
    assert_ne!(visible_mid, poisoned_mid);

    let dn_to_mid_headers = nspi_bound_headers(&service, "DNToMId").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &dn_to_mid_headers, lookup)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(
        u32::from_le_bytes(body[13..17].try_into().unwrap()),
        visible_mid
    );
}

#[tokio::test]
async fn mapi_over_http_resolve_names_ranks_exact_contact_before_partial_account() {
    let mut partial = FakeStore::account();
    partial.account_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    partial.email = "bob.alias@example.test".to_string();
    partial.display_name = "Bob Example Alias".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![partial])),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "Bob Contact",
            "bob@example.test",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("bob@example.test", &[0x3003_001F, 0x3001_001F]);
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_contact_legacy_dn(
            "bob@example.test",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ))
    ));
    assert!(contains_bytes(&body, &utf16z("Bob Contact")));
    assert!(!contains_bytes(&body, &utf16z("bob.alias@example.test")));
}

#[tokio::test]
async fn mapi_over_http_nspi_get_matches_ranks_distribution_list_exact_smtp_first() {
    let mut display_name_account = FakeStore::account();
    display_name_account.account_id =
        Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    display_name_account.email = "sales.account@example.test".to_string();
    display_name_account.display_name = "sales@example.test".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![display_name_account])),
        group_aliases: Arc::new(Mutex::new(vec![(
            Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
            "Sales".to_string(),
            "sales@example.test".to_string(),
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request(
        "sales@example.test",
        &[0x3001_001F, 0x39FE_001F, 0x3900_0003],
    );
    let headers = nspi_bound_headers(&service, "GetMatches").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    let body = response_bytes(response).await;
    assert_eq!(body[9], 1);
    let group_name = utf16z("Sales");
    let account_name = utf16z("sales@example.test");
    let group_position = body
        .windows(group_name.len())
        .position(|window| window == group_name.as_slice())
        .expect("distribution list row");
    let account_position = body
        .windows(account_name.len())
        .position(|window| window == account_name.as_slice())
        .expect("account row");
    assert!(group_position < account_position);
}

#[tokio::test]
async fn mapi_over_http_nspi_distribution_list_members_are_bounded_to_canonical_rows() {
    let mut bob = FakeStore::account();
    bob.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    bob.email = "bob@example.test".to_string();
    bob.display_name = "Bob Member".to_string();

    let group_id = Uuid::from_bytes([0x34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        group_aliases: Arc::new(Mutex::new(vec![(
            group_id,
            "Sales".to_string(),
            "sales@example.test".to_string(),
        )])),
        group_alias_members: Arc::new(Mutex::new(HashMap::from([(
            group_id,
            vec![
                "bob@example.test".to_string(),
                "mallory@external.test".to_string(),
            ],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut request = Vec::new();
    request.extend_from_slice(&0x4000_0034u32.to_le_bytes());
    for tag in [0x8009_000Du32, 0x8CE2_0003, 0x8CE3_0003] {
        request.extend_from_slice(&tag.to_le_bytes());
    }
    let headers = nspi_bound_headers(&service, "GetProps").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &0x8009_000Du32.to_le_bytes()));
    assert!(contains_bytes(&body, &utf16z("Bob Member")));
    assert!(contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(contains_bytes(
        &body,
        &[
            0x8CE2_0003u32.to_le_bytes().as_slice(),
            0u32.to_le_bytes().as_slice(),
            1u32.to_le_bytes().as_slice(),
        ]
        .concat()
    ));
    assert!(contains_bytes(
        &body,
        &[
            0x8CE3_0003u32.to_le_bytes().as_slice(),
            0u32.to_le_bytes().as_slice(),
            0u32.to_le_bytes().as_slice(),
        ]
        .concat()
    ));
    assert!(!contains_bytes(&body, b"mallory@external.test"));
    assert!(!contains_bytes(&body, &utf16z("mallory@external.test")));
}

#[tokio::test]
async fn mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        omit_principal_from_directory: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &[0; 32])
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &matches_headers, &[0; 32])
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let request = resolve_names_request("alice@example.test", &[0x3003_001F, 0x3001_001F]);
    let resolve_headers = nspi_bound_headers(&service, "ResolveNames").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &resolve_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));

    let partial_request = resolve_names_request("alice", &[0x3003_001F, 0x3001_001F]);
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &resolve_headers, &partial_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
    assert_eq!(body[21], 0);
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let dn_to_mid_request = b"\0\0\0\0\xff\x01\0\0\0/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test\0\0\0\0\0";
    let dn_to_mid_headers = nspi_bound_headers(&service, "DNToMId").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &dn_to_mid_headers, dn_to_mid_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let self_mid = u32::from_le_bytes(body[13..17].try_into().unwrap());
    assert_ne!(self_mid, 0);

    let mut props_request = Vec::new();
    props_request.extend_from_slice(&self_mid.to_le_bytes());
    props_request.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    props_request.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &props_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert!(contains_bytes(
        &body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));
    assert!(contains_bytes(&body, &utf16z("Alice")));

    let outlook_stat_props_request = hex_bytes(
        "00000000ff000000000000000000000000000000000000000000000000b00400000904000009080000ff0100000002016d8c00000000",
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Nspi,
            &props_headers,
            &outlook_stat_props_request,
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 1);
    assert_eq!(
        u32::from_le_bytes(body[21..25].try_into().unwrap()),
        0x8C6D_0102
    );
    assert_eq!(u32::from_le_bytes(body[25..29].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[29..33].try_into().unwrap()), 16);
    assert_eq!(
        &body[33..49],
        FakeStore::account().account_id.to_bytes_le().as_slice()
    );
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let proxy_addresses_request = hex_bytes(
        "00000000ff000000000000000012000080000000000000000000000000b00400000904000009080000ff010000001f100f8000000000",
    );
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &proxy_addresses_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 1);
    assert_eq!(
        u32::from_le_bytes(body[21..25].try_into().unwrap()),
        0x800F_101F
    );
    assert_eq!(u32::from_le_bytes(body[25..29].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[29..33].try_into().unwrap()), 1);
    assert!(contains_bytes(&body, &utf16z("SMTP:alice@example.test")));
}

#[tokio::test]
async fn mapi_over_http_query_rows_stays_in_authenticated_tenant() {
    let mut same_tenant = FakeStore::account();
    same_tenant.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    same_tenant.email = "bob@example.test".to_string();
    same_tenant.display_name = "Bob".to_string();

    let mut other_tenant = FakeStore::account();
    other_tenant.tenant_id = Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);
    other_tenant.account_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    other_tenant.email = "mallory@other.test".to_string();
    other_tenant.display_name = "Mallory".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![same_tenant, other_tenant])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &[0; 32])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let request = resolve_names_request("mallory@other.test", &[0x3003_001F, 0x3001_001F]);
    let resolve_headers = nspi_bound_headers(&service, "ResolveNames").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &resolve_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
    assert_eq!(body[21], 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let matches_request = resolve_names_request("mallory@other.test", &[0x3003_001F, 0x3001_001F]);
    let matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &matches_headers, &matches_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[9], 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let dn_to_mid_headers = nspi_bound_headers(&service, "DNToMId").await;
    let response = service
        .handle_mapi(
            MapiEndpoint::Nspi,
            &dn_to_mid_headers,
            b"mallory@other.test\0",
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &matches_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));
}

#[tokio::test]
async fn mapi_over_http_nspi_query_rows_honors_requested_count() {
    let mut bob = FakeStore::account();
    bob.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    bob.email = "bob@example.test".to_string();
    bob.display_name = "Bob".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut request = Vec::new();
    request.extend_from_slice(&0u32.to_le_bytes());
    request.extend_from_slice(&[0; 36]);
    request.extend_from_slice(&0u32.to_le_bytes());
    request.extend_from_slice(&1u32.to_le_bytes());

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(body[8], 0);
    assert_eq!(body[9], 1);
    let tag_count = u32::from_le_bytes(body[10..14].try_into().unwrap()) as usize;
    let row_count_offset = 14 + tag_count * 4;
    assert_eq!(
        u32::from_le_bytes(
            body[row_count_offset..row_count_offset + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(!contains_bytes(&body, &utf16z("bob@example.test")));
}

#[tokio::test]
async fn mapi_over_http_nspi_requested_string8_columns_stay_tenant_scoped() {
    let mut same_tenant = FakeStore::account();
    same_tenant.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    same_tenant.email = "bob@example.test".to_string();
    same_tenant.display_name = "Bob".to_string();

    let mut other_tenant = FakeStore::account();
    other_tenant.tenant_id = Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);
    other_tenant.account_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    other_tenant.email = "mallory@other.test".to_string();
    other_tenant.display_name = "Mallory".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![same_tenant, other_tenant])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut request = Vec::new();
    for tag in [0x3003_001Eu32, 0x3001_001E, 0x3002_001E] {
        request.extend_from_slice(&tag.to_le_bytes());
    }

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &0x3003_001Eu32.to_le_bytes()));
    assert!(contains_bytes(
        &body,
        format!("{}\0", test_account_legacy_dn("alice@example.test")).as_bytes()
    ));
    assert!(contains_bytes(
        &body,
        format!("{}\0", test_account_legacy_dn("bob@example.test")).as_bytes()
    ));
    assert!(contains_bytes(&body, b"EX\0"));
    assert!(!contains_bytes(&body, b"SMTP\0"));
    assert!(!contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(!contains_bytes(&body, b"mallory@other.test"));

    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert!(contains_bytes(&body, &0x3001_001Eu32.to_le_bytes()));
    assert!(contains_bytes(
        &body,
        format!("{}\0", test_account_legacy_dn("alice@example.test")).as_bytes()
    ));
    assert!(contains_bytes(&body, b"Alice\0"));
    assert!(!contains_bytes(&body, &utf16z("Alice")));
    assert!(!contains_bytes(&body, b"mallory@other.test"));
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

#[tokio::test]
async fn mapi_over_http_nspi_minimal_ids_use_identity_mapping_not_uuid_prefix() {
    let mut first = FakeStore::account();
    first.account_id = Uuid::parse_str("11111111-1111-0000-0000-000000000001").unwrap();
    first.email = "first@example.test".to_string();
    first.display_name = "First".to_string();

    let mut second = FakeStore::account();
    second.account_id = Uuid::parse_str("11111111-1111-0000-0000-000000000002").unwrap();
    second.email = "second@example.test".to_string();
    second.display_name = "Second".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![first, second])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let first_request = b"first@example.test\0";
    let first_headers = nspi_bound_headers(&service, "GetMatches").await;
    let first_response = service
        .handle_mapi(MapiEndpoint::Nspi, &first_headers, first_request)
        .await
        .unwrap();
    let first_body = response_bytes(first_response).await;
    let first_id = u32::from_le_bytes(first_body[14..18].try_into().unwrap());

    let second_request = b"second@example.test\0";
    let second_headers = nspi_bound_headers(&service, "GetMatches").await;
    let second_response = service
        .handle_mapi(MapiEndpoint::Nspi, &second_headers, second_request)
        .await
        .unwrap();
    let second_body = response_bytes(second_response).await;
    let second_id = u32::from_le_bytes(second_body[14..18].try_into().unwrap());

    assert_ne!(first_id, second_id);
    assert_ne!(first_id, 0x9111_1111);
    assert_ne!(second_id, 0x9111_1111);
}

#[tokio::test]
async fn mapi_over_http_resolve_names_returns_no_match_for_unknown_name() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("nobody@example.test", &[0x3003_001F, 0x3001_001F]);
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
    assert_eq!(body[21], 0);
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(!contains_bytes(&body, &utf16z("nobody@example.test")));
}

#[tokio::test]
async fn mapi_over_http_nspi_bootstrap_requests_return_success() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for request_type in [
        "CompareMIds",
        "DNToMId",
        "GetMatches",
        "GetPropList",
        "GetProps",
        "GetSpecialTable",
        "GetTemplateInfo",
        "QueryColumns",
        "QueryRows",
        "ResortRestriction",
        "SeekEntries",
        "UpdateStat",
    ] {
        let headers = nspi_bound_headers(&service, request_type).await;
        let response = service
            .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 32])
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{request_type}");
        assert_eq!(
            response.headers().get("x-requesttype").unwrap(),
            request_type,
            "{request_type}"
        );
        assert_eq!(
            response.headers().get("x-responsecode").unwrap(),
            "0",
            "{request_type}"
        );
        let body = response_bytes(response).await;
        assert!(body.len() >= 12, "{request_type}");
        assert_eq!(
            u32::from_le_bytes(body[0..4].try_into().unwrap()),
            0,
            "{request_type}"
        );
        assert_eq!(
            u32::from_le_bytes(body[4..8].try_into().unwrap()),
            0,
            "{request_type}"
        );

        match request_type {
            "GetMatches" => {
                assert_eq!(body[8], 0, "{request_type}");
                assert_eq!(body[9], 1, "{request_type}");
                assert_eq!(
                    u32::from_le_bytes(body[10..14].try_into().unwrap()),
                    1,
                    "{request_type}"
                );
                assert_ne!(
                    u32::from_le_bytes(body[14..18].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
                assert_eq!(body[18], 1, "{request_type}");
                assert!(contains_bytes(&body, &0x3001_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &0x39FE_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &utf16z("alice@example.test")));
                assert!(contains_bytes(&body, &utf16z("Alice")));
            }
            "QueryRows" | "SeekEntries" => {
                assert!(contains_bytes(&body, &0x3001_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &0x39FE_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &utf16z("alice@example.test")));
                assert!(contains_bytes(&body, &utf16z("Alice")));
            }
            "GetProps" | "GetTemplateInfo" => {
                assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
                assert_eq!(body[12], 1, "{request_type}");
                assert!(contains_bytes(&body, &utf16z("alice@example.test")));
                assert!(contains_bytes(&body, &utf16z("Alice")));
            }
            "ResortRestriction" => {
                assert!(body.len() >= 19, "{request_type}");
                assert_eq!(body[8], 0, "{request_type}");
                assert_eq!(body[9], 1, "{request_type}");
                assert_eq!(
                    u32::from_le_bytes(body[10..14].try_into().unwrap()),
                    1,
                    "{request_type}"
                );
                assert_ne!(
                    u32::from_le_bytes(body[14..18].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
            }
            "GetPropList" | "QueryColumns" => {
                assert_eq!(body[8], 1, "{request_type}");
                assert!(contains_bytes(&body, &0x3001_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &0x39FE_001Fu32.to_le_bytes()));
            }
            "GetSpecialTable" => {
                assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
                assert!(contains_bytes(&body, &utf16z("Global Address List")));
                let mut offset = 22usize;
                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    6,
                    "{request_type}"
                );
                offset += 4;

                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0x0FFF_0102,
                    "{request_type}"
                );
                offset += 8;
                let entry_id_len =
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                assert!(entry_id_len > 28, "{request_type}");
                assert_eq!(
                    &body[offset + 24..offset + 28],
                    &0x0000_0100u32.to_le_bytes(),
                    "{request_type}"
                );
                assert!(
                    body[offset..offset + entry_id_len].ends_with(b"\0"),
                    "{request_type}"
                );
                offset += entry_id_len;

                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0x3600_0003,
                    "{request_type}"
                );
                offset += 8;
                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0x0000_0009,
                    "{request_type}"
                );
                offset += 4;

                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0x3005_0003,
                    "{request_type}"
                );
                offset += 8;
                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
                offset += 4;

                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0xFFFD_0003,
                    "{request_type}"
                );
                offset += 8;
                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
                offset += 4;

                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0x3001_001F,
                    "{request_type}"
                );
                offset += 8;
                assert_eq!(body[offset], 0xFF, "{request_type}");
                offset += 1 + utf16z("Global Address List").len();

                assert_eq!(
                    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                    0xFFFB_000B,
                    "{request_type}"
                );
                offset += 8;
                assert_eq!(body[offset], 0, "{request_type}");
            }
            "DNToMId" => {
                assert_eq!(body[8], 1, "{request_type}");
                assert_eq!(
                    u32::from_le_bytes(body[9..13].try_into().unwrap()),
                    1,
                    "{request_type}"
                );
                assert_ne!(
                    u32::from_le_bytes(body[13..17].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
                assert_eq!(
                    u32::from_le_bytes(body[17..21].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn mapi_over_http_nspi_mutation_requests_return_parseable_disabled_errors() {
    let contacts = Arc::new(Mutex::new(vec![FakeStore::contact(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "Bob Contact",
        "bob@example.test",
    )]));
    let deleted_contacts = Arc::new(Mutex::new(Vec::new()));
    let mapi_identities = Arc::new(Mutex::new(HashMap::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: contacts.clone(),
        deleted_contacts: deleted_contacts.clone(),
        mapi_identities: mapi_identities.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for request_type in ["ModLinkAtt", "ModProps"] {
        let headers = nspi_bound_headers(&service, request_type).await;
        let response = service
            .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 32])
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{request_type}");
        assert_eq!(
            response.headers().get("x-requesttype").unwrap(),
            request_type,
            "{request_type}"
        );
        assert_eq!(
            response.headers().get("x-responsecode").unwrap(),
            "16",
            "{request_type}"
        );
        let body = String::from_utf8(response_bytes(response).await).unwrap();
        assert!(body.contains("disabled"), "{request_type}: {body}");
        assert!(
            body.contains("canonical accounts, contacts, and group aliases"),
            "{request_type}: {body}"
        );
    }
    let stored_contacts = contacts.lock().unwrap();
    assert_eq!(stored_contacts.len(), 1);
    assert_eq!(stored_contacts[0].name, "Bob Contact");
    assert_eq!(stored_contacts[0].email, "bob@example.test");
    assert!(deleted_contacts.lock().unwrap().is_empty());
    assert!(mapi_identities.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_dn_to_mid_resolves_outlook_unprefixed_legacy_dn_to_principal() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = b"\0\0\0\0\xff\x01\0\0\0/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test\0\0\0\0\0";
    let headers = nspi_bound_headers(&service, "DNToMId").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(body[8], 1);
    assert_eq!(u32::from_le_bytes(body[9..13].try_into().unwrap()), 1);
    let matched_id = u32::from_le_bytes(body[13..17].try_into().unwrap());
    assert_ne!(matched_id, 0);
    assert_eq!(matched_id & 0x8000_0000, 0x8000_0000);
    assert_ne!(matched_id, 0xaaaa_aaaa);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
}

#[tokio::test]
async fn mapi_over_http_dn_to_mid_resolves_connect_display_name_legacy_dn_to_principal() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request =
        b"\0\0\0\0\xff\x01\0\0\0/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0\0\0\0\0";
    let headers = nspi_bound_headers(&service, "DNToMId").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let matched_id = u32::from_le_bytes(body[13..17].try_into().unwrap());
    assert_ne!(matched_id, 0);
    assert_eq!(matched_id & 0x8000_0000, 0x8000_0000);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
}

#[tokio::test]
async fn mapi_over_http_unbind_consumes_nspi_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let cookie = bind
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut unbind_headers = mapi_headers("Unbind");
    unbind_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &unbind_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Unbind");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert!(response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
}
