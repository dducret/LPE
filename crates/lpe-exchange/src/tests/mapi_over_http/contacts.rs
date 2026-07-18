use super::*;

#[tokio::test]
async fn mapi_over_http_contact_link_copy_to_uses_message_content_root() {
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
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CONTACTS_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC),
    );
    rops.extend_from_slice(&[0x4D, 0x00, 0x02, 0x03]);
    rops.push(0); // Level: include subobjects.
    rops.extend_from_slice(&0x0000_2000u32.to_le_bytes()); // BestBody.
    rops.push(0x09); // Unicode | ForceUnicode, as sent by Outlook.
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
    let chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(chunks.len(), 1, "{response_rops:02x?}");
    let transfer = &chunks[0].1;

    // [MS-OXCFXICS] 2.2.4.2 and 2.2.4.4: CopyTo on a Message object
    // produces messageContent, not a message wrapped in StartFAIMsg/EndMessage.
    assert!(
        transfer.starts_with(&PID_TAG_PARENT_SOURCE_KEY.to_le_bytes()),
        "unexpected messageContent root: {transfer:02x?}"
    );
    assert!(!contains_bytes(transfer, &0x4010_0003u32.to_le_bytes()));
    assert!(!contains_bytes(transfer, &0x400D_0003u32.to_le_bytes()));
    assert!(contains_bytes(
        transfer,
        &utf16z("IPM.Microsoft.ContactLink.TimeStamp")
    ));
}

#[tokio::test]
async fn mapi_over_http_outlook_contact_create_resolves_named_email_addresses() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let contacts = store.contacts.clone();
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

    let address_guid = [
        0x04, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let named_properties = [0x8083u32, 0x8093u32];
    let mut named_rops = vec![0x56, 0x00, 0x00, 0x02];
    named_rops.extend_from_slice(&(named_properties.len() as u16).to_le_bytes());
    for lid in named_properties {
        named_rops.push(0x00);
        named_rops.extend_from_slice(&address_guid);
        named_rops.extend_from_slice(&lid.to_le_bytes());
    }
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&named_rops, &[1])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(&response_rops[..6], &[0x56, 0x00, 0, 0, 0, 0]);
    let property_ids = response_rops[8..12]
        .chunks_exact(2)
        .map(|value| u16::from_le_bytes(value.try_into().unwrap()))
        .collect::<Vec<_>>();
    let tag = |index: usize| (u32::from(property_ids[index]) << 16) | 0x001F;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "Élodie Müller");
    append_mapi_utf16_property(&mut property_values, tag(0), "elodie@example.test");
    append_mapi_utf16_property(&mut property_values, tag(1), "e.mueller@example.test");
    append_mapi_utf16_property(&mut property_values, 0x3A16_001F, "Société Zürich");
    append_mapi_utf16_property(&mut property_values, 0x3A1C_001F, "+41 44 555 01 02");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(15));
    append_rop_set_properties(&mut rops, 1, 5, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    renew_mapi_request_id(&mut execute_headers);
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
        !response_rops
            .windows(4)
            .any(|window| window == 0x8004_0102u32.to_le_bytes())
            && !response_rops
                .windows(4)
                .any(|window| window == 0x8004_010Fu32.to_le_bytes()),
        "Outlook contact create returned an error: {response_rops:02x?}"
    );

    let stored = contacts.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].name, "Élodie Müller");
    assert_eq!(stored[0].email, "elodie@example.test");
    assert_eq!(stored[0].organization_name, "Société Zürich");
    assert!(stored[0]
        .emails_json
        .to_string()
        .contains("elodie@example.test"));
    assert!(stored[0]
        .emails_json
        .to_string()
        .contains("e.mueller@example.test"));
    let contact_id = stored[0].id;
    drop(stored);

    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, tag(0), "elodie.updated@example.test");
    append_mapi_utf16_property(&mut update_values, tag(1), "e.updated@example.test");
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
    let stored = contacts.lock().unwrap();
    assert_eq!(stored[0].email, "elodie.updated@example.test");
    assert!(stored[0]
        .emails_json
        .to_string()
        .contains("e.updated@example.test"));
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
async fn mapi_over_http_delete_contact_virtual_folder_is_noop_acknowledged() {
    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // deletion flags
    ]);
    append_mapi_wire_id(&mut rops, crate::mapi::identity::CONTACTS_FOLDER_ID);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(u32::from_le_bytes(delete[2..6].try_into().unwrap()), 0);
    assert_eq!(delete[6], 0);
}

#[tokio::test]
async fn mapi_over_http_contacts_sync_exports_associated_config_deletes() {
    let contact_id = Uuid::parse_str("fb129372-d6b6-4d69-99f7-977ab2a8094f").unwrap();
    let config_id = Uuid::parse_str("61616161-6161-4161-8161-616161616181").unwrap();
    let config_object_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 54,
    );
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
        .mapi_identities
        .lock()
        .unwrap()
        .insert(config_id, config_object_id);
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
        deleted_associated_config_ids: vec![crate::store::MapiAssociatedConfigChange {
            folder_id: crate::mapi::identity::CONTACTS_FOLDER_ID,
            config_id,
        }],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops_for_store(
        store,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        b"client-content-state",
    )
    .await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    let deleted_idset = stream.deleted_idset.as_deref().unwrap();
    let contact_object_id = crate::mapi::identity::legacy_migration_object_id(&contact_id);
    assert!(strict_replid_globset_contains_counter(
        deleted_idset,
        &globcnt_bytes(config_object_id >> 16)
    )
    .unwrap());
    assert!(strict_replid_globset_contains_counter(
        deleted_idset,
        &globcnt_bytes(contact_object_id >> 16)
    )
    .unwrap());
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
async fn mapi_over_http_contacts_search_content_sync_uses_search_folder_parent() {
    let account = FakeStore::account();
    let contacts = FakeStore::collection("default", "contacts", "Contacts");
    let contact = FakeStore::contact(
        "71717171-7171-7171-7171-717171717171",
        "Contact Search One",
        "one@example.test",
    );
    let store = FakeStore {
        session: Some(account.clone()),
        contact_collections: Arc::new(Mutex::new(vec![contacts])),
        contacts: Arc::new(Mutex::new(vec![contact])),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
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
        }])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 57,
        current_modseq: 43,
        ..Default::default()
    };

    let response_rops = content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
        &[],
    )
    .await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert_eq!(stream.message_changes[0].subject, "Contact Search One");
    assert!(!stream.message_changes[0].associated);
    assert!(stream.message_changes[0]
        .body_tags
        .contains(&PID_TAG_DISPLAY_NAME_W));
    assert!(stream.message_changes[0]
        .body_tags
        .contains(&PID_TAG_SUBJECT_W));
    assert!(stream.message_changes[0]
        .body_tags
        .contains(&0x001A_001Fu32));
    assert_eq!(
        stream.message_changes[0].parent_source_key,
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID)
    );

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(
                    crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
                )
                .unwrap()
                .id,
            ),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .expect("Contacts Search content checkpoint");
    assert_eq!(checkpoint.last_change_sequence, 57);
    assert_eq!(checkpoint.last_modseq, 43);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(|id| id.as_u64()),
        Some(crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID)
    );
}

#[tokio::test]
async fn mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags() {
    // Outlook trace 202607181515, request :34 creates this Contacts FAI and
    // sends SaveFlags=0x0e before reading the saved Message object. The 0x08
    // bit is not a [MS-OXCMSG] section 2.2.3.3.1 SaveFlags value and MUST be
    // ignored; ForceSave subsumes the compatible KeepOpenReadWrite behavior.
    let account = FakeStore::account();
    let associated_configs = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(account.clone()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        associated_configs: associated_configs.clone(),
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

    let dictionary = br#"<?xml version="1.0"?><UserConfiguration><Info version="Outlook.16"/><Data><e k="18-piImportedContactNickNames" v="3-True"/><e k="18-OLPrefsVersion" v="9-1"/></Data></UserConfiguration>"#;
    let mut class_value = Vec::new();
    append_mapi_utf16_property(
        &mut class_value,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Configuration.ContactPrefs",
    );
    let mut roaming_value = Vec::new();
    append_mapi_binary_property(&mut roaming_value, 0x7C07_0102, dictionary);
    let mut subject_values = Vec::new();
    append_mapi_utf16_property(
        &mut subject_values,
        PID_TAG_SUBJECT_W,
        "IPM.Configuration.ContactPrefs",
    );
    append_mapi_utf16_property(
        &mut subject_values,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "IPM.Configuration.ContactPrefs",
    );

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 1, &class_value);
    append_rop_set_properties(&mut rops, 1, 1, &roaming_value);
    append_rop_set_properties(&mut rops, 1, 2, &subject_values);
    append_rop_save_changes_message_with_flags(&mut rops, 1, 1, 0x0E);
    append_rop_get_properties_specific(&mut rops, 1, &[PID_TAG_MESSAGE_CLASS_W, 0x7C07_0102]);

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
    assert!(
        contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]),
        "Outlook ContactPrefs SaveChangesMessage 0x0e failed: {response_rops:02x?}"
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.ContactPrefs")
    ));
    assert!(contains_bytes(
        &response_rops,
        b"piImportedContactNickNames"
    ));

    let configs = associated_configs.lock().unwrap();
    assert_eq!(configs.len(), 1, "{response_rops:02x?}");
    assert_eq!(configs[0].account_id, account.account_id);
    assert_eq!(
        configs[0].folder_id,
        crate::mapi::identity::CONTACTS_FOLDER_ID
    );
    assert_eq!(configs[0].message_class, "IPM.Configuration.ContactPrefs");
    assert_eq!(
        configs[0].properties_json["0x7c070102"]["value"],
        dictionary
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
}

#[tokio::test]
async fn mapi_over_http_virtual_contact_link_config_accepts_outlook_marker_property() {
    let account = FakeStore::account();
    let associated_configs = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
        associated_configs: associated_configs.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let config_object_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFEC);
    let mut property_values = Vec::new();
    append_mapi_i32_property(&mut property_values, 0x800F_0003, 1);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CONTACTS_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
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
    assert!(
        contains_bytes(&response_rops, &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );

    let configs = associated_configs.lock().unwrap();
    assert_eq!(configs.len(), 1, "{response_rops:02x?}");
    assert_eq!(configs[0].account_id, account.account_id);
    assert_eq!(
        configs[0].folder_id,
        crate::mapi::identity::CONTACTS_FOLDER_ID
    );
    assert_eq!(
        configs[0].message_class,
        "IPM.Microsoft.ContactLink.TimeStamp"
    );
    assert_eq!(configs[0].properties_json["0x800f0003"]["type"], "i32");
    assert_eq!(configs[0].properties_json["0x800f0003"]["value"], 1);
}

#[tokio::test]
async fn mapi_over_http_builtin_contacts_search_get_search_criteria_uses_fixed_folder_id() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: Uuid::parse_str("34343434-3434-4434-8434-343434343450").unwrap(),
            account_id: account.account_id,
            role: "contacts_search".to_string(),
            display_name: "Contacts Search".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "contact".to_string(),
            scope_json: serde_json::json!({"scope": "contacts_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_contacts_search"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: true,
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
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
    );
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

    assert!(contains_bytes(&response_rops, &[0x31, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::identity::wire_id_bytes_from_object_id(
            crate::mapi::identity::CONTACTS_FOLDER_ID
        )
        .unwrap()
    ));
}
