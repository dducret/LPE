use super::*;

#[tokio::test]
async fn mapi_over_http_outlook_contacts_open_resolves_standard_named_properties_without_create() {
    // Outlook trace 202607190710, request :152 opens the Contacts contents table
    // and resolves this exact 38-property set with CreateFlag=0. [MS-OXCPRPT]
    // sections 3.2.5.9 and 3.2.5.10 require stable mailbox mappings in both
    // directions; NoCreate must not allocate protocol state.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let psetid_address = [
        0x04, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let psetid_common = [
        0x08, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let address_lids = [
        0x80C3u32, 0x80D3, 0x80B3, 0x8080, 0x8090, 0x80A0, 0x80C2, 0x80D2, 0x80B2, 0x8084, 0x8094,
        0x80A4, 0x80C4, 0x80D4, 0x80B4, 0x8085, 0x8095, 0x80A5, 0x80C5, 0x80D5, 0x80B5, 0x8086,
        0x8096, 0x80A6, 0x80C6, 0x80D6, 0x80B6, 0x8005, 0x8029, 0x8028, 0x802C, 0x802D, 0x802E,
        0x8055, 0x8054, 0x804C, 0x8064,
    ];

    let existing_email1_display_name = MapiNamedProperty {
        guid: psetid_address,
        kind: MapiNamedPropertyKind::Lid(0x8080),
    };
    let existing_email1_display_name_id = crate::mapi::properties::DYNAMIC_NAMED_PROPERTY_ID_START;
    {
        let mut mappings = store.mapi_named_properties.lock().unwrap();
        mappings.by_property.insert(
            (account.account_id, existing_email1_display_name.clone()),
            existing_email1_display_name_id,
        );
        mappings.by_id.insert(
            (account.account_id, existing_email1_display_name_id),
            existing_email1_display_name,
        );
    }

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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CONTACTS_FOLDER_ID);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable.
    rops.extend_from_slice(&[0x56, 0x00, 0x01, 0x00]); // NoCreate on Contacts.
    rops.extend_from_slice(&38u16.to_le_bytes());
    for lid in address_lids {
        rops.push(0x00);
        rops.extend_from_slice(&psetid_address);
        rops.extend_from_slice(&lid.to_le_bytes());
    }
    rops.push(0x00);
    rops.extend_from_slice(&psetid_common);
    rops.extend_from_slice(&0x8552u32.to_le_bytes());

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

    assert_eq!(&response_rops[..8], &[0x02, 0x01, 0, 0, 0, 0, 0, 0]);
    assert_eq!(&response_rops[8..18], &[0x05, 0x02, 0, 0, 0, 0, 0, 0, 0, 0]);
    let named = &response_rops[18..];
    assert_eq!(&named[..6], &[0x56, 0x01, 0, 0, 0, 0], "{named:02x?}");
    assert_eq!(u16::from_le_bytes(named[6..8].try_into().unwrap()), 38);
    let property_ids = named[8..]
        .chunks_exact(2)
        .map(|bytes| u16::from_le_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(property_ids.len(), 38);
    assert!(property_ids.iter().all(|property_id| *property_id != 0));
    assert_eq!(property_ids[3], existing_email1_display_name_id);
    let mut unique_ids = property_ids.clone();
    unique_ids.sort_unstable();
    unique_ids.dedup();
    assert_eq!(unique_ids.len(), property_ids.len());
    assert_eq!(
        store
            .fetch_mapi_named_properties(account.account_id, None)
            .await
            .unwrap()
            .len(),
        1,
        "NoCreate must not persist additional named-property mappings"
    );
}

#[tokio::test]
async fn mapi_over_http_contact_link_copy_to_uses_message_content_root() {
    let associated_configs = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
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

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        2,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
    );
    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.ContactLink.TimeStamp",
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "IPM.Microsoft.ContactLink.TimeStamp",
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_save_changes_message(&mut rops, 2, 2);
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
    assert_eq!(associated_configs.lock().unwrap().len(), 1);
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
    let contact_mapi_id = saved_message_id_from_response(&response_rops, 1)
        .expect("RopSaveChangesMessage returned the created Contact MID");

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
        contact_mapi_id,
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
async fn mapi_over_http_replays_outlook_contact_sync_import_then_save() {
    // Outlook trace 202607201648, requests :256 and :259.
    // [MS-OXCFXICS] sections 2.2.3.2.4.2.1, 3.3.4.3.3.2.2.1, and
    // 3.3.5.8.7 require SourceKey/LastModificationTime/ChangeKey/PCL followed
    // by SetProperties and SaveChangesMessage. [MS-OXCMSG] sections 2.2.3.3
    // and 3.2.5.3 require the successful Save to return the imported MID.
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        next_mapi_global_counter: Arc::new(Mutex::new(0x0000_0003_0b19)),
        ..Default::default()
    };
    let imported_global_counter = store
        .reserve_mapi_local_replica_ids(account.account_id, 1)
        .await
        .unwrap();
    let imported_message_id = crate::mapi::identity::mapi_store_id(imported_global_counter);
    let imported_source_key = crate::mapi::identity::source_key_for_object_id(imported_message_id);
    let change_key = vec![
        0xc7, 0x66, 0xe6, 0xaf, 0x10, 0x7e, 0x2e, 0x4b, 0xa1, 0x95, 0x4a, 0x22, 0xd0, 0xe3, 0x13,
        0xff, 0x00, 0x00, 0x04, 0x5f,
    ];
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);
    let imported_last_modification_time = test_filetime("2026-07-20", "14:46");
    let contacts = store.contacts.clone();
    let emails = store.emails.clone();
    let mapi_identities = store.mapi_identities.clone();
    let identity_source_keys = store.mapi_identity_source_keys.clone();
    let identity_change_numbers = store.mapi_identity_change_numbers.clone();
    let identity_change_keys = store.mapi_identity_change_keys.clone();
    let identity_predecessor_change_lists = store.mapi_identity_predecessor_change_lists.clone();
    let identity_last_modification_times = store.mapi_identity_last_modification_times.clone();
    let restart_store = store.clone();
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
        crate::mapi::identity::CONTACTS_FOLDER_ID,
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
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7e, 0x02, 0, 0, 0, 0]
    ));

    let mut identity_values = Vec::new();
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_SOURCE_KEY,
        &imported_source_key,
    );
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time,
    );
    append_mapi_binary_property(&mut identity_values, PID_TAG_CHANGE_KEY, &change_key);
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &predecessor_change_list,
    );
    let mut import_rops = vec![0x72, 0x00, 0x00, 0x01, 0x00];
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
    assert!(
        contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]),
        "RopSynchronizationImportMessageChange failed: {import_response:02x?}"
    );
    assert!(contacts.lock().unwrap().is_empty());

    let mut contact_values = Vec::new();
    append_mapi_utf16_property(&mut contact_values, PID_TAG_MESSAGE_CLASS_W, "IPM.Contact");
    append_mapi_utf16_property(
        &mut contact_values,
        PID_TAG_DISPLAY_NAME_W,
        "René Maguaretaz",
    );
    append_mapi_utf16_property(
        &mut contact_values,
        0x39FE_001F,
        "rene.maguaretaz@example.test",
    );
    append_mapi_utf16_property(&mut contact_values, 0x3A16_001F, "Maison");
    append_mapi_utf16_property(&mut contact_values, 0x3A08_001F, "+41 22 555 01 02");
    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 1, 5, &contact_values);
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
    let save_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&save_response, &[0x0a, 0x01, 0, 0, 0, 0]));
    let mut expected_save = vec![0x0c, 0x01, 0, 0, 0, 0, 0x01];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(imported_message_id));
    assert!(
        contains_bytes(&save_response, &expected_save),
        "RopSaveChangesMessage must commit the imported Contact MID; expected {expected_save:02x?}, got {save_response:02x?}"
    );

    let stored_contacts = contacts.lock().unwrap();
    assert_eq!(stored_contacts.len(), 1);
    assert_eq!(stored_contacts[0].collection_id, "default");
    assert_eq!(stored_contacts[0].name, "René Maguaretaz");
    assert_eq!(stored_contacts[0].email, "rene.maguaretaz@example.test");
    assert_eq!(stored_contacts[0].organization_name, "Maison");
    assert_eq!(stored_contacts[0].phone, "+41 22 555 01 02");
    let contact_id = stored_contacts[0].id;
    drop(stored_contacts);
    assert!(emails.lock().unwrap().is_empty());
    assert_eq!(
        mapi_identities.lock().unwrap()[&contact_id],
        imported_message_id
    );
    assert_eq!(
        identity_source_keys.lock().unwrap()[&contact_id],
        imported_source_key
    );
    assert_eq!(
        identity_change_keys.lock().unwrap()[&contact_id],
        change_key
    );
    assert_eq!(
        identity_predecessor_change_lists.lock().unwrap()[&contact_id],
        predecessor_change_list
    );
    assert_eq!(
        identity_last_modification_times.lock().unwrap()[&contact_id],
        imported_last_modification_time as u64
    );
    let server_change_number = identity_change_numbers.lock().unwrap()[&contact_id];
    assert_ne!(server_change_number, imported_global_counter);

    // Outlook can retry the same ImportMessageChange + Save after losing the
    // acknowledgement. [MS-OXCFXICS] sections 3.1.5.6.1 and 3.2.5.9.4.2
    // require the server PCL to make that replay a successful no-op.
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
    let (retry_import_response, retry_import_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(
        contains_bytes(&retry_import_response, &[0x72, 0x01, 0, 0, 0, 0]),
        "retried RopSynchronizationImportMessageChange failed: {retry_import_response:02x?}"
    );

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &retry_import_handles)),
        )
        .await
        .unwrap();
    let retry_save_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &retry_save_response,
        &[0x0a, 0x01, 0, 0, 0, 0]
    ));
    assert!(
        contains_bytes(&retry_save_response, &expected_save),
        "retried RopSaveChangesMessage must acknowledge the existing Contact MID; expected {expected_save:02x?}, got {retry_save_response:02x?}"
    );
    assert_eq!(contacts.lock().unwrap().len(), 1);
    assert_eq!(
        identity_change_numbers.lock().unwrap()[&contact_id],
        server_change_number
    );

    // A later cached-mode edit keeps the same MID/SourceKey but uploads a
    // successor CK/PCL and changed content. The server must update the one
    // canonical Contact and issue a distinct server CN.
    let successor_change_key = vec![
        0xc7, 0x66, 0xe6, 0xaf, 0x10, 0x7e, 0x2e, 0x4b, 0xa1, 0x95, 0x4a, 0x22, 0xd0, 0xe3, 0x13,
        0xff, 0x00, 0x00, 0x04, 0x60,
    ];
    let mut successor_predecessor_change_list = vec![successor_change_key.len() as u8];
    successor_predecessor_change_list.extend_from_slice(&successor_change_key);
    let successor_last_modification_time = test_filetime("2026-07-20", "14:48");
    let mut successor_identity_values = Vec::new();
    append_mapi_binary_property(
        &mut successor_identity_values,
        PID_TAG_SOURCE_KEY,
        &imported_source_key,
    );
    append_mapi_i64_property(
        &mut successor_identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        successor_last_modification_time,
    );
    append_mapi_binary_property(
        &mut successor_identity_values,
        PID_TAG_CHANGE_KEY,
        &successor_change_key,
    );
    append_mapi_binary_property(
        &mut successor_identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &successor_predecessor_change_list,
    );
    let mut successor_import_rops = vec![0x72, 0x00, 0x00, 0x01, 0x00];
    successor_import_rops.extend_from_slice(&4u16.to_le_bytes());
    successor_import_rops.extend_from_slice(&successor_identity_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &successor_import_rops,
                &[collector_handles[2], u32::MAX],
            )),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (successor_import_response, successor_import_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &successor_import_response,
        &[0x72, 0x01, 0, 0, 0, 0]
    ));

    let mut successor_contact_values = Vec::new();
    append_mapi_utf16_property(
        &mut successor_contact_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Contact",
    );
    append_mapi_utf16_property(
        &mut successor_contact_values,
        PID_TAG_DISPLAY_NAME_W,
        "René Maguaretaz modifié",
    );
    append_mapi_utf16_property(
        &mut successor_contact_values,
        0x39FE_001F,
        "rene.updated@example.test",
    );
    append_mapi_utf16_property(&mut successor_contact_values, 0x3A16_001F, "LPE");
    append_mapi_utf16_property(
        &mut successor_contact_values,
        0x3A08_001F,
        "+41 22 555 01 03",
    );
    let mut successor_save_rops = Vec::new();
    append_rop_set_properties(&mut successor_save_rops, 1, 5, &successor_contact_values);
    append_rop_save_changes_message_with_flags(&mut successor_save_rops, 1, 1, 0x08);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&successor_save_rops, &successor_import_handles)),
        )
        .await
        .unwrap();
    let successor_save_response = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&successor_save_response, &expected_save),
        "successor RopSaveChangesMessage must retain the Contact MID: {successor_save_response:02x?}"
    );
    let stored_contacts = contacts.lock().unwrap();
    assert_eq!(stored_contacts.len(), 1);
    assert_eq!(stored_contacts[0].id, contact_id);
    assert_eq!(stored_contacts[0].name, "René Maguaretaz modifié");
    assert_eq!(stored_contacts[0].email, "rene.updated@example.test");
    drop(stored_contacts);
    let successor_server_change_number = identity_change_numbers.lock().unwrap()[&contact_id];
    assert_ne!(successor_server_change_number, server_change_number);
    assert_eq!(
        identity_change_keys.lock().unwrap()[&contact_id],
        successor_change_key
    );
    assert_eq!(
        identity_predecessor_change_lists.lock().unwrap()[&contact_id],
        successor_predecessor_change_list
    );

    // Rebuild the MAPI snapshot as a fresh Outlook connection would. The
    // imported identity must survive outside the session handle and remain
    // the identity advertised by the next content synchronization.
    let response_rops = content_sync_response_rops_for_store(
        restart_store,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        &[],
    )
    .await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1, "{response_rops:02x?}");
    let synchronized = &stream.message_changes[0];
    assert_eq!(synchronized.mid, Some(imported_message_id));
    assert_eq!(synchronized.source_key, imported_source_key);
    assert_eq!(synchronized.change_key, successor_change_key);
    assert_eq!(
        synchronized.predecessor_change_list,
        successor_predecessor_change_list
    );
    assert_eq!(
        synchronized.change_number,
        Some(successor_server_change_number)
    );
    assert_eq!(
        synchronized.last_modification_time,
        Some(successor_last_modification_time as u64)
    );
}

#[tokio::test]
async fn mapi_over_http_contact_sync_import_save_reports_deleted_source_key() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        next_mapi_global_counter: Arc::new(Mutex::new(0x0000_0003_0b19)),
        ..Default::default()
    };
    let imported_global_counter = store
        .reserve_mapi_local_replica_ids(account.account_id, 1)
        .await
        .unwrap();
    store
        .add_mapi_local_replica_deleted_ranges(
            account.account_id,
            crate::mapi::identity::CONTACTS_FOLDER_ID,
            &[crate::store::MapiLocalReplicaDeletedRange {
                replica_guid: Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID),
                min_global_counter: imported_global_counter,
                max_global_counter: imported_global_counter,
            }],
        )
        .await
        .unwrap();
    let imported_message_id = crate::mapi::identity::mapi_store_id(imported_global_counter);
    let imported_source_key = crate::mapi::identity::source_key_for_object_id(imported_message_id);
    let change_key = vec![
        0xc7, 0x66, 0xe6, 0xaf, 0x10, 0x7e, 0x2e, 0x4b, 0xa1, 0x95, 0x4a, 0x22, 0xd0, 0xe3, 0x13,
        0xff, 0x00, 0x00, 0x04, 0x60,
    ];
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);

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

    let mut collector_rops = Vec::new();
    append_rop_open_folder(
        &mut collector_rops,
        0,
        1,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
    );
    collector_rops.extend_from_slice(&[0x7e, 0x00, 0x01, 0x02, 0x01]);
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

    let mut identity_values = Vec::new();
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_SOURCE_KEY,
        &imported_source_key,
    );
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-20", "14:47"),
    );
    append_mapi_binary_property(&mut identity_values, PID_TAG_CHANGE_KEY, &change_key);
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &predecessor_change_list,
    );
    let mut import_rops = vec![0x72, 0x00, 0x00, 0x01, 0x00];
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

    let mut contact_values = Vec::new();
    append_mapi_utf16_property(&mut contact_values, PID_TAG_MESSAGE_CLASS_W, "IPM.Contact");
    append_mapi_utf16_property(
        &mut contact_values,
        PID_TAG_DISPLAY_NAME_W,
        "Contact supprimé",
    );
    append_mapi_utf16_property(&mut contact_values, 0x39FE_001F, "deleted@example.test");
    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 1, 3, &contact_values);
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
    let save_response = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &save_response,
        &[0x0C, 0x01, 0x0A, 0x01, 0x04, 0x80]
    ), "[MS-OXCFXICS] section 3.3.4.3.3.2.2.1 and [MS-OXCDATA] section 2.4 require ecObjectDeleted on RopSaveChangesMessage: {save_response:02x?}");
    assert!(contacts.lock().unwrap().is_empty());
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
    let response_rops = response_rops_from_execute_response(response).await;
    let contact_mapi_id = saved_message_id_from_response(&response_rops, 1)
        .expect("RopSaveChangesMessage returned the created Contact MID");
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
        contact_mapi_id,
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
        contact_mapi_id,
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
    append_rop_delete_messages(&mut delete_rops, 1, &[contact_mapi_id]);
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
async fn mapi_over_http_created_contact_link_config_accepts_outlook_marker_property() {
    let account = FakeStore::account();
    let associated_configs = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555501",
            "inbox",
            "Inbox",
        )])),
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
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.ContactLink.TimeStamp",
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "IPM.Microsoft.ContactLink.TimeStamp",
    );
    append_mapi_i32_property(&mut property_values, 0x800F_0003, 1);

    let mut rops = Vec::new();
    append_rop_create_associated_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
    );
    append_rop_set_properties(&mut rops, 1, 3, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);

    let mut headers = mapi_headers("Execute");
    headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x06, 0x01, 0, 0, 0, 0])
            && contains_bytes(&response_rops, &[0x0A, 0x01, 0, 0, 0, 0, 0, 0])
            && contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]),
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
