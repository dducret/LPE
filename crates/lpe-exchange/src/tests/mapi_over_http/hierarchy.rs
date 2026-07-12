use super::*;

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
    assert!(contains_bytes(
        &response_rops,
        &0x36E5_001Fu32.to_le_bytes()
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
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
async fn mapi_over_http_empty_store_root_and_ipm_subtree_report_virtual_children() {
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
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    append_rop_get_properties_specific(&mut rops, 1, &[0x360A_000B, 0x0FFF_0102, 0x0FF6_0102]);
    append_rop_open_folder(
        &mut rops,
        0,
        2,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_get_properties_specific(&mut rops, 2, &[0x360A_000B, 0x0FFF_0102, 0x0FF6_0102]);

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
        &[0x07, 0x01, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x07, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    for folder_id in [
        crate::mapi::identity::ROOT_FOLDER_ID,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    ] {
        let entry_id =
            crate::mapi::identity::folder_entry_id_from_object_id(account.account_id, folder_id)
                .unwrap();
        assert!(contains_bytes(&response_rops, &entry_id));
        assert!(contains_bytes(
            &response_rops,
            &crate::mapi::identity::instance_key_for_object_id(folder_id)
        ));
    }
}

#[tokio::test]
async fn mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_display_name() {
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
    let restriction = mapi_content_restriction(0x3001_001F, "Top of Information Store");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x360A_000Bu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
    let ipm_subtree_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    )
    .unwrap();
    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Top of Information Store")
    ));
    assert!(contains_bytes(&response_rops, &ipm_subtree_entry_id));
}

#[tokio::test]
async fn mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_entry_id() {
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
    let ipm_subtree_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    )
    .unwrap();
    let mut restriction = Vec::new();
    append_search_property_binary(&mut restriction, 0x0FFF_0102, 0x04, &ipm_subtree_entry_id);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x360A_000Bu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Top of Information Store")
    ));
    assert!(contains_bytes(&response_rops, &ipm_subtree_entry_id));
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
    let create = &response_rops[8..];

    assert_eq!(create[0], 0x1C);
    assert_eq!(create[1], 0x02);
    assert_eq!(u32::from_le_bytes(create[2..6].try_into().unwrap()), 0);
    assert_eq!(
        u64::from_le_bytes(create[6..14].try_into().unwrap()),
        test_mapi_uuid_id(&Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap())
    );
    assert_eq!(create[14], 0);
    assert_eq!(create.len(), 15);

    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].account_id, FakeStore::account().account_id);
    assert_eq!(created[0].name, "MAPI Projects");
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcfold_folder_examples_use_canonical_mailboxes() {
    let source_id = Uuid::parse_str("31313131-3131-4131-8131-313131313132").unwrap();
    let target_parent_id = Uuid::parse_str("32323232-3232-4232-8232-323232323233").unwrap();
    let delete_id = Uuid::parse_str("66666666-6666-4666-8666-666666666662").unwrap();
    let source_mapi_id = test_mapi_uuid_id(&source_id);
    let target_parent_mapi_id = test_mapi_uuid_id(&target_parent_id);
    let delete_mapi_id = test_mapi_uuid_id(&delete_id);
    crate::mapi::identity::remember_mapi_identity(source_id, source_mapi_id);
    crate::mapi::identity::remember_mapi_identity(target_parent_id, target_parent_mapi_id);
    crate::mapi::identity::remember_mapi_identity(delete_id, delete_mapi_id);
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let destroyed_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let updated_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&source_id.to_string(), "custom", "Projects"),
            FakeStore::mailbox(&target_parent_id.to_string(), "custom", "Clients"),
            FakeStore::mailbox(&delete_id.to_string(), "custom", "Remove Me"),
        ])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([
            (source_id, source_mapi_id),
            (target_parent_id, target_parent_mapi_id),
            (delete_id, delete_mapi_id),
        ]))),
        created_mailboxes: created_mailboxes.clone(),
        destroyed_mailboxes: destroyed_mailboxes.clone(),
        updated_mailboxes: updated_mailboxes.clone(),
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
    append_rop_open_folder(&mut rops, 0, 2, target_parent_mapi_id);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x03, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x00, // fail if the folder already exists
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Folder1"));
    rops.extend_from_slice(&utf16z(""));
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x05, // DEL_MESSAGES | DEL_FOLDERS
    ]);
    append_mapi_wire_id(&mut rops, delete_mapi_id);
    rops.extend_from_slice(&[
        0x35, 0x00, 0x01, 0x02, // RopMoveFolder
        0x01, // asynchronous requested; LPE completes synchronously
        0x01, // Unicode name
    ]);
    append_mapi_wire_id(&mut rops, source_mapi_id);
    rops.extend_from_slice(&utf16z("Folder1"));
    rops.extend_from_slice(&[
        0x36, 0x00, 0x01, 0x02, // RopCopyFolder
        0x01, // asynchronous requested; LPE completes synchronously
        0x01, // recursive copy
        0x01, // Unicode name
    ]);
    append_mapi_wire_id(&mut rops, source_mapi_id);
    rops.extend_from_slice(&utf16z("Folder1"));

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
    assert!(contains_bytes(&response_rops, &[0x1C, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x1D, 0x01, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x35, 0x01, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x36, 0x01, 0, 0, 0, 0, 0]));

    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 2);
    assert_eq!(created[0].name, "Folder1");
    assert_eq!(created[0].parent_id, None);
    assert_eq!(created[1].name, "Folder1");
    assert_eq!(created[1].parent_id, Some(target_parent_id));
    assert_eq!(destroyed_mailboxes.lock().unwrap().as_slice(), &[delete_id]);
    let updated = updated_mailboxes.lock().unwrap();
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].mailbox_id, source_id);
    assert_eq!(updated[0].name.as_deref(), Some("Folder1"));
    assert_eq!(updated[0].parent_id, Some(Some(target_parent_id)));
}

#[tokio::test]
async fn mapi_over_http_microsoft_create_folder_rejects_invalid_type_and_reserved_field() {
    for (folder_type, reserved, name) in [
        (0x03, 0x00, "Invalid Folder Type"),
        (0x01, 0x01, "Invalid Reserved Field"),
    ] {
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
            0x1C,
            0x00,
            0x01,
            0x02, // RopCreateFolder
            folder_type,
            0x01, // Unicode names
            0x00, // do not open existing
            reserved,
        ]);
        rops.extend_from_slice(&utf16z(name));
        rops.extend_from_slice(&utf16z(""));

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
            &[0x1C, 0x02, 0x57, 0x00, 0x07, 0x80]
        ));
        assert!(created_mailboxes.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn mapi_over_http_create_search_folder_persists_only_after_criteria() {
    let account = FakeStore::account();
    let search_folders = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555504",
            "inbox",
            "Inbox",
        )])),
        search_folders: search_folders.clone(),
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

    let mut restriction = Vec::new();
    append_search_property_multi_string(&mut restriction, 0x9000_101F, 0x04, &["Finance"]);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::SEARCH_FOLDER_ID);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x02, // search folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Finance Category"));
    rops.extend_from_slice(&utf16z(""));
    append_rop_set_search_criteria(
        &mut rops,
        2,
        &restriction,
        &[crate::mapi::identity::INBOX_FOLDER_ID],
        0x0000_0005,
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

    assert!(contains_bytes(&response_rops, &[0x1C, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x30, 0x02, 0, 0, 0, 0]));
    let stored = search_folders.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].account_id, account.account_id);
    assert_eq!(stored[0].display_name, "Finance Category");
    assert_eq!(
        stored[0].restriction_json,
        serde_json::json!({
            "kind": "mapi_bounded",
            "all": [
                {"field": "category", "equals": "Finance"}
            ]
        })
    );
}

#[tokio::test]
async fn mapi_over_http_create_root_search_folder_accepts_criteria() {
    let account = FakeStore::account();
    let search_folders = Arc::new(Mutex::new(Vec::new()));
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-4555-9555-555555555504",
            "inbox",
            "Inbox",
        )])),
        created_mailboxes: created_mailboxes.clone(),
        search_folders: search_folders.clone(),
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

    let mut restriction = Vec::new();
    append_search_property_multi_string(&mut restriction, 0x9000_101F, 0x04, &["Finance"]);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x02, // search folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Contact Search"));
    rops.extend_from_slice(&utf16z(""));
    append_rop_set_search_criteria(
        &mut rops,
        2,
        &restriction,
        &[crate::mapi::identity::INBOX_FOLDER_ID],
        0x0002_0026,
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

    assert!(contains_bytes(&response_rops, &[0x1C, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x30, 0x02, 0, 0, 0, 0]));
    assert!(created_mailboxes.lock().unwrap().is_empty());
    let stored = search_folders.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].account_id, account.account_id);
    assert_eq!(stored[0].display_name, "Contact Search");
}

#[tokio::test]
async fn mapi_over_http_create_search_folder_without_criteria_is_not_persisted() {
    let search_folders = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        search_folders: search_folders.clone(),
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::SEARCH_FOLDER_ID);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x02, // search folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Categories Rename Search Folder"));
    rops.extend_from_slice(&utf16z(""));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x1C, 0x02, 0, 0, 0, 0]));
    assert!(search_folders.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_delete_staged_search_folder_succeeds_without_persistence() {
    let search_folders = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        search_folders: search_folders.clone(),
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

    let mut create_rops = Vec::new();
    append_rop_open_folder(
        &mut create_rops,
        0,
        1,
        crate::mapi::identity::SEARCH_FOLDER_ID,
    );
    create_rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x02, // search folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    create_rops.extend_from_slice(&utf16z("Categories Rename Search Folder"));
    create_rops.extend_from_slice(&utf16z(""));
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&create_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let create = &response_rops[8..];
    assert_eq!(create[0], 0x1C);
    assert_eq!(u32::from_le_bytes(create[2..6].try_into().unwrap()), 0);
    let folder_id = crate::mapi::identity::object_id_from_wire_id(&create[6..14]).unwrap();
    assert!(search_folders.lock().unwrap().is_empty());

    renew_mapi_request_id(&mut execute_headers);
    let mut delete_rops = Vec::new();
    append_rop_open_folder(
        &mut delete_rops,
        0,
        1,
        crate::mapi::identity::SEARCH_FOLDER_ID,
    );
    delete_rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // deletion flags
    ]);
    append_mapi_wire_id(&mut delete_rops, folder_id);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(u32::from_le_bytes(delete[2..6].try_into().unwrap()), 0);
    assert_eq!(delete[6], 0);
    assert!(search_folders.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_delete_persisted_search_folder_removes_definition() {
    let account = FakeStore::account();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-3434343434d1").unwrap();
    let search_folder_mapi_id =
        crate::mapi::identity::legacy_migration_object_id(&search_folder_id);
    let search_folders = Arc::new(Mutex::new(vec![SearchFolderDefinition {
        id: search_folder_id,
        account_id: account.account_id,
        role: "custom".to_string(),
        display_name: "Categories Rename Search Folder".to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "message".to_string(),
        scope_json: serde_json::json!({"kind": "manual"}),
        restriction_json: serde_json::json!({"kind": "mapi_bounded", "all": []}),
        excluded_folder_roles: Vec::new(),
        is_builtin: false,
    }]));
    let deleted_search_folders = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(account),
        search_folders: search_folders.clone(),
        deleted_search_folders: deleted_search_folders.clone(),
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::SEARCH_FOLDER_ID);
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // deletion flags
    ]);
    append_mapi_wire_id(&mut rops, search_folder_mapi_id);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(u32::from_le_bytes(delete[2..6].try_into().unwrap()), 0);
    assert_eq!(delete[6], 0);
    assert!(search_folders.lock().unwrap().is_empty());
    assert_eq!(
        deleted_search_folders.lock().unwrap().as_slice(),
        &[search_folder_id]
    );
}

#[tokio::test]
async fn mapi_over_http_create_folder_advertised_special_folder_opens_existing_even_without_flag() {
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Sync Issues"));
    rops.extend_from_slice(&utf16z(""));

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
    let mut expected = vec![0x1C, 0x02, 0, 0, 0, 0];
    expected.extend_from_slice(&mapi_wire_id_bytes(
        crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
    ));
    expected.extend_from_slice(&[1, 0]);
    assert!(contains_bytes(&response_rops, &expected));
    assert!(created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_create_folder_quick_step_settings_opens_advertised_special_folder() {
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "66666666-6666-6666-6666-666666666666",
            "custom",
            "Quick Step Settings",
        )])),
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, IPM subtree
    ];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x01, // open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Quick Step Settings"));
    rops.extend_from_slice(&utf16z(""));

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
    let mut expected = vec![0x1C, 0x02, 0, 0, 0, 0];
    expected.extend_from_slice(&mapi_wire_id_bytes(
        crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
    ));
    expected.extend_from_slice(&[1, 0]);
    assert!(contains_bytes(&response_rops, &expected));
    assert!(created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_create_folder_invalid_type_returns_invalid_parameter() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, IPM subtree
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x00, // invalid folder type
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Invalid Folder"));
    rops.extend_from_slice(&utf16z(""));

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
    let create = &response_rops[8..];

    assert_eq!(create[0], 0x1C);
    assert_eq!(create[1], 0x02);
    assert_eq!(
        u32::from_le_bytes(create[2..6].try_into().unwrap()),
        0x8007_0057
    );
}

#[tokio::test]
async fn mapi_over_http_create_folder_duplicate_name_returns_duplicate_name() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "66666666-6666-6666-6666-666666666666",
            "custom",
            "MAPI Projects",
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
    let create = &response_rops[8..];

    assert_eq!(create[0], 0x1C);
    assert_eq!(create[1], 0x02);
    assert_eq!(
        u32::from_le_bytes(create[2..6].try_into().unwrap()),
        0x8004_0604
    );
}

#[tokio::test]
async fn mapi_over_http_create_folder_under_custom_parent_preserves_parent() {
    let parent_id = Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap();
    let top_level_same_name_id = Uuid::parse_str("88888888-8888-4888-8888-888888888888").unwrap();
    let parent_folder_id = test_mapi_uuid_id(&parent_id);
    crate::mapi::identity::remember_mapi_identity(parent_id, parent_folder_id);
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&parent_id.to_string(), "custom", "Projects"),
            FakeStore::mailbox(&top_level_same_name_id.to_string(), "custom", "Child"),
        ])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(parent_id, parent_folder_id)]))),
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, custom parent
    ];
    append_mapi_wire_id(&mut rops, parent_folder_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("Child"));
    rops.extend_from_slice(&utf16z(""));

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
    let create = &response_rops[8..];

    assert_eq!(create[0], 0x1C);
    assert_eq!(create[1], 0x02);
    assert_eq!(u32::from_le_bytes(create[2..6].try_into().unwrap()), 0);
    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].name, "Child");
    assert_eq!(created[0].parent_id, Some(parent_id));
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(u32::from_le_bytes(delete[2..6].try_into().unwrap()), 0);
    assert_eq!(delete[6], 0);
    assert_eq!(destroyed_mailboxes.lock().unwrap().as_slice(), &[custom_id]);
}

#[tokio::test]
async fn mapi_over_http_microsoft_delete_folder_rejects_reserved_flag_bits() {
    let custom_id = Uuid::parse_str("66666666-6666-4666-8666-666666666661").unwrap();
    let destroyed_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &custom_id.to_string(),
            "custom",
            "Reserved Flag Folder",
        )])),
        destroyed_mailboxes: destroyed_mailboxes.clone(),
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(1));
    rops.push(0);
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x02, // reserved DeleteFolderFlags bit
    ]);
    append_mapi_wire_id(&mut rops, test_mapi_uuid_id(&custom_id));

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
        &[0x1D, 0x01, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(destroyed_mailboxes.lock().unwrap().is_empty());
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
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
async fn mapi_over_http_microsoft_folder_move_copy_accepts_nonzero_boolean_fields() {
    let source_id = Uuid::parse_str("33333333-3333-4333-8333-333333333334").unwrap();
    let target_parent_id = Uuid::parse_str("34343434-3434-4434-8434-343434343435").unwrap();
    let source_mapi_id = test_mapi_uuid_id(&source_id);
    let target_parent_mapi_id = test_mapi_uuid_id(&target_parent_id);
    crate::mapi::identity::remember_mapi_identity(source_id, source_mapi_id);
    crate::mapi::identity::remember_mapi_identity(target_parent_id, target_parent_mapi_id);
    let updated_mailboxes = Arc::new(Mutex::new(Vec::new()));
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
        updated_mailboxes: updated_mailboxes.clone(),
        created_mailboxes: created_mailboxes.clone(),
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

    for (rop_id, booleans, name) in [
        (0x35, vec![0x02, 0x02], utf16z("Moved Projects")),
        (0x36, vec![0x02, 0x02, 0x02], utf16z("Copied Projects")),
    ] {
        let mut rops = Vec::new();
        append_rop_open_folder(
            &mut rops,
            0,
            1,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        );
        append_rop_open_folder(&mut rops, 0, 2, target_parent_mapi_id);
        rops.extend_from_slice(&[rop_id, 0x00, 0x01, 0x02]);
        rops.extend_from_slice(&booleans);
        append_mapi_wire_id(&mut rops, source_mapi_id);
        rops.extend_from_slice(&name);

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
            &[rop_id, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
        ));
        renew_mapi_request_id(&mut execute_headers);
    }

    let updated = updated_mailboxes.lock().unwrap();
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].name.as_deref(), Some("Moved Projects"));
    assert_eq!(updated[0].parent_id, Some(Some(target_parent_id)));
    drop(updated);

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
async fn mapi_over_http_open_message_recovers_unique_message_folder_mismatch() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let sent = FakeStore::mailbox("77777777-7777-7777-7777-777777777777", "sent", "Sent");
    let target = FakeStore::email(
        "87878787-8787-8787-8787-878787878787",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Folder mismatch open",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
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
        test_mapi_folder_id(7),
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
    assert!(contains_bytes(&response_rops, &[0x03, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x07, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Folder mismatch open")
    ));
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

    let client_blob = vec![0x44; 1040];
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
    let mut overridden = 1040u16.to_le_bytes().to_vec();
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

    let client_blob = vec![0x55; 1040];
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
    let mut persisted = 1040u16.to_le_bytes().to_vec();
    persisted.extend_from_slice(&client_blob);
    assert!(contains_bytes(&response_rops, &persisted));

    let mut canonical = 20u16.to_le_bytes().to_vec();
    canonical.extend_from_slice(account.account_id.as_bytes());
    canonical.extend_from_slice(&1u32.to_le_bytes());
    assert!(!contains_bytes(&response_rops, &canonical));
}

#[tokio::test]
async fn mapi_over_http_folder_extended_flags_survive_reconnect() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    let stored_folder_profile_values = store.mapi_folder_profile_property_values.clone();
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

    let client_flags = vec![0x01, 0x04, 0x00, 0x00, 0x20, 0x00];
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36DA_0102, &client_flags);

    let mut set_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut set_rops, test_mapi_folder_id(5));
    set_rops.push(0);
    set_rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on Inbox
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
    assert!(stored_folder_profile_values
        .lock()
        .unwrap()
        .values()
        .any(|value| value == &client_flags));

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
    append_mapi_wire_id(&mut get_rops, test_mapi_folder_id(5));
    get_rops.push(0);
    get_rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on reopened Inbox
    ]);
    get_rops.extend_from_slice(&4096u16.to_le_bytes());
    get_rops.extend_from_slice(&1u16.to_le_bytes());
    get_rops.extend_from_slice(&0x36DA_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&get_rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let mut persisted = (client_flags.len() as u16).to_le_bytes().to_vec();
    persisted.extend_from_slice(&client_flags);
    assert!(contains_bytes(&response_rops, &persisted));
}

#[tokio::test]
async fn mapi_over_http_delete_folder_local_default_named_view_is_noop_success() {
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
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_delete_messages(
        &mut rops,
        1,
        &[crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID],
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
    assert!(contains_bytes(
        &response_rops,
        &[0x1E, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
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
async fn mapi_over_http_per_user_information_rops_reject_folder_handle_blob_batch() {
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
        &[0x63, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x64, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
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
async fn mapi_over_http_microsoft_hard_delete_messages_and_subfolders_hard_deletes_trash_contents()
{
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
async fn mapi_over_http_microsoft_empty_folder_rops_accept_nonzero_boolean_fields() {
    let trash_id = Uuid::parse_str("77777777-7777-4777-8777-000000002590").unwrap();
    for (rop_id, message_id) in [
        (
            0x58,
            Uuid::parse_str("33333333-3333-4333-8333-333333333334").unwrap(),
        ),
        (
            0x92,
            Uuid::parse_str("33333333-3333-4333-8333-333333333335").unwrap(),
        ),
    ] {
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
                "EmptyFolder nonzero booleans",
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
        rops.extend_from_slice(&[rop_id, 0x00, 0x01, 0x02, 0x02]);

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
            &[rop_id, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
        ));
        assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
    }
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
async fn mapi_over_http_hard_delete_messages_and_subfolders_deletes_child_folder_contents() {
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
        &[parent_message_id, child_message_id]
    );
    let canonical = canonical_emails.lock().unwrap();
    assert!(canonical.iter().all(|email| email.id != parent_message_id));
    assert!(canonical.iter().all(|email| email.id != child_message_id));
}

#[tokio::test]
async fn mapi_over_http_empty_folder_keeps_child_folder_contents() {
    let parent_id = Uuid::parse_str("77777777-7777-4777-8777-777777777780").unwrap();
    let child_id = Uuid::parse_str("77777777-7777-4777-8777-777777777781").unwrap();
    let parent_message_id = Uuid::parse_str("77777777-7777-4777-8777-777777777782").unwrap();
    let child_message_id = Uuid::parse_str("77777777-7777-4777-8777-777777777783").unwrap();
    let parent = FakeStore::mailbox(&parent_id.to_string(), "", "Parent");
    let mut child = FakeStore::mailbox(&child_id.to_string(), "", "Child");
    child.parent_id = Some(parent_id);
    let parent_folder_id = test_mapi_folder_id(0x1781);
    let child_folder_id = test_mapi_folder_id(0x1782);
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
        &[parent_message_id]
    );
    let canonical = canonical_emails.lock().unwrap();
    assert!(canonical.iter().all(|email| email.id != parent_message_id));
    assert!(canonical.iter().any(|email| email.id == child_message_id));
}

#[tokio::test]
async fn mapi_over_http_open_folder_accepts_additional_ren_junk_alias() {
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
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 42,
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
    let empty = Vec::new();
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
            &empty,
        ],
    );

    let mut set_rops = Vec::new();
    append_rop_open_folder(&mut set_rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_set_properties(&mut set_rops, 1, 1, &property_values);
    let set_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&set_rops, &[1])),
        )
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&set_response);
    let set_response_rops = response_rops_from_execute_response(set_response).await;
    assert!(contains_bytes(
        &set_response_rops,
        &[0x0A, 0x01, 0, 0, 0, 0, 0, 0]
    ));

    let mut open_headers = mapi_headers("Execute");
    open_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, stale_junk_id);
    append_rop_get_properties_specific(&mut open_rops, 1, &[0x3001_001F, 0x3613_001F]);
    let open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &open_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let open_response_rops = response_rops_from_execute_response(open_response).await;
    assert!(contains_bytes(
        &open_response_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&open_response_rops, &utf16z("Junk E-mail")));
    assert!(contains_bytes(&open_response_rops, &utf16z("IPF.Note")));
}

#[tokio::test]
async fn mapi_over_http_open_folder_rejects_unlearned_client_local_folder_id() {
    let client_local_folder_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER + 78,
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, client_local_folder_id);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
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
    {
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
    assert!(contains_bytes(
        &response_rops,
        &[0x03, 0x01, 0x00, 0x01, 0x00]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("invoice")));
}

#[tokio::test]
async fn mapi_over_http_microsoft_set_search_criteria_rejects_initial_empty_folder_scope() {
    let account = FakeStore::account();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343493").unwrap();
    let search_folder_mapi_id = test_mapi_uuid_id(&search_folder_id);
    crate::mapi::identity::remember_mapi_identity(search_folder_id, search_folder_mapi_id);
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: search_folder_id,
            account_id: account.account_id,
            role: "custom".to_string(),
            display_name: "No initial scope".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"original": "scope"}),
            restriction_json: serde_json::json!({"original": "restriction"}),
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
    restriction.extend_from_slice(&1u16.to_le_bytes());
    append_search_content(&mut restriction, 0x0037_001F, "invoice");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &restriction, &[], 0x0000_000A);
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
        &[0x30, 0x01, 0x05, 0x06, 0x04, 0x80]
    ));
    let stored = stored_search_folders.lock().unwrap();
    assert_eq!(
        stored[0].scope_json,
        serde_json::json!({"original": "scope"})
    );
    assert_eq!(
        stored[0].restriction_json,
        serde_json::json!({"original": "restriction"})
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_set_search_criteria_rejects_scope_containing_search_folder() {
    let account = FakeStore::account();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343494").unwrap();
    let search_folder_mapi_id = test_mapi_uuid_id(&search_folder_id);
    crate::mapi::identity::remember_mapi_identity(search_folder_id, search_folder_mapi_id);
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: search_folder_id,
            account_id: account.account_id,
            role: "custom".to_string(),
            display_name: "Self scoped".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"original": "scope"}),
            restriction_json: serde_json::json!({"original": "restriction"}),
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
    restriction.extend_from_slice(&1u16.to_le_bytes());
    append_search_content(&mut restriction, 0x0037_001F, "invoice");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[search_folder_mapi_id],
        0x0000_000A,
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

    assert!(contains_bytes(
        &response_rops,
        &[0x30, 0x01, 0x90, 0x04, 0x00, 0x00]
    ));
    let stored = stored_search_folders.lock().unwrap();
    assert_eq!(
        stored[0].scope_json,
        serde_json::json!({"original": "scope"})
    );
    assert_eq!(
        stored[0].restriction_json,
        serde_json::json!({"original": "restriction"})
    );
}
