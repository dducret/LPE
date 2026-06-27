use super::*;

#[tokio::test]
async fn mapi_over_http_public_folder_logon_allocates_public_folder_store_handle() {
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

    assert_eq!(response_rop[0], 0xFE);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6], 0x00);
    assert_eq!(response_rop.len(), 70);
    assert_eq!(
        &response_rop[7..15],
        &crate::mapi::identity::wire_id_bytes_from_object_id(
            crate::mapi::identity::PUBLIC_FOLDERS_ROOT_FOLDER_ID
        )
        .unwrap()
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
async fn mapi_over_http_public_folder_replica_rops_validate_canonical_folder_ids() {
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
    rops.extend_from_slice(&[0x00, 0x00]);

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
        &[0x42, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x45, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
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
async fn mapi_over_http_microsoft_public_folder_replica_rops_require_logon_handle_and_shape() {
    let public_root_id = crate::mapi::identity::PUBLIC_FOLDERS_ROOT_FOLDER_ID;
    let mut rops = vec![
        0x42, 0x00, 0x01, // RopGetOwningServers on missing handle 1.
    ];
    append_mapi_wire_id(&mut rops, public_root_id);
    append_rop_open_folder(&mut rops, 0, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x45, 0x00, 0x02, // RopPublicFolderIsGhosted on folder handle 2.
    ]);
    append_mapi_wire_id(&mut rops, public_root_id);
    rops.extend_from_slice(&[
        0x42, 0x00, 0x00, // RopGetOwningServers succeeds on the logon handle.
    ]);
    append_mapi_wire_id(&mut rops, public_root_id);
    rops.extend_from_slice(&[
        0x45, 0x00, 0x00, // RopPublicFolderIsGhosted succeeds on the logon handle.
    ]);
    append_mapi_wire_id(&mut rops, public_root_id);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x42, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x45, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x42, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x45, 0x00, 0, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_public_folder_logon_is_supported_without_private_store_flag() {
    let mut rops = vec![0xFE, 0x00, 0x01, 0x00]; // Public-folder RopLogon.
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());

    let response_rops = execute_rops_response_rops(&rops, &[u32::MAX]).await;

    assert_eq!(response_rops[0], 0xFE);
    assert_eq!(response_rops[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[6], 0x00);
    assert_eq!(response_rops.len(), 70);
}

#[tokio::test]
async fn mapi_over_http_public_folder_logon_exposes_empty_public_hierarchy_table() {
    let mut rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
    ]);

    let response_rops = execute_rops_response_rops(&rops, &[u32::MAX, u32::MAX]).await;
    let hierarchy_offset = 70;

    assert_eq!(response_rops[0], 0xFE);
    assert_eq!(response_rops[hierarchy_offset], 0x04);
    assert_eq!(response_rops[hierarchy_offset + 1], 0x01);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[hierarchy_offset + 2..hierarchy_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rops[hierarchy_offset + 6..hierarchy_offset + 10]
                .try_into()
                .unwrap()
        ),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_public_folder_hierarchy_table_lists_canonical_roots() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", None, "Public Root"),
            FakeStore::public_folder(
                "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "Team Posts",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let logon_cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&3u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3602_0003u32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3603_0003u32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&50u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&logon_cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let hierarchy_cookie = mapi_cookie_header(&hierarchy_response);
    let body = response_bytes(hierarchy_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rops[0], 0x04);
    assert_eq!(response_rops[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u32::from_le_bytes(response_rops[6..10].try_into().unwrap()),
        1
    );
    let query_offset = 10 + 7;
    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(contains_bytes(response_rops, &utf16z("Public Root")));

    let root_mapi_id = crate::mapi::identity::mapped_mapi_object_id(
        &Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap(),
    )
    .unwrap();
    let mut child_hierarchy_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut child_hierarchy_rops, root_mapi_id);
    child_hierarchy_rops.push(0);
    child_hierarchy_rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable on opened public root.
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    child_hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    child_hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    child_hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    child_hierarchy_rops.extend_from_slice(&50u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&hierarchy_cookie).unwrap());
    let child_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&child_hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(child_response.status(), StatusCode::OK);
    let body = response_bytes(child_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
    let child_query_offset = 8 + 10 + 7;
    assert_eq!(response_rops[0], 0x02);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u32::from_le_bytes(response_rops[14..18].try_into().unwrap()),
        1
    );
    assert_eq!(response_rops[child_query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[child_query_offset + 7..child_query_offset + 9]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(contains_bytes(response_rops, &utf16z("Team Posts")));
}

#[tokio::test]
async fn mapi_over_http_microsoft_public_folder_create_folder_uses_canonical_store() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let existing_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(root_id, None, "Public Root"),
            FakeStore::public_folder(existing_id, Some(root_id), "Existing Public"),
        ])),
        ..Default::default()
    };
    let public_folders = store.public_folders.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut create_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut create_rops, root_mapi_id);
    create_rops.push(0);
    create_rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    create_rops.extend_from_slice(&utf16z("Created Public"));
    create_rops.extend_from_slice(&utf16z(""));
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let create_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&create_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(create_response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(create_response).await;
    let create_offset = response_rops
        .windows(6)
        .position(|window| window == [0x1C, 0x02, 0, 0, 0, 0])
        .unwrap();
    assert_eq!(response_rops[create_offset + 14], 0);
    {
        let folders = public_folders.lock().unwrap();
        assert_eq!(folders.len(), 3);
        assert_eq!(
            folders[2].parent_folder_id,
            Some(Uuid::parse_str(root_id).unwrap())
        );
        assert_eq!(folders[2].display_name, "Created Public");
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut open_existing_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut open_existing_rops, root_mapi_id);
    open_existing_rops.push(0);
    open_existing_rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x01, // open existing
        0x00, // reserved
    ]);
    open_existing_rops.extend_from_slice(&utf16z("Existing Public"));
    open_existing_rops.extend_from_slice(&utf16z(""));
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_existing_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let create_offset = response_rops
        .windows(6)
        .position(|window| window == [0x1C, 0x02, 0, 0, 0, 0])
        .unwrap();
    assert_eq!(response_rops[create_offset + 14], 0);
    assert_eq!(response_rops.len(), create_offset + 15);
}

#[tokio::test]
async fn mapi_over_http_microsoft_public_folder_delete_folder_uses_canonical_store() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let child_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(root_id, None, "Public Root"),
            FakeStore::public_folder(child_id, Some(root_id), "Deleted Public"),
        ])),
        ..Default::default()
    };
    let public_folders = store.public_folders.clone();
    let deleted_public_folders = store.deleted_public_folders.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let child_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(child_id).unwrap()).unwrap();
    let mut delete_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut delete_rops, root_mapi_id);
    delete_rops.push(0);
    delete_rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // empty-folder-only default
    ]);
    append_mapi_wire_id(&mut delete_rops, child_mapi_id);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let delete_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(delete_response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(delete_response).await;
    let delete_offset = response_rops
        .windows(6)
        .position(|window| window == [0x1D, 0x01, 0, 0, 0, 0])
        .unwrap();
    assert_eq!(response_rops[delete_offset + 6], 0);
    assert_eq!(
        deleted_public_folders.lock().unwrap().as_slice(),
        &[Uuid::parse_str(child_id).unwrap()]
    );
    let folders = public_folders.lock().unwrap();
    assert_eq!(folders[1].lifecycle_state, "deleted");
}

#[tokio::test]
async fn mapi_over_http_microsoft_public_folder_empty_folder_deletes_canonical_items() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folder_items = store.public_folder_items.clone();
    let deleted_public_folder_items = store.deleted_public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut empty_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut empty_rops, root_mapi_id);
    empty_rops.push(0);
    empty_rops.extend_from_slice(&[
        0x58, 0x00, 0x01, // RopEmptyFolder
        0x00, // synchronous
        0x00, // do not delete associated messages
    ]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let empty_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&empty_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(empty_response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(empty_response).await;
    let empty_offset = response_rops
        .windows(6)
        .position(|window| window == [0x58, 0x01, 0, 0, 0, 0])
        .unwrap();
    assert_eq!(response_rops[empty_offset + 6], 0);
    assert_eq!(
        deleted_public_folder_items.lock().unwrap().as_slice(),
        &[Uuid::parse_str(item_id).unwrap()]
    );
    let items = public_folder_items.lock().unwrap();
    assert_eq!(items[0].lifecycle_state, "deleted");
}

#[tokio::test]
async fn mapi_over_http_microsoft_public_folder_copy_folder_uses_canonical_store() {
    let source_parent_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let source_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    let target_parent_id = "dddddddd-eeee-ffff-0000-111111111111";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(source_parent_id, None, "Source Parent"),
            FakeStore::public_folder(source_id, Some(source_parent_id), "Source Public"),
            FakeStore::public_folder(target_parent_id, None, "Target Parent"),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            source_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folders = store.public_folders.clone();
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let source_parent_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(source_parent_id).unwrap())
            .unwrap();
    let source_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(source_id).unwrap()).unwrap();
    let target_parent_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(target_parent_id).unwrap())
            .unwrap();
    let mut copy_rops = Vec::new();
    append_rop_open_folder(&mut copy_rops, 0, 1, source_parent_mapi_id);
    append_rop_open_folder(&mut copy_rops, 0, 2, target_parent_mapi_id);
    copy_rops.extend_from_slice(&[
        0x36, 0x00, 0x01, 0x02, // RopCopyFolder
        0x00, // synchronous
        0x01, // recursive
        0x01, // Unicode name
    ]);
    append_mapi_wire_id(&mut copy_rops, source_mapi_id);
    copy_rops.extend_from_slice(&utf16z("Copied Public"));
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let copy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&copy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(copy_response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(copy_response).await;
    assert!(contains_bytes(&response_rops, &[0x36, 0x01, 0, 0, 0, 0, 0]));
    let folders = public_folders.lock().unwrap();
    let copied_folder = folders
        .iter()
        .find(|folder| folder.display_name == "Copied Public")
        .unwrap();
    assert_eq!(
        copied_folder.parent_folder_id,
        Some(Uuid::parse_str(target_parent_id).unwrap())
    );
    let items = public_folder_items.lock().unwrap();
    let copied_item = items
        .iter()
        .find(|item| item.public_folder_id == copied_folder.id)
        .unwrap();
    assert_eq!(copied_item.subject, "Public post");
}

#[tokio::test]
async fn mapi_over_http_microsoft_public_folder_move_folder_uses_canonical_store() {
    let source_parent_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let source_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    let target_parent_id = "dddddddd-eeee-ffff-0000-111111111111";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(source_parent_id, None, "Source Parent"),
            FakeStore::public_folder(source_id, Some(source_parent_id), "Source Public"),
            FakeStore::public_folder(target_parent_id, None, "Target Parent"),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            source_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let public_folders = store.public_folders.clone();
    let public_folder_items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let source_parent_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(source_parent_id).unwrap())
            .unwrap();
    let source_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(source_id).unwrap()).unwrap();
    let target_parent_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(target_parent_id).unwrap())
            .unwrap();
    let mut move_rops = Vec::new();
    append_rop_open_folder(&mut move_rops, 0, 1, source_parent_mapi_id);
    append_rop_open_folder(&mut move_rops, 0, 2, target_parent_mapi_id);
    move_rops.extend_from_slice(&[
        0x35, 0x00, 0x01, 0x02, // RopMoveFolder
        0x00, // synchronous
        0x01, // Unicode name
    ]);
    append_mapi_wire_id(&mut move_rops, source_mapi_id);
    move_rops.extend_from_slice(&utf16z("Moved Public"));
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let move_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&move_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(move_response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(move_response).await;
    assert!(contains_bytes(&response_rops, &[0x35, 0x01, 0, 0, 0, 0, 0]));
    let folders = public_folders.lock().unwrap();
    let moved_folder = folders
        .iter()
        .find(|folder| folder.id == Uuid::parse_str(source_id).unwrap())
        .unwrap();
    assert_eq!(moved_folder.display_name, "Moved Public");
    assert_eq!(
        moved_folder.parent_folder_id,
        Some(Uuid::parse_str(target_parent_id).unwrap())
    );
    let items = public_folder_items.lock().unwrap();
    assert_eq!(
        items[0].public_folder_id,
        Uuid::parse_str(source_id).unwrap()
    );
    assert_eq!(items[0].subject, "Public post");
}

#[tokio::test]
async fn mapi_over_http_public_folder_contents_table_lists_canonical_items() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            "cccccccc-dddd-eeee-ffff-000000000000",
            root_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);
    let hierarchy_response_rops = response_rops_from_execute_response(hierarchy_response).await;
    assert!(contains_bytes(
        &hierarchy_response_rops,
        &utf16z("Public Root")
    ));

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut rops, root_mapi_id);
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
    let contents_offset = 8;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(
        &response_rops[contents_offset + 2..contents_offset + 6],
        &[0, 0, 0, 0]
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(contains_bytes(&response_rops, &utf16z("Public post")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Post")));
}

#[tokio::test]
async fn mapi_over_http_public_folder_conversation_members_table_applies_restriction() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![
            FakeStore::public_folder_item(
                "cccccccc-dddd-eeee-ffff-000000000001",
                root_id,
                "Public restricted match",
            ),
            FakeStore::public_folder_item(
                "cccccccc-dddd-eeee-ffff-000000000002",
                root_id,
                "Public restricted miss",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut restriction = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    restriction.extend_from_slice(&0x0037_001Fu32.to_le_bytes()); // PidTagSubject.
    append_mapi_utf16_property(&mut restriction, 0x0037_001F, "Public restricted match");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut rops, root_mapi_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02,
        0xC8, // RopGetContentsTable ConversationMembers|UseUnicode|DeferredErrors.
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes()); // PidTagSubject.
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes()); // PidTagMessageClass.
    rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]); // RopRestrict.
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
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
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Public restricted match")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Public restricted miss")
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_contents_table_findrow_finds_restricted_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![
            FakeStore::public_folder_item(
                "cccccccc-dddd-eeee-ffff-000000000003",
                root_id,
                "Public findrow miss",
            ),
            FakeStore::public_folder_item(
                "cccccccc-dddd-eeee-ffff-000000000004",
                root_id,
                "Public findrow match",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let restriction = mapi_content_restriction(0x0037_001F, "Public findrow match");
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder on public root.
    ];
    append_mapi_wire_id(&mut rops, root_mapi_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes()); // PidTagSubject.
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes()); // PidTagMessageClass.
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Public findrow match")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Public findrow miss")
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_rules_table_is_empty_without_protocol_local_rules() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let rule_id = Uuid::parse_str("aaaaaaaa-4444-4111-8111-aaaaaaaaaaaa").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        mailbox_rules: Arc::new(Mutex::new(vec![MailboxRule {
            id: rule_id,
            name: "Private Reports".to_string(),
            is_active: true,
            source_kind: "sieve_script".to_string(),
            condition_summary: "header Subject contains report".to_string(),
            action_summary: "fileinto Reports".to_string(),
            supported_outlook_projection: true,
            unsupported_exchange_features: Vec::new(),
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
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[
        0x3F, 0x00, 0x01, 0x02, 0x00, // RopGetRulesTable
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&8u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x3F, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0, 0]
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Private Reports")));
}

#[tokio::test]
async fn mapi_over_http_public_folder_get_properties_reads_canonical_folder() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_get_properties_specific(
        &mut rops,
        1,
        &[0x3001_001F, 0x3613_001F, 0x6749_0014, 0x3602_0003],
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
    assert!(contains_bytes(&response_rops, &utf16z("Public Root")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Note")));
    assert!(contains_bytes(&response_rops, &0u32.to_le_bytes()));
    let mut parent_folder_id = Vec::new();
    append_mapi_wire_id(
        &mut parent_folder_id,
        crate::mapi::identity::PUBLIC_FOLDERS_ROOT_FOLDER_ID,
    );
    assert!(contains_bytes(&response_rops, &parent_folder_id));
}

#[tokio::test]
async fn mapi_over_http_public_folder_permissions_table_projects_canonical_grants() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let root_uuid = Uuid::parse_str(root_id).unwrap();
    let delegate_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_permissions: Arc::new(Mutex::new(vec![PublicFolderPermission {
            id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
            public_folder_id: root_uuid,
            principal_account_id: delegate_id,
            principal_email: "delegate@example.test".to_string(),
            principal_display_name: "Public Delegate".to_string(),
            rights: PublicFolderRights {
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            created_at: "2026-05-07T12:00:00Z".to_string(),
            updated_at: "2026-05-07T12:00:00Z".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id = crate::mapi::identity::mapped_mapi_object_id(&root_uuid).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[0x3E, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6672_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&8u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x3E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &utf16z("Public Delegate")));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::permissions::rights_from_grant(true, true, false, false).to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_modify_permissions_writes_canonical_grants() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let root_uuid = Uuid::parse_str(root_id).unwrap();
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(81);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        ..Default::default()
    };
    let observed_permissions = store.public_folder_permissions.clone();
    let observed_audits = store.mapi_folder_permission_audits.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id = crate::mapi::identity::mapped_mapi_object_id(&root_uuid).unwrap();
    let mut add_rops = Vec::new();
    append_rop_open_folder(&mut add_rops, 0, 1, root_mapi_id);
    add_rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    add_rops.extend_from_slice(&1u16.to_le_bytes());
    add_rops.push(0x01);
    add_rops.extend_from_slice(&2u16.to_le_bytes());
    add_rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    add_rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    add_rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    add_rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, true, false) as i32)
            .to_le_bytes(),
    );
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let add_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&add_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(add_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&add_response);
    let add_response_rops = response_rops_from_execute_response(add_response).await;
    assert!(contains_bytes(
        &add_response_rops,
        &[0x40, 0x01, 0, 0, 0, 0]
    ));
    let permissions = observed_permissions.lock().unwrap().clone();
    let delegate_permission = permissions
        .iter()
        .find(|permission| permission.principal_account_id == delegate.account_id)
        .expect("delegate permission was not written");
    assert_eq!(delegate_permission.public_folder_id, root_uuid);
    assert!(delegate_permission.rights.may_read);
    assert!(delegate_permission.rights.may_write);
    assert!(delegate_permission.rights.may_delete);
    assert!(!delegate_permission.rights.may_share);
    drop(permissions);

    let mut remove_rops = Vec::new();
    append_rop_open_folder(&mut remove_rops, 0, 1, root_mapi_id);
    remove_rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    remove_rops.extend_from_slice(&1u16.to_le_bytes());
    remove_rops.push(0x04);
    remove_rops.extend_from_slice(&1u16.to_le_bytes());
    remove_rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    remove_rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let remove_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&remove_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(remove_response.status(), StatusCode::OK);
    let remove_response_rops = response_rops_from_execute_response(remove_response).await;
    assert!(contains_bytes(
        &remove_response_rops,
        &[0x40, 0x01, 0, 0, 0, 0]
    ));
    assert!(observed_permissions
        .lock()
        .unwrap()
        .iter()
        .all(|permission| permission.principal_account_id != delegate.account_id));
    let audits = observed_audits.lock().unwrap();
    assert_eq!(audits[0].action, "mapi-modify-public-folder-permissions");
    assert_eq!(audits[1].action, "mapi-modify-public-folder-permissions");
}

#[tokio::test]
async fn mapi_over_http_public_folder_modify_permissions_requires_share_right() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let root_uuid = Uuid::parse_str(root_id).unwrap();
    let principal = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "delegate@example.test".to_string(),
        display_name: "Public Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let grantee = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        email: "grantee@example.test".to_string(),
        display_name: "Grantee".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let grantee_member_id = crate::mapi::identity::mapi_store_id(82);
    let mut public_root = FakeStore::public_folder(root_id, None, "Public Root");
    public_root.rights.may_share = false;
    let store = FakeStore {
        session: Some(principal),
        public_folders: Arc::new(Mutex::new(vec![public_root])),
        directory_accounts: Arc::new(Mutex::new(vec![grantee.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            grantee.account_id,
            grantee_member_id,
        )]))),
        ..Default::default()
    };
    let observed_permissions = store.public_folder_permissions.clone();
    let observed_audits = store.mapi_folder_permission_audits.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id = crate::mapi::identity::mapped_mapi_object_id(&root_uuid).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(grantee_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, false, false) as i32)
            .to_le_bytes(),
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
    assert!(contains_bytes(
        &response_rops,
        &[0x40, 0x01, 0x05, 0x00, 0x07, 0x80]
    ));
    assert!(observed_permissions.lock().unwrap().is_empty());
    assert!(observed_audits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_public_folder_modify_permissions_rejects_unknown_member_without_grant() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let root_uuid = Uuid::parse_str(root_id).unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let observed_permissions = store.public_folder_permissions.clone();
    let observed_audits = store.mapi_folder_permission_audits.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id = crate::mapi::identity::mapped_mapi_object_id(&root_uuid).unwrap();
    let unknown_member_id = crate::mapi::identity::mapi_store_id(1984);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(unknown_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, false, false) as i32)
            .to_le_bytes(),
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
    assert!(contains_bytes(
        &response_rops,
        &[0x40, 0x01, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(observed_permissions.lock().unwrap().is_empty());
    assert!(observed_audits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_public_folder_message_status_is_session_local() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Status public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus
    append_mapi_wire_id(&mut rops, item_mapi_id);
    rops.extend_from_slice(&[0x20, 0x00, 0x01]); // RopSetMessageStatus
    append_mapi_wire_id(&mut rops, item_mapi_id);
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus
    append_mapi_wire_id(&mut rops, item_mapi_id);

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
    assert_eq!(
        response_rops
            .windows(10)
            .filter(|window| *window == [0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0].as_slice())
            .count(),
        2
    );
    assert!(contains_bytes(
        &response_rops,
        &[0x20, 0x01, 0, 0, 0, 0, 0x20, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_open_message_reads_canonical_item_properties() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_open_message(&mut rops, 1, 2, root_mapi_id, item_mapi_id);
    append_rop_get_properties_specific(&mut rops, 2, &[0x0037_001F, 0x001A_001F, 0x1000_001F]);

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
    assert_eq!(response_rops[0], 0x02);
    assert_eq!(response_rops[8], 0x03);
    assert!(contains_bytes(&response_rops, &utf16z("Public post")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Post")));
    assert!(contains_bytes(&response_rops, &utf16z("Public body")));
}

#[tokio::test]
async fn mapi_over_http_public_folder_content_sync_exports_canonical_items() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Public sync post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
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
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    let message = &stream.message_changes[0];
    assert_eq!(message.subject, "Public sync post");
    assert!(message.body_tags.contains(&PID_TAG_BODY_W));
    assert!(contains_bytes(&response_rops, &utf16z("Public body")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Post")));
}

#[tokio::test]
async fn mapi_over_http_public_folder_content_sync_exports_canonical_read_state() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let read_item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let unread_item_id = "dddddddd-eeee-ffff-0000-111111111111";
    let mut read_item = FakeStore::public_folder_item(read_item_id, root_id, "Read public post");
    read_item.is_read = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![
            read_item,
            FakeStore::public_folder_item(unread_item_id, root_id, "Unread public post"),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x08, 0x00, // content sync, ReadState
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
    let read_item_id = Uuid::parse_str(read_item_id).unwrap();
    let unread_item_id = Uuid::parse_str(unread_item_id).unwrap();
    assert!(contains_bytes(
        &response_rops,
        &mapi_read_message_idset_property(&[read_item_id])
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_unread_message_idset_property(&[unread_item_id])
    ));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.read_idset.is_some());
    assert!(stream.unread_idset.is_some());
}

#[tokio::test]
async fn mapi_over_http_public_folder_set_read_flags_updates_canonical_read_state() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Unread public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_set_read_flags(&mut rops, 1, 0x01, &[item_mapi_id]);

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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x66, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(items.lock().unwrap()[0].is_read);
}

#[tokio::test]
async fn mapi_over_http_public_folder_set_message_read_flag_updates_canonical_read_state() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Unread public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_open_message(&mut rops, 1, 2, root_mapi_id, item_mapi_id);
    rops.extend_from_slice(&[
        0x11, 0x00, 0x02, 0x02, 0x01, // RopSetMessageReadFlag
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
        &[0x11, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01]
    ));
    assert!(items.lock().unwrap()[0].is_read);
}

#[tokio::test]
async fn mapi_over_http_public_folder_create_message_saves_canonical_post_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x001A_001F, "IPM.Post");
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "MAPI public post");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Public post body");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, root_mapi_id);
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x02, 0x02, 0x00, // RopSaveChangesMessage
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
        &[0x06, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x02, 0x00, 0x00, 0x00, 0x00, 0x02]
    ));
    let items = items.lock().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].public_folder_id, Uuid::parse_str(root_id).unwrap());
    assert_eq!(items[0].message_class, "IPM.Post");
    assert_eq!(items[0].subject, "MAPI public post");
    assert_eq!(items[0].body_text, "Public post body");
}

#[tokio::test]
async fn mapi_over_http_public_folder_create_message_rejects_recipients() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Recipient public post");
    let to_row = mapi_wrapped_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, root_mapi_id);
    rops.push(0);
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
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
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(items.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_public_folder_set_properties_updates_canonical_post_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Old public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x001A_001F, "IPM.Post");
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Updated public post");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Updated public body");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_open_message(&mut rops, 1, 2, root_mapi_id, item_mapi_id);
    append_rop_set_properties(&mut rops, 2, 3, &property_values);
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
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x02, 0x00, 0x00, 0x00, 0x00, 0x02]
    ));
    let items = items.lock().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].message_class, "IPM.Post");
    assert_eq!(items[0].subject, "Updated public post");
    assert_eq!(items[0].body_text, "Updated public body");
    assert_eq!(items[0].change_counter, 2);
}

#[tokio::test]
async fn mapi_over_http_public_folder_item_custom_named_property_round_trips() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let custom_tag = 0x8001_001F;
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, custom_tag, "public opaque value");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_open_message(&mut rops, 1, 2, root_mapi_id, item_mapi_id);
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_get_properties_specific(&mut rops, 2, &[custom_tag]);

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
        &utf16z("public opaque value")
    ));

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, root_mapi_id);
    append_rop_open_message(&mut delete_rops, 1, 2, root_mapi_id, item_mapi_id);
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
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0B, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("public opaque value")
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_body_stream_updates_canonical_post_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Old public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let stream_body = utf16z("Stream updated public body");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_open_message(&mut rops, 1, 2, root_mapi_id, item_mapi_id);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream, PidTagBody.
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[0x2D, 0x00, 0x03]); // RopWriteStream.
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(&stream_body);
    rops.extend_from_slice(&[0x5D, 0x00, 0x03]); // RopCommitStream.
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
    assert!(contains_bytes(
        &response_rops,
        &[0x2B, 0x03, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x2D, 0x03, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x5D, 0x03, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x02, 0x00, 0x00, 0x00, 0x00, 0x02]
    ));
    let items = items.lock().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].body_text, "Stream updated public body");
    assert_eq!(items[0].change_counter, 2);
}

#[tokio::test]
async fn mapi_over_http_public_folder_delete_message_deletes_canonical_post_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Deleted public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let deleted_items = store.deleted_public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    append_rop_delete_messages(&mut rops, 1, &[item_mapi_id]);

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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x1E, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    assert_eq!(items.lock().unwrap()[0].lifecycle_state, "deleted");
    assert_eq!(
        deleted_items.lock().unwrap().as_slice(),
        &[Uuid::parse_str(item_id).unwrap()]
    );
}

#[tokio::test]
async fn mapi_over_http_public_folder_hard_delete_message_deletes_canonical_post_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Hard deleted public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let deleted_items = store.deleted_public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[
        0x91, 0x00, 0x01, 0x00, 0x00, // RopHardDeleteMessages.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, item_mapi_id);

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
    assert!(contains_bytes(
        &response_rops,
        &[0x91, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    assert_eq!(items.lock().unwrap()[0].lifecycle_state, "deleted");
    assert_eq!(
        deleted_items.lock().unwrap().as_slice(),
        &[Uuid::parse_str(item_id).unwrap()]
    );
}

#[tokio::test]
async fn mapi_over_http_public_folder_recursive_purge_deletes_canonical_items() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let deleted_items = store.deleted_public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![0x04, 0x00, 0x00, 0x01, 0x04];
    hierarchy_rops.extend_from_slice(&[0x15, 0x00, 0x01, 0x00, 0x01]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[0x92, 0x00, 0x01, 0x00, 0x00]);

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

    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x92, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    assert_eq!(items.lock().unwrap()[0].lifecycle_state, "deleted");
    assert_eq!(
        deleted_items.lock().unwrap().as_slice(),
        &[Uuid::parse_str(item_id).unwrap()]
    );
}

#[tokio::test]
async fn mapi_over_http_public_folder_copy_message_copies_canonical_post_item() {
    let source_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let target_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(source_id, None, "Source Public"),
            FakeStore::public_folder(target_id, None, "Target Public"),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            source_id,
            "Copied public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let source_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(source_id).unwrap()).unwrap();
    let target_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(target_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, source_mapi_id);
    append_rop_open_folder(&mut rops, 0, 2, target_mapi_id);
    rops.extend_from_slice(&[
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, copy.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, item_mapi_id);
    rops.push(0);
    rops.push(1);

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
        &[0x33, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    let items = items.lock().unwrap();
    assert_eq!(items.len(), 2);
    assert!(items.iter().any(|item| {
        item.id == Uuid::parse_str(item_id).unwrap()
            && item.public_folder_id == Uuid::parse_str(source_id).unwrap()
            && item.lifecycle_state == "active"
    }));
    assert!(items.iter().any(|item| {
        item.public_folder_id == Uuid::parse_str(target_id).unwrap()
            && item.subject == "Copied public post"
            && item.lifecycle_state == "active"
    }));
}

#[tokio::test]
async fn mapi_over_http_public_folder_move_message_moves_canonical_post_item() {
    let source_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let target_id = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![
            FakeStore::public_folder(source_id, None, "Source Public"),
            FakeStore::public_folder(target_id, None, "Target Public"),
        ])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            source_id,
            "Moved public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let deleted_items = store.deleted_public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let source_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(source_id).unwrap()).unwrap();
    let target_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(target_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, source_mapi_id);
    append_rop_open_folder(&mut rops, 0, 2, target_mapi_id);
    rops.extend_from_slice(&[
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, move.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, item_mapi_id);
    rops.push(0);
    rops.push(0);

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
        &[0x33, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
    ));
    let items = items.lock().unwrap();
    assert_eq!(items.len(), 2);
    assert!(items.iter().any(|item| {
        item.id == Uuid::parse_str(item_id).unwrap()
            && item.public_folder_id == Uuid::parse_str(source_id).unwrap()
            && item.lifecycle_state == "deleted"
    }));
    assert!(items.iter().any(|item| {
        item.public_folder_id == Uuid::parse_str(target_id).unwrap()
            && item.subject == "Moved public post"
            && item.lifecycle_state == "active"
    }));
    assert_eq!(
        deleted_items.lock().unwrap().as_slice(),
        &[Uuid::parse_str(item_id).unwrap()]
    );
}

#[tokio::test]
async fn mapi_over_http_public_folder_sync_import_updates_canonical_post_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "23010000-0000-0000-0000-000000000000";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
            item_id,
            root_id,
            "Old imported public post",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let item_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(item_id).unwrap()).unwrap();
    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(item_mapi_id),
    );
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Imported public post");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Imported public body");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
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
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    let items = items.lock().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].subject, "Imported public post");
    assert_eq!(items[0].body_text, "Imported public body");
    assert_eq!(items[0].change_counter, 2);
}

#[tokio::test]
async fn mapi_over_http_public_folder_sync_import_creates_canonical_post_item() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let items = store.public_folder_items.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let imported_mapi_id = test_mapi_folder_id(0x241);
    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SOURCE_KEY,
        &mapi_mailstore::source_key_for_store_id(imported_mapi_id),
    );
    append_mapi_utf16_property(&mut property_values, 0x001A_001F, "IPM.Post");
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "Imported new public post",
    );
    append_mapi_utf16_property(
        &mut property_values,
        0x1000_001F,
        "Imported new public body",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.push(0);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x03, 0x00, 0x00, 0x00, 0x00, 0x03]
    ));
    let items = items.lock().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].public_folder_id, Uuid::parse_str(root_id).unwrap());
    assert_eq!(items[0].message_class, "IPM.Post");
    assert_eq!(items[0].subject, "Imported new public post");
    assert_eq!(items[0].body_text, "Imported new public body");
}

#[tokio::test]
async fn mapi_over_http_public_folder_per_user_lookup_returns_canonical_folder_identity() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let root_long_term_id = crate::mapi::identity::long_term_id_from_object_id(root_mapi_id)
        .expect("public folder MAPI identity should have a LongTermID");
    let mut rops = vec![0x60, 0x00, 0x00]; // RopGetPerUserLongTermIds.
    rops.extend_from_slice(&crate::mapi::identity::STORE_REPLICA_GUID);
    rops.extend_from_slice(&[0x61, 0x00, 0x00]); // RopGetPerUserGuid.
    rops.extend_from_slice(&root_long_term_id);

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
    let response_rops = response_rops_from_execute_response(response).await;
    let mut long_term_ids_response = vec![0x60, 0x00, 0, 0, 0, 0];
    long_term_ids_response.extend_from_slice(&1u16.to_le_bytes());
    long_term_ids_response.extend_from_slice(&root_long_term_id);
    assert!(contains_bytes(&response_rops, &long_term_ids_response));
    let mut guid_response = vec![0x61, 0x00, 0, 0, 0, 0];
    guid_response.extend_from_slice(&crate::mapi::identity::STORE_REPLICA_GUID);
    assert!(contains_bytes(&response_rops, &guid_response));
}

#[tokio::test]
async fn mapi_over_http_public_folder_per_user_information_round_trips_canonical_read_state() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let items = Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
        item_id,
        root_id,
        "Read state post",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: items.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let root_long_term_id = crate::mapi::identity::long_term_id_from_object_id(root_mapi_id)
        .expect("public folder MAPI identity should have a LongTermID");
    let mut read_rops = vec![0x63, 0x00, 0x00];
    read_rops.extend_from_slice(&root_long_term_id);
    read_rops.push(0);
    read_rops.extend_from_slice(&0u32.to_le_bytes());
    read_rops.extend_from_slice(&1024u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let read_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&read_rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(read_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&read_response);
    let read_response_rops = response_rops_from_execute_response(read_response).await;
    assert!(contains_bytes(&read_response_rops, b"LPEPFU1\0"));
    let mut empty_per_user_stream = b"LPEPFU1\0".to_vec();
    empty_per_user_stream.extend_from_slice(&0u16.to_le_bytes());
    assert!(contains_bytes(&read_response_rops, &empty_per_user_stream));

    let mut per_user_stream = Vec::new();
    per_user_stream.extend_from_slice(b"LPEPFU1\0");
    per_user_stream.extend_from_slice(&1u16.to_le_bytes());
    per_user_stream.extend_from_slice(Uuid::parse_str(item_id).unwrap().as_bytes());
    per_user_stream.push(1);
    per_user_stream.extend_from_slice(&1i64.to_le_bytes());
    let mut write_rops = vec![0x64, 0x00, 0x00];
    write_rops.extend_from_slice(&root_long_term_id);
    write_rops.push(1);
    write_rops.extend_from_slice(&0u32.to_le_bytes());
    write_rops.extend_from_slice(&(per_user_stream.len() as u16).to_le_bytes());
    write_rops.extend_from_slice(&per_user_stream);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let write_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&write_rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(write_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&write_response);
    let write_response_rops = response_rops_from_execute_response(write_response).await;
    assert!(contains_bytes(
        &write_response_rops,
        &[0x64, 0x00, 0, 0, 0, 0]
    ));
    assert!(items.lock().unwrap()[0].is_read);

    let mut read_after_write_rops = vec![0x63, 0x00, 0x00];
    read_after_write_rops.extend_from_slice(&root_long_term_id);
    read_after_write_rops.push(0);
    read_after_write_rops.extend_from_slice(&0u32.to_le_bytes());
    read_after_write_rops.extend_from_slice(&1024u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let read_after_write_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&read_after_write_rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(read_after_write_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&read_after_write_response);
    let read_after_write_response_rops =
        response_rops_from_execute_response(read_after_write_response).await;
    assert!(contains_bytes(
        &read_after_write_response_rops,
        Uuid::parse_str(item_id).unwrap().as_bytes()
    ));

    items.lock().unwrap()[0].lifecycle_state = "deleted".to_string();
    let mut write_deleted_rops = vec![0x64, 0x00, 0x00];
    write_deleted_rops.extend_from_slice(&root_long_term_id);
    write_deleted_rops.push(1);
    write_deleted_rops.extend_from_slice(&0u32.to_le_bytes());
    write_deleted_rops.extend_from_slice(&(per_user_stream.len() as u16).to_le_bytes());
    write_deleted_rops.extend_from_slice(&per_user_stream);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let write_deleted_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&write_deleted_rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(write_deleted_response.status(), StatusCode::OK);
    let write_deleted_response_rops =
        response_rops_from_execute_response(write_deleted_response).await;
    let mut invalid_parameter = vec![0x64, 0x00];
    invalid_parameter.extend_from_slice(&0x8007_0057u32.to_le_bytes());
    assert!(contains_bytes(
        &write_deleted_response_rops,
        &invalid_parameter
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_per_user_information_rejects_exchange_blob_without_state_change(
) {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let item_id = "cccccccc-dddd-eeee-ffff-000000000000";
    let items = Arc::new(Mutex::new(vec![FakeStore::public_folder_item(
        item_id,
        root_id,
        "Read state post",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_items: items.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let root_long_term_id = crate::mapi::identity::long_term_id_from_object_id(root_mapi_id)
        .expect("public folder MAPI identity should have a LongTermID");
    let exchange_blob = b"\x01\x00\x00\x00ExchangePerUserBlob";
    let mut write_rops = vec![0x64, 0x00, 0x00];
    write_rops.extend_from_slice(&root_long_term_id);
    write_rops.push(1);
    write_rops.extend_from_slice(&0u32.to_le_bytes());
    write_rops.extend_from_slice(&(exchange_blob.len() as u16).to_le_bytes());
    write_rops.extend_from_slice(exchange_blob);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let write_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&write_rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(write_response.status(), StatusCode::OK);
    let write_response_rops = response_rops_from_execute_response(write_response).await;
    let mut invalid_parameter = vec![0x64, 0x00];
    invalid_parameter.extend_from_slice(&0x8007_0057u32.to_le_bytes());
    assert!(contains_bytes(&write_response_rops, &invalid_parameter));
    assert!(!items.lock().unwrap()[0].is_read);
}

#[tokio::test]
async fn mapi_over_http_public_folder_is_ghosted_validates_canonical_folder() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_replicas: Arc::new(Mutex::new(vec![FakeStore::public_folder_replica(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            root_id,
            "LPE-MBX-01",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[0x42, 0x00, 0x00]);
    append_mapi_wire_id(&mut rops, root_mapi_id);
    rops.extend_from_slice(&[0x45, 0x00, 0x00]);
    append_mapi_wire_id(&mut rops, root_mapi_id);
    rops.extend_from_slice(&[0x45, 0x00, 0x00]);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(0x7fff));

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
    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x42, 0x00, 0, 0, 0, 0, 1, 0]
    ));
    assert!(contains_bytes(&response_rops, b"LPE-MBX-01\0"));
    assert!(contains_bytes(&response_rops, &[0x45, 0x00, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x45, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_get_owning_servers_uses_ordered_canonical_replicas() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let mut replica_two = FakeStore::public_folder_replica(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        root_id,
        "LPE-MBX-02",
    );
    replica_two.sort_order = 20;
    let mut replica_one = FakeStore::public_folder_replica(
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
        root_id,
        "LPE-MBX-01",
    );
    replica_one.sort_order = 10;
    let mut replica_zero = FakeStore::public_folder_replica(
        "dddddddd-dddd-dddd-dddd-dddddddddddd",
        root_id,
        "LPE-MBX-00",
    );
    replica_zero.sort_order = 10;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        public_folder_replicas: Arc::new(Mutex::new(vec![replica_two, replica_one, replica_zero])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x00]; // Public-folder RopLogon.
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut hierarchy_rops = vec![
        0x04, 0x00, 0x00, 0x01, 0x04, // RopGetHierarchyTable on public logon.
        0x12, 0x00, 0x01, 0x00, // RopSetColumns
    ];
    hierarchy_rops.extend_from_slice(&1u16.to_le_bytes());
    hierarchy_rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    hierarchy_rops.extend_from_slice(&[
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows
    ]);
    hierarchy_rops.extend_from_slice(&10u16.to_le_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(hierarchy_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&hierarchy_response);

    let root_mapi_id =
        crate::mapi::identity::mapped_mapi_object_id(&Uuid::parse_str(root_id).unwrap()).unwrap();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, root_mapi_id);
    rops.extend_from_slice(&[0x42, 0x00, 0x00]);
    append_mapi_wire_id(&mut rops, root_mapi_id);

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
    assert!(contains_bytes(
        &response_rops,
        &[0x42, 0x00, 0, 0, 0, 0, 3, 0, 3, 0]
    ));
    let first = response_rops
        .windows(b"LPE-MBX-00\0".len())
        .position(|window| window == b"LPE-MBX-00\0")
        .unwrap();
    let second = response_rops
        .windows(b"LPE-MBX-01\0".len())
        .position(|window| window == b"LPE-MBX-01\0")
        .unwrap();
    let third = response_rops
        .windows(b"LPE-MBX-02\0".len())
        .position(|window| window == b"LPE-MBX-02\0")
        .unwrap();
    assert!(first < second && second < third);
}

#[tokio::test]
async fn mapi_over_http_private_logon_per_user_lookup_returns_public_folder_identity() {
    let root_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        public_folders: Arc::new(Mutex::new(vec![FakeStore::public_folder(
            root_id,
            None,
            "Public Root",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x01]; // Private-mailbox RopLogon.
    let legacy_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}\0",
        FakeStore::account().email
    );
    logon_rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rops.extend_from_slice(legacy_dn.as_bytes());
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    let cookie = mapi_cookie_header(&logon_response);

    let mut rops = vec![0x60, 0x00, 0x00]; // RopGetPerUserLongTermIds on private logon.
    rops.extend_from_slice(&crate::mapi::identity::STORE_REPLICA_GUID);

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
    let cookie = mapi_cookie_header(&response);
    let response_rops = response_rops_from_execute_response(response).await;
    let mut long_term_ids_response = vec![0x60, 0x00, 0, 0, 0, 0];
    long_term_ids_response.extend_from_slice(&1u16.to_le_bytes());
    assert!(contains_bytes(&response_rops, &long_term_ids_response));
    let root_long_term_id: [u8; 24] = response_rops[8..32].try_into().unwrap();

    let mut rops = vec![0x61, 0x00, 0x00]; // RopGetPerUserGuid on private logon.
    rops.extend_from_slice(&root_long_term_id);
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
    let response_rops = response_rops_from_execute_response(response).await;
    let mut guid_response = vec![0x61, 0x00, 0, 0, 0, 0];
    guid_response.extend_from_slice(&crate::mapi::identity::STORE_REPLICA_GUID);
    assert!(contains_bytes(&response_rops, &guid_response));
}
