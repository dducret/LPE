use super::*;

#[tokio::test]
async fn mapi_over_http_extended_execute_release_keeps_handle_table() {
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
            &execute_body(&rpc_proxy_wrapped_rop_buffer(&[0x01, 0x00, 0x00], &[1])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    assert_eq!(rop_buffer_size, 14);
    assert_eq!(&body[16..24], &[0, 0, 4, 0, 6, 0, 6, 0]);
    assert_eq!(&body[24..26], &[2, 0]);
    assert_eq!(&body[26..30], &u32::MAX.to_le_bytes());
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];

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
            response_rop_buffer[2 + response_rop_size..6 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rop_buffer[6 + response_rop_size..10 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        2
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rop_buffer[10 + response_rop_size..14 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        3
    );
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];

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
async fn mapi_over_http_findrow_rejects_invalid_microsoft_find_row_flags() {
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
    let restriction = mapi_content_restriction(0x3001_001F, "Top of Information Store");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x02, // RopFindRow with invalid FindRowFlags
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
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_query_rows_lists_root_hierarchy_without_ipm_children() {
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
    let query_offset = 8 + 10 + 7;

    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        13
    );
    assert!(contains_bytes(
        response_rops,
        &utf16z("Top of Information Store")
    ));
    assert!(contains_bytes(response_rops, &utf16z("Common Views")));
    assert!(!contains_bytes(response_rops, &utf16z("Inbox")));
    assert!(!contains_bytes(response_rops, &utf16z("Archive")));
}

#[tokio::test]
async fn mapi_over_http_query_rows_rejects_invalid_microsoft_forward_read_value() {
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
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x02, // RopQueryRows with invalid ForwardRead
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_seek_row_rejects_invalid_microsoft_origin_value() {
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
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x18, 0x00, 0x02, 0x03, // RopSeekRow with invalid Origin
    ]);
    rops.extend_from_slice(&0i32.to_le_bytes());
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x18, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_sort_table_rejects_hierarchy_tables() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "custom",
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
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
        &[0x13, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_sort_table_rejects_misplaced_maximum_category() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-4888-8888-888888888889",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "MaximumCategory validation",
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
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes()); // SortOrderCount
    rops.extend_from_slice(&0u16.to_le_bytes()); // CategoryCount
    rops.extend_from_slice(&0u16.to_le_bytes()); // ExpandedCount
    rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&0x0E06_0040u32.to_le_bytes());
    rops.push(0x04); // MaximumCategory without a preceding category.
    rops.extend_from_slice(&[
        0x16, 0x00, 0x02, // RopGetStatus proves the batch stayed aligned.
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
        &[0x13, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x16, 0x02, 0, 0, 0, 0, 0]));
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];

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
async fn mapi_over_http_microsoft_reset_table_requires_new_set_columns() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            {
                let mut email = FakeStore::email(
                    "81818181-8181-8181-8181-818181818181",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "First page message",
                );
                email.received_at = "2026-05-03T12:00:00Z".to_string();
                email
            },
            {
                let mut email = FakeStore::email(
                    "82828282-8282-8282-8282-828282828282",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Second page message",
                );
                email.received_at = "2026-05-03T11:00:00Z".to_string();
                email
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];
    let query_offsets = response_rops
        .windows(6)
        .enumerate()
        .filter_map(|(offset, window)| (window == [0x15, 0x02, 0, 0, 0, 0]).then_some(offset))
        .collect::<Vec<_>>();

    assert_eq!(query_offsets.len(), 2);
    let first_query = &response_rops[query_offsets[0]..query_offsets[1]];
    let second_query = &response_rops[query_offsets[1]..];
    assert_eq!(u16::from_le_bytes(first_query[7..9].try_into().unwrap()), 1);
    assert_eq!(
        u16::from_le_bytes(second_query[7..9].try_into().unwrap()),
        1
    );
    assert!(contains_bytes(first_query, &utf16z("First page message")));
    assert!(!contains_bytes(first_query, &utf16z("Second page message")));
    assert!(contains_bytes(second_query, &utf16z("Second page message")));
    assert!(contains_bytes(response_rops, &[0x81, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x15, 0x02, 0xB9, 0x04, 0x00, 0x00]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_query_position_reports_table_cursor() {
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
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];

    assert!(
        contains_bytes(
            response_rops,
            &[0x17, 0x02, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]
        ),
        "{response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_seek_row_fractional_moves_table_cursor() {
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
async fn mapi_over_http_seek_row_fractional_rejects_zero_denominator_without_batch_drift() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "87878787-8787-8787-8787-878787878787",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Fractional invalid",
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
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x1A, 0x00, 0x02, // RopSeekRowFractional with invalid denominator.
    ]);
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition proves the batch stayed aligned.
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
        &[0x1A, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_categorized_table_sort_query_and_expand_rows() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let mut first = FakeStore::email(
        "87878787-8787-8787-8787-878787878787",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MS-OXCTABL first",
    );
    first.categories = vec!["Project".to_string()];
    let mut second = FakeStore::email(
        "88888888-8888-8888-8888-888888888888",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MS-OXCTABL second",
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

    let folder_id = test_mapi_folder_id(5);
    let category_tag = 0x9000_001F;
    let category_id = test_category_id(folder_id, category_tag, "Project");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&3u16.to_le_bytes());
    for tag in [0x674D_0014, category_tag, PID_TAG_SUBJECT_W] {
        rops.extend_from_slice(&tag.to_le_bytes());
    }
    rops.extend_from_slice(&[0x13, 0x00, 0x02, 0x00]); // RopSortTable
    rops.extend_from_slice(&2u16.to_le_bytes()); // SortOrderCount
    rops.extend_from_slice(&1u16.to_le_bytes()); // CategoryCount
    rops.extend_from_slice(&0u16.to_le_bytes()); // ExpandedCount: collapsed category headers only.
    rops.extend_from_slice(&category_tag.to_le_bytes());
    rops.push(0); // TABLE_SORT_ASCEND
    rops.extend_from_slice(&0x0E06_0040u32.to_le_bytes());
    rops.push(1); // TABLE_SORT_DESCEND
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows
    rops.extend_from_slice(&50u16.to_le_bytes());
    rops.extend_from_slice(&[0x59, 0x00, 0x02]); // RopExpandRow
    rops.extend_from_slice(&0u16.to_le_bytes()); // MaxRowCount: expand but return no rows.
    rops.extend_from_slice(&category_id.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows
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
    assert!(
        contains_bytes(&response_rops, &[0x13, 0x02, 0, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0x01, 0]),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(&response_rops, &utf16z("Project")));
    assert!(
        contains_bytes(&response_rops, &[0x59, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0x02, 0]),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(&response_rops, &utf16z("MS-OXCTABL first")));
    assert!(contains_bytes(&response_rops, &utf16z("MS-OXCTABL second")));
}

#[tokio::test]
async fn mapi_over_http_deleted_items_mixed_categorized_sort_query_rows() {
    let account = FakeStore::account();
    let trash_id = Uuid::parse_str("70707070-7070-4070-8070-707070707071").unwrap();
    let mail_id = Uuid::parse_str("71717171-7171-4171-8171-717171717172").unwrap();
    let event_id = Uuid::parse_str("72727272-7272-4272-8272-727272727273").unwrap();
    let event_mapi_id = 0x0000_0000_0050_0001;
    let mut trash = FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items");
    trash.total_emails = 1;
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![trash])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &mail_id.to_string(),
            &trash_id.to_string(),
            "trash",
            "Deleted mail",
        )])),
        deleted_calendar_events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-07-17".to_string(),
            time: "09:00".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Deleted appointment".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(event_id, event_mapi_id)]))),
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

    // A categorized Deleted Items view must keep mail and appointments in the
    // same MS-OXCTABL row space. Grouping by MessageClass makes both canonical
    // object types observable without relying on trace-specific subjects.
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    let columns: [u32; 7] = [
        0x674D_0014, // PidTagInstID
        0x674E_0003, // PidTagInstanceNum
        0x0FF5_0003, // PidTagRowType
        0x3005_0003, // PidTagDepth
        PID_TAG_CONTENT_COUNT,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_SUBJECT_W,
    ];
    rops.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for property_tag in columns {
        rops.extend_from_slice(&property_tag.to_le_bytes());
    }
    rops.extend_from_slice(&[0x13, 0x00, 0x02, 0x00]); // RopSortTable
    rops.extend_from_slice(&2u16.to_le_bytes()); // SortOrderCount
    rops.extend_from_slice(&1u16.to_le_bytes()); // CategoryCount
    rops.extend_from_slice(&1u16.to_le_bytes()); // ExpandedCount
    rops.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    rops.push(0); // TABLE_SORT_ASCEND
    rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    rops.push(0); // TABLE_SORT_ASCEND
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows
    rops.extend_from_slice(&50u16.to_le_bytes());

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
        &[0x05, 0x02, 0, 0, 0, 0, 2, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x13, 0x02, 0, 0, 0, 0, 0]));
    // Two expanded categories produce two headers plus two leaf rows.
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0x04, 0]
    ));
    assert_eq!(
        response_rops
            .windows(utf16z("IPM.Appointment").len())
            .filter(|window| *window == utf16z("IPM.Appointment"))
            .count(),
        2
    );
    assert_eq!(
        response_rops
            .windows(utf16z("IPM.Note").len())
            .filter(|window| *window == utf16z("IPM.Note"))
            .count(),
        2
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Deleted appointment")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Deleted mail")));
}

#[tokio::test]
async fn mapi_over_http_deleted_items_mixed_offset_page_keeps_global_sort_order() {
    let account = FakeStore::account();
    let trash_id = Uuid::parse_str("73737373-7373-4373-8373-737373737373").unwrap();
    let event_id = Uuid::parse_str("74747474-7474-4474-8474-747474747474").unwrap();
    let event_mapi_id = 0x0000_0000_0051_0001;
    let mut trash = FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items");
    trash.total_emails = 3;
    let emails = [
        ("75757575-7575-4575-8575-757575757571", "Alpha mail"),
        ("75757575-7575-4575-8575-757575757572", "Delta mail"),
        ("75757575-7575-4575-8575-757575757573", "Echo mail"),
    ]
    .into_iter()
    .map(|(id, subject)| FakeStore::email(id, &trash_id.to_string(), "trash", subject))
    .collect();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![trash])),
        emails: Arc::new(Mutex::new(emails)),
        deleted_calendar_events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-07-17".to_string(),
            time: "09:00".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Bravo appointment".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(event_id, event_mapi_id)]))),
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    rops.extend_from_slice(&[0x13, 0x00, 0x02, 0x00]); // RopSortTable
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x18, 0x00, 0x02, 0x00]); // RopSeekRow, beginning + 2
    rops.extend_from_slice(&2i32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows
    rops.extend_from_slice(&1u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &utf16z("Delta mail")));
    assert!(!contains_bytes(&response_rops, &utf16z("Alpha mail")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Bravo appointment")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Echo mail")));
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
    let response_rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size =
        u16::from_le_bytes(response_rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &response_rop_buffer[2..2 + response_rop_size];

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
async fn mapi_over_http_microsoft_oxctabl_4_1_to_4_4_contents_table_setcolumns_sort_query_rows() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let mut older = FakeStore::email(
        "89898989-8989-8989-8989-898989898989",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Older delivery",
    );
    older.received_at = "2026-05-03T09:00:00Z".to_string();
    let mut newer = FakeStore::email(
        "90909090-9090-9090-9090-909090909090",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Newer delivery",
    );
    newer.received_at = "2026-05-03T12:00:00Z".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![older, newer])),
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
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    for column in [
        0x6748_0014u32, // PidTagFolderId
        0x674A_0014u32, // PidTagMid
        0x674D_0014u32, // PidTagInstID
        0x674E_0003u32, // PidTagInstanceNum
        0x0037_001Fu32, // PidTagSubject
        0x0E06_0040u32, // PidTagMessageDeliveryTime
    ] {
        if column == 0x6748_0014 {
            rops.extend_from_slice(&6u16.to_le_bytes());
        }
        rops.extend_from_slice(&column.to_le_bytes());
    }
    rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0x0E06_0040u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let newer_subject = utf16z("Newer delivery");
    let older_subject = utf16z("Older delivery");
    let newer_offset = response_rops
        .windows(newer_subject.len())
        .position(|window| window == newer_subject)
        .unwrap();
    let older_offset = response_rops
        .windows(older_subject.len())
        .position(|window| window == older_subject)
        .unwrap();

    assert!(contains_bytes(
        &response_rops,
        &[0x05, 0x02, 0, 0, 0, 0, 2, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x13, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
    assert!(newer_offset < older_offset);
}

#[tokio::test]
async fn mapi_over_http_query_rows_reads_backward_from_table_position() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            {
                let mut email = FakeStore::email(
                    "91919191-9191-9191-9191-919191919191",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Backward first",
                );
                email.received_at = "2026-05-03T12:00:00Z".to_string();
                email
            },
            {
                let mut email = FakeStore::email(
                    "92929292-9292-9292-9292-929292929292",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Backward second",
                );
                email.received_at = "2026-05-03T11:00:00Z".to_string();
                email
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
        .windows(6)
        .enumerate()
        .filter_map(|(offset, window)| (window == [0x15, 0x02, 0, 0, 0, 0]).then_some(offset))
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
    let restriction = mapi_content_restriction(0x0037_001F, "Needle target");

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
async fn mapi_over_http_microsoft_comment_restriction_wraps_find_row_predicate() {
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
                "Comment first",
            ),
            FakeStore::email(
                "97979797-9797-9797-9797-979797979797",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Needle comment target",
            ),
            FakeStore::email(
                "98989898-9898-9898-9898-989898989898",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Comment last",
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

    let inner_restriction = mapi_content_restriction(0x0037_001F, "Needle comment target");
    let mut restriction = vec![0x0A, 0x01];
    append_mapi_utf16_property(&mut restriction, 0x3001_001F, "MS-OXCDATA comment");
    restriction.push(0x01);
    restriction.extend_from_slice(&inner_restriction);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x4F, 0x00, 0x02, 0x00]); // RopFindRow
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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Comment first")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Needle comment target")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Comment last")));
}

#[tokio::test]
async fn mapi_over_http_microsoft_compare_properties_restriction_filters_find_row() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let mut older = FakeStore::email(
        "96969696-9696-9696-9696-969696969696",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Compare first",
    );
    older.sent_at = Some("2026-05-03T11:59:00Z".to_string());
    let mut target = FakeStore::email(
        "97979797-9797-9797-9797-979797979797",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Needle compare target",
    );
    target.sent_at = Some(target.received_at.clone());
    let missing_submit_time = FakeStore::email(
        "98989898-9898-9898-9898-989898989898",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Compare last",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![older, target, missing_submit_time])),
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

    let mut restriction = vec![0x05, 0x04]; // RES_COMPAREPROPS, RELOP_EQ.
    restriction.extend_from_slice(&0x0E06_0040u32.to_le_bytes()); // PidTagMessageDeliveryTime.
    restriction.extend_from_slice(&0x0039_0040u32.to_le_bytes()); // PidTagClientSubmitTime.

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x4F, 0x00, 0x02, 0x00]); // RopFindRow
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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Compare first")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Needle compare target")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Compare last")));
}

#[tokio::test]
async fn mapi_over_http_microsoft_count_restriction_wraps_find_row_predicate() {
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
                "Count first",
            ),
            FakeStore::email(
                "97979797-9797-9797-9797-979797979797",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Needle count target",
            ),
            FakeStore::email(
                "98989898-9898-9898-9898-989898989898",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Count last",
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

    let inner_restriction = mapi_content_restriction(0x0037_001F, "Needle count target");
    let mut restriction = vec![0x0B]; // RES_COUNT.
    restriction.extend_from_slice(&1u32.to_le_bytes());
    restriction.extend_from_slice(&inner_restriction);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x4F, 0x00, 0x02, 0x00]); // RopFindRow
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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Count first")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Needle count target")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Count last")));
}

#[tokio::test]
async fn mapi_over_http_microsoft_count_restriction_limits_query_rows() {
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
                "Count query first",
            ),
            FakeStore::email(
                "97979797-9797-9797-9797-979797979797",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Count query second",
            ),
            FakeStore::email(
                "98989898-9898-9898-9898-989898989898",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Count query last",
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

    let mut first_match = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    first_match.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    append_mapi_utf16_property(&mut first_match, 0x0037_001F, "Count query first");
    let mut second_match = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    second_match.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    append_mapi_utf16_property(&mut second_match, 0x0037_001F, "Count query second");
    let mut inner_restriction = vec![0x01]; // RES_OR.
    inner_restriction.extend_from_slice(&2u16.to_le_bytes());
    inner_restriction.extend_from_slice(&first_match);
    inner_restriction.extend_from_slice(&second_match);

    let mut restriction = vec![0x0B]; // RES_COUNT.
    restriction.extend_from_slice(&1u32.to_le_bytes());
    restriction.extend_from_slice(&inner_restriction);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]); // RopRestrict
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x14, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &utf16z("Count query first")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Count query second")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Count query last")));
}

#[tokio::test]
async fn mapi_over_http_microsoft_conversation_members_table_filters_root_messages() {
    let conversation_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let other_conversation_id = Uuid::parse_str("99999999-8888-7777-6666-555555555555").unwrap();
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let sent = FakeStore::mailbox("77777777-7777-7777-7777-777777777777", "sent", "Sent");
    let mut first = FakeStore::email(
        "11111111-1111-1111-1111-111111111111",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Conversation root message",
    );
    first.thread_id = conversation_id;
    let mut second = FakeStore::email(
        "22222222-2222-2222-2222-222222222222",
        "77777777-7777-7777-7777-777777777777",
        "sent",
        "Conversation sent reply",
    );
    second.thread_id = conversation_id;
    let mut unrelated = FakeStore::email(
        "33333333-3333-3333-3333-333333333333",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Unrelated conversation",
    );
    unrelated.thread_id = other_conversation_id;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
        emails: Arc::new(Mutex::new(vec![first, second, unrelated])),
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

    let mut conversation_index = vec![0x01, 0, 0, 0, 0, 0];
    conversation_index.extend_from_slice(conversation_id.as_bytes());
    let mut restriction = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    restriction.extend_from_slice(&0x0071_0102u32.to_le_bytes()); // PidTagConversationIndex.
    append_mapi_binary_property(&mut restriction, 0x0071_0102, &conversation_index);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02,
        0xC8, // RopGetContentsTable ConversationMembers|UseUnicode|DeferredErrors.
        0x12, 0x00, 0x02, 0x00, // RopSetColumns.
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes()); // PidTagSubject.
    rops.extend_from_slice(&0x0071_0102u32.to_le_bytes()); // PidTagConversationIndex.
    rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]); // RopRestrict.
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, 0x05, 0x00, // RopQueryRows, forward, 5 rows.
    ]);

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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x05, 0x02, 0, 0, 0, 0, 3, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Conversation root message")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Conversation sent reply")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Unrelated conversation")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_subrestriction_matches_message_attachments() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let first_id = Uuid::parse_str("96969696-9696-9696-9696-969696969696").unwrap();
    let target_id = Uuid::parse_str("97979797-9797-9797-9797-979797979797").unwrap();
    let last_id = Uuid::parse_str("98989898-9898-9898-9898-989898989898").unwrap();
    let mut first = FakeStore::email(
        &first_id.to_string(),
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment subrestriction first",
    );
    first.has_attachments = true;
    let mut target = FakeStore::email(
        &target_id.to_string(),
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Needle attachment target",
    );
    target.has_attachments = true;
    let last = FakeStore::email(
        &last_id.to_string(),
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment subrestriction last",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![first, target, last])),
        attachments: Arc::new(Mutex::new(HashMap::from([
            (
                first_id,
                vec![ActiveSyncAttachment {
                    id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap(),
                    message_id: first_id,
                    file_name: "other-report.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    disposition: Some("attachment".to_string()),
                    content_id: None,
                    size_octets: 128,
                    file_reference: "attachment:first:other-report.pdf".to_string(),
                }],
            ),
            (
                target_id,
                vec![ActiveSyncAttachment {
                    id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap(),
                    message_id: target_id,
                    file_name: "target-report.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    disposition: Some("attachment".to_string()),
                    content_id: None,
                    size_octets: 256,
                    file_reference: "attachment:target:target-report.pdf".to_string(),
                }],
            ),
        ]))),
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

    let mut restriction = vec![0x09]; // RES_SUBRESTRICTION.
    restriction.extend_from_slice(&0x0E13_000Du32.to_le_bytes()); // PidTagMessageAttachments.
    restriction.extend_from_slice(&[0x04, 0x04]); // RES_PROPERTY, RELOP_EQ.
    restriction.extend_from_slice(&0x3707_001Fu32.to_le_bytes()); // PidTagAttachLongFilename.
    append_mapi_utf16_property(&mut restriction, 0x3707_001F, "target-report.pdf");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x4F, 0x00, 0x02, 0x00]); // RopFindRow
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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Attachment subrestriction first")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Needle attachment target")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Attachment subrestriction last")
    ));
}

#[tokio::test]
async fn mapi_over_http_expand_row_on_folder_cannot_delete_messages() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let message_id = Uuid::parse_str("abababab-abab-4aba-8bab-abababababab").unwrap();
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
            "ExpandRow must not delete",
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
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[0x59, 0x00, 0x01]);
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
        &[0x59, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(moved_emails.lock().unwrap().is_empty());
    assert!(deleted_emails.lock().unwrap().is_empty());
    assert!(canonical_emails
        .lock()
        .unwrap()
        .iter()
        .any(|email| email.id == message_id));
}

#[tokio::test]
async fn mapi_over_http_get_contents_table_rejects_soft_deletes_flag() {
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
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x20, // RopGetContentsTable SoftDeletes
    ]);

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
        &[0x05, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_contents_table_rejects_invalid_microsoft_table_flags_without_batch_drift(
) {
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x01, // RopGetContentsTable with invalid TableFlags bit.
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ]);

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
        &[0x05, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_contents_table_requires_microsoft_folder_handle_without_batch_drift() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "89898989-8989-8989-8989-898989898989";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "Contents table object validation",
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::INBOX_FOLDER_ID,
        test_mapi_message_id(message_id),
    );
    rops.extend_from_slice(&[
        0x05, 0x00, 0x02, 0x03, 0x00, // RopGetContentsTable on a Message handle.
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ]);

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
        &[0x05, 0x03, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_hierarchy_table_rejects_invalid_microsoft_table_flags_without_batch_drift(
) {
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x01, // RopGetHierarchyTable with invalid TableFlags bit.
        0x7B, 0x00, 0x00, // RopGetStoreState proves the batch stayed aligned.
    ]);

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
        &[0x04, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_query_rows_uses_paged_content_table_lookup() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            {
                let mut email = FakeStore::email(
                    "87878787-8787-8787-8787-878787878787",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Paged first",
                );
                email.received_at = "2026-05-03T12:00:00Z".to_string();
                email
            },
            {
                let mut email = FakeStore::email(
                    "88888888-8888-8888-8888-888888888888",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Paged second",
                );
                email.received_at = "2026-05-03T11:00:00Z".to_string();
                email
            },
            {
                let mut email = FakeStore::email(
                    "89898989-8989-8989-8989-898989898989",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Paged third",
                );
                email.received_at = "2026-05-03T10:00:00Z".to_string();
                email
            },
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
async fn mapi_over_http_microsoft_seek_row_moves_contents_table_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            {
                let mut email = FakeStore::email(
                    "87878787-8787-8787-8787-878787878787",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Seek first",
                );
                email.received_at = "2026-05-03T12:00:00Z".to_string();
                email
            },
            {
                let mut email = FakeStore::email(
                    "88888888-8888-8888-8888-888888888888",
                    "55555555-5555-5555-5555-555555555555",
                    "inbox",
                    "Seek second",
                );
                email.received_at = "2026-05-03T11:00:00Z".to_string();
                email
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
async fn mapi_over_http_microsoft_oxcmsg_get_attachment_table_lists_canonical_attachments() {
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
                disposition: None,
                content_id: None,
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

    assert!(contains_bytes(response_rops, &[0x21, 0x03, 0, 0, 0, 0],));
    assert!(contains_bytes(response_rops, &utf16z("brief.pdf")));
    assert!(contains_bytes(response_rops, &utf16z("application/pdf")));
    assert!(contains_bytes(response_rops, &5u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_get_attachment_table_rejects_invalid_microsoft_table_flags() {
    let message_id = "30303030-3030-3030-3030-303030303030";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment table flags message",
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
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x21, 0x00, 0x02, 0x03, 0x41, // RopGetAttachmentTable with invalid TableFlags
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
    assert!(contains_bytes(
        &response_rops,
        &[0x21, 0x03, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_saved_embedded_message_reopens_from_attachment_table() {
    let message_id = "42424242-4242-4242-4242-424242424242";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Saved embedded parent",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_uuid}:{attachment_id}");
    let blob =
        b"LPE-MAPI-EMBEDDED-MESSAGE\0Subject:Saved child\r\nBody-Length:10\r\nChild body\r\nHtml-Length:0\r\n"
            .to_vec();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: message_uuid,
                file_name: "saved-child.msg".to_string(),
                media_type: "application/vnd.ms-outlook".to_string(),
                disposition: Some("attachment".to_string()),
                content_id: None,
                size_octets: blob.len() as u64,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: "saved-child.msg".to_string(),
                media_type: "application/vnd.ms-outlook".to_string(),
                blob_bytes: blob,
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
        0x21, 0x00, 0x02, 0x03, 0x00, // RopGetAttachmentTable
        0x12, 0x00, 0x03, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0E21_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3705_0003u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x03, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&10u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x22, 0x00, 0x02, 0x03, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x46, 0x00, 0x03, 0x04, // RopOpenEmbeddedMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x04, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x21, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &5u32.to_le_bytes()));
    assert!(contains_bytes(&response_rops, &[0x22, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x46, 0x04, 0, 0, 0, 0, 0]));
    assert!(open_embedded_message_response_contains_subject(
        &response_rops,
        "Saved child"
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Saved child")));
    assert!(contains_bytes(&response_rops, &utf16z("Child body")));
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
            "Archive",
            "IPF.Note",
            crate::mapi::identity::ARCHIVE_FOLDER_ID,
        ),
    ] {
        assert!(contains_bytes(&response_rops, &utf16z(name)));
        assert!(contains_bytes(&response_rops, &utf16z(class)));
        assert!(contains_bytes(
            &response_rops,
            &mapi_mailstore::source_key_for_store_id(folder_id)
        ));
    }
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Conversation History")
    ));
    for name in [
        "Conflicts",
        "Local Failures",
        "Server Failures",
        "Quick Contacts",
        "IM Contact List",
        "Conversation Action Settings",
        "Quick Step Settings",
    ] {
        assert!(!contains_bytes(&response_rops, &utf16z(name)));
    }
    assert!(!contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(
            crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID
        )
    ));
}

#[tokio::test]
async fn mapi_over_http_real_conversation_history_mailbox_stays_out_of_startup_hierarchy_table() {
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
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6748_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6749_0014u32.to_le_bytes());
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
    assert_eq!(
        u32::from_le_bytes(response_rops[14..18].try_into().unwrap()),
        OUTLOOK_IPM_HIERARCHY_TABLE_FOLDER_COUNT
    );
    let query_offset = 8 + 10 + 7;
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        OUTLOOK_IPM_HIERARCHY_TABLE_FOLDER_COUNT as u16
    );
    let source_key = mapi_mailstore::source_key_for_store_id(
        crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
    );
    assert_eq!(
        response_rops
            .windows(source_key.len())
            .filter(|window| *window == source_key.as_slice())
            .count(),
        0
    );
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Conversation History")
    ));
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
    ] {
        let (_, container_class) = rows
            .iter()
            .find(|(display_name, _)| display_name == name)
            .unwrap_or_else(|| panic!("{name} hierarchy row"));
        assert_eq!(container_class, "");
    }
    let (_, freebusy_container_class) = rows
        .iter()
        .find(|(display_name, _)| display_name == "FreeBusy Data")
        .expect("FreeBusy Data hierarchy row");
    assert_eq!(freebusy_container_class, "IPF.Note");
    let (_, shortcuts_container_class) = rows
        .iter()
        .find(|(display_name, _)| display_name == "Shortcuts")
        .expect("Shortcuts hierarchy row");
    assert_eq!(shortcuts_container_class, "IPF.ShortcutFolder");
    assert!(!contains_bytes(&response_rops, &utf16z("IPF.Root")));
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
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x6674_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6682_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6684_0102u32.to_le_bytes());
    rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&0u16.to_le_bytes());
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
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x14, 0x02, 0, 0, 0, 0, 0]));
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
async fn mapi_over_http_microsoft_table_control_rops_require_table_handles() {
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
        0x38, 0x00, 0x01, // RopAbort on the folder handle.
        0x81, 0x00, 0x01, // RopResetTable on the folder handle.
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns on the contents table handle.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x16, 0x00, 0x02, // RopGetStatus on the contents table handle.
        0x17, 0x00, 0x02, // RopQueryPosition on the contents table handle.
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows on the contents table handle.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x38, 0x00, 0x02, // RopAbort on the contents table handle.
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
        &[0x38, 0x01, 0x02, 0x01, 0x04, 0x80]
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
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x38, 0x02, 0x14, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x81, 0x02, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_table_requires_private_logon_handle() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[0x68, 0x00, 0x01]); // RopGetReceiveFolderTable on folder handle.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x68, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_known_unmodeled_table_column_type_does_not_abort_buffer() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_000Du32.to_le_bytes()); // PtypObject is known but unmodeled.
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01, 0x01, 0x00]); // RopQueryRows

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x05, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
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
async fn mapi_over_http_microsoft_set_search_criteria_reuses_previous_scope_and_restriction() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555509").unwrap();
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
            display_name: "Reuse search criteria".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({
                "kind": "mapi_bounded",
                "scope": "folders",
                "recursive": true,
                "folderIds": [inbox_id.to_string()],
                "folderRoles": ["inbox"]
            }),
            restriction_json: serde_json::json!({
                "kind": "mapi_bounded",
                "all": [{"field": "subject", "contains": "previous"}]
            }),
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

    let mut new_restriction = vec![0x00];
    new_restriction.extend_from_slice(&1u16.to_le_bytes());
    append_search_content(&mut new_restriction, 0x0037_001F, "updated");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &new_restriction, &[], 0x0000_000A);
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
                "recursive": false,
                "folderIds": [inbox_id.to_string()],
                "folderRoles": ["inbox"]
            })
        );
        assert_eq!(
            stored[0].restriction_json,
            serde_json::json!({
                "kind": "mapi_bounded",
                "all": [{"field": "subject", "contains": "updated"}]
            })
        );
    }

    renew_mapi_request_id(&mut execute_headers);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &[], &[test_mapi_folder_id(5)], 0x0000_000A);
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
            "recursive": false,
            "folderIds": [inbox_id.to_string()],
            "folderRoles": ["inbox"]
        })
    );
    assert_eq!(
        stored[0].restriction_json,
        serde_json::json!({
            "kind": "mapi_bounded",
            "all": [{"field": "subject", "contains": "updated"}]
        })
    );
}

#[tokio::test]
async fn mapi_over_http_set_search_criteria_rejects_unsupported_restriction() {
    let account = FakeStore::account();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343498").unwrap();
    let search_folder_mapi_id = test_mapi_uuid_id(&search_folder_id);
    crate::mapi::identity::remember_mapi_identity(search_folder_id, search_folder_mapi_id);
    let search_folders = Arc::new(Mutex::new(vec![SearchFolderDefinition {
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
    }]));
    let store = FakeStore {
        session: Some(account.clone()),
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

    renew_mapi_request_id(&mut execute_headers);
    let mut unsupported_not_restriction = vec![0x02];
    append_search_property_bool(&mut unsupported_not_restriction, 0x0E69_000B, 0x04, false);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &unsupported_not_restriction, &[], 0);
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

    renew_mapi_request_id(&mut execute_headers);
    let mut unsupported_size_restriction = vec![0x07, 0x03];
    unsupported_size_restriction.extend_from_slice(&0x0E08_0003u32.to_le_bytes());
    unsupported_size_restriction.extend_from_slice(&4096u32.to_le_bytes());
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &unsupported_size_restriction, &[], 0);
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

    renew_mapi_request_id(&mut execute_headers);
    let mut unsupported_recipient_restriction = Vec::new();
    append_search_content(
        &mut unsupported_recipient_restriction,
        0x0E04_001F,
        "bob@example.test",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &unsupported_recipient_restriction, &[], 0);
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

    renew_mapi_request_id(&mut execute_headers);
    let mut unsupported_category_content_restriction = Vec::new();
    append_search_content(
        &mut unsupported_category_content_restriction,
        0x9000_101F,
        "Finance",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &unsupported_category_content_restriction,
        &[],
        0,
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
        &[0x30, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));

    for unsupported_type in [0x05, 0x09, 0x0A, 0x0B] {
        renew_mapi_request_id(&mut execute_headers);
        let unsupported_restriction = vec![unsupported_type];
        let mut rops = Vec::new();
        append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
        append_rop_set_search_criteria(&mut rops, 1, &unsupported_restriction, &[], 0);
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

    renew_mapi_request_id(&mut execute_headers);
    let mut unsupported_bcc_restriction = Vec::new();
    append_search_content(
        &mut unsupported_bcc_restriction,
        0x0E02_001F,
        "secret@example.test",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &unsupported_bcc_restriction, &[], 0);
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

    renew_mapi_request_id(&mut execute_headers);
    let mut unsupported_bitmask_restriction = Vec::new();
    append_search_bitmask(
        &mut unsupported_bitmask_restriction,
        0x0E07_0003,
        true,
        0x0000_0002,
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &unsupported_bitmask_restriction, &[], 0);
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

    renew_mapi_request_id(&mut execute_headers);
    let mut unsupported_exists_restriction = Vec::new();
    append_search_exists(&mut unsupported_exists_restriction, 0x0037_001F);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &unsupported_exists_restriction, &[], 0);
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

    renew_mapi_request_id(&mut execute_headers);
    let mut trailing_bytes_restriction = Vec::new();
    append_search_property_bool(&mut trailing_bytes_restriction, 0x0E1B_000B, 0x04, true);
    trailing_bytes_restriction.extend_from_slice(&[0xEE, 0xEE]);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(&mut rops, 1, &trailing_bytes_restriction, &[], 0);
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

    let stored = search_folders.lock().unwrap();
    assert_eq!(stored[0].scope_json, serde_json::json!({}));
    assert_eq!(stored[0].restriction_json, serde_json::json!({}));
}
