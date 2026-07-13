use super::*;

#[tokio::test]
async fn mapi_over_http_microsoft_categorized_table_collapse_state_restores_bookmark() {
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
    rops.extend_from_slice(&0x9000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x13, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x9000_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&10u16.to_le_bytes());
    let category_id = test_category_id(test_mapi_folder_id(5), 0x9000_001F, "Project");
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
    let bookmark = 1u32.to_le_bytes();
    rops.extend_from_slice(&[0x19, 0x00, 0x02]); // RopSeekRowBookmark
    rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    rops.extend_from_slice(&bookmark);
    rops.extend_from_slice(&0i32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x89, 0x00, 0x02]); // RopFreeBookmark
    rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    rops.extend_from_slice(&bookmark);
    rops.extend_from_slice(&[0x19, 0x00, 0x02]); // RopSeekRowBookmark after RopFreeBookmark
    rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    rops.extend_from_slice(&bookmark);
    rops.extend_from_slice(&0i32.to_le_bytes());
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x13, 0x02, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
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
    assert!(contains_bytes(
        &response_rops,
        &[0x19, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x89, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x19, 0x02, 0x05, 0x04, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_table_bookmarks_restore_contents_cursor_and_free() {
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

    let mut third_rops = vec![0x19, 0x00, 0x02]; // RopSeekRowBookmark after RopFreeBookmark
    third_rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    third_rops.extend_from_slice(&bookmark);
    third_rops.extend_from_slice(&0i32.to_le_bytes());
    third_rops.push(1);

    renew_mapi_request_id(&mut execute_headers);
    let third_request = execute_body(&rop_buffer(&third_rops, &[u32::MAX, u32::MAX, 3]));
    let third_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &third_request)
        .await
        .unwrap();

    assert_eq!(third_response.status(), StatusCode::OK);
    let third_response_rops = response_rops_from_execute_response(third_response).await;
    assert!(contains_bytes(
        &third_response_rops,
        &[0x19, 0x02, 0x05, 0x04, 0x04, 0x80]
    ));
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
async fn mapi_over_http_browses_recoverable_versions_and_purges_virtual_folders() {
    let version_item = FakeStore::recoverable_item(
        "abababab-abab-abab-abab-abababababac",
        "versions",
        "Version projection subject",
    );
    let purge_item = FakeStore::recoverable_item(
        "abababab-abab-abab-abab-abababababad",
        "purges",
        "Purge projection subject",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        recoverable_items: Arc::new(Mutex::new(vec![version_item, purge_item])),
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
        crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&10u16.to_le_bytes());
    append_rop_open_folder(
        &mut rops,
        0,
        3,
        crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x05, 0x00, 0x03, 0x04, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x04, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x04, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&10u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, 3, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &utf16z("Version projection subject")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Purge projection subject")
    ));
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
async fn mapi_over_http_recoverable_copy_is_rejected_without_restore_side_effect() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let item_id = Uuid::parse_str("bebebebe-bebe-bebe-bebe-bebebebebebe").unwrap();
    let item = FakeStore::recoverable_item(&item_id.to_string(), "deletions", "Copy me");
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
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, copy
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        crate::mapi_store::mapi_recoverable_item_id(&item_id),
    );
    rops.push(0);
    rops.push(1);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, 2, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(restored.lock().unwrap().is_empty());
    assert!(contains_bytes(
        &response_rops,
        &[0x33, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
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
        0x91, 0x00, 0x01, 0x00, 0x00, // RopHardDeleteMessages
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
    assert!(contains_bytes(&response_rops, &[0x91, 0x01, 0, 0, 0, 0, 1]));
}

#[tokio::test]
async fn mapi_over_http_recoverable_delete_messages_is_bounded_rejection() {
    let item_id = Uuid::parse_str("cececece-cece-cece-cece-cececececece").unwrap();
    let item = FakeStore::recoverable_item(&item_id.to_string(), "deletions", "Soft delete held");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        recoverable_items: Arc::new(Mutex::new(vec![item])),
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
        0x1E, 0x00, 0x01, 0x00, 0x00, // RopDeleteMessages
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
    assert!(contains_bytes(&response_rops, &[0x1E, 0x01, 0, 0, 0, 0, 1]));
}

#[tokio::test]
async fn mapi_over_http_recoverable_empty_folder_reports_partial_when_retention_blocks() {
    let item_id = Uuid::parse_str("dededede-dede-dede-dede-dededededede").unwrap();
    let mut item = FakeStore::recoverable_item(&item_id.to_string(), "deletions", "Retained item");
    item.retained_until = Some("2026-06-30T00:00:00Z".to_string());
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

    assert!(purged.lock().unwrap().is_empty());
    assert!(contains_bytes(&response_rops, &[0x58, 0x01, 0, 0, 0, 0, 1]));
}

#[tokio::test]
async fn mapi_over_http_recoverable_root_empty_folder_is_parseable_not_supported() {
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
        crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
    );
    rops.extend_from_slice(&[0x92, 0x00, 0x01, 0x00, 0x00]);

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
        &[0x92, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_recoverable_root_message_mutations_are_parseable_not_supported() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let item_id = Uuid::parse_str("cfcfcfcf-cfcf-cfcf-cfcf-cfcfcfcfcfcf").unwrap();
    let item = FakeStore::recoverable_item(&item_id.to_string(), "deletions", "Root mutation");
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
        crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
    );
    append_rop_open_folder(&mut rops, 0, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x91, 0x00, 0x01, 0x00, 0x00, // RopHardDeleteMessages
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(
        &mut rops,
        crate::mapi_store::mapi_recoverable_item_id(&item_id),
    );
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

    assert!(restored.lock().unwrap().is_empty());
    assert!(purged.lock().unwrap().is_empty());
    assert!(contains_bytes(
        &response_rops,
        &[0x91, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x33, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
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
    assert!(contains_bytes(
        &response_rops,
        &[0x2A, 0x03, 0, 0, 0, 0, 0x00, 0x01, 0x01, 0x00]
    ));
}
