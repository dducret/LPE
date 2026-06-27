use super::*;

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
    props_request.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    props_request.extend_from_slice(&0x300B_0102u32.to_le_bytes());
    props_request.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    props_request.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    props_request.extend_from_slice(&0x3E04_0003u32.to_le_bytes());
    props_request.extend_from_slice(&0x8888_0003u32.to_le_bytes());
    props_request.extend_from_slice(&0x8CA8_001Eu32.to_le_bytes());
    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &props_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert!(contains_bytes(&body, b"EX:"));
    assert!(contains_bytes(&body, &0x3E04_0003u32.to_le_bytes()));
    assert!(contains_bytes(&body, &0x8888_0003u32.to_le_bytes()));
    assert!(contains_bytes(&body, &0x8CA8_001Eu32.to_le_bytes()));
    let mut search_key = format!(
        "EX:{}",
        test_contact_legacy_dn(
            "bob.contact@example.test",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        .to_ascii_uppercase()
    )
    .into_bytes();
    search_key.push(0);
    assert!(contains_bytes(&body, &search_key));
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
async fn mapi_over_http_nspi_get_props_returns_microsoft_contact_detail_columns() {
    let mut contact = FakeStore::contact(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "Bob Contact",
        "bob.contact@example.test",
    );
    contact.phone = "+41 22 555 0100".to_string();
    contact.team = "Engineering".to_string();
    contact.organization_name = "Fabrikam".to_string();
    contact.job_title = "Architect".to_string();
    contact.structured_name.given = "Bob".to_string();
    contact.structured_name.family = "Contact".to_string();
    contact.structured_name.nickname = "Bobby".to_string();
    contact.structured_name.phonetic_given = "Bahb".to_string();
    contact.structured_name.phonetic_family = "Kontaakt".to_string();
    contact.phones_json = serde_json::json!([
        {"label": "work", "phone": "+41 22 555 0100"},
        {"label": "mobile", "phone": "+41 79 555 0100"},
        {"label": "home", "phone": "+41 22 555 0199"},
        {"label": "business2", "phone": "+41 22 555 0101"}
    ]);
    contact.addresses_json = serde_json::json!([{
        "full": "1 Example Way, Geneva 1201",
        "street": "1 Example Way",
        "city": "Geneva",
        "state": "GE",
        "country": "Switzerland",
        "postalCode": "1201"
    }]);

    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![contact])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let lookup = b"bob.contact@example.test\0";
    let matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &matches_headers, lookup)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[9], 1);
    let visible_mid = u32::from_le_bytes(body[14..18].try_into().unwrap());

    let mut props_request = Vec::new();
    props_request.extend_from_slice(&visible_mid.to_le_bytes());
    for tag in [
        0x3A06_001Fu32, // PidTagGivenName
        0x3A0B_001Fu32, // PidTagSurname
        0x3A4F_001Fu32, // PidTagNickname
        0x3A08_001Fu32, // PidTagBusinessTelephoneNumber
        0x3A09_001Fu32, // PidTagHomeTelephoneNumber
        0x3A1A_001Fu32, // PidTagPrimaryTelephoneNumber
        0x3A1B_001Fu32, // PidTagBusiness2TelephoneNumber
        0x3A1C_001Fu32, // PidTagMobileTelephoneNumber
        0x3A16_001Fu32, // PidTagCompanyName
        0x3A17_001Fu32, // PidTagTitle
        0x3A18_001Fu32, // PidTagDepartmentName
        0x3A15_001Fu32, // PidTagPostalAddress
        0x3A26_001Fu32, // PidTagCountry
        0x3A27_001Fu32, // PidTagLocality
        0x3A28_001Fu32, // PidTagStateOrProvince
        0x3A29_001Fu32, // PidTagStreetAddress
        0x3A2A_001Fu32, // PidTagPostalCode
        0x3A8D_001Fu32, // PidTagAddressBookPhoneticGivenName
        0x3A8E_001Fu32, // PidTagAddressBookPhoneticSurname
    ] {
        props_request.extend_from_slice(&tag.to_le_bytes());
    }
    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &props_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;

    assert_eq!(body[12], 1);
    for value in [
        "Bob",
        "Contact",
        "Bobby",
        "+41 22 555 0100",
        "+41 22 555 0199",
        "+41 22 555 0101",
        "+41 79 555 0100",
        "Fabrikam",
        "Architect",
        "Engineering",
        "1 Example Way, Geneva 1201",
        "Switzerland",
        "Geneva",
        "GE",
        "1 Example Way",
        "1201",
        "Bahb",
        "Kontaakt",
    ] {
        assert!(contains_bytes(&body, &utf16z(value)), "missing {value}");
    }
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
async fn mapi_over_http_microsoft_oxnspi_hierarchy_and_query_rows_example_round_trips() {
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

    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    assert_eq!(bind.status(), StatusCode::OK);
    assert_eq!(bind.headers().get("x-responsecode").unwrap(), "0");
    let cookie = mapi_cookie_header(&bind);
    let bind_body = response_bytes(bind).await;
    assert_eq!(u32::from_le_bytes(bind_body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(bind_body[4..8].try_into().unwrap()), 0);
    assert_ne!(&bind_body[8..24], &[0; 16]);

    let mut special_headers = mapi_headers("GetSpecialTable");
    special_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut special_request = Vec::new();
    special_request.extend_from_slice(&0x0000_0004u32.to_le_bytes());
    special_request.extend_from_slice(&[0; 36]);
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &special_headers, &special_request)
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
    assert_eq!(body[17], 1);
    assert_eq!(u32::from_le_bytes(body[18..22].try_into().unwrap()), 4);
    for tag in [
        0x0FFF_0102u32, // PidTagEntryId
        0x3600_0003,    // PidTagContainerFlags
        0x3005_0003,    // PidTagDepth
        0xFFFD_0003,    // PidTagAddressBookContainerId
        0x3001_001F,    // PidTagDisplayName
        0xFFFB_000B,    // PidTagAddressBookIsMaster
    ] {
        assert!(contains_bytes(&body, &tag.to_le_bytes()));
    }
    assert!(contains_bytes(&body, &utf16z("Global Address List")));

    let mut query_headers = mapi_headers("QueryRows");
    query_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut query_request = Vec::new();
    query_request.extend_from_slice(&0u32.to_le_bytes());
    query_request.extend_from_slice(&[0; 36]);
    query_request.extend_from_slice(&0u32.to_le_bytes());
    query_request.extend_from_slice(&2u32.to_le_bytes());
    query_request.extend_from_slice(&0u32.to_le_bytes());
    for tag in [
        0x0FFF_0102u32, // PidTagEntryId
        0x3001_001F,    // PidTagDisplayName
        0x39FE_001F,    // PidTagSmtpAddress
        0x3A17_001F,    // PidTagTitle
    ] {
        query_request.extend_from_slice(&tag.to_le_bytes());
    }
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &query_request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(body[8], 0);
    assert_eq!(body[9], 1);
    assert_eq!(u32::from_le_bytes(body[10..14].try_into().unwrap()), 4);
    assert_eq!(
        u32::from_le_bytes(body[14..18].try_into().unwrap()),
        0x0FFF_0102
    );
    assert_eq!(
        u32::from_le_bytes(body[18..22].try_into().unwrap()),
        0x3001_001F
    );
    assert_eq!(
        u32::from_le_bytes(body[22..26].try_into().unwrap()),
        0x39FE_001F
    );
    assert_eq!(
        u32::from_le_bytes(body[26..30].try_into().unwrap()),
        0x3A17_001F
    );
    assert_eq!(u32::from_le_bytes(body[30..34].try_into().unwrap()), 2);
    assert!(contains_bytes(&body, &utf16z("Alice")));
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("Bob")));
    assert!(contains_bytes(&body, &utf16z("bob@example.test")));

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
    assert_eq!(body[29], 0xFF);
    assert_eq!(u32::from_le_bytes(body[30..34].try_into().unwrap()), 1);
    assert!(contains_bytes(&body, &utf16z("SMTP:alice@example.test")));

    let mut outlook_account_row_request = Vec::new();
    outlook_account_row_request.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    outlook_account_row_request.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    outlook_account_row_request.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    outlook_account_row_request.extend_from_slice(&0x3002_001Fu32.to_le_bytes());
    outlook_account_row_request.extend_from_slice(&0x300B_0102u32.to_le_bytes());
    outlook_account_row_request.extend_from_slice(&0x3E04_0003u32.to_le_bytes());
    outlook_account_row_request.extend_from_slice(&0x8888_0003u32.to_le_bytes());
    outlook_account_row_request.extend_from_slice(&0x800F_101Fu32.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Nspi,
            &props_headers,
            &outlook_account_row_request,
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 8);
    assert!(contains_bytes(&body, &0x3E04_0003u32.to_le_bytes()));
    assert!(contains_bytes(&body, &0x8888_0003u32.to_le_bytes()));
    assert!(contains_bytes(&body, &0x800F_101Fu32.to_le_bytes()));
    let proxy_tag_offset = body
        .windows(4)
        .position(|bytes| bytes == 0x800F_101Fu32.to_le_bytes())
        .unwrap();
    let proxy_value_offset = proxy_tag_offset + 8;
    assert_eq!(body[proxy_value_offset], 0xFF);
    assert_eq!(
        u32::from_le_bytes(
            body[proxy_value_offset + 1..proxy_value_offset + 5]
                .try_into()
                .unwrap()
        ),
        1
    );
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
        "DNToEPH",
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
                assert!(contains_bytes(&body, &utf16z("All Users")));
                assert!(contains_bytes(&body, &utf16z("All Groups")));
                assert!(contains_bytes(&body, &utf16z("All Contacts")));
                assert_eq!(
                    u32::from_le_bytes(body[18..22].try_into().unwrap()),
                    4,
                    "{request_type}"
                );
                let mut offset = 22usize;
                let container_entry_id = |dn: &[u8]| {
                    let mut value = Vec::new();
                    value.extend_from_slice(&[0, 0, 0, 0]);
                    value.extend_from_slice(&[
                        0xdc, 0xa7, 0x40, 0xc8, 0xc0, 0x42, 0x10, 0x1a, 0xb4, 0xb9, 0x08, 0x00,
                        0x2b, 0x2f, 0xe1, 0x82,
                    ]);
                    value.extend_from_slice(&1u32.to_le_bytes());
                    value.extend_from_slice(&0x0000_0100u32.to_le_bytes());
                    value.extend_from_slice(dn);
                    value
                };
                let gal_entry_id = container_entry_id(b"/\0");
                let special_rows: [(&str, &[u8], u32, u32, u32, u8, Option<&[u8]>); 4] = [
                    (
                        "Global Address List",
                        b"/\0".as_slice(),
                        0u32,
                        0u32,
                        0x0000_000B,
                        0u8,
                        Some([].as_slice()),
                    ),
                    (
                        "All Users",
                        b"/guid=741f6fd38e1a654f9d422dfb451c8f11\0".as_slice(),
                        1,
                        2,
                        0x0000_0009,
                        0,
                        Some(gal_entry_id.as_slice()),
                    ),
                    (
                        "All Groups",
                        b"/guid=741f6fd38e1a654f9d422dfb451c8f12\0".as_slice(),
                        1,
                        3,
                        0x0000_0009,
                        0,
                        Some(gal_entry_id.as_slice()),
                    ),
                    (
                        "All Contacts",
                        b"/guid=741f6fd38e1a654f9d422dfb451c8f13\0".as_slice(),
                        1,
                        4,
                        0x0000_0009,
                        0,
                        Some(gal_entry_id.as_slice()),
                    ),
                ];
                for (name, dn, depth, container_id, flags, is_master, parent_entry_id) in
                    special_rows
                {
                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0,
                        "{request_type}: {name}"
                    );
                    offset += 4;
                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        7,
                        "{request_type}: {name}"
                    );
                    offset += 4;

                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0x0FFF_0102,
                        "{request_type}: {name}"
                    );
                    offset += 8;
                    let entry_id_len =
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    assert_eq!(entry_id_len, 28 + dn.len(), "{request_type}: {name}");
                    assert_eq!(
                        &body[offset + 24..offset + 28],
                        &0x0000_0100u32.to_le_bytes(),
                        "{request_type}: {name}"
                    );
                    assert_eq!(
                        &body[offset + 28..offset + entry_id_len],
                        dn,
                        "{request_type}: {name}"
                    );
                    offset += entry_id_len;

                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0x3600_0003,
                        "{request_type}: {name}"
                    );
                    offset += 8;
                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        flags,
                        "{request_type}: {name}"
                    );
                    offset += 4;

                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0x3005_0003,
                        "{request_type}: {name}"
                    );
                    offset += 8;
                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        depth,
                        "{request_type}: {name}"
                    );
                    offset += 4;

                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0xFFFD_0003,
                        "{request_type}: {name}"
                    );
                    offset += 8;
                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        container_id,
                        "{request_type}: {name}"
                    );
                    offset += 4;

                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0x3001_001F,
                        "{request_type}: {name}"
                    );
                    offset += 8;
                    assert_eq!(body[offset], 0xFF, "{request_type}: {name}");
                    offset += 1 + utf16z(name).len();

                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0xFFFB_000B,
                        "{request_type}: {name}"
                    );
                    offset += 8;
                    assert_eq!(body[offset], is_master, "{request_type}: {name}");
                    offset += 1;

                    let parent_entry_id = parent_entry_id.unwrap();
                    assert_eq!(
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()),
                        0xFFFC_0102,
                        "{request_type}: {name}"
                    );
                    offset += 8;
                    let parent_entry_id_len =
                        u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    assert_eq!(
                        &body[offset..offset + parent_entry_id_len],
                        parent_entry_id,
                        "{request_type}: {name}"
                    );
                    offset += parent_entry_id_len;
                }
            }
            "DNToEPH" | "DNToMId" => {
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
async fn mapi_over_http_dn_to_eph_resolves_outlook_legacy_dn_to_principal() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = b"\0\0\0\0\xff\x01\0\0\0/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test\0\0\0\0\0";
    let headers = nspi_bound_headers(&service, "DNToEPH").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "DNToEPH");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(body[8], 1);
    assert_eq!(u32::from_le_bytes(body[9..13].try_into().unwrap()), 1);
    let matched_id = u32::from_le_bytes(body[13..17].try_into().unwrap());
    assert_ne!(matched_id, 0);
    assert_eq!(matched_id & 0x8000_0000, 0x8000_0000);
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
