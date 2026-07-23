use super::*;

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
async fn mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect_request_id = "{11111111-2222-3333-4444-555555555555}:4101";
    let client_info = "{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}:4102";
    let mut connect_headers = mapi_headers("Connect");
    connect_headers.insert("x-requestid", HeaderValue::from_static(connect_request_id));
    connect_headers.insert("x-clientinfo", HeaderValue::from_static(client_info));

    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &connect_headers, b"")
        .await
        .unwrap();

    assert_eq!(connect.status(), StatusCode::OK);
    assert_eq!(connect.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(
        connect.headers().get("x-requestid").unwrap(),
        connect_request_id
    );
    assert_eq!(connect.headers().get("x-clientinfo").unwrap(), client_info);
    assert_eq!(connect.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(connect.headers().get("x-pendingperiod").unwrap(), "15000");
    assert_eq!(
        connect.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    let set_cookies = connect
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
    let connect_cookie = mapi_cookie_header(&connect);
    let connect_raw = raw_response_bytes(connect).await;
    assert!(connect_raw.starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 0\r\n"));

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rops.extend_from_slice(legacy_dn);
    let execute_request_id = "{11111111-2222-3333-4444-555555555555}:4103";
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("x-requestid", HeaderValue::from_static(execute_request_id));
    execute_headers.insert("x-clientinfo", HeaderValue::from_static(client_info));
    execute_headers.insert("cookie", HeaderValue::from_str(&connect_cookie).unwrap());
    let execute = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rops, &[])),
        )
        .await
        .unwrap();

    assert_eq!(execute.status(), StatusCode::OK);
    assert_eq!(execute.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(
        execute.headers().get("x-requestid").unwrap(),
        execute_request_id
    );
    assert_eq!(execute.headers().get("x-clientinfo").unwrap(), client_info);
    assert_eq!(execute.headers().get("x-responsecode").unwrap(), "0");
    let execute_cookie = mapi_cookie_header(&execute);
    assert!(execute_cookie.contains("MapiContext="));
    assert!(execute_cookie.contains("MapiSequence="));
    let execute_body = response_bytes(execute).await;
    let (execute_rops, execute_handles) =
        response_rops_and_handles_from_execute_body(&execute_body);
    assert_eq!(execute_rops[0], 0xFE);
    assert_eq!(execute_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(execute_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(execute_rops[6], 0x01);
    assert_eq!(execute_handles, vec![1]);

    let reconnect_request_id = "{11111111-2222-3333-4444-555555555555}:4104";
    let mut reconnect_headers = mapi_headers("Connect");
    reconnect_headers.insert(
        "x-requestid",
        HeaderValue::from_static(reconnect_request_id),
    );
    reconnect_headers.insert("x-clientinfo", HeaderValue::from_static(client_info));
    reconnect_headers.insert("cookie", HeaderValue::from_str(&execute_cookie).unwrap());
    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_headers, b"")
        .await
        .unwrap();

    assert_eq!(reconnect.status(), StatusCode::OK);
    assert_eq!(reconnect.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(
        reconnect.headers().get("x-requestid").unwrap(),
        reconnect_request_id
    );
    assert_eq!(reconnect.headers().get("x-responsecode").unwrap(), "0");
    let reconnect_cookie = mapi_cookie_header(&reconnect);
    assert_ne!(reconnect_cookie, execute_cookie);
    assert!(raw_response_bytes(reconnect)
        .await
        .starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 0\r\n"));

    let disconnect_request_id = "{11111111-2222-3333-4444-555555555555}:4105";
    let mut disconnect_headers = mapi_headers("Disconnect");
    disconnect_headers.insert(
        "x-requestid",
        HeaderValue::from_static(disconnect_request_id),
    );
    disconnect_headers.insert("x-clientinfo", HeaderValue::from_static(client_info));
    disconnect_headers.insert("cookie", HeaderValue::from_str(&reconnect_cookie).unwrap());
    let disconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &disconnect_headers, b"")
        .await
        .unwrap();

    assert_eq!(disconnect.status(), StatusCode::OK);
    assert_eq!(
        disconnect.headers().get("x-requesttype").unwrap(),
        "Disconnect"
    );
    assert_eq!(
        disconnect.headers().get("x-requestid").unwrap(),
        disconnect_request_id
    );
    assert_eq!(
        disconnect.headers().get("x-clientinfo").unwrap(),
        client_info
    );
    assert_eq!(disconnect.headers().get("x-responsecode").unwrap(), "0");
    assert!(disconnect
        .headers()
        .get_all("set-cookie")
        .iter()
        .any(|cookie| cookie.to_str().unwrap().contains("Max-Age=0")));
    let disconnect_body = response_bytes(disconnect).await;
    assert_eq!(disconnect_body.len(), 12);
    assert_eq!(
        u32::from_le_bytes(disconnect_body[0..4].try_into().unwrap()),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_store_load_failure_after_logon_is_unknown_failure_with_session_cookies() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        fail_fetch_mapi_event_versions: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rops.extend_from_slice(legacy_dn);
    let mut logon_headers = mapi_headers("Execute");
    logon_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &logon_headers,
            &execute_body(&rop_buffer(&logon_rops, &[])),
        )
        .await
        .unwrap();
    assert_eq!(logon.headers().get("x-responsecode").unwrap(), "0");

    let mut open_folder_rops = Vec::new();
    append_rop_open_folder(
        &mut open_folder_rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    let mut open_folder_headers = mapi_headers("Execute");
    open_folder_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&logon)).unwrap(),
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &open_folder_headers,
            &execute_body(&rop_buffer(&open_folder_rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // [MS-OXCMAPIHTTP] section 2.2.3.3.3: a server-side store failure is
    // Unknown Failure (1), not Invalid Header (4).
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "1");
    assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    // [MS-OXCMAPIHTTP] sections 3.1.5.2 and 3.2.5.2 require the complete
    // Session Context cookie set on the next request and response.
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=")));
    let body = response_bytes(response).await;
    assert!(String::from_utf8(body)
        .unwrap()
        .contains("forced durable MAPI Event version load failure"));
}

#[tokio::test]
async fn mapi_over_http_malformed_execute_body_is_invalid_body_with_session_cookies() {
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

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &[0; 4])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // [MS-OXCMAPIHTTP] section 2.2.3.3.3: code 12 is Invalid Request Body.
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "12");
    assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
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
    assert!(String::from_utf8(response_bytes(response).await)
        .unwrap()
        .contains("invalid Execute request body"));
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
    // [MS-OXCMAPIHTTP] section 3.2.5.5 keeps an idle NotificationWait open
    // for up to five minutes, so this test deliberately does not await its
    // final response body.
    drop(response);
}

#[tokio::test]
async fn mapi_over_http_notification_wait_streams_processing_and_pending_frames() {
    use tokio_stream::StreamExt;

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

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        service.handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b""),
    )
    .await
    .expect("NotificationWait must return its immediate chunked response")
    .unwrap();

    // [MS-OXCMAPIHTTP] sections 2.2.7, 3.2.2, and 3.2.5.2 require a server
    // to flush PROCESSING immediately, then PENDING every X-PendingPeriod.
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "NotificationWait"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(response.headers().get("x-pendingperiod").unwrap(), "15000");
    assert_eq!(
        response.headers().get("transfer-encoding").unwrap(),
        "chunked"
    );
    assert!(response.headers().get("content-length").is_none());

    let mut frames = response.into_body().into_data_stream();
    assert_eq!(
        frames.next().await.unwrap().unwrap().as_ref(),
        b"PROCESSING\r\n"
    );

    tokio::time::sleep(Duration::from_secs(15)).await;
    let pending = tokio::time::timeout(Duration::from_secs(1), frames.next())
        .await
        .expect("NotificationWait must emit PENDING after X-PendingPeriod")
        .unwrap()
        .unwrap();
    assert_eq!(pending.as_ref(), b"PENDING\r\n");
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcmapihttp_ping_refreshes_idle_session_context() {
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
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
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
    let session_id = cookie
        .split("; ")
        .find_map(|part| part.strip_prefix("MapiContext="))
        .expect("Connect should set a MapiContext cookie")
        .to_string();
    let _active_request = crate::mapi::begin_active_session_request_for_test(&session_id);

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
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcmsg_name_to_id_mapping_works_on_message_object() {
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
    let ps_mapi_guid = [
        0x28, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let named_header = utf16z("X-LPE-Message-Name");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x56, 0x00, 0x02, 0x02, // RopGetPropertyIdsFromNames on Message, create missing.
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.push(0x00);
    rops.extend_from_slice(&ps_mapi_guid);
    rops.extend_from_slice(&0x8503u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&FAKE_PS_INTERNET_HEADERS_GUID);
    rops.push(named_header.len() as u8);
    rops.extend_from_slice(&named_header);
    rops.extend_from_slice(&[
        0x55, 0x00, 0x02, // RopGetNamesFromPropertyIds on the same Message object.
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x8503u16.to_le_bytes());
    rops.extend_from_slice(&0x9001u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x06, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x56, 0x02, 0, 0, 0, 0, 2, 0, 0x03, 0x85, 0x01, 0x90]
    ));
    assert!(contains_bytes(&response_rops, &[0x55, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("x-lpe-message-name")
    ));
}

#[tokio::test]
async fn mapi_over_http_open_attachment_rejects_invalid_microsoft_flags_without_batch_drift() {
    let message_id = "34343434-3434-3434-3434-343434343435";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Invalid attachment flag message",
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
        0x22, 0x00, 0x02, 0x03, 0x02, // RopOpenAttachment with invalid OpenAttachmentFlags.
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // RopGetStoreState proves the batch stayed aligned.

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
    assert!(contains_bytes(
        &response_rops,
        &[0x22, 0x03, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
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
async fn mapi_over_http_microsoft_set_search_criteria_rejects_invalid_search_flags() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555508").unwrap();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-343434343492").unwrap();
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
            display_name: "Invalid search flags".to_string(),
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
    for flags in [
        0x8000_0000u32, // Unknown flag bit.
        0x0000_0003,    // STOP_SEARCH and RESTART_SEARCH.
        0x0000_000C,    // RECURSIVE_SEARCH and SHALLOW_SEARCH.
        0x0003_0000,    // CONTENT_INDEXED_SEARCH and NON_CONTENT_INDEXED_SEARCH.
    ] {
        let mut rops = Vec::new();
        append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
        append_rop_set_search_criteria(
            &mut rops,
            1,
            &restriction,
            &[test_mapi_folder_id(5)],
            flags,
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
            &[0x30, 0x01, 0x57, 0x00, 0x07, 0x80]
        ));
        renew_mapi_request_id(&mut execute_headers);
    }

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
