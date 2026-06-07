use super::*;

#[tokio::test]
async fn mapi_over_http_rejects_missing_authentication() {
    let store = FakeStore::default();
    let service = ExchangeService::new(store);

    let error = service
        .handle_mapi(MapiEndpoint::Emsmdb, &HeaderMap::new(), b"")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing account authentication"));
}

#[tokio::test]
async fn rpc_proxy_challenges_missing_authentication_with_basic() {
    let store = FakeStore::default();
    let service = ExchangeService::new(store);
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();

    let response = service
        .handle_rpc_proxy(&Method::GET, &uri, &HeaderMap::new(), b"")
        .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(axum::http::header::WWW_AUTHENTICATE),
        Some(&HeaderValue::from_static("Basic realm=\"LPE RPC\""))
    );
    let body = response_text(response).await;
    assert!(body.contains("missing account authentication"));
}

#[tokio::test]
async fn rpc_proxy_challenges_anonymous_msrpch_echo_ping() {
    let store = FakeStore::default();
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service.handle_rpc_proxy(&method, &uri, &headers, b"").await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(axum::http::header::WWW_AUTHENTICATE),
        Some(&HeaderValue::from_static("Basic realm=\"LPE RPC\""))
    );
    let body = response_text(response).await;
    assert!(body.contains("missing account authentication"));
}

#[tokio::test]
async fn rpc_proxy_answers_authenticated_msrpch_echo_ping() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service.handle_rpc_proxy(&method, &uri, &headers, b"").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("echo"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.as_ref(),
        &[
            0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x40, 0x00, 0x00, 0x00
        ]
    );
}

#[tokio::test]
async fn rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6002".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("endpoint-ping"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 28);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(u16::from_le_bytes([body[18], body[19]]), 1);
    assert_eq!(
        u32::from_le_bytes([body[20], body[21], body[22], body[23]]),
        2
    );
}

#[tokio::test]
async fn rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6001".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("endpoint-ping"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 28);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
}

#[tokio::test]
async fn rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6004".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("endpoint-ping"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 72);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(body[28], 0x05);
    assert_eq!(body[30], 0x14);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
}

#[tokio::test]
async fn rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let endpoint_query = "mail.conn-b1-before-out.example.test:6004";
    let in_method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let in_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));
    let mut conn_b1 = hex_bytes(
        "0500140310000000680000000000000000000600\
         06000000010000000300000076ed340685c5dd390e9a6acbc8cb9951\
         03000000a6c4ac6df261ef9fc3804d0c73a59fff\
         040000000000004005000000e09304000c0000005475b4942dd08746bf4c3d2821816b2c",
    );
    conn_b1[32..48].copy_from_slice(&[0x11; 16]);

    let response = service
        .handle_rpc_proxy_in_data_channel(&in_method, &in_uri, &headers, Body::from(conn_b1))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );

    tokio::task::yield_now().await;

    let out_method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let out_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&out_method, &out_uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 72);
    assert_eq!(body[28], 0x05);
    assert_eq!(body[30], 0x14);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
    assert_eq!(u16::from_le_bytes([body[46], body[47]]), 3);
    assert_eq!(
        u32::from_le_bytes([body[48], body[49], body[50], body[51]]),
        6
    );
    assert_eq!(
        u32::from_le_bytes([body[60], body[61], body[62], body[63]]),
        0x0001_0000
    );
}

#[tokio::test]
async fn rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first()
{
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let endpoint_query = "mail.conn-b1-after-out.example.test:6004";
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let out_method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let out_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&out_method, &out_uri, &headers, &connect_body)
        .await;

    let in_method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let in_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let mut conn_b1 = hex_bytes(
        "0500140310000000680000000000000000000600\
         06000000010000000300000076ed340685c5dd390e9a6acbc8cb9951\
         03000000a6c4ac6df261ef9fc3804d0c73a59fff\
         040000000000004005000000e09304000c0000005475b4942dd08746bf4c3d2821816b2c",
    );
    conn_b1[32..48].copy_from_slice(&connect_body[32..48]);

    let in_response = service
        .handle_rpc_proxy_in_data_channel(&in_method, &in_uri, &headers, Body::from(conn_b1))
        .await;

    assert_eq!(in_response.status(), StatusCode::OK);
    tokio::task::yield_now().await;

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 72);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
}

#[tokio::test]
async fn rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let endpoint_query = "mail.conn-b1-before-bind.example.test:6001";
    let in_method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let in_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));
    let mut conn_b1 = hex_bytes(
        "0500140310000000680000000000000000000600\
         06000000010000000300000076ed340685c5dd390e9a6acbc8cb9951\
         03000000a6c4ac6df261ef9fc3804d0c73a59fff\
         040000000000004005000000e09304000c0000005475b4942dd08746bf4c3d2821816b2c",
    );
    conn_b1[32..48].copy_from_slice(&[0x11; 16]);

    let response = service
        .handle_rpc_proxy_in_data_channel(&in_method, &in_uri, &headers, Body::from(conn_b1))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    tokio::task::yield_now().await;

    let out_method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let out_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&out_method, &out_uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 184);
    assert_eq!(body[28], 0x05);
    assert_eq!(body[30], 0x14);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
    assert_eq!(body[72], 0x05);
    assert_eq!(body[74], 0x0c);
    assert_eq!(u16::from_le_bytes([body[80], body[81]]), 112);
}

#[tokio::test]
async fn rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6001".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service
        .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, Body::from("bind-bytes"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
}

#[tokio::test]
async fn rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6004".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service
        .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, Body::from("bind-bytes"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
}

#[tokio::test]
async fn rpc_proxy_opens_authenticated_referral_in_data_channel_without_buffering_body() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6002".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service
        .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, Body::from("bind-bytes"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
}

#[test]
fn rpc_proxy_classifies_referral_endpoint_as_streaming_in_data_channel() {
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6002".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    assert!(is_rpc_proxy_in_data_channel_request(
        &method, &uri, &headers
    ));
}

#[test]
fn rpc_proxy_classifies_zero_length_endpoint_in_data_as_echo_probe() {
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6001".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));
    headers.insert("content-length", HeaderValue::from_static("0"));

    assert!(!is_rpc_proxy_in_data_channel_request(
        &method, &uri, &headers
    ));
}

#[tokio::test]
async fn rpc_proxy_answers_zero_length_endpoint_in_data_echo_probe() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6001".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));
    headers.insert("content-length", HeaderValue::from_static("0"));

    let response = service.handle_rpc_proxy(&method, &uri, &headers, b"").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("echo"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 20);
}

#[test]
fn rpc_proxy_in_channel_endpoint_ping_request_gets_success_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];

    let mut buffer = request.to_vec();
    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("endpoint response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 52);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 0);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        28
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0
    );
}

#[test]
fn rpc_proxy_in_channel_bind_request_gets_bind_ack_response() {
    let bind = hex_bytes(
        "05000b1310000000a400280003000000\
         f80ff80f010000000200000002000100\
         e0f544153c61d11193df00c04fd7bd0901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         03000100e0f544153c61d11193df00c04fd7bd0901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut buffer = bind;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("bind ack response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 136);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 48);
    assert_eq!(response[28], 2);
    assert_eq!(
        &response[36..56],
        &[
            0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11, 0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10,
            0x48, 0x60, 0x02, 0x00, 0x00, 0x00
        ]
    );
    assert_eq!(u16::from_le_bytes([response[56], response[57]]), 2);
    assert_eq!(response[80], 10);
    assert_eq!(response[81], 2);
    assert_eq!(&response[88..96], b"NTLMSSP\0");
}

#[test]
fn rpc_proxy_in_channel_bind_ack_negotiates_bind_time_features() {
    let bind = hex_bytes(
        "05000b0310000000d000280096000000\
         f80ff80f000000000300000000000100\
         80bda8af8a7dc911bef408002b10298901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         0100010080bda8af8a7dc911bef408002b10298901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0200010080bda8af8a7dc911bef408002b10298901000000\
         2c1cb76c12984045030000000000000001000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut buffer = bind;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("bind ack response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        150
    );
    assert_eq!(response[28], 3);
    assert_eq!(u16::from_le_bytes(response[32..34].try_into().unwrap()), 0);
    assert_eq!(u16::from_le_bytes(response[56..58].try_into().unwrap()), 2);
    assert_eq!(u16::from_le_bytes(response[80..82].try_into().unwrap()), 3);
    assert_eq!(u16::from_le_bytes(response[82..84].try_into().unwrap()), 0);
    assert_eq!(&response[84..104], &[0; 20]);
}

#[test]
fn rpc_proxy_referral_endpoint_management_ping_uses_bound_context_before_rfri_heuristic() {
    let endpoint_query = "mail.management.example.test:6002";
    let bind = hex_bytes(
        "05000b0310000000d000280002000000\
         f80ff80f000000000300000000000100\
         80bda8af8a7dc911bef408002b10298901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         0100010080bda8af8a7dc911bef408002b10298901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0200010080bda8af8a7dc911bef408002b10298901000000\
         2c1cb76c12984045030000000000000001000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut buffer = bind;

    let bind_response =
        rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
            .expect("management bind response");

    assert_eq!(bind_response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([
            bind_response[12],
            bind_response[13],
            bind_response[14],
            bind_response[15]
        ]),
        2
    );

    let mut auth3 = vec![0u8; 250];
    auth3[0..8].copy_from_slice(&[0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00]);
    auth3[8..10].copy_from_slice(&250u16.to_le_bytes());
    auth3[10..12].copy_from_slice(&0x00deu16.to_le_bytes());
    auth3[12..16].copy_from_slice(&2u32.to_le_bytes());
    let management = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ];
    let mut request = auth3;
    request.extend_from_slice(&management);

    let response = rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut request)
        .expect("management ping response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        4
    );
    assert!(!contains_bytes(&response, b"mail.management.example.test"));
}

#[test]
fn rpc_proxy_in_channel_alter_context_request_gets_alter_context_response() {
    let alter_context = hex_bytes(
        "05000e03100000007400000004000000\
         f80ff80f010000000200000002000100\
         00dbf1a447ca6710b31f00dd010662da00005100\
         045d888aeb1cc9119fe808002b10486002000000\
         0300010000dbf1a447ca6710b31f00dd010662da00005100\
         33057171babe37498319b5dbef9ccc3601000000",
    );
    let mut buffer = alter_context;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("alter context response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x0f, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 136);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 48);
    assert_eq!(response[28], 2);
    assert_eq!(u16::from_le_bytes([response[56], response[57]]), 2);
    assert_eq!(&response[88..96], b"NTLMSSP\0");
}

#[test]
fn rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response() {
    let mut buffer = emsmdb_rpc_request(51, 10, 160);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("emsmdb connect response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        51
    );
    assert_eq!(&response[24..28], &[0; 4]);
    assert_eq!(&response[28..44], Uuid::nil().as_bytes());
    assert_eq!(
        u32::from_le_bytes(response[44..48].try_into().unwrap()),
        60_000
    );
    assert_eq!(*response.last().unwrap(), 0);
}

#[test]
fn rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response() {
    let mut buffer = emsmdb_rpc_request(52, 11, 160);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("emsmdb rpc ext2 response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        52
    );
    assert_eq!(&response[24..28], &[0; 4]);
    assert_eq!(&response[28..44], Uuid::nil().as_bytes());
    assert!(response
        .windows(8)
        .any(|window| window == [0, 0, 4, 0, 0, 0, 0, 0]));
}

#[test]
fn rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context() {
    let mut buffer = emsmdb_rpc_request(53, 1, 64);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("emsmdb disconnect response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        53
    );
    assert_eq!(&response[24..44], &[0; 20]);
    assert_eq!(u32::from_le_bytes(response[44..48].try_into().unwrap()), 0);
}

#[test]
fn rpc_proxy_mailstore_management_stats_accepts_rca_short_stub() {
    let mut buffer = vec![0u8; 626];
    buffer[0..64].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ]);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("management stats response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(u32::from_le_bytes(response[24..28].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[28..32].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[48..52].try_into().unwrap()), 0);
    assert_eq!(buffer.len(), 562);
}

#[tokio::test]
async fn rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut connect = emsmdb_rpc_request(61, 10, 160);
    let connect_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut connect,
    )
    .await
    .expect("connect response");
    let context = rpc_response_context(&connect_response);

    let logon_request = rpc_proxy_bootstrap_logon_execute_rop(&principal.email);
    let mut execute = emsmdb_rpc_ext2_request(62, &context, &logon_request);
    let execute_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("execute response");
    let rop_response = rpc_response_rpc_header_ext(&execute_response);

    let static_marker = [b"LPEEMSMDB".as_slice(), b"CTX0001".as_slice()].concat();
    assert!(contains_bytes(
        &rop_response,
        &FakeStore::account().account_id.to_bytes_le()
    ));
    assert!(!contains_bytes(&execute_response, &static_marker));
}

#[tokio::test]
async fn rpc_proxy_emsmdb_query_rows_reads_root_hierarchy_without_ipm_children() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 7;
    let archive = FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "custom", "Archive");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut connect = emsmdb_rpc_request(63, 10, 160);
    let connect_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut connect,
    )
    .await
    .expect("connect response");
    let context = rpc_response_context(&connect_response);

    let logon_request = rpc_proxy_bootstrap_logon_execute_rop(&principal.email);
    let mut execute = emsmdb_rpc_ext2_request(64, &context, &logon_request);
    rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("logon response");

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
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());
    let table_request = rpc_proxy_wrapped_rop_buffer(&rops, &[1, u32::MAX, u32::MAX]);
    let mut execute = emsmdb_rpc_ext2_request(65, &context, &table_request);
    let table_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("table response");
    let rop_response = rpc_response_rpc_header_ext(&table_response);

    assert!(contains_bytes(
        &rop_response,
        &utf16z("Top of Information Store")
    ));
    assert!(contains_bytes(&rop_response, &utf16z("Common Views")));
    assert!(!contains_bytes(&rop_response, &utf16z("Inbox")));
    assert!(!contains_bytes(&rop_response, &utf16z("Archive")));
}

#[tokio::test]
async fn rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut execute = emsmdb_rpc_request(66, 11, 160);

    let execute_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("execute fault");

    assert_eq!(execute_response[0..4], [0x05, 0x00, 0x03, 0x03]);
    assert_eq!(rpc_response_call_id(&execute_response), 66);
    assert_eq!(rpc_response_fault_status(&execute_response), 5);
    assert!(!contains_bytes(
        &execute_response,
        &[0, 0, 4, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let context = [0u8; 20];
    let logon_request = rpc_proxy_bootstrap_logon_execute_rop(&principal.email);
    let mut execute = emsmdb_rpc_ext2_request(67, &context, &logon_request);

    let execute_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("execute fault");

    assert_eq!(execute_response[0..4], [0x05, 0x00, 0x03, 0x03]);
    assert_eq!(rpc_response_call_id(&execute_response), 67);
    assert_eq!(rpc_response_fault_status(&execute_response), 5);
    assert!(!contains_bytes(
        &execute_response,
        FakeStore::account().account_id.as_bytes()
    ));
}

#[test]
fn rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack() {
    let endpoint_query = "mail.example.test:6001";
    mark_rpc_proxy_out_endpoint_bind_ack(endpoint_query);
    let bind = hex_bytes(
        "05000b1310000000a400280003000000\
         f80ff80f010000000200000002000100\
         e0f544153c61d11193df00c04fd7bd0901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         03000100e0f544153c61d11193df00c04fd7bd0901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut auth3 = vec![0u8; 250];
    auth3[0..8].copy_from_slice(&[0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00]);
    auth3[8..10].copy_from_slice(&250u16.to_le_bytes());
    auth3[10..12].copy_from_slice(&0x00deu16.to_le_bytes());
    auth3[12..16].copy_from_slice(&2u32.to_le_bytes());
    let management = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&bind);
    buffer.extend_from_slice(&auth3);
    buffer.extend_from_slice(&management);

    let response = rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
        .expect("management response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
}

#[test]
fn rpc_proxy_address_book_in_channel_answers_actual_bind_before_management_probe() {
    let endpoint_query = "mail.address-book-bind.example.test:6004";
    let bind = hex_bytes(
        "05000b0310000000d000280030000000\
         f80ff80f000000000300000000000100\
         80bda8af8a7dc911bef408002b10298901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         0100010080bda8af8a7dc911bef408002b10298901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0200010080bda8af8a7dc911bef408002b10298901000000\
         2c1cb76c12984045030000000000000001000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut auth3 = vec![0u8; 250];
    auth3[0..8].copy_from_slice(&[0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00]);
    auth3[8..10].copy_from_slice(&250u16.to_le_bytes());
    auth3[10..12].copy_from_slice(&0x00deu16.to_le_bytes());
    auth3[12..16].copy_from_slice(&48u32.to_le_bytes());
    let management = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&bind);
    buffer.extend_from_slice(&auth3);
    buffer.extend_from_slice(&management);

    let bind_response =
        rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
            .expect("bind response");

    assert_eq!(bind_response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([
            bind_response[12],
            bind_response[13],
            bind_response[14],
            bind_response[15]
        ]),
        48
    );

    let response = rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
        .expect("management response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        48
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_bind_request_gets_context_handle_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x60, 0x00, 0x10, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x2c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0xe4, 0x04, 0x00, 0x00, 0x09, 0x04, 0x00, 0x00, 0x09, 0x04, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut buffer = request.to_vec();

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("nspi bind response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 76);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 16);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        28
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        0
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        0
    );
    assert_eq!(&response[32..40], b"LPE\0NSPI");
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0
    );
    assert_eq!(&response[52..60], &[0x0a, 0x02, 0x00, 0x00, 0, 0, 0, 0]);
}

#[test]
fn rpc_proxy_in_channel_nspi_update_stat_request_gets_success_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x60, 0x00, 0x10, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x2c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x02, 0x00,
    ];
    let mut buffer = request.to_vec();
    buffer.resize(96, 0);

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("nspi update stat response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u16::from_le_bytes([response[8], response[9]]) as usize,
        response.len()
    );
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        44
    );
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0x04e4
    );
    assert_eq!(
        u32::from_le_bytes([response[56], response[57], response[58], response[59]]),
        0x0409
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0xd0, 0x00, 0x10, 0x00, 0x04, 0x00, 0x00,
        0x00, 0x98, 0x00, 0x00, 0x00, 0x02, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4c, 0x50,
        0x45, 0x00, 0x4e, 0x53, 0x50, 0x49, 0x43, 0x54, 0x58, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00,
    ];
    let mut buffer = request.to_vec();
    buffer.resize(208, 0);
    buffer[72..76].copy_from_slice(&0x3003_001eu32.to_le_bytes());
    buffer[76..80].copy_from_slice(&0x3001_001eu32.to_le_bytes());
    let requested_name: Vec<u8> = "=SMTP:fabien@l-p-e.ch\0"
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    buffer[112..112 + requested_name.len()].copy_from_slice(&requested_name);

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u16::from_le_bytes([response[8], response[9]]) as usize,
        response.len()
    );
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        (response.len() - 24) as u32
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[32], response[33], response[34], response[35]]),
        1
    );
    assert!(response
        .windows(b"fabien@l-p-e.ch".len())
        .any(|window| window == b"fabien@l-p-e.ch"));
    assert!(response
        .windows(b"Fabien".len())
        .any(|window| window == b"Fabien"));
    assert!(response.windows(12).any(|window| {
        window[0..4] == 0x3003_001eu32.to_le_bytes()
            && window[4..8] == 0u32.to_le_bytes()
            && window[8..12] == 0x001eu32.to_le_bytes()
    }));
    assert!(response.windows(12).any(|window| {
        window[0..4] == 0x3001_001eu32.to_le_bytes()
            && window[4..8] == 0u32.to_le_bytes()
            && window[8..12] == 0x001eu32.to_le_bytes()
    }));
    let return_offset = response.len() - 4;
    assert_eq!(
        u32::from_le_bytes([
            response[return_offset],
            response[return_offset + 1],
            response[return_offset + 2],
            response[return_offset + 3]
        ]),
        0
    );
}

#[test]
fn rpc_proxy_address_book_endpoint_resolves_names_on_alternate_context_id() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0xd0, 0x00, 0x10, 0x00, 0x04, 0x00, 0x00,
        0x00, 0x98, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4c, 0x50,
        0x45, 0x00, 0x4e, 0x53, 0x50, 0x49, 0x43, 0x54, 0x58, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00,
    ];
    let mut buffer = request.to_vec();
    buffer.resize(208, 0);
    buffer[72..76].copy_from_slice(&0x3003_001eu32.to_le_bytes());
    buffer[76..80].copy_from_slice(&0x3001_001eu32.to_le_bytes());
    let requested_name: Vec<u8> = "=SMTP:fabien@l-p-e.ch\0"
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    buffer[112..112 + requested_name.len()].copy_from_slice(&requested_name);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6004", &mut buffer)
            .expect("resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
    assert!(response
        .windows(b"fabien@l-p-e.ch".len())
        .any(|window| window == b"fabien@l-p-e.ch"));
}

#[tokio::test]
async fn rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 626];
    buffer[0..16].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x4d, 0x00, 0x00,
        0x00,
    ]);
    buffer[16..24].copy_from_slice(&[0x10, 0x00, 0x00, 0x00, 0x07, 0x00, 0x63, 0x00]);
    let requested_name: Vec<u8> = "=SMTP:alice@example.test\0"
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    buffer[320..320 + requested_name.len()].copy_from_slice(&requested_name);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6004",
        &mut buffer,
    )
    .await
    .expect("fallback resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        77
    );
    assert!(response
        .windows(b"alice@example.test".len())
        .any(|window| window == b"alice@example.test"));
    assert!(buffer.is_empty());
}

#[tokio::test]
async fn rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 250];
    buffer[0..16].copy_from_slice(&[
        0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00, 0xfa, 0x00, 0xde, 0x00, 0x7f, 0x00, 0x00,
        0x00,
    ]);
    buffer[16..24].copy_from_slice(&[0xf8, 0x0f, 0xf8, 0x0f, 0x0a, 0x02, 0x00, 0x00]);
    let authenticated_name: Vec<u8> = "test@l-p-e.ch\0"
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    buffer[80..80 + authenticated_name.len()].copy_from_slice(&authenticated_name);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6004",
        &mut buffer,
    )
    .await;

    assert!(response.is_none());
    assert!(buffer.is_empty());
}

#[test]
fn rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu() {
    let chunk = hex_bytes(
        "05001403100000001c00000000000000020001000500000030750000\
         0500000310000000d000100009000000980000000200140000000000\
         4c5045004e535049435458000000000100000000000000000000000000000000000000000000000000000000\
         e404000009040000090400000000020003000000020000000000000002000000\
         1e0003301e000130010000000100000004000200140000000000000014000000\
         3d0053004d00540050003a00740065007300740040006c002d0070002d0065002e00630068000000\
         00000000000000000a0208000000000001000000000000000000000000000000",
    );
    let mut buffer = chunk;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("resolve names response");

    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        9
    );
    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
}

#[test]
fn rpc_proxy_in_channel_nspi_unbind_request_gets_success_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x50, 0x00, 0x10, 0x00, 0x05, 0x00, 0x00,
        0x00, 0x18, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00,
    ];
    let mut buffer = request.to_vec();
    buffer.resize(80, 0);

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("nspi unbind response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        5
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        24
    );
    assert_eq!(
        u32::from_le_bytes([response[44], response[45], response[46], response[47]]),
        0
    );
}

#[tokio::test]
async fn rpc_proxy_address_book_management_stats_accepts_rca_short_stub() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 626];
    buffer[0..64].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x7f, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ]);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6004",
        &mut buffer,
    )
    .await
    .expect("address book management stats response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        127
    );
    assert_eq!(u32::from_le_bytes(response[24..28].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[28..32].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[48..52].try_into().unwrap()), 0);
    assert_eq!(buffer.len(), 562);
}

#[test]
fn rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses() {
    for (opnum, call_id) in [
        (3u16, 11u32),
        (4, 12),
        (5, 13),
        (6, 14),
        (7, 15),
        (8, 16),
        (9, 17),
        (10, 18),
        (12, 19),
        (13, 20),
        (16, 21),
        (17, 22),
        (18, 23),
        (19, 24),
    ] {
        let mut buffer = nspi_rpc_request(call_id, opnum, 96);

        let response = rpc_proxy_in_channel_response_for_buffer(&mut buffer)
            .unwrap_or_else(|| panic!("nspi opnum {opnum} response"));

        assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03], "opnum {opnum}");
        assert_eq!(
            u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
            call_id,
            "opnum {opnum}"
        );
        assert_eq!(
            u32::from_le_bytes([
                response[response.len() - 4],
                response[response.len() - 3],
                response[response.len() - 2],
                response[response.len() - 1]
            ]),
            0,
            "opnum {opnum}"
        );
    }
}

#[test]
fn rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response() {
    let mut buffer = nspi_rpc_request(26, 17, 96);
    buffer[52..56].copy_from_slice(&0x3001_001fu32.to_le_bytes());
    buffer[56..60].copy_from_slice(&0x3003_001fu32.to_le_bytes());

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("get names from ids response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        26
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        0
    );
    assert_ne!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        0
    );
    assert_eq!(
        u32::from_le_bytes([response[32], response[33], response[34], response[35]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[36], response[37], response[38], response[39]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0x3001_001f
    );
    assert_eq!(
        u32::from_le_bytes([response[60], response[61], response[62], response[63]]),
        0x3003_001f
    );
    assert_eq!(
        u32::from_le_bytes([
            response[response.len() - 4],
            response[response.len() - 3],
            response[response.len() - 2],
            response[response.len() - 1]
        ]),
        0
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response() {
    let mut buffer = nspi_rpc_request(27, 19, 160);
    buffer[72..76].copy_from_slice(&0x3003_001eu32.to_le_bytes());
    buffer[76..80].copy_from_slice(&0x3001_001eu32.to_le_bytes());
    buffer[96..117].copy_from_slice(b"=SMTP:alias@l-p-e.ch\0");

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        27
    );
    assert!(response
        .windows(b"alias@l-p-e.ch".len())
        .any(|window| window == b"alias@l-p-e.ch"));
    assert!(response
        .windows(b"Alias".len())
        .any(|window| window == b"Alias"));
}

#[test]
fn rpc_proxy_in_channel_referral_opnums_get_server_name_responses() {
    for (opnum, call_id) in [(0u16, 31u32), (1, 32)] {
        let mut buffer = rfri_rpc_request(call_id, opnum, 96);

        let response =
            rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6002", &mut buffer)
                .unwrap_or_else(|| panic!("rfri opnum {opnum} response"));

        assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03], "opnum {opnum}");
        assert_eq!(
            u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
            call_id,
            "opnum {opnum}"
        );
        if opnum == 0 {
            assert_eq!(
                u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
                0,
                "RfrGetNewDSA ppszUnused"
            );
            assert_ne!(
                u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
                0,
                "RfrGetNewDSA ppszServer outer pointer"
            );
            assert_ne!(
                u32::from_le_bytes([response[32], response[33], response[34], response[35]]),
                0,
                "RfrGetNewDSA ppszServer string pointer"
            );
        } else {
            assert_ne!(
                u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
                0,
                "RfrGetFQDNFromServerDN ppszServerFQDN string pointer"
            );
        }
        assert!(response
            .windows(b"mail.example.test".len())
            .any(|window| window == b"mail.example.test"));
        assert_eq!(
            u32::from_le_bytes([
                response[response.len() - 4],
                response[response.len() - 3],
                response[response.len() - 2],
                response[response.len() - 1]
            ]),
            0,
            "opnum {opnum}"
        );
    }
}

#[tokio::test]
async fn rpc_proxy_referral_get_fqdn_accepts_rca_short_stub() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 626];
    buffer[0..64].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x7f, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ]);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6002",
        &mut buffer,
    )
    .await
    .expect("referral response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        127
    );
    assert!(response
        .windows(b"mail.example.test".len())
        .any(|window| window == b"mail.example.test"));
    assert_eq!(buffer.len(), 562);
}

fn nspi_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8> {
    rpc_request(call_id, 2, opnum, fragment_length)
}

fn rfri_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8> {
    rpc_request(call_id, 0, opnum, fragment_length)
}

fn emsmdb_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8> {
    rpc_request(call_id, 3, opnum, fragment_length)
}

fn emsmdb_rpc_ext2_request(call_id: u32, context: &[u8], rop_buffer: &[u8]) -> Vec<u8> {
    let mut stub = Vec::new();
    stub.extend_from_slice(context);
    stub.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
    stub.extend_from_slice(&0u32.to_le_bytes());
    stub.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
    stub.extend_from_slice(rop_buffer);
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
    let fragment_length = 24 + stub.len();
    let mut request = rpc_request(call_id, 3, 11, fragment_length);
    request[16..20].copy_from_slice(&(stub.len() as u32).to_le_bytes());
    request[24..].copy_from_slice(&stub);
    request
}

fn rpc_request(call_id: u32, context_id: u16, opnum: u16, fragment_length: usize) -> Vec<u8> {
    let mut request = vec![0u8; fragment_length];
    request[0..8].copy_from_slice(&[0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00]);
    request[8..10].copy_from_slice(&(fragment_length as u16).to_le_bytes());
    request[10..12].copy_from_slice(&0x0010u16.to_le_bytes());
    request[12..16].copy_from_slice(&call_id.to_le_bytes());
    request[16..20].copy_from_slice(&(fragment_length as u32 - 24).to_le_bytes());
    request[20..22].copy_from_slice(&context_id.to_le_bytes());
    request[22..24].copy_from_slice(&opnum.to_le_bytes());
    request
}

fn rpc_response_context(response: &[u8]) -> [u8; 20] {
    response[24..44].try_into().unwrap()
}

fn rpc_response_call_id(response: &[u8]) -> u32 {
    u32::from_le_bytes(response[12..16].try_into().unwrap())
}

fn rpc_response_fault_status(response: &[u8]) -> u32 {
    u32::from_le_bytes(response[24..28].try_into().unwrap())
}

fn rpc_response_rpc_header_ext(response: &[u8]) -> Vec<u8> {
    let offset = response
        .windows(4)
        .position(|window| window == [0, 0, 4, 0])
        .expect("RPC_HEADER_EXT response");
    let size = u16::from_le_bytes(response[offset + 4..offset + 6].try_into().unwrap()) as usize;
    response[offset..offset + 8 + size].to_vec()
}

#[test]
fn rpc_proxy_in_channel_scans_endpoint_ping_after_auth_fragment() {
    let auth = [
        0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00, 0xfa, 0x00, 0xde, 0x00, 0x02, 0x00, 0x00,
        0x00,
    ];
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut chunk = Vec::new();
    chunk.extend_from_slice(&auth);
    chunk.extend_from_slice(&[0u8; 234]);
    chunk.extend_from_slice(&request);

    let mut buffer = chunk;
    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("endpoint response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
}

#[test]
fn rpc_proxy_in_channel_buffers_split_endpoint_ping_request() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut buffer = request[..18].to_vec();

    assert!(rpc_proxy_in_channel_response_for_buffer(&mut buffer).is_none());
    assert_eq!(buffer, request[..18]);

    buffer.extend_from_slice(&request[18..]);
    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("endpoint response");

    assert!(buffer.is_empty());
    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
}

#[tokio::test]
async fn rpc_proxy_accepts_authenticated_rca_probe_without_405() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();

    let response = service
        .handle_rpc_proxy(&Method::GET, &uri, &bearer_headers(), b"")
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("auth-accepted"))
    );
    let body = response_text(response).await;
    assert!(body.contains("Use MAPI over HTTP for mailbox access"));
}
