use super::*;

#[tokio::test]
async fn mapi_over_http_open_and_create_require_logon_or_folder_input_objects() {
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
            "Wrong input object",
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

    let folder_id = crate::mapi::identity::INBOX_FOLDER_ID;
    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, folder_id);
    append_rop_open_message(&mut rops, 1, 2, folder_id, test_mapi_message_id(message_id));
    append_rop_open_folder(&mut rops, 2, 3, folder_id);
    append_rop_open_message(&mut rops, 2, 4, folder_id, test_mapi_message_id(message_id));
    append_rop_create_message(&mut rops, 2, 5, folder_id);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert_ne!(response_handles[2], u32::MAX, "message must be live");

    for (rop_id, output_index, error) in [
        (0x02, 3, [0x02, 0x01, 0x04, 0x80]),
        (0x03, 4, [0xB9, 0x04, 0x00, 0x00]),
        (0x06, 5, [0x02, 0x01, 0x04, 0x80]),
    ] {
        let mut expected = vec![rop_id, output_index];
        expected.extend_from_slice(&error);
        assert!(
            contains_bytes(&response_rops, &expected),
            "ROP {rop_id:#04x} accepted the live Message: {response_rops:02x?}"
        );
        assert_eq!(
            response_handles
                .get(usize::from(output_index))
                .copied()
                .unwrap_or(u32::MAX),
            u32::MAX,
            "a rejected ROP must not bind its output handle"
        );
    }
}
