use super::*;

#[tokio::test]
async fn mapi_over_http_sync_import_deletes_success_response_is_exactly_six_bytes() {
    let message_id = "74747474-7474-4747-8747-747474747474";
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "MS-OXCROPS 2.2.13.5.2 exact response",
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents
        0x74, 0x00, 0x02, 0x02, // RopSynchronizationImportDeletes, HardDelete
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0000_1102u32.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    let source_key =
        crate::mapi::identity::source_key_for_object_id(test_mapi_message_id(message_id));
    rops.extend_from_slice(&(source_key.len() as u16).to_le_bytes());
    rops.extend_from_slice(&source_key);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(
        response_rops,
        [
            0x02, 0x01, 0, 0, 0, 0, 0, 0, // RopOpenFolder
            0x7E, 0x02, 0, 0, 0, 0, // RopSynchronizationOpenCollector
            // [MS-OXCROPS] 2.2.13.5.2 and [MS-OXCFXICS] 2.2.3.2.4.5.2:
            // RopId, InputHandleIndex, ReturnValue; no PartialCompletion byte.
            0x74, 0x02, 0, 0, 0, 0,
        ]
    );
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[Uuid::parse_str(message_id).unwrap()]
    );
}
